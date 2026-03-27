"""
ai_enrichment_service.py
─────────────────────────
Groq-powered AI enrichment modules.
All modules accept a clean profile dict and return structured JSON + token_usage.
Optional system_prompt / user_template overrides allow per-org customisation
(stored in DB via enrichment_config_service).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from datetime import datetime, timezone

import httpx

logger = logging.getLogger(__name__)

GROQ_URL    = "https://api.groq.com/openai/v1/chat/completions"
MODEL_FAST  = "llama-3.1-8b-instant"
MODEL_SMART = "llama-3.3-70b-versatile"

_STRICT = (
    "STRICT RULES:\n"
    "- Return ONLY valid JSON. No markdown, no explanation.\n"
    "- If a field cannot be determined from the data, use null.\n"
    "- Keep values concise and factual."
)


# ── Key helpers ───────────────────────────────────────────────────────────────

def _groq_key() -> str:
    try:
        import keys_service as ks
        return ks.get("GROQ_API_KEY") or os.getenv("GROQ_API_KEY", "")
    except Exception:
        return os.getenv("GROQ_API_KEY", "")


def _smart_model() -> str:
    """Return the configured smart model, falling back to MODEL_SMART default."""
    try:
        import keys_service as ks
        return ks.get("GROQ_LLM_MODEL") or os.getenv("GROQ_LLM_MODEL", MODEL_SMART)
    except Exception:
        return os.getenv("GROQ_LLM_MODEL", MODEL_SMART)


# ── Profile cleaning ──────────────────────────────────────────────────────────

def clean_profile(raw) -> dict:
    """Accept BrightData profile dict or array. Strip heavy/noisy fields."""
    if isinstance(raw, list):
        raw = raw[0] if raw else {}
    p = dict(raw)
    for k in ("_activity_full", "recommendations", "certifications_details",
              "skills_label", "volunteer_experience", "accomplishments"):
        p.pop(k, None)
    if isinstance(p.get("activity"), list):
        p["activity"] = p["activity"][:15]
    if isinstance(p.get("posts"), list):
        p["posts"] = p["posts"][:10]
    if isinstance(p.get("education"), list):
        p["education"] = p["education"][:5]
    if isinstance(p.get("skills"), list):
        p["skills"] = p["skills"][:20]
    if isinstance(p.get("about"), str) and len(p["about"]) > 1000:
        p["about"] = p["about"][:1000]
    return p


def _ps(p: dict) -> str:
    """Compact JSON string for prompt."""
    return json.dumps(p, ensure_ascii=False)


# ── Per-module smart profile extractors ───────────────────────────────────────
# Each extractor pulls ONLY the fields relevant to that module.
# This reduces tokens by 60-80% and improves result quality.

def _first_n_text(items: list, n: int, text_key: str = "text", max_chars: int = 200) -> list[str]:
    """Return first n non-empty text strings, truncated to max_chars. Items may be strings or dicts."""
    out = []
    for item in (items or []):
        if isinstance(item, str):
            t = item.strip()
        elif isinstance(item, dict):
            t = (item.get(text_key) or item.get("title") or item.get("article_title") or "").strip()
        else:
            continue
        if t:
            out.append(t[:max_chars])
        if len(out) >= n:
            break
    return out


def _experience_summary(positions: list) -> list[dict]:
    out = []
    for p in (positions or [])[:4]:
        if isinstance(p, str):
            out.append({"role": p})
            continue
        if not isinstance(p, dict):
            continue
        entry = {}
        if p.get("title"):    entry["title"]    = p["title"]
        if p.get("company"):  entry["company"]   = p["company"]
        if p.get("duration"): entry["duration"]  = p["duration"]
        if p.get("date_range"): entry["duration"] = p["date_range"]
        if entry:
            out.append(entry)
    return out


def _edu_summary(education: list) -> list[str]:
    out = []
    for e in (education or [])[:3]:
        if isinstance(e, str):
            out.append(e)
            continue
        if not isinstance(e, dict):
            continue
        parts = [
            e.get("field_of_study") or e.get("degree") or e.get("title") or "",
            e.get("school") or e.get("institute") or "",
        ]
        s = " @ ".join(x for x in parts if x)
        if s:
            out.append(s)
    return out


def _extract_identity(p: dict) -> dict:
    """Fields needed: basic identity only."""
    return {k: v for k, v in {
        "full_name":    f"{p.get('first_name','')} {p.get('last_name','')}".strip() or None,
        "first_name":   p.get("first_name"),
        "last_name":    p.get("last_name"),
        "title":        p.get("title") or p.get("position") or p.get("headline"),
        "company":      p.get("current_company_name") or p.get("current_company"),
        "linkedin_url": p.get("linkedin_url") or p.get("url"),
        "location":     p.get("city") or p.get("location"),
        "country":      p.get("country_code") or p.get("country"),
        "timezone":     p.get("timezone"),
        "followers":    p.get("followers"),
        "connections":  p.get("connections"),
    }.items() if v is not None}


def _extract_contact(p: dict) -> dict:
    """Fields needed: contact discovery (company domain is key for email inference)."""
    return {k: v for k, v in {
        "full_name":      f"{p.get('first_name','')} {p.get('last_name','')}".strip() or None,
        "title":          p.get("title") or p.get("position"),
        "company":        p.get("current_company_name") or p.get("current_company"),
        "company_domain": p.get("current_company_domain") or p.get("company_domain"),
        "email":          p.get("email"),
        "phone":          p.get("phone"),
        "linkedin_url":   p.get("linkedin_url") or p.get("url"),
        "location":       p.get("city") or p.get("location"),
        "country_code":   p.get("country_code"),
    }.items() if v is not None}


def _extract_scores(p: dict) -> dict:
    """~1000 tokens: engagement signals + about snippet + top posts for scoring accuracy."""
    posts    = p.get("posts") or []
    activity = p.get("activity") or []
    return {k: v for k, v in {
        "title":          p.get("title") or p.get("position"),
        "company":        p.get("current_company_name") or p.get("current_company"),
        "industry":       p.get("industry") or p.get("company_industry"),
        "location":       p.get("city") or p.get("location"),
        "country":        p.get("country_code"),
        "followers":      p.get("followers"),
        "connections":    p.get("connections"),
        "about":          (p.get("about") or "")[:400] or None,
        "skills":         (p.get("skills") or [])[:12],
        "post_count":     len(posts),
        "activity_count": len(activity),
        "recent_posts":   _first_n_text(posts, 4, max_chars=150),
        "experience":     _experience_summary(p.get("positions") or p.get("experience")),
        "education":      _edu_summary(p.get("educations_details") or p.get("education")),
    }.items() if v is not None}


def _extract_icp_match(p: dict) -> dict:
    """~1000 tokens: seniority + company + about + skills for accurate ICP verdict."""
    return {k: v for k, v in {
        "title":             p.get("title") or p.get("position"),
        "company":           p.get("current_company_name") or p.get("current_company"),
        "industry":          p.get("industry") or p.get("company_industry"),
        "location":          p.get("city") or p.get("location"),
        "country":           p.get("country_code"),
        "followers":         p.get("followers"),
        "connections":       p.get("connections"),
        "about":             (p.get("about") or "")[:500] or None,
        "skills":            (p.get("skills") or [])[:15],
        "experience":        _experience_summary(p.get("positions") or p.get("experience")),
        "education":         _edu_summary(p.get("educations_details") or p.get("education")),
        "honors_and_awards": len(p.get("honors_and_awards") or []),
    }.items() if v is not None}


def _extract_behavioural(p: dict) -> dict:
    """~1000 tokens: posts + activity at 150 chars each — topic signals, not full text."""
    return {k: v for k, v in {
        "title":          p.get("title") or p.get("position"),
        "about":          (p.get("about") or "")[:400] or None,
        "posts":          _first_n_text(p.get("posts"),    8, max_chars=150),
        "activity_liked": _first_n_text(p.get("activity"), 10, max_chars=150),
    }.items() if v is not None}


def _extract_pitch(p: dict) -> dict:
    """~1000 tokens: rich role context + posts + activity for strategic pitch quality."""
    return {k: v for k, v in {
        "title":          p.get("title") or p.get("position"),
        "company":        p.get("current_company_name") or p.get("current_company"),
        "industry":       p.get("industry") or p.get("company_industry"),
        "location":       p.get("city") or p.get("location"),
        "about":          (p.get("about") or "")[:500] or None,
        "skills":         (p.get("skills") or [])[:10],
        "posts":          _first_n_text(p.get("posts"),    6, max_chars=200),
        "activity_liked": _first_n_text(p.get("activity"), 8, max_chars=150),
        "experience":     _experience_summary(p.get("positions") or p.get("experience")),
    }.items() if v is not None}


def _extract_activity(p: dict) -> dict:
    """~1000 tokens: post + liked activity titles for warm-signal detection."""
    return {k: v for k, v in {
        "title":          p.get("title") or p.get("position"),
        "posts":          _first_n_text(p.get("posts"),    10, max_chars=150),
        "activity_liked": _first_n_text(p.get("activity"), 12, max_chars=150),
    }.items() if v is not None}


def _extract_tags(p: dict) -> dict:
    """~1000 tokens: role + skills + about + post topics for accurate CRM tagging."""
    return {k: v for k, v in {
        "title":          p.get("title") or p.get("position"),
        "company":        p.get("current_company_name") or p.get("current_company"),
        "industry":       p.get("industry") or p.get("company_industry"),
        "location":       p.get("city") or p.get("location"),
        "country":        p.get("country_code"),
        "followers":      p.get("followers"),
        "about":          (p.get("about") or "")[:400] or None,
        "skills":         (p.get("skills") or [])[:15],
        "posts":          _first_n_text(p.get("posts"),    8, max_chars=150),
        "activity_liked": _first_n_text(p.get("activity"), 8, max_chars=150),
        "experience":     _experience_summary(p.get("positions") or p.get("experience")),
    }.items() if v is not None}


def _extract_outreach(p: dict) -> dict:
    """~1000 tokens: rich personal context for hyper-personalized outreach writing."""
    return {k: v for k, v in {
        "first_name":     p.get("first_name"),
        "last_name":      p.get("last_name"),
        "title":          p.get("title") or p.get("position"),
        "company":        p.get("current_company_name") or p.get("current_company"),
        "industry":       p.get("industry") or p.get("company_industry"),
        "location":       p.get("city") or p.get("location"),
        "about":          (p.get("about") or "")[:600] or None,
        "skills":         (p.get("skills") or [])[:8],
        "recent_posts":   _first_n_text(p.get("posts"),    6, max_chars=250),
        "activity_liked": _first_n_text(p.get("activity"), 6, max_chars=200),
        "experience":     _experience_summary(p.get("positions") or p.get("experience")),
    }.items() if v is not None}


def _extract_persona(p: dict) -> dict:
    """~1000 tokens: career + behavioural signals for psychographic persona analysis."""
    return {k: v for k, v in {
        "title":          p.get("title") or p.get("position"),
        "company":        p.get("current_company_name") or p.get("current_company"),
        "industry":       p.get("industry") or p.get("company_industry"),
        "location":       p.get("city") or p.get("location"),
        "followers":      p.get("followers"),
        "connections":    p.get("connections"),
        "about":          (p.get("about") or "")[:500] or None,
        "skills":         (p.get("skills") or [])[:12],
        "posts":          _first_n_text(p.get("posts"),    6, max_chars=200),
        "activity_liked": _first_n_text(p.get("activity"), 8, max_chars=180),
        "experience":     _experience_summary(p.get("positions") or p.get("experience")),
        "education":      _edu_summary(p.get("educations_details") or p.get("education")),
    }.items() if v is not None}


def _apply_template(template: str | None, profile_str: str) -> str | None:
    """Replace {{profile_json}} placeholder in a user_template with actual profile data."""
    if not template:
        return None
    return template.replace("{{profile_json}}", profile_str)


# ── Core LLM call ─────────────────────────────────────────────────────────────

def _is_rate_limited(r) -> bool:
    """True when Groq rejects due to TPM, TPD, size limits, or decommissioned model (should try fallback)."""
    body = r.text
    if r.status_code in (413, 429):
        return (
            "rate_limit_exceeded" in body
            or "too large"         in body
            or "tokens per minute" in body
            or "tokens per day"    in body
            or "request_too_large" in body
        )
    if r.status_code == 400 and "model_decommissioned" in body:
        return True
    return False


async def _groq(
    system: str,
    user: str,
    model: str = MODEL_SMART,
    temperature: float = 0.2,
) -> tuple[dict, dict]:
    """
    Call Groq API → (parsed_result_dict, token_usage_dict).
    Fallback chain on rate-limit / size / decommissioned errors:
      1. Requested model (from LIO Config)
      2. Smart model from LIO Config (if different from requested)
      3. MODEL_FAST — llama-3.1-8b-instant (separate daily quota, always available)
      4. MODEL_FAST + truncate user prompt to 3 000 chars
    """
    key = _groq_key()
    if not key:
        raise RuntimeError("GROQ_API_KEY not configured")

    smart = _smart_model()

    seen: set[str] = set()
    attempts: list[tuple[str, str]] = []
    for m in [model, smart, MODEL_FAST]:
        if m not in seen:
            attempts.append((m, user))
            seen.add(m)
    # Final safety: fast model + truncated prompt
    if (MODEL_FAST, user[:3000]) not in [(a, b) for a, b in attempts]:
        attempts.append((MODEL_FAST, user[:3000]))

    last_err = ""
    r = None
    for attempt_model, attempt_user in attempts:
        payload = {
            "model": attempt_model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user",   "content": attempt_user},
            ],
            "temperature": temperature,
            "max_tokens": 1500,
            "response_format": {"type": "json_object"},
        }
        async with httpx.AsyncClient(timeout=60) as c:
            r = await c.post(
                GROQ_URL,
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json=payload,
            )
        if _is_rate_limited(r):
            last_err = f"Groq {r.status_code}: {r.text[:300]}"
            logger.warning("[Groq] Limit/error on %s — trying fallback", attempt_model)
            continue
        if not r.is_success:
            raise RuntimeError(f"Groq {r.status_code}: {r.text[:400]}")
        break
    else:
        raise RuntimeError(f"Groq failed after all fallbacks: {last_err}")

    data = r.json()

    raw = data["choices"][0]["message"]["content"].strip()
    raw = re.sub(r'^```(?:json)?\s*', '', raw, flags=re.MULTILINE)
    raw = re.sub(r'\s*```$',          '', raw, flags=re.MULTILINE).strip()

    usage = data.get("usage", {})
    token_usage = {
        "prompt_tokens":     usage.get("prompt_tokens", 0),
        "completion_tokens": usage.get("completion_tokens", 0),
        "total_tokens":      usage.get("total_tokens", 0),
        "model":             model,
    }
    return json.loads(raw), token_usage


def _add_usage(result: dict, token_usage: dict) -> dict:
    result["token_usage"] = token_usage
    return result


# ── Module implementations ────────────────────────────────────────────────────

def _temp(t: float | None, default: float) -> float:
    """Return configured temperature or default (handles 0.0 which is falsy)."""
    return t if t is not None else default


async def run_identity(
    profile: dict,
    system_prompt: str | None = None,
    user_template: str | None = None,
    model: str | None = None,
    temperature: float | None = None,
) -> dict:
    ps = _ps(_extract_identity(profile))
    sys = system_prompt or f"You are a data extraction expert. Extract identity fields from a LinkedIn profile JSON. {_STRICT}"
    usr = _apply_template(user_template, ps) or (
        f"Profile JSON:\n{ps}\n\n"
        "Return ONLY this JSON (fill from profile data):\n"
        '{"full_name":"","first_name":"","last_name":"","title":"","company":"",'
        '"linkedin_url":"","location":"","timezone":"IANA tz e.g. Asia/Kolkata","followers":0}'
    )
    result, tu = await _groq(sys, usr, model or MODEL_FAST, _temp(temperature, 0.2))
    return _add_usage(result, tu)


async def run_contact(
    profile: dict,
    system_prompt: str | None = None,
    user_template: str | None = None,
    model: str | None = None,
    temperature: float | None = None,
) -> dict:
    ps = _ps(_extract_contact(profile))
    sys = system_prompt or (
        "You are a contact intelligence analyst. Extract or infer contact info from a LinkedIn profile. "
        "Infer work email from company name/domain patterns visible in the data if possible. "
        f"{_STRICT}"
    )
    usr = _apply_template(user_template, ps) or (
        f"Profile JSON:\n{ps}\n\n"
        "Return ONLY this JSON:\n"
        '{"work_email":null,"email_confidence":0.0,"personal_email":null,'
        '"bounce_risk":"low|medium|high","phone":null,'
        '"waterfall_steps":["describe inference steps used"]}'
    )
    result, tu = await _groq(sys, usr, model or MODEL_FAST, _temp(temperature, 0.2))
    return _add_usage(result, tu)


async def run_scores(
    profile: dict,
    system_prompt: str | None = None,
    user_template: str | None = None,
    model: str | None = None,
    temperature: float | None = None,
) -> dict:
    ps = _ps(_extract_scores(profile))
    sys = system_prompt or (
        "You are a B2B lead scoring analyst. Score this LinkedIn profile for ICP fit and buying intent. "
        f"{_STRICT}\n"
        "- All scores are integers 0-100.\n"
        "- best_send_window: day + time range + timezone."
    )
    usr = _apply_template(user_template, ps) or (
        f"Profile JSON:\n{ps}\n\n"
        "Return ONLY this JSON:\n"
        '{"icp_score":0,"icp_breakdown":{"title_match":0,"company_size_fit":0,'
        '"geo_fit":0,"engagement_level":0},"intent_score":0,"best_send_window":""}'
    )
    result, tu = await _groq(sys, usr, model or MODEL_FAST, _temp(temperature, 0.2))
    return _add_usage(result, tu)


async def run_icp_match(
    profile: dict,
    system_prompt: str | None = None,
    user_template: str | None = None,
    model: str | None = None,
    temperature: float | None = None,
) -> dict:
    ps = _ps(_extract_icp_match(profile))
    sys = system_prompt or (
        "You are a B2B ICP (Ideal Customer Profile) analyst. "
        f"{_STRICT}\n"
        "- icp_verdict: 'Strong Match' | 'Moderate Match' | 'Weak Match' | 'No Match'\n"
        "- company_size: estimate (e.g. '51-200', '<10', '1000+')"
    )
    usr = _apply_template(user_template, ps) or (
        f"Profile JSON:\n{ps}\n\n"
        "Return ONLY this JSON:\n"
        '{"is_decision_maker":true,"industry":"","company_size":"","geo_fit":true,'
        '"education_level":"","awards_count":0,"icp_verdict":""}'
    )
    result, tu = await _groq(sys, usr, model or MODEL_FAST, _temp(temperature, 0.2))
    return _add_usage(result, tu)


async def run_behavioural_signals(
    profile: dict,
    system_prompt: str | None = None,
    user_template: str | None = None,
    model: str | None = None,
    temperature: float | None = None,
) -> dict:
    ps = _ps(_extract_behavioural(profile))
    sys = system_prompt or (
        "You are a B2B behavioural analyst. Analyse this prospect's LinkedIn posts and activity feed. "
        f"{_STRICT}\n"
        "- posts_about: 10-20 short topic strings (5-10 words each) from their OWN posts\n"
        "- engages_with: 10-20 short topic strings from content they liked/commented on\n"
        "- engagement_style: 10-20 sentences describing writing style and tone\n"
        "- decision_pattern: 10-20 sentences describing how they make decisions\n"
        "- pain_points: specific operational pains (not generic buzzwords), 5-10 items\n"
        "- warm_signal: describe any concrete buying signal with specifics, or null"
    )
    usr = _apply_template(user_template, ps) or (
        f"Profile JSON:\n{ps}\n\n"
        "Return ONLY this JSON:\n"
        '{"posts_about":[],"engages_with":[],"engagement_style":"","decision_pattern":"","pain_points":[],"warm_signal":null}'
    )
    result, tu = await _groq(sys, usr, model or MODEL_FAST, _temp(temperature, 0.2))
    return _add_usage(result, tu)


async def run_pitch_intelligence(
    profile: dict,
    system_prompt: str | None = None,
    user_template: str | None = None,
    model: str | None = None,
    temperature: float | None = None,
) -> dict:
    ps = _ps(_extract_pitch(profile))
    sys = system_prompt or (
        "You are a world-class B2B sales strategist. Generate pitch intelligence from a LinkedIn profile. "
        f"{_STRICT}\n"
        "- pain_point: specific to their current role and activity — not generic\n"
        "- do_not_pitch: specific topics/tools this person would reject or already has"
    )
    usr = _apply_template(user_template, ps) or (
        f"Profile JSON:\n{ps}\n\n"
        'Return ONLY this JSON:\n{"pain_point":"","value_prop":"","best_angle":"","cta":"","do_not_pitch":[]}'
    )
    result, tu = await _groq(sys, usr, model or MODEL_SMART, _temp(temperature, 0.2))
    return _add_usage(result, tu)


async def run_activity(
    profile: dict,
    system_prompt: str | None = None,
    user_template: str | None = None,
    model: str | None = None,
    temperature: float | None = None,
) -> dict:
    ps = _ps(_extract_activity(profile))
    sys = system_prompt or (
        "You are an activity signal analyst. Analyse this prospect's LinkedIn posts and liked activity. "
        f"{_STRICT}\n"
        "- type: 'post' | 'like' | 'comment'\n"
        "- warm_signal: true only if the activity indicates buying intent, hiring, or tool evaluation\n"
        "- engagement_frequency: 'daily' | 'weekly' | 'monthly' | 'rare'"
    )
    usr = _apply_template(user_template, ps) or (
        f"Profile JSON:\n{ps}\n\n"
        "Return ONLY this JSON:\n"
        '{"recent_activity":[{"type":"","title":"","warm_signal":false,"signal_reason":""}],'
        '"authored_posts_count":0,"engagement_frequency":""}'
    )
    result, tu = await _groq(sys, usr, model or MODEL_FAST, _temp(temperature, 0.2))
    return _add_usage(result, tu)


async def run_tags(
    profile: dict,
    system_prompt: str | None = None,
    user_template: str | None = None,
    model: str | None = None,
    temperature: float | None = None,
) -> dict:
    ps = _ps(_extract_tags(profile))
    sys = system_prompt or (
        "You are a B2B CRM segmentation expert. Generate smart tags from a LinkedIn profile. "
        f"{_STRICT}\n"
        "- 6-12 tags total\n"
        "- Max 3 words per tag\n"
        "- Cover: role, industry, company size, location, behaviour, buying signals"
    )
    usr = _apply_template(user_template, ps) or (
        f"Profile JSON:\n{ps}\n\n"
        'Return ONLY this JSON:\n{"tags":["tag1","tag2"]}'
    )
    result, tu = await _groq(sys, usr, model or MODEL_FAST, _temp(temperature, 0.2))
    return _add_usage(result, tu)


async def run_outreach(
    profile: dict,
    system_prompt: str | None = None,
    user_template: str | None = None,
    model: str | None = None,
    temperature: float | None = None,
) -> dict:
    # Prompts come from LIO Config (enrichment_config_service DEFAULT_AI_MODULE_PROMPTS).
    # Fallbacks below are safety nets only — routes always supply config values.
    ps = _ps(_extract_outreach(profile))
    sys = system_prompt or (
        "You are a world-class B2B SDR. Write hyper-personalised cold outreach from LinkedIn profile data.\n\n"
        "STRICT OUTPUT RULES:\n"
        "- Return ONLY a valid JSON object. No markdown, no explanation, no extra keys.\n"
        "- Every key listed below is MANDATORY. Never omit a key. Never return an empty string for any field.\n"
        "- Use null only if a field is truly impossible to fill.\n\n"
        "REQUIRED JSON SCHEMA (all 10 keys must be present):\n"
        "{\n"
        '  "linkedin_note": "CONNECTION REQUEST — max 100 words, reference a specific post or topic, end with a question, zero sales pitch",\n'
        '  "cold_email": {\n'
        '    "subject": "SUBJECT — 10 to 20 words, no questions, no ALL CAPS, intriguing but honest",\n'
        '    "greeting": "GREETING — always Hi [FirstName], — first name only",\n'
        '    "opening": "OPENING — 1 sentence referencing a SPECIFIC post, activity or topic they care about. Never start with I noticed / I came across / Hope you",\n'
        '    "body": "BODY — 5 to 10 sentences: pain you spotted from their profile → how your product solves it → one concrete proof point or stat",\n'
        '    "cta": "CTA — 1 soft-ask sentence, ask a specific question or suggest a 15-min call, never say let me know if interested",\n'
        '    "sign_off": "Best,\\n[Your Name]",\n'
        '    "full_email": "COMPLETE EMAIL — greeting + newline + opening + newline + body + newline + cta + newline + sign_off combined into one ready-to-send string"\n'
        '  },\n'
        '  "follow_up": {\n'
        '    "day3": "FOLLOW-UP DAY 3 — 1 to 2 sentences, friendly bump if no reply, add one new angle",\n'
        '    "day7": "FOLLOW-UP DAY 7 — 1 sentence final bump, include a relevant insight or question"\n'
        '  }\n'
        "}"
    )
    usr = _apply_template(user_template, ps) or (
        f"Prospect LinkedIn profile data:\n{ps}\n\n"
        "Using the profile above, write fully personalised outreach. "
        "Fill EVERY field with real content — no placeholder text, no empty strings.\n\n"
        "Return ONLY this exact JSON structure with all 10 keys filled:\n"
        '{"linkedin_note":"","cold_email":{"subject":"","greeting":"","opening":"","body":"","cta":"","sign_off":"","full_email":""},"follow_up":{"day3":"","day7":""}}'
    )
    result, tu = await _groq(sys, usr, model or MODEL_SMART, _temp(temperature, 0.3))
    return _add_usage(result, tu)


async def run_persona_analysis(
    profile: dict,
    system_prompt: str | None = None,
    user_template: str | None = None,
    model: str | None = None,
    temperature: float | None = None,
) -> dict:
    ps = _ps(_extract_persona(profile))
    sys = system_prompt or (
        "You are an expert buyer persona analyst. Generate a full psychographic and behavioural persona from a LinkedIn profile. "
        f"{_STRICT}\n"
        "- personality: 'Driver' | 'Analytical' | 'Amiable' | 'Expressive'\n"
        "- risk_tolerance: 'low' | 'moderate' | 'high'\n"
        "- crm_score.tier: 'A' | 'B' | 'C' | 'D' (A = best)\n"
        "- crm_score.score: integer 0-100"
    )
    usr = _apply_template(user_template, ps) or (
        f"Profile JSON:\n{ps}\n\n"
        "Return ONLY this JSON:\n"
        '{"identity_summary":"","interests":[],"skills":[],"behavior_analysis":"",'
        '"writing_style":"","psychographic_profile":{"personality":"","motivation":"","risk_tolerance":""},'
        '"buying_signals":[],"smart_tags":[],'
        '"outreach_intelligence":{"best_channel":"","best_time":"","tone":""},'
        '"crm_score":{"score":0,"tier":"","reason":""}}'
    )
    result, tu = await _groq(sys, usr, model or MODEL_SMART, _temp(temperature, 0.2))
    return _add_usage(result, tu)


def run_meta() -> dict:
    return {
        "enriched_at": datetime.now(timezone.utc).isoformat(),
        "source":      "BrightData + Groq",
        "model":       MODEL_SMART,
        "version":     "1.0.0",
    }


async def run_full_enrichment(profile: dict, module_configs: list | None = None) -> dict:
    """Run all 10 AI modules concurrently via asyncio.gather.

    module_configs: list of per-org prompt overrides from enrichment_config_service.
    """
    def _ov(mid: str) -> dict:
        if not module_configs:
            return {}
        for m in module_configs:
            if m.get("id") == mid:
                temp = m.get("temperature")
                return {
                    "system_prompt": m.get("system") or None,
                    "user_template": m.get("user_template") or None,
                    "model":         m.get("model") or None,
                    "temperature":   float(temp) if temp is not None else None,
                }
        return {}

    results = await asyncio.gather(
        run_identity(profile,             **_ov("identity")),
        run_contact(profile,              **_ov("contact")),
        run_scores(profile,               **_ov("scores")),
        run_icp_match(profile,            **_ov("icp_match")),
        run_behavioural_signals(profile,  **_ov("behavioural_signals")),
        run_pitch_intelligence(profile,   **_ov("pitch_intelligence")),
        run_activity(profile,             **_ov("activity")),
        run_tags(profile,                 **_ov("tags")),
        run_outreach(profile,             **_ov("outreach")),
        run_persona_analysis(profile,     **_ov("persona_analysis")),
        return_exceptions=True,
    )

    keys = [
        "identity", "contact", "scores", "icp_match",
        "behavioural_signals", "pitch_intelligence",
        "activity", "tags", "outreach", "persona_analysis",
    ]

    total_prompt = total_completion = 0
    out: dict = {}
    for k, v in zip(keys, results):
        if isinstance(v, Exception):
            logger.error("[AI full-enrichment] Module %s failed: %s", k, v)
            out[k] = {"error": str(v)}
        else:
            out[k] = v
            tu = v.get("token_usage", {})
            total_prompt     += tu.get("prompt_tokens", 0)
            total_completion += tu.get("completion_tokens", 0)

    out["meta"] = {
        **run_meta(),
        "token_usage": {
            "prompt_tokens":     total_prompt,
            "completion_tokens": total_completion,
            "total_tokens":      total_prompt + total_completion,
            "model":             MODEL_SMART,
        },
    }
    return out
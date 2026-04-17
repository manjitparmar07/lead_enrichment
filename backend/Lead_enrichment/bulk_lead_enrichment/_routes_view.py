"""
_routes_view.py
---------------
The 4 view sub-routers and all their helper functions, models, and prompts:
  _linkedin_enrich_router   — GET  /leads/view/linkedin
  _email_enrich_router      — imported from email_lead_enrichment
  _outreach_enrich_router   — POST /leads/view/outreach
  _company_enrich_router    — POST /leads/view/company

Lines ~1627–2802 of the original lead_enrichment_brightdata_routes.py.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, field_validator

from Lead_enrichment.bulk_lead_enrichment import lead_enrichment_brightdata_service as svc

from Lead_enrichment.bulk_lead_enrichment._shared import (
    _lead_cache_get,
    _lead_cache_set,
    _lead_cache_delete,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Concurrency / locking state (local to view routes)
# ─────────────────────────────────────────────────────────────────────────────

# Per-company locks for deduplication
_company_locks: dict = {}
_company_locks_ref: dict = {}

# Outreach LLM concurrency cap
_OUTREACH_CONCURRENCY = int(os.getenv("OUTREACH_CONCURRENCY", "30"))


# ─────────────────────────────────────────────────────────────────────────────
# Helper functions
# ─────────────────────────────────────────────────────────────────────────────

def _extract_outreach_from_prompt(prompt: str) -> dict | None:
    """
    If the system_prompt already contains completed outreach copy, extract
    fields and return a dict. Returns None if no completed copy is detected.

    Handles both formats produced by system_prompt_generator_service:
      Format A — "Email Subject Line: ..." / "LinkedIn Connection Note: ..."
      Format B — "Email subject line: ..." / "LinkedIn connection note: ..."
    """
    import re as _re

    has_subject  = bool(_re.search(r"Email [Ss]ubject [Ll]ine\s*:", prompt))
    has_linkedin = bool(_re.search(r"LinkedIn [Cc]onnection [Nn]ote\s*:", prompt))
    if not (has_subject and has_linkedin):
        return None

    def _grab(pattern, text, flags=_re.IGNORECASE | _re.DOTALL):
        m = _re.search(pattern, text, flags)
        return m.group(1).strip() if m else ""

    subject        = _grab(r"Email [Ss]ubject [Ll]ine\s*:\s*(.+?)(?:\n|$)", prompt)
    linkedin_note  = _grab(r"LinkedIn [Cc]onnection [Nn]ote\s*:\s*(.+?)(?:\n\n|\Z)", prompt)
    best_channel   = _grab(r"Best [Cc]hannel\s*:\s*(.+?)(?:\n|$)", prompt)
    best_time      = _grab(r"Best [Ss]end [Tt]ime\s*:\s*(.+?)(?:\n|$)", prompt)
    outreach_angle = _grab(r"Primary [Aa]ngle\s*:\s*(.+?)(?:\n|\Z)", prompt)

    body_match = (
        _re.search(r"Email\s*:\s*\n+(.+?)(?=LinkedIn [Cc]onnection [Nn]ote)", prompt, _re.DOTALL)
        or _re.search(r"Email [Ss]ubject [Ll]ine\s*:.+?\n\n(.+?)(?=LinkedIn [Cc]onnection [Nn]ote|Best [Cc]hannel)", prompt, _re.DOTALL)
    )
    email_body = body_match.group(1).strip() if body_match else ""

    return {
        "cold_email":      {"subject": subject, "body": email_body},
        "linkedin_note":   linkedin_note,
        "best_channel":    best_channel,
        "best_time":       best_time,
        "outreach_angle":  outreach_angle,
    }


async def _quick_llm(system: str, user: str, max_tokens: int = 2000) -> str:
    """Lightweight LLM call for view-endpoint AI generation. HuggingFace only."""
    import httpx as _httpx
    errors = []
    hf_token = svc._hf_token()
    if not hf_token:
        raise RuntimeError("No LLM provider configured: HF_TOKEN is not set.")
    for attempt in range(2):
        try:
            async with _httpx.AsyncClient(timeout=60) as c:
                r = await c.post(
                    "https://router.huggingface.co/v1/chat/completions",
                    headers={"Content-Type": "application/json", "Authorization": f"Bearer {hf_token}"},
                    json={"model": svc._hf_model(),
                          "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
                          "temperature": 0.35, "max_tokens": max_tokens},
                )
                if r.is_success:
                    return r.json()["choices"][0]["message"]["content"].strip()
                try:
                    body_txt = r.json().get("error", {}).get("message") or r.text
                except Exception:
                    body_txt = r.text
                errors.append(f"HuggingFace attempt {attempt+1}: HTTP {r.status_code}: {body_txt}")
                if r.status_code not in (429, 503, 502):
                    break
        except Exception as e:
            errors.append(f"HuggingFace attempt {attempt+1}: {e}")
        if attempt == 0:
            await asyncio.sleep(1)
    raise RuntimeError(" | ".join(errors))


def _template_outreach(lead: dict) -> dict:
    """
    Generate a basic template-based outreach when LLM is unavailable and DB has no stored values.
    Ensures subject, body, and linkedin_note are never returned empty.
    """
    name    = lead.get("name") or "there"
    first   = name.split()[0] if name and name != "there" else name
    title   = lead.get("title") or "professional"
    company = lead.get("company") or "your company"

    subject = f"Quick question for {first}"

    body = (
        f"Your work at {company} caught my attention — "
        f"the challenges facing {title.lower()}s right now are significant, and "
        f"the approaches that actually move the needle are few.\n\n"
        f"We help teams like yours cut through the noise on one specific problem. "
        f"I'd rather show you a concrete result than pitch broadly.\n\n"
        f"Would it make sense to spend 15 minutes seeing if there's a fit?"
    )

    linkedin_note = (
        f"Hi {first} — your background at {company} stood out. "
        f"I'd love to connect and share something relevant to your work."
    )[:300]

    return {
        "cold_email":     {"subject": subject, "body": body},
        "linkedin_note":  linkedin_note,
        "best_channel":   "Email",
        "best_time":      "Tuesday–Thursday, 9–11 AM local time",
        "outreach_angle": "Insight",
    }


def _parse_json_field(lead: dict, key: str, default):
    """Safely parse a JSON string field from a lead row."""
    raw = lead.get(key)
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def _get_full_data(lead: dict) -> dict:
    return _parse_json_field(lead, "full_data", {})


def _build_outreach_lead_ctx(lead: dict) -> str:
    """
    Build a structured, signal-rich lead context string for the outreach LLM call.
    Works generically for any lead — DB row or inline lead_data.
    Only includes fields that have actual values, so the LLM sees real signals
    not empty placeholders.
    """
    sections: list[str] = []

    # ── Identity ──────────────────────────────────────────────────────────────
    identity_parts = []
    if lead.get("name"):         identity_parts.append(f"Name: {lead['name']}")
    if lead.get("title"):        identity_parts.append(f"Title: {lead['title']}")
    if lead.get("company"):      identity_parts.append(f"Company: {lead['company']}")
    if lead.get("seniority_level"): identity_parts.append(f"Seniority: {lead['seniority_level']}")
    if lead.get("location"):     identity_parts.append(f"Location: {lead['location']}")
    elif lead.get("city") or lead.get("country"):
        identity_parts.append(f"Location: {', '.join(filter(None, [lead.get('city',''), lead.get('country','')]))}")
    if lead.get("followers"):    identity_parts.append(f"Followers: {lead['followers']}")
    if lead.get("connections"):  identity_parts.append(f"Connections: {lead['connections']}")
    if identity_parts:
        sections.append("## Lead Identity\n" + " | ".join(identity_parts))

    # ── About ─────────────────────────────────────────────────────────────────
    about = str(lead.get("about") or "").strip()
    if about:
        sections.append(f"## About\n{about[:400]}")

    # ── Career History ────────────────────────────────────────────────────────
    career = lead.get("career_history") or []
    if career:
        career_lines = [
            f"- {c.get('title','')} at {c.get('company','')}"
            for c in career[:4] if c.get("title") or c.get("company")
        ]
        if career_lines:
            sections.append("## Career History\n" + "\n".join(career_lines))

    # ── Scores & Signals ──────────────────────────────────────────────────────
    signal_parts = []
    if lead.get("score_tier"):   signal_parts.append(f"Score tier: {lead['score_tier']}")
    if lead.get("lead_score"):   signal_parts.append(f"Score: {lead['lead_score']}")
    if lead.get("warm_signal") and str(lead["warm_signal"]).lower() not in ("none", "no warm signal detected", "false", ""):
        signal_parts.append(f"Warm signal: {lead['warm_signal']}")
    if lead.get("industry"):     signal_parts.append(f"Industry: {lead['industry']}")
    tags = lead.get("tags") or []
    if tags:                     signal_parts.append(f"Tags: {', '.join(tags)}")
    if signal_parts:
        sections.append("## Signals\n" + " | ".join(signal_parts))

    # ── Behavioural Context ───────────────────────────────────────────────────
    behaviour_parts = []
    if lead.get("engages_with"):       behaviour_parts.append(f"Engages with: {lead['engages_with']}")
    if lead.get("decision_pattern"):   behaviour_parts.append(f"Decision pattern: {lead['decision_pattern']}")
    if lead.get("communication_style"): behaviour_parts.append(f"Communication style: {lead['communication_style']}")
    bs = lead.get("behavioural_signals") or {}
    if isinstance(bs, str):
        try: bs = json.loads(bs)
        except Exception: bs = {}
    if isinstance(bs, dict):
        if not lead.get("engages_with") and bs.get("engages_with"):
            behaviour_parts.append(f"Engages with: {bs['engages_with']}")
        if not lead.get("decision_pattern") and bs.get("decision_pattern"):
            behaviour_parts.append(f"Decision pattern: {bs['decision_pattern']}")
        if bs.get("pain_point_hint"):
            behaviour_parts.append(f"Pain hint: {bs['pain_point_hint']}")
    if behaviour_parts:
        sections.append("## Behavioural Context\n" + " | ".join(behaviour_parts))

    # ── Pitch Intelligence ────────────────────────────────────────────────────
    pitch = lead.get("pitch_intelligence") or {}
    if isinstance(pitch, str):
        try: pitch = json.loads(pitch)
        except Exception:
            if pitch.strip():
                sections.append(f"## Pitch Intelligence\n{pitch[:500]}")
            pitch = {}
    if isinstance(pitch, dict) and pitch:
        pitch_parts = []
        if pitch.get("best_angle"):        pitch_parts.append(f"Best angle: {pitch['best_angle']}")
        if pitch.get("top_pain_point"):    pitch_parts.append(f"Top pain: {pitch['top_pain_point']}")
        if pitch.get("best_value_prop"):   pitch_parts.append(f"Value prop: {pitch['best_value_prop']}")
        if pitch.get("suggested_cta"):     pitch_parts.append(f"Suggested CTA: {pitch['suggested_cta']}")
        if pitch.get("do_not_pitch"):
            dnp = pitch["do_not_pitch"]
            pitch_parts.append(f"Do NOT pitch: {', '.join(dnp) if isinstance(dnp, list) else dnp}")
        if pitch_parts:
            sections.append("## Pitch Intelligence\n" + " | ".join(pitch_parts))

    # ── Real LinkedIn Posts ───────────────────────────────────────────────────
    raw_posts = None
    for field in ("linkedin_posts", "activity_feed"):
        val = lead.get(field)
        if val and str(val).strip() not in ("null", "[]", "{}"):
            try:
                parsed = json.loads(val) if isinstance(val, str) else val
                if isinstance(parsed, list) and parsed:
                    raw_posts = parsed
                    break
            except Exception:
                pass

    if raw_posts:
        post_lines = []
        for p in raw_posts[:4]:
            if not isinstance(p, dict):
                continue
            text = (p.get("text") or p.get("title") or p.get("content") or "").strip()
            date = p.get("date") or p.get("published_at") or ""
            likes = p.get("likes") or p.get("num_likes") or ""
            if text:
                snippet = text[:200] + ("..." if len(text) > 200 else "")
                meta = " | ".join(filter(None, [date[:10] if date else "", f"{likes} likes" if likes else ""]))
                post_lines.append(f'- "{snippet}"' + (f" ({meta})" if meta else ""))
        if post_lines:
            sections.append("## Recent LinkedIn Posts (real signals — use these)\n" + "\n".join(post_lines))

    return "\n\n".join(sections)


def _lead_from_ai_information(data: dict) -> dict:
    """
    Map a full ai_information payload (as returned by the enrichment API)
    to the flat lead dict format used internally by outreach logic.
    """
    identity   = data.get("identity") or {}
    profile    = identity.get("profile") or {}
    scores     = data.get("scores") or {}
    breakdown  = scores.get("breakdown") or {}
    activity   = data.get("activity") or {}
    pitch      = data.get("pitch_intelligence") or {}
    behaviour  = data.get("behavioural_signals") or {}
    contact    = data.get("contact") or {}
    tags       = data.get("tags") or []

    city    = profile.get("city") or identity.get("location") or ""
    country = profile.get("country") or identity.get("country") or ""
    location = ", ".join(filter(None, [city, country]))

    feed = activity.get("feed") or []
    activity_summary = "; ".join(
        item.get("title", "")[:120] for item in feed[:5] if item.get("title")
    )

    posts_raw = activity.get("posts") or activity.get("linkedin_posts") or []
    linkedin_posts_json = json.dumps(posts_raw) if posts_raw else None

    pitch_str = (
        f"Best angle: {pitch.get('best_angle','')} | "
        f"Top pain point: {pitch.get('top_pain_point','')} | "
        f"Best value prop: {pitch.get('best_value_prop','')} | "
        f"Suggested CTA: {pitch.get('suggested_cta','')} | "
        f"Do not pitch: {', '.join(pitch.get('do_not_pitch') or [])}"
    )

    return {
        "id":                data.get("lead_id") or identity.get("lead_id"),
        "name":              identity.get("name") or profile.get("full_name"),
        "title":             identity.get("title") or profile.get("current_title"),
        "company":           identity.get("company"),
        "industry":          profile.get("department") or (tags[0] if tags else ""),
        "location":          location,
        "linkedin_url":      identity.get("linkedin_url") or data.get("linkedin_url"),
        "score_tier":        scores.get("score_tier") or breakdown.get("score_tier"),
        "lead_score":        scores.get("total_score") or scores.get("combined_score"),
        "warm_signal":       activity.get("warm_signal") or behaviour.get("warm_signal"),
        "pitch_intelligence": pitch_str,
        "about":             identity.get("about") or profile.get("about"),
        "followers":         identity.get("followers"),
        "connections":       identity.get("connections"),
        "career_history":    profile.get("career_history") or [],
        "top_skills":        profile.get("top_skills") or [],
        "tags":              tags,
        "behavioural_signals": behaviour,
        "recent_activity":   activity_summary,
        "linkedin_posts":    linkedin_posts_json,
        "activity_feed":     json.dumps(feed[:10]) if feed else None,
        "email":             contact.get("work_email") or contact.get("email"),
        "seniority_level":   profile.get("seniority_level"),
        "decision_pattern":  behaviour.get("decision_pattern"),
        "communication_style": behaviour.get("communication_style"),
        "engages_with":      behaviour.get("engages_with"),
        "cold_email":        None,
        "email_subject":     None,
        "linkedin_note":     None,
        "best_channel":      None,
        "outreach_angle":    None,
        "best_send_time":    None,
        "outreach_sequence": None,
    }


async def _resolve_lead(leadenrich_id: str) -> dict:
    """
    Resolve a lead by its leadenrich_id (returned from bulk job results / Ably events).
    Checks Redis cache (30 min TTL) before hitting the DB.
    Raises 400 if id is missing, 404 if not found in DB.
    """
    if not leadenrich_id:
        raise HTTPException(
            status_code=400,
            detail="leadenrich_id is required. Get it from bulk job results or real-time Ably events.",
        )
    cached = await _lead_cache_get(leadenrich_id)
    if cached:
        return cached
    lead = await svc.get_lead(leadenrich_id)
    if not lead:
        raise HTTPException(
            status_code=404,
            detail=f"Lead not found: {leadenrich_id}. Make sure bulk enrichment has completed for this lead.",
        )
    await _lead_cache_set(leadenrich_id, lead)
    return lead


# ─────────────────────────────────────────────────────────────────────────────
# View models
# ─────────────────────────────────────────────────────────────────────────────

class ViewEmailRequest(BaseModel):
    leadenrich_id: str
    system_prompt: Optional[str] = None


class ViewOutreachRequest(BaseModel):
    leadenrich_id: Optional[str] = None
    lead_data:     Optional[dict] = None
    system_prompt: Optional[str] = None


class ViewCompanyRequest(BaseModel):
    leadenrich_id: Optional[str] = None
    company_linkedin_url: Optional[str] = None
    system_prompt: Optional[str] = None
    include_raw: bool = False

    @field_validator("company_linkedin_url")
    @classmethod
    def validate_company_url(cls, v):
        if v is None:
            return v
        v = v.strip()
        if "/company/" not in v:
            raise ValueError("company_linkedin_url must be a LinkedIn company URL (must contain /company/)")
        return v


# ─────────────────────────────────────────────────────────────────────────────
# 1. LinkedIn Enrichment
# ─────────────────────────────────────────────────────────────────────────────

_linkedin_enrich_router = APIRouter(
    prefix="/leads",
    tags=["LinkedIn Enrichment"],
)

_LINKEDIN_DESC = """
Returns the full LinkedIn-sourced enrichment for a person, structured into 8 sections.

**How to call:**
Pass the same LinkedIn URL you submitted to the bulk API:
```
GET /api/leads/view/linkedin?url=https://www.linkedin.com/in/johndoe
```
Or use the internal `lead_id` returned in job results:
```
GET /api/leads/view/linkedin?lead_id=abc123def456
```

**Sections returned:**
- **identity** — Name, title, company, location, about, followers, connections, skills
- **contact** — Work email, phone, source, confidence, verified, bounce_risk
- **scores** — Total score, ICP / intent / timing, tier, completeness, reasons
- **icp_match** — ICP fit score, product category, match detail
- **behavioural_signals** — Buying signals, engagement patterns, intent level
- **pitch_intelligence** — Primary pitch, pain points, value propositions
- **activity** — Recent posts, warm signal, hiring signals, activity feed
- **tags** — Auto-generated persona and topic tags
"""


@_linkedin_enrich_router.get(
    "/view/linkedin",
    summary="LinkedIn Enrichment",
    description=_LINKEDIN_DESC,
)
async def linkedin_enrichment(
    leadenrich_id: str = Query(..., description="Lead ID returned from bulk enrichment job results"),
):
    lead = await _resolve_lead(leadenrich_id)
    return svc._format_linkedin_enrich(lead)


# ─────────────────────────────────────────────────────────────────────────────
# 2. Email Enrichment — imported from email_lead_enrichment
# ─────────────────────────────────────────────────────────────────────────────
from Lead_enrichment.email_lead_enrichment.email_enrichment_routes import router as _email_enrich_router


# ─────────────────────────────────────────────────────────────────────────────
# Default outreach system prompt
# ─────────────────────────────────────────────────────────────────────────────

_DEFAULT_OUTREACH_SYSTEM = (
    "You are a senior B2B SDR writing cold outreach on behalf of the sender's company.\n\n"

    "WHAT TO WRITE:\n"
    "1. Email subject — curiosity-driven, under 8 words, no spam triggers, no company names.\n"
    "2. Cold email body — exactly 3 paragraphs, under 150 words total:\n"
    "   Para 1 — SPECIFIC HOOK: Reference something real and specific about the lead — "
    "their company's focus, a recent LinkedIn post they wrote, their career move, or their stated pain point. "
    "This must NOT be something that applies to every CEO or every company. "
    "If LinkedIn posts are provided, use a concrete detail from one of them.\n"
    "   Para 2 — VALUE BRIDGE: State the one problem you solve that is most relevant to this specific lead. "
    "Give one concrete business outcome — not 'save time' or 'boost efficiency', "
    "but a real result tied to their context (e.g. 'close the pipeline gap without adding headcount').\n"
    "   Para 3 — DISCOVERY CTA: Ask one smart question tied to their specific situation. "
    "It must feel like it comes from someone who already understands their world — not 'what are your biggest challenges?'.\n"
    "3. LinkedIn note — under 300 chars, references something real about the lead, conversational, zero pitch.\n"
    "4. Best channel: Email / LinkedIn / Phone — based on the lead's seniority and activity.\n"
    "5. Best send time — in the lead's local timezone, specific day + time.\n"
    "6. Primary outreach angle: Pain-based / Trigger / Insight.\n\n"

    "STRICT RULES:\n"
    "- Never use: 'I hope this finds you well', 'reaching out because', 'I noticed', 'synergy', "
    "'game-changer', 'innovative solution', 'I came across your profile', 'one outcome is', "
    "'automate manual work', or any phrase that sounds AI-generated.\n"
    "- Never fabricate statistics or percentages — only state outcomes you can logically infer.\n"
    "- If no LinkedIn posts are available, anchor Para 1 in the lead's role, company, or career history.\n"
    "- Keep the tone direct, human, and professional — like a message from a smart colleague, not a salesperson.\n\n"

    "Return ONLY a valid JSON object — no markdown, no explanation:\n"
    "{\"cold_email\": {\"subject\": \"...\", \"body\": \"...\"}, "
    "\"linkedin_note\": \"...\", \"best_channel\": \"...\", \"best_time\": \"...\", \"outreach_angle\": \"...\"}"
)


# ─────────────────────────────────────────────────────────────────────────────
# 3. Outreach Enrichment
# ─────────────────────────────────────────────────────────────────────────────

_outreach_enrich_router = APIRouter(
    prefix="/leads",
    tags=["Outreach Enrichment"],
)

_OUTREACH_DESC = """
Returns AI-generated outreach assets for a lead.

**How to call:**
```
POST /api/leads/view/outreach
Content-Type: application/json

{ "leadenrich_id": "abc123def456", "system_prompt": "..." }
```
`system_prompt` is optional — omit it to get stored outreach data only.
When provided, fresh outreach copy is generated live and returned in `ai_generated.outreach`.

**Fields returned:**
- **cold_email** — Personalised cold email (subject + body)
- **linkedin_note** — Short LinkedIn connection message
- **sequence** — Multi-step follow-up sequence
- **best_time** — Recommended send time based on activity
- **warm_signal** — Latest warm engagement signal
- **best_channel** — Recommended outreach channel
- **outreach_angle** — Primary pitch angle
- **ai_generated** — Present only when `system_prompt` is supplied
"""


@_outreach_enrich_router.post(
    "/view/outreach",
    summary="Outreach Enrichment",
    description=_OUTREACH_DESC,
)
async def outreach_enrichment(body: ViewOutreachRequest):
    if not body.leadenrich_id and not body.lead_data:
        raise HTTPException(400, "Provide either leadenrich_id or lead_data.")

    lead = None
    if body.leadenrich_id:
        try:
            lead = await _resolve_lead(body.leadenrich_id)
        except HTTPException:
            pass

    if lead is None:
        if not body.lead_data:
            raise HTTPException(404, f"Lead not found: {body.leadenrich_id}. Provide lead_data as a fallback.")
        lead = _lead_from_ai_information(body.lead_data)

    system_prompt = body.system_prompt or _DEFAULT_OUTREACH_SYSTEM
    lead_id = lead.get("id")

    # Always generate fresh outreach on every call
    ai_outreach = None
    try:
        lead_ctx = _build_outreach_lead_ctx(lead)
        raw = await _quick_llm(
            system_prompt,
            f"Generate personalised B2B outreach for this lead.\n\n{lead_ctx}\n\n"
            "Return ONLY a valid JSON object — no explanation, no markdown — with this exact schema: "
            "{\"cold_email\": {\"subject\": \"...\", \"body\": \"...\"}, "
            "\"linkedin_note\": \"...\", \"best_channel\": \"...\", \"best_time\": \"...\", \"outreach_angle\": \"...\"}",
            max_tokens=2000,
        )
        try:
            cleaned = raw.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
            import re as _re
            _match = _re.search(r'\{[\s\S]*\}', cleaned)
            if _match:
                cleaned = _match.group(0)
            ai_outreach = json.loads(cleaned)
        except Exception as _je:
            logger.warning("[OutreachView] JSON parse failed: %s | raw=%s", _je, raw[:300])
            ai_outreach = None
    except Exception as _e:
        logger.warning("[OutreachView] AI generation failed: %s", _e)

    # Save generated outreach to DB
    if ai_outreach and lead_id:
        ai_email = ai_outreach.get("cold_email") or {}
        try:
            from db import get_pool
            async with get_pool().acquire() as _conn:
                await _conn.execute(
                    "UPDATE enriched_leads SET email_subject=$1, cold_email=$2, linkedin_note=$3 WHERE id=$4",
                    ai_email.get("subject", ""),
                    ai_email.get("body", ""),
                    ai_outreach.get("linkedin_note", ""),
                    lead_id,
                )
            await _lead_cache_delete(lead_id)
        except Exception as _e:
            logger.warning("[OutreachView] DB update failed for lead=%s: %s", lead_id, _e)

    # ── Resolve subject / body / linkedin_note ───────────────────────────────
    # Priority: LLM output → stored DB values → template fallback
    # Every field is guaranteed non-empty before building the response.

    _tmpl = None  # lazy-loaded template

    def _tmpl_fallback() -> dict:
        nonlocal _tmpl
        if _tmpl is None:
            _tmpl = _template_outreach(lead)
        return _tmpl

    if ai_outreach:
        ai_email = ai_outreach.get("cold_email") or {}
        _subject      = ai_email.get("subject", "").strip()
        _body         = ai_email.get("body", "").strip()
        _linkedin_note = ai_outreach.get("linkedin_note", "").strip()
    else:
        _subject = _body = _linkedin_note = ""

    # Fill gaps from DB if LLM left any field empty
    if not _subject or not _body or not _linkedin_note:
        full         = _get_full_data(lead)
        outreach_data = full.get("outreach") or {}
        if not _subject:
            _subject = (lead.get("email_subject") or outreach_data.get("email_subject") or "").strip()
        if not _body:
            _body = (lead.get("cold_email") or outreach_data.get("cold_email") or "").strip()
        if not _linkedin_note:
            _linkedin_note = (lead.get("linkedin_note") or outreach_data.get("linkedin_note") or "").strip()

    # Final guarantee — if still empty after DB, use template
    if not _subject:
        logger.warning("[OutreachView] subject empty after LLM+DB for lead=%s — using template", lead_id)
        _subject = _tmpl_fallback()["cold_email"]["subject"]
    if not _body:
        logger.warning("[OutreachView] body empty after LLM+DB for lead=%s — using template", lead_id)
        _body = _tmpl_fallback()["cold_email"]["body"]
    if not _linkedin_note:
        logger.warning("[OutreachView] linkedin_note empty after LLM+DB for lead=%s — using template", lead_id)
        _linkedin_note = _tmpl_fallback()["linkedin_note"]

    cold_email_block = {
        "subject":    _subject,
        "greeting":   "",
        "opening":    "",
        "body":       _body,
        "cta":        "",
        "sign_off":   "",
        "full_email": _body,
    }
    linkedin_note_val = _linkedin_note

    # Use template meta fields if ai_outreach has no channel/time/angle
    if not ai_outreach:
        ai_outreach = _tmpl_fallback()

    full = _get_full_data(lead)
    outreach_data = full.get("outreach") or {}

    return {
        "lead_id":      lead.get("id"),
        "linkedin_url": lead.get("linkedin_url"),
        "name":         lead.get("name"),
        "title":        lead.get("title"),
        "company":      lead.get("company"),
        "score_tier":   lead.get("score_tier"),

        "cold_email":     cold_email_block,
        "linkedin_note":  linkedin_note_val,
        "follow_up":      {"day3": "", "day7": ""},

        "sequence":       _parse_json_field(lead, "outreach_sequence", {}),
        "best_time":      (ai_outreach.get("best_time") if ai_outreach else None) or outreach_data.get("best_time") or lead.get("best_send_time"),
        "best_channel":   (ai_outreach.get("best_channel") if ai_outreach else None) or lead.get("best_channel"),
        "outreach_angle": (ai_outreach.get("outreach_angle") if ai_outreach else None) or lead.get("outreach_angle"),
        "warm_signal":    lead.get("warm_signal"),

        "ai_generated":   {"outreach": ai_outreach} if ai_outreach else None,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 4. Company Enrichment
# ─────────────────────────────────────────────────────────────────────────────

_company_enrich_router = APIRouter(
    prefix="/leads",
    tags=["Company Enrichment"],
)

_COMPANY_DESC = """
You are an expert B2B CRM data analyst. Your job is to enrich raw company data
from LinkedIn into a fully structured, CRM-ready profile.

You must return ONLY a valid JSON object — no markdown, no explanation, no preamble.
Follow the exact schema provided. Infer and derive fields intelligently from the data.
""".strip()


def build_prompt(raw_json: str) -> str:
    return f"""
Enrich the following LinkedIn company data into a complete CRM profile.

RAW DATA:
{raw_json}

Return a JSON object with EXACTLY this schema:

{{
  "company_identity": {{
    "legal_name": "",
    "brand_name": "",
    "tagline": "",
    "description_short": "",          // 1-sentence, professional tone
    "description_long": "",           // 3-5 sentences, for CRM about section
    "founded_year": 0,
    "company_stage": "",              // Startup / Growth / Mature / Enterprise
    "organization_type": "",
    "industry_primary": "",
    "industry_secondary": [],
    "hq_city": "",
    "hq_state": "",
    "hq_country": "",
    "hq_full_address": "",
    "office_locations": [],
    "global_reach": true,
    "website": "",
    "linkedin_url": "",
    "crunchbase_url": ""
  }},

  "tags": [],                          // 8-12 broad CRM tags e.g. "SaaS", "B2B", "IT Services"

  "tech_tags": [],                     // 10-15 specific tech tags e.g. "Blockchain", "React Native"

  "services": [
    {{
      "name": "",
      "category": "",                  // e.g. "Development", "Marketing", "Consulting"
      "description": "",
      "target_audience": ""
    }}
  ],

  "key_people": [
    {{
      "name": "",
      "title": "",
      "seniority": "",                 // C-Suite / VP / Manager / Individual Contributor
      "department": "",
      "linkedin_url": "",
      "is_decision_maker": false,
      "contact_confidence": ""        // High / Medium / Low
    }}
  ],

  "contact_info": {{
    "primary_email": "",
    "emails": [],
    "phone_primary": "",
    "phones": [],
    "whatsapp": "",
    "preferred_contact_channel": ""
  }},

  "financials": {{
    "funding_status": "",             // Bootstrapped / Seed / Series A / etc.
    "total_funding_usd": 0,
    "last_round_type": "",
    "last_round_date": "",
    "last_round_amount_usd": 0,
    "revenue_range": "",              // e.g. "$1M-$5M" — infer from size/stage
    "employee_count_linkedin": 0,
    "employee_count_range": ""
  }},

  "social_presence": {{
    "linkedin_followers": 0,
    "linkedin_engagement_avg": "",    // Low / Medium / High — from post likes
    "posting_frequency": "",          // Active / Moderate / Inactive
    "content_themes": [],             // e.g. "Team Culture", "Tech Insights", "Product"
    "last_post_date": ""
  }},

  "activities": [
    {{
      "date": "",
      "type": "",                     // Post / Event / Announcement / Award
      "summary": "",
      "engagement_score": 0,          // likes + comments
      "sentiment": ""                 // Positive / Neutral / Promotional
    }}
  ],

  "interests": [],                    // 8-10 topics the company cares about
                                      // e.g. "AI/ML", "Startup Ecosystem", "Women in Tech"

  "icp_fit": {{
    "ideal_customer_profile": "",     // Who they typically sell to
    "target_company_size": "",
    "target_industries": [],
    "target_geographies": [],
    "deal_type": ""                   // Project-based / Retainer / SaaS / Mixed
  }},

  "competitive_intelligence": {{
    "direct_competitors": [],
    "market_position": "",            // Niche / Mid-Market / Enterprise
    "differentiators": [],
    "weaknesses_inferred": []         // inferred from data gaps or signals
  }},

  "crm_metadata": {{
    "lead_score": 0,                  // 0–100, based on activity, size, funding
    "lead_temperature": "",           // Hot / Warm / Cold
    "outreach_priority": "",          // High / Medium / Low
    "best_outreach_time": "",         // inferred from post timing
    "recommended_first_message": "",  // 1–2 sentence icebreaker for outreach
    "crm_tags": [],
    "data_source": "LinkedIn",
    "data_scraped_at": "",
    "enrichment_confidence": ""       // High / Medium / Low
  }}
}}"""


async def _do_company_enrichment(
    company_linkedin: str, lead: Optional[dict], org_id: str
) -> dict:
    """Full enrichment pipeline: BrightData scrape → LLM → DB save. Returns result dict."""
    from Lead_enrichment.company_lead_enrichment import company_service as cs
    from datetime import datetime, timezone

    # Step 1: BrightData scrape
    bd_data = await cs._fetch_bd_company_profile(company_linkedin)
    if not bd_data:
        raise ValueError("BrightData returned no data for this company LinkedIn URL")

    # Step 2: Apollo raw from DB (read previously saved data — no live Apollo API call)
    apollo_raw: dict = {}
    if lead and lead.get("id"):
        try:
            from db import get_pool
            async with get_pool().acquire() as _conn:
                _row = await _conn.fetchrow(
                    "SELECT apollo_raw FROM enriched_leads WHERE id=$1 AND apollo_raw IS NOT NULL LIMIT 1",
                    lead["id"],
                )
            if _row and _row["apollo_raw"]:
                _parsed = json.loads(_row["apollo_raw"]) if isinstance(_row["apollo_raw"], str) else _row["apollo_raw"]
                if isinstance(_parsed, dict):
                    apollo_raw = _parsed
        except Exception:
            pass

    # Step 3: LLM enrichment
    crm_brief: Optional[dict] = None
    try:
        crm_brief = await svc._enrich_company_with_llm(
            bd_data, apollo_raw,
            bd_data.get("name") or (lead.get("company") if lead else "") or ""
        )
    except Exception as _e:
        logger.warning("[ViewCompany] LLM failed: %s", _e)

    # Step 4: Save to DB
    try:
        now = datetime.now(timezone.utc).isoformat()
        record = {
            "id":                   cs._company_id(company_linkedin),
            "company_linkedin_url": company_linkedin,
            "org_id":               org_id,
            "raw_data":             json.dumps(bd_data, default=str),
            "apollo_raw":           json.dumps(apollo_raw, default=str) if apollo_raw else None,
            "crm_brief":            json.dumps(crm_brief, default=str) if crm_brief else None,
            "last_enriched_at":     now,
            "updated_at":           now,
        }
        cs._map_bd_company_profile(bd_data, record)
        await cs._upsert_company(record)
    except Exception as _e:
        logger.warning("[ViewCompany] DB save failed: %s", _e)

    return {
        "lead_id":          lead.get("id") if lead else None,
        "linkedin_url":     lead.get("linkedin_url") if lead else None,
        "company_linkedin": company_linkedin,
        "brightdata":       bd_data,
        "apollo_raw":       apollo_raw,
        "crm_brief":        crm_brief,
        "cached":           False,
    }


@_company_enrich_router.post(
    "/view/company",
    summary="Company Enrichment",
    description=_COMPANY_DESC,
)
async def company_enrichment(body: ViewCompanyRequest):
    import re as _re
    from Lead_enrichment.bulk_lead_enrichment._company import enrich_company_waterfall
    from db import get_pool

    if not body.leadenrich_id and not body.company_linkedin_url:
        raise HTTPException(
            status_code=400,
            detail="Provide either leadenrich_id or company_linkedin_url.",
        )

    def _pj(val):
        """Parse JSON string → dict/list, or return as-is."""
        if isinstance(val, str):
            try:
                return json.loads(val)
            except Exception:
                return val
        return val

    # ── Resolve lead + company LinkedIn URL ──────────────────────────────────
    lead: Optional[dict] = None
    company_linkedin = ""

    if body.leadenrich_id:
        lead = await svc.get_lead(body.leadenrich_id)
        if not lead:
            raise HTTPException(status_code=404, detail=f"Lead not found: {body.leadenrich_id}")

        company_linkedin = lead.get("company_linkedin") or ""

        # Parse raw_brightdata for fallback lookups
        raw_bd = _pj(lead.get("raw_brightdata") or {})
        if not isinstance(raw_bd, dict):
            raw_bd = {}

        # Fallback 1: raw_brightdata known fields
        if not company_linkedin:
            for _f in ("current_company_link", "company_linkedin_url", "company_url", "linkedin"):
                _v = (raw_bd.get(_f) or "").strip()
                if _v:
                    if "linkedin.com/company/" in _v:
                        company_linkedin = _v
                        break
                    elif "/" not in _v and len(_v) < 80:
                        company_linkedin = f"https://www.linkedin.com/company/{_v}/"
                        break

        # Fallback 1b: company_id in raw_brightdata
        if not company_linkedin:
            _co_id = (raw_bd.get("current_company_company_id") or raw_bd.get("company_id") or "").strip()
            if _co_id:
                company_linkedin = f"https://www.linkedin.com/company/{_co_id}/"

        # Fallback 1c: experience[0] in raw_brightdata
        if not company_linkedin:
            _exp = raw_bd.get("experience") or []
            if _exp and isinstance(_exp[0], dict):
                _e0 = _exp[0]
                for _ef in ("url", "company_url", "linkedin_url", "company_linkedin_url"):
                    _v = (_e0.get(_ef) or "").strip()
                    if _v and "linkedin.com/company/" in _v:
                        company_linkedin = _v
                        break
                if not company_linkedin:
                    _eid = (_e0.get("company_id") or _e0.get("linkedin_company_id") or
                            _e0.get("company_linkedin_id") or "").strip()
                    if _eid:
                        company_linkedin = f"https://www.linkedin.com/company/{_eid}/"

        # Fallback 2: apollo_raw → organization.linkedin_url
        if not company_linkedin:
            _ap_raw = _pj(lead.get("apollo_raw") or {})
            if isinstance(_ap_raw, dict):
                _ap = (_ap_raw.get("organization") or {}).get("linkedin_url") or ""
                _ap = _ap.strip()
                if _ap:
                    if "linkedin.com/company/" in _ap:
                        company_linkedin = _ap
                    elif "/" not in _ap:
                        company_linkedin = f"https://www.linkedin.com/company/{_ap}/"

        # Fallback 3: linkedin_enrich → company_identity.linkedin_url
        if not company_linkedin:
            _li = _pj(lead.get("linkedin_enrich") or {})
            if isinstance(_li, dict):
                _ci = (_li.get("company_identity") or {}).get("linkedin_url") or ""
                if "linkedin.com/company/" in _ci:
                    company_linkedin = _ci.strip()

        # Fallback 4: full_data → company_identity / company_profile
        if not company_linkedin:
            _fd = _pj(lead.get("full_data") or {})
            if isinstance(_fd, dict):
                for _path in [
                    (_fd.get("company_identity") or {}).get("linkedin_url"),
                    (_fd.get("company_profile") or {}).get("linkedin_url"),
                ]:
                    if _path and "linkedin.com/company/" in str(_path):
                        company_linkedin = str(_path).strip()
                        break

        if not company_linkedin:
            logger.warning(
                "[ViewCompany] No company_linkedin for lead=%s company=%s website=%s",
                body.leadenrich_id, lead.get("company"), lead.get("company_website"),
            )
            raise HTTPException(
                status_code=400,
                detail=(
                    f"No company LinkedIn URL found for lead '{lead.get('name', body.leadenrich_id)}'. "
                    f"Company: {lead.get('company') or 'unknown'}. "
                    "Please pass company_linkedin_url directly in the request body."
                ),
            )

        # Persist resolved URL back to DB if it was missing
        if company_linkedin and not lead.get("company_linkedin"):
            try:
                await svc._upsert_lead({"id": lead["id"], "company_linkedin": company_linkedin})
                await _lead_cache_delete(body.leadenrich_id)
            except Exception:
                pass

    else:
        company_linkedin = (body.company_linkedin_url or "").strip()
        lead = None

    # ── Cache check — enriched_leads.company_crm_brief ───────────────────────
    # Only cache when we have a lead_id (so we know which row to check)
    if lead and lead.get("id"):
        raw_crm = lead.get("company_crm_brief")
        crm_cached = _pj(raw_crm) if raw_crm else None
        if crm_cached and isinstance(crm_cached, dict) and crm_cached:
            logger.info("[ViewCompany] Cache HIT for lead=%s", lead["id"])
            return {
                "success":          True,
                "cached":           True,
                "lead_id":          lead["id"],
                "linkedin_url":     lead.get("linkedin_url"),
                "company_linkedin": company_linkedin,
                "crm_brief":        crm_cached,
            }

    # ── Deduplicate concurrent requests via per-company lock ─────────────────
    lock_key = company_linkedin.lower().rstrip("/")
    if lock_key not in _company_locks:
        _company_locks[lock_key] = asyncio.Lock()
    _company_locks_ref[lock_key] = _company_locks_ref.get(lock_key, 0) + 1
    lock = _company_locks[lock_key]

    try:
        async with lock:
            # Double-check cache after acquiring lock (another request may have finished)
            if lead and lead.get("id"):
                try:
                    async with get_pool().acquire() as _c:
                        _row = await _c.fetchrow(
                            "SELECT company_crm_brief FROM enriched_leads WHERE id=$1", lead["id"]
                        )
                    if _row and _row["company_crm_brief"]:
                        crm_fresh = _pj(_row["company_crm_brief"])
                        if crm_fresh and isinstance(crm_fresh, dict):
                            logger.info("[ViewCompany] Cache HIT (post-lock) for lead=%s", lead["id"])
                            return {
                                "success":          True,
                                "cached":           True,
                                "lead_id":          lead["id"],
                                "linkedin_url":     lead.get("linkedin_url"),
                                "company_linkedin": company_linkedin,
                                "crm_brief":        crm_fresh,
                            }
                except Exception:
                    pass

            # ── Run the new waterfall ─────────────────────────────────────────
            logger.info("[ViewCompany] Cache MISS — running waterfall for %s", company_linkedin)

            # Build profile dict for the waterfall (same shape as BD person profile)
            profile: dict = {}
            company_name = ""
            domain = ""
            lead_id = ""

            if lead:
                lead_id = lead.get("id") or ""
                company_name = lead.get("company") or ""
                website = lead.get("company_website") or ""
                if website:
                    _m = _re.search(r"https?://(?:www\.)?([^/?\s]+)", website)
                    if _m:
                        domain = _m.group(1).lower()
                profile = {
                    "current_company_link":              company_linkedin,
                    "current_company_logo":              lead.get("company_logo") or "",
                    "current_company_description":       lead.get("company_description") or "",
                    "current_company_employees_count":   lead.get("employee_count") or 0,
                    "current_company_industry":          lead.get("industry") or "",
                    "current_company_founded":           lead.get("founded_year") or "",
                }
            else:
                # Only company_linkedin_url was given — minimal profile
                profile = {"current_company_link": company_linkedin}

            try:
                company_data, _ = await asyncio.wait_for(
                    enrich_company_waterfall(
                        company_name=company_name,
                        domain=domain,
                        profile=profile,
                        lead_id=lead_id,
                    ),
                    timeout=150,
                )
            except asyncio.TimeoutError:
                logger.error("[ViewCompany] Waterfall timed out for %s", company_linkedin)
                return {
                    "success":          False,
                    "cached":           False,
                    "lead_id":          lead["id"] if lead else None,
                    "linkedin_url":     lead.get("linkedin_url") if lead else None,
                    "company_linkedin": company_linkedin,
                    "crm_brief":        None,
                    "error":            "Enrichment timed out (>150s). Please retry.",
                }
            except Exception as _e:
                logger.error("[ViewCompany] Waterfall error for %s: %s", company_linkedin, _e, exc_info=True)
                return {
                    "success":          False,
                    "cached":           False,
                    "lead_id":          lead["id"] if lead else None,
                    "linkedin_url":     lead.get("linkedin_url") if lead else None,
                    "company_linkedin": company_linkedin,
                    "crm_brief":        None,
                    "error":            str(_e),
                }

            crm_brief = company_data.get("company_crm_brief") or None

            # Persist company fields + crm_brief to enriched_leads (if we have a lead)
            if lead_id and company_data:
                _fields = [
                    "company_logo", "company_description", "company_phone", "company_linkedin",
                    "company_twitter", "company_website", "employee_count", "industry",
                    "tech_stack", "hq_location", "founded_year", "funding_stage", "total_funding",
                    "annual_revenue", "hiring_velocity",
                ]
                _updates = {k: company_data[k] for k in _fields if k in company_data and company_data[k]}
                if crm_brief:
                    _updates["company_crm_brief"] = json.dumps(crm_brief, default=str) if isinstance(crm_brief, dict) else crm_brief
                if _updates:
                    try:
                        _ser = {}
                        for k, v in _updates.items():
                            _ser[k] = json.dumps(v, default=str) if isinstance(v, (list, dict)) else v
                        _set = ", ".join(f"{k} = ${i+1}" for i, k in enumerate(_ser))
                        _vals = list(_ser.values()) + [lead_id]
                        async with get_pool().acquire() as _c:
                            await _c.execute(
                                f"UPDATE enriched_leads SET {_set}, updated_at=NOW() WHERE id=${len(_vals)}",
                                *_vals,
                            )
                        logger.info("[ViewCompany] enriched_leads updated for lead=%s", lead_id)
                    except Exception as _de:
                        logger.warning("[ViewCompany] DB update failed for lead=%s: %s", lead_id, _de)

            _resp = {
                "success":          True,
                "cached":           False,
                "lead_id":          lead["id"] if lead else None,
                "linkedin_url":     lead.get("linkedin_url") if lead else None,
                "company_linkedin": company_linkedin,
                "crm_brief":        crm_brief,
            }
            if crm_brief is None:
                _resp["crm_brief_error"] = (
                    company_data.get("_llm_error")
                    or "LLM failed to generate company CRM brief — check HuggingFace credits (402 Payment Required)"
                )
            return _resp

    finally:
        _company_locks_ref[lock_key] -= 1
        if _company_locks_ref[lock_key] == 0:
            _company_locks.pop(lock_key, None)
            _company_locks_ref.pop(lock_key, None)


async def _enrich_company_with_ai(bd: dict) -> dict:
    """
    Pass raw BrightData company data to the LLM and generate lead-enrichment insights:
    company summary, ICP fit, pain points, buying signals, intent signals, outreach angle.
    """
    now_iso = __import__("datetime").datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    posts_text = " | ".join(
        p.get("text", "")[:200]
        for p in (bd.get("updates") or bd.get("posts") or [])[:5]
        if p.get("text")
    )
    similar = ", ".join(
        s.get("title", "") for s in (bd.get("similar") or [])[:5] if s.get("title")
    )
    employees_sample = ", ".join(
        e.get("title", "") for e in (bd.get("employees") or [])[:4] if e.get("title")
    )
    additional_info = str(bd.get("additional_information") or "")[:400]

    context = f"""
Company Name: {bd.get("name", "")}
LinkedIn URL: {bd.get("url", "")}
Industry: {bd.get("industries", "")}
Organization Type: {bd.get("organization_type", "")}
Company Size: {bd.get("company_size", "")}
Employees on LinkedIn: {bd.get("employees_in_linkedin", "")}
Headquarters: {bd.get("headquarters", "")}
Founded: {bd.get("founded", "")}
Website: {bd.get("website", "")}
Followers: {bd.get("followers", "")}
Slogan: {bd.get("slogan", "")}
About: {str(bd.get("about") or bd.get("description") or "")[:600]}
Recent Posts (sample): {posts_text[:600]}
Similar Companies: {similar}
Notable Employees: {employees_sample}
Additional Info: {additional_info}
""".strip()

    system_prompt = (
        "You are a B2B lead enrichment AI. Given raw LinkedIn company data from BrightData, "
        "generate structured sales intelligence in JSON. Be concise and data-driven. "
        "Return only valid JSON — no markdown, no explanation."
    )

    user_prompt = f"""Analyse this company as a B2B sales prospect and return JSON with these exact keys:

{{
  "company_summary": "2-3 sentence summary of what the company does and its market position",
  "icp_fit": {{
    "score": 0-100,
    "tier": "HOT | WARM | COOL | COLD",
    "explanation": "why this score"
  }},
  "pain_points": ["pain point 1", "pain point 2", "pain point 3"],
  "buying_signals": ["signal 1", "signal 2"],
  "intent_signals": {{
    "hiring_activity": "what roles they are hiring — from additional_info/posts",
    "recent_news": "any notable events from posts",
    "growth_stage": "early / scaling / enterprise / government"
  }},
  "outreach": {{
    "recommended_angle": "the best angle to approach this company",
    "cold_email_subject": "subject line",
    "cold_email_body": "max 100 words, personalised to their context",
    "linkedin_note": "max 300 chars connection note"
  }},
  "tags": ["tag1", "tag2", "tag3", "tag4"],
  "account_pitch": "one sentence pitch for this account"
}}

Company data:
{context}

Enrichment date: {now_iso}"""

    try:
        raw = await _quick_llm(system_prompt, user_prompt, max_tokens=2000)
        m = __import__("re").search(r"\{.*\}", raw, __import__("re").DOTALL)
        if m:
            return json.loads(m.group())
        return {"raw": raw}
    except Exception as _e:
        logger.warning("[ViewCompany] AI enrichment failed: %s", _e)
        return {"error": str(_e)}


async def _company_ai_analysis(lead: dict, full: dict, system_prompt: Optional[str]) -> Optional[dict]:
    """Run LLM company analysis when caller provides a system_prompt."""
    if not system_prompt or not lead.get("company"):
        return None
    try:
        wi = full.get("website_intelligence") or {}
        company_ctx = (
            f"Company: {lead.get('company')} | Domain: {lead.get('company_domain','')} | "
            f"Industry: {lead.get('industry','')} | Employees: {lead.get('employee_count','')} | "
            f"Revenue: {lead.get('revenue','')} | Location: {lead.get('location','')} | "
            f"Tech stack: {_parse_json_field(lead, 'wappalyzer_tech', [])} | "
            f"Website summary: {str(wi)[:800]}"
        )
        result = await _quick_llm(
            system_prompt,
            f"Analyse this company as a potential sales prospect.\n\n{company_ctx}\n\n"
            "Return JSON: {{\"fit_summary\": \"...\", \"key_pain_points\": [...], "
            "\"buying_signals\": [...], \"recommended_approach\": \"...\", \"icp_score\": 0-100}}",
            max_tokens=1500,
        )
        return {"company_analysis": result}
    except Exception as _e:
        logger.warning("[CompanyView] AI analysis failed: %s", _e)
        return None

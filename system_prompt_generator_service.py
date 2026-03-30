"""
system_prompt_generator_service.py
------------------------------------
Two-phase flow:

  Phase 1 — Scrape
    scrape_url(url) → returns raw_data dict (LinkedIn profile OR website content)

  Phase 2 — Generate prompts
    generate_section_prompt(section_key, raw_data) → returns a ready-to-use
    system prompt string (plain text, not JSON) for that section.

Sections:
  identity, contact, scores, icp_match, behavioural_signals,
  pitch_intelligence, activity, tags, outreach, person_analysis
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from typing import Optional
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)

# ── Key helpers ────────────────────────────────────────────────────────────────

def _k(name: str, default: str = "") -> str:
    try:
        import keys_service as _ks
        return _ks.get(name) or os.getenv(name, default)
    except Exception:
        return os.getenv(name, default)

def _bd_api_key()         -> str: return _k("BRIGHT_DATA_API_KEY")
def _bd_profile_dataset() -> str: return _k("BD_PROFILE_DATASET_ID", "gd_l1viktl72bvl7bjuj0")
def _groq_key()           -> str: return _k("GROQ_API_KEY")
def _groq_gen_model()     -> str: return _k("GROQ_LLM_MODEL", "llama-3.3-70b-versatile")
def _wb_llm_host()        -> str: return _k("WB_LLM_HOST", "http://ai-llm.worksbuddy.ai")
def _wb_llm_key()         -> str: return _k("WB_LLM_API_KEY")
def _wb_llm_model()       -> str: return _k("WB_LLM_DEFAULT_MODEL", "wb-pro")
def _hf_token()           -> str: return _k("HF_TOKEN")
def _hf_model()           -> str: return _k("HF_MODEL", "meta-llama/Meta-Llama-3.1-70B-Instruct")

BD_BASE = "https://api.brightdata.com/datasets/v3"

# ── HTTP clients ───────────────────────────────────────────────────────────────

_HTTP_LIMITS = httpx.Limits(max_connections=20, max_keepalive_connections=10)
_bd_client: Optional[httpx.AsyncClient] = None
_web_client: Optional[httpx.AsyncClient] = None


def _get_bd_client() -> httpx.AsyncClient:
    global _bd_client
    if _bd_client is None or _bd_client.is_closed:
        _bd_client = httpx.AsyncClient(timeout=90.0, limits=_HTTP_LIMITS)
    return _bd_client


def _get_web_client() -> httpx.AsyncClient:
    global _web_client
    if _web_client is None or _web_client.is_closed:
        _web_client = httpx.AsyncClient(
            timeout=30.0, limits=_HTTP_LIMITS, follow_redirects=True,
        )
    return _web_client


# ── LinkedIn URL normalisation ─────────────────────────────────────────────────

_LI_RE = re.compile(r"linkedin\.com/in/([^/?#\s]+)", re.IGNORECASE)

def _normalize_linkedin(url: str) -> str:
    m = _LI_RE.search(url)
    if not m:
        return url.strip()
    return f"https://www.linkedin.com/in/{m.group(1).rstrip('/')}/"


def is_linkedin_url(url: str) -> bool:
    return bool(_LI_RE.search(url))


# ── Phase 1: Scrape ────────────────────────────────────────────────────────────

async def scrape_linkedin_profile(linkedin_url: str) -> dict:
    """Trigger BrightData snapshot, poll until ready, return first profile."""
    api_key    = _bd_api_key()
    dataset_id = _bd_profile_dataset()
    if not api_key:
        raise RuntimeError("BRIGHT_DATA_API_KEY not configured")

    url_norm = _normalize_linkedin(linkedin_url)
    client   = _get_bd_client()

    trigger = await client.post(
        f"{BD_BASE}/trigger",
        params={"dataset_id": dataset_id, "include_errors": "true"},
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json=[{"url": url_norm}],
    )
    trigger.raise_for_status()
    snapshot_id = trigger.json().get("snapshot_id")
    if not snapshot_id:
        raise RuntimeError(f"BrightData trigger returned no snapshot_id: {trigger.text[:300]}")

    logger.info("[SPG] BrightData snapshot: %s for %s", snapshot_id, url_norm)

    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        await asyncio.sleep(5)
        st = await client.get(
            f"{BD_BASE}/snapshot/{snapshot_id}/status",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        st.raise_for_status()
        state = st.json().get("status", "")
        if state == "ready":
            break
        if state in ("failed", "error"):
            raise RuntimeError(f"BrightData snapshot failed: {st.json()}")

    res = await client.get(
        f"{BD_BASE}/snapshot/{snapshot_id}",
        headers={"Authorization": f"Bearer {api_key}"},
        params={"format": "json"},
    )
    res.raise_for_status()
    rows = res.json()
    profiles = [r for r in rows if not r.get("error") and r.get("name")]
    if not profiles:
        raise RuntimeError(f"BrightData returned only errors: {rows[0] if rows else 'empty'}")
    return profiles[0]


async def scrape_website(website_url: str) -> dict:
    """Scrape homepage + /about, return structured text dict with LLM-extracted fields."""
    client = _get_web_client()
    headers = {"User-Agent": "Mozilla/5.0 (compatible; WorksBuddy/1.0)"}

    async def _fetch(url: str) -> tuple[str, str]:
        try:
            r = await client.get(url, headers=headers)
            r.raise_for_status()
            html = r.text
        except Exception as e:
            logger.warning("[SPG] fetch %s: %s", url, e)
            return "", ""
        title_m = re.search(r"<title[^>]*>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
        title = title_m.group(1).strip() if title_m else ""
        text = re.sub(r"<style[^>]*>.*?</style>", " ", html, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r"<script[^>]*>.*?</script>", " ", text, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return title, text[:8000]

    parsed = urlparse(website_url)
    base   = f"{parsed.scheme}://{parsed.netloc}"

    title, homepage = await _fetch(website_url)
    _, about        = await _fetch(f"{base}/about")

    desc = ""
    try:
        r = await client.get(website_url, headers=headers)
        m = re.search(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']', r.text, re.IGNORECASE)
        desc = m.group(1).strip() if m else ""
    except Exception:
        pass

    raw = {
        "url": website_url, "title": title,
        "description": desc, "homepage_text": homepage, "about_text": about,
    }

    # LLM extraction — pull structured fields from the raw page text
    combined_text = f"{homepage}\n\n{about}"[:6000]
    extract_system = (
        "You are a data extraction assistant. Extract structured company information from website text. "
        "Return ONLY a valid JSON object with these keys (use null if not found, arrays for lists):\n"
        "company_name, tagline, products (array), services (array), target_audience, industry, "
        "key_features (array of short phrases), company_size, founded_year, location, social_links (object with keys like linkedin/twitter/facebook)"
    )
    extract_user = (
        f"Extract company information from this website text:\n\nURL: {website_url}\nTitle: {title}\nMeta description: {desc}\n\n{combined_text}"
    )
    try:
        llm_raw = await _call_llm(extract_system, extract_user, groq_model="llama-3.1-8b-instant", max_tokens=1500)
        # Strip markdown fences if present
        llm_clean = re.sub(r"^```(?:json)?\s*|\s*```$", "", llm_raw.strip(), flags=re.MULTILINE)
        extracted = json.loads(llm_clean)
        if isinstance(extracted, dict):
            raw.update({k: v for k, v in extracted.items() if v is not None})
    except Exception as e:
        logger.warning("[SPG] LLM extraction failed for %s: %s", website_url, e)

    return raw


async def scrape_url(url: str) -> tuple[str, dict]:
    """
    Scrape a URL and return (source_type, raw_data).
    source_type is 'linkedin' or 'website'.
    """
    if is_linkedin_url(url):
        return "linkedin", await scrape_linkedin_profile(url)
    return "website", await scrape_website(url)


# ── LLM call ──────────────────────────────────────────────────────────────────

async def _call_llm(
    system: str,
    user: str,
    groq_model: str = "",
    max_tokens: int = 2000,
    temperature: float = 0.4,
) -> str:
    """
    Try providers in order: WorksBuddy LLM → HuggingFace featherless → Groq.
    groq_model defaults to GROQ_LLM_MODEL config key (llama-3.3-70b-versatile).
    """
    errors = []
    messages = [{"role": "system", "content": system}, {"role": "user", "content": user}]

    # 1. WorksBuddy internal LLM
    wb_host = _wb_llm_host()
    wb_key  = _wb_llm_key()
    if wb_host and wb_key:
        try:
            async with httpx.AsyncClient(timeout=90) as c:
                r = await c.post(
                    f"{wb_host.rstrip('/')}/v1/chat/completions",
                    headers={"Content-Type": "application/json", "Authorization": f"Bearer {wb_key}"},
                    json={"model": _wb_llm_model(), "messages": messages,
                          "temperature": temperature, "max_tokens": max_tokens},
                )
                if r.is_success:
                    return r.json()["choices"][0]["message"]["content"].strip()
                errors.append(f"WB LLM: HTTP {r.status_code}")
        except Exception as e:
            errors.append(f"WB LLM: {e}")

    # 2. HuggingFace featherless-ai (OpenAI-compatible)
    hf_token = _hf_token()
    if hf_token:
        hf_mdl = _hf_model()
        try:
            async with httpx.AsyncClient(timeout=90) as c:
                r = await c.post(
                    "https://api-inference.huggingface.co/v1/chat/completions",
                    headers={"Content-Type": "application/json", "Authorization": f"Bearer {hf_token}"},
                    json={"model": hf_mdl, "messages": messages,
                          "temperature": temperature, "max_tokens": max_tokens},
                )
                if r.is_success:
                    return r.json()["choices"][0]["message"]["content"].strip()
                try:
                    body_txt = r.json().get("error", {}).get("message") or r.text
                except Exception:
                    body_txt = r.text
                errors.append(f"HuggingFace {hf_mdl}: HTTP {r.status_code}: {body_txt}")
        except Exception as e:
            errors.append(f"HuggingFace: {e}")

    # 3. Groq
    g_key = _groq_key()
    g_mdl = groq_model or _groq_gen_model()
    if g_key:
        try:
            async with httpx.AsyncClient(timeout=90) as c:
                r = await c.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={"Content-Type": "application/json", "Authorization": f"Bearer {g_key}"},
                    json={"model": g_mdl, "messages": messages,
                          "temperature": temperature, "max_tokens": max_tokens},
                )
                if r.is_success:
                    return r.json()["choices"][0]["message"]["content"].strip()
                try:
                    body_txt = r.json().get("error", {}).get("message") or r.text
                except Exception:
                    body_txt = r.text
                errors.append(f"Groq {g_mdl}: HTTP {r.status_code}: {body_txt}")
        except Exception as e:
            errors.append(f"Groq {g_mdl}: {e}")

    raise RuntimeError(" | ".join(errors) if errors else "No LLM provider configured.")


async def _call_llm_generate(system: str, user: str) -> str:
    """High-quality generation call — uses best available model with generous token budget."""
    return await _call_llm(system, user, max_tokens=4096, temperature=0.35)


def _compact(data: dict, max_chars: int = 4000) -> str:
    """Compact profile dict to a readable text block for the LLM prompt."""
    return json.dumps(data, default=str, ensure_ascii=False)[:max_chars]


# ── Phase 2: Generate one system prompt per section ────────────────────────────
# Each function receives raw_data (the scraped profile/website dict) and returns
# a ready-to-use plain-text system prompt string.

_WRITER_SYSTEM = (
    "You are a senior prompt engineer specialising in B2B sales AI systems. "
    "Your job is to write rich, ready-to-use system prompts for an AI assistant. "
    "The prompts must be written in second person ('You are…'), incorporate the "
    "specific data provided, be concise and actionable. "
    "Output ONLY the system prompt text — no preamble, no explanation, no markdown fences."
)


async def prompt_identity(raw: dict) -> str:
    user = (
        "Write a system prompt for an AI that will present this lead's professional identity. "
        "Include their full name, current title, company, location, and a 2–3 sentence professional summary. "
        "The prompt should instruct the AI to introduce the lead clearly and accurately.\n\n"
        f"Profile data:\n{_compact(raw)}"
    )
    return await _call_llm_generate(_WRITER_SYSTEM, user)


async def prompt_contact(raw: dict) -> str:
    user = (
        "Write a system prompt for an AI that will present this lead's contact information. "
        "Include email, phone, LinkedIn URL, Twitter/X handle, and any other relevant contact details. "
        "The prompt should instruct the AI to present contact data clearly, noting confidence levels.\n\n"
        f"Profile data:\n{_compact(raw)}"
    )
    return await _call_llm_generate(_WRITER_SYSTEM, user)


async def prompt_scores(raw: dict) -> str:
    user = (
        "Write a system prompt for an AI that will score this lead. "
        "The AI should compute: ICP Fit Score (0–40), Intent Score (0–30), Timing Score (0–20), "
        "Engagement Score (0–10), and a Total Score (0–100). It should assign a tier "
        "(Hot/Warm/Cool/Cold) and write a 2-sentence score explanation. "
        "Ground the scoring rubric in the specific facts about this lead.\n\n"
        f"Profile data:\n{_compact(raw)}"
    )
    return await _call_llm_generate(_WRITER_SYSTEM, user)


async def prompt_icp_match(raw: dict) -> str:
    user = (
        "Write a system prompt for an AI that will assess this lead's ICP (Ideal Customer Profile) fit. "
        "The AI should identify fit reasons, gaps, decision-maker likelihood, budget authority, "
        "and assign an ICP match tier (Strong/Moderate/Weak/No Fit). "
        "Use specific facts from this lead's profile to make the analysis concrete.\n\n"
        f"Profile data:\n{_compact(raw)}"
    )
    return await _call_llm_generate(_WRITER_SYSTEM, user)


async def prompt_behavioural_signals(raw: dict) -> str:
    user = (
        "Write a system prompt for an AI that will identify behavioural and intent signals for this lead. "
        "The AI should detect: funding events, hiring surges, job changes, LinkedIn activity, "
        "news mentions, product launches, competitor usage, and warm signals. "
        "Anchor the prompt in real signals visible in this lead's data.\n\n"
        f"Profile data:\n{_compact(raw)}"
    )
    return await _call_llm_generate(_WRITER_SYSTEM, user)


async def prompt_pitch_intelligence(raw: dict) -> str:
    user = (
        "Write a system prompt for an AI sales coach that will generate pitch intelligence for this lead. "
        "The AI should identify the primary pain point, 3 personalised value propositions, "
        "the best pitch angle, likely objections with responses, and 3 conversation starters. "
        "Make it hyper-personalised to this specific person and their company.\n\n"
        f"Profile data:\n{_compact(raw)}"
    )
    return await _call_llm_generate(_WRITER_SYSTEM, user)


async def prompt_activity(raw: dict) -> str:
    user = (
        "Write a system prompt for an AI that will summarise this lead's recent professional activity. "
        "The AI should extract recent posts, interactions, activity themes, posting frequency, "
        "and last active date. Reference the actual activity data available for this lead.\n\n"
        f"Profile data:\n{_compact(raw)}"
    )
    return await _call_llm_generate(_WRITER_SYSTEM, user)


async def prompt_tags(raw: dict) -> str:
    user = (
        "Write a system prompt for an AI that will auto-tag this lead for CRM categorisation. "
        "The AI should generate: auto_tags (5–10 concise tags like 'SaaS', 'Series A', 'VP Engineering'), "
        "company_tags, persona_tags, and intent_tags. "
        "Base the tagging logic on this lead's specific profile and company.\n\n"
        f"Profile data:\n{_compact(raw)}"
    )
    return await _call_llm_generate(_WRITER_SYSTEM, user)


async def prompt_outreach(raw: dict) -> str:
    user = (
        "Write a system prompt for an AI copywriter that will generate personalised outreach for this lead. "
        "The AI should produce: a compelling email subject line, a 3-paragraph cold email (under 150 words), "
        "a LinkedIn connection note (under 300 chars), the best outreach channel, best send time, "
        "and the primary outreach angle. "
        "Make all copy hyper-personalised using this lead's specific signals and background.\n\n"
        f"Profile data:\n{_compact(raw)}"
    )
    return await _call_llm_generate(_WRITER_SYSTEM, user)


async def prompt_person_analysis(raw: dict) -> str:
    user = (
        "Write a system prompt for an AI that will perform a deep person analysis for sales intelligence. "
        "The AI should assess: personality type, communication style "
        "(Direct/Analytical/Expressive/Amiable), motivations, decision-making style, "
        "professional values, career trajectory, influence level, and risk appetite. "
        "Tailor the analysis framework to the actual background of this person.\n\n"
        f"Profile data:\n{_compact(raw)}"
    )
    return await _call_llm_generate(_WRITER_SYSTEM, user)


# ── Section registry ───────────────────────────────────────────────────────────

SECTIONS = [
    ("identity",            "Identity",            prompt_identity),
    ("contact",             "Contact",             prompt_contact),
    ("scores",              "Scores",              prompt_scores),
    ("icp_match",           "ICP Match",           prompt_icp_match),
    ("behavioural_signals", "Behavioural Signal",  prompt_behavioural_signals),
    ("pitch_intelligence",  "Pitch Intelligence",  prompt_pitch_intelligence),
    ("activity",            "Activity",            prompt_activity),
    ("tags",                "Tags",                prompt_tags),
    ("outreach",            "Outreach",            prompt_outreach),
    ("person_analysis",     "Person Analysis",     prompt_person_analysis),
]

SECTION_MAP = {key: (label, fn) for key, label, fn in SECTIONS}

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
def _bd_company_dataset() -> str: return _k("BD_COMPANY_DATASET_ID", "gd_l1vikfnt1wgvvqz95w")
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

_LI_RE         = re.compile(r"linkedin\.com/in/([^/?#\s]+)", re.IGNORECASE)
_LI_COMPANY_RE = re.compile(r"linkedin\.com/company/([^/?#\s]+)", re.IGNORECASE)


def _normalize_linkedin(url: str) -> str:
    m = _LI_RE.search(url)
    if not m:
        return url.strip()
    return f"https://www.linkedin.com/in/{m.group(1).rstrip('/')}/"


def _normalize_linkedin_company(url: str) -> str:
    m = _LI_COMPANY_RE.search(url)
    if not m:
        return url.strip()
    # Always use www — strips country subdomains (in., nl., fr., etc.)
    return f"https://www.linkedin.com/company/{m.group(1).rstrip('/')}/"


def is_linkedin_url(url: str) -> bool:
    return bool(_LI_RE.search(url))


def is_linkedin_company_url(url: str) -> bool:
    return bool(_LI_COMPANY_RE.search(url))


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


async def _poll_bd_snapshot(snapshot_id: str, api_key: str, interval: int = 5, timeout: int = 120) -> list[dict]:
    """Poll /progress/{snapshot_id} until ready, then fetch /snapshot/{snapshot_id}."""
    deadline = time.monotonic() + timeout
    headers  = {"Authorization": f"Bearer {api_key}"}
    client   = _get_bd_client()
    while time.monotonic() < deadline:
        await asyncio.sleep(interval)
        st = await client.get(f"{BD_BASE}/progress/{snapshot_id}", headers=headers)
        st.raise_for_status()
        state = st.json().get("status") or st.json().get("state", "")
        if state == "ready":
            res = await client.get(
                f"{BD_BASE}/snapshot/{snapshot_id}",
                headers=headers,
                params={"format": "json"},
            )
            res.raise_for_status()
            data = res.json()
            return data if isinstance(data, list) else [data]
        if state in ("failed", "error"):
            raise RuntimeError(f"BrightData snapshot {snapshot_id} failed: {st.json()}")
    raise TimeoutError(f"BrightData snapshot {snapshot_id} not ready after {timeout}s")


async def scrape_linkedin_company(linkedin_url: str) -> dict:
    """
    Fetch company profile from BrightData company dataset.

    Attempt 1 — sync /scrape (immediate data)
    Attempt 2 — poll snapshot_id returned by /scrape
    Attempt 3 — /trigger → poll /progress → /snapshot
    """
    api_key    = _bd_api_key()
    dataset_id = _bd_company_dataset()
    if not api_key:
        raise RuntimeError("BRIGHT_DATA_API_KEY not configured")

    url_norm = _normalize_linkedin_company(linkedin_url)
    headers  = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    client   = _get_bd_client()

    logger.info("[SPG Company] Scraping %s (dataset=%s)", url_norm, dataset_id)

    # ── Attempt 1: sync /scrape ────────────────────────────────────────────────
    snapshot_id = None
    try:
        resp = await client.post(
            f"{BD_BASE}/scrape",
            params={"dataset_id": dataset_id, "format": "json"},
            headers=headers,
            json=[{"url": url_norm}],
        )
        if resp.status_code in (200, 202):
            body = resp.json()
            rows = body if isinstance(body, list) else [body]
            companies = [r for r in rows if not r.get("error") and (r.get("name") or r.get("company_name") or r.get("description"))]
            if companies:
                logger.info("[SPG Company] Sync /scrape OK for %s", url_norm)
                return _normalize_company_from_bd(companies[0], url_norm)
            # Async job started — grab snapshot_id
            for row in rows:
                if isinstance(row, dict) and row.get("snapshot_id"):
                    snapshot_id = row["snapshot_id"]
                    break
            if isinstance(body, dict) and body.get("snapshot_id"):
                snapshot_id = body["snapshot_id"]
    except Exception as e:
        logger.warning("[SPG Company] /scrape error: %s", e)

    # ── Attempt 2: poll snapshot from /scrape response ─────────────────────────
    if snapshot_id:
        try:
            rows = await _poll_bd_snapshot(snapshot_id, api_key)
            companies = [r for r in rows if not r.get("error") and (r.get("name") or r.get("company_name"))]
            if companies:
                logger.info("[SPG Company] Async poll OK for %s", url_norm)
                return _normalize_company_from_bd(companies[0], url_norm)
        except Exception as e:
            logger.warning("[SPG Company] Snapshot poll failed: %s", e)

    # ── Attempt 3: /trigger → poll /progress → /snapshot ──────────────────────
    logger.info("[SPG Company] Falling back to /trigger for %s", url_norm)
    tr = await client.post(
        f"{BD_BASE}/trigger",
        params={"dataset_id": dataset_id, "format": "json", "include_errors": "true"},
        headers=headers,
        json=[{"url": url_norm}],
    )
    tr.raise_for_status()
    snap_id = tr.json().get("snapshot_id") or tr.json().get("id")
    if not snap_id:
        raise RuntimeError(f"BrightData /trigger returned no snapshot_id: {tr.text[:300]}")

    rows = await _poll_bd_snapshot(snap_id, api_key)
    companies = [r for r in rows if not r.get("error") and (r.get("name") or r.get("company_name"))]
    if not companies:
        raise RuntimeError(f"BrightData returned only errors for {url_norm}: {rows[0] if rows else 'empty'}")
    return _normalize_company_from_bd(companies[0], url_norm)


async def scrape_website(website_url: str) -> dict:
    """Scrape homepage + /about, return structured text dict with LLM-extracted fields."""
    client = _get_web_client()
    http_headers = {"User-Agent": "Mozilla/5.0 (compatible; WorksBuddy/1.0)"}

    async def _fetch(url: str) -> tuple[str, str]:
        try:
            r = await client.get(url, headers=http_headers)
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
        r = await client.get(website_url, headers=http_headers)
        m = re.search(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']', r.text, re.IGNORECASE)
        desc = m.group(1).strip() if m else ""
    except Exception:
        pass

    # LLM extraction — pull structured fields from the raw page text
    combined_text = f"{homepage}\n\n{about}"[:6000]
    extract_system = (
        "You are a data extraction assistant. Extract structured company information from website text. "
        "Return ONLY a valid JSON object with these exact keys (use null if not found, arrays for lists):\n"
        "company_name, company_description, company_slogan, industry, hq_location, founded_year, "
        "employee_count, organization_type, funding_stage, total_funding, "
        "specialties (array of strings), products (array of strings), services (array of strings), "
        "target_audience, key_features (array of short phrases), "
        "company_email, company_phone, company_twitter, crunchbase_url, "
        "social_links (object with keys: linkedin, twitter, facebook, instagram, youtube)"
    )
    extract_user = (
        f"Extract company information from this website text:\n\n"
        f"URL: {website_url}\nTitle: {title}\nMeta description: {desc}\n\n{combined_text}"
    )
    extracted: dict = {}
    try:
        llm_raw = await _call_llm(extract_system, extract_user, max_tokens=1500)
        llm_clean = re.sub(r"^```(?:json)?\s*|\s*```$", "", llm_raw.strip(), flags=re.MULTILINE)
        parsed_llm = json.loads(llm_clean)
        if isinstance(parsed_llm, dict):
            extracted = {k: v for k, v in parsed_llm.items() if v is not None}
    except Exception as e:
        logger.warning("[SPG] LLM extraction failed for %s: %s", website_url, e)

    return _normalize_company_from_website(website_url, title, desc, extracted)


# ── Unified company profile normalizers ────────────────────────────────────────

_SOCIAL_DOMAINS = {"linkedin.com", "facebook.com", "twitter.com", "x.com", "instagram.com", "youtube.com"}


def _safe_int(v) -> int:
    try:
        return int(v) if v else 0
    except (TypeError, ValueError):
        return 0


def _normalize_company_from_bd(c: dict, linkedin_url: str = "") -> dict:
    """Map raw BrightData company record → unified company profile schema."""
    out: dict = {"source": "linkedin_company"}

    out["company_name"] = c.get("name") or c.get("company_name") or ""
    out["company_linkedin_url"] = linkedin_url or c.get("url") or c.get("linkedin_url") or ""

    # Website
    website = (
        c.get("website") or c.get("website_url") or c.get("company_website")
        or c.get("homepage") or ""
    )
    if website:
        m = re.search(r"https?://(?:www\.)?([^/?\s]+)", website)
        if m and not any(m.group(1).lower() == s or m.group(1).lower().endswith("." + s) for s in _SOCIAL_DOMAINS):
            out["company_website"] = website
    out.setdefault("company_website", "")

    out["company_logo"] = (
        c.get("logo") or c.get("logo_url") or c.get("company_logo")
        or c.get("profile_pic_url") or c.get("image_url") or ""
    )
    out["company_description"] = str(
        c.get("description") or c.get("about") or c.get("company_description")
        or c.get("summary") or c.get("overview") or ""
    )[:800]
    out["company_slogan"] = str(c.get("slogan") or c.get("tagline") or "")[:200]

    # Industry
    ind = c.get("industry") or c.get("industries") or c.get("category") or ""
    out["industry"] = ind[0] if isinstance(ind, list) else str(ind)

    # Location
    hq = c.get("headquarters") or c.get("hq_location") or c.get("location") or ""
    if isinstance(hq, dict):
        parts = [hq.get("city", ""), hq.get("region", ""), hq.get("country", "")]
        out["hq_location"] = ", ".join(p for p in parts if p)
    else:
        out["hq_location"] = str(hq)

    out["founded_year"] = str(c.get("founded") or c.get("founded_year") or c.get("founded_on") or "")

    # Employee count
    emp = 0
    if isinstance(c.get("employees_in_linkedin"), (int, float)) and c["employees_in_linkedin"] > 0:
        emp = int(c["employees_in_linkedin"])
    else:
        for f in ("employee_count", "staff_count"):
            n = _safe_int(c.get(f))
            if n > 0:
                emp = n
                break
        if not emp:
            size_str = str(c.get("company_size") or "")
            nums = re.findall(r"\d+", size_str.replace(",", ""))
            emp = int(nums[-1]) if nums else 0
    out["employee_count"] = emp

    out["linkedin_followers"] = _safe_int(c.get("followers") or c.get("followers_count"))
    out["organization_type"]  = str(c.get("organization_type") or c.get("org_type") or c.get("type") or "")

    # Funding
    funding_obj = c.get("funding") if isinstance(c.get("funding"), dict) else {}
    out["funding_stage"]    = str(funding_obj.get("last_round_type") or c.get("funding_stage") or "")
    out["total_funding"]    = str(funding_obj.get("last_round_raised") or c.get("total_funding") or "")
    last_fd = funding_obj.get("last_round_date") or c.get("last_funding_date") or ""
    out["last_funding_date"] = str(last_fd)[:10]

    # Specialties → same key as website "specialties"
    specs = c.get("specialties") or c.get("specializations") or []
    if isinstance(specs, str):
        out["specialties"] = [s.strip() for s in specs.split(",") if s.strip()][:15]
    else:
        out["specialties"] = list(specs)[:15]

    # Products / services — BD doesn't have these explicitly; derive from specialties
    out["products"]        = []
    out["services"]        = out["specialties"][:]
    out["target_audience"] = ""
    out["key_features"]    = out["specialties"][:5]

    out["company_email"]   = str(c.get("email") or c.get("company_email") or "")
    out["company_phone"]   = str(c.get("phone") or c.get("phone_number") or c.get("company_phone") or "")
    out["company_twitter"] = str(c.get("twitter") or c.get("twitter_url") or "")
    out["crunchbase_url"]  = str(c.get("crunchbase_url") or c.get("crunchbase") or "")

    # Social links
    social: dict = {}
    if out["company_linkedin_url"]:
        social["linkedin"] = out["company_linkedin_url"]
    if out["company_twitter"]:
        social["twitter"] = out["company_twitter"]
    out["social_links"] = social

    # Similar companies
    similar = c.get("similar") or c.get("similar_companies") or []
    out["similar_companies"] = [
        {
            "name":     s.get("title", ""),
            "industry": s.get("subtitle", ""),
            "location": s.get("location", ""),
            "linkedin": s.get("Links", ""),
        }
        for s in similar[:10] if isinstance(s, dict) and s.get("title")
    ]

    # LinkedIn posts
    updates = c.get("updates") or c.get("posts") or []
    out["linkedin_posts"] = [
        {
            "post_id":        u.get("post_id", ""),
            "date":           u.get("date", ""),
            "text":           (u.get("text") or "")[:500],
            "likes_count":    u.get("likes_count", 0),
            "comments_count": u.get("comments_count", 0),
            "post_url":       u.get("post_url", ""),
        }
        for u in updates[:10] if isinstance(u, dict) and (u.get("text") or u.get("post_id"))
    ]

    return out


def _normalize_company_from_website(url: str, title: str, meta_desc: str, llm: dict) -> dict:
    """Map website LLM-extracted data → unified company profile schema."""
    out: dict = {"source": "website"}

    out["company_name"]        = str(llm.get("company_name") or title or "")
    out["company_linkedin_url"] = str((llm.get("social_links") or {}).get("linkedin") or "")
    out["company_website"]     = url
    out["company_logo"]        = ""
    out["company_description"] = str(llm.get("company_description") or meta_desc or "")[:800]
    out["company_slogan"]      = str(llm.get("company_slogan") or llm.get("tagline") or "")[:200]

    out["industry"]            = str(llm.get("industry") or "")
    out["hq_location"]         = str(llm.get("hq_location") or llm.get("location") or "")
    out["founded_year"]        = str(llm.get("founded_year") or "")

    # employee_count — LLM may return int or "51-200" string
    emp_raw = llm.get("employee_count") or llm.get("company_size") or 0
    if isinstance(emp_raw, int):
        out["employee_count"] = emp_raw
    else:
        nums = re.findall(r"\d+", str(emp_raw).replace(",", ""))
        out["employee_count"] = int(nums[-1]) if nums else 0

    out["linkedin_followers"] = 0
    out["organization_type"]  = str(llm.get("organization_type") or "")
    out["funding_stage"]      = str(llm.get("funding_stage") or "")
    out["total_funding"]      = str(llm.get("total_funding") or "")
    out["last_funding_date"]  = ""

    def _to_list(v) -> list:
        if not v:
            return []
        if isinstance(v, list):
            return [str(i) for i in v if i]
        return [str(v)]

    out["specialties"]     = _to_list(llm.get("specialties"))
    out["products"]        = _to_list(llm.get("products"))
    out["services"]        = _to_list(llm.get("services"))
    out["target_audience"] = str(llm.get("target_audience") or "")
    out["key_features"]    = _to_list(llm.get("key_features"))

    out["company_email"]   = str(llm.get("company_email") or "")
    out["company_phone"]   = str(llm.get("company_phone") or "")
    out["company_twitter"] = str(llm.get("company_twitter") or (llm.get("social_links") or {}).get("twitter") or "")
    out["crunchbase_url"]  = str(llm.get("crunchbase_url") or "")

    social = llm.get("social_links") or {}
    if out["company_linkedin_url"]:
        social.setdefault("linkedin", out["company_linkedin_url"])
    out["social_links"] = {k: str(v) for k, v in social.items() if v}

    out["similar_companies"] = []
    out["linkedin_posts"]    = []

    return out


async def scrape_url(url: str) -> tuple[str, dict]:
    """
    Scrape a URL and return (source_type, profile).
    source_type: 'linkedin' | 'linkedin_company' | 'website'
    All company sources (linkedin_company, website) return the same unified schema.
    """
    if is_linkedin_company_url(url):
        return "linkedin_company", await scrape_linkedin_company(url)
    if is_linkedin_url(url):
        return "linkedin", await scrape_linkedin_profile(url)
    return "website", await scrape_website(url)


# ── LLM call ──────────────────────────────────────────────────────────────────

async def _call_llm(
    system: str,
    user: str,
    max_tokens: int = 2000,
    temperature: float = 0.4,
) -> str:
    """
    Try providers in order: WorksBuddy LLM → HuggingFace.
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

async def prompt_identity(raw: dict) -> str:
    system = (
        "You are a professional B2B intelligence writer. "
        "Your job is to write a clear, accurate identity snapshot of a sales prospect. "
        "Cover: full name, current title, company, location, years in role, career background, and a "
        "2–3 sentence professional summary that captures who this person is and why they matter. "
        "Be factual, specific, and avoid generic filler. "
        "Output ONLY the system prompt text — no preamble, no explanation, no markdown fences."
    )
    user = (
        "Using the profile data below, write a system prompt that instructs an AI to introduce this lead "
        "with a precise professional identity snapshot. Embed the actual name, title, company, and key "
        "career facts so the output is specific to this person — not a template.\n\n"
        f"Profile data:\n{_compact(raw)}"
    )
    return await _call_llm_generate(system, user)


async def prompt_contact(raw: dict) -> str:
    system = (
        "You are a B2B data quality specialist. "
        "Your job is to present a lead's contact information clearly and with confidence scoring. "
        "Cover: work email (with verified/guessed/not-found status), personal email, direct phone, "
        "LinkedIn URL, Twitter/X handle, and any other reachable channels. "
        "Flag low-confidence data explicitly. Never invent contact details. "
        "Output ONLY the system prompt text — no preamble, no explanation, no markdown fences."
    )
    user = (
        "Using the profile data below, write a system prompt that instructs an AI to present this lead's "
        "contact information. Mention which specific channels are available for this person and how "
        "confident we are in each. Make the prompt actionable for a sales rep who needs to reach out.\n\n"
        f"Profile data:\n{_compact(raw)}"
    )
    return await _call_llm_generate(system, user)


async def prompt_scores(raw: dict) -> str:
    tenant_name     = raw.get("company_name") or ""
    tenant_desc     = raw.get("company_description") or ""
    tenant_industry = raw.get("industry") or ""
    tenant_audience = raw.get("target_audience") or ""
    tenant_services = ", ".join(raw.get("services") or raw.get("specialties") or [])

    tenant_context = (
        f"TENANT (the company doing the scoring):\n"
        f"- Company: {tenant_name}\n"
        f"- What they sell: {tenant_desc}\n"
        f"- Industry: {tenant_industry}\n"
        f"- Services: {tenant_services}\n"
        f"- Their target audience: {tenant_audience}\n"
    )

    system = (
        "You are a B2B lead scoring expert. "
        "Your job is to score a prospect across four dimensions specifically for the tenant company provided. "
        "Scoring breakdown:\n"
        "  ICP Fit (0–40) — how well the lead's role, seniority, company size, and industry matches "
        "the TENANT'S target audience and services — not a generic ICP.\n"
        "  Intent Score (0–30) — hiring activity, funding, job change, tech adoption signals relevant "
        "to what the TENANT sells.\n"
        "  Timing Score (0–20) — recency of signals that indicate NOW is a good time for the tenant to reach out.\n"
        "  Engagement Score (0–10) — LinkedIn activity and responsiveness signals.\n"
        "Assign a tier: Hot (80–100) / Warm (55–79) / Cool (30–54) / Cold (0–29). "
        "Every score must be grounded in the tenant's specific business context — not a universal rubric. "
        "Output ONLY the system prompt text — no preamble, no explanation, no markdown fences."
    )
    user = (
        f"{tenant_context}\n"
        "The data above describes the TENANT — the company whose sales team will use this system prompt. "
        "Write a system prompt that instructs an AI to score a B2B LEAD (a different person/company, "
        "whose profile will be injected at runtime) specifically for this tenant's product and ICP.\n\n"
        "The system prompt must:\n"
        "- Define ICP Fit criteria based on the tenant's actual target audience and services above\n"
        "- Define which intent signals matter for this tenant (relevant to what they sell)\n"
        "- Instruct the AI to ground every score in the lead's observable data — not assumptions\n"
        "- Output instructions only — NOT pre-filled scores\n\n"
        "Do NOT score the tenant's own data. Write a reusable system prompt template."
    )
    return await _call_llm_generate(system, user)


async def prompt_icp_match(raw: dict) -> str:
    tenant_name     = raw.get("company_name") or ""
    tenant_desc     = raw.get("company_description") or ""
    tenant_industry = raw.get("industry") or ""
    tenant_audience = raw.get("target_audience") or ""
    tenant_services = ", ".join(raw.get("services") or raw.get("specialties") or [])
    tenant_location = raw.get("hq_location") or ""

    tenant_context = (
        f"TENANT (the company whose ICP we are matching against):\n"
        f"- Company: {tenant_name}\n"
        f"- What they sell: {tenant_desc}\n"
        f"- Industry: {tenant_industry}\n"
        f"- Services: {tenant_services}\n"
        f"- Their target audience: {tenant_audience}\n"
        f"- HQ / primary market: {tenant_location}\n"
    )

    system = (
        "You are an ICP (Ideal Customer Profile) analyst. "
        "Your job is to assess how well a prospect matches the TENANT'S ideal customer — not a generic B2B ICP. "
        "The tenant's ICP is defined by who they sell to, what they sell, and where they operate. "
        "Evaluate the lead against the tenant's specific criteria:\n"
        "  - Seniority and decision-making authority relative to what the tenant sells\n"
        "  - Company size and growth stage that the tenant typically serves\n"
        "  - Industry and vertical fit with the tenant's services\n"
        "  - Geographic fit with the tenant's primary market\n"
        "  - Budget signals relevant to the tenant's price point\n"
        "  - Tech stack or operational signals that indicate need for the tenant's solution\n"
        "Assign a match tier: Strong Fit / Moderate Fit / Weak Fit / No Fit. "
        "List the top 3 fit reasons and top 2 gaps specific to this tenant. "
        "Be honest — a weak fit for this tenant is not a weak lead universally. "
        "Output ONLY the system prompt text — no preamble, no explanation, no markdown fences."
    )
    user = (
        f"{tenant_context}\n"
        "The data above describes the TENANT — the company whose sales team will use this system prompt. "
        "Write a system prompt that instructs an AI to assess whether a B2B LEAD "
        "(a different person/company whose profile will be injected at runtime) is a good fit "
        "for THIS specific tenant.\n\n"
        "The system prompt must:\n"
        "- Embed the tenant's ICP criteria from above so the AI knows exactly what a good customer looks like for this tenant\n"
        "- Instruct the AI to evaluate the LEAD's seniority, company size, industry, geography, and budget signals "
        "against these criteria — not generic B2B standards\n"
        "- Instruct the AI to assign: Strong Fit / Moderate Fit / Weak Fit / No Fit\n"
        "- Instruct the AI to list top 3 fit reasons and top 2 gaps anchored in the tenant's services\n"
        "- Output instructions only — NOT a pre-filled ICP verdict for any specific lead\n\n"
        "Do NOT evaluate the tenant's own profile. Write a reusable system prompt template."
    )
    return await _call_llm_generate(system, user)


async def prompt_behavioural_signals(raw: dict) -> str:
    tenant_name     = raw.get("company_name") or ""
    tenant_desc     = raw.get("company_description") or ""
    tenant_services = ", ".join(raw.get("services") or raw.get("specialties") or [])
    tenant_audience = raw.get("target_audience") or ""

    tenant_context = (
        f"TENANT (the company evaluating these signals):\n"
        f"- Company: {tenant_name}\n"
        f"- What they sell: {tenant_desc}\n"
        f"- Services: {tenant_services}\n"
        f"- Their target audience: {tenant_audience}\n"
    )

    system = (
        "You are a B2B intent data analyst. "
        "Your job is to surface buying signals and behavioural triggers that are relevant to the TENANT'S product — "
        "not every possible signal that exists. A signal only matters if it indicates the lead may need "
        "what the tenant sells.\n\n"
        "Detect and categorise signals in order of relevance to the tenant:\n"
        "  - Hiring signals (are they hiring roles that the tenant's product supports or replaces?)\n"
        "  - Funding events (new budget = new vendor decisions)\n"
        "  - Leadership changes (new decision-maker = new buying window)\n"
        "  - Product launches or rebrands (growth signals that create new needs)\n"
        "  - LinkedIn post topics that overlap with the tenant's solution area\n"
        "  - Technology adoption or competitor mentions relevant to the tenant's space\n"
        "  - News coverage indicating scale, expansion, or pain\n\n"
        "Rate overall signal strength for THIS tenant: Strong / Moderate / Weak. "
        "Flag the single most actionable signal for the tenant's SDR at the top. "
        "Ignore signals that have no relevance to the tenant's business. "
        "Output ONLY the system prompt text — no preamble, no explanation, no markdown fences."
    )
    user = (
        f"{tenant_context}\n"
        "The data above describes the TENANT — the company whose sales team will use this system prompt. "
        "Write a system prompt that instructs an AI to analyse a B2B LEAD's behavioural signals "
        "(a different person/company whose profile will be injected at runtime) and surface only the signals "
        "relevant to this tenant's product and solution area.\n\n"
        "The system prompt must:\n"
        "- Define which signal categories matter for this tenant (based on what they sell above)\n"
        "- Instruct the AI to filter out irrelevant signals\n"
        "- Instruct the AI to flag the single most actionable signal for this tenant's SDR\n"
        "- Instruct the AI to rate signal strength (Strong/Moderate/Weak) for THIS tenant specifically\n"
        "- Output instructions only — NOT pre-filled signal analysis\n\n"
        "Do NOT analyse the tenant's own data. Write a reusable system prompt template."
    )
    return await _call_llm_generate(system, user)


async def prompt_pitch_intelligence(raw: dict) -> str:
    tenant_name     = raw.get("company_name") or ""
    tenant_desc     = raw.get("company_description") or ""
    tenant_services = ", ".join(raw.get("services") or raw.get("specialties") or [])
    tenant_audience = raw.get("target_audience") or ""
    tenant_features = ", ".join(raw.get("key_features") or [])

    tenant_context = (
        f"TENANT (the company doing the pitching):\n"
        f"- Company: {tenant_name}\n"
        f"- What they sell: {tenant_desc}\n"
        f"- Services: {tenant_services}\n"
        f"- Key differentiators: {tenant_features}\n"
        f"- Their target audience: {tenant_audience}\n"
    )

    system = (
        "You are a B2B sales strategist. "
        "Your job is to build tactical pitch intelligence for a sales rep at the TENANT company "
        "who is about to contact this specific prospect.\n\n"
        "All pitch intelligence must be anchored in what the TENANT sells — not generic sales advice:\n"
        "1. Primary pain point — the #1 problem this lead likely faces that the TENANT's product/service solves. "
        "   Do not name pains the tenant cannot address.\n"
        "2. 3 value propositions — each directly maps a lead pain to a specific TENANT service or capability, "
        "   with a one-line concrete outcome (not 'save time' — a real measurable result).\n"
        "3. Best pitch angle for this tenant+lead combination — ROI / Speed to value / Risk reduction / Competitive advantage.\n"
        "4. Top 2 likely objections this lead would raise, with rebuttals specific to the tenant's offering.\n"
        "5. 3 conversation starters the tenant's SDR can use — questions that feel natural, not scripted, "
        "   and open discovery about the lead's need for what the tenant sells.\n\n"
        "Be concise, tactical, and specific to this tenant's business. No generic sales fluff. "
        "Output ONLY the system prompt text — no preamble, no explanation, no markdown fences."
    )
    user = (
        f"{tenant_context}\n"
        "The data above describes the TENANT — the company whose SDR will use this system prompt. "
        "Write a system prompt that instructs an AI to generate pitch intelligence for approaching "
        "a B2B LEAD (a different person/company whose profile will be injected at runtime).\n\n"
        "The system prompt must:\n"
        "- Open by identifying the tenant's name and what they sell (embed from above)\n"
        "- Instruct the AI to identify the lead's #1 pain point that the TENANT's services can address\n"
        "- Instruct the AI to generate 3 value props mapped to tenant services (from the list above) — "
        "  not generic props, only services this tenant actually offers\n"
        "- Instruct the AI to pick the best pitch angle for this tenant+lead combination\n"
        "- Instruct the AI to anticipate objections specific to the tenant's offering and price point\n"
        "- Instruct the AI to write conversation starters that feel natural for a rep from this tenant\n"
        "- Output instructions only — NOT pre-filled pitch intelligence\n\n"
        "Do NOT generate pitch intelligence for the tenant's own data. Write a reusable system prompt template."
    )
    return await _call_llm_generate(system, user)


async def prompt_activity(raw: dict) -> str:
    system = (
        "You are a social intelligence analyst for B2B sales. "
        "Your job is to interpret a prospect's recent professional activity and extract sales-relevant insights. "
        "Analyse: topics they post or comment about, content they engage with (likes/shares), "
        "posting frequency and consistency, tone (thought leader / lurker / promoter), "
        "recent interactions that reveal priorities or pain points, and last active date. "
        "Conclude with: what this activity tells us about their current focus and the best hook to use. "
        "Output ONLY the system prompt text — no preamble, no explanation, no markdown fences."
    )
    user = (
        "Using the profile data below, write a system prompt that instructs an AI to analyse this lead's "
        "actual activity patterns. Reference their real posts, likes, and interaction themes so the AI "
        "produces insight relevant to this specific person — not a generic activity template.\n\n"
        f"Profile data:\n{_compact(raw)}"
    )
    return await _call_llm_generate(system, user)


async def prompt_tags(raw: dict) -> str:
    system = (
        "You are a CRM data architect specialising in B2B lead classification. "
        "Your job is to generate precise, useful tags that help sales teams filter and segment leads. "
        "Produce four tag sets: "
        "auto_tags (5–10 tags: role type, seniority, industry, company stage — e.g. 'VP Engineering', 'Series B', 'SaaS'), "
        "company_tags (3–5 tags describing the company — e.g. 'Scale-up', 'Remote-first', 'AI-native'), "
        "persona_tags (3–5 tags describing the person's buyer persona — e.g. 'Technical Buyer', 'Budget Holder'), "
        "intent_tags (2–4 tags for current intent signals — e.g. 'Hiring Now', 'Recently Funded', 'Switching Stack'). "
        "Tags must be concise (1–3 words), consistent, and CRM-friendly. "
        "Output ONLY the system prompt text — no preamble, no explanation, no markdown fences."
    )
    user = (
        "Using the profile data below, write a system prompt that instructs an AI to generate CRM tags "
        "for this lead. Ground the tagging logic in their actual role, company size, industry, and "
        "visible signals — so the tags are accurate and immediately useful for segmentation.\n\n"
        f"Profile data:\n{_compact(raw)}"
    )
    return await _call_llm_generate(system, user)


async def prompt_outreach(raw: dict) -> str:
    # Extract tenant company context from scraped data
    tenant_name     = raw.get("company_name") or ""
    tenant_desc     = raw.get("company_description") or ""
    tenant_industry = raw.get("industry") or ""
    tenant_services = raw.get("services") or raw.get("specialties") or []
    tenant_audience = raw.get("target_audience") or ""

    tenant_context = (
        f"SENDER COMPANY (the tenant whose SDR will send this outreach):\n"
        f"- Company name: {tenant_name}\n"
        f"- What they do: {tenant_desc}\n"
        f"- Industry: {tenant_industry}\n"
        f"- All services they offer: {', '.join(tenant_services)}\n"
        f"- Target audience: {tenant_audience}\n"
    )

    system = (
        "You are a senior B2B sales strategist and cold outreach copywriter. "
        "Your job is to write a SYSTEM PROMPT that will be used at runtime to generate personalised cold outreach "
        "FROM the sender company TO a specific B2B lead whose profile will be provided at runtime.\n\n"
        "RULES for the system prompt you write:\n"
        "1. Open with: 'You are an SDR at [Sender Company Name]. We help [one-line: who they serve + what outcome they deliver].'\n"
        "2. From the sender's service list, instruct the AI to pick ONLY the ONE service most relevant to the lead's "
        "role, industry, and pain — mention ONLY that service in the email body. Never list all services.\n"
        "3. Instruct the AI to always mention [Sender Company Name] by name in Para 2 — never use 'we' before introducing who 'we' is.\n"
        "4. Instruct the AI to write a cold email — 3 tight paragraphs, under 150 words total:\n"
        "   Para 1 — SPECIFIC HOOK (not generic): Reference what the lead's company actually DOES or a real signal "
        "from their career/role (e.g. 'You're building X at Y' or 'After your move from Z to Y'). "
        "Name the real challenge that comes with their stage — not 'managing growth is hard'. "
        "This paragraph must NOT be something that could apply to any CEO. Make it specific to THIS lead.\n"
        "   Para 2 — SENDER BRIDGE: Mention [Sender Company Name] by name. State the ONE relevant service. "
        "Connect it directly to the lead's specific stage or pain. State ONE concrete outcome — "
        "not 'save time' or 'boost efficiency', but a real business result (e.g. 'handle 10x clients without adding headcount').\n"
        "   Para 3 — DISCOVERY CTA: Ask a single smart question that reveals the lead's current approach or situation. "
        "It must be tied to their specific context — not 'what's your biggest challenge?'. "
        "The question should feel like it came from someone who already understands their world.\n"
        "5. Instruct the AI to write a LinkedIn note — under 300 chars, references something specific about the lead, "
        "conversational tone, zero pitch, sounds human.\n"
        "6. Instruct the AI to determine: best channel (Email/LinkedIn/Phone), best send time in lead's timezone, "
        "and primary outreach angle (Pain-based/Trigger/Insight).\n"
        "7. Instruct the AI to NEVER use: 'I noticed', 'I hope this finds you well', 'reaching out because', "
        "'managing growth can be challenging', 'automate manual work', 'one outcome is', "
        "'synergy', 'game-changer', 'innovative solution', or any phrase that sounds AI-generated.\n"
        "8. Instruct the AI to return ONLY a valid JSON object — no markdown, no explanation — with this schema:\n"
        "{\"cold_email\": {\"subject\": \"...\", \"body\": \"...\"}, "
        "\"linkedin_note\": \"...\", \"best_channel\": \"...\", \"best_time\": \"...\", \"outreach_angle\": \"...\"}\n\n"
        "Output ONLY the system prompt text — no preamble, no explanation, no markdown fences."
    )
    user = (
        f"{tenant_context}\n"
        "Write the system prompt for this sender company. It will be injected at runtime alongside the lead's full profile.\n"
        "The system prompt must be so specific and precise that it is IMPOSSIBLE for the AI to write a generic cold email. "
        "Every instruction must force the AI to anchor copy in the lead's real data and the sender's actual capability — "
        "not describe what good outreach looks like in theory.\n\n"
        f"Full scraped company profile for reference:\n{_compact(raw)}"
    )
    return await _call_llm_generate(system, user)


async def prompt_person_analysis(raw: dict) -> str:
    system = (
        "You are a sales psychologist and B2B buyer profiler. "
        "Your job is to build a deep person analysis that helps sales reps communicate more effectively. "
        "Assess: "
        "1. Communication style — Direct / Analytical / Expressive / Amiable (with evidence from their profile). "
        "2. Decision-making pattern — Data-driven / Relationship-driven / Speed-driven / Consensus-driven. "
        "3. Primary motivations — career growth / team impact / cost saving / innovation / risk avoidance. "
        "4. Likely objections rooted in personality (not just price/timing). "
        "5. How to build rapport quickly — what topics resonate, what to avoid. "
        "6. Influence level — final decision-maker / strong influencer / end user / gatekeeper. "
        "7. Risk appetite — conservative / moderate / aggressive. "
        "Be specific. Use their actual career history, posts, and company context as evidence. "
        "Output ONLY the system prompt text — no preamble, no explanation, no markdown fences."
    )
    user = (
        "Using the profile data below, write a system prompt that instructs an AI to perform a deep person "
        "analysis for sales intelligence on this specific individual. Anchor every dimension — communication "
        "style, motivations, decision pattern — in real evidence from their profile and career history.\n\n"
        f"Profile data:\n{_compact(raw)}"
    )
    return await _call_llm_generate(system, user)


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

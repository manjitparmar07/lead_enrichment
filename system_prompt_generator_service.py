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
        llm_raw = await _call_llm(extract_system, extract_user, groq_model="llama-3.1-8b-instant", max_tokens=1500)
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

    # 3. Groq — try primary model then fallbacks on 429 rate-limit
    g_key = _groq_key()
    if g_key:
        primary = groq_model or _groq_gen_model()
        # Fallback chain (only live models — verified 2026-03-30):
        # llama-3.3-70b-versatile → llama-4-scout → llama-3.1-8b-instant → compound-beta → allam-2-7b
        _GROQ_LIVE = [
            "llama-3.3-70b-versatile",
            "meta-llama/llama-4-scout-17b-16e-instruct",
            "llama-3.1-8b-instant",
            "compound-beta",
            "allam-2-7b",
        ]
        groq_models = [primary] + [m for m in _GROQ_LIVE if m != primary]
        for g_mdl in groq_models:
            try:
                async with httpx.AsyncClient(timeout=90) as c:
                    r = await c.post(
                        "https://api.groq.com/openai/v1/chat/completions",
                        headers={"Content-Type": "application/json", "Authorization": f"Bearer {g_key}"},
                        json={"model": g_mdl, "messages": messages,
                              "temperature": temperature, "max_tokens": max_tokens},
                    )
                    if r.is_success:
                        logger.info("[LLM] Groq success with model: %s", g_mdl)
                        return r.json()["choices"][0]["message"]["content"].strip()
                    try:
                        body_txt = r.json().get("error", {}).get("message") or r.text
                    except Exception:
                        body_txt = r.text
                    if r.status_code in (429, 400):
                        logger.warning("[LLM] Groq %s skipped (%s), trying next model", g_mdl, r.status_code)
                        errors.append(f"Groq {g_mdl}: HTTP {r.status_code}")
                        continue
                    errors.append(f"Groq {g_mdl}: HTTP {r.status_code}: {body_txt}")
                    break  # other errors — stop trying
            except Exception as e:
                errors.append(f"Groq {g_mdl}: {e}")
                break

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

"""
company_service.py
------------------
WorksBuddy Lead Enrichment — Company Intelligence Layer.

Implements all 6 Company Enrichment Flow priorities:

  P1  company_enrichments table + company_id FK on enriched_leads
      7-day cache with idempotent upsert; shared across all leads from same company

  P2  LinkedIn Company Posts (Phase 1C)
      10–15 posts from Bright Data company dataset; stored as JSON array

  P3  Company AI Analysis
      Tags (industry focus, tech maturity, culture signals, growth stage)
      Account-level pitch intelligence (pain, value prop, objections)
      Uses WB_LLM_HOST → Groq fallback → rule-based fallback

  P4  Free Signal Layer
      Crunchbase  — funding round, stage, investors (public HTML scrape)
      Google News — recent mentions (last 30 days)
      Wappalyzer  — tech stack fingerprinting via HTML meta-tags

  P5  Combined person + company score
      company_boost (0–20 extra points added to lead's total_score)
      Stored in company_enrichments.company_score and enriched_leads.combined_score

  P6  Cache + refresh logic
      7-day TTL; signal-triggered auto-refresh on funding/news change
      check_and_refresh() is idempotent — safe to call every enrich_single()
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import httpx
from db import get_pool, named_args

logger = logging.getLogger(__name__)

# ── Cache TTL — default; per-org value read from workspace_configs via get_company_cache_ttl() ──
CACHE_TTL_DAYS = 7


async def get_company_cache_ttl(org_id: str) -> int:
    """Return company cache TTL in days for the org (falls back to 7)."""
    try:
        from enrichment_config_service import get_scoring_config
        sc = await get_scoring_config(org_id)
        return int(sc.get("company_cache_ttl_days", CACHE_TTL_DAYS))
    except Exception:
        return CACHE_TTL_DAYS


# ─────────────────────────────────────────────────────────────────────────────
# P1  —  Database
# ─────────────────────────────────────────────────────────────────────────────

async def init_company_db() -> None:
    """
    Create company_enrichments table and add company_id + combined_score
    columns to enriched_leads.  Fully idempotent — safe to call on every startup.
    """
    async with get_pool().acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS company_enrichments (
                id                  TEXT PRIMARY KEY,
                company_linkedin_url TEXT NOT NULL,
                org_id              TEXT NOT NULL DEFAULT 'default',
                name                TEXT,
                domain              TEXT,
                website             TEXT,
                logo                TEXT,
                description         TEXT,
                industry            TEXT,
                employee_count      INTEGER DEFAULT 0,
                hq_location         TEXT,
                founded_year        TEXT,
                funding_stage       TEXT,
                total_funding       TEXT,
                last_funding_date   TEXT,
                lead_investor       TEXT,
                company_twitter     TEXT,
                company_email       TEXT,
                company_phone       TEXT,
                linkedin_posts      TEXT DEFAULT '[]',
                post_themes         TEXT,
                crunchbase_data     TEXT DEFAULT '{}',
                news_mentions       TEXT DEFAULT '[]',
                wappalyzer_tech     TEXT DEFAULT '[]',
                company_tags        TEXT DEFAULT '[]',
                culture_signals     TEXT DEFAULT '{}',
                account_pitch       TEXT DEFAULT '{}',
                company_score       INTEGER DEFAULT 0,
                company_score_tier  TEXT DEFAULT 'C',
                last_enriched_at    TEXT,
                cache_expires_at    TEXT,
                refresh_triggered_by TEXT,
                raw_data            TEXT DEFAULT '{}',
                created_at          TEXT DEFAULT '',
                updated_at          TEXT DEFAULT ''
            )
        """)
        for idx in [
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_co_linkedin ON company_enrichments(company_linkedin_url, org_id)",
            "CREATE INDEX IF NOT EXISTS idx_co_org ON company_enrichments(org_id, company_score DESC)",
            "CREATE INDEX IF NOT EXISTS idx_co_expires ON company_enrichments(cache_expires_at)",
        ]:
            await conn.execute(idx)

        for col, col_type in [
            ("company_id",          "TEXT"),
            ("combined_score",      "INTEGER DEFAULT 0"),
            ("company_score",       "INTEGER DEFAULT 0"),
        ]:
            await conn.execute(f"ALTER TABLE enriched_leads ADD COLUMN IF NOT EXISTS {col} {col_type}")

        # New company_enrichments columns from richer BD company dataset
        for col, col_type in [
            ("linkedin_followers",  "INTEGER DEFAULT 0"),
            ("company_slogan",      "TEXT"),
            ("similar_companies",   "TEXT DEFAULT '[]'"),
            ("organization_type",   "TEXT"),
            ("crm_brief",           "TEXT"),   # Qwen2.5-72B full CRM profile (JSON)
            ("apollo_raw",          "TEXT"),   # Raw Apollo org dict saved from email enrichment
        ]:
            await conn.execute(f"ALTER TABLE company_enrichments ADD COLUMN IF NOT EXISTS {col} {col_type}")
        # Mirror linkedin_followers onto enriched_leads too
        await conn.execute(
            "ALTER TABLE enriched_leads ADD COLUMN IF NOT EXISTS linkedin_followers INTEGER DEFAULT 0"
        )

        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_leads_company ON enriched_leads(company_id)"
        )

    logger.info("[CompanyService] DB ready (PostgreSQL)")


def _company_id(linkedin_url: str) -> str:
    """Stable 16-char ID from company LinkedIn URL (domain-normalised)."""
    norm = linkedin_url.lower().rstrip("/").split("?")[0]
    return hashlib.sha256(norm.encode()).hexdigest()[:16]


async def _get_company(linkedin_url: str, org_id: str = "default") -> Optional[dict]:
    """Fetch cached company enrichment. Returns None if not found or stale."""
    cid = _company_id(linkedin_url)
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM company_enrichments WHERE id=$1 AND org_id=$2",
            cid, org_id,
        )
    return dict(row) if row else None


async def _upsert_company(record: dict) -> None:
    cols = [k for k in record if k != "id"]
    all_keys = ["id"] + cols
    placeholders = ", ".join(f"${i + 1}" for i in range(len(all_keys)))
    updates = ", ".join(f"{k}=EXCLUDED.{k}" for k in cols)
    sql = f"""
        INSERT INTO company_enrichments ({', '.join(all_keys)})
        VALUES ({placeholders})
        ON CONFLICT(id) DO UPDATE SET {updates}
    """
    args = [record[k] for k in all_keys]
    async with get_pool().acquire() as conn:
        await conn.execute(sql, *args)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _safe_json(v: Any) -> str:
    if isinstance(v, str):
        return v
    try:
        return json.dumps(v, default=str)
    except Exception:
        return "[]"


def _k(name: str, default: str = "") -> str:
    try:
        import keys_service as _ks
        return _ks.get(name) or os.getenv(name, default)
    except Exception:
        return os.getenv(name, default)


def _bd_api_key()         -> str: return _k("BRIGHT_DATA_API_KEY")
def _bd_company_dataset() -> str: return _k("BD_COMPANY_DATASET_ID", "gd_l1vikfnt1wgvvqz95w")
def _wb_llm_host()        -> str: return _k("WB_LLM_HOST", "http://ai-llm.worksbuddy.ai")
def _wb_llm_key()         -> str: return _k("WB_LLM_API_KEY")
def _wb_llm_model()       -> str: return _k("WB_LLM_DEFAULT_MODEL", "wb-pro")


BD_BASE = "https://api.brightdata.com/datasets/v3"


def _bd_headers() -> dict:
    return {
        "Authorization": f"Bearer {_bd_api_key()}",
        "Content-Type": "application/json",
    }


# ─────────────────────────────────────────────────────────────────────────────
# P2  —  LinkedIn Company Posts (Phase 1C)
# ─────────────────────────────────────────────────────────────────────────────

async def _fetch_bd_company_profile(company_linkedin_url: str) -> dict:
    """
    Fetch full company profile from Bright Data company dataset.

    Returns the raw BD company dict with all fields, or {} on failure.
    Fields include: name, followers, employees_in_linkedin, about, company_size,
    organization_type, industries, website, founded, headquarters, logo, image,
    similar[], updates[], slogan, affiliated[], additional_information, employees[]
    """
    if not _bd_api_key() or not company_linkedin_url:
        return {}

    # Normalize URL: strip tracking params + canonicalize to www.linkedin.com
    clean_url = re.sub(r"https?://[a-z]{2}\.linkedin\.com", "https://www.linkedin.com", company_linkedin_url)
    clean_url = clean_url.split("?")[0].rstrip("/")

    dataset_id = _bd_company_dataset()
    logger.info("[CompanyBD] Fetching company profile for: %s", clean_url)

    try:
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(
                f"{BD_BASE}/scrape",
                params={
                    "dataset_id":     dataset_id,
                    "format":         "json",
                    "include_errors": "true",
                    "notify":         "false",
                },
                headers=_bd_headers(),
                json={"input": [{"url": clean_url}]},
            )
            logger.info("[CompanyBD] BD response status=%s body=%.300s", resp.status_code, resp.text)
            if resp.status_code not in (200, 202):
                logger.warning("[CompanyBD] BD error %s: %s", resp.status_code, resp.text[:300])
                return {}

            body = resp.json()
            data_list = body if isinstance(body, list) else body.get("data") or []
            if data_list and isinstance(data_list[0], dict):
                company_data = data_list[0]
            elif isinstance(body, dict) and body.get("name"):
                company_data = body
            else:
                company_data = body if isinstance(body, dict) else {}

            # Poll if async snapshot
            snap_id = (
                company_data.get("snapshot_id")
                if isinstance(company_data, dict)
                else None
            )
            if snap_id:
                from lead_enrichment_brightdata_service import poll_snapshot
                polled = await poll_snapshot(snap_id, interval=5, timeout=120)
                if polled:
                    company_data = polled[0] if isinstance(polled, list) else polled

            logger.info("[CompanyBD] Profile fetched for: %s", company_linkedin_url)
            return company_data if isinstance(company_data, dict) else {}

    except Exception as e:
        logger.warning("[CompanyBD] Failed: %s", e)
        return {}


def _map_bd_company_profile(bd: dict, record: dict) -> None:
    """
    Map Bright Data company profile fields into the internal record dict in-place.

    BD field          → internal field
    name              → name
    about             → description
    website           → website
    website_simplified→ domain (fallback)
    logo              → logo
    followers         → linkedin_followers
    employees_in_linkedin → employee_count (if not already set)
    company_size      → (used to parse employee_count range)
    industries        → industry
    founded           → founded_year
    headquarters      → hq_location
    organization_type → organization_type
    slogan            → company_slogan
    similar[]         → similar_companies
    updates[]         → posts (via _normalize_posts)
    """
    if not bd or not isinstance(bd, dict):
        return

    if bd.get("name"):
        record["name"] = bd["name"]

    if bd.get("about"):
        record["description"] = bd["about"]

    if bd.get("website"):
        record["website"] = bd["website"]

    # domain from website_simplified or parsed from website
    if bd.get("website_simplified") and not record.get("domain"):
        record["domain"] = bd["website_simplified"]
    elif bd.get("website") and not record.get("domain"):
        m = re.search(r"https?://(?:www\.)?([^/?\s]+)", bd["website"])
        if m:
            record["domain"] = m.group(1)

    if bd.get("logo"):
        record["logo"] = bd["logo"]

    if bd.get("followers"):
        record["linkedin_followers"] = int(bd["followers"] or 0)

    # employee_count — prefer followers-based number, parse company_size range
    if bd.get("employees_in_linkedin") and not record.get("employee_count"):
        record["employee_count"] = int(bd["employees_in_linkedin"] or 0)
    if bd.get("company_size") and not record.get("employee_count"):
        # "51-200 employees" → take upper bound
        m = re.search(r"(\d[\d,]*)", str(bd["company_size"]))
        if m:
            record["employee_count"] = int(m.group(1).replace(",", ""))

    if bd.get("industries"):
        record["industry"] = bd["industries"]

    if bd.get("founded"):
        record["founded_year"] = str(bd["founded"])

    if bd.get("headquarters"):
        record["hq_location"] = bd["headquarters"]

    if bd.get("organization_type"):
        record["organization_type"] = bd["organization_type"]

    if bd.get("slogan"):
        record["company_slogan"] = bd["slogan"]

    # similar companies
    similar_raw = bd.get("similar") or []
    if similar_raw and isinstance(similar_raw, list):
        similar_list = [
            {
                "title":    s.get("title", ""),
                "subtitle": s.get("subtitle", ""),
                "url":      s.get("Links", ""),
            }
            for s in similar_raw if isinstance(s, dict)
        ]
        record["similar_companies"] = json.dumps(similar_list, default=str)

    logger.info(
        "[CompanyBD] Mapped fields: name=%s industry=%s employees=%s followers=%s",
        record.get("name"), record.get("industry"),
        record.get("employee_count"), record.get("linkedin_followers"),
    )


async def _fetch_company_posts(company_linkedin_url: str) -> list[dict]:
    """Fetch posts only — thin wrapper over _fetch_bd_company_profile."""
    bd = await _fetch_bd_company_profile(company_linkedin_url)
    return _normalize_posts(bd)


def _normalize_posts(company_data: dict) -> list[dict]:
    """Extract and normalize posts from a company profile dict."""
    if not isinstance(company_data, dict):
        return []

    raw_posts = (
        company_data.get("posts")
        or company_data.get("company_posts")
        or company_data.get("updates")
        or []
    )

    posts = []
    for p in raw_posts[:15]:
        if not isinstance(p, dict):
            continue
        text = (
            p.get("text")
            or p.get("content")
            or p.get("body")
            or p.get("article_title")
            or ""
        )
        if not text:
            continue
        posts.append({
            "text":     str(text)[:500],
            "date":     p.get("date") or p.get("published_at") or p.get("created_at") or "",
            "likes":    int(p.get("likes") or p.get("num_likes") or 0),
            "comments": int(p.get("comments") or p.get("num_comments") or 0),
            "url":      p.get("post_url") or p.get("url") or "",
            "media_type": p.get("media_type") or p.get("type") or "text",
        })

    logger.info("[CompanyPosts] Extracted %d posts", len(posts))
    return posts


def _summarize_post_themes(posts: list[dict]) -> str:
    """Build a 2-sentence summary of post themes for AI analysis."""
    if not posts:
        return ""
    texts = " ".join(p.get("text", "") for p in posts[:10])[:1500]
    # Simple keyword extraction — AI will do deeper analysis
    themes = set()
    kw_map = [
        (["AI", "machine learning", "LLM", "GPT", "artificial intelligence"], "AI & ML"),
        (["hiring", "job", "careers", "join our team", "we're growing"],      "Hiring"),
        (["product", "launch", "release", "feature", "update"],               "Product Updates"),
        (["funding", "investment", "series", "raised"],                       "Funding Activity"),
        (["partnership", "partner", "collaboration"],                         "Partnerships"),
        (["award", "recognition", "ranked", "top"],                           "Awards & Recognition"),
        (["customer", "case study", "success", "client"],                     "Customer Stories"),
        (["culture", "team", "diversity", "values"],                          "Company Culture"),
        (["event", "conference", "webinar", "summit"],                        "Events"),
        (["data", "analytics", "insight", "report"],                          "Thought Leadership"),
    ]
    for keywords, theme in kw_map:
        if any(kw.lower() in texts.lower() for kw in keywords):
            themes.add(theme)

    return ", ".join(list(themes)[:6]) if themes else "General company updates"


# ─────────────────────────────────────────────────────────────────────────────
# P4  —  Free Signal Layer
# ─────────────────────────────────────────────────────────────────────────────

async def _fetch_crunchbase_signals(company_name: str, domain: str = "") -> dict:
    """
    Scrape Crunchbase public company page for funding signals.
    No API key required — uses public HTML.

    Returns: {funding_stage, total_funding, last_funding_date, investors, employee_range}
    """
    if not company_name:
        return {}

    slug = re.sub(r"[^a-z0-9]+", "-", company_name.lower()).strip("-")
    url = f"https://www.crunchbase.com/organization/{slug}"

    try:
        async with httpx.AsyncClient(
            timeout=15,
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            },
        ) as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                logger.debug("[Crunchbase] %s → %s", url, resp.status_code)
                return {}

            html = resp.text

            signals: dict = {"source": "crunchbase", "url": url}

            # Funding stage
            stage_m = re.search(
                r'"funding_stage":\s*"([^"]+)"'
                r'|Series\s+([A-F])\b'
                r'|(Seed|Pre-Seed|Angel|Series\s+[A-F]|Growth|Late Stage|IPO)',
                html, re.IGNORECASE
            )
            if stage_m:
                signals["funding_stage"] = (
                    stage_m.group(1) or stage_m.group(2) or stage_m.group(3) or ""
                ).strip()

            # Total funding
            fund_m = re.search(
                r'\$([0-9.,]+[MBK])\s*(?:Total\s+Funding|raised|funding)',
                html, re.IGNORECASE
            )
            if fund_m:
                signals["total_funding"] = f"${fund_m.group(1)}"

            # Last funding date
            date_m = re.search(
                r'"announced_on":\s*"(\d{4}-\d{2}-\d{2})"', html
            )
            if date_m:
                signals["last_funding_date"] = date_m.group(1)

            # Investors
            inv_m = re.findall(r'"name":\s*"([^"]{3,50})".*?"entity_type":\s*"investor"', html)
            if inv_m:
                signals["investors"] = list(dict.fromkeys(inv_m))[:5]

            # Employee range
            emp_m = re.search(
                r'(\d+[-–]\d+|\d+\+)\s*(?:employees|people|team members)',
                html, re.IGNORECASE
            )
            if emp_m:
                signals["employee_range"] = emp_m.group(1)

            logger.info("[Crunchbase] Signals for %s: %s", company_name, list(signals.keys()))
            return signals

    except Exception as e:
        logger.debug("[Crunchbase] Scrape failed for %s: %s", company_name, e)
        return {}


async def _fetch_google_news(company_name: str) -> list[dict]:
    """
    Fetch recent Google News mentions for the company.
    Uses Google News RSS (no API key) — last 30 days.

    Returns: [{title, url, date, source}]
    """
    if not company_name:
        return []

    query = company_name.replace(" ", "+") + "&tbs=qdr:m"
    rss_url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"

    try:
        async with httpx.AsyncClient(
            timeout=10,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; WorksBuddy/1.0)"},
        ) as client:
            resp = await client.get(rss_url)
            if resp.status_code != 200:
                return []

            text = resp.text
            items = re.findall(
                r"<item>(.*?)</item>", text, re.DOTALL
            )

            mentions = []
            for item in items[:10]:
                title_m = re.search(r"<title>(.*?)</title>", item, re.DOTALL)
                link_m  = re.search(r"<link>(.*?)</link>",   item, re.DOTALL)
                date_m  = re.search(r"<pubDate>(.*?)</pubDate>", item, re.DOTALL)
                src_m   = re.search(r"<source[^>]*>(.*?)</source>", item, re.DOTALL)

                title = re.sub(r"<[^>]+>", "", title_m.group(1) if title_m else "").strip()
                if not title or company_name.lower() not in title.lower():
                    continue

                mentions.append({
                    "title":  title[:200],
                    "url":    (link_m.group(1) if link_m else "").strip(),
                    "date":   (date_m.group(1) if date_m else "").strip()[:30],
                    "source": re.sub(r"<[^>]+>", "", src_m.group(1) if src_m else "").strip(),
                })

            logger.info("[GoogleNews] %d mentions for %s", len(mentions), company_name)
            return mentions

    except Exception as e:
        logger.debug("[GoogleNews] Failed for %s: %s", company_name, e)
        return []


async def _fetch_wappalyzer_tech(website: str) -> list[str]:
    """
    Detect tech stack from website HTML headers + meta-tags.
    Poor-man's Wappalyzer — checks for known fingerprints in page source.

    Returns: list of detected technology names
    """
    if not website:
        return []

    url = website if website.startswith("http") else f"https://{website}"

    # Technology fingerprints: (regex pattern, tech name)
    FINGERPRINTS: list[tuple[str, str]] = [
        # Frontend
        (r"react|__REACT_DEVTOOLS|data-reactroot", "React"),
        (r"__vue__|Vue\.js|v-bind", "Vue.js"),
        (r"angular\.js|ng-app|ng-controller", "Angular"),
        (r"next\.js|__NEXT_DATA__|_next/static", "Next.js"),
        (r"nuxt\.js|__NUXT__|_nuxt/", "Nuxt.js"),
        (r"svelte", "Svelte"),
        # Backend / Platform
        (r'<meta name="generator" content="WordPress', "WordPress"),
        (r"wp-content|wp-includes", "WordPress"),
        (r"Shopify\.theme|cdn\.shopify\.com", "Shopify"),
        (r"hubspot|hs-scripts|hs-analytics", "HubSpot"),
        (r"webflow|Webflow", "Webflow"),
        (r"squarespace", "Squarespace"),
        (r"wix\.com", "Wix"),
        # Analytics
        (r"google-analytics\.com|gtag\(|ga\.js", "Google Analytics"),
        (r"segment\.com|analytics\.js|analytics\.identify", "Segment"),
        (r"mixpanel", "Mixpanel"),
        (r"amplitude", "Amplitude"),
        (r"hotjar", "Hotjar"),
        (r"heap\.io|window\.heap", "Heap Analytics"),
        (r"plausible\.io", "Plausible"),
        # CRM / Marketing
        (r"intercom\.io|window\.Intercom", "Intercom"),
        (r"drift\.com|window\.drift", "Drift"),
        (r"zendesk|zEsettings", "Zendesk"),
        (r"salesforce|pardot\.com", "Salesforce"),
        (r"marketo\.com|munchkin\.js", "Marketo"),
        (r"customer\.io", "Customer.io"),
        # Infra / CDN
        (r"cloudflare\.com|__cfduid", "Cloudflare"),
        (r"fastly\.net", "Fastly"),
        (r"amazonaws\.com", "AWS"),
        (r"storage\.googleapis\.com", "Google Cloud"),
        (r"azurewebsites\.net|azure\.com", "Azure"),
        # Payments
        (r"stripe\.com|Stripe\(", "Stripe"),
        (r"paddle\.com", "Paddle"),
        (r"chargebee\.com", "Chargebee"),
        # Auth
        (r"auth0\.com", "Auth0"),
        (r"clerk\.com", "Clerk"),
        (r"okta\.com", "Okta"),
    ]

    try:
        async with httpx.AsyncClient(
            timeout=12,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; WorksBuddy/1.0)"},
        ) as client:
            resp = await client.get(url)
            if resp.status_code not in (200, 206):
                return []

            html = resp.text[:50000]  # limit to first 50K chars
            detected = []
            for pattern, tech in FINGERPRINTS:
                if re.search(pattern, html, re.IGNORECASE):
                    detected.append(tech)

            # Also check X-Powered-By header
            powered_by = resp.headers.get("X-Powered-By", "")
            if "PHP" in powered_by:
                detected.append("PHP")
            if "Express" in powered_by:
                detected.append("Express.js")
            if "ASP.NET" in powered_by:
                detected.append("ASP.NET")

            detected = list(dict.fromkeys(detected))  # deduplicate preserving order
            logger.info("[Wappalyzer] %d tech detected on %s: %s", len(detected), website, detected[:8])
            return detected[:20]

    except Exception as e:
        logger.debug("[Wappalyzer] Failed for %s: %s", website, e)
        return []


# ─────────────────────────────────────────────────────────────────────────────
# P3  —  Company AI Analysis
# ─────────────────────────────────────────────────────────────────────────────

async def _call_llm(messages: list[dict], max_tokens: int = 600) -> Optional[str]:
    """Shared LLM caller — WB_LLM_HOST → Groq → None."""
    # Try WB_LLM_HOST first
    wb_host = _wb_llm_host()
    if wb_host:
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                r = await client.post(
                    f"{wb_host}/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {_wb_llm_key()}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model":      _wb_llm_model(),
                        "messages":   messages,
                        "max_tokens": max_tokens,
                        "temperature": 0.3,
                    },
                )
                if r.status_code == 200:
                    return r.json()["choices"][0]["message"]["content"]
        except Exception as e:
            logger.debug("[CompanyAI] WB LLM failed: %s", e)

    # HuggingFace fallback
    hf_token = _k("HF_TOKEN")
    if hf_token:
        try:
            async with httpx.AsyncClient(timeout=60) as client:
                r = await client.post(
                    "https://router.huggingface.co/v1/chat/completions",
                    headers={"Authorization": f"Bearer {hf_token}", "Content-Type": "application/json"},
                    json={
                        "model":      _k("HF_MODEL", "Qwen/Qwen2.5-72B-Instruct"),
                        "messages":   messages,
                        "max_tokens": max_tokens,
                        "temperature": 0.3,
                    },
                )
                if r.status_code == 200:
                    return r.json()["choices"][0]["message"]["content"]
        except Exception as e:
            logger.debug("[CompanyAI] HuggingFace failed: %s", e)

    return None


def _parse_json(raw: str) -> Optional[dict]:
    """Extract JSON from LLM output (strips markdown fences)."""
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            try:
                return json.loads(m.group())
            except Exception:
                pass
    return None


def _rule_based_company_analysis(record: dict) -> dict:
    """Deterministic fallback when LLM is unavailable."""
    name        = record.get("name") or ""
    industry    = record.get("industry") or ""
    emp_count   = int(record.get("employee_count") or 0)
    funding     = record.get("funding_stage") or ""
    tech        = json.loads(record.get("wappalyzer_tech") or "[]")
    posts_text  = record.get("post_themes") or ""

    tags = []
    tag_map = [
        (["saas", "b2b", "software", "platform"],           "SaaS Platform"),
        (["fintech", "finance", "banking", "payment"],      "Fintech"),
        (["ai", "machine learning", "llm"],                 "AI-First"),
        (["ecommerce", "e-commerce", "retail", "shopify"],  "eCommerce"),
        (["health", "medical", "healthcare"],               "HealthTech"),
        (["security", "cybersecurity", "soc"],              "Security"),
        (["devtools", "developer", "api", "infrastructure"],"Developer Tools"),
        (["marketplace", "network", "platform"],            "Marketplace"),
        (["startup", "seed", "series a"],                   "Early Stage"),
        (["enterprise", "fortune 500", "global"],           "Enterprise"),
    ]
    combined = f"{name} {industry} {posts_text}".lower()
    for keywords, tag in tag_map:
        if any(k in combined for k in keywords):
            tags.append(tag)
        if len(tags) >= 8:
            break

    # Growth stage signal from employee count + funding
    if emp_count < 20:
        growth_stage = "Seed / Pre-Seed"
    elif emp_count < 100:
        growth_stage = "Early Growth (Series A/B)"
    elif emp_count < 500:
        growth_stage = "Scaling (Series B/C)"
    elif emp_count < 2000:
        growth_stage = "Growth Stage"
    else:
        growth_stage = "Enterprise / Late Stage"

    if funding and funding.lower() not in growth_stage.lower():
        growth_stage = f"{funding} — {growth_stage}"

    # Tech maturity
    modern_tech = {"React", "Next.js", "Vue.js", "Segment", "Amplitude", "Stripe", "AWS", "Cloudflare"}
    overlap = modern_tech & set(tech)
    tech_maturity = "Modern Stack" if len(overlap) >= 3 else ("Mixed Stack" if overlap else "Unknown / Legacy")

    culture_signals = {
        "growth_stage":   growth_stage,
        "tech_maturity":  tech_maturity,
        "hiring_velocity": "Active" if "Hiring" in posts_text else "Unknown",
        "content_themes": posts_text or "Not determined",
        "community_presence": "Active" if len(json.loads(record.get("linkedin_posts") or "[]")) >= 5 else "Low",
    }

    account_pitch = {
        "top_account_pain":   f"Scaling {name} efficiently in the {industry or 'tech'} space",
        "best_value_prop":    "Accelerate growth without proportional headcount increase",
        "executive_objection": "Already have a solution in place — need ROI proof",
        "best_angle":         "Trigger event" if "Funding" in posts_text else "Pain-based",
        "account_cta":        "Send a 2-line insight email referencing their recent activity",
    }

    return {
        "company_tags":    tags,
        "culture_signals": culture_signals,
        "account_pitch":   account_pitch,
    }


async def run_company_ai_analysis(record: dict, ws_config: dict) -> dict:
    """
    Run AI analysis on the company record.

    Returns:
        company_tags     — list of tag strings
        culture_signals  — dict (growth_stage, tech_maturity, hiring_velocity, …)
        account_pitch    — dict (pain, value_prop, objection, angle, cta)
    """
    name       = record.get("name") or ""
    industry   = record.get("industry") or ""
    emp_count  = record.get("employee_count") or 0
    funding    = record.get("funding_stage") or ""
    website    = record.get("website") or ""
    tech       = json.loads(record.get("wappalyzer_tech") or "[]")
    post_themes = record.get("post_themes") or _summarize_post_themes(
        json.loads(record.get("linkedin_posts") or "[]")
    )
    news = json.loads(record.get("news_mentions") or "[]")
    cb   = json.loads(record.get("crunchbase_data") or "{}")

    product_ctx = ""
    if ws_config.get("product_name"):
        product_ctx = f"""
Seller's product: {ws_config['product_name']}
Value proposition: {ws_config.get('value_proposition', '')}
Target titles: {', '.join(ws_config.get('target_titles') or [])}"""

    prompt = f"""Analyse this B2B company and return ONLY a JSON object with 3 keys.

Company: {name}
Industry: {industry}
Employees: {emp_count}
Funding stage: {funding}
Website: {website}
Tech stack: {', '.join(tech[:10]) if tech else 'Unknown'}
Post themes: {post_themes}
Recent news: {'; '.join(m.get('title','') for m in news[:3])}
Crunchbase funding: {cb.get('total_funding','')} from {cb.get('investors',[])}
{product_ctx}

Return ONLY valid JSON with exactly these 3 keys:

{{
  "company_tags": ["tag1", "tag2", ...],
  "culture_signals": {{
    "growth_stage": "one sentence description of company growth stage",
    "tech_maturity": "Modern Stack / Mixed / Legacy — 1 sentence",
    "hiring_velocity": "Actively hiring / Stable / Contracting — 1 sentence",
    "content_themes": "What they post about — 1 sentence",
    "community_presence": "High / Medium / Low — 1 sentence"
  }},
  "account_pitch": {{
    "top_account_pain": "company's #1 pain right now — 1 sentence",
    "best_value_prop": "which value prop resonates at account level — 1 sentence",
    "executive_objection": "most likely exec objection — 1 sentence",
    "best_angle": "trigger event / pain / social proof / curiosity",
    "account_cta": "best account-level CTA — low friction, 1 sentence"
  }}
}}

Rules:
- company_tags: 6–10 short tags (2–4 words max) describing their market, growth stage, tech focus
- All values must be based ONLY on the data above
- Return ONLY the JSON object, no explanation"""

    raw = await _call_llm(
        [
            {"role": "system", "content": "You are a B2B account intelligence AI. Analyse companies and return structured JSON only."},
            {"role": "user",   "content": prompt},
        ],
        max_tokens=700,
    )

    if raw:
        data = _parse_json(raw)
        if data and "company_tags" in data:
            logger.info("[CompanyAI] Done for %s — tags=%d", name, len(data.get("company_tags", [])))
            return {
                "company_tags":    data.get("company_tags") or [],
                "culture_signals": data.get("culture_signals") or {},
                "account_pitch":   data.get("account_pitch") or {},
            }

    logger.debug("[CompanyAI] LLM unavailable — using rule-based fallback for %s", name)
    return _rule_based_company_analysis(record)


# ─────────────────────────────────────────────────────────────────────────────
# P5  —  Company Score (0–20 company_boost added to combined_score)
# ─────────────────────────────────────────────────────────────────────────────

def compute_company_score(record: dict) -> tuple[int, str]:
    """
    Compute a 0–100 company health / ICP fit score.
    Also returns a tier letter: A+ / A / B / C.

    Scoring matrix:
      Funding signals       0–25  (stage richness + recency)
      Tech stack signals    0–20  (modern stack indicates higher buy-readiness)
      News mentions         0–15  (recent press = growth momentum)
      LinkedIn post quality 0–15  (active brand = mature company)
      Employee count match  0–15  (vs workspace ICP)
      AI tag richness       0–10  (AI analysis quality signal)

    The company_boost (0–20) added to person score = min(20, score // 5).
    """
    score = 0

    # Funding signals (0–25)
    funding_stage = (record.get("funding_stage") or "").lower()
    funding_weights = {
        "series c": 25, "series d": 25, "series e": 25, "series f": 25,
        "series b": 22, "growth": 22, "late stage": 22,
        "series a": 18, "seed": 12, "pre-seed": 8, "angel": 8,
        "ipo": 25, "public": 20, "bootstrapped": 10,
    }
    for stage, pts in funding_weights.items():
        if stage in funding_stage:
            score += pts
            break

    # Recent funding bonus (+5 if within 12 months)
    last_funding = record.get("last_funding_date") or ""
    if last_funding:
        try:
            fd = datetime.fromisoformat(last_funding.replace("Z", "+00:00"))
            if (datetime.now(timezone.utc) - fd).days < 365:
                score += 5
        except Exception:
            pass

    # Tech stack (0–20)
    tech = json.loads(record.get("wappalyzer_tech") or "[]")
    modern_indicators = {
        "Segment", "Amplitude", "Mixpanel", "Heap Analytics",
        "Stripe", "Chargebee", "Paddle",
        "React", "Next.js", "Vue.js", "Svelte",
        "Intercom", "Drift",
        "AWS", "Google Cloud", "Cloudflare",
    }
    icp_overlap = len(modern_indicators & set(tech))
    score += min(20, icp_overlap * 4)

    # News mentions (0–15)
    news = json.loads(record.get("news_mentions") or "[]")
    score += min(15, len(news) * 3)

    # LinkedIn posts (0–15)
    posts = json.loads(record.get("linkedin_posts") or "[]")
    score += min(15, len(posts) * 2)

    # Employee count (0–15) — sweet spot 50-500 for most B2B SaaS ICP
    emp = int(record.get("employee_count") or 0)
    if 50 <= emp <= 500:
        score += 15
    elif 20 <= emp < 50 or 500 < emp <= 2000:
        score += 10
    elif 10 <= emp < 20 or 2000 < emp <= 10000:
        score += 5

    # AI tag richness (0–10)
    tags = json.loads(record.get("company_tags") or "[]")
    score += min(10, len(tags))

    score = min(100, score)

    if score >= 80:
        tier = "A+"
    elif score >= 65:
        tier = "A"
    elif score >= 45:
        tier = "B"
    else:
        tier = "C"

    return score, tier


def compute_combined_score(person_score: int, company_score: int) -> int:
    """
    Combined score = person_score + company_boost (0–20).
    Caps at 100.  "Top 1%" leads have combined_score >= 90.
    """
    company_boost = min(20, company_score // 5)
    return min(100, person_score + company_boost)


# ─────────────────────────────────────────────────────────────────────────────
# P6  —  Cache + Refresh Logic
# ─────────────────────────────────────────────────────────────────────────────

def _is_cache_valid(record: dict) -> bool:
    """Return True if cached company record is still within 7-day TTL."""
    expires = record.get("cache_expires_at")
    if not expires:
        return False
    try:
        exp_dt = datetime.fromisoformat(expires.replace("Z", "+00:00"))
        return datetime.now(timezone.utc) < exp_dt
    except Exception:
        return False


def _signal_changed(old: dict, new_signals: dict) -> bool:
    """
    Return True if a significant signal has changed — triggers early refresh.
    Checks: funding_stage, total_funding, last_funding_date
    """
    for key in ("funding_stage", "total_funding", "last_funding_date"):
        old_v = (old.get(key) or "").lower().strip()
        new_v = (new_signals.get(key) or "").lower().strip()
        if new_v and old_v != new_v:
            logger.info("[CompanyCache] Signal changed: %s  %r → %r", key, old_v, new_v)
            return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Main Entry Point
# ─────────────────────────────────────────────────────────────────────────────

async def enrich_company(
    company_linkedin_url: str,
    org_id: str = "default",
    ws_config: Optional[dict] = None,
    known_data: Optional[dict] = None,
    force_refresh: bool = False,
) -> dict:
    """
    Full company enrichment — orchestrates P1–P6.

    Args:
        company_linkedin_url  LinkedIn company page URL (linkedin.com/company/…)
        org_id                Workspace / tenant identifier
        ws_config             Workspace ICP config (for AI analysis)
        known_data            Data already known from the person profile scrape
                              (name, domain, industry, employee_count, etc.)
        force_refresh         Skip cache even if valid

    Returns:
        Full company enrichment dict (all columns of company_enrichments).
        Also includes computed fields:
          company_score, company_score_tier, company_boost, company_tags,
          culture_signals, account_pitch
    """
    cid = _company_id(company_linkedin_url)
    now = datetime.now(timezone.utc)
    ws  = ws_config or {}

    # ── P6: Cache check ───────────────────────────────────────────────────────
    if not force_refresh:
        cached = await _get_company(company_linkedin_url, org_id)
        if cached and _is_cache_valid(cached):
            logger.info("[CompanyEnrich] Cache HIT for %s (expires %s)",
                        company_linkedin_url, cached.get("cache_expires_at", "?"))
            # Deserialise JSON fields before returning
            return _deserialise_record(cached)

    logger.info("[CompanyEnrich] Starting full enrichment: %s", company_linkedin_url)

    # ── Bootstrap record from known_data ─────────────────────────────────────
    record: dict = {
        "id":                  cid,
        "company_linkedin_url": company_linkedin_url,
        "org_id":              org_id,
        "name":                "",
        "domain":              "",
        "website":             "",
        "logo":                "",
        "description":         "",
        "industry":            "",
        "employee_count":      0,
        "hq_location":         "",
        "founded_year":        "",
        "funding_stage":       "",
        "total_funding":       "",
        "last_funding_date":   "",
        "lead_investor":       "",
        "company_twitter":     "",
        "company_email":       "",
        "company_phone":       "",
        "linkedin_posts":      "[]",
        "post_themes":         "",
        "crunchbase_data":     "{}",
        "news_mentions":       "[]",
        "wappalyzer_tech":     "[]",
        "company_tags":        "[]",
        "culture_signals":     "{}",
        "account_pitch":       "{}",
        "company_score":       0,
        "company_score_tier":  "C",
        "refresh_triggered_by": "",
        "raw_data":            "{}",
        "last_enriched_at":    now.isoformat(),
        "cache_expires_at":    (now + timedelta(days=CACHE_TTL_DAYS)).isoformat(),
        "updated_at":          now.isoformat(),
        # New fields from richer BD company dataset
        "linkedin_followers":  0,
        "company_slogan":      "",
        "similar_companies":   "[]",
        "organization_type":   "",
    }

    # Merge in data already extracted from person profile
    if known_data:
        for field in ("name", "domain", "website", "logo", "description",
                      "industry", "employee_count", "hq_location", "founded_year",
                      "funding_stage", "total_funding", "last_funding_date",
                      "lead_investor", "company_twitter", "company_email", "company_phone",
                      "linkedin_followers", "company_slogan", "organization_type"):
            if known_data.get(field):
                record[field] = known_data[field]
        # similar_companies comes in as a list — serialise to JSON string
        if known_data.get("similar_companies"):
            sc = known_data["similar_companies"]
            record["similar_companies"] = json.dumps(sc, default=str) if isinstance(sc, list) else sc

    # ── P2: LinkedIn Company Profile + Posts from Bright Data ────────────────
    bd_profile = await _fetch_bd_company_profile(company_linkedin_url)
    _map_bd_company_profile(bd_profile, record)
    posts = _normalize_posts(bd_profile)
    record["linkedin_posts"] = json.dumps(posts, default=str)
    record["post_themes"]    = _summarize_post_themes(posts)
    logger.info("[CompanyEnrich] P2 BD profile mapped, posts: %d", len(posts))

    # ── P4a: Crunchbase funding signals ───────────────────────────────────────
    cb_data = await _fetch_crunchbase_signals(
        record.get("name") or "", record.get("domain") or ""
    )
    record["crunchbase_data"] = json.dumps(cb_data, default=str)

    # P6: Check if funding signal changed from cache — triggers immediate refresh note
    cached_for_signal = await _get_company(company_linkedin_url, org_id)
    if cached_for_signal and _signal_changed(cached_for_signal, cb_data):
        record["refresh_triggered_by"] = "funding_signal_change"
        logger.info("[CompanyEnrich] Signal change detected — refreshing cache for %s", company_linkedin_url)

    # Patch funding fields from Crunchbase if we don't already have them
    if cb_data.get("funding_stage") and not record["funding_stage"]:
        record["funding_stage"] = cb_data["funding_stage"]
    if cb_data.get("total_funding") and not record["total_funding"]:
        record["total_funding"] = cb_data["total_funding"]
    if cb_data.get("last_funding_date") and not record["last_funding_date"]:
        record["last_funding_date"] = cb_data["last_funding_date"]
    if cb_data.get("investors") and not record["lead_investor"]:
        investors = cb_data["investors"]
        record["lead_investor"] = investors[0] if investors else ""

    # ── P4b: Google News mentions ─────────────────────────────────────────────
    news = await _fetch_google_news(record.get("name") or "")
    record["news_mentions"] = json.dumps(news, default=str)
    logger.info("[CompanyEnrich] P4 news: %d mentions", len(news))

    # ── P4c: Wappalyzer tech stack ────────────────────────────────────────────
    tech = await _fetch_wappalyzer_tech(record.get("website") or "")
    record["wappalyzer_tech"] = json.dumps(tech, default=str)
    logger.info("[CompanyEnrich] P4 tech: %d detected", len(tech))

    # ── P3: Company AI Analysis ───────────────────────────────────────────────
    ai_result = await run_company_ai_analysis(record, ws)
    record["company_tags"]   = json.dumps(ai_result.get("company_tags") or [], default=str)
    record["culture_signals"] = json.dumps(ai_result.get("culture_signals") or {}, default=str)
    record["account_pitch"]  = json.dumps(ai_result.get("account_pitch") or {}, default=str)
    logger.info("[CompanyEnrich] P3 AI: tags=%d", len(ai_result.get("company_tags") or []))

    # ── P5: Company score ─────────────────────────────────────────────────────
    score, tier = compute_company_score(record)
    record["company_score"]      = score
    record["company_score_tier"] = tier
    logger.info("[CompanyEnrich] P5 score: %d (%s)", score, tier)

    # ── Raw data dump ─────────────────────────────────────────────────────────
    record["raw_data"] = json.dumps({
        "crunchbase": cb_data,
        "news_count": len(news),
        "tech_detected": tech,
        "post_count": len(posts),
    }, default=str)

    # ── P1: Upsert to DB ─────────────────────────────────────────────────────
    await _upsert_company(record)
    logger.info("[CompanyEnrich] Saved: company_id=%s score=%d", cid, score)

    return _deserialise_record(record)


def _deserialise_record(record: dict) -> dict:
    """Parse JSON string columns back into Python objects for in-memory use."""
    result = dict(record)
    for col in ("linkedin_posts", "news_mentions", "wappalyzer_tech",
                "company_tags", "culture_signals", "account_pitch",
                "crunchbase_data", "raw_data"):
        v = result.get(col)
        if isinstance(v, str) and v:
            try:
                result[col] = json.loads(v)
            except Exception:
                pass
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Convenience: get enriched company for a lead
# ─────────────────────────────────────────────────────────────────────────────

async def get_company_for_lead(company_linkedin_url: str, org_id: str = "default") -> Optional[dict]:
    """
    Return cached company enrichment (no re-fetch).
    Used by routes to return company context alongside a lead.
    """
    record = await _get_company(company_linkedin_url, org_id)
    return _deserialise_record(record) if record else None


async def list_companies(org_id: str, limit: int = 50, offset: int = 0) -> list[dict]:
    """List all enriched companies for an org, ordered by score."""
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            """SELECT id, company_linkedin_url, name, domain, industry,
                      employee_count, funding_stage, company_score, company_score_tier,
                      company_tags, culture_signals, last_enriched_at
               FROM company_enrichments
               WHERE org_id=$1
               ORDER BY company_score DESC
               LIMIT $2 OFFSET $3""",
            org_id, limit, offset,
        )

    results = []
    for row in rows:
        r = dict(row)
        for col in ("company_tags", "culture_signals"):
            if isinstance(r.get(col), str):
                try:
                    r[col] = json.loads(r[col])
                except Exception:
                    pass
        results.append(r)
    return results
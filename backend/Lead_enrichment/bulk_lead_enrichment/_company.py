"""
_company.py
-----------
Company enrichment waterfall: Bright Data → Clearbit → Apollo → LLM.

Covers:
  - _try_clearbit_logo
  - _normalize_apollo_org, _try_apollo_org, _try_apollo_company_search
  - _guess_domain_variants
  - _normalize_bd_company, _try_bd_company
  - _COMPANY_CRM_SYSTEM_PROMPT, _build_company_crm_user_prompt
  - _fetch_apollo_company_raw, _enrich_company_with_llm
  - enrich_company_waterfall
"""

from __future__ import annotations

import json
import logging
import re
from typing import Optional

import httpx

from db import get_pool
from ._utils import (
    _apollo_api_key,
    _bd_api_key,
    _bd_company_dataset,
    _hf_model,
    _lead_id,
    _safe_int,
)
from ._clients import BD_BASE
from ._brightdata import (
    _bd_headers,
    _clean_bd_linkedin_url,
    _normalize_linkedin_url,
    poll_snapshot,
)
from ._llm import _call_llm
from ._contact import _SOCIAL_DOMAINS
from ._website import scrape_website_intelligence

try:
    from analytics import api_usage_service as _usage
except ImportError:
    class _usage:  # type: ignore
        @staticmethod
        async def track(*args, **kwargs): pass

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Clearbit logo (free)
# ─────────────────────────────────────────────────────────────────────────────

async def _try_clearbit_logo(domain: str) -> Optional[str]:
    """Clearbit Logo API — free, no auth needed."""
    if not domain:
        return None
    logo_url = f"https://logo.clearbit.com/{domain}"
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as c:
            r = await c.get(logo_url)
            if r.status_code == 200 and r.headers.get("content-type", "").startswith("image"):
                logger.info("[Clearbit] Logo found: %s", logo_url)
                return logo_url
    except Exception as e:
        logger.debug("[Clearbit] %s", e)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Apollo org helpers
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_apollo_org(org: dict) -> dict:
    """Normalize Apollo organization dict → our standard company keys."""
    tech = [t.get("name") for t in (org.get("current_technologies") or [])[:12] if t.get("name")]
    website = org.get("website_url") or org.get("primary_domain") or ""
    if website and not website.startswith("http"):
        website = f"https://{website}"
    return {
        "company_logo":        org.get("logo_url"),
        "company_description": org.get("short_description"),
        "company_phone":       org.get("phone"),
        "company_linkedin":    org.get("linkedin_url"),
        "company_twitter":     org.get("twitter_url"),
        "company_website":     website,
        "employee_count":      org.get("estimated_num_employees") or 0,
        "industry":            org.get("industry"),
        "tech_stack":          tech,
        "hq_location":         ", ".join(filter(None, [org.get("city"), org.get("country")])),
        "founded_year":        str(org.get("founded_year") or ""),
        "funding_stage":       org.get("latest_funding_stage"),
        "total_funding":       str(org.get("total_funding") or ""),
        "annual_revenue":      org.get("annual_revenue_printed"),
        "hiring_velocity":     "Active" if org.get("currently_hiring") else "",
    }


async def _try_apollo_org(domain: str) -> dict:
    """Apollo.io organization enrichment by domain."""
    if not _apollo_api_key() or not domain:
        return {}
    try:
        async with httpx.AsyncClient(timeout=25) as c:
            r = await c.post(
                "https://api.apollo.io/v1/organizations/enrich",
                headers={"Content-Type": "application/json", "X-Api-Key": _apollo_api_key()},
                json={"domain": domain},
            )
            if r.status_code != 200:
                return {}
            org = r.json().get("organization") or {}
            if not org:
                return {}
            logger.info("[Apollo Org] Found by domain %s: %s", domain, org.get("name"))
            return _normalize_apollo_org(org)
    except Exception as e:
        logger.warning("[Apollo Org] %s", e)
    return {}


async def _try_apollo_company_search(company_name: str) -> dict:
    """Apollo.io company name search — finds company by name, returns real website."""
    if not _apollo_api_key() or not company_name:
        return {}

    headers = {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "X-Api-Key": _apollo_api_key(),
    }
    endpoints = [
        ("https://api.apollo.io/v1/mixed_companies/search",     {"q_organization_name": company_name, "page": 1, "per_page": 1}),
        ("https://api.apollo.io/api/v1/organizations/search",   {"q_organization_name": company_name, "page": 1, "per_page": 1}),
    ]

    for endpoint_url, payload in endpoints:
        try:
            async with httpx.AsyncClient(timeout=25) as c:
                r = await c.post(endpoint_url, headers=headers, json=payload)
                if r.status_code not in (200, 201):
                    continue
                body = r.json()
                orgs = (
                    body.get("organizations") or body.get("accounts")
                    or body.get("mixed_companies") or []
                )
                if not orgs:
                    continue
                org = orgs[0]
                logger.info("[Apollo Search] Found company: %r → %s", company_name,
                            org.get("website_url") or org.get("primary_domain") or "no website")
                result = _normalize_apollo_org(org)
                if result.get("company_website") or result.get("company_logo"):
                    return result
        except Exception as e:
            logger.debug("[Apollo Search] %s: %s", endpoint_url, e)

    guesses = _guess_domain_variants(company_name)
    for guess in guesses[:3]:
        try:
            result = await _try_apollo_org(guess)
            if result.get("company_logo") or result.get("company_description"):
                logger.info("[Apollo Search] Domain variant match: %s → %s", company_name, guess)
                result["company_website"] = result.get("company_website") or f"https://{guess}"
                return result
        except Exception:
            pass

    return {}


def _guess_domain_variants(company_name: str) -> list[str]:
    """Generate likely domain variants for a company name."""
    name = company_name.lower()
    for noise in [" private limited", " pvt ltd", " pvt. ltd.", " ltd.", " llc", " inc.", " inc",
                  " limited", " corp.", " corp", " co.", " co", " technologies", " technology",
                  " solutions", " services", " group", " international"]:
        name = name.replace(noise, "")
    name = name.strip()
    slug = re.sub(r"[^a-z0-9\s]", "", name).strip()
    words = slug.split()

    if not words:
        return []
    if len(words) == 1:
        return [f"{words[0]}.com", f"{words[0]}tech.com", f"{words[0]}solutions.com"]
    if len(words) == 2:
        a, b = words[0], words[1]
        return [f"{a}{b}.com", f"{a}-{b}.com", f"{b}{a}.com", f"{a}.com"]
    joined = "".join(words)
    first = words[0]
    acronym = "".join(w[0] for w in words)
    variants = [f"{joined}.com", f"{first}.com"]
    if len(acronym) >= 2:
        variants.insert(0, f"{acronym}.com")
    return variants


# ─────────────────────────────────────────────────────────────────────────────
# BrightData company normalization + fetch
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_bd_company(c: dict) -> dict:
    """Normalize a raw Bright Data company record → our standard company keys."""
    result: dict = {}

    website = (
        c.get("website") or c.get("website_url") or c.get("company_website")
        or c.get("url") or c.get("homepage") or ""
    )
    if website:
        m = re.search(r"https?://(?:www\.)?([^/?\s]+)", website)
        if m and not any(m.group(1).lower() == s or m.group(1).lower().endswith("." + s) for s in _SOCIAL_DOMAINS):
            result["company_website"] = website

    logo = (
        c.get("logo") or c.get("logo_url") or c.get("company_logo")
        or c.get("profile_pic_url") or c.get("image_url")
    )
    if logo and isinstance(logo, str) and logo.startswith("http"):
        result["company_logo"] = logo

    desc = (
        c.get("description") or c.get("about") or c.get("company_description")
        or c.get("tagline") or c.get("summary") or c.get("overview")
    )
    if desc:
        result["company_description"] = str(desc)[:800]

    emp_count = 0
    employees_in_linkedin = c.get("employees_in_linkedin")
    if isinstance(employees_in_linkedin, (int, float)) and employees_in_linkedin > 0:
        emp_count = int(employees_in_linkedin)
    else:
        for field in ("employee_count", "staff_count"):
            n = _safe_int(c.get(field))
            if n > 0:
                emp_count = n
                break
        if not emp_count:
            size_str = str(c.get("company_size") or "")
            m_size = re.search(r"(\d[\d,]*)\s*(?:employees|$)", size_str.replace(",", ""))
            if m_size:
                nums = re.findall(r"\d+", size_str.replace(",", ""))
                emp_count = int(nums[-1]) if nums else 0
    if emp_count > 0:
        result["employee_count"] = emp_count

    followers = _safe_int(c.get("followers") or c.get("followers_count"))
    if followers > 0:
        result["linkedin_followers"] = followers

    ind = c.get("industry") or c.get("industries") or c.get("category")
    if ind:
        result["industry"] = ind[0] if isinstance(ind, list) else str(ind)

    hq = c.get("headquarters") or c.get("hq_location") or c.get("location")
    if hq:
        if isinstance(hq, dict):
            parts = [hq.get("city", ""), hq.get("region", ""), hq.get("country", "")]
            result["hq_location"] = ", ".join(p for p in parts if p)
        else:
            result["hq_location"] = str(hq)

    founded = c.get("founded") or c.get("founded_year") or c.get("founded_on")
    if founded:
        result["founded_year"] = str(founded)

    specs = c.get("specialties") or c.get("specializations") or []
    if specs:
        if isinstance(specs, str):
            result["tech_stack"] = [s.strip() for s in specs.split(",") if s.strip()][:10]
        else:
            result["tech_stack"] = specs[:10]

    funding_obj = c.get("funding") if isinstance(c.get("funding"), dict) else {}
    last_round_type = (
        funding_obj.get("last_round_type")
        or c.get("funding_stage") or c.get("company_type") or c.get("type")
    )
    if last_round_type:
        result["funding_stage"] = str(last_round_type)

    last_round_raised = funding_obj.get("last_round_raised") or c.get("total_funding")
    if last_round_raised:
        result["total_funding"] = str(last_round_raised)

    last_round_date = funding_obj.get("last_round_date") or c.get("last_funding_date")
    if last_round_date:
        result["last_funding_date"] = str(last_round_date)[:10]

    org_type = c.get("organization_type") or c.get("org_type") or c.get("type")
    if org_type:
        result["organization_type"] = str(org_type)

    slogan = c.get("slogan") or c.get("tagline")
    if slogan:
        result["company_slogan"] = str(slogan)[:200]

    similar = c.get("similar") or c.get("similar_companies") or []
    if similar and isinstance(similar, list):
        similar_clean = [
            {"name": s.get("title", ""), "industry": s.get("subtitle", ""),
             "location": s.get("location", ""), "linkedin": s.get("Links", "")}
            for s in similar[:10] if isinstance(s, dict) and s.get("title")
        ]
        if similar_clean:
            result["similar_companies"] = similar_clean

    updates = c.get("updates") or c.get("posts") or []
    if updates and isinstance(updates, list):
        posts_clean = []
        for u in updates[:10]:
            if not isinstance(u, dict):
                continue
            post = {
                "post_id": u.get("post_id", ""), "date": u.get("date", ""),
                "text": (u.get("text") or "")[:500],
                "likes_count": u.get("likes_count", 0),
                "comments_count": u.get("comments_count", 0),
                "post_url": u.get("post_url", ""),
            }
            if post["text"] or post["post_id"]:
                posts_clean.append(post)
        if posts_clean:
            result["linkedin_posts"] = posts_clean

    crunchbase_url = c.get("crunchbase_url") or c.get("crunchbase")
    if crunchbase_url:
        result["crunchbase_data"] = {"url": str(crunchbase_url)}

    phone = c.get("phone") or c.get("phone_number") or c.get("company_phone")
    if phone:
        result["company_phone"] = str(phone)

    email = c.get("email") or c.get("company_email")
    if email:
        result["company_email"] = str(email)

    tw = c.get("twitter") or c.get("twitter_url")
    if tw:
        result["company_twitter"] = str(tw)

    for url_field in ("linkedin_url", "company_linkedin", "url"):
        v = c.get(url_field) or ""
        if v and "linkedin.com" in v.lower():
            result[url_field] = _clean_bd_linkedin_url(v)

    return result


async def _try_bd_company(company_linkedin_url: str) -> dict:
    """
    Fetch LinkedIn company profile via Bright Data company dataset.
    Tries sync /scrape, falls back to async trigger+poll.
    """
    if not _bd_api_key() or not company_linkedin_url:
        return {}
    if "linkedin.com/company/" not in company_linkedin_url:
        return {}

    dataset_id = _bd_company_dataset()
    logger.info("[BD Company] Fetching: %s (dataset=%s)", company_linkedin_url, dataset_id)

    snapshot_id = None
    try:
        async with httpx.AsyncClient(timeout=90) as client:
            resp = await client.post(
                f"{BD_BASE}/scrape",
                params={"dataset_id": dataset_id, "notify": "false", "include_errors": "true", "format": "json"},
                headers=_bd_headers(),
                json={"input": [{"url": company_linkedin_url}]},
            )
            await _usage.track("brightdata", "company")
            if resp.status_code not in (200, 202):
                logger.warning("[BD Company] /scrape failed %s: %s", resp.status_code, resp.text[:200])
            else:
                body = resp.json()
                data_list = body if isinstance(body, list) else body.get("data") or []
                if data_list and isinstance(data_list[0], dict):
                    c = data_list[0]
                    if c.get("name") or c.get("description") or c.get("website") or c.get("logo"):
                        result = _normalize_bd_company(c)
                        if result:
                            logger.info("[BD Company] Sync OK — %d fields", len(result))
                            return result
                if isinstance(body, dict):
                    snapshot_id = body.get("snapshot_id")
                if not snapshot_id and data_list and isinstance(data_list[0], dict):
                    snapshot_id = data_list[0].get("snapshot_id")
                if snapshot_id:
                    logger.info("[BD Company] Async job started — snapshot_id=%s", snapshot_id)
    except Exception as e:
        logger.warning("[BD Company] /scrape error: %s", e)

    if snapshot_id:
        try:
            data = await poll_snapshot(snapshot_id, interval=5, timeout=120)
            if data:
                c = data[0] if isinstance(data, list) else data
                result = _normalize_bd_company(c)
                if result:
                    logger.info("[BD Company] Async poll OK — %d fields", len(result))
                    return result
        except Exception as e:
            logger.warning("[BD Company] Snapshot poll failed for %s: %s", snapshot_id, e)

    logger.warning("[BD Company] All methods failed for %s", company_linkedin_url)
    return {}


# ─────────────────────────────────────────────────────────────────────────────
# Company LLM enrichment
# ─────────────────────────────────────────────────────────────────────────────

_COMPANY_CRM_SYSTEM_PROMPT = """You are an expert B2B CRM data analyst. Enrich raw company data from 3 sources into a fully structured CRM profile.

Return ONLY a valid JSON object — no markdown, no explanation, no preamble, no code fences.
The output JSON must use EXACTLY the keys and structure in the schema — no extra keys, no missing keys.

DATA SOURCES (use all 3):
1. brightdata_linkedin_data — LinkedIn profile: headcount, followers, posts, location, key_people
2. apollo_data             — Apollo.io: funding, tech_stack, revenue, industry
3. website_intelligence    — Live website scrape: contact info (phones_found, emails_found), services, ICP, messaging

CONTACT INFO RULES (critical):
- phone_primary / phones[]: use phones_found from website_intelligence if available — never invent numbers
- primary_email / emails[]: use emails_found from website_intelligence if available — never invent emails
- If no phone/email found in any source, leave as empty string "" or []

STRICT RULES:
- Return ONLY valid JSON. No markdown, no code fences, no extra text.
- NEVER use "Unknown", "N/A", or placeholder values.
- NEVER hallucinate phone numbers or email addresses.
- Arrays must have real, distinct items — use [] if none found.
- Infer missing fields intelligently from available signals.
- revenue_range: infer from employee_count + funding_stage + industry.
- lead_score: 0–100 (company size + activity + funding + relevance).
- lead_temperature: "Hot" (hiring+funded+posting) | "Warm" (moderate) | "Cold" (minimal).
- activities[]: extract from LinkedIn posts in brightdata_linkedin_data.
- key_people[]: extract from LinkedIn employees/team data in brightdata_linkedin_data.
- services[]: extract from website_intelligence product/service pages.
- competitive_intelligence: use similar_companies from brightdata if available."""


def _build_company_crm_user_prompt(bd_raw: dict, apollo_raw: dict, company_name: str, website_intel: dict | None = None) -> str:
    combined = {
        "company_name":             company_name,
        "brightdata_linkedin_data": bd_raw,
        "apollo_data":              apollo_raw,
        "website_intelligence":     website_intel or {},
    }
    raw_json = json.dumps(combined, separators=(",", ":"), ensure_ascii=False, default=str)
    schema = """{
  "company_identity": {
    "legal_name": "string",
    "brand_name": "string",
    "tagline": "string",
    "description_short": "string - 1 sentence",
    "description_long": "string - 2-3 sentences",
    "founded_year": 0,
    "company_stage": "Early Stage Startup | Growth Stage | Established | Enterprise",
    "organization_type": "string",
    "industry_primary": "string",
    "industry_secondary": ["string"],
    "hq_city": "string",
    "hq_state": "string",
    "hq_country": "2-letter ISO code e.g. DE",
    "hq_full_address": "string",
    "office_locations": ["string"],
    "global_reach": true,
    "website": "https://...",
    "linkedin_url": "https://linkedin.com/company/...",
    "crunchbase_url": "string or empty"
  },
  "tags": ["string - 3 to 6 topic tags"],
  "tech_tags": ["string - technologies used, empty array if none found"],
  "services": [
    {"name": "string", "category": "string", "description": "string", "target_audience": "string"}
  ],
  "key_people": [
    {"name": "string", "title": "string", "seniority": "string", "department": "string", "linkedin_url": "string", "is_decision_maker": false, "contact_confidence": "High | Medium | Low"}
  ],
  "contact_info": {
    "primary_email": "string - from website emails_found or empty",
    "emails": ["string - from website emails_found"],
    "phone_primary": "string - from website phones_found or empty",
    "phones": ["string - from website phones_found"],
    "whatsapp": "string or empty",
    "preferred_contact_channel": "Email | Phone | LinkedIn | Website"
  },
  "financials": {
    "funding_status": "Bootstrapped | Pre-Seed | Seed | Series A | Series B | Series C+ | Public | Unknown",
    "total_funding_usd": 0,
    "last_round_type": "string",
    "last_round_date": "YYYY-MM-DD or empty",
    "last_round_amount_usd": 0,
    "revenue_range": "<$1M | $1M-$10M | $10M-$50M | $50M-$100M | $100M+",
    "employee_count_linkedin": 0,
    "employee_count_range": "1-10 | 11-50 | 51-200 | 201-500 | 501-1000 | 1000+"
  },
  "social_presence": {
    "linkedin_followers": 0,
    "linkedin_engagement_avg": "High | Medium | Low",
    "posting_frequency": "Daily | Weekly | Monthly | Rarely",
    "content_themes": ["string"],
    "last_post_date": "YYYY-MM-DD or empty"
  },
  "activities": [
    {"date": "YYYY-MM-DD", "type": "Post | Event | Funding | Hiring | Product Launch", "summary": "string", "engagement_score": 0, "sentiment": "Positive | Neutral | Negative"}
  ],
  "interests": ["string - 3 to 6 interest areas"],
  "icp_fit": {
    "ideal_customer_profile": "string",
    "target_company_size": "string",
    "target_industries": ["string"],
    "target_geographies": ["string"],
    "deal_type": "string"
  },
  "competitive_intelligence": {
    "direct_competitors": ["string"],
    "market_position": "Leader | Challenger | Emerging Player | Niche",
    "differentiators": ["string"],
    "weaknesses_inferred": ["string"]
  },
  "crm_metadata": {
    "lead_score": 0,
    "lead_temperature": "Hot | Warm | Cold",
    "outreach_priority": "High | Medium | Low",
    "best_outreach_time": "Morning | Afternoon | Evening",
    "recommended_first_message": "string - personalised opening line",
    "crm_tags": ["string"],
    "data_source": "LinkedIn",
    "data_scraped_at": "ISO datetime string",
    "enrichment_confidence": "High | Medium | Low"
  }
}"""
    return (
        f"Enrich the following company data into a complete CRM profile.\n\n"
        f"RAW DATA (3 sources — use all):\n{raw_json}\n\n"
        f"IMPORTANT: Use phones_found and emails_found from website_intelligence for contact_info. "
        f"Do NOT invent phone numbers or emails.\n\n"
        f"Return a JSON object with EXACTLY this schema (same keys, same nesting — no additions, no removals):\n{schema}"
    )


async def _fetch_apollo_company_raw(company_name: str, domain: str) -> dict:
    """Fetch raw Apollo org dict for LLM — tries domain enrich then name search."""
    if not _apollo_api_key():
        return {}
    if domain:
        try:
            async with httpx.AsyncClient(timeout=25) as c:
                r = await c.post(
                    "https://api.apollo.io/v1/organizations/enrich",
                    headers={"Content-Type": "application/json", "X-Api-Key": _apollo_api_key()},
                    json={"domain": domain},
                )
                if r.status_code == 200:
                    org = r.json().get("organization") or {}
                    if org.get("name"):
                        return org
        except Exception as e:
            logger.debug("[Apollo Raw] domain enrich failed: %s", e)
    if company_name:
        for endpoint_url, payload in [
            ("https://api.apollo.io/v1/mixed_companies/search",    {"q_organization_name": company_name, "page": 1, "per_page": 1}),
            ("https://api.apollo.io/api/v1/organizations/search",  {"q_organization_name": company_name, "page": 1, "per_page": 1}),
        ]:
            try:
                async with httpx.AsyncClient(timeout=25) as c:
                    r = await c.post(
                        endpoint_url,
                        headers={"Content-Type": "application/json", "X-Api-Key": _apollo_api_key()},
                        json=payload,
                    )
                    if r.status_code in (200, 201):
                        body = r.json()
                        orgs = body.get("organizations") or body.get("accounts") or body.get("mixed_companies") or []
                        if orgs:
                            return orgs[0]
            except Exception as e:
                logger.debug("[Apollo Raw] name search failed: %s", e)
    return {}


async def _enrich_company_with_llm(bd_raw: dict, apollo_raw: dict, company_name: str, website_intel: dict | None = None) -> dict:
    """Call LLM with combined BD + Apollo + website data. Returns CRM profile dict."""
    if not bd_raw and not apollo_raw and not website_intel:
        return {}
    try:
        user_prompt = _build_company_crm_user_prompt(bd_raw, apollo_raw, company_name, website_intel)
        logger.info("[CompanyLLM] Calling LLM for company: %s (prompt=%d chars)", company_name, len(user_prompt))
        raw = await _call_llm(
            [
                {"role": "system", "content": _COMPANY_CRM_SYSTEM_PROMPT},
                {"role": "user",   "content": user_prompt},
            ],
            max_tokens=6000,
            temperature=0.2,
            hf_first=True,
        )
        if not raw:
            logger.warning("[CompanyLLM] LLM returned empty response for %s", company_name)
            return {}
        raw = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL)
        raw = re.sub(r"<think>.*", "", raw, flags=re.DOTALL).strip()
        logger.info("[CompanyLLM] Raw LLM response for %s: %d chars", company_name, len(raw))
        start = raw.find("{")
        if start == -1:
            logger.warning("[CompanyLLM] No JSON object in response for %s — preview: %r", company_name, raw[:300])
            return {}
        depth, end = 0, -1
        for i, ch in enumerate(raw[start:], start):
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    end = i
                    break
        if end == -1:
            logger.warning("[CompanyLLM] JSON truncated (max_tokens hit) for %s — len=%d", company_name, len(raw))
            return {}
        parsed = json.loads(raw[start:end + 1])
        logger.info("[CompanyLLM] CRM brief generated for %s (%d top-level keys)", company_name, len(parsed))
        return parsed
    except json.JSONDecodeError as e:
        logger.warning("[CompanyLLM] JSON parse failed for %s: %s", company_name, e)
        return {}
    except Exception as e:
        logger.warning("[CompanyLLM] LLM enrichment failed for %s: %s", company_name, e)
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# Main waterfall
# ─────────────────────────────────────────────────────────────────────────────

async def enrich_company_waterfall(
    company_name: str,
    domain: str,
    profile: dict,
    lead_id: str = "",
) -> tuple[dict, list[dict]]:
    """
    Company data waterfall:
      0a. BrightData company — DB cache first, API only if missing
      0b. Website scrape    — DB cache first, scrape only if missing
          (website URL taken from: profile → BD result → domain fallback)
      0c. Apollo raw        — DB only, never call API
      1.  Person profile fields from BrightData person scrape
      2.  Clearbit Logo
      3.  LLM enrichment — BD + Apollo + Website combined

    BD and website run in parallel ONLY when website URL is already known.
    If website URL only becomes known after BD (empty domain/website initially),
    website scrape runs after BD completes.
    """
    import asyncio as _asyncio

    log: list[dict] = []
    result: dict = {}

    raw_link = profile.get("current_company_link") or profile.get("current_company_url") or ""
    company_linkedin_url = raw_link if "linkedin.com/company/" in raw_link else ""

    # Determine initial website URL from profile (exclude social domains)
    raw_website = profile.get("current_company_website") or profile.get("company_website") or ""
    if not raw_website and "linkedin.com/company/" not in (profile.get("current_company_link") or ""):
        raw_website = profile.get("current_company_link") or ""
    m = re.search(r"https?://(?:www\.)?([^/?\s]+)", raw_website)
    actual_website = raw_website if (m and not any(
        m.group(1).lower() == s or m.group(1).lower().endswith("." + s)
        for s in _SOCIAL_DOMAINS
    )) else ""
    actual_website = actual_website or (f"https://{domain}" if domain else "")

    async def _noop() -> dict:
        return {}

    # ── Step 0a: BrightData company — DB cache first ──────────────────────────
    bd_raw: dict = {}
    bd_co: dict = {}

    if lead_id:
        try:
            async with get_pool().acquire() as _c:
                _row = await _c.fetchrow(
                    "SELECT raw_brightdata_company FROM enriched_leads WHERE id=$1 AND raw_brightdata_company IS NOT NULL",
                    lead_id,
                )
            if _row and _row["raw_brightdata_company"]:
                _parsed = json.loads(_row["raw_brightdata_company"]) if isinstance(_row["raw_brightdata_company"], str) else _row["raw_brightdata_company"]
                if isinstance(_parsed, dict) and _parsed:
                    bd_co = _parsed
                    bd_raw = dict(_parsed)
                    result.update(_parsed)
                    logger.info("[CompanyWaterfall] BD company from DB cache for lead %s", lead_id)
                    log.append({"step": "0a", "source": "BrightData Company (DB cache)", "fields_found": list(bd_co.keys()), "note": "cached"})
        except Exception as _e:
            logger.debug("[CompanyWaterfall] BD DB cache read failed: %s", _e)

    if not bd_co and company_linkedin_url:
        logger.info("[CompanyWaterfall] BD company cache miss — calling API for %s", company_linkedin_url)
        try:
            _bd_result = await _try_bd_company(company_linkedin_url)
            if isinstance(_bd_result, dict) and _bd_result:
                bd_co = _bd_result
                bd_raw = dict(_bd_result)
                result.update(_bd_result)
                log.append({"step": "0a", "source": "BrightData Company (API)", "fields_found": list(bd_co.keys()), "note": company_linkedin_url})
                # Save to DB
                if lead_id:
                    try:
                        async with get_pool().acquire() as _c:
                            await _c.execute(
                                "UPDATE enriched_leads SET raw_brightdata_company=$1, updated_at=NOW() WHERE id=$2",
                                json.dumps(bd_co, default=str), lead_id,
                            )
                        logger.info("[CompanyWaterfall] raw_brightdata_company saved for lead %s", lead_id)
                    except Exception as _e:
                        logger.warning("[CompanyWaterfall] Failed to save raw_brightdata_company: %s", _e)
        except Exception as _e:
            logger.warning("[CompanyWaterfall] BD company API failed: %s", _e)
    elif not company_linkedin_url:
        logger.info("[CompanyWaterfall] No company LinkedIn URL — skipped BD company scrape")

    # After BD: update website URL if BD returned one and we didn't have it
    if bd_co.get("company_website") and not actual_website:
        actual_website = bd_co["company_website"]
        m2 = re.search(r"https?://(?:www\.)?([^/?\s]+)", actual_website)
        if m2:
            domain = m2.group(1).lower()
        logger.info("[CompanyWaterfall] Website URL from BD: %s", actual_website)
    elif bd_co.get("company_website"):
        # BD has a better URL — prefer it
        _bd_site = bd_co["company_website"]
        m2 = re.search(r"https?://(?:www\.)?([^/?\s]+)", _bd_site)
        if m2:
            actual_website = _bd_site
            domain = m2.group(1).lower()

    # ── Step 0b: Website scrape — DB cache first ──────────────────────────────
    website_intel: dict = {}

    if lead_id:
        try:
            async with get_pool().acquire() as _c:
                _row = await _c.fetchrow(
                    "SELECT raw_website_scrap FROM enriched_leads WHERE id=$1 AND raw_website_scrap IS NOT NULL",
                    lead_id,
                )
            if _row and _row["raw_website_scrap"]:
                _parsed = json.loads(_row["raw_website_scrap"]) if isinstance(_row["raw_website_scrap"], str) else _row["raw_website_scrap"]
                if isinstance(_parsed, dict) and _parsed.get("status") not in (None, "unreachable"):
                    website_intel = _parsed
                    logger.info("[CompanyWaterfall] Website intel from DB cache for lead %s", lead_id)
                    log.append({"step": "0b", "source": "Website Scrape (DB cache)", "fields_found": [k for k, v in website_intel.items() if v and k not in ("status", "website", "pages_scraped")], "note": "cached"})
        except Exception as _e:
            logger.debug("[CompanyWaterfall] Website DB cache read failed: %s", _e)

    if not website_intel and actual_website:
        logger.info("[CompanyWaterfall] Website cache miss — scraping %s", actual_website)
        try:
            _w = await scrape_website_intelligence(actual_website)
            if isinstance(_w, dict) and _w.get("status") not in (None, "unreachable"):
                website_intel = _w
                log.append({"step": "0b", "source": "Website Scrape (Live)", "fields_found": [k for k, v in website_intel.items() if v and k not in ("status", "website", "pages_scraped")], "note": f"pages={website_intel.get('pages_scraped', [])}"})
                # Save to DB
                if lead_id:
                    try:
                        async with get_pool().acquire() as _c:
                            await _c.execute(
                                "UPDATE enriched_leads SET raw_website_scrap=$1, updated_at=NOW() WHERE id=$2",
                                json.dumps(website_intel, default=str), lead_id,
                            )
                        logger.info("[CompanyWaterfall] raw_website_scrap saved for lead %s", lead_id)
                    except Exception as _e:
                        logger.warning("[CompanyWaterfall] Failed to save raw_website_scrap: %s", _e)
        except Exception as _e:
            logger.warning("[CompanyWaterfall] Website scrape failed: %s", _e)
    elif not actual_website:
        logger.info("[CompanyWaterfall] No website URL — skipped website scrape for %s", company_name)

    if website_intel and not result.get("company_description") and website_intel.get("company_description"):
        result["company_description"] = website_intel["company_description"]

    # ── Step 1: Person profile fields ─────────────────────────────────────────
    profile_fields = {
        "company_logo":        profile.get("current_company_logo") or profile.get("company_logo") or profile.get("logo"),
        "company_linkedin":    company_linkedin_url or None,
        "company_website":     actual_website or None,
        "company_description": profile.get("current_company_description"),
        "employee_count":      _safe_int(profile.get("current_company_employees_count")),
        "industry":            profile.get("current_company_industry"),
        "founded_year":        str(profile.get("current_company_founded") or ""),
    }
    filled = [k for k, v in profile_fields.items() if v and (k not in result or not result[k])]
    if filled:
        result.update({k: profile_fields[k] for k in filled})
        log.append({"step": 1, "source": "BrightData (profile)", "fields_found": filled})

    # ── Step 2: Clearbit logo ──────────────────────────────────────────────────
    if not result.get("company_logo") and domain:
        logo = await _try_clearbit_logo(domain)
        if logo:
            result["company_logo"] = logo
            log.append({"step": 2, "source": "Clearbit Logo", "fields_found": ["company_logo"], "note": domain})

    # ── Step 3: Apollo — DB only, never call API here ─────────────────────────
    apollo_raw: dict = {}
    _lid = lead_id or (profile.get("input_url") or profile.get("url") or "")
    if _lid:
        try:
            _lookup_id = _lid if not _lid.startswith("http") else _lead_id(_normalize_linkedin_url(_lid))
            async with get_pool().acquire() as _conn:
                _row = await _conn.fetchrow(
                    "SELECT apollo_raw FROM enriched_leads WHERE id=$1 AND apollo_raw IS NOT NULL LIMIT 1",
                    _lookup_id,
                )
            if _row and _row["apollo_raw"]:
                _parsed = json.loads(_row["apollo_raw"]) if isinstance(_row["apollo_raw"], str) else _row["apollo_raw"]
                if isinstance(_parsed, dict) and _parsed:
                    apollo_raw = _parsed
                    logger.info("[CompanyWaterfall] Apollo raw from DB (%d keys)", len(apollo_raw))
                    log.append({"step": 3, "source": "Apollo.io (DB cache)", "fields_found": list(apollo_raw.keys())[:8], "note": "from email enrichment"})
        except Exception as _e:
            logger.debug("[CompanyWaterfall] Apollo DB lookup failed: %s", _e)

    if not apollo_raw:
        logger.info("[CompanyWaterfall] Apollo raw not in DB — skipping API call for %s", company_name)

    # ── Step 4: LLM — BrightData + Apollo + Website ───────────────────────────
    bd_for_llm = {
        **bd_raw,
        "company_name":        company_name,
        "linkedin_url":        company_linkedin_url,
        "from_person_profile": {k: v for k, v in profile_fields.items() if v},
    }
    if bd_for_llm or apollo_raw or website_intel:
        llm_result = await _enrich_company_with_llm(bd_for_llm, apollo_raw, company_name, website_intel or None)
        if llm_result:
            result["company_crm_brief"] = llm_result
            ci   = llm_result.get("company_identity") or {}
            fin  = llm_result.get("financials") or {}
            soc  = llm_result.get("social_presence") or {}
            meta = llm_result.get("crm_metadata") or {}
            _llm_fills = {
                "company_description": ci.get("description_long") or ci.get("description_short"),
                "industry":            ci.get("industry_primary"),
                "founded_year":        str(ci.get("founded_year") or ""),
                "hq_location":         ", ".join(filter(None, [ci.get("hq_city"), ci.get("hq_country")])),
                "employee_count":      fin.get("employee_count_linkedin") or 0,
                "funding_stage":       fin.get("funding_status"),
                "total_funding":       str(fin.get("total_funding_usd") or ""),
                "linkedin_followers":  soc.get("linkedin_followers") or 0,
                "tech_stack":          llm_result.get("tech_tags") or [],
                "company_tags":        llm_result.get("tags") or [],
            }
            for k, v in _llm_fills.items():
                if v and not result.get(k):
                    result[k] = v
            log.append({
                "step": 4, "source": f"LLM {_hf_model().split('/')[-1]} (BD+Apollo+Website)",
                "fields_found": list(llm_result.keys()),
                "note": f"lead_score={meta.get('lead_score', 0)} temp={meta.get('lead_temperature', '')}",
            })

    result["_verified_domain"] = domain
    return result, log

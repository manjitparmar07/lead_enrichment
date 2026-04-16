"""
_website.py
-----------
Stage 3 — Website Intelligence scraping.

Covers:
  - _extract_avatar (profile photo extraction from BD response)
  - _fetch_page (HTTP fetch → BeautifulSoup parse → text + structured data)
  - _extract_contacts_from_soup (tel: + mailto: links — most accurate)
  - _extract_social_links (social media URLs from anchor hrefs)
  - scrape_website_intelligence (LLM-powered website analysis)

Library: httpx (async HTTP) + BeautifulSoup4 (HTML parsing)
  - BeautifulSoup gives accurate tel:/mailto: link extraction
  - Much more reliable than regex on raw HTML
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Optional

import httpx
from bs4 import BeautifulSoup

from ._llm import _call_llm, _parse_json_from_llm

logger = logging.getLogger(__name__)

_SOCIAL_PLATFORMS = [
    ("linkedin",   "linkedin.com"),
    ("twitter",    "twitter.com"),
    ("twitter",    "x.com"),
    ("facebook",   "facebook.com"),
    ("instagram",  "instagram.com"),
    ("youtube",    "youtube.com"),
    ("tiktok",     "tiktok.com"),
    ("pinterest",  "pinterest.com"),
    ("whatsapp",   "wa.me"),
    ("telegram",   "t.me"),
    ("github",     "github.com"),
]


def _extract_avatar(profile: dict) -> Optional[str]:
    """Extract avatar/photo URL from Bright Data profile fields."""
    for key in ["avatar_url", "profile_pic_src", "profile_picture", "image_url", "image", "photo", "avatar"]:
        v = profile.get(key)
        if v and isinstance(v, str) and v.startswith("http"):
            return v
    return None


def _extract_contacts_from_soup(soup: BeautifulSoup) -> tuple[list[str], list[str]]:
    """
    Extract phones and emails from tel: and mailto: anchor hrefs.
    This is far more accurate than regex on raw text — site owners put
    the correct number in the href even if the display text is formatted differently.
    """
    phones: list[str] = []
    emails: list[str] = []

    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if href.startswith("tel:"):
            num = href[4:].strip().replace(" ", "").replace("-", "")
            # Keep only if has 7+ digits
            if len(re.sub(r"\D", "", num)) >= 7 and num not in phones:
                phones.append(href[4:].strip())  # keep original formatting
        elif href.startswith("mailto:"):
            email = href[7:].strip().split("?")[0]  # strip ?subject= etc
            if email and "@" in email and email not in emails:
                emails.append(email)

    # Fallback regex on page text for phones (if tel: links are sparse)
    return phones[:5], emails[:5]


def _extract_social_links(soup: BeautifulSoup) -> dict[str, str]:
    """
    Extract social media profile URLs from all anchor hrefs on the page.
    Returns dict like: { "linkedin": "https://linkedin.com/company/...", "instagram": "..." }
    """
    found: dict[str, str] = {}
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href.startswith("http"):
            continue
        for platform, domain in _SOCIAL_PLATFORMS:
            if platform not in found and domain in href.lower():
                found[platform] = href
    return found


def _extract_phones_regex_fallback(text: str) -> list[str]:
    """
    Regex fallback for phones — used only when tel: links found nothing.
    Less accurate but covers sites that display numbers as plain text.
    """
    phones = list(dict.fromkeys(re.findall(
        r'(?<!\d)(\+?[\d][\d\s\-().]{6,18}[\d])(?!\d)',
        text
    )))
    return [p.strip() for p in phones if len(re.sub(r"\D", "", p)) >= 7][:5]


def _extract_emails_regex_fallback(text: str) -> list[str]:
    """
    Regex fallback for emails — used only when mailto: links found nothing.
    """
    emails = list(dict.fromkeys(re.findall(
        r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
        text
    )))
    return [e for e in emails if not any(x in e.lower() for x in ["@2x", ".png", ".jpg", ".svg"])][:5]


async def _fetch_page(url: str, timeout: int = 12) -> tuple[str, Optional[BeautifulSoup]]:
    """
    Fetch a URL. Returns (clean_text, soup).
    soup is None on failure — used for contact/social extraction on homepage.
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as c:
            r = await c.get(url, headers=headers)
            if r.status_code != 200:
                return "", None
            html = r.text

            soup = BeautifulSoup(html, "html.parser")

            # Remove script/style tags before text extraction
            for tag in soup(["script", "style", "noscript"]):
                tag.decompose()

            text = soup.get_text(separator=" ", strip=True)
            text = re.sub(r"\s+", " ", text).strip()
            return text[:4000], soup

    except Exception as e:
        logger.debug("[WebScrape] %s → %s", url, e)
        return "", None


async def scrape_contact_page(domain: str) -> dict:
    """
    Lightweight contact scraper — only hits homepage + /contact + /contact-us.
    Used in the email enrichment waterfall as a free fallback before pattern guessing.

    Returns: {email, phone, source, confidence} or {} if nothing found.
    - email: first business email found (mailto: links first, regex fallback)
    - phone: first phone found (tel: links first, regex fallback)
    - source: "website_scrape"
    - confidence: "medium"
    """
    if not domain:
        return {}

    base = domain.strip().rstrip("/")
    if not base.startswith("http"):
        base = f"https://{base}"
    base_www = base.replace("://", "://www.", 1) if "://www." not in base else None

    # Only contact-focused pages — fast and targeted
    urls_to_try = [
        f"{base}/contact",
        f"{base}/contact-us",
        base,                         # homepage (footer often has email/phone)
    ]
    if base_www:
        urls_to_try += [
            f"{base_www}/contact",
            f"{base_www}/contact-us",
            base_www,
        ]

    fetch_tasks = [_fetch_page(url, timeout=8) for url in urls_to_try]
    results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

    found_emails: list[str] = []
    found_phones: list[str] = []
    all_text = ""

    for result in results:
        if isinstance(result, Exception):
            continue
        text, soup = result
        if not text:
            continue
        all_text += " " + text
        if soup:
            _p, _e = _extract_contacts_from_soup(soup)
            for e in _e:
                if e not in found_emails:
                    found_emails.append(e)
            for p in _p:
                if p not in found_phones:
                    found_phones.append(p)
        if found_emails and found_phones:
            break  # We have both — stop early

    # Regex fallback if structured extraction found nothing
    if not found_emails and all_text:
        found_emails = _extract_emails_regex_fallback(all_text)
    if not found_phones and all_text:
        found_phones = _extract_phones_regex_fallback(all_text)

    if not found_emails and not found_phones:
        return {}

    # Filter out image/asset false positives that slip through regex
    clean_emails = [
        e for e in found_emails
        if "@" in e and not any(ext in e.lower() for ext in [".png", ".jpg", ".svg", ".gif", ".webp"])
    ]

    result_data: dict = {"source": "website_scrape", "confidence": "medium"}
    if clean_emails:
        result_data["email"] = clean_emails[0]
        logger.info("[WebContactScrape] Found email: %s from %s", clean_emails[0], domain)
    if found_phones:
        result_data["phone"] = found_phones[0]
        logger.info("[WebContactScrape] Found phone: %s from %s", found_phones[0], domain)

    return result_data


async def scrape_website_intelligence(website: str) -> dict:
    """
    Scrapes homepage, about, product, services, contact, pricing, careers, blog pages.

    Uses BeautifulSoup for:
      - tel: links  → accurate phone numbers
      - mailto: links → accurate email addresses
      - social media hrefs → LinkedIn, Instagram, Twitter, Facebook, YouTube etc

    LLM then generates structured company intelligence from page content.
    """
    if not website:
        return {}

    base = website.strip().rstrip("/")
    if not base.startswith("http"):
        base = f"https://{base}"
    base_www = base.replace("://", "://www.", 1) if "://www." not in base else None

    page_candidates = [
        ("homepage",  base),
        ("homepage",  base_www or base),
        ("about",     f"{base}/about"),
        ("about",     f"{base}/about-us"),
        ("about",     f"{base}/who-we-are"),
        ("product",   f"{base}/product"),
        ("product",   f"{base}/products"),
        ("product",   f"{base}/platform"),
        ("services",  f"{base}/services"),
        ("services",  f"{base}/solutions"),
        ("features",  f"{base}/features"),
        ("contact",   f"{base}/contact"),
        ("contact",   f"{base}/contact-us"),
        ("contact",   f"{base}/get-in-touch"),
        ("pricing",   f"{base}/pricing"),
        ("pricing",   f"{base}/plans"),
        ("careers",   f"{base}/careers"),
        ("careers",   f"{base}/jobs"),
        ("blog",      f"{base}/blog"),
        ("blog",      f"{base}/news"),
    ]

    # Fetch all pages in parallel
    tasks = [(page_type, url, _fetch_page(url)) for page_type, url in page_candidates]
    results = await asyncio.gather(*[t[2] for t in tasks], return_exceptions=True)

    pages_fetched: dict[str, str] = {}         # page_type → text
    pages_soup: dict[str, BeautifulSoup] = {}  # page_type → soup (for contact/social extraction)

    for (page_type, url, _), result in zip(tasks, results):
        if isinstance(result, Exception):
            continue
        text, soup = result
        if text and len(text) > 20:
            if len(text) > len(pages_fetched.get(page_type, "")):
                pages_fetched[page_type] = text
                if soup is not None:
                    pages_soup[page_type] = soup
                logger.info("[WebScrape] ✓ %s (%d chars) from %s", page_type, len(text), url)

    if not pages_fetched:
        logger.warning("[WebScrape] No pages fetched for: %s", base)
        return {"website": base, "status": "unreachable", "pages_scraped": []}

    # ── Contact extraction (priority: contact page → homepage → all) ──────────
    found_phones: list[str] = []
    found_emails: list[str] = []

    # 1. tel:/mailto: from contact page (most reliable)
    for page_key in ("contact", "homepage", "about"):
        soup = pages_soup.get(page_key)
        if soup:
            _p, _e = _extract_contacts_from_soup(soup)
            found_phones = list(dict.fromkeys(found_phones + _p))[:5]
            found_emails = list(dict.fromkeys(found_emails + _e))[:5]

    # 2. tel:/mailto: from remaining pages
    for page_key, soup in pages_soup.items():
        if page_key not in ("contact", "homepage", "about"):
            _p, _e = _extract_contacts_from_soup(soup)
            found_phones = list(dict.fromkeys(found_phones + _p))[:5]
            found_emails = list(dict.fromkeys(found_emails + _e))[:5]

    # 3. Regex fallback only if structured extraction found nothing
    all_raw_text = " ".join(pages_fetched.values())
    if not found_phones:
        found_phones = _extract_phones_regex_fallback(all_raw_text)
    if not found_emails:
        found_emails = _extract_emails_regex_fallback(all_raw_text)

    # ── Social media links (from homepage first, then all pages) ─────────────
    social_links: dict[str, str] = {}
    for page_key in ("homepage", "about", "contact"):
        soup = pages_soup.get(page_key)
        if soup:
            _s = _extract_social_links(soup)
            for platform, url in _s.items():
                if platform not in social_links:
                    social_links[platform] = url

    # ── LLM call ──────────────────────────────────────────────────────────────
    combined = ""
    for page_type, text in pages_fetched.items():
        combined += f"\n\n=== {page_type.upper()} PAGE ===\n{text[:2000]}"

    prompt = f"""You are a B2B market intelligence analyst. Analyze this website content and extract ONLY factual, observable information.

Website: {base}
Pages scraped: {list(pages_fetched.keys())}

SCRAPED CONTENT:
{combined[:9000]}

Return ONLY valid JSON with these exact keys. Use null for missing data, never hallucinate:
{{
  "company_description": "2-3 sentence factual summary of what the company does",
  "product_offerings": ["list of actual products/services they offer"],
  "value_proposition": "their core value proposition in 1 sentence, taken from site copy",
  "target_customers": ["specific job roles, company types, or industries they serve"],
  "use_cases": ["specific use cases or problems they solve"],
  "business_model": "SaaS / Marketplace / Agency / Hardware / Service / Other",
  "product_category": "e.g. CRM, HR Software, Analytics Platform, Dev Tools",
  "market_positioning": "enterprise / mid-market / SMB / startup-focused / premium / budget",
  "pricing_signals": "freemium / subscription / per-seat / usage-based / custom-enterprise / not-visible",
  "pricing_tiers": [],
  "key_messaging": ["top 3-5 messaging themes or keywords used on site"],
  "integrations_mentioned": ["any third-party tools or integrations mentioned"],
  "tech_stack_clues": ["tech/frameworks visible in site content or job postings"],
  "hiring_signals": "active / moderate / limited / no-careers-page",
  "open_roles": [],
  "recent_blog_topics": [],
  "problem_solved": "1 sentence: what customer pain does this company address"
}}"""

    raw = await _call_llm([
        {"role": "system", "content": "B2B website intelligence extractor. Return ONLY valid JSON, no explanation, no markdown."},
        {"role": "user", "content": prompt},
    ], max_tokens=1400, temperature=0.15)

    if raw:
        data = _parse_json_from_llm(raw)
        if data and ("company_description" in data or "product_offerings" in data):
            data["website"]        = base
            data["status"]         = "scraped"
            data["pages_scraped"]  = list(pages_fetched.keys())
            if found_phones:
                data["phones_found"] = found_phones
            if found_emails:
                data["emails_found"] = found_emails
            if social_links:
                data["social_links"] = social_links
            logger.info(
                "[WebScrape] Done — pages=%d phones=%d emails=%d socials=%s",
                len(pages_fetched), len(found_phones), len(found_emails), list(social_links.keys()),
            )
            return data

    # ── Rule-based fallback (LLM unavailable) ─────────────────────────────────
    logger.warning("[WebScrape] LLM unavailable — using rule-based inference for: %s", base)
    all_text_lower = all_raw_text.lower()
    page_title = list(pages_fetched.values())[0][:200] if pages_fetched else ""

    bm = "Service"
    if any(k in all_text_lower for k in ["saas", "software as a service", "subscription", "per seat", "per user"]):
        bm = "SaaS"
    elif any(k in all_text_lower for k in ["marketplace", "platform", "connect buyers", "connect sellers"]):
        bm = "Marketplace"
    elif any(k in all_text_lower for k in ["agency", "consulting", "outsourcing", "staffing", "development company"]):
        bm = "Agency / Services"
    elif any(k in all_text_lower for k in ["hardware", "device", "iot", "physical"]):
        bm = "Hardware"

    cat = ""
    for kw, cat_name in [
        ("crm", "CRM"), ("sales", "Sales Software"), ("hr", "HR Software"),
        ("payroll", "Payroll"), ("inventory", "Inventory Management"),
        ("analytics", "Analytics"), ("marketing", "Marketing Software"),
        ("ecommerce", "E-Commerce"), ("erp", "ERP"), ("accounting", "Accounting"),
        ("logistics", "Logistics"), ("healthcare", "Healthcare Tech"),
        ("fintech", "FinTech"), ("edtech", "EdTech"),
        ("mobile app", "Mobile App Development"), ("software development", "Software Development"),
        ("web development", "Web Development"),
    ]:
        if kw in all_text_lower:
            cat = cat_name
            break

    hiring = "active" if "careers" in pages_fetched or "jobs" in pages_fetched else "limited"

    result = {
        "website":          base,
        "status":           "partial",
        "pages_scraped":    list(pages_fetched.keys()),
        "company_description": page_title if page_title else None,
        "business_model":   bm,
        "product_category": cat,
        "hiring_signals":   hiring,
        "value_proposition": None,
        "product_offerings": [],
        "target_customers": [],
        "use_cases":        [],
        "key_messaging":    [],
        "pricing_signals":  "not-visible",
        "open_roles":       [],
        "problem_solved":   None,
    }
    if found_phones:
        result["phones_found"] = found_phones
    if found_emails:
        result["emails_found"] = found_emails
    if social_links:
        result["social_links"] = social_links
    return result

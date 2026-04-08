"""
_brightdata.py
--------------
Bright Data API integration — profile scraping, normalization, snapshot management.

Covers:
  - _normalize_linkedin_url, _clean_bd_linkedin_url
  - _normalize_bd_profile (~490 lines: education/skills/certs/activity/recommendations)
  - fetch_profile_sync (sync scrape with OCR fallback)
  - _extract_profile_via_ocr, _llm_extract_profile_from_ocr
  - trigger_batch_snapshot, poll_snapshot
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import Optional

import httpx

from ._utils import (
    _bd_api_key,
    _bd_profile_dataset,
    _safe_int,
)
from ._clients import (
    BD_BASE,
    BD_REQUEST_TIMEOUT,
    _bd_circuit_ok,
    _bd_circuit_success,
    _bd_circuit_failure,
    _bd_rate_limit_wait,
    _bd_backoff,
)
from ._llm import _call_llm, _parse_json_from_llm

try:
    from analytics import api_usage_service as _usage
except ImportError:
    class _usage:  # type: ignore
        @staticmethod
        async def track(*args, **kwargs): pass

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# URL normalisation
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_linkedin_url(url: str) -> str:
    url = url.strip().rstrip("/")
    if not url.startswith("http"):
        url = f"https://{url}"
    url = re.sub(r"https?://[a-z]{2}\.linkedin\.com", "https://www.linkedin.com", url)
    return url


def _clean_bd_linkedin_url(url: str) -> str:
    """
    Normalise a LinkedIn URL returned by BrightData to a canonical form.
    - Country-specific domains → www.linkedin.com
    - Mixed-case paths → lowercase
    - Tracking query params stripped
    - Trailing slashes removed
    """
    if not url or not isinstance(url, str):
        return url or ""
    url = url.strip()
    if "linkedin.com/" in url.lower():
        url = url.split("?")[0]
    url = re.sub(r"https?://[a-z]{2}\.linkedin\.com", "https://www.linkedin.com", url, flags=re.IGNORECASE)
    url = url.rstrip("/")
    url = url.lower()
    return url


def _bd_headers() -> dict:
    return {"Authorization": f"Bearer {_bd_api_key()}", "Content-Type": "application/json"}


# ─────────────────────────────────────────────────────────────────────────────
# Profile normalization
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_bd_profile(raw: dict) -> dict:
    """
    Normalize a raw Bright Data person profile response to the flat dict
    shape the rest of the service expects.
    """
    p = dict(raw)

    # ── current_company nested → flat ────────────────────────────────────────
    cc = p.get("current_company") or {}
    if isinstance(cc, dict):
        if not p.get("current_company_link"):
            p["current_company_link"] = cc.get("link") or cc.get("url") or ""
        if not p.get("current_company_name"):
            p["current_company_name"] = cc.get("name") or ""
        if not p.get("current_company_logo"):
            p["current_company_logo"] = cc.get("logo") or cc.get("image") or ""
        if not p.get("current_company_company_id"):
            p["current_company_company_id"] = cc.get("company_id") or cc.get("id") or ""

    # ── Build company LinkedIn URL from company_id when link is missing ──────
    if not p.get("current_company_link") or "linkedin.com/company/" not in (p.get("current_company_link") or ""):
        co_id = p.get("current_company_company_id") or ""
        if not co_id:
            exp = p.get("experience") or []
            if exp and isinstance(exp[0], dict):
                first_exp = exp[0]
                co_id = (
                    first_exp.get("company_id") or first_exp.get("linkedin_company_id")
                    or first_exp.get("company_linkedin_id") or ""
                )
                exp_url = (
                    first_exp.get("url") or first_exp.get("company_url")
                    or first_exp.get("linkedin_url") or first_exp.get("company_linkedin_url") or ""
                )
                if exp_url and "linkedin.com/company/" in exp_url:
                    p["current_company_link"] = _clean_bd_linkedin_url(exp_url)
                    co_id = ""
        if co_id and not p.get("current_company_link"):
            p["current_company_link"] = f"https://www.linkedin.com/company/{co_id.lower()}/"

    # ── Avatar / profile photo ────────────────────────────────────────────────
    if not p.get("avatar_url"):
        p["avatar_url"] = p.get("avatar") or p.get("profile_image") or p.get("photo") or ""

    # ── Banner image ─────────────────────────────────────────────────────────
    if not p.get("banner_image"):
        p["banner_image"] = p.get("background_image") or p.get("cover_image") or ""

    # ── Education ────────────────────────────────────────────────────────────
    edu_raw = p.get("education")
    edu_str = p.get("educations_details") or p.get("education_details") or ""

    if edu_raw and isinstance(edu_raw, list):
        def _edu_year(val: str) -> str:
            if val and isinstance(val, str):
                return val[:4]
            return val or ""

        normalized_edu = []
        for e in edu_raw:
            if not isinstance(e, dict):
                continue
            school = (e.get("school") or e.get("institution") or e.get("university")
                      or e.get("name") or e.get("title") or "")
            degree = (e.get("degree") or e.get("field_of_study") or e.get("description") or "")
            if not degree and not (e.get("school") or e.get("institution") or e.get("university") or e.get("name")):
                degree = ""
            start = _edu_year(e.get("start_year") or e.get("start") or "")
            end   = _edu_year(e.get("end_year")   or e.get("end")   or "")
            years = (e.get("years") or e.get("date_range") or e.get("dates")
                     or (f"{start}–{end}".strip("–") if start or end else ""))
            normalized_edu.append({"school": school, "degree": degree, "years": years})
        p["education"] = [e for e in normalized_edu if e["school"]]
    elif not edu_raw and edu_str and isinstance(edu_str, str):
        p["education"] = [{"school": part.strip(), "degree": "", "years": ""}
                          for part in edu_str.split(";") if part.strip()]

    # ── Languages ────────────────────────────────────────────────────────────
    langs_raw = p.get("languages") or []
    if langs_raw and isinstance(langs_raw, list):
        normalized_langs = []
        for lang in langs_raw:
            if isinstance(lang, dict):
                normalized_langs.append({
                    "name":        lang.get("name") or lang.get("title") or lang.get("language") or "",
                    "proficiency": lang.get("proficiency") or lang.get("subtitle") or lang.get("level") or "",
                })
            elif isinstance(lang, str) and lang.strip():
                normalized_langs.append({"name": lang.strip(), "proficiency": ""})
        p["languages"] = [l for l in normalized_langs if l["name"]]

    # ── Certifications ───────────────────────────────────────────────────────
    certs_raw = p.get("certifications") or []
    if certs_raw and isinstance(certs_raw, list):
        normalized_certs = []
        for cert in certs_raw:
            if isinstance(cert, dict):
                normalized_certs.append({
                    "name":           cert.get("name") or cert.get("title") or "",
                    "issuer":         cert.get("issuer") or cert.get("subtitle") or cert.get("organization") or "",
                    "date":           cert.get("date") or cert.get("meta") or cert.get("issued_at") or "",
                    "credential_url": cert.get("credential_url") or cert.get("url") or "",
                    "credential_id":  cert.get("credential_id") or "",
                })
            elif isinstance(cert, str) and cert.strip():
                normalized_certs.append({"name": cert.strip(), "issuer": "", "date": "", "credential_url": "", "credential_id": ""})
        p["certifications"] = [c for c in normalized_certs if c["name"]]

    # ── Skills ───────────────────────────────────────────────────────────────
    skills = p.get("skills")

    if skills and isinstance(skills, list):
        flat = []
        for s in skills:
            if isinstance(s, dict):
                flat.append(s.get("name") or s.get("skill") or s.get("title") or "")
            elif isinstance(s, str):
                flat.append(s)
        skills = [s.strip() for s in flat if s.strip()]
        p["skills"] = skills

    if not skills:
        skills_str = p.get("skills_label") or p.get("top_skills") or ""
        if skills_str and isinstance(skills_str, str):
            skills = [s.strip() for s in skills_str.split(",") if s.strip()]
            p["skills"] = skills

    if not skills:
        inferred = []
        for cert in (p.get("certifications") or []):
            if isinstance(cert, dict):
                name = cert.get("name") or ""
                if name and len(name) < 60:
                    inferred.append(name)
        if inferred:
            p["skills"] = inferred[:6]

    if not skills:
        about_lower = (p.get("about") or "").lower()
        skill_keywords = [
            ("roofing", "Roofing"), ("construction", "Construction"), ("project management", "Project Management"),
            ("python", "Python"), ("javascript", "JavaScript"), ("react", "React"),
            ("aws", "AWS"), ("azure", "Azure"), ("machine learning", "Machine Learning"),
            ("data analysis", "Data Analysis"), ("sales", "Sales"), ("marketing", "Marketing"),
            ("leadership", "Leadership"), ("finance", "Finance"), ("accounting", "Accounting"),
            ("design", "Design"), ("product management", "Product Management"),
            ("software engineering", "Software Engineering"), ("devops", "DevOps"),
            ("cybersecurity", "Cybersecurity"), ("hr", "Human Resources"),
            ("operations", "Operations Management"), ("supply chain", "Supply Chain"),
        ]
        inferred = [label for kw, label in skill_keywords if kw in about_lower]
        if inferred:
            p["skills"] = inferred[:8]

    # ── Timezone ─────────────────────────────────────────────────────────────
    if not p.get("timezone"):
        _TZ_MAP = {
            "IN": "Asia/Kolkata",      "US": "America/New_York",  "GB": "Europe/London",
            "DE": "Europe/Berlin",     "FR": "Europe/Paris",      "AU": "Australia/Sydney",
            "CA": "America/Toronto",   "SG": "Asia/Singapore",    "AE": "Asia/Dubai",
            "JP": "Asia/Tokyo",        "CN": "Asia/Shanghai",     "BR": "America/Sao_Paulo",
            "MX": "America/Mexico_City","NL": "Europe/Amsterdam", "SE": "Europe/Stockholm",
            "NO": "Europe/Oslo",       "DK": "Europe/Copenhagen", "FI": "Europe/Helsinki",
            "PL": "Europe/Warsaw",     "IT": "Europe/Rome",       "ES": "Europe/Madrid",
            "RU": "Europe/Moscow",     "ZA": "Africa/Johannesburg","NG": "Africa/Lagos",
            "KE": "Africa/Nairobi",    "EG": "Africa/Cairo",      "PK": "Asia/Karachi",
            "BD": "Asia/Dhaka",        "LK": "Asia/Colombo",      "NP": "Asia/Kathmandu",
            "TH": "Asia/Bangkok",      "VN": "Asia/Ho_Chi_Minh",  "MY": "Asia/Kuala_Lumpur",
            "ID": "Asia/Jakarta",      "PH": "Asia/Manila",       "KR": "Asia/Seoul",
            "HK": "Asia/Hong_Kong",    "TW": "Asia/Taipei",       "IL": "Asia/Jerusalem",
            "SA": "Asia/Riyadh",       "TR": "Europe/Istanbul",   "AR": "America/Argentina/Buenos_Aires",
            "CO": "America/Bogota",    "PE": "America/Lima",      "CL": "America/Santiago",
            "NZ": "Pacific/Auckland",
        }
        p["timezone"] = _TZ_MAP.get(p.get("country_code") or p.get("country") or "", "")

    # ── Position inference ───────────────────────────────────────────────────
    if not p.get("position"):
        about_text = p.get("about") or ""
        candidate  = ""

        m = re.match(
            r"^I(?:'m| am) (?:a |an |the )(.+?)(?:\s+with\b|\s+at\b|\s+of\b|\.|,|–|-)",
            about_text, re.IGNORECASE,
        )
        if m:
            candidate = m.group(1).strip().rstrip("and").strip()

        if not candidate:
            m = re.match(r"^([A-Z][^.!?\n]{3,60}?)\s+(?:at|@)\s+[A-Z]", about_text)
            if m:
                candidate = m.group(1).strip()

        if not candidate:
            m = re.match(
                r"^([A-Z][^.!?\n]{3,60}?)\s+(?:specializ|focus|with \d|based in|helping|who )",
                about_text, re.IGNORECASE,
            )
            if m:
                candidate = m.group(1).strip()

        if not candidate and about_text:
            first_sentence = re.split(r"[.!?\n]", about_text)[0].strip()
            if 5 < len(first_sentence) < 60 and not re.search(r"\b(I |we |our |you )", first_sentence, re.IGNORECASE):
                candidate = first_sentence

        if not candidate:
            exp = p.get("experience") or []
            if isinstance(exp, list) and exp:
                first_exp = exp[0] if isinstance(exp[0], dict) else {}
                candidate = first_exp.get("title") or first_exp.get("position") or ""

        if candidate and 3 < len(candidate) < 80:
            p["position"] = candidate

        if not p.get("position"):
            p["position"] = p.get("headline") or p.get("bio") or p.get("title") or p.get("job_title") or ""

    # ── Honors & Awards → merge into certifications ──────────────────────────
    honors = p.get("honors_and_awards") or []
    if honors and isinstance(honors, list):
        existing_certs = p.get("certifications") or []
        existing_names = {c.get("name", "").lower() for c in existing_certs if isinstance(c, dict)}
        for h in honors:
            if not isinstance(h, dict):
                continue
            title = h.get("title") or ""
            if not title or title.lower() in existing_names:
                continue
            date_raw = h.get("date") or ""
            date_str = date_raw[:4] if date_raw else ""
            existing_certs.append({
                "name":           title,
                "issuer":         h.get("publication") or h.get("issuer") or "",
                "date":           date_str,
                "credential_url": h.get("url") or "",
                "credential_id":  "",
            })
        p["certifications"] = existing_certs

    # ── Posts ─────────────────────────────────────────────────────────────────
    posts = p.get("posts") or []
    if posts and isinstance(posts, list):
        p["_posts"] = [
            {
                "title":       (post.get("title") or "")[:300],
                "attribution": (post.get("attribution") or "")[:500],
                "link":        post.get("link") or "",
                "created_at":  post.get("created_at") or "",
                "interaction": post.get("interaction") or "",
                "id":          post.get("id") or "",
            }
            for post in posts if isinstance(post, dict)
        ]
        existing_ids = {a.get("id") for a in (p.get("activity") or []) if isinstance(a, dict)}
        for post in posts:
            if not isinstance(post, dict):
                continue
            if post.get("id") in existing_ids:
                continue
            synthetic = {
                "interaction": "Posted",
                "title":       (post.get("title") or "")[:300],
                "attribution": (post.get("attribution") or "")[:500],
                "link":        post.get("link") or "",
                "created_at":  post.get("created_at") or "",
                "id":          post.get("id") or "",
            }
            if not p.get("activity"):
                p["activity"] = []
            p["activity"].append(synthetic)
    else:
        p["_posts"] = []

    # ── Activity: extract emails + phones + hiring signals ───────────────────
    activity = p.get("activity") or []
    extracted_emails: list[str] = []
    extracted_phones: list[str] = []
    hiring_posts: list[str] = []
    email_re = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
    phone_re  = re.compile(r"(?<!\d)(\+?[\d][\d\s\-]{6,13}[\d])(?!\d)")

    for item in activity:
        if not isinstance(item, dict):
            continue
        text = (item.get("title") or "") + " " + (item.get("description") or "")
        for em in email_re.findall(text):
            em_lower = em.lower()
            if em_lower not in extracted_emails and "linkedin.com" not in em_lower:
                extracted_emails.append(em_lower)
        for ph in phone_re.findall(text):
            cleaned = re.sub(r"[\s\-]", "", ph)
            if 7 <= len(cleaned.lstrip("+")) <= 15 and cleaned not in extracted_phones:
                extracted_phones.append(cleaned)
        if any(kw in text.lower() for kw in ["hiring", "we're looking", "open position", "job opening", "apply"]):
            title_snip = (item.get("title") or "")[:120]
            if title_snip:
                hiring_posts.append(title_snip)

    if extracted_emails:
        p["_activity_emails"] = extracted_emails
        logger.info("[BDNorm] Activity emails found: %s", extracted_emails)
    if extracted_phones:
        p["_activity_phones"] = extracted_phones
        logger.info("[BDNorm] Activity phones found: %s", extracted_phones)
    if hiring_posts:
        p["_hiring_signals"] = hiring_posts[:10]

    # ── Recommendations ───────────────────────────────────────────────────────
    recs = (
        p.get("recommendations")
        or p.get("received_recommendations")
        or p.get("testimonials")
        or []
    )
    if recs and isinstance(recs, list):
        def _rec_text(r):
            if isinstance(r, str):
                return r[:300]
            if isinstance(r, dict):
                return (
                    r.get("text") or r.get("description") or r.get("recommendation_text")
                    or r.get("content") or ""
                )[:300]
            return ""

        p["recommendations"] = [
            {
                "recommender_name":     (r.get("recommender_name") or r.get("author_name") or r.get("name") or "") if isinstance(r, dict) else "",
                "recommender_headline": (r.get("recommender_headline") or r.get("author_headline") or r.get("title") or "") if isinstance(r, dict) else "",
                "recommender_url":      (r.get("recommender_url") or r.get("profile_link") or r.get("url") or "") if isinstance(r, dict) else "",
                "recommender_image":    (r.get("recommender_image") or r.get("profile_image_url") or r.get("image") or "") if isinstance(r, dict) else "",
                "text":                 _rec_text(r),
                "date":                 (r.get("date") or r.get("recommendation_date") or "") if isinstance(r, dict) else "",
                "relationship":         (r.get("relationship") or r.get("connection_type") or "") if isinstance(r, dict) else "",
            }
            for r in recs if r
        ]
        p["recommendations_text"] = " | ".join(
            r["text"] for r in p["recommendations"] if r["text"]
        )[:600]
        if not p.get("recommendations_count"):
            p["recommendations_count"] = len(p["recommendations"])
    else:
        if not p.get("recommendations"):
            p["recommendations"] = []
        p["recommendations_count"] = _safe_int(p.get("recommendations_count"))

    # ── People also viewed ───────────────────────────────────────────────────
    pav = p.get("people_also_viewed") or p.get("similar_profiles") or []
    if pav and isinstance(pav, list):
        p["_similar_profiles"] = [
            {
                "name":     x.get("name", ""),
                "title":    x.get("title") or x.get("headline") or x.get("position") or "",
                "link":     x.get("profile_link") or x.get("url") or x.get("link") or "",
                "image":    x.get("profile_image_url") or x.get("image") or x.get("profile_image") or "",
                "location": x.get("location", ""),
                "degree":   x.get("degree") or x.get("connection_degree") or "",
            }
            for x in pav if isinstance(x, dict)
        ]

    # ── Activity: store full list for UI ─────────────────────────────────────
    if activity:
        p["_activity_full"] = [
            {
                "interaction": item.get("interaction", ""),
                "title":       (item.get("title") or "")[:300],
                "attribution": (item.get("attribution") or "")[:500],
                "link":        item.get("link", ""),
                "created_at":  item.get("created_at", ""),
                "id":          item.get("id", ""),
                "type":        "post" if (item.get("interaction") or "").lower().startswith("post") else "activity",
            }
            for item in activity if isinstance(item, dict)
        ]

    # ── Profile flags ─────────────────────────────────────────────────────────
    p["_influencer"]           = bool(p.get("influencer"))
    p["_memorialized_account"] = bool(p.get("memorialized_account"))
    p["_bio_links"]            = p.get("bio_links") or []
    p["_bd_scrape_timestamp"]  = p.get("timestamp") or ""

    # ── Followers / connections ───────────────────────────────────────────────
    p["followers"]   = _safe_int(p.get("followers"))
    p["connections"] = _safe_int(p.get("connections"))

    # ── Name split ────────────────────────────────────────────────────────────
    if not p.get("name") and p.get("full_name"):
        p["name"] = p["full_name"]
    if not p.get("first_name") and p.get("name"):
        parts = p["name"].split()
        p["first_name"] = parts[0] if parts else ""
        p["last_name"] = " ".join(parts[1:]) if len(parts) > 1 else ""

    # ── Position: final safety net ────────────────────────────────────────────
    if not p.get("position"):
        exp = p.get("experience") or []
        exp_title = ""
        if isinstance(exp, list) and exp and isinstance(exp[0], dict):
            exp_title = exp[0].get("title") or exp[0].get("position") or ""
        p["position"] = (
            p.get("headline") or p.get("bio") or p.get("job_title")
            or exp_title or ""
        )
    if not p.get("position"):
        p["_needs_title_inference"] = True

    # ── Location ─────────────────────────────────────────────────────────────
    city_raw = p.get("city") or p.get("location") or ""
    if city_raw and not p.get("location"):
        p["location"] = city_raw
    if p.get("city") and "," in p["city"]:
        p["city"] = p["city"].split(",")[0].strip()
    if not p.get("country"):
        p["country"] = p.get("country_code", "")

    # ── LinkedIn URL normalization ────────────────────────────────────────────
    for url_field in ("url", "input_url", "linkedin_url"):
        v = p.get(url_field) or ""
        if v and "linkedin.com" in v.lower():
            p[url_field] = _clean_bd_linkedin_url(v)

    if p.get("current_company_link") and "linkedin.com" in p["current_company_link"].lower():
        p["current_company_link"] = _clean_bd_linkedin_url(p["current_company_link"])

    return p


# ─────────────────────────────────────────────────────────────────────────────
# Sync scrape (with OCR fallback)
# ─────────────────────────────────────────────────────────────────────────────

async def fetch_profile_sync(linkedin_url: str) -> dict:
    """
    Primary: Bright Data sync /scrape endpoint → normalize with _normalize_bd_profile.
    Handles rate limits (429), circuit breaker, and falls back to OCR on failure.
    """
    url = _normalize_linkedin_url(linkedin_url)

    if _bd_api_key():
        if not _bd_circuit_ok():
            logger.warning("[BrightData] Circuit breaker OPEN — skipping BD, using OCR fallback")
            return await _extract_profile_via_ocr(url)

        logger.info("[BrightData] Sync scrape: %s", url)
        try:
            async with httpx.AsyncClient(timeout=BD_REQUEST_TIMEOUT) as client:
                resp = await client.post(
                    f"{BD_BASE}/scrape",
                    params={"dataset_id": _bd_profile_dataset(), "format": "json"},
                    headers=_bd_headers(),
                    json=[{"url": url}],
                )
                await _usage.track("brightdata", "profile")

                if resp.status_code == 429:
                    wait = _bd_rate_limit_wait(resp)
                    logger.warning("[BrightData] Sync rate-limited — waiting %.1fs before OCR fallback", wait)
                    _bd_circuit_failure()
                    await asyncio.sleep(wait)
                    return await _extract_profile_via_ocr(url)

                if resp.status_code in (400, 401, 402, 403):
                    err_text = resp.text[:200]
                    logger.error("[BrightData] Account/auth error %s: %s — aborting", resp.status_code, err_text)
                    _bd_circuit_failure()
                    return {"_bd_error": True, "_bd_status": resp.status_code, "_bd_message": err_text}

                if resp.status_code >= 500:
                    logger.warning("[BrightData] Server error %s — trying OCR fallback", resp.status_code)
                    _bd_circuit_failure()
                    return await _extract_profile_via_ocr(url)

                if resp.status_code == 200:
                    data = resp.json()

                    if isinstance(data, dict) and data.get("snapshot_id") and not data.get("name"):
                        snap_id = data["snapshot_id"]
                        logger.info("[BrightData] Sync returned snapshot_id=%s — polling async", snap_id)
                        try:
                            rows = await poll_snapshot(snap_id, interval=5, timeout=120)
                            data = rows
                        except Exception as poll_err:
                            logger.warning("[BrightData] Snapshot poll failed (%s) — trying OCR fallback", poll_err)
                            data = []

                    raw = data[0] if isinstance(data, list) and data else data
                    if isinstance(raw, dict) and (
                        raw.get("name") or raw.get("full_name")
                        or raw.get("first_name") or raw.get("linkedin_url")
                    ):
                        if not raw.get("name") and raw.get("full_name"):
                            raw["name"] = raw["full_name"]
                        profile = _normalize_bd_profile(raw)
                        _bd_circuit_success()
                        logger.info("[BrightData] OK — %s | company=%s | avatar=%s | activity=%d",
                                    profile.get("name"),
                                    profile.get("current_company_name") or "?",
                                    "✓" if profile.get("avatar_url") else "✗",
                                    len(raw.get("activity") or []))
                        return profile

                    logger.warning("[BrightData] Empty profile — trying OCR fallback")
                else:
                    logger.warning("[BrightData] Unexpected %s — trying OCR fallback", resp.status_code)

        except (httpx.TimeoutException, httpx.ConnectError) as e:
            logger.warning("[BrightData] Network error (%s) — trying OCR fallback", e)
            _bd_circuit_failure()
        except Exception as e:
            logger.warning("[BrightData] Error: %s — trying OCR fallback", e)
    else:
        logger.info("[BrightData] No API key — using OCR fallback")

    return await _extract_profile_via_ocr(url)


async def _extract_profile_via_ocr(linkedin_url: str) -> dict:
    """OCR fallback — Playwright not available in production."""
    logger.warning("[OCR] OCR fallback not available — returning empty profile for %s", linkedin_url)
    return {"url": linkedin_url}


async def _llm_extract_profile_from_ocr(linkedin_url: str, ocr_text: str) -> dict:
    snippet = ocr_text[:4500]
    prompt = f"""Extract LinkedIn profile data from this OCR text.
LinkedIn URL: {linkedin_url}

OCR TEXT:
---
{snippet}
---

Return ONLY valid JSON with these exact keys (use empty string or 0 if not found):
{{
  "name": "", "first_name": "", "last_name": "",
  "position": "", "current_company_name": "",
  "location": "", "city": "", "country": "",
  "about": "",
  "followers": 0, "connections": 0,
  "skills": [],
  "experience": [{{"title":"","company":"","duration":""}}],
  "education": [{{"school":"","degree":"","years":""}}],
  "certifications": [], "languages": []
}}"""

    raw = await _call_llm([
        {"role": "system", "content": "Precise JSON extractor. Return only valid JSON."},
        {"role": "user", "content": prompt},
    ], max_tokens=900, temperature=0)

    if raw:
        result = _parse_json_from_llm(raw)
        if result:
            return result

    lines = [l.strip() for l in ocr_text.split("\n") if l.strip() and len(l.strip()) > 3]
    return {"name": lines[0] if lines else "", "about": ocr_text[:500]}


# ─────────────────────────────────────────────────────────────────────────────
# Batch snapshot trigger & poll
# ─────────────────────────────────────────────────────────────────────────────

async def trigger_batch_snapshot(
    urls: list[str],
    webhook_url: Optional[str] = None,
    notify_url: Optional[str] = None,
    webhook_auth: Optional[str] = None,
) -> str:
    if not _bd_api_key():
        raise ValueError("BRIGHT_DATA_API_KEY not configured")
    if not _bd_circuit_ok():
        raise RuntimeError("BD circuit breaker OPEN — skipping trigger")

    params: dict[str, str] = {
        "dataset_id": _bd_profile_dataset(), "format": "json", "include_errors": "true",
    }
    if webhook_url:
        params["endpoint"] = webhook_url; params["uncompressed_webhook"] = "true"
    if notify_url:
        params["notify"] = notify_url
    if webhook_auth:
        params["auth_header"] = webhook_auth

    payload = [{"url": _normalize_linkedin_url(u)} for u in urls]
    last_exc: Exception = RuntimeError("No attempts made")

    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=60) as client:
                resp = await client.post(
                    f"{BD_BASE}/trigger", params=params,
                    headers=_bd_headers(), json=payload,
                )
            if resp.status_code == 429:
                wait = _bd_rate_limit_wait(resp)
                logger.warning("[BD] trigger rate-limited — waiting %.1fs (attempt %d/3)", wait, attempt + 1)
                _bd_circuit_failure()
                await asyncio.sleep(wait)
                continue
            if resp.status_code >= 500:
                wait = _bd_backoff(attempt)
                logger.warning("[BD] trigger %d — retrying in %.1fs (attempt %d/3)", resp.status_code, wait, attempt + 1)
                _bd_circuit_failure()
                await asyncio.sleep(wait)
                continue
            resp.raise_for_status()
            snapshot_id = resp.json().get("snapshot_id") or resp.json().get("id")
            if not snapshot_id:
                raise ValueError(f"No snapshot_id in response: {resp.text[:200]}")
            _bd_circuit_success()
            # Track one "profile" call per URL submitted
            try:
                for _ in urls:
                    await _usage.track("brightdata", "profile")
            except Exception:
                pass
            return snapshot_id
        except (httpx.TimeoutException, httpx.ConnectError) as e:
            wait = _bd_backoff(attempt)
            logger.warning("[BD] trigger network error (%s) — retrying in %.1fs (attempt %d/3)", e, wait, attempt + 1)
            _bd_circuit_failure()
            last_exc = e
            await asyncio.sleep(wait)

    raise RuntimeError(f"trigger_batch_snapshot failed after 3 attempts: {last_exc}") from last_exc


async def poll_snapshot(snapshot_id: str, interval: int = 15, timeout: int = 600) -> list[dict]:
    """
    Poll BrightData until snapshot is ready, with exponential backoff on errors,
    rate-limit handling, partial-response filtering, and circuit-breaker integration.
    """
    if not _bd_api_key():
        raise ValueError("BRIGHT_DATA_API_KEY not configured")
    if not _bd_circuit_ok():
        raise RuntimeError("BD circuit breaker OPEN — skipping poll")

    deadline    = time.time() + timeout
    poll_errors = 0
    MAX_POLL_ERRORS = 5

    async with httpx.AsyncClient(timeout=60) as client:
        while time.time() < deadline:
            try:
                s = await client.get(f"{BD_BASE}/progress/{snapshot_id}", headers=_bd_headers())
            except (httpx.TimeoutException, httpx.ConnectError) as e:
                poll_errors += 1
                wait = _bd_backoff(poll_errors - 1)
                logger.warning("[BD] poll network error (%s) — waiting %.1fs (%d/%d)", e, wait, poll_errors, MAX_POLL_ERRORS)
                _bd_circuit_failure()
                if poll_errors >= MAX_POLL_ERRORS:
                    raise RuntimeError(f"poll_snapshot {snapshot_id}: too many network errors") from e
                await asyncio.sleep(wait)
                continue

            if s.status_code == 429:
                wait = _bd_rate_limit_wait(s)
                logger.warning("[BD] poll rate-limited — waiting %.1fs", wait)
                await asyncio.sleep(wait)
                continue

            if s.status_code >= 500:
                poll_errors += 1
                wait = _bd_backoff(poll_errors - 1)
                logger.warning("[BD] poll %d — waiting %.1fs (%d/%d)", s.status_code, wait, poll_errors, MAX_POLL_ERRORS)
                _bd_circuit_failure()
                if poll_errors >= MAX_POLL_ERRORS:
                    raise RuntimeError(f"poll_snapshot {snapshot_id}: too many server errors")
                await asyncio.sleep(wait)
                continue

            s.raise_for_status()
            poll_errors = 0
            _bd_circuit_success()

            state = s.json().get("status") or s.json().get("state", "")

            if state == "ready":
                try:
                    d = await client.get(
                        f"{BD_BASE}/snapshot/{snapshot_id}",
                        params={"format": "json"}, headers=_bd_headers(),
                    )
                except (httpx.TimeoutException, httpx.ConnectError) as e:
                    logger.warning("[BD] snapshot download error (%s) — retrying poll", e)
                    _bd_circuit_failure()
                    await asyncio.sleep(_bd_backoff(0))
                    continue

                if d.status_code == 429:
                    wait = _bd_rate_limit_wait(d)
                    logger.warning("[BD] snapshot download rate-limited — waiting %.1fs", wait)
                    await asyncio.sleep(wait)
                    continue

                d.raise_for_status()
                _bd_circuit_success()

                raw = d.json()
                rows: list[dict] = raw if isinstance(raw, list) else [raw]

                ok_rows  = [r for r in rows if isinstance(r, dict) and not r.get("error") and not r.get("_error")]
                err_rows = [r for r in rows if r not in ok_rows]
                if err_rows:
                    logger.warning("[BD] snapshot %s: %d/%d items had errors: %s",
                                   snapshot_id, len(err_rows), len(rows),
                                   [r.get("error") or r.get("_error") for r in err_rows[:3]])
                if not ok_rows and err_rows:
                    logger.warning("[BD] snapshot %s: all items errored — returning to pipeline for per-lead retry", snapshot_id)
                    return err_rows

                return ok_rows if ok_rows else rows

            if state in ("failed", "error"):
                _bd_circuit_failure()
                raise RuntimeError(f"Snapshot {snapshot_id} failed (state={state})")

            elapsed = time.time() - (deadline - timeout)
            wait = min(interval + elapsed * 0.05, interval * 3)
            await asyncio.sleep(wait)

    raise TimeoutError(f"Snapshot {snapshot_id} not ready after {timeout}s")

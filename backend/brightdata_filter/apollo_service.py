"""
apollo_service.py
-----------------
Apollo People Search — POST /api/v1/mixed_people/api_search.

Flow:
  1. Call Apollo search with filter params
  2. Map each result to enriched_leads shape
  3. Upsert into enriched_leads (enrichment_source = 'apollo_search')

Note: Apollo search returns partially-obfuscated data (last name masked).
      Records are saved as-is; a separate bulk_match call can enrich further.

Public API:
  search_and_save(filters, org_id, per_page, page) -> dict
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

import httpx

from Lead_enrichment.bulk_lead_enrichment._utils import _apollo_api_key, _apollo_master_api_key, _apollo_webhook_base, _safe_int
from Lead_enrichment.bulk_lead_enrichment._db import _upsert_lead

logger = logging.getLogger(__name__)

# Apollo People API Search — optimised for API use, no credit cost.
# URL: api/v1/mixed_people/api_search (note: "api/" prefix, NOT just "v1/")
# Auth: requires a MASTER API key (Settings → Integrations → API → Create Master Key).
#       A regular API key returns 403 regardless of plan.
APOLLO_SEARCH_URL = "https://api.apollo.io/api/v1/mixed_people/api_search"
APOLLO_ENRICH_URL = "https://api.apollo.io/api/v1/people/match"

DEFAULT_PER_PAGE = 20
MAX_PER_PAGE     = 100

# Seniority options Apollo accepts
SENIORITY_OPTIONS = [
    "owner", "founder", "c_suite", "partner", "vp", "head",
    "director", "manager", "senior", "entry", "intern",
]


# ─────────────────────────────────────────────────────────────────────────────
# ID helper — Apollo records don't have LinkedIn URLs so we generate a stable
# ID from the Apollo person ID to avoid collisions with real LinkedIn leads.
# ─────────────────────────────────────────────────────────────────────────────

def _apollo_lead_id(apollo_id: str) -> str:
    return hashlib.md5(f"apollo:{apollo_id}".encode()).hexdigest()[:16]


def _apollo_ref_url(apollo_id: str) -> str:
    """A stable reference URL stored as linkedin_url for Apollo-sourced records."""
    return f"https://app.apollo.io/#/people/{apollo_id}"


# ─────────────────────────────────────────────────────────────────────────────
# Mapper
# ─────────────────────────────────────────────────────────────────────────────

def _map_apollo_person(person: dict, org_id: str) -> Optional[dict]:
    """
    Map one Apollo search result to the enriched_leads column shape.
    Returns None if there is no Apollo ID to key on.
    """
    apollo_id = person.get("id") or ""
    if not apollo_id:
        return None

    org  = person.get("organization") or {}
    now  = datetime.now(timezone.utc).isoformat()

    first_name = person.get("first_name") or ""
    # Apollo obfuscates last names in search — store as-is ("Po***r")
    last_name  = person.get("last_name") or person.get("last_name_obfuscated") or ""
    name       = f"{first_name} {last_name}".strip()
    title      = person.get("title") or person.get("headline") or ""

    return {
        "id":                _apollo_lead_id(apollo_id),
        # Apollo records use a reference URL — not a real LinkedIn URL
        "linkedin_url":      _apollo_ref_url(apollo_id),
        "organization_id":   org_id,
        "status":            "apollo_search",
        # Identity
        "name":              name,
        "first_name":        first_name,
        "last_name":         last_name,
        "avatar_url":        person.get("photo_url") or "",
        # Professional
        "title":             title,
        "company":           org.get("name") or person.get("organization_name") or "",
        # Location
        "city":              person.get("city") or "",
        "country":           person.get("country") or "",
        # Contact (Apollo withholds actual values in search results)
        "work_email":        person.get("email") or "",
        "direct_phone":      "",
        # Company context
        "industry":          org.get("industry") or "",
        "employee_count":    _safe_int(org.get("num_employees") or org.get("employee_count") or 0),
        # Raw — stored in apollo_raw (text column)
        "apollo_raw":        json.dumps({"person": person, "organization": org}, default=str),
        # Timestamps
        "enriched_at":       now,
        "created_at":        now,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Enrichment helper (credits mode)
# ─────────────────────────────────────────────────────────────────────────────

async def _enrich_person(apollo_id: str, api_key: str, hint: Optional[dict] = None) -> tuple[Optional[dict], str]:
    """
    Call Apollo people/match to get the full profile for a given apollo_id.
    Costs credits.

    Pass `hint` (the raw search-result person dict) to send additional
    matching fields alongside the id — Apollo uses them as tiebreakers
    when the ID alone doesn't resolve.

    Returns (person_dict, error_reason):
      - (dict, "")        → success, person_dict has full data
      - (None, reason)    → failed, reason explains why
    """
    h = hint or {}

    webhook_base = _apollo_webhook_base()
    payload: dict[str, Any] = {
        "api_key":                api_key,
        "id":                     apollo_id,
        "reveal_personal_emails": True,
    }

    # Phone reveal is async — Apollo needs a webhook URL to POST the result back.
    # Only enable when APOLLO_WEBHOOK_BASE_URL is configured in backend/.env.
    if webhook_base:
        payload["reveal_phone_number"] = True
        payload["webhook_url"] = f"{webhook_base}/api/bd-filter/apollo/phone-webhook"
        logger.info("[ApolloEnrich] Phone reveal enabled — webhook=%s", payload["webhook_url"])

    # Extra matching hints — help Apollo resolve when ID alone isn't enough
    if h.get("first_name"):
        payload["first_name"] = h["first_name"]
    if h.get("last_name"):
        # Even obfuscated last names help narrow the match
        payload["last_name"] = h["last_name"]
    if h.get("linkedin_url"):
        payload["linkedin_url"] = h["linkedin_url"]
    if h.get("email"):
        payload["email"] = h["email"]
    org = h.get("organization") or {}
    if org.get("name"):
        payload["organization_name"] = org["name"]

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            APOLLO_ENRICH_URL,
            headers={
                "Content-Type":  "application/json",
                "Cache-Control": "no-cache",
                "X-Api-Key":     api_key,
            },
            json=payload,
        )

    logger.info(
        "[ApolloEnrich] people/match  id=%s  status=%d  body=%s",
        apollo_id, resp.status_code, resp.text[:500],
    )

    if resp.status_code != 200:
        return None, f"HTTP {resp.status_code}: {resp.text[:150]}"

    data = resp.json()

    # Apollo sometimes returns 200 with error info or null person
    error_code = data.get("error_code") or data.get("error")
    if error_code:
        return None, f"Apollo error: {error_code} — {data.get('error_message', '')}"

    person = data.get("person")
    if not person:
        return None, "people/match returned null person (ID not found or no credits)"

    logger.info(
        "[ApolloEnrich] SUCCESS  id=%s  name=%s %s  email=%s  phone_count=%d  linkedin=%s",
        apollo_id,
        person.get("first_name", ""),
        person.get("last_name", ""),
        person.get("email", "—"),
        len(person.get("phone_numbers") or []),
        person.get("linkedin_url", "—"),
    )
    return person, ""


def _map_enriched_person(person: dict, org_id: str) -> Optional[dict]:
    """
    Map a full Apollo people/match response to the enriched_leads column shape.
    This has real last name, email, phone, and linkedin_url.
    """
    apollo_id = person.get("id") or ""
    if not apollo_id:
        return None

    org  = person.get("organization") or {}
    now  = datetime.now(timezone.utc).isoformat()

    first_name = person.get("first_name") or ""
    last_name  = person.get("last_name") or ""
    name       = f"{first_name} {last_name}".strip()
    title      = person.get("title") or person.get("headline") or ""

    # Real LinkedIn URL when available
    linkedin_url = person.get("linkedin_url") or _apollo_ref_url(apollo_id)

    # Best email — prefer work email
    email = (
        person.get("email")
        or person.get("work_email")
        or ""
    )

    # Phone — first revealed personal or work number
    phones     = person.get("phone_numbers") or []
    direct_phone = phones[0].get("sanitized_number", "") if phones else ""

    # Location
    city    = person.get("city") or ""
    state   = person.get("state") or ""
    country = person.get("country") or ""
    location_parts = [p for p in [city, state, country] if p]

    return {
        "id":                _apollo_lead_id(apollo_id),
        "linkedin_url":      linkedin_url,
        "organization_id":   org_id,
        "status":            "apollo_enriched",
        # Identity
        "name":              name,
        "first_name":        first_name,
        "last_name":         last_name,
        "avatar_url":        person.get("photo_url") or "",
        # Professional
        "title":             title,
        "company":           org.get("name") or person.get("organization_name") or "",
        "company_linkedin":  org.get("linkedin_url") or "",
        "company_logo":      org.get("logo_url") or "",
        # Location
        "city":              city,
        "country":           country,
        # Contact (real values from enrichment)
        "work_email":        email,
        "direct_phone":      direct_phone,
        # Company context
        "industry":          org.get("industry") or "",
        "employee_count":    _safe_int(org.get("num_employees") or org.get("employee_count") or 0),
        # Raw
        "apollo_raw":        json.dumps({"person": person, "organization": org}, default=str),
        # Timestamps
        "enriched_at":       now,
        "created_at":        now,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

async def search_and_save(
    filters: dict,
    org_id:  str = "default",
    per_page: int = DEFAULT_PER_PAGE,
    page:     int = 1,
    enrich:   bool = False,
) -> dict:
    """
    Search Apollo People and upsert results into enriched_leads.

    Args:
        filters:  Apollo filter fields (all optional, combine as needed):
                  {
                    "person_titles":        ["cto", "chief technology officer"],
                    "person_locations":     ["London, GB", "Manchester, GB"],
                    "organization_names":   ["Stripe", "Shopify"],
                    "organization_locations": ["California, US"],
                    "q_keywords":           "machine learning saas",
                    "person_seniorities":   ["c_suite", "vp", "director"],
                    "contact_email_status": ["verified"],
                  }
        org_id:   Organisation ID for saved records
        per_page: Results per page (1–100)
        page:     Page number (1-based)
        enrich:   When True, call people/match for each result to get full profile
                  (costs credits). When False, use free search only.

    Returns:
        {
          "total_entries": int,   # Apollo total matches
          "page":          int,
          "per_page":      int,
          "enrich_mode":   bool,  # whether enrichment was requested
          "saved":         int,
          "skipped":       int,
          "failed":        int,
          "results":       list[dict],
        }
    """
    per_page        = min(max(1, per_page), MAX_PER_PAGE)
    api_key         = _apollo_master_api_key()   # free search — master key
    enrich_api_key  = _apollo_api_key()          # credits enrichment — regular key
    if not api_key:
        raise RuntimeError("APOLLO_API_KEY_MASTER is not configured in backend/.env")
    if enrich and not enrich_api_key:
        raise RuntimeError("APOLLO_API_KEY is not configured in backend/.env (needed for Credits mode)")

    # Build request body — only include filters that have values
    # Apollo v1 requires api_key in the body (not just the X-Api-Key header)
    body: dict[str, Any] = {"api_key": api_key, "per_page": per_page, "page": page}

    # Array filters
    for key in ("person_titles", "person_locations", "organization_names",
                "organization_locations", "person_seniorities", "contact_email_status"):
        val = filters.get(key)
        if val:
            body[key] = val if isinstance(val, list) else [val]

    # Scalar filter
    if filters.get("q_keywords"):
        body["q_keywords"] = filters["q_keywords"].strip()

    logger.info(
        "[ApolloSearch] Calling mixed_people/search  body=%s  org=%s  enrich=%s",
        json.dumps(body), org_id, enrich,
    )

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            APOLLO_SEARCH_URL,
            headers={
                "Content-Type": "application/json",
                "Cache-Control": "no-cache",
                "X-Api-Key":     api_key,
            },
            json=body,
        )

    if resp.status_code == 403:
        body_text = resp.text[:400]
        logger.error("[ApolloSearch] 403 Forbidden: %s", body_text)
        raise RuntimeError(
            "APOLLO_MASTER_KEY_REQUIRED: "
            "The Apollo People Search endpoint requires a Master API key, not a regular key. "
            "Go to Apollo → Settings → Integrations → API → Create Master API Key, "
            "then update APOLLO_API_KEY in backend/.env."
        )

    if resp.status_code != 200:
        logger.error("[ApolloSearch] API error %d: %s", resp.status_code, resp.text[:300])
        raise RuntimeError(
            f"Apollo API returned {resp.status_code}: {resp.text[:200]}"
        )

    data = resp.json()

    # `mixed_people/search` nests total in `pagination`, while `api_search` put it top-level.
    # Handle both shapes for robustness.
    pagination   = data.get("pagination") or {}
    total        = (
        data.get("total_entries")
        or pagination.get("total_entries")
        or 0
    )
    raw_people   = data.get("people") or []

    logger.info(
        "[ApolloSearch] total_entries=%d  returned=%d  page=%d",
        total, len(raw_people), page,
    )

    saved:           list[dict] = []
    skipped:         int = 0
    failed:          int = 0
    enrich_ok:       int = 0
    enrich_failed:   int = 0
    enrich_failures: list[str] = []

    for person in raw_people:
        try:
            apollo_id = person.get("id") or ""

            if enrich and apollo_id:
                # Credits mode — fetch full profile via people/match
                enriched, reason = await _enrich_person(apollo_id, enrich_api_key, hint=person)
                if enriched:
                    lead = _map_enriched_person(enriched, org_id)
                    enrich_ok += 1
                else:
                    enrich_failed += 1
                    enrich_failures.append(f"id={apollo_id}: {reason}")
                    logger.warning(
                        "[ApolloSearch] Enrich failed for id=%s reason=%s — saving free data instead",
                        apollo_id, reason,
                    )
                    lead = _map_apollo_person(person, org_id)
                    if lead:
                        lead["status"] = "apollo_search"
            else:
                lead = _map_apollo_person(person, org_id)

            if lead is None:
                skipped += 1
                continue
            await _upsert_lead(lead)
            saved.append(lead)
        except Exception as exc:
            failed += 1
            logger.error("[ApolloSearch] Save error: %s  person_id=%s", exc, person.get("id"))

    logger.info(
        "[ApolloSearch] Done  total=%d  saved=%d  skipped=%d  failed=%d"
        "  enrich_ok=%d  enrich_failed=%d  enrich=%s",
        total, len(saved), skipped, failed, enrich_ok, enrich_failed, enrich,
    )
    if enrich_failures:
        logger.warning("[ApolloSearch] Enrich failure details: %s", enrich_failures)

    return {
        "total_entries":   total,
        "page":            page,
        "per_page":        per_page,
        "enrich_mode":     enrich,
        "saved":           len(saved),
        "skipped":         skipped,
        "failed":          failed,
        "enrich_ok":       enrich_ok,
        "enrich_failed":   enrich_failed,
        "enrich_failures": enrich_failures,
        "results":         saved,
    }

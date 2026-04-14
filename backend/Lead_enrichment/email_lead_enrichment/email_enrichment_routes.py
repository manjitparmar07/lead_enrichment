"""
email_enrichment_routes.py — Single & Bulk Lead Email Enrichment
-----------------------------------------------------------------
Both endpoints share the same flow via _find_email_for_lead():

  1. If email already in DB → return cached
  2. Extract first_name, last_name, domain from lead record
  3. Call Apollo.io → if found, verify via ValidEmail.net → save + return
  4. If Apollo misses → generate 3 pattern guesses → verify each via ValidEmail.net
     → save best guess → return all guesses

Endpoints:
  POST /api/leads/view/email          — single lead_id (lead must exist in DB)
  POST /api/leads/email/bulk          — list of lead_ids (up to 500), waits for all, returns together
  POST /api/leads/email/bulk/stream   — same as bulk but streams each result as SSE the moment it finishes
  POST /api/leads/email/find          — no lead_id needed; pass name + company/domain directly (manual/LIO entries)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from typing import List, Union
from urllib.parse import urlparse

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

try:
    from analytics import api_usage_service as _usage
except ImportError:
    class _usage:  # type: ignore
        @staticmethod
        async def track(*args, **kwargs): pass

import Lead_enrichment.bulk_lead_enrichment.lead_enrichment_brightdata_service as svc
from db import get_pool

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/leads", tags=["Email Enrichment"])

# Concurrency cap for bulk — avoids hammering Apollo/ValidEmail at once
_BULK_EMAIL_SEM = asyncio.Semaphore(10)

VALIDEMAIL_TOKEN = os.getenv("VALIDEMAIL_TOKEN", "")
VALIDEMAIL_URL   = "https://api.ValidEmail.net/"


# ── Request models ─────────────────────────────────────────────────────────────

class ViewEmailRequest(BaseModel):
    """
    Unified: pass leadenrich_id to look up from DB,
    OR pass first_name + domain/linkedin_url directly (manual/LIO entries).
    """
    leadenrich_id: str = ""       # lead in enriched_leads table
    first_name: str = ""          # used when no leadenrich_id
    last_name: str = ""
    company_website: str = ""     # e.g. "acme.com" or "https://www.acme.com"
    domain: str = ""              # overrides company_website extraction
    linkedin_url: str = ""
    company: str = ""
    force_refresh: bool = False


class DirectLeadInput(BaseModel):
    """One entry in a bulk request that has no lead_id — same as the direct fields on ViewEmailRequest."""
    first_name: str
    last_name: str = ""
    company_website: str = ""
    domain: str = ""
    linkedin_url: str = ""
    company: str = ""
    ref: str = ""   # optional caller-assigned ID echoed back in the result so you can match rows


class BulkEmailRequest(BaseModel):
    lead_ids: List[Union[str, DirectLeadInput]] = []  # string = DB lead_id, object = direct fields
    force_refresh: bool = False


# ── Shared helpers ─────────────────────────────────────────────────────────────

def _normalize_name(s: str) -> str:
    _char_map = {
        'æ': 'ae', 'ø': 'o', 'å': 'aa', 'ö': 'oe', 'ü': 'ue', 'ä': 'ae',
        'é': 'e',  'è': 'e', 'ê': 'e',  'ë': 'e',  'ñ': 'n',  'ç': 'c',
        'í': 'i',  'ì': 'i', 'î': 'i',  'ï': 'i',  'ó': 'o',  'ò': 'o',
        'ô': 'o',  'ú': 'u', 'ù': 'u',  'û': 'u',  'ý': 'y',  'ß': 'ss',
    }
    return ''.join(_char_map.get(c.lower(), c) for c in s)


def _extract_domain(url: str) -> str:
    if not url:
        return ""
    try:
        netloc = urlparse(url if url.startswith("http") else f"https://{url}").netloc
        return netloc.replace("www.", "").split(":")[0].lower()
    except Exception:
        return ""


async def _verify_email(email: str) -> dict:
    """
    Verify email via ValidEmail.net.
    Returns: { verified: bool, bounce_risk: str | None }
    """
    token = VALIDEMAIL_TOKEN or os.getenv("VALIDEMAIL_TOKEN", "")
    if not token or not email:
        return {"verified": False, "bounce_risk": None}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(VALIDEMAIL_URL, params={"email": email, "token": token})
        if resp.status_code != 200:
            logger.warning("[ValidEmail] HTTP %s for %s", resp.status_code, email)
            return {"verified": False, "bounce_risk": None}
        data = resp.json()
        if "Message" in data or "message" in data:
            logger.warning("[ValidEmail] API message for %s: %s", email,
                           data.get("Message") or data.get("message"))
            return {"verified": False, "bounce_risk": None}
        valid      = bool(data.get("valid", False))
        result     = (data.get("result") or "").lower()
        disposable = bool(data.get("disposable", False))
        if not valid or result == "invalid":
            bounce_risk = "high"
        elif result == "catch-all":
            bounce_risk = "medium"
        elif disposable:
            bounce_risk = "high"
        else:
            bounce_risk = "low"
        logger.info("[ValidEmail] %s → valid=%s result=%s bounce_risk=%s",
                    email, valid, result, bounce_risk)
        await _usage.track("validemail", "verify")
        return {"verified": valid, "bounce_risk": bounce_risk}
    except Exception as e:
        logger.warning("[ValidEmail] Error verifying %s: %s", email, e)
        return {"verified": False, "bounce_risk": None}


def _generate_guessed_emails(first: str, last: str, domain: str) -> list[str]:
    """3 common corporate email patterns."""
    if not first or not domain:
        return []
    f  = first.strip().split()[0].lower()
    l  = (last.strip().split()[-1].lower()) if last.strip() else ""
    fi = f[0] if f else ""
    if l:
        return [f"{f}.{l}@{domain}", f"{fi}{l}@{domain}", f"{f}{l}@{domain}"]
    return [f"{f}@{domain}"]


def _extract_lead_identity(lead: dict) -> tuple[str, str, str, str]:
    """
    Extract (first, last, domain, linkedin_url) from a lead record.
    Checks raw_brightdata → name column for name,
    and company_website → raw_brightdata → crm_brief → enrichment_data for domain.
    """
    raw_bd = lead.get("raw_brightdata") or {}
    if isinstance(raw_bd, str):
        try:
            raw_bd = json.loads(raw_bd)
        except Exception:
            raw_bd = {}

    # Name
    first = (raw_bd.get("first_name") or "").strip()
    last  = (raw_bd.get("last_name")  or "").strip()
    if not first:
        first, last = svc._parse_name(raw_bd.get("name") or lead.get("name") or "")
    first = _normalize_name(re.sub(r'\s+[A-Z]\.$', '', first).strip())
    last  = _normalize_name(re.sub(r'\s+[A-Z]\.$', '', last).strip())

    # Domain — priority: company_website col → raw BD → crm_brief → enrichment_data
    domain = _extract_domain(lead.get("company_website") or "")

    if not domain:
        bd_co = raw_bd.get("company") or {}
        if isinstance(bd_co, dict):
            domain = _extract_domain(bd_co.get("website") or "")
        if not domain:
            domain = _extract_domain(raw_bd.get("company_website") or "")

    if not domain:
        try:
            crm = lead.get("crm_brief") or ""
            if isinstance(crm, str) and crm:
                crm = json.loads(crm)
            domain = _extract_domain((crm or {}).get("their_company", {}).get("website", ""))
        except Exception:
            pass

    if not domain:
        try:
            enr = lead.get("enrichment_data") or ""
            if isinstance(enr, str) and enr:
                enr = json.loads(enr)
            domain = _extract_domain((enr or {}).get("company", {}).get("website", ""))
        except Exception:
            pass

    linkedin_url = lead.get("linkedin_url") or raw_bd.get("url") or ""
    return first, last, domain, linkedin_url


# ── Synthetic lead builder (direct fields → lead dict) ────────────────────────

def _build_synthetic_lead(entry: DirectLeadInput) -> dict:
    """
    Convert a DirectLeadInput (no DB row) into the same dict shape that
    _find_email_for_lead / _extract_lead_identity expect.
    The DB UPDATE inside the flow silently affects 0 rows — safe no-op.
    """
    first = _normalize_name(re.sub(r'\s+[A-Z]\.$', '', entry.first_name.strip()).strip())
    last  = _normalize_name(re.sub(r'\s+[A-Z]\.$', '', entry.last_name.strip()).strip())
    # Always run _extract_domain — handles both plain domains ("lbmsolution.com")
    # and full URLs ("https://lbmsolution.com") correctly
    domain = _extract_domain(entry.domain or entry.company_website)
    ref_id = entry.ref or f"{first}_{last}_{domain}".strip("_")
    return {
        "id":               ref_id,
        "name":             f"{first} {last}".strip(),
        "company":          entry.company or domain,
        "company_website":  entry.company_website or domain,
        "linkedin_url":     entry.linkedin_url.strip(),
        "work_email":       None,
        "email_source":     None,
        "email_confidence": None,
        "email_verified":   None,
        "bounce_risk":      None,
        "enrichment_source": None,
        "direct_phone":     None,
        "raw_brightdata":   json.dumps({"first_name": first, "last_name": last}),
        "crm_brief":        None,
        "enrichment_data":  None,
    }


# ── Core per-lead email logic (shared by single + bulk) ───────────────────────

async def _find_email_for_lead(lead: dict, pool, *, force_refresh: bool = False) -> dict:
    """
    Flow (identical for single and bulk):
      1. Cache hit  → return immediately if work_email already set
      2. Apollo.io  → if found, verify via ValidEmail.net → save → return
      3. Pattern guesses → verify each via ValidEmail.net → save best → return all
    """
    lead_id = lead["id"]
    first, last, domain, linkedin_url = _extract_lead_identity(lead)

    # ── 1. Cache hit ──────────────────────────────────────────────────────────
    # Only trust the cache if the stored email is actually good quality:
    #   - verified by ValidEmail.net, OR
    #   - came from Apollo (authoritative source), OR
    #   - bounce risk is low or medium
    # A guessed + unverified + high-bounce email is NOT a valid cache hit —
    # fall through and re-run Apollo so we can find a better address.
    _cached_email    = lead.get("work_email")
    _cached_verified = bool(lead.get("email_verified"))
    _cached_source   = lead.get("email_source") or ""
    _cached_bounce   = lead.get("bounce_risk") or "high"

    _is_quality_cache = (
        _cached_email and not force_refresh and (
            _cached_verified                          # ValidEmail confirmed it
            or _cached_source == "apollo"             # Apollo is authoritative
            or _cached_bounce in ("low", "medium")    # acceptable bounce risk
        )
    )

    if _is_quality_cache:
        return {
            "lead_id":           lead_id,
            "linkedin_url":      linkedin_url,
            "name":              lead.get("name"),
            "company":           lead.get("company"),
            "work_email":        _cached_email,
            "email":             _cached_email,
            "source":            _cached_source,
            "message":           f"Email already found: {_cached_email}"
                                 + (f" | Phone: {lead.get('direct_phone')}" if lead.get("direct_phone") else ""),
            "confidence":        lead.get("email_confidence"),
            "verified":          _cached_verified,
            "bounce_risk":       _cached_bounce,
            "enrichment_source": lead.get("enrichment_source"),
            "phone":             lead.get("direct_phone") or lead.get("phone"),
            "all_emails":        [_cached_email],
            "activity_emails":   [],
            "activity_phones":   [],
            "ai_generated":      None,
            "guessed_emails":    [],
        }

    # Poor-quality cached email (unverified guess / high bounce) — clear it and re-enrich
    if _cached_email and not force_refresh and not _is_quality_cache:
        logger.info(
            "[EmailView] Skipping low-quality cache for lead %s "
            "(source=%s verified=%s bounce=%s) — re-running enrichment",
            lead_id, _cached_source, _cached_verified, _cached_bounce,
        )

    work_email:    str | None    = None
    phone:         str | None    = None
    source:        str           = "apollo"
    confidence:    str | None    = None
    apollo_raw:    dict          = {}
    verified:      bool          = False
    bounce_risk:   str | None    = None
    guessed_emails: list[dict]   = []

    # ── Guard: need at least first name + (domain or linkedin) ───────────────
    if not first or (not domain and not linkedin_url):
        logger.warning("[EmailView] Missing params for lead %s — first=%r domain=%r",
                       lead_id, first, domain)
        # Still try pattern guesses if we at least have name + domain
        if first and domain:
            guesses = _generate_guessed_emails(first, last, domain)
            verifications = await asyncio.gather(*[_verify_email(e) for e in guesses])
            # Only return verified guesses — invalid ones are not useful
            guessed_emails = [
                {"email": e, "verified": v["verified"], "bounce_risk": v["bounce_risk"], "source": "guessed"}
                for e, v in zip(guesses, verifications)
                if v["verified"]
            ]
        return {
            "lead_id":           lead_id,
            "linkedin_url":      linkedin_url,
            "name":              lead.get("name"),
            "company":           lead.get("company"),
            "work_email":        None, "email": None,
            "source":            "not_found",
            "message":           f"Not found — {lead.get('name', 'this lead')} at {lead.get('company', 'their company')}.",
            "confidence":        None, "verified": False, "bounce_risk": None,
            "enrichment_source": None, "phone": None,
            "all_emails":        [], "activity_emails": [], "activity_phones": [],
            "ai_generated":      None,
            "guessed_emails":    guessed_emails,
        }

    # ── 2. Apollo ─────────────────────────────────────────────────────────────
    try:
        apollo_result = await svc._try_apollo(first, last, domain, linkedin_url=linkedin_url)
    except Exception as e:
        logger.warning("[EmailView] Apollo error for lead %s: %s", lead_id, e)
        apollo_result = None

    if apollo_result and apollo_result.get("_credit_exhausted"):
        logger.warning("[EmailView] Apollo credits exhausted for lead %s", lead_id)
        apollo_result = None

    if apollo_result and apollo_result.get("email"):
        work_email = apollo_result["email"]
        phone      = apollo_result.get("phone")
        confidence = apollo_result.get("confidence")
        apollo_raw = apollo_result.get("_apollo_raw") or {}

        # Verify via ValidEmail.net
        verification = await _verify_email(work_email)
        verified     = verification["verified"]
        bounce_risk  = verification["bounce_risk"]

        async with pool.acquire() as conn:
            await conn.execute(
                """UPDATE enriched_leads
                   SET work_email=$1, email_source=$2, email_confidence=$3,
                       enrichment_source=$4, direct_phone=COALESCE(direct_phone, $5),
                       apollo_raw=$6, email_verified=$7, bounce_risk=$8,
                       updated_at=NOW()
                   WHERE id=$9""",
                work_email, source,
                str(confidence) if confidence else None,
                "apollo", phone,
                json.dumps(apollo_raw, default=str) if apollo_raw else None,
                1 if verified else 0,
                bounce_risk,
                lead_id,
            )
        logger.info("[EmailView] Apollo found email=%s verified=%s for lead %s",
                    work_email, verified, lead_id)

    else:
        # ── 3. Pattern guesses + ValidEmail.net ───────────────────────────────
        logger.info("[EmailView] Apollo found nothing for lead %s — generating guesses", lead_id)

        if first and domain:
            guesses = _generate_guessed_emails(first, last, domain)
            verifications = await asyncio.gather(*[_verify_email(e) for e in guesses])
            all_guesses = [
                {"email": e, "verified": v["verified"], "bounce_risk": v["bounce_risk"], "source": "guessed"}
                for e, v in zip(guesses, verifications)
            ]
            # Only keep verified guesses for the response — don't surface invalid ones
            guessed_emails = [g for g in all_guesses if g["verified"]]
            logger.info("[EmailView] Guesses for lead %s — valid: %s invalid: %s",
                        lead_id,
                        [g["email"] for g in guessed_emails],
                        [g["email"] for g in all_guesses if not g["verified"]])

            # Only accept a verified guess — unverified guesses are treated as not_found
            best = next((g for g in all_guesses if g["verified"]), None)
            if best:
                work_email  = best["email"]
                verified    = best["verified"]
                bounce_risk = best["bounce_risk"]
                source      = "guessed"
                async with pool.acquire() as conn:
                    await conn.execute(
                        """UPDATE enriched_leads
                           SET work_email=$1, email_source='guessed',
                               email_confidence='low', enrichment_source='pattern',
                               email_verified=$2, bounce_risk=$3,
                               updated_at=NOW()
                           WHERE id=$4""",
                        work_email, 1 if verified else 0, bounce_risk, lead_id,
                    )
                logger.info("[EmailView] Saved verified guess email=%s for lead %s", work_email, lead_id)
            else:
                # No verified guess found — mark as not_found, don't save junk email
                async with pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE enriched_leads SET work_email=NULL, email_source='not_found', updated_at=NOW() WHERE id=$1",
                        lead_id,
                    )
                logger.info("[EmailView] No verified guess for lead %s — marked not_found", lead_id)
        else:
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE enriched_leads SET work_email=NULL, email_source='not_found', updated_at=NOW() WHERE id=$1",
                    lead_id,
                )

    _message = (
        f"Email found: {work_email} | Source: {source} | Verified: {'Yes' if verified else 'No'}"
        f" | Bounce risk: {bounce_risk or 'unknown'}" + (f" | Phone: {phone}" if phone else "")
        if work_email
        else f"Not found — {lead.get('name', 'this lead')} at {lead.get('company', 'their company')}."
    )

    return {
        "lead_id":           lead_id,
        "linkedin_url":      linkedin_url,
        "name":              lead.get("name"),
        "company":           lead.get("company"),
        "work_email":        work_email,
        "email":             work_email,
        "source":            source if work_email else "not_found",
        "message":           _message,
        "confidence":        confidence,
        "verified":          verified,
        "bounce_risk":       bounce_risk,
        "enrichment_source": source if work_email else None,
        "phone":             phone,
        "all_emails":        [work_email] if work_email else [],
        "activity_emails":   [],
        "activity_phones":   [],
        "ai_generated":      None,
        "guessed_emails":    guessed_emails,
    }


# ── Single endpoint — unified: lead_id OR direct fields ───────────────────────

@router.post(
    "/view/email",
    summary="Find and verify email — by lead_id or by name + domain directly",
    description="""
Two ways to call this endpoint — same flow, same response shape:

**With a lead_id** (lead exists in the enrichment DB):
```json
{ "leadenrich_id": "abc123def456" }
```

**Without a lead_id** (manual LIO entry, cold prospect, imported contact):
```json
{
  "first_name": "John",
  "last_name":  "Doe",
  "company_website": "acme.com",
  "linkedin_url": "https://linkedin.com/in/johndoe"
}
```

Flow: Apollo.io → ValidEmail.net → pattern guesses → ValidEmail.net.
When fields are passed directly the DB update is a silent no-op (no row to update).
""",
)
async def email_enrichment(body: ViewEmailRequest):
    pool = await get_pool()

    if body.leadenrich_id:
        # ── Path A: lead exists in DB — fetch and enrich ──────────────────
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM enriched_leads WHERE id = $1", body.leadenrich_id
            )
        if not row:
            raise HTTPException(status_code=404, detail=f"Lead {body.leadenrich_id} not found")
        return await _find_email_for_lead(dict(row), pool, force_refresh=body.force_refresh)

    # ── Path B: no lead_id — build synthetic lead dict from request fields ──
    if not body.first_name.strip() or (
        not body.domain.strip() and not body.company_website.strip() and not body.linkedin_url.strip()
    ):
        raise HTTPException(
            status_code=400,
            detail="Provide leadenrich_id OR (first_name + one of: company_website, domain, linkedin_url)",
        )

    synthetic_lead = _build_synthetic_lead(DirectLeadInput(
        first_name=body.first_name,
        last_name=body.last_name,
        company_website=body.company_website,
        domain=body.domain,
        linkedin_url=body.linkedin_url,
        company=body.company,
    ))
    return await _find_email_for_lead(synthetic_lead, pool, force_refresh=True)


# ── Shared: build the flat list of lead dicts for bulk operations ──────────────

async def _resolve_bulk_leads(body: BulkEmailRequest, pool) -> list[dict]:
    """
    lead_ids items can be strings (DB lead_id) or DirectLeadInput objects (direct fields).
    Returns a flat list of lead dicts ready for _find_email_for_lead.
    """
    string_ids   = [item for item in body.lead_ids if isinstance(item, str)]
    direct_items = [item for item in body.lead_ids if isinstance(item, DirectLeadInput)]

    all_leads: list[dict] = []

    # ── String IDs → fetch from DB ────────────────────────────────────────────
    if string_ids:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM enriched_leads WHERE id = ANY($1::text[])",
                string_ids,
            )
        leads_by_id = {row["id"]: dict(row) for row in rows}
        for lid in string_ids:
            if lid in leads_by_id:
                all_leads.append(leads_by_id[lid])
            else:
                all_leads.append({"id": lid, "_not_found": True, "name": None, "company": None})

    # ── Direct-field objects → synthetic dict ─────────────────────────────────
    for entry in direct_items:
        all_leads.append(_build_synthetic_lead(entry))

    return all_leads


# ── Bulk endpoint ──────────────────────────────────────────────────────────────

@router.post(
    "/email/bulk",
    summary="Find emails for multiple leads in parallel",
    description="""
Accepts **lead_ids** (DB leads), **direct field objects** (no lead_id), or both in one request.

```json
{
  "lead_ids": ["id1", "id2"],
  "leads": [
    { "first_name": "John", "last_name": "Doe", "domain": "acme.com", "ref": "row-1" },
    { "first_name": "Jane", "last_name": "Smith", "linkedin_url": "https://linkedin.com/in/jane" }
  ],
  "force_refresh": false
}
```

- Up to 10 leads run concurrently (rate-limit safe).
- Synchronous response — no webhook or polling needed.
""",
)
async def bulk_email_enrichment(body: BulkEmailRequest):
    total = len(body.lead_ids)
    if total == 0:
        return {"total": 0, "found": 0, "cached": 0, "skipped": 0, "results": []}
    if total > 500:
        raise HTTPException(status_code=400, detail="Maximum 500 items per request.")

    pool = await get_pool()
    all_leads = await _resolve_bulk_leads(body, pool)

    results: list[dict] = []

    async def _enrich_one(lead: dict) -> dict:
        if lead.get("_not_found"):
            return {
                "lead_id": lead["id"], "name": None, "company": None,
                "work_email": None, "email": None,
                "source": "not_found", "message": f"Lead {lead['id']} not found in database.",
                "confidence": None, "verified": False, "bounce_risk": None,
                "enrichment_source": None, "phone": None,
                "all_emails": [], "activity_emails": [], "activity_phones": [],
                "ai_generated": None, "guessed_emails": [],
            }
        async with _BULK_EMAIL_SEM:
            return await _find_email_for_lead(lead, pool, force_refresh=body.force_refresh)

    lead_results = await asyncio.gather(*[_enrich_one(l) for l in all_leads], return_exceptions=True)

    for r in lead_results:
        if isinstance(r, Exception):
            logger.warning("[BulkEmail] error: %s", r)
            results.append({
                "lead_id": None, "work_email": None, "email": None,
                "source": "error", "message": str(r),
                "confidence": None, "verified": False, "bounce_risk": None,
            })
        else:
            results.append(r)

    cached  = sum(1 for r in results if r.get("work_email") and r.get("message", "").startswith("Email already found"))
    found   = sum(1 for r in results if r.get("work_email") and not r.get("message", "").startswith("Email already found"))
    skipped = sum(1 for r in results if not r.get("work_email"))

    return {"total": total, "found": found, "cached": cached, "skipped": skipped, "results": results}


# ── Bulk stream endpoint ───────────────────────────────────────────────────────

@router.post(
    "/email/bulk/stream",
    summary="Stream email enrichment results one by one as each lead finishes",
    description="""
Same as `/email/bulk` but streams via SSE — each result arrives the moment it finishes.

Accepts **lead_ids**, **direct field objects**, or both:

```json
{
  "lead_ids": ["id1", "id2"],
  "leads": [
    { "first_name": "John", "last_name": "Doe", "domain": "acme.com", "ref": "row-1" },
    { "first_name": "Jane", "last_name": "Smith", "linkedin_url": "https://linkedin.com/in/jane" }
  ],
  "force_refresh": false
}
```

Each SSE event: `data: <JSON>\\n\\n`. Final event has `"done": true` with summary counts.
""",
)
async def bulk_email_stream(body: BulkEmailRequest):
    total = len(body.lead_ids)
    if total == 0:
        async def _empty():
            yield f"data: {json.dumps({'done': True, 'total': 0, 'found': 0, 'cached': 0, 'skipped': 0})}\n\n"
        return StreamingResponse(_empty(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    if total > 500:
        raise HTTPException(status_code=400, detail="Maximum 500 items per request.")

    pool = await get_pool()
    all_leads = await _resolve_bulk_leads(body, pool)

    queue: asyncio.Queue = asyncio.Queue()
    total_tasks = len(all_leads)

    async def _enrich_and_enqueue(lead: dict) -> None:
        if lead.get("_not_found"):
            await queue.put({
                "lead_id": lead["id"], "name": None, "company": None,
                "work_email": None, "email": None,
                "source": "not_found", "message": f"Lead {lead['id']} not found in database.",
                "confidence": None, "verified": False, "bounce_risk": None,
                "enrichment_source": None, "phone": None,
                "all_emails": [], "activity_emails": [], "activity_phones": [],
                "ai_generated": None, "guessed_emails": [],
            })
            return
        try:
            async with _BULK_EMAIL_SEM:
                result = await _find_email_for_lead(lead, pool, force_refresh=body.force_refresh)
        except Exception as e:
            logger.warning("[BulkEmailStream] Error for lead %s: %s", lead.get("id"), e)
            result = {
                "lead_id": lead.get("id"), "work_email": None, "email": None,
                "source": "error", "message": str(e),
                "confidence": None, "verified": False, "bounce_risk": None,
            }
        await queue.put(result)

    async def _generate():
        # Fire all tasks — they push into the queue as they finish
        tasks = [asyncio.create_task(_enrich_and_enqueue(lead)) for lead in all_leads]

        found = cached = skipped = 0

        for _ in range(total_tasks):
            result = await queue.get()

            if result.get("work_email"):
                if result.get("message", "").startswith("Email already found"):
                    cached += 1
                else:
                    found += 1
            else:
                skipped += 1

            yield f"data: {json.dumps(result, default=str)}\n\n"

        await asyncio.gather(*tasks, return_exceptions=True)
        yield f"data: {json.dumps({'done': True, 'total': total_tasks, 'found': found, 'cached': cached, 'skipped': skipped})}\n\n"

    return StreamingResponse(
        _generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )

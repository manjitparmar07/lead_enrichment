"""
email_enrichment_routes.py — Single Lead Email Enrichment
----------------------------------------------------------
Endpoint:
  POST /api/leads/view/email  — Find email for a single lead via Apollo,
                                then verify it via ValidEmail.net
"""

from __future__ import annotations

import json
import logging
import os
import re

import httpx
from fastapi import APIRouter
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

VALIDEMAIL_TOKEN = os.getenv("VALIDEMAIL_TOKEN", "")
VALIDEMAIL_URL   = "https://api.ValidEmail.net/"


# ── Request model ─────────────────────────────────────────────────────────────

class ViewEmailRequest(BaseModel):
    leadenrich_id: str


# ── Helper: resolve lead from DB ──────────────────────────────────────────────

async def _resolve_lead(lead_id: str) -> dict:
    from fastapi import HTTPException
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM enriched_leads WHERE id = $1", lead_id
        )
    if not row:
        raise HTTPException(status_code=404, detail=f"Lead {lead_id} not found")
    return dict(row)


# ── Name normalizer ───────────────────────────────────────────────────────────

def _normalize_name(s: str) -> str:
    _char_map = {
        'æ': 'ae', 'ø': 'o', 'å': 'aa', 'ö': 'oe', 'ü': 'ue', 'ä': 'ae',
        'é': 'e',  'è': 'e', 'ê': 'e',  'ë': 'e',  'ñ': 'n',  'ç': 'c',
        'í': 'i',  'ì': 'i', 'î': 'i',  'ï': 'i',  'ó': 'o',  'ò': 'o',
        'ô': 'o',  'ú': 'u', 'ù': 'u',  'û': 'u',  'ý': 'y',  'ß': 'ss',
    }
    return ''.join(_char_map.get(c.lower(), c) for c in s)


# ── ValidEmail.net verification ───────────────────────────────────────────────

async def _verify_email(email: str) -> dict:
    """
    Verify email via ValidEmail.net API.
    Returns: { verified: bool, bounce_risk: str | None }

    API response keys:
      valid       — true/false
      result      — "valid" | "invalid" | "catch-all" | "unknown"
      disposable  — true/false
      reason      — why it passed/failed
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

        # Credits exhausted or error message
        if "Message" in data or "message" in data:
            msg = data.get("Message") or data.get("message", "")
            logger.warning("[ValidEmail] API message for %s: %s", email, msg)
            return {"verified": False, "bounce_risk": None}

        valid      = bool(data.get("valid", False))
        result     = (data.get("result") or "").lower()
        disposable = bool(data.get("disposable", False))

        # bounce_risk: high if invalid/disposable, medium if catch-all, low if valid
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


# ── Guessed email generator (used when Apollo finds nothing) ──────────────────

def _generate_guessed_emails(first: str, last: str, domain: str) -> list[str]:
    """
    Generate up to 3 common corporate email patterns.
    Uses only the final word of a multi-word last name (e.g. "Nathalia Fernandes" → "fernandes").
    """
    if not first or not domain:
        return []

    # Use only first word of first name, last word of last name
    f  = first.strip().split()[0].lower()
    _last_parts = last.strip().split() if last else []
    l  = _last_parts[-1].lower() if _last_parts else ""
    fi = f[0] if f else ""

    candidates: list[str] = []

    if l:
        candidates.append(f"{f}.{l}@{domain}")    # yessica.fernandes@deel.com
        candidates.append(f"{fi}{l}@{domain}")     # yfernandes@deel.com
        candidates.append(f"{f}{l}@{domain}")      # yessicafernandes@deel.com
    else:
        candidates.append(f"{f}@{domain}")         # yessica@deel.com

    return candidates[:3]


# ── Endpoint ──────────────────────────────────────────────────────────────────

@router.post("/view/email", summary="Find and verify email for a single lead via Apollo + ValidEmail")
async def email_enrichment(body: ViewEmailRequest):
    """
    Flow:
    1. Fetch lead from DB
    2. If email already saved → return it with verification status
    3. Call Apollo to find email
    4. If Apollo returns email → verify via ValidEmail.net → save + return
    5. If Apollo returns nothing → generate 3 guessed emails → verify each → return all 3
    """
    lead = await _resolve_lead(body.leadenrich_id)

    # ── Parse BD raw data ─────────────────────────────────────────────────────
    raw_bd = lead.get("raw_brightdata") or {}
    if isinstance(raw_bd, str):
        try:
            raw_bd = json.loads(raw_bd)
        except Exception:
            raw_bd = {}

    # Extract name
    first = (raw_bd.get("first_name") or "").strip()
    last  = (raw_bd.get("last_name") or "").strip()
    if not first:
        first, last = svc._parse_name(raw_bd.get("name") or lead.get("name") or "")

    # Strip trailing initials (e.g. "Djernæs N." → "Djernæs")
    first = re.sub(r'\s+[A-Z]\.$', '', first).strip()
    last  = re.sub(r'\s+[A-Z]\.$', '', last).strip()

    first_apollo = _normalize_name(first)
    last_apollo  = _normalize_name(last)

    # Extract domain — check multiple sources in priority order
    domain = ""
    from urllib.parse import urlparse

    def _extract_domain(url: str) -> str:
        if not url:
            return ""
        try:
            netloc = urlparse(url if url.startswith("http") else f"https://{url}").netloc
            return netloc.replace("www.", "").split(":")[0].lower()
        except Exception:
            return ""

    # 1. company_website column
    domain = _extract_domain(lead.get("company_website") or "")

    # 2. raw_brightdata company website
    if not domain:
        _bd_company = raw_bd.get("company") or {}
        if isinstance(_bd_company, dict):
            domain = _extract_domain(_bd_company.get("website") or "")
        if not domain:
            domain = _extract_domain(raw_bd.get("company_website") or "")

    # 3. crm_brief JSON — their_company.website
    if not domain:
        try:
            _crm = lead.get("crm_brief") or ""
            if isinstance(_crm, str) and _crm:
                _crm = json.loads(_crm)
            _site = (_crm or {}).get("their_company", {}).get("website", "")
            domain = _extract_domain(_site)
        except Exception:
            pass

    # 4. enrichment_data JSON
    if not domain:
        try:
            _enrich = lead.get("enrichment_data") or ""
            if isinstance(_enrich, str) and _enrich:
                _enrich = json.loads(_enrich)
            _site = (_enrich or {}).get("company", {}).get("website", "")
            domain = _extract_domain(_site)
        except Exception:
            pass

    linkedin_url = lead.get("linkedin_url") or raw_bd.get("url") or ""

    # ── Already has email — return immediately ────────────────────────────────
    if lead.get("work_email"):
        return {
            "lead_id":           lead.get("id"),
            "linkedin_url":      linkedin_url,
            "name":              lead.get("name"),
            "company":           lead.get("company"),
            "work_email":        lead.get("work_email"),
            "email":             lead.get("work_email"),
            "source":            lead.get("email_source"),
            "message":           f"Email already found: {lead.get('work_email')}" + (f" | Phone: {lead.get('direct_phone')}" if lead.get("direct_phone") else ""),
            "confidence":        lead.get("email_confidence"),
            "verified":          bool(lead.get("email_verified")),
            "bounce_risk":       lead.get("bounce_risk"),
            "enrichment_source": lead.get("enrichment_source"),
            "phone":             lead.get("direct_phone") or lead.get("phone"),
            "all_emails":        [lead.get("work_email")],
            "activity_emails":   [],
            "activity_phones":   [],
            "ai_generated":      None,
            "guessed_emails":    [],
        }

    # ── Missing required params ───────────────────────────────────────────────
    if not first_apollo or (not domain and not linkedin_url):
        logger.warning("[EmailView] Cannot call Apollo — missing first=%r domain=%r linkedin=%r for lead %s",
                       first, domain, linkedin_url, lead.get("id"))
        # Still try guessing if we have name + domain
        guessed = []
        if first_apollo and domain:
            guesses = _generate_guessed_emails(first_apollo, last_apollo, domain)
            import asyncio
            verifications = await asyncio.gather(*[_verify_email(e) for e in guesses])
            guessed = [
                {"email": e, "verified": v["verified"], "bounce_risk": v["bounce_risk"], "source": "guessed"}
                for e, v in zip(guesses, verifications)
            ]
        return {
            "lead_id":           lead.get("id"),
            "linkedin_url":      linkedin_url,
            "name":              lead.get("name"),
            "company":           lead.get("company"),
            "work_email":        None, "email": None,
            "source":            "missing_params",
            "message":           "Cannot search email — missing first name and company domain or LinkedIn URL.",
            "confidence":        None, "verified": False, "bounce_risk": None,
            "enrichment_source": None, "phone": None,
            "all_emails":        [], "activity_emails": [], "activity_phones": [],
            "ai_generated":      None,
            "guessed_emails":    guessed,
        }

    # ── Apollo call ───────────────────────────────────────────────────────────
    try:
        apollo_result = await svc._try_apollo(first_apollo, last_apollo, domain, linkedin_url=linkedin_url)
    except Exception as _e:
        logger.warning("[EmailView] Apollo call failed for %s: %s", lead.get("id"), _e)
        apollo_result = None

    # Credit exhausted — fall through to guessed email generation
    if apollo_result and apollo_result.get("_credit_exhausted"):
        logger.warning("[EmailView] Apollo credits exhausted for lead %s — generating guessed emails", lead.get("id"))
        apollo_result = None  # treat as not found so guesses are generated below

    # ── Apollo found email ────────────────────────────────────────────────────
    work_email  = None
    phone       = None
    source      = "apollo"
    confidence  = None
    apollo_raw  = {}
    verified    = False
    bounce_risk = None
    guessed_emails: list[dict] = []

    if apollo_result and apollo_result.get("email"):
        work_email = apollo_result["email"]
        phone      = apollo_result.get("phone")
        confidence = apollo_result.get("confidence")
        apollo_raw = apollo_result.get("_apollo_raw") or {}

        # Verify via ValidEmail
        verification = await _verify_email(work_email)
        verified     = verification["verified"]
        bounce_risk  = verification["bounce_risk"]

        # Save to DB
        pool = await get_pool()
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
                lead["id"],
            )
        logger.info("[EmailView] Apollo found email=%s verified=%s bounce_risk=%s for lead %s",
                    work_email, verified, bounce_risk, lead.get("id"))

    else:
        # ── Apollo found nothing — generate 3 guessed emails and verify each ─
        logger.info("[EmailView] Apollo found no email for lead %s — generating guesses", lead.get("id"))

        if first_apollo and domain:
            import asyncio
            guesses = _generate_guessed_emails(first_apollo, last_apollo, domain)
            verifications = await asyncio.gather(*[_verify_email(e) for e in guesses])
            guessed_emails = [
                {
                    "email":       e,
                    "verified":    v["verified"],
                    "bounce_risk": v["bounce_risk"],
                    "source":      "guessed",
                }
                for e, v in zip(guesses, verifications)
            ]
            logger.info("[EmailView] Guessed %d emails for lead %s: %s",
                        len(guessed_emails), lead.get("id"),
                        [g["email"] for g in guessed_emails])

            # Save best verified guess to DB — pick first verified one,
            # fallback to first guess if none verified
            best_guess = next(
                (g for g in guessed_emails if g["verified"]),
                guessed_emails[0] if guessed_emails else None,
            )
            if best_guess:
                work_email  = best_guess["email"]
                verified    = best_guess["verified"]
                bounce_risk = best_guess["bounce_risk"]
                source      = "guessed"
                pool = await get_pool()
                async with pool.acquire() as conn:
                    await conn.execute(
                        """UPDATE enriched_leads
                           SET work_email=$1, email_source='guessed',
                               email_confidence='low', enrichment_source='pattern',
                               email_verified=$2, bounce_risk=$3,
                               updated_at=NOW()
                           WHERE id=$4""",
                        work_email,
                        1 if verified else 0,
                        bounce_risk,
                        lead["id"],
                    )
                logger.info("[EmailView] Saved best guess email=%s verified=%s for lead %s",
                            work_email, verified, lead.get("id"))
            else:
                pool = await get_pool()
                async with pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE enriched_leads SET email_source='not_found', updated_at=NOW() WHERE id=$1",
                        lead["id"],
                    )
        else:
            pool = await get_pool()
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE enriched_leads SET email_source='not_found', updated_at=NOW() WHERE id=$1",
                    lead["id"],
                )

    _message = (
        f"Email found: {work_email} | Source: {source} | Verified: {'Yes' if verified else 'No'} | Bounce risk: {bounce_risk or 'unknown'}"
        + (f" | Phone: {phone}" if phone else "")
        if work_email
        else (
            f"No email found for {lead.get('name', 'this lead')} at {lead.get('company', 'their company')}."
        )
    )

    return {
        "lead_id":           lead.get("id"),
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
        "enrichment_source": "apollo" if work_email else None,
        "phone":             phone,
        "all_emails":        [work_email] if work_email else [],
        "activity_emails":   [],
        "activity_phones":   [],
        "ai_generated":      None,
        "guessed_emails":    guessed_emails,
    }

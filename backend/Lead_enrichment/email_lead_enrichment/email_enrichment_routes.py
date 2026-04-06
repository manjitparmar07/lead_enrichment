"""
email_enrichment_routes.py — Standalone Email Enrichment
---------------------------------------------------------
Endpoints:
  POST /api/email-enrich/start          — start email enrichment job for leads without emails
  GET  /api/email-enrich/jobs/{job_id}  — poll job status
  GET  /api/email-enrich/candidates     — count/list leads without emails
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from urllib.parse import urlparse

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request
from pydantic import BaseModel

from db import get_pool
from Lead_enrichment.bulk_lead_enrichment import lead_enrichment_brightdata_service as svc
from config import enrichment_config_service as cfg_svc

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/email-enrich", tags=["Email Enrichment"])


# ── Auth helper ───────────────────────────────────────────────────────────────

def _get_org_id(request: Request) -> str:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return "default"
    token = auth[7:].strip()
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return "default"
        padded = parts[1] + "=" * (4 - len(parts[1]) % 4)
        payload = json.loads(base64.b64decode(padded))
        return str(payload.get("organization_id", "default"))
    except Exception:
        return "default"


# ── Domain extraction ─────────────────────────────────────────────────────────

def _extract_domain(url: str) -> str:
    if not url:
        return ""
    if not url.startswith("http"):
        url = "https://" + url
    try:
        netloc = urlparse(url).netloc
        # strip www. and any port
        netloc = netloc.split(":")[0]
        if netloc.startswith("www."):
            netloc = netloc[4:]
        return netloc.lower()
    except Exception:
        return ""


# ── Job state in Redis (falls back to in-process dict) ───────────────────────

_job_store: dict = {}   # fallback when Redis unavailable

# Max concurrent email enrichment jobs — prevents event loop starvation and
# external API (Hunter.io/Apollo) rate-limit exhaustion under high load.
_EMAIL_JOB_CONCURRENCY = int(os.getenv("EMAIL_JOB_CONCURRENCY", "10"))
_email_job_sem: asyncio.Semaphore = asyncio.Semaphore(_EMAIL_JOB_CONCURRENCY)


async def _get_redis():
    try:
        import redis.asyncio as aioredis
        client = aioredis.from_url(
            os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            decode_responses=True,
        )
        await client.ping()
        return client
    except Exception:
        return None


async def _job_set(job_id: str, data: dict) -> None:
    r = await _get_redis()
    if r:
        try:
            await r.setex(f"email_enrich:job:{job_id}", 86400, json.dumps(data))
            return
        except Exception:
            pass
    _job_store[job_id] = data


async def _job_get(job_id: str) -> dict | None:
    r = await _get_redis()
    if r:
        try:
            raw = await r.get(f"email_enrich:job:{job_id}")
            if raw:
                return json.loads(raw)
        except Exception:
            pass
    return _job_store.get(job_id)


# ── DB helpers ────────────────────────────────────────────────────────────────

async def _count_candidates(org_id: str) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT COUNT(*) AS cnt
            FROM enriched_leads
            WHERE organization_id = $1
              AND (work_email IS NULL OR work_email = '')
              AND (
                (first_name IS NOT NULL AND first_name != '' AND company_website IS NOT NULL AND company_website != '')
                OR (name IS NOT NULL AND name != '' AND company_website IS NOT NULL AND company_website != '')
              )
            """,
            org_id,
        )
        return int(row["cnt"]) if row else 0


async def _fetch_candidates(org_id: str, limit: int = 2000) -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, first_name, last_name, name, company_website, linkedin_url, company, title
            FROM enriched_leads
            WHERE organization_id = $1
              AND (work_email IS NULL OR work_email = '')
              AND (
                (first_name IS NOT NULL AND first_name != '' AND company_website IS NOT NULL AND company_website != '')
                OR (name IS NOT NULL AND name != '' AND company_website IS NOT NULL AND company_website != '')
              )
            ORDER BY total_score DESC NULLS LAST
            LIMIT $2
            """,
            org_id,
            limit,
        )
        return [dict(r) for r in rows]


async def _fetch_specific_leads(org_id: str, lead_ids: list[str]) -> list[dict]:
    if not lead_ids:
        return []
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, first_name, last_name, name, company_website, linkedin_url, company, title
            FROM enriched_leads
            WHERE organization_id = $1
              AND id = ANY($2::text[])
              AND (work_email IS NULL OR work_email = '')
            """,
            org_id,
            lead_ids,
        )
        return [dict(r) for r in rows]


async def _save_email_result(lead_id: str, org_id: str, result: dict) -> None:
    email = result.get("email")
    if not email:
        return
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE enriched_leads
            SET work_email      = $1,
                email_source    = $2,
                email_confidence= $3,
                email_verified  = $4,
                bounce_risk     = $5,
                enrichment_source = $6
            WHERE id = $7 AND organization_id = $8
              AND (work_email IS NULL OR work_email = '')
            """,
            email,
            result.get("source"),
            str(result.get("confidence", "")) if result.get("confidence") else None,
            1 if result.get("verified") else 0,
            result.get("bounce_risk"),
            result.get("source"),
            lead_id,
            org_id,
        )


# ── Background job runner ─────────────────────────────────────────────────────

async def _run_email_enrichment_job(
    job_id: str,
    org_id: str,
    leads: list[dict],
) -> None:
    # Acquire slot — at most _EMAIL_JOB_CONCURRENCY jobs run concurrently.
    # Extra jobs queue here (in the event loop) rather than blasting the external APIs.
    async with _email_job_sem:
        await _run_email_enrichment_job_inner(job_id, org_id, leads)


async def _run_email_enrichment_job_inner(
    job_id: str,
    org_id: str,
    leads: list[dict],
) -> None:
    total = len(leads)
    found = 0
    failed = 0

    await _job_set(job_id, {
        "job_id": job_id,
        "org_id": org_id,
        "status": "running",
        "total": total,
        "processed": 0,
        "found": 0,
        "failed": 0,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "results": [],
    })

    results_preview: list[dict] = []

    for i, lead in enumerate(leads):
        lead_id = lead["id"]
        try:
            # Resolve first/last name
            first = (lead.get("first_name") or "").strip()
            last = (lead.get("last_name") or "").strip()
            if not first and lead.get("name"):
                parts = lead["name"].strip().split()
                first = parts[0] if parts else ""
                last = " ".join(parts[1:]) if len(parts) > 1 else ""

            domain = _extract_domain(lead.get("company_website") or "")

            if not first or not domain:
                failed += 1
                continue

            # Retry up to 3 times — guards against transient Hunter.io/Apollo errors
            contact = {}
            _last_exc: Exception | None = None
            for _attempt in range(3):
                try:
                    contact = await svc.find_contact_info(
                        first=first,
                        last=last,
                        domain=domain,
                        linkedin_url=lead.get("linkedin_url") or "",
                    )
                    _last_exc = None
                    break
                except Exception as exc:
                    _last_exc = exc
                    if _attempt < 2:
                        await asyncio.sleep(2 ** _attempt)   # 1s, 2s backoff

            if _last_exc is not None:
                raise _last_exc

            if contact.get("email"):
                await _save_email_result(lead_id, org_id, contact)
                found += 1
                results_preview.append({
                    "lead_id": lead_id,
                    "name": lead.get("name") or f"{first} {last}".strip(),
                    "company": lead.get("company"),
                    "email": contact["email"],
                    "source": contact.get("source"),
                    "confidence": contact.get("confidence"),
                    "verified": contact.get("verified", False),
                })
                # Keep last 50 results for preview
                if len(results_preview) > 50:
                    results_preview = results_preview[-50:]
            else:
                failed += 1

        except Exception as exc:
            logger.warning("[EmailEnrich] Lead %s failed: %s", lead_id, exc)
            failed += 1

        # Update progress every 5 leads
        if (i + 1) % 5 == 0 or (i + 1) == total:
            await _job_set(job_id, {
                "job_id": job_id,
                "org_id": org_id,
                "status": "running",
                "total": total,
                "processed": i + 1,
                "found": found,
                "failed": failed,
                "started_at": datetime.now(timezone.utc).isoformat(),
                "results": results_preview,
            })

        # Small yield to avoid blocking event loop
        await asyncio.sleep(0)

    await _job_set(job_id, {
        "job_id": job_id,
        "org_id": org_id,
        "status": "completed",
        "total": total,
        "processed": total,
        "found": found,
        "failed": failed,
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "results": results_preview,
    })
    logger.info("[EmailEnrich] Job %s done — %d/%d emails found", job_id, found, total)


# ── Request models ────────────────────────────────────────────────────────────

class StartEmailEnrichRequest(BaseModel):
    lead_ids: list[str] = []          # empty = enrich ALL leads without email
    limit: int = 500                   # cap when using "all without email"


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/candidates", summary="Leads without email (count + sample)")
async def get_candidates(
    request: Request,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
):
    org_id = _get_org_id(request)
    total = await _count_candidates(org_id)
    leads = await _fetch_candidates(org_id, limit=per_page * page)
    offset = (page - 1) * per_page
    page_leads = leads[offset: offset + per_page]
    return {
        "total": total,
        "page": page,
        "per_page": per_page,
        "leads": [
            {
                "lead_id": l["id"],
                "name": l.get("name") or f"{l.get('first_name','')} {l.get('last_name','')}".strip(),
                "company": l.get("company"),
                "title": l.get("title"),
                "linkedin_url": l.get("linkedin_url"),
                "has_domain": bool(_extract_domain(l.get("company_website") or "")),
            }
            for l in page_leads
        ],
    }


@router.post("/start", summary="Start email enrichment job")
async def start_email_enrichment(
    body: StartEmailEnrichRequest,
    request: Request,
    background_tasks: BackgroundTasks,
):
    org_id = _get_org_id(request)

    # Fetch the leads to process
    if body.lead_ids:
        leads = await _fetch_specific_leads(org_id, body.lead_ids)
    else:
        leads = await _fetch_candidates(org_id, limit=min(body.limit, 2000))

    if not leads:
        return {"job_id": None, "message": "No eligible leads found (missing domain or already have email)", "total": 0}

    job_id = str(uuid.uuid4())
    background_tasks.add_task(_run_email_enrichment_job, job_id, org_id, leads)

    return {
        "job_id": job_id,
        "total": len(leads),
        "message": f"Email enrichment started for {len(leads)} leads",
    }


@router.get("/jobs/{job_id}", summary="Poll email enrichment job status")
async def get_job_status(job_id: str, request: Request):
    org_id = _get_org_id(request)
    job = await _job_get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.get("org_id") != org_id:
        raise HTTPException(status_code=403, detail="Forbidden")
    return job


# ── ValidEmail.net Integration ────────────────────────────────────────────────

VALIDEMAIL_API = "https://api.ValidEmail.net/"


def _validemail_token() -> str:
    try:
        from auth import keys_service as ks
        return ks.get("VALIDEMAIL_TOKEN") or os.getenv("VALIDEMAIL_TOKEN", "")
    except Exception:
        return os.getenv("VALIDEMAIL_TOKEN", "")


async def _check_one_email(email: str, token: str) -> dict:
    """Call ValidEmail.net for a single email. Returns raw result dict."""
    import httpx
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(VALIDEMAIL_API, params={"email": email, "token": token})
        if r.status_code == 200:
            data = r.json()
            # Normalise common field names across ValidEmail API versions
            return {
                "email":       email,
                "valid":       data.get("valid", data.get("is_valid", False)),
                "disposable":  data.get("disposable", data.get("is_disposable", False)),
                "role":        data.get("role", data.get("is_role", False)),
                "dns_valid":   data.get("dns_valid", data.get("mx_found", None)),
                "smtp_valid":  data.get("smtp_valid", data.get("smtp_check", None)),
                "score":       data.get("score", None),
                "reason":      data.get("reason", data.get("message", "")),
                "raw":         data,
                "error":       None,
            }
        return {"email": email, "valid": False, "error": f"API {r.status_code}: {r.text[:200]}"}
    except Exception as e:
        return {"email": email, "valid": False, "error": str(e)}


class ValidateEmailsRequest(BaseModel):
    emails: list[str]


class ValidateLeadsRequest(BaseModel):
    limit: int = 500
    save_results: bool = True   # update bounce_risk on enriched_leads row


@router.post("/validate", summary="Validate emails via ValidEmail.net")
async def validate_emails(body: ValidateEmailsRequest, request: Request):
    """
    Validate a list of email addresses using ValidEmail.net.
    Returns validity, disposable flag, score, and SMTP/DNS checks.
    """
    token = _validemail_token()
    if not token:
        raise HTTPException(400, "VALIDEMAIL_TOKEN not configured. Add it in Tool Configuration.")

    emails = [e.strip().lower() for e in body.emails if e.strip()]
    if not emails:
        raise HTTPException(400, "No emails provided")
    if len(emails) > 100:
        raise HTTPException(400, "Max 100 emails per request")

    # Run concurrently with a semaphore (max 10 parallel calls)
    sem = asyncio.Semaphore(10)

    async def _bounded(email: str) -> dict:
        async with sem:
            return await _check_one_email(email, token)

    results = await asyncio.gather(*[_bounded(e) for e in emails])

    valid_count   = sum(1 for r in results if r.get("valid"))
    invalid_count = sum(1 for r in results if not r.get("valid"))

    return {
        "total":   len(results),
        "valid":   valid_count,
        "invalid": invalid_count,
        "results": results,
    }


@router.post("/validate-leads", summary="Validate all lead emails in the org via ValidEmail.net")
async def validate_lead_emails(body: ValidateLeadsRequest, request: Request, background_tasks: BackgroundTasks):
    """
    Fetch leads that have a work_email, validate via ValidEmail.net,
    optionally save bounce_risk back to enriched_leads.
    Returns a job_id to poll status.
    """
    token = _validemail_token()
    if not token:
        raise HTTPException(400, "VALIDEMAIL_TOKEN not configured. Add it in Tool Configuration.")

    org_id = _get_org_id(request)

    # Fetch leads with emails
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, work_email, name
            FROM enriched_leads
            WHERE organization_id = $1
              AND work_email IS NOT NULL
              AND work_email != ''
            ORDER BY created_at DESC
            LIMIT $2
            """,
            org_id, min(body.limit, 2000),
        )
    leads = [dict(r) for r in rows]

    if not leads:
        return {"job_id": None, "message": "No leads with email found", "total": 0}

    job_id = str(uuid.uuid4())
    background_tasks.add_task(
        _run_validate_leads_job,
        job_id, org_id, leads, token, body.save_results,
    )
    return {
        "job_id":  job_id,
        "total":   len(leads),
        "message": f"Email validation started for {len(leads)} leads",
    }


async def _run_validate_leads_job(
    job_id: str,
    org_id: str,
    leads: list[dict],
    token: str,
    save_results: bool,
) -> None:
    await _job_set(job_id, {
        "job_id": job_id, "org_id": org_id,
        "status": "running", "total": len(leads),
        "processed": 0, "valid": 0, "invalid": 0, "errors": 0,
        "started_at": datetime.now(timezone.utc).isoformat(),
    })

    sem     = asyncio.Semaphore(10)
    pool    = get_pool()
    results = []

    async def _process(lead: dict):
        async with sem:
            email  = lead["work_email"]
            result = await _check_one_email(email, token)
            result["lead_id"] = str(lead["id"])
            result["name"]    = lead.get("name", "")
            results.append(result)

            if save_results and result.get("error") is None:
                # Map ValidEmail validity to bounce_risk
                if result.get("valid"):
                    bounce = "low" if not result.get("disposable") else "medium"
                else:
                    bounce = "high"
                try:
                    async with pool.acquire() as conn:
                        await conn.execute(
                            "UPDATE enriched_leads SET bounce_risk=$1 WHERE id=$2 AND organization_id=$3",
                            bounce, lead["id"], org_id,
                        )
                except Exception as e:
                    logger.warning("[ValidEmail] DB update failed for %s: %s", email, e)

            # Update job progress
            job = await _job_get(job_id) or {}
            job["processed"] = job.get("processed", 0) + 1
            if result.get("valid"):
                job["valid"] = job.get("valid", 0) + 1
            elif result.get("error"):
                job["errors"] = job.get("errors", 0) + 1
            else:
                job["invalid"] = job.get("invalid", 0) + 1
            await _job_set(job_id, job)

    await asyncio.gather(*[_process(lead) for lead in leads])

    job = await _job_get(job_id) or {}
    job["status"]       = "completed"
    job["completed_at"] = datetime.now(timezone.utc).isoformat()
    job["results"]      = results
    await _job_set(job_id, job)


# ── Apollo People Match ───────────────────────────────────────────────────────

def _apollo_key() -> str:
    try:
        from auth import keys_service as ks
        return ks.get("APOLLO_API_KEY") or os.getenv("APOLLO_API_KEY", "")
    except Exception:
        return os.getenv("APOLLO_API_KEY", "")


class ApolloMatchRequest(BaseModel):
    linkedin_url: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    domain: str | None = None
    reveal_personal_emails: bool = True


@router.post("/apollo/match", summary="Find email via Apollo people/match")
async def apollo_people_match(body: ApolloMatchRequest, request: Request):
    """
    Calls Apollo POST /v1/people/match and returns the matched person's
    email, phone, and basic profile data.

    At least one of linkedin_url or domain must be provided.
    """
    api_key = _apollo_key()
    if not api_key:
        raise HTTPException(400, "APOLLO_API_KEY not configured. Add it in Tool Configuration.")

    if not body.linkedin_url and not body.domain:
        raise HTTPException(400, "Provide at least linkedin_url or domain.")

    payload: dict = {"reveal_personal_emails": body.reveal_personal_emails}
    if body.first_name:
        payload["first_name"] = body.first_name
    if body.last_name:
        payload["last_name"] = body.last_name
    if body.domain:
        payload["domain"] = body.domain
    if body.linkedin_url:
        payload["linkedin_url"] = body.linkedin_url

    import httpx
    try:
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post(
                "https://api.apollo.io/v1/people/match",
                headers={"Content-Type": "application/json", "X-Api-Key": api_key},
                json=payload,
            )
    except Exception as e:
        raise HTTPException(502, f"Apollo request failed: {e}")

    if r.status_code == 422:
        body_json = r.json()
        err = body_json.get("error", "")
        if "insufficient credits" in err.lower():
            raise HTTPException(402, "Apollo credit balance exhausted.")
        raise HTTPException(422, body_json)

    if r.status_code != 200:
        raise HTTPException(r.status_code, r.text[:300])

    data = r.json()
    p   = data.get("person") or {}
    org = data.get("organization") or {}

    email  = p.get("email") or ""
    phones = p.get("phone_numbers") or []

    if email and "placeholder" in email.lower():
        email = ""

    return {
        "email":           email or None,
        "email_status":    p.get("email_status"),
        "first_name":      p.get("first_name"),
        "last_name":       p.get("last_name"),
        "title":           p.get("title"),
        "linkedin_url":    p.get("linkedin_url"),
        "photo_url":       p.get("photo_url"),
        "twitter_url":     p.get("twitter_url"),
        "phone":           p.get("phone") or (phones[0].get("sanitized_number") if phones else None),
        "city":            p.get("city"),
        "country":         p.get("country"),
        "company_name":    org.get("name"),
        "company_domain":  org.get("primary_domain"),
        "company_website": org.get("website_url"),
        "company_linkedin": org.get("linkedin_url"),
        "found":           bool(email),
        "source":          "apollo",
    }

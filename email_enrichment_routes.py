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
import lead_enrichment_brightdata_service as svc
import enrichment_config_service as cfg_svc

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

            contact = await svc.find_contact_info(
                first=first,
                last=last,
                domain=domain,
                linkedin_url=lead.get("linkedin_url") or "",
            )

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

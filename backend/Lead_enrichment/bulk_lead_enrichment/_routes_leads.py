"""
_routes_leads.py
----------------
Queue stats, AI processing queue, lead list, export CSV/JSON, re-enrich,
refresh, audit, lead CRUD (get/delete/outreach/company/crm-brief), lead notes.

Lines ~1176–1625 of the original lead_enrichment_brightdata_routes.py.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
from typing import Optional

import jwt

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, field_validator

from Lead_enrichment.bulk_lead_enrichment import lead_enrichment_brightdata_service as svc
from config import enrichment_config_service as cfg_svc

from Lead_enrichment.bulk_lead_enrichment._shared import (
    _format_lead,
    _get_org_id,
    _validate_token,
    _lead_cache_delete,
    _outreach_sem,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Lead Enrichment"])


# ── Lead results ───────────────────────────────────────────────────────────

@router.get("/queue/stats", include_in_schema=False)
async def queue_stats():
    """
    Real-time queue system snapshot.

    Returns active tenant count, per-tenant queue depth, current chunk size,
    worker status, and Redis memory usage.

    Useful for monitoring scale: how many tenants are active, how deep
    each tenant's queue is, and whether the system is under memory pressure.
    """
    from Lead_enrichment.bulk_lead_enrichment.queue_manager import get_queue_stats
    return await get_queue_stats()


# ── AI Processing Queue ───────────────────────────────────────────────────────

class _AIProcessRequest(BaseModel):
    token: str
    job_id: str
    lead_ids: list[str]
    system_prompt: str
    forward_to_lio: bool = False

    @field_validator("lead_ids")
    @classmethod
    def _validate_lead_ids(cls, v):
        if not v:
            raise ValueError("lead_ids cannot be empty")
        if len(v) > 5000:
            raise ValueError("max 5000 lead_ids per request")
        return list(dict.fromkeys(v))


@router.post("/jobs/ai-process", include_in_schema=False)
async def enqueue_ai_processing(request: Request):
    """
    Enqueue raw_profile leads for AI processing via Qwen 70B / HuggingFace / WB LLM.

    Body:
      token         — JWT for org_id / sso_id
      job_id        — existing job to track progress against
      lead_ids      — list of lead IDs (must have status='scraping' or 'ai_queued')
      system_prompt — system prompt to use for every lead
      forward_to_lio — whether to forward completed leads to LIO

    Status transitions: scraping → ai_queued → ai_processing → completed

    Returns: { enqueued: N, chunks: N, workers: N }
    """
    import redis.asyncio as aioredis
    from Lead_enrichment.bulk_lead_enrichment.queue_manager import push_ai_job, get_ai_queue_stats, AI_WORKERS

    body_raw = await request.body()
    try:
        body = _AIProcessRequest.model_validate(json.loads(body_raw))
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e))

    JWT_SECRET = os.getenv("JWT_SECRET", "")
    try:
        decoded = jwt.decode(
            body.token,
            JWT_SECRET,
            algorithms=["HS256"],
            options={"verify_exp": False},
        )
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {e}")

    org_id = decoded.get("org_id") or decoded.get("organizationId", "")
    sso_id = decoded.get("sso_id") or decoded.get("userId", "")
    if not org_id:
        raise HTTPException(status_code=401, detail="org_id missing from token")

    try:
        r = aioredis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)
        chunks_enqueued = await push_ai_job(
            job_id=body.job_id,
            org_id=org_id,
            lead_ids=body.lead_ids,
            system_prompt=body.system_prompt,
            r=r,
            sso_id=sso_id,
            forward_to_lio=body.forward_to_lio,
        )
        await r.aclose()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Redis error: {e}")

    return {
        "ok":      True,
        "enqueued": len(body.lead_ids),
        "chunks":  chunks_enqueued,
        "workers": AI_WORKERS,
        "job_id":  body.job_id,
        "org_id":  org_id,
    }


@router.get("/queue/ai-stats", include_in_schema=False)
async def ai_queue_stats():
    """Real-time AI processing queue snapshot."""
    from Lead_enrichment.bulk_lead_enrichment.queue_manager import get_ai_queue_stats
    return await get_ai_queue_stats()


@router.post("/queue/ai-dlq/retry", include_in_schema=False)
async def ai_dlq_retry(index: int = 0):
    """Retry one item from the AI dead-letter queue by index (default: first item)."""
    from Lead_enrichment.bulk_lead_enrichment.queue_manager import retry_ai_dlq
    ok = await retry_ai_dlq(index)
    return {"ok": ok}


@router.get("/", include_in_schema=False)
async def list_leads(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    job_id: Optional[str] = Query(None),
    min_score: Optional[int] = Query(None, ge=0, le=100),
    tier: Optional[str] = Query(None, pattern="^(hot|warm|cool|cold)$"),
    sort_by: Optional[str] = Query(None),
    sort_dir: Optional[str] = Query(None),
    q: Optional[str] = Query(None),
):
    """List all enriched leads."""
    result = await svc.list_leads(
        limit=limit, offset=offset,
        job_id=job_id, min_score=min_score, tier=tier,
        sort_by=sort_by, sort_dir=sort_dir, q=q,
    )
    result["leads"] = [_format_lead(l) for l in result["leads"]]
    return result


@router.get("/export/csv", include_in_schema=False)
async def export_leads_csv(
    request: Request,
    job_id: Optional[str] = Query(None),
    min_score: Optional[int] = Query(None, ge=0, le=100),
    tier: Optional[str] = Query(None),
):
    """Export enriched leads as CSV download (enhanced — includes contact, company, outreach fields)."""
    org_id = _get_org_id(request)
    result = await svc.list_leads(
        limit=5000, offset=0, org_id=org_id,
        job_id=job_id, min_score=min_score, tier=tier,
    )
    leads = result["leads"]

    _CSV_FIELDS = [
        "name", "first_name", "last_name", "title", "seniority_level", "department",
        "company", "industry", "employee_count", "hq_location", "company_website",
        "funding_stage", "total_funding",
        "work_email", "email_source", "email_confidence", "email_verified",
        "direct_phone", "twitter",
        "city", "country", "timezone",
        "linkedin_url",
        "total_score", "score_tier", "icp_fit_score", "intent_score",
        "timing_score", "engagement_score", "score_explanation",
        "email_subject", "outreach_angle",
        "followers", "connections",
        "job_id", "enriched_at",
    ]

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=_CSV_FIELDS, extrasaction="ignore")
    writer.writeheader()
    for lead in leads:
        row = {f: lead.get(f, "") for f in _CSV_FIELDS}
        writer.writerow(row)

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=leads_enriched.csv"},
    )


@router.get("/export/json", include_in_schema=False)
async def export_leads_json(
    request: Request,
    job_id: Optional[str] = Query(None),
    min_score: Optional[int] = Query(None, ge=0, le=100),
    tier: Optional[str] = Query(None),
):
    """Export enriched leads as JSON download."""
    org_id = _get_org_id(request)
    result = await svc.list_leads(
        limit=5000, offset=0, org_id=org_id,
        job_id=job_id, min_score=min_score, tier=tier,
    )
    leads = [_format_lead(l, full=True) for l in result["leads"]]
    output = json.dumps({"total": result["total"], "leads": leads}, default=str, indent=2)
    return StreamingResponse(
        iter([output]),
        media_type="application/json",
        headers={"Content-Disposition": "attachment; filename=leads_enriched.json"},
    )


# ── Bulk Re-enrichment ──────────────────────────────────────────────────────

class ReEnrichJobRequest(BaseModel):
    token: Optional[str] = None


@router.post("/jobs/{job_id}/re-enrich", include_in_schema=False)
async def re_enrich_job(job_id: str, body: ReEnrichJobRequest, request: Request):
    """
    Re-submit all LinkedIn URLs from a completed job as a new bulk job.

    Useful for refreshing stale data or retrying a high-failure-rate job.
    Returns the new job_id.
    """
    org_id = _get_org_id(request)
    if body.token:
        org_id, _ = _validate_token(body.token)

    job = await svc.get_job(job_id, org_id=org_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    result = await svc.list_leads(limit=5000, offset=0, org_id=org_id, job_id=job_id)
    urls = [l["linkedin_url"] for l in result["leads"] if l.get("linkedin_url")]
    if not urls:
        raise HTTPException(status_code=400, detail="No leads found in this job to re-enrich")

    try:
        new_job = await svc.enrich_bulk(urls=urls, org_id=org_id)
        await svc.audit_log(
            org_id, "re_enrich",
            job_id=new_job["id"],
            meta={"original_job_id": job_id, "urls_count": len(urls)},
        )
        return {
            "success": True,
            "new_job_id": new_job["id"],
            "submitted_urls": len(urls),
            "original_job_id": job_id,
            "message": f"Re-enrichment started for {len(urls)} URLs. Track at GET /api/leads/jobs/{new_job['id']}",
        }
    except Exception as e:
        logger.error("[ReEnrichJob] %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Re-enrichment failed: {e}")


# ── Scheduled / Manual Refresh ─────────────────────────────────────────────

class RefreshLeadsRequest(BaseModel):
    older_than_days: int = 90
    tier: Optional[str] = None
    min_score: int = 0
    limit: int = 200
    token: Optional[str] = None


@router.post("/refresh", include_in_schema=False)
async def schedule_refresh(body: RefreshLeadsRequest, request: Request):
    """
    Find stale leads matching the criteria and re-submit them as a new bulk job.

    ```json
    { "older_than_days": 90, "tier": "hot", "min_score": 70, "limit": 200 }
    ```
    """
    org_id = _get_org_id(request)
    if body.token:
        org_id, _ = _validate_token(body.token)

    stale = await svc.get_stale_leads(
        org_id=org_id,
        older_than_days=body.older_than_days,
        tier=body.tier,
        min_score=body.min_score,
        limit=body.limit,
    )
    if not stale:
        return {"success": True, "job": None, "queued_urls": 0, "message": "No stale leads found matching criteria"}

    urls = [l["linkedin_url"] for l in stale if l.get("linkedin_url")]
    try:
        new_job = await svc.enrich_bulk(urls=urls, org_id=org_id)
        await svc.audit_log(
            org_id, "refresh_scheduled",
            job_id=new_job["id"],
            meta={
                "older_than_days": body.older_than_days,
                "tier": body.tier,
                "min_score": body.min_score,
                "queued_urls": len(urls),
            },
        )
        return {
            "success": True,
            "job_id": new_job["id"],
            "queued_urls": len(urls),
            "message": f"Refresh job started for {len(urls)} stale leads. Track at GET /api/leads/jobs/{new_job['id']}",
        }
    except Exception as e:
        logger.error("[RefreshLeads] %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Refresh failed: {e}")


# ── Audit Log ───────────────────────────────────────────────────────────────

@router.get("/audit", include_in_schema=False)
async def get_audit_log(
    request: Request,
    action: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """
    Audit log for lead enrichment actions in this org.

    Filter by action: enrich_single | bulk_submit | delete | re_enrich | refresh_scheduled
    """
    org_id = _get_org_id(request)
    return await svc.list_audit_log(org_id, action=action, limit=limit, offset=offset)


@router.get("/{lead_id}/raw-data", include_in_schema=False)
async def get_lead_raw_data(lead_id: str):
    """Return the 4 raw data blobs stored in DB for a lead — no processing."""
    from db import get_pool
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow(
            """SELECT raw_brightdata, raw_brightdata_company, apollo_raw, raw_website_scrap
               FROM enriched_leads WHERE id = $1""",
            lead_id,
        )
    if not row:
        raise HTTPException(status_code=404, detail="Lead not found")

    def _parse(val):
        if val is None:
            return None
        try:
            return json.loads(val)
        except Exception:
            return val

    return {
        "brightdata_profile":  _parse(row["raw_brightdata"]),
        "brightdata_company":  _parse(row["raw_brightdata_company"]),
        "apollo_raw":          _parse(row["apollo_raw"]),
        "website_scrap":       _parse(row["raw_website_scrap"]),
    }


@router.get("/{lead_id}", include_in_schema=False)
async def get_lead(lead_id: str):
    """Get full detail for a single enriched lead."""
    lead = await svc.get_lead(lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    return _format_lead(lead, full=True)


@router.post("/{lead_id}/outreach", include_in_schema=False)
async def regenerate_outreach(lead_id: str):
    """
    Re-generate AI outreach copy (cold email + LinkedIn note + sequence)
    for an already-enriched lead.
    """
    async with _outreach_sem:
        try:
            result = await svc.regenerate_outreach_for_lead(lead_id)
        except RuntimeError as exc:
            logger.warning("[RegenerateOutreach] LLM unavailable for lead=%s: %s", lead_id, exc)
            raise HTTPException(status_code=503, detail="LLM is rate-limited or unavailable. Please wait a moment and try again.")
        except Exception as exc:
            logger.error("[RegenerateOutreach] Unexpected error for lead=%s: %s", lead_id, exc)
            raise HTTPException(status_code=500, detail=f"Regeneration failed: {exc}")
    if not result:
        raise HTTPException(status_code=404, detail="Lead not found")
    return {"success": True, "lead": _format_lead(result, full=True)}


@router.post("/{lead_id}/company", include_in_schema=False)
async def regenerate_company(lead_id: str):
    """
    Re-run AI analysis (culture_signals, account_pitch, company_tags)
    for the company associated with this lead.
    """
    result = await svc.regenerate_company_for_lead(lead_id)
    if not result:
        raise HTTPException(status_code=404, detail="Lead not found")
    return {"success": True, "lead": _format_lead(result, full=True)}


@router.post("/{lead_id}/crm-brief", include_in_schema=False)
async def regenerate_crm_brief(lead_id: str, request: Request):
    """
    Re-run the CRM brief LLM call for an already-enriched lead.
    Uses raw_profile from DB + lio_system_prompt + lio_model from workspace_configs.
    """
    org_id = _get_org_id(request)

    lead = await svc.get_lead(lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    if not lead.get("raw_profile") and not lead.get("raw_brightdata"):
        raise HTTPException(status_code=422, detail="No raw profile saved for this lead — re-enrich first")

    try:
        result = await svc.regenerate_crm_brief_for_lead(lead_id, org_id=org_id)
        crm_brief = result.get("crm_brief") if result else None
        if crm_brief:
            return {"success": True, "lead_id": lead_id, "crm_brief": crm_brief}
        return {
            "success": False,
            "lead_id": lead_id,
            "crm_brief": None,
            "error": "LLM returned no content — check HuggingFace credits (402 Payment Required)",
        }
    except Exception as exc:
        logger.error("[RegenerateCrmBrief] Failed for lead=%s: %s", lead_id, exc)
        return {
            "success": False,
            "lead_id": lead_id,
            "crm_brief": None,
            "error": str(exc),
        }


@router.delete("/{lead_id}", include_in_schema=False)
async def delete_lead(lead_id: str, request: Request, background_tasks: BackgroundTasks):
    """Delete an enriched lead record."""
    org_id = _get_org_id(request)
    lead = await svc.get_lead(lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    deleted = await svc.delete_lead(lead_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Lead not found")
    await _lead_cache_delete(lead_id)
    background_tasks.add_task(
        svc.audit_log, org_id, "delete",
        lead_id=lead_id, linkedin_url=lead.get("linkedin_url"),
    )
    return {"success": True, "message": "Lead deleted"}


# ── Lead Notes ──────────────────────────────────────────────────────────────

class AddNoteRequest(BaseModel):
    note: str


@router.post("/{lead_id}/notes", include_in_schema=False)
async def add_note(lead_id: str, body: AddNoteRequest, request: Request):
    """Add a manual note to a lead."""
    org_id = _get_org_id(request)
    if not body.note.strip():
        raise HTTPException(status_code=400, detail="Note cannot be empty")
    lead = await svc.get_lead(lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    note = await svc.add_lead_note(lead_id, org_id, body.note)
    return {"success": True, "note": note}


@router.get("/{lead_id}/notes", include_in_schema=False)
async def list_notes(lead_id: str, request: Request):
    """List all notes for a lead."""
    org_id = _get_org_id(request)
    notes = await svc.list_lead_notes(lead_id, org_id)
    return {"notes": notes, "count": len(notes)}


@router.delete("/{lead_id}/notes/{note_id}", include_in_schema=False)
async def delete_note(lead_id: str, note_id: str, request: Request):
    """Delete a note from a lead."""
    org_id = _get_org_id(request)
    deleted = await svc.delete_lead_note(note_id, org_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Note not found")
    return {"success": True, "message": "Note deleted"}

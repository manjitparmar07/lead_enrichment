"""
_routes_enrich.py
-----------------
Enrich stream/single/bulk routes + job management routes (list jobs, sub-jobs,
job detail, stop job, rerun job, delete job, LIO DLQ routes).

Lines ~499–1043 of the original lead_enrichment_brightdata_routes.py.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Optional

import jwt

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, field_validator

from Lead_enrichment.bulk_lead_enrichment import lead_enrichment_brightdata_service as svc
from config import enrichment_config_service as cfg_svc

from Lead_enrichment.bulk_lead_enrichment._shared import (
    _format_lead,
    _get_org_id,
    _validate_token,
    _skip_lio_sem,
    BulkEnrichRequest,
    _validate_linkedin_url,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Lead Enrichment"])


# ─────────────────────────────────────────────────────────────────────────────
# Request models local to this file
# ─────────────────────────────────────────────────────────────────────────────

class SingleEnrichRequest(BaseModel):
    linkedin_url: str
    generate_outreach: bool = True
    engagement_data: Optional[dict] = None
    force_refresh: bool = False
    forward_to_lio: bool = False

    @field_validator("linkedin_url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        return _validate_linkedin_url(v)


class SingleEnrichPublicRequest(BaseModel):
    linkedin_url: str
    token: str

    @field_validator("linkedin_url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        return _validate_linkedin_url(v)


# ─────────────────────────────────────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/enrich/stream", include_in_schema=False)
async def enrich_stream(body: SingleEnrichRequest, request: Request):
    """
    Stream a single LinkedIn profile enrichment as Server-Sent Events.

    Yields one JSON event per stage (loading → done) as the enrichment progresses:
      stage: profile | company | contact | website | scoring | complete

    Each event:  data: {"stage": "...", "status": "loading"|"done"|"error", "data": {...}}
    """
    org_id = _get_org_id(request)

    async def gen():
        try:
            async for event in svc.enrich_single_stream(
                linkedin_url=body.linkedin_url,
                org_id=org_id,
            ):
                yield f"data: {json.dumps(event)}\n\n"
        except Exception as e:
            logger.error("[EnrichStream] %s", e, exc_info=True)
            yield f"data: {json.dumps({'stage': 'complete', 'status': 'error', 'error': str(e)})}\n\n"

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/enrich", include_in_schema=False)
async def enrich_single(body: SingleEnrichRequest, request: Request, background_tasks: BackgroundTasks):
    """
    Enrich a single LinkedIn profile URL.

    Calls Bright Data sync API (~30–60s), finds email via waterfall,
    scores the lead (3-layer model), and generates AI outreach if score ≥ threshold.

    Pass a single URL:
    ```json
    {"linkedin_url": "https://www.linkedin.com/in/someone"}
    ```
    """
    org_id = _get_org_id(request)

    # ── Duplicate detection ────────────────────────────────────────────────────
    if not body.force_refresh:
        existing = await svc.check_existing_lead(body.linkedin_url, org_id)
        if existing:
            if existing.get("_stale"):
                return {
                    "success": True,
                    "cache_hit": True,
                    "stale": True,
                    "stale_warning": (
                        f"Lead enriched {existing.get('enriched_at', 'previously')} — "
                        "data may be outdated. Pass force_refresh=true to re-enrich."
                    ),
                    "lead": _format_lead(existing),
                }
            return {
                "success": True,
                "cache_hit": True,
                "stale": False,
                "lead": _format_lead(existing),
            }

    tools_available = await cfg_svc.get_available_tools(org_id)
    try:
        lead = await svc.enrich_single(
            linkedin_url=body.linkedin_url,
            engagement_data=body.engagement_data,
            generate_outreach_flag=body.generate_outreach,
            org_id=org_id,
            tools=tools_available,
            forward_to_lio=body.forward_to_lio,
        )
        if lead and not lead.get("error"):
            lead_id = lead.get("id")
            await cfg_svc.deduct_credit(org_id, "brightdata", lead_id=lead_id, reason="profile enrichment")
            email_source = str(lead.get("email_source") or "")
            if "apollo" in email_source:
                await cfg_svc.deduct_credit(org_id, "apollo", lead_id=lead_id, reason="email discovery")
            background_tasks.add_task(
                svc.audit_log, org_id, "enrich_single",
                lead_id=lead_id, linkedin_url=body.linkedin_url,
                meta={"force_refresh": body.force_refresh},
            )
        return {
            "success": True,
            "cache_hit": False,
            "lead": _format_lead(lead),
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("[EnrichSingle] %s", e, exc_info=True)
        return {"success": False, "cache_hit": False, "lead": None, "error": str(e)}


@router.post(
    "/enrich/single",
    tags=["LinkedIn Enrichment"],
    summary="Single LinkedIn Enrich",
    description="""
Enrich **one** LinkedIn profile URL synchronously.

Pass your JWT token (from Get Token) in the request body — same token used for the Bulk API.
The response includes the full enriched lead **plus** the structured `linkedin_enrich` view.

**Cache behaviour:** If the URL was already enriched, the cached result is returned instantly
(`_cache_hit: true`). Fresh enrichments call Bright Data (~30–60s).

```json
{
  "linkedin_url": "https://www.linkedin.com/in/johndoe",
  "token": "<YOUR_TOKEN>"
}
```
""",
)
async def enrich_single_public(body: SingleEnrichPublicRequest):
    org_id, sso_id = _validate_token(body.token)
    tools_available = await cfg_svc.get_available_tools(org_id)
    try:
        lead = await svc.enrich_single(
            linkedin_url=body.linkedin_url,
            org_id=org_id,
            sso_id=sso_id,
            tools=tools_available,
        )
        if lead and not lead.get("error"):
            lead_id = lead.get("id")
            if not lead.get("_cache_hit"):
                await cfg_svc.deduct_credit(org_id, "brightdata", lead_id=lead_id, reason="profile enrichment")
                email_source = str(lead.get("email_source") or "")
                if "apollo" in email_source:
                    await cfg_svc.deduct_credit(org_id, "apollo", lead_id=lead_id, reason="email discovery")
        return {
            "success":        True,
            "_cache_hit":     lead.get("_cache_hit", False),
            "lead":           _format_lead(lead),
            "linkedin_enrich": lead.get("linkedin_enrich"),
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("[EnrichSinglePublic] %s", e, exc_info=True)
        return {"success": False, "_cache_hit": False, "lead": None, "linkedin_enrich": None, "error": str(e)}


@router.post("/enrich/bulk")
async def enrich_bulk(request: Request):
    """
    Enrich multiple LinkedIn profile URLs asynchronously.

    No Authorization header required. Pass JWT in the `token` body field —
    organization_id is decoded from it and echoed back in the response.

    ```json
    {
      "linkedin_urls": ["https://www.linkedin.com/in/alice"],
      "token": "<YOUR_JWT_TOKEN>",
      "webhook_url": "https://your-app.com/api/leads/webhook/brightdata"
    }
    ```
    """
    try:
        raw = await request.json()
        if isinstance(raw, str):
            raw = json.loads(raw)
        body = BulkEnrichRequest(**raw)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Invalid request body: {e}")

    org_id, sso_id = _validate_token(body.token)

    urls_to_submit = body.linkedin_urls
    skipped_urls: list[str] = []

    if body.skip_existing:
        filtered: list[str] = []
        existing_map = await svc.check_existing_leads_batch(urls_to_submit, org_id)
        for url in urls_to_submit:
            lead_id = svc._lead_id(svc._normalize_linkedin_url(url))
            existing = existing_map.get(lead_id)
            lead_status = (existing or {}).get("status", "")
            if existing and not existing.get("_stale") and lead_status in ("completed", "enriched"):
                existing["linkedin_enrich"] = svc._format_linkedin_enrich(existing)
                _lead = dict(existing)
                _sso  = sso_id
                async def _capped_lio_send(_l=_lead, _s=_sso):
                    async with _skip_lio_sem:
                        await svc.send_to_lio(_l, sso_id=_s, force=True)
                asyncio.create_task(_capped_lio_send())
                skipped_urls.append(url)
            else:
                filtered.append(url)
        urls_to_submit = filtered

    if not urls_to_submit:
        return {
            "success": True,
            "job": None,
            "skipped_urls": skipped_urls,
            "submitted_count": 0,
            "message": "All submitted URLs are already enriched. Pass skip_existing=false or use /re-enrich to force refresh.",
        }

    try:
        job = await svc.enrich_bulk(
            urls=urls_to_submit,
            webhook_url=body.webhook_url,
            notify_url=body.notify_url,
            webhook_auth=body.webhook_auth,
            org_id=org_id,
            sso_id=sso_id,
            forward_to_lio=body.forward_to_lio,
            system_prompt=body.system_prompt,
        )
        await svc.audit_log(
            org_id, "bulk_submit", job_id=job["id"],
            meta={"submitted": len(urls_to_submit), "skipped": len(skipped_urls)},
        )
        resp = {
            "success": True,
            "job": job,
            "submitted_count": len(urls_to_submit),
            "skipped_count": len(skipped_urls),
            "skipped_urls": skipped_urls if len(skipped_urls) <= 20 else skipped_urls[:20],
            "message": (
                f"Batch job started for {len(urls_to_submit)} URLs"
                + (f" ({len(skipped_urls)} skipped — already enriched)" if skipped_urls else "")
                + f". Track progress at GET /api/leads/jobs/{job['id']}"
            ),
        }
        if body.token is not None:
            resp["token"] = body.token
        return resp
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("[EnrichBulk] %s", e, exc_info=True)
        return {"success": False, "job": None, "submitted_count": 0, "skipped_count": 0, "skipped_urls": [], "error": str(e)}


# ── Job management ─────────────────────────────────────────────────────────

@router.get("/jobs")
async def list_jobs(
    status: str = Query(""),
    q: str = Query(""),
    limit: int = Query(20, ge=1, le=200),
):
    """List enrichment jobs."""
    jobs = await svc.list_jobs(limit=limit)
    if status:
        jobs = [j for j in jobs if j.get("status") == status]
    if q:
        q_lower = q.lower()
        jobs = [j for j in jobs if q_lower in j.get("id", "").lower()]

    if jobs:
        job_ids = [j["id"] for j in jobs]
        from db import get_pool
        async with get_pool().acquire() as conn:
            placeholders = ",".join(f"${i+1}" for i in range(len(job_ids)))
            sub_rows = await conn.fetch(
                f"SELECT * FROM enrichment_sub_jobs WHERE job_id IN ({placeholders}) ORDER BY chunk_index",
                *job_ids,
            )
        sub_map: dict[str, list] = {j["id"]: [] for j in jobs}
        for row in sub_rows:
            sub_map[row["job_id"]].append(dict(row))
        for job in jobs:
            job["sub_jobs"] = sub_map.get(job["id"], [])
    return {"jobs": jobs, "total": len(jobs)}


@router.get("/jobs/{job_id}/sub-jobs", include_in_schema=False)
async def get_sub_jobs(job_id: str):
    """List sub-jobs (chunks) for a specific enrichment job."""
    job = await svc.get_job(job_id)
    org_id = job.get("organization_id", "default") if job else "default"
    sub_jobs = await svc.list_sub_jobs(job_id, org_id=org_id)
    return {"sub_jobs": sub_jobs, "total": len(sub_jobs)}


@router.get("/jobs/{job_id}")
async def get_job(job_id: str):
    """Get status and progress of a specific enrichment job."""
    from db import get_pool
    async with get_pool().acquire() as conn:
        job_row = await conn.fetchrow("SELECT * FROM enrichment_jobs WHERE id=$1", job_id)
        if not job_row:
            raise HTTPException(status_code=404, detail="Job not found")
        job = dict(job_row)
        org_id = job.get("organization_id", "default")

        leads_count = await conn.fetchval("SELECT COUNT(*) FROM enriched_leads WHERE job_id=$1", job_id)
        sub_rows = await conn.fetch(
            "SELECT * FROM enrichment_sub_jobs WHERE job_id=$1 AND organization_id=$2 ORDER BY chunk_index",
            job_id, org_id,
        )

    return {**job, "leads_count": leads_count, "sub_jobs": [dict(r) for r in sub_rows]}


# ── LIO Dead-Letter Queue ─────────────────────────────────────────────────────

@router.get("/lio/dlq/count")
async def lio_dlq_count():
    """Return number of failed LIO deliveries waiting in the dead-letter queue."""
    try:
        import redis.asyncio as aioredis
        redis_url = os.getenv("REDIS_URL", "")
        if not redis_url:
            return {"count": 0, "error": "REDIS_URL not configured"}
        r = aioredis.from_url(redis_url, decode_responses=True)
        count = await r.llen("lio:dlq")
        await r.close()
        return {"count": count}
    except Exception as e:
        return {"count": 0, "error": str(e)}


@router.post("/lio/dlq/replay")
async def lio_dlq_replay(max_items: int = 50):
    """Replay up to max_items failed LIO deliveries from the dead-letter queue."""
    result = await svc.lio_dlq_replay(max_items=max_items)
    return result


@router.post(
    "/jobs/{job_id}/stop",
    summary="Stop / cancel a running job",
    description=(
        "Cancels a running or pending enrichment job.\n\n"
        "**What this does:**\n"
        "- Marks the job and all pending sub-jobs as `cancelled` in the database.\n"
        "- Calls the BrightData snapshot cancel API to stop any in-progress scrape.\n"
        "- Any webhook results that arrive after cancellation are automatically discarded.\n\n"
        "**Cancellable statuses:** `running`, `pending`, `fallback`\n\n"
        "**BrightData cancel endpoint called:**\n"
        "`POST https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}/cancel`"
    ),
    responses={
        200: {
            "description": "Job cancelled successfully",
            "content": {
                "application/json": {
                    "example": {
                        "job_id": "3f1b2c4d-...",
                        "status": "cancelled",
                        "snapshots_cancelled": 4,
                    }
                }
            },
        },
        400: {"description": "Job is not in a cancellable state"},
        404: {"description": "Job not found"},
    },
)
async def stop_job(job_id: str):
    from db import get_pool
    import asyncio as _asyncio
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, snapshot_id FROM enrichment_jobs WHERE id=$1", job_id
        )
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")
        if row["status"] not in ("running", "pending", "fallback"):
            raise HTTPException(status_code=400, detail=f"Job is already {row['status']}")
        await conn.execute(
            "UPDATE enrichment_jobs SET status='cancelled', error='Stopped by user' WHERE id=$1",
            job_id,
        )
        await conn.execute(
            "UPDATE enrichment_sub_jobs SET status='cancelled' WHERE job_id=$1 AND status IN ('pending','running')",
            job_id,
        )
        sub_snap_rows = await conn.fetch(
            "SELECT snapshot_id FROM enrichment_sub_jobs WHERE job_id=$1 AND snapshot_id IS NOT NULL",
            job_id,
        )

    snapshot_ids = {r["snapshot_id"] for r in sub_snap_rows if r["snapshot_id"]}
    if row["snapshot_id"]:
        snapshot_ids.add(row["snapshot_id"])

    if snapshot_ids:
        await _asyncio.gather(*[svc.cancel_brightdata_snapshot(sid) for sid in snapshot_ids])

    return {
        "job_id": job_id,
        "status": "cancelled",
        "snapshots_cancelled": len(snapshot_ids),
    }


@router.post(
    "/jobs/{job_id}/rerun",
    summary="Rerun a failed or cancelled job",
    description=(
        "Reruns the BrightData snapshot for an existing enrichment job.\n\n"
        "**What this does:**\n"
        "- Calls the BrightData snapshot rerun API using the job's existing `snapshot_id`.\n"
        "- Updates the job's `snapshot_id` to the new one returned by BrightData.\n"
        "- Resets the job status to `running` and clears any previous error.\n"
        "- Resets all sub-jobs to `pending` so progress tracking starts fresh.\n\n"
        "**Rerunnable statuses:** `failed`, `cancelled`, `completed`\n\n"
        "**BrightData rerun endpoint called:**\n"
        "`POST https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}/rerun`"
    ),
    responses={
        200: {
            "description": "Job rerun triggered successfully",
            "content": {
                "application/json": {
                    "example": {
                        "job_id": "3f1b2c4d-...",
                        "status": "running",
                        "old_snapshot_id": "s_abc123",
                        "new_snapshot_id": "s_xyz789",
                    }
                }
            },
        },
        400: {"description": "Job has no snapshot_id to rerun, or is already running"},
        404: {"description": "Job not found"},
    },
)
async def rerun_job(job_id: str):
    from db import get_pool
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, snapshot_id FROM enrichment_jobs WHERE id=$1", job_id
        )
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")
        if row["status"] in ("running", "pending"):
            raise HTTPException(status_code=400, detail=f"Job is already {row['status']}")
        old_snapshot_id = row["snapshot_id"]
        if not old_snapshot_id:
            raise HTTPException(status_code=400, detail="Job has no snapshot_id — cannot rerun (was not a BrightData batch job)")

    try:
        new_snapshot_id = await svc.rerun_brightdata_snapshot(old_snapshot_id)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"BrightData rerun failed: {e}")

    async with get_pool().acquire() as conn:
        await conn.execute(
            "UPDATE enrichment_jobs SET status='running', snapshot_id=$1, error=NULL, updated_at=NOW() WHERE id=$2",
            new_snapshot_id, job_id,
        )
        await conn.execute(
            "UPDATE enrichment_sub_jobs SET status='pending', processed=0, failed=0, updated_at=NOW() WHERE job_id=$1",
            job_id,
        )

    return {
        "job_id": job_id,
        "status": "running",
        "old_snapshot_id": old_snapshot_id,
        "new_snapshot_id": new_snapshot_id,
    }


@router.delete("/jobs/{job_id}", include_in_schema=False)
async def delete_job(job_id: str):
    """Delete a job and all its sub-jobs from the database."""
    from db import get_pool
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow("SELECT id FROM enrichment_jobs WHERE id=$1", job_id)
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")
        await conn.execute("DELETE FROM enrichment_sub_jobs WHERE job_id=$1", job_id)
        await conn.execute("DELETE FROM enrichment_jobs WHERE id=$1", job_id)
    return {"job_id": job_id, "deleted": True}

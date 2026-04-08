"""
_routes_webhooks.py
-------------------
Webhook routes: brightdata webhook, notify webhook.

Lines ~1044–1175 of the original lead_enrichment_brightdata_routes.py.
"""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from fastapi.responses import JSONResponse

from Lead_enrichment.bulk_lead_enrichment import lead_enrichment_brightdata_service as svc

from Lead_enrichment.bulk_lead_enrichment._shared import (
    _get_org_id,
    MAX_WEBHOOK_INFLIGHT,
    WEBHOOK_SECRET,
)

# _webhook_inflight is a mutable module-level int in _shared.
# We import the module itself so mutations are shared across all importers.
import Lead_enrichment.bulk_lead_enrichment._shared as _shared

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Lead Enrichment"])


# ── Webhooks ───────────────────────────────────────────────────────────────

@router.post("/webhook/brightdata", include_in_schema=False)
async def webhook_brightdata(request: Request, background_tasks: BackgroundTasks):
    """
    Receives enriched profile data from Bright Data (batch delivery).
    Bright Data POSTs an array of profile objects to this endpoint.

    Configure via Bright Data dashboard or API trigger:
      endpoint=https://your-app.com/api/leads/webhook/brightdata
    """
    if WEBHOOK_SECRET:
        auth = request.headers.get("Authorization", "")
        if auth != f"Bearer {WEBHOOK_SECRET}":
            raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        profiles = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    if not isinstance(profiles, list):
        profiles = [profiles]

    job_id      = request.query_params.get("job_id")
    sub_job_id  = request.query_params.get("sub_job_id")
    snapshot_id = request.query_params.get("snapshot_id")

    # ── Backpressure guard ────────────────────────────────────────────────────
    if _shared._webhook_inflight >= MAX_WEBHOOK_INFLIGHT:
        logger.warning("[Webhook] inflight limit reached (%d) — returning 429", _shared._webhook_inflight)
        return JSONResponse(status_code=429, content={"ok": False, "error": "too many in-flight webhooks"})

    # ── Idempotency check: guard DB calls within BrightData's 10s window ─────
    async def _pre_response() -> dict:
        if snapshot_id:
            if await svc.is_snapshot_processed(snapshot_id):
                logger.info("[Webhook] snapshot %s already processed — skipping", snapshot_id)
                return {"ok": True, "duplicate": True, "message": "Snapshot already processed"}
            org_id = _get_org_id(request)
            await svc.mark_snapshot_processed(snapshot_id, job_id, org_id)
        return {}

    try:
        early = await asyncio.wait_for(_pre_response(), timeout=9.0)
    except asyncio.TimeoutError:
        logger.warning("[Webhook] pre-response timed out for snapshot %s — accepting anyway", snapshot_id)
        return JSONResponse(status_code=202, content={"ok": True, "timeout": True, "received": len(profiles)})

    if early.get("duplicate"):
        return early

    # Process in background so webhook returns quickly (Bright Data timeout is 10s).
    async def _bg_task():
        _shared._webhook_inflight += 1
        try:
            await svc.process_webhook_profiles(profiles, job_id, sub_job_id)
        finally:
            _shared._webhook_inflight -= 1

    background_tasks.add_task(_bg_task)

    return {"ok": True, "received": len(profiles)}


@router.post("/webhook/notify", include_in_schema=False)
async def webhook_notify(request: Request, background_tasks: BackgroundTasks):
    """
    Bright Data snapshot-ready notification.
    Body: {"snapshot_id": "s_xxx", "status": "ready"}
    Triggers async download and processing of the snapshot.
    """
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    status = data.get("status") or data.get("state", "")
    snapshot_id = data.get("snapshot_id") or data.get("id")
    job_id = request.query_params.get("job_id")
    sub_job_id = request.query_params.get("sub_job_id")

    if status == "ready" and snapshot_id:
        # ── Backpressure guard ────────────────────────────────────────────────
        if _shared._webhook_inflight >= MAX_WEBHOOK_INFLIGHT:
            logger.warning("[WebhookNotify] inflight limit reached (%d) — returning 429", _shared._webhook_inflight)
            return JSONResponse(status_code=429, content={"ok": False, "error": "too many in-flight webhooks"})

        # ── Idempotency check within BrightData's 10s window ─────────────────
        async def _pre_response_notify() -> dict:
            if await svc.is_snapshot_processed(snapshot_id):
                logger.info("[WebhookNotify] snapshot %s already processed — skipping", snapshot_id)
                return {"ok": True, "duplicate": True, "message": "Snapshot already processed"}
            org_id = _get_org_id(request)
            await svc.mark_snapshot_processed(snapshot_id, job_id, org_id)
            return {}

        try:
            early = await asyncio.wait_for(_pre_response_notify(), timeout=9.0)
        except asyncio.TimeoutError:
            logger.warning("[WebhookNotify] pre-response timed out for snapshot %s — accepting anyway", snapshot_id)
            return JSONResponse(status_code=202, content={"ok": True, "timeout": True})

        if early.get("duplicate"):
            return early

        async def _download_and_process():
            _shared._webhook_inflight += 1
            try:
                profiles = await svc.poll_snapshot(snapshot_id, interval=5, timeout=120)
                await svc.process_webhook_profiles(profiles, job_id=job_id, sub_job_id=sub_job_id)
                if job_id:
                    await svc._update_job(job_id, status="completed")
            except Exception as e:
                logger.error("[Notify] Download failed for %s: %s", snapshot_id, e)
                if job_id:
                    await svc._update_job(job_id, status="failed", error=str(e))
            finally:
                _shared._webhook_inflight -= 1

        background_tasks.add_task(_download_and_process)

    return {"ok": True}

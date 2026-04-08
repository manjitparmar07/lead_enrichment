"""
lead_enrichment_brightdata_routes.py
--------------------------------------
FastAPI router for Worksbuddy Lead Enrichment (Bright Data powered).

This file is now a thin assembly layer:
  - Re-exports all shared symbols from _shared.py (for backward compatibility
    with any code that imports directly from this module).
  - Imports sub-routers and includes them into the main `router`.
  - Re-exports the 4 special view routers that main.py mounts separately.

Route implementations live in:
  _routes_enrich.py   — enrich stream/single/bulk + job management
  _routes_webhooks.py — brightdata + notify webhooks
  _routes_leads.py    — queue stats, lead list/CRUD/export, notes
  _routes_view.py     — 4 view sub-routers (linkedin/email/outreach/company)
  _routes_lio.py      — LIO prompt/config/workspace/status/analyze routes

Shared setup (Redis cache, JWT helpers, LIO helpers, models) lives in _shared.py.

Endpoints:
  POST   /api/leads/enrich            — single LinkedIn URL (sync, ~60s)
  POST   /api/leads/enrich/bulk       — bulk URLs (async, returns job_id)
  GET    /api/leads/jobs              — list all jobs
  GET    /api/leads/jobs/{job_id}     — job status + progress
  POST   /api/leads/jobs/{job_id}/stop  — cancel a running job + BrightData snapshot
  POST   /api/leads/jobs/{job_id}/rerun — rerun a failed/cancelled job via BrightData
  POST   /api/leads/webhook/brightdata — Bright Data batch results webhook
  POST   /api/leads/webhook/notify    — Bright Data snapshot-ready notification
  GET    /api/leads                   — list enriched leads (filterable)
  GET    /api/leads/{lead_id}         — single lead detail
  POST   /api/leads/{lead_id}/outreach — regenerate outreach copy
  DELETE /api/leads/{lead_id}         — delete lead
  GET    /api/leads/export/csv        — export leads as CSV
"""

from __future__ import annotations

from fastapi import APIRouter

# ── Re-export everything from _shared so existing imports keep working ────────
from Lead_enrichment.bulk_lead_enrichment._shared import (  # noqa: F401
    # Redis cache
    _LEAD_CACHE_TTL,
    _lead_cache_ttl,
    _get_lead_redis,
    _lead_cache_get,
    _lead_cache_set,
    _lead_cache_delete,
    # JWT / org
    _JWT_SECRET,
    _JWT_ALGORITHM,
    WEBHOOK_SECRET,
    MAX_WEBHOOK_INFLIGHT,
    _webhook_inflight,
    _SKIP_LIO_CONCURRENCY,
    _skip_lio_sem,
    _OUTREACH_CONCURRENCY,
    _outreach_sem,
    _get_org_id,
    _decode_token,
    _validate_token,
    # LIO helpers
    _lio_fill,
    _lio_strip_md,
    _LIO_RESULT_KEYS,
    _build_lio_ctx,
    _lio_stage_stream,
    _lio_setup,
    _lio_llm_keys,
    # Models
    _validate_linkedin_url,
    BulkEnrichRequest,
    RegenerateOutreachRequest,
    # Formatter
    _format_lead,
)

# ── Main router ───────────────────────────────────────────────────────────────
router = APIRouter(prefix="/leads", tags=["Lead Enrichment"])

# ── Include sub-routers ───────────────────────────────────────────────────────
from Lead_enrichment.bulk_lead_enrichment._routes_enrich import router as _enrich_router
from Lead_enrichment.bulk_lead_enrichment._routes_webhooks import router as _webhooks_router
from Lead_enrichment.bulk_lead_enrichment._routes_leads import router as _leads_router
from Lead_enrichment.bulk_lead_enrichment._routes_lio import router as _lio_router

router.include_router(_enrich_router)
router.include_router(_webhooks_router)
router.include_router(_leads_router)
router.include_router(_lio_router)

# ── View sub-routers — re-exported for main.py which mounts them separately ──
# main.py does:
#   from Lead_enrichment.bulk_lead_enrichment.lead_enrichment_brightdata_routes import (
#       _linkedin_enrich_router, _email_enrich_router,
#       _outreach_enrich_router, _company_enrich_router,
#   )
from Lead_enrichment.bulk_lead_enrichment._routes_view import (  # noqa: F401
    _linkedin_enrich_router,
    _email_enrich_router,
    _outreach_enrich_router,
    _company_enrich_router,
)

"""
analytics_routes.py
--------------------
Analytics endpoints for the Lead Enrichment platform.

GET /api/leads/analytics          — full dashboard payload
GET /api/leads/analytics/summary  — summary counts only
GET /api/leads/analytics/tiers    — tier distribution
GET /api/leads/analytics/scores   — score breakdown + distribution
GET /api/leads/analytics/trend    — daily enrichment trend
GET /api/leads/analytics/jobs     — job-level stats
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from fastapi import APIRouter, Query, Request

from analytics import analytics_service as svc
from analytics import api_usage_service as _usage_svc

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/leads/analytics", tags=["Analytics"])


# ── Org extraction (same pattern as routes file) ──────────────────────────────

def _get_org_id(request: Request) -> str:
    return "default"


# ─────────────────────────────────────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────────────────────────────────────

@router.get("")
async def full_analytics(
    request: Request,
    trend_days: int = Query(30, ge=7, le=365, description="Days for enrichment trend"),
):
    """
    Complete analytics dashboard payload.

    Returns all metrics in one call:
    - **summary**: totals, avg score, email discovery rate
    - **tier_distribution**: hot/warm/cool/cold counts + percentages
    - **score_breakdown**: avg ICP, intent, timing, engagement sub-scores
    - **score_distribution**: bucketed score histogram (0-19, 20-39, ...)
    - **enrichment_trend**: daily counts for last N days
    - **top_companies**: companies with most leads + avg score
    - **top_industries**: industry frequency breakdown
    - **top_titles**: most common job titles
    - **seniority_breakdown**: C-level, VP, Director, Manager, IC counts
    - **job_stats**: total jobs, success rate, failure rate
    """
    org_id = _get_org_id(request)
    return await svc.get_full_analytics(org_id, trend_days=trend_days)


@router.get("/summary")
async def analytics_summary(request: Request):
    """
    High-level summary counts only.

    Returns:
    - `total_leads` — total enriched leads for this org
    - `enriched_last_7d` / `enriched_last_30d` — recent activity
    - `avg_total_score` — average lead score (0-100)
    - `email_discovery_rate` — fraction of leads with a work email (0-1)
    - `avg_data_completeness` — average data completeness score
    """
    org_id = _get_org_id(request)
    return await svc.get_summary(org_id)


@router.get("/tiers")
async def analytics_tiers(request: Request):
    """
    Hot / warm / cool / cold tier distribution.

    Each tier contains `count` and `pct` (percentage of total).
    """
    org_id = _get_org_id(request)
    return await svc.get_tier_distribution(org_id)


@router.get("/scores")
async def analytics_scores(request: Request):
    """
    Score breakdown (averages) + histogram distribution.
    """
    org_id = _get_org_id(request)
    breakdown = await svc.get_score_breakdown(org_id)
    distribution = await svc.get_score_distribution(org_id)
    return {"breakdown": breakdown, "distribution": distribution}


@router.get("/trend")
async def analytics_trend(
    request: Request,
    days: int = Query(30, ge=7, le=365, description="Number of days"),
):
    """
    Daily enrichment counts for the past N days.

    Returns a list of `{ "date": "YYYY-MM-DD", "count": N }` objects,
    zero-filled for days with no enrichments.
    """
    org_id = _get_org_id(request)
    return await svc.get_enrichment_trend(org_id, days=days)


@router.get("/jobs")
async def analytics_jobs(request: Request):
    """
    Job-level statistics: total submitted, processed, failed, success rate.
    """
    org_id = _get_org_id(request)
    return await svc.get_job_stats(org_id)


@router.get("/companies")
async def analytics_top_companies(
    request: Request,
    limit: int = Query(10, ge=1, le=50),
):
    """Top companies by lead count + average score."""
    org_id = _get_org_id(request)
    return await svc.get_top_companies(org_id, limit=limit)


@router.get("/industries")
async def analytics_top_industries(
    request: Request,
    limit: int = Query(10, ge=1, le=50),
):
    """Industry frequency breakdown."""
    org_id = _get_org_id(request)
    return await svc.get_top_industries(org_id, limit=limit)


@router.get("/titles")
async def analytics_top_titles(
    request: Request,
    limit: int = Query(15, ge=1, le=100),
):
    """Most common job titles."""
    org_id = _get_org_id(request)
    return await svc.get_top_titles(org_id, limit=limit)


@router.get("/seniority")
async def analytics_seniority(request: Request):
    """Seniority level breakdown with avg scores."""
    org_id = _get_org_id(request)
    return await svc.get_seniority_breakdown(org_id)


# ── API Usage endpoints ────────────────────────────────────────────────────────

@router.get("/api-usage")
async def api_usage_summary(
    days: int = Query(30, ge=1, le=365, description="Number of days to include"),
):
    """
    API call counts per service for the last N days.

    Returns totals + breakdown by call_type for:
    - brightdata (profile, company)
    - apollo (person_match, org_enrich)
    - huggingface (llm_call)
    - validemail (verify)
    """
    return await _usage_svc.get_summary(days)


@router.get("/api-usage/detail")
async def api_usage_detail(
    days: int = Query(30, ge=1, le=365, description="Number of days to include"),
):
    """Raw daily rows from api_usage_log — one row per (api, call_type, day)."""
    return await _usage_svc.get_usage(days)

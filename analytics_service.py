"""
analytics_service.py
--------------------
Lead enrichment analytics — aggregated stats per org.

Queries enriched_leads + enrichment_jobs for dashboard-ready metrics:
  - Tier distribution (hot / warm / cool / cold)
  - Score breakdown (avg ICP, intent, timing, engagement)
  - Email discovery rate
  - Enrichment trend (daily counts for last N days)
  - Top companies / industries / job titles
  - Seniority breakdown
  - Job-level success stats
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from db import get_pool

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _pct(part: int, total: int) -> float:
    return round(part / total * 100, 1) if total else 0.0


def _safe_avg(value: Any) -> float:
    try:
        return round(float(value), 1) if value is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────

async def get_summary(org_id: str) -> dict:
    """
    High-level counts: total leads, recent enrichments, avg score, email rate.
    """
    now = datetime.now(timezone.utc)
    cutoff_7d  = (now - timedelta(days=7)).isoformat()
    cutoff_30d = (now - timedelta(days=30)).isoformat()

    async with get_pool().acquire() as conn:
        row = dict(await conn.fetchrow(
            "SELECT COUNT(*) total, AVG(total_score) avg_score, AVG(data_completeness_score) avg_dc "
            "FROM enriched_leads WHERE organization_id=$1",
            org_id,
        ))
        last_7d = await conn.fetchval(
            "SELECT COUNT(*) FROM enriched_leads WHERE organization_id=$1 AND enriched_at>=$2",
            org_id, cutoff_7d,
        )
        last_30d = await conn.fetchval(
            "SELECT COUNT(*) FROM enriched_leads WHERE organization_id=$1 AND enriched_at>=$2",
            org_id, cutoff_30d,
        )
        with_email = await conn.fetchval(
            "SELECT COUNT(*) FROM enriched_leads WHERE organization_id=$1 AND work_email IS NOT NULL AND work_email!=''",
            org_id,
        )
        verified_emails = await conn.fetchval(
            "SELECT COUNT(*) FROM enriched_leads WHERE organization_id=$1 AND email_verified=1",
            org_id,
        )

    total = int(row["total"] or 0)
    email_rate = round(with_email / total, 3) if total else 0.0

    return {
        "total_leads": total,
        "enriched_last_7d": int(last_7d),
        "enriched_last_30d": int(last_30d),
        "avg_total_score": _safe_avg(row["avg_score"]),
        "avg_data_completeness": _safe_avg(row["avg_dc"]),
        "email_discovery_rate": email_rate,
        "email_discovery_pct": round(email_rate * 100, 1),
        "verified_email_count": int(verified_emails),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Tier Distribution
# ─────────────────────────────────────────────────────────────────────────────

async def get_tier_distribution(org_id: str) -> dict:
    """
    Counts + percentages for hot / warm / cool / cold leads.
    """
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            "SELECT score_tier, COUNT(*) cnt FROM enriched_leads "
            "WHERE organization_id=$1 GROUP BY score_tier",
            org_id,
        )

    counts: dict[str, int] = {t: 0 for t in ("hot", "warm", "cool", "cold")}
    for row in rows:
        tier = (row[0] or "cold").lower()
        if tier in counts:
            counts[tier] = int(row[1])
        else:
            counts["cold"] += int(row[1])

    total = sum(counts.values())
    return {
        tier: {"count": cnt, "pct": _pct(cnt, total)}
        for tier, cnt in counts.items()
    }


# ─────────────────────────────────────────────────────────────────────────────
# Score Breakdown
# ─────────────────────────────────────────────────────────────────────────────

async def get_score_breakdown(org_id: str) -> dict:
    """
    Average sub-scores across all leads for the org.
    """
    async with get_pool().acquire() as conn:
        row = dict(await conn.fetchrow(
            """SELECT
                AVG(icp_fit_score)    avg_icp,
                AVG(intent_score)     avg_intent,
                AVG(timing_score)     avg_timing,
                AVG(engagement_score) avg_engagement,
                AVG(total_score)      avg_total,
                MAX(total_score)      max_score,
                MIN(total_score)      min_score,
                COUNT(*)              cnt
            FROM enriched_leads WHERE organization_id=$1""",
            org_id,
        ))

    return {
        "avg_icp_score":        _safe_avg(row["avg_icp"]),
        "avg_intent_score":     _safe_avg(row["avg_intent"]),
        "avg_timing_score":     _safe_avg(row["avg_timing"]),
        "avg_engagement_score": _safe_avg(row["avg_engagement"]),
        "avg_total_score":      _safe_avg(row["avg_total"]),
        "max_score":            int(row["max_score"] or 0),
        "min_score":            int(row["min_score"] or 0),
        "leads_scored":         int(row["cnt"] or 0),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Enrichment Trend
# ─────────────────────────────────────────────────────────────────────────────

async def get_enrichment_trend(org_id: str, days: int = 30) -> list[dict]:
    """
    Daily enrichment counts for the past `days` days.
    Returns a list sorted oldest → newest, with zero-fill for missing days.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            """SELECT SUBSTRING(enriched_at, 1, 10) day, COUNT(*) cnt
               FROM enriched_leads
               WHERE organization_id=$1 AND enriched_at>=$2
               GROUP BY day ORDER BY day""",
            org_id, cutoff,
        )

    today = datetime.now(timezone.utc).date()
    date_map: dict[str, int] = {}
    for i in range(days):
        d = (today - timedelta(days=days - 1 - i)).isoformat()
        date_map[d] = 0
    for row in rows:
        day = row[0]
        if day and day in date_map:
            date_map[day] = int(row[1])

    return [{"date": d, "count": c} for d, c in date_map.items()]


# ─────────────────────────────────────────────────────────────────────────────
# Top Lists
# ─────────────────────────────────────────────────────────────────────────────

async def get_top_companies(org_id: str, limit: int = 10) -> list[dict]:
    """Most common companies in enriched leads."""
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            """SELECT company, COUNT(*) cnt, AVG(total_score) avg_score
               FROM enriched_leads
               WHERE organization_id=$1 AND company IS NOT NULL AND company!=''
               GROUP BY company ORDER BY cnt DESC LIMIT $2""",
            org_id, limit,
        )
    return [
        {"company": r[0], "lead_count": int(r[1]), "avg_score": _safe_avg(r[2])}
        for r in rows
    ]


async def get_top_industries(org_id: str, limit: int = 10) -> list[dict]:
    """Most common industries across enriched leads."""
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            """SELECT industry, COUNT(*) cnt
               FROM enriched_leads
               WHERE organization_id=$1 AND industry IS NOT NULL AND industry!=''
               GROUP BY industry ORDER BY cnt DESC LIMIT $2""",
            org_id, limit,
        )
    return [{"industry": r[0], "count": int(r[1])} for r in rows]


async def get_top_titles(org_id: str, limit: int = 15) -> list[dict]:
    """Most common job titles."""
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            """SELECT title, COUNT(*) cnt
               FROM enriched_leads
               WHERE organization_id=$1 AND title IS NOT NULL AND title!=''
               GROUP BY title ORDER BY cnt DESC LIMIT $2""",
            org_id, limit,
        )
    return [{"title": r[0], "count": int(r[1])} for r in rows]


async def get_seniority_breakdown(org_id: str) -> list[dict]:
    """Counts per seniority level."""
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            """SELECT seniority_level, COUNT(*) cnt, AVG(total_score) avg_score
               FROM enriched_leads
               WHERE organization_id=$1 AND seniority_level IS NOT NULL AND seniority_level!=''
               GROUP BY seniority_level ORDER BY cnt DESC""",
            org_id,
        )
    return [
        {"seniority": r[0], "count": int(r[1]), "avg_score": _safe_avg(r[2])}
        for r in rows
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Job Stats
# ─────────────────────────────────────────────────────────────────────────────

async def get_job_stats(org_id: str) -> dict:
    """
    Aggregate stats across all enrichment jobs for the org.
    """
    async with get_pool().acquire() as conn:
        row = dict(await conn.fetchrow(
            """SELECT
                COUNT(*)          total_jobs,
                SUM(total_urls)   total_submitted,
                SUM(processed)    total_processed,
                SUM(failed)       total_failed,
                COUNT(CASE WHEN status='completed'              THEN 1 END) completed,
                COUNT(CASE WHEN status='failed'                 THEN 1 END) failed_jobs,
                COUNT(CASE WHEN status='running'                THEN 1 END) running,
                COUNT(CASE WHEN status='completed_with_errors'  THEN 1 END) partial
            FROM enrichment_jobs WHERE organization_id=$1""",
            org_id,
        ))

    total_sub  = int(row["total_submitted"] or 0)
    total_proc = int(row["total_processed"] or 0)
    total_fail = int(row["total_failed"] or 0)

    return {
        "total_jobs":       int(row["total_jobs"] or 0),
        "completed_jobs":   int(row["completed"] or 0),
        "failed_jobs":      int(row["failed_jobs"] or 0),
        "running_jobs":     int(row["running"] or 0),
        "partial_jobs":     int(row["partial"] or 0),
        "total_submitted":  total_sub,
        "total_processed":  total_proc,
        "total_failed":     total_fail,
        "success_rate":     _pct(total_proc, total_sub),
        "failure_rate":     _pct(total_fail, total_sub),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Score Distribution Buckets
# ─────────────────────────────────────────────────────────────────────────────

async def get_score_distribution(org_id: str) -> list[dict]:
    """
    Buckets: 0-19, 20-39, 40-59, 60-79, 80-100
    """
    buckets = [
        ("0-19",   0,  19),
        ("20-39",  20, 39),
        ("40-59",  40, 59),
        ("60-79",  60, 79),
        ("80-100", 80, 100),
    ]
    result = []
    async with get_pool().acquire() as conn:
        for label, lo, hi in buckets:
            cnt = await conn.fetchval(
                "SELECT COUNT(*) FROM enriched_leads WHERE organization_id=$1 AND total_score>=$2 AND total_score<=$3",
                org_id, lo, hi,
            )
            result.append({"range": label, "count": int(cnt)})
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Full Analytics Response
# ─────────────────────────────────────────────────────────────────────────────

async def get_full_analytics(org_id: str, trend_days: int = 30) -> dict:
    """
    Aggregate all analytics into one response for the dashboard.
    """
    (
        summary,
        tier_dist,
        score_bk,
        trend,
        top_companies,
        top_industries,
        top_titles,
        seniority,
        job_stats,
        score_dist,
    ) = await _gather(
        get_summary(org_id),
        get_tier_distribution(org_id),
        get_score_breakdown(org_id),
        get_enrichment_trend(org_id, days=trend_days),
        get_top_companies(org_id),
        get_top_industries(org_id),
        get_top_titles(org_id),
        get_seniority_breakdown(org_id),
        get_job_stats(org_id),
        get_score_distribution(org_id),
    )

    return {
        "summary":             summary,
        "tier_distribution":   tier_dist,
        "score_breakdown":     score_bk,
        "score_distribution":  score_dist,
        "enrichment_trend":    trend,
        "top_companies":       top_companies,
        "top_industries":      top_industries,
        "top_titles":          top_titles,
        "seniority_breakdown": seniority,
        "job_stats":           job_stats,
    }


async def _gather(*coros):
    """Run multiple async calls concurrently."""
    import asyncio
    return await asyncio.gather(*coros)

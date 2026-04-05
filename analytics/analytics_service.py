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

# ── Redis cache (optional — silently disabled if Redis unavailable) ────────────
_REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
_redis_client: Any = None


async def _redis() -> Any:
    """Lazy-init Redis client. Returns None if unavailable."""
    global _redis_client
    if _redis_client is not None:
        return _redis_client
    try:
        import redis.asyncio as aioredis
        client = aioredis.from_url(_REDIS_URL, decode_responses=True)
        await client.ping()
        _redis_client = client
    except Exception:
        _redis_client = None
    return _redis_client


async def _cache_get(key: str) -> Any:
    r = await _redis()
    if not r:
        return None
    try:
        raw = await r.get(key)
        return json.loads(raw) if raw else None
    except Exception:
        return None


async def _cache_set(key: str, value: Any, ttl: int = 300) -> None:
    r = await _redis()
    if not r:
        return
    try:
        await r.setex(key, ttl, json.dumps(value, default=str))
    except Exception:
        pass


async def invalidate_analytics_cache(org_id: str) -> None:
    """Call this after enriching/deleting leads to bust stale analytics."""
    r = await _redis()
    if not r:
        return
    try:
        keys = await r.keys(f"analytics:{org_id}:*")
        if keys:
            await r.delete(*keys)
    except Exception:
        pass

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
    Single query with conditional aggregates — was 5 round-trips.
    """
    cache_key = f"analytics:{org_id}:summary"
    cached = await _cache_get(cache_key)
    if cached:
        return cached

    now = datetime.now(timezone.utc)
    cutoff_7d  = (now - timedelta(days=7)).isoformat()
    cutoff_30d = (now - timedelta(days=30)).isoformat()

    async with get_pool().acquire() as conn:
        row = dict(await conn.fetchrow(
            """SELECT
                COUNT(*)                                                                    AS total,
                COUNT(*) FILTER (WHERE enriched_at >= $2)                                  AS last_7d,
                COUNT(*) FILTER (WHERE enriched_at >= $3)                                  AS last_30d,
                COUNT(*) FILTER (WHERE work_email IS NOT NULL AND work_email != '')         AS with_email,
                COUNT(*) FILTER (WHERE email_verified = 1)                                 AS verified,
                AVG(total_score)             AS avg_score,
                AVG(data_completeness_score) AS avg_dc
            FROM enriched_leads WHERE organization_id=$1""",
            org_id, cutoff_7d, cutoff_30d,
        ))

    total      = int(row["total"] or 0)
    with_email = int(row["with_email"] or 0)
    email_rate = round(with_email / total, 3) if total else 0.0

    result = {
        "total_leads":           total,
        "enriched_last_7d":      int(row["last_7d"] or 0),
        "enriched_last_30d":     int(row["last_30d"] or 0),
        "avg_total_score":       _safe_avg(row["avg_score"]),
        "avg_data_completeness": _safe_avg(row["avg_dc"]),
        "email_discovery_rate":  email_rate,
        "email_discovery_pct":   round(email_rate * 100, 1),
        "verified_email_count":  int(row["verified"] or 0),
    }
    await _cache_set(cache_key, result, ttl=300)
    return result


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
    Single query with FILTER aggregates — was 5 round-trips.
    """
    async with get_pool().acquire() as conn:
        row = dict(await conn.fetchrow(
            """SELECT
                COUNT(*) FILTER (WHERE total_score BETWEEN  0 AND  19) AS b0,
                COUNT(*) FILTER (WHERE total_score BETWEEN 20 AND  39) AS b1,
                COUNT(*) FILTER (WHERE total_score BETWEEN 40 AND  59) AS b2,
                COUNT(*) FILTER (WHERE total_score BETWEEN 60 AND  79) AS b3,
                COUNT(*) FILTER (WHERE total_score BETWEEN 80 AND 100) AS b4
            FROM enriched_leads WHERE organization_id=$1""",
            org_id,
        ))
    return [
        {"range": label, "count": int(row[f"b{i}"] or 0)}
        for i, label in enumerate(["0-19", "20-39", "40-59", "60-79", "80-100"])
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Full Analytics Response
# ─────────────────────────────────────────────────────────────────────────────

async def get_full_analytics(org_id: str, trend_days: int = 30) -> dict:
    """
    Aggregate all analytics into one response for the dashboard.
    Cached in Redis for 5 minutes per org.
    """
    cache_key = f"analytics:{org_id}:full:{trend_days}"
    cached = await _cache_get(cache_key)
    if cached:
        return cached

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

    result = {
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
    await _cache_set(cache_key, result, ttl=300)
    return result


async def _gather(*coros):
    """Run multiple async calls concurrently."""
    import asyncio
    return await asyncio.gather(*coros)

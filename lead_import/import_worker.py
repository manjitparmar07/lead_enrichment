"""
import_worker.py
----------------
Redis-backed import chunk workers.

Flow:
  1. import_service._stage_and_chunk() runs after POST /start:
       - Parses file → COPY all rows to import_stage_{job_id}
       - Splits into 50K-row chunk staging tables
       - RPUSHes task metadata to wb:import:chunks

  2. 4 workers here BLPOP from wb:import:chunks

  3. Each worker:
       - Runs INSERT SELECT (with xmax dedup counting) from chunk staging table
       - Drops chunk staging table
       - HINCRBYs progress in wb:import:job:{job_id}
       - If last chunk → writes final counts to DB → marks job completed

Redis keys:
  wb:import:chunks              List  task queue (JSON)
  wb:import:job:{job_id}        Hash  done, total_chunks, new_count, updated_count, skipped, total
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)

QUEUE_KEY    = "wb:import:chunks"
JOB_KEY      = "wb:import:job:{job_id}"
WORKER_COUNT = int(os.getenv("IMPORT_WORKER_COUNT", "4"))
REDIS_URL    = os.getenv("REDIS_URL", "redis://localhost:6379/0")

_worker_tasks: list[asyncio.Task] = []


# ─────────────────────────────────────────────────────────────────────────────
# Redis connection helper
# ─────────────────────────────────────────────────────────────────────────────

async def _make_redis() -> Any:
    import redis.asyncio as aioredis
    return aioredis.from_url(REDIS_URL, decode_responses=True)


# ─────────────────────────────────────────────────────────────────────────────
# INSERT SQL (shared with import_service for consistency)
# ─────────────────────────────────────────────────────────────────────────────

def _insert_sql(staging: str) -> str:
    """Return the INSERT SELECT + xmax CTE SQL for one chunk staging table."""
    return f"""
        WITH upsert AS (
            INSERT INTO enriched_leads (
                id, organization_id,
                linkedin_url, name, first_name, last_name,
                work_email, personal_email, direct_phone, twitter,
                title, seniority_level, department,
                city, country, timezone,
                company, industry, company_website,
                employee_count, hq_location, founded_year,
                funding_stage, total_funding, annual_revenue,
                tags, about, email_source, email_confidence,
                connections, followers,
                lead_source, enrichment_source, status, created_at, enriched_at
            )
            SELECT
                CASE
                    WHEN NULLIF(TRIM(linkedin_url),'') IS NOT NULL
                        THEN LEFT(MD5(LOWER(TRIM(linkedin_url))), 16)
                    WHEN NULLIF(LOWER(TRIM(work_email)),'') IS NOT NULL
                        THEN LEFT(MD5(LOWER(TRIM(work_email))), 16)
                    ELSE _row_uuid
                END,
                $1,
                NULLIF(TRIM(linkedin_url),''),
                NULLIF(TRIM(name),''), NULLIF(TRIM(first_name),''), NULLIF(TRIM(last_name),''),
                LOWER(NULLIF(TRIM(work_email),'')),
                LOWER(NULLIF(TRIM(personal_email),'')),
                NULLIF(TRIM(direct_phone),''), NULLIF(TRIM(twitter),''),
                NULLIF(TRIM(title),''), NULLIF(TRIM(seniority_level),''), NULLIF(TRIM(department),''),
                NULLIF(TRIM(city),''), NULLIF(TRIM(country),''), NULLIF(TRIM(timezone),''),
                NULLIF(TRIM(company),''), NULLIF(TRIM(industry),''), NULLIF(TRIM(company_website),''),
                CASE WHEN employee_count ~ '^[0-9]+$' THEN employee_count::integer ELSE 0 END,
                NULLIF(TRIM(hq_location),''), NULLIF(TRIM(founded_year),''),
                NULLIF(TRIM(funding_stage),''), NULLIF(TRIM(total_funding),''),
                NULLIF(TRIM(annual_revenue),''),
                NULLIF(TRIM(tags),''), NULLIF(TRIM(about),''),
                NULLIF(TRIM(email_source),''), NULLIF(TRIM(email_confidence),''),
                CASE WHEN connections ~ '^[0-9]+$' THEN connections::integer ELSE 0 END,
                CASE WHEN followers ~ '^[0-9]+$' THEN followers::integer ELSE 0 END,
                'Import', $2, 'enriched', $3, $3
            FROM {staging}
            WHERE TRIM(COALESCE(linkedin_url, work_email, personal_email, '')) != ''
            ON CONFLICT (id) DO UPDATE SET
                name            = COALESCE(EXCLUDED.name,            enriched_leads.name),
                first_name      = COALESCE(EXCLUDED.first_name,      enriched_leads.first_name),
                last_name       = COALESCE(EXCLUDED.last_name,       enriched_leads.last_name),
                work_email      = COALESCE(EXCLUDED.work_email,      enriched_leads.work_email),
                personal_email  = COALESCE(EXCLUDED.personal_email,  enriched_leads.personal_email),
                direct_phone    = COALESCE(EXCLUDED.direct_phone,    enriched_leads.direct_phone),
                twitter         = COALESCE(EXCLUDED.twitter,         enriched_leads.twitter),
                title           = COALESCE(EXCLUDED.title,           enriched_leads.title),
                seniority_level = COALESCE(EXCLUDED.seniority_level, enriched_leads.seniority_level),
                department      = COALESCE(EXCLUDED.department,      enriched_leads.department),
                city            = COALESCE(EXCLUDED.city,            enriched_leads.city),
                country         = COALESCE(EXCLUDED.country,         enriched_leads.country),
                company         = COALESCE(EXCLUDED.company,         enriched_leads.company),
                industry        = COALESCE(EXCLUDED.industry,        enriched_leads.industry),
                company_website = COALESCE(EXCLUDED.company_website, enriched_leads.company_website),
                employee_count  = COALESCE(EXCLUDED.employee_count,  enriched_leads.employee_count),
                tags            = COALESCE(EXCLUDED.tags,            enriched_leads.tags),
                about           = COALESCE(EXCLUDED.about,           enriched_leads.about),
                enriched_at     = EXCLUDED.enriched_at
            RETURNING (xmax = 0)::int AS is_new
        )
        SELECT
            SUM(is_new)::int              AS new_count,
            (COUNT(*) - SUM(is_new))::int AS updated_count
        FROM upsert
    """


# ─────────────────────────────────────────────────────────────────────────────
# Core chunk processor
# ─────────────────────────────────────────────────────────────────────────────

async def _process_chunk(pool: Any, r: Any, raw: str) -> None:
    task         = json.loads(raw)
    job_id       = task["job_id"]
    total_chunks = int(task["total_chunks"])
    staging      = task["staging"]
    org_id       = task["org_id"]
    filename     = task["filename"]
    now          = task["now"]

    try:
        async with pool.acquire() as conn:
            await conn.execute("SET synchronous_commit = OFF")
            row = await conn.fetchrow(_insert_sql(staging), org_id, filename, now)
            await conn.execute(f"DROP TABLE IF EXISTS {staging}")

        new_count     = int(row["new_count"]     or 0)
        updated_count = int(row["updated_count"] or 0)

        # Write counts directly to DB — survives server restarts
        async with pool.acquire() as conn:
            await conn.execute(
                """UPDATE enrichment_jobs
                   SET new_count     = new_count     + $1,
                       updated_count = updated_count + $2,
                       processed     = processed     + $3,
                       updated_at    = $4
                   WHERE id = $5""",
                new_count, updated_count, new_count + updated_count, now, job_id,
            )

        # Track chunk completion in Redis (only for last-chunk detection)
        done = int(await r.hincrby(f"wb:import:job:{job_id}", "done", 1))

        logger.info("[import-worker] %s chunk %d/%d done — %d new, %d updated",
                    job_id[:8], done, total_chunks, new_count, updated_count)

        # Last chunk → finalise job
        if done >= total_chunks:
            await _finalise(pool, r, job_id, now)

    except Exception as exc:
        err_str = str(exc)
        # Detect disk-full errors early and surface a clear message
        if "No space left" in err_str or "FileFallocate" in err_str or "ENOSPC" in err_str:
            err_str = (
                "Database disk full — no space left on device. "
                "Free up space or upgrade your database plan, then retry."
            )
        logger.exception("[import-worker] chunk failed for job %s: %s", job_id[:8], exc)
        # Drop orphaned staging table
        try:
            async with pool.acquire() as conn:
                await conn.execute(f"DROP TABLE IF EXISTS {staging}")
        except Exception:
            pass
        # Check if ALL chunks have been attempted (done + 1 >= total)
        done = int(await r.hincrby(f"wb:import:job:{job_id}", "done", 1))
        if done >= total_chunks:
            await _finalise(pool, r, job_id, now, error=err_str[:500])
        else:
            # Partial failure — mark job as failed now so user sees the error
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE enrichment_jobs SET status='failed', error=$1, updated_at=$2 WHERE id=$3",
                    err_str[:500], now, job_id,
                )


async def _finalise(pool: Any, r: Any, job_id: str, now: str, error: Optional[str] = None) -> None:
    """Mark job complete/failed. Counts are already in DB from each chunk update."""
    status = "failed" if error else "completed"

    async with pool.acquire() as conn:
        # Fetch current counts from DB (already accumulated per-chunk)
        row = await conn.fetchrow(
            "SELECT new_count, updated_count FROM enrichment_jobs WHERE id=$1", job_id
        )
        new_count     = int(row["new_count"]     or 0) if row else 0
        updated_count = int(row["updated_count"] or 0) if row else 0

        await conn.execute(
            """UPDATE enrichment_jobs
               SET status=$1, error=$2, updated_at=$3
               WHERE id=$4""",
            status, error, now, job_id,
        )

    # Clean up Redis job state
    await r.delete(f"wb:import:job:{job_id}")
    logger.info("[import-worker] job %s finalised — %s  new=%d  updated=%d",
                job_id[:8], status, new_count, updated_count)


# ─────────────────────────────────────────────────────────────────────────────
# Worker loop
# ─────────────────────────────────────────────────────────────────────────────

async def _worker(worker_id: int) -> None:
    from db import get_pool
    r    = await _make_redis()
    pool = get_pool()
    logger.info("[import-worker-%d] started — watching %s", worker_id, QUEUE_KEY)

    while True:
        try:
            result = await r.blpop(QUEUE_KEY, timeout=2)
            if result is None:
                continue
            _, raw = result
            await _process_chunk(pool, r, raw)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.exception("[import-worker-%d] unhandled: %s", worker_id, exc)
            await asyncio.sleep(1)

    await r.aclose()
    logger.info("[import-worker-%d] stopped", worker_id)


# ─────────────────────────────────────────────────────────────────────────────
# Lifecycle
# ─────────────────────────────────────────────────────────────────────────────

async def start_import_workers() -> None:
    global _worker_tasks
    _worker_tasks = [
        asyncio.create_task(_worker(i), name=f"import-worker-{i}")
        for i in range(1, WORKER_COUNT + 1)
    ]
    logger.info("[ImportWorkers] %d workers started — queue: %s", WORKER_COUNT, QUEUE_KEY)


async def stop_import_workers() -> None:
    for t in _worker_tasks:
        t.cancel()
    if _worker_tasks:
        await asyncio.gather(*_worker_tasks, return_exceptions=True)
    _worker_tasks.clear()
    logger.info("[ImportWorkers] stopped")

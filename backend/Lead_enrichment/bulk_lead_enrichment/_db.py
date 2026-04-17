"""
_db.py
------
All database operations for lead enrichment.

Covers:
  - Schema init (init_leads_db)
  - Lead upsert (_upsert_lead), get, list, delete
  - Job / sub-job CRUD (_update_job, _create_sub_job, _update_sub_job, list_sub_jobs, get_job, list_jobs)
  - LIO results (get_lead_lio_results, save_lead_lio_results)
  - Duplicate detection (check_existing_lead, check_existing_leads_batch, get_stale_leads)
  - Snapshot idempotency (mark_snapshot_processed, is_snapshot_processed)
  - Audit log (audit_log, list_audit_log)
  - Lead notes (add_lead_note, list_lead_notes, delete_lead_note)
  - Redis helpers (_get_redis, _push_lead_task, _publish_lead_done, _publish_job_done)
  - LinkedIn enrichment view formatter (_format_linkedin_enrich)
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

from db import get_pool

from ._utils import (
    _clean_lead_strings,
    _normalise_person_text,
    _lead_id,
    _parse_json_safe,
    _pipeline_log,
)

try:
    import redis.asyncio as aioredis
    _REDIS_AVAILABLE = True
except ImportError:
    _REDIS_AVAILABLE = False

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
_CACHE_TTL_DAYS: int = int(os.getenv("LEAD_CACHE_TTL_DAYS", "90"))

_redis_pool: Any = None


# ─────────────────────────────────────────────────────────────────────────────
# Redis helpers
# ─────────────────────────────────────────────────────────────────────────────

async def _get_redis() -> Any:
    """Lazy-init async Redis client. Returns None if unavailable."""
    global _redis_pool
    if not _REDIS_AVAILABLE:
        return None
    if _redis_pool is None:
        try:
            _redis_pool = aioredis.from_url(REDIS_URL, decode_responses=True)
            await _redis_pool.ping()
            logger.info("[Redis] Connected: %s", REDIS_URL)
        except Exception as e:
            logger.warning("[Redis] Not available (%s) — in-process fallback active", e)
            _redis_pool = None
    return _redis_pool


async def _push_lead_task(queue: str, task: dict) -> bool:
    """Push a lead task to a Redis queue. Returns True if pushed."""
    r = await _get_redis()
    if not r:
        return False
    try:
        await r.rpush(queue, json.dumps(task))
        return True
    except Exception as e:
        logger.warning("[Redis] Push failed: %s", e)
        return False


async def _publish_lead_done(org_id: str, job_id: str, lead: dict) -> None:
    """Publish lead:done event to Ably — real-time frontend update."""
    try:
        from realtime import ably_service
        channel = f"tenant:{org_id}:job:{job_id}"
        await ably_service.publish(channel, "lead:done", {
            "job_id":         job_id,
            "org_id":         org_id,
            "lead_id":        lead.get("id"),
            "linkedin_url":   lead.get("linkedin_url"),
            "name":           lead.get("name"),
            "score":          lead.get("total_score"),
            "tier":           lead.get("score_tier"),
            "email":          lead.get("work_email"),
            "company":        lead.get("company"),
            "title":          lead.get("title"),
            "cache_hit":      bool(lead.get("_cache_hit")),
            "linkedin_enrich": lead.get("linkedin_enrich"),
        })
    except Exception as e:
        logger.debug("[Ably] lead:done publish failed: %s", e)


async def _publish_job_done(org_id: str, job_id: str, processed: int, failed: int) -> None:
    """Publish job:done event when all leads in a bulk job are processed."""
    try:
        from realtime import ably_service
        channel = f"tenant:{org_id}:job:{job_id}"
        await ably_service.publish(channel, "job:done", {
            "job_id":    job_id,
            "org_id":    org_id,
            "processed": processed,
            "failed":    failed,
        })
    except Exception as e:
        logger.debug("[Ably] job:done publish failed: %s", e)


# ─────────────────────────────────────────────────────────────────────────────
# Schema init
# ─────────────────────────────────────────────────────────────────────────────

async def init_leads_db() -> None:
    async with get_pool().acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS enriched_leads (
                id               TEXT PRIMARY KEY,
                linkedin_url     TEXT NOT NULL,
                -- Identity
                name             TEXT,
                first_name       TEXT,
                last_name        TEXT,
                work_email       TEXT,
                personal_email   TEXT,
                direct_phone     TEXT,
                twitter          TEXT,
                city             TEXT,
                country          TEXT,
                timezone         TEXT,
                -- Professional
                title            TEXT,
                seniority_level  TEXT,
                department       TEXT,
                years_in_role    TEXT,
                company          TEXT,
                previous_companies TEXT,
                top_skills       TEXT,
                education        TEXT,
                certifications   TEXT,
                languages        TEXT,
                -- Company
                company_website  TEXT,
                industry         TEXT,
                employee_count   INTEGER DEFAULT 0,
                hq_location      TEXT,
                founded_year     TEXT,
                funding_stage    TEXT,
                total_funding    TEXT,
                last_funding_date TEXT,
                lead_investor    TEXT,
                annual_revenue   TEXT,
                tech_stack       TEXT,
                hiring_velocity  TEXT,
                -- Company enrichment (waterfall)
                avatar_url       TEXT,
                company_logo     TEXT,
                company_email    TEXT,
                company_description TEXT,
                company_linkedin TEXT,
                company_twitter  TEXT,
                company_phone    TEXT,
                waterfall_log    TEXT,
                -- Website Intelligence (Stage 3)
                website_intelligence TEXT,
                product_offerings TEXT,
                value_proposition TEXT,
                target_customers TEXT,
                business_model   TEXT,
                pricing_signals  TEXT,
                product_category TEXT,
                data_completeness_score INTEGER DEFAULT 0,
                -- Intent
                recent_funding_event TEXT,
                hiring_signal    TEXT,
                job_change       TEXT,
                linkedin_activity TEXT,
                news_mention     TEXT,
                product_launch   TEXT,
                competitor_usage TEXT,
                review_activity  TEXT,
                -- Scoring
                icp_fit_score    INTEGER DEFAULT 0,
                intent_score     INTEGER DEFAULT 0,
                timing_score     INTEGER DEFAULT 0,
                engagement_score INTEGER DEFAULT 0,
                total_score      INTEGER DEFAULT 0,
                score_tier       TEXT,
                score_explanation TEXT,
                icp_match_tier   TEXT,
                disqualification_flags TEXT,
                -- Outreach
                email_subject    TEXT,
                cold_email       TEXT,
                linkedin_note    TEXT,
                best_channel     TEXT,
                best_send_time   TEXT,
                outreach_angle   TEXT,
                sequence_type    TEXT,
                outreach_sequence TEXT,
                last_contacted   TEXT,
                email_status     TEXT,
                -- CRM
                lead_source      TEXT DEFAULT 'LinkedIn URL',
                enrichment_source TEXT,
                data_completeness INTEGER DEFAULT 0,
                crm_stage        TEXT,
                tags             TEXT,
                assigned_owner   TEXT,
                -- Meta
                full_data        TEXT,
                raw_profile      TEXT,
                about            TEXT,
                followers        INTEGER DEFAULT 0,
                connections      INTEGER DEFAULT 0,
                email_source     TEXT,
                email_confidence TEXT,
                status           TEXT DEFAULT 'enriched',
                job_id           TEXT,
                organization_id  TEXT NOT NULL DEFAULT 'default',
                enriched_at      TEXT,
                created_at       TEXT DEFAULT '',
                lio_results_json TEXT
            )
        """)
        await conn.execute("ALTER TABLE enriched_leads ADD COLUMN IF NOT EXISTS lio_results_json TEXT")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS enrichment_jobs (
                id              TEXT PRIMARY KEY,
                snapshot_id     TEXT,
                total_urls      INTEGER DEFAULT 0,
                processed       INTEGER DEFAULT 0,
                failed          INTEGER DEFAULT 0,
                status          TEXT DEFAULT 'pending',
                error           TEXT,
                webhook_url     TEXT,
                organization_id TEXT NOT NULL DEFAULT 'default',
                created_at      TEXT DEFAULT '',
                updated_at      TEXT DEFAULT ''
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS enrichment_sub_jobs (
                id              TEXT PRIMARY KEY,
                job_id          TEXT NOT NULL,
                chunk_index     INTEGER NOT NULL,
                total_urls      INTEGER DEFAULT 0,
                processed       INTEGER DEFAULT 0,
                failed          INTEGER DEFAULT 0,
                status          TEXT DEFAULT 'pending',
                organization_id TEXT NOT NULL DEFAULT 'default',
                created_at      TEXT DEFAULT '',
                updated_at      TEXT DEFAULT ''
            )
        """)
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sub_jobs_job ON enrichment_sub_jobs(job_id)"
        )
        await conn.execute("ALTER TABLE enrichment_sub_jobs ADD COLUMN IF NOT EXISTS snapshot_id TEXT")
        _NEW_COLS = [
            ("work_email", "TEXT"), ("personal_email", "TEXT"),
            ("direct_phone", "TEXT"), ("twitter", "TEXT"),
            ("city", "TEXT"), ("country", "TEXT"), ("timezone", "TEXT"),
            ("seniority_level", "TEXT"), ("department", "TEXT"),
            ("years_in_role", "TEXT"), ("previous_companies", "TEXT"),
            ("top_skills", "TEXT"), ("education", "TEXT"),
            ("certifications", "TEXT"), ("languages", "TEXT"),
            ("company_website", "TEXT"), ("industry", "TEXT"),
            ("employee_count", "INTEGER DEFAULT 0"),
            ("hq_location", "TEXT"), ("founded_year", "TEXT"),
            ("funding_stage", "TEXT"), ("total_funding", "TEXT"),
            ("last_funding_date", "TEXT"), ("lead_investor", "TEXT"),
            ("annual_revenue", "TEXT"), ("tech_stack", "TEXT"),
            ("hiring_velocity", "TEXT"), ("recent_funding_event", "TEXT"),
            ("hiring_signal", "TEXT"), ("job_change", "TEXT"),
            ("linkedin_activity", "TEXT"), ("news_mention", "TEXT"),
            ("product_launch", "TEXT"), ("competitor_usage", "TEXT"),
            ("review_activity", "TEXT"), ("icp_fit_score", "INTEGER DEFAULT 0"),
            ("timing_score", "INTEGER DEFAULT 0"),
            ("score_explanation", "TEXT"), ("icp_match_tier", "TEXT"),
            ("disqualification_flags", "TEXT"), ("email_subject", "TEXT"),
            ("cold_email", "TEXT"), ("linkedin_note", "TEXT"),
            ("best_channel", "TEXT"), ("best_send_time", "TEXT"),
            ("outreach_angle", "TEXT"), ("sequence_type", "TEXT"),
            ("last_contacted", "TEXT"), ("email_status", "TEXT"),
            ("lead_source", "TEXT"), ("enrichment_source", "TEXT"),
            ("data_completeness", "INTEGER DEFAULT 0"),
            ("crm_stage", "TEXT"), ("tags", "TEXT"),
            ("assigned_owner", "TEXT"), ("full_data", "TEXT"),
            ("avatar_url", "TEXT"), ("company_logo", "TEXT"),
            ("company_email", "TEXT"), ("company_description", "TEXT"),
            ("company_linkedin", "TEXT"), ("company_twitter", "TEXT"),
            ("company_phone", "TEXT"), ("waterfall_log", "TEXT"),
            ("website_intelligence", "TEXT"), ("product_offerings", "TEXT"),
            ("value_proposition", "TEXT"), ("target_customers", "TEXT"),
            ("business_model", "TEXT"), ("pricing_signals", "TEXT"),
            ("product_category", "TEXT"), ("data_completeness_score", "INTEGER DEFAULT 0"),
            ("organization_id", "TEXT DEFAULT 'default'"),
            ("activity_feed", "TEXT"), ("auto_tags", "TEXT"),
            ("behavioural_signals", "TEXT"), ("pitch_intelligence", "TEXT"),
            ("warm_signal", "TEXT"), ("email_verified", "INTEGER DEFAULT 0"),
            ("bounce_risk", "TEXT"), ("company_id", "TEXT"),
            ("company_score", "INTEGER DEFAULT 0"), ("combined_score", "INTEGER DEFAULT 0"),
            ("company_tags", "TEXT"), ("culture_signals", "TEXT"),
            ("account_pitch", "TEXT"), ("wappalyzer_tech", "TEXT"),
            ("news_mentions", "TEXT"), ("crunchbase_data", "TEXT"),
            ("linkedin_posts", "TEXT"), ("company_score_tier", "TEXT"),
            ("crm_brief", "TEXT"), ("raw_brightdata", "JSONB"),
            ("apollo_raw", "TEXT"),
            ("raw_website_scrap", "TEXT"),
            ("raw_brightdata_company", "TEXT"),
            ("company_crm_brief", "TEXT"),
            ("updated_at", "TIMESTAMPTZ DEFAULT NOW()"),
            ("lio_sent_at", "TIMESTAMPTZ"),
        ]
        for col, col_type in _NEW_COLS:
            await conn.execute(f"ALTER TABLE enriched_leads ADD COLUMN IF NOT EXISTS {col} {col_type}")
        await conn.execute("ALTER TABLE enrichment_jobs ADD COLUMN IF NOT EXISTS organization_id TEXT DEFAULT 'default'")
        await conn.execute("ALTER TABLE enrichment_jobs ADD COLUMN IF NOT EXISTS sso_id TEXT NOT NULL DEFAULT ''")
        await conn.execute("ALTER TABLE enrichment_jobs ADD COLUMN IF NOT EXISTS forward_to_lio BOOLEAN NOT NULL DEFAULT FALSE")
        await conn.execute("ALTER TABLE enrichment_audit_log ADD COLUMN IF NOT EXISTS step TEXT")
        await conn.execute("ALTER TABLE enrichment_audit_log ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'ok'")
        await conn.execute("ALTER TABLE enrichment_audit_log ADD COLUMN IF NOT EXISTS duration_ms INTEGER")
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS idx_leads_org        ON enriched_leads(organization_id, total_score DESC)",
            "CREATE INDEX IF NOT EXISTS idx_jobs_org         ON enrichment_jobs(organization_id, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_leads_org_date   ON enriched_leads(organization_id, enriched_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_leads_org_tier   ON enriched_leads(organization_id, score_tier)",
            "CREATE INDEX IF NOT EXISTS idx_leads_org_email  ON enriched_leads(organization_id) WHERE work_email IS NOT NULL AND work_email != ''",
            "CREATE INDEX IF NOT EXISTS idx_jobs_org_status  ON enrichment_jobs(organization_id, status, created_at DESC)",
        ]:
            await conn.execute(idx_sql)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_snapshots (
                snapshot_id  TEXT PRIMARY KEY,
                job_id       TEXT,
                org_id       TEXT,
                processed_at TEXT DEFAULT ''
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS enrichment_audit_log (
                id           TEXT PRIMARY KEY,
                org_id       TEXT NOT NULL,
                action       TEXT NOT NULL,
                lead_id      TEXT,
                linkedin_url TEXT,
                job_id       TEXT,
                user_email   TEXT,
                meta         TEXT,
                created_at   TEXT DEFAULT ''
            )
        """)
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_audit_org ON enrichment_audit_log(org_id, created_at DESC)"
        )
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS lead_notes (
                id         TEXT PRIMARY KEY,
                lead_id    TEXT NOT NULL,
                org_id     TEXT NOT NULL,
                note       TEXT NOT NULL,
                created_at TEXT DEFAULT ''
            )
        """)
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_notes_lead ON lead_notes(lead_id)"
        )
    logger.info("[LeadEnrichmentBD] DB initialized (PostgreSQL)")


# ─────────────────────────────────────────────────────────────────────────────
# Lead upsert
# ─────────────────────────────────────────────────────────────────────────────

async def _upsert_lead(lead: dict) -> None:
    lead_id   = lead.get("id", "?")
    lead_url  = lead.get("linkedin_url", "?")
    lead_name = lead.get("name", "?")

    _pipeline_log.info("[DB-SAVE] ── START ── id=%s  url=%s  name=%s  status=%s  fields=%d",
                       lead_id, lead_url, lead_name, lead.get("status", "?"), len(lead))

    # Step 1: clear placeholder emails
    for email_col in ("work_email", "personal_email", "email"):
        v = lead.get(email_col)
        if v and isinstance(v, str) and "placeholder" in v.lower():
            _pipeline_log.warning("[DB-SAVE] [STEP-1] clearing placeholder email  col=%s  val=%s", email_col, v)
            logger.warning("[DB] Clearing placeholder email in %s: %s", email_col, v)
            lead[email_col] = None

    # Step 2: strip emojis from text fields
    _pipeline_log.info("[DB-SAVE] [STEP-2] stripping emojis from text fields")
    _clean_lead_strings(lead, ["name", "first_name", "last_name", "title", "company",
                                "about", "location", "city", "country"])

    # Step 3: rewrite first-person text
    _pipeline_log.info("[DB-SAVE] [STEP-3] normalising person text (first→third person)")
    _normalise_person_text(lead)

    # Step 4: type audit — coerce dict/list → JSON str
    _pipeline_log.info("[DB-SAVE] [STEP-4] type audit — scanning %d fields", len(lead))
    coerced = []
    for k, v in lead.items():
        vtype = type(v).__name__
        if isinstance(v, (dict, list)):
            coerced.append(k)
            _pipeline_log.warning(
                "[DB-SAVE] [TYPE-COERCE] field=%-30s  type=%-6s  preview=%s",
                k, vtype, str(v)[:120]
            )
            lead[k] = json.dumps(v, default=str)
        else:
            _pipeline_log.debug(
                "[DB-SAVE] [TYPE-OK]     field=%-30s  type=%-6s  val=%s",
                k, vtype, str(v)[:80] if v is not None else "NULL"
            )
    if coerced:
        _pipeline_log.warning("[DB-SAVE] [STEP-4] coerced %d dict/list fields → JSON str: %s", len(coerced), coerced)
    else:
        _pipeline_log.info("[DB-SAVE] [STEP-4] all field types OK — no coercion needed")

    # Step 5: build SQL
    cols = [k for k in lead if k != "id"]
    all_keys = ["id"] + cols

    _REQUIRED_INSERT_FIELDS = {"linkedin_url", "organization_id"}
    _is_partial = not _REQUIRED_INSERT_FIELDS.issubset(set(cols))

    if _is_partial:
        set_clause = ", ".join(f"{k}=${i + 2}" for i, k in enumerate(cols))
        sql = f"UPDATE enriched_leads SET {set_clause} WHERE id=$1"
        args = [lead["id"]] + [lead[k] for k in cols]
        _pipeline_log.info("[DB-SAVE] [STEP-5] SQL built — partial UPDATE %d cols, executing", len(cols))
    else:
        placeholders = ", ".join(f"${i + 1}" for i in range(len(all_keys)))
        updates = ", ".join(f"{k}=EXCLUDED.{k}" for k in cols)
        sql = f"""
            INSERT INTO enriched_leads ({', '.join(all_keys)})
            VALUES ({placeholders})
            ON CONFLICT(id) DO UPDATE SET {updates}
        """
        args = [lead[k] for k in all_keys]
        _pipeline_log.info("[DB-SAVE] [STEP-5] SQL built — %d columns, executing INSERT/UPDATE", len(all_keys))

    # Step 6: execute
    try:
        async with get_pool().acquire() as conn:
            await conn.execute(sql, *args)
        _pipeline_log.info("[DB-SAVE] [STEP-6] ✓ DB write SUCCESS  id=%s  status=%s", lead_id, lead.get("status"))
    except Exception as db_err:
        _pipeline_log.error("[DB-SAVE] [STEP-6] ✗ DB write FAILED  id=%s  error=%s", lead_id, db_err)
        for i, k in enumerate(all_keys):
            v = lead.get(k)
            _pipeline_log.error("[DB-SAVE]   $%-3d  %-30s  %-10s  %s", i + 1, k, type(v).__name__, str(v)[:100])
        raise

    # Step 7: bust Redis cache
    if lead_id and _redis_pool:
        try:
            await _redis_pool.delete(f"lead:row:{lead_id}")
            _pipeline_log.info("[DB-SAVE] [STEP-7] Redis cache busted  key=lead:row:%s", lead_id)
        except Exception:
            pass

    _pipeline_log.info("[DB-SAVE] ── DONE ── id=%s", lead_id)


# ─────────────────────────────────────────────────────────────────────────────
# Job management
# ─────────────────────────────────────────────────────────────────────────────

async def _update_job(job_id: str, **kwargs) -> None:
    if not kwargs:
        return
    kwargs["updated_at"] = datetime.now(timezone.utc)
    keys = list(kwargs.keys())
    sets = ", ".join(f"{k}=${i + 1}" for i, k in enumerate(keys))
    args = [kwargs[k] for k in keys] + [job_id]
    async with get_pool().acquire() as conn:
        await conn.execute(f"UPDATE enrichment_jobs SET {sets} WHERE id=${len(args)}", *args)


async def _create_sub_job(
    sub_job_id: str, job_id: str, chunk_index: int, total_urls: int, org_id: str,
    snapshot_id: Optional[str] = None,
) -> None:
    now = datetime.now(timezone.utc)
    async with get_pool().acquire() as conn:
        await conn.execute(
            """INSERT INTO enrichment_sub_jobs
               (id, job_id, chunk_index, total_urls, processed, failed, status, organization_id, snapshot_id, created_at, updated_at)
               VALUES ($1,$2,$3,$4,0,0,'pending',$5,$6,$7,$8)
               ON CONFLICT(id) DO NOTHING""",
            sub_job_id, job_id, chunk_index, total_urls, org_id, snapshot_id, now, now,
        )


async def _update_sub_job(sub_job_id: str, **kwargs) -> None:
    if not kwargs:
        return
    kwargs["updated_at"] = datetime.now(timezone.utc)
    keys = list(kwargs.keys())
    sets = ", ".join(f"{k}=${i + 1}" for i, k in enumerate(keys))
    args = [kwargs[k] for k in keys] + [sub_job_id]
    async with get_pool().acquire() as conn:
        await conn.execute(f"UPDATE enrichment_sub_jobs SET {sets} WHERE id=${len(args)}", *args)


async def list_sub_jobs(job_id: str, org_id: str = "default") -> list[dict]:
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM enrichment_sub_jobs WHERE job_id=$1 AND organization_id=$2 ORDER BY chunk_index",
            job_id, org_id,
        )
        return [dict(r) for r in rows]


async def get_job(job_id: str, org_id: Optional[str] = None) -> Optional[dict]:
    async with get_pool().acquire() as conn:
        if org_id:
            row = await conn.fetchrow(
                "SELECT * FROM enrichment_jobs WHERE id=$1 AND organization_id=$2", job_id, org_id
            )
        else:
            row = await conn.fetchrow("SELECT * FROM enrichment_jobs WHERE id=$1", job_id)
        return dict(row) if row else None


async def list_jobs(limit: int = 20, org_id: Optional[str] = None) -> list[dict]:
    async with get_pool().acquire() as conn:
        if org_id:
            rows = await conn.fetch(
                "SELECT * FROM enrichment_jobs WHERE organization_id=$1 ORDER BY created_at DESC LIMIT $2",
                org_id, limit,
            )
        else:
            rows = await conn.fetch(
                "SELECT * FROM enrichment_jobs ORDER BY created_at DESC LIMIT $1", limit
            )
        return [dict(r) for r in rows]


# ─────────────────────────────────────────────────────────────────────────────
# Lead CRUD
# ─────────────────────────────────────────────────────────────────────────────

async def get_lead(lead_id: str) -> Optional[dict]:
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM enriched_leads WHERE id=$1", lead_id)
        return dict(row) if row else None


async def get_lead_by_url(linkedin_url: str) -> Optional[dict]:
    """Look up an enriched lead by its LinkedIn URL."""
    from ._brightdata import _normalize_linkedin_url
    lid = _lead_id(_normalize_linkedin_url(linkedin_url))
    return await get_lead(lid)


async def list_leads(
    limit: int = 50, offset: int = 0,
    org_id: Optional[str] = None,
    job_id: Optional[str] = None,
    min_score: Optional[int] = None,
    tier: Optional[str] = None,
    sort_by: Optional[str] = None,
    sort_dir: Optional[str] = None,
    q: Optional[str] = None,
) -> dict:
    _SORTABLE = {
        "score": "total_score",
        "name": "name",
        "company": "company",
        "enriched_at": "enriched_at",
        "created_at": "created_at",
    }
    sort_col = _SORTABLE.get(sort_by or "", "total_score")
    sort_order = "ASC" if (sort_dir or "").upper() == "ASC" else "DESC"
    clauses: list[str] = []
    params: list = []
    if org_id:
        params.append(org_id); clauses.append(f"organization_id=${len(params)}")
    if job_id:
        params.append(job_id); clauses.append(f"job_id=${len(params)}")
    if min_score is not None:
        params.append(min_score); clauses.append(f"total_score>=${len(params)}")
    if tier:
        params.append(tier); clauses.append(f"score_tier=${len(params)}")
    if q:
        params.append(f"%{q.lower()}%")
        n = len(params)
        clauses.append(f"(LOWER(name) LIKE ${n} OR LOWER(company) LIKE ${n})")
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params_page = params + [limit, offset]
    async with get_pool().acquire() as conn:
        total = await conn.fetchval(f"SELECT COUNT(*) FROM enriched_leads {where}", *params)
        rows = await conn.fetch(
            f"SELECT * FROM enriched_leads {where} ORDER BY {sort_col} {sort_order} NULLS LAST, enriched_at DESC LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}",
            *params_page,
        )
    return {"total": total, "leads": [dict(r) for r in rows]}


async def delete_lead(lead_id: str) -> bool:
    async with get_pool().acquire() as conn:
        status = await conn.execute("DELETE FROM enriched_leads WHERE id=$1", lead_id)
    return status != "DELETE 0"


# ─────────────────────────────────────────────────────────────────────────────
# LIO results
# ─────────────────────────────────────────────────────────────────────────────

async def get_lead_lio_results(lead_id: str) -> dict:
    """Return saved LIO stage results for a lead (empty dict if none)."""
    async with get_pool().acquire() as conn:
        raw = await conn.fetchval(
            "SELECT lio_results_json FROM enriched_leads WHERE id=$1", lead_id
        )
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


async def save_lead_lio_results(lead_id: str, results: dict) -> None:
    """Merge new stage results into saved LIO results for a lead."""
    async with get_pool().acquire() as conn:
        raw = await conn.fetchval(
            "SELECT lio_results_json FROM enriched_leads WHERE id=$1", lead_id
        )
        existing: dict = {}
        if raw:
            try:
                existing = json.loads(raw)
            except Exception:
                pass
        existing.update(results)
        await conn.execute(
            "UPDATE enriched_leads SET lio_results_json=$1 WHERE id=$2",
            json.dumps(existing), lead_id,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Duplicate detection
# ─────────────────────────────────────────────────────────────────────────────

async def check_existing_lead(linkedin_url: str, org_id: str) -> Optional[dict]:
    """
    Return the lead row if already enriched for this org.
    Adds _cache_hit=True and _stale=True/False to the returned dict.
    """
    from ._brightdata import _normalize_linkedin_url
    lid = _lead_id(_normalize_linkedin_url(linkedin_url))
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM enriched_leads WHERE id=$1 AND organization_id=$2",
            lid, org_id,
        )
    if not row:
        return None
    lead = dict(row)
    enriched_at = lead.get("enriched_at") or lead.get("created_at", "")
    stale = False
    try:
        ea = datetime.fromisoformat(enriched_at.replace("Z", "+00:00"))
        stale = (datetime.now(timezone.utc) - ea) > timedelta(days=_CACHE_TTL_DAYS)
    except Exception:
        pass
    lead["_cache_hit"] = True
    lead["_stale"] = stale
    return lead


async def check_existing_leads_batch(linkedin_urls: list[str], org_id: str) -> dict[str, dict]:
    """
    Batch version of check_existing_lead — single DB query for N URLs.
    Returns {lead_id: lead_row} for all URLs already enriched for this org.
    """
    if not linkedin_urls:
        return {}
    from ._brightdata import _normalize_linkedin_url
    lead_ids = [_lead_id(_normalize_linkedin_url(u)) for u in linkedin_urls]
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM enriched_leads WHERE id = ANY($1) AND organization_id=$2",
            lead_ids, org_id,
        )
    result: dict[str, dict] = {}
    for row in rows:
        lead = dict(row)
        enriched_at = lead.get("enriched_at") or lead.get("created_at", "")
        stale = False
        try:
            ea = datetime.fromisoformat(str(enriched_at).replace("Z", "+00:00"))
            stale = (datetime.now(timezone.utc) - ea) > timedelta(days=_CACHE_TTL_DAYS)
        except Exception:
            pass
        lead["_cache_hit"] = True
        lead["_stale"] = stale
        result[lead["id"]] = lead
    return result


async def get_stale_leads(
    org_id: str,
    older_than_days: int = 90,
    tier: Optional[str] = None,
    min_score: int = 0,
    limit: int = 200,
) -> list[dict]:
    """Find leads older than `older_than_days` that are candidates for re-enrichment."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=older_than_days)).isoformat()
    params: list = [org_id, cutoff]
    clauses = ["organization_id=$1", "enriched_at<$2"]
    if tier:
        params.append(tier); clauses.append(f"score_tier=${len(params)}")
    if min_score:
        params.append(min_score); clauses.append(f"total_score>=${len(params)}")
    max_limit = int(os.getenv("LEAD_REFRESH_LIMIT", "200"))
    limit = min(limit, max_limit)
    params.append(limit)
    where = "WHERE " + " AND ".join(clauses)
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            f"SELECT id, linkedin_url, enriched_at, total_score, score_tier "
            f"FROM enriched_leads {where} ORDER BY total_score DESC LIMIT ${len(params)}",
            *params,
        )
        return [dict(r) for r in rows]


# ─────────────────────────────────────────────────────────────────────────────
# Snapshot idempotency
# ─────────────────────────────────────────────────────────────────────────────

async def mark_snapshot_processed(snapshot_id: str, job_id: Optional[str], org_id: str) -> None:
    """Record that a snapshot has been processed (idempotency guard)."""
    async with get_pool().acquire() as conn:
        await conn.execute(
            "INSERT INTO processed_snapshots(snapshot_id, job_id, org_id) VALUES($1,$2,$3) ON CONFLICT(snapshot_id) DO NOTHING",
            snapshot_id, job_id, org_id,
        )


async def is_snapshot_processed(snapshot_id: str) -> bool:
    """Return True if this snapshot_id was already processed."""
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM processed_snapshots WHERE snapshot_id=$1", snapshot_id
        )
        return row is not None


# ─────────────────────────────────────────────────────────────────────────────
# Audit log
# ─────────────────────────────────────────────────────────────────────────────

async def audit_log(
    org_id: str,
    action: str,
    lead_id: Optional[str] = None,
    linkedin_url: Optional[str] = None,
    job_id: Optional[str] = None,
    user_email: Optional[str] = None,
    meta: Optional[dict] = None,
) -> None:
    """Write an audit log entry (fire-and-forget — never raises)."""
    try:
        entry_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        async with get_pool().acquire() as conn:
            await conn.execute(
                """INSERT INTO enrichment_audit_log
                   (id, org_id, action, lead_id, linkedin_url, job_id, user_email, meta, created_at)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)""",
                entry_id, org_id, action, lead_id, linkedin_url,
                job_id, user_email,
                json.dumps(meta) if meta else None,
                now,
            )
    except Exception as e:
        logger.debug("[AuditLog] write failed: %s", e)


async def list_audit_log(
    org_id: str,
    action: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> dict:
    """List audit log entries for an org."""
    params: list = [org_id]
    clauses = ["org_id=$1"]
    if action:
        params.append(action); clauses.append(f"action=${len(params)}")
    where = "WHERE " + " AND ".join(clauses)
    params_page = params + [limit, offset]
    async with get_pool().acquire() as conn:
        total = await conn.fetchval(f"SELECT COUNT(*) FROM enrichment_audit_log {where}", *params)
        rows = await conn.fetch(
            f"SELECT * FROM enrichment_audit_log {where} ORDER BY created_at DESC LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}",
            *params_page,
        )
    rows = [dict(r) for r in rows]
    for row in rows:
        if row.get("meta"):
            try:
                row["meta"] = json.loads(row["meta"])
            except Exception:
                pass
    return {"total": int(total), "entries": rows}


# ─────────────────────────────────────────────────────────────────────────────
# Lead notes
# ─────────────────────────────────────────────────────────────────────────────

async def add_lead_note(lead_id: str, org_id: str, note: str) -> dict:
    """Add a text note to a lead. Returns the created note row."""
    note_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    async with get_pool().acquire() as conn:
        await conn.execute(
            "INSERT INTO lead_notes(id, lead_id, org_id, note, created_at) VALUES($1,$2,$3,$4,$5)",
            note_id, lead_id, org_id, note.strip(), now,
        )
        row = await conn.fetchrow("SELECT * FROM lead_notes WHERE id=$1", note_id)
        return dict(row)


async def list_lead_notes(lead_id: str, org_id: str) -> list[dict]:
    """Return all notes for a lead, newest first."""
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM lead_notes WHERE lead_id=$1 AND org_id=$2 ORDER BY created_at DESC",
            lead_id, org_id,
        )
        return [dict(r) for r in rows]


async def delete_lead_note(note_id: str, org_id: str) -> bool:
    """Delete a note by id (org-scoped for safety)."""
    async with get_pool().acquire() as conn:
        status = await conn.execute(
            "DELETE FROM lead_notes WHERE id=$1 AND org_id=$2", note_id, org_id
        )
    return status != "DELETE 0"


# ─────────────────────────────────────────────────────────────────────────────
# LinkedIn enrichment view formatter
# ─────────────────────────────────────────────────────────────────────────────

def _format_linkedin_enrich(lead: dict, include_contact: bool = True) -> dict:
    """
    Build the structured LinkedIn Enrichment view from a raw DB lead row.
    Shared by enrich_single() and the /view/linkedin route.
    Pass include_contact=False for bulk enrichment to omit the contact block.
    """
    full         = _parse_json_safe(lead.get("full_data"), {})
    lead_scoring = full.get("lead_scoring") or {}  # noqa: F841

    crm = _parse_json_safe(lead.get("crm_brief"), None)
    result = {
        "lead_id":      lead.get("id"),
        "linkedin_url": lead.get("linkedin_url"),
        "enriched_at":  lead.get("enriched_at"),
        "cache_hit":    bool(lead.get("_cache_hit")),
        "crm_brief":    crm,
    }
    if crm is None:
        result["crm_brief_error"] = lead.get("_crm_brief_error") or "crm_brief not yet generated"
    return result

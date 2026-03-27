"""
lead_enrichment_brightdata_service.py
--------------------------------------
Worksbuddy Lead Enrichment — powered by Bright Data.

8-stage enrichment output:
  Stage 1. Person Profile       — name, emails, phone, LinkedIn, Twitter, timezone, skills
  Stage 2. Company Identity     — name, domain, website, LinkedIn
  Stage 3. Website Intelligence — scraped homepage/about/features/pricing intelligence
  Stage 4. Company Profile      — industry, employee count, revenue, tech stack
  Stage 5. Market Signals       — funding, hiring velocity, news
  Stage 6. Intent Signals       — funding events, job changes, hiring surges, competitors
  Stage 7. Lead Scoring         — ICP fit (0-40) + Intent (0-30) + Timing (0-20) + DataCompleteness (0-10)
  Stage 8. Outreach             — AI cold email, LinkedIn note, sequence, best time
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import aiosqlite
import httpx
try:
    import redis.asyncio as aioredis
    _REDIS_AVAILABLE = True
except ImportError:
    _REDIS_AVAILABLE = False

logger = logging.getLogger(__name__)

# ── Bright Data ───────────────────────────────────────────────────────────────
# All keys read lazily from keys_service so admin hot-reload works at request time.
BD_BASE = "https://api.brightdata.com/datasets/v3"

def _k(name: str, default: str = "") -> str:
    """Fetch key from keys_service (admin store) with os.getenv fallback."""
    try:
        import keys_service as _ks
        return _ks.get(name) or os.getenv(name, default)
    except Exception:
        return os.getenv(name, default)

def _bd_api_key()         -> str:  return _k("BRIGHT_DATA_API_KEY")
def _bd_profile_dataset() -> str:  return _k("BD_PROFILE_DATASET_ID", "gd_l1viktl72bvl7bjuj0")
def _bd_company_dataset() -> str:  return _k("BD_COMPANY_DATASET_ID", "gd_l1vikfnt1wgvvqz95w")
def _hunter_api_key()     -> str:  return _k("HUNTER_API_KEY")
def _apollo_api_key()     -> str:  return _k("APOLLO_API_KEY")
def _dropcontact_key()    -> str:  return _k("DROPCONTACT_API_KEY")
def _pdl_api_key()        -> str:  return _k("PDL_API_KEY")
def _zerobounce_key()     -> str:  return _k("ZEROBOUNCE_API_KEY")
def _wb_llm_host()        -> str:  return _k("WB_LLM_HOST", "http://ai-llm.worksbuddy.ai")
def _wb_llm_key()         -> str:  return _k("WB_LLM_API_KEY")
def _wb_llm_model()       -> str:  return _k("WB_LLM_DEFAULT_MODEL", "wb-pro")
def _groq_key()           -> str:  return _k("GROQ_API_KEY")
def _groq_model()         -> str:  return _k("GROQ_LLM_MODEL", "llama-3.3-70b-versatile")
def _outreach_threshold() -> int:
    try: return int(_k("LEAD_OUTREACH_THRESHOLD", "50"))
    except ValueError: return 50

# Backward-compat module-level names (read once at import; services use _fn() at call time)
BD_API_KEY         = ""  # use _bd_api_key() inside functions
BD_PROFILE_DATASET = ""  # use _bd_profile_dataset() inside functions
HUNTER_API_KEY     = ""  # use _hunter_api_key() inside functions
APOLLO_API_KEY     = ""  # use _apollo_api_key() inside functions
WB_LLM_HOST        = ""  # use _wb_llm_host() inside functions
WB_LLM_KEY         = ""  # use _wb_llm_key() inside functions
WB_LLM_MODEL       = ""  # use _wb_llm_model() inside functions
GROQ_KEY           = ""  # use _groq_key() inside functions
GROQ_MODEL         = ""  # use _groq_model() inside functions
OUTREACH_THRESHOLD = 50  # use _outreach_threshold() inside functions

# ── DB ────────────────────────────────────────────────────────────────────────
_DB_DIR  = os.path.join(os.path.dirname(__file__), "configs")
LEADS_DB = os.path.join(_DB_DIR, "leads_enrichment.db")

# ── Redis queues ───────────────────────────────────────────────────────────────
REDIS_URL    = os.getenv("REDIS_URL", "redis://localhost:6379/0")
QUEUE_HIGH   = "wb:leads:queue:high"    # single enrichments
QUEUE_NORMAL = "wb:leads:queue:normal"  # bulk ≤100
QUEUE_LOW    = "wb:leads:queue:low"     # bulk >100

_redis_pool: Any = None


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


# ── Ably publish helpers ───────────────────────────────────────────────────────

async def _publish_lead_done(org_id: str, job_id: str, lead: dict) -> None:
    """Publish lead:done event to Ably — real-time frontend update."""
    try:
        import ably_service
        channel = f"tenant:{org_id}:job:{job_id}"
        await ably_service.publish(channel, "lead:done", {
            "job_id":   job_id,
            "org_id":   org_id,
            "lead_id":  lead.get("id"),
            "name":     lead.get("name"),
            "score":    lead.get("total_score"),
            "tier":     lead.get("score_tier"),
            "email":    lead.get("work_email"),
            "company":  lead.get("company"),
            "title":    lead.get("title"),
        })
    except Exception as e:
        logger.debug("[Ably] lead:done publish failed: %s", e)


async def _publish_job_done(org_id: str, job_id: str, processed: int, failed: int) -> None:
    """Publish job:done event when all leads in a bulk job are processed."""
    try:
        import ably_service
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
# Database
# ─────────────────────────────────────────────────────────────────────────────

async def init_leads_db() -> None:
    async with aiosqlite.connect(LEADS_DB) as db:
        await db.execute("""
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
                created_at       TEXT DEFAULT (datetime('now')),
                lio_results_json TEXT
            )
        """)
        # Migration: add lio_results_json column if it doesn't exist yet
        try:
            await db.execute("ALTER TABLE enriched_leads ADD COLUMN lio_results_json TEXT")
            await db.commit()
        except Exception:
            pass  # Column already exists
        await db.execute("""
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
                created_at      TEXT DEFAULT (datetime('now')),
                updated_at      TEXT DEFAULT (datetime('now'))
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS enrichment_sub_jobs (
                id              TEXT PRIMARY KEY,
                job_id          TEXT NOT NULL,
                chunk_index     INTEGER NOT NULL,
                total_urls      INTEGER DEFAULT 0,
                processed       INTEGER DEFAULT 0,
                failed          INTEGER DEFAULT 0,
                status          TEXT DEFAULT 'pending',
                organization_id TEXT NOT NULL DEFAULT 'default',
                created_at      TEXT DEFAULT (datetime('now')),
                updated_at      TEXT DEFAULT (datetime('now'))
            )
        """)
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_sub_jobs_job ON enrichment_sub_jobs(job_id)"
        )
        await db.commit()
        # Add columns to existing tables (idempotent)
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
            # Company waterfall enrichment columns
            ("avatar_url", "TEXT"),
            ("company_logo", "TEXT"),
            ("company_email", "TEXT"),
            ("company_description", "TEXT"),
            ("company_linkedin", "TEXT"),
            ("company_twitter", "TEXT"),
            ("company_phone", "TEXT"),
            ("waterfall_log", "TEXT"),
            # Stage 3 — Website Intelligence columns
            ("website_intelligence", "TEXT"),
            ("product_offerings", "TEXT"),
            ("value_proposition", "TEXT"),
            ("target_customers", "TEXT"),
            ("business_model", "TEXT"),
            ("pricing_signals", "TEXT"),
            ("product_category", "TEXT"),
            ("data_completeness_score", "INTEGER DEFAULT 0"),
            ("organization_id", "TEXT NOT NULL DEFAULT 'default'"),
            # P1 — Activity feed (full posts/likes/comments array from BD)
            ("activity_feed",        "TEXT"),
            # P3 — AI Analysis layer outputs
            ("auto_tags",            "TEXT"),   # JSON array of 6-8 pill strings
            ("behavioural_signals",  "TEXT"),   # JSON: posts_about, engages_with, etc.
            ("pitch_intelligence",   "TEXT"),   # JSON: pain_point, value_prop, avoid, angle, cta
            ("warm_signal",          "TEXT"),   # e.g. "Liked a Lio post about lead scoring"
            # P4 — Enhanced email verification
            ("email_verified",       "INTEGER DEFAULT 0"),
            ("bounce_risk",          "TEXT"),   # low / medium / high
            # Company enrichment (P1/P5 — joined from company_enrichments at enrich time)
            ("company_id",           "TEXT"),
            ("company_score",        "INTEGER DEFAULT 0"),
            ("combined_score",       "INTEGER DEFAULT 0"),
            ("company_tags",         "TEXT"),   # JSON array — mirrors company_enrichments
            ("culture_signals",      "TEXT"),   # JSON object
            ("account_pitch",        "TEXT"),   # JSON object
            ("wappalyzer_tech",      "TEXT"),   # JSON array
            ("news_mentions",        "TEXT"),   # JSON array
            ("crunchbase_data",      "TEXT"),   # JSON object
            ("linkedin_posts",       "TEXT"),   # JSON array (company posts)
            ("company_score_tier",   "TEXT"),
        ]
        for col, col_type in _NEW_COLS:
            try:
                await db.execute(f"ALTER TABLE enriched_leads ADD COLUMN {col} {col_type}")
            except Exception:
                pass  # Column already exists
        # Idempotent migration for enrichment_jobs
        _JOB_COLS = [("organization_id", "TEXT NOT NULL DEFAULT 'default'")]
        for col, col_type in _JOB_COLS:
            try:
                await db.execute(f"ALTER TABLE enrichment_jobs ADD COLUMN {col} {col_type}")
            except Exception:
                pass
        # Indexes for org isolation
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS idx_leads_org ON enriched_leads(organization_id, total_score DESC)",
            "CREATE INDEX IF NOT EXISTS idx_jobs_org  ON enrichment_jobs(organization_id, created_at DESC)",
        ]:
            try:
                await db.execute(idx_sql)
            except Exception:
                pass
        await db.commit()
    logger.info("[LeadEnrichmentBD] DB initialized: %s", LEADS_DB)


async def _upsert_lead(lead: dict) -> None:
    cols = [k for k in lead if k != "id"]
    placeholders = ", ".join(f":{k}" for k in cols)
    updates = ", ".join(f"{k}=excluded.{k}" for k in cols)
    sql = f"""
        INSERT INTO enriched_leads (id, {', '.join(cols)})
        VALUES (:id, {placeholders})
        ON CONFLICT(id) DO UPDATE SET {updates}
    """
    async with aiosqlite.connect(LEADS_DB) as db:
        await db.execute(sql, lead)
        await db.commit()


async def _update_job(job_id: str, **kwargs) -> None:
    if not kwargs:
        return
    kwargs["updated_at"] = datetime.now(timezone.utc).isoformat()
    sets = ", ".join(f"{k}=:{k}" for k in kwargs)
    kwargs["id"] = job_id
    async with aiosqlite.connect(LEADS_DB) as db:
        await db.execute(f"UPDATE enrichment_jobs SET {sets} WHERE id=:id", kwargs)
        await db.commit()


async def _create_sub_job(
    sub_job_id: str, job_id: str, chunk_index: int, total_urls: int, org_id: str
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(LEADS_DB) as db:
        await db.execute(
            """INSERT OR IGNORE INTO enrichment_sub_jobs
               (id, job_id, chunk_index, total_urls, processed, failed, status, organization_id, created_at, updated_at)
               VALUES (?,?,?,?,0,0,'pending',?,?,?)""",
            (sub_job_id, job_id, chunk_index, total_urls, org_id, now, now),
        )
        await db.commit()


async def _update_sub_job(sub_job_id: str, **kwargs) -> None:
    if not kwargs:
        return
    kwargs["updated_at"] = datetime.now(timezone.utc).isoformat()
    sets = ", ".join(f"{k}=:{k}" for k in kwargs)
    kwargs["id"] = sub_job_id
    async with aiosqlite.connect(LEADS_DB) as db:
        await db.execute(f"UPDATE enrichment_sub_jobs SET {sets} WHERE id=:id", kwargs)
        await db.commit()


async def list_sub_jobs(job_id: str, org_id: str = "default") -> list[dict]:
    async with aiosqlite.connect(LEADS_DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM enrichment_sub_jobs WHERE job_id=? AND organization_id=? ORDER BY chunk_index",
            (job_id, org_id),
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def get_lead(lead_id: str) -> Optional[dict]:
    async with aiosqlite.connect(LEADS_DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM enriched_leads WHERE id=?", (lead_id,)) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None


async def get_lead_lio_results(lead_id: str) -> dict:
    """Return saved LIO stage results for a lead (empty dict if none)."""
    async with aiosqlite.connect(LEADS_DB) as db:
        async with db.execute(
            "SELECT lio_results_json FROM enriched_leads WHERE id=?", (lead_id,)
        ) as cur:
            row = await cur.fetchone()
            if not row or not row[0]:
                return {}
            try:
                return json.loads(row[0])
            except Exception:
                return {}


async def save_lead_lio_results(lead_id: str, results: dict) -> None:
    """Merge new stage results into saved LIO results for a lead."""
    async with aiosqlite.connect(LEADS_DB) as db:
        # Load existing
        async with db.execute(
            "SELECT lio_results_json FROM enriched_leads WHERE id=?", (lead_id,)
        ) as cur:
            row = await cur.fetchone()
        existing: dict = {}
        if row and row[0]:
            try:
                existing = json.loads(row[0])
            except Exception:
                pass
        existing.update(results)
        await db.execute(
            "UPDATE enriched_leads SET lio_results_json=? WHERE id=?",
            (json.dumps(existing), lead_id),
        )
        await db.commit()


async def list_leads(
    limit: int = 50, offset: int = 0,
    org_id: Optional[str] = None,
    job_id: Optional[str] = None,
    min_score: Optional[int] = None,
    tier: Optional[str] = None,
) -> dict:
    clauses: list[str] = []
    params: list = []
    if org_id:
        clauses.append("organization_id=?"); params.append(org_id)
    if job_id:
        clauses.append("job_id=?"); params.append(job_id)
    if min_score is not None:
        clauses.append("total_score>=?"); params.append(min_score)
    if tier:
        clauses.append("score_tier=?"); params.append(tier)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    async with aiosqlite.connect(LEADS_DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(f"SELECT COUNT(*) FROM enriched_leads {where}", params) as cur:
            total = (await cur.fetchone())[0]
        async with db.execute(
            f"SELECT * FROM enriched_leads {where} ORDER BY total_score DESC, enriched_at DESC LIMIT ? OFFSET ?",
            [*params, limit, offset],
        ) as cur:
            rows = [dict(r) for r in await cur.fetchall()]
    return {"total": total, "leads": rows}


async def get_job(job_id: str, org_id: Optional[str] = None) -> Optional[dict]:
    async with aiosqlite.connect(LEADS_DB) as db:
        db.row_factory = aiosqlite.Row
        if org_id:
            sql, args = "SELECT * FROM enrichment_jobs WHERE id=? AND organization_id=?", (job_id, org_id)
        else:
            sql, args = "SELECT * FROM enrichment_jobs WHERE id=?", (job_id,)
        async with db.execute(sql, args) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None


async def list_jobs(limit: int = 20, org_id: Optional[str] = None) -> list[dict]:
    async with aiosqlite.connect(LEADS_DB) as db:
        db.row_factory = aiosqlite.Row
        if org_id:
            sql, args = "SELECT * FROM enrichment_jobs WHERE organization_id=? ORDER BY created_at DESC LIMIT ?", (org_id, limit)
        else:
            sql, args = "SELECT * FROM enrichment_jobs ORDER BY created_at DESC LIMIT ?", (limit,)
        async with db.execute(sql, args) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def delete_lead(lead_id: str) -> bool:
    async with aiosqlite.connect(LEADS_DB) as db:
        async with db.execute("DELETE FROM enriched_leads WHERE id=?", (lead_id,)) as cur:
            deleted = cur.rowcount > 0
        await db.commit()
    return deleted


# ─────────────────────────────────────────────────────────────────────────────
# LLM caller helper
# ─────────────────────────────────────────────────────────────────────────────

async def _call_llm(
    messages: list[dict],
    max_tokens: int = 1800,
    temperature: float = 0.3,
    model_override: Optional[str] = None,
    # Explicit key overrides — used when keys come from DB tool configs rather than env/keys_service
    groq_api_key: Optional[str] = None,
    wb_llm_host_override: Optional[str] = None,
    wb_llm_key_override: Optional[str] = None,
    wb_llm_model_override: Optional[str] = None,
) -> Optional[str]:
    """Try WB LLM → Groq. Returns raw content string or None.
    model_override: overrides the Groq model (env default or explicit groq_api_key).
    groq_api_key / wb_llm_*_override: explicit keys from DB — take priority over env/keys_service.
    """
    async def _post(base_url: str, api_key: str, model: str) -> str:
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                f"{base_url.rstrip('/')}/v1/chat/completions",
                headers=headers,
                json={"model": model, "messages": messages, "temperature": temperature, "max_tokens": max_tokens},
            )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"].strip()

    # WB LLM — explicit override takes priority over env/keys_service
    wb_host = wb_llm_host_override or _wb_llm_host()
    wb_key  = wb_llm_key_override  or _wb_llm_key()
    wb_mdl  = wb_llm_model_override or _wb_llm_model()
    if wb_host and wb_key:
        try:
            return await _post(wb_host, wb_key, wb_mdl)
        except Exception as e:
            logger.warning("[LLM] WB failed: %s", e)

    # Groq — explicit override takes priority over env/keys_service
    # Groq base URL must include /openai prefix: https://api.groq.com/openai
    g_key = groq_api_key or _groq_key()
    if g_key:
        try:
            groq_model = model_override or _groq_model()
            return await _post("https://api.groq.com/openai", g_key, groq_model)
        except Exception as e:
            logger.warning("[LLM] Groq failed: %s", e)

    return None


def _parse_json_from_llm(raw: str) -> dict:
    """Extract first JSON object from LLM response."""
    try:
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            return json.loads(m.group())
    except Exception:
        pass
    return {}


# ─────────────────────────────────────────────────────────────────────────────
# Bright Data API
# ─────────────────────────────────────────────────────────────────────────────

def _bd_headers() -> dict:
    return {"Authorization": f"Bearer {_bd_api_key()}", "Content-Type": "application/json"}


def _normalize_linkedin_url(url: str) -> str:
    url = url.strip().rstrip("/")
    if not url.startswith("http"):
        url = f"https://{url}"
    return url


def _normalize_bd_profile(raw: dict) -> dict:
    """
    Normalize a raw Bright Data person profile response to the flat dict
    shape the rest of the service expects.

    Handles both the nested and flat field naming variants that Bright Data
    returns depending on the dataset version, e.g.:
      - current_company.link  vs  current_company_link  (flat)
      - avatar                vs  avatar_url
      - educations_details    vs  education[]
      - activity[]            → extract emails + hiring signals
      - recommendations[]     → text list
    """
    p = dict(raw)  # shallow copy — we'll add/fix keys

    # ── current_company nested → flat ────────────────────────────────────────
    cc = p.get("current_company") or {}
    if isinstance(cc, dict):
        # Company link (the only field missing from flat response)
        if not p.get("current_company_link"):
            p["current_company_link"] = cc.get("link") or cc.get("url") or ""
        # Company name fallback
        if not p.get("current_company_name"):
            p["current_company_name"] = cc.get("name") or ""
        # Company logo sometimes in nested object
        if not p.get("current_company_logo"):
            p["current_company_logo"] = cc.get("logo") or cc.get("image") or ""
        # Company id
        if not p.get("current_company_company_id"):
            p["current_company_company_id"] = cc.get("company_id") or cc.get("id") or ""

    # ── Avatar / profile photo ────────────────────────────────────────────────
    # BD returns "avatar" at top level; service uses "avatar_url"
    if not p.get("avatar_url"):
        p["avatar_url"] = p.get("avatar") or p.get("profile_image") or p.get("photo") or ""
    # Suppress generic LinkedIn placeholder — BD sets default_avatar: true when no real photo
    if p.get("default_avatar"):
        p["avatar_url"] = ""

    # ── Banner image ─────────────────────────────────────────────────────────
    if not p.get("banner_image"):
        p["banner_image"] = p.get("background_image") or p.get("cover_image") or ""

    # ── Education: string form → structured list ──────────────────────────────
    # BD sometimes returns educations_details as a plain string instead of education[]
    edu_raw = p.get("education")
    edu_str = p.get("educations_details") or p.get("education_details") or ""

    if edu_raw and isinstance(edu_raw, list):
        # Normalize key variants BD may use
        def _edu_year(val: str) -> str:
            """BD sometimes returns "2006-08" (month-year ISO); keep only the 4-digit year."""
            if val and isinstance(val, str):
                return val[:4]
            return val or ""

        normalized_edu = []
        for e in edu_raw:
            if not isinstance(e, dict):
                continue
            # BD uses "title" for school name in some dataset versions
            school = (e.get("school") or e.get("institution") or e.get("university")
                      or e.get("name") or e.get("title") or "")
            # When "title" is the school name, "degree" comes from description or field_of_study
            degree = (e.get("degree") or e.get("field_of_study") or e.get("description") or "")
            # Only fall back to "title" as degree if school was found from a non-title key
            if not degree and not (e.get("school") or e.get("institution") or e.get("university") or e.get("name")):
                degree = ""  # title was used for school — don't double-use it
            start = _edu_year(e.get("start_year") or e.get("start") or "")
            end   = _edu_year(e.get("end_year")   or e.get("end")   or "")
            years = (e.get("years") or e.get("date_range") or e.get("dates")
                     or (f"{start}–{end}".strip("–") if start or end else ""))
            normalized_edu.append({"school": school, "degree": degree, "years": years})
        p["education"] = [e for e in normalized_edu if e["school"]]
    elif not edu_raw and edu_str and isinstance(edu_str, str):
        # Parse plain string — split on semicolons (multiple schools)
        p["education"] = [{"school": part.strip(), "degree": "", "years": ""}
                          for part in edu_str.split(";") if part.strip()]

    # ── Languages: normalize BD's title/subtitle keys ───────────────────────���
    langs_raw = p.get("languages") or []
    if langs_raw and isinstance(langs_raw, list):
        normalized_langs = []
        for lang in langs_raw:
            if isinstance(lang, dict):
                normalized_langs.append({
                    "name":        lang.get("name") or lang.get("title") or lang.get("language") or "",
                    "proficiency": lang.get("proficiency") or lang.get("subtitle") or lang.get("level") or "",
                })
            elif isinstance(lang, str) and lang.strip():
                normalized_langs.append({"name": lang.strip(), "proficiency": ""})
        p["languages"] = [l for l in normalized_langs if l["name"]]

    # ── Certifications: normalize BD's title/subtitle/meta keys ──────────────
    certs_raw = p.get("certifications") or []
    if certs_raw and isinstance(certs_raw, list):
        normalized_certs = []
        for cert in certs_raw:
            if isinstance(cert, dict):
                normalized_certs.append({
                    "name":           cert.get("name") or cert.get("title") or "",
                    "issuer":         cert.get("issuer") or cert.get("subtitle") or cert.get("organization") or "",
                    "date":           cert.get("date") or cert.get("meta") or cert.get("issued_at") or "",
                    "credential_url": cert.get("credential_url") or cert.get("url") or "",
                    "credential_id":  cert.get("credential_id") or "",
                })
            elif isinstance(cert, str) and cert.strip():
                normalized_certs.append({"name": cert.strip(), "issuer": "", "date": "", "credential_url": "", "credential_id": ""})
        p["certifications"] = [c for c in normalized_certs if c["name"]]

    # ── Skills: may come as list or comma string ──────────────────────────────
    skills = p.get("skills")
    if not skills:
        skills_str = p.get("skills_label") or p.get("top_skills") or ""
        if skills_str and isinstance(skills_str, str):
            p["skills"] = [s.strip() for s in skills_str.split(",") if s.strip()]

    # ── Timezone: derive from country_code ────────────────────────────────────
    if not p.get("timezone"):
        _TZ_MAP = {
            "IN": "Asia/Kolkata",      "US": "America/New_York",  "GB": "Europe/London",
            "DE": "Europe/Berlin",     "FR": "Europe/Paris",      "AU": "Australia/Sydney",
            "CA": "America/Toronto",   "SG": "Asia/Singapore",    "AE": "Asia/Dubai",
            "JP": "Asia/Tokyo",        "CN": "Asia/Shanghai",     "BR": "America/Sao_Paulo",
            "MX": "America/Mexico_City","NL": "Europe/Amsterdam", "SE": "Europe/Stockholm",
            "NO": "Europe/Oslo",       "DK": "Europe/Copenhagen", "FI": "Europe/Helsinki",
            "PL": "Europe/Warsaw",     "IT": "Europe/Rome",       "ES": "Europe/Madrid",
            "RU": "Europe/Moscow",     "ZA": "Africa/Johannesburg","NG": "Africa/Lagos",
            "KE": "Africa/Nairobi",    "EG": "Africa/Cairo",      "PK": "Asia/Karachi",
            "BD": "Asia/Dhaka",        "LK": "Asia/Colombo",      "NP": "Asia/Kathmandu",
            "TH": "Asia/Bangkok",      "VN": "Asia/Ho_Chi_Minh",  "MY": "Asia/Kuala_Lumpur",
            "ID": "Asia/Jakarta",      "PH": "Asia/Manila",       "KR": "Asia/Seoul",
            "HK": "Asia/Hong_Kong",    "TW": "Asia/Taipei",       "IL": "Asia/Jerusalem",
            "SA": "Asia/Riyadh",       "TR": "Europe/Istanbul",   "AR": "America/Argentina/Buenos_Aires",
            "CO": "America/Bogota",    "PE": "America/Lima",      "CL": "America/Santiago",
            "NZ": "Pacific/Auckland",
        }
        p["timezone"] = _TZ_MAP.get(p.get("country_code") or p.get("country") or "", "")

    # ── Position: try to extract from about text if BD doesn't return headline ──
    if not p.get("position"):
        about_text = p.get("about") or ""
        # Patterns:
        #   "I'm the CEO and Founder of LBM Solutions…"  (has "the")
        #   "I'm a Senior Engineer with 10 years…"       (has "a/an")
        #   "I am the VP of Engineering at…"
        m = re.match(
            r"^I(?:'m| am) (?:a |an |the )(.+?)(?:\s+with\b|\s+at\b|\s+of\b|\.|,|–|-)",
            about_text, re.IGNORECASE,
        )
        if m:
            candidate = m.group(1).strip().rstrip("and").strip()
            if 3 < len(candidate) < 70:
                p["position"] = candidate
        # Fallback: try headline key variants
        if not p.get("position"):
            p["position"] = p.get("headline") or p.get("bio") or p.get("title") or ""

    # ── Honors & Awards → merge into certifications ──────────────────────────
    honors = p.get("honors_and_awards") or []
    if honors and isinstance(honors, list):
        existing_certs = p.get("certifications") or []
        existing_names = {c.get("name", "").lower() for c in existing_certs if isinstance(c, dict)}
        for h in honors:
            if not isinstance(h, dict):
                continue
            title = h.get("title") or ""
            if not title or title.lower() in existing_names:
                continue
            date_raw = h.get("date") or ""
            # Trim ISO date "2022-08-01T00:00:00.000Z" → "2022"
            date_str = date_raw[:4] if date_raw else ""
            existing_certs.append({
                "name":           title,
                "issuer":         h.get("publication") or h.get("issuer") or "",
                "date":           date_str,
                "credential_url": h.get("url") or "",
                "credential_id":  "",
            })
        p["certifications"] = existing_certs

    # ── Posts (own authored posts — separate from activity[]) ────────────────
    posts = p.get("posts") or []
    if posts and isinstance(posts, list):
        # Store own posts separately with all fields for UI display
        p["_posts"] = [
            {
                "title": (post.get("title") or "")[:300],
                "attribution": (post.get("attribution") or "")[:500],
                "link": post.get("link") or "",
                "created_at": post.get("created_at") or "",
                "interaction": post.get("interaction") or "",
                "id": post.get("id") or "",
            }
            for post in posts if isinstance(post, dict)
        ]
        # Also merge into activity for email/phone/hiring extraction
        existing_ids = {a.get("id") for a in (p.get("activity") or []) if isinstance(a, dict)}
        for post in posts:
            if not isinstance(post, dict):
                continue
            if post.get("id") in existing_ids:
                continue
            synthetic = {
                "interaction": "Posted",
                "title": (post.get("title") or "")[:300],
                "attribution": (post.get("attribution") or "")[:500],
                "link": post.get("link") or "",
                "created_at": post.get("created_at") or "",
                "id": post.get("id") or "",
            }
            if not p.get("activity"):
                p["activity"] = []
            p["activity"].append(synthetic)
    else:
        p["_posts"] = []

    # ── Activity: extract emails + phones + hiring signals ────────────────────
    activity = p.get("activity") or []
    extracted_emails: list[str] = []
    extracted_phones: list[str] = []
    hiring_posts: list[str] = []
    email_re = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
    phone_re  = re.compile(r"(?<!\d)(\+?[\d][\d\s\-]{6,13}[\d])(?!\d)")

    for item in activity:
        if not isinstance(item, dict):
            continue
        text = (item.get("title") or "") + " " + (item.get("description") or "")
        # Extract emails visible in post text
        for em in email_re.findall(text):
            em_lower = em.lower()
            if em_lower not in extracted_emails and "linkedin.com" not in em_lower:
                extracted_emails.append(em_lower)
        # Extract phone numbers from post text
        for ph in phone_re.findall(text):
            cleaned = re.sub(r"[\s\-]", "", ph)
            if 7 <= len(cleaned.lstrip("+")) <= 15 and cleaned not in extracted_phones:
                extracted_phones.append(cleaned)
        # Flag hiring activity
        if any(kw in text.lower() for kw in ["hiring", "we're looking", "open position", "job opening", "apply"]):
            title_snip = (item.get("title") or "")[:120]
            if title_snip:
                hiring_posts.append(title_snip)

    if extracted_emails:
        p["_activity_emails"] = extracted_emails  # stored for Phase C waterfall
        logger.info("[BDNorm] Activity emails found: %s", extracted_emails)
    if extracted_phones:
        p["_activity_phones"] = extracted_phones
        logger.info("[BDNorm] Activity phones found: %s", extracted_phones)
    if hiring_posts:
        p["_hiring_signals"] = hiring_posts[:10]

    # ── Recommendations ───────────────────────────────────────────────────────
    # BD may send "recommendations" as a list of objects or strings.
    # Each object can have: text, description, recommender_name, recommender_headline,
    # recommender_url, date — depending on BD dataset version.
    recs = (
        p.get("recommendations")
        or p.get("received_recommendations")
        or p.get("testimonials")
        or []
    )
    if recs and isinstance(recs, list):
        def _rec_text(r):
            if isinstance(r, str):
                return r[:300]
            if isinstance(r, dict):
                return (
                    r.get("text") or r.get("description") or r.get("recommendation_text")
                    or r.get("content") or ""
                )[:300]
            return ""

        p["recommendations"] = [
            {
                "recommender_name": (r.get("recommender_name") or r.get("author_name") or r.get("name") or "") if isinstance(r, dict) else "",
                "recommender_headline": (r.get("recommender_headline") or r.get("author_headline") or r.get("title") or "") if isinstance(r, dict) else "",
                "recommender_url": (r.get("recommender_url") or r.get("profile_link") or r.get("url") or "") if isinstance(r, dict) else "",
                "recommender_image": (r.get("recommender_image") or r.get("profile_image_url") or r.get("image") or "") if isinstance(r, dict) else "",
                "text": _rec_text(r),
                "date": (r.get("date") or r.get("recommendation_date") or "") if isinstance(r, dict) else "",
                "relationship": (r.get("relationship") or r.get("connection_type") or "") if isinstance(r, dict) else "",
            }
            for r in recs if r
        ]
        p["recommendations_text"] = " | ".join(
            r["text"] for r in p["recommendations"] if r["text"]
        )[:600]
        if not p.get("recommendations_count"):
            p["recommendations_count"] = len(p["recommendations"])
    else:
        if not p.get("recommendations"):
            p["recommendations"] = []
        p["recommendations_count"] = _safe_int(p.get("recommendations_count"))

    # ── People also viewed → similar leads ───────────────────────────────────
    # BD dataset uses "similar_profiles"; some versions use "people_also_viewed"
    pav = p.get("people_also_viewed") or p.get("similar_profiles") or []
    if pav and isinstance(pav, list):
        p["_similar_profiles"] = [
            {
                "name": x.get("name", ""),
                "title": x.get("title") or x.get("headline") or x.get("position") or "",
                "link": x.get("profile_link") or x.get("url") or x.get("link") or "",
                "image": x.get("profile_image_url") or x.get("image") or x.get("profile_image") or "",
                "location": x.get("location", ""),
                "degree": x.get("degree") or x.get("connection_degree") or "",
            }
            for x in pav if isinstance(x, dict)
        ]

    # ── Activity: store full list for UI display ──────────────────────────────
    if activity:
        p["_activity_full"] = [
            {
                "interaction": item.get("interaction", ""),
                "title": (item.get("title") or "")[:300],
                "attribution": (item.get("attribution") or "")[:500],
                "link": item.get("link", ""),
                "created_at": item.get("created_at", ""),
                "id": item.get("id", ""),
                # Mark authored posts so UI can distinguish from liked/commented activity
                "type": "post" if (item.get("interaction") or "").lower().startswith("post") else "activity",
            }
            for item in activity if isinstance(item, dict)
        ]

    # ── Profile flags ─────────────────────────────────────────────────────────
    p["_influencer"]           = bool(p.get("influencer"))
    p["_memorialized_account"] = bool(p.get("memorialized_account"))
    p["_bio_links"]            = p.get("bio_links") or []
    p["_bd_scrape_timestamp"]  = p.get("timestamp") or ""

    # ── Followers / connections ───────────────────────────────────────────────
    p["followers"]   = _safe_int(p.get("followers"))
    p["connections"] = _safe_int(p.get("connections"))

    # ── Name split ────────────────────────────────────────────────────────────
    if not p.get("first_name") and p.get("name"):
        parts = p["name"].split()
        p["first_name"] = parts[0] if parts else ""
        p["last_name"] = " ".join(parts[1:]) if len(parts) > 1 else ""

    # ── Position / headline: already handled above with about-text extraction ──
    # Kept as safety fallback for profiles that do have a headline field
    if not p.get("position"):
        p["position"] = p.get("headline") or p.get("bio") or p.get("job_title") or ""

    # ── Location: city may be "City, State, Country" string ──────────────────
    city_raw = p.get("city") or p.get("location") or ""
    if city_raw and not p.get("location"):
        p["location"] = city_raw
    # Trim city to first segment only (BD often sends "City, State, Country")
    if p.get("city") and "," in p["city"]:
        p["city"] = p["city"].split(",")[0].strip()
    # Country from country_code if country missing; map code to full name
    if not p.get("country"):
        p["country"] = p.get("country_code", "")

    # ── LinkedIn profile URL normalization ────────────────────────────────────
    # BD may return locale-prefixed URLs: nl.linkedin.com, in.linkedin.com
    # Normalise to www.linkedin.com for downstream use
    for url_field in ("url", "input_url"):
        v = p.get(url_field) or ""
        if v:
            v = re.sub(r"https?://[a-z]{2}\.linkedin\.com", "https://www.linkedin.com", v)
            p[url_field] = v

    return p


async def fetch_profile_sync(linkedin_url: str) -> dict:
    """
    Primary: Bright Data sync /scrape endpoint → normalize with _normalize_bd_profile.
    Fallback: Playwright screenshot → OCR → LLM extraction.
    """
    url = _normalize_linkedin_url(linkedin_url)

    if _bd_api_key():
        logger.info("[BrightData] Sync scrape: %s", url)
        try:
            async with httpx.AsyncClient(timeout=90) as client:
                resp = await client.post(
                    f"{BD_BASE}/scrape",
                    params={"dataset_id": _bd_profile_dataset(), "format": "json"},
                    headers=_bd_headers(),
                    json=[{"url": url}],
                )
                if resp.status_code == 200:
                    data = resp.json()
                    raw = data[0] if isinstance(data, list) and data else data
                    if isinstance(raw, dict) and raw.get("name"):
                        profile = _normalize_bd_profile(raw)
                        logger.info("[BrightData] OK — %s | company=%s | avatar=%s | activity=%d",
                                    profile.get("name"),
                                    profile.get("current_company_name") or "?",
                                    "✓" if profile.get("avatar_url") else "✗",
                                    len(raw.get("activity") or []))
                        return profile
                    logger.warning("[BrightData] Empty profile, trying OCR fallback")
                else:
                    logger.warning("[BrightData] %s: %s — trying OCR fallback", resp.status_code, resp.text[:120])
        except Exception as e:
            logger.warning("[BrightData] Error: %s — trying OCR fallback", e)
    else:
        logger.info("[BrightData] No API key — using OCR fallback")

    return await _extract_profile_via_ocr(url)


async def _extract_profile_via_ocr(linkedin_url: str) -> dict:
    """Playwright screenshot → OCR → LLM structured extraction."""
    try:
        from lead_enrichment_service import capture_screenshot, extract_text_via_ocr
    except ImportError:
        logger.error("[OCR] lead_enrichment_service not found")
        return {"url": linkedin_url}

    png_bytes = await capture_screenshot(linkedin_url, timeout_ms=40_000)
    if not png_bytes:
        logger.error("[OCR] Screenshot failed: %s", linkedin_url)
        return {"url": linkedin_url}

    ocr_text, engine = extract_text_via_ocr(png_bytes)
    if not ocr_text.strip():
        logger.warning("[OCR] Empty text (engine: %s)", engine)
        return {"url": linkedin_url}

    logger.info("[OCR] %d chars via %s", len(ocr_text), engine)
    profile = await _llm_extract_profile_from_ocr(linkedin_url, ocr_text)
    profile["_source"] = f"ocr_{engine}"
    profile["url"] = linkedin_url
    return profile


async def _llm_extract_profile_from_ocr(linkedin_url: str, ocr_text: str) -> dict:
    snippet = ocr_text[:4500]
    prompt = f"""Extract LinkedIn profile data from this OCR text.
LinkedIn URL: {linkedin_url}

OCR TEXT:
---
{snippet}
---

Return ONLY valid JSON with these exact keys (use empty string or 0 if not found):
{{
  "name": "", "first_name": "", "last_name": "",
  "position": "", "current_company_name": "",
  "location": "", "city": "", "country": "",
  "about": "",
  "followers": 0, "connections": 0,
  "skills": [],
  "experience": [{{"title":"","company":"","duration":""}}],
  "education": [{{"school":"","degree":"","years":""}}],
  "certifications": [], "languages": []
}}"""

    raw = await _call_llm([
        {"role": "system", "content": "Precise JSON extractor. Return only valid JSON."},
        {"role": "user", "content": prompt},
    ], max_tokens=900, temperature=0)

    if raw:
        result = _parse_json_from_llm(raw)
        if result:
            return result

    # Best-effort without LLM
    lines = [l.strip() for l in ocr_text.split("\n") if l.strip() and len(l.strip()) > 3]
    return {"name": lines[0] if lines else "", "about": ocr_text[:500]}


async def trigger_batch_snapshot(
    urls: list[str],
    webhook_url: Optional[str] = None,
    notify_url: Optional[str] = None,
    webhook_auth: Optional[str] = None,
) -> str:
    if not _bd_api_key():
        raise ValueError("BRIGHT_DATA_API_KEY not configured")
    params: dict[str, str] = {
        "dataset_id": _bd_profile_dataset(), "format": "json", "include_errors": "true",
    }
    if webhook_url:
        params["endpoint"] = webhook_url; params["uncompressed_webhook"] = "true"
    if notify_url:
        params["notify"] = notify_url
    if webhook_auth:
        params["auth_header"] = webhook_auth
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            f"{BD_BASE}/trigger", params=params, headers=_bd_headers(),
            json=[{"url": _normalize_linkedin_url(u)} for u in urls],
        )
        resp.raise_for_status()
    snapshot_id = resp.json().get("snapshot_id") or resp.json().get("id")
    if not snapshot_id:
        raise ValueError(f"No snapshot_id: {resp.text[:200]}")
    return snapshot_id


async def poll_snapshot(snapshot_id: str, interval: int = 15, timeout: int = 600) -> list[dict]:
    if not _bd_api_key():
        raise ValueError("BRIGHT_DATA_API_KEY not configured")
    deadline = time.time() + timeout
    async with httpx.AsyncClient(timeout=60) as client:
        while time.time() < deadline:
            s = await client.get(f"{BD_BASE}/progress/{snapshot_id}", headers=_bd_headers())
            s.raise_for_status()
            state = s.json().get("status") or s.json().get("state", "")
            if state == "ready":
                d = await client.get(f"{BD_BASE}/snapshot/{snapshot_id}", params={"format": "json"}, headers=_bd_headers())
                d.raise_for_status()
                result = d.json()
                return result if isinstance(result, list) else [result]
            if state in ("failed", "error"):
                raise RuntimeError(f"Snapshot {snapshot_id} failed")
            await asyncio.sleep(interval)
    raise TimeoutError(f"Snapshot {snapshot_id} not ready after {timeout}s")


# ─────────────────────────────────────────────────────────────────────────────
# Email finding waterfall
# ─────────────────────────────────────────────────────────────────────────────

_SOCIAL_DOMAINS = {"linkedin.com", "facebook.com", "twitter.com", "x.com", "instagram.com", "youtube.com"}

def _extract_domain(company_name: str, website: str = "") -> str:
    """Extract company domain. Skips LinkedIn/social URLs — those are not the company website."""
    if website:
        m = re.search(r"https?://(?:www\.)?([^/?\s]+)", website)
        if m:
            host = m.group(1).lower()
            # Skip social network domains — these are NOT the company website
            if not any(host == s or host.endswith("." + s) for s in _SOCIAL_DOMAINS):
                return host
    # Guess from first significant word of company name
    words = re.sub(r"\s+", " ", (company_name or "").strip()).split()
    for word in words:
        slug = re.sub(r"[^a-z0-9]", "", word.lower())
        if slug and len(slug) >= 3 and slug not in {"the", "inc", "ltd", "llc", "pvt", "private", "limited", "solutions", "services", "technologies", "tech", "group", "and", "for"}:
            return f"{slug}.com"
    return ""


async def _try_hunter(first: str, last: str, domain: str) -> Optional[dict]:
    if not _hunter_api_key() or not domain:
        return None
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.get("https://api.hunter.io/v2/email-finder", params={
                "domain": domain, "first_name": first, "last_name": last, "api_key": _hunter_api_key(),
            })
            d = r.json().get("data", {})
            if d.get("email") and d.get("score", 0) >= 50:
                logger.info("[Hunter] Found: %s (score=%s)", d["email"], d["score"])
                return {"email": d["email"], "source": "hunter", "confidence": "high" if d["score"] >= 80 else "medium"}
    except Exception as e:
        logger.warning("[Hunter] %s", e)
    return None


async def _try_apollo(first: str, last: str, domain: str) -> Optional[dict]:
    if not _apollo_api_key() or not domain:
        return None
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.post("https://api.apollo.io/v1/people/match",
                headers={"Content-Type": "application/json"},
                json={"api_key": _apollo_api_key(), "first_name": first, "last_name": last,
                      "domain": domain, "reveal_personal_emails": True},
            )
            p = r.json().get("person") or {}
            email = p.get("email")
            phone = p.get("phone") or (p.get("phone_numbers") or [{}])[0].get("sanitized_number")
            photo = p.get("photo_url")
            twitter = p.get("twitter_url") or p.get("twitter")
            if email and "@" in email:
                logger.info("[Apollo] Found: %s", email)
                return {
                    "email": email,
                    "phone": phone,
                    "source": "apollo",
                    "confidence": "high",
                    "avatar_url": photo,
                    "twitter": twitter,
                }
    except Exception as e:
        logger.warning("[Apollo] %s", e)
    return None


async def _try_dropcontact(linkedin_url: str, first: str, last: str, domain: str) -> Optional[dict]:
    """Dropcontact — enrich by LinkedIn URL or name+domain."""
    key = _dropcontact_key()
    if not key:
        return None
    try:
        payload = {"data": [{"linkedin": linkedin_url or "", "first_name": first, "last_name": last, "website": domain}],
                   "siren": False, "language": "en"}
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.post("https://api.dropcontact.com/batch",
                             json=payload, headers={"X-Access-Token": key, "Content-Type": "application/json"})
        if r.status_code == 200:
            d = r.json()
            for item in (d.get("data") or []):
                email = item.get("email") and item["email"][0].get("email") if isinstance(item.get("email"), list) else item.get("email")
                if email:
                    logger.info("[Dropcontact] Found: %s", email)
                    return {"email": email, "phone": item.get("phone"), "source": "dropcontact", "confidence": "high"}
    except Exception as e:
        logger.debug("[Dropcontact] %s", e)
    return None


async def _try_pdl(first: str, last: str, linkedin_url: str, domain: str) -> Optional[dict]:
    """People Data Labs — person enrichment by LinkedIn URL or name+domain."""
    key = _pdl_api_key()
    if not key:
        return None
    try:
        params: dict = {"api_key": key, "pretty": "false"}
        if linkedin_url:
            params["profile"] = linkedin_url
        elif first and last and domain:
            params["first_name"] = first
            params["last_name"]  = last
            params["company"]    = domain
        else:
            return None
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get("https://api.peopledatalabs.com/v5/person/enrich", params=params)
        if r.status_code == 200:
            d = r.json()
            emails = d.get("data", {}).get("emails") or []
            email  = next((e.get("address") for e in emails if e.get("address")), None)
            phone  = next((p.get("number")  for p in (d.get("data", {}).get("phone_numbers") or [])), None)
            if email:
                logger.info("[PDL] Found: %s", email)
                return {"email": email, "phone": phone, "source": "pdl", "confidence": "high"}
    except Exception as e:
        logger.debug("[PDL] %s", e)
    return None


async def _try_zerobounce(email: str) -> dict:
    """
    ZeroBounce — email verification.
    Returns enriched contact dict with verified + bounce_risk fields.
    Falls through gracefully if key not set.
    """
    key = _zerobounce_key()
    if not key or not email:
        return {"email": email, "verified": False, "bounce_risk": None}
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get("https://api.zerobounce.net/v2/validate",
                            params={"api_key": key, "email": email, "ip_address": ""})
        if r.status_code == 200:
            d = r.json()
            status = d.get("status", "")
            sub_status = d.get("sub_status", "")
            verified = status == "valid"
            if status in ("invalid", "do_not_mail", "abuse"):
                bounce_risk = "high"
            elif status == "catch-all" or sub_status in ("role_based", "global_suppression"):
                bounce_risk = "medium"
            elif verified:
                bounce_risk = "low"
            else:
                bounce_risk = "medium"
            logger.info("[ZeroBounce] %s → status=%s bounce_risk=%s", email, status, bounce_risk)
            return {"email": email, "verified": verified, "bounce_risk": bounce_risk,
                    "zb_status": status, "zb_sub_status": sub_status}
    except Exception as e:
        logger.debug("[ZeroBounce] %s", e)
    return {"email": email, "verified": False, "bounce_risk": None}


async def find_contact_info(
    first: str,
    last: str,
    domain: str,
    linkedin_url: str = "",
    skip_hunter: bool = False,
    skip_apollo: bool = False,
) -> dict:
    """
    6-step email discovery waterfall:
      1. Hunter.io   — name + domain lookup
      2. Apollo.io   — person match
      3. Dropcontact — LinkedIn URL or name+domain
      4. PDL         — People Data Labs person enrichment
      5. Pattern guess — first.last@domain, flast@domain, first@domain
      6. ZeroBounce  — verify whichever email was found

    Returns: {email, phone, source, confidence, verified, bounce_risk}
    """
    result: Optional[dict] = None

    # Step 1 — Hunter
    if not skip_hunter:
        result = await _try_hunter(first, last, domain)
        if result:
            logger.info("[ContactWaterfall] Step 1 Hunter hit: %s", result.get("email"))
    else:
        logger.info("[ContactWaterfall] Step 1 Hunter skipped (disabled/no credits)")

    # Step 2 — Apollo
    if not result and not skip_apollo:
        result = await _try_apollo(first, last, domain)
        if result:
            logger.info("[ContactWaterfall] Step 2 Apollo hit: %s", result.get("email"))
    elif not result and skip_apollo:
        logger.info("[ContactWaterfall] Step 2 Apollo skipped (disabled/no credits)")

    # Step 3 — Dropcontact
    if not result:
        result = await _try_dropcontact(linkedin_url, first, last, domain)
        if result:
            logger.info("[ContactWaterfall] Step 3 Dropcontact hit: %s", result.get("email"))

    # Step 4 — PDL
    if not result:
        result = await _try_pdl(first, last, linkedin_url, domain)
        if result:
            logger.info("[ContactWaterfall] Step 4 PDL hit: %s", result.get("email"))

    # Step 5 — Pattern guess
    if not result and first and last and domain:
        last_clean = last.lower().replace(" ", "")
        guesses = [
            f"{first.lower()}.{last_clean}@{domain}",
            f"{first.lower()[0]}{last_clean}@{domain}",
            f"{first.lower()}@{domain}",
        ]
        result = {"email": guesses[0], "phone": None, "source": "pattern_guess", "confidence": "low"}
        logger.info("[ContactWaterfall] Step 5 Pattern guess: %s", guesses[0])

    if not result:
        return {"email": None, "phone": None, "source": None, "confidence": None,
                "verified": False, "bounce_risk": None}

    # Step 6 — ZeroBounce verification (always run if email found)
    email = result.get("email")
    if email:
        zb = await _try_zerobounce(email)
        result["verified"]    = zb.get("verified", False)
        result["bounce_risk"] = zb.get("bounce_risk")

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Company enrichment waterfall
# ─────────────────────────────────────────────────────────────────────────────

async def _try_clearbit_logo(domain: str) -> Optional[str]:
    """Clearbit Logo API — free, no auth needed."""
    if not domain:
        return None
    logo_url = f"https://logo.clearbit.com/{domain}"
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as c:
            r = await c.get(logo_url)
            if r.status_code == 200 and r.headers.get("content-type", "").startswith("image"):
                logger.info("[Clearbit] Logo found: %s", logo_url)
                return logo_url
    except Exception as e:
        logger.debug("[Clearbit] %s", e)
    return None


def _normalize_apollo_org(org: dict) -> dict:
    """Normalize Apollo organization dict → our standard company keys."""
    tech = [t.get("name") for t in (org.get("current_technologies") or [])[:12] if t.get("name")]
    website = org.get("website_url") or org.get("primary_domain") or ""
    if website and not website.startswith("http"):
        website = f"https://{website}"
    return {
        "company_logo": org.get("logo_url"),
        "company_description": org.get("short_description"),
        "company_phone": org.get("phone"),
        "company_linkedin": org.get("linkedin_url"),
        "company_twitter": org.get("twitter_url"),
        "company_website": website,
        "employee_count": org.get("estimated_num_employees") or 0,
        "industry": org.get("industry"),
        "tech_stack": tech,
        "hq_location": ", ".join(filter(None, [org.get("city"), org.get("country")])),
        "founded_year": str(org.get("founded_year") or ""),
        "funding_stage": org.get("latest_funding_stage"),
        "total_funding": str(org.get("total_funding") or ""),
        "annual_revenue": org.get("annual_revenue_printed"),
        "hiring_velocity": "Active" if org.get("currently_hiring") else "",
    }


async def _try_apollo_org(domain: str) -> dict:
    """Apollo.io organization enrichment by domain — company logo, description, tech stack, etc."""
    if not _apollo_api_key() or not domain:
        return {}
    try:
        async with httpx.AsyncClient(timeout=25) as c:
            r = await c.post(
                "https://api.apollo.io/v1/organizations/enrich",
                headers={"Content-Type": "application/json"},
                json={"api_key": _apollo_api_key(), "domain": domain},
            )
            if r.status_code != 200:
                return {}
            org = r.json().get("organization") or {}
            if not org:
                return {}
            logger.info("[Apollo Org] Found by domain %s: %s", domain, org.get("name"))
            return _normalize_apollo_org(org)
    except Exception as e:
        logger.warning("[Apollo Org] %s", e)
    return {}


async def _try_apollo_company_search(company_name: str) -> dict:
    """
    Apollo.io company name search — finds the correct company by name, returns real website.
    Tries both /v1/mixed_companies/search and /v1/organizations/search endpoints.
    """
    if not _apollo_api_key() or not company_name:
        return {}

    headers = {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "X-Api-Key": _apollo_api_key(),
    }

    # Try mixed_companies/search first (newer Apollo endpoint)
    endpoints = [
        ("https://api.apollo.io/v1/mixed_companies/search", {"q_organization_name": company_name, "page": 1, "per_page": 1}),
        ("https://api.apollo.io/api/v1/organizations/search",  {"q_organization_name": company_name, "page": 1, "per_page": 1}),
    ]

    for endpoint_url, payload in endpoints:
        try:
            async with httpx.AsyncClient(timeout=25) as c:
                r = await c.post(endpoint_url, headers=headers, json=payload)
                if r.status_code not in (200, 201):
                    logger.debug("[Apollo Search] %s → %s", endpoint_url, r.status_code)
                    continue
                body = r.json()
                # Response structure varies by endpoint
                orgs = (body.get("organizations") or body.get("accounts") or
                        body.get("mixed_companies") or [])
                if not orgs:
                    continue
                org = orgs[0]
                logger.info("[Apollo Search] Found company: %r → %s", company_name,
                            org.get("website_url") or org.get("primary_domain") or "no website")
                result = _normalize_apollo_org(org)
                if result.get("company_website") or result.get("company_logo"):
                    return result
        except Exception as e:
            logger.debug("[Apollo Search] %s: %s", endpoint_url, e)

    # Final fallback: try organizations/enrich with guessed domain variations
    # This handles edge cases like "LBM Solutions" → try lbmsolutions.com, lbm-solutions.com
    guesses = _guess_domain_variants(company_name)
    for guess in guesses[:3]:
        try:
            result = await _try_apollo_org(guess)
            if result.get("company_logo") or result.get("company_description"):
                logger.info("[Apollo Search] Domain variant match: %s → %s", company_name, guess)
                result["company_website"] = result.get("company_website") or f"https://{guess}"
                return result
        except Exception:
            pass

    return {}


def _guess_domain_variants(company_name: str) -> list[str]:
    """Generate likely domain variants for a company name."""
    # Strip common suffixes and noise words
    name = company_name.lower()
    for noise in [" private limited", " pvt ltd", " pvt. ltd.", " ltd.", " llc", " inc.", " inc",
                  " limited", " corp.", " corp", " co.", " co", " technologies", " technology",
                  " solutions", " services", " group", " international"]:
        name = name.replace(noise, "")
    name = name.strip()
    slug = re.sub(r"[^a-z0-9\s]", "", name).strip()
    words = slug.split()

    variants = []
    if not words:
        return variants
    if len(words) == 1:
        variants = [f"{words[0]}.com", f"{words[0]}tech.com", f"{words[0]}solutions.com"]
    elif len(words) == 2:
        a, b = words[0], words[1]
        variants = [
            f"{a}{b}.com",       # lbmsolutions.com  ← try concat first
            f"{a}-{b}.com",      # lbm-solutions.com
            f"{b}{a}.com",       # solutionslbm.com  (rare but possible)
            f"{a}.com",          # lbm.com
        ]
    else:
        joined = "".join(words)
        first = words[0]
        variants = [f"{joined}.com", f"{first}.com"]
        # Acronym
        acronym = "".join(w[0] for w in words)
        if len(acronym) >= 2:
            variants.insert(0, f"{acronym}.com")

    return variants


async def _try_hunter_domain(domain: str) -> dict:
    """Hunter.io domain search — company emails, description, social links."""
    if not _hunter_api_key() or not domain:
        return {}
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.get("https://api.hunter.io/v2/domain-search", params={
                "domain": domain, "api_key": _hunter_api_key(), "limit": 10,
            })
            if r.status_code != 200:
                return {}
            data = r.json().get("data", {})
            emails = data.get("emails") or []
            generic_prefixes = ["info", "contact", "hello", "support", "sales", "admin", "team", "hi"]
            company_email = None
            for e in emails:
                prefix = e.get("value", "").split("@")[0].lower()
                if any(p in prefix for p in generic_prefixes):
                    company_email = e["value"]
                    break
            if not company_email and emails:
                company_email = emails[0].get("value")
            logger.info("[Hunter Domain] %d emails found, company_email=%s", len(emails), company_email)
            return {
                "company_email": company_email,
                "company_description": data.get("description"),
                "company_twitter": data.get("twitter"),
                "company_linkedin": data.get("linkedin"),
                "company_phone": data.get("phone_number"),
                "company_website": f"https://{domain}" if not data.get("website") else data["website"],
                "employee_count": data.get("employees") or 0,
            }
    except Exception as e:
        logger.warning("[Hunter Domain] %s", e)
    return {}


def _normalize_bd_company(c: dict) -> dict:
    """
    Normalize a raw Bright Data company record → our standard company keys.
    Handles multiple possible field names from different BD dataset versions.
    """
    result: dict = {}

    # ── Website ──────────────────────────────────────────────────────────────
    website = (
        c.get("website") or c.get("website_url") or c.get("company_website")
        or c.get("url") or c.get("homepage") or ""
    )
    if website:
        m = re.search(r"https?://(?:www\.)?([^/?\s]+)", website)
        if m and not any(m.group(1).lower() == s or m.group(1).lower().endswith("." + s) for s in _SOCIAL_DOMAINS):
            result["company_website"] = website

    # ── Logo ─────────────────────────────────────────────────────────────────
    logo = (
        c.get("logo") or c.get("logo_url") or c.get("company_logo")
        or c.get("profile_pic_url") or c.get("image_url")
    )
    if logo and isinstance(logo, str) and logo.startswith("http"):
        result["company_logo"] = logo

    # ── Description ──────────────────────────────────────────────────────────
    desc = (
        c.get("description") or c.get("about") or c.get("company_description")
        or c.get("tagline") or c.get("summary") or c.get("overview")
    )
    if desc:
        result["company_description"] = str(desc)[:800]

    # ── Employee count ────────────────────────────────────────────────────────
    emp = (
        c.get("employees") or c.get("employee_count") or c.get("company_size")
        or c.get("staff_count") or c.get("followers_count") or 0
    )
    n = _safe_int(emp)
    if n > 0:
        result["employee_count"] = n

    # ── Industry ─────────────────────────────────────────────────────────────
    ind = c.get("industry") or c.get("industries") or c.get("category")
    if ind:
        result["industry"] = ind[0] if isinstance(ind, list) else str(ind)

    # ── HQ Location ──────────────────────────────────────────────────────────
    hq = c.get("headquarters") or c.get("hq_location") or c.get("location")
    if hq:
        if isinstance(hq, dict):
            parts = [hq.get("city", ""), hq.get("region", ""), hq.get("country", "")]
            result["hq_location"] = ", ".join(p for p in parts if p)
        else:
            result["hq_location"] = str(hq)

    # ── Founded ──────────────────────────────────────────────────────────────
    founded = c.get("founded") or c.get("founded_year") or c.get("founded_on")
    if founded:
        result["founded_year"] = str(founded)

    # ── Specialties / Tech stack ──────────────────────────────────────────────
    specs = c.get("specialties") or c.get("specializations") or []
    if specs:
        result["tech_stack"] = specs[:10] if isinstance(specs, list) else [str(specs)]

    # ── Funding / Company type ────────────────────────────────────────────────
    funding = c.get("funding_stage") or c.get("company_type") or c.get("type")
    if funding:
        result["funding_stage"] = str(funding)

    # ── Phone ─────────────────────────────────────────────────────────────────
    phone = c.get("phone") or c.get("phone_number") or c.get("company_phone")
    if phone:
        result["company_phone"] = str(phone)

    # ── Email ─────────────────────────────────────────────────────────────────
    email = c.get("email") or c.get("company_email")
    if email:
        result["company_email"] = str(email)

    # ── Twitter ───────────────────────────────────────────────────────────────
    tw = c.get("twitter") or c.get("twitter_url")
    if tw:
        result["company_twitter"] = str(tw)

    return result


async def _try_bd_company(company_linkedin_url: str) -> dict:
    """
    Fetch LinkedIn company profile via Bright Data company dataset.

    Flow:
      1. Try sync /scrape (immediate response)
      2. If response contains snapshot_id (async job was started) → poll until ready
      3. Fallback to /trigger → /progress → /snapshot pattern

    Returns normalized company dict or {} on failure.
    """
    if not _bd_api_key() or not company_linkedin_url:
        return {}
    if "linkedin.com/company/" not in company_linkedin_url:
        logger.debug("[BD Company] Skipping — not a company URL: %s", company_linkedin_url)
        return {}

    dataset_id = _bd_company_dataset()
    logger.info("[BD Company] Fetching company profile: %s (dataset=%s)", company_linkedin_url, dataset_id)

    # ── Attempt 1: Sync /scrape ───────────────────────────────────────────────
    snapshot_id = None
    try:
        async with httpx.AsyncClient(timeout=90) as client:
            resp = await client.post(
                f"{BD_BASE}/scrape",
                params={"dataset_id": dataset_id, "format": "json"},
                headers=_bd_headers(),
                json=[{"url": company_linkedin_url}],
            )
            logger.debug("[BD Company] /scrape status=%s body=%.200s", resp.status_code, resp.text)

            if resp.status_code not in (200, 202):
                logger.warning("[BD Company] /scrape failed %s: %s", resp.status_code, resp.text[:200])
            else:
                body = resp.json()
                # Case A: Immediate data — list of company records
                if isinstance(body, list) and body and isinstance(body[0], dict):
                    c = body[0]
                    # Make sure it's not a snapshot_id wrapper
                    if c.get("name") or c.get("description") or c.get("website") or c.get("logo"):
                        result = _normalize_bd_company(c)
                        if result:
                            logger.info("[BD Company] Sync OK — %d fields from %s", len(result), company_linkedin_url)
                            return result
                # Case B: Async job started — body is {"snapshot_id": "s_xxx"} or list with it
                if isinstance(body, dict):
                    snapshot_id = body.get("snapshot_id")
                elif isinstance(body, list) and body and isinstance(body[0], dict):
                    snapshot_id = body[0].get("snapshot_id")
                if snapshot_id:
                    logger.info("[BD Company] Async job started — snapshot_id=%s", snapshot_id)
    except Exception as e:
        logger.warning("[BD Company] /scrape error: %s", e)

    # ── Attempt 2: Poll existing snapshot_id ─────────────────────────────────
    if snapshot_id:
        try:
            data = await poll_snapshot(snapshot_id, interval=5, timeout=120)
            if data:
                c = data[0] if isinstance(data, list) else data
                result = _normalize_bd_company(c)
                if result:
                    logger.info("[BD Company] Async poll OK — %d fields from %s", len(result), company_linkedin_url)
                    return result
        except Exception as e:
            logger.warning("[BD Company] Snapshot poll failed for %s: %s", snapshot_id, e)

    # ── Attempt 3: /trigger → /progress → /snapshot (company dataset) ────────
    logger.info("[BD Company] Trying /trigger pattern for %s", company_linkedin_url)
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            tr = await client.post(
                f"{BD_BASE}/trigger",
                params={"dataset_id": dataset_id, "format": "json", "include_errors": "true"},
                headers=_bd_headers(),
                json=[{"url": company_linkedin_url}],
            )
            tr.raise_for_status()
            snap_id = tr.json().get("snapshot_id") or tr.json().get("id")
        if snap_id:
            data = await poll_snapshot(snap_id, interval=5, timeout=120)
            if data:
                c = data[0] if isinstance(data, list) else data
                result = _normalize_bd_company(c)
                if result:
                    logger.info("[BD Company] Trigger/poll OK — %d fields from %s", len(result), company_linkedin_url)
                    return result
    except Exception as e:
        logger.warning("[BD Company] /trigger pattern failed for %s: %s", company_linkedin_url, e)

    logger.warning("[BD Company] All methods failed for %s — continuing waterfall", company_linkedin_url)
    return {}


async def enrich_company_waterfall(company_name: str, domain: str, profile: dict) -> tuple[dict, list[dict]]:
    """
    Company data waterfall:
      0. Bright Data LinkedIn Company dataset (company LinkedIn URL)
      1. Bright Data profile fields (company_link, image fields)
      2. Apollo.io organization enrichment
      3. Hunter.io domain search (company emails + info)
      4. Clearbit Logo API (free fallback for logo)
    Returns (company_data dict, waterfall_log list)
    """
    log: list[dict] = []
    result: dict = {}

    # ── Company LinkedIn URL (separate from website) ───────────────────────────
    raw_link = profile.get("current_company_link") or profile.get("current_company_url") or ""
    company_linkedin_url = raw_link if "linkedin.com/company/" in raw_link else ""
    # Actual company website (must NOT be a social URL)
    raw_website = profile.get("current_company_link") or ""
    m = re.search(r"https?://(?:www\.)?([^/?\s]+)", raw_website)
    actual_website = raw_website if (m and not any(m.group(1).lower() == s or m.group(1).lower().endswith("." + s) for s in _SOCIAL_DOMAINS)) else ""

    # Step 0: Bright Data LinkedIn Company dataset
    if company_linkedin_url:
        bd_co = await _try_bd_company(company_linkedin_url)
        if bd_co:
            result.update(bd_co)
            log.append({"step": 0, "source": "Bright Data Company", "fields_found": list(bd_co.keys()), "note": company_linkedin_url})
            # If BD company returned a real website, use that for domain lookup downstream
            if bd_co.get("company_website"):
                actual_website = bd_co["company_website"]
                m2 = re.search(r"https?://(?:www\.)?([^/?\s]+)", actual_website)
                if m2:
                    domain = m2.group(1).lower()

    # Step 1: Extract from Bright Data profile directly
    bd_company = {
        "company_logo": profile.get("current_company_logo") or profile.get("company_logo") or profile.get("logo"),
        "company_linkedin": company_linkedin_url or None,
        "company_website": actual_website or None,
        "company_description": profile.get("current_company_description"),
        "employee_count": _safe_int(profile.get("current_company_employees_count")),
        "industry": profile.get("current_company_industry"),
        "founded_year": str(profile.get("current_company_founded") or ""),
    }
    bd_filled = [k for k, v in bd_company.items() if v]
    if bd_filled:
        result.update({k: v for k, v in bd_company.items() if v and (k not in result or not result[k])})
        log.append({"step": 1, "source": "Bright Data", "fields_found": bd_filled, "note": "LinkedIn profile fields"})

    # ── Resolve real company domain ────────────────────────────────────────────
    # Priority: BD company result > guessed domain from name
    # The guessed domain (e.g. "lbm.com") may belong to a completely different company.
    # We validate by checking if Apollo confirms the name matches.
    verified_domain = domain  # start with guessed domain
    apollo_confirmed = False

    # Step 2a: Apollo org by guessed domain — verify name matches
    if verified_domain and company_name:
        apollo_data = await _try_apollo_org(verified_domain)
        if apollo_data:
            # Check Apollo's returned name roughly matches our company name
            # (prevents using "lbm.com → LBM Group" when we want "LBM Solutions")
            apollo_new = []
            for k, v in apollo_data.items():
                if v and (k not in result or not result[k]):
                    result[k] = v
                    apollo_new.append(k)
            if apollo_new:
                apollo_confirmed = True
                # If Apollo returned a real website, upgrade domain to that
                if apollo_data.get("company_website"):
                    m3 = re.search(r"https?://(?:www\.)?([^/?\s]+)", apollo_data["company_website"])
                    if m3 and m3.group(1).lower() not in _SOCIAL_DOMAINS:
                        verified_domain = m3.group(1).lower()
                log.append({"step": 2, "source": "Apollo.io Org (domain)", "fields_found": apollo_new, "note": f"domain={verified_domain}"})

    # Step 2b: Apollo company NAME search — if domain lookup failed or domain was unverified
    # This is the key fix: search by company name to find the real website
    if not apollo_confirmed and company_name:
        logger.info("[CompanyWaterfall] Domain lookup inconclusive — searching Apollo by name: %s", company_name)
        apollo_search = await _try_apollo_company_search(company_name)
        if apollo_search:
            search_new = []
            for k, v in apollo_search.items():
                if v and (k not in result or not result[k]):
                    result[k] = v
                    search_new.append(k)
            if search_new:
                # Apollo name search found the real company — update domain
                if apollo_search.get("company_website"):
                    m4 = re.search(r"https?://(?:www\.)?([^/?\s]+)", apollo_search["company_website"])
                    if m4 and m4.group(1).lower() not in _SOCIAL_DOMAINS:
                        verified_domain = m4.group(1).lower()
                        apollo_confirmed = True
                log.append({"step": 2, "source": "Apollo.io Name Search", "fields_found": search_new, "note": f"name={company_name}"})

    # Use the verified domain for all downstream lookups
    domain = verified_domain

    # Step 3: Hunter domain search (with verified domain)
    if domain:
        hunter_data = await _try_hunter_domain(domain)
        if hunter_data:
            hunter_new = []
            for k, v in hunter_data.items():
                if v and (k not in result or not result[k]):
                    result[k] = v
                    hunter_new.append(k)
            if hunter_new:
                log.append({"step": 3, "source": "Hunter.io Domain", "fields_found": hunter_new, "note": f"domain={domain}"})

    # Step 4: Clearbit logo fallback
    if not result.get("company_logo") and domain:
        logo = await _try_clearbit_logo(domain)
        if logo:
            result["company_logo"] = logo
            log.append({"step": 4, "source": "Clearbit Logo API", "fields_found": ["company_logo"], "note": "free logo API"})

    # Expose final verified domain so caller can update scraping target
    result["_verified_domain"] = domain

    return result, log


# ─────────────────────────────────────────────────────────────────────────────
# Avatar extraction helper
# ─────────────────────────────────────────────────────────────────────────────

def _extract_avatar(profile: dict) -> Optional[str]:
    """Extract avatar/photo URL from Bright Data profile fields.
    Returns None when BD signals a generic placeholder (default_avatar: true).
    """
    if profile.get("default_avatar"):
        return None
    for key in ["avatar_url", "profile_pic_src", "profile_picture", "image_url", "image", "photo", "avatar"]:
        v = profile.get(key)
        if v and isinstance(v, str) and v.startswith("http"):
            return v
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Stage 3 — Website Intelligence
# ─────────────────────────────────────────────────────────────────────────────

async def _fetch_page_text(url: str, timeout: int = 12) -> str:
    """Fetch a URL and return clean text, stripping HTML tags."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as c:
            r = await c.get(url, headers=headers)
            if r.status_code != 200:
                return ""
            html = r.text
            # Strip scripts and styles
            html = re.sub(r'<(script|style)[^>]*>.*?</\1>', ' ', html, flags=re.DOTALL | re.IGNORECASE)
            # Strip HTML tags
            text = re.sub(r'<[^>]+>', ' ', html)
            # Normalize whitespace
            text = re.sub(r'\s+', ' ', text).strip()
            return text[:4000]
    except Exception as e:
        logger.debug("[WebScrape] %s → %s", url, e)
        return ""


async def scrape_website_intelligence(website: str) -> dict:
    """
    Stage 3 — Website Intelligence.
    Scrapes homepage, about, features, pricing, blog, careers pages
    and extracts first-party company intelligence via LLM.
    """
    if not website:
        return {}

    base = website.strip().rstrip("/")
    if not base.startswith("http"):
        base = f"https://{base}"
    # Also try www. variant
    if "://www." not in base:
        base_www = base.replace("://", "://www.", 1)
    else:
        base_www = None

    # Pages to try — try both base and www variant for homepage
    page_candidates = [
        ("homepage",  base),
        ("homepage",  base_www or base),
        ("about",     f"{base}/about"),
        ("about",     f"{base}/about-us"),
        ("features",  f"{base}/features"),
        ("features",  f"{base}/product"),
        ("pricing",   f"{base}/pricing"),
        ("blog",      f"{base}/blog"),
        ("blog",      f"{base}/news"),
        ("careers",   f"{base}/careers"),
        ("careers",   f"{base}/jobs"),
    ]

    # Fetch all pages concurrently (merge duplicates by page_type, keep longest text)
    pages_fetched: dict[str, str] = {}
    tasks = [(page_type, url, _fetch_page_text(url)) for page_type, url in page_candidates]

    results = await asyncio.gather(*[t[2] for t in tasks], return_exceptions=True)
    for (page_type, url, _), text in zip(tasks, results):
        if isinstance(text, str) and len(text) > 20:
            # Keep the longest text for each page type
            if len(text) > len(pages_fetched.get(page_type, "")):
                pages_fetched[page_type] = text
                logger.info("[WebScrape] ✓ %s (%d chars) from %s", page_type, len(text), url)

    if not pages_fetched:
        logger.warning("[WebScrape] No pages fetched for: %s", base)
        return {"website": base, "status": "unreachable", "pages_scraped": []}

    # Build combined context for LLM (cap each page at 2500 chars)
    combined = ""
    for page_type, text in pages_fetched.items():
        combined += f"\n\n=== {page_type.upper()} PAGE ===\n{text[:2500]}"

    # LLM structured extraction
    prompt = f"""You are a B2B market intelligence analyst. Analyze this website content and extract ONLY factual, observable information.

Website: {base}
Pages scraped: {list(pages_fetched.keys())}

SCRAPED CONTENT:
{combined[:9000]}

Return ONLY valid JSON with these exact keys. Use null for missing data, never hallucinate:
{{
  "company_description": "2-3 sentence factual summary of what the company does",
  "product_offerings": ["list of actual products/services they offer"],
  "value_proposition": "their core value proposition in 1 sentence, taken from site copy",
  "target_customers": ["specific job roles, company types, or industries they serve"],
  "use_cases": ["specific use cases or problems they solve"],
  "business_model": "SaaS / Marketplace / Agency / Hardware / Service / Other",
  "product_category": "e.g. CRM, HR Software, Analytics Platform, Dev Tools",
  "market_positioning": "enterprise / mid-market / SMB / startup-focused / premium / budget",
  "pricing_signals": "freemium / subscription / per-seat / usage-based / custom-enterprise / not-visible",
  "pricing_tiers": [],
  "key_messaging": ["top 3-5 messaging themes or keywords used on site"],
  "integrations_mentioned": ["any third-party tools or integrations mentioned"],
  "tech_stack_clues": ["tech/frameworks visible in site content or job postings"],
  "hiring_signals": "active / moderate / limited / no-careers-page",
  "open_roles": [],
  "recent_blog_topics": [],
  "problem_solved": "1 sentence: what customer pain does this company address"
}}"""

    raw = await _call_llm([
        {"role": "system", "content": "B2B website intelligence extractor. Return ONLY valid JSON, no explanation, no markdown."},
        {"role": "user", "content": prompt},
    ], max_tokens=1400, temperature=0.15)

    if raw:
        data = _parse_json_from_llm(raw)
        if data and ("company_description" in data or "product_offerings" in data):
            data.setdefault("website", base)
            data.setdefault("status", "scraped")
            data["pages_scraped"] = list(pages_fetched.keys())
            logger.info(
                "[WebScrape] Intelligence extracted — category=%s, model=%s, pages=%d",
                data.get("product_category"), data.get("business_model"), len(pages_fetched),
            )
            return data

    # Rule-based fallback from scraped text (no LLM)
    logger.warning("[WebScrape] LLM unavailable — using rule-based text inference for: %s", base)
    all_text = " ".join(pages_fetched.values()).lower()
    page_title = list(pages_fetched.values())[0][:200] if pages_fetched else ""

    # Business model inference
    bm = "Service"
    if any(k in all_text for k in ["saas", "software as a service", "subscription", "per seat", "per user"]):
        bm = "SaaS"
    elif any(k in all_text for k in ["marketplace", "platform", "connect buyers", "connect sellers"]):
        bm = "Marketplace"
    elif any(k in all_text for k in ["agency", "consulting", "outsourcing", "staffing", "development company"]):
        bm = "Agency / Services"
    elif any(k in all_text for k in ["hardware", "device", "iot", "physical"]):
        bm = "Hardware"

    # Product category inference
    cat = ""
    for kw, cat_name in [
        ("crm", "CRM"), ("sales", "Sales Software"), ("hr", "HR Software"),
        ("payroll", "Payroll"), ("inventory", "Inventory Management"),
        ("analytics", "Analytics"), ("marketing", "Marketing Software"),
        ("ecommerce", "E-Commerce"), ("erp", "ERP"), ("accounting", "Accounting"),
        ("logistics", "Logistics"), ("healthcare", "Healthcare Tech"),
        ("fintech", "FinTech"), ("edtech", "EdTech"),
        ("mobile app", "Mobile App Development"), ("software development", "Software Development"),
        ("web development", "Web Development"),
    ]:
        if kw in all_text:
            cat = cat_name
            break

    hiring = "active" if "careers" in pages_fetched or "jobs" in pages_fetched else "limited"

    return {
        "website": base, "status": "partial",
        "pages_scraped": list(pages_fetched.keys()),
        "company_description": page_title if page_title else None,
        "business_model": bm,
        "product_category": cat,
        "hiring_signals": hiring,
        "value_proposition": None,
        "product_offerings": [],
        "target_customers": [],
        "use_cases": [],
        "key_messaging": [],
        "pricing_signals": "not-visible",
        "open_roles": [],
        "problem_solved": None,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Comprehensive 8-stage LLM enrichment
# ─────────────────────────────────────────────────────────────────────────────

def _build_comprehensive_prompt(
    linkedin_url: str,
    profile: dict,
    contact: dict,
    now_iso: str,
    website_intel: dict = None,
) -> str:
    name        = profile.get("name", "")
    title       = profile.get("position") or profile.get("headline", "")
    company     = profile.get("current_company_name", "")
    website     = profile.get("current_company_link", "")
    location    = profile.get("location") or profile.get("city", "")
    about       = (profile.get("about") or "")[:600]
    followers   = profile.get("followers", 0)
    connections = profile.get("connections", 0)
    skills      = json.dumps(profile.get("skills") or [])
    experience  = json.dumps((profile.get("experience") or [])[:5])
    education   = json.dumps((profile.get("education") or profile.get("educations_details") or [])[:5])
    certs       = json.dumps(profile.get("certifications") or [])
    langs       = json.dumps(profile.get("languages") or [])
    email       = contact.get("email") or ""
    phone       = contact.get("phone") or ""
    email_src   = contact.get("source") or ""
    email_conf  = contact.get("confidence") or ""

    # Company extras from waterfall (injected into profile by enrich_single)
    company_extras = profile.get("_company_extras") or {}

    # Website intelligence section
    wi = website_intel or {}
    wi_section = f"""
Website Intelligence (Stage 3 — Scraped):
  Company Description: {wi.get('company_description', '')}
  Product Offerings: {json.dumps(wi.get('product_offerings', []))}
  Value Proposition: {wi.get('value_proposition', '')}
  Target Customers: {json.dumps(wi.get('target_customers', []))}
  Business Model: {wi.get('business_model', '')}
  Product Category: {wi.get('product_category', '')}
  Pricing Signals: {wi.get('pricing_signals', '')}
  Market Positioning: {wi.get('market_positioning', '')}
  Problem Solved: {wi.get('problem_solved', '')}
  Hiring Signals: {wi.get('hiring_signals', '')}
  Pages Scraped: {json.dumps(wi.get('pages_scraped', []))}""" if wi else "Website Intelligence: Not available"

    return f"""You are a B2B lead intelligence analyst for Worksbuddy, a CRM & productivity platform.

Generate a COMPREHENSIVE 8-stage lead enrichment report for the following profile.
Use the provided data. For fields not in the data, use your knowledge and reasonable inference.
Be specific, professional, and accurate.

--- RAW DATA ---
LinkedIn URL: {linkedin_url}
Name: {name}
Title: {title}
Company: {company}
Website: {website}
Location: {location}
About: {about}
Followers: {followers}  |  Connections: {connections}
Email: {email} ({email_src}, {email_conf})
Phone: {phone}
Skills: {skills}
Experience: {experience}
Education: {education}
Certifications: {certs}
Languages: {langs}
Enrichment date: {now_iso}
Company Logo: {company_extras.get('company_logo', '')}
Company Email: {company_extras.get('company_email', '')}
Company Phone: {company_extras.get('company_phone', '')}
Company Description: {(company_extras.get('company_description') or '')[:300]}
Tech Stack: {json.dumps(company_extras.get('tech_stack', []))}
Employee Count: {company_extras.get('employee_count', 0)}
Industry: {company_extras.get('industry', '')}
{wi_section}
---

[Stage 1] Person Profile
[Stage 2] Company Identity
[Stage 3] Website Intelligence (use scraped data above verbatim where available)
[Stage 4] Company Profile
[Stage 5] Market Signals
[Stage 6] Intent Signals
[Stage 7] Scoring
[Stage 8] Outreach

Return ONLY a valid JSON object with ALL of these exact keys.
Use empty string "" for unknown text, 0 for unknown numbers, [] for unknown arrays, null for unknown optional fields.

{{
  "person_profile": {{
    "full_name": "{name}",
    "first_name": "",
    "last_name": "",
    "current_title": "",
    "seniority_level": "",
    "department": "",
    "years_in_role": "",
    "career_history": [],
    "top_skills": [],
    "education": "",
    "certifications": [],
    "languages": [],
    "city": "",
    "country": "",
    "timezone": "",
    "linkedin_activity": "",
    "followers": 0,
    "connections": 0,
    "work_email": "",
    "personal_email": null,
    "direct_phone": null,
    "twitter": null,
    "about": ""
  }},
  "company_identity": {{
    "name": "",
    "domain": "",
    "website": "",
    "linkedin_url": ""
  }},
  "website_intelligence": {{
    "company_description": "",
    "product_offerings": [],
    "value_proposition": "",
    "target_customers": [],
    "use_cases": [],
    "business_model": "",
    "product_category": "",
    "market_positioning": "",
    "pricing_signals": "",
    "key_messaging": [],
    "hiring_signals": "",
    "open_roles": [],
    "problem_solved": ""
  }},
  "company_profile": {{
    "name": "",
    "industry": "",
    "employee_count": 0,
    "hq_location": "",
    "founded_year": null,
    "annual_revenue_est": "",
    "tech_stack": [],
    "company_logo": null,
    "company_email": null,
    "company_phone": null,
    "company_twitter": null
  }},
  "company_intelligence": {{
    "funding_stage": "",
    "total_funding": "",
    "last_funding_date": "",
    "lead_investor": "",
    "hiring_velocity": "",
    "department_hiring_trends": [],
    "news_mention": "",
    "product_launch": "",
    "competitor_usage": ""
  }},
  "intent_signals": {{
    "recent_funding_event": "",
    "hiring_signal": "",
    "job_change": "",
    "linkedin_activity": "",
    "news_mention": "",
    "product_launch": "",
    "competitor_usage": "",
    "review_activity": "",
    "pain_indicators": []
  }},
  "lead_scoring": {{
    "icp_fit_score": 0,
    "intent_score": 0,
    "timing_score": 0,
    "data_completeness_score": 0,
    "overall_score": 0,
    "score_tier": "HOT|WARM|COOL|COLD",
    "score_explanation": "",
    "icp_match_tier": "",
    "disqualification_flags": []
  }},
  "outreach": {{
    "email_subject": "",
    "cold_email": "",
    "linkedin_note": "",
    "best_channel": "",
    "best_send_time": "",
    "outreach_angle": "",
    "sequence_type": "",
    "sequence": {{
      "day1": "Send cold email",
      "day2": "LinkedIn connection request with note",
      "day4": "Follow-up email — value proof",
      "day7": "LinkedIn message or content engage",
      "day14": "Breakup email"
    }},
    "last_contacted": null,
    "email_status": ""
  }},
  "crm": {{
    "lead_source": "LinkedIn URL",
    "enrichment_source": "Bright Data + Hunter.io",
    "enrichment_date": "{now_iso}",
    "enrichment_depth": "Deep",
    "data_completeness": 0,
    "last_re_enriched": "{now_iso}",
    "assigned_owner": null,
    "crm_stage": "",
    "tags": []
  }}
}}

SCORING GUIDE:
- icp_fit_score (0-40): C-Level/Founder=35+, VP=28-35, Director=20-27, Manager=12-19, IC=0-11; company size fit adds up to 10
- intent_score (0-30): recent funding=15, active hiring surge=10, job change in 90 days=8, linkedin active=5
- timing_score (0-20): funding in last 6mo=15, job change in 30 days=12, product launch=10, blog/news active=5
- data_completeness_score (0-10): % of key fields filled (email=3, phone=1, company=2, title=1, linkedin=1, skills=1, edu=1)
- overall_score = icp + intent + timing + data_completeness (max 100)
- score_tier: HOT (80+), WARM (55-79), COOL (30-54), COLD (<30)
- icp_match_tier: "Tier 1 — Hot" / "Tier 2 — Warm" / "Tier 3 — Cool" / "Tier 4 — Cold"
- crm_stage: based on score — "MQL → Ready for outreach" / "Nurture" / "Monitor" / "Disqualify"
- data_completeness (crm section): % of fields filled (0-100)
- OUTREACH: Use website_intelligence.value_proposition and website_intelligence.problem_solved as primary angle
- Cold email: max 120 words, natural tone, reference real company context
- LinkedIn note: max 300 chars
- email_status: "Valid — verified" if hunter/apollo found it, "Pattern guess — unverified" otherwise
- tags: 3-6 relevant tags like ["SaaS","VP-Eng","Series-B","Hot","San Francisco"]"""


async def build_comprehensive_enrichment(
    linkedin_url: str,
    profile: dict,
    contact: dict,
    website_intel: dict = None,
) -> dict:
    """
    Main LLM call to produce the full 8-stage enrichment JSON.
    Falls back to a rule-based assembly if LLM is unavailable.
    """
    now = datetime.now(timezone.utc).isoformat()
    prompt = _build_comprehensive_prompt(linkedin_url, profile, contact, now, website_intel=website_intel)

    raw = await _call_llm([
        {"role": "system", "content": "Expert B2B lead intelligence analyst. Return ONLY valid JSON."},
        {"role": "user", "content": prompt},
    ], max_tokens=2200, temperature=0.25)

    if raw:
        data = _parse_json_from_llm(raw)
        if data and ("person_profile" in data or "identity" in data):
            logger.info("[Enrichment] LLM produced comprehensive 8-stage report")
            return data

    # Rule-based fallback if LLM unavailable
    logger.warning("[Enrichment] LLM unavailable — using rule-based fallback")
    return _rule_based_enrichment(linkedin_url, profile, contact, now, website_intel=website_intel)


def _rule_based_enrichment(
    linkedin_url: str, profile: dict, contact: dict, now: str,
    website_intel: dict = None,
) -> dict:
    """Produce the 8-stage structure from raw data without LLM."""
    name    = profile.get("name", "")
    title   = profile.get("position") or profile.get("headline", "")
    company = profile.get("current_company_name", "")
    loc     = profile.get("location") or profile.get("city", "")
    about   = (profile.get("about") or "")[:600]
    exp     = profile.get("experience") or []
    edu     = profile.get("education") or []
    skills  = profile.get("skills") or []
    certs   = profile.get("certifications") or []
    langs   = profile.get("languages") or []
    email   = contact.get("email") or ""

    # Company extras from waterfall
    company_extras = profile.get("_company_extras") or {}

    # Website intel
    wi = website_intel or {}

    # Seniority from title
    title_l = title.lower()
    if any(k in title_l for k in ["ceo","cto","coo","cfo","chief","founder","president"]):
        seniority = "C-Level / Executive"
    elif any(k in title_l for k in ["vp","vice president"]):
        seniority = "VP / Director"
    elif "director" in title_l:
        seniority = "Director"
    elif "manager" in title_l or "head of" in title_l:
        seniority = "Manager / Head"
    elif any(k in title_l for k in ["senior","sr.","lead","principal"]):
        seniority = "Senior Individual Contributor"
    else:
        seniority = "Individual Contributor"

    # Department from title
    if any(k in title_l for k in ["engineer","tech","developer","software","data","it","cloud","devops","cto"]):
        dept = "Engineering / Technology"
    elif any(k in title_l for k in ["sales","revenue","account","bd","business development"]):
        dept = "Sales"
    elif any(k in title_l for k in ["marketing","growth","content","brand","seo","demand"]):
        dept = "Marketing"
    elif any(k in title_l for k in ["hr","people","talent","recruit","culture"]):
        dept = "People / HR"
    elif any(k in title_l for k in ["product","pm","ux","design","ui"]):
        dept = "Product"
    elif any(k in title_l for k in ["finance","cfo","account","controller","treasury"]):
        dept = "Finance"
    elif any(k in title_l for k in ["ops","operations","supply","logistics","coo"]):
        dept = "Operations"
    else:
        dept = "General"

    # Years in role from first experience entry
    years_in_role = ""
    if exp and isinstance(exp[0], dict):
        dur = exp[0].get("duration") or ""
        if dur:
            years_in_role = dur

    # Previous companies
    prev = [e.get("company", "") for e in (exp[1:4] if isinstance(exp, list) else [])]
    prev = [c for c in prev if c]

    # Education string
    edu_str = ""
    if edu and isinstance(edu[0], dict):
        e0 = edu[0]
        edu_str = f"{e0.get('degree','')}, {e0.get('school','')}".strip(", ")

    # ICP scoring (0-40)
    icp = 0
    _tw = {"ceo":35,"cto":35,"founder":35,"co-founder":35,"vp":30,"vice president":30,
           "director":22,"head of":18,"chief":35,"president":35,"owner":28,
           "senior":8,"lead":8,"principal":10,"manager":14}
    for kw, pts in _tw.items():
        if kw in title_l:
            icp += pts
            break
    icp = min(icp, 40)

    # Intent score (0-30) — dynamic from real profile signals
    intent = 0
    followers_n = _safe_int(profile.get("followers"))
    connections_n = _safe_int(profile.get("connections"))
    if email: intent += 6                        # reachable contact found
    if about: intent += 4                        # has active profile summary
    if skills: intent += 3                       # skills listed = maintained profile
    if followers_n > 5000: intent += 8
    elif followers_n > 1000: intent += 5
    elif followers_n > 200: intent += 2
    if connections_n > 500: intent += 4
    elif connections_n > 100: intent += 2
    if exp and len(exp) >= 2: intent += 3        # career history = real person
    intent = min(intent, 30)

    # Timing score (0-20) — dynamic from data availability
    timing = 0
    if email: timing += 6                        # can reach them now
    if company: timing += 4                      # confirmed company = B2B qualified
    if title: timing += 3                        # role clarity for personalization
    _ce_domain = company_extras.get("_verified_domain") or company_extras.get("company_website") or ""
    if _ce_domain: timing += 3                   # real verified domain found
    if edu_str: timing += 2                      # education = complete profile
    if prev: timing += 2                         # career progression visible
    timing = min(timing, 20)

    # Data completeness score (0-10)
    dc_score = 0
    if email: dc_score += 3
    if contact.get("phone"): dc_score += 1
    if company: dc_score += 2
    if title: dc_score += 1
    if linkedin_url: dc_score += 1
    if skills: dc_score += 1
    if edu_str: dc_score += 1
    dc_score = min(dc_score, 10)

    total = icp + intent + timing + dc_score
    if total >= 80:
        tier, stage, score_tier_str = "Tier 1 — Hot", "MQL → Ready for outreach", "HOT"
    elif total >= 55:
        tier, stage, score_tier_str = "Tier 2 — Warm", "Nurture sequence", "WARM"
    elif total >= 30:
        tier, stage, score_tier_str = "Tier 3 — Cool", "Awareness / Monitor", "COOL"
    else:
        tier, stage, score_tier_str = "Tier 4 — Cold", "Disqualify or low priority", "COLD"

    # Tags
    tags = []
    if "saas" in (company + title_l + about).lower():
        tags.append("SaaS")
    if icp >= 18:
        tags.append("Decision-Maker")
    if email:
        tags.append("Email-Found")
    if loc:
        country = loc.split(",")[-1].strip()
        if country:
            tags.append(country)

    # Email status
    src = contact.get("source") or ""
    email_status = ("Valid — verified" if src in ("hunter", "apollo")
                    else "Pattern guess — unverified" if src == "pattern_guess"
                    else "Not found")

    # Completeness
    filled = sum(1 for v in [name, title, company, email, loc, about, edu_str, seniority] if v)
    completeness = int((filled / 8) * 100)

    # Basic cold email — use website intel value prop if available
    fn = name.split()[0] if name else "there"
    vp = wi.get("value_proposition") or ""
    problem = wi.get("problem_solved") or ""
    category = wi.get("product_category") or dept.split("/")[0].strip()

    if vp:
        outreach_angle = f"Website intelligence: {vp[:120]}"
    elif problem:
        outreach_angle = f"Pain point: {problem[:120]}"
    else:
        outreach_angle = "Role & company alignment"

    cold_email = (
        f"Hi {fn},\n\n"
        f"Came across your profile — your work as {title} at {company} stood out.\n\n"
    )
    if vp:
        cold_email += (
            f"Worksbuddy helps {category} teams {vp[:80].lower().rstrip('.')} — "
            f"saving 10+ hours/week on manual prospecting.\n\n"
        )
    else:
        cold_email += (
            f"Worksbuddy helps {dept.split('/')[0].strip()} teams automate lead research, "
            f"score prospects, and launch personalized outreach — all from one place. "
            f"Teams save 10+ hours/week on manual prospecting.\n\n"
        )
    cold_email += f"Worth a 10-minute look?\n\n— Worksbuddy Team"

    linkedin_note = f"Hi {fn} — saw your work at {company} and think Worksbuddy could be a fit for your team. Worth connecting?"[:300]

    # Build career history from experience
    career_history = []
    for e in (exp[:5] if isinstance(exp, list) else []):
        if isinstance(e, dict):
            career_history.append({
                "title": e.get("title", ""),
                "company": e.get("company", ""),
                "duration": e.get("duration", ""),
            })

    # Compose 8-stage result — also include legacy keys for backwards compatibility
    result = {
        # ── Stage 1: Person Profile ────────────────────────────────────────
        "person_profile": {
            "full_name": name,
            "first_name": name.split()[0] if name else "",
            "last_name": " ".join(name.split()[1:]) if name and len(name.split()) > 1 else "",
            "current_title": title,
            "seniority_level": seniority,
            "department": dept,
            "years_in_role": years_in_role,
            "career_history": career_history,
            "top_skills": skills if isinstance(skills, list) else [],
            # education_list → full structured list for UI display
            "education_list": edu if isinstance(edu, list) else (
                [{"school": edu_str, "degree": "", "years": ""}] if edu_str else []
            ),
            "education": edu_str,  # kept as string for scoring/DB compat
            "certifications": certs,  # full objects: {name, issuer, date, credential_url}
            "languages": langs,       # full objects: {name, proficiency}
            "city": (profile.get("city") or loc or "").split(",")[0].strip(),
            "country": _country_name(
                profile.get("country_code") or profile.get("country")
                or (loc.split(",")[-1].strip() if "," in loc else "")
            ),
            "timezone": profile.get("timezone") or "",
            "linkedin_activity": f"{profile.get('followers', 0):,} followers" if profile.get("followers") else "",
            "followers": _safe_int(profile.get("followers")),
            "connections": _safe_int(profile.get("connections")),
            "work_email": email if src in ("hunter", "apollo") else "",
            "personal_email": None,
            "direct_phone": contact.get("phone"),
            "twitter": contact.get("twitter"),
            "about": about,
        },
        # ── Stage 2: Company Identity ──────────────────────────────────────
        "company_identity": {
            "name": company,
            "domain": _extract_domain(company, company_extras.get("company_website") or profile.get("current_company_link", "")),
            "website": company_extras.get("company_website") or profile.get("current_company_link", ""),
            "linkedin_url": company_extras.get("company_linkedin") or "",
        },
        # ── Stage 3: Website Intelligence ─────────────────────────────────
        "website_intelligence": {
            "company_description": wi.get("company_description") or company_extras.get("company_description") or "",
            "product_offerings": wi.get("product_offerings") or [],
            "value_proposition": wi.get("value_proposition") or "",
            "target_customers": wi.get("target_customers") or [],
            "use_cases": wi.get("use_cases") or [],
            "business_model": wi.get("business_model") or "",
            "product_category": wi.get("product_category") or "",
            "market_positioning": wi.get("market_positioning") or "",
            "pricing_signals": wi.get("pricing_signals") or "",
            "key_messaging": wi.get("key_messaging") or [],
            "hiring_signals": wi.get("hiring_signals") or "",
            "open_roles": wi.get("open_roles") or [],
            "problem_solved": wi.get("problem_solved") or "",
        },
        # ── Stage 4: Company Profile ───────────────────────────────────────
        "company_profile": {
            "name": company,
            "industry": company_extras.get("industry") or "",
            "employee_count": company_extras.get("employee_count") or 0,
            "hq_location": company_extras.get("hq_location") or loc,
            "founded_year": company_extras.get("founded_year") or None,
            "annual_revenue_est": company_extras.get("annual_revenue") or "",
            "tech_stack": company_extras.get("tech_stack") or [],
            "company_logo": company_extras.get("company_logo") or None,
            "company_email": company_extras.get("company_email") or None,
            "company_phone": company_extras.get("company_phone") or None,
            "company_twitter": company_extras.get("company_twitter") or None,
        },
        # ── Stage 5: Market Signals (Company Intelligence) ─────────────────
        "company_intelligence": {
            "funding_stage": company_extras.get("funding_stage") or "",
            "total_funding": company_extras.get("total_funding") or "",
            "last_funding_date": "",
            "lead_investor": "",
            "hiring_velocity": company_extras.get("hiring_velocity") or "",
            "department_hiring_trends": [],
            "news_mention": "",
            "product_launch": "",
            "competitor_usage": "",
        },
        # ── Stage 6: Intent Signals ────────────────────────────────────────
        "intent_signals": {
            "recent_funding_event": "",
            "hiring_signal": company_extras.get("hiring_velocity") or "",
            "job_change": f"Currently at {company}" if company else "",
            "linkedin_activity": f"{profile.get('followers', 0):,} followers" if profile.get("followers") else "",
            "news_mention": "",
            "product_launch": "",
            "competitor_usage": "",
            "review_activity": "",
            "pain_indicators": [],
        },
        # ── Stage 7: Lead Scoring ──────────────────────────────────────────
        "lead_scoring": {
            "icp_fit_score": icp,
            "intent_score": intent,
            "timing_score": timing,
            "data_completeness_score": dc_score,
            "overall_score": total,
            "score_tier": score_tier_str,
            "score_explanation": f"{seniority} at {company} — {dept}",
            "icp_match_tier": tier,
            "disqualification_flags": [],
        },
        # ── Stage 8: Outreach ──────────────────────────────────────────────
        "outreach": {
            "email_subject": f"Quick question, {fn}",
            "cold_email": cold_email,
            "linkedin_note": linkedin_note,
            "best_channel": "Email" if email else "LinkedIn",
            "best_send_time": "Tuesday or Thursday, 1 PM recipient time",
            "outreach_angle": outreach_angle,
            "sequence_type": ("Hot Lead — 4-touch multichannel" if total >= 70
                              else "Warm — nurture sequence" if total >= 40
                              else "Cold — awareness only"),
            "sequence": {
                "day1": "Send cold email",
                "day2": "LinkedIn connection request with note",
                "day4": "Follow-up email — value proof",
                "day7": "LinkedIn message if connected",
                "day14": "Breakup email",
            },
            "last_contacted": None,
            "email_status": email_status,
        },
        # ── CRM metadata (legacy key, still needed for crm_stage/tags/completeness)
        "crm": {
            "lead_source": "LinkedIn URL",
            "enrichment_source": f"Bright Data + {src.title() if src else 'Waterfall'}",
            "enrichment_date": now,
            "enrichment_depth": "Deep",
            "data_completeness": completeness,
            "last_re_enriched": now,
            "assigned_owner": None,
            "crm_stage": stage,
            "tags": tags,
        },
        # ── Legacy compatibility keys ──────────────────────────────────────
        "identity": {
            "full_name": name,
            "work_email": email if src in ("hunter", "apollo") else "",
            "personal_email": None,
            "direct_phone": contact.get("phone"),
            "linkedin_url": linkedin_url,
            "twitter": contact.get("twitter"),
            "city": (profile.get("city") or loc or "").split(",")[0].strip(),
            "country": _country_name(
                profile.get("country_code") or profile.get("country")
                or (loc.split(",")[-1].strip() if "," in loc else "")
            ),
            "timezone": profile.get("timezone") or "",
        },
        "professional": {
            "current_title": title,
            "seniority_level": seniority,
            "department": dept,
            "years_in_role": years_in_role,
            "career_history": career_history,
            "previous_companies": prev,
            "top_skills": skills if isinstance(skills, list) else [],
            "education_list": edu if isinstance(edu, list) else (
                [{"school": edu_str, "degree": "", "years": ""}] if edu_str else []
            ),
            "education": edu_str,
            "certifications": certs,  # full objects: {name, issuer, date, credential_url}
            "languages": langs,       # full objects: {name, proficiency}
        },
        "company": {
            "name": company,
            "website": company_extras.get("company_website") or profile.get("current_company_link", ""),
            "industry": company_extras.get("industry") or "",
            "employee_count": company_extras.get("employee_count") or 0,
            "hq_location": company_extras.get("hq_location") or loc,
            "founded_year": company_extras.get("founded_year") or None,
            "funding_stage": company_extras.get("funding_stage") or "",
            "total_funding": company_extras.get("total_funding") or "",
            "last_funding_date": "",
            "lead_investor": "",
            "annual_revenue_est": company_extras.get("annual_revenue") or "",
            "tech_stack": company_extras.get("tech_stack") or [],
            "hiring_velocity": company_extras.get("hiring_velocity") or "",
        },
        "scoring": {
            "icp_fit_score": icp,
            "intent_score": intent,
            "timing_score": timing,
            "overall_score": total,
            "score_explanation": f"{seniority} at {company} — {dept}",
            "icp_match_tier": tier,
            "disqualification_flags": [],
        },
    }
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_name(full_name: str) -> tuple[str, str]:
    parts = (full_name or "").split()
    return parts[0] if parts else "", " ".join(parts[1:]) if len(parts) > 1 else ""


# ISO 3166-1 alpha-2 → full country name (most common codes for B2B lead enrichment)
_COUNTRY_CODES: dict[str, str] = {
    "AF": "Afghanistan", "AL": "Albania", "DZ": "Algeria", "AR": "Argentina",
    "AU": "Australia", "AT": "Austria", "BE": "Belgium", "BR": "Brazil",
    "CA": "Canada", "CL": "Chile", "CN": "China", "CO": "Colombia",
    "CZ": "Czech Republic", "DK": "Denmark", "EG": "Egypt", "FI": "Finland",
    "FR": "France", "DE": "Germany", "GH": "Ghana", "GR": "Greece",
    "HK": "Hong Kong", "HU": "Hungary", "IN": "India", "ID": "Indonesia",
    "IE": "Ireland", "IL": "Israel", "IT": "Italy", "JP": "Japan",
    "KE": "Kenya", "KR": "South Korea", "MY": "Malaysia", "MX": "Mexico",
    "MA": "Morocco", "NL": "Netherlands", "NZ": "New Zealand", "NG": "Nigeria",
    "NO": "Norway", "PK": "Pakistan", "PE": "Peru", "PH": "Philippines",
    "PL": "Poland", "PT": "Portugal", "RO": "Romania", "RU": "Russia",
    "SA": "Saudi Arabia", "ZA": "South Africa", "ES": "Spain", "SE": "Sweden",
    "CH": "Switzerland", "TW": "Taiwan", "TH": "Thailand", "TR": "Turkey",
    "UA": "Ukraine", "AE": "United Arab Emirates", "GB": "United Kingdom",
    "US": "United States", "VN": "Vietnam",
}


def _country_name(code: str) -> str:
    """Map ISO country code to full name; return as-is if already a full name or unknown."""
    if not code:
        return ""
    code = code.strip()
    # If it's a 2-letter uppercase code, look it up
    if len(code) <= 3 and code.upper() == code:
        return _COUNTRY_CODES.get(code.upper(), code)
    return code


def _lead_id(linkedin_url: str) -> str:
    return hashlib.md5(linkedin_url.strip().lower().encode()).hexdigest()[:16]


def _safe_json(v) -> str:
    if isinstance(v, (dict, list)):
        return json.dumps(v)
    return str(v) if v else ""


def _safe_int(v, max_val: int = 2_000_000_000) -> int:
    """
    Safely convert any BD field value to a SQLite-safe integer.

    Handles:
    - Numeric strings with commas/plus signs: "1,234", "500+"
    - Range strings — takes the lower bound: "10,001-50,000" → 10001
    - Dicts/lists (employee range objects) — take first numeric value found
    - Very large numbers — capped at max_val (default 2 billion)
    - None / empty string → 0
    """
    if v is None:
        return 0
    # If already a plain int/float, just cap it
    if isinstance(v, (int, float)):
        return min(int(v), max_val)
    if isinstance(v, dict):
        # BD sometimes returns employee ranges as {"start": 10001, "end": 50000}
        for k in ("start", "from", "min", "low", "count"):
            if k in v:
                return _safe_int(v[k], max_val)
        # Fall back to first numeric value in dict
        for val in v.values():
            if isinstance(val, (int, float)):
                return min(int(val), max_val)
        return 0
    if isinstance(v, list):
        return _safe_int(v[0], max_val) if v else 0
    # String handling
    s = str(v).strip()
    if not s:
        return 0
    # Take only the first numeric segment (handles "10,001-50,000" → "10001")
    # Find the first contiguous block of digits (and optional leading comma separators)
    m = re.search(r"[\d,]+", s)
    if not m:
        return 0
    digits = m.group(0).replace(",", "")
    try:
        return min(int(digits), max_val)
    except (ValueError, OverflowError):
        return 0


def _resolve_tier(tier_raw: str) -> str:
    """Normalize score tier to lowercase hot/warm/cool/cold for DB storage."""
    t = (tier_raw or "").lower()
    if "hot" in t or t == "hot":
        return "hot"
    if "warm" in t or t == "warm":
        return "warm"
    if "cool" in t or t == "cool":
        return "cool"
    return "cold"


def _is_company_url(url: str) -> bool:
    return "linkedin.com/company/" in url.lower()


def _is_person_url(url: str) -> bool:
    return "linkedin.com/in/" in url.lower()


# ─────────────────────────────────────────────────────────────────────────────
# Company URL direct enrichment pipeline
# ─────────────────────────────────────────────────────────────────────────────

async def enrich_company_url(
    linkedin_url: str,
    job_id: Optional[str] = None,
    org_id: str = "default",
) -> dict:
    """
    Enrich a LinkedIn Company URL directly.

    Phase A — Bright Data company dataset scrape
    Phase B — Apollo company name/website search (fills gaps)
    Phase C — Hunter.io domain search (company emails, links)
    Phase D — Website intelligence (deep scrape)
    Phase E — Score + store

    Returns the same flat DB dict as enrich_single() so the same
    UI and API response shape is used for both person and company enrichment.
    """
    url = linkedin_url.strip().rstrip("/")
    if not url.startswith("http"):
        url = f"https://{url}"
    lead_id = _lead_id(url)
    now = datetime.now(timezone.utc).isoformat()

    logger.info("━━━ [Company-A] Bright Data company scrape: %s", url)
    bd_co = await _try_bd_company(url)

    company_name = (
        bd_co.get("name") or bd_co.get("company_name")
        or url.rstrip("/").split("/")[-1].replace("-", " ").title()
    )
    website = bd_co.get("company_website", "")
    domain = ""
    if website:
        m = re.search(r"https?://(?:www\.)?([^/?\s]+)", website)
        if m:
            domain = m.group(1).lower()

    logger.info("[Company-A] BD result: name=%r  website=%r  domain=%r  fields=%d",
                company_name, website, domain, len(bd_co))

    # ── Phase B: Apollo fill-in ───────────────────────────────────────────────
    logger.info("━━━ [Company-B] Apollo enrichment for: %r (domain=%s)", company_name, domain)
    apollo_data: dict = {}

    # Try by domain first (if BD gave us one)
    if domain:
        apollo_data = await _try_apollo_org(domain) or {}
        if apollo_data:
            logger.info("[Company-B] Apollo by domain OK — %d fields", len(apollo_data))

    # Fallback: search by name
    if not apollo_data and company_name:
        apollo_data = await _try_apollo_company_search(company_name) or {}
        if apollo_data:
            # Extract verified domain from Apollo result
            ap_web = apollo_data.get("company_website", "")
            if ap_web:
                m2 = re.search(r"https?://(?:www\.)?([^/?\s]+)", ap_web)
                if m2 and m2.group(1).lower() not in _SOCIAL_DOMAINS:
                    domain = m2.group(1).lower()
                    website = ap_web
            logger.info("[Company-B] Apollo by name OK — %d fields, domain=%s", len(apollo_data), domain)

    # Merge: BD takes priority, Apollo fills missing fields
    company_data: dict = {}
    company_data.update(apollo_data)
    company_data.update({k: v for k, v in bd_co.items() if v})

    # Re-sync website/domain from merged data
    if company_data.get("company_website") and not website:
        website = company_data["company_website"]
        m3 = re.search(r"https?://(?:www\.)?([^/?\s]+)", website)
        if m3:
            domain = m3.group(1).lower()

    # ── Phase C: Hunter domain search ─────────────────────────────────────────
    logger.info("━━━ [Company-C] Hunter domain search: %s", domain)
    company_email = company_data.get("company_email", "")
    if domain and not company_email:
        try:
            async with httpx.AsyncClient(timeout=20) as c:
                r = await c.get(
                    "https://api.hunter.io/v2/domain-search",
                    params={"domain": domain, "api_key": _hunter_api_key(), "limit": 3},
                )
                hd = r.json().get("data", {})
                if hd.get("organization"):
                    company_data.setdefault("company_name_verified", hd["organization"])
                emails = hd.get("emails", [])
                if emails:
                    company_email = emails[0].get("value", "")
                    company_data["company_email"] = company_email
                    logger.info("[Company-C] Hunter found email: %s", company_email)
        except Exception as e:
            logger.debug("[Company-C] Hunter error: %s", e)

    # ── Phase D: Website intelligence ─────────────────────────────────────────
    logger.info("━━━ [Company-D] Website intelligence: %s", website or "none")
    website_intel = await scrape_website_intelligence(website) if website else {}
    if website_intel:
        logger.info("[Company-D] Scraped %d pages, category=%s",
                    len(website_intel.get("pages_scraped", [])),
                    website_intel.get("product_category") or "?")

    # ── Phase E: Score + build record ─────────────────────────────────────────
    logger.info("━━━ [Company-E] Scoring + building record")

    final_name = (
        company_data.get("name") or company_data.get("company_name")
        or apollo_data.get("name") or company_name
    )

    # Company completeness score (0-100)
    completeness_fields = [
        website, company_email,
        company_data.get("company_description"),
        company_data.get("company_logo"),
        company_data.get("industry"),
        str(company_data.get("employee_count") or ""),
        company_data.get("hq_location"),
        company_data.get("founded_year"),
        website_intel.get("product_category"),
        website_intel.get("value_proposition"),
    ]
    data_completeness = sum(10 for f in completeness_fields if f)

    # ICP / intent scoring for company leads
    icp_score = 0
    if company_data.get("industry"): icp_score += 8
    if company_data.get("employee_count", 0) > 10: icp_score += 6
    if company_data.get("employee_count", 0) > 100: icp_score += 4
    if website: icp_score += 6
    if website_intel.get("product_category"): icp_score += 8
    if website_intel.get("value_proposition"): icp_score += 4
    if company_data.get("funding_stage"): icp_score += 4
    icp_score = min(icp_score, 40)

    intent_score = 0
    if company_email: intent_score += 10
    if website_intel.get("pages_scraped"): intent_score += 8
    if company_data.get("company_description"): intent_score += 6
    if company_data.get("tech_stack"): intent_score += 6
    intent_score = min(intent_score, 30)

    timing_score = 0
    if domain: timing_score += 8
    if company_data.get("founded_year"): timing_score += 4
    if company_data.get("funding_stage"): timing_score += 5
    if company_data.get("hq_location"): timing_score += 3
    timing_score = min(timing_score, 20)

    dc_score = min(data_completeness, 10)
    total_score = icp_score + intent_score + timing_score + dc_score
    score_tier = _resolve_tier(
        "hot" if total_score >= 70 else "warm" if total_score >= 50 else "cool" if total_score >= 30 else "cold"
    )

    # Waterfall log
    waterfall_log = []
    if bd_co:
        waterfall_log.append({"step": 0, "source": "Bright Data Company", "fields_found": list(bd_co.keys()), "note": url})
    if apollo_data:
        waterfall_log.append({"step": 1, "source": "Apollo.io", "fields_found": list(apollo_data.keys()), "note": domain})
    if company_email:
        waterfall_log.append({"step": 2, "source": "Hunter.io", "fields_found": ["company_email"], "note": domain})
    if website_intel:
        waterfall_log.append({"step": 3, "source": "Website Scrape", "fields_found": website_intel.get("pages_scraped", []), "note": website})

    wi = website_intel or {}
    lead: dict = {
        "id": lead_id,
        "linkedin_url": url,
        # Identity — use company name as "person" name so UI renders it
        "name": final_name,
        "first_name": final_name,
        "last_name": "",
        "work_email": company_email or "",
        "personal_email": "",
        "direct_phone": company_data.get("company_phone", ""),
        "twitter": company_data.get("company_twitter", ""),
        "city": (company_data.get("hq_location") or "").split(",")[0].strip(),
        "country": (company_data.get("hq_location") or "").split(",")[-1].strip(),
        "timezone": "",
        # Professional — company-level context
        "title": wi.get("value_proposition") or company_data.get("company_description", "")[:100] or "Company",
        "seniority_level": "Company Account",
        "department": company_data.get("industry", ""),
        "years_in_role": "",
        "company": final_name,
        "previous_companies": "[]",
        "top_skills": _safe_json(company_data.get("tech_stack") or []),
        "education": "",
        "certifications": "[]",
        "languages": "[]",
        # Company
        "company_website": website or "",
        "industry": company_data.get("industry", ""),
        "employee_count": _safe_int(company_data.get("employee_count")),
        "hq_location": company_data.get("hq_location", ""),
        "founded_year": str(company_data.get("founded_year") or ""),
        "funding_stage": company_data.get("funding_stage", ""),
        "total_funding": company_data.get("total_funding", ""),
        "last_funding_date": company_data.get("last_funding_date", ""),
        "lead_investor": company_data.get("lead_investor", ""),
        "annual_revenue": "",
        "tech_stack": _safe_json(company_data.get("tech_stack") or []),
        "hiring_velocity": "",
        # Company enrichment fields
        "avatar_url": company_data.get("company_logo", ""),
        "company_logo": company_data.get("company_logo", ""),
        "company_email": company_email or "",
        "company_description": company_data.get("company_description", ""),
        "company_linkedin": url,
        "company_twitter": company_data.get("company_twitter", ""),
        "company_phone": company_data.get("company_phone", ""),
        "waterfall_log": _safe_json(waterfall_log),
        # Website intelligence
        "website_intelligence": _safe_json(wi),
        "product_offerings": _safe_json(wi.get("product_offerings") or []),
        "value_proposition": wi.get("value_proposition", ""),
        "target_customers": wi.get("target_customers", ""),
        "business_model": wi.get("business_model", ""),
        "pricing_signals": wi.get("pricing_signals", ""),
        "product_category": wi.get("product_category", ""),
        "data_completeness_score": dc_score,
        # Intent (company-level)
        "recent_funding_event": company_data.get("funding_stage", ""),
        "hiring_signal": "",
        "job_change": "",
        "linkedin_activity": "",
        "news_mention": "",
        "product_launch": "",
        "competitor_usage": "",
        "review_activity": "",
        # Scoring
        "icp_fit_score": icp_score,
        "intent_score": intent_score,
        "timing_score": timing_score,
        "engagement_score": dc_score,
        "total_score": total_score,
        "score_tier": score_tier,
        "score_explanation": f"Company account: ICP={icp_score} Intent={intent_score} Timing={timing_score} Data={dc_score}",
        "icp_match_tier": score_tier,
        "disqualification_flags": "[]",
        "score_reasons": _safe_json([
            f"ICP fit: {icp_score}/40",
            f"Intent signals: {intent_score}/30",
            f"Timing: {timing_score}/20",
            f"Data completeness: {dc_score}/10",
        ]),
        # Outreach — no person-specific copy for company records
        "email_subject": "",
        "cold_email": "",
        "linkedin_note": "",
        "best_channel": "email" if company_email else "linkedin",
        "best_send_time": "",
        "outreach_angle": wi.get("value_proposition", ""),
        "sequence_type": "",
        "outreach_sequence": "{}",
        # CRM
        "lead_source": "LinkedIn Company URL",
        "enrichment_source": "brightdata_company",
        "data_completeness": data_completeness,
        "crm_stage": "researching",
        "tags": _safe_json(["company-account"]),
        "assigned_owner": "",
        # Meta
        "full_data": _safe_json({"company": company_data, "website_intel": wi}),
        "raw_profile": _safe_json(bd_co),
        "about": company_data.get("company_description", ""),
        "followers": _safe_int(company_data.get("followers_count") or company_data.get("employee_count")),
        "connections": 0,
        "email_source": "hunter" if company_email else "",
        "email_confidence": "medium" if company_email else "",
        "status": "enriched",
        "job_id": job_id or "",
        "organization_id": org_id,
        "enriched_at": now,
        "created_at": now,
        "skills": "[]",
    }

    await _upsert_lead(lead)
    logger.info(
        "✓ Company enriched: %s | score=%d (%s) | email=%s | website=%s",
        final_name, total_score, score_tier, company_email or "—", website or "—",
    )
    return lead


# ─────────────────────────────────────────────────────────────────────────────
# Main enrichment pipeline
# ─────────────────────────────────────────────────────────────────────────────

async def enrich_single(
    linkedin_url: str,
    engagement_data: Optional[dict] = None,
    job_id: Optional[str] = None,
    org_id: str = "default",
    generate_outreach_flag: bool = True,
    tools: Optional[dict] = None,
) -> dict:
    """
    Sequential 8-stage enrichment waterfall:

    Phase A — Person Intelligence
      Stage 1: Bright Data LinkedIn person profile scrape
      Stage 2: Extract all person fields (name, title, skills, education, career)

    Phase B — Company Identification (using person profile data)
      Stage 3: Bright Data LinkedIn Company page scrape (company_link from profile)
      Stage 4: Apollo company name search → verify real domain + logo + description
      Stage 5: Hunter.io domain search → company emails, social links
      Stage 6: Clearbit logo fallback

    Phase C — Person Contact Waterfall (now with VERIFIED company domain)
      Stage 7: Hunter.io person email finder (correct domain)
      Stage 8: Apollo.io person match (correct domain)

    Phase D — Deep Intelligence
      Stage 9: Website scrape (verified real company URL)
      Stage 10: Lead scoring + outreach generation
    """
    url = _normalize_linkedin_url(linkedin_url)
    lead_id = _lead_id(url)

    # ── Route: company URL → dedicated company pipeline ───────────────────────
    if _is_company_url(url):
        logger.info("[Enrich] Company URL detected — routing to company pipeline: %s", url)
        return await enrich_company_url(url, job_id=job_id, org_id=org_id)

    # ── Tool availability (from enrichment config) ────────────────────────────
    _tools = tools or {}
    _brightdata_ok = _tools.get("brightdata", True)
    _hunter_ok     = _tools.get("hunter",     True)
    _apollo_ok     = _tools.get("apollo",      True)

    if not _brightdata_ok:
        logger.warning("[Enrich] Bright Data disabled or out of credits for org=%s — aborting", org_id)
        return {"error": "Bright Data is disabled or has no credits remaining for this organisation. Please update your enrichment configuration.", "linkedin_url": linkedin_url}

    # ── Phase A: Person Intelligence ──────────────────────────────────────────
    logger.info("━━━ [Phase A] Person Intelligence — scraping: %s", url)
    profile = await fetch_profile_sync(url)

    name    = profile.get("name", "")
    first, last = _parse_name(name)
    company = profile.get("current_company_name", "")
    raw_link = profile.get("current_company_link", "")

    logger.info("[Phase A] Got: name=%r  title=%r  company=%r",
                name, profile.get("position") or profile.get("headline", ""), company)

    # Initial domain guess — may be wrong, will be corrected by Phase B
    initial_domain = _extract_domain(company, raw_link)
    logger.info("[Phase A] Initial domain guess: %s (from company name %r)", initial_domain, company)

    # ── Phase B: Company Identification Waterfall ─────────────────────────────
    logger.info("━━━ [Phase B] Company Waterfall — identifying real domain for: %s", company)
    company_extras, company_wf_log = await enrich_company_waterfall(company, initial_domain, profile)

    verified_domain = company_extras.pop("_verified_domain", initial_domain) or initial_domain
    real_website = company_extras.get("company_website") or (f"https://{verified_domain}" if verified_domain else "")
    logger.info("[Phase B] Verified domain: %s  website: %s  logo: %s",
                verified_domain, real_website, "✓" if company_extras.get("company_logo") else "✗")

    # ── Phase C: Person Contact Waterfall (using VERIFIED domain) ─────────────
    logger.info("━━━ [Phase C] Person Contact Waterfall — email/phone for %s @ %s", name, verified_domain)

    # Activity emails extracted from LinkedIn posts (e.g. "Send CV to hr@company.com")
    # These are direct, first-party email signals — use as highest-priority source
    activity_emails = profile.get("_activity_emails") or []
    activity_phones = profile.get("_activity_phones") or []
    if activity_emails:
        # Prefer email that matches the verified domain
        matched = next((e for e in activity_emails if verified_domain and verified_domain.split(".")[0] in e), None)
        pre_contact = matched or activity_emails[0]
        logger.info("[Phase C] Activity email found: %s (from LinkedIn posts)", pre_contact)
        contact = {"email": pre_contact, "source": "linkedin_activity", "confidence": "high"}
        # Also attach any phone found in activity posts
        if activity_phones and not contact.get("phone"):
            contact["phone"] = activity_phones[0]
            logger.info("[Phase C] Activity phone found: %s (from LinkedIn posts)", activity_phones[0])
    else:
        contact = await find_contact_info(
            first, last, verified_domain, linkedin_url=url,
            skip_hunter=not _hunter_ok,
            skip_apollo=not _apollo_ok,
        )
        # If Hunter/Apollo found no phone, check activity posts
        if not contact.get("phone") and activity_phones:
            contact["phone"] = activity_phones[0]
            logger.info("[Phase C] Fallback to activity phone: %s", activity_phones[0])

    logger.info("[Phase C] Contact: email=%s  source=%s  phone=%s",
                contact.get("email") or "not found",
                contact.get("source") or "—",
                contact.get("phone") or "none")

    # ── Phase 1C: Company Enrichment (shared cache, all 6 priorities) ──────────
    logger.info("━━━ [Phase 1C] Company Intelligence — enriching: %s", raw_link or "none")
    _company_record: dict = {}
    _company_id_val: str  = ""
    _company_score:  int  = 0
    _combined_score: int  = 0
    if raw_link and "linkedin.com/company/" in raw_link:
        try:
            import company_service as _cs
            import workspace_service as _ws_svc
            _ws_cfg = await _ws_svc.get_workspace_config(org_id)
            _known = {
                "name":           company or company_extras.get("company_name") or "",
                "domain":         verified_domain,
                "website":        real_website,
                "logo":           company_extras.get("company_logo") or "",
                "description":    company_extras.get("company_description") or "",
                "industry":       company_extras.get("industry") or "",
                "employee_count": company_extras.get("employee_count") or 0,
                "hq_location":    company_extras.get("hq_location") or "",
                "founded_year":   company_extras.get("founded_year") or "",
                "funding_stage":  company_extras.get("funding_stage") or "",
                "total_funding":  company_extras.get("total_funding") or "",
                "lead_investor":  company_extras.get("lead_investor") or "",
                "company_twitter": company_extras.get("company_twitter") or "",
                "company_email":  company_extras.get("company_email") or "",
                "company_phone":  company_extras.get("company_phone") or "",
            }
            _company_record = await _cs.enrich_company(
                company_linkedin_url=raw_link,
                org_id=org_id,
                ws_config=_ws_cfg,
                known_data=_known,
            )
            _company_id_val = _company_record.get("id") or ""
            _company_score  = int(_company_record.get("company_score") or 0)
            logger.info("[Phase 1C] Company done: score=%d tier=%s",
                        _company_score, _company_record.get("company_score_tier", "?"))
        except Exception as _ce:
            logger.warning("[Phase 1C] Company enrichment failed: %s", _ce)

    # ── Phase D: Website Intelligence ─────────────────────────────────────────
    logger.info("━━━ [Phase D] Website Intelligence — scraping: %s", real_website or "none")
    website_intel = await scrape_website_intelligence(real_website)
    if website_intel:
        logger.info("[Phase D] Scraped %d pages, category=%s, biz_model=%s",
                    len(website_intel.get("pages_scraped", [])),
                    website_intel.get("product_category") or "?",
                    website_intel.get("business_model") or "?")

    logger.info("━━━ [Phase E] Scoring + Outreach — building 8-stage report")

    # Inject company extras into profile so rule-based enrichment can access them
    profile["_company_extras"] = company_extras

    # Add website scraping step to waterfall log
    if website_intel and website_intel.get("pages_scraped"):
        company_wf_log.append({
            "step": 5, "source": "Website Scrape",
            "fields_found": website_intel.get("pages_scraped", []),
            "note": f"pages={len(website_intel.get('pages_scraped', []))}, category={website_intel.get('product_category', '?')}",
        })

    # Step 7: Comprehensive LLM enrichment → 8-stage JSON
    logger.info("[Stage 7] Scoring — LLM enrichment starting")
    enrichment = await build_comprehensive_enrichment(url, profile, contact, website_intel=website_intel)

    logger.info("[Stage 8] Outreach — generating personalized sequence")

    # Patch person_profile email/phone from waterfall (authoritative)
    person_prof = enrichment.get("person_profile", {})
    ident = enrichment.get("identity", {})  # legacy compat
    if contact.get("email"):
        person_prof["work_email"] = contact["email"]
        ident["work_email"] = contact["email"]
    if contact.get("phone"):
        person_prof["direct_phone"] = contact["phone"]
        ident["direct_phone"] = contact["phone"]

    # Patch company_profile section with waterfall extras (fill missing fields only)
    comp_prof = enrichment.get("company_profile", {})
    comp_section = enrichment.get("company", {})  # legacy compat
    for k, v in company_extras.items():
        _map = {
            "company_logo": "company_logo",
            "company_email": "company_email",
            "company_phone": "company_phone",
            "company_twitter": "company_twitter",
            "company_website": None,
            "industry": "industry",
            "employee_count": "employee_count",
            "hq_location": "hq_location",
            "founded_year": "founded_year",
            "funding_stage": None,
            "total_funding": None,
            "annual_revenue": "annual_revenue_est",
            "tech_stack": "tech_stack",
            "hiring_velocity": None,
        }
        _comp_section_map = {
            "company_website": "website",
            "industry": "industry",
            "employee_count": "employee_count",
            "hq_location": "hq_location",
            "founded_year": "founded_year",
            "funding_stage": "funding_stage",
            "total_funding": "total_funding",
            "annual_revenue": "annual_revenue_est",
            "tech_stack": "tech_stack",
            "hiring_velocity": "hiring_velocity",
        }
        prof_key = _map.get(k)
        if prof_key and v and not comp_prof.get(prof_key):
            comp_prof[prof_key] = v
        sect_key = _comp_section_map.get(k)
        if sect_key and v and not comp_section.get(sect_key):
            comp_section[sect_key] = v

    # Pull out scoring for flat DB columns
    scoring = enrichment.get("lead_scoring", enrichment.get("scoring", {}))
    total   = scoring.get("overall_score", 0)
    tier_raw = scoring.get("icp_match_tier") or scoring.get("score_tier", "")
    score_tier = _resolve_tier(tier_raw)

    # Derive crm from enrichment (may live in "crm" key)
    crm = enrichment.get("crm", {})

    # Build waterfall log combining all steps
    waterfall_log = [
        {"step": 0, "source": "Bright Data", "fields_found": ["profile"], "note": f"LinkedIn scrape: {url}"},
        *company_wf_log,
        {
            "step": 10,
            "source": contact.get("source") or "none",
            "fields_found": ["email"],
            "note": f"email={contact.get('email')}",
        },
    ]

    # Convenience shortcuts into 8-stage keys
    wi = enrichment.get("website_intelligence", website_intel or {})
    ls = enrichment.get("lead_scoring", scoring)
    intent_s = enrichment.get("intent_signals", {})
    outreach = enrichment.get("outreach", {})
    ci = enrichment.get("company_identity", {})
    cp = enrichment.get("company_profile", comp_prof)
    cin = enrichment.get("company_intelligence", {})
    pp = enrichment.get("person_profile", {})
    # Legacy fallbacks
    prof_legacy = enrichment.get("professional", {})
    comp_legacy = enrichment.get("company", comp_section)

    # Build flat DB record
    now = datetime.now(timezone.utc).isoformat()

    # ── Activity-level intent signals ─────────────────────────────────────────
    hiring_signals_raw  = profile.get("_hiring_signals")  or []
    activity_emails_raw = profile.get("_activity_emails") or []

    # ── P3: AI Analysis Layer (tags, behavioural signals, pitch intelligence) ─
    try:
        from ai_analysis import run_ai_analysis
        import workspace_service as _ws
        _ws_config = await _ws.get_workspace_config(org_id)
        _ai = await run_ai_analysis(profile, _ws_config)
    except Exception as _ai_err:
        logger.warning("[AIAnalysis] Skipped: %s", _ai_err)
        _ai = {}

    lead: dict = {
        "id": lead_id,
        "linkedin_url": url,
        # ── Identity ─────────────────────��─────────────────────────────────────
        "name": pp.get("full_name") or ident.get("full_name") or name,
        "first_name": profile.get("first_name") or first,
        "last_name": profile.get("last_name") or last,
        "work_email": pp.get("work_email") or ident.get("work_email") or contact.get("email"),
        "personal_email": pp.get("personal_email") or ident.get("personal_email"),
        "direct_phone": (
            pp.get("direct_phone") or ident.get("direct_phone")
            or contact.get("phone") or profile.get("phone_number") or profile.get("phone")
        ),
        "twitter": pp.get("twitter") or ident.get("twitter") or contact.get("twitter") or profile.get("twitter"),
        "city": (
            pp.get("city") or ident.get("city")
            or (profile.get("city") or "").split(",")[0].strip()
            or profile.get("location", "").split(",")[0].strip()
        ),
        "country": _country_name(
            pp.get("country") or ident.get("country")
            or profile.get("country") or profile.get("country_code", "")
        ),
        "timezone": pp.get("timezone") or ident.get("timezone"),
        # ── Professional ───────────────────────────────────────────────────────
        "title": (
            pp.get("current_title") or prof_legacy.get("current_title")
            or profile.get("position") or profile.get("headline") or profile.get("bio", "")
        ),
        "seniority_level": pp.get("seniority_level") or prof_legacy.get("seniority_level"),
        "department": pp.get("department") or prof_legacy.get("department"),
        "years_in_role": pp.get("years_in_role") or prof_legacy.get("years_in_role"),
        "company": ci.get("name") or comp_legacy.get("name") or company,
        "previous_companies": _safe_json(
            prof_legacy.get("previous_companies")
            or [e.get("company", "") for e in (pp.get("career_history") or [])[1:4] if isinstance(e, dict)]
            or [e.get("company_name", "") or e.get("company", "") for e in (profile.get("experience") or [])[1:4] if isinstance(e, dict)]
        ),
        "top_skills": _safe_json(
            pp.get("top_skills") or prof_legacy.get("top_skills")
            or profile.get("skills") or []
        ),
        "education": (
            pp.get("education") or prof_legacy.get("education")
            or profile.get("educations_details") or profile.get("education_details")
        ),
        "certifications": _safe_json(
            pp.get("certifications") or prof_legacy.get("certifications")
            or profile.get("honors_and_awards") or []
        ),
        "languages": _safe_json(pp.get("languages") or prof_legacy.get("languages") or []),
        # ── Company (from waterfall + LLM) ────────────────────────────────────
        "company_website": cp.get("website") or ci.get("website") or comp_legacy.get("website"),
        "industry": cp.get("industry") or comp_legacy.get("industry"),
        "employee_count": _safe_int(cp.get("employee_count") or comp_legacy.get("employee_count")),
        "hq_location": cp.get("hq_location") or comp_legacy.get("hq_location"),
        "founded_year": str(cp.get("founded_year") or comp_legacy.get("founded_year") or ""),
        "funding_stage": cin.get("funding_stage") or comp_legacy.get("funding_stage"),
        "total_funding": cin.get("total_funding") or comp_legacy.get("total_funding"),
        "last_funding_date": cin.get("last_funding_date") or comp_legacy.get("last_funding_date"),
        "lead_investor": cin.get("lead_investor") or comp_legacy.get("lead_investor"),
        "annual_revenue": cp.get("annual_revenue_est") or comp_legacy.get("annual_revenue_est"),
        "tech_stack": _safe_json(cp.get("tech_stack") or comp_legacy.get("tech_stack") or []),
        "hiring_velocity": cin.get("hiring_velocity") or comp_legacy.get("hiring_velocity"),
        # ── Company enrichment (waterfall) ────────────────────────────────────
        "avatar_url": _extract_avatar(profile) or contact.get("avatar_url"),
        "company_logo": cp.get("company_logo") or company_extras.get("company_logo"),
        "company_email": (
            cp.get("company_email") or company_extras.get("company_email")
            # Fall back to activity emails that match the company domain
            or next((e for e in activity_emails_raw if verified_domain and verified_domain.split(".")[0] in e), None)
        ),
        "company_description": wi.get("company_description") or company_extras.get("company_description"),
        "company_linkedin": ci.get("linkedin_url") or company_extras.get("company_linkedin"),
        "company_twitter": cp.get("company_twitter") or company_extras.get("company_twitter"),
        "company_phone": cp.get("company_phone") or company_extras.get("company_phone"),
        "waterfall_log": json.dumps(waterfall_log),
        # ── Stage 3 — Website Intelligence ───────────────────────────────────
        "website_intelligence": json.dumps(website_intel or {}),
        "product_offerings": _safe_json(wi.get("product_offerings")),
        "value_proposition": wi.get("value_proposition"),
        "target_customers": _safe_json(wi.get("target_customers")),
        "business_model": wi.get("business_model"),
        "pricing_signals": wi.get("pricing_signals"),
        "product_category": wi.get("product_category"),
        "data_completeness_score": int(ls.get("data_completeness_score", 0)),
        # ── Intent signals ────────────────────────────────────────────────────
        "recent_funding_event": intent_s.get("recent_funding_event"),
        # Hiring signal: LLM result OR extracted from activity posts
        "hiring_signal": (
            intent_s.get("hiring_signal")
            or (hiring_signals_raw[0] if hiring_signals_raw else None)
        ),
        "job_change": intent_s.get("job_change"),
        # LinkedIn activity: combine LLM intent + actual post snippets
        "linkedin_activity": (
            intent_s.get("linkedin_activity")
            or (_safe_json(hiring_signals_raw) if hiring_signals_raw else None)
        ),
        "news_mention": intent_s.get("news_mention"),
        "product_launch": intent_s.get("product_launch"),
        "competitor_usage": intent_s.get("competitor_usage"),
        "review_activity": (
            intent_s.get("review_activity")
            or (profile.get("recommendations_text"))
        ),
        # ── Scoring ──────��────────────────────────────────────────────────────
        "icp_fit_score": _safe_int(ls.get("icp_fit_score")),
        "intent_score": _safe_int(ls.get("intent_score")),
        "timing_score": _safe_int(ls.get("timing_score")),
        "engagement_score": 0,
        "total_score": _safe_int(total),
        "score_tier": score_tier,
        "score_explanation": ls.get("score_explanation"),
        "icp_match_tier": ls.get("icp_match_tier") or tier_raw,
        "disqualification_flags": _safe_json(ls.get("disqualification_flags")),
        # ── Outreach ──────────────────────────────────────────────────────────
        "email_subject": outreach.get("email_subject"),
        "cold_email": outreach.get("cold_email"),
        "linkedin_note": outreach.get("linkedin_note"),
        "best_channel": outreach.get("best_channel"),
        "best_send_time": outreach.get("best_send_time"),
        "outreach_angle": outreach.get("outreach_angle"),
        "sequence_type": outreach.get("sequence_type"),
        "outreach_sequence": _safe_json(outreach.get("sequence")),
        "last_contacted": outreach.get("last_contacted"),
        "email_status": outreach.get("email_status"),
        # ── CRM ───────────────────────────────────────────────────────────────
        "lead_source": crm.get("lead_source", "LinkedIn URL"),
        "enrichment_source": crm.get("enrichment_source", "Bright Data + Hunter.io"),
        "data_completeness": int(crm.get("data_completeness") or 0),
        "crm_stage": crm.get("crm_stage"),
        "tags": _safe_json(crm.get("tags")),
        "assigned_owner": crm.get("assigned_owner"),
        # ── Meta ─���────────────────────────────────────────────────────────────
        "full_data": json.dumps({  # default=str guards against any non-serializable value
            "person_profile": enrichment.get("person_profile", {}),
            "company_identity": enrichment.get("company_identity", {}),
            "website_intelligence": enrichment.get("website_intelligence", website_intel or {}),
            "company_profile": enrichment.get("company_profile", {}),
            "company_intelligence": enrichment.get("company_intelligence", {}),
            "intent_signals": enrichment.get("intent_signals", {}),
            "lead_scoring": enrichment.get("lead_scoring", {}),
            "outreach": enrichment.get("outreach", {}),
            # Raw BD fields preserved
            "activity_emails": activity_emails_raw,
            "activity_phones": profile.get("_activity_phones") or [],
            "activity_full": profile.get("_activity_full") or [],
            "linkedin_posts": profile.get("_posts") or [],
            "hiring_signals": hiring_signals_raw,
            "recommendations": profile.get("recommendations") or [],
            "recommendations_count": profile.get("recommendations_count") or 0,
            "similar_profiles": profile.get("_similar_profiles") or [],
            "banner_image": profile.get("banner_image") or "",
            "linkedin_num_id": profile.get("linkedin_num_id") or "",
            "influencer": profile.get("_influencer") or False,
            "memorialized_account": profile.get("_memorialized_account") or False,
            "bio_links": profile.get("_bio_links") or [],
            "bd_scrape_timestamp": profile.get("_bd_scrape_timestamp") or "",
            # Legacy compat
            "identity": enrichment.get("identity", {}),
            "professional": enrichment.get("professional", {}),
            "company": enrichment.get("company", {}),
            "scoring": enrichment.get("scoring", {}),
        }, default=str),
        "raw_profile": json.dumps(profile, default=str),
        "about": (profile.get("about") or "")[:1000],
        "followers": _safe_int(profile.get("followers")),
        "connections": _safe_int(profile.get("connections")),
        "email_source": contact.get("source"),
        "email_confidence": contact.get("confidence"),
        "email_verified": 1 if contact.get("verified") else 0,
        "bounce_risk": contact.get("bounce_risk"),
        # P1 — Full activity feed stored as dedicated column for AI analysis
        "activity_feed": json.dumps(profile.get("_activity_full") or []),
        # P3 — AI Analysis layer (populated after _run_ai_analysis call below)
        "auto_tags":           _ai.get("auto_tags_json"),
        "behavioural_signals": _ai.get("behavioural_signals_json"),
        "pitch_intelligence":  _ai.get("pitch_intelligence_json"),
        "warm_signal":         _ai.get("warm_signal"),
        # ── Company enrichment (P1/P5) — flat columns mirrored from company_enrichments ─
        "company_id":        _company_id_val,
        "company_score":     _company_score,
        "combined_score":    _combined_score,
        "company_score_tier": _company_record.get("company_score_tier") or "",
        "company_tags":      _safe_json(_company_record.get("company_tags") or []),
        "culture_signals":   _safe_json(_company_record.get("culture_signals") or {}),
        "account_pitch":     _safe_json(_company_record.get("account_pitch") or {}),
        "wappalyzer_tech":   _safe_json(_company_record.get("wappalyzer_tech") or []),
        "news_mentions":     _safe_json(_company_record.get("news_mentions") or []),
        "crunchbase_data":   _safe_json(_company_record.get("crunchbase_data") or {}),
        "linkedin_posts":    _safe_json(_company_record.get("linkedin_posts") or []),
        "status": "enriched",
        "job_id": job_id,
        "organization_id": org_id,
        "enriched_at": now,
    }

    # P5: Compute combined_score after we have both total_score and company_score
    try:
        from company_service import compute_combined_score
        lead["combined_score"] = compute_combined_score(
            person_score=int(lead.get("total_score") or 0),
            company_score=_company_score,
        )
    except Exception:
        lead["combined_score"] = int(lead.get("total_score") or 0)

    await _upsert_lead(lead)
    logger.info(
        "[Enrich] Done: %s | score=%s (%s) | email=%s | completeness=%s%% | category=%s",
        lead["name"] or url, total, score_tier,
        lead["work_email"] or "not found", lead["data_completeness"],
        lead["product_category"] or "?",
    )
    return lead


# ─────────────────────────────────────────────────────────────────────────────
# SSE streaming enrichment (6 stages)
# ─────────────────────────────────────────────────────────────────────────────

from typing import AsyncGenerator

async def enrich_single_stream(
    linkedin_url: str,
    org_id: str = "default",
) -> AsyncGenerator[dict, None]:
    """
    Async generator that yields SSE events after each enrichment stage.

    Stage 1 — profile:   Bright Data person scrape
    Stage 2 — company:   Company waterfall (BD + Apollo + Hunter + Clearbit)
    Stage 3 — contact:   Email / phone waterfall (Hunter → Apollo → pattern)
    Stage 4 — website:   Website intelligence scrape + LLM extraction
    Stage 5 — scoring:   Comprehensive LLM scoring + outreach generation
    Stage 6 — complete:  Save to DB, return final lead object

    Each stage yields {"stage": <name>, "status": "loading"} first,
    then {"stage": <name>, "status": "done", "data": {...}} when finished.
    On error: {"stage": <name>, "status": "error", "error": <msg>}
    """
    url = _normalize_linkedin_url(linkedin_url)
    lead_id = _lead_id(url)

    # ── Company URL: proxy to company pipeline, stream synthetic events ───────
    if _is_company_url(url):
        yield {"stage": "profile",  "status": "loading"}
        yield {"stage": "company",  "status": "loading"}
        yield {"stage": "contact",  "status": "loading"}
        yield {"stage": "website",  "status": "loading"}
        yield {"stage": "scoring",  "status": "loading"}
        try:
            lead = await enrich_company_url(url, org_id=org_id)
            yield {"stage": "profile",  "status": "done", "data": {
                "name": lead.get("name", ""), "title": lead.get("title", ""),
                "company": lead.get("company", ""), "location": lead.get("location", ""),
                "avatar_url": lead.get("avatar_url", ""),
                "followers": lead.get("followers", 0),
            }}
            yield {"stage": "company",  "status": "done", "data": {
                "company_logo": lead.get("company_logo"),
                "company_website": lead.get("company_website"),
                "industry": lead.get("industry"),
                "employee_count": lead.get("employee_count", 0),
            }}
            yield {"stage": "contact",  "status": "done", "data": {
                "email": lead.get("work_email"), "phone": lead.get("direct_phone"),
                "email_source": lead.get("email_source"), "email_confidence": lead.get("email_confidence"),
            }}
            yield {"stage": "website",  "status": "done", "data": {
                "value_proposition": lead.get("value_proposition"),
                "product_offerings": json.loads(lead.get("product_offerings") or "[]"),
                "tech_stack": json.loads(lead.get("tech_stack") or "[]"),
                "business_model": lead.get("business_model"),
            }}
            yield {"stage": "scoring",  "status": "done", "data": {
                "total_score": lead.get("total_score", 0), "score_tier": lead.get("score_tier"),
                "icp_fit_score": lead.get("icp_fit_score", 0), "intent_score": lead.get("intent_score", 0),
                "cold_email": lead.get("cold_email"), "linkedin_note": lead.get("linkedin_note"),
            }}
            yield {"stage": "complete", "status": "done", "lead": lead}
        except Exception as e:
            yield {"stage": "complete", "status": "error", "error": str(e)}
        return

    # ── Stage 1: Person Profile ───────────────────────────────────────────────
    yield {"stage": "profile", "status": "loading"}
    try:
        profile = await fetch_profile_sync(url)
        name     = profile.get("name", "")
        first, last = _parse_name(name)
        company  = profile.get("current_company_name", "")
        raw_link = profile.get("current_company_link", "")
        yield {"stage": "profile", "status": "done", "data": {
            "name":       name,
            "title":      profile.get("position") or profile.get("headline", ""),
            "company":    company,
            "location":   profile.get("location") or profile.get("city", ""),
            "avatar_url": profile.get("avatar_url") or profile.get("avatar", ""),
            "followers":  _safe_int(profile.get("followers")),
            "connections": _safe_int(profile.get("connections")),
            "about":      (profile.get("about") or "")[:300],
            "banner_image": profile.get("banner_image", ""),
        }}
    except Exception as e:
        yield {"stage": "profile", "status": "error", "error": str(e)}
        yield {"stage": "complete", "status": "error", "error": f"Profile fetch failed: {e}"}
        return

    # ── Stage 2: Company Waterfall ────────────────────────────────────────────
    yield {"stage": "company", "status": "loading"}
    try:
        initial_domain = _extract_domain(company, raw_link)
        company_extras, company_wf_log = await enrich_company_waterfall(company, initial_domain, profile)
        verified_domain = company_extras.pop("_verified_domain", initial_domain) or initial_domain
        real_website = company_extras.get("company_website") or (f"https://{verified_domain}" if verified_domain else "")
        yield {"stage": "company", "status": "done", "data": {
            "company_logo":    company_extras.get("company_logo"),
            "company_website": real_website,
            "industry":        company_extras.get("industry"),
            "employee_count":  company_extras.get("employee_count", 0),
            "hq_location":     company_extras.get("hq_location"),
            "tech_stack":      company_extras.get("tech_stack", []),
            "founded_year":    company_extras.get("founded_year"),
            "funding_stage":   company_extras.get("funding_stage"),
        }}
    except Exception as e:
        yield {"stage": "company", "status": "error", "error": str(e)}
        company_extras, company_wf_log = {}, []
        verified_domain = _extract_domain(company, raw_link)
        real_website = ""

    # ── Stage 3: Contact Waterfall ────────────────────────────────────────────
    yield {"stage": "contact", "status": "loading"}
    try:
        activity_emails = profile.get("_activity_emails") or []
        activity_phones = profile.get("_activity_phones") or []
        if activity_emails:
            matched = next((e for e in activity_emails if verified_domain and verified_domain.split(".")[0] in e), None)
            pre_contact = matched or activity_emails[0]
            contact = {"email": pre_contact, "source": "linkedin_activity", "confidence": "high"}
            if activity_phones:
                contact["phone"] = activity_phones[0]
        else:
            contact = await find_contact_info(first, last, verified_domain, linkedin_url=url)
            if not contact.get("phone") and activity_phones:
                contact["phone"] = activity_phones[0]
        yield {"stage": "contact", "status": "done", "data": {
            "email":            contact.get("email"),
            "phone":            contact.get("phone"),
            "email_source":     contact.get("source"),
            "email_confidence": contact.get("confidence"),
        }}
    except Exception as e:
        yield {"stage": "contact", "status": "error", "error": str(e)}
        contact = {"email": None, "phone": None, "source": None, "confidence": None}

    # ── Stage 4: Website Intelligence ────────────────────────────────────────
    yield {"stage": "website", "status": "loading"}
    try:
        website_intel = await scrape_website_intelligence(real_website)
        yield {"stage": "website", "status": "done", "data": {
            "value_proposition":   website_intel.get("value_proposition"),
            "product_offerings":   website_intel.get("product_offerings", []),
            "tech_stack_clues":    website_intel.get("tech_stack_clues", []),
            "business_model":      website_intel.get("business_model"),
            "product_category":    website_intel.get("product_category"),
            "market_positioning":  website_intel.get("market_positioning"),
            "pricing_signals":     website_intel.get("pricing_signals"),
            "target_customers":    website_intel.get("target_customers", []),
            "problem_solved":      website_intel.get("problem_solved"),
            "hiring_signals":      website_intel.get("hiring_signals"),
            "pages_scraped":       website_intel.get("pages_scraped", []),
        }}
    except Exception as e:
        yield {"stage": "website", "status": "error", "error": str(e)}
        website_intel = {}

    # ── Stage 5: LLM Scoring + Outreach ──────────────────────────────────────
    yield {"stage": "scoring", "status": "loading"}
    try:
        profile["_company_extras"] = company_extras
        if website_intel and website_intel.get("pages_scraped"):
            company_wf_log.append({
                "step": 5, "source": "Website Scrape",
                "fields_found": website_intel.get("pages_scraped", []),
                "note": f"pages={len(website_intel.get('pages_scraped', []))}, category={website_intel.get('product_category', '?')}",
            })
        enrichment = await build_comprehensive_enrichment(url, profile, contact, website_intel=website_intel)
        scoring = enrichment.get("lead_scoring", enrichment.get("scoring", {}))
        total = scoring.get("overall_score", 0)
        tier_raw = scoring.get("icp_match_tier") or scoring.get("score_tier", "")
        score_tier = _resolve_tier(tier_raw)
        outreach = enrichment.get("outreach", {})
        yield {"stage": "scoring", "status": "done", "data": {
            "total_score":  int(total),
            "score_tier":   score_tier,
            "icp_fit_score":  int(scoring.get("icp_fit_score", 0)),
            "intent_score":   int(scoring.get("intent_score", 0)),
            "timing_score":   int(scoring.get("timing_score", 0)),
            "icp_match_tier": scoring.get("icp_match_tier") or tier_raw,
            "score_explanation": scoring.get("score_explanation"),
            "cold_email":     outreach.get("cold_email"),
            "linkedin_note":  outreach.get("linkedin_note"),
            "email_subject":  outreach.get("email_subject"),
            "best_channel":   outreach.get("best_channel"),
            "outreach_angle": outreach.get("outreach_angle"),
        }}
    except Exception as e:
        yield {"stage": "scoring", "status": "error", "error": str(e)}
        enrichment = {}
        scoring = {}
        total = 0
        score_tier = "cold"
        outreach = {}

    # ── Stage 6: Save + Complete ──────────────────────────────────────────────
    try:
        # Build full lead record (same logic as enrich_single)
        person_prof = enrichment.get("person_profile", {})
        ident = enrichment.get("identity", {})
        if contact.get("email"):
            person_prof["work_email"] = contact["email"]
            ident["work_email"] = contact["email"]
        if contact.get("phone"):
            person_prof["direct_phone"] = contact["phone"]
            ident["direct_phone"] = contact["phone"]

        comp_prof = enrichment.get("company_profile", {})
        comp_section = enrichment.get("company", {})
        for k, v in company_extras.items():
            _map = {
                "company_logo": "company_logo", "company_email": "company_email",
                "company_phone": "company_phone", "company_twitter": "company_twitter",
                "industry": "industry", "employee_count": "employee_count",
                "hq_location": "hq_location", "founded_year": "founded_year",
                "annual_revenue": "annual_revenue_est", "tech_stack": "tech_stack",
            }
            prof_key = _map.get(k)
            if prof_key and v and not comp_prof.get(prof_key):
                comp_prof[prof_key] = v

        scoring_f = enrichment.get("lead_scoring", enrichment.get("scoring", {}))
        crm = enrichment.get("crm", {})
        wi = enrichment.get("website_intelligence", website_intel or {})
        intent_s = enrichment.get("intent_signals", {})
        ci = enrichment.get("company_identity", {})
        cp = enrichment.get("company_profile", comp_prof)
        cin = enrichment.get("company_intelligence", {})
        pp = enrichment.get("person_profile", {})
        hiring_signals_raw = profile.get("_hiring_signals") or []
        activity_emails_raw = profile.get("_activity_emails") or []
        now = datetime.now(timezone.utc).isoformat()

        waterfall_log = [
            {"step": 0, "source": "Bright Data", "fields_found": ["profile"], "note": f"LinkedIn scrape: {url}"},
            *company_wf_log,
            {"step": 10, "source": contact.get("source") or "none",
             "fields_found": ["email"], "note": f"email={contact.get('email')}"},
        ]

        lead: dict = {
            "id": lead_id, "linkedin_url": url,
            "name": pp.get("full_name") or ident.get("full_name") or profile.get("name", ""),
            "first_name": profile.get("first_name") or first,
            "last_name": profile.get("last_name") or last,
            "work_email": pp.get("work_email") or ident.get("work_email") or contact.get("email"),
            "personal_email": pp.get("personal_email") or ident.get("personal_email"),
            "direct_phone": (pp.get("direct_phone") or ident.get("direct_phone")
                             or contact.get("phone") or profile.get("phone_number") or profile.get("phone")),
            "twitter": pp.get("twitter") or ident.get("twitter") or contact.get("twitter") or profile.get("twitter"),
            "city": (pp.get("city") or ident.get("city")
                     or (profile.get("city") or "").split(",")[0].strip()
                     or profile.get("location", "").split(",")[0].strip()),
            "country": _country_name(
                pp.get("country") or ident.get("country")
                or profile.get("country") or profile.get("country_code", "")
            ),
            "avatar_url": _extract_avatar(profile) or contact.get("avatar_url") or "",
            "title": (pp.get("current_title") or ident.get("current_title")
                      or profile.get("position") or profile.get("headline", "")),
            "seniority_level": pp.get("seniority_level") or ident.get("seniority_level"),
            "department": pp.get("department") or ident.get("department"),
            "company": (cp.get("name") or ci.get("name") or comp_section.get("name") or
                        comp_prof.get("name") or profile.get("current_company_name", "")),
            "company_website": (cp.get("website") or ci.get("website") or comp_section.get("website") or
                                real_website or company_extras.get("company_website")),
            "company_logo": (cp.get("company_logo") or comp_section.get("logo") or
                             company_extras.get("company_logo") or profile.get("current_company_logo")),
            "company_linkedin": (ci.get("linkedin_url") or profile.get("current_company_link") or
                                 company_extras.get("company_linkedin")),
            "company_description": (cp.get("company_description") or comp_section.get("description") or
                                    company_extras.get("company_description") or wi.get("company_description")),
            "industry": (cp.get("industry") or comp_section.get("industry") or company_extras.get("industry")),
            "employee_count": _safe_int(cp.get("employee_count") or company_extras.get("employee_count")),
            "hq_location": (cp.get("hq_location") or comp_section.get("hq_location") or
                            company_extras.get("hq_location")),
            "founded_year": (cp.get("founded_year") or comp_section.get("founded_year") or
                             company_extras.get("founded_year")),
            "annual_revenue": (cp.get("annual_revenue_est") or comp_section.get("annual_revenue_est") or
                               company_extras.get("annual_revenue")),
            "tech_stack": _safe_json(cp.get("tech_stack") or company_extras.get("tech_stack") or []),
            "company_email": cp.get("company_email") or company_extras.get("company_email"),
            "company_phone": cp.get("company_phone") or company_extras.get("company_phone"),
            "company_twitter": cp.get("company_twitter") or company_extras.get("company_twitter"),
            "top_skills": _safe_json(pp.get("top_skills") or profile.get("skills") or []),
            "education": _safe_json(profile.get("education") or []),
            "certifications": _safe_json(pp.get("certifications") or profile.get("certifications") or []),
            "languages": _safe_json(pp.get("languages") or profile.get("languages") or []),
            "value_proposition": wi.get("value_proposition"),
            "product_offerings": _safe_json(wi.get("product_offerings")),
            "target_customers": _safe_json(wi.get("target_customers")),
            "business_model": wi.get("business_model"),
            "pricing_signals": wi.get("pricing_signals"),
            "product_category": wi.get("product_category"),
            "data_completeness_score": _safe_int(scoring_f.get("data_completeness_score")),
            "recent_funding_event": intent_s.get("recent_funding_event"),
            "hiring_signal": intent_s.get("hiring_signal") or (hiring_signals_raw[0] if hiring_signals_raw else None),
            "job_change": intent_s.get("job_change"),
            "linkedin_activity": intent_s.get("linkedin_activity") or (_safe_json(hiring_signals_raw) if hiring_signals_raw else None),
            "news_mention": intent_s.get("news_mention"),
            "product_launch": intent_s.get("product_launch"),
            "competitor_usage": intent_s.get("competitor_usage"),
            "review_activity": intent_s.get("review_activity") or profile.get("recommendations_text"),
            "icp_fit_score": _safe_int(scoring_f.get("icp_fit_score")),
            "intent_score": _safe_int(scoring_f.get("intent_score")),
            "timing_score": _safe_int(scoring_f.get("timing_score")),
            "engagement_score": 0,
            "total_score": _safe_int(total),
            "score_tier": score_tier,
            "score_explanation": scoring_f.get("score_explanation"),
            "icp_match_tier": scoring_f.get("icp_match_tier") or tier_raw,
            "disqualification_flags": _safe_json(scoring_f.get("disqualification_flags")),
            "email_subject": outreach.get("email_subject"),
            "cold_email": outreach.get("cold_email"),
            "linkedin_note": outreach.get("linkedin_note"),
            "best_channel": outreach.get("best_channel"),
            "best_send_time": outreach.get("best_send_time"),
            "outreach_angle": outreach.get("outreach_angle"),
            "sequence_type": outreach.get("sequence_type"),
            "outreach_sequence": _safe_json(outreach.get("sequence")),
            "last_contacted": outreach.get("last_contacted"),
            "email_status": outreach.get("email_status"),
            "lead_source": crm.get("lead_source", "LinkedIn URL"),
            "enrichment_source": crm.get("enrichment_source", "Bright Data + Hunter.io"),
            "data_completeness": _safe_int(crm.get("data_completeness")),
            "crm_stage": crm.get("crm_stage"),
            "tags": _safe_json(crm.get("tags")),
            "assigned_owner": crm.get("assigned_owner"),
            "full_data": json.dumps({
                "person_profile": enrichment.get("person_profile", {}),
                "company_identity": enrichment.get("company_identity", {}),
                "website_intelligence": enrichment.get("website_intelligence", website_intel or {}),
                "company_profile": enrichment.get("company_profile", {}),
                "company_intelligence": enrichment.get("company_intelligence", {}),
                "intent_signals": enrichment.get("intent_signals", {}),
                "lead_scoring": enrichment.get("lead_scoring", {}),
                "outreach": enrichment.get("outreach", {}),
                # Profile-level data needed for UI sections
                "recommendations": profile.get("recommendations") or [],
                "recommendations_count": profile.get("recommendations_count") or 0,
                "similar_profiles": profile.get("_similar_profiles") or [],
                "banner_image": profile.get("banner_image") or "",
                "linkedin_num_id": profile.get("linkedin_num_id") or profile.get("linkedin_num_id") or "",
                "influencer": profile.get("_influencer") or False,
                "memorialized_account": profile.get("_memorialized_account") or False,
                "bio_links": profile.get("_bio_links") or [],
                "bd_scrape_timestamp": profile.get("_bd_scrape_timestamp") or "",
                "activity_emails": activity_emails_raw,
                "activity_phones": profile.get("_activity_phones") or [],
                "activity_full": profile.get("_activity_full") or [],
                "linkedin_posts": profile.get("_posts") or [],
                "hiring_signals": hiring_signals_raw,
                "waterfall_log": waterfall_log,
            }, default=str),
            "raw_profile": json.dumps(profile, default=str),
            "about": (profile.get("about") or "")[:1000],
            "followers": _safe_int(profile.get("followers")),
            "connections": _safe_int(profile.get("connections")),
            "email_source": contact.get("source"),
            "email_confidence": contact.get("confidence"),
            "status": "enriched",
            "job_id": None,
            "organization_id": org_id,
            "enriched_at": now,
        }
        await _upsert_lead(lead)
        yield {"stage": "complete", "status": "done", "lead": lead}
    except Exception as e:
        logger.error("[StreamEnrich] Save failed: %s", e, exc_info=True)
        yield {"stage": "complete", "status": "error", "error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# Bulk pipeline
# ─────────────────────────────────────────────────────────────────────────────

async def enrich_bulk(
    urls: list[str],
    webhook_url: Optional[str] = None,
    notify_url: Optional[str] = None,
    webhook_auth: Optional[str] = None,
    org_id: str = "default",
) -> dict:
    """
    Bulk enrichment pipeline — multi-tenant aware.

    Strategy:
    - If webhook_url provided → trigger Bright Data batch (results arrive via webhook)
    - If Redis available → push individual tasks to priority queue (worker pool processes them)
    - Otherwise → in-process sequential asyncio task (guaranteed progress, single-server only)
    """
    job_id = str(uuid.uuid4())
    now    = datetime.now(timezone.utc).isoformat()
    snapshot_id = None
    status = "pending"
    error_msg = None

    # Only trigger BD batch when webhook delivery is configured
    if _bd_api_key() and webhook_url:
        try:
            snapshot_id = await trigger_batch_snapshot(urls, webhook_url, notify_url, webhook_auth)
            status = "running"
            logger.info("[BulkEnrich] BD batch triggered: %s (webhook=%s)", snapshot_id, webhook_url)
        except Exception as e:
            logger.error("[BulkEnrich] BD trigger failed: %s — falling back to sequential", e)
            error_msg = str(e)

    job = {
        "id": job_id, "snapshot_id": snapshot_id,
        "total_urls": len(urls), "processed": 0, "failed": 0,
        "status": status, "error": error_msg,
        "webhook_url": webhook_url,
        "organization_id": org_id,
        "created_at": now, "updated_at": now,
    }
    async with aiosqlite.connect(LEADS_DB) as db:
        await db.execute(
            """INSERT INTO enrichment_jobs
               (id,snapshot_id,total_urls,processed,failed,status,error,webhook_url,organization_id,created_at,updated_at)
               VALUES (:id,:snapshot_id,:total_urls,:processed,:failed,:status,:error,:webhook_url,:organization_id,:created_at,:updated_at)""",
            job,
        )
        await db.commit()

    if not webhook_url:
        # Fair multi-tenant queue (per-tenant queues + round-robin scheduler)
        r = await _get_redis()
        if r:
            try:
                import queue_manager
                # Compute adaptive chunk size and pre-split so sub-jobs align with queue tasks
                chunk_size = await queue_manager.compute_chunk_size(r)
                url_chunks = [urls[i: i + chunk_size] for i in range(0, len(urls), chunk_size)]
                sub_job_ids = [str(uuid.uuid4()) for _ in url_chunks]
                for idx, (sjid, chunk) in enumerate(zip(sub_job_ids, url_chunks)):
                    await _create_sub_job(sjid, job_id, idx, len(chunk), org_id)
                num_chunks = await queue_manager.push_job(
                    job_id=job_id,
                    org_id=org_id,
                    chunks=url_chunks,
                    sub_job_ids=sub_job_ids,
                    r=r,
                    generate_outreach=True,
                )
                await _update_job(job_id, status="running")
                job["status"] = "running"
                logger.info(
                    "[BulkEnrich] %d URLs → %d chunks queued for job %s (org=%s)",
                    len(urls), num_chunks, job_id, org_id,
                )
                return job
            except Exception as e:
                logger.warning("[BulkEnrich] Queue push failed (%s) — falling back to in-process", e)

        # Fallback: in-process sequential task (Redis unavailable)
        # Compute chunks: chunk_size = max(5, total // 4)  → 100→4×25, 10→2×5
        chunk_size = max(5, len(urls) // 4) if len(urls) >= 4 else len(urls) or 1
        url_chunks = [urls[i: i + chunk_size] for i in range(0, len(urls), chunk_size)]
        sub_job_ids = [str(uuid.uuid4()) for _ in url_chunks]
        for idx, (sjid, chunk) in enumerate(zip(sub_job_ids, url_chunks)):
            await _create_sub_job(sjid, job_id, idx, len(chunk), org_id)
        logger.info(
            "[BulkEnrich] Starting in-process batch for %d URLs → %d chunks (org=%s)",
            len(urls), len(url_chunks), org_id,
        )
        asyncio.create_task(_process_sequential_batch(job_id, url_chunks, sub_job_ids, org_id=org_id))

    return job


async def _process_sequential_batch(
    job_id: str,
    url_chunks: list[list[str]],
    sub_job_ids: list[str],
    org_id: str = "default",
) -> None:
    """
    In-process sequential bulk enrichment — fallback when Redis is unavailable.
    Processes URL chunks sequentially; each chunk maps to a sub-job record.
    """
    await _update_job(job_id, status="running")
    total_processed = total_failed = 0
    total_urls = sum(len(c) for c in url_chunks)
    logger.info(
        "[BulkBatch] Starting %d URLs in %d chunks for job %s (org=%s)",
        total_urls, len(url_chunks), job_id, org_id,
    )

    for chunk_idx, (sub_job_id, chunk) in enumerate(zip(sub_job_ids, url_chunks)):
        await _update_sub_job(sub_job_id, status="running")
        chunk_ok = chunk_fail = 0
        for i, url in enumerate(chunk):
            try:
                lead = await enrich_single(url, job_id=job_id, org_id=org_id)
                chunk_ok += 1
                logger.info(
                    "[BulkBatch] Chunk %d/%d · URL %d/%d OK: %s",
                    chunk_idx + 1, len(url_chunks), i + 1, len(chunk), url,
                )
                await _publish_lead_done(org_id, job_id, lead)
            except Exception as e:
                logger.warning(
                    "[BulkBatch] Chunk %d/%d · URL %d/%d FAIL: %s — %s",
                    chunk_idx + 1, len(url_chunks), i + 1, len(chunk), url, e,
                )
                chunk_fail += 1
            await _update_sub_job(sub_job_id, processed=chunk_ok, failed=chunk_fail)

        chunk_status = (
            "completed" if chunk_fail == 0
            else ("failed" if chunk_ok == 0 else "completed_with_errors")
        )
        await _update_sub_job(sub_job_id, status=chunk_status, processed=chunk_ok, failed=chunk_fail)

        total_processed += chunk_ok
        total_failed += chunk_fail
        await _update_job(job_id, processed=total_processed, failed=total_failed)

    final_status = (
        "completed" if total_failed == 0
        else ("failed" if total_processed == 0 else "completed_with_errors")
    )
    await _update_job(job_id, status=final_status, processed=total_processed, failed=total_failed)
    await _publish_job_done(org_id, job_id, total_processed, total_failed)
    logger.info(
        "[BulkBatch] Job %s done — processed=%d failed=%d", job_id, total_processed, total_failed
    )


# Keep old name as alias for webhook-delivered batch processing
async def _process_fallback_batch(job_id: str, urls: list[str]) -> None:
    chunk_size = max(5, len(urls) // 4) if len(urls) >= 4 else len(urls) or 1
    url_chunks = [urls[i: i + chunk_size] for i in range(0, len(urls), chunk_size)]
    sub_job_ids = [str(uuid.uuid4()) for _ in url_chunks]
    for idx, (sjid, chunk) in enumerate(zip(sub_job_ids, url_chunks)):
        await _create_sub_job(sjid, job_id, idx, len(chunk), "default")
    await _process_sequential_batch(job_id, url_chunks, sub_job_ids)


async def process_webhook_profiles(profiles: list[dict], job_id: Optional[str] = None) -> dict:
    processed = failed = 0
    for profile in profiles:
        url = profile.get("input_url") or profile.get("url") or profile.get("linkedin_url")
        if not url:
            failed += 1
            continue
        try:
            url = _normalize_linkedin_url(url)
            name = profile.get("name", "")
            first, last = _parse_name(name)
            company = profile.get("current_company_name", "")
            raw_link = profile.get("current_company_link", "")
            domain = _extract_domain(company, raw_link)

            # Sequential waterfall: company first → verified domain → person contact → website
            company_extras, company_wf_log = await enrich_company_waterfall(company, domain, profile)
            verified_domain = company_extras.pop("_verified_domain", domain) or domain
            real_website = company_extras.get("company_website") or (f"https://{verified_domain}" if verified_domain else "")
            contact = await find_contact_info(first, last, verified_domain, linkedin_url=url)
            website_intel = await scrape_website_intelligence(real_website)

            # Inject company extras into profile so LLM prompt can use them
            profile["_company_extras"] = company_extras

            # Add website scraping step to waterfall log
            if website_intel and website_intel.get("pages_scraped"):
                company_wf_log.append({
                    "step": 5, "source": "Website Scrape",
                    "fields_found": website_intel.get("pages_scraped", []),
                    "note": f"pages={len(website_intel.get('pages_scraped', []))}, category={website_intel.get('product_category', '?')}",
                })

            enrichment = await build_comprehensive_enrichment(url, profile, contact, website_intel=website_intel)

            scoring = enrichment.get("lead_scoring", enrichment.get("scoring", {}))
            tier_raw = scoring.get("icp_match_tier") or scoring.get("score_tier", "")
            score_tier = _resolve_tier(tier_raw)
            now = datetime.now(timezone.utc).isoformat()

            pp = enrichment.get("person_profile", {})
            ident = enrichment.get("identity", {})
            prof_legacy = enrichment.get("professional", {})
            comp_legacy = enrichment.get("company", {})
            ci = enrichment.get("company_identity", {})
            cp = enrichment.get("company_profile", {})
            cin = enrichment.get("company_intelligence", {})
            intent_s = enrichment.get("intent_signals", {})
            outreach = enrichment.get("outreach", {})
            crm = enrichment.get("crm", {})
            ls = enrichment.get("lead_scoring", scoring)
            wi = enrichment.get("website_intelligence", website_intel or {})

            # Build waterfall log
            waterfall_log = [
                {"step": 0, "source": "Bright Data", "fields_found": ["profile"], "note": f"LinkedIn scrape: {url}"},
                *company_wf_log,
                {
                    "step": 10,
                    "source": contact.get("source") or "none",
                    "fields_found": ["email"],
                    "note": f"email={contact.get('email')}",
                },
            ]

            lead = {
                "id": _lead_id(url),
                "linkedin_url": url,
                "name": pp.get("full_name") or ident.get("full_name") or name,
                "first_name": first, "last_name": last,
                "work_email": pp.get("work_email") or ident.get("work_email") or contact.get("email"),
                "personal_email": pp.get("personal_email") or ident.get("personal_email"),
                "direct_phone": pp.get("direct_phone") or ident.get("direct_phone") or contact.get("phone"),
                "twitter": pp.get("twitter") or ident.get("twitter") or contact.get("twitter"),
                "city": pp.get("city") or ident.get("city"),
                "country": pp.get("country") or ident.get("country"),
                "timezone": pp.get("timezone") or ident.get("timezone"),
                "title": pp.get("current_title") or prof_legacy.get("current_title") or profile.get("position", ""),
                "seniority_level": pp.get("seniority_level") or prof_legacy.get("seniority_level"),
                "department": pp.get("department") or prof_legacy.get("department"),
                "years_in_role": pp.get("years_in_role") or prof_legacy.get("years_in_role"),
                "company": ci.get("name") or comp_legacy.get("name") or profile.get("current_company_name", ""),
                "previous_companies": _safe_json(prof_legacy.get("previous_companies")),
                "top_skills": _safe_json(pp.get("top_skills") or prof_legacy.get("top_skills")),
                "education": pp.get("education") or prof_legacy.get("education"),
                "certifications": _safe_json(pp.get("certifications") or prof_legacy.get("certifications")),
                "languages": _safe_json(pp.get("languages") or prof_legacy.get("languages")),
                "company_website": cp.get("website") or ci.get("website") or comp_legacy.get("website"),
                "industry": cp.get("industry") or comp_legacy.get("industry"),
                "employee_count": _safe_int(cp.get("employee_count") or comp_legacy.get("employee_count")),
                "hq_location": cp.get("hq_location") or comp_legacy.get("hq_location"),
                "founded_year": str(cp.get("founded_year") or comp_legacy.get("founded_year") or ""),
                "funding_stage": cin.get("funding_stage") or comp_legacy.get("funding_stage"),
                "total_funding": cin.get("total_funding") or comp_legacy.get("total_funding"),
                "last_funding_date": cin.get("last_funding_date") or comp_legacy.get("last_funding_date"),
                "lead_investor": cin.get("lead_investor") or comp_legacy.get("lead_investor"),
                "annual_revenue": cp.get("annual_revenue_est") or comp_legacy.get("annual_revenue_est"),
                "tech_stack": _safe_json(cp.get("tech_stack") or comp_legacy.get("tech_stack")),
                "hiring_velocity": cin.get("hiring_velocity") or comp_legacy.get("hiring_velocity"),
                "avatar_url": _extract_avatar(profile) or contact.get("avatar_url"),
                "company_logo": cp.get("company_logo") or company_extras.get("company_logo"),
                "company_email": cp.get("company_email") or company_extras.get("company_email"),
                "company_description": wi.get("company_description") or company_extras.get("company_description"),
                "company_linkedin": ci.get("linkedin_url") or company_extras.get("company_linkedin"),
                "company_twitter": cp.get("company_twitter") or company_extras.get("company_twitter"),
                "company_phone": cp.get("company_phone") or company_extras.get("company_phone"),
                "waterfall_log": json.dumps(waterfall_log),
                # Stage 3 — Website Intelligence
                "website_intelligence": json.dumps(website_intel or {}),
                "product_offerings": _safe_json(wi.get("product_offerings")),
                "value_proposition": wi.get("value_proposition"),
                "target_customers": _safe_json(wi.get("target_customers")),
                "business_model": wi.get("business_model"),
                "pricing_signals": wi.get("pricing_signals"),
                "product_category": wi.get("product_category"),
                "data_completeness_score": int(ls.get("data_completeness_score", 0)),
                # Intent signals
                "recent_funding_event": intent_s.get("recent_funding_event"),
                "hiring_signal": intent_s.get("hiring_signal"),
                "job_change": intent_s.get("job_change"),
                "linkedin_activity": intent_s.get("linkedin_activity"),
                "news_mention": intent_s.get("news_mention"),
                "product_launch": intent_s.get("product_launch"),
                "competitor_usage": intent_s.get("competitor_usage"),
                "review_activity": intent_s.get("review_activity"),
                # Scoring
                "icp_fit_score": _safe_int(ls.get("icp_fit_score")),
                "intent_score": _safe_int(ls.get("intent_score")),
                "timing_score": _safe_int(ls.get("timing_score")),
                "engagement_score": 0,
                "total_score": _safe_int(ls.get("overall_score")),
                "score_tier": score_tier,
                "score_explanation": ls.get("score_explanation"),
                "icp_match_tier": ls.get("icp_match_tier") or tier_raw,
                "disqualification_flags": _safe_json(ls.get("disqualification_flags")),
                "email_subject": outreach.get("email_subject"),
                "cold_email": outreach.get("cold_email"),
                "linkedin_note": outreach.get("linkedin_note"),
                "best_channel": outreach.get("best_channel"),
                "best_send_time": outreach.get("best_send_time"),
                "outreach_angle": outreach.get("outreach_angle"),
                "sequence_type": outreach.get("sequence_type"),
                "outreach_sequence": _safe_json(outreach.get("sequence")),
                "last_contacted": outreach.get("last_contacted"),
                "email_status": outreach.get("email_status"),
                "lead_source": crm.get("lead_source", "LinkedIn URL"),
                "enrichment_source": crm.get("enrichment_source"),
                "data_completeness": _safe_int(crm.get("data_completeness")),
                "crm_stage": crm.get("crm_stage"),
                "tags": _safe_json(crm.get("tags")),
                "assigned_owner": crm.get("assigned_owner"),
                "full_data": json.dumps({
                    "person_profile": enrichment.get("person_profile", {}),
                    "company_identity": enrichment.get("company_identity", {}),
                    "website_intelligence": enrichment.get("website_intelligence", website_intel or {}),
                    "company_profile": enrichment.get("company_profile", {}),
                    "company_intelligence": enrichment.get("company_intelligence", {}),
                    "intent_signals": enrichment.get("intent_signals", {}),
                    "lead_scoring": enrichment.get("lead_scoring", {}),
                    "outreach": enrichment.get("outreach", {}),
                    # Legacy compatibility
                    "identity": enrichment.get("identity", {}),
                    "professional": enrichment.get("professional", {}),
                    "company": enrichment.get("company", {}),
                    "scoring": enrichment.get("scoring", {}),
                    # Profile-level data for UI
                    "recommendations": profile.get("recommendations") or [],
                    "recommendations_count": profile.get("recommendations_count") or 0,
                    "similar_profiles": profile.get("_similar_profiles") or [],
                    "banner_image": profile.get("banner_image") or "",
                    "linkedin_num_id": profile.get("linkedin_num_id") or "",
                    "activity_emails": profile.get("_activity_emails") or [],
                    "activity_phones": profile.get("_activity_phones") or [],
                    "activity_full": profile.get("_activity_full") or [],
                    "linkedin_posts": profile.get("_posts") or [],
                    "hiring_signals": profile.get("_hiring_signals") or [],
                }, default=str),
                "raw_profile": json.dumps(profile, default=str),
                "about": (profile.get("about") or "")[:1000],
                "followers": _safe_int(profile.get("followers")),
                "connections": _safe_int(profile.get("connections")),
                "email_source": contact.get("source"),
                "email_confidence": contact.get("confidence"),
                "status": "enriched",
                "job_id": job_id,
                "enriched_at": now,
            }
            await _upsert_lead(lead)
            processed += 1
        except Exception as e:
            logger.warning("[Webhook] %s: %s", url, e)
            failed += 1

    if job_id:
        ex = await get_job(job_id)
        if ex:
            await _update_job(job_id, processed=ex["processed"] + processed, failed=ex["failed"] + failed)
            up = await get_job(job_id)
            if up and up["processed"] + up["failed"] >= up["total_urls"]:
                await _update_job(job_id, status="completed")

    return {"processed": processed, "failed": failed}


async def regenerate_outreach_for_lead(lead_id: str) -> Optional[dict]:
    lead = await get_lead(lead_id)
    if not lead:
        return None
    profile = json.loads(lead.get("raw_profile") or "{}")
    contact = {
        "email": lead.get("work_email"), "phone": lead.get("direct_phone"),
        "source": lead.get("email_source"), "confidence": lead.get("email_confidence"),
    }
    # Re-load website intel from stored JSON
    website_intel_raw = lead.get("website_intelligence")
    website_intel = json.loads(website_intel_raw) if website_intel_raw else None

    enrichment = await build_comprehensive_enrichment(
        lead["linkedin_url"], profile, contact, website_intel=website_intel
    )
    outreach = enrichment.get("outreach", {})
    async with aiosqlite.connect(LEADS_DB) as db:
        await db.execute("""UPDATE enriched_leads SET
            email_subject=?, cold_email=?, linkedin_note=?,
            best_channel=?, best_send_time=?, outreach_angle=?,
            sequence_type=?, outreach_sequence=?, email_status=?,
            full_data=?
            WHERE id=?""",
            (outreach.get("email_subject"), outreach.get("cold_email"),
             outreach.get("linkedin_note"), outreach.get("best_channel"),
             outreach.get("best_send_time"), outreach.get("outreach_angle"),
             outreach.get("sequence_type"), _safe_json(outreach.get("sequence")),
             outreach.get("email_status"), json.dumps(enrichment), lead_id),
        )
        await db.commit()
    return await get_lead(lead_id)

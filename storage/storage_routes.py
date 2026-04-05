"""
storage_routes.py
-----------------
GET /api/storage/stats            — total leads, imported, enriched, unmapped records
GET /api/storage/leads            — paginated list of all enriched_leads
GET /api/storage/unmapped         — paginated list of import_unmapped_data
GET /api/storage/unmapped/{lead_id} — all unmapped fields for a specific lead
"""

from __future__ import annotations

import json
import logging

from fastapi import APIRouter, Query

from db import get_pool

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/storage", tags=["storage"])


# ─────────────────────────────────────────────────────────────────────────────
# Stats
# ─────────────────────────────────────────────────────────────────────────────

async def _ensure_unmapped_table(conn) -> None:
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS import_unmapped_data (
            id TEXT PRIMARY KEY, lead_id TEXT NOT NULL,
            org_id TEXT NOT NULL DEFAULT 'default',
            source_file TEXT, job_id TEXT,
            raw_data TEXT NOT NULL, created_at TEXT
        )
    """)
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_unmapped_lead ON import_unmapped_data(lead_id)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_unmapped_org  ON import_unmapped_data(org_id, created_at DESC)")


@router.get("/stats")
async def storage_stats():
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            """WITH lead_stats AS (
                   SELECT
                       COUNT(*) AS total_leads,
                       COUNT(*) FILTER (
                           WHERE enrichment_source IS NOT NULL
                             AND enrichment_source NOT LIKE '%bright%'
                       ) AS imported_leads,
                       COUNT(*) FILTER (WHERE total_score > 0) AS enriched_leads
                   FROM enriched_leads
               ),
               sources AS (
                   SELECT COALESCE(enrichment_source, 'unknown') AS src, COUNT(*) AS cnt
                   FROM enriched_leads
                   GROUP BY src ORDER BY cnt DESC LIMIT 10
               ),
               unmapped AS (
                   SELECT COUNT(*) AS cnt FROM import_unmapped_data
               )
               SELECT
                   (SELECT total_leads    FROM lead_stats) AS total_leads,
                   (SELECT imported_leads FROM lead_stats) AS imported_leads,
                   (SELECT enriched_leads FROM lead_stats) AS enriched_leads,
                   (SELECT cnt FROM unmapped)              AS unmapped_records,
                   (SELECT json_agg(json_build_object('source', src, 'count', cnt)
                           ORDER BY cnt DESC)
                    FROM sources)                          AS by_source
            """
        )

    r = rows[0]
    by_source = r["by_source"] or []
    if isinstance(by_source, str):
        by_source = json.loads(by_source)
    return {
        "total_leads":      int(r["total_leads"] or 0),
        "imported_leads":   int(r["imported_leads"] or 0),
        "enriched_leads":   int(r["enriched_leads"] or 0),
        "unmapped_records": int(r["unmapped_records"] or 0),
        "by_source":        by_source,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Leads list
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/leads")
async def list_leads(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200),
    q: str = Query(""),       # search: name / email / company
    source: str = Query(""),  # filter by enrichment_source
    status: str = Query(""),  # filter by status
):
    offset = (page - 1) * limit
    params: list = []
    clauses: list[str] = []

    if q:
        params.append(f"%{q.lower()}%")
        n = len(params)
        clauses.append(
            f"(LOWER(name) LIKE ${n} OR LOWER(work_email) LIKE ${n} "
            f"OR LOWER(company) LIKE ${n} OR LOWER(linkedin_url) LIKE ${n})"
        )
    if source:
        params.append(source)
        clauses.append(f"enrichment_source=${len(params)}")
    if status:
        params.append(status)
        clauses.append(f"status=${len(params)}")

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    params_page = params + [limit, offset]
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            f"""SELECT
                    id, linkedin_url, name, first_name, last_name,
                    work_email, company, title, seniority_level,
                    city, country, industry, total_score, score_tier,
                    status, lead_source, enrichment_source,
                    created_at, enriched_at,
                    COUNT(*) OVER() AS total_count
                FROM enriched_leads
                {where}
                ORDER BY created_at DESC NULLS LAST
                LIMIT ${len(params_page)-1} OFFSET ${len(params_page)}""",
            *params_page,
        )

    total_int = int(rows[0]["total_count"] if rows else 0)
    return {
        "total": total_int,
        "page": page,
        "pages": max(1, -(-total_int // limit)),
        "limit": limit,
        "leads": [{k: v for k, v in r.items() if k != "total_count"} for r in rows],
    }


# ─────────────────────────────────────────────────────────────────────────────
# Unmapped data list
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/unmapped")
async def list_unmapped(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200),
    lead_id: str = Query(""),
    source_file: str = Query(""),
):
    offset = (page - 1) * limit
    params: list = []
    clauses: list[str] = []

    if lead_id:
        params.append(lead_id)
        clauses.append(f"lead_id=${len(params)}")
    if source_file:
        params.append(f"%{source_file}%")
        clauses.append(f"source_file ILIKE ${len(params)}")

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    params_page = params + [limit, offset]
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            f"""SELECT id, lead_id, source_file, job_id, raw_data, created_at,
                       COUNT(*) OVER() AS total_count
                FROM import_unmapped_data
                {where}
                ORDER BY created_at DESC NULLS LAST
                LIMIT ${len(params_page)-1} OFFSET ${len(params_page)}""",
            *params_page,
        )

    total_int = int(rows[0]["total_count"] if rows else 0)
    return {
        "total": total_int,
        "page": page,
        "pages": max(1, -(-total_int // limit)),
        "limit": limit,
        "records": [{k: v for k, v in r.items() if k != "total_count"} for r in rows],
    }


# ─────────────────────────────────────────────────────────────────────────────
# Unmapped fields for a specific lead
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/unmapped/{lead_id:path}")
async def lead_unmapped(lead_id: str):
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            """SELECT id, source_file, job_id, raw_data, created_at
               FROM import_unmapped_data
               WHERE lead_id=$1
               ORDER BY created_at DESC""",
            lead_id,
        )
    return {"lead_id": lead_id, "records": [dict(r) for r in rows]}

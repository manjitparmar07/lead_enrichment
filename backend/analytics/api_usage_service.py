"""
api_usage_service.py — Track API call counts for BrightData, Apollo, HuggingFace
----------------------------------------------------------------------------------
Writes directly to DB on every track() call — no in-memory buffer.
Each call is a single cheap UPSERT (increment counter by 1).
"""

from __future__ import annotations

import logging
from datetime import date

logger = logging.getLogger(__name__)


async def _init_table() -> None:
    """Create api_usage_log table if not exists."""
    try:
        from db import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS api_usage_log (
                    id          SERIAL PRIMARY KEY,
                    api         TEXT NOT NULL,
                    call_type   TEXT NOT NULL,
                    day         DATE NOT NULL DEFAULT CURRENT_DATE,
                    count       BIGINT NOT NULL DEFAULT 0,
                    UNIQUE (api, call_type, day)
                )
            """)
    except Exception as e:
        logger.warning("[ApiUsage] Table init failed: %s", e)


async def track(api: str, call_type: str = "call") -> None:
    """
    Increment counter for an API call. Writes directly to DB.

    api:       "brightdata" | "apollo" | "huggingface" | "validemail"
    call_type: "profile" | "company" | "person_match" | "llm_call" | "verify"
    """
    try:
        from db import get_pool
        pool = await get_pool()
        today = date.today()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO api_usage_log (api, call_type, day, count)
                VALUES ($1, $2, $3, 1)
                ON CONFLICT (api, call_type, day)
                DO UPDATE SET count = api_usage_log.count + 1
            """, api, call_type, today)
    except Exception as e:
        logger.warning("[ApiUsage] track(%s, %s) failed: %s", api, call_type, e)


async def get_usage(days: int = 30) -> list[dict]:
    """Return raw daily rows from api_usage_log for the last N days."""
    try:
        from db import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT api, call_type, day::text AS day, count
                FROM api_usage_log
                WHERE day >= CURRENT_DATE - ($1 * INTERVAL '1 day')
                ORDER BY day DESC, api, call_type
            """, days)
        return [dict(r) for r in rows]
    except Exception as e:
        logger.warning("[ApiUsage] get_usage failed: %s", e)
        return []


async def get_summary(days: int = 30) -> dict:
    """Return totals per API + breakdown by call_type for the last N days."""
    rows = await get_usage(days)
    totals: dict[str, int] = {}
    by_type: dict[str, dict] = {}

    for r in rows:
        api   = r["api"]
        ctype = r["call_type"]
        cnt   = r["count"]
        totals[api] = totals.get(api, 0) + cnt
        if api not in by_type:
            by_type[api] = {}
        by_type[api][ctype] = by_type[api].get(ctype, 0) + cnt

    return {
        "period_days": days,
        "totals":      totals,
        "by_type":     by_type,
    }

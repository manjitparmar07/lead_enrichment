"""
api_usage_service.py — Track API call counts for BrightData, Apollo, HuggingFace
----------------------------------------------------------------------------------
Uses a simple DB table: api_usage_log
Increments counter every time an external API is called.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timezone

logger = logging.getLogger(__name__)

# ── In-memory buffer (flush to DB every N calls or on demand) ─────────────────
# Avoids a DB write on every single API call — batches them.
_buffer: dict[tuple, int] = {}   # (api, call_type, date) → count
_buffer_lock = asyncio.Lock()
_FLUSH_EVERY  = 10               # flush after 10 accumulated calls


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


async def _flush() -> None:
    """Flush in-memory buffer to DB."""
    async with _buffer_lock:
        if not _buffer:
            return
        snapshot = dict(_buffer)
        _buffer.clear()

    try:
        from db import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            for (api, call_type, day), cnt in snapshot.items():
                await conn.execute("""
                    INSERT INTO api_usage_log (api, call_type, day, count)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (api, call_type, day)
                    DO UPDATE SET count = api_usage_log.count + EXCLUDED.count
                """, api, call_type, day, cnt)
    except Exception as e:
        logger.warning("[ApiUsage] Flush failed: %s", e)
        # Put back into buffer
        async with _buffer_lock:
            for k, v in snapshot.items():
                _buffer[k] = _buffer.get(k, 0) + v


async def track(api: str, call_type: str = "call") -> None:
    """
    Increment counter for an API call. Fire-and-forget safe.

    api:       "brightdata" | "apollo" | "huggingface" | "validemail" | "dropcontact" | "pdl" | "zerobounce"
    call_type: "profile" | "company" | "person_match" | "org_enrich" | "llm_call" | "verify" etc.
    """
    today = date.today()
    key = (api, call_type, today)

    async with _buffer_lock:
        _buffer[key] = _buffer.get(key, 0) + 1
        total = sum(_buffer.values())

    if total >= _FLUSH_EVERY:
        asyncio.create_task(_flush())


async def get_usage(days: int = 30) -> list[dict]:
    """Return API usage stats for the last N days."""
    try:
        await _flush()   # flush buffer first so counts are up-to-date
        from db import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT api, call_type, day, count
                FROM api_usage_log
                WHERE day >= CURRENT_DATE - $1::int
                ORDER BY day DESC, api, call_type
            """, days)
        return [dict(r) for r in rows]
    except Exception as e:
        logger.warning("[ApiUsage] get_usage failed: %s", e)
        return []


async def get_summary(days: int = 30) -> dict:
    """Return totals per API for the last N days."""
    rows = await get_usage(days)
    totals: dict[str, int] = {}
    by_type: dict[str, dict] = {}

    for r in rows:
        api  = r["api"]
        ctype = r["call_type"]
        cnt  = r["count"]
        totals[api] = totals.get(api, 0) + cnt
        if api not in by_type:
            by_type[api] = {}
        by_type[api][ctype] = by_type[api].get(ctype, 0) + cnt

    return {
        "period_days": days,
        "totals":      totals,
        "by_type":     by_type,
        "detail":      rows,
    }

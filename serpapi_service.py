"""
serpapi_service.py — LinkedIn URL finder via SerpAPI (Google search)
"""
import os
import uuid
import logging
from datetime import datetime, timezone
from typing import Optional
import aiohttp
from db import get_pool

logger = logging.getLogger(__name__)

SERPAPI_BASE = "https://serpapi.com/search.json"


def _serpapi_key() -> str:
    return os.getenv("SERPAPI_KEY", "")


# ── DB init ───────────────────────────────────────────────────────────────────

async def init_serpapi_db() -> None:
    async with get_pool().acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS linkedin_searches (
                id          TEXT PRIMARY KEY,
                query       TEXT NOT NULL,
                linkedin_urls JSONB NOT NULL DEFAULT '[]',
                result_count  INT  DEFAULT 0,
                org_id      TEXT DEFAULT 'default',
                created_at  TIMESTAMPTZ DEFAULT NOW()
            )
        """)
    logger.info("[SerpAPI] DB ready")


# ── Core search ───────────────────────────────────────────────────────────────

async def search_linkedin_urls(query: str, org_id: str = "default", num: int = 10, filters: dict | None = None) -> dict:
    """Search Google via SerpAPI for LinkedIn profile URLs matching query."""
    key = _serpapi_key()
    if not key:
        raise ValueError("SERPAPI_KEY not configured")

    filters = filters or {}
    num = max(1, min(num, 100))  # clamp 1–100

    # SerpAPI / Google returns 10 results per page max.
    # To get more, paginate using `start` (0, 10, 20 …).
    pages_needed = (num + 9) // 10  # ceil(num / 10)

    # Build query string — apply exact title and exclude keywords modifiers
    q_string = f"site:linkedin.com/in {query}"
    exclude = filters.get("excludeKeywords") or ""
    if exclude:
        # Append each comma-separated word as a Google minus operator
        for word in [w.strip() for w in exclude.split(",") if w.strip()]:
            q_string += f" -{word}"

    linkedin_urls: list[str] = []
    seen: set[str] = set()

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
        for page in range(pages_needed):
            if len(linkedin_urls) >= num:
                break
            params = {
                "engine": "google",
                "q": q_string,
                "api_key": key,
                "num": 10,
                "start": page * 10,
            }
            # Apply SerpAPI filter params only when explicitly set
            if filters.get("gl"):
                params["gl"] = filters["gl"]
            if filters.get("hl"):
                params["hl"] = filters["hl"]
            if filters.get("tbs"):
                params["tbs"] = filters["tbs"]
            async with session.get(SERPAPI_BASE, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()

            for r in data.get("organic_results", []):
                link = r.get("link", "")
                if "linkedin.com/in/" in link:
                    clean = link.split("?")[0].rstrip("/")
                    if clean not in seen:
                        seen.add(clean)
                        linkedin_urls.append(clean)

            # Stop early if Google has no more results
            if not data.get("organic_results"):
                break

    # Save to DB
    record_id = str(uuid.uuid4())
    async with get_pool().acquire() as conn:
        await conn.execute(
            """INSERT INTO linkedin_searches (id, query, linkedin_urls, result_count, org_id, created_at)
               VALUES ($1, $2, $3::jsonb, $4, $5, $6)""",
            record_id,
            query,
            __import__("json").dumps(linkedin_urls),
            len(linkedin_urls),
            org_id,
            datetime.now(timezone.utc),
        )

    logger.info("[SerpAPI] query=%r → %d URLs", query, len(linkedin_urls))
    return {"id": record_id, "query": query, "linkedin_urls": linkedin_urls, "result_count": len(linkedin_urls)}


async def bulk_search(queries: list[str], org_id: str = "default", num: int = 10, filters: dict | None = None) -> list[dict]:
    """Run multiple queries concurrently."""
    import asyncio
    results = await asyncio.gather(
        *[search_linkedin_urls(q, org_id, num, filters) for q in queries],
        return_exceptions=True,
    )
    output = []
    for q, r in zip(queries, results):
        if isinstance(r, Exception):
            logger.warning("[SerpAPI] query=%r failed: %s", q, r)
            output.append({"query": q, "linkedin_urls": [], "result_count": 0, "error": str(r)})
        else:
            output.append(r)
    return output


# ── DB reads ─────────────────────────────────────────────────────────────────

async def list_searches(org_id: str = "default", limit: int = 50) -> list[dict]:
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            """SELECT id, query, linkedin_urls, result_count, created_at
               FROM linkedin_searches
               WHERE org_id = $1
               ORDER BY created_at DESC
               LIMIT $2""",
            org_id, limit,
        )
    return [dict(r) for r in rows]


async def delete_search(search_id: str, org_id: str = "default") -> bool:
    async with get_pool().acquire() as conn:
        result = await conn.execute(
            "DELETE FROM linkedin_searches WHERE id=$1 AND org_id=$2",
            search_id, org_id,
        )
    return result == "DELETE 1"

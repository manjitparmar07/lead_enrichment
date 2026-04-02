"""
serpapi_routes.py — LinkedIn URL finder API
"""
import logging
from typing import Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import serpapi_service as svc

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/linkedin-finder", tags=["LinkedIn Finder"])


# ── Request models ────────────────────────────────────────────────────────────

class SearchRequest(BaseModel):
    query: str
    org_id: Optional[str] = "default"


class AdvancedFilters(BaseModel):
    gl: Optional[str] = None              # Google country code (e.g. "us", "in")
    hl: Optional[str] = "en"             # Interface language (e.g. "en", "hi")
    tbs: Optional[str] = None            # Time-based search (e.g. "qdr:w", "qdr:m")
    excludeKeywords: Optional[str] = None  # Comma-separated words to exclude
    exactTitle: Optional[bool] = False   # Wrap role/title in quotes


class BulkSearchRequest(BaseModel):
    queries: list[str]
    org_id: Optional[str] = "default"
    num: Optional[int] = 10              # results per query, max 100
    filters: Optional[AdvancedFilters] = None


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/search")
async def search(req: SearchRequest):
    """Search Google for LinkedIn profile URLs matching a query."""
    if not req.query.strip():
        raise HTTPException(status_code=422, detail="query is required")
    try:
        result = await svc.search_linkedin_urls(req.query.strip(), req.org_id)
        return {"success": True, **result}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("[LinkedInFinder] search failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/bulk-search")
async def bulk_search(req: BulkSearchRequest):
    """Run multiple queries concurrently and return all found LinkedIn URLs."""
    if not req.queries:
        raise HTTPException(status_code=422, detail="queries list is required")
    req.queries = [q.strip() for q in req.queries if q.strip()]
    if len(req.queries) > 20:
        raise HTTPException(status_code=422, detail="Max 20 queries per request")
    try:
        filters = req.filters.model_dump() if req.filters else {}
        results = await svc.bulk_search(req.queries, req.org_id, req.num or 10, filters)
        all_urls = list({u for r in results for u in r.get("linkedin_urls", [])})
        return {"success": True, "results": results, "total_urls": len(all_urls), "all_urls": all_urls}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("[LinkedInFinder] bulk search failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/searches")
async def list_searches(org_id: str = "default", limit: int = 50):
    """List past LinkedIn searches saved to DB."""
    rows = await svc.list_searches(org_id, limit)
    return {"success": True, "searches": rows, "count": len(rows)}


@router.delete("/searches/{search_id}")
async def delete_search(search_id: str, org_id: str = "default"):
    """Delete a saved search."""
    deleted = await svc.delete_search(search_id, org_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Search not found")
    return {"success": True}

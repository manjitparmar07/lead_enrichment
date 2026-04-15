"""
filter_routes.py
----------------
FastAPI routes for both BrightData and Apollo people search.

BrightData endpoints:
  POST /api/bd-filter/search                — filter BrightData LinkedIn dataset → save to DB
  GET  /api/bd-filter/records               — list saved BrightData filter records

Apollo endpoints:
  POST /api/bd-filter/apollo/search         — search Apollo People → save to DB
  GET  /api/bd-filter/apollo/records        — list saved Apollo search records
  GET  /api/bd-filter/apollo/seniorities    — list available seniority options
  POST /api/bd-filter/apollo/phone-webhook  — Apollo async phone reveal callback
"""

from __future__ import annotations

import logging
from typing import Any, List, Optional

import httpx
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request
from pydantic import BaseModel, Field

from Lead_enrichment.bulk_lead_enrichment._shared import _validate_token
from Lead_enrichment.bulk_lead_enrichment._utils import _bd_api_key, _apollo_master_api_key
from db import get_pool

from . import filter_service as bd_svc
from . import apollo_service as ap_svc

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/bd-filter", tags=["BrightData & Apollo Filter"])


# ─────────────────────────────────────────────────────────────────────────────
# ── BrightData models ────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────

class BDSearchRequest(BaseModel):
    """
    BrightData compound / simple filter.
    Pass the full BrightData filter object under `filters`.

    Simple:
      {"name": "city", "operator": "includes", "value": "London"}

    Compound (AND / OR):
      {
        "operator": "and",
        "filters": [
          {"name": "position", "operator": "includes", "value": "cto"},
          {"name": "city",     "operator": "includes", "value": "London"}
        ]
      }
    """
    token:   str         = Field(..., description="JWT auth token")
    filters: dict        = Field(..., description="BrightData filter object (simple or compound)")
    limit:   int         = Field(20, ge=1, description="Max records to return (1 to any number)")
    timeout: int         = Field(1800, ge=30, le=7200, description="Max seconds to wait for snapshot")


# ─────────────────────────────────────────────────────────────────────────────
# ── Apollo models ─────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────

class ApolloSearchRequest(BaseModel):
    token:    str                   = Field(..., description="JWT auth token")
    # All filter fields are optional — combine as needed
    person_titles:          Optional[List[str]] = Field(None, description="Job titles e.g. ['cto', 'chief technology officer']")
    person_locations:       Optional[List[str]] = Field(None, description="Person's own location e.g. ['London, GB', 'California, US']")
    organization_names:     Optional[List[str]] = Field(None, description="Company names e.g. ['Stripe', 'Shopify']")
    organization_locations: Optional[List[str]] = Field(None, description="Company HQ location e.g. ['San Francisco, US']")
    person_seniorities:     Optional[List[str]] = Field(None, description="Seniority levels e.g. ['c_suite', 'vp', 'director']")
    contact_email_status:   Optional[List[str]] = Field(None, description="Email status e.g. ['verified']")
    q_keywords:             Optional[str]       = Field(None, description="Free-text keywords")
    per_page: int  = Field(20, ge=1, le=100, description="Results per page")
    page:     int  = Field(1, ge=1, description="Page number")
    enrich:   bool = Field(False, description="When True, call people/match per result for full data (costs credits)")


# ─────────────────────────────────────────────────────────────────────────────
# ── Shared records helper ─────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────

async def _fetch_records(
    sources: list,
    page: int,
    page_size: int,
    org_id: Optional[str] = None,
) -> dict:
    """
    Fetch records matching any of the provided status values.
    If org_id is None, returns records across all organisations.
    """
    offset = (page - 1) * page_size
    async with get_pool().acquire() as conn:
        if org_id:
            rows = await conn.fetch(
                """
                SELECT
                    id, linkedin_url, name, first_name, last_name,
                    title, company, company_linkedin, company_logo,
                    avatar_url, city, country, company_description,
                    industry, employee_count, top_skills, education,
                    linkedin_followers, work_email, direct_phone,
                    enriched_at, created_at, status, organization_id
                FROM enriched_leads
                WHERE organization_id = $1
                  AND status = ANY($2)
                ORDER BY created_at DESC
                LIMIT $3 OFFSET $4
                """,
                org_id, sources, page_size, offset,
            )
            total_row = await conn.fetchrow(
                "SELECT COUNT(*) AS total FROM enriched_leads WHERE organization_id=$1 AND status=ANY($2)",
                org_id, sources,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT
                    id, linkedin_url, name, first_name, last_name,
                    title, company, company_linkedin, company_logo,
                    avatar_url, city, country, company_description,
                    industry, employee_count, top_skills, education,
                    linkedin_followers, work_email, direct_phone,
                    enriched_at, created_at, status, organization_id
                FROM enriched_leads
                WHERE status = ANY($1)
                ORDER BY created_at DESC
                LIMIT $2 OFFSET $3
                """,
                sources, page_size, offset,
            )
            total_row = await conn.fetchrow(
                "SELECT COUNT(*) AS total FROM enriched_leads WHERE status=ANY($1)",
                sources,
            )
    return {
        "total":     total_row["total"] if total_row else 0,
        "page":      page,
        "page_size": page_size,
        "records":   [dict(r) for r in rows],
    }


# ─────────────────────────────────────────────────────────────────────────────
# ══ BrightData routes ════════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/search")
async def bd_search(body: BDSearchRequest, background_tasks: BackgroundTasks):
    """
    Trigger a BrightData LinkedIn People filter snapshot.
    Returns immediately with snapshot_id + status "processing".
    Poll GET /bd-filter/status/{snapshot_id} for live progress and results.
    """
    org_id, _ = _validate_token(body.token)

    if not body.filters:
        raise HTTPException(status_code=400, detail="filters object is required")

    try:
        result = await bd_svc.trigger_search(
            filters=body.filters,
            org_id=org_id,
            limit=body.limit,
            timeout=body.timeout,
        )
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))

    # Run the long-running poll→download→save in the background
    background_tasks.add_task(
        bd_svc.run_snapshot_job,
        result["snapshot_id"],
        org_id,
        body.timeout,
    )

    return result


@router.get("/status/{snapshot_id}")
async def bd_snapshot_status(snapshot_id: str):
    """
    Poll the status of a BrightData snapshot job.

    status values:
      processing   — snapshot triggered, waiting for BrightData
      building     — BrightData is building the snapshot
      downloading  — snapshot ready, downloading records
      saving       — records being saved to DB
      done         — complete; result fields populated
      failed       — error field contains reason
      not_found    — unknown snapshot_id
    """
    return bd_svc.get_job_status(snapshot_id)


@router.get("/records")
async def bd_records(
    token:     Optional[str] = Query(None),
    page:      int           = Query(1,  ge=1),
    page_size: int           = Query(50, ge=1, le=200),
):
    """List leads saved via BrightData filter search. Token optional — omit to get all orgs."""
    org_id = None
    if token:
        try:
            org_id, _ = _validate_token(token)
        except Exception:
            pass
    return await _fetch_records(["bd_filter"], page, page_size, org_id=org_id)


# ─────────────────────────────────────────────────────────────────────────────
# Credits / usage check — BrightData balance + Apollo usage stats
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/credits")
async def get_credits(token: Optional[str] = Query(None)):
    """
    Fetch live usage/balance info from BrightData and Apollo.

    BrightData: /customer/balance (requires account-level key).
                Falls back to /account/info or /status for key validation.
    Apollo:     /api/v1/usage_stats/api_usage_stats (master key required).
    """
    bd_key     = _bd_api_key()
    apollo_key = _apollo_master_api_key()

    bd_result     = {}
    apollo_result = {}

    async with httpx.AsyncClient(timeout=15.0) as client:
        # ── BrightData ───────────────────────────────────────────────────────
        if not bd_key:
            bd_result = {"error": "BRIGHT_DATA_API_KEY not configured"}
        else:
            # Try /customer/balance first (requires account-management scope)
            try:
                resp = await client.get(
                    "https://api.brightdata.com/customer/balance",
                    headers={"Authorization": f"Bearer {bd_key}"},
                )
                logger.info("[Credits] BD /customer/balance  status=%d  body=%s", resp.status_code, resp.text[:300])
                if resp.status_code == 200:
                    bd_result = resp.json()
                elif resp.status_code == 403:
                    # Datasets API key has no account-management scope.
                    # Fall back to /status to at least confirm the key works.
                    s2 = await client.get(
                        "https://api.brightdata.com/status",
                        headers={"Authorization": f"Bearer {bd_key}"},
                    )
                    logger.info("[Credits] BD /status  status=%d  body=%s", s2.status_code, s2.text[:300])
                    if s2.status_code == 200:
                        bd_result = {
                            "note": "Balance requires an account-level API key. Datasets key is active.",
                            **s2.json(),
                        }
                    else:
                        bd_result = {"error": f"Key invalid (balance: 403, status: {s2.status_code})"}
                else:
                    bd_result = {"error": f"HTTP {resp.status_code}", "detail": resp.text[:200]}
            except Exception as e:
                bd_result = {"error": str(e)}

        # ── Apollo ───────────────────────────────────────────────────────────
        if not apollo_key:
            apollo_result = {"error": "APOLLO_API_KEY_MASTER not configured"}
        else:
            try:
                resp = await client.post(
                    "https://api.apollo.io/api/v1/usage_stats/api_usage_stats",
                    headers={
                        "Content-Type": "application/json",
                        "Cache-Control": "no-cache",
                        "X-Api-Key": apollo_key,
                    },
                    json={"api_key": apollo_key},
                )
                logger.info("[Credits] Apollo usage_stats  status=%d  body=%s", resp.status_code, resp.text[:500])
                if resp.status_code == 200:
                    apollo_result = resp.json()
                else:
                    apollo_result = {"error": f"HTTP {resp.status_code}", "detail": resp.text[:200]}
            except Exception as e:
                apollo_result = {"error": str(e)}

    return {
        "brightdata": bd_result,
        "apollo":     apollo_result,
    }


# ─────────────────────────────────────────────────────────────────────────────
# ══ Apollo routes ═════════════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/apollo/search")
async def apollo_search(body: ApolloSearchRequest):
    """
    Search Apollo People (mixed_people/api_search) and save results to enriched_leads.
    Returns Apollo total_entries, page info, and saved lead dicts.
    """
    org_id, _ = _validate_token(body.token)

    # Build filters dict from request fields (skip None / empty)
    filters: dict[str, Any] = {}
    for key in ("person_titles", "person_locations", "organization_names",
                "organization_locations", "person_seniorities", "contact_email_status"):
        val = getattr(body, key, None)
        if val:
            filters[key] = val
    if body.q_keywords and body.q_keywords.strip():
        filters["q_keywords"] = body.q_keywords.strip()

    if not filters:
        raise HTTPException(status_code=400, detail="At least one filter field is required")

    try:
        result = await ap_svc.search_and_save(
            filters=filters,
            org_id=org_id,
            per_page=body.per_page,
            page=body.page,
            enrich=body.enrich,
        )
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))

    return result


@router.get("/apollo/records")
async def apollo_records(
    token:     Optional[str] = Query(None),
    page:      int           = Query(1,  ge=1),
    page_size: int           = Query(50, ge=1, le=200),
):
    """List leads saved via Apollo people search (free + enriched). Token optional — omit to get all orgs."""
    org_id = None
    if token:
        try:
            org_id, _ = _validate_token(token)
        except Exception:
            pass
    return await _fetch_records(["apollo_search", "apollo_enriched"], page, page_size, org_id=org_id)


@router.get("/apollo/seniorities")
async def apollo_seniority_options():
    """Return the list of valid Apollo seniority filter values."""
    return {"seniorities": ap_svc.SENIORITY_OPTIONS}


# ─────────────────────────────────────────────────────────────────────────────
# Apollo async phone-reveal webhook
# Apollo POSTs here when a phone number is ready (reveal_phone_number=True).
# No auth token needed — Apollo calls this directly.
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/apollo/phone-webhook")
async def apollo_phone_webhook(request: Request):
    """
    Receive Apollo's async phone-reveal callback and update direct_phone
    in enriched_leads for the matching lead.

    Apollo payload shape:
      { "person": { "id": "<apollo_id>", "phone_numbers": [...], ... } }
    """
    try:
        body = await request.json()
    except Exception:
        logger.warning("[ApolloPhoneWebhook] Could not parse JSON body")
        return {"ok": False, "reason": "invalid json"}

    person = body.get("person") or {}
    apollo_id = person.get("id") or ""

    if not apollo_id:
        logger.warning("[ApolloPhoneWebhook] No person.id in payload: %s", str(body)[:200])
        return {"ok": False, "reason": "no person.id"}

    phones = person.get("phone_numbers") or []
    phone  = phones[0].get("sanitized_number", "") if phones else ""

    if not phone:
        logger.info("[ApolloPhoneWebhook] No phone in payload for id=%s", apollo_id)
        return {"ok": True, "updated": False, "reason": "no phone number in payload"}

    lead_id = ap_svc._apollo_lead_id(apollo_id)

    async with get_pool().acquire() as conn:
        result = await conn.execute(
            "UPDATE enriched_leads SET direct_phone = $1 WHERE id = $2",
            phone, lead_id,
        )

    updated = result != "UPDATE 0"
    logger.info(
        "[ApolloPhoneWebhook] apollo_id=%s  lead_id=%s  phone=%s  updated=%s",
        apollo_id, lead_id, phone, updated,
    )

    return {"ok": True, "updated": updated, "lead_id": lead_id, "phone": phone}

"""
company_routes.py
------------------
REST endpoints for company enrichment data.

GET  /api/companies                  — list all enriched companies for org
GET  /api/companies/{company_id}     — get one company by ID
GET  /api/leads/{lead_id}/company    — get company enrichment for a specific lead
POST /api/companies/refresh          — force-refresh a company by LinkedIn URL
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from auth.auth_routes import get_current_user
from Lead_enrichment.company_lead_enrichment import company_service as cs
from db import get_pool

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Companies"])


# ── List companies for org ────────────────────────────────────────────────────

@router.get("/companies")
async def list_companies(
    limit: int  = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    current_user: dict = Depends(get_current_user),
):
    org_id = str(current_user.get("organization_id", "default"))
    companies = await cs.list_companies(org_id, limit=limit, offset=offset)
    return {"companies": companies, "total": len(companies), "offset": offset}


# ── Get one company by ID ─────────────────────────────────────────────────────

@router.get("/companies/{company_id}")
async def get_company(
    company_id: str,
    current_user: dict = Depends(get_current_user),
):
    org_id = str(current_user.get("organization_id", "default"))
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM company_enrichments WHERE id=$1 AND org_id=$2",
            company_id, org_id,
        )
    if not row:
        raise HTTPException(status_code=404, detail="Company not found")
    return cs._deserialise_record(dict(row))


# ── Get company for a specific lead ──────────────────────────────────────────

@router.post("/leads/{lead_id}/company")
async def get_lead_company(
    lead_id: str,
    current_user: dict = Depends(get_current_user),
):
    org_id = str(current_user.get("organization_id", "default"))

    async with get_pool().acquire() as conn:
        lead_row = await conn.fetchrow(
            "SELECT company_id, company_linkedin FROM enriched_leads WHERE id=$1 AND organization_id=$2",
            lead_id, org_id,
        )
        if not lead_row:
            raise HTTPException(status_code=404, detail="Lead not found")

        cid = lead_row["company_id"]
        if not cid:
            return {"company": None, "message": "No company enrichment linked to this lead"}

        row = await conn.fetchrow(
            "SELECT * FROM company_enrichments WHERE id=$1 AND org_id=$2",
            cid, org_id,
        )

    if not row:
        return {"company": None, "message": "Company enrichment not yet available"}

    return {"company": cs._deserialise_record(dict(row))}


# ── Force-refresh a company ───────────────────────────────────────────────────

class RefreshRequest(BaseModel):
    company_linkedin_url: str


@router.post("/companies/refresh")
async def refresh_company(
    body: RefreshRequest,
    current_user: dict = Depends(get_current_user),
):
    org_id = str(current_user.get("organization_id", "default"))

    if "linkedin.com/company/" not in body.company_linkedin_url:
        raise HTTPException(status_code=400, detail="Must be a LinkedIn company URL")

    try:
        record = await cs.enrich_company(
            company_linkedin_url=body.company_linkedin_url,
            org_id=org_id,
            force_refresh=True,
        )
        return {
            "status": "refreshed",
            "company_id": record.get("id"),
            "company_score": record.get("company_score"),
            "company_score_tier": record.get("company_score_tier"),
        }
    except Exception as e:
        logger.error("[CompanyRefresh] Failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
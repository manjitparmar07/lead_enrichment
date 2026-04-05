"""
workspace_routes.py
--------------------
GET    /api/workspace/config          — get config for caller's org
PUT    /api/workspace/config          — create / update config
DELETE /api/workspace/config          — reset to defaults
"""

from __future__ import annotations

import base64
import json
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from workspace import workspace_service as ws

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/workspace", tags=["Workspace Config"])


def _get_org_id(request: Request) -> str:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return "default"
    token = auth[7:].strip()
    try:
        parts   = token.split(".")
        padded  = parts[1] + "=" * (4 - len(parts[1]) % 4)
        payload = json.loads(base64.b64decode(padded))
        return str(payload.get("organization_id", "default"))
    except Exception:
        return "default"


class WorkspaceConfigIn(BaseModel):
    product_name:       Optional[str]       = None
    value_proposition:  Optional[str]       = None
    target_titles:      Optional[list[str]] = None
    target_industries:  Optional[list[str]] = None
    company_size_min:   Optional[int]       = None
    company_size_max:   Optional[int]       = None
    tone:               Optional[str]       = None   # peer / formal / casual
    cta_style:          Optional[str]       = None   # question / demo / resource / short_ask
    banned_phrases:     Optional[list[str]] = None
    case_studies:       Optional[list[str]] = None


@router.get("/config")
async def get_config(request: Request):
    org_id = _get_org_id(request)
    return await ws.get_workspace_config(org_id)


@router.put("/config")
async def upsert_config(body: WorkspaceConfigIn, request: Request):
    org_id = _get_org_id(request)
    data   = {k: v for k, v in body.model_dump().items() if v is not None}
    if not data:
        raise HTTPException(status_code=400, detail="No fields to update")
    return await ws.upsert_workspace_config(org_id, data)


@router.delete("/config")
async def delete_config(request: Request):
    org_id = _get_org_id(request)
    await ws.delete_workspace_config(org_id)
    return {"success": True, "message": "Config reset to defaults"}

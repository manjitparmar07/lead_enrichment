"""
custom_features_routes.py
──────────────────────────
CRUD + execution endpoints for custom AI features.

GET    /api/v1/features              — list features for org
POST   /api/v1/features              — create feature
GET    /api/v1/features/hf/models    — search HuggingFace models
GET    /api/v1/features/{id}         — get feature by id
PUT    /api/v1/features/{id}         — update feature
DELETE /api/v1/features/{id}         — delete feature
POST   /api/v1/features/run/{slug}   — execute feature by endpoint slug
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from custom_features import custom_features_service as svc

router = APIRouter(prefix="/v1/features", tags=["Custom Features"])


# ── Org helper ────────────────────────────────────────────────────────────────

def _org(request: Request) -> str:
    return "default"


# ── Request models ────────────────────────────────────────────────────────────

class FeatureCreate(BaseModel):
    feature_code:   str
    feature_name:   str
    module:         str = "CUSTOM"
    endpoint_slug:  str
    task_type:      str = "ANALYZE"
    description:    Optional[str] = None
    system_prompt:  Optional[str] = None
    model_provider: str = "huggingface"
    model_name:     Optional[str] = None
    temperature:    float = 0.3
    input_params:   List[Dict[str, Any]] = []


class FeatureUpdate(BaseModel):
    feature_code:   Optional[str]   = None
    feature_name:   Optional[str]   = None
    module:         Optional[str]   = None
    endpoint_slug:  Optional[str]   = None
    task_type:      Optional[str]   = None
    description:    Optional[str]   = None
    system_prompt:  Optional[str]   = None
    model_provider: Optional[str]   = None
    model_name:     Optional[str]   = None
    temperature:    Optional[float] = None
    input_params:   Optional[List[Dict[str, Any]]] = None
    is_active:      Optional[bool]  = None


class RunRequest(BaseModel):
    inputs: Dict[str, Any] = {}
    system_prompt_override: Optional[str] = None


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get("")
async def list_features(request: Request):
    org_id = _org(request)
    features = await svc.list_features(org_id)
    return {"features": features}


@router.post("")
async def create_feature(body: FeatureCreate, request: Request):
    org_id = _org(request)
    try:
        feature = await svc.create_feature(org_id, body.dict())
        return {"feature": feature}
    except Exception as e:
        msg = str(e).lower()
        if "unique" in msg or "duplicate" in msg:
            raise HTTPException(409, "Feature code or endpoint slug already exists for this org")
        raise HTTPException(500, str(e))


# Must be declared BEFORE /{feature_id} so FastAPI doesn't try to parse "hf" as int
@router.get("/hf/models")
async def search_hf_models(q: str = "", limit: int = 20):
    models = await svc.search_hf_models(q, max(1, min(limit, 50)))
    return {"models": models}


@router.post("/run/{slug}")
async def run_feature(slug: str, body: RunRequest, request: Request):
    org_id = _org(request)
    try:
        result = await svc.execute_feature(org_id, slug, body.inputs, body.system_prompt_override)
        return result
    except ValueError as e:
        raise HTTPException(404, str(e))
    except RuntimeError as e:
        raise HTTPException(500, str(e))


@router.get("/{feature_id}")
async def get_feature(feature_id: int, request: Request):
    org_id  = _org(request)
    feature = await svc.get_feature_by_id(org_id, feature_id)
    if not feature:
        raise HTTPException(404, "Feature not found")
    return {"feature": feature}


@router.put("/{feature_id}")
async def update_feature(feature_id: int, body: FeatureUpdate, request: Request):
    org_id = _org(request)
    # Only send fields explicitly set by the client
    try:
        data = body.dict(exclude_unset=True)
    except Exception:
        data = {k: v for k, v in body.dict().items() if v is not None}

    feature = await svc.update_feature(org_id, feature_id, data)
    if not feature:
        raise HTTPException(404, "Feature not found")
    return {"feature": feature}


@router.delete("/{feature_id}")
async def delete_feature(feature_id: int, request: Request):
    org_id = _org(request)
    ok     = await svc.delete_feature(org_id, feature_id)
    if not ok:
        raise HTTPException(404, "Feature not found")
    return {"ok": True}

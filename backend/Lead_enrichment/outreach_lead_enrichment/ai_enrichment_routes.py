"""
ai_enrichment_routes.py
────────────────────────
POST /api/v1/ai/*  — AI enrichment endpoints powered by HuggingFace.
GET  /api/v1/ai/config   — return 10 AI module prompt configs for the org
PUT  /api/v1/ai/config   — save custom prompt configs for the org

All enrichment endpoints accept:
  { "profile": { ...BrightData profile... } }
  OR
  { "profile": [ ...BrightData array... ] }   (auto-unwraps first item)

All responses include token_usage.
"""
from __future__ import annotations

import base64
import json
from typing import List, Union

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from Lead_enrichment.outreach_lead_enrichment import ai_enrichment_service as svc
from config import enrichment_config_service as cfg_svc

router = APIRouter(prefix="/v1/ai", tags=["AI Enrichment"])


# ── Org helper ────────────────────────────────────────────────────────────────

def _org(request: Request) -> str:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return "default"
    token = auth[7:].strip()
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return "default"
        pad     = parts[1] + "=" * (4 - len(parts[1]) % 4)
        payload = json.loads(base64.b64decode(pad))
        return str(payload.get("organization_id", "default"))
    except Exception:
        return "default"


# ── Request models ────────────────────────────────────────────────────────────

class ProfileRequest(BaseModel):
    profile: Union[dict, list]


class AiPromptsUpdate(BaseModel):
    prompts: List[dict]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _profile(body: ProfileRequest) -> dict:
    return svc.clean_profile(body.profile)


def _module_overrides(module_configs: list, module_id: str) -> dict:
    """Return {system, user_template} overrides for a module id, or {}."""
    for m in module_configs:
        if m.get("id") == module_id:
            temp = m.get("temperature")
            return {
                "system_prompt": m.get("system") or None,
                "user_template": m.get("user_template") or None,
                "model":         m.get("model") or None,
                "temperature":   float(temp) if temp is not None else None,
            }
    return {}


# ── Config endpoints ──────────────────────────────────────────────────────────

@router.get("/config", include_in_schema=False)
async def get_ai_config(request: Request):
    org_id = _org(request)
    prompts = await cfg_svc.get_ai_module_prompts(org_id)
    return {"prompts": prompts}


@router.put("/config", include_in_schema=False)
async def save_ai_config(body: AiPromptsUpdate, request: Request):
    org_id = _org(request)
    await cfg_svc.save_ai_module_prompts(org_id, body.prompts)
    return {"ok": True}


# ── AI module endpoints ───────────────────────────────────────────────────────

@router.post("/identity")
async def identity(body: ProfileRequest, request: Request):
    org_id = _org(request)
    modules = await cfg_svc.get_ai_module_prompts(org_id)
    ov = _module_overrides(modules, "identity")
    try:
        return await svc.run_identity(_profile(body), **ov)
    except RuntimeError as e:
        raise HTTPException(500, str(e))


@router.post("/contact")
async def contact(body: ProfileRequest, request: Request):
    org_id = _org(request)
    modules = await cfg_svc.get_ai_module_prompts(org_id)
    ov = _module_overrides(modules, "contact")
    try:
        return await svc.run_contact(_profile(body), **ov)
    except RuntimeError as e:
        raise HTTPException(500, str(e))


@router.post("/scores")
async def scores(body: ProfileRequest, request: Request):
    org_id = _org(request)
    modules = await cfg_svc.get_ai_module_prompts(org_id)
    ov = _module_overrides(modules, "scores")
    try:
        return await svc.run_scores(_profile(body), **ov)
    except RuntimeError as e:
        raise HTTPException(500, str(e))


@router.post("/icp-match")
async def icp_match(body: ProfileRequest, request: Request):
    org_id = _org(request)
    modules = await cfg_svc.get_ai_module_prompts(org_id)
    ov = _module_overrides(modules, "icp_match")
    try:
        return await svc.run_icp_match(_profile(body), **ov)
    except RuntimeError as e:
        raise HTTPException(500, str(e))


@router.post("/behavioural-signals")
async def behavioural_signals(body: ProfileRequest, request: Request):
    org_id = _org(request)
    modules = await cfg_svc.get_ai_module_prompts(org_id)
    ov = _module_overrides(modules, "behavioural_signals")
    try:
        return await svc.run_behavioural_signals(_profile(body), **ov)
    except RuntimeError as e:
        raise HTTPException(500, str(e))


@router.post("/pitch-intelligence")
async def pitch_intelligence(body: ProfileRequest, request: Request):
    org_id = _org(request)
    modules = await cfg_svc.get_ai_module_prompts(org_id)
    ov = _module_overrides(modules, "pitch_intelligence")
    try:
        return await svc.run_pitch_intelligence(_profile(body), **ov)
    except RuntimeError as e:
        raise HTTPException(500, str(e))


@router.post("/activity")
async def activity(body: ProfileRequest, request: Request):
    org_id = _org(request)
    modules = await cfg_svc.get_ai_module_prompts(org_id)
    ov = _module_overrides(modules, "activity")
    try:
        return await svc.run_activity(_profile(body), **ov)
    except RuntimeError as e:
        raise HTTPException(500, str(e))


@router.post("/tags")
async def tags(body: ProfileRequest, request: Request):
    org_id = _org(request)
    modules = await cfg_svc.get_ai_module_prompts(org_id)
    ov = _module_overrides(modules, "tags")
    try:
        return await svc.run_tags(_profile(body), **ov)
    except RuntimeError as e:
        raise HTTPException(500, str(e))


@router.post("/outreach")
async def outreach(body: ProfileRequest, request: Request):
    org_id = _org(request)
    modules = await cfg_svc.get_ai_module_prompts(org_id)
    ov = _module_overrides(modules, "outreach")
    try:
        return await svc.run_outreach(_profile(body), **ov)
    except RuntimeError as e:
        raise HTTPException(500, str(e))


@router.post("/persona-analysis")
async def persona_analysis(body: ProfileRequest, request: Request):
    org_id = _org(request)
    modules = await cfg_svc.get_ai_module_prompts(org_id)
    ov = _module_overrides(modules, "persona_analysis")
    try:
        return await svc.run_persona_analysis(_profile(body), **ov)
    except RuntimeError as e:
        raise HTTPException(500, str(e))


@router.post("/meta", include_in_schema=False)
async def meta(body: ProfileRequest):
    return svc.run_meta()


@router.post("/full-enrichment", include_in_schema=False)
async def full_enrichment(body: ProfileRequest, request: Request):
    org_id = _org(request)
    modules = await cfg_svc.get_ai_module_prompts(org_id)
    try:
        return await svc.run_full_enrichment(_profile(body), module_configs=modules)
    except RuntimeError as e:
        raise HTTPException(500, str(e))
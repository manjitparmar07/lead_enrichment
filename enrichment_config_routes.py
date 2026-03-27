"""
enrichment_config_routes.py — Lead Enrichment Tool Configuration API
Per-org CRUD: tool enable/disable, API keys, credit management.

Endpoints:
  GET  /api/config/tools                           — list all tools + config + credits
  GET  /api/config/tools/{tool}                    — single tool detail
  PUT  /api/config/tools/{tool}                    — update enable/key/extra
  PUT  /api/config/tools/{tool}/credits            — set total credits (optionally reset used)
  POST /api/config/tools/{tool}/credits/add        — top-up credits
  GET  /api/config/tools/{tool}/credits/usage      — usage log for a tool
  GET  /api/config/credits                         — credit summary for all tools
  GET  /api/config/credits/usage                   — full usage log
  GET  /api/config/registry                        — static tool definitions
"""
import base64
import json
import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

import enrichment_config_service as cfg

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/config", tags=["Enrichment Config"])


# ── Org helper (mirrors pattern in lead_enrichment_brightdata_routes.py) ───────

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


# ── Request Models ─────────────────────────────────────────────────────────────

class ToolUpdate(BaseModel):
    is_enabled:   Optional[bool]           = None
    api_key:      Optional[str]            = None
    extra_config: Optional[Dict[str, Any]] = None


class CreditsSet(BaseModel):
    total_credits: int
    reset_used:    bool = False


class CreditsAdd(BaseModel):
    amount: int


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get("/tools")
async def list_tools(request: Request):
    """List all available tools with their config and credit status for the org."""
    return await cfg.get_full_config(_org(request))


@router.get("/tools/{tool_name}")
async def get_tool(tool_name: str, request: Request):
    """Get single tool config + credits."""
    if tool_name not in cfg.TOOL_REGISTRY:
        raise HTTPException(404, f"Unknown tool: {tool_name}")
    full = await cfg.get_full_config(_org(request))
    return next(t for t in full if t["tool_name"] == tool_name)


@router.put("/tools/{tool_name}")
async def update_tool(tool_name: str, body: ToolUpdate, request: Request):
    """Enable/disable a tool, update its API key, or update extra config fields."""
    if tool_name not in cfg.TOOL_REGISTRY:
        raise HTTPException(404, f"Unknown tool: {tool_name}")
    org_id   = _org(request)
    existing = await cfg.get_tool_row(org_id, tool_name)

    is_enabled = body.is_enabled if body.is_enabled is not None else (
        bool(existing["is_enabled"]) if existing else False
    )
    await cfg.upsert_tool_config(
        org_id, tool_name,
        is_enabled=is_enabled,
        api_key=body.api_key,
        extra_config=body.extra_config,
    )
    full = await cfg.get_full_config(org_id)
    return next(t for t in full if t["tool_name"] == tool_name)


@router.put("/tools/{tool_name}/credits")
async def set_tool_credits(tool_name: str, body: CreditsSet, request: Request):
    """Set total credits for a tool. Pass reset_used=true to reset consumed count."""
    if tool_name not in cfg.TOOL_REGISTRY:
        raise HTTPException(404, f"Unknown tool: {tool_name}")
    if body.total_credits < 0:
        raise HTTPException(400, "total_credits must be >= 0")
    org_id = _org(request)
    await cfg.set_credits(org_id, tool_name, body.total_credits, body.reset_used)
    return await cfg.get_credit_row(org_id, tool_name)


@router.post("/tools/{tool_name}/credits/add")
async def add_tool_credits(tool_name: str, body: CreditsAdd, request: Request):
    """Add to an existing credit balance (top-up)."""
    if tool_name not in cfg.TOOL_REGISTRY:
        raise HTTPException(404, f"Unknown tool: {tool_name}")
    if body.amount <= 0:
        raise HTTPException(400, "amount must be > 0")
    org_id = _org(request)
    await cfg.add_credits(org_id, tool_name, body.amount)
    return await cfg.get_credit_row(org_id, tool_name)


@router.get("/tools/{tool_name}/credits/usage")
async def tool_credit_usage(tool_name: str, request: Request):
    """Credit usage history for a specific tool."""
    if tool_name not in cfg.TOOL_REGISTRY:
        raise HTTPException(404, f"Unknown tool: {tool_name}")
    return await cfg.get_usage_log(_org(request), tool_name)


@router.get("/credits")
async def all_credits(request: Request):
    """Credit summary for all tools."""
    full = await cfg.get_full_config(_org(request))
    return [
        {
            "tool_name":         t["tool_name"],
            "label":             t["label"],
            "credit_unit":       t["credit_unit"],
            "total_credits":     t["total_credits"],
            "used_credits":      t["used_credits"],
            "remaining_credits": t["remaining_credits"],
            "credit_pct":        t["credit_pct"],
        }
        for t in full
    ]


@router.get("/credits/usage")
async def all_credit_usage(request: Request):
    """Full credit usage history across all tools."""
    return await cfg.get_usage_log(_org(request))


@router.get("/registry")
async def tool_registry():
    """Static tool definitions (available to all orgs)."""
    return cfg.TOOL_REGISTRY
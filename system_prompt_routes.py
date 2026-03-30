"""
system_prompt_routes.py
-----------------------
API endpoints for tenant system-prompt management.

GET    /api/v1/system-prompt/default                        — hardcoded default prompt
GET    /api/v1/system-prompt/generate?org_id=<id>           — assembled prompt (default + workspace + dynamic)
GET    /api/v1/system-prompt/prompts?org_id=<id>            — list all dynamic prompt rows
POST   /api/v1/system-prompt/prompts?org_id=<id>            — add a dynamic prompt section
PUT    /api/v1/system-prompt/prompts/{prompt_id}?org_id=<id> — update a dynamic prompt section
DELETE /api/v1/system-prompt/prompts/{prompt_id}?org_id=<id> — remove a dynamic prompt section

No auth required — org_id is passed as a query parameter.
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

import system_prompt_service as sps

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/v1/system-prompt",
    tags=["System Prompt"],
)


# ── Request / Response models ─────────────────────────────────────────────────

class PromptIn(BaseModel):
    name: str = Field(..., description="Human-readable label for this prompt section")
    key: str = Field(..., description="Unique section key, e.g. 'tone', 'restrictions', 'context'")
    content: str = Field(..., description="The prompt text for this section")
    is_active: Optional[bool] = Field(True, description="Whether this section is included during generation")
    priority: Optional[int] = Field(100, description="Lower priority = injected earlier. Default: 100")


class PromptUpdate(BaseModel):
    name: Optional[str] = None
    key: Optional[str] = None
    content: Optional[str] = None
    is_active: Optional[bool] = None
    priority: Optional[int] = None


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get(
    "/default",
    summary="Get Default System Prompt",
    description=(
        "Returns the hardcoded baseline system prompt used by the platform. "
        "This is always the foundation of every tenant's assembled prompt. No auth required."
    ),
)
async def get_default_prompt():
    return {
        "system_prompt": sps.DEFAULT_SYSTEM_PROMPT,
        "description": "Hardcoded platform baseline — always included in the generated prompt.",
    }


@router.get(
    "/generate",
    summary="Generate System Prompt",
    description=(
        "Assembles and returns the final system prompt for the given org.\n\n"
        "The prompt is built in three layers:\n"
        "1. **Default** — hardcoded platform baseline\n"
        "2. **Workspace** — tenant's ICP config (product, tone, target titles, etc.)\n"
        "3. **Dynamic** — active rows from the tenant's `system_prompts` table, ordered by priority\n\n"
        "Use the returned `system_prompt` string as-is when initialising your AI calls."
    ),
)
async def generate_prompt(org_id: str = Query(..., description="Your organisation ID")):
    result = await sps.generate_system_prompt(org_id)
    return result


@router.get(
    "/prompts",
    summary="List Dynamic Prompt Sections",
    description=(
        "Returns all dynamic prompt sections saved for the given org. "
        "Each section is injected into the generated system prompt in `priority` order."
    ),
)
async def list_prompts(org_id: str = Query(..., description="Your organisation ID")):
    prompts = await sps.list_prompts(org_id)
    return {"org_id": org_id, "count": len(prompts), "prompts": prompts}


@router.post(
    "/prompts",
    summary="Add Dynamic Prompt Section",
    description=(
        "Adds a new dynamic prompt section for the tenant. "
        "The section will be appended to the generated system prompt according to its `priority`.\n\n"
        "**Tip:** Use `key` as a stable identifier for upsert-like workflows "
        "(e.g. always overwrite `'tone'` instead of creating duplicates)."
    ),
    status_code=201,
)
async def create_prompt(body: PromptIn, org_id: str = Query(..., description="Your organisation ID")):
    created = await sps.create_prompt(org_id, body.model_dump())
    return created


@router.put(
    "/prompts/{prompt_id}",
    summary="Update Dynamic Prompt Section",
    description="Updates any field of an existing dynamic prompt section owned by the tenant.",
)
async def update_prompt(
    prompt_id: str,
    body: PromptUpdate,
    org_id: str = Query(..., description="Your organisation ID"),
):
    updated = await sps.update_prompt(org_id, prompt_id, body.model_dump())
    if updated is None:
        raise HTTPException(status_code=404, detail="Prompt not found")
    return updated


@router.delete(
    "/prompts/{prompt_id}",
    summary="Delete Dynamic Prompt Section",
    description="Permanently removes a dynamic prompt section from the tenant's configuration.",
)
async def delete_prompt(
    prompt_id: str,
    org_id: str = Query(..., description="Your organisation ID"),
):
    deleted = await sps.delete_prompt(org_id, prompt_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Prompt not found")
    return {"success": True, "deleted_id": prompt_id}

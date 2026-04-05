"""
system_prompt_generator_routes.py
------------------------------------
Two-phase API:

  Phase 1 — Scrape
    POST /api/prompt-generator/scrape
      body: { url }
      returns: { source_type, profile }

  Phase 2 — Generate prompts (SSE stream)
    POST /api/prompt-generator/generate
      body: { profile, source_type, sections? }
      streams SSE events per section

  Save
    POST /api/prompt-generator/save
      body: { section_key, section_label, content, name? }
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from Lead_enrichment.outreach_lead_enrichment import system_prompt_generator_service as spg_svc

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/prompt-generator", tags=["System Prompt Generator"])

_SSE_HEADERS = {
    "Cache-Control": "no-cache",
    "X-Accel-Buffering": "no",
}


def _get_org_id(request: Request) -> str:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return "default"
    token = auth[7:].strip()
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return "default"
        payload_b64 = parts[1] + "=" * (4 - len(parts[1]) % 4)
        payload = json.loads(base64.b64decode(payload_b64))
        return str(payload.get("organization_id", "default"))
    except Exception:
        return "default"


# ── Request models ─────────────────────────────────────────────────────────────

class ScrapeRequest(BaseModel):
    url: str


class GenerateRequest(BaseModel):
    profile: dict
    source_type: str
    sections: Optional[list[str]] = None  # None = all 10


class SavePromptRequest(BaseModel):
    section_key: str
    section_label: str
    content: str
    name: Optional[str] = None


# ── Phase 1: Scrape ────────────────────────────────────────────────────────────

@router.post("/scrape", include_in_schema=False)
async def scrape_url(body: ScrapeRequest):
    """
    Scrape a LinkedIn profile (via BrightData) or website (via httpx).
    Returns raw profile/website data the frontend will display then pass back for generation.
    """
    url = body.url.strip()
    if not url:
        raise HTTPException(status_code=400, detail="url is required")

    try:
        source_type, profile = await spg_svc.scrape_url(url)
    except Exception as e:
        logger.error("[SPG scrape] %s", e)
        raise HTTPException(status_code=502, detail=str(e))

    return {"source_type": source_type, "profile": profile}


# ── Phase 2: Generate prompts (single JSON) ───────────────────────────────────

@router.post("/generate", include_in_schema=False)
async def generate_prompts(body: GenerateRequest):
    """
    Generate all section prompts in parallel and return a single JSON object.

    Response:
      {
        "prompts": {
          "identity":            { "label": "Identity",           "prompt": "..." },
          "contact":             { "label": "Contact",            "prompt": "..." },
          "scores":              { "label": "Scores",             "prompt": "..." },
          "icp_match":           { "label": "ICP Match",          "prompt": "..." },
          "behavioural_signals": { "label": "Behavioural Signal", "prompt": "..." },
          "pitch_intelligence":  { "label": "Pitch Intelligence", "prompt": "..." },
          "activity":            { "label": "Activity",           "prompt": "..." },
          "tags":                { "label": "Tags",               "prompt": "..." },
          "outreach":            { "label": "Outreach",           "prompt": "..." },
          "person_analysis":     { "label": "Person Analysis",    "prompt": "..." }
        }
      }
    """
    profile   = body.profile
    requested = set(body.sections) if body.sections else None

    sections_to_run = [
        (key, label, fn)
        for key, label, fn in spg_svc.SECTIONS
        if not requested or key in requested
    ]

    async def _run(key: str, label: str, fn):
        try:
            text = await fn(profile)
            return key, {"label": label, "prompt": text, "error": None}
        except Exception as e:
            logger.error("[SPG generate] section %s: %s", key, e)
            return key, {"label": label, "prompt": None, "error": str(e)}

    results = await asyncio.gather(*[_run(k, l, f) for k, l, f in sections_to_run])
    prompts = {key: val for key, val in results}

    return {"prompts": prompts}


# ── Save a prompt ─────────────────────────────────────────────────────────────

@router.post("/save", include_in_schema=False)
async def save_as_prompt(body: SavePromptRequest, request: Request):
    """Save a generated section prompt to the system_prompts table."""
    org_id = _get_org_id(request)

    import uuid
    from datetime import datetime, timezone
    from db import get_pool

    now       = datetime.now(timezone.utc).isoformat()
    prompt_id = str(uuid.uuid4())
    name      = body.name or f"Generated: {body.section_label}"
    key       = f"spg_{body.section_key}"

    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO system_prompts
                (id, org_id, name, key, content, is_active, priority, created_at, updated_at)
            VALUES ($1,$2,$3,$4,$5,TRUE,100,$6,$6)
            ON CONFLICT (org_id, key) DO UPDATE
              SET content    = EXCLUDED.content,
                  name       = EXCLUDED.name,
                  updated_at = EXCLUDED.updated_at
            """,
            prompt_id, org_id, name, key, body.content, now,
        )

    return {"success": True, "id": prompt_id, "key": key}


# ── List saved prompts ────────────────────────────────────────────────────────

@router.get("/saved", include_in_schema=False)
async def list_saved_prompts(request: Request):
    """Return all spg_* prompts saved for this org."""
    org_id = _get_org_id(request)
    from db import get_pool
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, name, key, content, is_active, updated_at
            FROM system_prompts
            WHERE org_id=$1 AND key LIKE 'spg_%'
            ORDER BY updated_at DESC
            """,
            org_id,
        )
    return {
        "prompts": [
            {
                "id":         str(r["id"]),
                "name":       r["name"],
                "key":        r["key"],
                "section_key": r["key"].removeprefix("spg_"),
                "content":    r["content"],
                "is_active":  r["is_active"],
                "updated_at": str(r["updated_at"]),
            }
            for r in rows
        ]
    }

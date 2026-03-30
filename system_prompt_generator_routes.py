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

import base64
import json
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

import system_prompt_generator_service as spg_svc

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


# ── Phase 2: Generate prompts (SSE) ───────────────────────────────────────────

@router.post("/generate", include_in_schema=False)
async def generate_prompts(body: GenerateRequest):
    """
    Stream system prompt generation for each section as SSE.

    SSE events:
      {"status": "section_start",  "key": "...", "label": "..."}
      {"status": "section_done",   "key": "...", "label": "...", "prompt": "..."}
      {"status": "section_error",  "key": "...", "label": "...", "error": "..."}
      {"status": "complete", "done": true}
      {"status": "error",    "error": "...", "done": true}
    """
    profile  = body.profile
    requested = set(body.sections) if body.sections else None

    async def _stream():
        try:
            for key, label, fn in spg_svc.SECTIONS:
                if requested and key not in requested:
                    continue

                yield f"data: {json.dumps({'status': 'section_start', 'key': key, 'label': label})}\n\n"
                try:
                    prompt_text = await fn(profile)
                    yield f"data: {json.dumps({'status': 'section_done', 'key': key, 'label': label, 'prompt': prompt_text})}\n\n"
                except Exception as e:
                    logger.error("[SPG generate] section %s: %s", key, e)
                    yield f"data: {json.dumps({'status': 'section_error', 'key': key, 'label': label, 'error': str(e)})}\n\n"

            yield f"data: {json.dumps({'status': 'complete', 'done': True})}\n\n"

        except Exception as e:
            logger.error("[SPG generate] stream error: %s", e)
            yield f"data: {json.dumps({'status': 'error', 'error': str(e), 'done': True})}\n\n"

    return StreamingResponse(_stream(), media_type="text/event-stream", headers=_SSE_HEADERS)


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

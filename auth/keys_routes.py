"""
keys_routes.py — Admin API for centralized key management

Endpoints (all under /api/admin/keys):
  GET    /api/admin/keys                       — list all sections + masked values
  GET    /api/admin/keys/{section}/{key}/reveal — reveal raw value (sensitive, log-audited)
  PUT    /api/admin/keys/{section}/{key}        — update a key value
  POST   /api/admin/keys/{section}              — add a new key to a section
  DELETE /api/admin/keys/{section}/{key}        — delete a key
  POST   /api/admin/keys/reload                 — hot-reload from disk + sync to env

Security: protected by the same WB_API_KEY middleware as all other /api/* routes.
Keys are NEVER passed in by API callers — this is purely admin management.
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from auth import keys_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/keys", tags=["Admin – Key Management"])


# ── Schemas ────────────────────────────────────────────────────────────────────

class UpdateKeyRequest(BaseModel):
    value: str


class AddKeyRequest(BaseModel):
    key_name: str
    value: str
    label: str = ""
    description: str = ""
    sensitive: bool = True


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.get("", summary="List all key sections with masked values")
async def list_keys():
    """
    Returns all key sections from system_keys.json.
    Sensitive values are masked (first 4 + last 4 chars visible).
    Non-sensitive values are shown in full.
    """
    return {
        "sections": keys_service.get_all_sections(),
        "message": "Sensitive values are masked. Use /reveal to see the full value.",
    }


@router.get("/{section}/{key_name}/reveal", summary="Reveal raw value of a key")
async def reveal_key(section: str, key_name: str):
    """
    Returns the unmasked value for a specific key.
    Every call is logged for audit purposes.
    """
    value = keys_service.reveal_key(section, key_name)
    if value is None:
        raise HTTPException(status_code=404, detail=f"Key '{key_name}' not found in section '{section}'")
    logger.warning(f"[KeysAdmin] REVEAL requested for {section}/{key_name}")
    return {"section": section, "key_name": key_name, "value": value}


@router.put("/{section}/{key_name}", summary="Update a key value")
async def update_key(section: str, key_name: str, body: UpdateKeyRequest):
    """
    Update the value of an existing key. Immediately syncs to os.environ
    so running services pick up the new value without restart.
    """
    ok = keys_service.update_key(section, key_name, body.value)
    if not ok:
        raise HTTPException(
            status_code=404,
            detail=f"Key '{key_name}' not found in section '{section}'. Use POST to add a new key.",
        )
    return {"ok": True, "message": f"Key '{key_name}' updated successfully. Hot-reloaded into runtime."}


@router.post("/{section}", summary="Add a new key to a section")
async def add_key(section: str, body: AddKeyRequest):
    """
    Add a brand-new key to an existing section.
    Returns 409 if the key already exists (use PUT to update).
    """
    ok = keys_service.add_key(
        section=section,
        key_name=body.key_name,
        value=body.value,
        label=body.label,
        description=body.description,
        sensitive=body.sensitive,
    )
    if not ok:
        # Check if section exists or key already exists
        sections = keys_service.get_all_sections()
        if section not in sections:
            raise HTTPException(status_code=404, detail=f"Section '{section}' not found.")
        raise HTTPException(
            status_code=409,
            detail=f"Key '{body.key_name}' already exists in section '{section}'. Use PUT to update.",
        )
    return {"ok": True, "message": f"Key '{body.key_name}' added to section '{section}'."}


@router.delete("/{section}/{key_name}", summary="Delete a key")
async def delete_key(section: str, key_name: str):
    """
    Permanently removes a key from the JSON store and from os.environ.
    """
    ok = keys_service.delete_key(section, key_name)
    if not ok:
        raise HTTPException(status_code=404, detail=f"Key '{key_name}' not found in section '{section}'")
    return {"ok": True, "message": f"Key '{key_name}' deleted from section '{section}'."}


@router.post("/reload", summary="Hot-reload keys from disk")
async def reload_keys():
    """
    Re-reads system_keys.json from disk and pushes all values to os.environ.
    Use this after manually editing the JSON file.
    """
    keys_service.reload()
    sections = keys_service.get_all_sections()
    total = sum(len(s["keys"]) for s in sections.values())
    return {"ok": True, "message": f"Reloaded {total} keys from system_keys.json."}

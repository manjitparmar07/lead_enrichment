"""
workspace_service.py
---------------------
Per-workspace (tenant) ICP and pitch configuration.

Each org in Lio defines their own:
  - product name + value proposition
  - target titles, industries, company sizes
  - outreach tone and CTA style
  - banned phrases and case studies

The AI analysis layer reads this config to personalise every generated
tag, signal, pitch intelligence output, and outreach message for that
workspace's specific product and ICP.

Table: workspace_configs
  org_id           TEXT PRIMARY KEY
  product_name     TEXT
  value_proposition TEXT
  target_titles    TEXT  (JSON array)
  target_industries TEXT (JSON array)
  company_size_min INTEGER
  company_size_max INTEGER
  tone             TEXT  (peer / formal / casual)
  cta_style        TEXT  (question / demo / resource / short_ask)
  banned_phrases   TEXT  (JSON array)
  case_studies     TEXT  (JSON array of strings)
  created_at       TEXT
  updated_at       TEXT
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

from db import get_pool

logger = logging.getLogger(__name__)


# ── Schema ────────────────────────────────────────────────────────────────────

async def init_workspace_db() -> None:
    async with get_pool().acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS workspace_configs (
                org_id            TEXT PRIMARY KEY,
                product_name      TEXT,
                value_proposition TEXT,
                target_titles     TEXT DEFAULT '[]',
                target_industries TEXT DEFAULT '[]',
                company_size_min  INTEGER DEFAULT 0,
                company_size_max  INTEGER DEFAULT 0,
                tone              TEXT DEFAULT 'peer',
                cta_style         TEXT DEFAULT 'question',
                banned_phrases    TEXT DEFAULT '[]',
                case_studies      TEXT DEFAULT '[]',
                created_at        TEXT DEFAULT '',
                updated_at        TEXT DEFAULT ''
            )
        """)
    logger.info("[WorkspaceService] DB ready")


# ── CRUD ─────────────────────────────────────────────────────────────────────

async def get_workspace_config(org_id: str) -> dict:
    """
    Return workspace config for org_id.
    Returns a default config dict (not saved) if none exists yet —
    so AI analysis always gets something sensible.
    """
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM workspace_configs WHERE org_id=$1", org_id
        )

    if row:
        cfg = dict(row)
        # Deserialise JSON arrays
        for k in ("target_titles", "target_industries", "banned_phrases", "case_studies"):
            try:
                cfg[k] = json.loads(cfg[k] or "[]")
            except Exception:
                cfg[k] = []
        return cfg

    # Default config — used by AI analysis when workspace hasn't configured yet
    return {
        "org_id":            org_id,
        "product_name":      "",
        "value_proposition": "",
        "target_titles":     ["CEO", "CTO", "VP Engineering", "Head of Product", "Founder"],
        "target_industries": ["SaaS", "Fintech", "EdTech", "AI", "B2B Software"],
        "company_size_min":  10,
        "company_size_max":  500,
        "tone":              "peer",
        "cta_style":         "question",
        "banned_phrases":    ["synergy", "game-changer", "innovative solution", "disruptive"],
        "case_studies":      [],
    }


async def upsert_workspace_config(org_id: str, data: dict) -> dict:
    """Create or update workspace config. Returns saved record."""
    from db import named_args
    now = datetime.now(timezone.utc).isoformat()

    # Serialise JSON arrays
    for k in ("target_titles", "target_industries", "banned_phrases", "case_studies"):
        if k in data and isinstance(data[k], list):
            data[k] = json.dumps(data[k])

    async with get_pool().acquire() as conn:
        exists = await conn.fetchrow(
            "SELECT org_id FROM workspace_configs WHERE org_id=$1", org_id
        )
        if exists:
            update_data = {k: v for k, v in data.items() if k != "org_id"}
            update_data["updated_at"] = now
            update_data["org_id"] = org_id
            sets = ", ".join(f"{k}=:{k}" for k in update_data if k != "org_id")
            sql, args = named_args(
                f"UPDATE workspace_configs SET {sets}, updated_at=:updated_at WHERE org_id=:org_id",
                update_data,
            )
            await conn.execute(sql, *args)
        else:
            data["org_id"]     = org_id
            data["created_at"] = now
            data["updated_at"] = now
            cols = list(data.keys())
            placeholders = ", ".join(f"${i + 1}" for i in range(len(cols)))
            args = [data[k] for k in cols]
            await conn.execute(
                f"INSERT INTO workspace_configs ({', '.join(cols)}) VALUES ({placeholders})",
                *args,
            )

    logger.info("[WorkspaceService] Config upserted for org=%s", org_id)
    return await get_workspace_config(org_id)


async def delete_workspace_config(org_id: str) -> bool:
    async with get_pool().acquire() as conn:
        await conn.execute("DELETE FROM workspace_configs WHERE org_id=$1", org_id)
    return True

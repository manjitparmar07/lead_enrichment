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

import aiosqlite

logger  = logging.getLogger(__name__)
_DB_DIR = os.path.join(os.path.dirname(__file__), "configs")
WS_DB   = os.path.join(_DB_DIR, "leads_enrichment.db")   # same DB as leads


# ── Schema ────────────────────────────────────────────────────────────────────

async def init_workspace_db() -> None:
    async with aiosqlite.connect(WS_DB) as db:
        await db.execute("""
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
                created_at        TEXT DEFAULT (datetime('now')),
                updated_at        TEXT DEFAULT (datetime('now'))
            )
        """)
        await db.commit()
    logger.info("[WorkspaceService] DB ready")


# ── CRUD ─────────────────────────────────────────────────────────────────────

async def get_workspace_config(org_id: str) -> dict:
    """
    Return workspace config for org_id.
    Returns a default config dict (not saved) if none exists yet —
    so AI analysis always gets something sensible.
    """
    async with aiosqlite.connect(WS_DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM workspace_configs WHERE org_id=?", (org_id,)
        ) as cur:
            row = await cur.fetchone()

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
    now = datetime.now(timezone.utc).isoformat()

    # Serialise JSON arrays
    for k in ("target_titles", "target_industries", "banned_phrases", "case_studies"):
        if k in data and isinstance(data[k], list):
            data[k] = json.dumps(data[k])

    async with aiosqlite.connect(WS_DB) as db:
        # Check if exists
        async with db.execute(
            "SELECT org_id FROM workspace_configs WHERE org_id=?", (org_id,)
        ) as cur:
            exists = await cur.fetchone()

        if exists:
            sets = ", ".join(f"{k}=:{k}" for k in data if k != "org_id")
            data["org_id"] = org_id
            data["updated_at"] = now
            await db.execute(
                f"UPDATE workspace_configs SET {sets}, updated_at=:updated_at WHERE org_id=:org_id",
                data,
            )
        else:
            data["org_id"]     = org_id
            data["created_at"] = now
            data["updated_at"] = now
            cols         = ", ".join(data.keys())
            placeholders = ", ".join(f":{k}" for k in data.keys())
            await db.execute(
                f"INSERT INTO workspace_configs ({cols}) VALUES ({placeholders})", data
            )
        await db.commit()

    logger.info("[WorkspaceService] Config upserted for org=%s", org_id)
    return await get_workspace_config(org_id)


async def delete_workspace_config(org_id: str) -> bool:
    async with aiosqlite.connect(WS_DB) as db:
        await db.execute("DELETE FROM workspace_configs WHERE org_id=?", (org_id,))
        await db.commit()
    return True

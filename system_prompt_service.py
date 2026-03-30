"""
system_prompt_service.py
------------------------
Manages the system prompt pipeline for each tenant.

A tenant's final system prompt is assembled in three layers:
  1. DEFAULT_SYSTEM_PROMPT  — hardcoded baseline, always present
  2. Workspace config       — personalises {product_name}, {tone}, ICP, etc.
  3. Dynamic prompts        — rows in system_prompts table, ordered by priority
                              Each row adds or overrides a named section.

Table: system_prompts
  id          TEXT PRIMARY KEY
  org_id      TEXT NOT NULL
  name        TEXT NOT NULL     — human-readable label
  key         TEXT NOT NULL     — section key (e.g. "context", "tone", "restrictions")
  content     TEXT NOT NULL     — prompt text for this section
  is_active   BOOLEAN           — false = skip during generation
  priority    INTEGER           — lower = injected earlier in the prompt
  created_at  TEXT
  updated_at  TEXT
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from db import get_pool, named_args
import workspace_service as ws

logger = logging.getLogger(__name__)

# ── Default system prompt ─────────────────────────────────────────────────────

DEFAULT_SYSTEM_PROMPT = """\
You are an AI-powered lead intelligence assistant for the WorksBuddy Lead Enrichment platform.

Your role is to help sales and marketing teams understand, score, and engage with their leads effectively.

## Core Capabilities
- Analyse LinkedIn profiles and professional data to extract actionable insights
- Score lead quality against the tenant's Ideal Customer Profile (ICP)
- Detect behavioural signals and buying intent from professional activity
- Generate personalised, context-aware outreach messages (cold email, LinkedIn note)
- Summarise company intelligence: market position, tech stack, growth signals

## Behaviour Guidelines
- Be accurate, concise, and professional in all responses
- Ground every insight in the provided lead or company data — never fabricate details
- When data is missing or unclear, say so rather than guessing
- Respect the tenant's configured tone and banned phrases
- Prioritise actionability: every output should help the user take a clear next step

## Output Format
- Use structured output (JSON, markdown tables, or labelled sections) unless a plain answer is more appropriate
- Keep outreach copy under 150 words unless instructed otherwise
- Include a confidence indicator (High / Medium / Low) when scoring or inferring intent\
"""


# ── Schema ────────────────────────────────────────────────────────────────────

async def init_system_prompts_db() -> None:
    async with get_pool().acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS system_prompts (
                id          TEXT PRIMARY KEY,
                org_id      TEXT NOT NULL,
                name        TEXT NOT NULL,
                key         TEXT NOT NULL,
                content     TEXT NOT NULL,
                is_active   BOOLEAN DEFAULT TRUE,
                priority    INTEGER DEFAULT 100,
                created_at  TEXT DEFAULT '',
                updated_at  TEXT DEFAULT ''
            )
        """)
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_system_prompts_org ON system_prompts(org_id)"
        )
    logger.info("[SystemPromptService] DB ready")


# ── CRUD ──────────────────────────────────────────────────────────────────────

async def list_prompts(org_id: str) -> list[dict]:
    """Return all prompt rows for the org, ordered by priority."""
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM system_prompts WHERE org_id=$1 ORDER BY priority ASC, created_at ASC",
            org_id,
        )
    return [dict(r) for r in rows]


async def get_prompt(org_id: str, prompt_id: str) -> Optional[dict]:
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM system_prompts WHERE id=$1 AND org_id=$2",
            prompt_id, org_id,
        )
    return dict(row) if row else None


async def create_prompt(org_id: str, data: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    row = {
        "id":         str(uuid.uuid4()),
        "org_id":     org_id,
        "name":       data["name"],
        "key":        data["key"],
        "content":    data["content"],
        "is_active":  data.get("is_active", True),
        "priority":   data.get("priority", 100),
        "created_at": now,
        "updated_at": now,
    }
    cols = list(row.keys())
    placeholders = ", ".join(f"${i + 1}" for i in range(len(cols)))
    args = [row[k] for k in cols]
    async with get_pool().acquire() as conn:
        await conn.execute(
            f"INSERT INTO system_prompts ({', '.join(cols)}) VALUES ({placeholders})",
            *args,
        )
    logger.info("[SystemPromptService] Created prompt id=%s org=%s", row["id"], org_id)
    return row


async def update_prompt(org_id: str, prompt_id: str, data: dict) -> Optional[dict]:
    existing = await get_prompt(org_id, prompt_id)
    if not existing:
        return None

    allowed = {"name", "key", "content", "is_active", "priority"}
    update = {k: v for k, v in data.items() if k in allowed and v is not None}
    if not update:
        return existing

    update["updated_at"] = datetime.now(timezone.utc).isoformat()
    update["id"] = prompt_id
    update["org_id"] = org_id

    sets = ", ".join(f"{k}=:{k}" for k in update if k not in ("id", "org_id"))
    sql, args = named_args(
        f"UPDATE system_prompts SET {sets} WHERE id=:id AND org_id=:org_id",
        update,
    )
    async with get_pool().acquire() as conn:
        await conn.execute(sql, *args)
    return await get_prompt(org_id, prompt_id)


async def delete_prompt(org_id: str, prompt_id: str) -> bool:
    async with get_pool().acquire() as conn:
        result = await conn.execute(
            "DELETE FROM system_prompts WHERE id=$1 AND org_id=$2",
            prompt_id, org_id,
        )
    deleted = result != "DELETE 0"
    if deleted:
        logger.info("[SystemPromptService] Deleted prompt id=%s org=%s", prompt_id, org_id)
    return deleted


# ── Generation ────────────────────────────────────────────────────────────────

async def generate_system_prompt(org_id: str) -> dict:
    """
    Assemble the final system prompt for the tenant.

    Returns:
        {
          "system_prompt": "<assembled text>",
          "layers": {
            "default": "...",
            "workspace": "...",      # injected context from workspace_configs
            "dynamic": [...]         # active rows from system_prompts, in priority order
          }
        }
    """
    # Layer 1: default
    default_text = DEFAULT_SYSTEM_PROMPT

    # Layer 2: workspace config personalisation
    cfg = await ws.get_workspace_config(org_id)
    workspace_section = _build_workspace_section(cfg)

    # Layer 3: dynamic prompts (active only, ordered by priority)
    dynamic_rows = [r for r in await list_prompts(org_id) if r["is_active"]]
    dynamic_sections = [r["content"] for r in dynamic_rows]

    # Assemble
    parts = [default_text]
    if workspace_section:
        parts.append(workspace_section)
    parts.extend(dynamic_sections)

    assembled = "\n\n---\n\n".join(parts)

    return {
        "system_prompt": assembled,
        "layers": {
            "default":   default_text,
            "workspace": workspace_section,
            "dynamic":   [
                {"id": r["id"], "name": r["name"], "key": r["key"], "content": r["content"], "priority": r["priority"]}
                for r in dynamic_rows
            ],
        },
    }


def _build_workspace_section(cfg: dict) -> str:
    """Turn workspace config into an injectable prompt section."""
    lines = ["## Tenant Configuration"]

    if cfg.get("product_name"):
        lines.append(f"- **Product:** {cfg['product_name']}")
    if cfg.get("value_proposition"):
        lines.append(f"- **Value proposition:** {cfg['value_proposition']}")

    titles = cfg.get("target_titles") or []
    if titles:
        lines.append(f"- **Target titles:** {', '.join(titles)}")

    industries = cfg.get("target_industries") or []
    if industries:
        lines.append(f"- **Target industries:** {', '.join(industries)}")

    size_min = cfg.get("company_size_min", 0)
    size_max = cfg.get("company_size_max", 0)
    if size_min or size_max:
        lines.append(f"- **Target company size:** {size_min}–{size_max} employees")

    tone = cfg.get("tone", "peer")
    lines.append(f"- **Tone:** {tone}")

    cta = cfg.get("cta_style", "question")
    lines.append(f"- **CTA style:** {cta}")

    banned = cfg.get("banned_phrases") or []
    if banned:
        lines.append(f"- **Banned phrases (never use):** {', '.join(banned)}")

    case_studies = cfg.get("case_studies") or []
    if case_studies:
        lines.append("- **Available case studies:**")
        for cs in case_studies:
            lines.append(f"  - {cs}")

    # Only return section if there's real content beyond the header
    if len(lines) <= 1:
        return ""
    return "\n".join(lines)

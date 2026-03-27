"""
enrichment_config_service.py — Lead Enrichment Tool Configuration + Credits
Manages per-org: tool enable/disable, API keys, credit allocation + usage tracking.
"""
import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aiosqlite

_DB_DIR  = os.path.join(os.path.dirname(__file__), "configs")
LEADS_DB = os.path.join(_DB_DIR, "leads_enrichment.db")

# ── Tool Registry ──────────────────────────────────────────────────────────────

TOOL_REGISTRY: Dict[str, Dict] = {
    "brightdata": {
        "label":       "Bright Data",
        "description": "LinkedIn profile & company enrichment — primary data source",
        "category":    "enrichment",
        "credit_unit": "profile lookup",
        "config_fields": [
            {"key": "api_key",            "label": "API Key",            "sensitive": True,  "placeholder": "Enter Bright Data API key"},
            {"key": "profile_dataset_id", "label": "Profile Dataset ID", "sensitive": False, "default": "gd_l1viktl72bvl7bjuj0"},
            {"key": "company_dataset_id", "label": "Company Dataset ID", "sensitive": False, "default": "gd_l1vikfnt1wgvvqz95w"},
        ],
    },
    "hunter": {
        "label":       "Hunter.io",
        "description": "Email discovery and domain verification",
        "category":    "email",
        "credit_unit": "email search",
        "config_fields": [
            {"key": "api_key", "label": "API Key", "sensitive": True, "placeholder": "Enter Hunter.io API key"},
        ],
    },
    "apollo": {
        "label":       "Apollo.io",
        "description": "Email discovery and B2B contact data",
        "category":    "email",
        "credit_unit": "email search",
        "config_fields": [
            {"key": "api_key", "label": "API Key", "sensitive": True, "placeholder": "Enter Apollo.io API key"},
        ],
    },
    "groq": {
        "label":       "Groq LLM",
        "description": "Fast AI inference — scoring & outreach generation (fallback LLM)",
        "category":    "ai",
        "credit_unit": "LLM call",
        "config_fields": [
            {"key": "api_key", "label": "API Key",    "sensitive": True,  "placeholder": "Enter Groq API key"},
            {"key": "model",   "label": "Model Name", "sensitive": False, "default": "llama-3.3-70b-versatile"},
        ],
    },
    "wb_llm": {
        "label":       "WorksBuddy LLM",
        "description": "Internal hosted LLM — primary AI engine for enrichment",
        "category":    "ai",
        "credit_unit": "LLM call",
        "config_fields": [
            {"key": "host",    "label": "Host URL",           "sensitive": False, "default": "http://ai-llm.worksbuddy.ai"},
            {"key": "api_key", "label": "API Key (optional)", "sensitive": True,  "placeholder": "Leave empty if not required"},
            {"key": "model",   "label": "Default Model",      "sensitive": False, "default": "wb-pro"},
        ],
    },
    "ably": {
        "label":       "Ably Realtime",
        "description": "Live progress updates during bulk enrichment",
        "category":    "realtime",
        "credit_unit": "message",
        "config_fields": [
            {"key": "api_key", "label": "API Key", "sensitive": True, "placeholder": "appId.keyId:keySecret"},
        ],
    },
}


# ── DB Init ────────────────────────────────────────────────────────────────────

async def init_config_db() -> None:
    async with aiosqlite.connect(LEADS_DB) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS enrichment_tool_configs (
                id           TEXT PRIMARY KEY,
                org_id       TEXT NOT NULL,
                tool_name    TEXT NOT NULL,
                is_enabled   INTEGER NOT NULL DEFAULT 1,
                api_key      TEXT,
                extra_config TEXT,
                created_at   TEXT NOT NULL,
                updated_at   TEXT NOT NULL,
                UNIQUE(org_id, tool_name)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS enrichment_tool_credits (
                id            TEXT PRIMARY KEY,
                org_id        TEXT NOT NULL,
                tool_name     TEXT NOT NULL,
                total_credits INTEGER NOT NULL DEFAULT 0,
                used_credits  INTEGER NOT NULL DEFAULT 0,
                reset_at      TEXT,
                created_at    TEXT NOT NULL,
                updated_at    TEXT NOT NULL,
                UNIQUE(org_id, tool_name)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS enrichment_credit_usage_log (
                id               TEXT PRIMARY KEY,
                org_id           TEXT NOT NULL,
                tool_name        TEXT NOT NULL,
                credits_deducted INTEGER NOT NULL,
                lead_id          TEXT,
                job_id           TEXT,
                reason           TEXT,
                created_at       TEXT NOT NULL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS workspace_configs (
                org_id            TEXT PRIMARY KEY,
                lio_system_prompt TEXT DEFAULT '',
                lio_model         TEXT DEFAULT '',
                updated_at        TEXT NOT NULL DEFAULT ''
            )
        """)
        # Migrate existing DBs that may be missing new columns
        for col, default in [
            ("lio_system_prompt", "''"), ("lio_model", "''"),
            ("lio_prompts_json", "''"), ("workspace_context_json", "''"),
            ("ai_prompts_json", "''"),
        ]:
            try:
                await db.execute(f"ALTER TABLE workspace_configs ADD COLUMN {col} TEXT DEFAULT {default}")
            except Exception:
                pass  # Column already exists — ignore
        await db.execute("CREATE INDEX IF NOT EXISTS idx_tool_cfg_org  ON enrichment_tool_configs(org_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_credits_org   ON enrichment_tool_credits(org_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_usage_log_org ON enrichment_credit_usage_log(org_id, tool_name)")
        await db.commit()


# ── Helpers ────────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _mask(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 8:
        return "••••••••"
    return value[:4] + "••••••••" + value[-4:]


# ── Tool Config CRUD ───────────────────────────────────────────────────────────

async def get_tool_row(org_id: str, tool_name: str) -> Optional[dict]:
    async with aiosqlite.connect(LEADS_DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM enrichment_tool_configs WHERE org_id=? AND tool_name=?",
            (org_id, tool_name),
        ) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None


async def list_tool_rows(org_id: str) -> List[dict]:
    async with aiosqlite.connect(LEADS_DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM enrichment_tool_configs WHERE org_id=? ORDER BY tool_name",
            (org_id,),
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def upsert_tool_config(
    org_id:       str,
    tool_name:    str,
    is_enabled:   bool,
    api_key:      Optional[str] = None,
    extra_config: Optional[dict] = None,
) -> None:
    now      = _now()
    existing = await get_tool_row(org_id, tool_name)
    row_id   = existing["id"] if existing else str(uuid.uuid4())

    final_key   = api_key      if api_key      is not None else (existing["api_key"]      if existing else None)
    final_extra = json.dumps(extra_config) if extra_config is not None else (
        existing["extra_config"] if existing else None
    )

    async with aiosqlite.connect(LEADS_DB) as db:
        await db.execute("""
            INSERT INTO enrichment_tool_configs
                (id, org_id, tool_name, is_enabled, api_key, extra_config, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(org_id, tool_name) DO UPDATE SET
                is_enabled   = excluded.is_enabled,
                api_key      = excluded.api_key,
                extra_config = excluded.extra_config,
                updated_at   = excluded.updated_at
        """, (row_id, org_id, tool_name, 1 if is_enabled else 0, final_key, final_extra, now, now))
        await db.commit()

    # Sync known keys to keys_service so ai_enrichment_service picks them up
    _TOOL_KEY_MAP = {
        "groq":       ("llm",             "GROQ_API_KEY"),
        "brightdata": ("lead_enrichment", "BRIGHT_DATA_API_KEY"),
        "hunter":     ("lead_enrichment", "HUNTER_API_KEY"),
        "apollo":     ("lead_enrichment", "APOLLO_API_KEY"),
    }
    if final_key and tool_name in _TOOL_KEY_MAP:
        try:
            import keys_service as ks
            section, key_name = _TOOL_KEY_MAP[tool_name]
            ks.update_key(section, key_name, final_key)
        except Exception:
            pass  # keys_service unavailable — os.environ fallback still works
    if final_key and tool_name == "groq":
        os.environ["GROQ_API_KEY"] = final_key
    # Sync Groq model name if provided in extra_config
    if tool_name == "groq" and final_extra:
        try:
            ec = json.loads(final_extra) if isinstance(final_extra, str) else final_extra
            model = ec.get("model", "") if isinstance(ec, dict) else ""
            if model:
                try:
                    import keys_service as ks
                    ks.update_key("llm", "GROQ_LLM_MODEL", model)
                except Exception:
                    pass
                os.environ["GROQ_LLM_MODEL"] = model
        except Exception:
            pass


# ── Credits CRUD ───────────────────────────────────────────────────────────────

async def get_credit_row(org_id: str, tool_name: str) -> Optional[dict]:
    async with aiosqlite.connect(LEADS_DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM enrichment_tool_credits WHERE org_id=? AND tool_name=?",
            (org_id, tool_name),
        ) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None


async def list_credit_rows(org_id: str) -> List[dict]:
    async with aiosqlite.connect(LEADS_DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM enrichment_tool_credits WHERE org_id=? ORDER BY tool_name",
            (org_id,),
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def set_credits(org_id: str, tool_name: str, total: int, reset_used: bool = False) -> None:
    now      = _now()
    existing = await get_credit_row(org_id, tool_name)
    row_id   = existing["id"] if existing else str(uuid.uuid4())
    used     = 0 if reset_used else (existing["used_credits"] if existing else 0)

    async with aiosqlite.connect(LEADS_DB) as db:
        await db.execute("""
            INSERT INTO enrichment_tool_credits
                (id, org_id, tool_name, total_credits, used_credits, reset_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(org_id, tool_name) DO UPDATE SET
                total_credits = excluded.total_credits,
                used_credits  = excluded.used_credits,
                reset_at      = excluded.reset_at,
                updated_at    = excluded.updated_at
        """, (row_id, org_id, tool_name, total, used, now if reset_used else None, now, now))
        await db.commit()


async def add_credits(org_id: str, tool_name: str, amount: int) -> None:
    existing = await get_credit_row(org_id, tool_name)
    if not existing:
        await set_credits(org_id, tool_name, amount)
        return
    async with aiosqlite.connect(LEADS_DB) as db:
        await db.execute(
            "UPDATE enrichment_tool_credits SET total_credits=total_credits+?, updated_at=? WHERE org_id=? AND tool_name=?",
            (amount, _now(), org_id, tool_name),
        )
        await db.commit()


async def deduct_credit(
    org_id:    str,
    tool_name: str,
    amount:    int  = 1,
    lead_id:   Optional[str] = None,
    job_id:    Optional[str] = None,
    reason:    Optional[str] = None,
) -> bool:
    """Deduct credits. Returns True if OK (or no limit set), False if insufficient."""
    row = await get_credit_row(org_id, tool_name)
    if not row or row["total_credits"] == 0:
        return True  # No credit limit configured → always allow

    remaining = row["total_credits"] - row["used_credits"]
    if remaining < amount:
        return False

    now = _now()
    async with aiosqlite.connect(LEADS_DB) as db:
        await db.execute(
            "UPDATE enrichment_tool_credits SET used_credits=used_credits+?, updated_at=? WHERE org_id=? AND tool_name=?",
            (amount, now, org_id, tool_name),
        )
        await db.execute(
            """INSERT INTO enrichment_credit_usage_log
               (id, org_id, tool_name, credits_deducted, lead_id, job_id, reason, created_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            (str(uuid.uuid4()), org_id, tool_name, amount, lead_id, job_id, reason, now),
        )
        await db.commit()
    return True


async def is_tool_available(org_id: str, tool_name: str) -> bool:
    """True if tool is enabled AND has credits (or no limit set)."""
    cfg = await get_tool_row(org_id, tool_name)
    if cfg is not None and not cfg["is_enabled"]:
        return False
    row = await get_credit_row(org_id, tool_name)
    if not row or row["total_credits"] == 0:
        return True  # No limit
    return (row["total_credits"] - row["used_credits"]) > 0


async def get_available_tools(org_id: str) -> Dict[str, bool]:
    """Availability map for all registered tools."""
    result = {}
    for tool in TOOL_REGISTRY:
        result[tool] = await is_tool_available(org_id, tool)
    return result


# ── Credit Usage Log ───────────────────────────────────────────────────────────

async def get_usage_log(
    org_id: str, tool_name: Optional[str] = None, limit: int = 50
) -> List[dict]:
    async with aiosqlite.connect(LEADS_DB) as db:
        db.row_factory = aiosqlite.Row
        if tool_name:
            sql    = "SELECT * FROM enrichment_credit_usage_log WHERE org_id=? AND tool_name=? ORDER BY created_at DESC LIMIT ?"
            params = (org_id, tool_name, limit)
        else:
            sql    = "SELECT * FROM enrichment_credit_usage_log WHERE org_id=? ORDER BY created_at DESC LIMIT ?"
            params = (org_id, limit)
        async with db.execute(sql, params) as cur:
            return [dict(r) for r in await cur.fetchall()]


# ── Combined Full Config View ──────────────────────────────────────────────────

async def get_full_config(org_id: str) -> List[dict]:
    """All tools from registry merged with per-org config + credit data."""
    cfgs    = {c["tool_name"]: c for c in await list_tool_rows(org_id)}
    credits = {c["tool_name"]: c for c in await list_credit_rows(org_id)}

    result = []
    for tool_name, tdef in TOOL_REGISTRY.items():
        cfg = cfgs.get(tool_name, {})
        crd = credits.get(tool_name, {})

        api_raw = cfg.get("api_key") or ""
        extra   = {}
        try:
            extra = json.loads(cfg["extra_config"]) if cfg.get("extra_config") else {}
        except Exception:
            pass

        total     = crd.get("total_credits", 0)
        used      = crd.get("used_credits",  0)
        remaining = total - used

        result.append({
            "tool_name":         tool_name,
            "label":             tdef["label"],
            "description":       tdef["description"],
            "category":          tdef["category"],
            "credit_unit":       tdef["credit_unit"],
            "config_fields":     tdef["config_fields"],
            # per-org state
            "is_enabled":        bool(cfg.get("is_enabled", 0)) if cfg else False,
            "is_configured":     bool(api_raw),
            "api_key_masked":    _mask(api_raw) if api_raw else None,
            "extra_config":      extra,
            # credits
            "total_credits":     total,
            "used_credits":      used,
            "remaining_credits": remaining,
            "credit_pct":        round(remaining / total * 100) if total > 0 else 0,
        })

    return result

# ── LIO 5-Prompt defaults ──────────────────────────────────────────────────────

_STRICT = (
    "STRICT RULES:\n"
    "- Return ONLY valid JSON (no markdown, no explanation)\n"
    "- No assumptions beyond given data. If a field is missing, use null.\n"
    "- Keep values short and structured. No buzzwords unless present in input."
)

# Bump this string whenever DEFAULT_LIO_PROMPTS change — forces DB reset on next load
_PROMPT_VERSION = "v3"

DEFAULT_LIO_PROMPTS: List[dict] = [
    # ── Stage 0 ───────────────────────────────────────────────────────────────
    {
        "_v": _PROMPT_VERSION,
        "id": 0,
        "name": "Company Intelligence",
        "description": "Extracts structured company data: stage, model, primary offering, target customer, pain points, tech maturity.",
        "recommended_tier": "fast",
        "model": "llama-3.1-8b-instant",
        "temperature": 0.2,
        "system": f"You are a B2B company analyst. Extract structured company intelligence from the LinkedIn profile data below.\n\n{_STRICT}",
        "user_template": (
            "LinkedIn Profile (BrightData):\n"
            "{raw_brightdata_json}\n\n"
            "Return ONLY this exact JSON — infer everything from the profile above:\n"
            '{{\n'
            '  "company_stage": "startup | growth | enterprise",\n'
            '  "business_model": "1 sentence",\n'
            '  "primary_offering": "1 sentence",\n'
            '  "target_customer": "1 sentence",\n'
            '  "likely_pain_points": ["pain1", "pain2", "pain3"],\n'
            '  "tech_maturity": "low | medium | high"\n'
            '}}'
        ),
    },
    # ── Stage 1 ───────────────────────────────────────────────────────────────
    {
        "_v": _PROMPT_VERSION,
        "id": 1,
        "name": "Auto Tags",
        "description": "Generates 6–8 behavioural tags describing what the prospect actively does and cares about.",
        "recommended_tier": "fast",
        "model": "llama-3.1-8b-instant",
        "temperature": 0.3,
        "system": (
            "You are a B2B sales researcher. Generate behavioural tags from LinkedIn profile data.\n\n"
            f"{_STRICT}\n"
            "- Return ONLY a valid JSON array of strings.\n"
            "- Tags must be 2-3 words max. Avoid job titles. Avoid 'leader', 'expert', 'professional'."
        ),
        "user_template": (
            "LinkedIn Profile (BrightData):\n"
            "{raw_brightdata_json}\n\n"
            "Generate 6-8 tags describing their interests, tools used, topics they post about, and business mindset.\n"
            "Base tags on their posts, activity (liked content), and about section.\n\n"
            'Return ONLY: ["tag1", "tag2", "tag3", ...]'
        ),
    },
    # ── Stage 2 ───────────────────────────────────────────────────────────────
    {
        "_v": _PROMPT_VERSION,
        "id": 2,
        "name": "Behavioural Signals",
        "description": "Extracts decision patterns, communication style, and pain hints from LinkedIn activity.",
        "recommended_tier": "fast",
        "model": "llama-3.1-8b-instant",
        "temperature": 0.3,
        "system": (
            "You are a B2B behavioural analyst. Extract decision patterns from LinkedIn activity data.\n\n"
            f"{_STRICT}\n"
            "- Each value is 1 sentence max.\n"
            "- Base every field on the actual posts and activity data provided.\n"
            "- Extract the ARGUMENT they endorse, not just the topic."
        ),
        "user_template": (
            "LinkedIn Profile (BrightData):\n"
            "{raw_brightdata_json}\n\n"
            "Analyse the prospect's posts, liked posts (activity), and about section.\n"
            "Return ONLY this exact JSON:\n"
            '{{\n'
            '  "posts_about": "1 sentence — specific themes and arguments they publish",\n'
            '  "engages_with": "1 sentence — what positions they endorse via likes/comments",\n'
            '  "communication_style": "1 sentence — tone, formality, peer vs broadcast",\n'
            '  "decision_pattern": "1 sentence — how they evaluate ideas (proven vs new, etc.)",\n'
            '  "pain_point_hint": "1 sentence — their most likely operational pain RIGHT NOW",\n'
            '  "warm_signal": "1 sentence on any WorksBuddy/automation brand engagement, or null"\n'
            '}}'
        ),
    },
    # ── Stage 3 ───────────────────────────────────────────────────────────────
    {
        "_v": _PROMPT_VERSION,
        "id": 3,
        "name": "Buying Signals",
        "description": "Identifies buying readiness: intent level, trigger events, timing score, and recommended action.",
        "recommended_tier": "fast",
        "model": "llama-3.1-8b-instant",
        "temperature": 0.2,
        "system": (
            "You are a B2B intent detection engine. Identify buying readiness signals.\n\n"
            f"{_STRICT}\n"
            "- timing_score must be an integer 0–100.\n"
            "- intent_level: only 'low', 'medium', or 'high'.\n"
            "- trigger_events: max 3 items, 1 sentence each."
        ),
        "user_template": (
            "LinkedIn Profile (BrightData):\n"
            "{raw_brightdata_json}\n\n"
            "Behavioural signals (from previous analysis):\n{behavioural_signals}\n\n"
            "Company intel:\n{company_intel}\n\n"
            "Return ONLY this exact JSON:\n"
            '{{\n'
            '  "intent_level": "low | medium | high",\n'
            '  "trigger_events": ["event1", "event2"],\n'
            '  "timing_score": 0,\n'
            '  "recommended_action": "reach_now | nurture | ignore"\n'
            '}}'
        ),
    },
    # ── Stage 4 ───────────────────────────────────────────────────────────────
    {
        "_v": _PROMPT_VERSION,
        "id": 4,
        "name": "Pitch Intelligence",
        "description": "Generates conversion-focused pitch intel: core pain, value prop, angle, personalization hook, CTA strategy.",
        "recommended_tier": "quality",
        "model": "llama-3.3-70b-versatile",
        "temperature": 0.4,
        "system": (
            "You are a world-class B2B sales strategist. Generate conversion-focused pitch intelligence.\n\n"
            f"{_STRICT}\n"
            "- personalization_hook: MUST reference a specific post title or liked post from their activity. Never use their job title.\n"
            "- do_not_pitch: 3-5 specific things tailored to THIS person, not generic."
        ),
        "user_template": (
            "Prospect: {first_name} {last_name}, {inferred_title} at {company} ({industry})\n\n"
            "Behavioural signals:\n{behavioural_signals}\n\n"
            "Buying signals:\n{buying_signals}\n\n"
            "Company intel:\n{company_intel}\n\n"
            "Our product: {product_name}\n"
            "Value prop: {value_proposition}\n"
            "Tone: {tone}\n"
            "Banned phrases: {banned_phrases}\n\n"
            "Return ONLY this exact JSON:\n"
            '{{\n'
            '  "core_pain": "The #1 pain this person feels RIGHT NOW — specific to role and activity",\n'
            '  "value_prop": "Single sentence that lands hardest for THIS person",\n'
            '  "angle": "pain | social_proof | trigger_event | curiosity | peer_insight",\n'
            '  "personalization_hook": "Exact opening hook referencing a specific post or liked post",\n'
            '  "do_not_pitch": ["specific thing 1", "specific thing 2", "specific thing 3"],\n'
            '  "cta_strategy": "question | demo | soft"\n'
            '}}'
        ),
    },
    # ── Stage 5 ───────────────────────────────────────────────────────────────
    {
        "_v": _PROMPT_VERSION,
        "id": 5,
        "name": "Outreach Generator",
        "description": "Writes a hyper-personalized cold email and LinkedIn note using pitch intelligence.",
        "recommended_tier": "quality",
        "model": "llama-3.3-70b-versatile",
        "temperature": 0.7,
        "system": (
            "You are a top-performing B2B SDR. Write concise, high-conversion outreach.\n\n"
            f"{_STRICT}\n"
            "- Email body: max 120 words.\n"
            "- LinkedIn connection_note: max 40 words. follow_up: max 40 words.\n"
            "- NEVER start with 'I noticed', 'I came across', 'Hope you're doing well'.\n"
            "- Start with the personalization_hook from pitch intelligence.\n"
            "- No banned phrases."
        ),
        "user_template": (
            "Prospect: {first_name} {last_name}, {inferred_title} at {company}\n"
            "Location: {city}, {country}\n\n"
            "Pitch intelligence:\n{pitch_intelligence}\n\n"
            "Behavioural signals:\n{behavioural_signals}\n\n"
            "Our product: {product_name}\n"
            "Tone: {tone}\n"
            "CTA style: {cta_style}\n"
            "Banned phrases: {banned_phrases}\n"
            "Case study: {case_study}\n\n"
            "Return ONLY this exact JSON:\n"
            '{{\n'
            '  "email": {{\n'
            '    "subject": "max 6 words, no clickbait",\n'
            '    "body": "max 120 words"\n'
            '  }},\n'
            '  "linkedin": {{\n'
            '    "connection_note": "max 40 words",\n'
            '    "follow_up": "max 40 words — use after they connect"\n'
            '  }}\n'
            '}}'
        ),
    },
    # ── Stage 6 ───────────────────────────────────────────────────────────────
    {
        "_v": _PROMPT_VERSION,
        "id": 6,
        "name": "Lead Score",
        "description": "Scores the lead 0–100 with priority tier, reasoning, and recommended next action.",
        "recommended_tier": "fast",
        "model": "llama-3.1-8b-instant",
        "temperature": 0.2,
        "system": (
            "You are a CRM scoring analyst. Score leads based on all signals provided.\n\n"
            f"{_STRICT}\n"
            "- lead_score: integer 0-100.\n"
            "- priority: only 'high', 'medium', or 'low'.\n"
            "- reason: 2 sentences max.\n"
            "- next_best_action: 1 actionable sentence."
        ),
        "user_template": (
            "Prospect: {first_name} {last_name}, {inferred_title} at {company} ({industry})\n"
            "ICP fit score: {icp_fit_score}/35\n"
            "Intent level: {intent_level}\n"
            "Timing score: {timing_score}\n"
            "Warm signal count: {warm_signal_count}\n"
            "Email found: {email_status}\n"
            "Company stage: {company_stage}\n\n"
            "Behavioural signals:\n{behavioural_signals}\n\n"
            "Buying signals:\n{buying_signals}\n\n"
            "Return ONLY this exact JSON:\n"
            '{{\n'
            '  "lead_score": 0,\n'
            '  "priority": "high | medium | low",\n'
            '  "reason": "2 sentences explaining the score",\n'
            '  "next_best_action": "1 actionable sentence for the SDR"\n'
            '}}'
        ),
    },
]


# ── Workspace config (LIO system prompt, etc.) ─────────────────────────────────

WORKSPACE_CONTEXT_DEFAULTS = {
    "product_name":        "WorksBuddy",
    "value_proposition":   "AI-powered HR and recruitment automation that saves teams 10+ hours/week",
    "target_titles":       "HR Manager, Talent Acquisition, CHRO, Recruiter, People Operations",
    "tone":                "professional yet conversational",
    "banned_phrases":      "synergy, leverage, game-changer, revolutionary, disruptive",
    "case_study":          "",
    "cta_style":           "question",
}


async def get_workspace_config(org_id: str) -> dict:
    """Return workspace config dict for the org."""
    async with aiosqlite.connect(LEADS_DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT lio_system_prompt, lio_model, lio_prompts_json, workspace_context_json, ai_prompts_json FROM workspace_configs WHERE org_id=?",
            (org_id,),
        ) as cur:
            row = await cur.fetchone()
            if not row:
                return {}
            return {
                "lio_system_prompt":      row["lio_system_prompt"] or "",
                "lio_model":              row["lio_model"] or "",
                "lio_prompts_json":       row["lio_prompts_json"] or "",
                "workspace_context_json": row["workspace_context_json"] or "",
                "ai_prompts_json":        row["ai_prompts_json"] or "",
            }


async def save_workspace_config(org_id: str, updates: dict) -> dict:
    """Persist workspace config updates for the org (merges with existing)."""
    now      = datetime.now(timezone.utc).isoformat()
    existing = await get_workspace_config(org_id)
    lio_prompt           = updates.get("lio_system_prompt",      existing.get("lio_system_prompt", ""))
    lio_model            = updates.get("lio_model",              existing.get("lio_model", ""))
    lio_prompts_json     = updates.get("lio_prompts_json",       existing.get("lio_prompts_json", ""))
    workspace_ctx_json   = updates.get("workspace_context_json", existing.get("workspace_context_json", ""))
    ai_prompts_json      = updates.get("ai_prompts_json",        existing.get("ai_prompts_json", ""))
    async with aiosqlite.connect(LEADS_DB) as db:
        await db.execute(
            """INSERT INTO workspace_configs
                   (org_id, lio_system_prompt, lio_model, lio_prompts_json, workspace_context_json, ai_prompts_json, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(org_id) DO UPDATE SET
                 lio_system_prompt      = excluded.lio_system_prompt,
                 lio_model              = excluded.lio_model,
                 lio_prompts_json       = excluded.lio_prompts_json,
                 workspace_context_json = excluded.workspace_context_json,
                 ai_prompts_json        = excluded.ai_prompts_json,
                 updated_at             = excluded.updated_at""",
            (org_id, lio_prompt, lio_model, lio_prompts_json, workspace_ctx_json, ai_prompts_json, now),
        )
        await db.commit()
    return {"lio_system_prompt": lio_prompt, "lio_model": lio_model}


async def get_workspace_context(org_id: str) -> dict:
    """Return workspace product context (product_name, value_prop, etc.), falls back to defaults."""
    cfg = await get_workspace_config(org_id)
    raw = cfg.get("workspace_context_json", "").strip()
    if raw:
        try:
            saved = json.loads(raw)
            if isinstance(saved, dict):
                merged = dict(WORKSPACE_CONTEXT_DEFAULTS)
                merged.update(saved)
                return merged
        except Exception:
            pass
    import copy
    return copy.deepcopy(WORKSPACE_CONTEXT_DEFAULTS)


async def save_workspace_context(org_id: str, context: dict) -> None:
    """Persist workspace product context for this org."""
    await save_workspace_config(org_id, {"workspace_context_json": json.dumps(context)})


async def get_lio_prompts(org_id: str) -> List[dict]:
    """Return the 7 LIO prompt configs for this org (falls back to defaults)."""
    cfg = await get_workspace_config(org_id)
    raw = cfg.get("lio_prompts_json", "").strip()
    if raw:
        try:
            saved = json.loads(raw)
            if isinstance(saved, list) and len(saved) == len(DEFAULT_LIO_PROMPTS) and saved[0].get("_v") == _PROMPT_VERSION:
                return saved
        except Exception:
            pass
    import copy
    return copy.deepcopy(DEFAULT_LIO_PROMPTS)


async def save_lio_prompts(org_id: str, prompts: List[dict]) -> None:
    """Persist the 5 LIO prompt configs for this org."""
    await save_workspace_config(org_id, {"lio_prompts_json": json.dumps(prompts)})


# ── AI Enrichment Module Prompt Defaults ───────────────────────────────────────

_AI_PROMPT_VERSION = "v3"
_MODEL_FAST  = "llama-3.1-8b-instant"
_MODEL_SMART = "llama-3.3-70b-versatile"

DEFAULT_AI_MODULE_PROMPTS: List[dict] = [
    {
        "_v": _AI_PROMPT_VERSION,
        "id": "identity", "name": "Identity", "group": "core",
        "description": "Extracts full name, title, company, location, LinkedIn URL, timezone and follower count.",
        "model": _MODEL_FAST, "temperature": 0.2,
        "system": f"You are a data extraction expert. Extract identity fields from a LinkedIn profile JSON. {_STRICT}",
        "user_template": (
            "Profile JSON:\n{{profile_json}}\n\n"
            "Return ONLY this JSON (fill from profile data):\n"
            '{"full_name":"","first_name":"","last_name":"","title":"","company":"",'
            '"linkedin_url":"","location":"","timezone":"IANA tz e.g. Asia/Kolkata","followers":0}'
        ),
    },
    {
        "_v": _AI_PROMPT_VERSION,
        "id": "contact", "name": "Contact", "group": "core",
        "description": "Infers work email, personal email, bounce risk, phone, and waterfall steps used.",
        "model": _MODEL_FAST, "temperature": 0.2,
        "system": (
            "You are a contact intelligence analyst. Extract or infer contact info from a LinkedIn profile. "
            "Infer work email from company name/domain patterns visible in the data if possible. "
            f"{_STRICT}"
        ),
        "user_template": (
            "Profile JSON:\n{{profile_json}}\n\n"
            "Return ONLY this JSON:\n"
            '{"work_email":null,"email_confidence":0.0,"personal_email":null,'
            '"bounce_risk":"low|medium|high","phone":null,'
            '"waterfall_steps":["describe inference steps used"]}'
        ),
    },
    {
        "_v": _AI_PROMPT_VERSION,
        "id": "scores", "name": "Scores", "group": "core",
        "description": "Scores ICP fit (0-100), intent score, and recommends best send window.",
        "model": _MODEL_FAST, "temperature": 0.2,
        "system": (
            "You are a B2B lead scoring analyst. Score this LinkedIn profile for ICP fit and buying intent. "
            f"{_STRICT}\n"
            "- All scores are integers 0-100.\n"
            "- best_send_window: day + time range + timezone."
        ),
        "user_template": (
            "Profile JSON:\n{{profile_json}}\n\n"
            "Return ONLY this JSON:\n"
            '{"icp_score":0,"icp_breakdown":{"title_match":0,"company_size_fit":0,'
            '"geo_fit":0,"engagement_level":0},"intent_score":0,"best_send_window":""}'
        ),
    },
    {
        "_v": _AI_PROMPT_VERSION,
        "id": "icp_match", "name": "ICP Match", "group": "intelligence",
        "description": "Determines decision-maker status, industry, company size, geo fit, and ICP verdict.",
        "model": _MODEL_FAST, "temperature": 0.2,
        "system": (
            "You are a B2B ICP (Ideal Customer Profile) analyst. "
            f"{_STRICT}\n"
            "- icp_verdict: 'Strong Match' | 'Moderate Match' | 'Weak Match' | 'No Match'\n"
            "- company_size: estimate (e.g. '51-200', '<10', '1000+')"
        ),
        "user_template": (
            "Profile JSON:\n{{profile_json}}\n\n"
            "Return ONLY this JSON:\n"
            '{"is_decision_maker":true,"industry":"","company_size":"","geo_fit":true,'
            '"education_level":"","awards_count":0,"icp_verdict":""}'
        ),
    },
    {
        "_v": _AI_PROMPT_VERSION,
        "id": "behavioural_signals", "name": "Behavioural Signals", "group": "intelligence",
        "description": "Analyses posts and activity to extract topics, engagement style, pain points, and warm signals.",
        "model": _MODEL_FAST, "temperature": 0.3,
        "system": (
            "You are a B2B behavioural analyst. Analyse this prospect's LinkedIn posts and activity feed. "
            f"{_STRICT}\n"
            "- posts_about: 3-6 short topic strings (2-4 words each) from their OWN posts\n"
            "- engages_with: 3-6 short topic strings from content they liked/commented on\n"
            "- engagement_style: 1-2 sentences describing writing style and tone\n"
            "- decision_pattern: 1-2 sentences describing how they make decisions\n"
            "- pain_points: specific operational pains (not generic buzzwords), 2-4 items\n"
            "- warm_signal: describe any concrete buying signal with specifics, or null"
        ),
        "user_template": (
            "Profile JSON:\n{{profile_json}}\n\n"
            "Return ONLY this JSON:\n"
            '{"posts_about":[],"engages_with":[],"engagement_style":"","decision_pattern":"","pain_points":[],"warm_signal":null}'
        ),
    },
    {
        "_v": _AI_PROMPT_VERSION,
        "id": "pitch_intelligence", "name": "Pitch Intelligence", "group": "intelligence",
        "description": "Generates core pain point, value prop, best angle, CTA, and topics to avoid.",
        "model": _MODEL_SMART, "temperature": 0.4,
        "system": (
            "You are a world-class B2B sales strategist. Generate pitch intelligence from a LinkedIn profile. "
            f"{_STRICT}\n"
            "- pain_point: specific to their current role and activity — not generic\n"
            "- do_not_pitch: specific topics/tools this person would reject or already has"
        ),
        "user_template": (
            "Profile JSON:\n{{profile_json}}\n\n"
            'Return ONLY this JSON:\n{"pain_point":"","value_prop":"","best_angle":"","cta":"","do_not_pitch":[]}'
        ),
    },
    {
        "_v": _AI_PROMPT_VERSION,
        "id": "activity", "name": "Activity", "group": "intelligence",
        "description": "Analyses recent LinkedIn activity for warm signals, post count, and engagement frequency.",
        "model": _MODEL_FAST, "temperature": 0.2,
        "system": (
            "You are an activity signal analyst. Analyse this prospect's LinkedIn posts and liked activity. "
            f"{_STRICT}\n"
            "- type: 'post' | 'like' | 'comment'\n"
            "- warm_signal: true only if the activity indicates buying intent, hiring, or tool evaluation\n"
            "- engagement_frequency: 'daily' | 'weekly' | 'monthly' | 'rare'"
        ),
        "user_template": (
            "Profile JSON:\n{{profile_json}}\n\n"
            "Return ONLY this JSON:\n"
            '{"recent_activity":[{"type":"","title":"","warm_signal":false,"signal_reason":""}],'
            '"authored_posts_count":0,"engagement_frequency":""}'
        ),
    },
    {
        "_v": _AI_PROMPT_VERSION,
        "id": "tags", "name": "Tags", "group": "output",
        "description": "Generates 6-12 smart CRM tags covering role, industry, behaviour, and buying signals.",
        "model": _MODEL_FAST, "temperature": 0.3,
        "system": (
            "You are a B2B CRM segmentation expert. Generate smart tags from a LinkedIn profile. "
            f"{_STRICT}\n"
            "- 6-12 tags total\n"
            "- Max 3 words per tag\n"
            "- Cover: role, industry, company size, location, behaviour, buying signals"
        ),
        "user_template": (
            "Profile JSON:\n{{profile_json}}\n\n"
            'Return ONLY this JSON:\n{"tags":["tag1","tag2"]}'
        ),
    },
    {
        "_v": _AI_PROMPT_VERSION,
        "id": "outreach", "name": "Outreach", "group": "output",
        "description": "Writes a LinkedIn note, full cold email (subject/greeting/opening/body/CTA/sign-off), and Day 3 + Day 7 follow-ups.",
        "model": _MODEL_SMART, "temperature": 0.3,
        "system": (
            "You are a world-class B2B SDR. Write hyper-personalised cold outreach from LinkedIn profile data.\n\n"
            "STRICT OUTPUT RULES:\n"
            "- Return ONLY a valid JSON object. No markdown, no explanation, no extra keys.\n"
            "- Every key listed below is MANDATORY. Never omit a key. Never return an empty string for any field.\n"
            "- Use null only if a field is truly impossible to fill.\n\n"
            "REQUIRED JSON SCHEMA (all 10 keys must be present):\n"
            "{\n"
            '  "linkedin_note": "CONNECTION REQUEST — max 100 words, reference a specific post or topic, end with a question, zero sales pitch",\n'
            '  "cold_email": {\n'
            '    "subject": "SUBJECT — 10 to 20 words, no questions, no ALL CAPS, intriguing but honest",\n'
            '    "greeting": "GREETING — always Hi [FirstName], — first name only",\n'
            '    "opening": "OPENING — 1 sentence referencing a SPECIFIC post, activity or topic they care about. Never start with I noticed / I came across / Hope you",\n'
            '    "body": "BODY — 5 to 10 sentences: pain you spotted from their profile → how your product solves it → one concrete proof point or stat",\n'
            '    "cta": "CTA — 1 soft-ask sentence, ask a specific question or suggest a 15-min call, never say let me know if interested",\n'
            '    "sign_off": "Best,\\n[Your Name]",\n'
            '    "full_email": "COMPLETE EMAIL — greeting + newline + opening + newline + body + newline + cta + newline + sign_off combined into one ready-to-send string"\n'
            '  },\n'
            '  "follow_up": {\n'
            '    "day3": "FOLLOW-UP DAY 3 — 1 to 2 sentences, friendly bump if no reply, add one new angle",\n'
            '    "day7": "FOLLOW-UP DAY 7 — 1 sentence final bump, include a relevant insight or question"\n'
            '  }\n'
            "}"
        ),
        "user_template": (
            "Prospect LinkedIn profile data:\n{{profile_json}}\n\n"
            "Using the profile above, write fully personalised outreach. "
            "Fill EVERY field with real content — no placeholder text, no empty strings.\n\n"
            "Return ONLY this exact JSON structure with all 10 keys filled:\n"
            '{"linkedin_note":"","cold_email":{"subject":"","greeting":"","opening":"","body":"","cta":"","sign_off":"","full_email":""},"follow_up":{"day3":"","day7":""}}'
        ),
    },
    {
        "_v": _AI_PROMPT_VERSION,
        "id": "persona_analysis", "name": "Persona Analysis", "group": "output",
        "description": "Full psychographic persona: personality type, buying signals, CRM score (A-D), and outreach intelligence.",
        "model": _MODEL_SMART, "temperature": 0.4,
        "system": (
            "You are an expert buyer persona analyst. Generate a full psychographic and behavioural persona from a LinkedIn profile. "
            f"{_STRICT}\n"
            "- personality: 'Driver' | 'Analytical' | 'Amiable' | 'Expressive'\n"
            "- risk_tolerance: 'low' | 'moderate' | 'high'\n"
            "- crm_score.tier: 'A' | 'B' | 'C' | 'D' (A = best)\n"
            "- crm_score.score: integer 0-100"
        ),
        "user_template": (
            "Profile JSON:\n{{profile_json}}\n\n"
            "Return ONLY this JSON:\n"
            '{"identity_summary":"","interests":[],"skills":[],"behavior_analysis":"",'
            '"writing_style":"","psychographic_profile":{"personality":"","motivation":"","risk_tolerance":""},'
            '"buying_signals":[],"smart_tags":[],'
            '"outreach_intelligence":{"best_channel":"","best_time":"","tone":""},'
            '"crm_score":{"score":0,"tier":"","reason":""}}'
        ),
    },
]


async def get_ai_module_prompts(org_id: str) -> List[dict]:
    """Return the 10 AI module prompt configs for this org (falls back to defaults)."""
    cfg = await get_workspace_config(org_id)
    raw = cfg.get("ai_prompts_json", "").strip()
    if raw:
        try:
            saved = json.loads(raw)
            if (isinstance(saved, list) and len(saved) == len(DEFAULT_AI_MODULE_PROMPTS)
                    and saved[0].get("_v") == _AI_PROMPT_VERSION):
                return saved
        except Exception:
            pass
    import copy
    return copy.deepcopy(DEFAULT_AI_MODULE_PROMPTS)


async def save_ai_module_prompts(org_id: str, prompts: List[dict]) -> None:
    """Persist the 10 AI module prompt configs for this org."""
    await save_workspace_config(org_id, {"ai_prompts_json": json.dumps(prompts)})

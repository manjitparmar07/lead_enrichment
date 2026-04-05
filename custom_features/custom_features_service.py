"""
custom_features_service.py
──────────────────────────
CRUD for custom API features + execution via Groq / HuggingFace / WorksBuddy LLM.
"""
from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

import httpx

from db import get_pool

logger = logging.getLogger(__name__)

GROQ_URL       = "https://api.groq.com/openai/v1/chat/completions"
HF_INFERENCE_URL = "https://api-inference.huggingface.co/models"
HF_SEARCH_URL    = "https://huggingface.co/api/models"


# ── DB init ───────────────────────────────────────────────────────────────────

async def init_custom_features_db() -> None:
    async with get_pool().acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS custom_features (
                id             SERIAL PRIMARY KEY,
                org_id         TEXT NOT NULL,
                feature_code   TEXT NOT NULL,
                feature_name   TEXT NOT NULL,
                module         TEXT NOT NULL DEFAULT 'CUSTOM',
                endpoint_slug  TEXT NOT NULL,
                task_type      TEXT NOT NULL DEFAULT 'ANALYZE',
                description    TEXT,
                system_prompt  TEXT,
                model_provider TEXT NOT NULL DEFAULT 'groq',
                model_name     TEXT,
                temperature    FLOAT DEFAULT 0.3,
                input_params   JSONB DEFAULT '[]',
                is_active      BOOLEAN DEFAULT TRUE,
                created_at     TIMESTAMPTZ DEFAULT NOW(),
                updated_at     TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(org_id, endpoint_slug),
                UNIQUE(org_id, feature_code)
            )
        """)
    logger.info("[CustomFeatures] DB ready")


# ── CRUD ─────────────────────────────────────────────────────────────────────

async def list_features(org_id: str) -> list[dict]:
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM custom_features WHERE org_id=$1 ORDER BY created_at DESC",
            org_id,
        )
    return [dict(r) for r in rows]


async def get_feature_by_id(org_id: str, feature_id: int) -> dict | None:
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM custom_features WHERE org_id=$1 AND id=$2",
            org_id, feature_id,
        )
    return dict(row) if row else None


async def get_feature_by_slug(org_id: str, slug: str) -> dict | None:
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM custom_features WHERE org_id=$1 AND endpoint_slug=$2 AND is_active=TRUE",
            org_id, slug,
        )
    return dict(row) if row else None


async def create_feature(org_id: str, data: dict) -> dict:
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO custom_features
                (org_id, feature_code, feature_name, module, endpoint_slug,
                 task_type, description, system_prompt, model_provider, model_name,
                 temperature, input_params)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb)
            RETURNING *
            """,
            org_id,
            data["feature_code"],
            data["feature_name"],
            data.get("module", "CUSTOM"),
            data["endpoint_slug"],
            data.get("task_type", "ANALYZE"),
            data.get("description"),
            data.get("system_prompt"),
            data.get("model_provider", "groq"),
            data.get("model_name"),
            float(data.get("temperature") or 0.3),
            json.dumps(data.get("input_params", [])),
        )
    return dict(row)


async def update_feature(org_id: str, feature_id: int, data: dict) -> dict | None:
    if not data:
        return await get_feature_by_id(org_id, feature_id)

    allowed_fields = {
        "feature_code", "feature_name", "module", "endpoint_slug", "task_type",
        "description", "system_prompt", "model_provider", "model_name",
        "temperature", "input_params", "is_active",
    }

    set_parts: list[str] = []
    values: list[Any] = [org_id, feature_id]
    idx = 3

    for field, value in data.items():
        if field not in allowed_fields:
            continue
        if field == "input_params":
            set_parts.append(f"{field} = ${idx}::jsonb")
            values.append(json.dumps(value) if not isinstance(value, str) else value)
        elif field == "temperature" and value is not None:
            set_parts.append(f"{field} = ${idx}")
            values.append(float(value))
        else:
            set_parts.append(f"{field} = ${idx}")
            values.append(value)
        idx += 1

    if not set_parts:
        return await get_feature_by_id(org_id, feature_id)

    set_parts.append("updated_at = NOW()")

    async with get_pool().acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE custom_features SET {', '.join(set_parts)} WHERE org_id=$1 AND id=$2 RETURNING *",
            *values,
        )
    return dict(row) if row else None


async def delete_feature(org_id: str, feature_id: int) -> bool:
    async with get_pool().acquire() as conn:
        result = await conn.execute(
            "DELETE FROM custom_features WHERE org_id=$1 AND id=$2",
            org_id, feature_id,
        )
    return result == "DELETE 1"


# ── Key helpers ───────────────────────────────────────────────────────────────

def _groq_key() -> str:
    try:
        import keys_service as ks
        return ks.get("GROQ_API_KEY") or os.getenv("GROQ_API_KEY", "")
    except Exception:
        return os.getenv("GROQ_API_KEY", "")


def _hf_key() -> str:
    try:
        import keys_service as ks
        return ks.get("HF_TOKEN") or os.getenv("HF_TOKEN", "")
    except Exception:
        return os.getenv("HF_TOKEN", "")


def _wb_host() -> str:
    try:
        import keys_service as ks
        return ks.get("WB_LLM_HOST") or os.getenv("WB_LLM_HOST", "")
    except Exception:
        return os.getenv("WB_LLM_HOST", "")


def _wb_key() -> str:
    try:
        import keys_service as ks
        return ks.get("WB_LLM_API_KEY") or os.getenv("WB_LLM_API_KEY", "")
    except Exception:
        return os.getenv("WB_LLM_API_KEY", "")


# ── JSON extraction ───────────────────────────────────────────────────────────

def _deep_parse_strings(obj: Any) -> Any:
    """Recursively try to JSON-parse any string values that look like JSON objects/arrays."""
    if isinstance(obj, dict):
        return {k: _deep_parse_strings(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_deep_parse_strings(v) for v in obj]
    if isinstance(obj, str):
        s = obj.strip()
        if s.startswith(("{", "[")):
            try:
                return _deep_parse_strings(json.loads(s))
            except Exception:
                pass
    return obj


def _extract_json(text: str) -> Any:
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.MULTILINE)
    text = re.sub(r"\s*```$", "", text, flags=re.MULTILINE).strip()
    try:
        return _deep_parse_strings(json.loads(text))
    except Exception:
        for pattern in [r"\{[\s\S]*\}", r"\[[\s\S]*\]"]:
            m = re.search(pattern, text)
            if m:
                try:
                    return _deep_parse_strings(json.loads(m.group()))
                except Exception:
                    pass
    return {"result": text}


# ── LLM callers ───────────────────────────────────────────────────────────────

async def _call_groq(
    system: str, user: str, model: str, temperature: float
) -> tuple[Any, dict]:
    key = _groq_key()
    if not key:
        raise RuntimeError("GROQ_API_KEY not configured")

    model = model or "llama-3.1-8b-instant"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user",   "content": user},
        ],
        "temperature": temperature,
        "max_tokens": 2000,
        "response_format": {"type": "json_object"},
    }
    async with httpx.AsyncClient(timeout=60) as c:
        r = await c.post(
            GROQ_URL,
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json=payload,
        )
    if not r.is_success:
        raise RuntimeError(f"Groq error {r.status_code}: {r.text[:400]}")

    data = r.json()
    raw = data["choices"][0]["message"]["content"].strip()
    usage = data.get("usage", {})
    token_usage = {
        "prompt_tokens":     usage.get("prompt_tokens", 0),
        "completion_tokens": usage.get("completion_tokens", 0),
        "total_tokens":      usage.get("total_tokens", 0),
        "model":    model,
        "provider": "groq",
    }
    return _extract_json(raw), token_usage


async def _call_huggingface(
    system: str, user: str, model: str, temperature: float
) -> tuple[Any, dict]:
    key = _hf_key()
    if not key:
        raise RuntimeError(
            "HF_TOKEN not configured. "
            "Add it in Tool Configuration → HF_TOKEN."
        )
    if not model:
        raise RuntimeError("HuggingFace model ID is required")

    # Use HuggingFace Router — OpenAI-compatible chat completions endpoint
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user",   "content": user},
        ],
        "temperature": max(0.01, temperature),
        "max_tokens": 1500,
    }
    async with httpx.AsyncClient(timeout=120) as c:
        r = await c.post(
            "https://router.huggingface.co/v1/chat/completions",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json=payload,
        )

    if r.status_code == 503:
        raise RuntimeError(
            f"HuggingFace model '{model}' is loading (~20s). Please retry shortly."
        )
    if not r.is_success:
        raise RuntimeError(f"HuggingFace error {r.status_code}: {r.text[:400]}")

    data   = r.json()
    raw    = data["choices"][0]["message"]["content"].strip()
    usage  = data.get("usage", {})
    token_usage = {
        "prompt_tokens":     usage.get("prompt_tokens", 0),
        "completion_tokens": usage.get("completion_tokens", 0),
        "total_tokens":      usage.get("total_tokens", 0),
        "model": model, "provider": "huggingface",
    }
    return _extract_json(raw), token_usage


async def _call_worksbuddy(
    system: str, user: str, model: str, temperature: float
) -> tuple[Any, dict]:
    host = _wb_host()
    if not host:
        raise RuntimeError("WB_LLM_HOST not configured")

    model = model or "llama-3.1-8b-instant"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user",   "content": user},
        ],
        "temperature": temperature,
        "max_tokens": 2000,
        "response_format": {"type": "json_object"},
    }
    headers = {"Content-Type": "application/json"}
    key = _wb_key()
    if key:
        headers["Authorization"] = f"Bearer {key}"

    async with httpx.AsyncClient(timeout=60) as c:
        r = await c.post(f"{host}/v1/chat/completions", headers=headers, json=payload)

    if not r.is_success:
        raise RuntimeError(f"WorksBuddy LLM error {r.status_code}: {r.text[:400]}")

    data = r.json()
    raw = data["choices"][0]["message"]["content"].strip()
    usage = data.get("usage", {})
    token_usage = {
        "prompt_tokens":     usage.get("prompt_tokens", 0),
        "completion_tokens": usage.get("completion_tokens", 0),
        "total_tokens":      usage.get("total_tokens", 0),
        "model":    model,
        "provider": "worksbuddy",
    }
    return _extract_json(raw), token_usage


# ── Feature execution ─────────────────────────────────────────────────────────

def _build_user_message(feature: dict, input_data: dict) -> str:
    params = feature.get("input_params") or []
    if isinstance(params, str):
        params = json.loads(params)

    lines: list[str] = []
    defined_keys: set[str] = set()
    for param in params:
        key   = param.get("key", "")
        label = param.get("label", key)
        value = input_data.get(key, "")
        defined_keys.add(key)
        if value:
            lines.append(f"{label}: {value}")

    for k, v in input_data.items():
        if k not in defined_keys and v:
            lines.append(f"{k}: {v}")

    return "\n".join(lines) if lines else json.dumps(input_data)


def _fill_template(system_prompt: str, input_data: dict) -> str:
    if not system_prompt:
        return system_prompt
    result = system_prompt
    for k, v in input_data.items():
        result = result.replace(f"{{{k}}}", str(v))
    return result


async def execute_feature(
    org_id: str,
    slug: str,
    input_data: dict,
    system_prompt_override: str | None = None,
) -> dict:
    feature = await get_feature_by_slug(org_id, slug)
    if not feature:
        raise ValueError(f"Feature '{slug}' not found or inactive")

    base_prompt = (
        system_prompt_override
        if system_prompt_override is not None
        else (feature.get("system_prompt") or "You are a helpful AI assistant. Return ONLY valid JSON.")
    )
    system = _fill_template(base_prompt, input_data)
    user   = _build_user_message(feature, input_data)
    temperature = float(feature.get("temperature") or 0.3)
    model      = feature.get("model_name") or ""
    provider   = (feature.get("model_provider") or "groq").lower()

    if provider == "huggingface":
        result, token_usage = await _call_huggingface(system, user, model, temperature)
    elif provider == "worksbuddy":
        result, token_usage = await _call_worksbuddy(system, user, model, temperature)
    else:
        result, token_usage = await _call_groq(system, user, model, temperature)

    return {
        "feature_code":  feature["feature_code"],
        "feature_name":  feature["feature_name"],
        "slug":          slug,
        "result":        result,
        "token_usage":   token_usage,
        "debug": {
            "system_prompt": system,
            "user_message":  user,
            "model_provider": provider,
            "model_name":    model,
            "temperature":   temperature,
        },
    }


# ── HuggingFace model search ──────────────────────────────────────────────────

async def search_hf_models(query: str, limit: int = 20) -> list[dict]:
    params = {
        "search":    query,
        "filter":    "text-generation",
        "limit":     limit,
        "sort":      "downloads",
        "direction": -1,
    }
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(HF_SEARCH_URL, params=params)
        if not r.is_success:
            return []
        return [
            {
                "id":           m.get("modelId") or m.get("id", ""),
                "downloads":    m.get("downloads", 0),
                "likes":        m.get("likes", 0),
                "pipeline_tag": m.get("pipeline_tag", ""),
            }
            for m in r.json()
            if not m.get("private", False) and (m.get("modelId") or m.get("id"))
        ]
    except Exception as e:
        logger.warning("[HuggingFace] Model search failed: %s", e)
        return []

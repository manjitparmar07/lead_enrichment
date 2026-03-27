"""
model_router.py — Smart Model Selection

Routes each request to the cheapest model that can handle it.

Tiers:
  fast     — low-latency, cheap  (simple Q&A, lookups, NLU)
  standard — balanced            (scoring, analysis, moderate generation)
  powerful — high-quality        (multi-step reasoning, planning, complex generation)

Decision factors:
  1. Task type (NLU / EXTRACT / SCORE / ANALYZE / GENERATE / PLAN / AUTOMATE)
  2. Estimated token count of the input
  3. Number of tools expected from the execution plan
"""

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

# ── Model tiers per provider ──────────────────────────────────────────────────
_TIERS: dict[str, dict[str, str]] = {
    "groq": {
        "fast":     "llama-3.1-8b-instant",
        "standard": "llama-3.3-70b-versatile",
        "powerful": "llama-3.3-70b-versatile",
    },
    "huggingface": {
        "fast":     "mistralai/Mistral-7B-Instruct-v0.3",
        "standard": "Qwen/Qwen2.5-72B-Instruct",
        "powerful": "Qwen/Qwen2.5-72B-Instruct",
    },
    "ollama": {
        "fast":     os.getenv("OLLAMA_FAST_MODEL",     "llama3.2:3b"),
        "standard": os.getenv("OLLAMA_STANDARD_MODEL", "llama3.1:8b"),
        "powerful": os.getenv("OLLAMA_POWERFUL_MODEL", "llama3.1:70b"),
    },
    "worksbuddy": {
        "fast":     os.getenv("WB_FAST_MODEL",     "wb-fast"),
        "standard": os.getenv("WB_STANDARD_MODEL", "wb-pro"),
        "powerful": os.getenv("WB_POWERFUL_MODEL", "wb-pro"),
    },
}

# ── Task type → base complexity tier ─────────────────────────────────────────
_TASK_TIER: dict[str, str] = {
    "NLU":         "fast",
    "EXTRACT":     "fast",
    "EXTRACT_VIS": "standard",
    "SCORE":       "standard",
    "ANALYZE":     "standard",
    "GENERATE":    "standard",
    "PLAN":        "powerful",
    "AUTOMATE":    "powerful",
}

_TIER_RANK = {"fast": 0, "standard": 1, "powerful": 2}
_RANK_TIER = {0: "fast", 1: "standard", 2: "powerful"}

# Token thresholds (1 token ≈ 4 chars)
_FAST_LIMIT     = 600    # < 600 tokens → fast model eligible
_STANDARD_LIMIT = 2000   # < 2000 tokens → standard eligible; above → powerful


def route_model(
    provider: str,
    task_type: str,
    user_message: str = "",
    data: Optional[dict] = None,
    tool_count: int = 0,
    requested_model: str = "",
) -> tuple[str, str]:
    """
    Return (model_name, complexity_tier).

    If caller already set a model → honour it (explicit override).
    Otherwise → auto-select based on complexity heuristics.
    """
    if requested_model:
        return requested_model, "explicit"

    base_tier = _TASK_TIER.get(task_type, "standard")
    rank       = _TIER_RANK[base_tier]

    # Upgrade tier for large inputs
    approx_tokens = (len(user_message) + len(str(data or {}))) // 4
    if approx_tokens > _STANDARD_LIMIT:
        rank = max(rank, 2)
    elif approx_tokens > _FAST_LIMIT:
        rank = max(rank, 1)

    # Upgrade tier for multi-tool plans
    if tool_count >= 3:
        rank = max(rank, 2)
    elif tool_count >= 2:
        rank = max(rank, 1)

    tier           = _RANK_TIER[rank]
    provider_tiers = _TIERS.get(provider.lower(), _TIERS["worksbuddy"])
    model          = provider_tiers[tier]

    logger.debug(
        f"[ModelRouter] provider={provider} task={task_type} "
        f"tokens≈{approx_tokens} tools={tool_count} → tier={tier} model={model}"
    )
    return model, tier

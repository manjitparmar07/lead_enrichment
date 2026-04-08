"""
_llm.py
-------
HuggingFace LLM integration.

Covers:
  - _call_llm: Qwen2.5-7B-Instruct via HF Router, 3 retries, semaphore-limited
  - _parse_json_from_llm: 5-strategy robust JSON extractor
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Optional

import httpx

from ._utils import _hf_token, _hf_model
from ._clients import _llm_semaphore

try:
    from analytics import api_usage_service as _usage
except ImportError:
    class _usage:  # type: ignore
        @staticmethod
        async def track(*args, **kwargs): pass

logger = logging.getLogger(__name__)


async def _call_llm(
    messages: list[dict],
    max_tokens: int = 1800,
    temperature: float = 0.3,
    model_override: Optional[str] = None,
    wb_llm_host_override: Optional[str] = None,
    wb_llm_key_override: Optional[str] = None,
    wb_llm_model_override: Optional[str] = None,
    hf_first: bool = False,
) -> Optional[str]:
    """HuggingFace only — Qwen2.5-7B-Instruct (fast, low latency).
    3 retries with 3s/6s backoff on transient failures.
    Semaphore-limited to max 10 concurrent calls.
    """
    async def _post(base_url: str, api_key: str, model: str, msgs: list, timeout: int = 90) -> str:
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(
                f"{base_url.rstrip('/')}/v1/chat/completions",
                headers=headers,
                json={"model": model, "messages": msgs, "temperature": temperature, "max_tokens": max_tokens},
            )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"].strip()

    hf_tok = _hf_token()
    if not hf_tok:
        logger.warning("[LLM] No HuggingFace token configured")
        return None

    hf_mod = _hf_model()
    payload_chars = sum(len(m["content"]) for m in messages)

    async with _llm_semaphore:
        for attempt in range(3):
            try:
                logger.info("[LLM] HuggingFace (%s) attempt %d/3 payload=%d chars", hf_mod, attempt + 1, payload_chars)
                result = await _post("https://router.huggingface.co", hf_tok, hf_mod, messages)
                logger.info("[LLM] HuggingFace OK — attempt %d", attempt + 1)
                await _usage.track("huggingface", "llm_call")
                return result
            except Exception as e:
                logger.warning("[LLM] HuggingFace failed (attempt %d/3): %s", attempt + 1, e)
                if attempt < 2:
                    await asyncio.sleep(3 * (attempt + 1))  # 3s, 6s

    logger.error("[LLM] All 3 HuggingFace attempts failed — returning None")
    return None


def _parse_json_from_llm(raw: str) -> dict:
    """
    Robustly extract a JSON object from an LLM response.

    Handles all common LLM output patterns in order:
      1. Strip <think>…</think> reasoning blocks (Qwen/DeepSeek models)
      2. Direct JSON parse (model returned clean JSON)
      3. Strip markdown fences (```json … ``` or ``` … ```)
      4. Extract first {...} block via regex
      5. Attempt lightweight repair (trailing commas, single quotes)
    Returns {} if all strategies fail.
    """
    if not raw or not isinstance(raw, str):
        return {}

    text = raw.strip()

    # 1. Strip <think>…</think> blocks produced by reasoning models
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()

    # 2. Try direct parse
    try:
        result = json.loads(text)
        if isinstance(result, dict):
            return result
    except Exception:
        pass

    # 3. Strip markdown fences
    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if fenced:
        try:
            result = json.loads(fenced.group(1))
            if isinstance(result, dict):
                return result
        except Exception:
            pass

    # 4. Extract outermost {...} block
    start = text.find("{")
    if start != -1:
        depth = 0
        for i, ch in enumerate(text[start:], start):
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    candidate = text[start:i + 1]
                    try:
                        result = json.loads(candidate)
                        if isinstance(result, dict):
                            return result
                    except Exception:
                        pass
                    break

    # 5. Lightweight repair: trailing commas, single-quoted keys/values
    try:
        candidate = text[text.find("{"):] if "{" in text else text
        repaired = re.sub(r",\s*([}\]])", r"\1", candidate)
        repaired = re.sub(r"'([^']*)'", r'"\1"', repaired)
        result = json.loads(repaired)
        if isinstance(result, dict):
            logger.warning("[LLM] JSON required repair — check model output quality")
            return result
    except Exception:
        pass

    logger.warning("[LLM] Could not parse JSON from model response (len=%d)", len(raw))
    return {}

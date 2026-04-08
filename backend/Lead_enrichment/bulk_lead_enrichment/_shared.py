"""
_shared.py
----------
Shared setup, helpers, and models used across the lead enrichment route files.

Imported by:
  lead_enrichment_brightdata_routes.py  (main router — mounts sub-routers)
  _routes_enrich.py
  _routes_webhooks.py
  _routes_leads.py
  _routes_view.py
  _routes_lio.py
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
from typing import Any, Optional

import jwt
import re as _lio_re

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, field_validator

from Lead_enrichment.bulk_lead_enrichment import lead_enrichment_brightdata_service as svc
from config import enrichment_config_service as cfg_svc

logger = logging.getLogger(__name__)

# ── JWT config ────────────────────────────────────────────────────────────────
_JWT_SECRET    = os.getenv("JWT_SECRET", "")
_JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")

# Webhook secret
WEBHOOK_SECRET = os.getenv("LEAD_WEBHOOK_SECRET", "")

# ── Webhook backpressure ──────────────────────────────────────────────────────
MAX_WEBHOOK_INFLIGHT: int = 100
_webhook_inflight: int = 0

# ── skip_existing LIO send rate cap ──────────────────────────────────────────
_SKIP_LIO_CONCURRENCY = int(os.getenv("SKIP_LIO_CONCURRENCY", "50"))
_skip_lio_sem: asyncio.Semaphore = asyncio.Semaphore(_SKIP_LIO_CONCURRENCY)

# ── Outreach LLM concurrency cap ─────────────────────────────────────────────
_OUTREACH_CONCURRENCY = int(os.getenv("OUTREACH_CONCURRENCY", "30"))
_outreach_sem: asyncio.Semaphore = asyncio.Semaphore(_OUTREACH_CONCURRENCY)


# ─────────────────────────────────────────────────────────────────────────────
# Lead row Redis cache
# ─────────────────────────────────────────────────────────────────────────────

_LEAD_CACHE_TTL = 1800

_lead_redis: Any = None
_lead_redis_fail_until: float = 0.0


async def _lead_cache_ttl(org_id: str) -> int:
    try:
        sc = await cfg_svc.get_scoring_config(org_id)
        return int(sc.get("lead_cache_ttl", 1800))
    except Exception:
        return _LEAD_CACHE_TTL


async def _get_lead_redis() -> Any:
    global _lead_redis, _lead_redis_fail_until
    if _lead_redis is not None:
        return _lead_redis
    import time
    if time.monotonic() < _lead_redis_fail_until:
        return None
    try:
        import redis.asyncio as aioredis
        client = aioredis.from_url(
            os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            decode_responses=True,
            socket_connect_timeout=2,
        )
        await client.ping()
        _lead_redis = client
    except Exception:
        _lead_redis = None
        _lead_redis_fail_until = time.monotonic() + 30
    return _lead_redis


async def _lead_cache_get(lead_id: str) -> Optional[dict]:
    r = await _get_lead_redis()
    if not r:
        return None
    try:
        raw = await r.get(f"lead:row:{lead_id}")
        return json.loads(raw) if raw else None
    except Exception:
        return None


async def _lead_cache_set(lead_id: str, lead: dict) -> None:
    r = await _get_lead_redis()
    if not r:
        return
    try:
        await r.setex(f"lead:row:{lead_id}", _LEAD_CACHE_TTL, json.dumps(lead, default=str))
    except Exception:
        pass


async def _lead_cache_delete(lead_id: str) -> None:
    r = await _get_lead_redis()
    if not r:
        return
    try:
        await r.delete(f"lead:row:{lead_id}")
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# JWT / org helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_org_id(request: Request) -> str:
    return "default"


def _decode_token(token: str) -> dict:
    """
    Verify and decode JWT using JWT_SECRET.
    Returns decoded payload dict, or {} if invalid/expired.
    """
    if not _JWT_SECRET:
        logger.error("[Auth] JWT_SECRET not configured — cannot verify token")
        return {}
    try:
        return jwt.decode(token.strip(), _JWT_SECRET, algorithms=[_JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        logger.warning("[Auth] Token expired")
        return {}
    except jwt.InvalidTokenError as e:
        logger.warning("[Auth] Invalid token: %s", e)
        return {}


def _validate_token(token: Optional[str]) -> tuple[str, str]:
    """
    Verify JWT with JWT_SECRET and return (org_id, sso_id).
    Raises HTTP 401 if token is missing or signature invalid/expired.
    platform check is skipped for OAuth tokens (isOAuthToken=true or no platform field).
    """
    if not token:
        raise HTTPException(status_code=401, detail="Token required.")
    payload = _decode_token(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid or expired token.")
    platform = payload.get("platform")
    is_oauth = payload.get("isOAuthToken", False)
    if platform and not is_oauth and platform != "worksbuddy":
        raise HTTPException(status_code=401, detail="Invalid token: platform mismatch.")
    org_id = str(payload.get("organization_id", "default"))
    sso_id = str(
        payload.get("sso_id") or payload.get("sub") or
        payload.get("user_id") or payload.get("id") or ""
    )
    return org_id, sso_id


# ─────────────────────────────────────────────────────────────────────────────
# LIO shared helpers
# ─────────────────────────────────────────────────────────────────────────────

def _lio_fill(template: str, ctx: dict) -> str:
    """Replace {var} placeholders using regex — safe against literal JSON braces."""
    def _sub(m):
        key = m.group(1)
        val = ctx.get(key)
        return str(val) if val is not None else m.group(0)
    return _lio_re.sub(r'\{([a-zA-Z_][a-zA-Z0-9_]*)\}', _sub, template)


def _lio_strip_md(text: str) -> str:
    """Strip markdown code fences that some LLMs wrap JSON in."""
    text = text.strip()
    text = _lio_re.sub(r'^```(?:json)?\s*', '', text, flags=_lio_re.MULTILINE)
    text = _lio_re.sub(r'\s*```$', '', text, flags=_lio_re.MULTILINE)
    return text.strip()


# Stage index → DB result key
_LIO_RESULT_KEYS = {
    0: "company_intel", 1: "tags", 2: "signals",
    3: "buying", 4: "pitch", 5: "outreach", 6: "score"
}


async def _build_lio_ctx(lead: dict, ws_ctx: dict, prev: dict) -> dict:
    """Build the full template-variable context dict from a lead row + workspace + prev stage results."""
    import json as _json

    full_data: dict = {}
    try: full_data = _json.loads(lead.get("full_data") or "{}")
    except Exception: pass
    raw_profile: dict = {}
    try: raw_profile = _json.loads(lead.get("raw_profile") or "{}")
    except Exception: pass

    activity: list = raw_profile.get("activity", []) or []
    own_posts: list = raw_profile.get("posts", []) or []

    def _act_str(a) -> str:
        return (a.get("title") or a.get("attribution") or a.get("action") or a.get("text") or "")[:180]

    name_parts = (lead.get("name") or "").split()

    about_text = (raw_profile.get("about") or raw_profile.get("summary")
                  or raw_profile.get("headline") or lead.get("about") or "")
    raw_title = (raw_profile.get("title") or raw_profile.get("headline")
                 or full_data.get("title") or lead.get("title") or "")
    if not raw_title and about_text:
        m = _lio_re.search(
            r"(?:I(?:'m| am)(?: the)?\s+|^)(CEO|CTO|CFO|COO|CMO|Founder|Co-Founder|"
            r"Director|VP|Head of \w+|President|Partner|Managing Director|MD)",
            about_text, _lio_re.IGNORECASE
        )
        if m:
            raw_title = m.group(1)

    industry = (raw_profile.get("industry") or full_data.get("industry") or lead.get("industry") or "")
    if not industry:
        all_post_text = " ".join(_act_str(p) for p in own_posts[:5]).lower()
        if any(w in all_post_text for w in ("blockchain", "crypto", "web3", "defi", "nft")):
            industry = "Blockchain / Web3 Technology"
        elif any(w in all_post_text for w in ("saas", "software", "platform", "app")):
            industry = "Software / SaaS"
        elif any(w in all_post_text for w in ("fintech", "finance", "banking", "payment")):
            industry = "Fintech"
        elif any(w in all_post_text for w in ("ai", "artificial intelligence", "machine learning", "llm")):
            industry = "Artificial Intelligence"

    desc_parts = []
    for k in ("company_description", "company_about", "company_summary", "company_specialty", "company_overview"):
        v = full_data.get(k) or raw_profile.get(k)
        if v: desc_parts.append(str(v)[:400])
    if about_text:
        desc_parts.append(f"Founder/Leader bio: {about_text[:400]}")
    if own_posts:
        post_lines = "\n".join(f"  • {p.get('title','')[:120]}" for p in own_posts[:4] if p.get("title"))
        if post_lines:
            desc_parts.append(f"Published posts:\n{post_lines}")
    awards = raw_profile.get("honors_and_awards", []) or []
    if awards:
        award_lines = "; ".join(f"{a.get('title','')} — {a.get('publication','')}" for a in awards[:3])
        desc_parts.append(f"Awards: {award_lines}")
    edu = raw_profile.get("educations_details") or ""
    if edu:
        desc_parts.append(f"Education: {str(edu)[:200]}")
    followers = raw_profile.get("followers") or 0
    connections = raw_profile.get("connections") or 0
    if followers or connections:
        desc_parts.append(f"LinkedIn: {followers} followers, {connections}+ connections")

    company_description = "\n\n".join(desc_parts) or "No description available"

    def _act_line(a) -> str:
        interaction = a.get("interaction", "")
        title = _act_str(a)
        return f"- [{interaction}] {title}" if interaction else f"- {title}"

    recent_posts_str = "\n".join(_act_line(a) for a in (activity + own_posts)[:10]) or "No recent activity"
    recent_activity_str = "\n".join(_act_line(a) for a in activity[:12]) or "No activity data"

    def _s(raw) -> str:
        if isinstance(raw, (dict, list)): return _json.dumps(raw)
        return str(raw or "")

    def _parse(raw):
        if isinstance(raw, (dict, list)): return raw
        try: return _json.loads(raw)
        except Exception: return raw

    ci_obj     = _parse(prev.get("company_intel", "")) if prev.get("company_intel") else {}
    buying_obj = _parse(prev.get("buying", ""))        if prev.get("buying")        else {}
    pitch_obj  = _parse(prev.get("pitch", ""))         if prev.get("pitch")         else {}

    def _act_brief(a) -> dict:
        return {"action": a.get("interaction", ""), "title": _act_str(a)[:200]}

    raw_brightdata_json = _json.dumps({
        "name": f"{raw_profile.get('first_name','')} {raw_profile.get('last_name','')}".strip(),
        "about": about_text[:800] if about_text else None,
        "title": raw_title or None,
        "company": raw_profile.get("current_company_name") or raw_profile.get("current_company"),
        "location": raw_profile.get("city") or raw_profile.get("location"),
        "country_code": raw_profile.get("country_code"),
        "followers": raw_profile.get("followers"),
        "connections": raw_profile.get("connections"),
        "education": raw_profile.get("educations_details"),
        "posts": [
            {
                "title": p.get("title", ""),
                "date": (p.get("created_at", "") or "")[:10],
                "attribution": (p.get("attribution", "") or "")[:200],
            }
            for p in own_posts[:5]
        ],
        "activity": [_act_brief(a) for a in activity[:15]],
        "honors_and_awards": [
            {"title": a.get("title", ""), "publication": a.get("publication", "")}
            for a in (raw_profile.get("honors_and_awards", []) or [])[:3]
        ],
    }, ensure_ascii=False, indent=2)

    return {
        "raw_brightdata_json": raw_brightdata_json,
        "first_name":     raw_profile.get("first_name")  or (name_parts[0] if name_parts else ""),
        "last_name":      raw_profile.get("last_name")   or (" ".join(name_parts[1:]) if len(name_parts) > 1 else ""),
        "inferred_title": raw_title,
        "company":        raw_profile.get("current_company_name") or raw_profile.get("current_company") or lead.get("company") or "",
        "about":          about_text,
        "industry":       industry,
        "city":           (raw_profile.get("city") or raw_profile.get("location") or "").split(",")[0].strip(),
        "country":        raw_profile.get("country") or lead.get("location") or "",
        "company_description": company_description,
        "recent_posts":    recent_posts_str,
        "recent_activity": recent_activity_str,
        "total_score":       str(lead.get("total_score") or 0),
        "icp_fit_score":     str(lead.get("icp_score") or 0),
        "score_tier":        lead.get("score_tier") or "",
        "warm_signal_count": str(len(own_posts)),
        "email_status":      "found" if lead.get("email") else "not found",
        "product_name":      ws_ctx.get("product_name", ""),
        "value_proposition": ws_ctx.get("value_proposition", ""),
        "target_titles":     ws_ctx.get("target_titles", ""),
        "tone":              ws_ctx.get("tone", "professional"),
        "banned_phrases":    ws_ctx.get("banned_phrases", ""),
        "case_study":        ws_ctx.get("case_study", ""),
        "cta_style":         ws_ctx.get("cta_style", "question"),
        "company_intel":       _s(prev.get("company_intel", "")),
        "auto_tags":           _s(prev.get("tags", "[]")),
        "behavioural_signals": _s(prev.get("signals", "")),
        "buying_signals":      _s(prev.get("buying", "")),
        "pitch_intelligence":  _s(prev.get("pitch", "")),
        "company_stage":        ci_obj.get("company_stage", "")       if isinstance(ci_obj, dict)     else "",
        "intent_level":         buying_obj.get("intent_level", "")    if isinstance(buying_obj, dict) else "",
        "timing_score":         str(buying_obj.get("timing_score", 0)) if isinstance(buying_obj, dict) else "0",
        "trigger_events":       _json.dumps(buying_obj.get("trigger_events", [])) if isinstance(buying_obj, dict) else "[]",
        "personalization_hook": pitch_obj.get("personalization_hook", "") if isinstance(pitch_obj, dict) else "",
        "core_pain":            pitch_obj.get("core_pain", "")         if isinstance(pitch_obj, dict) else "",
    }


async def _lio_stage_stream(lead_id: str, stage_idx: int, system_prompt: str, user_prompt: str,
                             wb_llm_key_db, wb_llm_host_db, wb_llm_model_db):
    """Async generator: calls LLM with pre-filled prompts and streams SSE result."""
    import httpx as _httpx
    import json as _json

    async def _call_llm(system: str, user: str) -> str:
        errors = []
        wb_host = wb_llm_host_db or svc._wb_llm_host()
        wb_key  = wb_llm_key_db  or svc._wb_llm_key()
        wb_mdl  = wb_llm_model_db or svc._wb_llm_model()
        if wb_host and wb_key:
            try:
                async with _httpx.AsyncClient(timeout=90) as c:
                    r = await c.post(
                        f"{wb_host.rstrip('/')}/v1/chat/completions",
                        headers={"Content-Type": "application/json", "Authorization": f"Bearer {wb_key}"},
                        json={"model": wb_mdl,
                              "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
                              "temperature": 0.3, "max_tokens": 2000},
                    )
                    if r.is_success:
                        return r.json()["choices"][0]["message"]["content"].strip()
                    errors.append(f"WB LLM: HTTP {r.status_code}")
            except Exception as e:
                errors.append(f"WB LLM: {e}")
        hf_token = svc._hf_token()
        if hf_token:
            try:
                async with _httpx.AsyncClient(timeout=90) as c:
                    r = await c.post(
                        "https://router.huggingface.co/v1/chat/completions",
                        headers={"Content-Type": "application/json", "Authorization": f"Bearer {hf_token}"},
                        json={"model": svc._hf_model(),
                              "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
                              "temperature": 0.3, "max_tokens": 2000},
                    )
                    if r.is_success:
                        return r.json()["choices"][0]["message"]["content"].strip()
                    try:
                        body_txt = r.json().get("error", {}).get("message") or r.text
                    except Exception:
                        body_txt = r.text
                    errors.append(f"HuggingFace: HTTP {r.status_code}: {body_txt}")
            except Exception as e:
                errors.append(f"HuggingFace: {e}")
        raise RuntimeError(" | ".join(errors) if errors else "No LLM provider configured.")

    stage_name = _LIO_RESULT_KEYS.get(stage_idx, f"stage_{stage_idx}")
    yield f"data: {_json.dumps({'status': 'running', 'stage': stage_idx, 'stage_name': stage_name})}\n\n"
    try:
        result = await _call_llm(system_prompt, user_prompt)
        result = _lio_strip_md(result)
        try:
            _json.loads(result)
        except Exception:
            pass
        rk = _LIO_RESULT_KEYS.get(stage_idx)
        if rk:
            try:
                await svc.save_lead_lio_results(lead_id, {rk: result})
            except Exception as _e:
                logger.warning("[LIO save stage %d] %s", stage_idx, _e)
        yield f"data: {_json.dumps({'status': 'done', 'stage': stage_idx, 'stage_name': stage_name, 'result': result, 'done': True})}\n\n"
    except Exception as e:
        logger.error("[LIO Stage %d] %s", stage_idx, e)
        yield f"data: {_json.dumps({'error': str(e), 'done': True})}\n\n"


async def _lio_setup(lead_id: str, stage_idx: int, prev: dict, request: Request):
    """Load lead, prompt config, workspace context, and LLM keys for a stage (legacy)."""
    org_id = _get_org_id(request)
    lead = await svc.get_lead(lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    prompts = await cfg_svc.get_lio_prompts(org_id)
    if stage_idx < 0 or stage_idx >= len(prompts):
        raise HTTPException(status_code=400, detail=f"Stage {stage_idx} not configured")
    prompt_cfg = prompts[stage_idx]
    ws_ctx = await cfg_svc.get_workspace_context(org_id)
    wb_row        = await cfg_svc.get_tool_row(org_id, "wb_llm")
    wb_llm_key_db = (wb_row or {}).get("api_key") or None
    wb_extra: dict = {}
    try:
        wb_extra = json.loads((wb_row or {}).get("extra_config") or "{}") if wb_row else {}
    except Exception:
        pass
    wb_llm_host_db  = wb_extra.get("host") or None
    wb_llm_model_db = wb_extra.get("model") or None
    ctx = await _build_lio_ctx(lead, ws_ctx, prev)
    return lead_id, stage_idx, prompt_cfg, ctx, wb_llm_key_db, wb_llm_host_db, wb_llm_model_db


async def _lio_llm_keys(request: Request):
    """Fetch LLM provider keys for the org — used by named LIO routes."""
    org_id = _get_org_id(request)
    wb_row        = await cfg_svc.get_tool_row(org_id, "wb_llm")
    wb_llm_key_db = (wb_row or {}).get("api_key") or None
    wb_extra: dict = {}
    try:
        wb_extra = json.loads((wb_row or {}).get("extra_config") or "{}") if wb_row else {}
    except Exception:
        pass
    return wb_llm_key_db, wb_extra.get("host"), wb_extra.get("model")


# ─────────────────────────────────────────────────────────────────────────────
# Request / Response models
# ─────────────────────────────────────────────────────────────────────────────

def _validate_linkedin_url(v: str) -> str:
    """Accept both person (linkedin.com/in/) and company (linkedin.com/company/) URLs."""
    v = v.strip()
    if not v:
        raise ValueError("linkedin_url is required")
    v_lower = v.lower()
    if "linkedin.com" not in v_lower:
        raise ValueError("Must be a LinkedIn URL (linkedin.com/in/... or linkedin.com/company/...)")
    if "/in/" not in v_lower and "/company/" not in v_lower:
        raise ValueError("URL must be a person profile (linkedin.com/in/...) or company page (linkedin.com/company/...)")
    return v


class BulkEnrichRequest(BaseModel):
    linkedin_urls: list[str]
    webhook_url: Optional[str] = None
    notify_url: Optional[str] = None
    webhook_auth: Optional[str] = None
    token: Optional[str] = None
    skip_existing: bool = True
    forward_to_lio: bool = False
    system_prompt: Optional[str] = None

    @field_validator("linkedin_urls")
    @classmethod
    def validate_urls(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("linkedin_urls must not be empty")
        if len(v) > 5000:
            raise ValueError("Maximum 5000 URLs per batch")
        cleaned = []
        for url in v:
            url = url.strip()
            url_lower = url.lower()
            if url and "linkedin.com" in url_lower and ("/in/" in url_lower or "/company/" in url_lower):
                cleaned.append(url)
        if not cleaned:
            raise ValueError("No valid LinkedIn URLs found (must be /in/ person or /company/ page URLs)")
        seen = set()
        deduped = []
        for url in cleaned:
            if url not in seen:
                seen.add(url)
                deduped.append(url)
        return deduped


class RegenerateOutreachRequest(BaseModel):
    pass


# ─────────────────────────────────────────────────────────────────────────────
# Response formatter
# ─────────────────────────────────────────────────────────────────────────────

def _format_lead(lead: dict, full: bool = False) -> dict:
    """Parse JSON fields and format for API response."""
    out = {k: v for k, v in lead.items() if k not in ("raw_profile", "skills")}

    try:
        out["score_reasons"] = json.loads(lead.get("score_reasons") or "[]")
    except Exception:
        out["score_reasons"] = []

    try:
        out["outreach_sequence"] = json.loads(lead.get("outreach_sequence") or "{}")
    except Exception:
        out["outreach_sequence"] = {}

    try:
        out["skills"] = json.loads(lead.get("skills") or "[]")
    except Exception:
        out["skills"] = []

    try:
        out["full_data"] = json.loads(lead.get("full_data") or "{}")
    except Exception:
        out["full_data"] = {}

    if full:
        try:
            out["raw_profile"] = json.loads(lead.get("raw_profile") or "{}")
        except Exception:
            out["raw_profile"] = {}

    return out

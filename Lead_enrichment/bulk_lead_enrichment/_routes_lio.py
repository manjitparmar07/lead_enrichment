"""
_routes_lio.py
--------------
All LIO routes: lio/prompt, lio/config, lio/workspace, lio/status, lio/results,
named stage endpoints, lio/analyze.

Lines ~2803–3572 of the original lead_enrichment_brightdata_routes.py.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from Lead_enrichment.bulk_lead_enrichment import lead_enrichment_brightdata_service as svc
from config import enrichment_config_service as cfg_svc

from Lead_enrichment.bulk_lead_enrichment._shared import (
    _get_org_id,
    _lio_fill,
    _lio_strip_md,
    _LIO_RESULT_KEYS,
    _lio_stage_stream,
    _lio_setup,
    _lio_llm_keys,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Lead Enrichment"])

# ─────────────────────────────────────────────────────────────────────────────
# LIO Request Models
# ─────────────────────────────────────────────────────────────────────────────

class LioPromptRequest(BaseModel):
    system_prompt: str
    model: Optional[str] = None


class LioPromptsConfigRequest(BaseModel):
    prompts: list


class WorkspaceContextRequest(BaseModel):
    product_name:      Optional[str] = None
    value_proposition: Optional[str] = None
    target_titles:     Optional[str] = None
    tone:              Optional[str] = None
    banned_phrases:    Optional[str] = None
    case_study:        Optional[str] = None
    cta_style:         Optional[str] = None


class LioStageRequest(BaseModel):
    stage_index: int
    context: dict = {}


class LioContextBody(BaseModel):
    system_prompt: str = ""
    user_prompt: str = ""
    brightdata_json: dict = {}
    stage_idx: int = 0


# ─────────────────────────────────────────────────────────────────────────────
# SSE helpers
# ─────────────────────────────────────────────────────────────────────────────

_SSE_HEADERS = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}

_STAGE_MODEL_DEFAULTS = {
    0: "llama-3.1-8b-instant",
    1: "llama-3.1-8b-instant",
    2: "llama-3.1-8b-instant",
    3: "llama-3.1-8b-instant",
    4: "llama-3.3-70b-versatile",
    5: "llama-3.3-70b-versatile",
    6: "llama-3.1-8b-instant",
}


async def _lio_stage_model(org_id: str, stage_idx: int) -> str:
    """Read the model for a specific LIO stage from DB; fall back to hardcoded default."""
    try:
        prompts = await cfg_svc.get_lio_prompts(org_id)
        if 0 <= stage_idx < len(prompts):
            db_model = prompts[stage_idx].get("model", "").strip()
            if db_model:
                return db_model
    except Exception:
        pass
    return _STAGE_MODEL_DEFAULTS.get(stage_idx, "llama-3.1-8b-instant")


def _lio_stream_resp(lead_id: str, body: LioContextBody, keys: tuple, model: str):
    wb_key, wb_host, wb_model = keys
    return StreamingResponse(
        _lio_stage_stream(lead_id, body.stage_idx, body.system_prompt, body.user_prompt,
                          wb_key, wb_host, wb_model),
        media_type="text/event-stream", headers=_SSE_HEADERS,
    )


# ─────────────────────────────────────────────────────────────────────────────
# LIO Routes
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/lio/prompt", include_in_schema=False)
async def get_lio_prompt(request: Request):
    """Get the saved LIO system prompt + model for this org."""
    org_id = _get_org_id(request)
    cfg = await cfg_svc.get_workspace_config(org_id)
    return {
        "system_prompt": cfg.get("lio_system_prompt", ""),
        "model":         cfg.get("lio_model", ""),
    }


@router.post("/lio/prompt", include_in_schema=False)
async def save_lio_prompt(body: LioPromptRequest, request: Request):
    """Save the LIO system prompt (and optional model) for this org."""
    org_id = _get_org_id(request)
    updates = {"lio_system_prompt": body.system_prompt}
    if body.model is not None:
        updates["lio_model"] = body.model
    await cfg_svc.save_workspace_config(org_id, updates)
    return {"success": True}


@router.get("/lio/config", include_in_schema=False)
async def get_lio_config(request: Request):
    """Get the 5 LIO prompt configurations for this org."""
    org_id = _get_org_id(request)
    prompts = await cfg_svc.get_lio_prompts(org_id)
    return {"prompts": prompts}


@router.put("/lio/config", include_in_schema=False)
async def save_lio_config(body: LioPromptsConfigRequest, request: Request):
    """Save the 5 LIO prompt configurations for this org."""
    org_id = _get_org_id(request)
    await cfg_svc.save_lio_prompts(org_id, body.prompts)
    return {"success": True}


@router.get("/lio/workspace", include_in_schema=False)
async def get_lio_workspace(request: Request):
    """Get workspace product context (product_name, value_prop, tone, etc.)."""
    org_id = _get_org_id(request)
    ctx = await cfg_svc.get_workspace_context(org_id)
    return ctx


@router.put("/lio/workspace", include_in_schema=False)
async def save_lio_workspace(body: WorkspaceContextRequest, request: Request):
    """Save workspace product context."""
    org_id = _get_org_id(request)
    updates = {k: v for k, v in body.model_dump().items() if v is not None}
    existing = await cfg_svc.get_workspace_context(org_id)
    existing.update(updates)
    await cfg_svc.save_workspace_context(org_id, existing)
    return {"success": True}


@router.get("/lio/status", include_in_schema=False)
async def get_lio_status(request: Request):
    """
    Return LIO provider status: which LLM providers have API keys configured,
    and which LLM model is selected for LIO analysis.
    """
    org_id = _get_org_id(request)
    wb_row    = await cfg_svc.get_tool_row(org_id, "wb_llm")
    workspace = await cfg_svc.get_workspace_config(org_id)

    wb_configured = bool(wb_row and wb_row.get("api_key") or wb_row and (wb_row.get("extra_config") or "{}") != "{}")
    wb_enabled    = bool(wb_row and wb_row.get("is_enabled"))

    hf_configured = bool(os.getenv("HF_TOKEN"))

    if not wb_configured and os.getenv("WB_LLM_API_KEY"):
        wb_configured = True
        wb_enabled = True

    return {
        "wb_llm":          {"configured": wb_configured, "enabled": wb_enabled},
        "huggingface":     {"configured": hf_configured, "enabled": hf_configured},
        "active_provider": "wb_llm" if (wb_configured and wb_enabled) else ("huggingface" if hf_configured else None),
        "lio_model":       workspace.get("lio_model", ""),
    }


@router.get("/lio/results/{lead_id}", include_in_schema=False)
async def get_lio_results(lead_id: str, request: Request):
    """Return persisted LIO stage results for a lead."""
    lead = await svc.get_lead(lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    results = await svc.get_lead_lio_results(lead_id)
    return {"lead_id": lead_id, "results": results}


# ── 7 named LIO stage endpoints ──────────────────────────────────────────────

@router.post("/{lead_id}/lio/company-intelligence", include_in_schema=False)
async def lio_company_intelligence(lead_id: str, body: LioContextBody, request: Request):
    """Stage 0 — Company Intelligence."""
    org_id = _get_org_id(request)
    keys = await _lio_llm_keys(request)
    return _lio_stream_resp(lead_id, body, keys, await _lio_stage_model(org_id, 0))


@router.post("/{lead_id}/lio/auto-tags", include_in_schema=False)
async def lio_auto_tags(lead_id: str, body: LioContextBody, request: Request):
    """Stage 1 — Auto Tags."""
    org_id = _get_org_id(request)
    keys = await _lio_llm_keys(request)
    return _lio_stream_resp(lead_id, body, keys, await _lio_stage_model(org_id, 1))


@router.post("/{lead_id}/lio/behavioural-signals", include_in_schema=False)
async def lio_behavioural_signals(lead_id: str, body: LioContextBody, request: Request):
    """Stage 2 — Behavioural Signals."""
    org_id = _get_org_id(request)
    keys = await _lio_llm_keys(request)
    return _lio_stream_resp(lead_id, body, keys, await _lio_stage_model(org_id, 2))


@router.post("/{lead_id}/lio/buying-signals", include_in_schema=False)
async def lio_buying_signals(lead_id: str, body: LioContextBody, request: Request):
    """Stage 3 — Buying Signals."""
    org_id = _get_org_id(request)
    keys = await _lio_llm_keys(request)
    return _lio_stream_resp(lead_id, body, keys, await _lio_stage_model(org_id, 3))


@router.post("/{lead_id}/lio/pitch-intelligence", include_in_schema=False)
async def lio_pitch_intelligence(lead_id: str, body: LioContextBody, request: Request):
    """Stage 4 — Pitch Intelligence."""
    org_id = _get_org_id(request)
    keys = await _lio_llm_keys(request)
    return _lio_stream_resp(lead_id, body, keys, await _lio_stage_model(org_id, 4))


@router.post("/{lead_id}/lio/outreach", include_in_schema=False)
async def lio_outreach(lead_id: str, body: LioContextBody, request: Request):
    """Stage 5 — Outreach Generator."""
    org_id = _get_org_id(request)
    keys = await _lio_llm_keys(request)
    return _lio_stream_resp(lead_id, body, keys, await _lio_stage_model(org_id, 5))


@router.post("/{lead_id}/lio/lead-score", include_in_schema=False)
async def lio_lead_score(lead_id: str, body: LioContextBody, request: Request):
    """Stage 6 — Lead Score."""
    org_id = _get_org_id(request)
    keys = await _lio_llm_keys(request)
    return _lio_stream_resp(lead_id, body, keys, await _lio_stage_model(org_id, 6))


@router.post("/lio/analyze/{lead_id}/stage", include_in_schema=False)
async def lio_analyze_stage(lead_id: str, body: LioStageRequest, request: Request):
    """
    Run ONE LIO stage and stream result as SSE.
    Pass accumulated previous stage results in body.context.

    Result keys by stage:
      0 → company_intel   1 → tags   2 → signals   3 → buying
      4 → pitch           5 → outreach              6 → score

    SSE events:
      {"status": "running", "stage": N, "stage_name": "..."}
      {"status": "done",    "stage": N, "stage_name": "...", "result": "..."}
      {"error": "...", "done": true}
    """
    org_id = _get_org_id(request)

    lead = await svc.get_lead(lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    prompts = await cfg_svc.get_lio_prompts(org_id)
    si = body.stage_index
    if si < 0 or si >= len(prompts):
        raise HTTPException(status_code=400, detail=f"Invalid stage_index {si} (0–{len(prompts)-1})")

    prompt_cfg = prompts[si]
    ws_ctx     = await cfg_svc.get_workspace_context(org_id)
    wb_row        = await cfg_svc.get_tool_row(org_id, "wb_llm")
    wb_llm_key_db = (wb_row or {}).get("api_key") or None
    wb_extra: dict = {}
    try:
        wb_extra = json.loads((wb_row or {}).get("extra_config") or "{}") if wb_row else {}
    except Exception:
        pass
    wb_llm_host_db  = wb_extra.get("host") or None
    wb_llm_model_db = wb_extra.get("model") or None

    # ── Parse lead profile ──────────────────────────────────────────────────
    full_data: dict = {}
    try: full_data = json.loads(lead.get("full_data") or "{}")
    except Exception: pass
    raw_profile: dict = {}
    try: raw_profile = json.loads(lead.get("raw_profile") or "{}")
    except Exception: pass

    activity: list = raw_profile.get("activity", []) or []
    own_posts: list = raw_profile.get("posts", []) or []

    def _act_str(a) -> str:
        return (a.get("title") or a.get("attribution") or a.get("action") or a.get("text") or "")[:180]

    name_parts = (lead.get("name") or "").split()

    import re as _re
    about_text = (raw_profile.get("about") or raw_profile.get("summary")
                  or raw_profile.get("headline") or lead.get("about") or "")
    raw_title = (raw_profile.get("title") or raw_profile.get("headline")
                 or full_data.get("title") or lead.get("title") or "")
    if not raw_title and about_text:
        m = _re.search(
            r"(?:I(?:'m| am)(?: the)?\s+|^)(CEO|CTO|CFO|COO|CMO|Founder|Co-Founder|"
            r"Director|VP|Head of \w+|President|Partner|Managing Director|MD)",
            about_text, _re.IGNORECASE
        )
        if m:
            raw_title = m.group(1)

    industry = (raw_profile.get("industry") or full_data.get("industry")
                or lead.get("industry") or "")
    if not industry and about_text:
        all_post_text = " ".join(_act_str(p) for p in own_posts[:5]).lower()
        if any(w in all_post_text for w in ("blockchain", "crypto", "web3", "defi", "nft")):
            industry = "Blockchain / Web3 Technology"
        elif any(w in all_post_text for w in ("ai", "artificial intelligence", "machine learning", "llm")):
            industry = "Artificial Intelligence"
        elif any(w in all_post_text for w in ("saas", "software", "platform", "app")):
            industry = "Software / SaaS"
        elif any(w in all_post_text for w in ("fintech", "finance", "banking", "payment")):
            industry = "Fintech"

    company_desc_parts = []
    for k in ("company_description", "company_about", "company_summary",
              "company_specialty", "company_overview"):
        v = full_data.get(k) or raw_profile.get(k)
        if v: company_desc_parts.append(str(v)[:400])
    if about_text:
        company_desc_parts.append(f"Founder/Leader bio: {about_text[:400]}")
    if own_posts:
        post_lines = "\n".join(
            f"  • {p.get('title', '')[:120]}" for p in own_posts[:4] if p.get("title")
        )
        if post_lines:
            company_desc_parts.append(f"Published posts (topics they care about):\n{post_lines}")
    awards = raw_profile.get("honors_and_awards", []) or []
    if awards:
        award_lines = "; ".join(
            f"{a.get('title','')} — {a.get('publication','')}" for a in awards[:3]
        )
        company_desc_parts.append(f"Awards/recognition: {award_lines}")
    edu = raw_profile.get("educations_details") or ""
    if edu:
        company_desc_parts.append(f"Education: {str(edu)[:200]}")
    followers = raw_profile.get("followers") or 0
    connections = raw_profile.get("connections") or 0
    if followers or connections:
        company_desc_parts.append(
            f"LinkedIn presence: {followers} followers, {connections}+ connections"
        )

    company_description = "\n\n".join(company_desc_parts) or "No company description available"

    def _act_feed_line(a) -> str:
        interaction = a.get("interaction", "")
        title = _act_str(a)
        return f"- [{interaction}] {title}" if interaction else f"- {title}"

    recent_posts_str = (
        "\n".join(_act_feed_line(a) for a in (activity + own_posts)[:10])
        or "No recent activity"
    )
    recent_activity_str = (
        "\n".join(_act_feed_line(a) for a in activity[:12])
        or "No activity data"
    )

    prev = body.context or {}

    def _s(raw) -> str:
        if isinstance(raw, (dict, list)): return json.dumps(raw)
        return str(raw or "")

    def _parse(raw) -> Any:
        if isinstance(raw, (dict, list)): return raw
        try: return json.loads(raw)
        except Exception: return raw

    ci_obj     = _parse(prev.get("company_intel", "")) if prev.get("company_intel") else {}
    buying_obj = _parse(prev.get("buying", ""))        if prev.get("buying")        else {}
    pitch_obj  = _parse(prev.get("pitch", ""))         if prev.get("pitch")         else {}

    ctx: dict = {
        "first_name":     raw_profile.get("first_name")  or (name_parts[0] if name_parts else ""),
        "last_name":      raw_profile.get("last_name")   or (" ".join(name_parts[1:]) if len(name_parts) > 1 else ""),
        "inferred_title": raw_title,
        "company":        (raw_profile.get("current_company_name") or raw_profile.get("current_company")
                           or lead.get("company") or ""),
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
        "company_stage":        ci_obj.get("company_stage", "")      if isinstance(ci_obj, dict)     else "",
        "intent_level":         buying_obj.get("intent_level", "")   if isinstance(buying_obj, dict) else "",
        "timing_score":         str(buying_obj.get("timing_score", 0)) if isinstance(buying_obj, dict) else "0",
        "trigger_events":       json.dumps(buying_obj.get("trigger_events", [])) if isinstance(buying_obj, dict) else "[]",
        "personalization_hook": pitch_obj.get("personalization_hook", "") if isinstance(pitch_obj, dict) else "",
        "core_pain":            pitch_obj.get("core_pain", "")        if isinstance(pitch_obj, dict) else "",
    }

    async def _stream():
        import httpx as _httpx

        def _fill(template: str, ctx: dict) -> str:
            import re as _re
            def _sub(m):
                key = m.group(1)
                val = ctx.get(key)
                return str(val) if val is not None else m.group(0)
            return _re.sub(r'\{([a-zA-Z_][a-zA-Z0-9_]*)\}', _sub, template)

        async def _call(system: str, user: str, model: str, temp: float) -> str:
            errors = []
            wb_host = wb_llm_host_db or svc._wb_llm_host()
            wb_key  = wb_llm_key_db  or svc._wb_llm_key()
            wb_mdl  = wb_llm_model_db or svc._wb_llm_model()
            if wb_host and wb_key:
                try:
                    async with _httpx.AsyncClient(timeout=90) as c:
                        r = await c.post(f"{wb_host.rstrip('/')}/v1/chat/completions",
                            headers={"Content-Type": "application/json", "Authorization": f"Bearer {wb_key}"},
                            json={"model": wb_mdl, "messages": [{"role":"system","content":system},{"role":"user","content":user}], "temperature": temp, "max_tokens": 2000})
                        if r.is_success: return r.json()["choices"][0]["message"]["content"].strip()
                        errors.append(f"WB LLM: HTTP {r.status_code}")
                except Exception as e: errors.append(f"WB LLM: {e}")
            hf_token = svc._hf_token()
            if hf_token:
                try:
                    async with _httpx.AsyncClient(timeout=90) as c:
                        r = await c.post("https://router.huggingface.co/v1/chat/completions",
                            headers={"Content-Type": "application/json", "Authorization": f"Bearer {hf_token}"},
                            json={"model": svc._hf_model(), "messages": [{"role":"system","content":system},{"role":"user","content":user}], "temperature": temp, "max_tokens": 2000})
                        if r.is_success: return r.json()["choices"][0]["message"]["content"].strip()
                        body_txt = ""
                        try: body_txt = r.json().get("error", {}).get("message") or r.text
                        except Exception: body_txt = r.text
                        errors.append(f"HuggingFace: HTTP {r.status_code}: {body_txt}")
                except Exception as e: errors.append(f"HuggingFace: {e}")
            raise RuntimeError(" | ".join(errors) if errors else "No LLM provider configured — add HF_TOKEN or WB_LLM_HOST.")

        import re as _re

        def _strip_md(text: str) -> str:
            text = text.strip()
            text = _re.sub(r'^```(?:json)?\s*', '', text, flags=_re.MULTILINE)
            text = _re.sub(r'\s*```$', '', text, flags=_re.MULTILINE)
            return text.strip()

        _RESULT_KEYS = {0: "company_intel", 1: "tags", 2: "signals",
                        3: "buying", 4: "pitch", 5: "outreach", 6: "score"}

        stage_name = prompt_cfg.get("name", f"Stage {si}")
        yield f"data: {json.dumps({'status': 'running', 'stage': si, 'stage_name': stage_name})}\n\n"
        try:
            user_prompt = _fill(prompt_cfg.get("user_template", ""), ctx)
            result = await _call(
                prompt_cfg.get("system", ""),
                user_prompt,
                prompt_cfg.get("model", "llama-3.1-8b-instant"),
                float(prompt_cfg.get("temperature", 0.3)),
            )
            result = _strip_md(result)
            rk = _RESULT_KEYS.get(si)
            if rk:
                try:
                    await svc.save_lead_lio_results(lead_id, {rk: result})
                except Exception as _e:
                    logger.warning("[LIO save stage %d] %s", si, _e)
            yield f"data: {json.dumps({'status': 'done', 'stage': si, 'stage_name': stage_name, 'result': result, 'done': True})}\n\n"
        except Exception as e:
            logger.error("[LIO Stage %d] %s", si, e)
            yield f"data: {json.dumps({'error': str(e), 'done': True})}\n\n"

    return StreamingResponse(_stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@router.post("/lio/analyze/{lead_id}", include_in_schema=False)
async def lio_analyze(lead_id: str, request: Request):
    """
    Run LIO 5-step enrichment chain. Each step uses its own prompt config,
    passes results to subsequent steps, and streams SSE events per step.

    SSE event shapes:
      {"step": N, "step_name": "...", "status": "running"|"done"|"error", "result"?: "...", "total": 5}
      {"done": true, "result": {"Auto Tags": "...", ...}}   ← final
      {"error": "...", "done": true}                        ← fatal error
    """
    org_id = _get_org_id(request)

    lead = await svc.get_lead(lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    # ── Load 5-prompt configs + workspace context ───────────────────────────
    prompts   = await cfg_svc.get_lio_prompts(org_id)
    ws_ctx    = await cfg_svc.get_workspace_context(org_id)

    # ── Load API keys from DB tool configs ─────────────────────────────────
    wb_row        = await cfg_svc.get_tool_row(org_id, "wb_llm")
    wb_llm_key_db = (wb_row or {}).get("api_key") or None
    wb_extra: dict = {}
    try:
        wb_extra = json.loads((wb_row or {}).get("extra_config") or "{}") if wb_row else {}
    except Exception:
        pass
    wb_llm_host_db  = wb_extra.get("host") or None
    wb_llm_model_db = wb_extra.get("model") or None

    # ── Parse lead profile data ─────────────────────────────────────────────
    full_data: dict = {}
    try: full_data = json.loads(lead.get("full_data") or "{}")
    except Exception: pass
    raw_profile: dict = {}
    try: raw_profile = json.loads(lead.get("raw_profile") or "{}")
    except Exception: pass

    # ── Extract activity / post signals ─────────────────────────────────────
    activity: list = raw_profile.get("activity", []) or []
    posts:    list = raw_profile.get("posts", []) or []

    def _act_title(a) -> str:
        return (a.get("title") or a.get("action") or a.get("text") or "")[:150]

    top_3_activity_titles = "\n".join(f"- {_act_title(a)}" for a in activity[:3]) or "No recent activity"
    activity_sample       = "\n".join(f"- {_act_title(a)}" for a in activity[:10]) or "No activity data"
    warm_signal_posts     = "\n".join(
        f"- {p.get('title') or p.get('text', '')[:150]}" for p in posts[:5]
    ) or "No brand engagement found"
    warm_signal_posts_titles = "\n".join(
        f"- {p.get('title') or p.get('text', '')[:80]}" for p in posts[:3]
    ) or "None"

    name_parts = (lead.get("name") or "").split()
    base_ctx: dict = {
        "first_name":    raw_profile.get("first_name")  or (name_parts[0] if name_parts else ""),
        "last_name":     raw_profile.get("last_name")   or (" ".join(name_parts[1:]) if len(name_parts) > 1 else ""),
        "inferred_title": raw_profile.get("title")      or lead.get("title") or "",
        "company":       (raw_profile.get("current_company_name") or raw_profile.get("current_company")
                          or lead.get("company") or ""),
        "about":         raw_profile.get("about") or raw_profile.get("summary") or raw_profile.get("headline") or "",
        "industry":      raw_profile.get("industry") or full_data.get("industry") or "",
        "city":          raw_profile.get("city") or "",
        "country":       raw_profile.get("country") or lead.get("location") or "",
        "top_3_activity_titles":  top_3_activity_titles,
        "activity_sample":        activity_sample,
        "warm_signal_posts":      warm_signal_posts,
        "warm_signal_posts_titles": warm_signal_posts_titles,
        "warm_signal_count":      str(len(posts)),
        "total_score":          str(lead.get("total_score") or 0),
        "icp_fit_score":        str(lead.get("icp_score") or 0),
        "intent_score":         str(lead.get("intent_score") or 0),
        "timing_score":         str(full_data.get("timing_score") or 0),
        "score_tier":           lead.get("score_tier") or "",
        "score_anomaly":        str(full_data.get("score_anomaly") or False),
        "score_anomaly_reason": str(full_data.get("score_anomaly_reason") or ""),
        "email_status":         "found" if lead.get("email") else "not found",
        "behavioural_signals": "",
        "top_pain_point": "", "best_value_prop": "", "trigger_event": "",
        "best_angle": "", "do_not_pitch": "", "suggested_cta": "", "opening_line": "",
        "product_name":      ws_ctx.get("product_name", ""),
        "value_proposition": ws_ctx.get("value_proposition", ""),
        "target_titles":     ws_ctx.get("target_titles", ""),
        "tone":              ws_ctx.get("tone", "professional"),
        "banned_phrases":    ws_ctx.get("banned_phrases", ""),
        "case_study":        ws_ctx.get("case_study", ""),
        "cta_style":         ws_ctx.get("cta_style", "question"),
    }

    async def stream_steps():
        import httpx as _httpx

        def _fill(template: str, ctx: dict) -> str:
            import re as _re
            def _sub(m):
                key = m.group(1)
                val = ctx.get(key)
                return str(val) if val is not None else m.group(0)
            return _re.sub(r'\{([a-zA-Z_][a-zA-Z0-9_]*)\}', _sub, template)

        async def _call_llm(system: str, user: str, model: str, temperature: float) -> str:
            """Try WB LLM (if configured) then HuggingFace. Raises on all failures."""
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
                                  "temperature": temperature, "max_tokens": 2000},
                        )
                        if r.is_success:
                            return r.json()["choices"][0]["message"]["content"].strip()
                        body = ""
                        try: body = r.json().get("error", {}).get("message") or r.text
                        except Exception: body = r.text
                        errors.append(f"WB LLM: HTTP {r.status_code}: {body}")
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
                                  "temperature": temperature, "max_tokens": 2000},
                        )
                        if r.is_success:
                            return r.json()["choices"][0]["message"]["content"].strip()
                        body = ""
                        try: body = r.json().get("error", {}).get("message") or r.text
                        except Exception: body = r.text
                        errors.append(f"HuggingFace: HTTP {r.status_code}: {body}")
                except Exception as e:
                    errors.append(f"HuggingFace: {e}")

            raise RuntimeError(
                " | ".join(errors) if errors
                else "No LLM provider configured — add HF_TOKEN or WB_LLM_HOST."
            )

        ctx = dict(base_ctx)
        step_results: dict = {}
        total = len(prompts)

        for i, prompt_cfg in enumerate(prompts):
            step_num  = i + 1
            step_name = prompt_cfg.get("name", f"Step {step_num}")
            model     = prompt_cfg.get("model", "llama-3.1-8b-instant")
            temp      = float(prompt_cfg.get("temperature", 0.3))
            system    = prompt_cfg.get("system", "")
            user_tpl  = prompt_cfg.get("user_template", "")

            yield f"data: {json.dumps({'step': step_num, 'step_name': step_name, 'status': 'running', 'total': total})}\n\n"

            try:
                user_prompt = _fill(user_tpl, ctx)
                result = await _call_llm(system, user_prompt, model, temp)
                step_results[step_name] = result

                # Feed results into context for downstream steps
                if step_num == 2:
                    ctx["behavioural_signals"] = result
                elif step_num == 3:
                    try:
                        pitch = json.loads(result)
                        ctx["top_pain_point"]  = pitch.get("top_pain_point", "")
                        ctx["best_value_prop"] = pitch.get("best_value_prop", "")
                        ctx["trigger_event"]   = str(pitch.get("trigger_event") or "")
                        ctx["best_angle"]      = pitch.get("best_angle", "")
                        ctx["do_not_pitch"]    = ", ".join(pitch.get("do_not_pitch") or [])
                        ctx["suggested_cta"]   = pitch.get("suggested_cta", "")
                        ctx["opening_line"]    = pitch.get("opening_line", "")
                    except Exception:
                        ctx["top_pain_point"] = result[:300]
                elif step_num == 4:
                    try:
                        outreach = json.loads(result)
                        if not ctx.get("trigger_event"):
                            ctx["trigger_event"] = outreach.get("email_subject", "")
                    except Exception:
                        pass

                yield f"data: {json.dumps({'step': step_num, 'step_name': step_name, 'status': 'done', 'result': result, 'total': total})}\n\n"

            except Exception as e:
                error_msg = str(e)
                logger.error("[LIO] Step %d (%s) failed: %s", step_num, step_name, error_msg)
                yield f"data: {json.dumps({'step': step_num, 'step_name': step_name, 'status': 'error', 'error': error_msg, 'total': total})}\n\n"
                yield f"data: {json.dumps({'error': f'Step {step_num} ({step_name}): {error_msg}', 'done': True})}\n\n"
                return

        yield f"data: {json.dumps({'done': True, 'result': step_results})}\n\n"

    return StreamingResponse(
        stream_steps(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )

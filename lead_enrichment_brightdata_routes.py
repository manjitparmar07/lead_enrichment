"""
lead_enrichment_brightdata_routes.py
--------------------------------------
FastAPI router for Worksbuddy Lead Enrichment (Bright Data powered).

Endpoints:
  POST   /api/leads/enrich            — single LinkedIn URL (sync, ~60s)
  POST   /api/leads/enrich/bulk       — bulk URLs (async, returns job_id)
  GET    /api/leads/jobs              — list all jobs
  GET    /api/leads/jobs/{job_id}     — job status + progress
  POST   /api/leads/webhook/brightdata — Bright Data batch results webhook
  POST   /api/leads/webhook/notify    — Bright Data snapshot-ready notification
  GET    /api/leads                   — list enriched leads (filterable)
  GET    /api/leads/{lead_id}         — single lead detail
  POST   /api/leads/{lead_id}/outreach — regenerate outreach copy
  DELETE /api/leads/{lead_id}         — delete lead
  GET    /api/leads/export/csv        — export leads as CSV
"""

from __future__ import annotations

import base64
import csv
import io
import json
import logging
import os
from typing import Any, Dict, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, HttpUrl, field_validator

import lead_enrichment_brightdata_service as svc
import enrichment_config_service as cfg_svc

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/leads", tags=["Lead Enrichment"])

# Webhook secret — set LEAD_WEBHOOK_SECRET in .env to protect incoming webhooks
WEBHOOK_SECRET = os.getenv("LEAD_WEBHOOK_SECRET", "")


def _get_org_id(request: Request) -> str:
    """
    Extract organization_id from the JWT in the Authorization header.
    Decodes without signature verification (trust: upstream SSO already validated it).
    Falls back to 'default' if no token or decode fails.
    """
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return "default"
    token = auth[7:].strip()
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return "default"
        payload_b64 = parts[1]
        # Re-add padding
        payload_b64 += "=" * (4 - len(payload_b64) % 4)
        payload = json.loads(base64.b64decode(payload_b64))
        return str(payload.get("organization_id", "default"))
    except Exception:
        return "default"


# ── LIO shared helpers ───────────────────────────────────────────────────────

import re as _lio_re

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

    # Infer title from about bio if missing
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

    # Infer industry from post topics if missing
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

    # Build rich company description
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

    # Build compact raw JSON for LLM (actual BrightData fields, truncated)
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
                             groq_api_key, wb_llm_key_db, wb_llm_host_db, wb_llm_model_db,
                             groq_model: str = "llama-3.3-70b-versatile"):
    """Async generator: calls LLM with pre-filled prompts and streams SSE result."""
    import httpx as _httpx
    import json as _json

    # Stage 1 (auto-tags) returns a JSON array — json_object mode not valid for arrays
    _use_json_object = stage_idx != 1

    async def _call_llm(system: str, user: str) -> str:
        errors = []
        wb_host = wb_llm_host_db or svc._wb_llm_host()
        wb_key  = wb_llm_key_db  or svc._wb_llm_key()
        wb_mdl  = wb_llm_model_db or svc._wb_llm_model()
        if wb_host and wb_key:
            try:
                async with _httpx.AsyncClient(timeout=90) as c:
                    r = await c.post(f"{wb_host.rstrip('/')}/v1/chat/completions",
                        headers={"Content-Type": "application/json", "Authorization": f"Bearer {wb_key}"},
                        json={"model": wb_mdl, "messages": [{"role":"system","content":system},{"role":"user","content":user}],
                              "temperature": 0.3, "max_tokens": 2000})
                    if r.is_success: return r.json()["choices"][0]["message"]["content"].strip()
                    errors.append(f"WB LLM: HTTP {r.status_code}")
            except Exception as e: errors.append(f"WB LLM: {e}")
        g_key = groq_api_key or svc._groq_key()
        if g_key:
            try:
                payload = {"model": groq_model,
                           "messages": [{"role":"system","content":system},{"role":"user","content":user}],
                           "temperature": 0.3, "max_tokens": 2000}
                if _use_json_object:
                    payload["response_format"] = {"type": "json_object"}
                async with _httpx.AsyncClient(timeout=90) as c:
                    r = await c.post("https://api.groq.com/openai/v1/chat/completions",
                        headers={"Content-Type": "application/json", "Authorization": f"Bearer {g_key}"},
                        json=payload)
                    if r.is_success: return r.json()["choices"][0]["message"]["content"].strip()
                    try: body_txt = r.json().get("error", {}).get("message") or r.text
                    except Exception: body_txt = r.text
                    errors.append(f"Groq ({groq_model}): HTTP {r.status_code}: {body_txt}")
            except Exception as e: errors.append(f"Groq ({groq_model}): {e}")
        raise RuntimeError(" | ".join(errors) if errors else "No LLM provider configured.")

    stage_name = _LIO_RESULT_KEYS.get(stage_idx, f"stage_{stage_idx}")
    yield f"data: {_json.dumps({'status': 'running', 'stage': stage_idx, 'stage_name': stage_name})}\n\n"
    try:
        result = await _call_llm(system_prompt, user_prompt)
        result = _lio_strip_md(result)
        # Validate it's proper JSON
        try: _json.loads(result)
        except Exception:
            # Wrap plain-string responses (e.g. tag arrays) — keep as-is if not parseable
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
    groq_row = await cfg_svc.get_tool_row(org_id, "groq")
    wb_row   = await cfg_svc.get_tool_row(org_id, "wb_llm")
    groq_api_key  = (groq_row or {}).get("api_key") or None
    wb_llm_key_db = (wb_row  or {}).get("api_key") or None
    wb_extra: dict = {}
    try: wb_extra = json.loads((wb_row or {}).get("extra_config") or "{}") if wb_row else {}
    except Exception: pass
    wb_llm_host_db  = wb_extra.get("host") or None
    wb_llm_model_db = wb_extra.get("model") or None
    ctx = await _build_lio_ctx(lead, ws_ctx, prev)
    return lead_id, stage_idx, prompt_cfg, ctx, groq_api_key, wb_llm_key_db, wb_llm_host_db, wb_llm_model_db


async def _lio_llm_keys(request: Request):
    """Fetch LLM provider keys for the org — used by named LIO routes."""
    org_id = _get_org_id(request)
    groq_row = await cfg_svc.get_tool_row(org_id, "groq")
    wb_row   = await cfg_svc.get_tool_row(org_id, "wb_llm")
    groq_api_key  = (groq_row or {}).get("api_key") or None
    wb_llm_key_db = (wb_row  or {}).get("api_key") or None
    wb_extra: dict = {}
    try: wb_extra = json.loads((wb_row or {}).get("extra_config") or "{}") if wb_row else {}
    except Exception: pass
    return groq_api_key, wb_llm_key_db, wb_extra.get("host"), wb_extra.get("model")


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


class SingleEnrichRequest(BaseModel):
    linkedin_url: str
    generate_outreach: bool = True
    engagement_data: Optional[dict] = None

    @field_validator("linkedin_url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        return _validate_linkedin_url(v)


class BulkEnrichRequest(BaseModel):
    linkedin_urls: list[str]
    webhook_url: Optional[str] = None   # Where Bright Data posts results
    notify_url: Optional[str] = None    # Snapshot-ready notification URL
    webhook_auth: Optional[str] = None  # Auth header value for your webhook
    token: Optional[str] = None         # Tenant/org token — echoed back in response + webhook

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
        return cleaned


class RegenerateOutreachRequest(BaseModel):
    pass  # No body needed — uses existing lead data


# ─────────────────────────────────────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/enrich/stream", include_in_schema=False)
async def enrich_stream(body: SingleEnrichRequest, request: Request):
    """
    Stream a single LinkedIn profile enrichment as Server-Sent Events.

    Yields one JSON event per stage (loading → done) as the enrichment progresses:
      stage: profile | company | contact | website | scoring | complete

    Each event:  data: {"stage": "...", "status": "loading"|"done"|"error", "data": {...}}
    """
    org_id = _get_org_id(request)

    async def gen():
        try:
            async for event in svc.enrich_single_stream(
                linkedin_url=body.linkedin_url,
                org_id=org_id,
            ):
                yield f"data: {json.dumps(event)}\n\n"
        except Exception as e:
            logger.error("[EnrichStream] %s", e, exc_info=True)
            yield f"data: {json.dumps({'stage': 'complete', 'status': 'error', 'error': str(e)})}\n\n"

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/enrich", include_in_schema=False)
async def enrich_single(body: SingleEnrichRequest, request: Request, background_tasks: BackgroundTasks):
    """
    Enrich a single LinkedIn profile URL.

    Calls Bright Data sync API (~30–60s), finds email via waterfall,
    scores the lead (3-layer model), and generates AI outreach if score ≥ threshold.

    Pass a single URL:
    ```json
    {"linkedin_url": "https://www.linkedin.com/in/someone"}
    ```
    """
    org_id = _get_org_id(request)
    # Load tool availability from enrichment config
    tools_available = await cfg_svc.get_available_tools(org_id)
    try:
        lead = await svc.enrich_single(
            linkedin_url=body.linkedin_url,
            engagement_data=body.engagement_data,
            generate_outreach_flag=body.generate_outreach,
            org_id=org_id,
            tools=tools_available,
        )
        # Deduct credits for tools that were actually used
        if lead and not lead.get("error"):
            lead_id = lead.get("id")
            await cfg_svc.deduct_credit(org_id, "brightdata", lead_id=lead_id, reason="profile enrichment")
            email_source = str(lead.get("enrichment_source") or "")
            if "hunter" in email_source:
                await cfg_svc.deduct_credit(org_id, "hunter", lead_id=lead_id, reason="email discovery")
            elif "apollo" in email_source:
                await cfg_svc.deduct_credit(org_id, "apollo", lead_id=lead_id, reason="email discovery")
        return {
            "success": True,
            "lead": _format_lead(lead),
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("[EnrichSingle] %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Enrichment failed: {e}")


def _decode_token(token: str) -> dict:
    """Decode JWT payload without signature verification. Returns {} on failure."""
    try:
        parts = token.strip().split(".")
        if len(parts) < 2:
            return {}
        pad = parts[1] + "=" * (4 - len(parts[1]) % 4)
        return json.loads(base64.b64decode(pad))
    except Exception:
        return {}


def _validate_token(token: Optional[str]) -> str:
    """
    Validate token and return org_id.
    Raises HTTP 401 if token is missing, invalid, or platform != 'worksbuddy'.
    """
    if not token:
        raise HTTPException(status_code=401, detail="Token required.")
    payload = _decode_token(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid token.")
    if payload.get("platform") != "worksbuddy":
        raise HTTPException(status_code=401, detail="Invalid token: platform mismatch.")
    return str(payload.get("organization_id", "default"))


@router.post("/enrich/bulk")
async def enrich_bulk(body: BulkEnrichRequest, request: Request):
    """
    Enrich multiple LinkedIn profile URLs asynchronously.

    No Authorization header required. Pass JWT in the `token` body field —
    organization_id is decoded from it and echoed back in the response.

    ```json
    {
      "linkedin_urls": ["https://www.linkedin.com/in/alice"],
      "token": "<YOUR_JWT_TOKEN>",
      "webhook_url": "https://your-app.com/api/leads/webhook/brightdata"
    }
    ```
    """
    org_id = _validate_token(body.token)
    try:
        job = await svc.enrich_bulk(
            urls=body.linkedin_urls,
            webhook_url=body.webhook_url,
            notify_url=body.notify_url,
            webhook_auth=body.webhook_auth,
            org_id=org_id,
        )
        resp = {
            "success": True,
            "job": job,
            "message": (
                f"Batch job started for {len(body.linkedin_urls)} URLs. "
                f"Track progress at GET /api/leads/jobs/{job['id']}"
            ),
        }
        if body.token is not None:
            resp["token"] = body.token
        return resp
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("[EnrichBulk] %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Batch trigger failed: {e}")


# ── Job management ─────────────────────────────────────────────────────────

@router.get("/jobs")
async def list_jobs():
    """List all enrichment jobs (most recent first), each with its sub-jobs."""
    jobs = await svc.list_jobs(limit=50)
    import asyncio as _asyncio
    sub_job_lists = await _asyncio.gather(
        *[svc.list_sub_jobs(j["id"]) for j in jobs],
        return_exceptions=True,
    )
    for job, sub_jobs in zip(jobs, sub_job_lists):
        job["sub_jobs"] = sub_jobs if not isinstance(sub_jobs, Exception) else []
    return {"jobs": jobs, "total": len(jobs)}


@router.get("/jobs/{job_id}/sub-jobs", include_in_schema=False)
async def get_sub_jobs(job_id: str):
    """List sub-jobs (chunks) for a specific enrichment job."""
    sub_jobs = await svc.list_sub_jobs(job_id)
    return {"sub_jobs": sub_jobs, "total": len(sub_jobs)}


@router.get("/jobs/{job_id}")
async def get_job(job_id: str):
    """Get status and progress of a specific enrichment job."""
    job = await svc.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    result = await svc.list_leads(limit=1, offset=0, job_id=job_id)
    sub_jobs = await svc.list_sub_jobs(job_id)
    return {**job, "leads_count": result["total"], "sub_jobs": sub_jobs}


# ── Webhooks ───────────────────────────────────────────────────────────────

@router.post("/webhook/brightdata", include_in_schema=False)
async def webhook_brightdata(request: Request, background_tasks: BackgroundTasks):
    """
    Receives enriched profile data from Bright Data (batch delivery).
    Bright Data POSTs an array of profile objects to this endpoint.

    Configure via Bright Data dashboard or API trigger:
      endpoint=https://your-app.com/api/leads/webhook/brightdata
    """
    # Optional auth check
    if WEBHOOK_SECRET:
        auth = request.headers.get("Authorization", "")
        if auth != f"Bearer {WEBHOOK_SECRET}":
            raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        profiles = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    if not isinstance(profiles, list):
        profiles = [profiles]

    # Extract job_id from query param if provided
    job_id = request.query_params.get("job_id")

    # Process in background so webhook returns quickly (Bright Data timeout is 10s)
    background_tasks.add_task(svc.process_webhook_profiles, profiles, job_id)

    return {"ok": True, "received": len(profiles)}


@router.post("/webhook/notify", include_in_schema=False)
async def webhook_notify(request: Request, background_tasks: BackgroundTasks):
    """
    Bright Data snapshot-ready notification.
    Body: {"snapshot_id": "s_xxx", "status": "ready"}
    Triggers async download and processing of the snapshot.
    """
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    status = data.get("status") or data.get("state", "")
    snapshot_id = data.get("snapshot_id") or data.get("id")
    job_id = request.query_params.get("job_id")

    if status == "ready" and snapshot_id:
        async def _download_and_process():
            try:
                profiles = await svc.poll_snapshot(snapshot_id, interval=5, timeout=120)
                await svc.process_webhook_profiles(profiles, job_id=job_id)
                if job_id:
                    await svc._update_job(job_id, status="completed")
            except Exception as e:
                logger.error("[Notify] Download failed for %s: %s", snapshot_id, e)
                if job_id:
                    await svc._update_job(job_id, status="failed", error=str(e))

        background_tasks.add_task(_download_and_process)

    return {"ok": True}


# ── Lead results ───────────────────────────────────────────────────────────

@router.get("/queue/stats", include_in_schema=False)
async def queue_stats():
    """
    Real-time queue system snapshot.

    Returns active tenant count, per-tenant queue depth, current chunk size,
    worker status, and Redis memory usage.

    Useful for monitoring scale: how many tenants are active, how deep
    each tenant's queue is, and whether the system is under memory pressure.
    """
    from queue_manager import get_queue_stats
    return await get_queue_stats()


@router.get("", include_in_schema=False)
async def list_leads(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    job_id: Optional[str] = Query(None),
    min_score: Optional[int] = Query(None, ge=0, le=100),
    tier: Optional[str] = Query(None, pattern="^(hot|warm|cool|cold)$"),
):
    """List enriched leads with optional filters."""
    result = await svc.list_leads(
        limit=limit, offset=offset,
        job_id=job_id, min_score=min_score, tier=tier,
    )
    result["leads"] = [_format_lead(l) for l in result["leads"]]
    return result


@router.get("/export/csv", include_in_schema=False)
async def export_leads_csv(
    job_id: Optional[str] = Query(None),
    min_score: Optional[int] = Query(None, ge=0, le=100),
    tier: Optional[str] = Query(None),
):
    """Export leads as CSV download."""
    result = await svc.list_leads(limit=5000, offset=0, job_id=job_id, min_score=min_score, tier=tier)
    leads = result["leads"]

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=[
        "name", "title", "company", "location", "email", "email_source",
        "email_confidence", "linkedin_url", "total_score", "score_tier",
        "icp_score", "intent_score", "engagement_score",
        "followers", "connections", "enriched_at",
    ])
    writer.writeheader()
    for lead in leads:
        writer.writerow({
            "name": lead.get("name", ""),
            "title": lead.get("title", ""),
            "company": lead.get("company", ""),
            "location": lead.get("location", ""),
            "email": lead.get("email", ""),
            "email_source": lead.get("email_source", ""),
            "email_confidence": lead.get("email_confidence", ""),
            "linkedin_url": lead.get("linkedin_url", ""),
            "total_score": lead.get("total_score", 0),
            "score_tier": lead.get("score_tier", ""),
            "icp_score": lead.get("icp_score", 0),
            "intent_score": lead.get("intent_score", 0),
            "engagement_score": lead.get("engagement_score", 0),
            "followers": lead.get("followers", 0),
            "connections": lead.get("connections", 0),
            "enriched_at": lead.get("enriched_at", ""),
        })

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=leads_enriched.csv"},
    )


@router.get("/{lead_id}", include_in_schema=False)
async def get_lead(lead_id: str):
    """Get full detail for a single enriched lead."""
    lead = await svc.get_lead(lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    return _format_lead(lead, full=True)


@router.post("/{lead_id}/outreach", include_in_schema=False)
async def regenerate_outreach(lead_id: str):
    """
    Re-generate AI outreach copy (cold email + LinkedIn note + sequence)
    for an already-enriched lead.
    """
    result = await svc.regenerate_outreach_for_lead(lead_id)
    if not result:
        raise HTTPException(status_code=404, detail="Lead not found")
    return {"success": True, "lead": _format_lead(result, full=True)}


@router.delete("/{lead_id}", include_in_schema=False)
async def delete_lead(lead_id: str):
    """Delete an enriched lead record."""
    deleted = await svc.delete_lead(lead_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Lead not found")
    return {"success": True, "message": "Lead deleted"}


# ─────────────────────────────────────────────────────────────────────────────
# LIO Enrichment — system-prompt-driven AI analysis
# ─────────────────────────────────────────────────────────────────────────────

class LioPromptRequest(BaseModel):
    system_prompt: str
    model: Optional[str] = None  # Groq model override (e.g. "deepseek-r1-distill-llama-70b")


class LioPromptsConfigRequest(BaseModel):
    prompts: list  # List of 5 prompt config dicts


class WorkspaceContextRequest(BaseModel):
    product_name:      Optional[str] = None
    value_proposition: Optional[str] = None
    target_titles:     Optional[str] = None
    tone:              Optional[str] = None
    banned_phrases:    Optional[str] = None
    case_study:        Optional[str] = None
    cta_style:         Optional[str] = None


class LioStageRequest(BaseModel):
    stage_index: int                  # 0-6
    context: dict = {}                # Accumulated results from previous stages
                                      # Keys: company_intel, tags, signals, buying, pitch, outreach, score


class LioContextBody(BaseModel):
    system_prompt: str = ""
    user_prompt: str = ""
    brightdata_json: dict = {}
    stage_idx: int = 0  # used to know which result key to save


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
    and which Groq model is selected for LIO analysis.
    """
    org_id = _get_org_id(request)
    # Check tool configs for WB LLM and Groq
    wb_row   = await cfg_svc.get_tool_row(org_id, "wb_llm")
    groq_row = await cfg_svc.get_tool_row(org_id, "groq")
    workspace = await cfg_svc.get_workspace_config(org_id)

    wb_configured   = bool(wb_row and wb_row.get("api_key") or wb_row and (wb_row.get("extra_config") or "{}") != "{}")
    groq_configured = bool(groq_row and groq_row.get("api_key"))
    wb_enabled      = bool(wb_row and wb_row.get("is_enabled"))
    groq_enabled    = bool(groq_row and groq_row.get("is_enabled"))

    # Also check env-level keys (fallback if no DB config)
    if not wb_configured and os.getenv("WB_LLM_API_KEY"):
        wb_configured = True
        wb_enabled = True
    if not groq_configured and os.getenv("GROQ_API_KEY"):
        groq_configured = True
        groq_enabled = True

    return {
        "wb_llm":  {"configured": wb_configured,   "enabled": wb_enabled},
        "groq":    {"configured": groq_configured,  "enabled": groq_enabled},
        "active_provider": "wb_llm" if (wb_configured and wb_enabled) else ("groq" if groq_configured else None),
        "lio_model": workspace.get("lio_model", ""),
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
# Frontend sends: { system_prompt, user_prompt (fully filled), brightdata_json, stage_idx }
# Backend: calls Groq → streams SSE with { status, stage, result, done }

_SSE_HEADERS = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}

def _lio_stream_resp(lead_id: str, body: LioContextBody, keys: tuple, model: str):
    groq_key, wb_key, wb_host, wb_model = keys
    return StreamingResponse(
        _lio_stage_stream(lead_id, body.stage_idx, body.system_prompt, body.user_prompt,
                          groq_key, wb_key, wb_host, wb_model, groq_model=model),
        media_type="text/event-stream", headers=_SSE_HEADERS,
    )


@router.post("/{lead_id}/lio/company-intelligence", include_in_schema=False)
async def lio_company_intelligence(lead_id: str, body: LioContextBody, request: Request):
    """Stage 0 — Company Intelligence."""
    keys = await _lio_llm_keys(request)
    return _lio_stream_resp(lead_id, body, keys, "llama-3.1-8b-instant")


@router.post("/{lead_id}/lio/auto-tags", include_in_schema=False)
async def lio_auto_tags(lead_id: str, body: LioContextBody, request: Request):
    """Stage 1 — Auto Tags."""
    keys = await _lio_llm_keys(request)
    return _lio_stream_resp(lead_id, body, keys, "llama-3.1-8b-instant")


@router.post("/{lead_id}/lio/behavioural-signals", include_in_schema=False)
async def lio_behavioural_signals(lead_id: str, body: LioContextBody, request: Request):
    """Stage 2 — Behavioural Signals."""
    keys = await _lio_llm_keys(request)
    return _lio_stream_resp(lead_id, body, keys, "llama-3.1-8b-instant")


@router.post("/{lead_id}/lio/buying-signals", include_in_schema=False)
async def lio_buying_signals(lead_id: str, body: LioContextBody, request: Request):
    """Stage 3 — Buying Signals."""
    keys = await _lio_llm_keys(request)
    return _lio_stream_resp(lead_id, body, keys, "llama-3.1-8b-instant")


@router.post("/{lead_id}/lio/pitch-intelligence", include_in_schema=False)
async def lio_pitch_intelligence(lead_id: str, body: LioContextBody, request: Request):
    """Stage 4 — Pitch Intelligence."""
    keys = await _lio_llm_keys(request)
    return _lio_stream_resp(lead_id, body, keys, "llama-3.3-70b-versatile")


@router.post("/{lead_id}/lio/outreach", include_in_schema=False)
async def lio_outreach(lead_id: str, body: LioContextBody, request: Request):
    """Stage 5 — Outreach Generator."""
    keys = await _lio_llm_keys(request)
    return _lio_stream_resp(lead_id, body, keys, "llama-3.3-70b-versatile")


@router.post("/{lead_id}/lio/lead-score", include_in_schema=False)
async def lio_lead_score(lead_id: str, body: LioContextBody, request: Request):
    """Stage 6 — Lead Score."""
    keys = await _lio_llm_keys(request)
    return _lio_stream_resp(lead_id, body, keys, "llama-3.1-8b-instant")


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
    groq_row   = await cfg_svc.get_tool_row(org_id, "groq")
    wb_row     = await cfg_svc.get_tool_row(org_id, "wb_llm")
    groq_api_key  = (groq_row or {}).get("api_key") or None
    wb_llm_key_db = (wb_row  or {}).get("api_key") or None
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

    # BrightData person profile field extraction
    activity: list = raw_profile.get("activity", []) or []
    own_posts: list = raw_profile.get("posts", []) or []  # person's own published posts

    def _act_str(a) -> str:
        """Extract readable text from an activity/post object."""
        return (a.get("title") or a.get("attribution") or a.get("action") or a.get("text") or "")[:180]

    name_parts = (lead.get("name") or "").split()

    # ── Infer title / role ─────────────────────────────────────────────────
    # BrightData person profiles often lack a `title` field — extract from `about`
    import re as _re
    about_text = (raw_profile.get("about") or raw_profile.get("summary")
                  or raw_profile.get("headline") or lead.get("about") or "")
    raw_title = (raw_profile.get("title") or raw_profile.get("headline")
                 or full_data.get("title") or lead.get("title") or "")
    if not raw_title and about_text:
        # Try to pull role from first sentence: "I'm the CEO and Founder of ..."
        m = _re.search(
            r"(?:I(?:'m| am)(?: the)?\s+|^)(CEO|CTO|CFO|COO|CMO|Founder|Co-Founder|"
            r"Director|VP|Head of \w+|President|Partner|Managing Director|MD)",
            about_text, _re.IGNORECASE
        )
        if m:
            raw_title = m.group(1)

    # ── Industry inference ─────────────────────────────────────────────────
    industry = (raw_profile.get("industry") or full_data.get("industry")
                or lead.get("industry") or "")
    if not industry and about_text:
        # Infer from post topics if possible
        all_post_text = " ".join(_act_str(p) for p in own_posts[:5]).lower()
        if any(w in all_post_text for w in ("blockchain", "crypto", "web3", "defi", "nft")):
            industry = "Blockchain / Web3 Technology"
        elif any(w in all_post_text for w in ("ai", "artificial intelligence", "machine learning", "llm")):
            industry = "Artificial Intelligence"
        elif any(w in all_post_text for w in ("saas", "software", "platform", "app")):
            industry = "Software / SaaS"
        elif any(w in all_post_text for w in ("fintech", "finance", "banking", "payment")):
            industry = "Fintech"

    # ── Build rich company_description from all available signals ──────────
    company_desc_parts = []
    # 1. Direct company description fields (enrichment-stage data)
    for k in ("company_description", "company_about", "company_summary",
              "company_specialty", "company_overview"):
        v = full_data.get(k) or raw_profile.get(k)
        if v: company_desc_parts.append(str(v)[:400])
    # 2. Prospect's own `about` bio (esp. useful for founders/CEOs)
    if about_text:
        company_desc_parts.append(f"Founder/Leader bio: {about_text[:400]}")
    # 3. Own published posts reveal business focus
    if own_posts:
        post_lines = "\n".join(
            f"  • {p.get('title', '')[:120]}" for p in own_posts[:4] if p.get("title")
        )
        if post_lines:
            company_desc_parts.append(f"Published posts (topics they care about):\n{post_lines}")
    # 4. Awards / recognition signal company credibility + domain
    awards = raw_profile.get("honors_and_awards", []) or []
    if awards:
        award_lines = "; ".join(
            f"{a.get('title','')} — {a.get('publication','')}" for a in awards[:3]
        )
        company_desc_parts.append(f"Awards/recognition: {award_lines}")
    # 5. Education
    edu = raw_profile.get("educations_details") or ""
    if edu:
        company_desc_parts.append(f"Education: {str(edu)[:200]}")
    # 6. Company size signals
    followers = raw_profile.get("followers") or 0
    connections = raw_profile.get("connections") or 0
    if followers or connections:
        company_desc_parts.append(
            f"LinkedIn presence: {followers} followers, {connections}+ connections"
        )

    company_description = "\n\n".join(company_desc_parts) or "No company description available"

    # ── Activity feed: liked posts reveal what they engage with ────────────
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

    # ── Base context ────────────────────────────────────────────────────────
    prev = body.context or {}

    def _s(raw) -> str:
        if isinstance(raw, (dict, list)): return json.dumps(raw)
        return str(raw or "")

    def _parse(raw) -> Any:
        if isinstance(raw, (dict, list)): return raw
        try: return json.loads(raw)
        except Exception: return raw

    # Extract sub-fields from previous stage results
    ci_obj     = _parse(prev.get("company_intel", "")) if prev.get("company_intel") else {}
    buying_obj = _parse(prev.get("buying", ""))        if prev.get("buying")        else {}
    pitch_obj  = _parse(prev.get("pitch", ""))         if prev.get("pitch")         else {}

    ctx: dict = {
        # Identity
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
        # Activity
        "recent_posts":    recent_posts_str,
        "recent_activity": recent_activity_str,
        # Scoring
        "total_score":       str(lead.get("total_score") or 0),
        "icp_fit_score":     str(lead.get("icp_score") or 0),
        "score_tier":        lead.get("score_tier") or "",
        "warm_signal_count": str(len(own_posts)),
        "email_status":      "found" if lead.get("email") else "not found",
        # Workspace
        "product_name":      ws_ctx.get("product_name", ""),
        "value_proposition": ws_ctx.get("value_proposition", ""),
        "target_titles":     ws_ctx.get("target_titles", ""),
        "tone":              ws_ctx.get("tone", "professional"),
        "banned_phrases":    ws_ctx.get("banned_phrases", ""),
        "case_study":        ws_ctx.get("case_study", ""),
        "cta_style":         ws_ctx.get("cta_style", "question"),
        # Previous stage outputs
        "company_intel":       _s(prev.get("company_intel", "")),
        "auto_tags":           _s(prev.get("tags", "[]")),
        "behavioural_signals": _s(prev.get("signals", "")),
        "buying_signals":      _s(prev.get("buying", "")),
        "pitch_intelligence":  _s(prev.get("pitch", "")),
        # Sub-fields extracted from previous stage JSON
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
            """Replace {var} placeholders using regex — safe against literal JSON braces."""
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
            g_key = groq_api_key or svc._groq_key()
            if g_key:
                try:
                    async with _httpx.AsyncClient(timeout=90) as c:
                        r = await c.post("https://api.groq.com/openai/v1/chat/completions",
                            headers={"Content-Type": "application/json", "Authorization": f"Bearer {g_key}"},
                            json={"model": model, "messages": [{"role":"system","content":system},{"role":"user","content":user}], "temperature": temp, "max_tokens": 2000})
                        if r.is_success: return r.json()["choices"][0]["message"]["content"].strip()
                        body_txt = ""
                        try: body_txt = r.json().get("error", {}).get("message") or r.text
                        except Exception: body_txt = r.text
                        errors.append(f"Groq ({model}): HTTP {r.status_code}: {body_txt}")
                except Exception as e: errors.append(f"Groq ({model}): {e}")
            raise RuntimeError(" | ".join(errors) if errors else "No LLM provider configured — add Groq or WB LLM key in Tool Configuration.")

        import re as _re

        def _strip_md(text: str) -> str:
            """Strip markdown code fences that some LLMs wrap JSON in."""
            text = text.strip()
            text = _re.sub(r'^```(?:json)?\s*', '', text, flags=_re.MULTILINE)
            text = _re.sub(r'\s*```$', '', text, flags=_re.MULTILINE)
            return text.strip()

        # Result-key map: stage index → context key used in frontend + DB
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
            # Persist this stage's result into the lead's lio_results_json
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
    groq_row = await cfg_svc.get_tool_row(org_id, "groq")
    wb_row   = await cfg_svc.get_tool_row(org_id, "wb_llm")
    groq_api_key  = (groq_row or {}).get("api_key") or None
    wb_llm_key_db = (wb_row  or {}).get("api_key") or None
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

    # ── Build base context dict (all {variable} placeholders for all 5 steps) ─
    name_parts = (lead.get("name") or "").split()
    base_ctx: dict = {
        # Identity
        "first_name":    raw_profile.get("first_name")  or (name_parts[0] if name_parts else ""),
        "last_name":     raw_profile.get("last_name")   or (" ".join(name_parts[1:]) if len(name_parts) > 1 else ""),
        "inferred_title": raw_profile.get("title")      or lead.get("title") or "",
        "company":       (raw_profile.get("current_company_name") or raw_profile.get("current_company")
                          or lead.get("company") or ""),
        "about":         raw_profile.get("about") or raw_profile.get("summary") or raw_profile.get("headline") or "",
        "industry":      raw_profile.get("industry") or full_data.get("industry") or "",
        "city":          raw_profile.get("city") or "",
        "country":       raw_profile.get("country") or lead.get("location") or "",
        # Activity
        "top_3_activity_titles":  top_3_activity_titles,
        "activity_sample":        activity_sample,
        "warm_signal_posts":      warm_signal_posts,
        "warm_signal_posts_titles": warm_signal_posts_titles,
        "warm_signal_count":      str(len(posts)),
        # Scoring
        "total_score":          str(lead.get("total_score") or 0),
        "icp_fit_score":        str(lead.get("icp_score") or 0),
        "intent_score":         str(lead.get("intent_score") or 0),
        "timing_score":         str(full_data.get("timing_score") or 0),
        "score_tier":           lead.get("score_tier") or "",
        "score_anomaly":        str(full_data.get("score_anomaly") or False),
        "score_anomaly_reason": str(full_data.get("score_anomaly_reason") or ""),
        "email_status":         "found" if lead.get("email") else "not found",
        # Step-chain placeholders (filled as steps complete)
        "behavioural_signals": "",
        "top_pain_point": "", "best_value_prop": "", "trigger_event": "",
        "best_angle": "", "do_not_pitch": "", "suggested_cta": "", "opening_line": "",
        # Workspace / product context
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
            """Replace {var} placeholders using regex — safe against literal JSON braces."""
            import re as _re
            def _sub(m):
                key = m.group(1)
                val = ctx.get(key)
                return str(val) if val is not None else m.group(0)
            return _re.sub(r'\{([a-zA-Z_][a-zA-Z0-9_]*)\}', _sub, template)

        async def _call_llm(system: str, user: str, model: str, temperature: float) -> str:
            """Try WB LLM (if configured) then Groq. Raises on all failures."""
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

            g_key = groq_api_key or svc._groq_key()
            if g_key:
                try:
                    async with _httpx.AsyncClient(timeout=90) as c:
                        r = await c.post(
                            "https://api.groq.com/openai/v1/chat/completions",
                            headers={"Content-Type": "application/json", "Authorization": f"Bearer {g_key}"},
                            json={"model": model,
                                  "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
                                  "temperature": temperature, "max_tokens": 2000},
                        )
                        if r.is_success:
                            return r.json()["choices"][0]["message"]["content"].strip()
                        body = ""
                        try: body = r.json().get("error", {}).get("message") or r.text
                        except Exception: body = r.text
                        errors.append(f"Groq ({model}): HTTP {r.status_code}: {body}")
                except Exception as e:
                    errors.append(f"Groq ({model}): {e}")

            raise RuntimeError(
                " | ".join(errors) if errors
                else "No LLM provider configured — add Groq or WB LLM key in Tool Configuration."
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


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _format_lead(lead: dict, full: bool = False) -> dict:
    """Parse JSON fields and format for API response."""
    out = {k: v for k, v in lead.items() if k not in ("raw_profile", "skills")}

    # Parse score_reasons JSON → list
    try:
        out["score_reasons"] = json.loads(lead.get("score_reasons") or "[]")
    except Exception:
        out["score_reasons"] = []

    # Parse outreach_sequence JSON → dict
    try:
        out["outreach_sequence"] = json.loads(lead.get("outreach_sequence") or "{}")
    except Exception:
        out["outreach_sequence"] = {}

    # Parse skills JSON → list
    try:
        out["skills"] = json.loads(lead.get("skills") or "[]")
    except Exception:
        out["skills"] = []

    # Always parse full_data from JSON string → dict so the modal shows structured data
    try:
        out["full_data"] = json.loads(lead.get("full_data") or "{}")
    except Exception:
        out["full_data"] = {}

    if full:
        # Include raw BrightData profile as parsed dict
        try:
            out["raw_profile"] = json.loads(lead.get("raw_profile") or "{}")
        except Exception:
            out["raw_profile"] = {}

    return out

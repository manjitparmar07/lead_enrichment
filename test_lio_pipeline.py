#!/usr/bin/env python3
"""
Test script: simulate a BrightData response → run through LIO pipeline → show CRM brief.
Tracks every step in an activity log.

Usage:
    cd backend
    python test_lio_pipeline.py [path_to_brightdata_json]
    python test_lio_pipeline.py  # uses embedded sample data
"""

import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# ── Load .env ─────────────────────────────────────────────────────────────────
from dotenv import load_dotenv
load_dotenv(dotenv_path=Path(__file__).parent / ".env")

# ── Bootstrap app ─────────────────────────────────────────────────────────────
import asyncpg

DATABASE_URL = os.getenv("DATABASE_URL", "")

# ─────────────────────────────────────────────────────────────────────────────
# Activity log — tracks every step with timing
# ─────────────────────────────────────────────────────────────────────────────
_log: list[dict] = []
_t0 = time.monotonic()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def log_step(step: str, status: str = "ok", detail: str = "", data: dict = None):
    elapsed = round((time.monotonic() - _t0) * 1000)
    entry = {
        "step":       step,
        "status":     status,
        "detail":     detail,
        "elapsed_ms": elapsed,
        "at":         _now(),
    }
    if data:
        entry["data"] = data
    _log.append(entry)

    icon = "✅" if status == "ok" else ("⚠️ " if status == "warn" else "❌")
    print(f"  {icon} [{elapsed:>5}ms] {step}", end="")
    if detail:
        print(f"  →  {detail}", end="")
    print()


def print_section(title: str):
    print(f"\n{'─'*60}")
    print(f"  {title}")
    print(f"{'─'*60}")


# ─────────────────────────────────────────────────────────────────────────────
# Adapt BrightData company/person JSON → pipeline-compatible profile dict
# ─────────────────────────────────────────────────────────────────────────────
def adapt_brightdata_profile(raw: dict) -> dict:
    """
    Maps any BrightData response (person OR company) to the internal profile
    format expected by _build_llm_profile and build_comprehensive_enrichment.
    """
    # Detect company vs person
    is_company = bool(raw.get("employees") or raw.get("organization_type") or raw.get("industries"))

    if is_company:
        # Map company fields → person-profile-like structure for LIO
        employees = raw.get("employees") or []
        posts = []
        for u in (raw.get("updates") or [])[:20]:
            if u.get("text"):
                posts.append({
                    "text":      u["text"],
                    "likes":     u.get("likes_count", 0),
                    "comments":  u.get("comments_count", 0),
                    "posted_at": u.get("date", ""),
                })

        profile = {
            # Identity
            "name":                 raw.get("name", ""),
            "full_name":            raw.get("name", ""),
            "position":             f"Company — {raw.get('industries', '')}",
            "headline":             raw.get("specialties", ""),
            "about":                raw.get("about", "") or raw.get("unformatted_about", ""),
            "location":             raw.get("headquarters", "") or (raw.get("locations") or [""])[0],
            "url":                  raw.get("url", ""),
            "input_url":            (raw.get("input") or {}).get("url", raw.get("url", "")),
            "followers":            raw.get("followers", 0),
            "connections":          raw.get("employees_in_linkedin", 0),
            # Company context
            "current_company_name": raw.get("name", ""),
            "current_company_link": raw.get("website", ""),
            "current_company": {
                "name":        raw.get("name", ""),
                "industry":    raw.get("industries", ""),
                "size":        raw.get("company_size", ""),
                "founded":     str(raw.get("founded", "")),
                "website":     raw.get("website", ""),
                "description": raw.get("about", "")[:500],
                "specialties": raw.get("specialties", ""),
                "logo":        raw.get("logo", ""),
            },
            # Posts — company updates used as authored content
            "posts":        posts,
            "recent_posts": posts,
            # Skills from specialties
            "skills": [s.strip() for s in (raw.get("specialties") or "").split(",") if s.strip()],
            # Employees as team signal
            "recommendations": [
                f"{e.get('title', '')} — {e.get('link', '')}"
                for e in employees[:10]
            ],
            # Raw source
            "_is_company_page": True,
            "_company_extras": {
                "company_logo":        raw.get("logo", ""),
                "company_size":        raw.get("company_size", ""),
                "company_description": raw.get("about", "")[:500],
                "industry":            raw.get("industries", ""),
                "employee_count":      raw.get("employees_in_linkedin", 0),
                "website":             raw.get("website", ""),
                "founded":             str(raw.get("founded", "")),
                "specialties":         raw.get("specialties", ""),
            },
        }
    else:
        # Already a person profile — pass through with standard keys
        profile = dict(raw)
        profile.setdefault("input_url", profile.get("url", ""))

    return profile


# ─────────────────────────────────────────────────────────────────────────────
# Save activity log to DB
# ─────────────────────────────────────────────────────────────────────────────
async def save_activity_log(lead_id: str, linkedin_url: str, org_id: str = "default"):
    if not DATABASE_URL:
        return
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        for entry in _log:
            import uuid
            await conn.execute(
                """INSERT INTO enrichment_audit_log
                   (id, org_id, action, lead_id, linkedin_url, step, status, duration_ms, meta, created_at)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)""",
                str(uuid.uuid4()), org_id,
                "pipeline_test", lead_id, linkedin_url,
                entry["step"], entry["status"],
                entry.get("elapsed_ms"),
                json.dumps({"detail": entry.get("detail", ""), **(entry.get("data") or {})}),
                datetime.fromisoformat(entry["at"]),
            )
        await conn.close()
        print(f"\n  Activity log saved to DB — {len(_log)} entries for lead {lead_id}")
    except Exception as e:
        print(f"\n  ⚠️  Could not save activity log to DB: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Main pipeline test
# ─────────────────────────────────────────────────────────────────────────────
async def run_pipeline_test(raw_data: dict):
    global _t0
    _t0 = time.monotonic()

    print_section("STEP 1 — Load & Validate BrightData Response")

    name = raw_data.get("name") or raw_data.get("full_name", "Unknown")
    url  = (raw_data.get("input") or {}).get("url") or raw_data.get("url") or raw_data.get("linkedin_url", "")
    is_company = bool(raw_data.get("employees") or raw_data.get("organization_type"))

    log_step("raw_data_loaded",  "ok",  f"name={name!r}  type={'company' if is_company else 'person'}  url={url}")
    log_step("url_detected",     "ok",  url)
    log_step("followers",        "ok",  str(raw_data.get("followers", 0)))
    if is_company:
        log_step("company_size", "ok", raw_data.get("company_size", "unknown"))
        log_step("updates_count","ok", f"{len(raw_data.get('updates') or [])} posts found")
    else:
        log_step("posts_count",  "ok", f"{len(raw_data.get('posts') or [])} authored posts")
        log_step("activity_count","ok",f"{len(raw_data.get('activity') or [])} interactions")

    # ── Step 2: Adapt profile ─────────────────────────────────────────────────
    print_section("STEP 2 — Adapt to Internal Profile Format")
    profile = adapt_brightdata_profile(raw_data)
    log_step("profile_adapted", "ok", f"keys={len(profile)}")
    log_step("posts_mapped",    "ok", f"{len(profile.get('posts') or [])} posts available for LLM")
    log_step("skills_mapped",   "ok", str(profile.get("skills", [])[:5]))

    # ── Step 3: Build LLM profile ─────────────────────────────────────────────
    print_section("STEP 3 — Build Optimized LLM Profile")
    sys.path.insert(0, str(Path(__file__).parent))
    from lead_enrichment_brightdata_service import _build_llm_profile, _trim_profile_to_budget

    optimized = _build_llm_profile(profile)
    raw_str   = json.dumps(optimized, separators=(",",":"), ensure_ascii=False, default=str)
    log_step("llm_profile_built", "ok", f"{len(raw_str):,} chars / ~{len(raw_str)//4:,} tokens")

    budget = 80000
    if len(raw_str) > budget:
        optimized = _trim_profile_to_budget(optimized, budget)
        raw_str   = json.dumps(optimized, separators=(",",":"), ensure_ascii=False, default=str)
        log_step("profile_trimmed", "warn", f"trimmed to {len(raw_str):,} chars")
    else:
        log_step("profile_size_ok", "ok", "within token budget")

    print(f"\n  Profile preview:")
    print(f"    name       : {optimized.get('name')}")
    print(f"    headline   : {optimized.get('headline')}")
    print(f"    company    : {(optimized.get('company_context') or {}).get('name')}")
    print(f"    location   : {optimized.get('location')}")
    print(f"    posts      : {len(optimized.get('authored_posts') or [])}")
    print(f"    interactions: {len(optimized.get('interactions') or [])}")

    # ── Step 4: Load system prompt ────────────────────────────────────────────
    print_section("STEP 4 — Load CRM Brief System Prompt")
    from lead_enrichment_brightdata_service import _DEFAULT_CRM_BRIEF_PROMPT
    from enrichment_config_service import get_workspace_config
    from db import init_pool

    await init_pool()

    try:
        cfg = await get_workspace_config("default")
        crm_prompt   = cfg.get("lio_system_prompt", "").strip() or _DEFAULT_CRM_BRIEF_PROMPT
        model_override = cfg.get("lio_model", "").strip() or None
        log_step("prompt_loaded", "ok",
                 f"{'custom workspace prompt' if cfg.get('lio_system_prompt') else 'default prompt'}  "
                 f"model={model_override or 'default'}")
    except Exception as e:
        crm_prompt   = _DEFAULT_CRM_BRIEF_PROMPT
        model_override = None
        log_step("prompt_loaded", "warn", f"fallback to default — {e}")

    log_step("prompt_length", "ok", f"{len(crm_prompt):,} chars")

    # ── Step 5: Call LLM ──────────────────────────────────────────────────────
    print_section("STEP 5 — Send to LIO (LLM Call)")
    from lead_enrichment_brightdata_service import _call_llm

    user_msg = f"Analyze this LinkedIn prospect data and return the JSON exactly as specified:\n\n{raw_str}"
    log_step("llm_call_start", "ok", f"max_tokens=4500  temperature=0.3")

    t_llm = time.monotonic()
    try:
        brief_raw = await _call_llm(
            [
                {"role": "system", "content": crm_prompt},
                {"role": "user",   "content": user_msg},
            ],
            max_tokens=4500,
            temperature=0.3,
            model_override=model_override,
            wb_llm_model_override=model_override,
        )
        llm_ms = round((time.monotonic() - t_llm) * 1000)

        if not brief_raw:
            log_step("llm_response", "error", "LLM returned None — check API keys / model config")
            return

        log_step("llm_response", "ok", f"{len(brief_raw):,} chars  took {llm_ms:,}ms")

    except Exception as e:
        log_step("llm_response", "error", str(e))
        print(f"\n  ❌ LLM call failed: {e}")
        return

    # ── Step 6: Parse & validate JSON ─────────────────────────────────────────
    print_section("STEP 6 — Parse & Validate CRM Brief JSON")
    import re

    brief_raw = re.sub(r"<think>.*?</think>", "", brief_raw, flags=re.DOTALL)
    brief_raw = re.sub(r"<think>.*", "", brief_raw, flags=re.DOTALL).strip()
    log_step("think_blocks_stripped", "ok")

    start = brief_raw.find("{")
    if start != -1:
        depth, end = 0, start
        for i, ch in enumerate(brief_raw[start:], start):
            if ch == "{": depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0: end = i; break
        brief_raw = brief_raw[start:end + 1]
    log_step("json_extracted", "ok", f"{len(brief_raw):,} chars")

    try:
        crm_brief = json.loads(brief_raw)
        log_step("json_parsed", "ok", "valid JSON ✓")
    except Exception as e:
        log_step("json_parsed", "error", f"Invalid JSON — {e}")
        print(f"\n  Raw LLM output (first 500 chars):\n{brief_raw[:500]}")
        return

    # Validate required top-level keys
    required = ["who_they_are","their_company","what_they_care_about",
                 "online_behaviour","communication","what_drives_them",
                 "buying_signals","smart_tags","outreach_blueprint",
                 "crm_scores","crm_import_fields","recent_activity"]
    missing  = [k for k in required if k not in crm_brief]
    if missing:
        log_step("schema_validation", "warn", f"missing keys: {missing}")
    else:
        log_step("schema_validation", "ok", "all required keys present ✓")

    # Check arrays have 4-5 items
    array_counts = {}
    for section, val in crm_brief.items():
        if isinstance(val, dict):
            for k, v in val.items():
                if isinstance(v, list):
                    array_counts[f"{section}.{k}"] = len(v)
        elif isinstance(val, list):
            array_counts[section] = len(val)

    thin = {k: v for k, v in array_counts.items() if v < 4}
    if thin:
        log_step("array_density", "warn", f"arrays with <4 items: {thin}")
    else:
        log_step("array_density", "ok", f"all arrays have ≥4 items ✓")

    # ── Step 7: Save to DB ────────────────────────────────────────────────────
    print_section("STEP 7 — Save to Database")
    from lead_enrichment_brightdata_service import _lead_id, _normalize_linkedin_url, _upsert_lead

    lead_url = url
    try:
        lead_url = _normalize_linkedin_url(url)
    except Exception:
        pass

    lead_id = _lead_id(lead_url)
    log_step("lead_id_computed", "ok", lead_id)

    try:
        await _upsert_lead({
            "id":             lead_id,
            "linkedin_url":   lead_url,
            "name":           profile.get("name", ""),
            "title":          profile.get("position", ""),
            "company":        profile.get("current_company_name", ""),
            "about":          (profile.get("about") or "")[:1000],
            "followers":      int(profile.get("followers") or 0),
            "connections":    int(profile.get("connections") or 0),
            "avatar_url":     profile.get("logo", "") or profile.get("avatar_url", ""),
            "company_logo":   (profile.get("_company_extras") or {}).get("company_logo", ""),
            "raw_brightdata": json.dumps(raw_data, default=str),
            "crm_brief":      json.dumps(crm_brief, default=str),
            "enriched_at":    _now(),
            "status":         "completed",
        })
        log_step("db_saved", "ok", f"lead_id={lead_id}")
    except Exception as e:
        log_step("db_saved", "error", str(e))

    # ── Step 8: Print results ─────────────────────────────────────────────────
    print_section("STEP 8 — CRM Brief Output")

    def show(label, val, indent=4):
        pad = " " * indent
        if isinstance(val, list):
            print(f"{pad}{label}:")
            for item in val:
                if isinstance(item, dict):
                    print(f"{pad}  • {json.dumps(item, ensure_ascii=False)}")
                else:
                    print(f"{pad}  • {item}")
        elif isinstance(val, dict):
            print(f"{pad}{label}:")
            for k, v in val.items():
                show(k, v, indent + 2)
        else:
            print(f"{pad}{label}: {val}")

    for section, content in crm_brief.items():
        print(f"\n  ┌─ {section.upper().replace('_',' ')} ─")
        if isinstance(content, dict):
            for k, v in content.items():
                show(k, v)
        elif isinstance(content, list):
            for item in content:
                if isinstance(item, dict):
                    print(f"    • {json.dumps(item, ensure_ascii=False)}")
                else:
                    print(f"    • {item}")
        else:
            print(f"    {content}")

    # ── Activity log summary ──────────────────────────────────────────────────
    print_section("ACTIVITY LOG SUMMARY")
    total_ms = round((time.monotonic() - _t0) * 1000)
    errors   = [e for e in _log if e["status"] == "error"]
    warns    = [e for e in _log if e["status"] == "warn"]

    print(f"  Total time : {total_ms:,}ms")
    print(f"  Steps      : {len(_log)}")
    print(f"  Errors     : {len(errors)}")
    print(f"  Warnings   : {len(warns)}")
    print()

    for entry in _log:
        icon = "✅" if entry["status"] == "ok" else ("⚠️ " if entry["status"] == "warn" else "❌")
        print(f"  {icon} {entry['step']:<35}  {entry['elapsed_ms']:>5}ms  {entry['detail'][:60]}")

    # Save activity log to DB
    await save_activity_log(lead_id, lead_url)

    print(f"\n  lead_id = {lead_id}")
    print(f"  Query DB: SELECT crm_brief FROM enriched_leads WHERE id='{lead_id}';\n")

    return crm_brief


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) > 1:
        json_path = Path(sys.argv[1])
        raw = json.loads(json_path.read_text())
        # Handle array wrapper [{ ... }]
        if isinstance(raw, list):
            raw = raw[0]
    else:
        print("Usage: python test_lio_pipeline.py path/to/brightdata.json")
        sys.exit(1)

    print("\n" + "═"*60)
    print("  LIO PIPELINE TEST")
    print("═"*60)

    asyncio.run(run_pipeline_test(raw))

"""
filter_service.py
-----------------
Search BrightData LinkedIn People dataset via the Datasets REST API (v1).

Flow:
  Step 1 — Trigger snapshot (POST /datasets/filter) → snapshot_id
  Step 2 — Background task polls /datasets/snapshots/{id} until ready
  Step 3 — Download /datasets/snapshots/{id}/download (retry on 202)
  Step 4 — Normalise → map → upsert to enriched_leads

The /search route returns immediately with snapshot_id + status "processing".
Frontend polls GET /status/{snapshot_id} until status == "done" or "failed".

Public API:
  trigger_search(filters, org_id, limit)         -> dict  (snapshot_id + status)
  run_snapshot_job(snapshot_id, org_id, timeout) -> None  (background coroutine)
  get_job_status(snapshot_id)                    -> dict  (status + result)
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Optional

import httpx

from Lead_enrichment.bulk_lead_enrichment._utils import (
    _bd_api_key,
    _lead_id,
    _parse_name,
    _safe_int,
)
from Lead_enrichment.bulk_lead_enrichment._brightdata import (
    _normalize_bd_profile,
    _clean_bd_linkedin_url,
)
from Lead_enrichment.bulk_lead_enrichment._db import _upsert_lead

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# BrightData Datasets REST API
# ─────────────────────────────────────────────────────────────────────────────

BD_API_BASE   = "https://api.brightdata.com"
BD_FILTER_URL = f"{BD_API_BASE}/datasets/filter"
BD_SNAP_URL   = f"{BD_API_BASE}/datasets/snapshots"

LINKEDIN_DATASET_ID = "gd_l1viktl72bvl7bjuj0"

DEFAULT_LIMIT   = 20
DEFAULT_TIMEOUT = 1800   # 30 min — BrightData can take 10-20 min for large filters
POLL_INTERVAL   = 10     # seconds between status polls

# ─────────────────────────────────────────────────────────────────────────────
# In-memory job store  {snapshot_id → job_dict}
# ─────────────────────────────────────────────────────────────────────────────

_jobs: dict[str, dict] = {}


def get_job_status(snapshot_id: str) -> dict:
    """Return current job dict, or a not-found sentinel."""
    return _jobs.get(snapshot_id, {"status": "not_found", "snapshot_id": snapshot_id})


# ─────────────────────────────────────────────────────────────────────────────
# Step 1 — trigger snapshot
# ─────────────────────────────────────────────────────────────────────────────

async def _trigger_snapshot(api_key: str, filters: dict, limit: int) -> str:
    payload: dict[str, Any] = {
        "dataset_id":    LINKEDIN_DATASET_ID,
        "filter":        filters,
        "records_limit": limit,
    }
    logger.info(
        "[BDFilter] POST /datasets/filter  dataset=%s  limit=%d  filter=%s",
        LINKEDIN_DATASET_ID, limit, json.dumps(filters),
    )
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            BD_FILTER_URL,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=payload,
        )
    logger.info("[BDFilter] Trigger  status=%d  body=%s", resp.status_code, resp.text[:300])
    if resp.status_code != 200:
        raise RuntimeError(
            f"BrightData /datasets/filter returned {resp.status_code}: {resp.text[:300]}"
        )
    data        = resp.json()
    snapshot_id = data.get("snapshot_id")
    if not snapshot_id:
        raise RuntimeError(f"No snapshot_id in BrightData response: {data}")
    return snapshot_id


# ─────────────────────────────────────────────────────────────────────────────
# Step 2 — poll status
# ─────────────────────────────────────────────────────────────────────────────

async def _wait_for_snapshot(api_key: str, snapshot_id: str, timeout: int) -> None:
    deadline = time.monotonic() + timeout
    async with httpx.AsyncClient(timeout=30.0) as client:
        while True:
            resp = await client.get(
                f"{BD_SNAP_URL}/{snapshot_id}",
                headers={"Authorization": f"Bearer {api_key}"},
            )
            if resp.status_code != 200:
                raise RuntimeError(
                    f"Snapshot status check returned {resp.status_code}: {resp.text[:200]}"
                )
            data   = resp.json()
            status = data.get("status", "unknown")
            logger.info("[BDFilter] Snapshot %s  status=%s", snapshot_id, status)

            # Update job status so frontend can show live progress
            if snapshot_id in _jobs:
                _jobs[snapshot_id]["bd_status"] = status

            if status == "ready":
                return
            if status == "failed":
                raise RuntimeError(f"BrightData snapshot {snapshot_id} failed: {data}")
            if time.monotonic() > deadline:
                raise RuntimeError(
                    f"Snapshot {snapshot_id} not ready after {timeout}s (last status: {status})"
                )
            await asyncio.sleep(POLL_INTERVAL)


# ─────────────────────────────────────────────────────────────────────────────
# Step 3 — download (retry on 202)
# ─────────────────────────────────────────────────────────────────────────────

async def _download_snapshot(api_key: str, snapshot_id: str, timeout: int) -> list[dict]:
    url      = f"{BD_SNAP_URL}/{snapshot_id}/download"
    deadline = time.monotonic() + timeout
    logger.info("[BDFilter] Downloading snapshot %s …", snapshot_id)

    async with httpx.AsyncClient(timeout=120.0) as client:
        while True:
            resp = await client.get(
                url,
                headers={"Authorization": f"Bearer {api_key}"},
                params={"format": "json"},
            )
            if resp.status_code == 200:
                break
            if resp.status_code == 202:
                if time.monotonic() > deadline:
                    raise RuntimeError(
                        f"Download still not ready (snapshot={snapshot_id}): {resp.text[:200]}"
                    )
                logger.info("[BDFilter] Download 202 — retrying in %ds", POLL_INTERVAL)
                await asyncio.sleep(POLL_INTERVAL)
                continue
            raise RuntimeError(
                f"BrightData download returned {resp.status_code}: {resp.text[:300]}"
            )

    records = resp.json()
    if not isinstance(records, list):
        if isinstance(records, dict):
            records = records.get("data") or records.get("records") or []
        else:
            records = []
    logger.info("[BDFilter] Downloaded %d records", len(records))
    return records


# ─────────────────────────────────────────────────────────────────────────────
# Profile mapper
# ─────────────────────────────────────────────────────────────────────────────

def _map_to_lead(profile: dict, org_id: str) -> Optional[dict]:
    raw_url = (
        profile.get("url") or profile.get("linkedin_url") or profile.get("input_url")
        or profile.get("profile_url") or profile.get("linkedin_profile_url")
        or profile.get("profile_link") or ""
    )
    linkedin_url = _clean_bd_linkedin_url(raw_url)

    if not linkedin_url or "linkedin.com/in/" not in linkedin_url:
        name    = profile.get("name") or ""
        company = profile.get("current_company_name") or ""
        title   = profile.get("position") or profile.get("headline") or profile.get("title") or ""
        key     = f"bd:{name}:{company}:{title}".lower().strip()
        syn_id  = hashlib.md5(key.encode()).hexdigest()[:16]
        linkedin_url = f"https://brightdata.filter/{syn_id}"

    lead_id_val        = _lead_id(linkedin_url)
    name               = profile.get("name") or ""
    first_name, last_name = _parse_name(name)
    skills_raw         = profile.get("skills") or []
    edu_raw            = profile.get("education") or []
    langs_raw          = profile.get("languages") or []
    certs_raw          = profile.get("certifications") or []
    experience         = profile.get("experience") or []
    prev: list[str]    = []
    for exp in experience[1:6]:
        if isinstance(exp, dict):
            co = exp.get("company") or exp.get("company_name") or ""
            if co:
                prev.append(co)
    activity      = profile.get("activity") or []
    followers     = _safe_int(profile.get("followers") or 0)
    now           = datetime.now(timezone.utc).isoformat()

    return {
        "id":                  lead_id_val,
        "linkedin_url":        linkedin_url,
        "organization_id":     org_id,
        "status":              "bd_filter",
        "name":                name,
        "first_name":          first_name,
        "last_name":           last_name,
        "avatar_url":          profile.get("avatar_url") or profile.get("avatar") or "",
        "title":               profile.get("position") or profile.get("headline") or profile.get("title") or "",
        "company":             profile.get("current_company_name") or "",
        "company_linkedin":    profile.get("current_company_link") or "",
        "company_logo":        profile.get("current_company_logo") or "",
        "company_description": profile.get("about") or profile.get("summary") or "",
        "city":                profile.get("city") or "",
        "country":             profile.get("country") or profile.get("country_code") or "",
        "twitter":             profile.get("twitter") or "",
        "top_skills":          json.dumps(skills_raw[:20]) if skills_raw else "[]",
        "education":           json.dumps(edu_raw) if edu_raw else "[]",
        "languages":           json.dumps(langs_raw) if langs_raw else "[]",
        "certifications":      json.dumps(certs_raw) if certs_raw else "[]",
        "previous_companies":  json.dumps(prev),
        "linkedin_followers":  followers,
        "activity_feed":       json.dumps(activity[:10]) if activity else "[]",
        "raw_brightdata":      json.dumps(profile, default=str),
        "enriched_at":         now,
        "created_at":          now,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Background job — runs the full poll → download → upsert pipeline
# ─────────────────────────────────────────────────────────────────────────────

async def run_snapshot_job(snapshot_id: str, org_id: str, timeout: int) -> None:
    """
    Called as a FastAPI BackgroundTask after the snapshot is triggered.
    Updates _jobs[snapshot_id] throughout so the frontend can poll status.
    """
    api_key = _bd_api_key()
    job     = _jobs[snapshot_id]

    try:
        # Phase 2 — poll
        job["status"]   = "building"
        job["bd_status"] = "scheduled"
        await _wait_for_snapshot(api_key, snapshot_id, timeout)

        # Phase 3 — download
        job["status"] = "downloading"
        raw_records   = await _download_snapshot(api_key, snapshot_id, timeout)

        # Phase 4 — save
        job["status"] = "saving"
        saved:   list[dict] = []
        skipped: int = 0
        failed:  int = 0

        for raw in raw_records:
            try:
                normalised = _normalize_bd_profile(raw)
                lead       = _map_to_lead(normalised, org_id)
                if lead is None:
                    skipped += 1
                    continue
                await _upsert_lead(lead)
                saved.append(lead)
            except Exception as exc:
                failed += 1
                logger.error("[BDFilter] Save error: %s  raw=%s", exc, str(raw)[:120])

        job.update({
            "status":         "done",
            "total_returned": len(raw_records),
            "saved":          len(saved),
            "skipped":        skipped,
            "failed":         failed,
            "results":        saved,
            "completed_at":   datetime.now(timezone.utc).isoformat(),
        })
        logger.info(
            "[BDFilter] Job done  snapshot=%s  returned=%d  saved=%d  skipped=%d  failed=%d",
            snapshot_id, len(raw_records), len(saved), skipped, failed,
        )

    except Exception as exc:
        job.update({"status": "failed", "error": str(exc)})
        logger.error("[BDFilter] Job failed  snapshot=%s  error=%s", snapshot_id, exc)


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

async def trigger_search(
    filters: dict,
    org_id:  str = "default",
    limit:   int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_TIMEOUT,
) -> dict:
    """
    Trigger a BrightData snapshot and register a background job.
    Returns immediately with snapshot_id and status "processing".
    Caller must schedule run_snapshot_job() as a background task.
    """
    limit   = max(1, limit)
    api_key = _bd_api_key()
    if not api_key:
        raise RuntimeError("BRIGHT_DATA_API_KEY is not configured in backend/.env")

    snapshot_id = await _trigger_snapshot(api_key, filters, limit)

    _jobs[snapshot_id] = {
        "snapshot_id":  snapshot_id,
        "dataset_id":   LINKEDIN_DATASET_ID,
        "org_id":       org_id,
        "timeout":      timeout,
        "status":       "processing",
        "bd_status":    "scheduled",
        "triggered_at": datetime.now(timezone.utc).isoformat(),
    }

    return {
        "snapshot_id": snapshot_id,
        "dataset_id":  LINKEDIN_DATASET_ID,
        "status":      "processing",
    }

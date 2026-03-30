"""
import_routes.py
----------------
POST /api/import/preview               — parse file → columns + sample + suggested mapping
POST /api/import/start                 — kick off chunked async import job, return {job_id}
GET  /api/import/status/{job_id}       — poll single job progress
GET  /api/import/folder-status/{id}    — poll all jobs in a folder batch (single request)
GET  /api/import/history               — paginated import history
GET  /api/import/imported-files        — filenames already imported (for folder dedup check)
"""

from __future__ import annotations

import json
import logging

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from import_service import (
    IMPORTABLE_FIELDS,
    get_folder_status,
    get_import_history,
    get_imported_filenames,
    parse_preview,
    start_import_job,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/import", tags=["import"])

MAX_FILE_SIZE = 200 * 1024 * 1024  # 200 MB


# ─────────────────────────────────────────────────────────────────────────────
# Preview
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/preview")
async def preview_file(file: UploadFile = File(...)):
    """
    Upload a file → column headers, first 10 rows, auto-suggested mapping, file_hash.
    file_hash lets the frontend detect duplicate files before starting import.
    """
    contents = await file.read()
    if len(contents) > MAX_FILE_SIZE:
        raise HTTPException(status_code=413, detail="File exceeds 200 MB limit")
    try:
        result = parse_preview(contents, file.filename or "upload")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("Preview failed")
        raise HTTPException(status_code=500, detail=str(exc))

    result["importable_fields"] = sorted(IMPORTABLE_FIELDS)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Start (async job)
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/start")
async def start_import(
    file:      UploadFile = File(...),
    mapping:   str        = Form(...),   # JSON: {"Source Col": "db_field", ...}
    org_id:    str        = Form(...),
    folder_id: str        = Form(None),  # Optional: groups files into a folder batch
):
    """
    Begin import as a background job.
    - Returns {job_id} immediately.
    - File is parsed → COPY to staging → split into 50K-row chunks → Redis queue.
    - 4 Redis workers process chunks in parallel (INSERT SELECT with ON CONFLICT dedup).
    - Pass folder_id to group multiple files; poll /folder-status/{folder_id} for batch progress.
    """
    contents = await file.read()
    if len(contents) > MAX_FILE_SIZE:
        raise HTTPException(status_code=413, detail="File exceeds 200 MB limit")

    try:
        mapping_dict: dict[str, str] = json.loads(mapping)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="mapping must be valid JSON")

    try:
        job_id = await start_import_job(
            contents,
            file.filename or "upload",
            mapping_dict,
            org_id,
            folder_id=folder_id or None,
        )
    except Exception as exc:
        logger.exception("Failed to start import job")
        raise HTTPException(status_code=500, detail=str(exc))

    return {"job_id": job_id, "status": "pending", "folder_id": folder_id}


# ─────────────────────────────────────────────────────────────────────────────
# Single-job status
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/status/{job_id}")
async def import_status(job_id: str):
    """
    Poll a single import job.

    Returns:
        {
          "job_id": "...",
          "status": "pending|running|completed|failed",
          "total": 200000,
          "processed": 154000,
          "new_count": 120000,    -- freshly inserted
          "updated_count": 34000, -- duplicate rows updated in-place
          "skipped": 5000,        -- rows with no identity key
          "pct": 77,
          "error": null
        }
    """
    from db import get_pool

    async with get_pool().acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT status, total_urls, processed, failed,
                   new_count, updated_count, skipped, error
            FROM enrichment_jobs WHERE id=$1
            """,
            job_id,
        )

    if not row:
        raise HTTPException(status_code=404, detail="Job not found")

    total     = int(row["total_urls"] or 0)
    processed = int(row["processed"]  or 0)
    pct       = round(processed / total * 100) if total else 0

    return {
        "job_id":        job_id,
        "status":        row["status"],
        "total":         total,
        "processed":     processed,
        "new_count":     int(row["new_count"]     or 0),
        "updated_count": int(row["updated_count"] or 0),
        "skipped":       int(row["skipped"]       or 0),
        "failed":        int(row["failed"]        or 0),
        "pct":           pct,
        "error":         row["error"],
    }


# ─────────────────────────────────────────────────────────────────────────────
# Folder batch status  (single poll for ALL files in a folder import)
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/folder-status/{folder_id}")
async def folder_status(folder_id: str, org_id: str):
    """
    Returns the status of all import jobs belonging to a folder batch.

    Response:
        {
          "folder_id": "...",
          "total_files": 10,
          "completed": 3,
          "failed": 0,
          "running": 7,
          "done": false,
          "total_new": 15000,
          "total_updated": 2000,
          "total_skipped": 500,
          "jobs": [
            { "id": "...", "filename": "...", "status": "running",
              "total": 50000, "processed": 22000, "pct": 44, ... },
            ...
          ]
        }
    """
    try:
        return await get_folder_status(folder_id, org_id)
    except Exception as exc:
        logger.exception("Failed to get folder status")
        raise HTTPException(status_code=500, detail=str(exc))


# ─────────────────────────────────────────────────────────────────────────────
# History
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/history")
async def import_history(org_id: str, page: int = 1, limit: int = 30):
    """Paginated import job history — shows filename, status, new/updated/skipped counts."""
    try:
        return await get_import_history(org_id, page=page, limit=limit)
    except Exception as exc:
        logger.exception("Failed to fetch import history")
        raise HTTPException(status_code=500, detail=str(exc))


# ─────────────────────────────────────────────────────────────────────────────
# Already-imported filenames (folder duplicate detection)
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/imported-files")
async def imported_files(org_id: str):
    """Returns list of filenames already successfully imported — for folder dedup warning."""
    try:
        return {"filenames": await get_imported_filenames(org_id)}
    except Exception as exc:
        logger.exception("Failed to fetch imported filenames")
        raise HTTPException(status_code=500, detail=str(exc))

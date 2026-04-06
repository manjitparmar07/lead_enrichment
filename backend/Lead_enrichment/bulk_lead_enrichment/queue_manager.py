"""
queue_manager.py
-----------------
WorksBuddy Lead Enrichment — Fair Multi-Tenant Queue System

Architecture:
  ┌──────────────────────────────────────────────────────────────┐
  │  INGESTION                                                   │
  │  push_job(job_id, org_id, urls)                             │
  │    → compute_chunk_size()  ← Redis memory + CPU adaptive   │
  │    → chunk URLs → RPUSH wb:leads:q:{org_id}  (per-tenant)  │
  │    → ZADD wb:leads:active_tenants  score=now  (round-robin) │
  └──────────────────────────────────┬───────────────────────────┘
                                     │
  ┌──────────────────────────────────▼───────────────────────────┐
  │  FAIR SCHEDULER  (_scheduler_loop)                           │
  │    Every 10ms:                                               │
  │    → ZPOPMIN wb:leads:active_tenants   (oldest-served first) │
  │    → LPOP wb:leads:q:{org_id}          (tenant's next chunk) │
  │    → RPUSH wb:leads:work               (hand to workers)     │
  │    → ZADD  wb:leads:active_tenants score=now  (re-schedule)  │
  └──────────────────────────────────┬───────────────────────────┘
                                     │
  ┌──────────────────────────────────▼───────────────────────────┐
  │  WORKER POOL  (N workers, default 4)                         │
  │    BLPOP wb:leads:work                                       │
  │    → asyncio.gather(*[enrich_single(url) for url in chunk]) │
  │    → update job progress → publish Ably events              │
  └──────────────────────────────────────────────────────────────┘

Dynamic Chunk Size:
  Computed at ingestion using Redis memory + system CPU:
  ┌─────────────────┬──────────────────┬──────────────┐
  │ Redis free mem  │ CPU load         │  Chunk size  │
  ├─────────────────┼──────────────────┼──────────────┤
  │ >70%            │ <60%             │  100         │
  │ >50%            │ <60%             │  50          │
  │ >30%            │ 60-80%           │  25          │
  │ >15%            │ 80-90%           │  15          │
  │ ≤15%            │ any              │  5           │
  └─────────────────┴──────────────────┴──────────────┘

Redis keys:
  wb:leads:q:{org_id}       List   per-tenant chunk queue
  wb:leads:active_tenants   ZSet   org_id → last_served_ts (round-robin)
  wb:leads:work             List   pre-scheduled work → workers
  wb:leads:sys_stats        Hash   last chunk_size, free_pct, cpu_pct, updated_at

Env vars:
  REDIS_URL              redis://localhost:6379/0
  LEAD_WORKER_COUNT      number of concurrent workers (default 4)
  CHUNK_MIN              min chunk size (default 5)
  CHUNK_MAX              max chunk size (default 100)
  CHUNK_DEFAULT          fallback chunk size (default 25)
  CHUNK_REFRESH_SECS     how often to recompute chunk size (default 30)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
REDIS_URL          = os.getenv("REDIS_URL", "redis://localhost:6379/0")
NUM_WORKERS        = int(os.getenv("LEAD_WORKER_COUNT", "4"))
CHUNK_MIN          = int(os.getenv("CHUNK_MIN", "5"))
CHUNK_MAX          = int(os.getenv("CHUNK_MAX", "100"))
CHUNK_DEFAULT      = int(os.getenv("CHUNK_DEFAULT", "25"))
CHUNK_REFRESH_SECS = int(os.getenv("CHUNK_REFRESH_SECS", "30"))

# ── Retry / DLQ config ────────────────────────────────────────────────────────
MAX_TASK_ATTEMPTS  = int(os.getenv("QUEUE_MAX_ATTEMPTS", "3"))   # retries before DLQ
# Lock TTL: chunk_size(25) × max_url_time(20s) = 500s — 600s gives buffer.
# Env-override for large chunks. Lower value means faster recovery on worker crash.
CHUNK_LOCK_TTL     = int(os.getenv("CHUNK_LOCK_TTL", "600"))      # exactly-once lock TTL (s)

# ── Autoscaler config ─────────────────────────────────────────────────────────
WORKER_MAX           = int(os.getenv("LEAD_WORKER_MAX", str(NUM_WORKERS * 3)))
AUTOSCALE_INTERVAL   = int(os.getenv("AUTOSCALE_INTERVAL_SECS", "15"))
SCALE_UP_THRESHOLD   = int(os.getenv("QUEUE_SCALE_UP_DEPTH",   "10"))
SCALE_DOWN_THRESHOLD = int(os.getenv("QUEUE_SCALE_DOWN_DEPTH", "2"))

# ── Redis keys ────────────────────────────────────────────────────────────────
KEY_ACTIVE_TENANTS = "wb:leads:active_tenants"  # ZSet: org_id → last_served_ts
KEY_WORK_QUEUE     = "wb:leads:work"             # List: scheduler → workers
KEY_SYS_STATS      = "wb:leads:sys_stats"        # Hash: monitoring
KEY_DLQ            = "wb:leads:dlq"              # List: dead-letter queue

def _tenant_queue_key(org_id: str) -> str:
    return f"wb:leads:q:{org_id}"

def _chunk_lock_key(sub_job_id: str) -> str:
    """Redis key for the exactly-once processing lock on a chunk."""
    return f"wb:leads:chunk_lock:{sub_job_id}"


# ═════════════════════════════════════════════════════════════════════════════
# AI Worker System — raw_profile → LLM (Qwen 70B / HuggingFace / WB) → completed
#
# Architecture:
#   push_ai_job()  →  wb:ai:q:{org_id}  →  _ai_scheduler_loop()
#                  →  wb:ai:work        →  _ai_worker()  →  DB upsert
#
# Redis keys (separate namespace — no clash with lead enrichment keys):
#   wb:ai:q:{org_id}       List   per-org chunks of lead_ids
#   wb:ai:active_orgs      ZSet   org_id → last_served_ts (round-robin)
#   wb:ai:work             List   scheduler → AI workers
#   wb:ai:lock:{lead_id}   String exactly-once per-lead lock (TTL 600s)
#   wb:ai:dlq              List   failed lead_ids after MAX retries
#   wb:ai:stats            Hash   monitoring: workers, throughput, errors
#
# Concurrency:
#   AI_WORKERS   = AI worker pool size (default 3)
#   AI_CHUNK     = lead_ids per chunk (fixed 5 — HF is slow, ~30-60s/call)
#   HF_SEM       = asyncio.Semaphore — caps parallel HF calls across all workers
#
# Status transitions:
#   scraping → ai_processing → completed
#                            ↘ failed  (after MAX_AI_ATTEMPTS)
# ═════════════════════════════════════════════════════════════════════════════

AI_WORKERS       = int(os.getenv("AI_WORKER_COUNT",   "3"))
AI_CHUNK         = int(os.getenv("AI_CHUNK_SIZE",     "5"))
AI_LOCK_TTL      = int(os.getenv("AI_LOCK_TTL",       "600"))   # seconds
MAX_AI_ATTEMPTS  = int(os.getenv("AI_MAX_ATTEMPTS",   "3"))
HF_CONCURRENCY   = int(os.getenv("HF_CONCURRENCY",    "20"))    # max parallel HF calls

KEY_AI_ACTIVE    = "wb:ai:active_orgs"   # ZSet: org_id → last_served_ts
KEY_AI_WORK      = "wb:ai:work"          # List: scheduler → AI workers
KEY_AI_DLQ       = "wb:ai:dlq"          # List: dead-letter queue
KEY_AI_STATS     = "wb:ai:stats"         # Hash: monitoring

def _ai_tenant_key(org_id: str) -> str:
    return f"wb:ai:q:{org_id}"

def _ai_lock_key(lead_id: str) -> str:
    return f"wb:ai:lock:{lead_id}"

# Global semaphore — created lazily on first use (must be in async context)
_hf_semaphore: Optional[asyncio.Semaphore] = None

def _get_hf_semaphore() -> asyncio.Semaphore:
    global _hf_semaphore
    if _hf_semaphore is None:
        _hf_semaphore = asyncio.Semaphore(HF_CONCURRENCY)
    return _hf_semaphore

# AI worker module state
_ai_worker_tasks: list[asyncio.Task]       = []
_ai_scheduler_task: Optional[asyncio.Task] = None
_ai_next_worker_id: int                    = 0

# ── Module state ──────────────────────────────────────────────────────────────
_worker_tasks: list[asyncio.Task]       = []
_scheduler_task: Optional[asyncio.Task] = None
_chunk_refresher_task: Optional[asyncio.Task] = None
_autoscaler_task: Optional[asyncio.Task] = None
_current_chunk_size: int = CHUNK_DEFAULT
_next_worker_id: int     = 0   # monotonically increasing; never reused


# ─────────────────────────────────────────────────────────────────────────────
# Redis connection
# ─────────────────────────────────────────────────────────────────────────────

async def _make_redis() -> Any:
    """Create a fresh async Redis connection. Returns None if package missing."""
    try:
        import redis.asyncio as aioredis
        return aioredis.from_url(REDIS_URL, decode_responses=True)
    except ImportError:
        logger.error("[QueueManager] redis package not installed — pip install redis>=5.0.0")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Dynamic chunk size — adapts to Redis memory + CPU pressure
# ─────────────────────────────────────────────────────────────────────────────

async def compute_chunk_size(r: Any) -> int:
    """
    Compute optimal chunk size based on:
      1. Redis free memory percentage
      2. System CPU utilisation (via psutil — gracefully skipped if not installed)

    Higher free resources → larger chunks → better throughput per worker.
    Memory pressure / high CPU → smaller chunks → avoid OOM / worker starvation.
    """
    global _current_chunk_size

    free_pct  = 100.0
    cpu_pct   = 0.0

    # ── Redis memory ──────────────────────────────────────────────────────────
    try:
        info   = await r.info("memory")
        used   = int(info.get("used_memory", 0))
        maxmem = int(info.get("maxmemory", 0))

        if maxmem > 0:
            free_pct = max(0.0, (maxmem - used) / maxmem * 100)
        else:
            # No maxmemory configured — fall back to OS available RAM
            try:
                import psutil
                vm = psutil.virtual_memory()
                free_pct = vm.available / vm.total * 100
            except ImportError:
                free_pct = 60.0  # assume reasonable headroom
    except Exception as e:
        logger.debug("[ChunkSize] Redis info failed: %s", e)

    # ── CPU utilisation ───────────────────────────────────────────────────────
    try:
        import psutil
        cpu_pct = psutil.cpu_percent(interval=None)  # non-blocking, uses cached value
    except ImportError:
        pass  # psutil optional
    except Exception:
        pass

    # ── Decision table ────────────────────────────────────────────────────────
    if free_pct > 70:
        chunk = CHUNK_MAX          # plenty of room — max throughput
    elif free_pct > 50:
        chunk = 50
    elif free_pct > 30:
        chunk = CHUNK_DEFAULT      # moderate pressure
    elif free_pct > 15:
        chunk = 15
    else:
        chunk = CHUNK_MIN          # memory critical — minimum chunks

    # CPU throttle: reduce chunk size under high CPU
    if cpu_pct > 90:
        chunk = max(CHUNK_MIN, chunk // 4)
    elif cpu_pct > 80:
        chunk = max(CHUNK_MIN, chunk // 2)
    elif cpu_pct > 60:
        chunk = max(CHUNK_MIN, int(chunk * 0.75))

    # Clamp to configured bounds
    chunk = max(CHUNK_MIN, min(CHUNK_MAX, chunk))
    _current_chunk_size = chunk

    # Persist stats for monitoring
    try:
        await r.hset(KEY_SYS_STATS, mapping={
            "chunk_size":  chunk,
            "free_pct":    round(free_pct, 1),
            "cpu_pct":     round(cpu_pct, 1),
            "updated_at":  round(time.time()),
        })
    except Exception:
        pass

    logger.debug(
        "[ChunkSize] chunk=%d  redis_free=%.1f%%  cpu=%.1f%%",
        chunk, free_pct, cpu_pct,
    )
    return chunk


async def _chunk_size_refresh_loop(r: Any) -> None:
    """Background task — re-evaluates chunk size every CHUNK_REFRESH_SECS seconds."""
    while True:
        try:
            await asyncio.sleep(CHUNK_REFRESH_SECS)
            await compute_chunk_size(r)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug("[ChunkSize] Refresh error: %s", e)


# ─────────────────────────────────────────────────────────────────────────────
# Ingestion — push a bulk job into the per-tenant queue
# ─────────────────────────────────────────────────────────────────────────────

async def push_job(
    job_id: str,
    org_id: str,
    chunks: list[list[str]],
    sub_job_ids: list[str],
    r: Any,
    generate_outreach: bool = True,
    sso_id: str = "",
    forward_to_lio: bool = False,
    system_prompt: str = "",
) -> int:
    """
    Push pre-split URL chunks to the per-tenant queue.

    - Caller computes chunk size via compute_chunk_size() and splits URLs before calling.
    - Each chunk is a single JSON task pushed to wb:leads:q:{org_id}.
    - sub_job_ids aligns with chunks so each worker can update the right sub-job record.
    - Org is registered in wb:leads:active_tenants with NX so it doesn't
      reset the round-robin position if already queued.

    Returns the number of chunks enqueued.
    """
    total_urls = sum(len(c) for c in chunks)
    tenant_key = _tenant_queue_key(org_id)

    pipe = r.pipeline()
    for seq, (chunk, sub_job_id) in enumerate(zip(chunks, sub_job_ids)):
        task = {
            "job_id":            job_id,
            "org_id":            org_id,
            "urls":              chunk,
            "sub_job_id":        sub_job_id,
            "chunk_seq":         seq,          # ordering: position within this job
            "total_chunks":      len(chunks),  # ordering: lets worker know when job is fully queued
            "attempt":           0,            # retry counter — incremented on re-queue
            "generate_outreach": generate_outreach,
            "sso_id":            sso_id,
            "forward_to_lio":    forward_to_lio,
            "system_prompt":     system_prompt or "",
        }
        pipe.rpush(tenant_key, json.dumps(task))

    # NX=True: only add to active set if not already present
    # preserves existing round-robin position for tenants with ongoing jobs
    pipe.zadd(KEY_ACTIVE_TENANTS, {org_id: time.time()}, nx=True)

    await pipe.execute()

    logger.info(
        "[Queue] Job %s queued: %d URLs → %d chunks org=%s",
        job_id, total_urls, len(chunks), org_id,
    )
    return len(chunks)


# ─────────────────────────────────────────────────────────────────────────────
# Fair Scheduler — round-robin across active tenants
# ─────────────────────────────────────────────────────────────────────────────

async def _scheduler_loop(r: Any) -> None:
    """
    Fair round-robin scheduler.

    Each iteration:
      1. ZPOPMIN active_tenants → tenant with oldest last-served timestamp
      2. LPOP wb:leads:q:{org_id} → their next chunk
      3. RPUSH wb:leads:work → hand off to worker pool
      4. ZADD active_tenants score=now → re-queue at back of round-robin
         (if queue empty, tenant is dropped from active set)

    This guarantees that 1000 tenants each submitting 500 URLs are served
    in strict round-robin — no tenant waits behind another's entire backlog.
    """
    logger.info("[Scheduler] Started — fair round-robin across tenants")

    while True:
        try:
            # Get tenant with oldest last-served time (lowest score)
            result = await r.zpopmin(KEY_ACTIVE_TENANTS, 1)
            if not result:
                await asyncio.sleep(0.05)   # no active tenants — idle
                continue

            org_id, _ = result[0]
            tenant_key = _tenant_queue_key(org_id)
            chunk_raw = await r.lpop(tenant_key)

            if chunk_raw:
                await r.rpush(KEY_WORK_QUEUE, chunk_raw)
                # Re-register with current timestamp → goes to back of round-robin
                await r.zadd(KEY_ACTIVE_TENANTS, {org_id: time.time()})
            else:
                # Tenant queue exhausted — already removed from ZSet by zpopmin
                logger.debug("[Scheduler] Org %s finished — removed from active set", org_id)

            await asyncio.sleep(0.01)   # 10ms yield prevents tight spin

        except asyncio.CancelledError:
            logger.info("[Scheduler] Stopped")
            break
        except Exception as e:
            logger.error("[Scheduler] Error: %s", e, exc_info=True)
            await asyncio.sleep(0.5)   # backoff on unexpected errors


# ─────────────────────────────────────────────────────────────────────────────
# Worker — processes chunks from wb:leads:work
# ─────────────────────────────────────────────────────────────────────────────

async def _push_to_dlq(r: Any, task: dict, reason: str) -> None:
    """Push a failed task to the dead-letter queue with metadata."""
    dlq_entry = {
        **task,
        "dlq_reason":   reason,
        "dlq_at":       round(time.time()),
        "dlq_attempts": task.get("attempt", 0),
    }
    try:
        await r.rpush(KEY_DLQ, json.dumps(dlq_entry))
        logger.error(
            "[DLQ] job=%s sub=%s seq=%s — moved to DLQ after %d attempts. Reason: %s",
            task.get("job_id", "—"), task.get("sub_job_id", "—"),
            task.get("chunk_seq", "—"), task.get("attempt", 0), reason,
        )
    except Exception as e:
        logger.error("[DLQ] Failed to write to DLQ: %s — task lost: %s", e, task.get("job_id"))


async def _requeue_task(r: Any, task: dict) -> None:
    """Re-push a task to its tenant queue with incremented attempt counter."""
    attempt = task.get("attempt", 0) + 1
    task = {**task, "attempt": attempt}
    org_id     = task.get("org_id", "default")
    tenant_key = _tenant_queue_key(org_id)
    await r.rpush(tenant_key, json.dumps(task))
    await r.zadd(KEY_ACTIVE_TENANTS, {org_id: time.time()}, nx=True)
    logger.warning(
        "[Queue] Requeued job=%s sub=%s (attempt %d/%d)",
        task.get("job_id", "—"), task.get("sub_job_id", "—"),
        attempt, MAX_TASK_ATTEMPTS,
    )


async def get_dlq_items(limit: int = 100) -> list[dict]:
    """Return up to `limit` items from the dead-letter queue (without removing them)."""
    r = await _make_redis()
    if r is None:
        return []
    try:
        raws = await r.lrange(KEY_DLQ, 0, limit - 1)
        return [json.loads(raw) for raw in raws if raw]
    except Exception as e:
        logger.error("[DLQ] get_dlq_items failed: %s", e)
        return []
    finally:
        await r.aclose()


async def retry_dlq_item(index: int = 0) -> bool:
    """
    Pop one item from the DLQ by index, reset its attempt counter, and re-enqueue it.
    Returns True if an item was retried, False if DLQ was empty or index out of range.
    """
    r = await _make_redis()
    if r is None:
        return False
    try:
        # LINDEX to inspect, then LREM to remove the specific item
        raw = await r.lindex(KEY_DLQ, index)
        if raw is None:
            return False
        task = json.loads(raw)
        await r.lrem(KEY_DLQ, 1, raw)
        task["attempt"] = 0
        await _requeue_task(r, task)
        logger.info("[DLQ] Retried job=%s sub=%s", task.get("job_id", "—"), task.get("sub_job_id", "—"))
        return True
    except Exception as e:
        logger.error("[DLQ] retry_dlq_item failed: %s", e)
        return False
    finally:
        await r.aclose()


async def _worker(worker_id: int, r: Any) -> None:
    """
    Worker loop.

    Pulls pre-scheduled chunks from wb:leads:work (BLPOP) and processes all
    URLs in the chunk concurrently via asyncio.gather, then updates job
    progress and publishes Ably events.

    Guarantees:
      - Exactly-once: SET NX chunk lock prevents duplicate processing.
      - Retry: total-failure chunks are re-queued up to MAX_TASK_ATTEMPTS times.
      - DLQ: chunks exceeding MAX_TASK_ATTEMPTS are pushed to wb:leads:dlq.
    """
    from lead_enrichment_brightdata_service import (
        enrich_single,
        _publish_lead_done,
        _publish_job_done,
        _update_job,
        _update_sub_job,
        get_job,
    )

    logger.info("[Worker-%d] Started", worker_id)

    while True:
        try:
            item = await r.blpop([KEY_WORK_QUEUE], timeout=2)
            if item is None:
                continue   # timeout — poll again

            _, raw = item
            try:
                task = json.loads(raw)
            except Exception:
                logger.warning("[Worker-%d] Invalid JSON task — discarded", worker_id)
                continue

            job_id         = task.get("job_id", "")
            org_id         = task.get("org_id", "default")
            urls           = task.get("urls", [])
            sub_job_id     = task.get("sub_job_id")
            chunk_seq      = task.get("chunk_seq", 0)
            attempt        = task.get("attempt", 0)
            gen_out        = task.get("generate_outreach", True)
            sso_id         = task.get("sso_id", "")
            forward_to_lio = task.get("forward_to_lio", False)
            system_prompt  = task.get("system_prompt") or None

            if not urls:
                continue

            # ── Exactly-once: claim chunk via SET NX ─────────────────────────
            # Prevents duplicate processing if the same chunk is somehow delivered
            # twice (e.g., re-queue race condition or operator retry).
            if sub_job_id:
                lock_key     = _chunk_lock_key(sub_job_id)
                lock_claimed = await r.set(lock_key, worker_id, nx=True, ex=CHUNK_LOCK_TTL)
                if not lock_claimed:
                    logger.warning(
                        "[Worker-%d] Chunk %s already claimed — skipping (exactly-once)",
                        worker_id, sub_job_id,
                    )
                    continue

            logger.info(
                "[Worker-%d] chunk=%d urls | job=%s sub=%s seq=%s attempt=%d org=%s",
                worker_id, len(urls), job_id or "—", sub_job_id or "—",
                chunk_seq, attempt, org_id,
            )

            # ── Check if job was cancelled before starting chunk ─────────────
            if job_id:
                try:
                    job_row = await get_job(job_id)
                    if job_row and job_row.get("status") == "cancelled":
                        logger.info("[Worker-%d] Job %s cancelled — skipping chunk", worker_id, job_id)
                        if sub_job_id:
                            await _update_sub_job(sub_job_id, status="cancelled")
                        continue
                except Exception:
                    pass

            # Mark sub-job as running
            if sub_job_id:
                try:
                    await _update_sub_job(sub_job_id, status="running")
                except Exception:
                    pass

            # ── Process entire chunk in parallel ─────────────────────────────
            results = await asyncio.gather(
                *[
                    enrich_single(
                        linkedin_url=url,
                        job_id=job_id or None,
                        org_id=org_id,
                        generate_outreach_flag=gen_out,
                        sso_id=sso_id,
                        forward_to_lio=forward_to_lio,
                        system_prompt=system_prompt,
                        skip_contact=True,
                    )
                    for url in urls
                ],
                return_exceptions=True,
            )

            ok    = sum(1 for r_ in results if not isinstance(r_, Exception))
            fail  = len(results) - ok
            cache = sum(1 for r_ in results if not isinstance(r_, Exception) and r_.get("_cache_hit"))

            if cache:
                logger.info("[Worker-%d] %d/%d served from cache", worker_id, cache, ok)

            if fail:
                for i, r_ in enumerate(results):
                    if isinstance(r_, Exception):
                        logger.warning(
                            "[Worker-%d] enrich_single failed for %s: %s",
                            worker_id, urls[i], r_,
                        )

            # ── Retry / DLQ on total failure ──────────────────────────────────
            # Only retry when EVERY URL in the chunk failed — partial success is
            # accepted so we don't re-process already-enriched leads.
            if ok == 0 and fail > 0:
                if attempt < MAX_TASK_ATTEMPTS - 1:
                    # Release the exactly-once lock so the retry can claim it
                    if sub_job_id:
                        try:
                            await r.delete(_chunk_lock_key(sub_job_id))
                        except Exception:
                            pass
                    await _requeue_task(r, task)
                    if sub_job_id:
                        try:
                            await _update_sub_job(sub_job_id, status="failed")
                        except Exception:
                            pass
                    continue
                else:
                    await _push_to_dlq(r, task, reason=f"all {fail} urls failed after {attempt + 1} attempts")
                    if sub_job_id:
                        try:
                            await _update_sub_job(sub_job_id, status="failed")
                        except Exception:
                            pass

            # ── Update sub-job progress ───────────────────────────────────────
            if sub_job_id and ok > 0:
                try:
                    sub_status = (
                        "completed" if fail == 0
                        else "completed_with_errors"
                    )
                    await _update_sub_job(sub_job_id, status=sub_status, processed=ok, failed=fail)
                except Exception as e:
                    logger.warning("[Worker-%d] Sub-job update failed: %s", worker_id, e)

            # ── Update job progress ───────────────────────────────────────────
            if job_id:
                try:
                    job = await get_job(job_id, org_id=org_id)
                    if job:
                        new_proc = job.get("processed", 0) + ok
                        new_fail = job.get("failed", 0) + fail
                        await _update_job(job_id, processed=new_proc, failed=new_fail)

                        # Publish per-lead events for successful results
                        for lead in results:
                            if not isinstance(lead, Exception):
                                await _publish_lead_done(org_id, job_id, lead)

                        # Check if job is fully done
                        total = job.get("total_urls", 0)
                        if new_proc + new_fail >= total:
                            status = (
                                "completed"
                                if new_fail == 0
                                else ("failed" if new_proc == 0 else "completed_with_errors")
                            )
                            await _update_job(job_id, status=status)
                            await _publish_job_done(org_id, job_id, new_proc, new_fail)
                            logger.info(
                                "[Worker-%d] Job %s DONE — processed=%d failed=%d status=%s",
                                worker_id, job_id, new_proc, new_fail, status,
                            )
                except Exception as e:
                    logger.error("[Worker-%d] Job update failed: %s", worker_id, e)

        except asyncio.CancelledError:
            logger.info("[Worker-%d] Cancelled — shutting down", worker_id)
            break
        except Exception as e:
            logger.error("[Worker-%d] Unexpected error: %s", worker_id, e, exc_info=True)
            await asyncio.sleep(1)   # backoff before retry


# ─────────────────────────────────────────────────────────────────────────────
# Autoscaler — dynamically grows / shrinks the worker pool
# ─────────────────────────────────────────────────────────────────────────────

async def _autoscaler_loop(r: Any) -> None:
    """
    Adjust the worker pool size based on wb:leads:work queue depth.

    Scale-up rule:  work_depth > SCALE_UP_THRESHOLD   AND workers < WORKER_MAX
    Scale-down rule: work_depth < SCALE_DOWN_THRESHOLD AND workers > NUM_WORKERS (base)

    Workers are spawned with ever-increasing IDs so log lines are unique.
    Scale-down cancels the most recently added worker (LIFO) to minimise
    disruption to in-flight tasks.
    """
    global _worker_tasks, _next_worker_id

    logger.info(
        "[Autoscaler] Started — base=%d max=%d  up@depth>%d  down@depth<%d  interval=%ds",
        NUM_WORKERS, WORKER_MAX, SCALE_UP_THRESHOLD, SCALE_DOWN_THRESHOLD, AUTOSCALE_INTERVAL,
    )

    while True:
        try:
            await asyncio.sleep(AUTOSCALE_INTERVAL)

            # Prune completed/cancelled worker tasks
            _worker_tasks = [t for t in _worker_tasks if not t.done()]
            current = len(_worker_tasks)

            depth = await r.llen(KEY_WORK_QUEUE)

            if depth > SCALE_UP_THRESHOLD and current < WORKER_MAX:
                _next_worker_id += 1
                wid = _next_worker_id
                r_worker = await _make_redis()
                task = asyncio.create_task(
                    _worker(wid, r_worker),
                    name=f"lead-worker-{wid}",
                )
                _worker_tasks.append(task)
                logger.info(
                    "[Autoscaler] Scaled UP   → %d workers (work_depth=%d)",
                    current + 1, depth,
                )

            elif depth < SCALE_DOWN_THRESHOLD and current > NUM_WORKERS:
                # Cancel the last-added worker (lowest risk to in-flight work)
                victim = _worker_tasks.pop()
                victim.cancel()
                logger.info(
                    "[Autoscaler] Scaled DOWN → %d workers (work_depth=%d)",
                    current - 1, depth,
                )

        except asyncio.CancelledError:
            logger.info("[Autoscaler] Stopped")
            break
        except Exception as e:
            logger.warning("[Autoscaler] Error: %s", e)


# ─────────────────────────────────────────────────────────────────────────────
# Lifecycle — start / stop
# ─────────────────────────────────────────────────────────────────────────────

async def start_queue_system() -> bool:
    """
    Start the full queue system:
      - 1 fair scheduler task
      - N worker tasks (LEAD_WORKER_COUNT, base size)
      - 1 chunk-size refresher task
      - 1 autoscaler task (grows/shrinks worker pool dynamically)

    Safe to call multiple times — no-ops if already running.
    Returns True if started, False if Redis unavailable.
    """
    global _worker_tasks, _scheduler_task, _chunk_refresher_task, _autoscaler_task, _next_worker_id

    if _worker_tasks or _scheduler_task:
        logger.debug("[QueueManager] Already running — start() no-op")
        return True

    r = await _make_redis()
    if r is None:
        logger.warning("[QueueManager] Redis package missing — queue system disabled")
        return False

    try:
        await r.ping()
    except Exception as e:
        logger.warning(
            "[QueueManager] Redis unreachable (%s) — queue system disabled. "
            "In-process fallback will handle bulk jobs.",
            e,
        )
        return False

    # Initial chunk size evaluation
    await compute_chunk_size(r)
    await r.aclose()

    logger.info(
        "[QueueManager] Starting — %d workers (max=%d), chunk_size=%d, Redis=%s",
        NUM_WORKERS, WORKER_MAX, _current_chunk_size, REDIS_URL,
    )

    # Chunk size refresher (shared connection)
    r_refresh = await _make_redis()
    _chunk_refresher_task = asyncio.create_task(
        _chunk_size_refresh_loop(r_refresh),
        name="chunk-size-refresher",
    )

    # Scheduler (dedicated connection)
    r_scheduler = await _make_redis()
    _scheduler_task = asyncio.create_task(
        _scheduler_loop(r_scheduler),
        name="lead-scheduler",
    )

    # Base worker pool (each gets its own connection to avoid contention)
    for i in range(NUM_WORKERS):
        _next_worker_id += 1
        r_worker = await _make_redis()
        task = asyncio.create_task(
            _worker(_next_worker_id, r_worker),
            name=f"lead-worker-{_next_worker_id}",
        )
        _worker_tasks.append(task)

    # Autoscaler (dedicated connection — reads work queue depth)
    r_autoscaler = await _make_redis()
    _autoscaler_task = asyncio.create_task(
        _autoscaler_loop(r_autoscaler),
        name="lead-autoscaler",
    )

    logger.info(
        "[QueueManager] Running — scheduler=1  workers=%d  autoscaler=1  chunk_refresher=1",
        NUM_WORKERS,
    )
    return True


async def stop_queue_system() -> None:
    """Cancel all queue system tasks (scheduler + workers + autoscaler + refresher)."""
    global _worker_tasks, _scheduler_task, _chunk_refresher_task, _autoscaler_task

    all_tasks = []
    if _scheduler_task:
        all_tasks.append(_scheduler_task)
        _scheduler_task = None
    if _chunk_refresher_task:
        all_tasks.append(_chunk_refresher_task)
        _chunk_refresher_task = None
    if _autoscaler_task:
        all_tasks.append(_autoscaler_task)
        _autoscaler_task = None
    all_tasks.extend(_worker_tasks)
    _worker_tasks = []

    if not all_tasks:
        return

    logger.info("[QueueManager] Stopping %d tasks…", len(all_tasks))
    for t in all_tasks:
        t.cancel()
    await asyncio.gather(*all_tasks, return_exceptions=True)
    logger.info("[QueueManager] All tasks stopped")


# ─────────────────────────────────────────────────────────────────────────────
# Monitoring — queue stats for /api/leads/queue/stats
# ─────────────────────────────────────────────────────────────────────────────

async def get_queue_stats() -> dict:
    """
    Return a snapshot of the queue system state for the monitoring endpoint.
    Safe to call even if Redis is unavailable.
    """
    r = await _make_redis()
    if r is None:
        return {"redis": "unavailable", "error": "redis package not installed"}

    try:
        await r.ping()
    except Exception as e:
        return {"redis": "unreachable", "error": str(e)}

    try:
        # Active tenants
        active_tenants = await r.zrange(KEY_ACTIVE_TENANTS, 0, -1, withscores=True)
        tenant_info = []
        for org_id, last_served in active_tenants:
            qlen = await r.llen(_tenant_queue_key(org_id))
            tenant_info.append({
                "org_id":       org_id,
                "queue_chunks": qlen,
                "last_served":  round(last_served),
            })

        # Work queue depth (scheduler → workers)
        work_depth = await r.llen(KEY_WORK_QUEUE)

        # Dead-letter queue depth
        dlq_depth = await r.llen(KEY_DLQ)

        # System stats
        sys_raw = await r.hgetall(KEY_SYS_STATS)

        # Redis memory summary
        mem_info = await r.info("memory")

        live_workers = [t for t in _worker_tasks if not t.done()]

        return {
            "redis":              "connected",
            "active_tenants":     len(active_tenants),
            "work_queue_depth":   work_depth,
            "dlq_depth":          dlq_depth,
            "workers_running":    len(live_workers),
            "workers_base":       NUM_WORKERS,
            "workers_max":        WORKER_MAX,
            "autoscaler_running": _autoscaler_task is not None and not _autoscaler_task.done(),
            "scheduler_running":  _scheduler_task is not None and not _scheduler_task.done(),
            "current_chunk_size": _current_chunk_size,
            "tenants":            tenant_info,
            "system": {
                "chunk_size":  int(sys_raw.get("chunk_size", CHUNK_DEFAULT)),
                "free_pct":    float(sys_raw.get("free_pct", 0)),
                "cpu_pct":     float(sys_raw.get("cpu_pct", 0)),
                "updated_at":  int(sys_raw.get("updated_at", 0)),
            },
            "redis_memory": {
                "used_mb":    round(int(mem_info.get("used_memory", 0)) / 1024 / 1024, 2),
                "max_mb":     round(int(mem_info.get("maxmemory", 0)) / 1024 / 1024, 2),
                "peak_mb":    round(int(mem_info.get("used_memory_peak", 0)) / 1024 / 1024, 2),
            },
        }
    except Exception as e:
        logger.error("[QueueStats] Failed: %s", e)
        return {"redis": "error", "error": str(e)}
    finally:
        await r.aclose()


# ─────────────────────────────────────────────────────────────────────────────
# AI Worker System — Ingestion
# ─────────────────────────────────────────────────────────────────────────────

async def push_ai_job(
    job_id: str,
    org_id: str,
    lead_ids: list[str],
    system_prompt: str,
    r: Any,
    sso_id: str = "",
    forward_to_lio: bool = False,
) -> int:
    """
    Push lead_ids into the per-org AI processing queue in chunks of AI_CHUNK.

    Each chunk is one task JSON:
      { job_id, org_id, lead_ids, system_prompt, sso_id, forward_to_lio, attempt }

    Returns the number of chunks enqueued.
    Also marks all leads as status='ai_queued' in DB so the UI shows pending state.
    """
    from lead_enrichment_brightdata_service import _upsert_lead

    if not lead_ids:
        return 0

    # Split into fixed-size chunks (HF is slow — small chunks = better back-pressure)
    chunks = [lead_ids[i:i + AI_CHUNK] for i in range(0, len(lead_ids), AI_CHUNK)]
    tenant_key = _ai_tenant_key(org_id)

    pipe = r.pipeline()
    for seq, chunk in enumerate(chunks):
        task = {
            "job_id":        job_id,
            "org_id":        org_id,
            "lead_ids":      chunk,
            "system_prompt": system_prompt,
            "sso_id":        sso_id,
            "forward_to_lio": forward_to_lio,
            "chunk_seq":     seq,
            "total_chunks":  len(chunks),
            "attempt":       0,
        }
        pipe.rpush(tenant_key, json.dumps(task))

    # NX: don't reset round-robin position if org already active
    pipe.zadd(KEY_AI_ACTIVE, {org_id: time.time()}, nx=True)
    await pipe.execute()

    # Mark all leads as ai_queued so the UI reflects pending state
    for lead_id in lead_ids:
        try:
            await _upsert_lead({"id": lead_id, "status": "ai_queued"})
        except Exception as e:
            logger.debug("[AI-Queue] status update failed for %s: %s", lead_id, e)

    logger.info(
        "[AI-Queue] Job %s enqueued: %d leads → %d chunks  org=%s",
        job_id, len(lead_ids), len(chunks), org_id,
    )
    return len(chunks)


# ─────────────────────────────────────────────────────────────────────────────
# AI Worker System — Fair Scheduler
# ─────────────────────────────────────────────────────────────────────────────

async def _ai_scheduler_loop(r: Any) -> None:
    """
    Same round-robin scheduler as _scheduler_loop but for the AI queue namespace.

    wb:ai:active_orgs (ZSet) → wb:ai:q:{org_id} (List) → wb:ai:work (List)
    """
    logger.info("[AI-Scheduler] Started — fair round-robin for AI processing")

    while True:
        try:
            result = await r.zpopmin(KEY_AI_ACTIVE, 1)
            if not result:
                await asyncio.sleep(0.1)
                continue

            org_id, _ = result[0]
            chunk_raw = await r.lpop(_ai_tenant_key(org_id))

            if chunk_raw:
                await r.rpush(KEY_AI_WORK, chunk_raw)
                await r.zadd(KEY_AI_ACTIVE, {org_id: time.time()})
            else:
                logger.debug("[AI-Scheduler] Org %s AI queue exhausted", org_id)

            await asyncio.sleep(0.01)  # 10ms yield — AI jobs are slower than URL jobs

        except asyncio.CancelledError:
            logger.info("[AI-Scheduler] Stopped")
            break
        except Exception as e:
            logger.error("[AI-Scheduler] Error: %s", e, exc_info=True)
            await asyncio.sleep(1)


# ─────────────────────────────────────────────────────────────────────────────
# AI Worker System — Worker
# ─────────────────────────────────────────────────────────────────────────────

async def _run_single_ai(
    lead_id: str,
    system_prompt: str,
    org_id: str,
    job_id: str,
    sso_id: str,
    forward_to_lio: bool,
    r: Any,
    worker_id: int,
) -> dict:
    """
    Process one lead through the LLM pipeline:
      1. Acquire exactly-once lock
      2. Fetch raw_brightdata from DB
      3. Call LLM (HF Qwen → WB, controlled by semaphore)
      4. Parse JSON response
      5. Upsert lead with enriched fields + status=completed
      6. Optionally forward to LIO

    Returns the updated lead dict on success.
    Raises on unrecoverable error (caller logs and counts as failed).
    """
    from lead_enrichment_brightdata_service import (
        get_lead,
        _upsert_lead,
        _call_llm,
        _format_linkedin_enrich,
        send_to_lio,
    )

    lock_key = _ai_lock_key(lead_id)
    acquired = await r.set(lock_key, worker_id, nx=True, ex=AI_LOCK_TTL)
    if not acquired:
        logger.warning("[AI-Worker-%d] Lead %s already locked — skipping", worker_id, lead_id)
        raise RuntimeError(f"lead {lead_id} already being processed")

    try:
        lead = await get_lead(lead_id)
        if not lead:
            raise ValueError(f"lead {lead_id} not found in DB")

        raw_bd = lead.get("raw_brightdata") or lead.get("raw_profile") or ""
        if isinstance(raw_bd, str):
            try:
                import json as _json
                raw_bd = _json.loads(raw_bd)
            except Exception:
                raw_bd = {"raw": raw_bd}

        # Mark as actively processing so UI can show spinner
        await _upsert_lead({"id": lead_id, "status": "ai_processing"})

        # Build messages: system_prompt + raw profile as user message
        import json as _json
        user_content = (
            f"Here is the raw LinkedIn profile data:\n\n"
            f"```json\n{_json.dumps(raw_bd, indent=2, default=str)[:12000]}\n```\n\n"
            f"Please analyze and return structured enrichment JSON as instructed."
        )
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_content},
        ]

        # Semaphore caps total parallel HF calls across ALL workers
        async with _get_hf_semaphore():
            response_text = await _call_llm(
                messages=messages,
                max_tokens=2000,
                temperature=0.2,
                hf_first=True,   # Qwen 70B via HuggingFace → WB LLM fallback
            )

        if not response_text:
            raise RuntimeError(f"LLM returned empty response for lead {lead_id}")

        # Try to parse JSON from response (model may wrap in markdown fence)
        enrichment = None
        import re
        json_match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", response_text, re.DOTALL)
        if json_match:
            try:
                enrichment = _json.loads(json_match.group(1))
            except Exception:
                pass
        if enrichment is None:
            try:
                enrichment = _json.loads(response_text)
            except Exception:
                enrichment = {"raw_response": response_text}

        # Build final lead update — merge LLM output with existing lead fields
        update: dict = {
            "id":        lead_id,
            "status":    "completed",
            "crm_brief": _json.dumps(enrichment, default=str),
        }

        # Pull well-known top-level fields from enrichment if present
        for field in ("score_tier", "icp_fit_score", "intent_score", "timing_score",
                      "total_score", "score_explanation", "cold_email", "linkedin_note",
                      "email_subject", "best_channel"):
            val = enrichment.get(field)
            if val is not None:
                update[field] = val

        # Pull nested scoring if LLM used lead_scoring key
        ls = enrichment.get("lead_scoring") or enrichment.get("scoring") or {}
        if ls:
            for src, dst in (
                ("icp_fit_score",  "icp_fit_score"),
                ("intent_score",   "intent_score"),
                ("timing_score",   "timing_score"),
                ("overall_score",  "total_score"),
                ("score_explanation", "score_explanation"),
            ):
                if ls.get(src) is not None and dst not in update:
                    update[dst] = ls[src]
            tier = ls.get("score_tier") or ls.get("icp_match_tier") or ""
            if tier and "score_tier" not in update:
                update["score_tier"] = tier

        # Pull outreach fields if nested
        outreach = enrichment.get("outreach") or {}
        for src, dst in (
            ("cold_email",     "cold_email"),
            ("linkedin_note",  "linkedin_note"),
            ("email_subject",  "email_subject"),
            ("best_channel",   "best_channel"),
        ):
            if outreach.get(src) and dst not in update:
                update[dst] = outreach[src]

        await _upsert_lead(update)

        # Forward to LIO if requested
        updated_lead = {**lead, **update}
        if forward_to_lio and updated_lead.get("name"):
            linkedin_enrich = _format_linkedin_enrich(updated_lead)
            updated_lead["linkedin_enrich"] = linkedin_enrich
            import asyncio as _asyncio
            _asyncio.create_task(send_to_lio(updated_lead, sso_id=sso_id))

        logger.info("[AI-Worker-%d] Lead %s → completed (tier=%s)", worker_id, lead_id, update.get("score_tier", "?"))
        return updated_lead

    except Exception:
        # Release lock on failure so retry can reclaim it
        try:
            await r.delete(lock_key)
        except Exception:
            pass
        raise


async def _ai_push_dlq(r: Any, task: dict, reason: str) -> None:
    """Push a failed AI task to the AI dead-letter queue."""
    entry = {**task, "dlq_reason": reason, "dlq_at": round(time.time())}
    try:
        await r.rpush(KEY_AI_DLQ, json.dumps(entry))
        logger.error(
            "[AI-DLQ] job=%s  leads=%s — moved to DLQ. Reason: %s",
            task.get("job_id", "—"), task.get("lead_ids", []), reason,
        )
    except Exception as e:
        logger.error("[AI-DLQ] Failed to write DLQ: %s", e)


async def _ai_requeue(r: Any, task: dict) -> None:
    """Re-push AI task with incremented attempt counter to the org queue."""
    attempt = task.get("attempt", 0) + 1
    task = {**task, "attempt": attempt}
    org_id = task.get("org_id", "default")
    await r.rpush(_ai_tenant_key(org_id), json.dumps(task))
    await r.zadd(KEY_AI_ACTIVE, {org_id: time.time()}, nx=True)
    logger.warning("[AI-Queue] Requeued job=%s (attempt %d/%d)", task.get("job_id", "—"), attempt, MAX_AI_ATTEMPTS)


async def _ai_worker(worker_id: int, r: Any) -> None:
    """
    AI Worker loop.

    Pulls chunks from wb:ai:work (BLPOP) and processes each lead_id via
    _run_single_ai() with asyncio.gather. Concurrency is capped by the global
    _hf_semaphore regardless of how many workers are running.

    Guarantees:
      - Exactly-once: per-lead SET NX lock (TTL AI_LOCK_TTL)
      - Retry:        full-failure chunks re-queued up to MAX_AI_ATTEMPTS
      - DLQ:          exceeded-attempts tasks pushed to wb:ai:dlq
    """
    from lead_enrichment_brightdata_service import _update_job, _update_sub_job, get_job

    logger.info("[AI-Worker-%d] Started", worker_id)

    while True:
        try:
            item = await r.blpop([KEY_AI_WORK], timeout=5)
            if item is None:
                continue

            _, raw = item
            try:
                task = json.loads(raw)
            except Exception:
                logger.warning("[AI-Worker-%d] Invalid JSON — discarded", worker_id)
                continue

            job_id        = task.get("job_id", "")
            org_id        = task.get("org_id", "default")
            lead_ids      = task.get("lead_ids", [])
            system_prompt = task.get("system_prompt", "")
            sso_id        = task.get("sso_id", "")
            forward_to_lio = task.get("forward_to_lio", False)
            attempt       = task.get("attempt", 0)
            chunk_seq     = task.get("chunk_seq", 0)

            if not lead_ids:
                continue

            logger.info(
                "[AI-Worker-%d] chunk=%d leads | job=%s  seq=%s  attempt=%d  org=%s",
                worker_id, len(lead_ids), job_id or "—", chunk_seq, attempt, org_id,
            )

            # Update stats: active worker count
            try:
                await r.hincrby(KEY_AI_STATS, "active_workers", 1)
            except Exception:
                pass

            # Process all lead_ids in this chunk concurrently
            results = await asyncio.gather(
                *[
                    _run_single_ai(
                        lead_id=lid,
                        system_prompt=system_prompt,
                        org_id=org_id,
                        job_id=job_id,
                        sso_id=sso_id,
                        forward_to_lio=forward_to_lio,
                        r=r,
                        worker_id=worker_id,
                    )
                    for lid in lead_ids
                ],
                return_exceptions=True,
            )

            try:
                await r.hincrby(KEY_AI_STATS, "active_workers", -1)
            except Exception:
                pass

            ok   = sum(1 for res in results if not isinstance(res, Exception))
            fail = len(results) - ok

            # Log individual failures
            for lid, res in zip(lead_ids, results):
                if isinstance(res, Exception):
                    logger.warning("[AI-Worker-%d] Lead %s failed: %s", worker_id, lid, res)

            # Update throughput + error counters
            try:
                pipe = r.pipeline()
                if ok:
                    pipe.hincrby(KEY_AI_STATS, "total_processed", ok)
                if fail:
                    pipe.hincrby(KEY_AI_STATS, "total_errors", fail)
                pipe.hset(KEY_AI_STATS, "last_processed_at", round(time.time()))
                await pipe.execute()
            except Exception:
                pass

            # Retry / DLQ on total failure
            if ok == 0 and fail > 0:
                if attempt < MAX_AI_ATTEMPTS - 1:
                    await _ai_requeue(r, task)
                else:
                    await _ai_push_dlq(r, task, reason=f"all {fail} leads failed after {attempt + 1} attempts")
                continue

            # Update parent job progress if job_id is set
            if job_id:
                try:
                    job = await get_job(job_id)
                    if job:
                        new_proc = (job.get("processed") or 0) + ok
                        new_fail = (job.get("failed") or 0) + fail
                        total    = job.get("total_urls") or 0
                        status   = None
                        if total and new_proc + new_fail >= total:
                            status = (
                                "completed" if new_fail == 0
                                else ("failed" if new_proc == 0 else "completed_with_errors")
                            )
                        await _update_job(job_id, processed=new_proc, failed=new_fail,
                                          **({"status": status} if status else {}))
                        if status:
                            logger.info(
                                "[AI-Worker-%d] Job %s DONE — processed=%d failed=%d status=%s",
                                worker_id, job_id, new_proc, new_fail, status,
                            )
                except Exception as e:
                    logger.error("[AI-Worker-%d] Job progress update failed: %s", worker_id, e)

        except asyncio.CancelledError:
            logger.info("[AI-Worker-%d] Cancelled — shutting down", worker_id)
            break
        except Exception as e:
            logger.error("[AI-Worker-%d] Unexpected error: %s", e, exc_info=True)
            await asyncio.sleep(1)


# ─────────────────────────────────────────────────────────────────────────────
# AI Worker System — Lifecycle
# ─────────────────────────────────────────────────────────────────────────────

async def start_ai_workers() -> bool:
    """
    Start the AI processing system:
      - 1 AI scheduler task  (_ai_scheduler_loop)
      - AI_WORKERS worker tasks (_ai_worker)

    Safe to call multiple times — no-ops if already running.
    Returns True on success, False if Redis unavailable.
    """
    global _ai_worker_tasks, _ai_scheduler_task, _ai_next_worker_id

    if _ai_worker_tasks or _ai_scheduler_task:
        logger.debug("[AI-Workers] Already running — start() no-op")
        return True

    r = await _make_redis()
    if r is None:
        logger.warning("[AI-Workers] Redis unavailable — AI worker system disabled")
        return False

    try:
        await r.ping()
        await r.aclose()
    except Exception as e:
        logger.warning("[AI-Workers] Redis unreachable (%s) — AI workers disabled", e)
        return False

    logger.info(
        "[AI-Workers] Starting — %d workers  chunk=%d  hf_concurrency=%d",
        AI_WORKERS, AI_CHUNK, HF_CONCURRENCY,
    )

    # Scheduler (dedicated connection)
    r_sched = await _make_redis()
    _ai_scheduler_task = asyncio.create_task(
        _ai_scheduler_loop(r_sched),
        name="ai-scheduler",
    )

    # Worker pool (each gets its own connection)
    for _ in range(AI_WORKERS):
        _ai_next_worker_id += 1
        wid = _ai_next_worker_id
        r_worker = await _make_redis()
        task = asyncio.create_task(
            _ai_worker(wid, r_worker),
            name=f"ai-worker-{wid}",
        )
        _ai_worker_tasks.append(task)

    logger.info("[AI-Workers] Running — scheduler=1  workers=%d", AI_WORKERS)
    return True


async def stop_ai_workers() -> None:
    """Cancel all AI worker tasks (scheduler + workers)."""
    global _ai_worker_tasks, _ai_scheduler_task

    all_tasks = []
    if _ai_scheduler_task:
        all_tasks.append(_ai_scheduler_task)
        _ai_scheduler_task = None
    all_tasks.extend(_ai_worker_tasks)
    _ai_worker_tasks = []

    if not all_tasks:
        return

    logger.info("[AI-Workers] Stopping %d tasks…", len(all_tasks))
    for t in all_tasks:
        t.cancel()
    await asyncio.gather(*all_tasks, return_exceptions=True)
    logger.info("[AI-Workers] All stopped")


async def scale_up_ai_workers(extra: int) -> int:
    """
    Spawn up to `extra` additional AI workers beyond the current pool.
    Called proactively at the start of a large bulk job so the AI pipeline
    is ready before enriched leads start arriving.
    Returns the number of workers actually spawned.
    """
    global _ai_worker_tasks, _ai_next_worker_id

    if not _ai_worker_tasks and not _ai_scheduler_task:
        # AI system not started yet — nothing to scale
        return 0

    _ai_worker_tasks = [t for t in _ai_worker_tasks if not t.done()]
    current = len(_ai_worker_tasks)
    ai_worker_max = int(os.getenv("AI_WORKER_MAX", str(AI_WORKERS * 4)))
    headroom = max(0, ai_worker_max - current)
    to_spawn = min(extra, headroom)

    for _ in range(to_spawn):
        _ai_next_worker_id += 1
        wid = _ai_next_worker_id
        r_worker = await _make_redis()
        task = asyncio.create_task(
            _ai_worker(wid, r_worker),
            name=f"ai-worker-{wid}",
        )
        _ai_worker_tasks.append(task)

    if to_spawn:
        logger.info(
            "[AI-Workers] Proactive scale-up: +%d workers → %d total (max=%d)",
            to_spawn, current + to_spawn, ai_worker_max,
        )
    return to_spawn


# ─────────────────────────────────────────────────────────────────────────────
# AI Worker System — Monitoring
# ─────────────────────────────────────────────────────────────────────────────

async def get_ai_queue_stats() -> dict:
    """Return a snapshot of AI queue state for monitoring endpoints."""
    r = await _make_redis()
    if r is None:
        return {"redis": "unavailable"}

    try:
        await r.ping()
    except Exception as e:
        return {"redis": "unreachable", "error": str(e)}

    try:
        active_orgs = await r.zrange(KEY_AI_ACTIVE, 0, -1, withscores=True)
        org_info = []
        for org_id, last_served in active_orgs:
            qlen = await r.llen(_ai_tenant_key(org_id))
            org_info.append({"org_id": org_id, "queue_chunks": qlen, "last_served": round(last_served)})

        work_depth = await r.llen(KEY_AI_WORK)
        dlq_depth  = await r.llen(KEY_AI_DLQ)
        stats_raw  = await r.hgetall(KEY_AI_STATS)
        live       = [t for t in _ai_worker_tasks if not t.done()]

        return {
            "redis":              "connected",
            "active_orgs":        len(active_orgs),
            "work_queue_depth":   work_depth,
            "dlq_depth":          dlq_depth,
            "workers_running":    len(live),
            "workers_configured": AI_WORKERS,
            "hf_concurrency":     HF_CONCURRENCY,
            "ai_chunk_size":      AI_CHUNK,
            "scheduler_running":  _ai_scheduler_task is not None and not _ai_scheduler_task.done(),
            "orgs":               org_info,
            "throughput": {
                "total_processed":   int(stats_raw.get("total_processed", 0)),
                "total_errors":      int(stats_raw.get("total_errors", 0)),
                "active_workers":    int(stats_raw.get("active_workers", 0)),
                "last_processed_at": int(stats_raw.get("last_processed_at", 0)),
            },
        }
    except Exception as e:
        logger.error("[AI-Stats] Failed: %s", e)
        return {"redis": "error", "error": str(e)}
    finally:
        await r.aclose()


async def retry_ai_dlq(index: int = 0) -> bool:
    """
    Pop one item from the AI DLQ, reset attempt counter, and re-enqueue.
    Returns True if an item was retried, False if DLQ empty.
    """
    r = await _make_redis()
    if r is None:
        return False
    try:
        raw = await r.lindex(KEY_AI_DLQ, index)
        if raw is None:
            return False
        task = json.loads(raw)
        await r.lrem(KEY_AI_DLQ, 1, raw)
        task["attempt"] = 0
        org_id = task.get("org_id", "default")
        await r.rpush(_ai_tenant_key(org_id), json.dumps(task))
        await r.zadd(KEY_AI_ACTIVE, {org_id: time.time()}, nx=True)
        logger.info("[AI-DLQ] Retried job=%s", task.get("job_id", "—"))
        return True
    except Exception as e:
        logger.error("[AI-DLQ] retry_ai_dlq failed: %s", e)
        return False
    finally:
        await r.aclose()

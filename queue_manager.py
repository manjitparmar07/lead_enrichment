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

# ── Redis keys ────────────────────────────────────────────────────────────────
KEY_ACTIVE_TENANTS = "wb:leads:active_tenants"  # ZSet: org_id → last_served_ts
KEY_WORK_QUEUE     = "wb:leads:work"             # List: scheduler → workers
KEY_SYS_STATS      = "wb:leads:sys_stats"        # Hash: monitoring

def _tenant_queue_key(org_id: str) -> str:
    return f"wb:leads:q:{org_id}"

# ── Module state ──────────────────────────────────────────────────────────────
_worker_tasks: list[asyncio.Task]    = []
_scheduler_task: Optional[asyncio.Task] = None
_chunk_refresher_task: Optional[asyncio.Task] = None
_current_chunk_size: int = CHUNK_DEFAULT


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
    for chunk, sub_job_id in zip(chunks, sub_job_ids):
        task = {
            "job_id":            job_id,
            "org_id":            org_id,
            "urls":              chunk,
            "sub_job_id":        sub_job_id,
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

async def _worker(worker_id: int, r: Any) -> None:
    """
    Worker loop.

    Pulls pre-scheduled chunks from wb:leads:work (BLPOP) and processes all
    URLs in the chunk concurrently via asyncio.gather, then updates job
    progress and publishes Ably events.
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
            gen_out        = task.get("generate_outreach", True)
            sso_id         = task.get("sso_id", "")
            forward_to_lio = task.get("forward_to_lio", False)
            system_prompt  = task.get("system_prompt") or None

            if not urls:
                continue

            logger.info(
                "[Worker-%d] chunk=%d urls | job=%s sub=%s org=%s",
                worker_id, len(urls), job_id or "—", sub_job_id or "—", org_id,
            )

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

            # ── Update sub-job progress ───────────────────────────────────────
            if sub_job_id:
                try:
                    sub_status = (
                        "completed" if fail == 0
                        else ("failed" if ok == 0 else "completed_with_errors")
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
# Lifecycle — start / stop
# ─────────────────────────────────────────────────────────────────────────────

async def start_queue_system() -> bool:
    """
    Start the full queue system:
      - 1 fair scheduler task
      - N worker tasks (LEAD_WORKER_COUNT)
      - 1 chunk-size refresher task

    Safe to call multiple times — no-ops if already running.
    Returns True if started, False if Redis unavailable.
    """
    global _worker_tasks, _scheduler_task, _chunk_refresher_task

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
        "[QueueManager] Starting — %d workers, chunk_size=%d, Redis=%s",
        NUM_WORKERS, _current_chunk_size, REDIS_URL,
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

    # Workers (each gets its own connection to avoid contention)
    for i in range(NUM_WORKERS):
        r_worker = await _make_redis()
        task = asyncio.create_task(
            _worker(i + 1, r_worker),
            name=f"lead-worker-{i + 1}",
        )
        _worker_tasks.append(task)

    logger.info(
        "[QueueManager] Running — scheduler=1  workers=%d  chunk_refresher=1",
        NUM_WORKERS,
    )
    return True


async def stop_queue_system() -> None:
    """Cancel all queue system tasks (scheduler + workers + refresher)."""
    global _worker_tasks, _scheduler_task, _chunk_refresher_task

    all_tasks = []
    if _scheduler_task:
        all_tasks.append(_scheduler_task)
        _scheduler_task = None
    if _chunk_refresher_task:
        all_tasks.append(_chunk_refresher_task)
        _chunk_refresher_task = None
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

        # System stats
        sys_raw = await r.hgetall(KEY_SYS_STATS)

        # Redis memory summary
        mem_info = await r.info("memory")

        return {
            "redis":            "connected",
            "active_tenants":   len(active_tenants),
            "work_queue_depth": work_depth,
            "workers_running":  len(_worker_tasks),
            "scheduler_running": _scheduler_task is not None and not _scheduler_task.done(),
            "current_chunk_size": _current_chunk_size,
            "tenants":          tenant_info,
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

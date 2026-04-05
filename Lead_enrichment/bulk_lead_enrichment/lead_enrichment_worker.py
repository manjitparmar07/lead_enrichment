"""
lead_enrichment_worker.py
--------------------------
Thin lifecycle shim — delegates everything to queue_manager.

The actual queue logic lives in queue_manager.py:
  - Per-tenant Redis queues (wb:leads:q:{org_id})
  - Fair round-robin scheduler
  - Dynamic chunk sizing (Redis memory + CPU)
  - Worker pool (asyncio.gather per chunk)

Call start_workers() on FastAPI startup and stop_workers() on shutdown.
These are kept for backwards compatibility — main.py already imports them.
"""

from __future__ import annotations

import logging
from Lead_enrichment.bulk_lead_enrichment.queue_manager import start_queue_system, stop_queue_system

logger = logging.getLogger(__name__)


async def start_workers() -> None:
    """Start the fair multi-tenant queue system (scheduler + workers)."""
    started = await start_queue_system()
    if not started:
        logger.warning(
            "[Workers] Queue system not started — Redis unavailable. "
            "Bulk jobs will use in-process sequential fallback."
        )


async def stop_workers() -> None:
    """Stop the queue system gracefully."""
    await stop_queue_system()

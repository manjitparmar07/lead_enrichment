"""
_clients.py
-----------
Shared HTTP client singletons, BD circuit breaker, concurrency controls.

Covers:
  - HTTP clients (_get_lio_client, _get_api_client, _get_bd_client, _get_web_client)
  - Bright Data circuit breaker (_bd_circuit_ok, _bd_circuit_success, _bd_circuit_failure)
  - Rate-limit and backoff helpers (_bd_rate_limit_wait, _bd_backoff)
  - Concurrency semaphores and in-flight lead tracking
  - Per-job webhook serialisation locks
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

BD_BASE = "https://api.brightdata.com/datasets/v3"

LIO_RECEIVE_URL = os.getenv("LIO_RECEIVE_URL", "https://api-lio.worksbuddy.ai/api/enrich/receive")
_JWT_SECRET     = os.getenv("JWT_SECRET", "")
_LIO_TOKEN      = os.getenv("LIO_TOKEN", "")   # dedicated bearer token for LIO auth

REDIS_URL    = os.getenv("REDIS_URL", "redis://localhost:6379/0")
QUEUE_HIGH   = "wb:leads:queue:high"
QUEUE_NORMAL = "wb:leads:queue:normal"
QUEUE_LOW    = "wb:leads:queue:low"

BD_REQUEST_TIMEOUT: int = int(os.getenv("BD_REQUEST_TIMEOUT", "90"))

# ─────────────────────────────────────────────────────────────────────────────
# Concurrency controls
# ─────────────────────────────────────────────────────────────────────────────

# Max 10 simultaneous HuggingFace LLM calls (HF paid tier safe limit)
_llm_semaphore: asyncio.Semaphore = asyncio.Semaphore(10)
# Max 20 simultaneous full enrichment pipelines (DB pool size limit)
_enrichment_semaphore: asyncio.Semaphore = asyncio.Semaphore(20)
# In-flight deduplication: tracks URLs currently being enriched
_in_flight_leads: dict[str, asyncio.Event] = {}

# Global BD trigger semaphore — shared across ALL enrich_bulk calls
_BD_TRIGGER_CONCURRENCY = int(os.getenv("BD_TRIGGER_CONCURRENCY", "50"))
_bd_trigger_semaphore: asyncio.Semaphore = asyncio.Semaphore(_BD_TRIGGER_CONCURRENCY)

# Per-job webhook serialisation locks
_job_webhook_locks: dict[str, asyncio.Lock] = {}

# ─────────────────────────────────────────────────────────────────────────────
# HTTP client singletons
# ─────────────────────────────────────────────────────────────────────────────

_HTTP_LIMITS = httpx.Limits(max_connections=40, max_keepalive_connections=20)

_lio_client: Optional[httpx.AsyncClient] = None
_api_client: Optional[httpx.AsyncClient] = None
_bd_client:  Optional[httpx.AsyncClient] = None
_web_client: Optional[httpx.AsyncClient] = None


def _get_lio_client() -> httpx.AsyncClient:
    global _lio_client
    if _lio_client is None or _lio_client.is_closed:
        headers = {}
        lio_auth = _LIO_TOKEN or _JWT_SECRET
        if lio_auth:
            headers["Authorization"] = f"Bearer {lio_auth}"
        _lio_client = httpx.AsyncClient(timeout=30.0, limits=_HTTP_LIMITS, headers=headers)
    return _lio_client


def _get_api_client() -> httpx.AsyncClient:
    """Shared client for Apollo, Dropcontact, PDL, ZeroBounce."""
    global _api_client
    if _api_client is None or _api_client.is_closed:
        _api_client = httpx.AsyncClient(timeout=25.0, limits=_HTTP_LIMITS)
    return _api_client


def _get_bd_client() -> httpx.AsyncClient:
    """Shared client for Bright Data API calls."""
    global _bd_client
    if _bd_client is None or _bd_client.is_closed:
        _bd_client = httpx.AsyncClient(timeout=float(BD_REQUEST_TIMEOUT), limits=_HTTP_LIMITS)
    return _bd_client


def _get_web_client() -> httpx.AsyncClient:
    """Shared client for website scraping (follow redirects)."""
    global _web_client
    if _web_client is None or _web_client.is_closed:
        _web_client = httpx.AsyncClient(
            timeout=30.0, limits=_HTTP_LIMITS, follow_redirects=True,
        )
    return _web_client


# ─────────────────────────────────────────────────────────────────────────────
# Bright Data circuit breaker
# States: "closed" (normal) → "open" (rejecting) → "half_open" (one test req)
# ─────────────────────────────────────────────────────────────────────────────

BD_CIRCUIT_FAILURE_THRESHOLD: int = 5
BD_CIRCUIT_RESET_TIMEOUT: int     = 60

_bd_circuit_state: str    = "closed"
_bd_circuit_failures: int = 0
_bd_circuit_open_until: float = 0.0


def _bd_circuit_ok() -> bool:
    """Return True if a BD request is allowed (circuit closed or half-open)."""
    global _bd_circuit_state, _bd_circuit_open_until
    if _bd_circuit_state == "closed":
        return True
    if _bd_circuit_state == "open":
        if time.time() >= _bd_circuit_open_until:
            _bd_circuit_state = "half_open"
            logger.info("[Circuit] BD circuit breaker → HALF-OPEN (testing recovery)")
            return True
        return False
    return True


def _bd_circuit_success() -> None:
    """Record a successful BD response; resets the circuit to CLOSED."""
    global _bd_circuit_state, _bd_circuit_failures
    if _bd_circuit_state != "closed":
        logger.info("[Circuit] BD circuit breaker → CLOSED (recovered)")
    _bd_circuit_state    = "closed"
    _bd_circuit_failures = 0


def _bd_circuit_failure() -> None:
    """Record a BD failure; opens the circuit once threshold is reached."""
    global _bd_circuit_state, _bd_circuit_failures, _bd_circuit_open_until
    _bd_circuit_failures += 1
    if _bd_circuit_failures >= BD_CIRCUIT_FAILURE_THRESHOLD or _bd_circuit_state == "half_open":
        _bd_circuit_state      = "open"
        _bd_circuit_open_until = time.time() + BD_CIRCUIT_RESET_TIMEOUT
        logger.error(
            "[Circuit] BD circuit breaker → OPEN for %ds (%d consecutive failures)",
            BD_CIRCUIT_RESET_TIMEOUT, _bd_circuit_failures,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Rate-limit / backoff helpers
# ─────────────────────────────────────────────────────────────────────────────

def _bd_rate_limit_wait(resp: httpx.Response) -> float:
    """
    Extract how long to wait from a 429 response.
    Checks X-RateLimit-Reset (epoch) then Retry-After (delta seconds).
    Falls back to 30s if neither header is present.
    """
    for header in ("X-RateLimit-Reset", "Retry-After", "x-ratelimit-reset", "retry-after"):
        val = resp.headers.get(header)
        if val:
            try:
                f = float(val)
                if f > 1_000_000_000:
                    f = max(0.0, f - time.time())
                return min(f, 120.0)
            except (ValueError, TypeError):
                pass
    return 30.0


def _bd_backoff(attempt: int, base: float = 2.0, cap: float = 60.0) -> float:
    """Return wait seconds for attempt (0-indexed): 2s, 4s, 8s, … capped at cap."""
    return min(base ** (attempt + 1), cap)

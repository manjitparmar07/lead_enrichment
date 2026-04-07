"""
lead_enrichment_brightdata_service.py
--------------------------------------
Worksbuddy Lead Enrichment — powered by Bright Data.

8-stage enrichment output:
  Stage 1. Person Profile       — name, emails, phone, LinkedIn, Twitter, timezone, skills
  Stage 2. Company Identity     — name, domain, website, LinkedIn
  Stage 3. Website Intelligence — scraped homepage/about/features/pricing intelligence
  Stage 4. Company Profile      — industry, employee count, revenue, tech stack
  Stage 5. Market Signals       — funding, hiring velocity, news
  Stage 6. Intent Signals       — funding events, job changes, hiring surges, competitors
  Stage 7. Lead Scoring         — ICP fit (0-40) + Intent (0-30) + Timing (0-20) + DataCompleteness (0-10)
  Stage 8. Outreach             — AI cold email, LinkedIn note, sequence, best time
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
from db import get_pool, named_args
try:
    import redis.asyncio as aioredis
    _REDIS_AVAILABLE = True
except ImportError:
    _REDIS_AVAILABLE = False

logger = logging.getLogger(__name__)

# ── Pipeline live log — writes to /tmp/pipeline.log, tail -f to watch in real-time ──
_pipeline_log = logging.getLogger("pipeline")
_pipeline_log.setLevel(logging.DEBUG)
if not _pipeline_log.handlers:
    _fh = logging.FileHandler("/tmp/pipeline.log")
    _fh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-7s  %(message)s", datefmt="%H:%M:%S"))
    _pipeline_log.addHandler(_fh)
    _pipeline_log.propagate = False

def _plog(job_id: str, url: str, stage: str, msg: str, level: str = "info") -> None:
    """Structured per-lead pipeline log entry."""
    short_url = url.rstrip("/").split("/")[-1] if url else "?"
    short_job = (job_id or "")[-8:]
    entry = f"[job={short_job}] [{short_url}] [{stage}] {msg}"
    getattr(_pipeline_log, level)(entry)

# ── Per-job webhook serialisation ──────────────────────────────────────────────
# Serialises concurrent webhook calls that share the same job_id so that
# DB progress updates and sub_job bookkeeping do not race.
# Different job_ids acquire independent locks — no cross-job blocking.
_job_webhook_locks: dict[str, asyncio.Lock] = {}

# ── Global BrightData trigger semaphore ────────────────────────────────────────
# Shared across ALL enrich_bulk calls — prevents BD API rate-limit exhaustion.
# Per-call semaphore (old approach) allowed N_orgs × 50 concurrent calls.
_BD_TRIGGER_CONCURRENCY = int(os.getenv("BD_TRIGGER_CONCURRENCY", "50"))
_bd_trigger_semaphore: asyncio.Semaphore = asyncio.Semaphore(_BD_TRIGGER_CONCURRENCY)

# ── Sequential fallback concurrency cap ────────────────────────────────────────
# When Redis is unavailable, enrich_bulk falls back to asyncio.create_task().
# This semaphore prevents unbounded concurrent background tasks under that path.
_SEQUENTIAL_FALLBACK_CONCURRENCY = int(os.getenv("SEQUENTIAL_FALLBACK_CONCURRENCY", "10"))
_sequential_fallback_sem: asyncio.Semaphore = asyncio.Semaphore(_SEQUENTIAL_FALLBACK_CONCURRENCY)

# ── Bright Data ───────────────────────────────────────────────────────────────
# All keys read lazily from keys_service so admin hot-reload works at request time.
BD_BASE = "https://api.brightdata.com/datasets/v3"

def _k(name: str, default: str = "") -> str:
    """Fetch key from keys_service (admin store) with os.getenv fallback."""
    try:
        from auth import keys_service as _ks
        return _ks.get(name) or os.getenv(name, default)
    except Exception:
        return os.getenv(name, default)

def _bd_api_key()         -> str:  return _k("BRIGHT_DATA_API_KEY")
def _bd_profile_dataset() -> str:  return _k("BD_PROFILE_DATASET_ID", "gd_l1viktl72bvl7bjuj0")
def _bd_company_dataset() -> str:  return _k("BD_COMPANY_DATASET_ID", "gd_l1vikfnt1wgvvqz95w")
def _hunter_api_key()     -> str:  return _k("HUNTER_API_KEY")
def _apollo_api_key()     -> str:  return _k("APOLLO_API_KEY")
def _dropcontact_key()    -> str:  return _k("DROPCONTACT_API_KEY")
def _pdl_api_key()        -> str:  return _k("PDL_API_KEY")
def _zerobounce_key()     -> str:  return _k("ZEROBOUNCE_API_KEY")
def _wb_llm_host()        -> str:  return _k("WB_LLM_HOST", "http://ai-llm.worksbuddy.ai")
def _wb_llm_key()         -> str:  return _k("WB_LLM_API_KEY")
def _wb_llm_model()       -> str:  return _k("WB_LLM_DEFAULT_MODEL", "wb-pro")
def _groq_key()           -> str:  return _k("GROQ_API_KEY")
def _groq_model()         -> str:  return _k("GROQ_LLM_MODEL", "llama-3.3-70b-versatile")
def _hf_token()           -> str:  return _k("HF_TOKEN")
def _hf_model()           -> str:  return _k("HF_MODEL", "Qwen/Qwen2.5-72B-Instruct")
def _outreach_threshold() -> int:
    try: return int(_k("LEAD_OUTREACH_THRESHOLD", "50"))
    except ValueError: return 50


async def _outreach_threshold_for_org(org_id: str) -> int:
    """Read outreach threshold from workspace_configs for org; fall back to env/default."""
    try:
        from config.enrichment_config_service import get_scoring_config
        sc = await get_scoring_config(org_id)
        return int(sc.get("outreach_threshold", 50))
    except Exception:
        return _outreach_threshold()

# Backward-compat module-level names (read once at import; services use _fn() at call time)
BD_API_KEY         = ""  # use _bd_api_key() inside functions
BD_PROFILE_DATASET = ""  # use _bd_profile_dataset() inside functions
HUNTER_API_KEY     = ""  # use _hunter_api_key() inside functions
APOLLO_API_KEY     = ""  # use _apollo_api_key() inside functions
WB_LLM_HOST        = ""  # use _wb_llm_host() inside functions
WB_LLM_KEY         = ""  # use _wb_llm_key() inside functions
WB_LLM_MODEL       = ""  # use _wb_llm_model() inside functions
GROQ_KEY           = ""  # use _groq_key() inside functions
GROQ_MODEL         = ""  # use _groq_model() inside functions
OUTREACH_THRESHOLD = 50  # use _outreach_threshold() inside functions

# ── DB ────────────────────────────────────────────────────────────────────────
# Pool is initialised by main.py at startup via db.init_pool().

# ── Redis queues ───────────────────────────────────────────────────────────────
REDIS_URL    = os.getenv("REDIS_URL", "redis://localhost:6379/0")
QUEUE_HIGH   = "wb:leads:queue:high"    # single enrichments
QUEUE_NORMAL = "wb:leads:queue:normal"  # bulk ≤100
QUEUE_LOW    = "wb:leads:queue:low"     # bulk >100

# ── Emoji stripping ───────────────────────────────────────────────────────────
_EMOJI_RE = re.compile(
    "[\U0001F600-\U0001F64F"   # emoticons
    "\U0001F300-\U0001F5FF"   # symbols & pictographs
    "\U0001F680-\U0001F6FF"   # transport & map symbols
    "\U0001F700-\U0001F77F"   # alchemical symbols
    "\U0001F780-\U0001F7FF"   # geometric shapes extended
    "\U0001F800-\U0001F8FF"   # supplemental arrows
    "\U0001F900-\U0001F9FF"   # supplemental symbols & pictographs
    "\U0001FA00-\U0001FA6F"   # chess symbols
    "\U0001FA70-\U0001FAFF"   # symbols & pictographs extended-A
    "\U00002702-\U000027B0"   # dingbats
    "\U000024C2-\U0001F251"   # enclosed characters
    "\U0001F1E0-\U0001F1FF"   # flags
    "\U00010000-\U0010FFFF"   # supplementary multilingual plane
    "]+",
    flags=re.UNICODE,
)

def _strip_emojis(value: str) -> str:
    """Remove emoji characters and clean up extra whitespace."""
    if not value or not isinstance(value, str):
        return value
    return _EMOJI_RE.sub("", value).strip()


def _clean_lead_strings(lead: dict, fields: list) -> None:
    """Strip emojis from specified string fields of a lead dict in-place."""
    for f in fields:
        v = lead.get(f)
        if v and isinstance(v, str):
            lead[f] = _strip_emojis(v)


# ── First-person → third-person normalisation ─────────────────────────────────

def _clean_title(title: str) -> str:
    """
    Convert first-person LinkedIn headlines into clean professional job titles.

    Examples:
      "I lead the International Cooperation Division"  → "International Cooperation Division Lead"
      "I'm a Senior Product Designer at Figma"         → "Senior Product Designer"
      "I help B2B companies scale their outbound"       → "B2B Growth Specialist"
      "I am Head of Sales"                              → "Head of Sales"
    """
    if not title or not isinstance(title, str):
        return title
    t = title.strip()

    # "I am X" / "I'm X" — extract X directly (already a title noun)
    m = re.match(r"^I(?:'m| am)\s+(?:a\s+|an\s+|the\s+)?(.+)", t, re.IGNORECASE)
    if m:
        rest = m.group(1).strip()
        # Drop trailing "at/with/of Company..." clause
        rest = re.split(r"\s+(?:at|with|of|@)\s+[A-Z]", rest)[0].strip().rstrip(",–-").strip()
        # If it still reads like a sentence (has a verb word), skip — handled below
        if not re.search(r"\b(help|work|lead|run|build|create|drive|grow|focus|speciali)\b", rest, re.IGNORECASE):
            return rest

    # "I lead/run/head/own/manage/oversee the X" → "X Lead/Manager/..."
    m = re.match(
        r"^I\s+(lead|run|head|own|manage|oversee|direct|build|drive|grow)\s+(?:the\s+|a\s+|an\s+)?(.+)",
        t, re.IGNORECASE,
    )
    if m:
        verb = m.group(1).lower()
        obj  = m.group(2).strip()
        obj  = re.split(r"\s+(?:at|with|for|@)\s+", obj)[0].strip().rstrip(".,–-").strip()
        suffix_map = {
            "lead": "Lead", "run": "Lead", "head": "Head",
            "own": "Owner", "manage": "Manager", "oversee": "Manager",
            "direct": "Director", "build": "Lead", "drive": "Lead", "grow": "Lead",
        }
        suffix = suffix_map.get(verb, "Lead")
        return f"{obj} {suffix}"

    # "I help X do Y" → "X Specialist" (too vague — return sanitised)
    m = re.match(r"^I\s+help\s+(.+?)\s+(?:to\s+)?\w+", t, re.IGNORECASE)
    if m:
        return t  # can't reliably extract a title — leave for LLM enrichment

    # Any other first-person opener — strip the "I [verb] " prefix
    m = re.match(r"^I\s+\w+\s+(.*)", t, re.IGNORECASE)
    if m:
        rest = m.group(1).strip().rstrip(".,–-").strip()
        if len(rest) > 3:
            return rest

    return t


def _to_third_person(text: str, first_name: str) -> str:
    """
    Rewrite a first-person LinkedIn 'about' section into third-person.

    "I lead..."     → "Firstname leads..."
    "I am / I'm"    → "Firstname is"
    "I've / I have" → "Firstname has"
    "I was"         → "Firstname was"
    "My "           → "Their "
    "me"            → "them"
    "we / our"      → "the team / their"
    """
    if not text or not isinstance(text, str):
        return text

    name = (first_name or "").strip() or "They"

    subs = [
        # Contractions first (order matters)
        (r"\bI'm\b",            f"{name} is"),
        (r"\bI've\b",           f"{name} has"),
        (r"\bI'd\b",            f"{name} would"),
        (r"\bI'll\b",           f"{name} will"),
        (r"\bI was\b",          f"{name} was"),
        (r"\bI have\b",         f"{name} has"),
        (r"\bI am\b",           f"{name} is"),
        # "I lead/run/help..." → conjugate verb (add s)
        (r"\bI (lead|run|help|work|build|drive|grow|focus|head|own|manage|oversee|create|speciali[sz]e)\b",
         lambda m, n=name: f"{n} {m.group(1)}s"),
        # Generic "I <verb>" → "Name <verb>s"  (only for simple present verbs)
        (r"\bI ([a-z]+)\b",     lambda m, n=name: f"{n} {m.group(1)}s"),
        # Possessive / object
        (r"\bMy\b",             "Their"),
        (r"\bmy\b",             "their"),
        (r"\bMe\b",             "Them"),
        (r"\bme\b",             "them"),
        # Plural first-person
        (r"\bWe\b",             "The team"),
        (r"\bwe\b",             "the team"),
        (r"\bOur\b",            "Their"),
        (r"\bour\b",            "their"),
    ]

    result = text
    for pattern, repl in subs:
        if callable(repl):
            result = re.sub(pattern, repl, result)
        else:
            result = re.sub(pattern, repl, result)

    return result.strip()


def _normalise_person_text(lead: dict) -> None:
    """
    Apply title cleaning and about→third-person rewrite in-place before DB save.
    - Cleans first-person titles and about text
    - Clears title if it's empty or duplicates the about text
    """
    first_name = (lead.get("first_name") or "").strip()

    title = lead.get("title") or ""
    if title and re.match(r"^I\b", title.strip(), re.IGNORECASE):
        title = _clean_title(title)
        lead["title"] = title

    about = lead.get("about") or ""
    if about and re.search(r"\bI\b|\bmy\b|\bwe\b", about, re.IGNORECASE):
        about = _to_third_person(about, first_name)
        lead["about"] = about

    # Clear title if it's effectively the same as about (e.g. full sentence stored as title)
    if title and about:
        title_clean = title.strip().lower()
        about_start = about.strip().lower()[:len(title_clean) + 10]
        if title_clean in about_start or about_start.startswith(title_clean):
            lead["title"] = ""


# ── Shared persistent HTTP clients (reuse TCP/TLS connections across requests) ─
# Each external service gets its own client so keep-alive pools don't interfere.
LIO_RECEIVE_URL = os.getenv("LIO_RECEIVE_URL", "https://api-lio.worksbuddy.ai/api/enrich/receive")
_JWT_SECRET     = os.getenv("JWT_SECRET", "")
_LIO_TOKEN      = os.getenv("LIO_TOKEN", "")   # dedicated bearer token for LIO auth
_lio_client: Optional[httpx.AsyncClient] = None
_api_client: Optional[httpx.AsyncClient] = None   # Hunter / Apollo / Dropcontact / PDL / ZeroBounce
_bd_client: Optional[httpx.AsyncClient] = None    # Bright Data
_web_client: Optional[httpx.AsyncClient] = None   # Website scraping (follow redirects)

# ── Concurrency controls for 1000-URL bulk jobs ───────────────────────────────
# Max 10 simultaneous HuggingFace LLM calls (HF paid tier safe limit)
_llm_semaphore: asyncio.Semaphore = asyncio.Semaphore(10)
# Max 20 simultaneous full enrichment pipelines (DB pool size limit)
_enrichment_semaphore: asyncio.Semaphore = asyncio.Semaphore(20)
# In-flight deduplication: tracks URLs currently being enriched
# Key = lead_id, Value = asyncio.Event (set when enrichment finishes)
_in_flight_leads: dict[str, asyncio.Event] = {}

_HTTP_LIMITS = httpx.Limits(max_connections=40, max_keepalive_connections=20)

# ── Bright Data timeout — override via BD_REQUEST_TIMEOUT env var ──────────────
BD_REQUEST_TIMEOUT: int = int(os.getenv("BD_REQUEST_TIMEOUT", "90"))


def _get_lio_client() -> httpx.AsyncClient:
    global _lio_client
    if _lio_client is None or _lio_client.is_closed:
        headers = {}
        # Use dedicated LIO_TOKEN if set, otherwise fall back to JWT_SECRET
        lio_auth = _LIO_TOKEN or _JWT_SECRET
        if lio_auth:
            headers["Authorization"] = f"Bearer {lio_auth}"
        _lio_client = httpx.AsyncClient(timeout=30.0, limits=_HTTP_LIMITS, headers=headers)
    return _lio_client


def _get_api_client() -> httpx.AsyncClient:
    """Shared client for Hunter, Apollo, Dropcontact, PDL, ZeroBounce."""
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


# ── Bright Data circuit breaker ────────────────────────────────────────────────
# States: "closed" (normal) → "open" (rejecting) → "half_open" (one test req)
BD_CIRCUIT_FAILURE_THRESHOLD: int = 5   # consecutive failures before opening
BD_CIRCUIT_RESET_TIMEOUT: int     = 60  # seconds in OPEN before moving to HALF-OPEN

_bd_circuit_state: str    = "closed"
_bd_circuit_failures: int = 0
_bd_circuit_open_until: float = 0.0     # epoch seconds


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
    # half_open: allow the probe request through
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


# ── Rate-limit wait helper ─────────────────────────────────────────────────────

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
                if f > 1_000_000_000:   # epoch timestamp → convert to delta
                    f = max(0.0, f - time.time())
                return min(f, 120.0)    # cap at 2 minutes
            except (ValueError, TypeError):
                pass
    return 30.0


# ── Exponential backoff helper ─────────────────────────────────────────────────

def _bd_backoff(attempt: int, base: float = 2.0, cap: float = 60.0) -> float:
    """Return wait seconds for attempt (0-indexed): 2s, 4s, 8s, … capped at cap."""
    return min(base ** (attempt + 1), cap)


# ── LIO Dead-Letter Queue ─────────────────────────────────────────────────────
_LIO_DLQ_KEY = "lio:dlq"

async def _lio_dlq_push(lead_id: str, payload: dict, reason: str = "") -> None:
    """Push a failed LIO delivery to Redis dead-letter queue for later replay."""
    try:
        import redis.asyncio as aioredis
        redis_url = os.getenv("REDIS_URL", "")
        if not redis_url:
            return
        r = aioredis.from_url(redis_url, decode_responses=True)
        entry = json.dumps({
            "lead_id":    lead_id,
            "payload":    payload,
            "reason":     reason,
            "failed_at":  datetime.now(timezone.utc).isoformat(),
        })
        await r.lpush(_LIO_DLQ_KEY, entry)
        await r.close()
        logger.info("[LIO-DLQ] Lead %s pushed to DLQ (reason=%s)", lead_id, reason)
    except Exception as e:
        logger.error("[LIO-DLQ] Failed to push lead %s to DLQ: %s", lead_id, e)


async def lio_dlq_replay(max_items: int = 50) -> dict:
    """Replay up to max_items entries from the LIO dead-letter queue."""
    try:
        import redis.asyncio as aioredis
        redis_url = os.getenv("REDIS_URL", "")
        if not redis_url:
            return {"replayed": 0, "failed": 0, "error": "REDIS_URL not set"}
        r = aioredis.from_url(redis_url, decode_responses=True)
        replayed = failed = 0
        for _ in range(max_items):
            raw = await r.rpop(_LIO_DLQ_KEY)
            if not raw:
                break
            try:
                entry = json.loads(raw)
                payload = entry.get("payload", {})
                lead_id = entry.get("lead_id", "unknown")
                client = _get_lio_client()
                resp = await client.post(LIO_RECEIVE_URL, json=payload)
                resp.raise_for_status()
                logger.info("[LIO-DLQ] Replayed lead %s → HTTP %s", lead_id, resp.status_code)
                replayed += 1
            except Exception as e:
                logger.warning("[LIO-DLQ] Replay failed: %s — re-queuing", e)
                await r.lpush(_LIO_DLQ_KEY, raw)  # put back for next retry
                failed += 1
        await r.close()
        return {"replayed": replayed, "failed": failed}
    except Exception as e:
        logger.error("[LIO-DLQ] Replay error: %s", e)
        return {"replayed": 0, "failed": 0, "error": str(e)}


_CRM_BRIEF_REQUIRED_KEYS = ("who_they_are", "crm_scores", "outreach_blueprint", "crm_import_fields")

def _validate_crm_brief(d: dict) -> bool:
    """
    Returns True if crm_brief has all required top-level keys and is not empty.
    False = treat as invalid → trigger regen.
    """
    if not d or not isinstance(d, dict):
        return False
    for key in _CRM_BRIEF_REQUIRED_KEYS:
        if key not in d or not d[key]:
            return False
    return True


async def send_to_lio_failed(
    linkedin_url: str,
    org_id: str,
    sso_id: str = "",
    reason: str = "enrichment_failed",
    error_message: str = "",
) -> None:
    """
    Notify LIO that a LinkedIn URL failed enrichment.
    Sends status=failed so LIO can mark the lead accordingly.
    """
    url = LIO_RECEIVE_URL
    if not url:
        return
    enrichment_data = {
        "status":        "failed",
        "input_url":     linkedin_url,
        "linkedin_url":  linkedin_url,
        "error_reason":  reason,
        "error_message": error_message or reason,
    }
    payload = {
        "enrichment_data": enrichment_data,
        "sso_id":          sso_id,
        "organization_id": org_id,
    }
    try:
        client = _get_lio_client()
        resp = await client.post(url, json=payload)
        logger.info("[LIO-FAILED] Sent failure for %s → HTTP %s", linkedin_url, resp.status_code)
    except Exception as e:
        logger.warning("[LIO-FAILED] Could not notify LIO for %s: %s", linkedin_url, e)


async def send_to_lio(lead: dict, sso_id: str = "", force: bool = False) -> None:
    """
    Fire-and-forget: forward an enriched lead to the LIO service.
    Called as asyncio.create_task() — never blocks enrich_single().
    force=True bypasses idempotency — use when user explicitly re-submits same URL.
    Logs a warning on failure but does not raise.
    """
    url = LIO_RECEIVE_URL
    if not url:
        logger.warning("[LIO] LIO_RECEIVE_URL is not set — skipping forward")
        return

    lead_id = lead.get("lead_id") or lead.get("id", "unknown")

    # Idempotency — skip if already sent, unless force=True (user re-submission)
    if not force:
        try:
            async with get_pool().acquire() as _conn:
                _row = await _conn.fetchrow(
                    "SELECT lio_sent_at FROM enriched_leads WHERE id=$1", lead_id
                )
                if _row and _row["lio_sent_at"]:
                    logger.info("[LIO] Lead %s already sent at %s — skipping (idempotent)", lead_id, _row["lio_sent_at"])
                    return
        except Exception:
            pass  # DB check failed — proceed

    # Redis secondary cache (24h TTL) — fast path if Redis available
    _lio_sent_key = f"lio:sent:{lead_id}"
    if not force:
        try:
            import redis.asyncio as aioredis
            _redis_url = os.getenv("REDIS_URL", "")
            if _redis_url:
                _r = aioredis.from_url(_redis_url, decode_responses=True)
                already_sent = await _r.get(_lio_sent_key)
                await _r.close()
                if already_sent:
                    logger.info("[LIO] Lead %s already sent (Redis) — skipping", lead_id)
                    return
        except Exception:
            pass  # Redis unavailable — proceed without Redis check

    # work_email is the DB column name; email is legacy/nested key — prefer work_email
    raw_email = lead.get("work_email") or lead.get("email", "") or ""
    # Strip Apollo/external placeholder emails — they are internal IDs, not real addresses
    email = raw_email if raw_email and "placeholder" not in raw_email.lower() else ""

    # phone: direct_phone is the DB column; phone is legacy
    phone = lead.get("direct_phone") or lead.get("phone", "") or ""

    # enrichment_data = the full linkedin_enrich structured view
    import copy
    linkedin_enrich = copy.deepcopy(lead.get("linkedin_enrich") or {})

    # Strip emojis from identity fields
    identity = linkedin_enrich.get("identity") or {}
    for f in ("name", "first_name", "last_name", "title", "company", "about", "location"):
        if identity.get(f):
            identity[f] = _strip_emojis(identity[f])
    profile = identity.get("profile") or {}
    for f in ("full_name", "first_name", "last_name", "current_title"):
        if profile.get(f):
            profile[f] = _strip_emojis(profile[f])

    # Clean placeholder + emoji in contact block
    contact_block = linkedin_enrich.get("contact") or {}
    for email_key in ("work_email", "email"):
        v = contact_block.get(email_key)
        if v and "placeholder" in str(v).lower():
            contact_block[email_key] = None

    # crm_brief is stored as JSON string in DB — parse back to dict for LIO
    _crm_brief_raw = lead.get("crm_brief")
    _crm_brief = None
    if _crm_brief_raw:
        try:
            _crm_brief = json.loads(_crm_brief_raw) if isinstance(_crm_brief_raw, str) else _crm_brief_raw
        except Exception:
            pass

    # Patch profile_image and company_logo from DB — LLM never has actual image URLs
    if _crm_brief and isinstance(_crm_brief, dict):
        who = _crm_brief.get("who_they_are")
        if isinstance(who, dict):
            if not who.get("profile_image"):
                who["profile_image"] = lead.get("avatar_url") or ""
            if not who.get("company_logo"):
                who["company_logo"] = lead.get("company_logo") or ""

        # Ensure crm_scores are integers — LLM sometimes returns strings like "High", "80%"
        scores = _crm_brief.get("crm_scores")
        if isinstance(scores, dict):
            for _sf in ("icp_fit", "engagement_score", "timing_score"):
                v = scores.get(_sf)
                if not isinstance(v, int):
                    try:
                        scores[_sf] = int(str(v).replace("%", "").strip())
                    except Exception:
                        scores[_sf] = 0

            # Fallback to DB-calculated scores when LLM returned 0
            # (LLM copies the template placeholder; DB scoring is the ground truth)
            if not scores.get("icp_fit"):
                scores["icp_fit"] = int(lead.get("icp_fit_score") or 0)
            if not scores.get("engagement_score"):
                scores["engagement_score"] = int(lead.get("intent_score") or 0)
            if not scores.get("timing_score"):
                scores["timing_score"] = int(lead.get("timing_score") or 0)

        # Inject lead_id into who_they_are — LLM output never has this
        if isinstance(_crm_brief.get("who_they_are"), dict):
            _crm_brief["who_they_are"].setdefault("lead_id", lead_id)

        # Inject contact block — LLM never has actual email/phone; pull from DB
        if "contact" not in _crm_brief:
            _crm_brief["contact"] = {
                "work_email":       email,
                "phone":            phone,
                "email_source":     lead.get("email_source", ""),
                "email_confidence": lead.get("email_confidence", ""),
                "email_verified":   bool(lead.get("email_verified")),
                "bounce_risk":      lead.get("bounce_risk", ""),
            }

        # Patch crm_import_fields.analyst_summary — build a narrative bio if LLM left it empty
        _cif = _crm_brief.get("crm_import_fields")
        if isinstance(_cif, dict) and not _cif.get("analyst_summary"):
            # Priority 1: score_explanation from scoring engine (already a sentence)
            _db_summary = lead.get("score_explanation") or ""
            if not _db_summary:
                # Priority 2: build narrative from crm_brief's own LLM-generated fields
                _wta = _crm_brief.get("who_they_are") or {}
                _wdt = _crm_brief.get("what_drives_them") or {}
                _ob  = _crm_brief.get("outreach_blueprint") or {}
                _persona    = _wta.get("persona") or _wta.get("seniority") or ""
                _trajectory = _wta.get("trajectory") or ""
                _strategy   = _ob.get("one_line_strategy") or ""
                _pain       = (_wdt.get("pain_points") or [None])[0] or ""
                _name_str   = lead.get("name") or _wta.get("name") or ""
                _company_str = lead.get("company") or _wta.get("company") or ""
                _title_str  = lead.get("title") or _wta.get("title") or ""
                _sentences: list[str] = []
                # Sentence 1: who they are
                if _name_str and _title_str and _company_str:
                    _sentences.append(f"{_name_str} is a {_title_str} at {_company_str}.")
                elif _name_str and _persona and _company_str:
                    _sentences.append(f"{_name_str} is a {_persona} at {_company_str}.")
                elif _trajectory:
                    _sentences.append(_trajectory if _trajectory.endswith(".") else _trajectory + ".")
                # Sentence 2: what makes them notable
                if _pain:
                    _sentences.append(_pain if _pain.endswith(".") else _pain + ".")
                # Sentence 3: sales relevance / strategy
                if _strategy:
                    _sentences.append(_strategy if _strategy.endswith(".") else _strategy + ".")
                elif lead.get("warm_signal"):
                    _sentences.append(lead["warm_signal"] if lead["warm_signal"].endswith(".") else lead["warm_signal"] + ".")
                _db_summary = " ".join(_sentences)
            _cif["analyst_summary"] = _db_summary

        # Inject company_intelligence — LLM schema doesn't include this; pull from DB/website_intel
        if "company_intelligence" not in _crm_brief:
            _wi = _parse_json_safe(lead.get("website_intelligence"), {})
            _prod_offer = _parse_json_safe(lead.get("product_offerings"), [])
            _tgt_custs  = _parse_json_safe(lead.get("target_customers"), [])
            _crm_brief["company_intelligence"] = {
                "description":       lead.get("company_description") or _wi.get("company_description") or "",
                "value_proposition": lead.get("value_proposition") or _wi.get("value_proposition") or "",
                "product_category":  lead.get("product_category") or _wi.get("product_category") or "",
                "product_offerings": _prod_offer[:10],
                "target_customers":  _tgt_custs,
                "pricing_signals":   lead.get("pricing_signals") or _wi.get("pricing_signals") or "",
            }

    # enrichment_data = LLM-structured output (shaped by lio_system_prompt).
    # Fallback maps linkedin_enrich into the same crm_brief JSON structure so LIO
    # always receives a consistent shape regardless of whether LLM ran.
    def _linkedin_enrich_to_crm_brief_shape(le: dict) -> dict:
        ident   = le.get("identity") or {}
        prof    = ident.get("profile") or {}
        scores  = le.get("scores") or {}
        brkdown = scores.get("breakdown") or {}
        bsig    = le.get("behavioural_signals") or {}
        pitch   = le.get("pitch_intelligence") or {}
        act     = le.get("activity") or {}
        co      = ident.get("profile") or {}
        tags    = le.get("tags") or []
        posts   = act.get("linkedin_posts") or []
        feed    = act.get("feed") or []
        return {
            "who_they_are": {
                "name":           ident.get("name", ""),
                "title":          ident.get("title", ""),
                "company":        ident.get("company", ""),
                "lead_id":        lead_id,
                "location":       ident.get("location") or ident.get("country", ""),
                "linkedin_url":   ident.get("linkedin_url", ""),
                "profile_image":  lead.get("avatar_url") or "",
                "company_logo":   lead.get("company_logo") or (le.get("company_profile") or {}).get("company_logo") or "",
                "followers":      ident.get("followers", 0),
                "connections":    ident.get("connections", 0),
                "persona":        prof.get("seniority_level", ""),
                "seniority":      prof.get("seniority_level", ""),
                "trajectory":     "",
                "decision_maker": "Yes" if "c-level" in (prof.get("seniority_level") or "").lower() or "vp" in (prof.get("seniority_level") or "").lower() else "Unknown",
            },
            "their_company": {
                "type":             "",
                "industry":         "",
                "stage":            "",
                "company_size":     "",
                "founded":          "",
                "website":          "",
                "company_tags":     [],
                "relevance_score":  0,
                "relevance_reason": "",
            },
            "what_they_care_about": {
                "primary_interests":  [bsig.get("posts_about", "")] if bsig.get("posts_about") else [],
                "also_interested_in": [bsig.get("engages_with", "")] if bsig.get("engages_with") else [],
                "passion_signals":    [],
            },
            "online_behaviour": {
                "activity_level":   "",
                "style":            bsig.get("communication_style", ""),
                "recurring_themes": [],
                "platform":         "LinkedIn",
            },
            "communication": {
                "tone":          "",
                "writing_style": bsig.get("communication_style", ""),
                "emotional_mode": "",
                "archetype":     "",
                "mirror_tip":    "",
            },
            "what_drives_them": {
                "values":      [],
                "motivators":  [],
                "pain_points": [bsig.get("pain_point_hint", "")] if bsig.get("pain_point_hint") else [],
                "ambitions":   [],
            },
            "buying_signals": {
                "intent_level":   brkdown.get("score_tier", ""),
                "trigger_events": [],
                "tools_used":     [],
                "decision_style": bsig.get("decision_pattern", ""),
                "intent_tags":    tags[:5],
            },
            "smart_tags": tags[:8],
            "outreach_blueprint": {
                "best_channel":      "",
                "best_approach":     pitch.get("best_angle", ""),
                "opening_hook_1":    pitch.get("suggested_cta", ""),
                "opening_hook_2":    "",
                "content_to_send":   "",
                "topics_to_avoid":   pitch.get("do_not_pitch") or [],
                "one_line_strategy": pitch.get("best_value_prop", ""),
            },
            "crm_scores": {
                "icp_fit":    brkdown.get("icp_fit_score", 0),
                "engagement": brkdown.get("intent_score", 0),
                "timing":     brkdown.get("timing_score", 0),
                "priority":   brkdown.get("score_tier", ""),
            },
            "crm_import_fields": {
                "buyer_type":     prof.get("seniority_level", ""),
                "buying_signal":   bsig.get("warm_signal", "") or "",
                "outreach_tone":   "",
                "hook_theme":      pitch.get("best_angle", ""),
                "avoidance":       ", ".join(pitch.get("do_not_pitch") or []),
                "tags":            tags[:5],
                "analyst_summary": (
                    f"{ident.get('name', '')} is a {ident.get('title', '')} at {ident.get('company', '')}. "
                    f"{bsig.get('pain_point_hint', '')}. "
                    f"{pitch.get('best_value_prop', '') or pitch.get('best_angle', '')}"
                ).strip(". ") + "." if ident.get("name") else pitch.get("top_pain_point", ""),
            },
            "recent_posts_summary": [
                {
                    "topic":         (p.get("text") or p.get("title") or "")[:80],
                    "tone":          "",
                    "key_message":   (p.get("text") or "")[:150],
                    "engagement":    str(p.get("likes") or p.get("num_likes") or 0),
                    "intent_signal": "",
                }
                for p in posts[:5]
            ] or [{"topic": "", "tone": "", "key_message": "", "engagement": "", "intent_signal": ""}],
            "recent_interactions_summary": [
                {
                    "type":          f.get("interaction", ""),
                    "content_topic": (f.get("title") or "")[:80],
                    "why_it_matters": "",
                    "intent_signal":  "",
                }
                for f in feed[:5]
            ] or [{"type": "", "content_topic": "", "why_it_matters": "", "intent_signal": ""}],
        }

    # ── Build enrichment_data matching exact LIO system prompt JSON schema ──────
    # _crm_brief (LLM-generated) takes priority if present.
    # Otherwise build from all available DB fields + raw BD data.
    full_data  = _parse_json_safe(lead.get("full_data"), {})
    wi         = _parse_json_safe(lead.get("website_intelligence"), {})
    raw_bd     = _parse_json_safe(lead.get("raw_brightdata"), {})
    tags_raw   = _parse_json_safe(lead.get("tags") or lead.get("auto_tags"), [])
    tags       = tags_raw if isinstance(tags_raw, list) else []
    skills     = _parse_json_safe(lead.get("top_skills"), [])
    tech_stack = _parse_json_safe(lead.get("tech_stack"), [])
    co_tags    = _parse_json_safe(lead.get("company_tags"), [])
    prod_offer = _parse_json_safe(lead.get("product_offerings"), [])
    tgt_custs  = _parse_json_safe(lead.get("target_customers"), [])

    # Raw BD experience + posts + activity
    bd_experience  = raw_bd.get("experience") or []
    bd_posts       = raw_bd.get("posts") or full_data.get("linkedin_posts") or []
    bd_activity    = raw_bd.get("activity") or full_data.get("activity_full") or []
    bd_current_co  = raw_bd.get("current_company") or {}

    # Derived signals
    seniority     = lead.get("seniority_level") or ""
    is_decision_maker = any(x in seniority.lower() for x in ("c-level","vp","director","founder","ceo","cto","coo","cmo","head","president","partner","managing"))
    trigger_events = [s for s in [
        lead.get("recent_funding_event"), lead.get("hiring_signal"),
        lead.get("job_change"), lead.get("news_mention"),
        lead.get("product_launch"), lead.get("competitor_usage"),
        lead.get("review_activity"), lead.get("linkedin_activity"),
    ] if s]

    # Build 3 post objects from BD posts
    def _post_obj(p: dict) -> dict:
        return {
            "topic":       (p.get("title") or p.get("text") or "")[:80],
            "tone":        "professional",
            "key_message": (p.get("text") or p.get("title") or "")[:150],
            "engagement":  str(p.get("num_likes") or p.get("likes") or 0),
            "intent_signal": "thought leadership" if p.get("num_likes", 0) > 50 else "awareness",
        }

    def _interaction_obj(a: dict) -> dict:
        return {
            "interaction_type": a.get("action") or a.get("interaction") or "engagement",
            "topic":            (a.get("title") or a.get("text") or a.get("attribution") or "")[:80],
            "insight":          (a.get("text") or a.get("title") or "")[:120],
            "intent_signal":    "active interest",
        }

    posts_data        = [_post_obj(p) for p in bd_posts[:3]]
    interactions_data = [_interaction_obj(a) for a in bd_activity[:3]]

    # Pad to minimum 3 items with placeholders if BD data is sparse
    blank_post = {"topic": f"Professional content at {lead.get('company','')}", "tone": "professional", "key_message": "Industry insights", "engagement": "N/A", "intent_signal": "awareness"}
    blank_int  = {"interaction_type": "LinkedIn engagement", "topic": f"{lead.get('industry') or lead.get('company','')} content", "insight": "Active on LinkedIn", "intent_signal": "awareness"}
    while len(posts_data) < 3:        posts_data.append(blank_post)
    while len(interactions_data) < 3: interactions_data.append(blank_int)

    if _crm_brief:
        enrichment_data = _crm_brief
    else:
        enrichment_data = {
            "who_they_are": {
                "name":           lead.get("name", ""),
                "title":          lead.get("title") or bd_current_co.get("title") or seniority or "",
                "company":        lead.get("company", ""),
                "location":       lead.get("city") or lead.get("country") or "",
                "lead_id":        lead_id,
                "linkedin_url":   lead.get("linkedin_url", ""),
                "profile_image":  lead.get("avatar_url") or "",
                "company_logo":   lead.get("company_logo") or "",
                "followers":      str(lead.get("followers") or 0),
                "connections":    str(lead.get("connections") or 0),
                "persona":        seniority or "Professional",
                "seniority":      seniority or "Individual Contributor",
                "trajectory":     lead.get("years_in_role") or (f"At {lead.get('company')} since joining" if lead.get("company") else ""),
                "decision_maker": "Yes" if is_decision_maker else "No",
            },
            "their_company": {
                "type":             lead.get("business_model") or wi.get("business_model") or "",
                "industry":         lead.get("industry") or wi.get("industry") or "",
                "stage":            lead.get("funding_stage") or "",
                "company_size":     str(lead.get("employee_count") or bd_current_co.get("employees") or ""),
                "founded":          lead.get("founded_year") or "",
                "website":          lead.get("company_website") or "",
                "company_tags":     (co_tags or prod_offer)[:5],
                "relevance_score":  str(lead.get("company_score") or lead.get("total_score") or ""),
                "relevance_reason": lead.get("company_description") or wi.get("company_description") or "",
            },
            "contact": {
                "work_email":       email,
                "phone":            phone,
                "email_source":     lead.get("email_source", ""),
                "email_confidence": lead.get("email_confidence", ""),
                "email_verified":   bool(lead.get("email_verified")),
                "bounce_risk":      lead.get("bounce_risk", ""),
            },
            "what_they_care_about": {
                "primary_interests":   (skills or prod_offer)[:5],
                "secondary_interests": tgt_custs[:5],
                "passion_signals":     (tags + [lead.get("company") or "", lead.get("industry") or "", seniority or "", lead.get("product_category") or ""])[:5],
            },
            "online_behaviour": {
                "activity_level":   "active" if (lead.get("followers") or 0) > 500 else "moderate",
                "content_style":    "professional thought leader" if bd_posts else "lurker/consumer",
                "recurring_themes": (skills + tags + [lead.get("industry") or "", lead.get("product_category") or "", lead.get("company") or ""])[:5],
                "primary_platform": "LinkedIn",
            },
            "communication": {
                "tone":            "professional",
                "writing_style":   "direct" if seniority and is_decision_maker else "collaborative",
                "emotional_mode":  "analytical",
                "archetype":       seniority or "Professional",
                "mirror_strategy": f"Lead with data and ROI — speak to {lead.get('company','their company')}'s scale and efficiency goals",
            },
            "what_drives_them": {
                "core_values":      (skills or ["Professional growth", "Team success", "Impact", "Efficiency", "Innovation"])[:5],
                "motivators":       [s for s in trigger_events if s][:5] or ["Career growth", "Team performance", "Business impact", "Efficiency", "Innovation"],
                "pain_points":      [lead.get("score_explanation") or f"Scaling {lead.get('department') or 'operations'} at {lead.get('company') or 'company'}"] + (tags[:4]),
                "career_ambitions": [f"Growing influence at {lead.get('company')}", "Building scalable processes", "Expanding team capabilities", "Driving measurable results", "Industry recognition"],
            },
            "buying_signals": {
                "intent_level":   lead.get("score_tier") or "",
                "trigger_events": (trigger_events or ["Active LinkedIn profile", f"Role at {lead.get('company')}", "Company growth signals", "Industry engagement", "Email found"])[:5],
                "tools_used":     (tech_stack or prod_offer)[:5],
                "decision_style": "consensus" if not is_decision_maker else "autonomous",
                "intent_tags":    tags[:5],
            },
            "smart_tags": (tags + [lead.get("company") or "", lead.get("industry") or "", seniority or "", lead.get("score_tier") or "", lead.get("product_category") or ""])[:5],
            "outreach_blueprint": {
                "best_channel":        lead.get("best_channel") or "Email",
                "approach_strategy":   lead.get("outreach_angle") or "",
                "opening_hooks":       [h for h in [
                    lead.get("email_subject"),
                    lead.get("linkedin_note"),
                    lead.get("outreach_angle"),
                    f"Quick question about scaling {lead.get('department') or 'your team'} at {lead.get('company')}",
                    f"How {lead.get('company')} could benefit from {lead.get('product_category') or 'our solution'}",
                ] if h][:5],
                "recommended_content": lead.get("cold_email") or "",
                "avoid_topics":        ["Generic outreach", "Price-first messaging", "Competitor comparisons", "Irrelevant use cases", "Mass-blast templates"],
                "one_line_strategy":   lead.get("outreach_angle") or lead.get("score_explanation") or "",
            },
            "crm_scores": {
                "icp_fit":        str(lead.get("icp_fit_score") or 0),
                "engagement_score": str(lead.get("intent_score") or 0),
                "timing_score":   str(lead.get("timing_score") or 0),
                "priority_level": lead.get("score_tier") or "",
            },
            "crm_import_fields": {
                "buyer_type":      seniority or "",
                "buying_signal":   lead.get("warm_signal") or (trigger_events[0] if trigger_events else ""),
                "outreach_tone":   lead.get("sequence_type") or "",
                "hook_theme":      lead.get("outreach_angle") or "",
                "avoidance":       "Generic pitch, price-first, unsolicited attachments",
                "tags":            (tags + [lead.get("company") or "", lead.get("industry") or "", seniority or "", lead.get("score_tier") or "", lead.get("product_category") or ""])[:5],
                "analyst_summary": (
                    lead.get("score_explanation")
                    or (
                        f"{lead.get('name', '')} is a {lead.get('title') or seniority or ''} at {lead.get('company') or ''}. "
                        f"{lead.get('warm_signal') or ''}. "
                        f"{lead.get('outreach_angle') or ''}"
                    ).strip(". ") + "."
                    if lead.get("name") else ""
                ),
            },
            "recent_activity": {
                "posts":        posts_data,
                "interactions": interactions_data,
            },
            "company_intelligence": {
                "description":       lead.get("company_description") or wi.get("company_description") or "",
                "value_proposition": lead.get("value_proposition") or wi.get("value_proposition") or "",
                "product_category":  lead.get("product_category") or wi.get("product_category") or "",
                "product_offerings": prod_offer[:10],
                "target_customers":  tgt_custs,
                "pricing_signals":   lead.get("pricing_signals") or wi.get("pricing_signals") or "",
            },
        }

    # ── Validate crm_brief — null, empty {}, or partial JSON ─────────────────
    _org_id_for_regen = lead.get("organization_id") or "default"
    _linkedin_url_for_fail = lead.get("linkedin_url", "")

    if not _validate_crm_brief(_crm_brief):
        # Issue 3 & 4: empty {} or partial/missing required keys
        if _crm_brief is not None:
            logger.warning("[LIO] Lead %s crm_brief is empty or partial — attempting regen", lead_id)
        else:
            logger.warning("[LIO] Lead %s crm_brief is null — attempting regen", lead_id)

        if lead.get("name"):
            try:
                # Await regen directly (we're already in a background task)
                regen_lead = await regenerate_crm_brief_for_lead(lead_id, org_id=_org_id_for_regen)
                if regen_lead:
                    regen_brief_raw = regen_lead.get("crm_brief")
                    regen_brief = None
                    if regen_brief_raw:
                        try:
                            regen_brief = json.loads(regen_brief_raw) if isinstance(regen_brief_raw, str) else regen_brief_raw
                        except Exception:
                            pass
                    if _validate_crm_brief(regen_brief):
                        logger.info("[LIO] Regen successful for lead %s — proceeding to send", lead_id)
                        _crm_brief = regen_brief
                    else:
                        raise RuntimeError("Regen produced invalid crm_brief")
                else:
                    raise RuntimeError("Regen returned no lead")
            except Exception as regen_err:
                logger.error("[LIO] Regen also failed for lead %s: %s — notifying LIO as failed", lead_id, regen_err)
                await send_to_lio_failed(_linkedin_url_for_fail, _org_id_for_regen, sso_id, reason="llm_failed")
                return
        else:
            logger.warning("[LIO] Lead %s has no name — cannot regen, notifying LIO as failed", lead_id)
            await send_to_lio_failed(_linkedin_url_for_fail, _org_id_for_regen, sso_id, reason="llm_failed")
            return

    _crm_brief["status"] = "success"
    _crm_brief["input_url"] = lead.get("linkedin_url") or ""
    payload = {
        "enrichment_data": _crm_brief,
        "sso_id":          sso_id,
        "organization_id": lead.get("organization_id", ""),
    }

    logger.info("[LIO] Sending lead %s to %s | sso_id=%s | org=%s",
                lead_id, url, sso_id, lead.get("organization_id", ""))
    _pipeline_log.info("[LIO-PAYLOAD] lead=%s\n%s", lead_id, json.dumps(payload, indent=2, default=str))

    # Retry with exponential backoff: 3 attempts, 5s / 10s delays between them
    _LIO_MAX_ATTEMPTS = 3
    for attempt in range(1, _LIO_MAX_ATTEMPTS + 1):
        try:
            client = _get_lio_client()
            resp = await client.post(url, json=payload)
            logger.info("[LIO] Response for lead %s → HTTP %s | body=%s",
                        lead_id, resp.status_code, resp.text[:500])
            resp.raise_for_status()
            logger.info("[LIO] Successfully forwarded lead %s (attempt %d)", lead_id, attempt)
            # Mark as sent in DB — primary idempotency record (Redis-independent)
            try:
                async with get_pool().acquire() as _conn:
                    await _conn.execute(
                        "UPDATE enriched_leads SET lio_sent_at=NOW() WHERE id=$1", lead_id
                    )
            except Exception:
                pass
            # Mark as sent in Redis (24h TTL) — fast-path cache
            try:
                import redis.asyncio as aioredis
                _redis_url = os.getenv("REDIS_URL", "")
                if _redis_url:
                    _r = aioredis.from_url(_redis_url, decode_responses=True)
                    await _r.set(_lio_sent_key, "1", ex=86400)
                    await _r.close()
            except Exception:
                pass
            return  # success — exit retry loop
        except httpx.TimeoutException:
            logger.warning("[LIO] Timeout lead %s (attempt %d/%d)", lead_id, attempt, _LIO_MAX_ATTEMPTS)
        except httpx.ConnectError as e:
            logger.warning("[LIO] Connection error lead %s (attempt %d/%d): %s", lead_id, attempt, _LIO_MAX_ATTEMPTS, e)
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            logger.warning("[LIO] HTTP %s lead %s (attempt %d/%d) | body=%s",
                           status, lead_id, attempt, _LIO_MAX_ATTEMPTS, e.response.text[:300])
            if status < 500 and status != 429:
                # 4xx errors (except rate-limit) are not retryable — go straight to DLQ
                await _lio_dlq_push(lead_id, payload, reason=f"HTTP {status}")
                return
        except Exception as e:
            logger.warning("[LIO] Unexpected error lead %s (attempt %d/%d): %s", lead_id, attempt, _LIO_MAX_ATTEMPTS, e)

        if attempt < _LIO_MAX_ATTEMPTS:
            await asyncio.sleep(1 * attempt)  # 1s then 2s before final attempt

    # All attempts exhausted — push to dead-letter queue for manual replay
    logger.error("[LIO] All %d attempts failed for lead %s — pushing to DLQ", _LIO_MAX_ATTEMPTS, lead_id)
    await _lio_dlq_push(lead_id, payload, reason="max_retries_exceeded")


_redis_pool: Any = None


async def _get_redis() -> Any:
    """Lazy-init async Redis client. Returns None if unavailable."""
    global _redis_pool
    if not _REDIS_AVAILABLE:
        return None
    if _redis_pool is None:
        try:
            _redis_pool = aioredis.from_url(REDIS_URL, decode_responses=True)
            await _redis_pool.ping()
            logger.info("[Redis] Connected: %s", REDIS_URL)
        except Exception as e:
            logger.warning("[Redis] Not available (%s) — in-process fallback active", e)
            _redis_pool = None
    return _redis_pool


async def _push_lead_task(queue: str, task: dict) -> bool:
    """Push a lead task to a Redis queue. Returns True if pushed."""
    r = await _get_redis()
    if not r:
        return False
    try:
        await r.rpush(queue, json.dumps(task))
        return True
    except Exception as e:
        logger.warning("[Redis] Push failed: %s", e)
        return False


# ── Ably publish helpers ───────────────────────────────────────────────────────

async def _publish_lead_done(org_id: str, job_id: str, lead: dict) -> None:
    """Publish lead:done event to Ably — real-time frontend update."""
    try:
        from realtime import ably_service
        channel = f"tenant:{org_id}:job:{job_id}"
        await ably_service.publish(channel, "lead:done", {
            "job_id":         job_id,
            "org_id":         org_id,
            "lead_id":        lead.get("id"),
            "linkedin_url":   lead.get("linkedin_url"),
            "name":           lead.get("name"),
            "score":          lead.get("total_score"),
            "tier":           lead.get("score_tier"),
            "email":          lead.get("work_email"),
            "company":        lead.get("company"),
            "title":          lead.get("title"),
            "cache_hit":      bool(lead.get("_cache_hit")),
            "linkedin_enrich": lead.get("linkedin_enrich"),
        })
    except Exception as e:
        logger.debug("[Ably] lead:done publish failed: %s", e)


async def _publish_job_done(org_id: str, job_id: str, processed: int, failed: int) -> None:
    """Publish job:done event when all leads in a bulk job are processed."""
    try:
        from realtime import ably_service
        channel = f"tenant:{org_id}:job:{job_id}"
        await ably_service.publish(channel, "job:done", {
            "job_id":    job_id,
            "org_id":    org_id,
            "processed": processed,
            "failed":    failed,
        })
    except Exception as e:
        logger.debug("[Ably] job:done publish failed: %s", e)


# ─────────────────────────────────────────────────────────────────────────────
# Database
# ─────────────────────────────────────────────────────────────────────────────

async def init_leads_db() -> None:
    async with get_pool().acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS enriched_leads (
                id               TEXT PRIMARY KEY,
                linkedin_url     TEXT NOT NULL,
                -- Identity
                name             TEXT,
                first_name       TEXT,
                last_name        TEXT,
                work_email       TEXT,
                personal_email   TEXT,
                direct_phone     TEXT,
                twitter          TEXT,
                city             TEXT,
                country          TEXT,
                timezone         TEXT,
                -- Professional
                title            TEXT,
                seniority_level  TEXT,
                department       TEXT,
                years_in_role    TEXT,
                company          TEXT,
                previous_companies TEXT,
                top_skills       TEXT,
                education        TEXT,
                certifications   TEXT,
                languages        TEXT,
                -- Company
                company_website  TEXT,
                industry         TEXT,
                employee_count   INTEGER DEFAULT 0,
                hq_location      TEXT,
                founded_year     TEXT,
                funding_stage    TEXT,
                total_funding    TEXT,
                last_funding_date TEXT,
                lead_investor    TEXT,
                annual_revenue   TEXT,
                tech_stack       TEXT,
                hiring_velocity  TEXT,
                -- Company enrichment (waterfall)
                avatar_url       TEXT,
                company_logo     TEXT,
                company_email    TEXT,
                company_description TEXT,
                company_linkedin TEXT,
                company_twitter  TEXT,
                company_phone    TEXT,
                waterfall_log    TEXT,
                -- Website Intelligence (Stage 3)
                website_intelligence TEXT,
                product_offerings TEXT,
                value_proposition TEXT,
                target_customers TEXT,
                business_model   TEXT,
                pricing_signals  TEXT,
                product_category TEXT,
                data_completeness_score INTEGER DEFAULT 0,
                -- Intent
                recent_funding_event TEXT,
                hiring_signal    TEXT,
                job_change       TEXT,
                linkedin_activity TEXT,
                news_mention     TEXT,
                product_launch   TEXT,
                competitor_usage TEXT,
                review_activity  TEXT,
                -- Scoring
                icp_fit_score    INTEGER DEFAULT 0,
                intent_score     INTEGER DEFAULT 0,
                timing_score     INTEGER DEFAULT 0,
                engagement_score INTEGER DEFAULT 0,
                total_score      INTEGER DEFAULT 0,
                score_tier       TEXT,
                score_explanation TEXT,
                icp_match_tier   TEXT,
                disqualification_flags TEXT,
                -- Outreach
                email_subject    TEXT,
                cold_email       TEXT,
                linkedin_note    TEXT,
                best_channel     TEXT,
                best_send_time   TEXT,
                outreach_angle   TEXT,
                sequence_type    TEXT,
                outreach_sequence TEXT,
                last_contacted   TEXT,
                email_status     TEXT,
                -- CRM
                lead_source      TEXT DEFAULT 'LinkedIn URL',
                enrichment_source TEXT,
                data_completeness INTEGER DEFAULT 0,
                crm_stage        TEXT,
                tags             TEXT,
                assigned_owner   TEXT,
                -- Meta
                full_data        TEXT,
                raw_profile      TEXT,
                about            TEXT,
                followers        INTEGER DEFAULT 0,
                connections      INTEGER DEFAULT 0,
                email_source     TEXT,
                email_confidence TEXT,
                status           TEXT DEFAULT 'enriched',
                job_id           TEXT,
                organization_id  TEXT NOT NULL DEFAULT 'default',
                enriched_at      TEXT,
                created_at       TEXT DEFAULT '',
                lio_results_json TEXT
            )
        """)
        await conn.execute("ALTER TABLE enriched_leads ADD COLUMN IF NOT EXISTS lio_results_json TEXT")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS enrichment_jobs (
                id              TEXT PRIMARY KEY,
                snapshot_id     TEXT,
                total_urls      INTEGER DEFAULT 0,
                processed       INTEGER DEFAULT 0,
                failed          INTEGER DEFAULT 0,
                status          TEXT DEFAULT 'pending',
                error           TEXT,
                webhook_url     TEXT,
                organization_id TEXT NOT NULL DEFAULT 'default',
                created_at      TEXT DEFAULT '',
                updated_at      TEXT DEFAULT ''
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS enrichment_sub_jobs (
                id              TEXT PRIMARY KEY,
                job_id          TEXT NOT NULL,
                chunk_index     INTEGER NOT NULL,
                total_urls      INTEGER DEFAULT 0,
                processed       INTEGER DEFAULT 0,
                failed          INTEGER DEFAULT 0,
                status          TEXT DEFAULT 'pending',
                organization_id TEXT NOT NULL DEFAULT 'default',
                created_at      TEXT DEFAULT '',
                updated_at      TEXT DEFAULT ''
            )
        """)
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sub_jobs_job ON enrichment_sub_jobs(job_id)"
        )
        # Add snapshot_id to sub_jobs (chunked BD approach — each chunk has its own snapshot)
        await conn.execute("ALTER TABLE enrichment_sub_jobs ADD COLUMN IF NOT EXISTS snapshot_id TEXT")
        # Add columns to existing tables (idempotent via IF NOT EXISTS)
        _NEW_COLS = [
            ("work_email", "TEXT"), ("personal_email", "TEXT"),
            ("direct_phone", "TEXT"), ("twitter", "TEXT"),
            ("city", "TEXT"), ("country", "TEXT"), ("timezone", "TEXT"),
            ("seniority_level", "TEXT"), ("department", "TEXT"),
            ("years_in_role", "TEXT"), ("previous_companies", "TEXT"),
            ("top_skills", "TEXT"), ("education", "TEXT"),
            ("certifications", "TEXT"), ("languages", "TEXT"),
            ("company_website", "TEXT"), ("industry", "TEXT"),
            ("employee_count", "INTEGER DEFAULT 0"),
            ("hq_location", "TEXT"), ("founded_year", "TEXT"),
            ("funding_stage", "TEXT"), ("total_funding", "TEXT"),
            ("last_funding_date", "TEXT"), ("lead_investor", "TEXT"),
            ("annual_revenue", "TEXT"), ("tech_stack", "TEXT"),
            ("hiring_velocity", "TEXT"), ("recent_funding_event", "TEXT"),
            ("hiring_signal", "TEXT"), ("job_change", "TEXT"),
            ("linkedin_activity", "TEXT"), ("news_mention", "TEXT"),
            ("product_launch", "TEXT"), ("competitor_usage", "TEXT"),
            ("review_activity", "TEXT"), ("icp_fit_score", "INTEGER DEFAULT 0"),
            ("timing_score", "INTEGER DEFAULT 0"),
            ("score_explanation", "TEXT"), ("icp_match_tier", "TEXT"),
            ("disqualification_flags", "TEXT"), ("email_subject", "TEXT"),
            ("cold_email", "TEXT"), ("linkedin_note", "TEXT"),
            ("best_channel", "TEXT"), ("best_send_time", "TEXT"),
            ("outreach_angle", "TEXT"), ("sequence_type", "TEXT"),
            ("last_contacted", "TEXT"), ("email_status", "TEXT"),
            ("lead_source", "TEXT"), ("enrichment_source", "TEXT"),
            ("data_completeness", "INTEGER DEFAULT 0"),
            ("crm_stage", "TEXT"), ("tags", "TEXT"),
            ("assigned_owner", "TEXT"), ("full_data", "TEXT"),
            ("avatar_url", "TEXT"), ("company_logo", "TEXT"),
            ("company_email", "TEXT"), ("company_description", "TEXT"),
            ("company_linkedin", "TEXT"), ("company_twitter", "TEXT"),
            ("company_phone", "TEXT"), ("waterfall_log", "TEXT"),
            ("website_intelligence", "TEXT"), ("product_offerings", "TEXT"),
            ("value_proposition", "TEXT"), ("target_customers", "TEXT"),
            ("business_model", "TEXT"), ("pricing_signals", "TEXT"),
            ("product_category", "TEXT"), ("data_completeness_score", "INTEGER DEFAULT 0"),
            ("organization_id", "TEXT DEFAULT 'default'"),
            ("activity_feed", "TEXT"), ("auto_tags", "TEXT"),
            ("behavioural_signals", "TEXT"), ("pitch_intelligence", "TEXT"),
            ("warm_signal", "TEXT"), ("email_verified", "INTEGER DEFAULT 0"),
            ("bounce_risk", "TEXT"), ("company_id", "TEXT"),
            ("company_score", "INTEGER DEFAULT 0"), ("combined_score", "INTEGER DEFAULT 0"),
            ("company_tags", "TEXT"), ("culture_signals", "TEXT"),
            ("account_pitch", "TEXT"), ("wappalyzer_tech", "TEXT"),
            ("news_mentions", "TEXT"), ("crunchbase_data", "TEXT"),
            ("linkedin_posts", "TEXT"), ("company_score_tier", "TEXT"),
            ("crm_brief", "TEXT"), ("raw_brightdata", "JSONB"),
            ("apollo_raw", "TEXT"),
            ("updated_at", "TIMESTAMPTZ DEFAULT NOW()"),
            ("lio_sent_at", "TIMESTAMPTZ"),
        ]
        for col, col_type in _NEW_COLS:
            await conn.execute(f"ALTER TABLE enriched_leads ADD COLUMN IF NOT EXISTS {col} {col_type}")
        await conn.execute("ALTER TABLE enrichment_jobs ADD COLUMN IF NOT EXISTS organization_id TEXT DEFAULT 'default'")
        await conn.execute("ALTER TABLE enrichment_jobs ADD COLUMN IF NOT EXISTS sso_id TEXT NOT NULL DEFAULT ''")
        await conn.execute("ALTER TABLE enrichment_jobs ADD COLUMN IF NOT EXISTS forward_to_lio BOOLEAN NOT NULL DEFAULT FALSE")
        await conn.execute("ALTER TABLE enrichment_audit_log ADD COLUMN IF NOT EXISTS step TEXT")
        await conn.execute("ALTER TABLE enrichment_audit_log ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'ok'")
        await conn.execute("ALTER TABLE enrichment_audit_log ADD COLUMN IF NOT EXISTS duration_ms INTEGER")
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS idx_leads_org        ON enriched_leads(organization_id, total_score DESC)",
            "CREATE INDEX IF NOT EXISTS idx_jobs_org         ON enrichment_jobs(organization_id, created_at DESC)",
            # Performance indexes — analytics + filtered list queries
            "CREATE INDEX IF NOT EXISTS idx_leads_org_date   ON enriched_leads(organization_id, enriched_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_leads_org_tier   ON enriched_leads(organization_id, score_tier)",
            "CREATE INDEX IF NOT EXISTS idx_leads_org_email  ON enriched_leads(organization_id) WHERE work_email IS NOT NULL AND work_email != ''",
            "CREATE INDEX IF NOT EXISTS idx_jobs_org_status  ON enrichment_jobs(organization_id, status, created_at DESC)",
        ]:
            await conn.execute(idx_sql)
        # ── processed_snapshots — webhook idempotency ─────────────────────────
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_snapshots (
                snapshot_id  TEXT PRIMARY KEY,
                job_id       TEXT,
                org_id       TEXT,
                processed_at TEXT DEFAULT ''
            )
        """)
        # ── enrichment_audit_log — who did what ────────────────────────────────
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS enrichment_audit_log (
                id           TEXT PRIMARY KEY,
                org_id       TEXT NOT NULL,
                action       TEXT NOT NULL,
                lead_id      TEXT,
                linkedin_url TEXT,
                job_id       TEXT,
                user_email   TEXT,
                meta         TEXT,
                created_at   TEXT DEFAULT ''
            )
        """)
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_audit_org ON enrichment_audit_log(org_id, created_at DESC)"
        )
        # ── lead_notes — manual annotations ───────────────────────────────────
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS lead_notes (
                id         TEXT PRIMARY KEY,
                lead_id    TEXT NOT NULL,
                org_id     TEXT NOT NULL,
                note       TEXT NOT NULL,
                created_at TEXT DEFAULT ''
            )
        """)
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_notes_lead ON lead_notes(lead_id)"
        )
    logger.info("[LeadEnrichmentBD] DB initialized (PostgreSQL)")


async def _upsert_lead(lead: dict) -> None:
    lead_id   = lead.get("id", "?")
    lead_url  = lead.get("linkedin_url", "?")
    lead_name = lead.get("name", "?")

    _pipeline_log.info("[DB-SAVE] ── START ── id=%s  url=%s  name=%s  status=%s  fields=%d",
                       lead_id, lead_url, lead_name, lead.get("status", "?"), len(lead))

    # ── Step 1: clear placeholder emails ──────────────────────────────────────
    for email_col in ("work_email", "personal_email", "email"):
        v = lead.get(email_col)
        if v and isinstance(v, str) and "placeholder" in v.lower():
            _pipeline_log.warning("[DB-SAVE] [STEP-1] clearing placeholder email  col=%s  val=%s", email_col, v)
            logger.warning("[DB] Clearing placeholder email in %s: %s", email_col, v)
            lead[email_col] = None

    # ── Step 2: strip emojis from text fields ─────────────────────────────────
    _pipeline_log.info("[DB-SAVE] [STEP-2] stripping emojis from text fields")
    _clean_lead_strings(lead, ["name", "first_name", "last_name", "title", "company",
                                "about", "location", "city", "country"])

    # ── Step 3: rewrite first-person text ─────────────────────────────────────
    _pipeline_log.info("[DB-SAVE] [STEP-3] normalising person text (first→third person)")
    _normalise_person_text(lead)

    # ── Step 4: type audit — log every field type, coerce dict/list → JSON str ─
    _pipeline_log.info("[DB-SAVE] [STEP-4] type audit — scanning %d fields", len(lead))
    coerced = []
    for k, v in lead.items():
        vtype = type(v).__name__
        if isinstance(v, (dict, list)):
            coerced.append(k)
            _pipeline_log.warning(
                "[DB-SAVE] [TYPE-COERCE] field=%-30s  type=%-6s  preview=%s",
                k, vtype, str(v)[:120]
            )
            lead[k] = json.dumps(v, default=str)
        else:
            _pipeline_log.debug(
                "[DB-SAVE] [TYPE-OK]     field=%-30s  type=%-6s  val=%s",
                k, vtype, str(v)[:80] if v is not None else "NULL"
            )
    if coerced:
        _pipeline_log.warning("[DB-SAVE] [STEP-4] coerced %d dict/list fields → JSON str: %s", len(coerced), coerced)
    else:
        _pipeline_log.info("[DB-SAVE] [STEP-4] all field types OK — no coercion needed")

    # ── Step 5: build SQL ──────────────────────────────────────────────────────
    cols = [k for k in lead if k != "id"]
    all_keys = ["id"] + cols
    placeholders = ", ".join(f"${i + 1}" for i in range(len(all_keys)))
    updates = ", ".join(f"{k}=EXCLUDED.{k}" for k in cols)
    sql = f"""
        INSERT INTO enriched_leads ({', '.join(all_keys)})
        VALUES ({placeholders})
        ON CONFLICT(id) DO UPDATE SET {updates}
    """
    args = [lead[k] for k in all_keys]
    _pipeline_log.info("[DB-SAVE] [STEP-5] SQL built — %d columns, executing INSERT/UPDATE", len(all_keys))

    # ── Step 6: execute ────────────────────────────────────────────────────────
    try:
        async with get_pool().acquire() as conn:
            await conn.execute(sql, *args)
        _pipeline_log.info("[DB-SAVE] [STEP-6] ✓ DB write SUCCESS  id=%s  status=%s", lead_id, lead.get("status"))
    except Exception as db_err:
        # Log each field + type to pinpoint exactly which one caused the failure
        _pipeline_log.error("[DB-SAVE] [STEP-6] ✗ DB write FAILED  id=%s  error=%s", lead_id, db_err)
        for i, k in enumerate(all_keys):
            v = lead.get(k)
            _pipeline_log.error("[DB-SAVE]   $%-3d  %-30s  %-10s  %s", i + 1, k, type(v).__name__, str(v)[:100])
        raise

    # ── Step 7: bust Redis cache ───────────────────────────────────────────────
    if lead_id and _redis_pool:
        try:
            await _redis_pool.delete(f"lead:row:{lead_id}")
            _pipeline_log.info("[DB-SAVE] [STEP-7] Redis cache busted  key=lead:row:%s", lead_id)
        except Exception:
            pass

    _pipeline_log.info("[DB-SAVE] ── DONE ── id=%s", lead_id)


async def _update_job(job_id: str, **kwargs) -> None:
    if not kwargs:
        return
    kwargs["updated_at"] = datetime.now(timezone.utc)
    keys = list(kwargs.keys())
    sets = ", ".join(f"{k}=${i + 1}" for i, k in enumerate(keys))
    args = [kwargs[k] for k in keys] + [job_id]
    async with get_pool().acquire() as conn:
        await conn.execute(f"UPDATE enrichment_jobs SET {sets} WHERE id=${len(args)}", *args)


async def _create_sub_job(
    sub_job_id: str, job_id: str, chunk_index: int, total_urls: int, org_id: str,
    snapshot_id: Optional[str] = None,
) -> None:
    now = datetime.now(timezone.utc)
    async with get_pool().acquire() as conn:
        await conn.execute(
            """INSERT INTO enrichment_sub_jobs
               (id, job_id, chunk_index, total_urls, processed, failed, status, organization_id, snapshot_id, created_at, updated_at)
               VALUES ($1,$2,$3,$4,0,0,'pending',$5,$6,$7,$8)
               ON CONFLICT(id) DO NOTHING""",
            sub_job_id, job_id, chunk_index, total_urls, org_id, snapshot_id, now, now,
        )


async def _update_sub_job(sub_job_id: str, **kwargs) -> None:
    if not kwargs:
        return
    kwargs["updated_at"] = datetime.now(timezone.utc)
    keys = list(kwargs.keys())
    sets = ", ".join(f"{k}=${i + 1}" for i, k in enumerate(keys))
    args = [kwargs[k] for k in keys] + [sub_job_id]
    async with get_pool().acquire() as conn:
        await conn.execute(f"UPDATE enrichment_sub_jobs SET {sets} WHERE id=${len(args)}", *args)


async def list_sub_jobs(job_id: str, org_id: str = "default") -> list[dict]:
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM enrichment_sub_jobs WHERE job_id=$1 AND organization_id=$2 ORDER BY chunk_index",
            job_id, org_id,
        )
        return [dict(r) for r in rows]


async def get_lead(lead_id: str) -> Optional[dict]:
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM enriched_leads WHERE id=$1", lead_id)
        return dict(row) if row else None


async def get_lead_by_url(linkedin_url: str) -> Optional[dict]:
    """Look up an enriched lead by its LinkedIn URL (same as get_lead(_lead_id(url)))."""
    lead_id = _lead_id(_normalize_linkedin_url(linkedin_url))
    return await get_lead(lead_id)


def _parse_json_safe(raw, default):
    """Parse a JSON string field, returning default on any failure."""
    if not raw:
        return default
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(raw)
    except Exception:
        return default


def _format_linkedin_enrich(lead: dict, include_contact: bool = True) -> dict:
    """
    Build the structured LinkedIn Enrichment view from a raw DB lead row.
    Shared by enrich_single() (embedded in response) and the /view/linkedin route.
    Pass include_contact=False for bulk enrichment to omit the contact block entirely.
    """
    full         = _parse_json_safe(lead.get("full_data"), {})
    lead_scoring = full.get("lead_scoring") or {}

    return {
        "lead_id":      lead.get("id"),
        "linkedin_url": lead.get("linkedin_url"),
        "enriched_at":  lead.get("enriched_at"),
        "cache_hit":    bool(lead.get("_cache_hit")),

        # "identity": {
        #     "first_name":   lead.get("first_name"),
        #     "last_name":    lead.get("last_name"),
        #     "name":         lead.get("name"),
        #     "title":        lead.get("title"),
        #     "company":      lead.get("company"),
        #     "location":     lead.get("location"),
        #     "country":      lead.get("country"),
        #     "timezone":     lead.get("timezone"),
        #     "about":        lead.get("about"),
        #     "linkedin_url": lead.get("linkedin_url"),
        #     "twitter_url":  lead.get("twitter_url"),
        #     "followers":    lead.get("followers"),
        #     "connections":  lead.get("connections"),
        #     "skills":       _parse_json_safe(lead.get("skills"), []),
        #     "profile":      full.get("person_profile", {}),
        # },

        # **({"contact": {
        #     "work_email":        lead.get("work_email"),
        #     "email":             lead.get("email"),
        #     "phone":             lead.get("phone"),
        #     "email_source":      lead.get("email_source"),
        #     "email_confidence":  lead.get("email_confidence"),
        #     "email_verified":    bool(lead.get("email_verified")),
        #     "bounce_risk":       lead.get("bounce_risk"),
        #     "enrichment_source": lead.get("enrichment_source"),
        # }} if include_contact else {}),

        # "scores": {
        #     "total_score":       lead.get("total_score"),
        #     "icp_score":         lead.get("icp_score"),
        #     "intent_score":      lead.get("intent_score"),
        #     "timing_score":      lead.get("timing_score"),
        #     "data_completeness": lead.get("data_completeness"),
        #     "score_tier":        lead.get("score_tier"),
        #     "combined_score":    lead.get("combined_score"),
        #     "score_reasons":     _parse_json_safe(lead.get("score_reasons"), []),
        #     "breakdown":         lead_scoring,
        # },

        # "icp_match": {
        #     "icp_score":        lead.get("icp_score"),
        #     "product_category": lead.get("product_category"),
        #     "score_tier":       lead.get("score_tier"),
        #     "icp_detail":       lead_scoring.get("icp_fit") or {},
        # },

        # "behavioural_signals": _parse_json_safe(lead.get("behavioural_signals"), {}),
        # "pitch_intelligence":  _parse_json_safe(lead.get("pitch_intelligence"), {}),

        # "activity": {
        #     "feed":           _parse_json_safe(lead.get("activity_feed"), []),
        #     "warm_signal":    lead.get("warm_signal"),
        #     "hiring_signals": full.get("hiring_signals", []),
        #     "linkedin_posts": full.get("linkedin_posts", []),
        # },

        # "tags": (
        #     _parse_json_safe(lead.get("auto_tags"), []) or
        #     _parse_json_safe(lead.get("tags"), [])
        # ),

        "crm_brief": _parse_json_safe(lead.get("crm_brief"), None),
    }


async def get_lead_lio_results(lead_id: str) -> dict:
    """Return saved LIO stage results for a lead (empty dict if none)."""
    async with get_pool().acquire() as conn:
        raw = await conn.fetchval(
            "SELECT lio_results_json FROM enriched_leads WHERE id=$1", lead_id
        )
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


async def save_lead_lio_results(lead_id: str, results: dict) -> None:
    """Merge new stage results into saved LIO results for a lead."""
    async with get_pool().acquire() as conn:
        raw = await conn.fetchval(
            "SELECT lio_results_json FROM enriched_leads WHERE id=$1", lead_id
        )
        existing: dict = {}
        if raw:
            try:
                existing = json.loads(raw)
            except Exception:
                pass
        existing.update(results)
        await conn.execute(
            "UPDATE enriched_leads SET lio_results_json=$1 WHERE id=$2",
            json.dumps(existing), lead_id,
        )


async def list_leads(
    limit: int = 50, offset: int = 0,
    org_id: Optional[str] = None,
    job_id: Optional[str] = None,
    min_score: Optional[int] = None,
    tier: Optional[str] = None,
    sort_by: Optional[str] = None,
    sort_dir: Optional[str] = None,
    q: Optional[str] = None,
) -> dict:
    _SORTABLE = {
        "score": "total_score",
        "name": "name",
        "company": "company",
        "enriched_at": "enriched_at",
        "created_at": "created_at",
    }
    sort_col = _SORTABLE.get(sort_by or "", "total_score")
    sort_order = "ASC" if (sort_dir or "").upper() == "ASC" else "DESC"
    clauses: list[str] = []
    params: list = []
    if org_id:
        params.append(org_id); clauses.append(f"organization_id=${len(params)}")
    if job_id:
        params.append(job_id); clauses.append(f"job_id=${len(params)}")
    if min_score is not None:
        params.append(min_score); clauses.append(f"total_score>=${len(params)}")
    if tier:
        params.append(tier); clauses.append(f"score_tier=${len(params)}")
    if q:
        params.append(f"%{q.lower()}%")
        n = len(params)
        clauses.append(f"(LOWER(name) LIKE ${n} OR LOWER(company) LIKE ${n})")
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params_page = params + [limit, offset]
    async with get_pool().acquire() as conn:
        total = await conn.fetchval(f"SELECT COUNT(*) FROM enriched_leads {where}", *params)
        rows = await conn.fetch(
            f"SELECT * FROM enriched_leads {where} ORDER BY {sort_col} {sort_order} NULLS LAST, enriched_at DESC LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}",
            *params_page,
        )
    return {"total": total, "leads": [dict(r) for r in rows]}


async def get_job(job_id: str, org_id: Optional[str] = None) -> Optional[dict]:
    async with get_pool().acquire() as conn:
        if org_id:
            row = await conn.fetchrow(
                "SELECT * FROM enrichment_jobs WHERE id=$1 AND organization_id=$2", job_id, org_id
            )
        else:
            row = await conn.fetchrow("SELECT * FROM enrichment_jobs WHERE id=$1", job_id)
        return dict(row) if row else None


async def list_jobs(limit: int = 20, org_id: Optional[str] = None) -> list[dict]:
    async with get_pool().acquire() as conn:
        if org_id:
            rows = await conn.fetch(
                "SELECT * FROM enrichment_jobs WHERE organization_id=$1 ORDER BY created_at DESC LIMIT $2",
                org_id, limit,
            )
        else:
            rows = await conn.fetch(
                "SELECT * FROM enrichment_jobs ORDER BY created_at DESC LIMIT $1", limit
            )
        return [dict(r) for r in rows]


async def delete_lead(lead_id: str) -> bool:
    async with get_pool().acquire() as conn:
        status = await conn.execute("DELETE FROM enriched_leads WHERE id=$1", lead_id)
    return status != "DELETE 0"


# ─────────────────────────────────────────────────────────────────────────────
# Duplicate Detection
# ─────────────────────────────────────────────────────────────────────────────

_CACHE_TTL_DAYS: int = int(os.getenv("LEAD_CACHE_TTL_DAYS", "90"))


async def check_existing_lead(linkedin_url: str, org_id: str) -> Optional[dict]:
    """
    Return (lead_row, is_stale) for an already-enriched lead, or None if not found.
    - is_stale=True  → enriched_at older than LEAD_CACHE_TTL_DAYS
    - is_stale=False → fresh cache hit
    """
    lead_id = _lead_id(_normalize_linkedin_url(linkedin_url))
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM enriched_leads WHERE id=$1 AND organization_id=$2",
            lead_id, org_id,
        )
    if not row:
        return None
    lead = dict(row)
    enriched_at = lead.get("enriched_at") or lead.get("created_at", "")
    stale = False
    try:
        from datetime import datetime, timezone, timedelta
        ea = datetime.fromisoformat(enriched_at.replace("Z", "+00:00"))
        stale = (datetime.now(timezone.utc) - ea) > timedelta(days=_CACHE_TTL_DAYS)
    except Exception:
        pass
    lead["_cache_hit"] = True
    lead["_stale"] = stale
    return lead


async def check_existing_leads_batch(linkedin_urls: list[str], org_id: str) -> dict[str, dict]:
    """
    Batch version of check_existing_lead — single DB query for N URLs.
    Returns a dict of {lead_id: lead_row} for all URLs already enriched for this org.
    Stale flag (_stale) is set per row same as check_existing_lead.
    """
    if not linkedin_urls:
        return {}
    from datetime import datetime, timezone, timedelta
    lead_ids = [_lead_id(_normalize_linkedin_url(u)) for u in linkedin_urls]
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM enriched_leads WHERE id = ANY($1) AND organization_id=$2",
            lead_ids, org_id,
        )
    result: dict[str, dict] = {}
    for row in rows:
        lead = dict(row)
        enriched_at = lead.get("enriched_at") or lead.get("created_at", "")
        stale = False
        try:
            ea = datetime.fromisoformat(str(enriched_at).replace("Z", "+00:00"))
            stale = (datetime.now(timezone.utc) - ea) > timedelta(days=_CACHE_TTL_DAYS)
        except Exception:
            pass
        lead["_cache_hit"] = True
        lead["_stale"] = stale
        result[lead["id"]] = lead
    return result


async def get_stale_leads(
    org_id: str,
    older_than_days: int = 90,
    tier: Optional[str] = None,
    min_score: int = 0,
    limit: int = 200,
) -> list[dict]:
    """
    Find leads older than `older_than_days` that are candidates for re-enrichment.
    """
    from datetime import datetime, timezone, timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(days=older_than_days)).isoformat()
    params: list = [org_id, cutoff]
    clauses = ["organization_id=$1", "enriched_at<$2"]
    if tier:
        params.append(tier); clauses.append(f"score_tier=${len(params)}")
    if min_score:
        params.append(min_score); clauses.append(f"total_score>=${len(params)}")
    max_limit = int(os.getenv("LEAD_REFRESH_LIMIT", "200"))
    limit = min(limit, max_limit)
    params.append(limit)
    where = "WHERE " + " AND ".join(clauses)
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            f"SELECT id, linkedin_url, enriched_at, total_score, score_tier "
            f"FROM enriched_leads {where} ORDER BY total_score DESC LIMIT ${len(params)}",
            *params,
        )
        return [dict(r) for r in rows]


# ─────────────────────────────────────────────────────────────────────────────
# Webhook Idempotency
# ─────────────────────────────────────────────────────────────────────────────

async def mark_snapshot_processed(snapshot_id: str, job_id: Optional[str], org_id: str) -> None:
    """Record that a snapshot has been processed (idempotency guard)."""
    async with get_pool().acquire() as conn:
        await conn.execute(
            "INSERT INTO processed_snapshots(snapshot_id, job_id, org_id) VALUES($1,$2,$3) ON CONFLICT(snapshot_id) DO NOTHING",
            snapshot_id, job_id, org_id,
        )


async def is_snapshot_processed(snapshot_id: str) -> bool:
    """Return True if this snapshot_id was already processed."""
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM processed_snapshots WHERE snapshot_id=$1", snapshot_id
        )
        return row is not None


# ─────────────────────────────────────────────────────────────────────────────
# Audit Logging
# ─────────────────────────────────────────────────────────────────────────────

async def audit_log(
    org_id: str,
    action: str,
    lead_id: Optional[str] = None,
    linkedin_url: Optional[str] = None,
    job_id: Optional[str] = None,
    user_email: Optional[str] = None,
    meta: Optional[dict] = None,
) -> None:
    """
    Write an audit log entry (fire-and-forget — never raises).

    Actions: enrich_single | bulk_submit | delete | re_enrich | refresh_scheduled
    """
    try:
        entry_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        async with get_pool().acquire() as conn:
            await conn.execute(
                """INSERT INTO enrichment_audit_log
                   (id, org_id, action, lead_id, linkedin_url, job_id, user_email, meta, created_at)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)""",
                entry_id, org_id, action, lead_id, linkedin_url,
                job_id, user_email,
                json.dumps(meta) if meta else None,
                now,
            )
    except Exception as e:
        logger.debug("[AuditLog] write failed: %s", e)


async def list_audit_log(
    org_id: str,
    action: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> dict:
    """List audit log entries for an org."""
    params: list = [org_id]
    clauses = ["org_id=$1"]
    if action:
        params.append(action); clauses.append(f"action=${len(params)}")
    where = "WHERE " + " AND ".join(clauses)
    params_page = params + [limit, offset]
    async with get_pool().acquire() as conn:
        total = await conn.fetchval(f"SELECT COUNT(*) FROM enrichment_audit_log {where}", *params)
        rows = await conn.fetch(
            f"SELECT * FROM enrichment_audit_log {where} ORDER BY created_at DESC LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}",
            *params_page,
        )
    rows = [dict(r) for r in rows]
    # Parse meta JSON
    for row in rows:
        if row.get("meta"):
            try:
                row["meta"] = json.loads(row["meta"])
            except Exception:
                pass
    return {"total": int(total), "entries": rows}


# ─────────────────────────────────────────────────────────────────────────────
# Lead Notes
# ─────────────────────────────────────────────────────────────────────────────

async def add_lead_note(lead_id: str, org_id: str, note: str) -> dict:
    """Add a text note to a lead. Returns the created note row."""
    note_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    async with get_pool().acquire() as conn:
        await conn.execute(
            "INSERT INTO lead_notes(id, lead_id, org_id, note, created_at) VALUES($1,$2,$3,$4,$5)",
            note_id, lead_id, org_id, note.strip(), now,
        )
        row = await conn.fetchrow("SELECT * FROM lead_notes WHERE id=$1", note_id)
        return dict(row)


async def list_lead_notes(lead_id: str, org_id: str) -> list[dict]:
    """Return all notes for a lead, newest first."""
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM lead_notes WHERE lead_id=$1 AND org_id=$2 ORDER BY created_at DESC",
            lead_id, org_id,
        )
        return [dict(r) for r in rows]


async def delete_lead_note(note_id: str, org_id: str) -> bool:
    """Delete a note by id (org-scoped for safety)."""
    async with get_pool().acquire() as conn:
        status = await conn.execute(
            "DELETE FROM lead_notes WHERE id=$1 AND org_id=$2", note_id, org_id
        )
    return status != "DELETE 0"


# ─────────────────────────────────────────────────────────────────────────────
# LLM caller helper
# ─────────────────────────────────────────────────────────────────────────────

async def _call_llm(
    messages: list[dict],
    max_tokens: int = 1800,
    temperature: float = 0.3,
    model_override: Optional[str] = None,
    groq_api_key: Optional[str] = None,
    wb_llm_host_override: Optional[str] = None,
    wb_llm_key_override: Optional[str] = None,
    wb_llm_model_override: Optional[str] = None,
    hf_first: bool = False,
) -> Optional[str]:
    """HuggingFace only — Qwen2.5-7B-Instruct (fast, low latency).
    3 retries with 3s/6s backoff on transient failures.
    Semaphore-limited to max 10 concurrent calls.
    """
    async def _post(base_url: str, api_key: str, model: str, msgs: list, timeout: int = 90) -> str:
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(
                f"{base_url.rstrip('/')}/v1/chat/completions",
                headers=headers,
                json={"model": model, "messages": msgs, "temperature": temperature, "max_tokens": max_tokens},
            )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"].strip()

    hf_token = _hf_token()
    if not hf_token:
        logger.warning("[LLM] No HuggingFace token configured")
        return None

    hf_model = _hf_model()  # Qwen/Qwen2.5-7B-Instruct from HF_MODEL env
    payload_chars = sum(len(m["content"]) for m in messages)

    async with _llm_semaphore:
        for attempt in range(3):
            try:
                logger.info("[LLM] HuggingFace (%s) attempt %d/3 payload=%d chars", hf_model, attempt + 1, payload_chars)
                result = await _post("https://router.huggingface.co", hf_token, hf_model, messages)
                logger.info("[LLM] HuggingFace OK — attempt %d", attempt + 1)
                return result
            except Exception as e:
                logger.warning("[LLM] HuggingFace failed (attempt %d/3): %s", attempt + 1, e)
                if attempt < 2:
                    await asyncio.sleep(3 * (attempt + 1))  # 3s, 6s

    logger.error("[LLM] All 3 HuggingFace attempts failed — returning None")
    return None


def _parse_json_from_llm(raw: str) -> dict:
    """
    Robustly extract a JSON object from an LLM response.

    Handles all common LLM output patterns in order:
      1. Strip <think>…</think> reasoning blocks (Qwen/DeepSeek models)
      2. Direct JSON parse (model returned clean JSON)
      3. Strip markdown fences (```json … ``` or ``` … ```)
      4. Extract first {...} block via regex
      5. Attempt lightweight repair (trailing commas, single quotes)
    Returns {} if all strategies fail.
    """
    if not raw or not isinstance(raw, str):
        return {}

    text = raw.strip()

    # 1. Strip <think>…</think> blocks produced by reasoning models
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()

    # 2. Try direct parse — model returned clean JSON
    try:
        result = json.loads(text)
        if isinstance(result, dict):
            return result
    except Exception:
        pass

    # 3. Strip markdown fences: ```json … ``` or ``` … ```
    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if fenced:
        try:
            result = json.loads(fenced.group(1))
            if isinstance(result, dict):
                return result
        except Exception:
            pass

    # 4. Extract outermost {...} block
    start = text.find("{")
    if start != -1:
        # Walk to find the matching closing brace
        depth = 0
        for i, ch in enumerate(text[start:], start):
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    candidate = text[start:i + 1]
                    try:
                        result = json.loads(candidate)
                        if isinstance(result, dict):
                            return result
                    except Exception:
                        pass
                    break

    # 5. Lightweight repair: trailing commas, single-quoted keys/values
    try:
        candidate = text[text.find("{"):] if "{" in text else text
        # Remove trailing commas before } or ]
        repaired = re.sub(r",\s*([}\]])", r"\1", candidate)
        # Replace single-quoted strings with double-quoted (simple cases)
        repaired = re.sub(r"'([^']*)'", r'"\1"', repaired)
        result = json.loads(repaired)
        if isinstance(result, dict):
            logger.warning("[LLM] JSON required repair — check model output quality")
            return result
    except Exception:
        pass

    logger.warning("[LLM] Could not parse JSON from model response (len=%d)", len(raw))
    return {}


# ─────────────────────────────────────────────────────────────────────────────
# Bright Data API
# ─────────────────────────────────────────────────────────────────────────────

def _bd_headers() -> dict:
    return {"Authorization": f"Bearer {_bd_api_key()}", "Content-Type": "application/json"}


def _normalize_linkedin_url(url: str) -> str:
    url = url.strip().rstrip("/")
    if not url.startswith("http"):
        url = f"https://{url}"
    # Normalise country-specific LinkedIn domains (in., nl., uk., de., etc.) → www.
    url = re.sub(r"https?://[a-z]{2}\.linkedin\.com", "https://www.linkedin.com", url)
    return url


def _clean_bd_linkedin_url(url: str) -> str:
    """
    Normalise a LinkedIn URL returned by BrightData to a canonical form.

    Handles every known BrightData variant so the URL always matches the
    lead_id key (MD5 of the lowercase normalised input URL):
      - Country-specific domains  mx./in./nl./de./… → www.linkedin.com
      - Mixed-case paths          Sumit-Singh → sumit-singh
      - Tracking query params     ?trk=… / ?originalSubdomain=… stripped
      - Trailing slashes          removed
    """
    if not url or not isinstance(url, str):
        return url or ""
    url = url.strip()
    # Strip tracking / query params from LinkedIn URLs only
    if "linkedin.com/" in url.lower():
        url = url.split("?")[0]
    # Normalise country-specific subdomains → www.linkedin.com
    url = re.sub(r"https?://[a-z]{2}\.linkedin\.com", "https://www.linkedin.com", url, flags=re.IGNORECASE)
    # Remove trailing slash
    url = url.rstrip("/")
    # Lowercase entire URL (LinkedIn paths are case-insensitive)
    url = url.lower()
    return url


def _normalize_bd_profile(raw: dict) -> dict:
    """
    Normalize a raw Bright Data person profile response to the flat dict
    shape the rest of the service expects.

    Handles both the nested and flat field naming variants that Bright Data
    returns depending on the dataset version, e.g.:
      - current_company.link  vs  current_company_link  (flat)
      - avatar                vs  avatar_url
      - educations_details    vs  education[]
      - activity[]            → extract emails + hiring signals
      - recommendations[]     → text list
    """
    p = dict(raw)  # shallow copy — we'll add/fix keys

    # ── current_company nested → flat ────────────────────────────────────────
    cc = p.get("current_company") or {}
    if isinstance(cc, dict):
        # Company link (the only field missing from flat response)
        if not p.get("current_company_link"):
            p["current_company_link"] = cc.get("link") or cc.get("url") or ""
        # Company name fallback
        if not p.get("current_company_name"):
            p["current_company_name"] = cc.get("name") or ""
        # Company logo sometimes in nested object
        if not p.get("current_company_logo"):
            p["current_company_logo"] = cc.get("logo") or cc.get("image") or ""
        # Company id
        if not p.get("current_company_company_id"):
            p["current_company_company_id"] = cc.get("company_id") or cc.get("id") or ""

    # ── Build company LinkedIn URL from company_id when link is missing ────────
    # BD returns company_id in current_company or experience[0] — use it to
    # construct a valid linkedin.com/company/{id} URL for the BD company dataset
    if not p.get("current_company_link") or "linkedin.com/company/" not in (p.get("current_company_link") or ""):
        co_id = p.get("current_company_company_id") or ""
        if not co_id:
            # Fall back to experience[0] company fields
            exp = p.get("experience") or []
            if exp and isinstance(exp[0], dict):
                first_exp = exp[0]
                co_id = (
                    first_exp.get("company_id") or first_exp.get("linkedin_company_id")
                    or first_exp.get("company_linkedin_id") or ""
                )
                # Some BD versions return a full URL on the experience entry
                exp_url = (
                    first_exp.get("url") or first_exp.get("company_url")
                    or first_exp.get("linkedin_url") or first_exp.get("company_linkedin_url") or ""
                )
                if exp_url and "linkedin.com/company/" in exp_url:
                    p["current_company_link"] = _clean_bd_linkedin_url(exp_url)
                    co_id = ""  # already set
        if co_id and not p.get("current_company_link"):
            p["current_company_link"] = f"https://www.linkedin.com/company/{co_id.lower()}/"

    # ── Avatar / profile photo ────────────────────────────────────────────────
    # BD returns "avatar" at top level; service uses "avatar_url"
    if not p.get("avatar_url"):
        p["avatar_url"] = p.get("avatar") or p.get("profile_image") or p.get("photo") or ""
    # Suppress generic LinkedIn placeholder — BD sets default_avatar: true when no real photo
    if p.get("default_avatar"):
        p["avatar_url"] = ""

    # ── Banner image ─────────────────────────────────────────────────────────
    if not p.get("banner_image"):
        p["banner_image"] = p.get("background_image") or p.get("cover_image") or ""

    # ── Education: string form → structured list ──────────────────────────────
    # BD sometimes returns educations_details as a plain string instead of education[]
    edu_raw = p.get("education")
    edu_str = p.get("educations_details") or p.get("education_details") or ""

    if edu_raw and isinstance(edu_raw, list):
        # Normalize key variants BD may use
        def _edu_year(val: str) -> str:
            """BD sometimes returns "2006-08" (month-year ISO); keep only the 4-digit year."""
            if val and isinstance(val, str):
                return val[:4]
            return val or ""

        normalized_edu = []
        for e in edu_raw:
            if not isinstance(e, dict):
                continue
            # BD uses "title" for school name in some dataset versions
            school = (e.get("school") or e.get("institution") or e.get("university")
                      or e.get("name") or e.get("title") or "")
            # When "title" is the school name, "degree" comes from description or field_of_study
            degree = (e.get("degree") or e.get("field_of_study") or e.get("description") or "")
            # Only fall back to "title" as degree if school was found from a non-title key
            if not degree and not (e.get("school") or e.get("institution") or e.get("university") or e.get("name")):
                degree = ""  # title was used for school — don't double-use it
            start = _edu_year(e.get("start_year") or e.get("start") or "")
            end   = _edu_year(e.get("end_year")   or e.get("end")   or "")
            years = (e.get("years") or e.get("date_range") or e.get("dates")
                     or (f"{start}–{end}".strip("–") if start or end else ""))
            normalized_edu.append({"school": school, "degree": degree, "years": years})
        p["education"] = [e for e in normalized_edu if e["school"]]
    elif not edu_raw and edu_str and isinstance(edu_str, str):
        # Parse plain string — split on semicolons (multiple schools)
        p["education"] = [{"school": part.strip(), "degree": "", "years": ""}
                          for part in edu_str.split(";") if part.strip()]

    # ── Languages: normalize BD's title/subtitle keys ───────────────────────���
    langs_raw = p.get("languages") or []
    if langs_raw and isinstance(langs_raw, list):
        normalized_langs = []
        for lang in langs_raw:
            if isinstance(lang, dict):
                normalized_langs.append({
                    "name":        lang.get("name") or lang.get("title") or lang.get("language") or "",
                    "proficiency": lang.get("proficiency") or lang.get("subtitle") or lang.get("level") or "",
                })
            elif isinstance(lang, str) and lang.strip():
                normalized_langs.append({"name": lang.strip(), "proficiency": ""})
        p["languages"] = [l for l in normalized_langs if l["name"]]

    # ── Certifications: normalize BD's title/subtitle/meta keys ──────────────
    certs_raw = p.get("certifications") or []
    if certs_raw and isinstance(certs_raw, list):
        normalized_certs = []
        for cert in certs_raw:
            if isinstance(cert, dict):
                normalized_certs.append({
                    "name":           cert.get("name") or cert.get("title") or "",
                    "issuer":         cert.get("issuer") or cert.get("subtitle") or cert.get("organization") or "",
                    "date":           cert.get("date") or cert.get("meta") or cert.get("issued_at") or "",
                    "credential_url": cert.get("credential_url") or cert.get("url") or "",
                    "credential_id":  cert.get("credential_id") or "",
                })
            elif isinstance(cert, str) and cert.strip():
                normalized_certs.append({"name": cert.strip(), "issuer": "", "date": "", "credential_url": "", "credential_id": ""})
        p["certifications"] = [c for c in normalized_certs if c["name"]]

    # ── Skills: normalize + infer when BD doesn't return them ────────────────
    skills = p.get("skills")

    # BD sometimes returns list of dicts: [{"name": "Python"}] — flatten to strings
    if skills and isinstance(skills, list):
        flat = []
        for s in skills:
            if isinstance(s, dict):
                flat.append(s.get("name") or s.get("skill") or s.get("title") or "")
            elif isinstance(s, str):
                flat.append(s)
        skills = [s.strip() for s in flat if s.strip()]
        p["skills"] = skills

    # Try comma-string fallbacks
    if not skills:
        skills_str = p.get("skills_label") or p.get("top_skills") or ""
        if skills_str and isinstance(skills_str, str):
            skills = [s.strip() for s in skills_str.split(",") if s.strip()]
            p["skills"] = skills

    # Infer from certifications when still empty
    if not skills:
        inferred = []
        for cert in (p.get("certifications") or []):
            if isinstance(cert, dict):
                name = cert.get("name") or ""
                if name and len(name) < 60:
                    inferred.append(name)
        if inferred:
            skills = inferred[:6]
            p["skills"] = skills

    # Infer from about text via keyword matching
    if not skills:
        about_lower = (p.get("about") or "").lower()
        skill_keywords = [
            ("roofing", "Roofing"), ("construction", "Construction"), ("project management", "Project Management"),
            ("python", "Python"), ("javascript", "JavaScript"), ("react", "React"),
            ("aws", "AWS"), ("azure", "Azure"), ("machine learning", "Machine Learning"),
            ("data analysis", "Data Analysis"), ("sales", "Sales"), ("marketing", "Marketing"),
            ("leadership", "Leadership"), ("finance", "Finance"), ("accounting", "Accounting"),
            ("design", "Design"), ("product management", "Product Management"),
            ("software engineering", "Software Engineering"), ("devops", "DevOps"),
            ("cybersecurity", "Cybersecurity"), ("hr", "Human Resources"),
            ("operations", "Operations Management"), ("supply chain", "Supply Chain"),
        ]
        inferred = [label for kw, label in skill_keywords if kw in about_lower]
        if inferred:
            p["skills"] = inferred[:8]

    # ── Timezone: derive from country_code ────────────────────────────────────
    if not p.get("timezone"):
        _TZ_MAP = {
            "IN": "Asia/Kolkata",      "US": "America/New_York",  "GB": "Europe/London",
            "DE": "Europe/Berlin",     "FR": "Europe/Paris",      "AU": "Australia/Sydney",
            "CA": "America/Toronto",   "SG": "Asia/Singapore",    "AE": "Asia/Dubai",
            "JP": "Asia/Tokyo",        "CN": "Asia/Shanghai",     "BR": "America/Sao_Paulo",
            "MX": "America/Mexico_City","NL": "Europe/Amsterdam", "SE": "Europe/Stockholm",
            "NO": "Europe/Oslo",       "DK": "Europe/Copenhagen", "FI": "Europe/Helsinki",
            "PL": "Europe/Warsaw",     "IT": "Europe/Rome",       "ES": "Europe/Madrid",
            "RU": "Europe/Moscow",     "ZA": "Africa/Johannesburg","NG": "Africa/Lagos",
            "KE": "Africa/Nairobi",    "EG": "Africa/Cairo",      "PK": "Asia/Karachi",
            "BD": "Asia/Dhaka",        "LK": "Asia/Colombo",      "NP": "Asia/Kathmandu",
            "TH": "Asia/Bangkok",      "VN": "Asia/Ho_Chi_Minh",  "MY": "Asia/Kuala_Lumpur",
            "ID": "Asia/Jakarta",      "PH": "Asia/Manila",       "KR": "Asia/Seoul",
            "HK": "Asia/Hong_Kong",    "TW": "Asia/Taipei",       "IL": "Asia/Jerusalem",
            "SA": "Asia/Riyadh",       "TR": "Europe/Istanbul",   "AR": "America/Argentina/Buenos_Aires",
            "CO": "America/Bogota",    "PE": "America/Lima",      "CL": "America/Santiago",
            "NZ": "Pacific/Auckland",
        }
        p["timezone"] = _TZ_MAP.get(p.get("country_code") or p.get("country") or "", "")

    # ── Position: multi-strategy inference when BD doesn't return headline ──────
    if not p.get("position"):
        about_text = p.get("about") or ""
        candidate  = ""

        # Strategy 1: "I'm a/an/the <title> at/with/of..."
        m = re.match(
            r"^I(?:'m| am) (?:a |an |the )(.+?)(?:\s+with\b|\s+at\b|\s+of\b|\.|,|–|-)",
            about_text, re.IGNORECASE,
        )
        if m:
            candidate = m.group(1).strip().rstrip("and").strip()

        # Strategy 2: "<Title> at <Company>" at start of about
        if not candidate:
            m = re.match(
                r"^([A-Z][^.!?\n]{3,60}?)\s+(?:at|@)\s+[A-Z]",
                about_text,
            )
            if m:
                candidate = m.group(1).strip()

        # Strategy 3: "<Title> specializing/focusing/with X years" — e.g. "Construction operator specializing in..."
        if not candidate:
            m = re.match(
                r"^([A-Z][^.!?\n]{3,60}?)\s+(?:specializ|focus|with \d|based in|helping|who )",
                about_text, re.IGNORECASE,
            )
            if m:
                candidate = m.group(1).strip()

        # Strategy 4: First sentence of about if it looks like a title (short, no verb)
        if not candidate and about_text:
            first_sentence = re.split(r"[.!?\n]", about_text)[0].strip()
            if 5 < len(first_sentence) < 60 and not re.search(r"\b(I |we |our |you )", first_sentence, re.IGNORECASE):
                candidate = first_sentence

        # Strategy 5: most-recent experience title
        if not candidate:
            exp = p.get("experience") or []
            if isinstance(exp, list) and exp:
                first_exp = exp[0] if isinstance(exp[0], dict) else {}
                candidate = first_exp.get("title") or first_exp.get("position") or ""

        if candidate and 3 < len(candidate) < 80:
            p["position"] = candidate

        # Final fallback: explicit headline key variants
        if not p.get("position"):
            p["position"] = p.get("headline") or p.get("bio") or p.get("title") or p.get("job_title") or ""

    # ── Honors & Awards → merge into certifications ──────────────────────────
    honors = p.get("honors_and_awards") or []
    if honors and isinstance(honors, list):
        existing_certs = p.get("certifications") or []
        existing_names = {c.get("name", "").lower() for c in existing_certs if isinstance(c, dict)}
        for h in honors:
            if not isinstance(h, dict):
                continue
            title = h.get("title") or ""
            if not title or title.lower() in existing_names:
                continue
            date_raw = h.get("date") or ""
            # Trim ISO date "2022-08-01T00:00:00.000Z" → "2022"
            date_str = date_raw[:4] if date_raw else ""
            existing_certs.append({
                "name":           title,
                "issuer":         h.get("publication") or h.get("issuer") or "",
                "date":           date_str,
                "credential_url": h.get("url") or "",
                "credential_id":  "",
            })
        p["certifications"] = existing_certs

    # ── Posts (own authored posts — separate from activity[]) ────────────────
    posts = p.get("posts") or []
    if posts and isinstance(posts, list):
        # Store own posts separately with all fields for UI display
        p["_posts"] = [
            {
                "title": (post.get("title") or "")[:300],
                "attribution": (post.get("attribution") or "")[:500],
                "link": post.get("link") or "",
                "created_at": post.get("created_at") or "",
                "interaction": post.get("interaction") or "",
                "id": post.get("id") or "",
            }
            for post in posts if isinstance(post, dict)
        ]
        # Also merge into activity for email/phone/hiring extraction
        existing_ids = {a.get("id") for a in (p.get("activity") or []) if isinstance(a, dict)}
        for post in posts:
            if not isinstance(post, dict):
                continue
            if post.get("id") in existing_ids:
                continue
            synthetic = {
                "interaction": "Posted",
                "title": (post.get("title") or "")[:300],
                "attribution": (post.get("attribution") or "")[:500],
                "link": post.get("link") or "",
                "created_at": post.get("created_at") or "",
                "id": post.get("id") or "",
            }
            if not p.get("activity"):
                p["activity"] = []
            p["activity"].append(synthetic)
    else:
        p["_posts"] = []

    # ── Activity: extract emails + phones + hiring signals ────────────────────
    activity = p.get("activity") or []
    extracted_emails: list[str] = []
    extracted_phones: list[str] = []
    hiring_posts: list[str] = []
    email_re = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
    phone_re  = re.compile(r"(?<!\d)(\+?[\d][\d\s\-]{6,13}[\d])(?!\d)")

    for item in activity:
        if not isinstance(item, dict):
            continue
        text = (item.get("title") or "") + " " + (item.get("description") or "")
        # Extract emails visible in post text
        for em in email_re.findall(text):
            em_lower = em.lower()
            if em_lower not in extracted_emails and "linkedin.com" not in em_lower:
                extracted_emails.append(em_lower)
        # Extract phone numbers from post text
        for ph in phone_re.findall(text):
            cleaned = re.sub(r"[\s\-]", "", ph)
            if 7 <= len(cleaned.lstrip("+")) <= 15 and cleaned not in extracted_phones:
                extracted_phones.append(cleaned)
        # Flag hiring activity
        if any(kw in text.lower() for kw in ["hiring", "we're looking", "open position", "job opening", "apply"]):
            title_snip = (item.get("title") or "")[:120]
            if title_snip:
                hiring_posts.append(title_snip)

    if extracted_emails:
        p["_activity_emails"] = extracted_emails  # stored for Phase C waterfall
        logger.info("[BDNorm] Activity emails found: %s", extracted_emails)
    if extracted_phones:
        p["_activity_phones"] = extracted_phones
        logger.info("[BDNorm] Activity phones found: %s", extracted_phones)
    if hiring_posts:
        p["_hiring_signals"] = hiring_posts[:10]

    # ── Recommendations ───────────────────────────────────────────────────────
    # BD may send "recommendations" as a list of objects or strings.
    # Each object can have: text, description, recommender_name, recommender_headline,
    # recommender_url, date — depending on BD dataset version.
    recs = (
        p.get("recommendations")
        or p.get("received_recommendations")
        or p.get("testimonials")
        or []
    )
    if recs and isinstance(recs, list):
        def _rec_text(r):
            if isinstance(r, str):
                return r[:300]
            if isinstance(r, dict):
                return (
                    r.get("text") or r.get("description") or r.get("recommendation_text")
                    or r.get("content") or ""
                )[:300]
            return ""

        p["recommendations"] = [
            {
                "recommender_name": (r.get("recommender_name") or r.get("author_name") or r.get("name") or "") if isinstance(r, dict) else "",
                "recommender_headline": (r.get("recommender_headline") or r.get("author_headline") or r.get("title") or "") if isinstance(r, dict) else "",
                "recommender_url": (r.get("recommender_url") or r.get("profile_link") or r.get("url") or "") if isinstance(r, dict) else "",
                "recommender_image": (r.get("recommender_image") or r.get("profile_image_url") or r.get("image") or "") if isinstance(r, dict) else "",
                "text": _rec_text(r),
                "date": (r.get("date") or r.get("recommendation_date") or "") if isinstance(r, dict) else "",
                "relationship": (r.get("relationship") or r.get("connection_type") or "") if isinstance(r, dict) else "",
            }
            for r in recs if r
        ]
        p["recommendations_text"] = " | ".join(
            r["text"] for r in p["recommendations"] if r["text"]
        )[:600]
        if not p.get("recommendations_count"):
            p["recommendations_count"] = len(p["recommendations"])
    else:
        if not p.get("recommendations"):
            p["recommendations"] = []
        p["recommendations_count"] = _safe_int(p.get("recommendations_count"))

    # ── People also viewed → similar leads ───────────────────────────────────
    # BD dataset uses "similar_profiles"; some versions use "people_also_viewed"
    pav = p.get("people_also_viewed") or p.get("similar_profiles") or []
    if pav and isinstance(pav, list):
        p["_similar_profiles"] = [
            {
                "name": x.get("name", ""),
                "title": x.get("title") or x.get("headline") or x.get("position") or "",
                "link": x.get("profile_link") or x.get("url") or x.get("link") or "",
                "image": x.get("profile_image_url") or x.get("image") or x.get("profile_image") or "",
                "location": x.get("location", ""),
                "degree": x.get("degree") or x.get("connection_degree") or "",
            }
            for x in pav if isinstance(x, dict)
        ]

    # ── Activity: store full list for UI display ──────────────────────────────
    if activity:
        p["_activity_full"] = [
            {
                "interaction": item.get("interaction", ""),
                "title": (item.get("title") or "")[:300],
                "attribution": (item.get("attribution") or "")[:500],
                "link": item.get("link", ""),
                "created_at": item.get("created_at", ""),
                "id": item.get("id", ""),
                # Mark authored posts so UI can distinguish from liked/commented activity
                "type": "post" if (item.get("interaction") or "").lower().startswith("post") else "activity",
            }
            for item in activity if isinstance(item, dict)
        ]

    # ── Profile flags ─────────────────────────────────────────────────────────
    p["_influencer"]           = bool(p.get("influencer"))
    p["_memorialized_account"] = bool(p.get("memorialized_account"))
    p["_bio_links"]            = p.get("bio_links") or []
    p["_bd_scrape_timestamp"]  = p.get("timestamp") or ""

    # ── Followers / connections ───────────────────────────────────────────────
    p["followers"]   = _safe_int(p.get("followers"))
    p["connections"] = _safe_int(p.get("connections"))

    # ── Name split ────────────────────────────────────────────────────────────
    # BD sometimes returns full_name instead of name — normalise here
    if not p.get("name") and p.get("full_name"):
        p["name"] = p["full_name"]
    if not p.get("first_name") and p.get("name"):
        parts = p["name"].split()
        p["first_name"] = parts[0] if parts else ""
        p["last_name"] = " ".join(parts[1:]) if len(parts) > 1 else ""

    # ── Position / headline: final safety net ────────────────────────────────
    if not p.get("position"):
        exp = p.get("experience") or []
        exp_title = ""
        if isinstance(exp, list) and exp and isinstance(exp[0], dict):
            exp_title = exp[0].get("title") or exp[0].get("position") or ""
        p["position"] = (
            p.get("headline") or p.get("bio") or p.get("job_title")
            or exp_title or ""
        )
    # Mark profile for async LLM title inference if still empty
    # (actual LLM call happens in build_comprehensive_enrichment — sync normalise can't await)
    if not p.get("position"):
        p["_needs_title_inference"] = True

    # ── Location: city may be "City, State, Country" string ──────────────────
    city_raw = p.get("city") or p.get("location") or ""
    if city_raw and not p.get("location"):
        p["location"] = city_raw
    # Trim city to first segment only (BD often sends "City, State, Country")
    if p.get("city") and "," in p["city"]:
        p["city"] = p["city"].split(",")[0].strip()
    # Country from country_code if country missing; map code to full name
    if not p.get("country"):
        p["country"] = p.get("country_code", "")

    # ── LinkedIn URL normalization (all URL fields from BD response) ──────────
    # Applies _clean_bd_linkedin_url to every field that may carry a LinkedIn
    # URL so that country domains, mixed-case paths, and tracking params are
    # all normalised consistently — regardless of which BD dataset version
    # returned the response.
    for url_field in ("url", "input_url", "linkedin_url"):
        v = p.get(url_field) or ""
        if v and "linkedin.com" in v.lower():
            p[url_field] = _clean_bd_linkedin_url(v)

    # Also clean the company link (may carry ?trk=… from profile topcard)
    if p.get("current_company_link") and "linkedin.com" in p["current_company_link"].lower():
        p["current_company_link"] = _clean_bd_linkedin_url(p["current_company_link"])

    return p


async def fetch_profile_sync(linkedin_url: str) -> dict:
    """
    Primary: Bright Data sync /scrape endpoint → normalize with _normalize_bd_profile.
    Handles rate limits (429), circuit breaker, and falls back to OCR on failure.
    """
    url = _normalize_linkedin_url(linkedin_url)

    if _bd_api_key():
        if not _bd_circuit_ok():
            logger.warning("[BrightData] Circuit breaker OPEN — skipping BD, using OCR fallback")
            return await _extract_profile_via_ocr(url)

        logger.info("[BrightData] Sync scrape: %s", url)
        try:
            async with httpx.AsyncClient(timeout=BD_REQUEST_TIMEOUT) as client:
                resp = await client.post(
                    f"{BD_BASE}/scrape",
                    params={"dataset_id": _bd_profile_dataset(), "format": "json"},
                    headers=_bd_headers(),
                    json=[{"url": url}],
                )

                if resp.status_code == 429:
                    wait = _bd_rate_limit_wait(resp)
                    logger.warning("[BrightData] Sync rate-limited — waiting %.1fs before OCR fallback", wait)
                    _bd_circuit_failure()
                    await asyncio.sleep(wait)
                    return await _extract_profile_via_ocr(url)

                if resp.status_code in (400, 401, 402, 403):
                    # Account/auth error — do NOT fall back to OCR, abort cleanly
                    err_text = resp.text[:200]
                    logger.error("[BrightData] Account/auth error %s: %s — aborting",
                                 resp.status_code, err_text)
                    _bd_circuit_failure()
                    return {"_bd_error": True, "_bd_status": resp.status_code, "_bd_message": err_text}

                if resp.status_code >= 500:
                    logger.warning("[BrightData] Server error %s — trying OCR fallback", resp.status_code)
                    _bd_circuit_failure()
                    return await _extract_profile_via_ocr(url)

                if resp.status_code == 200:
                    data = resp.json()

                    # BrightData returns snapshot_id when sync cannot be fulfilled
                    # immediately (API tier or profile takes too long) — poll async.
                    if isinstance(data, dict) and data.get("snapshot_id") and not data.get("name"):
                        snap_id = data["snapshot_id"]
                        logger.info("[BrightData] Sync returned snapshot_id=%s — polling async", snap_id)
                        try:
                            rows = await poll_snapshot(snap_id, interval=5, timeout=120)
                            data = rows
                        except Exception as poll_err:
                            logger.warning("[BrightData] Snapshot poll failed (%s) — trying OCR fallback", poll_err)
                            data = []

                    raw = data[0] if isinstance(data, list) and data else data
                    if isinstance(raw, dict) and (
                        raw.get("name") or raw.get("full_name")
                        or raw.get("first_name") or raw.get("linkedin_url")
                    ):
                        if not raw.get("name") and raw.get("full_name"):
                            raw["name"] = raw["full_name"]
                        profile = _normalize_bd_profile(raw)
                        _bd_circuit_success()
                        logger.info("[BrightData] OK — %s | company=%s | avatar=%s | activity=%d",
                                    profile.get("name"),
                                    profile.get("current_company_name") or "?",
                                    "✓" if profile.get("avatar_url") else "✗",
                                    len(raw.get("activity") or []))
                        return profile

                    logger.warning("[BrightData] Empty profile — trying OCR fallback")
                else:
                    logger.warning("[BrightData] Unexpected %s — trying OCR fallback", resp.status_code)

        except (httpx.TimeoutException, httpx.ConnectError) as e:
            logger.warning("[BrightData] Network error (%s) — trying OCR fallback", e)
            _bd_circuit_failure()
        except Exception as e:
            logger.warning("[BrightData] Error: %s — trying OCR fallback", e)
    else:
        logger.info("[BrightData] No API key — using OCR fallback")

    return await _extract_profile_via_ocr(url)


async def _extract_profile_via_ocr(linkedin_url: str) -> dict:
    """Playwright screenshot → OCR → LLM structured extraction."""
    try:
        from lead_enrichment_service import capture_screenshot, extract_text_via_ocr
    except ImportError:
        logger.error("[OCR] lead_enrichment_service not found")
        return {"url": linkedin_url}

    png_bytes = await capture_screenshot(linkedin_url, timeout_ms=40_000)
    if not png_bytes:
        logger.error("[OCR] Screenshot failed: %s", linkedin_url)
        return {"url": linkedin_url}

    ocr_text, engine = extract_text_via_ocr(png_bytes)
    if not ocr_text.strip():
        logger.warning("[OCR] Empty text (engine: %s)", engine)
        return {"url": linkedin_url}

    logger.info("[OCR] %d chars via %s", len(ocr_text), engine)
    profile = await _llm_extract_profile_from_ocr(linkedin_url, ocr_text)
    profile["_source"] = f"ocr_{engine}"
    profile["url"] = linkedin_url
    return profile


async def _llm_extract_profile_from_ocr(linkedin_url: str, ocr_text: str) -> dict:
    snippet = ocr_text[:4500]
    prompt = f"""Extract LinkedIn profile data from this OCR text.
LinkedIn URL: {linkedin_url}

OCR TEXT:
---
{snippet}
---

Return ONLY valid JSON with these exact keys (use empty string or 0 if not found):
{{
  "name": "", "first_name": "", "last_name": "",
  "position": "", "current_company_name": "",
  "location": "", "city": "", "country": "",
  "about": "",
  "followers": 0, "connections": 0,
  "skills": [],
  "experience": [{{"title":"","company":"","duration":""}}],
  "education": [{{"school":"","degree":"","years":""}}],
  "certifications": [], "languages": []
}}"""

    raw = await _call_llm([
        {"role": "system", "content": "Precise JSON extractor. Return only valid JSON."},
        {"role": "user", "content": prompt},
    ], max_tokens=900, temperature=0)

    if raw:
        result = _parse_json_from_llm(raw)
        if result:
            return result

    # Best-effort without LLM
    lines = [l.strip() for l in ocr_text.split("\n") if l.strip() and len(l.strip()) > 3]
    return {"name": lines[0] if lines else "", "about": ocr_text[:500]}


async def trigger_batch_snapshot(
    urls: list[str],
    webhook_url: Optional[str] = None,
    notify_url: Optional[str] = None,
    webhook_auth: Optional[str] = None,
) -> str:
    if not _bd_api_key():
        raise ValueError("BRIGHT_DATA_API_KEY not configured")
    if not _bd_circuit_ok():
        raise RuntimeError("BD circuit breaker OPEN — skipping trigger")

    params: dict[str, str] = {
        "dataset_id": _bd_profile_dataset(), "format": "json", "include_errors": "true",
    }
    if webhook_url:
        params["endpoint"] = webhook_url; params["uncompressed_webhook"] = "true"
    if notify_url:
        params["notify"] = notify_url
    if webhook_auth:
        params["auth_header"] = webhook_auth

    payload = [{"url": _normalize_linkedin_url(u)} for u in urls]
    last_exc: Exception = RuntimeError("No attempts made")

    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=60) as client:
                resp = await client.post(
                    f"{BD_BASE}/trigger", params=params,
                    headers=_bd_headers(), json=payload,
                )
            if resp.status_code == 429:
                wait = _bd_rate_limit_wait(resp)
                logger.warning("[BD] trigger rate-limited — waiting %.1fs (attempt %d/3)", wait, attempt + 1)
                _bd_circuit_failure()
                await asyncio.sleep(wait)
                continue
            if resp.status_code >= 500:
                wait = _bd_backoff(attempt)
                logger.warning("[BD] trigger %d — retrying in %.1fs (attempt %d/3)", resp.status_code, wait, attempt + 1)
                _bd_circuit_failure()
                await asyncio.sleep(wait)
                continue
            resp.raise_for_status()
            snapshot_id = resp.json().get("snapshot_id") or resp.json().get("id")
            if not snapshot_id:
                raise ValueError(f"No snapshot_id in response: {resp.text[:200]}")
            _bd_circuit_success()
            return snapshot_id
        except (httpx.TimeoutException, httpx.ConnectError) as e:
            wait = _bd_backoff(attempt)
            logger.warning("[BD] trigger network error (%s) — retrying in %.1fs (attempt %d/3)", e, wait, attempt + 1)
            _bd_circuit_failure()
            last_exc = e
            await asyncio.sleep(wait)

    raise RuntimeError(f"trigger_batch_snapshot failed after 3 attempts: {last_exc}") from last_exc


async def poll_snapshot(snapshot_id: str, interval: int = 15, timeout: int = 600) -> list[dict]:
    """
    Poll BrightData until snapshot is ready, with exponential backoff on errors,
    rate-limit handling, partial-response filtering, and circuit-breaker integration.
    """
    if not _bd_api_key():
        raise ValueError("BRIGHT_DATA_API_KEY not configured")
    if not _bd_circuit_ok():
        raise RuntimeError("BD circuit breaker OPEN — skipping poll")

    deadline    = time.time() + timeout
    poll_errors = 0
    MAX_POLL_ERRORS = 5

    async with httpx.AsyncClient(timeout=60) as client:
        while time.time() < deadline:
            # ── Progress check ────────────────────────────────────────────────
            try:
                s = await client.get(f"{BD_BASE}/progress/{snapshot_id}", headers=_bd_headers())
            except (httpx.TimeoutException, httpx.ConnectError) as e:
                poll_errors += 1
                wait = _bd_backoff(poll_errors - 1)
                logger.warning("[BD] poll network error (%s) — waiting %.1fs (%d/%d)", e, wait, poll_errors, MAX_POLL_ERRORS)
                _bd_circuit_failure()
                if poll_errors >= MAX_POLL_ERRORS:
                    raise RuntimeError(f"poll_snapshot {snapshot_id}: too many network errors") from e
                await asyncio.sleep(wait)
                continue

            if s.status_code == 429:
                wait = _bd_rate_limit_wait(s)
                logger.warning("[BD] poll rate-limited — waiting %.1fs", wait)
                await asyncio.sleep(wait)
                continue

            if s.status_code >= 500:
                poll_errors += 1
                wait = _bd_backoff(poll_errors - 1)
                logger.warning("[BD] poll %d — waiting %.1fs (%d/%d)", s.status_code, wait, poll_errors, MAX_POLL_ERRORS)
                _bd_circuit_failure()
                if poll_errors >= MAX_POLL_ERRORS:
                    raise RuntimeError(f"poll_snapshot {snapshot_id}: too many server errors")
                await asyncio.sleep(wait)
                continue

            s.raise_for_status()
            poll_errors = 0   # reset on clean response
            _bd_circuit_success()

            state = s.json().get("status") or s.json().get("state", "")

            if state == "ready":
                # ── Download snapshot ─────────────────────────────────────────
                try:
                    d = await client.get(
                        f"{BD_BASE}/snapshot/{snapshot_id}",
                        params={"format": "json"}, headers=_bd_headers(),
                    )
                except (httpx.TimeoutException, httpx.ConnectError) as e:
                    logger.warning("[BD] snapshot download error (%s) — retrying poll", e)
                    _bd_circuit_failure()
                    await asyncio.sleep(_bd_backoff(0))
                    continue

                if d.status_code == 429:
                    wait = _bd_rate_limit_wait(d)
                    logger.warning("[BD] snapshot download rate-limited — waiting %.1fs", wait)
                    await asyncio.sleep(wait)
                    continue

                d.raise_for_status()
                _bd_circuit_success()

                raw = d.json()
                rows: list[dict] = raw if isinstance(raw, list) else [raw]

                # ── Partial-response handling ─────────────────────────────────
                # BD may include per-item error objects alongside valid profiles.
                ok_rows = [r for r in rows if isinstance(r, dict) and not r.get("error") and not r.get("_error")]
                err_rows = [r for r in rows if r not in ok_rows]
                if err_rows:
                    logger.warning("[BD] snapshot %s: %d/%d items had errors: %s",
                                   snapshot_id, len(err_rows), len(rows),
                                   [r.get("error") or r.get("_error") for r in err_rows[:3]])
                if not ok_rows and err_rows:
                    # All items errored (e.g. private/hidden profiles) — return error rows
                    # so _process_one_webhook_profile can retry each one individually
                    # and mark them as failed gracefully instead of crashing the whole job.
                    logger.warning("[BD] snapshot %s: all items errored — returning to pipeline for per-lead retry", snapshot_id)
                    return err_rows

                return ok_rows if ok_rows else rows

            if state in ("failed", "error"):
                _bd_circuit_failure()
                raise RuntimeError(f"Snapshot {snapshot_id} failed (state={state})")

            # Still processing — backoff grows with elapsed time to avoid hammering
            elapsed = time.time() - (deadline - timeout)
            wait = min(interval + elapsed * 0.05, interval * 3)
            await asyncio.sleep(wait)

    raise TimeoutError(f"Snapshot {snapshot_id} not ready after {timeout}s")


# ─────────────────────────────────────────────────────────────────────────────
# Email finding waterfall
# ─────────────────────────────────────────────────────────────────────────────

_SOCIAL_DOMAINS = {"linkedin.com", "facebook.com", "twitter.com", "x.com", "instagram.com", "youtube.com"}

def _extract_domain(company_name: str, website: str = "") -> str:
    """Extract company domain. Skips LinkedIn/social URLs — those are not the company website."""
    if website:
        m = re.search(r"https?://(?:www\.)?([^/?\s]+)", website)
        if m:
            host = m.group(1).lower()
            # Skip social network domains — these are NOT the company website
            if not any(host == s or host.endswith("." + s) for s in _SOCIAL_DOMAINS):
                return host
    # Guess from first significant word of company name
    words = re.sub(r"\s+", " ", (company_name or "").strip()).split()
    for word in words:
        slug = re.sub(r"[^a-z0-9]", "", word.lower())
        if slug and len(slug) >= 3 and slug not in {"the", "inc", "ltd", "llc", "pvt", "private", "limited", "solutions", "services", "technologies", "tech", "group", "and", "for"}:
            return f"{slug}.com"
    return ""


async def _try_hunter(first: str, last: str, domain: str) -> Optional[dict]:
    if not _hunter_api_key() or not domain:
        return None
    try:
        c = _get_api_client()
        r = await c.get("https://api.hunter.io/v2/email-finder", params={
            "domain": domain, "first_name": first, "last_name": last, "api_key": _hunter_api_key(),
        })
        d = r.json().get("data", {})
        if d.get("email") and d.get("score", 0) >= 50:
            logger.info("[Hunter] Found: %s (score=%s)", d["email"], d["score"])
            return {"email": d["email"], "source": "hunter", "confidence": "high" if d["score"] >= 80 else "medium"}
    except Exception as e:
        logger.warning("[Hunter] %s", e)
    return None


async def _try_apollo(first: str, last: str, domain: str, linkedin_url: str = "") -> Optional[dict]:
    if not _apollo_api_key() or (not domain and not linkedin_url):
        return None
    try:
        payload: dict = {"first_name": first, "last_name": last, "reveal_personal_emails": True}
        if domain:
            payload["domain"] = domain
        if linkedin_url:
            payload["linkedin_url"] = linkedin_url
        c = _get_api_client()
        r = await c.post("https://api.apollo.io/v1/people/match",
            headers={"Content-Type": "application/json", "X-Api-Key": _apollo_api_key()},
            json=payload,
        )
        body = r.json()
        p   = body.get("person") or {}
        org = body.get("organization") or {}
        email   = p.get("email")
        phone   = p.get("phone") or (p.get("phone_numbers") or [{}])[0].get("sanitized_number")
        photo   = p.get("photo_url")
        twitter = p.get("twitter_url") or p.get("twitter")
        if email and "@" in email and "placeholder" not in email.lower():
            logger.info("[Apollo] Found: %s", email)
            # Extract company LinkedIn URL from Apollo org — propagates into company_extras
            _org_linkedin = org.get("linkedin_url") or ""
            if _org_linkedin and "linkedin.com/company/" not in _org_linkedin:
                _org_linkedin = ""
            return {
                "email":            email,
                "phone":            phone,
                "source":           "apollo",
                "confidence":       "high",
                "avatar_url":       photo,
                "twitter":          twitter,
                "company_linkedin": _org_linkedin or None,
                # Full raw Apollo response — saved to DB for company LLM enrichment
                "_apollo_raw": {
                    "person":       p,
                    "organization": org,
                },
            }
        if email and "placeholder" in email.lower():
            logger.warning("[Apollo] Rejected placeholder email: %s", email)
        # Check for credit exhaustion (422 with insufficient credits message)
        error_msg = body.get("error") or ""
        if r.status_code == 422 and "insufficient credits" in error_msg.lower():
            logger.warning("[Apollo] Credit exhausted: %s", error_msg)
            return {"_credit_exhausted": True, "source": "apollo_credit_exhausted"}
    except Exception as e:
        logger.warning("[Apollo] %s", e)
    return None


async def _try_dropcontact(linkedin_url: str, first: str, last: str, domain: str) -> Optional[dict]:
    """Dropcontact — enrich by LinkedIn URL or name+domain."""
    key = _dropcontact_key()
    if not key:
        return None
    try:
        payload = {"data": [{"linkedin": linkedin_url or "", "first_name": first, "last_name": last, "website": domain}],
                   "siren": False, "language": "en"}
        c = _get_api_client()
        r = await c.post("https://api.dropcontact.com/batch",
                         json=payload, headers={"X-Access-Token": key, "Content-Type": "application/json"})
        if r.status_code == 200:
            d = r.json()
            for item in (d.get("data") or []):
                email = item.get("email") and item["email"][0].get("email") if isinstance(item.get("email"), list) else item.get("email")
                if email:
                    logger.info("[Dropcontact] Found: %s", email)
                    return {"email": email, "phone": item.get("phone"), "source": "dropcontact", "confidence": "high"}
    except Exception as e:
        logger.debug("[Dropcontact] %s", e)
    return None


async def _try_pdl(first: str, last: str, linkedin_url: str, domain: str) -> Optional[dict]:
    """People Data Labs — person enrichment by LinkedIn URL or name+domain."""
    key = _pdl_api_key()
    if not key:
        return None
    try:
        params: dict = {"api_key": key, "pretty": "false"}
        if linkedin_url:
            params["profile"] = linkedin_url
        elif first and last and domain:
            params["first_name"] = first
            params["last_name"]  = last
            params["company"]    = domain
        else:
            return None
        c = _get_api_client()
        r = await c.get("https://api.peopledatalabs.com/v5/person/enrich", params=params)
        if r.status_code == 200:
            d = r.json()
            emails = d.get("data", {}).get("emails") or []
            email  = next((e.get("address") for e in emails if e.get("address")), None)
            phone  = next((p.get("number")  for p in (d.get("data", {}).get("phone_numbers") or [])), None)
            if email:
                logger.info("[PDL] Found: %s", email)
                return {"email": email, "phone": phone, "source": "pdl", "confidence": "high"}
    except Exception as e:
        logger.debug("[PDL] %s", e)
    return None


async def _try_zerobounce(email: str) -> dict:
    """
    ZeroBounce — email verification.
    Returns enriched contact dict with verified + bounce_risk fields.
    Falls through gracefully if key not set.
    """
    key = _zerobounce_key()
    if not key or not email:
        return {"email": email, "verified": False, "bounce_risk": None}
    try:
        c = _get_api_client()
        r = await c.get("https://api.zerobounce.net/v2/validate",
                        params={"api_key": key, "email": email, "ip_address": ""})
        if r.status_code == 200:
            d = r.json()
            status = d.get("status", "")
            sub_status = d.get("sub_status", "")
            verified = status == "valid"
            if status in ("invalid", "do_not_mail", "abuse"):
                bounce_risk = "high"
            elif status == "catch-all" or sub_status in ("role_based", "global_suppression"):
                bounce_risk = "medium"
            elif verified:
                bounce_risk = "low"
            else:
                bounce_risk = "medium"
            logger.info("[ZeroBounce] %s → status=%s bounce_risk=%s", email, status, bounce_risk)
            return {"email": email, "verified": verified, "bounce_risk": bounce_risk,
                    "zb_status": status, "zb_sub_status": sub_status}
    except Exception as e:
        logger.debug("[ZeroBounce] %s", e)
    return {"email": email, "verified": False, "bounce_risk": None}


async def find_contact_info(
    first: str,
    last: str,
    domain: str,
    linkedin_url: str = "",
    skip_hunter: bool = False,
    skip_apollo: bool = False,
) -> dict:
    """
    6-step email discovery waterfall:
      1. Hunter.io   — name + domain lookup
      2. Apollo.io   — person match
      3. Dropcontact — LinkedIn URL or name+domain
      4. PDL         — People Data Labs person enrichment
      5. Pattern guess — first.last@domain, flast@domain, first@domain
      6. ZeroBounce  — verify whichever email was found

    Returns: {email, phone, source, confidence, verified, bounce_risk}
    """
    result: Optional[dict] = None

    # Steps 1 + 2 — Hunter and Apollo in parallel (saves 2-4s vs sequential)
    run_hunter = not skip_hunter and bool(_hunter_api_key()) and bool(domain)
    run_apollo = not skip_apollo and bool(_apollo_api_key()) and bool(domain)

    async def _noop() -> None:
        return None

    if run_hunter or run_apollo:
        hunter_coro = _try_hunter(first, last, domain) if run_hunter else _noop()
        apollo_coro = _try_apollo(first, last, domain) if run_apollo else _noop()
        hunter_res, apollo_res = await asyncio.gather(hunter_coro, apollo_coro, return_exceptions=True)

        if isinstance(hunter_res, dict) and hunter_res.get("email"):
            result = hunter_res
            logger.info("[ContactWaterfall] Step 1 Hunter hit: %s", result.get("email"))
        elif isinstance(apollo_res, dict) and apollo_res.get("email"):
            result = apollo_res
            logger.info("[ContactWaterfall] Step 2 Apollo hit: %s", result.get("email"))
        else:
            if not run_hunter:
                logger.info("[ContactWaterfall] Step 1 Hunter skipped (disabled/no credits)")
            if not run_apollo:
                logger.info("[ContactWaterfall] Step 2 Apollo skipped (disabled/no credits)")

    # Steps 3 + 4 — Dropcontact and PDL in parallel (saves 2-4s vs sequential)
    if not result:
        run_dc  = bool(_dropcontact_key())
        run_pdl = bool(_pdl_api_key())
        if run_dc or run_pdl:
            dc_coro  = _try_dropcontact(linkedin_url, first, last, domain) if run_dc  else _noop()
            pdl_coro = _try_pdl(first, last, linkedin_url, domain)         if run_pdl else _noop()
            dc_res, pdl_res = await asyncio.gather(dc_coro, pdl_coro, return_exceptions=True)
            if isinstance(dc_res, dict) and dc_res.get("email"):
                result = dc_res
                logger.info("[ContactWaterfall] Step 3 Dropcontact hit: %s", result.get("email"))
            elif isinstance(pdl_res, dict) and pdl_res.get("email"):
                result = pdl_res
                logger.info("[ContactWaterfall] Step 4 PDL hit: %s", result.get("email"))

    # Step 5 — Pattern guess
    if not result and first and last and domain:
        last_clean = last.lower().replace(" ", "")
        guesses = [
            f"{first.lower()}.{last_clean}@{domain}",
            f"{first.lower()[0]}{last_clean}@{domain}",
            f"{first.lower()}@{domain}",
        ]
        result = {"email": guesses[0], "phone": None, "source": "pattern_guess", "confidence": "low"}
        logger.info("[ContactWaterfall] Step 5 Pattern guess: %s", guesses[0])

    if not result:
        return {"email": None, "phone": None, "source": None, "confidence": None,
                "verified": False, "bounce_risk": None}

    # Reject any placeholder emails that slipped through from external services
    if result.get("email") and "placeholder" in result["email"].lower():
        logger.warning("[ContactWaterfall] Dropping placeholder email from %s: %s",
                       result.get("source"), result.get("email"))
        result["email"] = None

    # Step 6 — ZeroBounce verification (always run if email found)
    email = result.get("email")
    if email:
        zb = await _try_zerobounce(email)
        result["verified"]    = zb.get("verified", False)
        result["bounce_risk"] = zb.get("bounce_risk")

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Company enrichment waterfall
# ─────────────────────────────────────────────────────────────────────────────

async def _try_clearbit_logo(domain: str) -> Optional[str]:
    """Clearbit Logo API — free, no auth needed."""
    if not domain:
        return None
    logo_url = f"https://logo.clearbit.com/{domain}"
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as c:
            r = await c.get(logo_url)
            if r.status_code == 200 and r.headers.get("content-type", "").startswith("image"):
                logger.info("[Clearbit] Logo found: %s", logo_url)
                return logo_url
    except Exception as e:
        logger.debug("[Clearbit] %s", e)
    return None


def _normalize_apollo_org(org: dict) -> dict:
    """Normalize Apollo organization dict → our standard company keys."""
    tech = [t.get("name") for t in (org.get("current_technologies") or [])[:12] if t.get("name")]
    website = org.get("website_url") or org.get("primary_domain") or ""
    if website and not website.startswith("http"):
        website = f"https://{website}"
    return {
        "company_logo": org.get("logo_url"),
        "company_description": org.get("short_description"),
        "company_phone": org.get("phone"),
        "company_linkedin": org.get("linkedin_url"),
        "company_twitter": org.get("twitter_url"),
        "company_website": website,
        "employee_count": org.get("estimated_num_employees") or 0,
        "industry": org.get("industry"),
        "tech_stack": tech,
        "hq_location": ", ".join(filter(None, [org.get("city"), org.get("country")])),
        "founded_year": str(org.get("founded_year") or ""),
        "funding_stage": org.get("latest_funding_stage"),
        "total_funding": str(org.get("total_funding") or ""),
        "annual_revenue": org.get("annual_revenue_printed"),
        "hiring_velocity": "Active" if org.get("currently_hiring") else "",
    }


async def _try_apollo_org(domain: str) -> dict:
    """Apollo.io organization enrichment by domain — company logo, description, tech stack, etc."""
    if not _apollo_api_key() or not domain:
        return {}
    try:
        async with httpx.AsyncClient(timeout=25) as c:
            r = await c.post(
                "https://api.apollo.io/v1/organizations/enrich",
                headers={"Content-Type": "application/json", "X-Api-Key": _apollo_api_key()},
                json={"domain": domain},
            )
            if r.status_code != 200:
                return {}
            org = r.json().get("organization") or {}
            if not org:
                return {}
            logger.info("[Apollo Org] Found by domain %s: %s", domain, org.get("name"))
            return _normalize_apollo_org(org)
    except Exception as e:
        logger.warning("[Apollo Org] %s", e)
    return {}


async def _try_apollo_company_search(company_name: str) -> dict:
    """
    Apollo.io company name search — finds the correct company by name, returns real website.
    Tries both /v1/mixed_companies/search and /v1/organizations/search endpoints.
    """
    if not _apollo_api_key() or not company_name:
        return {}

    headers = {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "X-Api-Key": _apollo_api_key(),
    }

    # Try mixed_companies/search first (newer Apollo endpoint)
    endpoints = [
        ("https://api.apollo.io/v1/mixed_companies/search", {"q_organization_name": company_name, "page": 1, "per_page": 1}),
        ("https://api.apollo.io/api/v1/organizations/search",  {"q_organization_name": company_name, "page": 1, "per_page": 1}),
    ]

    for endpoint_url, payload in endpoints:
        try:
            async with httpx.AsyncClient(timeout=25) as c:
                r = await c.post(endpoint_url, headers=headers, json=payload)
                if r.status_code not in (200, 201):
                    logger.debug("[Apollo Search] %s → %s", endpoint_url, r.status_code)
                    continue
                body = r.json()
                # Response structure varies by endpoint
                orgs = (body.get("organizations") or body.get("accounts") or
                        body.get("mixed_companies") or [])
                if not orgs:
                    continue
                org = orgs[0]
                logger.info("[Apollo Search] Found company: %r → %s", company_name,
                            org.get("website_url") or org.get("primary_domain") or "no website")
                result = _normalize_apollo_org(org)
                if result.get("company_website") or result.get("company_logo"):
                    return result
        except Exception as e:
            logger.debug("[Apollo Search] %s: %s", endpoint_url, e)

    # Final fallback: try organizations/enrich with guessed domain variations
    # This handles edge cases like "LBM Solutions" → try lbmsolutions.com, lbm-solutions.com
    guesses = _guess_domain_variants(company_name)
    for guess in guesses[:3]:
        try:
            result = await _try_apollo_org(guess)
            if result.get("company_logo") or result.get("company_description"):
                logger.info("[Apollo Search] Domain variant match: %s → %s", company_name, guess)
                result["company_website"] = result.get("company_website") or f"https://{guess}"
                return result
        except Exception:
            pass

    return {}


def _guess_domain_variants(company_name: str) -> list[str]:
    """Generate likely domain variants for a company name."""
    # Strip common suffixes and noise words
    name = company_name.lower()
    for noise in [" private limited", " pvt ltd", " pvt. ltd.", " ltd.", " llc", " inc.", " inc",
                  " limited", " corp.", " corp", " co.", " co", " technologies", " technology",
                  " solutions", " services", " group", " international"]:
        name = name.replace(noise, "")
    name = name.strip()
    slug = re.sub(r"[^a-z0-9\s]", "", name).strip()
    words = slug.split()

    variants = []
    if not words:
        return variants
    if len(words) == 1:
        variants = [f"{words[0]}.com", f"{words[0]}tech.com", f"{words[0]}solutions.com"]
    elif len(words) == 2:
        a, b = words[0], words[1]
        variants = [
            f"{a}{b}.com",       # lbmsolutions.com  ← try concat first
            f"{a}-{b}.com",      # lbm-solutions.com
            f"{b}{a}.com",       # solutionslbm.com  (rare but possible)
            f"{a}.com",          # lbm.com
        ]
    else:
        joined = "".join(words)
        first = words[0]
        variants = [f"{joined}.com", f"{first}.com"]
        # Acronym
        acronym = "".join(w[0] for w in words)
        if len(acronym) >= 2:
            variants.insert(0, f"{acronym}.com")

    return variants


async def _try_hunter_domain(domain: str) -> dict:
    """Hunter.io domain search — company emails, description, social links."""
    if not _hunter_api_key() or not domain:
        return {}
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.get("https://api.hunter.io/v2/domain-search", params={
                "domain": domain, "api_key": _hunter_api_key(), "limit": 10,
            })
            if r.status_code != 200:
                return {}
            data = r.json().get("data", {})
            emails = data.get("emails") or []
            generic_prefixes = ["info", "contact", "hello", "support", "sales", "admin", "team", "hi"]
            company_email = None
            for e in emails:
                prefix = e.get("value", "").split("@")[0].lower()
                if any(p in prefix for p in generic_prefixes):
                    company_email = e["value"]
                    break
            if not company_email and emails:
                company_email = emails[0].get("value")
            logger.info("[Hunter Domain] %d emails found, company_email=%s", len(emails), company_email)
            return {
                "company_email": company_email,
                "company_description": data.get("description"),
                "company_twitter": data.get("twitter"),
                "company_linkedin": data.get("linkedin"),
                "company_phone": data.get("phone_number"),
                "company_website": f"https://{domain}" if not data.get("website") else data["website"],
                "employee_count": data.get("employees") or 0,
            }
    except Exception as e:
        logger.warning("[Hunter Domain] %s", e)
    return {}


def _normalize_bd_company(c: dict) -> dict:
    """
    Normalize a raw Bright Data company record → our standard company keys.
    Handles multiple possible field names from different BD dataset versions.
    """
    result: dict = {}

    # ── Website ──────────────────────────────────────────────────────────────
    website = (
        c.get("website") or c.get("website_url") or c.get("company_website")
        or c.get("url") or c.get("homepage") or ""
    )
    if website:
        m = re.search(r"https?://(?:www\.)?([^/?\s]+)", website)
        if m and not any(m.group(1).lower() == s or m.group(1).lower().endswith("." + s) for s in _SOCIAL_DOMAINS):
            result["company_website"] = website

    # ── Logo ─────────────────────────────────────────────────────────────────
    logo = (
        c.get("logo") or c.get("logo_url") or c.get("company_logo")
        or c.get("profile_pic_url") or c.get("image_url")
    )
    if logo and isinstance(logo, str) and logo.startswith("http"):
        result["company_logo"] = logo

    # ── Description ──────────────────────────────────────────────────────────
    desc = (
        c.get("description") or c.get("about") or c.get("company_description")
        or c.get("tagline") or c.get("summary") or c.get("overview")
    )
    if desc:
        result["company_description"] = str(desc)[:800]

    # ── Employee count ────────────────────────────────────────────────────────
    # BD returns employees_in_linkedin (int) as the headcount.
    # The "employees" field is a list of employee profile objects — NOT a count.
    # Fallback: parse "company_size" string e.g. "51-200 employees" → take upper bound.
    emp_count = 0
    employees_in_linkedin = c.get("employees_in_linkedin")
    if isinstance(employees_in_linkedin, (int, float)) and employees_in_linkedin > 0:
        emp_count = int(employees_in_linkedin)
    else:
        # Try direct integer fields
        for field in ("employee_count", "staff_count"):
            n = _safe_int(c.get(field))
            if n > 0:
                emp_count = n
                break
        if not emp_count:
            # Parse "51-200 employees" → 200, "1001-5000 employees" → 5000
            size_str = str(c.get("company_size") or "")
            m_size = re.search(r"(\d[\d,]*)\s*(?:employees|$)", size_str.replace(",", ""))
            if m_size:
                nums = re.findall(r"\d+", size_str.replace(",", ""))
                emp_count = int(nums[-1]) if nums else 0
    if emp_count > 0:
        result["employee_count"] = emp_count

    # ── LinkedIn followers ────────────────────────────────────────────────────
    followers = _safe_int(c.get("followers") or c.get("followers_count"))
    if followers > 0:
        result["linkedin_followers"] = followers

    # ── Industry ─────────────────────────────────────────────────────────────
    ind = c.get("industry") or c.get("industries") or c.get("category")
    if ind:
        result["industry"] = ind[0] if isinstance(ind, list) else str(ind)

    # ── HQ Location ──────────────────────────────────────────────────────────
    hq = c.get("headquarters") or c.get("hq_location") or c.get("location")
    if hq:
        if isinstance(hq, dict):
            parts = [hq.get("city", ""), hq.get("region", ""), hq.get("country", "")]
            result["hq_location"] = ", ".join(p for p in parts if p)
        else:
            result["hq_location"] = str(hq)

    # ── Founded ──────────────────────────────────────────────────────────────
    founded = c.get("founded") or c.get("founded_year") or c.get("founded_on")
    if founded:
        result["founded_year"] = str(founded)

    # ── Specialties / Tech stack ──────────────────────────────────────────────
    specs = c.get("specialties") or c.get("specializations") or []
    if specs:
        if isinstance(specs, str):
            # BD returns as comma-separated string: "CRM Software, E-Commerce Software , ..."
            result["tech_stack"] = [s.strip() for s in specs.split(",") if s.strip()][:10]
        else:
            result["tech_stack"] = specs[:10]

    # ── Funding ───────────────────────────────────────────────────────────────
    # BD returns a structured funding object: {last_round_type, last_round_raised, rounds, last_round_date}
    funding_obj = c.get("funding") if isinstance(c.get("funding"), dict) else {}
    last_round_type = (
        funding_obj.get("last_round_type")
        or c.get("funding_stage") or c.get("company_type") or c.get("type")
    )
    if last_round_type:
        result["funding_stage"] = str(last_round_type)

    last_round_raised = funding_obj.get("last_round_raised") or c.get("total_funding")
    if last_round_raised:
        result["total_funding"] = str(last_round_raised)

    last_round_date = funding_obj.get("last_round_date") or c.get("last_funding_date")
    if last_round_date:
        # Strip time component if present: "2021-08-10T00:00:00.000Z" → "2021-08-10"
        result["last_funding_date"] = str(last_round_date)[:10]

    # ── Organization type (Privately Held, Public, etc.) ─────────────────────
    org_type = c.get("organization_type") or c.get("org_type") or c.get("type")
    if org_type:
        result["organization_type"] = str(org_type)

    # ── Company slogan ───────────────────────────────────────────────────────
    slogan = c.get("slogan") or c.get("tagline")
    if slogan:
        result["company_slogan"] = str(slogan)[:200]

    # ── Similar / Competitor companies ───────────────────────────────────────
    similar = c.get("similar") or c.get("similar_companies") or []
    if similar and isinstance(similar, list):
        # Extract just name + industry + location for each similar company
        similar_clean = [
            {
                "name": s.get("title", ""),
                "industry": s.get("subtitle", ""),
                "location": s.get("location", ""),
                "linkedin": s.get("Links", ""),
            }
            for s in similar[:10] if isinstance(s, dict) and s.get("title")
        ]
        if similar_clean:
            result["similar_companies"] = similar_clean

    # ── LinkedIn posts (updates feed) ─────────────────────────────────────────
    updates = c.get("updates") or c.get("posts") or []
    if updates and isinstance(updates, list):
        posts_clean = []
        for u in updates[:10]:
            if not isinstance(u, dict):
                continue
            post = {
                "post_id": u.get("post_id", ""),
                "date": u.get("date", ""),
                "text": (u.get("text") or "")[:500],
                "likes_count": u.get("likes_count", 0),
                "comments_count": u.get("comments_count", 0),
                "post_url": u.get("post_url", ""),
            }
            if post["text"] or post["post_id"]:
                posts_clean.append(post)
        if posts_clean:
            result["linkedin_posts"] = posts_clean

    # ── Crunchbase ────────────────────────────────────────────────────────────
    crunchbase_url = c.get("crunchbase_url") or c.get("crunchbase")
    if crunchbase_url:
        result["crunchbase_data"] = {"url": str(crunchbase_url)}

    # ── Phone ─────────────────────────────────────────────────────────────────
    phone = c.get("phone") or c.get("phone_number") or c.get("company_phone")
    if phone:
        result["company_phone"] = str(phone)

    # ── Email ─────────────────────────────────────────────────────────────────
    email = c.get("email") or c.get("company_email")
    if email:
        result["company_email"] = str(email)

    # ── Twitter ───────────────────────────────────────────────────────────────
    tw = c.get("twitter") or c.get("twitter_url")
    if tw:
        result["company_twitter"] = str(tw)

    # ── Normalise any LinkedIn URLs in the result ─────────────────────────────
    # BD company responses may carry country-prefixed or mixed-case LinkedIn
    # URLs in linkedin_url / url fields — clean them for consistency.
    for url_field in ("linkedin_url", "company_linkedin", "url"):
        v = c.get(url_field) or ""
        if v and "linkedin.com" in v.lower():
            result[url_field] = _clean_bd_linkedin_url(v)

    return result


async def _try_bd_company(company_linkedin_url: str) -> dict:
    """
    Fetch LinkedIn company profile via Bright Data company dataset.

    Flow:
      1. Try sync /scrape (immediate response)
      2. If response contains snapshot_id (async job was started) → poll until ready
      3. Fallback to /trigger → /progress → /snapshot pattern

    Returns normalized company dict or {} on failure.
    """
    if not _bd_api_key() or not company_linkedin_url:
        return {}
    if "linkedin.com/company/" not in company_linkedin_url:
        logger.debug("[BD Company] Skipping — not a company URL: %s", company_linkedin_url)
        return {}

    dataset_id = _bd_company_dataset()
    logger.info("[BD Company] Fetching company profile: %s (dataset=%s)", company_linkedin_url, dataset_id)

    # ── Sync /scrape with correct payload format ──────────────────────────────
    # Format: POST /scrape?dataset_id=...&notify=false&include_errors=true
    # Body: {"input": [{"url": "..."}]}
    snapshot_id = None
    try:
        async with httpx.AsyncClient(timeout=90) as client:
            resp = await client.post(
                f"{BD_BASE}/scrape",
                params={
                    "dataset_id":     dataset_id,
                    "notify":         "false",
                    "include_errors": "true",
                    "format":         "json",
                },
                headers=_bd_headers(),
                json={"input": [{"url": company_linkedin_url}]},
            )
            logger.debug("[BD Company] /scrape status=%s body=%.300s", resp.status_code, resp.text)

            if resp.status_code not in (200, 202):
                logger.warning("[BD Company] /scrape failed %s: %s", resp.status_code, resp.text[:200])
            else:
                body = resp.json()
                # Case A: Immediate data — list of company records
                data_list = body if isinstance(body, list) else body.get("data") or []
                if data_list and isinstance(data_list[0], dict):
                    c = data_list[0]
                    if c.get("name") or c.get("description") or c.get("website") or c.get("logo"):
                        result = _normalize_bd_company(c)
                        if result:
                            logger.info("[BD Company] Sync OK — %d fields from %s", len(result), company_linkedin_url)
                            return result
                # Case B: Async job — snapshot_id returned
                if isinstance(body, dict):
                    snapshot_id = body.get("snapshot_id")
                if not snapshot_id and data_list and isinstance(data_list[0], dict):
                    snapshot_id = data_list[0].get("snapshot_id")
                if snapshot_id:
                    logger.info("[BD Company] Async job started — snapshot_id=%s", snapshot_id)
    except Exception as e:
        logger.warning("[BD Company] /scrape error: %s", e)

    # ── Poll snapshot if async job was started ────────────────────────────────
    if snapshot_id:
        try:
            data = await poll_snapshot(snapshot_id, interval=5, timeout=120)
            if data:
                c = data[0] if isinstance(data, list) else data
                result = _normalize_bd_company(c)
                if result:
                    logger.info("[BD Company] Async poll OK — %d fields from %s", len(result), company_linkedin_url)
                    return result
        except Exception as e:
            logger.warning("[BD Company] Snapshot poll failed for %s: %s", snapshot_id, e)

    logger.warning("[BD Company] All methods failed for %s — continuing waterfall", company_linkedin_url)
    return {}


# ─────────────────────────────────────────────────────────────────────────────
# Company CRM LLM Enrichment — Qwen2.5-72B via HuggingFace Router
# ─────────────────────────────────────────────────────────────────────────────

_COMPANY_CRM_SYSTEM_PROMPT = """You are an expert B2B CRM data analyst. Your job is to enrich raw company data from LinkedIn and Apollo into a fully structured, CRM-ready profile.

You must return ONLY a valid JSON object — no markdown, no explanation, no preamble.
Follow the exact schema provided. Infer and derive fields intelligently from the data.

STRICT RULES:
- Return ONLY valid JSON — no markdown, no code fences, no extra text.
- NEVER leave any field empty — always fill with real data or a confident inference.
- Arrays must have at least 3 real, distinct items — never fewer.
- All strings must be non-empty, meaningful, and specific.
- Infer missing fields from available signals — never use "Unknown" or "N/A".
- revenue_range: infer from employee count + funding stage + industry.
- lead_score: 0–100 based on company size, activity, funding, and relevance.
- lead_temperature: Hot (actively hiring + funded + posting), Warm (moderate signals), Cold (minimal signals)."""


def _build_company_crm_user_prompt(bd_raw: dict, apollo_raw: dict, company_name: str) -> str:
    """Build the user prompt combining BrightData and Apollo raw company data."""
    combined = {
        "company_name": company_name,
        "brightdata_linkedin_data": bd_raw,
        "apollo_data": apollo_raw,
    }
    raw_json = json.dumps(combined, separators=(",", ":"), ensure_ascii=False, default=str)
    schema = """{
  "company_identity": {
    "legal_name": "", "brand_name": "", "tagline": "",
    "description_short": "", "description_long": "",
    "founded_year": 0, "company_stage": "", "organization_type": "",
    "industry_primary": "", "industry_secondary": [],
    "hq_city": "", "hq_state": "", "hq_country": "", "hq_full_address": "",
    "office_locations": [], "global_reach": true,
    "website": "", "linkedin_url": "", "crunchbase_url": ""
  },
  "tags": [],
  "tech_tags": [],
  "services": [{"name": "", "category": "", "description": "", "target_audience": ""}],
  "key_people": [{"name": "", "title": "", "seniority": "", "department": "", "linkedin_url": "", "is_decision_maker": false, "contact_confidence": ""}],
  "contact_info": {"primary_email": "", "emails": [], "phone_primary": "", "phones": [], "whatsapp": "", "preferred_contact_channel": ""},
  "financials": {"funding_status": "", "total_funding_usd": 0, "last_round_type": "", "last_round_date": "", "last_round_amount_usd": 0, "revenue_range": "", "employee_count_linkedin": 0, "employee_count_range": ""},
  "social_presence": {"linkedin_followers": 0, "linkedin_engagement_avg": "", "posting_frequency": "", "content_themes": [], "last_post_date": ""},
  "activities": [{"date": "", "type": "", "summary": "", "engagement_score": 0, "sentiment": ""}],
  "interests": [],
  "icp_fit": {"ideal_customer_profile": "", "target_company_size": "", "target_industries": [], "target_geographies": [], "deal_type": ""},
  "competitive_intelligence": {"direct_competitors": [], "market_position": "", "differentiators": [], "weaknesses_inferred": []},
  "crm_metadata": {"lead_score": 0, "lead_temperature": "", "outreach_priority": "", "best_outreach_time": "", "recommended_first_message": "", "crm_tags": [], "data_source": "LinkedIn", "data_scraped_at": "", "enrichment_confidence": ""}
}"""
    return (
        f"Enrich the following company data into a complete CRM profile.\n\n"
        f"RAW DATA:\n{raw_json}\n\n"
        f"Return a JSON object with EXACTLY this schema:\n{schema}"
    )


async def _fetch_apollo_company_raw(company_name: str, domain: str) -> dict:
    """Fetch raw Apollo org dict for LLM — tries domain enrich then name search."""
    if not _apollo_api_key():
        return {}
    # Try domain enrich first (more accurate)
    if domain:
        try:
            async with httpx.AsyncClient(timeout=25) as c:
                r = await c.post(
                    "https://api.apollo.io/v1/organizations/enrich",
                    headers={"Content-Type": "application/json", "X-Api-Key": _apollo_api_key()},
                    json={"domain": domain},
                )
                if r.status_code == 200:
                    org = r.json().get("organization") or {}
                    if org.get("name"):
                        logger.info("[Apollo Raw] Found by domain %s", domain)
                        return org
        except Exception as e:
            logger.debug("[Apollo Raw] domain enrich failed: %s", e)
    # Fallback: name search
    if company_name:
        try:
            for endpoint_url, payload in [
                ("https://api.apollo.io/v1/mixed_companies/search", {"q_organization_name": company_name, "page": 1, "per_page": 1}),
                ("https://api.apollo.io/api/v1/organizations/search", {"q_organization_name": company_name, "page": 1, "per_page": 1}),
            ]:
                async with httpx.AsyncClient(timeout=25) as c:
                    r = await c.post(endpoint_url,
                                     headers={"Content-Type": "application/json", "X-Api-Key": _apollo_api_key()},
                                     json=payload)
                    if r.status_code in (200, 201):
                        body = r.json()
                        orgs = body.get("organizations") or body.get("accounts") or body.get("mixed_companies") or []
                        if orgs:
                            logger.info("[Apollo Raw] Found by name %s", company_name)
                            return orgs[0]
        except Exception as e:
            logger.debug("[Apollo Raw] name search failed: %s", e)
    return {}


async def _enrich_company_with_llm(bd_raw: dict, apollo_raw: dict, company_name: str) -> dict:
    """
    Call Qwen2.5-72B (HuggingFace Router) with combined BD + Apollo company data.
    Returns parsed CRM profile dict, or {} on failure.
    """
    if not bd_raw and not apollo_raw:
        return {}
    try:
        user_prompt = _build_company_crm_user_prompt(bd_raw, apollo_raw, company_name)
        logger.info("[CompanyLLM] Calling Qwen2.5-72B for company: %s", company_name)
        raw = await _call_llm(
            [
                {"role": "system", "content": _COMPANY_CRM_SYSTEM_PROMPT},
                {"role": "user",   "content": user_prompt},
            ],
            max_tokens=4000,
            temperature=0.2,
            hf_first=True,  # Qwen2.5-72B via HF router → Groq fallback
        )
        if not raw:
            logger.warning("[CompanyLLM] No response from LLM for %s", company_name)
            return {}
        import re as _re
        raw = _re.sub(r"<think>.*?</think>", "", raw, flags=_re.DOTALL)
        raw = _re.sub(r"<think>.*", "", raw, flags=_re.DOTALL).strip()
        start = raw.find("{")
        if start == -1:
            return {}
        depth, end = 0, start
        for i, ch in enumerate(raw[start:], start):
            if ch == "{": depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    end = i
                    break
        parsed = json.loads(raw[start:end + 1])
        logger.info("[CompanyLLM] CRM brief generated for %s (%d top-level keys)", company_name, len(parsed))
        return parsed
    except Exception as e:
        logger.warning("[CompanyLLM] LLM enrichment failed for %s: %s", company_name, e)
        return {}


async def enrich_company_waterfall(company_name: str, domain: str, profile: dict) -> tuple[dict, list[dict]]:
    """
    Company data waterfall:
      0. Bright Data LinkedIn Company dataset (company LinkedIn URL from person profile)
      1. Bright Data person profile fields (logo, industry, employee count, etc.)
      2. Clearbit Logo API (free, logo-only fallback when BD returns no logo)
      3. Apollo.io raw data fetch (parallel with step 2)
      4. Qwen2.5-72B LLM enrichment — BD + Apollo → full CRM profile
    Returns (company_data dict, waterfall_log list)
    """
    log: list[dict] = []
    result: dict = {}

    # ── Extract company LinkedIn URL from person profile ───────────────────────
    raw_link = profile.get("current_company_link") or profile.get("current_company_url") or ""
    company_linkedin_url = raw_link if "linkedin.com/company/" in raw_link else ""

    # ── Actual company website from profile (not a social URL) ────────────────
    raw_website = profile.get("current_company_link") or ""
    m = re.search(r"https?://(?:www\.)?([^/?\s]+)", raw_website)
    actual_website = raw_website if (m and not any(
        m.group(1).lower() == s or m.group(1).lower().endswith("." + s)
        for s in _SOCIAL_DOMAINS
    )) else ""

    # Step 0: BrightData LinkedIn Company dataset (primary source)
    bd_raw: dict = {}
    if company_linkedin_url:
        bd_co = await _try_bd_company(company_linkedin_url)
        if bd_co:
            bd_raw = dict(bd_co)
            result.update(bd_co)
            log.append({
                "step": 0, "source": "Bright Data Company",
                "fields_found": list(bd_co.keys()), "note": company_linkedin_url,
            })
            # BD returned a real website — use it as verified domain
            if bd_co.get("company_website"):
                actual_website = bd_co["company_website"]
                m2 = re.search(r"https?://(?:www\.)?([^/?\s]+)", actual_website)
                if m2:
                    domain = m2.group(1).lower()
    else:
        logger.info("[CompanyWaterfall] No company LinkedIn URL in profile — skipping BD company scrape for %s", company_name)

    # Step 1: Person profile fields (fill any gaps BD didn't cover)
    profile_fields = {
        "company_logo":        profile.get("current_company_logo") or profile.get("company_logo") or profile.get("logo"),
        "company_linkedin":    company_linkedin_url or None,
        "company_website":     actual_website or None,
        "company_description": profile.get("current_company_description"),
        "employee_count":      _safe_int(profile.get("current_company_employees_count")),
        "industry":            profile.get("current_company_industry"),
        "founded_year":        str(profile.get("current_company_founded") or ""),
    }
    filled = [k for k, v in profile_fields.items() if v and (k not in result or not result[k])]
    if filled:
        result.update({k: profile_fields[k] for k in filled})
        log.append({"step": 1, "source": "Bright Data (profile)", "fields_found": filled, "note": "person profile fields"})

    # Step 2: Clearbit logo
    if not result.get("company_logo") and domain:
        logo = await _try_clearbit_logo(domain)
        if logo:
            result["company_logo"] = logo
            log.append({"step": 2, "source": "Clearbit Logo", "fields_found": ["company_logo"], "note": domain})

    # Step 3: Apollo raw — read from DB first (saved during email enrichment), API only as fallback
    apollo_raw: dict = {}
    lead_url = profile.get("input_url") or profile.get("url") or ""
    if lead_url:
        try:
            lead_id_for_apollo = _lead_id(_normalize_linkedin_url(lead_url))
            async with get_pool().acquire() as _conn:
                _row = await _conn.fetchrow(
                    "SELECT apollo_raw FROM enriched_leads WHERE id=$1 AND apollo_raw IS NOT NULL LIMIT 1",
                    lead_id_for_apollo,
                )
            if _row and _row["apollo_raw"]:
                _parsed = json.loads(_row["apollo_raw"]) if isinstance(_row["apollo_raw"], str) else _row["apollo_raw"]
                if isinstance(_parsed, dict) and _parsed:
                    apollo_raw = _parsed
                    logger.info("[CompanyWaterfall] apollo_raw loaded from DB for lead %s (%d keys)", lead_id_for_apollo, len(apollo_raw))
                    log.append({"step": 3, "source": "Apollo.io (from DB)", "fields_found": list(apollo_raw.keys())[:8], "note": "cached from email enrichment"})
        except Exception as _e:
            logger.debug("[CompanyWaterfall] apollo_raw DB lookup failed: %s", _e)

    # If not in DB, fetch fresh from Apollo API
    if not apollo_raw and (company_name or domain):
        apollo_raw = await _fetch_apollo_company_raw(company_name, domain)
        if apollo_raw:
            log.append({"step": 3, "source": "Apollo.io (API)", "fields_found": list(apollo_raw.keys())[:8], "note": apollo_raw.get("name", "")})

    # Step 4: Qwen2.5-72B LLM enrichment — BD + Apollo → full CRM profile
    # Merge profile company fields into bd_raw for richer LLM context
    bd_for_llm = {
        **bd_raw,
        "company_name": company_name,
        "linkedin_url": company_linkedin_url,
        "from_person_profile": {k: v for k, v in profile_fields.items() if v},
    }
    if bd_for_llm or apollo_raw:
        llm_result = await _enrich_company_with_llm(bd_for_llm, apollo_raw, company_name)
        if llm_result:
            result["company_crm_brief"] = llm_result
            # Also pull flat fields from LLM output to fill gaps in standard keys
            ci = llm_result.get("company_identity") or {}
            fin = llm_result.get("financials") or {}
            soc = llm_result.get("social_presence") or {}
            meta = llm_result.get("crm_metadata") or {}
            _llm_fills = {
                "company_description": ci.get("description_long") or ci.get("description_short"),
                "industry":            ci.get("industry_primary"),
                "founded_year":        str(ci.get("founded_year") or ""),
                "hq_location":         ", ".join(filter(None, [ci.get("hq_city"), ci.get("hq_country")])),
                "employee_count":      fin.get("employee_count_linkedin") or 0,
                "funding_stage":       fin.get("funding_status"),
                "total_funding":       str(fin.get("total_funding_usd") or ""),
                "linkedin_followers":  soc.get("linkedin_followers") or 0,
                "tech_stack":          llm_result.get("tech_tags") or [],
                "company_tags":        llm_result.get("tags") or [],
            }
            for k, v in _llm_fills.items():
                if v and not result.get(k):
                    result[k] = v
            log.append({
                "step": 4, "source": f"LLM {_hf_model().split('/')[-1]}",
                "fields_found": list(llm_result.keys()),
                "note": f"lead_score={meta.get('lead_score',0)} temp={meta.get('lead_temperature','')}",
            })

    # Expose final verified domain so caller can target the right website
    result["_verified_domain"] = domain

    return result, log


# ─────────────────────────────────────────────────────────────────────────────
# Avatar extraction helper
# ─────────────────────────────────────────────────────────────────────────────

def _extract_avatar(profile: dict) -> Optional[str]:
    """Extract avatar/photo URL from Bright Data profile fields.
    Returns None when BD signals a generic placeholder (default_avatar: true).
    """
    if profile.get("default_avatar"):
        return None
    for key in ["avatar_url", "profile_pic_src", "profile_picture", "image_url", "image", "photo", "avatar"]:
        v = profile.get(key)
        if v and isinstance(v, str) and v.startswith("http"):
            return v
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Stage 3 — Website Intelligence
# ─────────────────────────────────────────────────────────────────────────────

async def _fetch_page_text(url: str, timeout: int = 12) -> str:
    """Fetch a URL and return clean text, stripping HTML tags."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as c:
            r = await c.get(url, headers=headers)
            if r.status_code != 200:
                return ""
            html = r.text
            # Strip scripts and styles
            html = re.sub(r'<(script|style)[^>]*>.*?</\1>', ' ', html, flags=re.DOTALL | re.IGNORECASE)
            # Strip HTML tags
            text = re.sub(r'<[^>]+>', ' ', html)
            # Normalize whitespace
            text = re.sub(r'\s+', ' ', text).strip()
            return text[:4000]
    except Exception as e:
        logger.debug("[WebScrape] %s → %s", url, e)
        return ""


async def scrape_website_intelligence(website: str) -> dict:
    """
    Stage 3 — Website Intelligence.
    Scrapes homepage, about, features, pricing, blog, careers pages
    and extracts first-party company intelligence via LLM.
    """
    if not website:
        return {}

    base = website.strip().rstrip("/")
    if not base.startswith("http"):
        base = f"https://{base}"
    # Also try www. variant
    if "://www." not in base:
        base_www = base.replace("://", "://www.", 1)
    else:
        base_www = None

    # Pages to try — try both base and www variant for homepage
    page_candidates = [
        ("homepage",  base),
        ("homepage",  base_www or base),
        ("about",     f"{base}/about"),
        ("about",     f"{base}/about-us"),
        ("features",  f"{base}/features"),
        ("features",  f"{base}/product"),
        ("pricing",   f"{base}/pricing"),
        ("blog",      f"{base}/blog"),
        ("blog",      f"{base}/news"),
        ("careers",   f"{base}/careers"),
        ("careers",   f"{base}/jobs"),
    ]

    # Fetch all pages concurrently (merge duplicates by page_type, keep longest text)
    pages_fetched: dict[str, str] = {}
    tasks = [(page_type, url, _fetch_page_text(url)) for page_type, url in page_candidates]

    results = await asyncio.gather(*[t[2] for t in tasks], return_exceptions=True)
    for (page_type, url, _), text in zip(tasks, results):
        if isinstance(text, str) and len(text) > 20:
            # Keep the longest text for each page type
            if len(text) > len(pages_fetched.get(page_type, "")):
                pages_fetched[page_type] = text
                logger.info("[WebScrape] ✓ %s (%d chars) from %s", page_type, len(text), url)

    if not pages_fetched:
        logger.warning("[WebScrape] No pages fetched for: %s", base)
        return {"website": base, "status": "unreachable", "pages_scraped": []}

    # Build combined context for LLM (cap each page at 2500 chars)
    combined = ""
    for page_type, text in pages_fetched.items():
        combined += f"\n\n=== {page_type.upper()} PAGE ===\n{text[:2500]}"

    # LLM structured extraction
    prompt = f"""You are a B2B market intelligence analyst. Analyze this website content and extract ONLY factual, observable information.

Website: {base}
Pages scraped: {list(pages_fetched.keys())}

SCRAPED CONTENT:
{combined[:9000]}

Return ONLY valid JSON with these exact keys. Use null for missing data, never hallucinate:
{{
  "company_description": "2-3 sentence factual summary of what the company does",
  "product_offerings": ["list of actual products/services they offer"],
  "value_proposition": "their core value proposition in 1 sentence, taken from site copy",
  "target_customers": ["specific job roles, company types, or industries they serve"],
  "use_cases": ["specific use cases or problems they solve"],
  "business_model": "SaaS / Marketplace / Agency / Hardware / Service / Other",
  "product_category": "e.g. CRM, HR Software, Analytics Platform, Dev Tools",
  "market_positioning": "enterprise / mid-market / SMB / startup-focused / premium / budget",
  "pricing_signals": "freemium / subscription / per-seat / usage-based / custom-enterprise / not-visible",
  "pricing_tiers": [],
  "key_messaging": ["top 3-5 messaging themes or keywords used on site"],
  "integrations_mentioned": ["any third-party tools or integrations mentioned"],
  "tech_stack_clues": ["tech/frameworks visible in site content or job postings"],
  "hiring_signals": "active / moderate / limited / no-careers-page",
  "open_roles": [],
  "recent_blog_topics": [],
  "problem_solved": "1 sentence: what customer pain does this company address"
}}"""

    raw = await _call_llm([
        {"role": "system", "content": "B2B website intelligence extractor. Return ONLY valid JSON, no explanation, no markdown."},
        {"role": "user", "content": prompt},
    ], max_tokens=1400, temperature=0.15)

    if raw:
        data = _parse_json_from_llm(raw)
        if data and ("company_description" in data or "product_offerings" in data):
            data.setdefault("website", base)
            data.setdefault("status", "scraped")
            data["pages_scraped"] = list(pages_fetched.keys())
            logger.info(
                "[WebScrape] Intelligence extracted — category=%s, model=%s, pages=%d",
                data.get("product_category"), data.get("business_model"), len(pages_fetched),
            )
            return data

    # Rule-based fallback from scraped text (no LLM)
    logger.warning("[WebScrape] LLM unavailable — using rule-based text inference for: %s", base)
    all_text = " ".join(pages_fetched.values()).lower()
    page_title = list(pages_fetched.values())[0][:200] if pages_fetched else ""

    # Business model inference
    bm = "Service"
    if any(k in all_text for k in ["saas", "software as a service", "subscription", "per seat", "per user"]):
        bm = "SaaS"
    elif any(k in all_text for k in ["marketplace", "platform", "connect buyers", "connect sellers"]):
        bm = "Marketplace"
    elif any(k in all_text for k in ["agency", "consulting", "outsourcing", "staffing", "development company"]):
        bm = "Agency / Services"
    elif any(k in all_text for k in ["hardware", "device", "iot", "physical"]):
        bm = "Hardware"

    # Product category inference
    cat = ""
    for kw, cat_name in [
        ("crm", "CRM"), ("sales", "Sales Software"), ("hr", "HR Software"),
        ("payroll", "Payroll"), ("inventory", "Inventory Management"),
        ("analytics", "Analytics"), ("marketing", "Marketing Software"),
        ("ecommerce", "E-Commerce"), ("erp", "ERP"), ("accounting", "Accounting"),
        ("logistics", "Logistics"), ("healthcare", "Healthcare Tech"),
        ("fintech", "FinTech"), ("edtech", "EdTech"),
        ("mobile app", "Mobile App Development"), ("software development", "Software Development"),
        ("web development", "Web Development"),
    ]:
        if kw in all_text:
            cat = cat_name
            break

    hiring = "active" if "careers" in pages_fetched or "jobs" in pages_fetched else "limited"

    return {
        "website": base, "status": "partial",
        "pages_scraped": list(pages_fetched.keys()),
        "company_description": page_title if page_title else None,
        "business_model": bm,
        "product_category": cat,
        "hiring_signals": hiring,
        "value_proposition": None,
        "product_offerings": [],
        "target_customers": [],
        "use_cases": [],
        "key_messaging": [],
        "pricing_signals": "not-visible",
        "open_roles": [],
        "problem_solved": None,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Default CRM Brief system prompt — used when lio_system_prompt is not set in DB
# ─────────────────────────────────────────────────────────────────────────────

_DEFAULT_CRM_BRIEF_PROMPT = """You are a high-performance B2B sales intelligence analyst.

Your task is to analyze LinkedIn raw data (profile + posts + interactions + company context) and generate structured, insight-rich lead intelligence.

STRICT RULES:
- Return ONLY valid JSON — no markdown, no code fences, no extra text before or after.
- NEVER leave any field empty ("", [], 0 are all forbidden as final values — always fill with real data or a specific inference).
- If exact data is missing → generate a confident, specific inference based on available signals.
- Do NOT copy text verbatim — always synthesize into insight.
- Keep outputs concise, specific, and high-signal.
- Avoid generic B2B filler phrases (e.g. "results-driven", "passionate about").
- All arrays MUST contain 4 to 5 real, distinct items — never fewer.
- All strings MUST be non-empty, meaningful, and specific.

ANALYSIS LOGIC:
- authored_posts → define expertise, identity, and authority.
- interactions (likes/comments/reposts) → indicate buying intent and interest areas.
- company_context → derive business challenges, growth stage, and priorities.
- If experience data is missing → reconstruct from activity patterns and company context.
- Pain points MUST be specific and derived from the data — not generic.
- Buying signals MUST come from observed behaviour, not assumptions.
- Trigger events = recent role changes, company news, posts about challenges.
- Seniority and decision_maker fields MUST be inferred from title + experience.
- crm_scores MUST use INTEGER values (0–100) for icp_fit, engagement_score, timing_score. priority_level MUST be one of: "High", "Medium", "Low".
- crm_import_fields.analyst_summary MUST be 2–3 sentences in third person: (1) who they are professionally and what they do, (2) what makes them engaged or notable — expertise, community role, recent achievements, (3) why they are a valuable sales prospect. Be specific to THIS person. Do NOT use phrases like "results-driven" or "passionate about". Example: "Sarah Chen is a VP of Engineering at a Series B fintech startup scaling their payments infrastructure globally. She actively publishes about distributed systems challenges and engages with DevOps tooling content, signalling active vendor evaluation. Her decision-making authority and rapid growth context make her a high-priority prospect for infrastructure solutions."

OUTPUT — return ONLY this JSON with ALL fields fully populated (arrays must have 4–5 items each):
{"who_they_are":{"name":"","title":"","company":"","location":"","linkedin_url":"","profile_image":"","company_logo":"","followers":"","connections":"","persona":"","seniority":"","trajectory":"","decision_maker":""},"their_company":{"type":"","industry":"","stage":"","company_size":"","founded":"","website":"","company_tags":["","","","",""],"relevance_score":"","relevance_reason":""},"what_they_care_about":{"primary_interests":["","","","",""],"secondary_interests":["","","","",""],"passion_signals":["","","","",""]},"online_behaviour":{"activity_level":"","content_style":"","recurring_themes":["","","","",""],"primary_platform":""},"communication":{"tone":"","writing_style":"","emotional_mode":"","archetype":"","mirror_strategy":""},"what_drives_them":{"core_values":["","","","",""],"motivators":["","","","",""],"pain_points":["","","","",""],"career_ambitions":["","","","",""]},"buying_signals":{"intent_level":"","trigger_events":["","","","",""],"tools_used":["","","","",""],"decision_style":"","intent_tags":["","","","",""]},"smart_tags":["","","","",""],"outreach_blueprint":{"best_channel":"","approach_strategy":"","opening_hooks":["","","","",""],"recommended_content":"","avoid_topics":["","","","",""],"one_line_strategy":""},"crm_scores":{"icp_fit":0,"engagement_score":0,"timing_score":0,"priority_level":""},"crm_import_fields":{"buyer_type":"","buying_signal":"","outreach_tone":"","hook_theme":"","avoidance":"","tags":["","","","",""],"analyst_summary":""},"recent_activity":{"posts":[{"topic":"","tone":"","key_message":"","engagement":"","intent_signal":""},{"topic":"","tone":"","key_message":"","engagement":"","intent_signal":""},{"topic":"","tone":"","key_message":"","engagement":"","intent_signal":""}],"interactions":[{"interaction_type":"","topic":"","insight":"","intent_signal":""},{"interaction_type":"","topic":"","insight":"","intent_signal":""},{"interaction_type":"","topic":"","insight":"","intent_signal":""}]}}"""


# ─────────────────────────────────────────────────────────────────────────────
# Comprehensive 8-stage LLM enrichment
# ─────────────────────────────────────────────────────────────────────────────

def _build_comprehensive_prompt(
    linkedin_url: str,
    profile: dict,
    contact: dict,
    now_iso: str,
    website_intel: dict = None,
) -> str:
    name        = profile.get("name", "")
    title       = profile.get("position") or profile.get("headline", "")
    company     = profile.get("current_company_name", "")
    website     = profile.get("current_company_link", "")
    location    = profile.get("location") or profile.get("city", "")
    about       = (profile.get("about") or "")[:600]
    followers   = profile.get("followers", 0)
    connections = profile.get("connections", 0)
    skills      = json.dumps(profile.get("skills") or [])
    experience  = json.dumps((profile.get("experience") or [])[:5])
    education   = json.dumps((profile.get("education") or profile.get("educations_details") or [])[:5])
    certs       = json.dumps(profile.get("certifications") or [])
    langs       = json.dumps(profile.get("languages") or [])
    email       = contact.get("email") or ""
    phone       = contact.get("phone") or ""
    email_src   = contact.get("source") or ""
    email_conf  = contact.get("confidence") or ""

    # Company extras from waterfall (injected into profile by enrich_single)
    company_extras = profile.get("_company_extras") or {}

    # Website intelligence section
    wi = website_intel or {}
    wi_section = f"""
Website Intelligence (Stage 3 — Scraped):
  Company Description: {wi.get('company_description', '')}
  Product Offerings: {json.dumps(wi.get('product_offerings', []))}
  Value Proposition: {wi.get('value_proposition', '')}
  Target Customers: {json.dumps(wi.get('target_customers', []))}
  Business Model: {wi.get('business_model', '')}
  Product Category: {wi.get('product_category', '')}
  Pricing Signals: {wi.get('pricing_signals', '')}
  Market Positioning: {wi.get('market_positioning', '')}
  Problem Solved: {wi.get('problem_solved', '')}
  Hiring Signals: {wi.get('hiring_signals', '')}
  Pages Scraped: {json.dumps(wi.get('pages_scraped', []))}""" if wi else "Website Intelligence: Not available"

    return f"""You are a B2B lead intelligence analyst for Worksbuddy, a CRM & productivity platform.

Generate a COMPREHENSIVE 8-stage lead enrichment report for the following profile.
Use the provided data. For fields not in the data, use your knowledge and reasonable inference.
Be specific, professional, and accurate.

--- RAW DATA ---
LinkedIn URL: {linkedin_url}
Name: {name}
Title: {title}
Company: {company}
Website: {website}
Location: {location}
About: {about}
Followers: {followers}  |  Connections: {connections}
Email: {email} ({email_src}, {email_conf})
Phone: {phone}
Skills: {skills}
Experience: {experience}
Education: {education}
Certifications: {certs}
Languages: {langs}
Enrichment date: {now_iso}
Company Logo: {company_extras.get('company_logo', '')}
Company Email: {company_extras.get('company_email', '')}
Company Phone: {company_extras.get('company_phone', '')}
Company Description: {(company_extras.get('company_description') or '')[:300]}
Tech Stack: {json.dumps(company_extras.get('tech_stack', []))}
Employee Count: {company_extras.get('employee_count', 0)}
Industry: {company_extras.get('industry', '')}
{wi_section}
---

[Stage 1] Person Profile
[Stage 2] Company Identity
[Stage 3] Website Intelligence (use scraped data above verbatim where available)
[Stage 4] Company Profile
[Stage 5] Market Signals
[Stage 6] Intent Signals
[Stage 7] Scoring
[Stage 8] Outreach

Return ONLY a valid JSON object with ALL of these exact keys.
Use empty string "" for unknown text, 0 for unknown numbers, [] for unknown arrays, null for unknown optional fields.

{{
  "person_profile": {{
    "full_name": "{name}",
    "first_name": "",
    "last_name": "",
    "current_title": "",
    "seniority_level": "",
    "department": "",
    "years_in_role": "",
    "career_history": [],
    "top_skills": [],
    "education": "",
    "certifications": [],
    "languages": [],
    "city": "",
    "country": "",
    "timezone": "",
    "linkedin_activity": "",
    "followers": 0,
    "connections": 0,
    "work_email": "",
    "personal_email": null,
    "direct_phone": null,
    "twitter": null,
    "about": ""
  }},
  "company_identity": {{
    "name": "",
    "domain": "",
    "website": "",
    "linkedin_url": ""
  }},
  "website_intelligence": {{
    "company_description": "",
    "product_offerings": [],
    "value_proposition": "",
    "target_customers": [],
    "use_cases": [],
    "business_model": "",
    "product_category": "",
    "market_positioning": "",
    "pricing_signals": "",
    "key_messaging": [],
    "hiring_signals": "",
    "open_roles": [],
    "problem_solved": ""
  }},
  "company_profile": {{
    "name": "",
    "industry": "",
    "employee_count": 0,
    "hq_location": "",
    "founded_year": null,
    "annual_revenue_est": "",
    "tech_stack": [],
    "company_logo": null,
    "company_email": null,
    "company_phone": null,
    "company_twitter": null
  }},
  "company_intelligence": {{
    "funding_stage": "",
    "total_funding": "",
    "last_funding_date": "",
    "lead_investor": "",
    "hiring_velocity": "",
    "department_hiring_trends": [],
    "news_mention": "",
    "product_launch": "",
    "competitor_usage": ""
  }},
  "intent_signals": {{
    "recent_funding_event": "",
    "hiring_signal": "",
    "job_change": "",
    "linkedin_activity": "",
    "news_mention": "",
    "product_launch": "",
    "competitor_usage": "",
    "review_activity": "",
    "pain_indicators": []
  }},
  "lead_scoring": {{
    "icp_fit_score": 0,
    "intent_score": 0,
    "timing_score": 0,
    "data_completeness_score": 0,
    "overall_score": 0,
    "score_tier": "HOT|WARM|COOL|COLD",
    "score_explanation": "",
    "icp_match_tier": "",
    "disqualification_flags": []
  }},
  "outreach": {{
    "email_subject": "",
    "cold_email": "",
    "linkedin_note": "",
    "best_channel": "",
    "best_send_time": "",
    "outreach_angle": "",
    "sequence_type": "",
    "sequence": {{
      "day1": "Send cold email",
      "day2": "LinkedIn connection request with note",
      "day4": "Follow-up email — value proof",
      "day7": "LinkedIn message or content engage",
      "day14": "Breakup email"
    }},
    "last_contacted": null,
    "email_status": ""
  }},
  "crm": {{
    "lead_source": "LinkedIn URL",
    "enrichment_source": "Bright Data + Hunter.io",
    "enrichment_date": "{now_iso}",
    "enrichment_depth": "Deep",
    "data_completeness": 0,
    "last_re_enriched": "{now_iso}",
    "assigned_owner": null,
    "crm_stage": "",
    "tags": []
  }}
}}

SCORING GUIDE:
- icp_fit_score (0-40): C-Level/Founder=35+, VP=28-35, Director=20-27, Manager=12-19, IC=0-11; company size fit adds up to 10
- intent_score (0-30): recent funding=15, active hiring surge=10, job change in 90 days=8, linkedin active=5
- timing_score (0-20): funding in last 6mo=15, job change in 30 days=12, product launch=10, blog/news active=5
- data_completeness_score (0-10): % of key fields filled (email=3, phone=1, company=2, title=1, linkedin=1, skills=1, edu=1)
- overall_score = icp + intent + timing + data_completeness (max 100)
- score_tier: HOT (80+), WARM (55-79), COOL (30-54), COLD (<30)
- icp_match_tier: "Tier 1 — Hot" / "Tier 2 — Warm" / "Tier 3 — Cool" / "Tier 4 — Cold"
- crm_stage: based on score — "MQL → Ready for outreach" / "Nurture" / "Monitor" / "Disqualify"
- data_completeness (crm section): % of fields filled (0-100)
- OUTREACH: Use website_intelligence.value_proposition and website_intelligence.problem_solved as primary angle
- Cold email: max 120 words, natural tone, reference real company context
- LinkedIn note: max 300 chars
- email_status: "Valid — verified" if hunter/apollo found it, "Pattern guess — unverified" otherwise
- tags: 3-6 relevant tags like ["SaaS","VP-Eng","Series-B","Hot","San Francisco"]"""


def _render_lio_template(template: str, vars: dict) -> str:
    """Fill {placeholders} in a LIO user_template; missing keys become empty string."""
    class _SafeDict(dict):
        def __missing__(self, key):
            return ""
    return template.format_map(_SafeDict(vars))


def _parse_list_from_llm(raw: str) -> list:
    """Extract first JSON array from LLM response."""
    try:
        m = re.search(r"\[.*\]", raw, re.DOTALL)
        if m:
            result = json.loads(m.group())
            if isinstance(result, list):
                return result
    except Exception:
        pass
    return []


async def run_lio_pipeline(
    profile: dict,
    contact: dict,
    org_id: str = "default",
) -> dict:
    """
    Run all 7 LIO stages using the org's configured prompts + workspace context.

    Stages:
      0 — Company Intelligence
      1 — Auto Tags
      2 — Behavioural Signals
      3 — Buying Signals        (needs 0 + 2)
      4 — Pitch Intelligence    (needs 0 + 2 + 3 + workspace context)
      5 — Outreach Generator    (needs 2 + 4 + workspace context)
      6 — Lead Score            (needs 2 + 3)

    Returns dict with keys:
      company_intel, tags, behavioural_signals, buying_signals,
      pitch_intelligence, outreach, lead_score
    """
    from config.enrichment_config_service import get_lio_prompts, get_workspace_context

    prompts = await get_lio_prompts(org_id)
    ctx     = await get_workspace_context(org_id)

    name    = profile.get("name", "")
    first, last = _parse_name(name)
    title   = profile.get("position") or profile.get("headline", "")
    company = profile.get("current_company_name", "")
    company_extras = profile.get("_company_extras") or {}
    industry = company_extras.get("industry", "")
    city     = profile.get("city", "") or profile.get("location", "")
    country  = profile.get("country", "")
    email    = contact.get("email") or ""

    # Compact profile for LLM — strip heavy raw fields to stay within token limits
    compact = {k: v for k, v in profile.items() if k not in ("_activity_full", "_posts")}
    raw_json = json.dumps(compact, default=str)[:6000]

    results: dict = {}

    # ── Stage 0: Company Intelligence ─────────────────────────────────────────
    p = prompts[0]
    try:
        user = _render_lio_template(p["user_template"], {"raw_brightdata_json": raw_json})
        raw  = await _call_llm(
            [{"role": "system", "content": p["system"]}, {"role": "user", "content": user}],
            max_tokens=400, temperature=p.get("temperature", 0.2), model_override=p.get("model"),
        )
        results["company_intel"] = _parse_json_from_llm(raw) if raw else {}
    except Exception as e:
        logger.warning("[LIO:0] company_intel failed: %s", e)
        results["company_intel"] = {}

    # ── Stage 1: Auto Tags ─────────────────────────────────────────────────────
    p = prompts[1]
    try:
        user = _render_lio_template(p["user_template"], {"raw_brightdata_json": raw_json})
        raw  = await _call_llm(
            [{"role": "system", "content": p["system"]}, {"role": "user", "content": user}],
            max_tokens=200, temperature=p.get("temperature", 0.3), model_override=p.get("model"),
        )
        results["tags"] = _parse_list_from_llm(raw) if raw else []
    except Exception as e:
        logger.warning("[LIO:1] auto_tags failed: %s", e)
        results["tags"] = []

    # ── Stage 2: Behavioural Signals ──────────────────────────────────────────
    p = prompts[2]
    try:
        user = _render_lio_template(p["user_template"], {"raw_brightdata_json": raw_json})
        raw  = await _call_llm(
            [{"role": "system", "content": p["system"]}, {"role": "user", "content": user}],
            max_tokens=400, temperature=p.get("temperature", 0.3), model_override=p.get("model"),
        )
        results["behavioural_signals"] = _parse_json_from_llm(raw) if raw else {}
    except Exception as e:
        logger.warning("[LIO:2] behavioural_signals failed: %s", e)
        results["behavioural_signals"] = {}

    # ── Stage 3: Buying Signals ────────────────────────────────────────────────
    p = prompts[3]
    try:
        user = _render_lio_template(p["user_template"], {
            "raw_brightdata_json":  raw_json,
            "behavioural_signals":  json.dumps(results["behavioural_signals"]),
            "company_intel":        json.dumps(results["company_intel"]),
        })
        raw  = await _call_llm(
            [{"role": "system", "content": p["system"]}, {"role": "user", "content": user}],
            max_tokens=300, temperature=p.get("temperature", 0.2), model_override=p.get("model"),
        )
        results["buying_signals"] = _parse_json_from_llm(raw) if raw else {}
    except Exception as e:
        logger.warning("[LIO:3] buying_signals failed: %s", e)
        results["buying_signals"] = {}

    # ── Stage 4: Pitch Intelligence ────────────────────────────────────────────
    p = prompts[4]
    try:
        user = _render_lio_template(p["user_template"], {
            "first_name":          first,
            "last_name":           last,
            "inferred_title":      title,
            "company":             company,
            "industry":            industry,
            "behavioural_signals": json.dumps(results["behavioural_signals"]),
            "buying_signals":      json.dumps(results["buying_signals"]),
            "company_intel":       json.dumps(results["company_intel"]),
            "product_name":        ctx.get("product_name", ""),
            "value_proposition":   ctx.get("value_proposition", ""),
            "tone":                ctx.get("tone", "professional"),
            "banned_phrases":      ctx.get("banned_phrases", ""),
        })
        raw  = await _call_llm(
            [{"role": "system", "content": p["system"]}, {"role": "user", "content": user}],
            max_tokens=500, temperature=p.get("temperature", 0.4), model_override=p.get("model"),
        )
        results["pitch_intelligence"] = _parse_json_from_llm(raw) if raw else {}
    except Exception as e:
        logger.warning("[LIO:4] pitch_intelligence failed: %s", e)
        results["pitch_intelligence"] = {}

    # ── Stage 5: Outreach Generator ────────────────────────────────────────────
    p = prompts[5]
    try:
        user = _render_lio_template(p["user_template"], {
            "first_name":          first,
            "last_name":           last,
            "inferred_title":      title,
            "company":             company,
            "city":                city,
            "country":             country,
            "pitch_intelligence":  json.dumps(results["pitch_intelligence"]),
            "behavioural_signals": json.dumps(results["behavioural_signals"]),
            "product_name":        ctx.get("product_name", ""),
            "tone":                ctx.get("tone", "professional"),
            "cta_style":           ctx.get("cta_style", "question"),
            "banned_phrases":      ctx.get("banned_phrases", ""),
            "case_study":          ctx.get("case_study", ""),
        })
        raw  = await _call_llm(
            [{"role": "system", "content": p["system"]}, {"role": "user", "content": user}],
            max_tokens=600, temperature=p.get("temperature", 0.7), model_override=p.get("model"),
        )
        results["outreach"] = _parse_json_from_llm(raw) if raw else {}
    except Exception as e:
        logger.warning("[LIO:5] outreach failed: %s", e)
        results["outreach"] = {}

    # ── Stage 6: Lead Score ────────────────────────────────────────────────────
    p = prompts[6]
    try:
        buying = results.get("buying_signals", {})
        warm_count = len(profile.get("_activity_full") or [])
        user = _render_lio_template(p["user_template"], {
            "first_name":          first,
            "last_name":           last,
            "inferred_title":      title,
            "company":             company,
            "industry":            industry,
            "icp_fit_score":       0,
            "intent_level":        buying.get("intent_level", "medium"),
            "timing_score":        buying.get("timing_score", 0),
            "warm_signal_count":   warm_count,
            "email_status":        "found" if email else "not_found",
            "company_stage":       results["company_intel"].get("company_stage", ""),
            "behavioural_signals": json.dumps(results["behavioural_signals"]),
            "buying_signals":      json.dumps(buying),
        })
        raw  = await _call_llm(
            [{"role": "system", "content": p["system"]}, {"role": "user", "content": user}],
            max_tokens=300, temperature=p.get("temperature", 0.2), model_override=p.get("model"),
        )
        results["lead_score"] = _parse_json_from_llm(raw) if raw else {}
    except Exception as e:
        logger.warning("[LIO:6] lead_score failed: %s", e)
        results["lead_score"] = {}

    logger.info(
        "[LIO Pipeline] org=%s  company_intel=%s  tags=%d  behavioural=%s  "
        "buying=%s  pitch=%s  outreach=%s  score=%s",
        org_id,
        bool(results["company_intel"]), len(results["tags"]),
        bool(results["behavioural_signals"]), bool(results["buying_signals"]),
        bool(results["pitch_intelligence"]), bool(results["outreach"]),
        bool(results["lead_score"]),
    )
    return results


async def generate_outreach_with_lio(lead: dict, org_id: str = "default") -> dict:
    """
    Generate outreach for a lead using LIO Stage 5 (Outreach Generator) system + user prompt
    from the org's LIO config.  Uses profile data already stored on the lead row.

    Returns:
        {
          "cold_email":    {"subject": ..., "body": ...},
          "linkedin_note": ...,
          "linkedin_follow_up": ...,
        }
    """
    from config.enrichment_config_service import get_lio_prompts, get_workspace_context

    prompts = await get_lio_prompts(org_id)
    ctx     = await get_workspace_context(org_id)
    p5      = prompts[5]   # Stage 5 — Outreach Generator

    # ── Build vars from stored lead fields ────────────────────────────────────
    name = lead.get("name") or ""
    first, last = _parse_name(name)
    title   = lead.get("title")   or ""
    company = lead.get("company") or ""
    city    = lead.get("city")    or ""
    country = lead.get("country") or ""

    # ── Pull enrichment fields from top-level lead columns (primary) and full_data (fallback) ──
    full = {}
    try:
        raw_fd = lead.get("full_data")
        full = json.loads(raw_fd) if isinstance(raw_fd, str) else (raw_fd or {})
    except Exception:
        pass

    def _try_json(val):
        if not val:
            return {}
        if isinstance(val, dict):
            return val
        try:
            return json.loads(val)
        except Exception:
            return {}

    # pitch_intelligence is a top-level DB column (JSON string) — preferred over full_data
    pitch_intel = (
        _try_json(lead.get("pitch_intelligence")) or
        full.get("lead_scoring", {}).get("pitch_intelligence") or
        full.get("pitch_intelligence") or
        {}
    )

    # behavioural_signals is a top-level DB column (JSON string)
    behavioural_sigs = (
        _try_json(lead.get("behavioural_signals")) or
        full.get("behavioural_signals") or
        {}
    )

    # activity_feed — top 5 posts for personalization context
    activity_feed_raw = lead.get("activity_feed") or full.get("activity_feed") or []
    if isinstance(activity_feed_raw, str):
        try:
            activity_feed_raw = json.loads(activity_feed_raw)
        except Exception:
            activity_feed_raw = []
    recent_posts = [
        a.get("title", "")[:200]
        for a in (activity_feed_raw[:5] if isinstance(activity_feed_raw, list) else [])
        if a.get("title")
    ]

    # about / bio
    about = lead.get("about") or full.get("person_profile", {}).get("about") or ""

    vars_map = {
        "first_name":          first,
        "last_name":           last,
        "inferred_title":      title,
        "company":             company,
        "city":                city,
        "country":             country,
        "about":               about[:300] if about else "",
        "recent_posts":        json.dumps(recent_posts),
        "pitch_intelligence":  json.dumps(pitch_intel),
        "behavioural_signals": json.dumps(behavioural_sigs),
        "product_name":        ctx.get("product_name", ""),
        "tone":                ctx.get("tone", "professional"),
        "cta_style":           ctx.get("cta_style", "question"),
        "banned_phrases":      ctx.get("banned_phrases", ""),
        "case_study":          ctx.get("case_study", ""),
    }

    user_prompt = _render_lio_template(p5["user_template"], vars_map)

    logger.info(
        "[LIO Outreach] Running Stage 5 for lead=%s org=%s name=%r company=%r",
        lead.get("id"), org_id, name, company,
    )

    raw = await _call_llm(
        [
            {"role": "system", "content": p5["system"]},
            {"role": "user",   "content": user_prompt},
        ],
        max_tokens=2000,
        temperature=p5.get("temperature", 0.7),
        model_override=p5.get("model"),
    )

    if not raw:
        raise RuntimeError(
            "LLM unavailable — outreach could not be generated. "
            "Check that GROQ_API_KEY or WB_LLM_HOST is configured."
        )

    result = _parse_json_from_llm(raw)
    if not result:
        raise RuntimeError(f"LLM returned unparseable response: {raw[:200]}")

    cold_email_block = result.get("cold_email", {}) or {}
    follow_up_block  = result.get("follow_up",  {}) or {}

    logger.info(
        "[LIO Outreach] Generated for lead=%s — subject=%r note_len=%d",
        lead.get("id"),
        cold_email_block.get("subject", ""),
        len(result.get("linkedin_note", "") or ""),
    )

    full_email = cold_email_block.get("full_email") or ""
    body       = cold_email_block.get("body")       or full_email  # body → full_email fallback

    return {
        "cold_email": {
            "subject":    cold_email_block.get("subject")  or "",
            "greeting":   cold_email_block.get("greeting") or "",
            "opening":    cold_email_block.get("opening")  or "",
            "body":       body,
            "cta":        cold_email_block.get("cta")      or "",
            "sign_off":   cold_email_block.get("sign_off") or "",
            "full_email": full_email or body,   # full_email → body fallback
        },
        "linkedin_note": result.get("linkedin_note") or "",
        "follow_up": {
            "day3": follow_up_block.get("day3") or "",
            "day7": follow_up_block.get("day7") or "",
        },
    }


def _merge_lio_into_enrichment(enrichment: dict, lio: dict) -> dict:
    """
    Overlay LIO pipeline outputs onto the base enrichment dict.
    LIO results take priority for: outreach, scoring, tags, signals, pitch intel.
    """
    # ── Outreach (Stage 5) ─────────────────────────────────────────────────────
    lio_out = lio.get("outreach", {})
    lio_email = lio_out.get("email", {})
    lio_linkedin = lio_out.get("linkedin", {})
    if lio_email or lio_linkedin:
        outreach = enrichment.setdefault("outreach", {})
        if lio_email.get("subject"):
            outreach["email_subject"] = lio_email["subject"]
        if lio_email.get("body"):
            outreach["cold_email"] = lio_email["body"]
        if lio_linkedin.get("connection_note"):
            outreach["linkedin_note"] = lio_linkedin["connection_note"]
        if lio_linkedin.get("follow_up"):
            outreach["linkedin_follow_up"] = lio_linkedin["follow_up"]

    # ── Lead Score (Stage 6) ───────────────────────────────────────────────────
    lio_score = lio.get("lead_score", {})
    if lio_score:
        scoring = enrichment.setdefault("lead_scoring", {})
        if lio_score.get("lead_score") is not None:
            scoring["total_score"] = lio_score["lead_score"]
        if lio_score.get("priority"):
            scoring["icp_match_tier"] = lio_score["priority"]
        if lio_score.get("reason"):
            scoring["score_explanation"] = lio_score["reason"]
        if lio_score.get("next_best_action"):
            scoring["next_best_action"] = lio_score["next_best_action"]

    # ── Pitch Intelligence (Stage 4) ──────────────────────────────────────────
    if lio.get("pitch_intelligence"):
        enrichment.setdefault("lead_scoring", {})["pitch_intelligence"] = lio["pitch_intelligence"]

    # ── Tags (Stage 1) ─────────────────────────────────────────────────────────
    if lio.get("tags"):
        enrichment.setdefault("crm", {})["tags"] = lio["tags"]

    # ── Behavioural Signals (Stage 2) ──────────────────────────────────────────
    if lio.get("behavioural_signals"):
        enrichment["behavioural_signals"] = lio["behavioural_signals"]

    # ── Buying Signals → Intent Signals (Stage 3) ──────────────────────────────
    buying = lio.get("buying_signals", {})
    if buying:
        intent = enrichment.setdefault("intent_signals", {})
        intent["intent_level"]       = buying.get("intent_level")
        intent["trigger_events"]     = buying.get("trigger_events", [])
        intent["timing_score"]       = buying.get("timing_score")
        intent["recommended_action"] = buying.get("recommended_action")

    # ── Company Intel (Stage 0) ────────────────────────────────────────────────
    if lio.get("company_intel"):
        enrichment["company_intelligence"] = lio["company_intel"]

    return enrichment


async def build_comprehensive_enrichment(
    linkedin_url: str,
    profile: dict,
    contact: dict,
    website_intel: dict = None,
    org_id: str = "default",
    system_prompt_override: Optional[str] = None,
) -> dict:
    """
    Main LLM call to produce the full 8-stage enrichment JSON.
    System prompt and model are read from the org's LIO config (workspace_configs).
    Falls back to a rule-based assembly if LLM is unavailable.
    If system_prompt_override is provided it takes precedence over the config value.
    """
    from config.enrichment_config_service import get_workspace_config, get_scoring_config

    now    = datetime.now(timezone.utc).isoformat()

    # ── LLM title inference — only when no title could be extracted ───────────
    if profile.get("_needs_title_inference"):
        name    = profile.get("name") or ""
        company = profile.get("current_company_name") or ""
        about   = (profile.get("about") or "")[:400]
        exp     = profile.get("experience") or []
        exp_ctx = ""
        if exp and isinstance(exp, list) and isinstance(exp[0], dict):
            e0 = exp[0]
            exp_ctx = f"{e0.get('title','')} at {e0.get('company','')}".strip(" at")
        try:
            inferred = await _call_llm(
                [
                    {"role": "system", "content": "You extract professional job titles. Reply with ONLY the job title — 2-6 words, no punctuation, no first-person language."},
                    {"role": "user", "content": (
                        f"Person: {name}\nCompany: {company}\nRecent role: {exp_ctx}\nAbout: {about}\n\n"
                        "What is this person's professional job title? Reply with the title only."
                    )},
                ],
                max_tokens=20,
                temperature=0.1,
            )
            if inferred and len(inferred.strip()) > 2:
                profile = dict(profile)  # don't mutate caller's dict
                profile["position"] = inferred.strip().strip('"').strip("'")
                logger.info("[TitleInfer] Generated title for %s: %s", name, profile["position"])
        except Exception as _tie:
            logger.debug("[TitleInfer] Failed: %s", _tie)

    try:
        cfg, scoring_cfg = await asyncio.gather(
            get_workspace_config(org_id),
            get_scoring_config(org_id),
            return_exceptions=True,
        )
        if isinstance(cfg, Exception):
            cfg = {}
        if isinstance(scoring_cfg, Exception):
            scoring_cfg = None
        model_override   = cfg.get("lio_model", "").strip() or None
        crm_brief_prompt = cfg.get("lio_system_prompt", "").strip()
    except Exception:
        model_override   = None
        crm_brief_prompt = ""
        scoring_cfg      = None

    logger.debug(
        "[Enrichment] org=%s  crm_brief_prompt=%s  model=%s",
        org_id,
        "custom" if crm_brief_prompt else "none",
        model_override or "default",
    )

    # Base enrichment from rule-based assembly (scoring, ICP, signals, etc.)
    data = _rule_based_enrichment(linkedin_url, profile, contact, now, website_intel=website_intel, scoring_cfg=scoring_cfg)

    # ── CRM Brief — single LLM call using lio_system_prompt + raw profile ────
    # system: lio_system_prompt from workspace_configs (set in LIO Config UI)
    # user:   raw BrightData profile JSON
    # model:  lio_model from workspace_configs (wb-pro / qwen by default)
    effective_crm_prompt = (
        system_prompt_override.strip() if (system_prompt_override and system_prompt_override.strip())
        else crm_brief_prompt
        or _DEFAULT_CRM_BRIEF_PROMPT
    )
    if effective_crm_prompt:
        try:
            # Send FULL profile — no trimming. Qwen2.5-72B has 128K context window.
            # Groq has a 6K token limit so trimming was needed there, but HF Qwen handles full data.
            _optimized = _build_llm_profile(profile)
            raw_profile_str = json.dumps(_optimized, separators=(",", ":"), ensure_ascii=False, default=str)
            crm_brief_user = f"Analyze this LinkedIn prospect data and return the JSON exactly as specified:\n\n{raw_profile_str}"
            hf_ok  = bool(_hf_token())
            grq_ok = bool(_groq_key())
            logger.info("[Enrichment] CRM brief LLM call — hf=%s groq=%s org=%s model=Qwen2.5-72B-Instruct", hf_ok, grq_ok, org_id)
            # HuggingFace (Qwen2.5-72B) first → Groq fallback
            # hf_first=True skips WB LLM and goes directly to HF router
            brief = await _call_llm([
                {"role": "system", "content": effective_crm_prompt},
                {"role": "user",   "content": crm_brief_user},
            ], max_tokens=4500, temperature=0.3, model_override=model_override, wb_llm_model_override=model_override,
               hf_first=True)
            if not brief:
                logger.warning("[Enrichment] CRM brief — all LLM providers returned None (hf=%s groq=%s)", hf_ok, grq_ok)
            else:
                import re as _re
                # Strip ALL <think>...</think> blocks (qwen3, deepseek, etc.)
                brief = _re.sub(r"<think>.*?</think>", "", brief, flags=_re.DOTALL)
                # Strip any unclosed <think> block at end
                brief = _re.sub(r"<think>.*", "", brief, flags=_re.DOTALL).strip()
                # Strip any [inferred] markers the model may have added
                brief = brief.replace("[inferred]", "").replace("[Inferred]", "")
                # Extract first complete JSON object using brace counting
                _start = brief.find("{")
                if _start != -1:
                    _depth = 0
                    _end = _start
                    for _i, _ch in enumerate(brief[_start:], _start):
                        if _ch == "{": _depth += 1
                        elif _ch == "}":
                            _depth -= 1
                            if _depth == 0:
                                _end = _i
                                break
                    brief = brief[_start:_end + 1]
                # Try to parse as JSON — store as dict if valid, else raw string
                try:
                    data["crm_brief"] = json.loads(brief)
                except Exception:
                    data["crm_brief"] = brief
                logger.info("[Enrichment] CRM brief generated (%d chars)", len(brief))
        except Exception as _be:
            logger.warning("[Enrichment] CRM brief failed: %s", _be)

    return data


def _build_llm_profile(profile: dict) -> dict:
    """
    Extract and clean all fields meaningful for CRM/LLM analysis.
    Strips pure noise (people_also_viewed, HTML duplicates, logo URLs, internal IDs)
    but keeps ALL signal content — experience descriptions, posts, activity, recommendations,
    volunteer work, languages, groups — to maximise model context utilisation.
    Qwen2.5-72B has a 128K context window; we target ~30-40K chars of signal-dense content.
    """
    # ── Experience — full descriptions, drop HTML + logo URLs + internal IDs ───────
    def _clean_exp(items):
        out = []
        for e in (items or []):
            entry = {
                "title":       e.get("title", ""),
                "company":     e.get("company", ""),
                "start_date":  e.get("start_date", ""),
                "end_date":    e.get("end_date", ""),
                "location":    e.get("location", ""),
                "description": (e.get("description") or "")[:2000],  # plain text, cap long essays
            }
            # include sub_positions (career progression within same company)
            subs = e.get("sub_positions") or []
            if subs:
                entry["sub_positions"] = [
                    {
                        "title":       s.get("title", ""),
                        "start_date":  s.get("start_date", ""),
                        "end_date":    s.get("end_date", ""),
                        "description": (s.get("description") or "")[:500],
                    }
                    for s in subs
                ]
            out.append(entry)
        return out

    # ── Activity — full titles, drop image URLs, cap at 50 items ─────────────────
    def _clean_activity(items):
        out = []
        for a in (items or [])[:50]:
            title = (a.get("title") or "").strip()
            if not title:
                continue
            out.append({
                "interaction": a.get("interaction") or "liked",
                "title":       title[:400],
                "link":        a.get("link", ""),
            })
        return out

    # ── Posts — author's own posts (strongest intent signals) ────────────────────
    def _clean_posts(items):
        out = []
        for p in (items or [])[:30]:
            text = (p.get("text") or p.get("content") or p.get("title") or "").strip()
            if not text:
                continue
            out.append({
                "text":        text[:600],
                "likes":       p.get("num_likes") or p.get("likes", 0),
                "comments":    p.get("num_comments") or p.get("comments", 0),
                "posted_at":   p.get("posted_at") or p.get("date", ""),
            })
        return out

    # ── Education — full details ──────────────────────────────────────────────────
    def _clean_edu(items):
        out = []
        for e in (items or []):
            if isinstance(e, dict):
                out.append({
                    "school":       e.get("title") or e.get("school") or e.get("name", ""),
                    "degree":       e.get("subtitle") or e.get("degree", ""),
                    "field":        e.get("field_of_study") or e.get("field", ""),
                    "dates":        f"{e.get('start_year','')}-{e.get('end_year','')}".strip("-"),
                    "description":  (e.get("description") or "")[:300],
                })
        return out

    # ── Recommendations — full text, higher cap ────────────────────────────────────
    def _clean_recs(items):
        out = []
        for r in (items or []):
            if isinstance(r, str):
                text = r
            else:
                text = r.get("description") or r.get("text") or r.get("title") or ""
            if text:
                out.append(str(text)[:600])
        return out

    # ── Volunteer work ────────────────────────────────────────────────────────────
    def _clean_volunteer(items):
        out = []
        for v in (items or []):
            if isinstance(v, dict):
                out.append({
                    "role":        v.get("role") or v.get("title", ""),
                    "org":         v.get("organization") or v.get("company", ""),
                    "cause":       v.get("cause", ""),
                    "description": (v.get("description") or "")[:300],
                })
        return out

    # ── Current company — clean, no logo URLs ────────────────────────────────────
    def _clean_company(co: dict) -> dict:
        if not co:
            return {}
        return {
            "name":        co.get("name", ""),
            "industry":    co.get("industry", ""),
            "size":        co.get("company_size") or co.get("size", ""),
            "founded":     co.get("founded", ""),
            "website":     co.get("website", ""),
            "description": (co.get("description") or "")[:500],
            "specialties": co.get("specialities") or co.get("specialties", []),
        }

    # ── Build maximally signal-dense profile ──────────────────────────────────────
    # Keys are named to match the prompt's ANALYSIS LOGIC terminology:
    #   authored_posts      → expertise, identity, authority
    #   interactions        → buying intent (liked/commented/reposted)
    #   company_context     → challenges, stage, priorities
    _current_co = profile.get("current_company") or {}
    return {
        # ── Identity ────────────────────────────────────────────────────────────
        "name":            profile.get("name", ""),
        "headline":        profile.get("headline") or profile.get("position") or profile.get("title", ""),
        "location":        profile.get("location") or profile.get("city", ""),
        "country":         profile.get("country_code") or profile.get("country", ""),
        "linkedin_url":    profile.get("url") or profile.get("input_url", ""),
        "profile_image":   profile.get("avatar_url") or profile.get("profile_pic_url", ""),
        "about":           profile.get("about") or "",
        "followers":       profile.get("followers", 0),
        "connections":     profile.get("connections", 0),
        # ── Company context → derive challenges, stage, priorities ───────────
        "company_context": {
            **_clean_company(_current_co),
            "logo": _current_co.get("logo") or _current_co.get("logo_url", ""),
        },
        # ── Career history ───────────────────────────────────────────────────
        "experience":      _clean_exp(profile.get("experience")),
        "education":       _clean_edu(profile.get("education") or []),
        "skills":          [
            (s.get("name") if isinstance(s, dict) else s)
            for s in (profile.get("skills") or [])
        ][:40],
        "certifications":  [
            (c.get("title") or str(c))[:120]
            for c in (profile.get("certifications") or [])
        ][:15],
        "languages": [
            {"lang": (l.get("title") or l.get("name") or str(l)), "level": l.get("subtitle", "")}
            if isinstance(l, dict) else {"lang": str(l)}
            for l in (profile.get("languages") or [])
        ],
        "groups":          [
            g.get("name") or g.get("title") or str(g)
            for g in (profile.get("groups") or [])
        ][:20],
        "volunteer":       _clean_volunteer(profile.get("volunteer_experience") or profile.get("volunteer") or []),
        "recommendations": _clean_recs(profile.get("recommendations")),
        # ── Authored posts → expertise, identity, authority (HIGHEST SIGNAL) ─
        "authored_posts":  _clean_posts(profile.get("posts") or profile.get("recent_posts") or []),
        # ── Interactions → buying intent (liked / commented / reposted) ──────
        "interactions":    _clean_activity(profile.get("activity")),
    }


def _trim_profile_to_budget(optimized: dict, char_budget: int) -> dict:
    """
    Progressively reduce the optimized profile until it fits within char_budget.
    Order: remove low-signal sections first, then reduce caps on high-signal ones.
    The JSON structure is never changed — only content lengths/counts are reduced.
    """
    import copy
    p = copy.deepcopy(optimized)

    steps = [
        # Step 1 — drop lowest signal sections entirely
        lambda d: d.update({"volunteer": [], "groups": [], "honors_awards": None}) or d,
        # Step 2 — halve recommendations
        lambda d: d.update({"recommendations": d.get("recommendations", [])[:3]}) or d,
        # Step 3 — halve certifications + languages
        lambda d: d.update({"certifications": d.get("certifications", [])[:5], "languages": d.get("languages", [])[:3]}) or d,
        # Step 4 — trim interactions (buying intent signals)
        lambda d: d.update({"interactions": d.get("interactions", [])[:15]}) or d,
        # Step 5 — trim authored posts (identity signals)
        lambda d: d.update({"authored_posts": d.get("authored_posts", [])[:10]}) or d,
        # Step 6 — trim experience descriptions
        lambda d: d.update({"experience": [{**e, "description": (e.get("description") or "")[:400]} for e in d.get("experience", [])]}) or d,
        # Step 7 — drop recommendations entirely
        lambda d: d.update({"recommendations": []}) or d,
        # Step 8 — trim interactions further
        lambda d: d.update({"interactions": d.get("interactions", [])[:8]}) or d,
        # Step 9 — trim authored posts further
        lambda d: d.update({"authored_posts": d.get("authored_posts", [])[:5]}) or d,
        # Step 10 — trim about
        lambda d: d.update({"about": (d.get("about") or "")[:500]}) or d,
        # Step 11 — trim experience descriptions hard
        lambda d: d.update({"experience": [{**e, "description": (e.get("description") or "")[:150]} for e in d.get("experience", [])]}) or d,
        # Step 12 — cap skills
        lambda d: d.update({"skills": d.get("skills", [])[:15]}) or d,
    ]

    for step in steps:
        current = json.dumps(p, separators=(",", ":"), ensure_ascii=False, default=str)
        if len(current) <= char_budget:
            break
        step(p)

    return p


def _rule_based_enrichment(
    linkedin_url: str, profile: dict, contact: dict, now: str,
    website_intel: dict = None,
    scoring_cfg: dict = None,
) -> dict:
    """Produce the 8-stage structure from raw data without LLM."""
    name    = profile.get("name", "")
    title   = profile.get("position") or profile.get("headline", "")
    company = profile.get("current_company_name", "")
    loc     = profile.get("location") or profile.get("city", "")
    about   = (profile.get("about") or "")[:600]
    exp     = profile.get("experience") or []
    edu     = profile.get("education") or []
    skills  = profile.get("skills") or []
    certs   = profile.get("certifications") or []
    langs   = profile.get("languages") or []
    email   = contact.get("email") or ""

    # Company extras from waterfall
    company_extras = profile.get("_company_extras") or {}

    # Website intel
    wi = website_intel or {}

    # Seniority from title
    title_l = title.lower()
    if any(k in title_l for k in ["ceo","cto","coo","cfo","chief","founder","president"]):
        seniority = "C-Level / Executive"
    elif any(k in title_l for k in ["vp","vice president"]):
        seniority = "VP / Director"
    elif "director" in title_l:
        seniority = "Director"
    elif "manager" in title_l or "head of" in title_l:
        seniority = "Manager / Head"
    elif any(k in title_l for k in ["senior","sr.","lead","principal"]):
        seniority = "Senior Individual Contributor"
    else:
        seniority = "Individual Contributor"

    # Department from title
    if any(k in title_l for k in ["engineer","tech","developer","software","data","it","cloud","devops","cto"]):
        dept = "Engineering / Technology"
    elif any(k in title_l for k in ["sales","revenue","account","bd","business development"]):
        dept = "Sales"
    elif any(k in title_l for k in ["marketing","growth","content","brand","seo","demand"]):
        dept = "Marketing"
    elif any(k in title_l for k in ["hr","people","talent","recruit","culture"]):
        dept = "People / HR"
    elif any(k in title_l for k in ["product","pm","ux","design","ui"]):
        dept = "Product"
    elif any(k in title_l for k in ["finance","cfo","account","controller","treasury"]):
        dept = "Finance"
    elif any(k in title_l for k in ["ops","operations","supply","logistics","coo"]):
        dept = "Operations"
    else:
        dept = "General"

    # Years in role from first experience entry
    years_in_role = ""
    if exp and isinstance(exp[0], dict):
        dur = exp[0].get("duration") or ""
        if dur:
            years_in_role = dur

    # Previous companies
    prev = [e.get("company", "") for e in (exp[1:4] if isinstance(exp, list) else [])]
    prev = [c for c in prev if c]

    # Education string
    edu_str = ""
    if edu and isinstance(edu[0], dict):
        e0 = edu[0]
        edu_str = f"{e0.get('degree','')}, {e0.get('school','')}".strip(", ")

    # ICP scoring (0-40)
    icp = 0
    _tw = {"ceo":35,"cto":35,"founder":35,"co-founder":35,"vp":30,"vice president":30,
           "director":22,"head of":18,"chief":35,"president":35,"owner":28,
           "senior":8,"lead":8,"principal":10,"manager":14}
    for kw, pts in _tw.items():
        if kw in title_l:
            icp += pts
            break
    icp = min(icp, 40)

    # Intent score (0-30) — dynamic from real profile signals
    intent = 0
    followers_n = _safe_int(profile.get("followers"))
    connections_n = _safe_int(profile.get("connections"))
    if email: intent += 6                        # reachable contact found
    if about: intent += 4                        # has active profile summary
    if skills: intent += 3                       # skills listed = maintained profile
    if followers_n > 5000: intent += 8
    elif followers_n > 1000: intent += 5
    elif followers_n > 200: intent += 2
    if connections_n > 500: intent += 4
    elif connections_n > 100: intent += 2
    if exp and len(exp) >= 2: intent += 3        # career history = real person
    intent = min(intent, 30)

    # Timing score (0-20) — dynamic from data availability
    timing = 0
    if email: timing += 6                        # can reach them now
    if company: timing += 4                      # confirmed company = B2B qualified
    if title: timing += 3                        # role clarity for personalization
    _ce_domain = company_extras.get("_verified_domain") or company_extras.get("company_website") or ""
    if _ce_domain: timing += 3                   # real verified domain found
    if edu_str: timing += 2                      # education = complete profile
    if prev: timing += 2                         # career progression visible
    timing = min(timing, 20)

    # Data completeness score (0-10)
    dc_score = 0
    if email: dc_score += 3
    if contact.get("phone"): dc_score += 1
    if company: dc_score += 2
    if title: dc_score += 1
    if linkedin_url: dc_score += 1
    if skills: dc_score += 1
    if edu_str: dc_score += 1
    dc_score = min(dc_score, 10)

    total = icp + intent + timing + dc_score
    _sc = scoring_cfg or {}
    _hot  = _sc.get("hot",  80)
    _warm = _sc.get("warm", 55)
    _cool = _sc.get("cool", 30)
    if total >= _hot:
        tier, stage, score_tier_str = "Tier 1 — Hot", "MQL → Ready for outreach", "HOT"
    elif total >= _warm:
        tier, stage, score_tier_str = "Tier 2 — Warm", "Nurture sequence", "WARM"
    elif total >= _cool:
        tier, stage, score_tier_str = "Tier 3 — Cool", "Awareness / Monitor", "COOL"
    else:
        tier, stage, score_tier_str = "Tier 4 — Cold", "Disqualify or low priority", "COLD"

    # Tags
    tags = []
    if "saas" in (company + title_l + about).lower():
        tags.append("SaaS")
    if icp >= 18:
        tags.append("Decision-Maker")
    if email:
        tags.append("Email-Found")
    if loc:
        country = loc.split(",")[-1].strip()
        if country:
            tags.append(country)

    # Email status
    src = contact.get("source") or ""
    email_status = ("Valid — verified" if src in ("hunter", "apollo")
                    else "Pattern guess — unverified" if src == "pattern_guess"
                    else "Not found")

    # Completeness
    filled = sum(1 for v in [name, title, company, email, loc, about, edu_str, seniority] if v)
    completeness = int((filled / 8) * 100)

    # Basic cold email — use website intel value prop if available
    fn = name.split()[0] if name else "there"
    vp = wi.get("value_proposition") or ""
    problem = wi.get("problem_solved") or ""
    category = wi.get("product_category") or dept.split("/")[0].strip()

    if vp:
        outreach_angle = f"Website intelligence: {vp[:120]}"
    elif problem:
        outreach_angle = f"Pain point: {problem[:120]}"
    else:
        outreach_angle = "Role & company alignment"

    # Rule-based fallback subject: 10-20 words, specific to role + company
    _subj_role = title[:40] if title else f"{dept.split('/')[0].strip()} leader"
    cold_email_subject = f"Quick idea for how {company} could save 10+ hours a week on prospecting"

    _body_product_line = (
        f"Worksbuddy helps {category} teams {vp[:100].lower().rstrip('.')} — cutting manual prospecting time by over 10 hours per week."
        if vp else
        f"Worksbuddy helps {dept.split('/')[0].strip()} teams automate lead research, score prospects, and launch personalised outreach — all from a single platform, saving over 10 hours per week."
    )
    _body_pain = (
        f"We've seen that {dept.split('/')[0].strip()} teams at companies like {company} often spend too much time on manual data work that could be automated."
    )
    _body_proof = (
        f"One of our customers — a team similar in size to yours — reduced their prospecting cycle by 40% within the first month of using Worksbuddy."
    )
    _body_relevance = (
        f"Given your role as {_subj_role} at {company}, I thought this might be directly relevant to what you're working on right now."
    )

    cold_email = (
        f"Hi {fn},\n\n"
        f"Came across your profile and your work as {title} at {company} immediately caught my attention.\n\n"
        f"{_body_pain} {_body_product_line} "
        f"{_body_proof} {_body_relevance}\n\n"
        f"Would you be open to a quick 15-minute call this week to see if it could work for your team?\n\n"
        f"Best,\nWorksbuddy Team"
    )

    linkedin_note = (
        f"Hi {fn} — I noticed your work at {company} and thought Worksbuddy might be relevant to your team. "
        f"We help {dept.split('/')[0].strip()} teams cut manual prospecting time significantly. "
        f"Would love to connect — are you currently exploring ways to scale outreach?"
    )[:300]

    # Build career history from experience
    career_history = []
    for e in (exp[:5] if isinstance(exp, list) else []):
        if isinstance(e, dict):
            career_history.append({
                "title": e.get("title", ""),
                "company": e.get("company", ""),
                "duration": e.get("duration", ""),
            })

    # Compose 8-stage result — also include legacy keys for backwards compatibility
    result = {
        # ── Stage 1: Person Profile ────────────────────────────────────────
        "person_profile": {
            "full_name": name,
            "first_name": name.split()[0] if name else "",
            "last_name": " ".join(name.split()[1:]) if name and len(name.split()) > 1 else "",
            "current_title": title,
            "seniority_level": seniority,
            "department": dept,
            "years_in_role": years_in_role,
            "career_history": career_history,
            "top_skills": skills if isinstance(skills, list) else [],
            # education_list → full structured list for UI display
            "education_list": edu if isinstance(edu, list) else (
                [{"school": edu_str, "degree": "", "years": ""}] if edu_str else []
            ),
            "education": edu_str,  # kept as string for scoring/DB compat
            "certifications": certs,  # full objects: {name, issuer, date, credential_url}
            "languages": langs,       # full objects: {name, proficiency}
            "city": (profile.get("city") or loc or "").split(",")[0].strip(),
            "country": _country_name(
                profile.get("country_code") or profile.get("country")
                or (loc.split(",")[-1].strip() if "," in loc else "")
            ),
            "timezone": profile.get("timezone") or "",
            "linkedin_activity": f"{profile.get('followers', 0):,} followers" if profile.get("followers") else "",
            "followers": _safe_int(profile.get("followers")),
            "connections": _safe_int(profile.get("connections")),
            "work_email": email if src in ("hunter", "apollo") else "",
            "personal_email": None,
            "direct_phone": contact.get("phone"),
            "twitter": contact.get("twitter"),
            "about": about,
        },
        # ── Stage 2: Company Identity ──────────────────────────────────────
        "company_identity": {
            "name": company,
            "domain": _extract_domain(company, company_extras.get("company_website") or profile.get("current_company_link", "")),
            "website": company_extras.get("company_website") or profile.get("current_company_link", ""),
            "linkedin_url": company_extras.get("company_linkedin") or "",
        },
        # ── Stage 3: Website Intelligence ─────────────────────────────────
        "website_intelligence": {
            "company_description": wi.get("company_description") or company_extras.get("company_description") or "",
            "product_offerings": wi.get("product_offerings") or [],
            "value_proposition": wi.get("value_proposition") or "",
            "target_customers": wi.get("target_customers") or [],
            "use_cases": wi.get("use_cases") or [],
            "business_model": wi.get("business_model") or "",
            "product_category": wi.get("product_category") or "",
            "market_positioning": wi.get("market_positioning") or "",
            "pricing_signals": wi.get("pricing_signals") or "",
            "key_messaging": wi.get("key_messaging") or [],
            "hiring_signals": wi.get("hiring_signals") or "",
            "open_roles": wi.get("open_roles") or [],
            "problem_solved": wi.get("problem_solved") or "",
        },
        # ── Stage 4: Company Profile ───────────────────────────────────────
        "company_profile": {
            "name": company,
            "industry": company_extras.get("industry") or "",
            "employee_count": company_extras.get("employee_count") or 0,
            "hq_location": company_extras.get("hq_location") or loc,
            "founded_year": company_extras.get("founded_year") or None,
            "annual_revenue_est": company_extras.get("annual_revenue") or "",
            "tech_stack": company_extras.get("tech_stack") or [],
            "company_logo": company_extras.get("company_logo") or None,
            "company_email": company_extras.get("company_email") or None,
            "company_phone": company_extras.get("company_phone") or None,
            "company_twitter": company_extras.get("company_twitter") or None,
        },
        # ── Stage 5: Market Signals (Company Intelligence) ─────────────────
        "company_intelligence": {
            "funding_stage": company_extras.get("funding_stage") or "",
            "total_funding": company_extras.get("total_funding") or "",
            "last_funding_date": "",
            "lead_investor": "",
            "hiring_velocity": company_extras.get("hiring_velocity") or "",
            "department_hiring_trends": [],
            "news_mention": "",
            "product_launch": "",
            "competitor_usage": "",
        },
        # ── Stage 6: Intent Signals ────────────────────────────────────────
        "intent_signals": {
            "recent_funding_event": "",
            "hiring_signal": company_extras.get("hiring_velocity") or "",
            "job_change": f"Currently at {company}" if company else "",
            "linkedin_activity": f"{profile.get('followers', 0):,} followers" if profile.get("followers") else "",
            "news_mention": "",
            "product_launch": "",
            "competitor_usage": "",
            "review_activity": "",
            "pain_indicators": [],
        },
        # ── Stage 7: Lead Scoring ──────────────────────────────────────────
        "lead_scoring": {
            "icp_fit_score": icp,
            "intent_score": intent,
            "timing_score": timing,
            "data_completeness_score": dc_score,
            "overall_score": total,
            "score_tier": score_tier_str,
            "score_explanation": f"{seniority} at {company} — {dept}",
            "icp_match_tier": tier,
            "disqualification_flags": [],
        },
        # ── Stage 8: Outreach ──────────────────────────────────────────────
        "outreach": {
            "email_subject": cold_email_subject,
            "cold_email": cold_email,
            "linkedin_note": linkedin_note,
            "best_channel": "Email" if email else "LinkedIn",
            "best_send_time": "Tuesday or Thursday, 1 PM recipient time",
            "outreach_angle": outreach_angle,
            "sequence_type": ("Hot Lead — 4-touch multichannel" if total >= 70
                              else "Warm — nurture sequence" if total >= 40
                              else "Cold — awareness only"),
            "sequence": {
                "day1": "Send cold email",
                "day2": "LinkedIn connection request with note",
                "day4": "Follow-up email — value proof",
                "day7": "LinkedIn message if connected",
                "day14": "Breakup email",
            },
            "last_contacted": None,
            "email_status": email_status,
        },
        # ── CRM metadata (legacy key, still needed for crm_stage/tags/completeness)
        "crm": {
            "lead_source": "LinkedIn URL",
            "enrichment_source": f"Bright Data + {src.title() if src else 'Waterfall'}",
            "enrichment_date": now,
            "enrichment_depth": "Deep",
            "data_completeness": completeness,
            "last_re_enriched": now,
            "assigned_owner": None,
            "crm_stage": stage,
            "tags": tags,
        },
        # ── Legacy compatibility keys ──────────────────────────────────────
        "identity": {
            "full_name": name,
            "work_email": email if src in ("hunter", "apollo") else "",
            "personal_email": None,
            "direct_phone": contact.get("phone"),
            "linkedin_url": linkedin_url,
            "twitter": contact.get("twitter"),
            "city": (profile.get("city") or loc or "").split(",")[0].strip(),
            "country": _country_name(
                profile.get("country_code") or profile.get("country")
                or (loc.split(",")[-1].strip() if "," in loc else "")
            ),
            "timezone": profile.get("timezone") or "",
        },
        "professional": {
            "current_title": title,
            "seniority_level": seniority,
            "department": dept,
            "years_in_role": years_in_role,
            "career_history": career_history,
            "previous_companies": prev,
            "top_skills": skills if isinstance(skills, list) else [],
            "education_list": edu if isinstance(edu, list) else (
                [{"school": edu_str, "degree": "", "years": ""}] if edu_str else []
            ),
            "education": edu_str,
            "certifications": certs,  # full objects: {name, issuer, date, credential_url}
            "languages": langs,       # full objects: {name, proficiency}
        },
        "company": {
            "name": company,
            "website": company_extras.get("company_website") or profile.get("current_company_link", ""),
            "industry": company_extras.get("industry") or "",
            "employee_count": company_extras.get("employee_count") or 0,
            "hq_location": company_extras.get("hq_location") or loc,
            "founded_year": company_extras.get("founded_year") or None,
            "funding_stage": company_extras.get("funding_stage") or "",
            "total_funding": company_extras.get("total_funding") or "",
            "last_funding_date": "",
            "lead_investor": "",
            "annual_revenue_est": company_extras.get("annual_revenue") or "",
            "tech_stack": company_extras.get("tech_stack") or [],
            "hiring_velocity": company_extras.get("hiring_velocity") or "",
        },
        "scoring": {
            "icp_fit_score": icp,
            "intent_score": intent,
            "timing_score": timing,
            "overall_score": total,
            "score_explanation": f"{seniority} at {company} — {dept}",
            "icp_match_tier": tier,
            "disqualification_flags": [],
        },
    }
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_name(full_name: str) -> tuple[str, str]:
    parts = (full_name or "").split()
    return parts[0] if parts else "", " ".join(parts[1:]) if len(parts) > 1 else ""


# ISO 3166-1 alpha-2 → full country name (most common codes for B2B lead enrichment)
_COUNTRY_CODES: dict[str, str] = {
    "AF": "Afghanistan", "AL": "Albania", "DZ": "Algeria", "AR": "Argentina",
    "AU": "Australia", "AT": "Austria", "BE": "Belgium", "BR": "Brazil",
    "CA": "Canada", "CL": "Chile", "CN": "China", "CO": "Colombia",
    "CZ": "Czech Republic", "DK": "Denmark", "EG": "Egypt", "FI": "Finland",
    "FR": "France", "DE": "Germany", "GH": "Ghana", "GR": "Greece",
    "HK": "Hong Kong", "HU": "Hungary", "IN": "India", "ID": "Indonesia",
    "IE": "Ireland", "IL": "Israel", "IT": "Italy", "JP": "Japan",
    "KE": "Kenya", "KR": "South Korea", "MY": "Malaysia", "MX": "Mexico",
    "MA": "Morocco", "NL": "Netherlands", "NZ": "New Zealand", "NG": "Nigeria",
    "NO": "Norway", "PK": "Pakistan", "PE": "Peru", "PH": "Philippines",
    "PL": "Poland", "PT": "Portugal", "RO": "Romania", "RU": "Russia",
    "SA": "Saudi Arabia", "ZA": "South Africa", "ES": "Spain", "SE": "Sweden",
    "CH": "Switzerland", "TW": "Taiwan", "TH": "Thailand", "TR": "Turkey",
    "UA": "Ukraine", "AE": "United Arab Emirates", "GB": "United Kingdom",
    "US": "United States", "VN": "Vietnam",
}


def _country_name(code: str) -> str:
    """Map ISO country code to full name; return as-is if already a full name or unknown."""
    if not code:
        return ""
    code = code.strip()
    # If it's a 2-letter uppercase code, look it up
    if len(code) <= 3 and code.upper() == code:
        return _COUNTRY_CODES.get(code.upper(), code)
    return code


def _lead_id(linkedin_url: str) -> str:
    # Hash only the profile slug (/in/username) so all URL variants map to the same lead:
    # www vs no-www, country domains, query params, trailing slashes, protocol — all ignored
    m = re.search(r"/in/([^/?#\s]+)", linkedin_url.strip(), re.IGNORECASE)
    slug = m.group(1).lower().rstrip("/") if m else linkedin_url.strip().lower()
    return hashlib.md5(slug.encode()).hexdigest()[:16]


def _safe_json(v) -> str:
    if isinstance(v, (dict, list)):
        return json.dumps(v)
    return str(v) if v else ""


def _safe_int(v, max_val: int = 2_000_000_000) -> int:
    """
    Safely convert any BD field value to a SQLite-safe integer.

    Handles:
    - Numeric strings with commas/plus signs: "1,234", "500+"
    - Range strings — takes the lower bound: "10,001-50,000" → 10001
    - Dicts/lists (employee range objects) — take first numeric value found
    - Very large numbers — capped at max_val (default 2 billion)
    - None / empty string → 0
    """
    if v is None:
        return 0
    # If already a plain int/float, just cap it
    if isinstance(v, (int, float)):
        return min(int(v), max_val)
    if isinstance(v, dict):
        # BD sometimes returns employee ranges as {"start": 10001, "end": 50000}
        for k in ("start", "from", "min", "low", "count"):
            if k in v:
                return _safe_int(v[k], max_val)
        # Fall back to first numeric value in dict
        for val in v.values():
            if isinstance(val, (int, float)):
                return min(int(val), max_val)
        return 0
    if isinstance(v, list):
        return _safe_int(v[0], max_val) if v else 0
    # String handling
    s = str(v).strip()
    if not s:
        return 0
    # Take only the first numeric segment (handles "10,001-50,000" → "10001")
    # Find the first contiguous block of digits (and optional leading comma separators)
    m = re.search(r"[\d,]+", s)
    if not m:
        return 0
    digits = m.group(0).replace(",", "")
    try:
        return min(int(digits), max_val)
    except (ValueError, OverflowError):
        return 0


def _resolve_tier(tier_raw: str) -> str:
    """Normalize score tier to lowercase hot/warm/cool/cold for DB storage."""
    t = (tier_raw or "").lower()
    if "hot" in t or t == "hot":
        return "hot"
    if "warm" in t or t == "warm":
        return "warm"
    if "cool" in t or t == "cool":
        return "cool"
    return "cold"


def _patch_crm_brief_images(crm_brief: Optional[dict], avatar_url: str, company_logo: str) -> Optional[dict]:
    """Inject profile_image + company_logo into crm_brief.who_they_are before saving to DB.
    LLM never has real image URLs — must be patched from BD profile data.
    """
    if not crm_brief or not isinstance(crm_brief, dict):
        return crm_brief
    who = crm_brief.get("who_they_are")
    if isinstance(who, dict):
        if not who.get("profile_image") and avatar_url:
            who["profile_image"] = avatar_url
        if not who.get("company_logo") and company_logo:
            who["company_logo"] = company_logo
    return crm_brief


_SCORE_LABEL_MAP = {
    "very high": 90, "high": 75, "medium high": 65,
    "medium": 50,    "low medium": 35,
    "low": 20,       "very low": 10,
}

def _patch_crm_brief_scores(crm_brief: Optional[dict],
                             icp_fit_score: int = 0,
                             intent_score: int = 0,
                             timing_score: int = 0) -> Optional[dict]:
    """Normalize crm_scores to integers before saving to DB.
    LLM sometimes returns 'High', 'Very High' etc. instead of 0-100 integers.
    Falls back to DB-calculated scores when LLM returned 0.
    """
    if not crm_brief or not isinstance(crm_brief, dict):
        return crm_brief

    scores = crm_brief.get("crm_scores")
    if not isinstance(scores, dict):
        return crm_brief

    def _to_int(v) -> int:
        if isinstance(v, int):
            return v
        s = str(v).lower().replace("%", "").strip()
        if s in _SCORE_LABEL_MAP:
            return _SCORE_LABEL_MAP[s]
        try:
            return int(float(s))
        except Exception:
            return 0

    for field in ("icp_fit", "engagement_score", "timing_score"):
        scores[field] = _to_int(scores.get(field))

    # Fallback to DB scoring when LLM returned 0
    if not scores.get("icp_fit"):
        scores["icp_fit"] = icp_fit_score
    if not scores.get("engagement_score"):
        scores["engagement_score"] = intent_score
    if not scores.get("timing_score"):
        scores["timing_score"] = timing_score

    return crm_brief


def _is_company_url(url: str) -> bool:
    return "linkedin.com/company/" in url.lower()


def _is_person_url(url: str) -> bool:
    return "linkedin.com/in/" in url.lower()


# ─────────────────────────────────────────────────────────────────────────────
# Company URL direct enrichment pipeline
# ─────────────────────────────────────────────────────────────────────────────

async def enrich_company_url(
    linkedin_url: str,
    job_id: Optional[str] = None,
    org_id: str = "default",
    forward_to_lio: bool = False,
) -> dict:
    """
    Enrich a LinkedIn Company URL directly.

    Phase A — Bright Data company dataset scrape
    Phase B — Apollo company name/website search (fills gaps)
    Phase C — Hunter.io domain search (company emails, links)
    Phase D — Website intelligence (deep scrape)
    Phase E — Score + store

    Returns the same flat DB dict as enrich_single() so the same
    UI and API response shape is used for both person and company enrichment.
    """
    url = linkedin_url.strip().rstrip("/")
    if not url.startswith("http"):
        url = f"https://{url}"
    lead_id = _lead_id(url)
    now = datetime.now(timezone.utc).isoformat()

    logger.info("━━━ [Company-A] Bright Data company scrape: %s", url)
    bd_co = await _try_bd_company(url)

    company_name = (
        bd_co.get("name") or bd_co.get("company_name")
        or url.rstrip("/").split("/")[-1].replace("-", " ").title()
    )
    website = bd_co.get("company_website", "")
    domain = ""
    if website:
        m = re.search(r"https?://(?:www\.)?([^/?\s]+)", website)
        if m:
            domain = m.group(1).lower()

    logger.info("[Company-A] BD result: name=%r  website=%r  domain=%r  fields=%d",
                company_name, website, domain, len(bd_co))

    # ── Phase B: Apollo fill-in ───────────────────────────────────────────────
    logger.info("━━━ [Company-B] Apollo enrichment for: %r (domain=%s)", company_name, domain)
    apollo_data: dict = {}

    # Try by domain first (if BD gave us one)
    if domain:
        apollo_data = await _try_apollo_org(domain) or {}
        if apollo_data:
            logger.info("[Company-B] Apollo by domain OK — %d fields", len(apollo_data))

    # Fallback: search by name
    if not apollo_data and company_name:
        apollo_data = await _try_apollo_company_search(company_name) or {}
        if apollo_data:
            # Extract verified domain from Apollo result
            ap_web = apollo_data.get("company_website", "")
            if ap_web:
                m2 = re.search(r"https?://(?:www\.)?([^/?\s]+)", ap_web)
                if m2 and m2.group(1).lower() not in _SOCIAL_DOMAINS:
                    domain = m2.group(1).lower()
                    website = ap_web
            logger.info("[Company-B] Apollo by name OK — %d fields, domain=%s", len(apollo_data), domain)

    # Merge: BD takes priority, Apollo fills missing fields
    company_data: dict = {}
    company_data.update(apollo_data)
    company_data.update({k: v for k, v in bd_co.items() if v})

    # Re-sync website/domain from merged data
    if company_data.get("company_website") and not website:
        website = company_data["company_website"]
        m3 = re.search(r"https?://(?:www\.)?([^/?\s]+)", website)
        if m3:
            domain = m3.group(1).lower()

    # ── Phase C: Hunter domain search ─────────────────────────────────────────
    logger.info("━━━ [Company-C] Hunter domain search: %s", domain)
    company_email = company_data.get("company_email", "")
    if domain and not company_email:
        try:
            async with httpx.AsyncClient(timeout=20) as c:
                r = await c.get(
                    "https://api.hunter.io/v2/domain-search",
                    params={"domain": domain, "api_key": _hunter_api_key(), "limit": 3},
                )
                hd = r.json().get("data", {})
                if hd.get("organization"):
                    company_data.setdefault("company_name_verified", hd["organization"])
                emails = hd.get("emails", [])
                if emails:
                    company_email = emails[0].get("value", "")
                    company_data["company_email"] = company_email
                    logger.info("[Company-C] Hunter found email: %s", company_email)
        except Exception as e:
            logger.debug("[Company-C] Hunter error: %s", e)

    # ── Phase D: Website intelligence ─────────────────────────────────────────
    logger.info("━━━ [Company-D] Website intelligence: %s", website or "none")
    website_intel = await scrape_website_intelligence(website) if website else {}
    if website_intel:
        logger.info("[Company-D] Scraped %d pages, category=%s",
                    len(website_intel.get("pages_scraped", [])),
                    website_intel.get("product_category") or "?")

    # ── Phase E: Score + build record ─────────────────────────────────────────
    logger.info("━━━ [Company-E] Scoring + building record")

    final_name = (
        company_data.get("name") or company_data.get("company_name")
        or apollo_data.get("name") or company_name
    )

    # Company completeness score (0-100)
    completeness_fields = [
        website, company_email,
        company_data.get("company_description"),
        company_data.get("company_logo"),
        company_data.get("industry"),
        str(company_data.get("employee_count") or ""),
        company_data.get("hq_location"),
        company_data.get("founded_year"),
        website_intel.get("product_category"),
        website_intel.get("value_proposition"),
    ]
    data_completeness = sum(10 for f in completeness_fields if f)

    # ICP / intent scoring for company leads
    icp_score = 0
    if company_data.get("industry"): icp_score += 8
    if company_data.get("employee_count", 0) > 10: icp_score += 6
    if company_data.get("employee_count", 0) > 100: icp_score += 4
    if website: icp_score += 6
    if website_intel.get("product_category"): icp_score += 8
    if website_intel.get("value_proposition"): icp_score += 4
    if company_data.get("funding_stage"): icp_score += 4
    icp_score = min(icp_score, 40)

    intent_score = 0
    if company_email: intent_score += 10
    if website_intel.get("pages_scraped"): intent_score += 8
    if company_data.get("company_description"): intent_score += 6
    if company_data.get("tech_stack"): intent_score += 6
    intent_score = min(intent_score, 30)

    timing_score = 0
    if domain: timing_score += 8
    if company_data.get("founded_year"): timing_score += 4
    if company_data.get("funding_stage"): timing_score += 5
    if company_data.get("hq_location"): timing_score += 3
    timing_score = min(timing_score, 20)

    dc_score = min(data_completeness, 10)
    total_score = icp_score + intent_score + timing_score + dc_score
    score_tier = _resolve_tier(
        "hot" if total_score >= 70 else "warm" if total_score >= 50 else "cool" if total_score >= 30 else "cold"
    )

    # Waterfall log
    waterfall_log = []
    if bd_co:
        waterfall_log.append({"step": 0, "source": "Bright Data Company", "fields_found": list(bd_co.keys()), "note": url})
    if apollo_data:
        waterfall_log.append({"step": 1, "source": "Apollo.io", "fields_found": list(apollo_data.keys()), "note": domain})
    if company_email:
        waterfall_log.append({"step": 2, "source": "Hunter.io", "fields_found": ["company_email"], "note": domain})
    if website_intel:
        waterfall_log.append({"step": 3, "source": "Website Scrape", "fields_found": website_intel.get("pages_scraped", []), "note": website})

    wi = website_intel or {}
    lead: dict = {
        "id": lead_id,
        "linkedin_url": url,
        # Identity — use company name as "person" name so UI renders it
        "name": final_name,
        "first_name": final_name,
        "last_name": "",
        "work_email": company_email or "",
        "personal_email": "",
        "direct_phone": company_data.get("company_phone", ""),
        "twitter": company_data.get("company_twitter", ""),
        "city": (company_data.get("hq_location") or "").split(",")[0].strip(),
        "country": (company_data.get("hq_location") or "").split(",")[-1].strip(),
        "timezone": "",
        # Professional — company-level context
        "title": wi.get("value_proposition") or company_data.get("company_description", "")[:100] or "Company",
        "seniority_level": "Company Account",
        "department": company_data.get("industry", ""),
        "years_in_role": "",
        "company": final_name,
        "previous_companies": "[]",
        "top_skills": _safe_json(company_data.get("tech_stack") or []),
        "education": "",
        "certifications": "[]",
        "languages": "[]",
        # Company
        "company_website": website or "",
        "industry": company_data.get("industry", ""),
        "employee_count": _safe_int(company_data.get("employee_count")),
        "hq_location": company_data.get("hq_location", ""),
        "founded_year": str(company_data.get("founded_year") or ""),
        "funding_stage": company_data.get("funding_stage", ""),
        "total_funding": company_data.get("total_funding", ""),
        "last_funding_date": company_data.get("last_funding_date", ""),
        "lead_investor": company_data.get("lead_investor", ""),
        "annual_revenue": "",
        "tech_stack": _safe_json(company_data.get("tech_stack") or []),
        "hiring_velocity": "",
        # Company enrichment fields
        "avatar_url": company_data.get("company_logo", ""),
        "company_logo": company_data.get("company_logo", ""),
        "company_email": company_email or "",
        "company_description": company_data.get("company_description", ""),
        "company_linkedin": url,
        "company_twitter": company_data.get("company_twitter", ""),
        "company_phone": company_data.get("company_phone", ""),
        "waterfall_log": _safe_json(waterfall_log),
        # Website intelligence
        "website_intelligence": _safe_json(wi),
        "product_offerings": _safe_json(wi.get("product_offerings") or []),
        "value_proposition": wi.get("value_proposition", ""),
        "target_customers": wi.get("target_customers", ""),
        "business_model": wi.get("business_model", ""),
        "pricing_signals": wi.get("pricing_signals", ""),
        "product_category": wi.get("product_category", ""),
        "data_completeness_score": dc_score,
        # Intent (company-level)
        "recent_funding_event": company_data.get("funding_stage", ""),
        "hiring_signal": "",
        "job_change": "",
        "linkedin_activity": "",
        "news_mention": "",
        "product_launch": "",
        "competitor_usage": "",
        "review_activity": "",
        # Scoring
        "icp_fit_score": icp_score,
        "intent_score": intent_score,
        "timing_score": timing_score,
        "engagement_score": dc_score,
        "total_score": total_score,
        "score_tier": score_tier,
        "score_explanation": f"Company account: ICP={icp_score} Intent={intent_score} Timing={timing_score} Data={dc_score}",
        "icp_match_tier": score_tier,
        "disqualification_flags": "[]",
        "score_reasons": _safe_json([
            f"ICP fit: {icp_score}/40",
            f"Intent signals: {intent_score}/30",
            f"Timing: {timing_score}/20",
            f"Data completeness: {dc_score}/10",
        ]),
        # Outreach — no person-specific copy for company records
        "email_subject": "",
        "cold_email": "",
        "linkedin_note": "",
        "best_channel": "email" if company_email else "linkedin",
        "best_send_time": "",
        "outreach_angle": wi.get("value_proposition", ""),
        "sequence_type": "",
        "outreach_sequence": "{}",
        # CRM
        "lead_source": "LinkedIn Company URL",
        "enrichment_source": "brightdata_company",
        "data_completeness": data_completeness,
        "crm_stage": "researching",
        "tags": _safe_json(["company-account"]),
        "assigned_owner": "",
        # Meta
        "full_data": _safe_json({"company": company_data, "website_intel": wi}),
        "raw_profile": _safe_json(bd_co),
        "about": company_data.get("company_description", ""),
        "followers": _safe_int(company_data.get("followers_count") or company_data.get("employee_count")),
        "connections": 0,
        "email_source": "hunter" if company_email else "",
        "email_confidence": "medium" if company_email else "",
        "status": "enriched",
        "job_id": job_id or "",
        "organization_id": org_id,
        "enriched_at": now,
        "created_at": now,
        "skills": "[]",
    }

    await _upsert_lead(lead)
    logger.info(
        "✓ Company enriched: %s | score=%d (%s) | email=%s | website=%s",
        final_name, total_score, score_tier, company_email or "—", website or "—",
    )
    if not forward_to_lio and lead.get("name"):
        asyncio.create_task(send_to_lio(lead))
    return lead


# ─────────────────────────────────────────────────────────────────────────────
# Main enrichment pipeline
# ─────────────────────────────────────────────────────────────────────────────

async def enrich_single(
    linkedin_url: str,
    engagement_data: Optional[dict] = None,
    job_id: Optional[str] = None,
    org_id: str = "default",
    generate_outreach_flag: bool = True,
    tools: Optional[dict] = None,
    sso_id: str = "",
    forward_to_lio: bool = False,
    system_prompt: Optional[str] = None,
    skip_contact: bool = False,
) -> dict:
    """
    Sequential 8-stage enrichment waterfall:

    Phase A — Person Intelligence
      Stage 1: Bright Data LinkedIn person profile scrape
      Stage 2: Extract all person fields (name, title, skills, education, career)

    Phase B — Company Identification (using person profile data)
      Stage 3: Bright Data LinkedIn Company page scrape (company_link from profile)
      Stage 4: Apollo company name search → verify real domain + logo + description
      Stage 5: Hunter.io domain search → company emails, social links
      Stage 6: Clearbit logo fallback

    Phase C — Person Contact Waterfall (now with VERIFIED company domain)
      Stage 7: Hunter.io person email finder (correct domain)
      Stage 8: Apollo.io person match (correct domain)

    Phase D — Deep Intelligence
      Stage 9: Website scrape (verified real company URL)
      Stage 10: Lead scoring + outreach generation
    """
    url = _normalize_linkedin_url(linkedin_url)
    lead_id = _lead_id(url)

    # ── Route: company URL → dedicated company pipeline ───────────────────────
    if _is_company_url(url):
        logger.info("[Enrich] Company URL detected — routing to company pipeline: %s", url)
        return await enrich_company_url(url, job_id=job_id, org_id=org_id, forward_to_lio=forward_to_lio)

    # ── Cache check: return existing enrichment if already in DB ─────────────
    cached = await get_lead(lead_id)
    if cached and cached.get("status") == "enriched":
        logger.info("[Enrich] Cache HIT for %s (lead_id=%s) — skipping Bright Data", url, lead_id)
        cached["_cache_hit"] = True
        cached["linkedin_enrich"] = _format_linkedin_enrich(cached, include_contact=not skip_contact)
        if not forward_to_lio:
            asyncio.create_task(send_to_lio(cached, sso_id=sso_id))
        return cached

    # ── In-flight deduplication: same URL already being processed by another request ──
    if lead_id in _in_flight_leads:
        logger.info("[Enrich] URL %s already in-flight — waiting for first request to finish", url)
        await _in_flight_leads[lead_id].wait()
        # First request done — read result from DB
        result = await get_lead(lead_id)
        if result and result.get("status") == "enriched":
            result["_cache_hit"] = True
            result["linkedin_enrich"] = _format_linkedin_enrich(result, include_contact=not skip_contact)
            if not forward_to_lio:
                asyncio.create_task(send_to_lio(result, sso_id=sso_id))
            return result
        # First request failed — fall through and try own enrichment

    # Register this URL as in-flight so concurrent requests wait instead of duplicating
    _in_flight_event = asyncio.Event()
    _in_flight_leads[lead_id] = _in_flight_event

    logger.info("[Enrich] Cache MISS for %s — fetching from Bright Data", url)

    # ── Tool availability (from enrichment config) ────────────────────────────
    _tools = tools or {}
    _brightdata_ok = _tools.get("brightdata", True)
    _hunter_ok     = _tools.get("hunter",     True)
    _apollo_ok     = _tools.get("apollo",      True)

    if not _brightdata_ok:
        logger.warning("[Enrich] Bright Data disabled or out of credits for org=%s — aborting", org_id)
        return {"error": "Bright Data is disabled or has no credits remaining for this organisation. Please update your enrichment configuration.", "linkedin_url": linkedin_url}

    # ── Phase A: Person Intelligence ──────────────────────────────────────────
    logger.info("━━━ [Phase A] Person Intelligence — scraping: %s", url)
    profile = await fetch_profile_sync(url)

    # Bright Data account/auth error — abort without saving incomplete data to DB
    if profile.get("_bd_error"):
        msg = profile.get("_bd_message", "Bright Data account error")
        logger.error("[Enrich] Aborting enrichment for %s — BD error: %s", url, msg)
        return {"error": f"Bright Data account error: {msg}", "linkedin_url": linkedin_url}

    name    = profile.get("name", "")
    first, last = _parse_name(name)
    company = profile.get("current_company_name", "")
    raw_link = profile.get("current_company_link", "")

    logger.info("[Phase A] Got: name=%r  title=%r  company=%r",
                name, profile.get("position") or profile.get("headline", ""), company)

    # Initial domain guess — may be wrong, will be corrected by Phase B
    initial_domain = _extract_domain(company, raw_link)
    logger.info("[Phase A] Initial domain guess: %s (from company name %r)", initial_domain, company)

    # ── Phase B: Company Identification Waterfall ─────────────────────────────
    logger.info("━━━ [Phase B] Company Waterfall — identifying real domain for: %s", company)
    company_extras, company_wf_log = await enrich_company_waterfall(company, initial_domain, profile)

    verified_domain = company_extras.pop("_verified_domain", initial_domain) or initial_domain
    real_website = company_extras.get("company_website") or (f"https://{verified_domain}" if verified_domain else "")
    logger.info("[Phase B] Verified domain: %s  website: %s  logo: %s",
                verified_domain, real_website, "✓" if company_extras.get("company_logo") else "✗")

    # ── Phases C + 1C + D in parallel (all independent once verified_domain is known) ──
    logger.info("━━━ [Phase C+1C+D] Running contact / company / website in parallel")

    activity_emails = profile.get("_activity_emails") or []
    activity_phones = profile.get("_activity_phones") or []

    async def _phase_c() -> dict:
        if skip_contact:
            return {}
        if activity_emails:
            matched = next((e for e in activity_emails if verified_domain and verified_domain.split(".")[0] in e), None)
            pre_contact = matched or activity_emails[0]
            result = {"email": pre_contact, "source": "linkedin_activity", "confidence": "high"}
            if activity_phones:
                result["phone"] = activity_phones[0]
            return result
        c = await find_contact_info(
            first, last, verified_domain, linkedin_url=url,
            skip_hunter=not _hunter_ok,
            skip_apollo=not _apollo_ok,
        )
        if not c.get("phone") and activity_phones:
            c["phone"] = activity_phones[0]
        return c

    async def _phase_1c() -> tuple[dict, str, int]:
        if not (raw_link and "linkedin.com/company/" in raw_link):
            return {}, "", 0
        try:
            import company_service as _cs
            from workspace import workspace_service as _ws_svc
            _ws_cfg = await _ws_svc.get_workspace_config(org_id)
            _known = {
                "name":              company or company_extras.get("company_name") or "",
                "domain":            verified_domain,
                "website":           real_website,
                "logo":              company_extras.get("company_logo") or "",
                "description":       company_extras.get("company_description") or "",
                "industry":          company_extras.get("industry") or "",
                "employee_count":    company_extras.get("employee_count") or 0,
                "hq_location":       company_extras.get("hq_location") or "",
                "founded_year":      company_extras.get("founded_year") or "",
                "funding_stage":     company_extras.get("funding_stage") or "",
                "total_funding":     company_extras.get("total_funding") or "",
                "last_funding_date": company_extras.get("last_funding_date") or "",
                "lead_investor":     company_extras.get("lead_investor") or "",
                "company_twitter":   company_extras.get("company_twitter") or "",
                "company_email":     company_extras.get("company_email") or "",
                "company_phone":     company_extras.get("company_phone") or "",
                "linkedin_followers": company_extras.get("linkedin_followers") or 0,
                "company_slogan":    company_extras.get("company_slogan") or "",
                "organization_type": company_extras.get("organization_type") or "",
            }
            rec = await _cs.enrich_company(
                company_linkedin_url=raw_link,
                org_id=org_id,
                ws_config=_ws_cfg,
                known_data=_known,
            )
            return rec, rec.get("id") or "", int(rec.get("company_score") or 0)
        except Exception as _ce:
            logger.warning("[Phase 1C] Company enrichment failed: %s", _ce)
            return {}, "", 0

    (contact, (_company_record, _company_id_val, _company_score), website_intel) = await asyncio.gather(
        _phase_c(),
        _phase_1c(),
        scrape_website_intelligence(real_website),
    )
    _combined_score: int = 0

    logger.info("[Phase C] Contact: email=%s  source=%s  phone=%s",
                contact.get("email") or "not found",
                contact.get("source") or "—",
                contact.get("phone") or "none")
    if _company_record:
        logger.info("[Phase 1C] Company done: score=%d tier=%s",
                    _company_score, _company_record.get("company_score_tier", "?"))
    if website_intel:
        logger.info("[Phase D] Scraped %d pages, category=%s, biz_model=%s",
                    len(website_intel.get("pages_scraped", [])),
                    website_intel.get("product_category") or "?",
                    website_intel.get("business_model") or "?")

    logger.info("━━━ [Phase E] Scoring + Outreach — building 8-stage report")

    # Inject company extras into profile so rule-based enrichment can access them
    profile["_company_extras"] = company_extras

    # Add website scraping step to waterfall log
    if website_intel and website_intel.get("pages_scraped"):
        company_wf_log.append({
            "step": 5, "source": "Website Scrape",
            "fields_found": website_intel.get("pages_scraped", []),
            "note": f"pages={len(website_intel.get('pages_scraped', []))}, category={website_intel.get('product_category', '?')}",
        })

    # Step 7: Comprehensive LLM enrichment → 8-stage JSON + LIO pipeline
    logger.info("[Stage 7] Scoring — LLM enrichment starting")
    enrichment = await build_comprehensive_enrichment(url, profile, contact, website_intel=website_intel, org_id=org_id, system_prompt_override=system_prompt)

    logger.info("[Stage 8] Outreach — generating personalized sequence")

    # Patch person_profile email/phone from waterfall (authoritative)
    person_prof = enrichment.get("person_profile", {})
    ident = enrichment.get("identity", {})  # legacy compat
    if contact.get("email"):
        person_prof["work_email"] = contact["email"]
        ident["work_email"] = contact["email"]
    if contact.get("phone"):
        person_prof["direct_phone"] = contact["phone"]
        ident["direct_phone"] = contact["phone"]

    # Patch company_profile section with waterfall extras (fill missing fields only)
    comp_prof = enrichment.get("company_profile", {})
    comp_section = enrichment.get("company", {})  # legacy compat
    for k, v in company_extras.items():
        _map = {
            "company_logo": "company_logo",
            "company_email": "company_email",
            "company_phone": "company_phone",
            "company_twitter": "company_twitter",
            "company_website": None,
            "industry": "industry",
            "employee_count": "employee_count",
            "hq_location": "hq_location",
            "founded_year": "founded_year",
            "funding_stage": None,
            "total_funding": None,
            "annual_revenue": "annual_revenue_est",
            "tech_stack": "tech_stack",
            "hiring_velocity": None,
        }
        _comp_section_map = {
            "company_website": "website",
            "industry": "industry",
            "employee_count": "employee_count",
            "hq_location": "hq_location",
            "founded_year": "founded_year",
            "funding_stage": "funding_stage",
            "total_funding": "total_funding",
            "annual_revenue": "annual_revenue_est",
            "tech_stack": "tech_stack",
            "hiring_velocity": "hiring_velocity",
        }
        prof_key = _map.get(k)
        if prof_key and v and not comp_prof.get(prof_key):
            comp_prof[prof_key] = v
        sect_key = _comp_section_map.get(k)
        if sect_key and v and not comp_section.get(sect_key):
            comp_section[sect_key] = v

    # Pull out scoring for flat DB columns
    scoring = enrichment.get("lead_scoring", enrichment.get("scoring", {}))
    total   = scoring.get("overall_score", 0)
    tier_raw = scoring.get("icp_match_tier") or scoring.get("score_tier", "")
    score_tier = _resolve_tier(tier_raw)

    # Derive crm from enrichment (may live in "crm" key)
    crm = enrichment.get("crm", {})

    # Build waterfall log combining all steps
    waterfall_log = [
        {"step": 0, "source": "Bright Data", "fields_found": ["profile"], "note": f"LinkedIn scrape: {url}"},
        *company_wf_log,
        {
            "step": 10,
            "source": contact.get("source") or "none",
            "fields_found": ["email"],
            "note": f"email={contact.get('email')}",
        },
    ]

    # Convenience shortcuts into 8-stage keys
    wi = enrichment.get("website_intelligence", website_intel or {})
    ls = enrichment.get("lead_scoring", scoring)
    intent_s = enrichment.get("intent_signals", {})
    outreach = enrichment.get("outreach", {})
    ci = enrichment.get("company_identity", {})
    cp = enrichment.get("company_profile", comp_prof)
    cin = enrichment.get("company_intelligence", {})
    pp = enrichment.get("person_profile", {})
    # Legacy fallbacks
    prof_legacy = enrichment.get("professional", {})
    comp_legacy = enrichment.get("company", comp_section)

    # Build flat DB record
    now = datetime.now(timezone.utc).isoformat()

    # ── Activity-level intent signals ─────────────────────────────────────────
    hiring_signals_raw  = profile.get("_hiring_signals")  or []
    activity_emails_raw = profile.get("_activity_emails") or []

    # ── P3: AI Analysis Layer (tags, behavioural signals, pitch intelligence) ─
    try:
        from ai_analysis import run_ai_analysis, _rule_based_analysis
        from workspace import workspace_service as _ws
        _ws_config = await _ws.get_workspace_config(org_id)
        _ai = await run_ai_analysis(profile, _ws_config)
    except Exception as _ai_err:
        logger.warning("[AIAnalysis] Skipped (%s) — using rule-based fallback", _ai_err)
        try:
            from ai_analysis import _rule_based_analysis
            _ai = _rule_based_analysis(profile, {})
        except Exception:
            _ai = {}

    lead: dict = {
        "id": lead_id,
        "linkedin_url": url,
        # ── Identity ─────────────────────��─────────────────────────────────────
        "name": pp.get("full_name") or ident.get("full_name") or name,
        "first_name": profile.get("first_name") or first,
        "last_name": profile.get("last_name") or last,
        "work_email": pp.get("work_email") or ident.get("work_email") or contact.get("email"),
        "personal_email": pp.get("personal_email") or ident.get("personal_email"),
        "direct_phone": (
            pp.get("direct_phone") or ident.get("direct_phone")
            or contact.get("phone") or profile.get("phone_number") or profile.get("phone")
        ),
        "twitter": pp.get("twitter") or ident.get("twitter") or contact.get("twitter") or profile.get("twitter"),
        "city": (
            pp.get("city") or ident.get("city")
            or (profile.get("city") or "").split(",")[0].strip()
            or profile.get("location", "").split(",")[0].strip()
        ),
        "country": _country_name(
            pp.get("country") or ident.get("country")
            or profile.get("country") or profile.get("country_code", "")
        ),
        "timezone": pp.get("timezone") or ident.get("timezone"),
        # ── Professional ───────────────────────────────────────────────────────
        "title": (
            pp.get("current_title") or prof_legacy.get("current_title")
            or profile.get("position") or profile.get("headline") or profile.get("bio", "")
        ),
        "seniority_level": pp.get("seniority_level") or prof_legacy.get("seniority_level"),
        "department": pp.get("department") or prof_legacy.get("department"),
        "years_in_role": pp.get("years_in_role") or prof_legacy.get("years_in_role"),
        "company": ci.get("name") or comp_legacy.get("name") or company,
        "previous_companies": _safe_json(
            prof_legacy.get("previous_companies")
            or [e.get("company", "") for e in (pp.get("career_history") or [])[1:4] if isinstance(e, dict)]
            or [e.get("company_name", "") or e.get("company", "") for e in (profile.get("experience") or [])[1:4] if isinstance(e, dict)]
        ),
        "top_skills": _safe_json(
            pp.get("top_skills") or prof_legacy.get("top_skills")
            or profile.get("skills") or []
        ),
        "education": (
            pp.get("education") or prof_legacy.get("education")
            or profile.get("educations_details") or profile.get("education_details")
        ),
        "certifications": _safe_json(
            pp.get("certifications") or prof_legacy.get("certifications")
            or profile.get("honors_and_awards") or []
        ),
        "languages": _safe_json(pp.get("languages") or prof_legacy.get("languages") or []),
        # ── Company (from waterfall + LLM) ────────────────────────────────────
        "company_website": cp.get("website") or ci.get("website") or comp_legacy.get("website"),
        "industry": cp.get("industry") or comp_legacy.get("industry"),
        "employee_count": _safe_int(cp.get("employee_count") or comp_legacy.get("employee_count")),
        "hq_location": cp.get("hq_location") or comp_legacy.get("hq_location"),
        "founded_year": str(cp.get("founded_year") or comp_legacy.get("founded_year") or ""),
        "funding_stage": cin.get("funding_stage") or comp_legacy.get("funding_stage"),
        "total_funding": cin.get("total_funding") or comp_legacy.get("total_funding"),
        "last_funding_date": cin.get("last_funding_date") or comp_legacy.get("last_funding_date"),
        "lead_investor": cin.get("lead_investor") or comp_legacy.get("lead_investor"),
        "annual_revenue": cp.get("annual_revenue_est") or comp_legacy.get("annual_revenue_est"),
        "tech_stack": _safe_json(cp.get("tech_stack") or comp_legacy.get("tech_stack") or []),
        "hiring_velocity": cin.get("hiring_velocity") or comp_legacy.get("hiring_velocity"),
        # ── Company enrichment (waterfall) ────────────────────────────────────
        "avatar_url": _extract_avatar(profile) or contact.get("avatar_url"),
        "company_logo": cp.get("company_logo") or company_extras.get("company_logo"),
        "company_email": (
            cp.get("company_email") or company_extras.get("company_email")
            # Fall back to activity emails that match the company domain
            or next((e for e in activity_emails_raw if verified_domain and verified_domain.split(".")[0] in e), None)
        ),
        "company_description": wi.get("company_description") or company_extras.get("company_description"),
        "company_linkedin": ci.get("linkedin_url") or company_extras.get("company_linkedin") or contact.get("company_linkedin"),
        "company_twitter": cp.get("company_twitter") or company_extras.get("company_twitter"),
        "company_phone": cp.get("company_phone") or company_extras.get("company_phone"),
        "waterfall_log": json.dumps(waterfall_log),
        # ── Stage 3 — Website Intelligence ───────────────────────────────────
        "website_intelligence": json.dumps(website_intel or {}),
        "product_offerings": _safe_json(wi.get("product_offerings")),
        "value_proposition": wi.get("value_proposition"),
        "target_customers": _safe_json(wi.get("target_customers")),
        "business_model": wi.get("business_model"),
        "pricing_signals": wi.get("pricing_signals"),
        "product_category": wi.get("product_category"),
        "data_completeness_score": int(ls.get("data_completeness_score", 0)),
        # ── Intent signals ────────────────────────────────────────────────────
        "recent_funding_event": intent_s.get("recent_funding_event"),
        # Hiring signal: LLM result OR extracted from activity posts
        "hiring_signal": (
            intent_s.get("hiring_signal")
            or (hiring_signals_raw[0] if hiring_signals_raw else None)
        ),
        "job_change": intent_s.get("job_change"),
        # LinkedIn activity: combine LLM intent + actual post snippets
        "linkedin_activity": (
            intent_s.get("linkedin_activity")
            or (_safe_json(hiring_signals_raw) if hiring_signals_raw else None)
        ),
        "news_mention": intent_s.get("news_mention"),
        "product_launch": intent_s.get("product_launch"),
        "competitor_usage": intent_s.get("competitor_usage"),
        "review_activity": (
            intent_s.get("review_activity")
            or (profile.get("recommendations_text"))
        ),
        # ── Scoring ──────��────────────────────────────────────────────────────
        "icp_fit_score": _safe_int(ls.get("icp_fit_score")),
        "intent_score": _safe_int(ls.get("intent_score")),
        "timing_score": _safe_int(ls.get("timing_score")),
        "engagement_score": 0,
        "total_score": _safe_int(total),
        "score_tier": score_tier,
        "score_explanation": ls.get("score_explanation"),
        "icp_match_tier": ls.get("icp_match_tier") or tier_raw,
        "disqualification_flags": _safe_json(ls.get("disqualification_flags")),
        # ── Outreach ──────────────────────────────────────────────────────────
        "email_subject": outreach.get("email_subject"),
        "cold_email": outreach.get("cold_email"),
        "linkedin_note": outreach.get("linkedin_note"),
        "best_channel": outreach.get("best_channel"),
        "best_send_time": outreach.get("best_send_time"),
        "outreach_angle": outreach.get("outreach_angle"),
        "sequence_type": outreach.get("sequence_type"),
        "outreach_sequence": _safe_json(outreach.get("sequence")),
        "last_contacted": outreach.get("last_contacted"),
        "email_status": outreach.get("email_status"),
        # ── CRM ───────────────────────────────────────────────────────────────
        "lead_source": crm.get("lead_source", "LinkedIn URL"),
        "enrichment_source": crm.get("enrichment_source", "Bright Data + Hunter.io"),
        "data_completeness": int(crm.get("data_completeness") or 0),
        "crm_stage": crm.get("crm_stage"),
        "tags": _safe_json(crm.get("tags")),
        "assigned_owner": crm.get("assigned_owner"),
        # ── Meta ─���────────────────────────────────────────────────────────────
        "full_data": json.dumps({  # default=str guards against any non-serializable value
            "person_profile": enrichment.get("person_profile", {}),
            "company_identity": enrichment.get("company_identity", {}),
            "website_intelligence": enrichment.get("website_intelligence", website_intel or {}),
            "company_profile": enrichment.get("company_profile", {}),
            "company_intelligence": enrichment.get("company_intelligence", {}),
            "intent_signals": enrichment.get("intent_signals", {}),
            "lead_scoring": enrichment.get("lead_scoring", {}),
            "outreach": enrichment.get("outreach", {}),
            # Raw BD fields preserved
            "activity_emails": activity_emails_raw,
            "activity_phones": profile.get("_activity_phones") or [],
            "activity_full": profile.get("_activity_full") or [],
            "linkedin_posts": profile.get("_posts") or [],
            "hiring_signals": hiring_signals_raw,
            "recommendations": profile.get("recommendations") or [],
            "recommendations_count": profile.get("recommendations_count") or 0,
            "similar_profiles": profile.get("_similar_profiles") or [],
            "banner_image": profile.get("banner_image") or "",
            "linkedin_num_id": profile.get("linkedin_num_id") or "",
            "influencer": profile.get("_influencer") or False,
            "memorialized_account": profile.get("_memorialized_account") or False,
            "bio_links": profile.get("_bio_links") or [],
            "bd_scrape_timestamp": profile.get("_bd_scrape_timestamp") or "",
            # Legacy compat
            "identity": enrichment.get("identity", {}),
            "professional": enrichment.get("professional", {}),
            "company": enrichment.get("company", {}),
            "scoring": enrichment.get("scoring", {}),
        }, default=str),
        "raw_profile": json.dumps(profile, default=str),
        "crm_brief": json.dumps(enrichment.get("crm_brief"), default=str) if enrichment.get("crm_brief") is not None else None,
        "apollo_raw": json.dumps(contact.get("_apollo_raw"), default=str) if contact.get("_apollo_raw") else None,
        "about": (profile.get("about") or "")[:1000],
        "followers": _safe_int(profile.get("followers")),
        "connections": _safe_int(profile.get("connections")),
        "email_source": contact.get("source"),
        "email_confidence": contact.get("confidence"),
        "email_verified": 1 if contact.get("verified") else 0,
        "bounce_risk": contact.get("bounce_risk"),
        # P1 — Full activity feed stored as dedicated column for AI analysis
        "activity_feed": json.dumps(profile.get("_activity_full") or []),
        # P3 — AI Analysis layer (populated after _run_ai_analysis call below)
        "auto_tags":           _ai.get("auto_tags_json"),
        "behavioural_signals": _ai.get("behavioural_signals_json"),
        "pitch_intelligence":  _ai.get("pitch_intelligence_json"),
        "warm_signal":         _ai.get("warm_signal"),
        # ── Company enrichment (P1/P5) — flat columns mirrored from company_enrichments ─
        "company_id":        _company_id_val,
        "company_score":     _company_score,
        "combined_score":    _combined_score,
        "company_score_tier": _company_record.get("company_score_tier") or "",
        "company_tags":      _safe_json(_company_record.get("company_tags") or []),
        "culture_signals":   _safe_json(_company_record.get("culture_signals") or {}),
        "account_pitch":     _safe_json(_company_record.get("account_pitch") or {}),
        "wappalyzer_tech":   _safe_json(_company_record.get("wappalyzer_tech") or []),
        "news_mentions":     _safe_json(_company_record.get("news_mentions") or []),
        "crunchbase_data":   _safe_json(_company_record.get("crunchbase_data") or {}),
        "linkedin_posts":    _safe_json(_company_record.get("linkedin_posts") or []),
        "status": "enriched",
        "job_id": job_id,
        "organization_id": org_id,
        "enriched_at": now,
    }

    # P5: Compute combined_score after we have both total_score and company_score
    try:
        from company_service import compute_combined_score
        lead["combined_score"] = compute_combined_score(
            person_score=int(lead.get("total_score") or 0),
            company_score=_company_score,
        )
    except Exception:
        lead["combined_score"] = int(lead.get("total_score") or 0)

    await _upsert_lead(lead)
    logger.info(
        "[Enrich] Done: %s | score=%s (%s) | email=%s | completeness=%s%% | category=%s",
        lead["name"] or url, total, score_tier,
        lead["work_email"] or "not found", lead["data_completeness"],
        lead["product_category"] or "?",
    )

    # Embed structured LinkedIn Enrich view for immediate use by caller / LIO
    lead["_cache_hit"] = False
    lead["linkedin_enrich"] = _format_linkedin_enrich(lead, include_contact=not skip_contact)

    # If crm_brief generation failed or empty — regenerate in background
    _saved_brief_single = _parse_json_safe(lead.get("crm_brief"), None)
    if not _validate_crm_brief(_saved_brief_single) and lead.get("name"):
        logger.warning("[Enrich] crm_brief missing/invalid for %s — scheduling background regeneration", url)
        asyncio.create_task(regenerate_crm_brief_for_lead(lead_id, org_id=org_id))

    # Forward to LIO — only if BrightData returned real data (lead has a name)
    if not forward_to_lio and lead.get("name"):
        asyncio.create_task(send_to_lio(lead, sso_id=sso_id))

    # Release in-flight lock — wake up all waiters for this URL
    _in_flight_leads.pop(lead_id, None)
    _in_flight_event.set()

    return lead


# ─────────────────────────────────────────────────────────────────────────────
# SSE streaming enrichment (6 stages)
# ─────────────────────────────────────────────────────────────────────────────

from typing import AsyncGenerator

async def enrich_single_stream(
    linkedin_url: str,
    org_id: str = "default",
) -> AsyncGenerator[dict, None]:
    """
    Async generator that yields SSE events after each enrichment stage.

    Stage 1 — profile:   Bright Data person scrape
    Stage 2 — company:   Company waterfall (BD + Apollo + Hunter + Clearbit)
    Stage 3 — contact:   Email / phone waterfall (Hunter → Apollo → pattern)
    Stage 4 — website:   Website intelligence scrape + LLM extraction
    Stage 5 — scoring:   Comprehensive LLM scoring + outreach generation
    Stage 6 — complete:  Save to DB, return final lead object

    Each stage yields {"stage": <name>, "status": "loading"} first,
    then {"stage": <name>, "status": "done", "data": {...}} when finished.
    On error: {"stage": <name>, "status": "error", "error": <msg>}
    """
    url = _normalize_linkedin_url(linkedin_url)
    lead_id = _lead_id(url)

    # ── Company URL: proxy to company pipeline, stream synthetic events ───────
    if _is_company_url(url):
        yield {"stage": "profile",  "status": "loading"}
        yield {"stage": "company",  "status": "loading"}
        yield {"stage": "contact",  "status": "loading"}
        yield {"stage": "website",  "status": "loading"}
        yield {"stage": "scoring",  "status": "loading"}
        try:
            lead = await enrich_company_url(url, org_id=org_id, forward_to_lio=True)
            yield {"stage": "profile",  "status": "done", "data": {
                "name": lead.get("name", ""), "title": lead.get("title", ""),
                "company": lead.get("company", ""), "location": lead.get("location", ""),
                "avatar_url": lead.get("avatar_url", ""),
                "followers": lead.get("followers", 0),
            }}
            yield {"stage": "company",  "status": "done", "data": {
                "company_logo": lead.get("company_logo"),
                "company_website": lead.get("company_website"),
                "industry": lead.get("industry"),
                "employee_count": lead.get("employee_count", 0),
            }}
            _co_email = lead.get("work_email") or ""
            if "placeholder" in _co_email.lower():
                _co_email = ""
            yield {"stage": "contact",  "status": "done", "data": {
                "email": _co_email or None, "phone": lead.get("direct_phone"),
                "email_source": lead.get("email_source"), "email_confidence": lead.get("email_confidence"),
            }}
            yield {"stage": "website",  "status": "done", "data": {
                "value_proposition": lead.get("value_proposition"),
                "product_offerings": json.loads(lead.get("product_offerings") or "[]"),
                "tech_stack": json.loads(lead.get("tech_stack") or "[]"),
                "business_model": lead.get("business_model"),
            }}
            yield {"stage": "scoring",  "status": "done", "data": {
                "total_score": lead.get("total_score", 0), "score_tier": lead.get("score_tier"),
                "icp_fit_score": lead.get("icp_fit_score", 0), "intent_score": lead.get("intent_score", 0),
                "cold_email": lead.get("cold_email"), "linkedin_note": lead.get("linkedin_note"),
            }}
            yield {"stage": "complete", "status": "done", "lead": lead}
        except Exception as e:
            yield {"stage": "complete", "status": "error", "error": str(e)}
        return

    # ── Stage 1: Person Profile ───────────────────────────────────────────────
    yield {"stage": "profile", "status": "loading"}
    try:
        profile = await fetch_profile_sync(url)
        # Bright Data account/auth error — abort without saving incomplete data
        if profile.get("_bd_error"):
            err_msg = profile.get("_bd_message", "Bright Data account error")
            logger.error("[Stream] Aborting — BD account error: %s", err_msg)
            yield {"stage": "profile", "status": "error", "error": err_msg}
            yield {"stage": "complete", "status": "error", "error": f"Bright Data account error: {err_msg}"}
            return
        name     = profile.get("name", "")
        first, last = _parse_name(name)
        company  = profile.get("current_company_name", "")
        raw_link = profile.get("current_company_link", "")
        yield {"stage": "profile", "status": "done", "data": {
            "name":       name,
            "title":      profile.get("position") or profile.get("headline", ""),
            "company":    company,
            "location":   profile.get("location") or profile.get("city", ""),
            "avatar_url": profile.get("avatar_url") or profile.get("avatar", ""),
            "followers":  _safe_int(profile.get("followers")),
            "connections": _safe_int(profile.get("connections")),
            "about":      (profile.get("about") or "")[:300],
            "banner_image": profile.get("banner_image", ""),
        }}
    except Exception as e:
        yield {"stage": "profile", "status": "error", "error": str(e)}
        yield {"stage": "complete", "status": "error", "error": f"Profile fetch failed: {e}"}
        return

    # ── Stage 2: Company Waterfall ────────────────────────────────────────────
    yield {"stage": "company", "status": "loading"}
    try:
        initial_domain = _extract_domain(company, raw_link)
        company_extras, company_wf_log = await enrich_company_waterfall(company, initial_domain, profile)
        verified_domain = company_extras.pop("_verified_domain", initial_domain) or initial_domain
        real_website = company_extras.get("company_website") or (f"https://{verified_domain}" if verified_domain else "")
        yield {"stage": "company", "status": "done", "data": {
            "company_logo":    company_extras.get("company_logo"),
            "company_website": real_website,
            "industry":        company_extras.get("industry"),
            "employee_count":  company_extras.get("employee_count", 0),
            "hq_location":     company_extras.get("hq_location"),
            "tech_stack":      company_extras.get("tech_stack", []),
            "founded_year":    company_extras.get("founded_year"),
            "funding_stage":   company_extras.get("funding_stage"),
        }}
    except Exception as e:
        yield {"stage": "company", "status": "error", "error": str(e)}
        company_extras, company_wf_log = {}, []
        verified_domain = _extract_domain(company, raw_link)
        real_website = ""

    # ── Stage 3: Contact Waterfall ────────────────────────────────────────────
    yield {"stage": "contact", "status": "loading"}
    try:
        activity_emails = profile.get("_activity_emails") or []
        activity_phones = profile.get("_activity_phones") or []
        if activity_emails:
            matched = next((e for e in activity_emails if verified_domain and verified_domain.split(".")[0] in e), None)
            pre_contact = matched or activity_emails[0]
            contact = {"email": pre_contact, "source": "linkedin_activity", "confidence": "high"}
            if activity_phones:
                contact["phone"] = activity_phones[0]
        else:
            contact = await find_contact_info(first, last, verified_domain, linkedin_url=url)
            if not contact.get("phone") and activity_phones:
                contact["phone"] = activity_phones[0]
        _contact_email = contact.get("email") or ""
        if "placeholder" in _contact_email.lower():
            logger.warning("[Stream] Dropping placeholder email from contact stage: %s", _contact_email)
            _contact_email = ""
        yield {"stage": "contact", "status": "done", "data": {
            "email":            _contact_email or None,
            "phone":            contact.get("phone"),
            "email_source":     contact.get("source"),
            "email_confidence": contact.get("confidence"),
        }}
    except Exception as e:
        yield {"stage": "contact", "status": "error", "error": str(e)}
        contact = {"email": None, "phone": None, "source": None, "confidence": None}

    # ── Stage 4: Website Intelligence ────────────────────────────────────────
    yield {"stage": "website", "status": "loading"}
    try:
        website_intel = await scrape_website_intelligence(real_website)
        yield {"stage": "website", "status": "done", "data": {
            "value_proposition":   website_intel.get("value_proposition"),
            "product_offerings":   website_intel.get("product_offerings", []),
            "tech_stack_clues":    website_intel.get("tech_stack_clues", []),
            "business_model":      website_intel.get("business_model"),
            "product_category":    website_intel.get("product_category"),
            "market_positioning":  website_intel.get("market_positioning"),
            "pricing_signals":     website_intel.get("pricing_signals"),
            "target_customers":    website_intel.get("target_customers", []),
            "problem_solved":      website_intel.get("problem_solved"),
            "hiring_signals":      website_intel.get("hiring_signals"),
            "pages_scraped":       website_intel.get("pages_scraped", []),
        }}
    except Exception as e:
        yield {"stage": "website", "status": "error", "error": str(e)}
        website_intel = {}

    # ── Stage 5: LLM Scoring + Outreach ──────────────────────────────────────
    yield {"stage": "scoring", "status": "loading"}
    try:
        profile["_company_extras"] = company_extras
        if website_intel and website_intel.get("pages_scraped"):
            company_wf_log.append({
                "step": 5, "source": "Website Scrape",
                "fields_found": website_intel.get("pages_scraped", []),
                "note": f"pages={len(website_intel.get('pages_scraped', []))}, category={website_intel.get('product_category', '?')}",
            })
        enrichment = await build_comprehensive_enrichment(url, profile, contact, website_intel=website_intel, org_id=org_id)
        scoring = enrichment.get("lead_scoring", enrichment.get("scoring", {}))
        total = scoring.get("overall_score", 0)
        tier_raw = scoring.get("icp_match_tier") or scoring.get("score_tier", "")
        score_tier = _resolve_tier(tier_raw)
        outreach = enrichment.get("outreach", {})
        yield {"stage": "scoring", "status": "done", "data": {
            "total_score":  int(total),
            "score_tier":   score_tier,
            "icp_fit_score":  int(scoring.get("icp_fit_score", 0)),
            "intent_score":   int(scoring.get("intent_score", 0)),
            "timing_score":   int(scoring.get("timing_score", 0)),
            "icp_match_tier": scoring.get("icp_match_tier") or tier_raw,
            "score_explanation": scoring.get("score_explanation"),
            "cold_email":     outreach.get("cold_email"),
            "linkedin_note":  outreach.get("linkedin_note"),
            "email_subject":  outreach.get("email_subject"),
            "best_channel":   outreach.get("best_channel"),
            "outreach_angle": outreach.get("outreach_angle"),
        }}
    except Exception as e:
        yield {"stage": "scoring", "status": "error", "error": str(e)}
        enrichment = {}
        scoring = {}
        total = 0
        score_tier = "cold"
        outreach = {}

    # ── Stage 6: Save + Complete ──────────────────────────────────────────────
    try:
        # Build full lead record (same logic as enrich_single)
        person_prof = enrichment.get("person_profile", {})
        ident = enrichment.get("identity", {})
        if contact.get("email"):
            person_prof["work_email"] = contact["email"]
            ident["work_email"] = contact["email"]
        if contact.get("phone"):
            person_prof["direct_phone"] = contact["phone"]
            ident["direct_phone"] = contact["phone"]

        comp_prof = enrichment.get("company_profile", {})
        comp_section = enrichment.get("company", {})
        for k, v in company_extras.items():
            _map = {
                "company_logo": "company_logo", "company_email": "company_email",
                "company_phone": "company_phone", "company_twitter": "company_twitter",
                "industry": "industry", "employee_count": "employee_count",
                "hq_location": "hq_location", "founded_year": "founded_year",
                "annual_revenue": "annual_revenue_est", "tech_stack": "tech_stack",
            }
            prof_key = _map.get(k)
            if prof_key and v and not comp_prof.get(prof_key):
                comp_prof[prof_key] = v

        scoring_f = enrichment.get("lead_scoring", enrichment.get("scoring", {}))
        crm = enrichment.get("crm", {})
        wi = enrichment.get("website_intelligence", website_intel or {})
        intent_s = enrichment.get("intent_signals", {})
        ci = enrichment.get("company_identity", {})
        cp = enrichment.get("company_profile", comp_prof)
        cin = enrichment.get("company_intelligence", {})
        pp = enrichment.get("person_profile", {})
        hiring_signals_raw = profile.get("_hiring_signals") or []
        activity_emails_raw = profile.get("_activity_emails") or []
        now = datetime.now(timezone.utc).isoformat()

        waterfall_log = [
            {"step": 0, "source": "Bright Data", "fields_found": ["profile"], "note": f"LinkedIn scrape: {url}"},
            *company_wf_log,
            {"step": 10, "source": contact.get("source") or "none",
             "fields_found": ["email"], "note": f"email={contact.get('email')}"},
        ]

        lead: dict = {
            "id": lead_id, "linkedin_url": url,
            "name": pp.get("full_name") or ident.get("full_name") or profile.get("name", ""),
            "first_name": profile.get("first_name") or first,
            "last_name": profile.get("last_name") or last,
            "work_email": pp.get("work_email") or ident.get("work_email") or contact.get("email"),
            "personal_email": pp.get("personal_email") or ident.get("personal_email"),
            "direct_phone": (pp.get("direct_phone") or ident.get("direct_phone")
                             or contact.get("phone") or profile.get("phone_number") or profile.get("phone")),
            "twitter": pp.get("twitter") or ident.get("twitter") or contact.get("twitter") or profile.get("twitter"),
            "city": (pp.get("city") or ident.get("city")
                     or (profile.get("city") or "").split(",")[0].strip()
                     or profile.get("location", "").split(",")[0].strip()),
            "country": _country_name(
                pp.get("country") or ident.get("country")
                or profile.get("country") or profile.get("country_code", "")
            ),
            "avatar_url": _extract_avatar(profile) or contact.get("avatar_url") or "",
            "title": (pp.get("current_title") or ident.get("current_title")
                      or profile.get("position") or profile.get("headline", "")),
            "seniority_level": pp.get("seniority_level") or ident.get("seniority_level"),
            "department": pp.get("department") or ident.get("department"),
            "company": (cp.get("name") or ci.get("name") or comp_section.get("name") or
                        comp_prof.get("name") or profile.get("current_company_name", "")),
            "company_website": (cp.get("website") or ci.get("website") or comp_section.get("website") or
                                real_website or company_extras.get("company_website")),
            "company_logo": (cp.get("company_logo") or comp_section.get("logo") or
                             company_extras.get("company_logo") or profile.get("current_company_logo")),
            "company_linkedin": (ci.get("linkedin_url") or profile.get("current_company_link") or
                                 company_extras.get("company_linkedin")),
            "company_description": (cp.get("company_description") or comp_section.get("description") or
                                    company_extras.get("company_description") or wi.get("company_description")),
            "industry": (cp.get("industry") or comp_section.get("industry") or company_extras.get("industry")),
            "employee_count": _safe_int(cp.get("employee_count") or company_extras.get("employee_count")),
            "hq_location": (cp.get("hq_location") or comp_section.get("hq_location") or
                            company_extras.get("hq_location")),
            "founded_year": (cp.get("founded_year") or comp_section.get("founded_year") or
                             company_extras.get("founded_year")),
            "annual_revenue": (cp.get("annual_revenue_est") or comp_section.get("annual_revenue_est") or
                               company_extras.get("annual_revenue")),
            "tech_stack": _safe_json(cp.get("tech_stack") or company_extras.get("tech_stack") or []),
            "company_email": cp.get("company_email") or company_extras.get("company_email"),
            "company_phone": cp.get("company_phone") or company_extras.get("company_phone"),
            "company_twitter": cp.get("company_twitter") or company_extras.get("company_twitter"),
            "top_skills": _safe_json(pp.get("top_skills") or profile.get("skills") or []),
            "education": _safe_json(profile.get("education") or []),
            "certifications": _safe_json(pp.get("certifications") or profile.get("certifications") or []),
            "languages": _safe_json(pp.get("languages") or profile.get("languages") or []),
            "value_proposition": wi.get("value_proposition"),
            "product_offerings": _safe_json(wi.get("product_offerings")),
            "target_customers": _safe_json(wi.get("target_customers")),
            "business_model": wi.get("business_model"),
            "pricing_signals": wi.get("pricing_signals"),
            "product_category": wi.get("product_category"),
            "data_completeness_score": _safe_int(scoring_f.get("data_completeness_score")),
            "recent_funding_event": intent_s.get("recent_funding_event"),
            "hiring_signal": intent_s.get("hiring_signal") or (hiring_signals_raw[0] if hiring_signals_raw else None),
            "job_change": intent_s.get("job_change"),
            "linkedin_activity": intent_s.get("linkedin_activity") or (_safe_json(hiring_signals_raw) if hiring_signals_raw else None),
            "news_mention": intent_s.get("news_mention"),
            "product_launch": intent_s.get("product_launch"),
            "competitor_usage": intent_s.get("competitor_usage"),
            "review_activity": intent_s.get("review_activity") or profile.get("recommendations_text"),
            "icp_fit_score": _safe_int(scoring_f.get("icp_fit_score")),
            "intent_score": _safe_int(scoring_f.get("intent_score")),
            "timing_score": _safe_int(scoring_f.get("timing_score")),
            "engagement_score": 0,
            "total_score": _safe_int(total),
            "score_tier": score_tier,
            "score_explanation": scoring_f.get("score_explanation"),
            "icp_match_tier": scoring_f.get("icp_match_tier") or tier_raw,
            "disqualification_flags": _safe_json(scoring_f.get("disqualification_flags")),
            "email_subject": outreach.get("email_subject"),
            "cold_email": outreach.get("cold_email"),
            "linkedin_note": outreach.get("linkedin_note"),
            "best_channel": outreach.get("best_channel"),
            "best_send_time": outreach.get("best_send_time"),
            "outreach_angle": outreach.get("outreach_angle"),
            "sequence_type": outreach.get("sequence_type"),
            "outreach_sequence": _safe_json(outreach.get("sequence")),
            "last_contacted": outreach.get("last_contacted"),
            "email_status": outreach.get("email_status"),
            "lead_source": crm.get("lead_source", "LinkedIn URL"),
            "enrichment_source": crm.get("enrichment_source", "Bright Data + Hunter.io"),
            "data_completeness": _safe_int(crm.get("data_completeness")),
            "crm_stage": crm.get("crm_stage"),
            "tags": _safe_json(crm.get("tags")),
            "assigned_owner": crm.get("assigned_owner"),
            "full_data": json.dumps({
                "person_profile": enrichment.get("person_profile", {}),
                "company_identity": enrichment.get("company_identity", {}),
                "website_intelligence": enrichment.get("website_intelligence", website_intel or {}),
                "company_profile": enrichment.get("company_profile", {}),
                "company_intelligence": enrichment.get("company_intelligence", {}),
                "intent_signals": enrichment.get("intent_signals", {}),
                "lead_scoring": enrichment.get("lead_scoring", {}),
                "outreach": enrichment.get("outreach", {}),
                # Profile-level data needed for UI sections
                "recommendations": profile.get("recommendations") or [],
                "recommendations_count": profile.get("recommendations_count") or 0,
                "similar_profiles": profile.get("_similar_profiles") or [],
                "banner_image": profile.get("banner_image") or "",
                "linkedin_num_id": profile.get("linkedin_num_id") or profile.get("linkedin_num_id") or "",
                "influencer": profile.get("_influencer") or False,
                "memorialized_account": profile.get("_memorialized_account") or False,
                "bio_links": profile.get("_bio_links") or [],
                "bd_scrape_timestamp": profile.get("_bd_scrape_timestamp") or "",
                "activity_emails": activity_emails_raw,
                "activity_phones": profile.get("_activity_phones") or [],
                "activity_full": profile.get("_activity_full") or [],
                "linkedin_posts": profile.get("_posts") or [],
                "hiring_signals": hiring_signals_raw,
                "waterfall_log": waterfall_log,
            }, default=str),
            "raw_profile": json.dumps(profile, default=str),
            "about": (profile.get("about") or "")[:1000],
            "followers": _safe_int(profile.get("followers")),
            "connections": _safe_int(profile.get("connections")),
            "email_source": contact.get("source"),
            "email_confidence": contact.get("confidence"),
            "status": "enriched",
            "job_id": None,
            "organization_id": org_id,
            "enriched_at": now,
        }
        await _upsert_lead(lead)
        yield {"stage": "complete", "status": "done", "lead": lead}
    except Exception as e:
        logger.error("[StreamEnrich] Save failed: %s", e, exc_info=True)
        yield {"stage": "complete", "status": "error", "error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# Bulk pipeline
# ─────────────────────────────────────────────────────────────────────────────


async def _poll_and_process_snapshot(
    snapshot_id: str, job_id: str, org_id: str, sub_job_id: Optional[str] = None
) -> None:
    """Poll BrightData for snapshot results and process them (used when no webhook URL is set)."""
    _pipeline_log.info("[job=%s] [snapshot=%s] POLL_START — waiting for BrightData (interval=15s timeout=1800s)", (job_id or "")[-8:], snapshot_id)
    try:
        logger.info("[BulkPoll] Polling snapshot %s for job %s (sub_job=%s)", snapshot_id, job_id, sub_job_id or "—")
        profiles = await poll_snapshot(snapshot_id, interval=3, timeout=1800)
        _pipeline_log.info("[job=%s] [snapshot=%s] POLL_READY — %d profiles received", (job_id or "")[-8:], snapshot_id, len(profiles))
        logger.info("[BulkPoll] Snapshot %s ready — %d profiles, processing…", snapshot_id, len(profiles))
        result = await process_webhook_profiles(profiles, job_id=job_id, sub_job_id=sub_job_id)
        _pipeline_log.info("[job=%s] [snapshot=%s] POLL_DONE — processed=%d failed=%d", (job_id or "")[-8:], snapshot_id, result["processed"], result["failed"])
        logger.info("[BulkPoll] Done: processed=%d failed=%d", result["processed"], result["failed"])
    except Exception as e:
        _pipeline_log.error("[job=%s] [snapshot=%s] POLL_ERROR — %s", (job_id or "")[-8:], snapshot_id, e)
        logger.error("[BulkPoll] snapshot %s failed: %s", snapshot_id, e)
        if sub_job_id:
            await _update_sub_job(sub_job_id, status="failed")
        await _update_job(job_id, status="failed", error=str(e))


def _bd_chunk_size(total: int) -> int:
    """
    1 URL per chunk — each lead gets its own BrightData snapshot for granular progress.
    Max 200 leads per job, so max 200 parallel snapshots.
    """
    return 1


def _dynamic_chunk_size(total: int) -> int:
    """
    1 URL per chunk — each lead is processed independently for granular progress.
    """
    return 1


async def enrich_bulk(
    urls: list[str],
    webhook_url: Optional[str] = None,
    notify_url: Optional[str] = None,
    webhook_auth: Optional[str] = None,
    org_id: str = "default",
    sso_id: str = "",
    forward_to_lio: bool = False,
    system_prompt: Optional[str] = None,
) -> dict:
    """
    Bulk enrichment pipeline — multi-tenant aware.

    Strategy:
    - If webhook_url provided → trigger Bright Data batch (results arrive via webhook)
    - If Redis available → push individual tasks to priority queue (worker pool processes them)
    - Otherwise → in-process sequential asyncio task (guaranteed progress, single-server only)
    """
    job_id = str(uuid.uuid4())
    now    = datetime.now(timezone.utc)
    snapshot_id = None
    status = "pending"
    error_msg = None

    # Base app URL — used to build per-chunk webhook URLs
    app_url = os.getenv("APP_URL", "").rstrip("/")

    bd_triggered = False  # tracks whether BD path succeeded — prevents queue double-run

    # Insert the parent job row FIRST so FK constraints on enrichment_sub_jobs are satisfied
    job = {
        "id": job_id, "snapshot_id": None,
        "total_urls": len(urls), "processed": 0, "failed": 0,
        "status": "pending", "error": None,
        "webhook_url": webhook_url,
        "organization_id": org_id,
        "sso_id": sso_id,
        "forward_to_lio": forward_to_lio,
        "created_at": now, "updated_at": now,
    }
    _sql, _args = named_args(
        """INSERT INTO enrichment_jobs
           (id,snapshot_id,total_urls,processed,failed,status,error,webhook_url,organization_id,sso_id,forward_to_lio,created_at,updated_at)
           VALUES (:id,:snapshot_id,:total_urls,:processed,:failed,:status,:error,:webhook_url,:organization_id,:sso_id,:forward_to_lio,:created_at,:updated_at)""",
        job,
    )
    async with get_pool().acquire() as conn:
        await conn.execute(_sql, *_args)

    # Trigger BrightData — 1 URL per sub-job, each gets its own snapshot + webhook
    # Semaphore limits concurrent BD API calls to avoid rate-limiting (max 10 at a time)
    if _bd_api_key():
        try:
            # 1 URL per chunk — 100 URLs = 100 sub-jobs = 100 snapshots
            url_chunks  = [[u] for u in urls]
            sub_job_ids = [str(uuid.uuid4()) for _ in url_chunks]
            total_chunks = len(url_chunks)

            async def _trigger_bd_chunk(idx: int, sjid: str, chunk: list) -> str:
                async with _bd_trigger_semaphore:  # global cap across all orgs
                    if app_url:
                        # BD sends full data to chunk_webhook — no need for notify
                        chunk_webhook = f"{app_url}/api/leads/webhook/brightdata?job_id={job_id}&sub_job_id={sjid}"
                        chunk_notify  = None
                    elif webhook_url:
                        sep = "&" if "?" in webhook_url else "?"
                        chunk_webhook = f"{webhook_url}{sep}sub_job_id={sjid}"
                        chunk_notify  = notify_url
                    else:
                        chunk_webhook = None
                        chunk_notify  = None
                    snap_id = await trigger_batch_snapshot(chunk, chunk_webhook, chunk_notify, webhook_auth)
                    await _create_sub_job(sjid, job_id, idx, len(chunk), org_id, snapshot_id=snap_id)
                    if not chunk_webhook:
                        asyncio.create_task(_poll_and_process_snapshot(snap_id, job_id, org_id, sub_job_id=sjid))
                    logger.info(
                        "[BulkEnrich] Sub-job %d/%d | url=%s → snapshot=%s (webhook=%s)",
                        idx + 1, total_chunks, chunk[0], snap_id, chunk_webhook or "polling",
                    )
                    return snap_id

            snap_ids    = await asyncio.gather(*[_trigger_bd_chunk(i, sjid, chunk) for i, (sjid, chunk) in enumerate(zip(sub_job_ids, url_chunks))])
            snapshot_id = snap_ids[0] if snap_ids else None  # store first on parent job for compat
            # Set webhook_url on parent job row so stop/rerun routes can find it
            if not webhook_url and app_url:
                webhook_url = f"{app_url}/api/leads/webhook/brightdata?job_id={job_id}"
            status = "running"
            bd_triggered = True
            logger.info(
                "[BulkEnrich] %d URLs → %d sub-jobs triggered for job %s (org=%s)",
                len(urls), total_chunks, job_id, org_id,
            )
        except Exception as e:
            logger.error("[BulkEnrich] BD chunk trigger failed: %s — falling back to queue", e)
            error_msg = str(e)

    # Update parent job with results from BD trigger (snapshot_id, status, error, webhook_url)
    await _update_job(job_id, snapshot_id=snapshot_id, status=status, error=error_msg, webhook_url=webhook_url)
    job = {
        "id": job_id, "snapshot_id": snapshot_id,
        "total_urls": len(urls), "processed": 0, "failed": 0,
        "status": status, "error": error_msg,
        "webhook_url": webhook_url,
        "organization_id": org_id,
        "created_at": now, "updated_at": now,
    }

    if not webhook_url and not bd_triggered:
        # Fair multi-tenant queue (per-tenant queues + round-robin scheduler)
        r = await _get_redis()
        if r:
            try:
                import queue_manager
                chunk_size = _dynamic_chunk_size(len(urls))
                url_chunks = [urls[i: i + chunk_size] for i in range(0, len(urls), chunk_size)]
                sub_job_ids = [str(uuid.uuid4()) for _ in url_chunks]
                for idx, (sjid, chunk) in enumerate(zip(sub_job_ids, url_chunks)):
                    await _create_sub_job(sjid, job_id, idx, len(chunk), org_id)
                num_chunks = await queue_manager.push_job(
                    job_id=job_id,
                    org_id=org_id,
                    chunks=url_chunks,
                    sub_job_ids=sub_job_ids,
                    r=r,
                    generate_outreach=True,
                    sso_id=sso_id,
                    forward_to_lio=forward_to_lio,
                    system_prompt=system_prompt,
                )
                await _update_job(job_id, status="running")
                job["status"] = "running"
                # Proactively scale AI workers so the LLM pipeline keeps up with
                # incoming enriched leads — 1 extra worker per 10 URLs, capped by AI_WORKER_MAX
                extra_ai = max(1, len(urls) // 10)
                await queue_manager.scale_up_ai_workers(extra_ai)
                logger.info(
                    "[BulkEnrich] %d URLs → %d chunks queued for job %s (org=%s)",
                    len(urls), num_chunks, job_id, org_id,
                )
                return job
            except Exception as e:
                logger.warning("[BulkEnrich] Queue push failed (%s) — falling back to in-process", e)

        # Fallback: in-process sequential task (Redis unavailable)
        chunk_size = _dynamic_chunk_size(len(urls))
        url_chunks = [urls[i: i + chunk_size] for i in range(0, len(urls), chunk_size)]
        sub_job_ids = [str(uuid.uuid4()) for _ in url_chunks]
        for idx, (sjid, chunk) in enumerate(zip(sub_job_ids, url_chunks)):
            await _create_sub_job(sjid, job_id, idx, len(chunk), org_id)
        logger.info(
            "[BulkEnrich] Starting in-process batch for %d URLs → %d chunks (org=%s)",
            len(urls), len(url_chunks), org_id,
        )
        async def _sequential_with_cap():
            async with _sequential_fallback_sem:
                await _process_sequential_batch(job_id, url_chunks, sub_job_ids, org_id=org_id, sso_id=sso_id, forward_to_lio=forward_to_lio, system_prompt=system_prompt)

        asyncio.create_task(_sequential_with_cap())

    return job


async def _process_sequential_batch(
    job_id: str,
    url_chunks: list[list[str]],
    sub_job_ids: list[str],
    org_id: str = "default",
    sso_id: str = "",
    forward_to_lio: bool = False,
    system_prompt: Optional[str] = None,
) -> None:
    """
    In-process sequential bulk enrichment — fallback when Redis is unavailable.
    Processes URL chunks sequentially; each chunk maps to a sub-job record.
    """
    await _update_job(job_id, status="running")
    total_processed = total_failed = 0
    total_urls = sum(len(c) for c in url_chunks)
    logger.info(
        "[BulkBatch] Starting %d URLs in %d chunks for job %s (org=%s)",
        total_urls, len(url_chunks), job_id, org_id,
    )

    for chunk_idx, (sub_job_id, chunk) in enumerate(zip(sub_job_ids, url_chunks)):
        # Check if job was cancelled before each chunk
        _job_row = await get_job(job_id)
        if _job_row and _job_row.get("status") == "cancelled":
            logger.info("[BulkBatch] Job %s cancelled — stopping at chunk %d", job_id, chunk_idx)
            await _update_sub_job(sub_job_id, status="cancelled")
            break

        await _update_sub_job(sub_job_id, status="running")
        chunk_ok = chunk_fail = 0
        for i, url in enumerate(chunk):
            # Check cancellation between each URL
            _job_row = await get_job(job_id)
            if _job_row and _job_row.get("status") == "cancelled":
                logger.info("[BulkBatch] Job %s cancelled mid-chunk — stopping", job_id)
                break
            try:
                lead = await enrich_single(url, job_id=job_id, org_id=org_id, sso_id=sso_id, forward_to_lio=forward_to_lio, system_prompt=system_prompt, skip_contact=True)
                chunk_ok += 1
                logger.info(
                    "[BulkBatch] Chunk %d/%d · URL %d/%d OK: %s",
                    chunk_idx + 1, len(url_chunks), i + 1, len(chunk), url,
                )
                await _publish_lead_done(org_id, job_id, lead)
            except Exception as e:
                logger.warning(
                    "[BulkBatch] Chunk %d/%d · URL %d/%d FAIL: %s — %s",
                    chunk_idx + 1, len(url_chunks), i + 1, len(chunk), url, e,
                )
                chunk_fail += 1
            await _update_sub_job(sub_job_id, processed=chunk_ok, failed=chunk_fail)

        chunk_status = (
            "completed" if chunk_fail == 0
            else ("failed" if chunk_ok == 0 else "completed_with_errors")
        )
        await _update_sub_job(sub_job_id, status=chunk_status, processed=chunk_ok, failed=chunk_fail)

        total_processed += chunk_ok
        total_failed += chunk_fail
        await _update_job(job_id, processed=total_processed, failed=total_failed)

    final_status = (
        "completed" if total_failed == 0
        else ("failed" if total_processed == 0 else "completed_with_errors")
    )
    await _update_job(job_id, status=final_status, processed=total_processed, failed=total_failed)
    await _publish_job_done(org_id, job_id, total_processed, total_failed)
    logger.info(
        "[BulkBatch] Job %s done — processed=%d failed=%d", job_id, total_processed, total_failed
    )


# Keep old name as alias for webhook-delivered batch processing
async def _process_fallback_batch(job_id: str, urls: list[str]) -> None:
    chunk_size = 5
    url_chunks = [urls[i: i + chunk_size] for i in range(0, len(urls), chunk_size)]
    sub_job_ids = [str(uuid.uuid4()) for _ in url_chunks]
    for idx, (sjid, chunk) in enumerate(zip(sub_job_ids, url_chunks)):
        await _create_sub_job(sjid, job_id, idx, len(chunk), "default")
    await _process_sequential_batch(job_id, url_chunks, sub_job_ids)


async def _process_one_webhook_profile(profile: dict, job_id: Optional[str], org_id: str, sub_job_id: Optional[str] = None, sso_id: str = "") -> Optional[dict]:
    """
    Process a single BrightData profile through the full enrichment pipeline.
    Returns the enriched lead dict on success, None on failure.
    Designed to be called concurrently via asyncio.gather.
    """
    raw_url = profile.get("input_url") or profile.get("url") or profile.get("linkedin_url")
    if not raw_url:
        _pipeline_log.warning("[job=%s] PROFILE_SKIP — no URL in profile keys=%s", (job_id or "")[-8:], list(profile.keys())[:10])
        return None
    try:
        url = _normalize_linkedin_url(_clean_bd_linkedin_url(raw_url))
        _plog(job_id, url, "START", f"profile received from BD (keys={list(profile.keys())[:8]})")

        # ── Data validation ───────────────────────────────────────────────────────
        # A usable profile must have at least a name (or first+last) and must not
        # carry an explicit BD error flag. Partial profiles (no position/company)
        # are accepted and enriched further downstream.
        def _has_data(p: dict) -> bool:
            if p.get("error") or p.get("_bd_error") or p.get("_error"):
                return False
            has_name = bool(
                p.get("name") or p.get("full_name")
                or (p.get("first_name") and p.get("last_name"))
            )
            if not has_name:
                missing = [f for f in ("name", "full_name", "first_name") if not p.get(f)]
                logger.debug("[Validation] Profile missing identity fields: %s", missing)
                return False
            return True

        def _is_private_profile(p: dict) -> bool:
            """Detect BrightData private/hidden profile responses — no retry needed."""
            _PRIVATE_SIGNALS = ("hidden or private", "profile is private", "private profile", "profile not found")
            for _field in ("error", "_error", "_bd_message", "message", "about", "name"):
                _val = str(p.get(_field) or "").lower()
                if any(s in _val for s in _PRIVATE_SIGNALS):
                    return True
            return False

        # ── Private profile — fail immediately, no retries ────────────────────────
        if _is_private_profile(profile):
            _err_msg = profile.get("error") or profile.get("_bd_message") or "Profile is private or hidden"
            logger.warning("[Pipeline] Private/hidden profile for %s — marking failed, no retry", url)
            _plog(job_id, url, "FAILED", f"Private profile: {_err_msg}", "error")
            _lead_id_val = _lead_id(url)
            try:
                await _upsert_lead({
                    "id": _lead_id_val,
                    "linkedin_url": url,
                    "status": "failed",
                    "job_id": job_id,
                    "organization_id": org_id,
                    "score_explanation": "Profile is private or hidden on LinkedIn — BrightData cannot access it.",
                    "enriched_at": datetime.now(timezone.utc).isoformat(),
                })
            except Exception:
                pass
            asyncio.create_task(send_to_lio_failed(url, org_id, sso_id, reason="private_profile", error_message=_err_msg))
            return None

        # ── Retry if BrightData returned empty/partial/error profile ─────────────
        # Up to 3 re-scrape attempts with exponential back-off (2s, 4s, 8s).
        if not _has_data(profile):
            if profile.get("error") or profile.get("_error"):
                logger.warning("[Pipeline] BD returned error for %s: %s", url,
                               profile.get("error") or profile.get("_error"))
            for _attempt in range(3):
                wait = _bd_backoff(_attempt)
                logger.warning("[Pipeline] Empty BD profile for %s — retry %d/3 in %.1fs", url, _attempt + 1, wait)
                await asyncio.sleep(wait)
                retried = await fetch_profile_sync(url)
                if retried and _has_data(retried):
                    profile = retried
                    logger.info("[Pipeline] Retry %d succeeded for %s", _attempt + 1, url)
                    break
                # Check if retry itself returned a private profile
                if retried and _is_private_profile(retried):
                    logger.warning("[Pipeline] Retry confirmed private profile for %s — stopping retries", url)
                    break
            else:
                _plog(job_id, url, "VALIDATION", "all 3 retries returned empty/invalid profile — skipping", "error")
                logger.error("[Pipeline] All 3 retries returned empty/invalid profile for %s — skipping", url)
                asyncio.create_task(send_to_lio_failed(url, org_id, sso_id, reason="empty_after_retries", error_message="BrightData returned empty or invalid profile after 3 retries"))
                return None

        name = profile.get("name", "")
        first, last = _parse_name(name)
        company = profile.get("current_company_name", "")
        raw_link = profile.get("current_company_link", "")
        domain = _extract_domain(company, raw_link)
        lead_id = _lead_id(url)

        # ── Stage 1: SCRAPING — BrightData profile received, save basic data ──
        await _upsert_lead({
            "id": lead_id,
            "linkedin_url": url,
            "name": name,
            "first_name": first,
            "last_name": last,
            "title": profile.get("position", ""),
            "company": company,
            "avatar_url": _extract_avatar(profile),
            "raw_brightdata": json.dumps(profile, default=str),
            "about": (profile.get("about") or "")[:1000],
            "followers": _safe_int(profile.get("followers")),
            "connections": _safe_int(profile.get("connections")),
            "status": "scraping",
            "job_id": job_id,
            "organization_id": org_id,
            "enriched_at": datetime.now(timezone.utc).isoformat(),
        })
        _plog(job_id, url, "SCRAPING", f"saved to DB — name={name!r} company={company!r}")
        logger.info("[Pipeline] %s → scraping", url)

        # Mark sub_job as scraped (yellow) — BrightData data is in DB, LLM not started yet
        if sub_job_id:
            try:
                await _update_sub_job(sub_job_id, status="scraped")
            except Exception:
                pass

        # Company waterfall + contact lookup both removed from bulk path.
        # Company enrichment → POST /api/leads/view/company
        # Email enrichment  → POST /api/leads/view/email
        company_extras = {}
        company_wf_log = []
        contact = {}

        verified_domain = domain
        # real_website not used in bulk — website scrape is done via /view/company only
        profile["_company_extras"] = {}

        # ── Stage 2: ENRICHING — LLM only (website scrape removed from bulk path) ──
        # Website scrape (11 concurrent fetches + LLM ~30-60s) is NOT called here.
        # Use POST /view/company to fetch company/website intelligence on demand.
        async with get_pool().acquire() as _conn:
            await _conn.execute(
                "UPDATE enriched_leads SET status='enriching', updated_at=NOW() WHERE id=$1",
                lead_id,
            )
        _plog(job_id, url, "ENRICHING", f"email={contact.get('email') if isinstance(contact, dict) else '?'!r}")
        logger.info("[Pipeline] %s → enriching", url)

        website_intel = {}  # populated via /view/company — not in bulk path
        enrichment = await build_comprehensive_enrichment(url, profile, contact, website_intel={}, org_id=org_id)

        scoring = enrichment.get("lead_scoring", enrichment.get("scoring", {}))
        tier_raw = scoring.get("icp_match_tier") or scoring.get("score_tier", "")
        score_tier = _resolve_tier(tier_raw)
        now = datetime.now(timezone.utc).isoformat()

        pp = enrichment.get("person_profile", {})
        ident = enrichment.get("identity", {})
        prof_legacy = enrichment.get("professional", {})
        comp_legacy = enrichment.get("company", {})
        ci = enrichment.get("company_identity", {})
        cp = enrichment.get("company_profile", {})
        cin = enrichment.get("company_intelligence", {})
        intent_s = enrichment.get("intent_signals", {})
        outreach = enrichment.get("outreach", {})
        crm = enrichment.get("crm", {})
        ls = enrichment.get("lead_scoring", scoring)
        wi = enrichment.get("website_intelligence", website_intel or {})

        # Build waterfall log
        waterfall_log = [
            {"step": 0, "source": "Bright Data", "fields_found": ["profile"], "note": f"LinkedIn scrape: {url}"},
            *company_wf_log,
            {
                "step": 10,
                "source": contact.get("source") or "none",
                "fields_found": ["email"],
                "note": f"email={contact.get('email')}",
            },
        ]

        lead = {
            "id": _lead_id(url),
            "linkedin_url": url,
            "name": pp.get("full_name") or ident.get("full_name") or name,
            "first_name": first, "last_name": last,
            # Email fields intentionally empty — populated via POST /api/leads/view/email
            "work_email": None,
            "personal_email": None,
            "direct_phone": pp.get("direct_phone") or ident.get("direct_phone"),
            "twitter": pp.get("twitter") or ident.get("twitter") or contact.get("twitter"),
            "city": pp.get("city") or ident.get("city"),
            "country": pp.get("country") or ident.get("country"),
            "timezone": pp.get("timezone") or ident.get("timezone"),
            "title": pp.get("current_title") or prof_legacy.get("current_title") or profile.get("position", ""),
            "seniority_level": pp.get("seniority_level") or prof_legacy.get("seniority_level"),
            "department": pp.get("department") or prof_legacy.get("department"),
            "years_in_role": pp.get("years_in_role") or prof_legacy.get("years_in_role"),
            "company": ci.get("name") or comp_legacy.get("name") or profile.get("current_company_name", ""),
            "previous_companies": _safe_json(prof_legacy.get("previous_companies")),
            "top_skills": _safe_json(pp.get("top_skills") or prof_legacy.get("top_skills")),
            "education": pp.get("education") or prof_legacy.get("education"),
            "certifications": _safe_json(pp.get("certifications") or prof_legacy.get("certifications")),
            "languages": _safe_json(pp.get("languages") or prof_legacy.get("languages")),
            "company_website": cp.get("website") or ci.get("website") or comp_legacy.get("website"),
            "industry": cp.get("industry") or comp_legacy.get("industry"),
            "employee_count": _safe_int(cp.get("employee_count") or comp_legacy.get("employee_count")),
            "hq_location": cp.get("hq_location") or comp_legacy.get("hq_location"),
            "founded_year": str(cp.get("founded_year") or comp_legacy.get("founded_year") or ""),
            "funding_stage": cin.get("funding_stage") or comp_legacy.get("funding_stage"),
            "total_funding": cin.get("total_funding") or comp_legacy.get("total_funding"),
            "last_funding_date": cin.get("last_funding_date") or comp_legacy.get("last_funding_date"),
            "lead_investor": cin.get("lead_investor") or comp_legacy.get("lead_investor"),
            "annual_revenue": cp.get("annual_revenue_est") or comp_legacy.get("annual_revenue_est"),
            "tech_stack": _safe_json(cp.get("tech_stack") or comp_legacy.get("tech_stack")),
            "hiring_velocity": cin.get("hiring_velocity") or comp_legacy.get("hiring_velocity"),
            "avatar_url": _extract_avatar(profile) or contact.get("avatar_url"),
            "company_logo": cp.get("company_logo") or company_extras.get("company_logo"),
            "company_email": cp.get("company_email") or company_extras.get("company_email"),
            "company_description": wi.get("company_description") or company_extras.get("company_description"),
            "company_linkedin": ci.get("linkedin_url") or company_extras.get("company_linkedin") or contact.get("company_linkedin"),
            "company_twitter": cp.get("company_twitter") or company_extras.get("company_twitter"),
            "company_phone": cp.get("company_phone") or company_extras.get("company_phone"),
            "waterfall_log": json.dumps(waterfall_log),
            # Stage 3 — Website Intelligence
            "website_intelligence": json.dumps(website_intel or {}),
            "product_offerings": _safe_json(wi.get("product_offerings")),
            "value_proposition": wi.get("value_proposition"),
            "target_customers": _safe_json(wi.get("target_customers")),
            "business_model": wi.get("business_model"),
            "pricing_signals": wi.get("pricing_signals"),
            "product_category": wi.get("product_category"),
            "data_completeness_score": int(ls.get("data_completeness_score", 0)),
            # Intent signals
            "recent_funding_event": intent_s.get("recent_funding_event"),
            "hiring_signal": intent_s.get("hiring_signal"),
            "job_change": intent_s.get("job_change"),
            "linkedin_activity": intent_s.get("linkedin_activity"),
            "news_mention": intent_s.get("news_mention"),
            "product_launch": intent_s.get("product_launch"),
            "competitor_usage": intent_s.get("competitor_usage"),
            "review_activity": intent_s.get("review_activity"),
            # Scoring
            "icp_fit_score": _safe_int(ls.get("icp_fit_score")),
            "intent_score": _safe_int(ls.get("intent_score")),
            "timing_score": _safe_int(ls.get("timing_score")),
            "engagement_score": 0,
            "total_score": _safe_int(ls.get("overall_score")),
            "score_tier": score_tier,
            "score_explanation": ls.get("score_explanation"),
            "icp_match_tier": ls.get("icp_match_tier") or tier_raw,
            "disqualification_flags": _safe_json(ls.get("disqualification_flags")),
            "email_subject": outreach.get("email_subject"),
            "cold_email": outreach.get("cold_email"),
            "linkedin_note": outreach.get("linkedin_note"),
            "best_channel": outreach.get("best_channel"),
            "best_send_time": outreach.get("best_send_time"),
            "outreach_angle": outreach.get("outreach_angle"),
            "sequence_type": outreach.get("sequence_type"),
            "outreach_sequence": _safe_json(outreach.get("sequence")),
            "last_contacted": outreach.get("last_contacted"),
            "email_status": outreach.get("email_status"),
            "lead_source": crm.get("lead_source", "LinkedIn URL"),
            "enrichment_source": "Bright Data",
            "data_completeness": _safe_int(crm.get("data_completeness")),
            "crm_stage": crm.get("crm_stage"),
            "tags": _safe_json(crm.get("tags")),
            "assigned_owner": crm.get("assigned_owner"),
            "full_data": json.dumps({
                "person_profile": enrichment.get("person_profile", {}),
                "company_identity": enrichment.get("company_identity", {}),
                "website_intelligence": enrichment.get("website_intelligence", website_intel or {}),
                "company_profile": enrichment.get("company_profile", {}),
                "company_intelligence": enrichment.get("company_intelligence", {}),
                "intent_signals": enrichment.get("intent_signals", {}),
                "lead_scoring": enrichment.get("lead_scoring", {}),
                "outreach": enrichment.get("outreach", {}),
                # Legacy compatibility
                "identity": enrichment.get("identity", {}),
                "professional": enrichment.get("professional", {}),
                "company": enrichment.get("company", {}),
                "scoring": enrichment.get("scoring", {}),
                # Profile-level data for UI
                "recommendations": profile.get("recommendations") or [],
                "recommendations_count": profile.get("recommendations_count") or 0,
                "similar_profiles": profile.get("_similar_profiles") or [],
                "banner_image": profile.get("banner_image") or "",
                "linkedin_num_id": profile.get("linkedin_num_id") or "",
                "activity_emails": profile.get("_activity_emails") or [],
                "activity_phones": profile.get("_activity_phones") or [],
                "activity_full": profile.get("_activity_full") or [],
                "linkedin_posts": profile.get("_posts") or [],
                "hiring_signals": profile.get("_hiring_signals") or [],
            }, default=str),
            "raw_brightdata": json.dumps(profile, default=str),
            "about": (profile.get("about") or "")[:1000],
            "followers": _safe_int(profile.get("followers")),
            "connections": _safe_int(profile.get("connections")),
            # Email source fields empty — set when /view/email is called
            "email_source": None,
            "email_confidence": None,
            "apollo_raw": None,
            # ── Stage 3: COMPLETED — LLM done, full data saved ───────────────
            # Patch crm_brief before saving:
            #   1. profile_image + company_logo — LLM never has real image URLs
            #   2. crm_scores — LLM returns "High"/"Very High", normalize to integers
            "crm_brief": json.dumps(
                _patch_crm_brief_scores(
                    _patch_crm_brief_images(
                        enrichment.get("crm_brief"),
                        avatar_url=_extract_avatar(profile) or contact.get("avatar_url") or "",
                        company_logo=cp.get("company_logo") or company_extras.get("company_logo") or "",
                    ),
                    icp_fit_score=_safe_int(ls.get("icp_fit_score")),
                    intent_score=_safe_int(ls.get("intent_score")),
                    timing_score=_safe_int(ls.get("timing_score")),
                ),
                default=str,
            ) if enrichment.get("crm_brief") is not None else None,
            "status": "completed",
            "job_id": job_id,
            "organization_id": org_id,
            "enriched_at": now,
        }
        await _upsert_lead(lead)
        _plog(job_id, url, "COMPLETED", f"score_tier={score_tier!r} — lead saved and sent to LIO")
        logger.info("[Pipeline] %s → completed", url)

        # If crm_brief generation failed or empty — regenerate in background
        _saved_brief = _parse_json_safe(lead.get("crm_brief"), None)
        if not _validate_crm_brief(_saved_brief):
            logger.warning("[Pipeline] crm_brief missing/invalid for %s — scheduling background regeneration", url)
            asyncio.create_task(regenerate_crm_brief_for_lead(lead_id, org_id=org_id))

        return lead
    except Exception as e:
        import traceback
        _pipeline_log.error("[job=%s] [%s] FAILED — %s\n%s", (job_id or "")[-8:], raw_url, e, traceback.format_exc())
        logger.warning("[Webhook] %s: %s", raw_url, e)
        # Mark lead as failed if it was already saved in scraping/enriching stage
        try:
            lead_id = _lead_id(_normalize_linkedin_url(_clean_bd_linkedin_url(raw_url)))
            async with get_pool().acquire() as _conn:
                await _conn.execute(
                    "UPDATE enriched_leads SET status='failed', updated_at=NOW() WHERE id=$1 AND status IN ('scraping','enriching')",
                    lead_id,
                )
        except Exception:
            pass
        return None


async def rerun_brightdata_snapshot(snapshot_id: str) -> str:
    """Rerun a BrightData snapshot and return the new snapshot_id."""
    if not _bd_api_key():
        raise ValueError("BRIGHT_DATA_API_KEY not configured")
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{BD_BASE}/snapshot/{snapshot_id}/rerun",
            headers=_bd_headers(),
        )
        resp.raise_for_status()
    new_snapshot_id = resp.json().get("snapshot_id") or resp.json().get("id")
    if not new_snapshot_id:
        raise ValueError(f"No snapshot_id in rerun response: {resp.text[:200]}")
    logger.info("[Rerun] BD snapshot %s rerun → new snapshot_id=%s", snapshot_id, new_snapshot_id)
    return new_snapshot_id


async def cancel_brightdata_snapshot(snapshot_id: str) -> None:
    """Cancel an in-progress BrightData snapshot via the cancel API."""
    if not _bd_api_key():
        return
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{BD_BASE}/snapshot/{snapshot_id}/cancel",
                headers=_bd_headers(),
            )
            logger.info("[Cancel] BD snapshot %s cancel response: %s", snapshot_id, resp.status_code)
    except Exception as e:
        logger.warning("[Cancel] BD snapshot %s cancel failed: %s", snapshot_id, e)


async def process_webhook_profiles(
    profiles: list[dict],
    job_id: Optional[str] = None,
    sub_job_id: Optional[str] = None,
) -> dict:
    # Resolve org_id, sso_id, forward_to_lio from job record
    org_id = "default"
    sso_id = ""
    forward_to_lio = False
    if job_id:
        job = await get_job(job_id)
        if job:
            # Guard: discard webhook results if job was cancelled
            if job.get("status") == "cancelled":
                logger.info("[Webhook] job %s is cancelled — discarding %d profiles", job_id, len(profiles))
                return {"processed": 0, "failed": 0, "cancelled": True}
            org_id = job.get("organization_id") or "default"
            sso_id = job.get("sso_id") or ""
            forward_to_lio = bool(job.get("forward_to_lio", False))

    # Use pre-created sub_job (chunked BD path) or create one (legacy/webhook path)
    _sub_job_id = sub_job_id
    if job_id:
        try:
            if _sub_job_id:
                # Chunk was pre-created by enrich_bulk — mark it running
                await _update_sub_job(_sub_job_id, status="running", total_urls=len(profiles))
            else:
                async with get_pool().acquire() as conn:
                    chunk_index = await conn.fetchval(
                        "SELECT COUNT(*) FROM enrichment_sub_jobs WHERE job_id=$1", job_id
                    )
                _sub_job_id = str(uuid.uuid4())
                await _create_sub_job(_sub_job_id, job_id, int(chunk_index), len(profiles), org_id)
                await _update_sub_job(_sub_job_id, status="running")
        except Exception as e:
            logger.debug("[Webhook] sub_job setup failed: %s", e)
            _sub_job_id = None

    # Process all profiles fully in parallel — no lock needed.
    # The job counter UPDATE below uses LEAST() which is atomic at DB level
    # and handles concurrent increments safely without any application-level lock.
    coros = [_process_one_webhook_profile(p, job_id, org_id, sub_job_id=_sub_job_id, sso_id=sso_id) for p in profiles]
    results = await asyncio.gather(*coros, return_exceptions=True)

    ok_leads = [r for r in results if isinstance(r, dict)]
    processed = len(ok_leads)
    failed    = len(results) - processed

    logger.info("[Webhook] batch done: processed=%d failed=%d job=%s", processed, failed, job_id or "—")

    # Update sub_job to reflect completion of this chunk
    if _sub_job_id:
        try:
            sub_status = (
                "completed" if failed == 0
                else ("failed" if processed == 0 else "completed_with_errors")
            )
            await _update_sub_job(_sub_job_id, status=sub_status, processed=processed, failed=failed)
        except Exception as e:
            logger.debug("[Webhook] sub_job update failed: %s", e)

    # Atomic job progress update — avoids race condition when multiple webhook
    # calls arrive simultaneously (BrightData sends partial batches).
    # Cap increments so processed+failed never exceeds total_urls (prevents >100% progress).
    if job_id:
        async with get_pool().acquire() as conn:
            row = await conn.fetchrow(
                """UPDATE enrichment_jobs
                      SET processed   = LEAST(processed + $1, total_urls),
                          failed      = LEAST(failed + $2,    GREATEST(total_urls - LEAST(processed + $1, total_urls), 0)),
                          updated_at  = $3
                    WHERE id = $4
                RETURNING processed, failed, total_urls, organization_id, status""",
                processed, failed, datetime.now(timezone.utc), job_id,
            )
        if row:
            org_id = row["organization_id"] or org_id
            total_done = row["processed"] + row["failed"]
            if total_done >= row["total_urls"] and row["status"] not in ("completed", "failed", "completed_with_errors"):
                final_status = (
                    "completed" if row["failed"] == 0
                    else ("failed" if row["processed"] == 0 else "completed_with_errors")
                )
                await _update_job(job_id, status=final_status)
                await _publish_job_done(org_id, job_id, row["processed"], row["failed"])

    # Publish per-lead Ably events (real-time frontend updates)
    if job_id and ok_leads:
        for lead in ok_leads:
            await _publish_lead_done(org_id, job_id, lead)

    # Forward to LIO — mirror the single-enrich behaviour (send unless forward_to_lio=True)
    if not forward_to_lio and ok_leads:
        for lead in ok_leads:
            if lead.get("name"):
                lead.setdefault("linkedin_enrich", _format_linkedin_enrich(lead))
                asyncio.create_task(send_to_lio(lead, sso_id=sso_id))

    return {"processed": processed, "failed": failed}


async def regenerate_outreach_for_lead(lead_id: str) -> Optional[dict]:
    lead = await get_lead(lead_id)
    if not lead:
        return None

    org_id = lead.get("organization_id") or "default"

    # Use LIO Stage 5 system + user prompt — fast, targeted, config-driven
    # Let RuntimeError propagate so the route can return a proper error response
    generated = await generate_outreach_with_lio(lead, org_id=org_id)

    email_subject = generated["cold_email"]["subject"]
    cold_email    = generated["cold_email"]["full_email"] or generated["cold_email"]["body"]
    linkedin_note = generated["linkedin_note"]

    async with get_pool().acquire() as conn:
        await conn.execute(
            "UPDATE enriched_leads SET email_subject=$1, cold_email=$2, linkedin_note=$3 WHERE id=$4",
            email_subject, cold_email, linkedin_note, lead_id,
        )
    return await get_lead(lead_id)


async def regenerate_company_for_lead(lead_id: str) -> Optional[dict]:
    """
    Re-run AI analysis (culture_signals, account_pitch, company_tags) for the company
    associated with this lead. Updates both company_enrichments and enriched_leads tables.
    """
    lead = await get_lead(lead_id)
    if not lead:
        return None

    import company_service as cs
    from config.enrichment_config_service import get_workspace_config

    company_linkedin = lead.get("company_linkedin") or ""
    org_id = lead.get("organization_id") or "default"

    # Get current company record
    company_record = None
    if company_linkedin:
        company_record = await cs._get_company(company_linkedin, org_id)

    if not company_record:
        # Build a minimal record from lead fields so AI analysis can still run
        company_record = {
            "name":           lead.get("company") or "",
            "industry":       lead.get("industry") or "",
            "employee_count": lead.get("employee_count") or 0,
            "funding_stage":  lead.get("funding_stage") or "",
            "website":        lead.get("company_website") or "",
            "wappalyzer_tech": lead.get("wappalyzer_tech") or "[]",
            "linkedin_posts": lead.get("linkedin_posts") or "[]",
            "news_mentions":  lead.get("news_mentions") or "[]",
            "crunchbase_data": lead.get("crunchbase_data") or "{}",
        }

    ws_config = {}
    try:
        ws_config = await get_workspace_config(org_id)
    except Exception:
        pass

    # Run AI analysis
    ai = await cs.run_company_ai_analysis(company_record, ws_config)

    tags_json   = json.dumps(ai.get("company_tags") or [])
    culture_json = json.dumps(ai.get("culture_signals") or {})
    pitch_json   = json.dumps(ai.get("account_pitch") or {})

    # Update company record in company_enrichments table (if it exists)
    if company_linkedin and company_record.get("id"):
        await cs._upsert_company({
            **company_record,
            "company_tags":    tags_json,
            "culture_signals": culture_json,
            "account_pitch":   pitch_json,
        })

    # Update lead row
    async with get_pool().acquire() as conn:
        await conn.execute(
            "UPDATE enriched_leads SET company_tags=$1, culture_signals=$2, account_pitch=$3 WHERE id=$4",
            tags_json, culture_json, pitch_json, lead_id,
        )

    return await get_lead(lead_id)


async def regenerate_crm_brief_for_lead(lead_id: str, org_id: str = "") -> Optional[dict]:
    """
    Re-run the CRM brief LLM call for an already-enriched lead.
    Reads raw_profile from DB, uses lio_system_prompt + lio_model from
    workspace_configs, saves updated crm_brief back to enriched_leads.
    org_id is taken from the JWT (passed by the route) and falls back to
    the lead's stored organization_id so the right config is always loaded.
    """
    import re as _re
    from config.enrichment_config_service import get_workspace_config

    lead = await get_lead(lead_id)
    if not lead:
        return None

    # Prefer org_id from JWT (request), fall back to what's stored on the lead
    org_id = org_id or lead.get("organization_id") or "default"

    # Read raw BrightData profile from DB (raw_brightdata JSONB preferred, raw_profile TEXT fallback)
    raw_bd = lead.get("raw_brightdata")
    if raw_bd:
        profile = raw_bd if isinstance(raw_bd, dict) else (json.loads(raw_bd) if isinstance(raw_bd, str) else {})
    else:
        raw_profile_raw = lead.get("raw_profile")
        if not raw_profile_raw:
            raise RuntimeError("No raw BrightData profile found for this lead — cannot regenerate CRM brief.")
        try:
            profile = json.loads(raw_profile_raw) if isinstance(raw_profile_raw, str) else raw_profile_raw
        except Exception:
            profile = {}

    # Read config from workspace_configs
    try:
        cfg = await get_workspace_config(org_id)
        model_override   = cfg.get("lio_model", "").strip() or None
        crm_brief_prompt = cfg.get("lio_system_prompt", "").strip()
    except Exception:
        model_override   = None
        crm_brief_prompt = ""

    if not crm_brief_prompt:
        crm_brief_prompt = _DEFAULT_CRM_BRIEF_PROMPT

    # Build optimized, noise-free profile for the LLM; trim if combined content is too large
    optimized = _build_llm_profile(profile)
    _profile_budget = max(20000, 80000 - len(crm_brief_prompt))
    optimized = _trim_profile_to_budget(optimized, _profile_budget)
    optimized_str = json.dumps(optimized, separators=(",", ":"), ensure_ascii=False, default=str)
    logger.info(
        "[RegenerateCrmBrief] Optimized profile: %d chars (budget %d) ~%d tokens",
        len(optimized_str), _profile_budget, len(optimized_str) // 4,
    )

    brief = await _call_llm([
        {"role": "system", "content": crm_brief_prompt},
        {"role": "user",   "content": f"Analyze this LinkedIn prospect data and return the JSON exactly as specified:\n\n{optimized_str}"},
    ], max_tokens=4500, temperature=0.3, model_override=model_override, wb_llm_model_override=model_override,
       hf_first=True)

    if not brief:
        raise RuntimeError("LLM unavailable or returned no content.")

    # Strip <think>...</think> blocks (qwen3, deepseek, etc.)
    brief = _re.sub(r"<think>.*?</think>", "", brief, flags=_re.DOTALL)
    brief = _re.sub(r"<think>.*", "", brief, flags=_re.DOTALL).strip()
    # Strip any [inferred] markers the model may have added
    brief = brief.replace("[inferred]", "").replace("[Inferred]", "")

    # Extract first complete JSON object
    _start = brief.find("{")
    if _start != -1:
        _depth, _end = 0, _start
        for _i, _ch in enumerate(brief[_start:], _start):
            if _ch == "{": _depth += 1
            elif _ch == "}":
                _depth -= 1
                if _depth == 0:
                    _end = _i
                    break
        brief = brief[_start:_end + 1]

    try:
        crm_brief_db = json.dumps(json.loads(brief), default=str)
    except Exception:
        crm_brief_db = brief

    async with get_pool().acquire() as conn:
        await conn.execute(
            "UPDATE enriched_leads SET crm_brief=$1 WHERE id=$2",
            crm_brief_db, lead_id,
        )

    logger.info("[RegenerateCrmBrief] Updated crm_brief for lead=%s (%d chars)", lead_id, len(crm_brief_db))
    updated_lead = await get_lead(lead_id)
    # Auto-send to LIO after successful regen — but only if called standalone (not from send_to_lio itself)
    if updated_lead and updated_lead.get("name"):
        sso_id_for_lio = updated_lead.get("sso_id") or ""
        asyncio.create_task(send_to_lio(updated_lead, sso_id=sso_id_for_lio))
    return updated_lead

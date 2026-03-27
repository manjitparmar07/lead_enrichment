"""
ably_service.py
----------------
WorksBuddy Lead Enrichment — Ably real-time integration.

Two responsibilities:
  1. publish(channel, event, data)
       Called by workers after each lead/job completes.
       POSTs to Ably REST API — no SDK required.

  2. create_token_request(org_id, user_id)
       Returns a signed Ably TokenRequest for GET /api/ably/token.
       Capability scoped to tenant:{org_id}:job:* — subscribe only.
       API key never leaves the backend.

Token signing (Ably spec / RFC 2104):
  MAC = base64(HMAC-SHA256(keySecret,
      keyName + "\n" + ttl + "\n" + capability + "\n"
      + clientId + "\n" + timestamp + "\n" + nonce + "\n"))

Env:
  ABLY_API_KEY   appId.keyId:keySecret
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import secrets
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)

ABLY_REST_BASE = "https://rest.ably.io"
TOKEN_TTL_MS   = 3_600_000  # 1 hour


def _api_key() -> str:
    try:
        import keys_service as _ks
        return _ks.get("ABLY_API_KEY") or os.getenv("ABLY_API_KEY", "")
    except Exception:
        return os.getenv("ABLY_API_KEY", "")


def _split_key(api_key: str) -> tuple[str, str]:
    """'appId.keyId:keySecret' -> ('appId.keyId', 'keySecret')"""
    if not api_key or ":" not in api_key:
        raise ValueError("ABLY_API_KEY not set or malformed — expected appId.keyId:keySecret")
    key_name, key_secret = api_key.split(":", 1)
    return key_name, key_secret


# ─────────────────────────────────────────────────────────────────────────────
# Signed token request — pure HMAC, zero HTTP calls
# ─────────────────────────────────────────────────────────────────────────────

def create_token_request(org_id: str, user_id: str) -> dict:
    """
    Build and sign an Ably TokenRequest.

    Capability: "tenant:{org_id}:job:*" → ["subscribe"]
    - Tenant can only subscribe to their own job channels.
    - Cannot publish, cannot access other tenants.

    The frontend passes this dict to the Ably Realtime client which
    exchanges it with Ably servers directly — key stays on backend.
    """
    api_key              = _api_key()
    key_name, key_secret = _split_key(api_key)

    capability = json.dumps(
        {f"tenant:{org_id}:job:*": ["subscribe"]},
        separators=(",", ":"),
    )
    timestamp = int(time.time() * 1000)
    nonce     = secrets.token_hex(16)

    # Ably canonical signing string — fields separated by \n, trailing \n required
    canonical = "\n".join([
        key_name,
        str(TOKEN_TTL_MS),
        capability,
        user_id,
        str(timestamp),
        nonce,
        "",
    ])

    mac = base64.b64encode(
        hmac.new(
            key_secret.encode("utf-8"),
            canonical.encode("utf-8"),
            hashlib.sha256,
        ).digest()
    ).decode("utf-8")

    return {
        "keyName":    key_name,
        "ttl":        TOKEN_TTL_MS,
        "capability": capability,
        "clientId":   user_id,
        "timestamp":  timestamp,
        "nonce":      nonce,
        "mac":        mac,
    }


# ─────────────────────────────────────────────────────────────────────────────
# REST publish — called by workers after each lead / job event
# ─────────────────────────────────────────────────────────────────────────────

async def publish(channel: str, event: str, data: Any) -> None:
    """
    Publish a message to an Ably channel via REST.

      channel  'tenant:org123:job:<uuid>'
      event    'lead:done' | 'job:done'
      data     JSON-serialisable dict

    Never raises — a publish failure logs a warning but never
    interrupts the enrichment pipeline.
    """
    api_key = _api_key()
    if not api_key:
        logger.debug("[Ably] ABLY_API_KEY not set — skipping publish to %s", channel)
        return

    try:
        # ':' in channel names must be percent-encoded in the REST URL
        encoded_channel = channel.replace(":", "%3A")
        url     = f"{ABLY_REST_BASE}/channels/{encoded_channel}/messages"
        payload = {
            "name": event,
            "data": json.dumps(data) if isinstance(data, (dict, list)) else str(data),
        }
        auth = base64.b64encode(api_key.encode()).decode()

        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.post(
                url, json=payload,
                headers={"Authorization": f"Basic {auth}", "Content-Type": "application/json"},
            )

        if resp.status_code not in (200, 201):
            logger.warning(
                "[Ably] Publish failed  channel=%s  event=%s  status=%d  body=%s",
                channel, event, resp.status_code, resp.text[:200],
            )
        else:
            logger.debug("[Ably] Published  event=%s  channel=%s", event, channel)

    except Exception as e:
        logger.warning("[Ably] Publish error  channel=%s  event=%s: %s", channel, event, e)

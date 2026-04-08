"""
ably_routes.py
---------------
GET /api/ably/token  — issues a signed Ably TokenRequest for the frontend.

The frontend (LeadEnrichmentPage.jsx) calls:
  new Realtime({ authUrl: `${BACKEND}/ably/token`, authParams: { org_id, user_id } })

This endpoint:
  1. Extracts org_id + user_id from query params (set by frontend).
  2. Falls back to the JWT in Authorization header if params are missing.
  3. Returns a signed TokenRequest JSON — Ably client exchanges this with
     Ably servers directly, so the API key never reaches the browser.
"""

from __future__ import annotations

import base64
import json
import logging

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from realtime import ably_service

logger  = logging.getLogger(__name__)
router  = APIRouter(prefix="/ably", tags=["Ably"])


def _org_and_user_from_jwt(request: Request) -> tuple[str, str]:
    """Decode org_id and user_id from the JWT Authorization header (no verification)."""
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return "default", "anonymous"
    token = auth[7:].strip()
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return "default", "anonymous"
        padded  = parts[1] + "=" * (4 - len(parts[1]) % 4)
        payload = json.loads(base64.b64decode(padded))
        org_id  = str(payload.get("organization_id", "default"))
        user_id = str(payload.get("user_id") or payload.get("sub") or org_id)
        return org_id, user_id
    except Exception:
        return "default", "anonymous"


@router.get("/token")
async def ably_token(
    request: Request,
    org_id:  str = Query(default=""),
    user_id: str = Query(default=""),
):
    """
    Issue a signed Ably TokenRequest.

    Query params (set by the Ably Realtime authParams):
      org_id   — organisation ID (from JWT payload)
      user_id  — user ID (from JWT payload)

    Falls back to JWT header if params are empty.

    Response is a JSON object that the Ably client exchanges for a
    real connection token.  Format:
      { keyName, ttl, capability, clientId, timestamp, nonce, mac }
    """
    # Prefer explicit query params (sent by frontend Ably authParams)
    # Fall back to JWT header for server-to-server calls
    if not org_id or not user_id:
        org_id, user_id = _org_and_user_from_jwt(request)

    try:
        token_request = ably_service.create_token_request(org_id, user_id)
        logger.debug("[AblyToken] Issued token for org=%s user=%s", org_id, user_id)
        return JSONResponse(content=token_request)
    except ValueError as e:
        # ABLY_API_KEY not configured
        logger.warning("[AblyToken] %s", e)
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error("[AblyToken] Token request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Token generation failed")

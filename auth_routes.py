"""
auth_routes.py — WorksBuddy SSO login + JWT token issue

Flow:
  POST /api/auth/login  →  call WorksBuddy SSO → check admin + org → issue JWT
  GET  /api/auth/verify →  validate JWT → return user payload
"""
import os
from datetime import datetime, timedelta, timezone
from typing import Optional
from pathlib import Path

# Load .env if present
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

import httpx
import jwt
from fastapi import APIRouter, Depends, HTTPException, Header
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel

_bearer_scheme = HTTPBearer(auto_error=False)

router = APIRouter(tags=["auth"])

# ── Config ────────────────────────────────────────────────────────────────────
SSO_URL             = os.getenv("SSO_API_URL", "https://api-accounts.worksbuddy.ai")
JWT_SECRET          = os.getenv("JWT_SECRET", "")
JWT_ALGORITHM       = os.getenv("JWT_ALGORITHM", "HS256")
JWT_EXPIRE_HOURS    = int(os.getenv("JWT_ACCESS_TOKEN_EXPIRE_HOURS", "8"))
SSO_TIMEOUT         = 15


class LoginRequest(BaseModel):
    email: str
    password: str


# ── Token helpers ─────────────────────────────────────────────────────────────

def _issue_token(payload: dict) -> str:
    data = payload.copy()
    data["exp"] = datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRE_HOURS)
    data["iat"] = datetime.now(timezone.utc)
    return jwt.encode(data, JWT_SECRET, algorithm=JWT_ALGORITHM)


def _decode_token(token: str) -> dict:
    if not JWT_SECRET:
        raise HTTPException(status_code=500, detail="JWT_SECRET not configured on server.")
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Session expired. Please log in again.")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token.")


# ── FastAPI dependency ────────────────────────────────────────────────────────

async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer_scheme),
) -> dict:
    """FastAPI dependency — validates Bearer JWT and returns the decoded payload."""
    if not credentials:
        raise HTTPException(status_code=401, detail="Authorization header required.")
    return _decode_token(credentials.credentials)


# ── Routes ────────────────────────────────────────────────────────────────────

@router.post("/auth/login")
async def login(body: LoginRequest):
    """
    Authenticate via WorksBuddy SSO.
    Only 'admin' users from the WorksBuddy organization are allowed.
    """
    if not JWT_SECRET:
        raise HTTPException(status_code=500, detail="JWT_SECRET not configured on server.")

    # ── Call WorksBuddy SSO ───────────────────────────────────────────────────
    try:
        async with httpx.AsyncClient(timeout=SSO_TIMEOUT) as client:
            res = await client.post(
                f"{SSO_URL}/api/auth/login",
                json={"email": body.email, "password": body.password},
                headers={"Content-Type": "application/json"},
            )
    except httpx.TimeoutException:
        raise HTTPException(status_code=503, detail="Authentication service unavailable. Please try again.")
    except httpx.RequestError:
        raise HTTPException(status_code=503, detail="Cannot reach authentication service.")

    if res.status_code in (401, 403):
        raise HTTPException(status_code=401, detail="Invalid email or password.")
    if res.status_code >= 400:
        raise HTTPException(status_code=502, detail="Authentication service error. Please try again.")

    data = res.json()
    user = data.get("user") or data

    # ── Allowed organizations (name or slug match) ────────────────────────────
    ALLOWED_ORG_NAMES  = {"worksbuddy", "lbm solution", "lbmsolution"}
    ALLOWED_ORG_SLUGS  = {"worksbuddy", "lbm-solution", "lbmsolution"}

    org      = user.get("organization") or {}
    org_name = (org.get("name") or "").strip()
    org_slug = (org.get("slug") or "").strip()
    org_id   = str(org.get("id") or user.get("organization_id") or "")

    org_allowed = (
        org_name.lower() in ALLOWED_ORG_NAMES or
        org_slug.lower() in ALLOWED_ORG_SLUGS
    )

    # ── Extract role ──────────────────────────────────────────────────────────
    raw_role = user.get("role", "")
    role = raw_role.get("name", "").lower() if isinstance(raw_role, dict) else str(raw_role).lower()

    # ── Check isOwner ─────────────────────────────────────────────────────────
    is_owner = bool(user.get("isOwner") or user.get("is_owner"))

    # ── All three conditions must pass ────────────────────────────────────────
    if not (org_allowed and role == "admin" and is_owner):
        raise HTTPException(status_code=401, detail="Wrong credentials.")

    # ── Build user context ────────────────────────────────────────────────────
    user_id   = str(user.get("id") or user.get("user_id") or "")
    email     = user.get("email", "")
    first     = user.get("firstName") or user.get("first_name") or ""
    last      = user.get("lastName")  or user.get("last_name")  or ""
    full_name = f"{first} {last}".strip() or email
    org_display = org_name or "WorksBuddy"

    # ── Issue token ───────────────────────────────────────────────────────────
    token = _issue_token({
        "user_id":         user_id,
        "email":           email,
        "full_name":       full_name,
        "organization_id": org_id,
        "role":            role,
        "source":          "ai-brain",
        "platform":        "worksbuddy",
    })

    return {
        "access_token": token,
        "token_type":   "bearer",
        "expires_in":   JWT_EXPIRE_HOURS * 3600,
        "user": {
            "user_id":         user_id,
            "email":           email,
            "full_name":       full_name,
            "role":            role,
            "organization":    org_display,
            "organization_id": org_id,
        },
    }


@router.get("/auth/verify")
async def verify(authorization: Optional[str] = Header(None)):
    """Verify a JWT and return the user payload."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Authorization header required.")
    token = authorization.split(" ", 1)[1]
    payload = _decode_token(token)
    return {"valid": True, "user": payload}

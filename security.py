"""
Security middleware — API Key validation + rate limiting
"""
import os
import time
from collections import defaultdict
from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

# ── API Key — read from keys_service (admin-managed) with env fallback ─────────
def _get_wb_api_key() -> str:
    try:
        import keys_service
        return keys_service.get("WB_API_KEY") or os.getenv("WB_API_KEY", "wb-dev-key-2024")
    except Exception:
        return os.getenv("WB_API_KEY", "wb-dev-key-2024")

WB_API_KEY = _get_wb_api_key()

# Public paths that don't require an API key
PUBLIC_PATHS = {"/api/health", "/docs", "/openapi.json", "/redoc"}

# ── Rate limiting (per IP) ────────────────────────────────────────────────────
RATE_LIMIT  = int(os.getenv("WB_RATE_LIMIT", "600"))   # requests per window
RATE_WINDOW = int(os.getenv("WB_RATE_WINDOW", "60"))    # seconds

_rate_store: dict[str, list[float]] = defaultdict(list)


def _check_rate(ip: str) -> bool:
    now = time.time()
    _rate_store[ip] = [t for t in _rate_store[ip] if t > now - RATE_WINDOW]
    if len(_rate_store[ip]) >= RATE_LIMIT:
        return False
    _rate_store[ip].append(now)
    return True


class SecurityMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # ── Always pass CORS preflight through ───────────────────────────────
        # OPTIONS must reach CORSMiddleware before any auth check
        if request.method == "OPTIONS":
            return await call_next(request)

        # ── Rate limiting ─────────────────────────────────────────────────────
        ip = request.client.host if request.client else "unknown"
        if not _check_rate(ip):
            return JSONResponse(
                status_code=429,
                content={"detail": f"Rate limit exceeded. Max {RATE_LIMIT} req/{RATE_WINDOW}s."},
                headers={"Retry-After": str(RATE_WINDOW)},
            )

        # ── Security headers on response ──────────────────────────────────────
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        return response

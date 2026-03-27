"""
main.py — WorksBuddy Lead Enrichment (Standalone)
FastAPI app: only lead enrichment + auth + key management
"""
import logging
import os
from pathlib import Path

# ── Load .env if present ──────────────────────────────────────────────────────
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from auth_routes import router as auth_router
from lead_enrichment_brightdata_routes import (
    router as lead_enrichment_router,
    _linkedin_enrich_router,
    _email_enrich_router,
    _outreach_enrich_router,
    _company_enrich_router,
)
from keys_routes import router as keys_router
from ably_routes import router as ably_router
from workspace_routes import router as workspace_router
from enrichment_config_routes import router as enrichment_config_router
from company_routes import router as company_router
from ai_enrichment_routes import router as ai_enrichment_router
from analytics_routes import router as analytics_router
from security import SecurityMiddleware
import keys_service
import lead_enrichment_worker as worker
from lead_enrichment_brightdata_service import init_leads_db
from workspace_service import init_workspace_db
from enrichment_config_service import init_config_db
from company_service import init_company_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="WorksBuddy Lead Enrichment API",
    version="1.0.0",
    description=(
        "## Enrichment View APIs\n\n"
        "Each enriched lead exposes 4 focused views by `lead_id`:\n\n"
        "| Endpoint | Description |\n"
        "|---|---|\n"
        "| `GET /api/leads/{lead_id}/linkedin` | Full LinkedIn profile — identity, contact, scores, ICP, signals, activity, tags |\n"
        "| `GET /api/leads/{lead_id}/email` | Email enrichment — Apollo/Hunter result, confidence, verification |\n"
        "| `GET /api/leads/{lead_id}/outreach` | AI outreach — cold email, LinkedIn note, sequence, pitch |\n"
        "| `GET /api/leads/{lead_id}/company` | Company enrichment — website intel, market signals, intent, scores |\n\n"
        "## Bulk Enrichment\n\n"
        "- `POST /api/leads/enrich/bulk` — Submit up to 5000 LinkedIn URLs (JWT token in body)\n"
        "- `GET  /api/leads/jobs` — List all enrichment jobs\n"
        "- `GET  /api/leads/jobs/{job_id}` — Poll job status + progress\n\n"
        "## AI Enrichment\n\n"
        "Pass a BrightData profile object as `{ \"profile\": { ... } }` for standalone AI analysis.\n\n"
        "**Output:** Identity · Contact · Scores · ICP Match · Behavioural Signals · Pitch Intelligence · Tags · Outreach"
    ),
)

# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(auth_router,               prefix="/api", include_in_schema=False)
app.include_router(lead_enrichment_router,    prefix="/api")
# ── 4 Enrichment View APIs (visible in /docs) ─────────────────────────────────
app.include_router(_linkedin_enrich_router,   prefix="/api")
app.include_router(_email_enrich_router,      prefix="/api")
app.include_router(_outreach_enrich_router,   prefix="/api")
app.include_router(_company_enrich_router,    prefix="/api")
# ──────────────────────────────────────────────────────────────────────────────
app.include_router(keys_router,               prefix="/api", include_in_schema=False)
app.include_router(ably_router,               prefix="/api", include_in_schema=False)
app.include_router(workspace_router,          prefix="/api", include_in_schema=False)
app.include_router(enrichment_config_router,  prefix="/api", include_in_schema=False)
app.include_router(company_router,            prefix="/api", include_in_schema=False)
app.include_router(ai_enrichment_router,      prefix="/api")
app.include_router(analytics_router,          prefix="/api")

# ── CORS ──────────────────────────────────────────────────────────────────────
# Production requests go through nginx which owns CORS headers.
# This middleware covers direct access to port 8020 (dev / internal tools).
_PROD_ORIGINS = [
    "https://lead-enrichment.worksbuddy.ai",
    "https://leads.worksbuddy.ai",
]
_extra = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "").split(",") if o.strip()]
_origins = list(dict.fromkeys(_PROD_ORIGINS + _extra))

app.add_middleware(SecurityMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_origin_regex=r"https?://(localhost|127\.0\.0\.1|192\.168\.\d+\.\d+|10\.\d+\.\d+\.\d+)(:\d+)?",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# ── Startup / Shutdown ────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    keys_service.reload()
    await init_leads_db()
    await init_workspace_db()
    await init_config_db()
    await init_company_db()
    await worker.start_workers()
    logger.info("Lead Enrichment API started — http://0.0.0.0:%s", os.getenv("PORT", "8020"))


@app.on_event("shutdown")
async def shutdown():
    await worker.stop_workers()
    logger.info("Lead Enrichment API stopped")

# ── Health ────────────────────────────────────────────────────────────────────
@app.get("/api/health", tags=["Health"], include_in_schema=False)
async def health():
    return {"status": "ok", "service": "lead-enrichment"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8020")), reload=True)

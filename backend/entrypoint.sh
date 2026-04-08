#!/bin/sh
set -e

# ── Pre-flight checks ─────────────────────────────────────────────────────────

# 1. Critical env vars — fail fast with clear message
if [ -z "$DATABASE_URL" ]; then
    echo "ERROR: DATABASE_URL is not set. Mount backend/.env or set it in docker-compose environment."
    exit 1
fi
if [ -z "$JWT_SECRET" ]; then
    echo "ERROR: JWT_SECRET is not set. Auth will not work."
    exit 1
fi

# 2. Port conflict check
PORT="${PORT:-8020}"
if command -v ss >/dev/null 2>&1; then
    if ss -tlnp 2>/dev/null | grep -q ":${PORT} "; then
        echo "WARNING: Port ${PORT} is already in use. Container may fail to bind."
    fi
fi

# 3. Disk space warning (warn if < 500 MB free)
if command -v df >/dev/null 2>&1; then
    FREE_KB=$(df /app 2>/dev/null | awk 'NR==2 {print $4}')
    if [ -n "$FREE_KB" ] && [ "$FREE_KB" -lt 512000 ] 2>/dev/null; then
        echo "WARNING: Low disk space — ${FREE_KB} KB free on /app. Logs or uploads may fail."
    fi
fi

# ── Seed configs volume on first run ─────────────────────────────────────────
# If the mounted /app/configs is empty, copy the defaults baked into the image.
# cp -rn = no-overwrite, so existing files on the volume are never touched.
if [ -d /app/configs_seed ] && [ "$(ls -A /app/configs_seed 2>/dev/null)" ]; then
    cp -rn /app/configs_seed/. /app/configs/
fi

# ── Ensure system_keys.json exists (gitignored, so not baked into image) ─────
[ -f /app/configs/system_keys.json ] || echo '{}' > /app/configs/system_keys.json

# ── Redis URL default — use docker-compose service name if not overridden ─────
export REDIS_URL="${REDIS_URL:-redis://redis:6379}"

# ── Start gunicorn ────────────────────────────────────────────────────────────
exec gunicorn main:app \
    --worker-class uvicorn.workers.UvicornWorker \
    --workers 2 \
    --bind 0.0.0.0:${PORT:-8020} \
    --timeout 120 \
    --keep-alive 5 \
    --access-logfile - \
    --error-logfile - \
    --log-level info

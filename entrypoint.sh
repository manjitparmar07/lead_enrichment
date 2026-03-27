#!/bin/sh
set -e

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
    --bind 0.0.0.0:8020 \
    --timeout 120 \
    --keep-alive 5 \
    --access-logfile - \
    --error-logfile - \
    --log-level info

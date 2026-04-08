# ── Stage 1: Build ───────────────────────────────────────────────────────────
FROM node:20-alpine AS builder

WORKDIR /app

# Install dependencies first (layer cache)
COPY package.json package-lock.json ./
RUN npm ci --frozen-lockfile

# Copy source and build
COPY . .

# VITE_BACKEND_URL is empty so nginx handles the /api proxy at runtime.
# Override at build time if you need a direct absolute URL instead:
#   docker build --build-arg VITE_BACKEND_URL=https://api.example.com
ARG VITE_BACKEND_URL=""
ENV VITE_BACKEND_URL=$VITE_BACKEND_URL

RUN npm run build


# ── Stage 2: Serve ───────────────────────────────────────────────────────────
FROM nginx:1.27-alpine

# Default port — envsubst needs this set or nginx "listen ${PORT}" will fail
ENV PORT=80
ENV BACKEND_HOST=http://localhost:8020

# Static SPA — no proxy needed, React calls backend via VITE_BACKEND_URL
COPY nginx.conf /etc/nginx/templates/default.conf.template

# Copy built React bundle from builder
COPY --from=builder /app/dist /usr/share/nginx/html

EXPOSE ${PORT:-80}

CMD ["nginx", "-g", "daemon off;"]
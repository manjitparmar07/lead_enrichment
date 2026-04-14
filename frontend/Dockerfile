### Stage 1 — Build React app
FROM node:20-alpine AS builder

WORKDIR /app
COPY package*.json ./
RUN npm install --silent
COPY . .
RUN npm run build

### Stage 2 — Serve with Nginx
FROM nginx:alpine

# Install envsubst (part of gettext) for runtime PORT substitution
RUN apk add --no-cache gettext

# Remove default nginx config
RUN rm /etc/nginx/conf.d/default.conf

# Copy built assets
COPY --from=builder /app/dist /usr/share/nginx/html

# Nginx config template — uses $PORT injected by Railway at runtime
COPY nginx-docker.conf /etc/nginx/conf.d/default.conf.template

EXPOSE 80

# At startup: substitute $PORT in the config template, then start nginx
CMD ["/bin/sh", "-c", "export PORT=${PORT:-80} && envsubst '${PORT}' < /etc/nginx/conf.d/default.conf.template > /etc/nginx/conf.d/default.conf && nginx -g 'daemon off;'"]

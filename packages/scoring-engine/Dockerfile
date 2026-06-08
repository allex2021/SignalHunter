# ─── Stage 1: Builder ───────────────────────────────────────────────────────
FROM node:20-alpine AS builder

# Install build tools for native modules
RUN apk add --no-cache python3 make g++ git

WORKDIR /app

# Copy workspace root manifests first (for layer caching)
COPY package*.json ./
COPY packages/scoring-engine/package*.json ./packages/scoring-engine/
COPY packages/shared-types/package*.json ./packages/shared-types/

# Install dependencies (including workspace links)
RUN npm install --ignore-scripts

# Copy source
COPY packages/shared-types ./packages/shared-types
COPY packages/scoring-engine ./packages/scoring-engine

# Build shared-types first
WORKDIR /app/packages/shared-types
RUN npm run build 2>/dev/null || true

# Build scoring-engine
WORKDIR /app/packages/scoring-engine
RUN npm run build

# ─── Stage 2: Production Runtime ────────────────────────────────────────────
FROM node:20-alpine AS runtime

# Security hardening
RUN apk add --no-cache tini curl \
  && addgroup -g 1001 -S appgroup \
  && adduser -u 1001 -S appuser -G appgroup

WORKDIR /app

# Copy built artifacts and prod dependencies
COPY --from=builder /app/packages/scoring-engine/dist ./dist
COPY --from=builder /app/packages/scoring-engine/package*.json ./
COPY --from=builder /app/packages/shared-types/dist ./node_modules/@csa/shared-types/dist
COPY --from=builder /app/packages/shared-types/package*.json ./node_modules/@csa/shared-types/

# Install production dependencies only
RUN npm install --omit=dev --ignore-scripts \
  && npm cache clean --force

# Non-root user
USER appuser

# Environment defaults
ENV NODE_ENV=production \
    REDIS_URL=redis://redis:6379 \
    AI_BRAIN_URL=http://ai-brain:3000 \
    WS_PORT=3001 \
    SCORING_INTERVAL_MS=300000 \
    WINDOW_DURATION_MS=7200000

EXPOSE 3001

# Health check via WebSocket port
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
  CMD curl -sf http://localhost:3001/ || exit 1

# Use tini as init process for proper signal handling
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["node", "dist/aggregator.js"]

# =============================================================================
# AI Brain Gateway — Multi-stage Docker Build
# Stage 1: TypeScript compilation
# Stage 2: Lean production image (Node 20 Alpine, non-root user)
# =============================================================================

# ---- Stage 1: Builder -------------------------------------------------------
FROM node:20-alpine AS builder

LABEL stage="builder"

# Install build dependencies
RUN apk add --no-cache python3 make g++

WORKDIR /workspace

# Copy root package.json and workspace manifests first for better layer caching
COPY package.json package-lock.json* ./

# Copy package manifests for workspaces
COPY packages/shared-types/package.json ./packages/shared-types/
COPY apps/ai-brain/package.json ./apps/ai-brain/

# Install all dependencies (including devDependencies for TypeScript build)
RUN npm ci --workspace=packages/shared-types --workspace=apps/ai-brain --include-workspace-root 2>/dev/null || \
    npm install --workspace=packages/shared-types --workspace=apps/ai-brain

# Copy source code
COPY packages/shared-types/ ./packages/shared-types/
COPY apps/ai-brain/ ./apps/ai-brain/

# Build shared-types first (ai-brain depends on it)
RUN npm run build --workspace=packages/shared-types

# Build ai-brain
RUN npm run build --workspace=apps/ai-brain

# Prune dev dependencies for production install
RUN npm ci --workspace=packages/shared-types --workspace=apps/ai-brain --omit=dev 2>/dev/null || \
    npm install --workspace=packages/shared-types --workspace=apps/ai-brain --omit=dev

# ---- Stage 2: Production Runtime -------------------------------------------
FROM node:20-alpine AS production

LABEL maintainer="crypto-signal-aggregator"
LABEL description="AI Brain Gateway — Unified Free AI provider failover router"
LABEL version="1.0.0"

# Install curl for health check probing
RUN apk add --no-cache curl dumb-init

# Create non-root user and group for security
RUN addgroup -g 1001 -S appgroup && \
    adduser -u 1001 -S appuser -G appgroup

WORKDIR /app

# Copy compiled artifacts from builder
COPY --from=builder --chown=appuser:appgroup /workspace/packages/shared-types/dist ./packages/shared-types/dist/
COPY --from=builder --chown=appuser:appgroup /workspace/packages/shared-types/package.json ./packages/shared-types/
COPY --from=builder --chown=appuser:appgroup /workspace/apps/ai-brain/dist ./apps/ai-brain/dist/
COPY --from=builder --chown=appuser:appgroup /workspace/apps/ai-brain/package.json ./apps/ai-brain/
COPY --from=builder --chown=appuser:appgroup /workspace/node_modules ./node_modules/

# Copy root package.json for workspace resolution
COPY --from=builder --chown=appuser:appgroup /workspace/package.json ./

# Switch to non-root user
USER appuser

# Expose the gateway port
EXPOSE 3000

# Environment defaults (override via docker run -e or docker-compose)
ENV NODE_ENV=production \
    PORT=3000 \
    REDIS_URL=redis://redis:6379

# Health check — probe /health every 30s, timeout 5s, 3 retries, 10s start period
HEALTHCHECK --interval=30s --timeout=5s --retries=3 --start-period=10s \
    CMD curl -f http://localhost:${PORT}/health || exit 1

# Use dumb-init for proper signal forwarding to Node.js
# This ensures SIGTERM is correctly forwarded for graceful shutdown
ENTRYPOINT ["dumb-init", "--"]
CMD ["node", "apps/ai-brain/dist/gateway.js"]

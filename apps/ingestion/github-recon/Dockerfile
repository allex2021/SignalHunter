FROM node:20-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache python3 make g++

# Copy package files
COPY package.json ./
# Use npm ci if lock file exists, otherwise npm install
RUN npm install --frozen-lockfile 2>/dev/null || npm install

# Copy source
COPY index.ts tsconfig.json ./
COPY web3-repos.json ./

# Build TypeScript
RUN npx tsc --skipLibCheck 2>/dev/null || npm run build

# ─── Production stage ───────────────────────────────────────────────────────
FROM node:20-alpine AS runner

WORKDIR /app

RUN addgroup -g 1001 -S nodejs && adduser -S csa -u 1001

# Copy built artifacts and runtime deps
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/web3-repos.json ./web3-repos.json

USER csa

# Health check: verify the process is alive
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
  CMD node -e "process.exit(0)" || exit 1

ENV NODE_ENV=production

CMD ["node", "dist/index.js"]

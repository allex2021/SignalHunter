#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# health-check.sh — System Health Check for Crypto Signal Aggregator
#
# Usage: bash scripts/health-check.sh
# Exit code: 0 if all healthy, 1 if any service unhealthy
#
# Banglish logs: "System status check hocche...", "Sob service running ✓"
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

# ── Colors ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m' # No Color

# ── Config (read from .env if present) ───────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

if [[ -f "${PROJECT_ROOT}/.env" ]]; then
  # shellcheck disable=SC2046
  export $(grep -v '^#' "${PROJECT_ROOT}/.env" | grep -v '^$' | xargs) 2>/dev/null || true
fi

REDIS_HOST="${REDIS_HOST:-localhost}"
REDIS_PORT="${REDIS_PORT:-6379}"
AI_BRAIN_URL="${AI_BRAIN_URL:-http://localhost:3000}"
WS_PORT="${WS_PORT:-3001}"
TIMEOUT=5

# ── State ─────────────────────────────────────────────────────────────────────
TOTAL_CHECKS=0
FAILED_CHECKS=0

# ── Helpers ───────────────────────────────────────────────────────────────────
print_header() {
  echo ""
  echo -e "${BOLD}${CYAN}╔══════════════════════════════════════════════════════╗${NC}"
  echo -e "${BOLD}${CYAN}║     ⚡ ALPHA INTELLIGENCE — System Health Check      ║${NC}"
  echo -e "${BOLD}${CYAN}╚══════════════════════════════════════════════════════╝${NC}"
  echo ""
  echo -e "${DIM}Timestamp: $(date -u '+%Y-%m-%d %H:%M:%S UTC')${NC}"
  echo -e "${DIM}System status check hocche...${NC}"
  echo ""
}

print_section() {
  echo -e "${BOLD}${CYAN}── $1 ──────────────────────────────────────────────────${NC}"
}

pass() {
  local name="$1"
  local detail="${2:-}"
  TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
  echo -e "  ${GREEN}✓${NC} ${BOLD}${name}${NC}${detail:+  ${DIM}${detail}${NC}}"
}

fail() {
  local name="$1"
  local detail="${2:-}"
  TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
  FAILED_CHECKS=$((FAILED_CHECKS + 1))
  echo -e "  ${RED}✗${NC} ${BOLD}${name}${NC}${detail:+  ${DIM}${detail}${NC}}"
}

warn() {
  local name="$1"
  local detail="${2:-}"
  echo -e "  ${YELLOW}⚠${NC} ${BOLD}${name}${NC}${detail:+  ${DIM}${detail}${NC}}"
}

# ─────────────────────────────────────────────────────────────────────────────
print_header

# ── 1. Redis ──────────────────────────────────────────────────────────────────
print_section "Redis"

if command -v redis-cli &>/dev/null; then
  REDIS_RESPONSE=$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" --no-auth-warning ping 2>/dev/null || echo "FAILED")
  if [[ "$REDIS_RESPONSE" == "PONG" ]]; then
    # Get additional info
    REDIS_VERSION=$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" --no-auth-warning INFO server 2>/dev/null | grep "redis_version" | tr -d '\r' | cut -d: -f2 || echo "?")
    REDIS_CLIENTS=$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" --no-auth-warning INFO clients 2>/dev/null | grep "connected_clients" | tr -d '\r' | cut -d: -f2 || echo "?")
    pass "Redis" "${REDIS_HOST}:${REDIS_PORT} v${REDIS_VERSION// /} | clients=${REDIS_CLIENTS// /}"

    # Check for signal streams
    for STREAM in stream:github stream:whale stream:sec stream:social stream:reddit stream:signals; do
      STREAM_LEN=$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" --no-auth-warning XLEN "$STREAM" 2>/dev/null || echo "0")
      if [[ "$STREAM_LEN" -gt "0" ]]; then
        pass "  Stream ${STREAM}" "len=${STREAM_LEN}"
      else
        warn "  Stream ${STREAM}" "empty (len=0) — provider chole ki?"
      fi
    done
  else
    fail "Redis" "${REDIS_HOST}:${REDIS_PORT} — connection failed (Response: ${REDIS_RESPONSE})"
  fi
else
  warn "redis-cli" "Not installed — Redis check skip korchi"
fi
echo ""

# ── 2. AI Brain HTTP ──────────────────────────────────────────────────────────
print_section "AI Brain Gateway"

if command -v curl &>/dev/null; then
  HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    --connect-timeout "$TIMEOUT" \
    --max-time "$TIMEOUT" \
    "${AI_BRAIN_URL}/health" 2>/dev/null || echo "000")

  LATENCY=$(curl -s -o /dev/null -w "%{time_total}" \
    --connect-timeout "$TIMEOUT" \
    --max-time "$TIMEOUT" \
    "${AI_BRAIN_URL}/health" 2>/dev/null || echo "?")

  if [[ "$HTTP_STATUS" =~ ^(200|204)$ ]]; then
    pass "AI Brain Health" "${AI_BRAIN_URL}/health → HTTP ${HTTP_STATUS} (${LATENCY}s)"
  elif [[ "$HTTP_STATUS" == "000" ]]; then
    fail "AI Brain Health" "${AI_BRAIN_URL} — unreachable (connection refused or timeout)"
  else
    fail "AI Brain Health" "${AI_BRAIN_URL}/health → HTTP ${HTTP_STATUS}"
  fi

  # Check /v1/models endpoint
  MODELS_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    --connect-timeout "$TIMEOUT" \
    --max-time "$TIMEOUT" \
    "${AI_BRAIN_URL}/v1/models" 2>/dev/null || echo "000")

  if [[ "$MODELS_STATUS" =~ ^(200|204)$ ]]; then
    pass "AI Brain Models" "${AI_BRAIN_URL}/v1/models → HTTP ${MODELS_STATUS}"
  else
    warn "AI Brain Models" "${AI_BRAIN_URL}/v1/models → HTTP ${MODELS_STATUS}"
  fi
else
  warn "curl" "Not found — AI Brain HTTP check skip korchi"
fi
echo ""

# ── 3. Docker Containers ──────────────────────────────────────────────────────
print_section "Docker Containers"

if command -v docker &>/dev/null; then
  CONTAINER_NAMES=(
    "csa-redis"
    "csa-ai-brain"
    "csa-scoring-engine"
    "csa-github-collector"
    "csa-whale-collector"
    "csa-sec-collector"
    "csa-social-collector"
  )

  for CNAME in "${CONTAINER_NAMES[@]}"; do
    # Check if container exists
    CSTATUS=$(docker inspect --format='{{.State.Status}}' "$CNAME" 2>/dev/null || echo "not_found")
    CHEALTH=$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}' "$CNAME" 2>/dev/null || echo "—")

    case "$CSTATUS" in
      "running")
        if [[ "$CHEALTH" == "unhealthy" ]]; then
          fail "${CNAME}" "running but UNHEALTHY (health=${CHEALTH})"
        else
          pass "${CNAME}" "running (health=${CHEALTH})"
        fi
        ;;
      "not_found")
        warn "${CNAME}" "container exists na (dev mode e running thakte pare)"
        ;;
      "exited")
        EXIT_CODE=$(docker inspect --format='{{.State.ExitCode}}' "$CNAME" 2>/dev/null || echo "?")
        fail "${CNAME}" "exited with code ${EXIT_CODE}"
        ;;
      *)
        fail "${CNAME}" "status=${CSTATUS}"
        ;;
    esac
  done
else
  warn "docker" "Not installed — container checks skip korchi"
fi
echo ""

# ── 4. WebSocket Port ─────────────────────────────────────────────────────────
print_section "WebSocket Server"

if command -v nc &>/dev/null; then
  if nc -z -w "$TIMEOUT" localhost "$WS_PORT" 2>/dev/null; then
    pass "WebSocket Port ${WS_PORT}" "localhost:${WS_PORT} open ✓"
  else
    fail "WebSocket Port ${WS_PORT}" "localhost:${WS_PORT} — port closed or service down"
  fi
elif command -v lsof &>/dev/null; then
  if lsof -i :"$WS_PORT" -t &>/dev/null; then
    pass "WebSocket Port ${WS_PORT}" "localhost:${WS_PORT} open (via lsof)"
  else
    fail "WebSocket Port ${WS_PORT}" "localhost:${WS_PORT} — nothing listening"
  fi
else
  warn "Port Check" "nc/lsof not available — WebSocket port check skip korchi"
fi
echo ""

# ── 5. Environment ────────────────────────────────────────────────────────────
print_section "Environment"

ENV_FILE="${PROJECT_ROOT}/.env"
if [[ -f "$ENV_FILE" ]]; then
  pass ".env file" "${ENV_FILE}"

  REQUIRED_VARS=(
    "REDIS_URL"
    "AI_BRAIN_URL"
    "GEMINI_API_KEY"
  )
  for VAR in "${REQUIRED_VARS[@]}"; do
    if [[ -n "${!VAR:-}" ]]; then
      # Mask sensitive values
      VALUE="${!VAR}"
      if [[ "${VAR}" == *"KEY"* || "${VAR}" == *"SECRET"* || "${VAR}" == *"TOKEN"* ]]; then
        DISPLAY="${VALUE:0:8}****"
      else
        DISPLAY="${VALUE}"
      fi
      pass "  ${VAR}" "${DISPLAY}"
    else
      fail "  ${VAR}" "NOT SET — .env e add koro"
    fi
  done
else
  warn ".env file" "Not found at ${ENV_FILE} — .env.example theke copy koro"
fi
echo ""

# ── 6. Node.js Processes ─────────────────────────────────────────────────────
print_section "Node.js Processes"

PROCESS_PATTERNS=(
  "aggregator"
  "ai-brain"
  "github-collector"
  "whale-collector"
  "sec-collector"
)

for PATTERN in "${PROCESS_PATTERNS[@]}"; do
  PID=$(pgrep -f "$PATTERN" 2>/dev/null | head -1 || true)
  if [[ -n "$PID" ]]; then
    MEM=$(ps -o rss= -p "$PID" 2>/dev/null | awk '{printf "%.1fMB", $1/1024}' || echo "?")
    pass "  ${PATTERN}" "PID=${PID} mem=${MEM}"
  else
    warn "  ${PATTERN}" "process not found (docker e running thakte pare)"
  fi
done
echo ""

# ── Summary ───────────────────────────────────────────────────────────────────
echo -e "${BOLD}${CYAN}── Summary ──────────────────────────────────────────────${NC}"

if [[ "$FAILED_CHECKS" -eq 0 ]]; then
  echo -e "  ${GREEN}${BOLD}Sob service running ✓${NC}"
  echo -e "  ${DIM}${TOTAL_CHECKS} checks passed, 0 failed${NC}"
  EXIT_CODE=0
else
  echo -e "  ${RED}${BOLD}${FAILED_CHECKS}/${TOTAL_CHECKS} checks FAILED${NC}"
  echo -e "  ${YELLOW}${FAILED_CHECKS}ta service problem ache — log check koro${NC}"
  EXIT_CODE=1
fi

echo ""
echo -e "${DIM}Run 'bash scripts/start-all.sh' to start all services${NC}"
echo ""

exit $EXIT_CODE

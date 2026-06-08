#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# start-all.sh — Start all Crypto Signal Aggregator services in dev mode
#
# Usage: bash scripts/start-all.sh
#
# Banglish: "Sob service shuru korchi...", "Development mode e running"
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

# ── Colors ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

# ── Config ────────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LOGS_DIR="${PROJECT_ROOT}/logs"
PID_FILE="${PROJECT_ROOT}/.pids"

# All child PIDs
declare -a CHILD_PIDS=()

# ── Helpers ───────────────────────────────────────────────────────────────────
log() { echo -e "${CYAN}[$(date '+%H:%M:%S')]${NC} $*"; }
ok()  { echo -e "  ${GREEN}✓${NC} $*"; }
err() { echo -e "  ${RED}✗${NC} $*"; }
warn(){ echo -e "  ${YELLOW}⚠${NC} $*"; }

print_banner() {
  clear
  echo ""
  echo -e "${BOLD}${GREEN}╔════════════════════════════════════════════════════════════╗${NC}"
  echo -e "${BOLD}${GREEN}║  ⚡ ALPHA INTELLIGENCE — Development Mode Launcher         ║${NC}"
  echo -e "${BOLD}${GREEN}║  Crypto Signal Aggregator v1.0                             ║${NC}"
  echo -e "${BOLD}${GREEN}╚════════════════════════════════════════════════════════════╝${NC}"
  echo ""
  echo -e "${DIM}Project: ${PROJECT_ROOT}${NC}"
  echo -e "${DIM}Logs:    ${LOGS_DIR}${NC}"
  echo -e "${DIM}Development mode e running${NC}"
  echo ""
}

start_service() {
  local name="$1"
  local dir="$2"
  local cmd="$3"
  local log_file="${LOGS_DIR}/${name}.log"

  echo -ne "  ${CYAN}Starting ${name}...${NC}"

  # Ensure directory exists
  if [[ ! -d "$dir" ]]; then
    echo -e " ${YELLOW}⚠ Directory not found: ${dir}${NC}"
    return 1
  fi

  # Start in background, log to file
  (cd "$dir" && eval "$cmd" >> "$log_file" 2>&1) &
  local pid=$!
  CHILD_PIDS+=("$pid")

  # Give it a moment to check if it crashed immediately
  sleep 0.5

  if kill -0 "$pid" 2>/dev/null; then
    echo -e " ${GREEN}✓${NC} PID=${pid}"
    echo "$name:$pid" >> "$PID_FILE"
    return 0
  else
    echo -e " ${RED}✗ FAILED${NC} (check ${log_file})"
    return 1
  fi
}

# ── Cleanup handler ───────────────────────────────────────────────────────────
cleanup() {
  echo ""
  log "${YELLOW}SIGINT received — sob service band korchi...${NC}"

  # Kill all child processes
  for pid in "${CHILD_PIDS[@]:-}"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill -TERM "$pid" 2>/dev/null || true
      log "Killed PID ${pid}"
    fi
  done

  # Remove PID file
  rm -f "$PID_FILE"

  echo ""
  echo -e "${BOLD}${RED}All services stopped. Bye!${NC}"
  exit 0
}

trap cleanup SIGINT SIGTERM

# ─────────────────────────────────────────────────────────────────────────────
print_banner

# ── 0. Pre-flight checks ──────────────────────────────────────────────────────
log "Pre-flight check korchi..."
PREFLIGHT_OK=true

# Check .env
ENV_FILE="${PROJECT_ROOT}/.env"
if [[ -f "$ENV_FILE" ]]; then
  ok ".env file found — loading"
  # shellcheck disable=SC2046
  export $(grep -v '^#' "$ENV_FILE" | grep -v '^$' | xargs) 2>/dev/null || true
else
  warn ".env file nei! Copying from .env.example..."
  if [[ -f "${PROJECT_ROOT}/.env.example" ]]; then
    cp "${PROJECT_ROOT}/.env.example" "$ENV_FILE"
    warn "Copied .env.example → .env (please fill in API keys!)"
  else
    err ".env.example o nei — API keys manually set koro"
    PREFLIGHT_OK=false
  fi
fi

# Check Node.js
if command -v node &>/dev/null; then
  NODE_VER=$(node --version)
  ok "Node.js ${NODE_VER}"
  # Check major version >= 20
  NODE_MAJOR=$(echo "$NODE_VER" | tr -d 'v' | cut -d. -f1)
  if [[ "$NODE_MAJOR" -lt 20 ]]; then
    warn "Node.js ${NODE_VER} — v20+ recommended"
  fi
else
  err "Node.js not found — install from nodejs.org"
  PREFLIGHT_OK=false
fi

# Check npm/npx
if command -v npm &>/dev/null; then
  ok "npm $(npm --version)"
else
  err "npm not found"
  PREFLIGHT_OK=false
fi

# Check Python
if command -v python3 &>/dev/null; then
  PYTHON_VER=$(python3 --version 2>&1)
  ok "${PYTHON_VER}"
else
  warn "python3 not found — Python collectors will not start"
fi

if [[ "$PREFLIGHT_OK" != "true" ]]; then
  echo ""
  err "Pre-flight failed — requirements missing"
  exit 1
fi

echo ""

# ── 1. Setup logs directory ───────────────────────────────────────────────────
log "Logs directory setup korchi..."
mkdir -p "$LOGS_DIR"
# Clear old PID file
rm -f "$PID_FILE"
touch "$PID_FILE"
ok "Logs dir: ${LOGS_DIR}"
echo ""

# ── 2. Redis ──────────────────────────────────────────────────────────────────
log "Redis check korchi..."
REDIS_RUNNING=false

if command -v redis-cli &>/dev/null; then
  REDIS_PING=$(redis-cli ping 2>/dev/null || echo "FAILED")
  if [[ "$REDIS_PING" == "PONG" ]]; then
    ok "Redis already running"
    REDIS_RUNNING=true
  fi
fi

if [[ "$REDIS_RUNNING" != "true" ]]; then
  if command -v redis-server &>/dev/null; then
    log "Redis shuru korchi..."
    redis-server --daemonize yes \
      --logfile "${LOGS_DIR}/redis.log" \
      --loglevel notice \
      --save "" \
      --appendonly no \
      2>/dev/null || true

    sleep 1
    REDIS_PING=$(redis-cli ping 2>/dev/null || echo "FAILED")
    if [[ "$REDIS_PING" == "PONG" ]]; then
      ok "Redis started (daemon mode)"
    else
      err "Redis start failed — check ${LOGS_DIR}/redis.log"
      exit 1
    fi
  else
    warn "redis-server not found — assuming Redis is running elsewhere (Docker/remote)"
  fi
fi
echo ""

# ── 3. Install dependencies (if node_modules missing) ─────────────────────────
log "Dependencies check korchi..."

ROOT_PACKAGE="${PROJECT_ROOT}/package.json"
if [[ -f "$ROOT_PACKAGE" ]]; then
  if [[ ! -d "${PROJECT_ROOT}/node_modules" ]]; then
    log "node_modules nei — npm install korchi..."
    (cd "$PROJECT_ROOT" && npm install --silent) &
    wait $!
    ok "Dependencies installed"
  else
    ok "node_modules found"
  fi
fi
echo ""

# ── 4. Start Services ─────────────────────────────────────────────────────────
log "Sob service shuru korchi..."
echo ""

# ─── AI Brain Gateway (packages/ai-brain) ─────────────────────────────────
AI_BRAIN_DIR="${PROJECT_ROOT}/packages/ai-brain"
if [[ -d "$AI_BRAIN_DIR" ]]; then
  log "AI Brain Gateway..."
  start_service "ai-brain" "$AI_BRAIN_DIR" "npm run dev" || true
else
  warn "ai-brain package not found — skip"
fi

# Small stagger to avoid port conflicts
sleep 1

# ─── Scoring Engine (packages/scoring-engine) ─────────────────────────────
SCORING_DIR="${PROJECT_ROOT}/packages/scoring-engine"
if [[ -d "$SCORING_DIR" ]]; then
  log "Scoring Engine..."
  start_service "scoring-engine" "$SCORING_DIR" "npm run dev" || true
else
  warn "scoring-engine not found — skip"
fi

sleep 0.5

# ─── GitHub Collector (packages/github-collector) ─────────────────────────
GITHUB_DIR="${PROJECT_ROOT}/packages/github-collector"
if [[ -d "$GITHUB_DIR" ]]; then
  log "GitHub Collector..."
  start_service "github-collector" "$GITHUB_DIR" "npm run dev" || true
elif [[ -f "${PROJECT_ROOT}/collectors/github_collector.py" ]]; then
  log "GitHub Collector (Python)..."
  start_service "github-collector" "${PROJECT_ROOT}/collectors" "python3 github_collector.py" || true
else
  warn "github-collector not found — skip"
fi

sleep 0.5

# ─── Whale Tracker (packages/whale-tracker) ───────────────────────────────
WHALE_DIR="${PROJECT_ROOT}/packages/whale-tracker"
if [[ -d "$WHALE_DIR" ]]; then
  log "Whale Tracker..."
  start_service "whale-tracker" "$WHALE_DIR" "npm run dev" || true
elif [[ -f "${PROJECT_ROOT}/collectors/whale_tracker.py" ]]; then
  log "Whale Tracker (Python)..."
  start_service "whale-tracker" "${PROJECT_ROOT}/collectors" "python3 whale_tracker.py" || true
else
  warn "whale-tracker not found — skip"
fi

sleep 0.5

# ─── SEC Collector (packages/sec-collector or Python) ─────────────────────
SEC_DIR="${PROJECT_ROOT}/packages/sec-collector"
if [[ -d "$SEC_DIR" ]]; then
  log "SEC Collector..."
  start_service "sec-collector" "$SEC_DIR" "npm run dev" || true
elif [[ -f "${PROJECT_ROOT}/collectors/sec_collector.py" ]]; then
  log "SEC Collector (Python)..."
  start_service "sec-collector" "${PROJECT_ROOT}/collectors" "python3 sec_collector.py" || true
else
  warn "sec-collector not found — skip"
fi

sleep 0.5

# ─── Social Collector (X/Reddit) ──────────────────────────────────────────
SOCIAL_DIR="${PROJECT_ROOT}/packages/social-collector"
if [[ -d "$SOCIAL_DIR" ]]; then
  log "Social Collector..."
  start_service "social-collector" "$SOCIAL_DIR" "npm run dev" || true
elif [[ -f "${PROJECT_ROOT}/collectors/social_collector.py" ]]; then
  log "Social Collector (Python)..."
  start_service "social-collector" "${PROJECT_ROOT}/collectors" "python3 social_collector.py" || true
else
  warn "social-collector not found — skip"
fi

echo ""

# ── 5. Summary ────────────────────────────────────────────────────────────────
log "Startup complete!"
echo ""
echo -e "${BOLD}${GREEN}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}${GREEN}║  ✅ Services Started — Development Mode e Running        ║${NC}"
echo -e "${BOLD}${GREEN}╚══════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "  ${CYAN}🌐 Terminal UI:${NC}      open ${PROJECT_ROOT}/apps/terminal-ui/index.html"
echo -e "  ${CYAN}📡 WebSocket:${NC}        ws://localhost:${WS_PORT:-3001}"
echo -e "  ${CYAN}🧠 AI Brain:${NC}         ${AI_BRAIN_URL:-http://localhost:3000}"
echo -e "  ${CYAN}📊 Logs:${NC}             ${LOGS_DIR}/"
echo ""
echo -e "  ${DIM}Health check: bash scripts/health-check.sh${NC}"
echo -e "  ${DIM}Stop all:     Press Ctrl+C${NC}"
echo ""

# ── 6. Tail logs ─────────────────────────────────────────────────────────────
log "Service logs tail korchi (Ctrl+C to stop all)..."
echo ""

# Tail all log files that exist
LOG_FILES=()
for f in "${LOGS_DIR}"/*.log; do
  [[ -f "$f" ]] && LOG_FILES+=("$f")
done

if [[ ${#LOG_FILES[@]} -gt 0 ]]; then
  tail -f "${LOG_FILES[@]}" &
  TAIL_PID=$!
  CHILD_PIDS+=("$TAIL_PID")
fi

# Wait indefinitely (cleanup runs on Ctrl+C via trap)
wait

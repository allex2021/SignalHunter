#!/usr/bin/env bash
# ==============================================================================
# launch.sh — Alpha Intelligence Full System Launcher
# Starts Redis, AI Brain, Scoring Engine, all Ingestion agents, Terminal UI
# ==============================================================================

set -e

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$ROOT/logs"
mkdir -p "$LOG_DIR"

# ── Colors ────────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; DIM='\033[2m'; NC='\033[0m'

banner() {
  echo ""
  echo -e "${CYAN}${BOLD}"
  echo "  ╔══════════════════════════════════════════════════╗"
  echo "  ║       ⚡  ALPHA INTELLIGENCE LAUNCHER  ⚡        ║"
  echo "  ║        Crypto Signal Aggregator v1.0             ║"
  echo "  ╚══════════════════════════════════════════════════╝"
  echo -e "${NC}"
}

log_ok()   { echo -e "  ${GREEN}✅ $1${NC}"; }
log_info() { echo -e "  ${CYAN}ℹ  $1${NC}"; }
log_warn() { echo -e "  ${YELLOW}⚠  $1${NC}"; }
log_err()  { echo -e "  ${RED}✗  $1${NC}"; }
log_step() { echo -e "\n${BOLD}${CYAN}▶ $1${NC}"; }

banner

# ── Load .env ─────────────────────────────────────────────────────────────────
if [ -f "$ROOT/.env" ]; then
  set -a; source "$ROOT/.env"; set +a
  log_ok ".env loaded"
else
  log_err ".env not found — run: cp .env.example .env && nano .env"
  exit 1
fi

# ── Validate required API keys ────────────────────────────────────────────────
log_step "Pre-flight: API Key Check"

AI_OK=false
if [ -n "$GEMINI_API_KEY" ] && [ "$GEMINI_API_KEY" != "PASTE_GEMINI_KEY_HERE" ]; then
  log_ok "Gemini API key: configured"
  AI_OK=true
else
  log_warn "Gemini API key: MISSING (AI scoring will use failover)"
fi
if [ -n "$GROQ_API_KEY" ] && [ "$GROQ_API_KEY" != "PASTE_GROQ_KEY_HERE" ]; then
  log_ok "Groq API key: configured"
  AI_OK=true
else
  log_warn "Groq API key: MISSING"
fi
if [ -n "$MISTRAL_API_KEY" ] && [ "$MISTRAL_API_KEY" != "PASTE_MISTRAL_KEY_HERE" ]; then
  log_ok "Mistral API key: configured"
  AI_OK=true
else
  log_warn "Mistral API key: MISSING"
fi

if [ "$AI_OK" = false ]; then
  log_err "No AI provider keys configured! Add at least one to .env"
  log_err "Get Gemini free key: https://aistudio.google.com/app/apikey"
  exit 1
fi

if [ -n "$GITHUB_TOKEN" ]; then
  log_ok "GitHub token: configured (5000 req/hr)"
else
  log_warn "GitHub token: not set — using public rate limit (60 req/hr)"
fi

# ── Install Node deps ─────────────────────────────────────────────────────────
log_step "Installing Node.js dependencies"
cd "$ROOT"
npm install --silent 2>&1 | tail -2
log_ok "Node modules ready"

# ── Build shared-types ────────────────────────────────────────────────────────
log_step "Building shared-types package"
npm run build --workspace=packages/shared-types 2>&1 | tail -3
log_ok "shared-types built"

# ── Install Python deps ───────────────────────────────────────────────────────
log_step "Installing Python dependencies"
VENV="$ROOT/.venv"
if [ ! -d "$VENV" ]; then
  python3 -m venv "$VENV"
  log_ok "Python venv created"
fi
source "$VENV/bin/activate"

pip3 install -q --upgrade pip
pip3 install -q \
  web3==6.15.1 \
  redis==5.0.4 \
  python-dotenv==1.0.1 \
  aiohttp==3.9.5 \
  feedparser==6.0.11 \
  asyncpraw==7.7.1 \
  pyttsx3==2.91 \
  lxml==5.2.1 2>&1 | tail -3

log_ok "Python packages installed (venv: $VENV)"

# ── Start Redis ───────────────────────────────────────────────────────────────
log_step "Starting Redis (embedded via npm)"

# Kill any previous launcher
pkill -f "redis-launcher.js" 2>/dev/null || true
sleep 0.5

node "$ROOT/scripts/redis-launcher.js" > "$LOG_DIR/redis.log" 2>&1 &
REDIS_PID=$!
echo $REDIS_PID > "$LOG_DIR/redis.pid"
log_info "Redis starting (PID $REDIS_PID)..."

# Wait for Redis to be ready (up to 15s)
for i in $(seq 1 15); do
  if node -e "const r=require('ioredis'); const c=new r(); c.ping().then(()=>{c.disconnect();process.exit(0)}).catch(()=>process.exit(1))" 2>/dev/null; then
    log_ok "Redis is ready ✓"
    break
  fi
  if [ $i -eq 15 ]; then
    log_err "Redis failed to start after 15s — check logs/redis.log"
    cat "$LOG_DIR/redis.log" | tail -10
    exit 1
  fi
  sleep 1
done

# ── Start AI Brain Gateway ────────────────────────────────────────────────────
log_step "Starting AI Brain Gateway (port ${AI_BRAIN_PORT:-3000})"

pkill -f "apps/ai-brain" 2>/dev/null || true
sleep 0.3

cd "$ROOT"
npm run dev --workspace=apps/ai-brain > "$LOG_DIR/ai-brain.log" 2>&1 &
BRAIN_PID=$!
echo $BRAIN_PID > "$LOG_DIR/ai-brain.pid"
log_info "AI Brain starting (PID $BRAIN_PID)..."

# Wait for AI Brain health endpoint
for i in $(seq 1 20); do
  if curl -sf "http://localhost:${AI_BRAIN_PORT:-3000}/health" > /dev/null 2>&1; then
    log_ok "AI Brain ready → http://localhost:${AI_BRAIN_PORT:-3000}/health"
    break
  fi
  if [ $i -eq 20 ]; then
    log_warn "AI Brain not responding yet — check logs/ai-brain.log"
  fi
  sleep 1
done

# ── Start Scoring Engine ──────────────────────────────────────────────────────
log_step "Starting Scoring Engine (WebSocket port ${WS_PORT:-3001})"

pkill -f "packages/scoring-engine" 2>/dev/null || true
sleep 0.3

npm run dev --workspace=packages/scoring-engine > "$LOG_DIR/scoring-engine.log" 2>&1 &
SCORE_PID=$!
echo $SCORE_PID > "$LOG_DIR/scoring-engine.pid"
log_ok "Scoring Engine started (PID $SCORE_PID)"

# ── Start GitHub Recon ────────────────────────────────────────────────────────
log_step "Starting GitHub Recon Agent (100 Web3 repos)"

pkill -f "github-recon" 2>/dev/null || true
sleep 0.3

cd "$ROOT/apps/ingestion/github-recon"
npx tsx index.ts > "$LOG_DIR/github-recon.log" 2>&1 &
GITHUB_PID=$!
echo $GITHUB_PID > "$LOG_DIR/github-recon.pid"
log_ok "GitHub Recon started (PID $GITHUB_PID)"

# ── Start Whale Tracker ───────────────────────────────────────────────────────
log_step "Starting Whale Tracker (ETH mainnet public RPC)"

pkill -f "whale-tracker" 2>/dev/null || true
sleep 0.3

cd "$ROOT"
source "$VENV/bin/activate"
REDIS_URL="${REDIS_URL:-redis://localhost:6379}" \
ETH_RPC_URL="${ETH_RPC_URL:-https://eth.llamarpc.com}" \
python3 apps/ingestion/whale-tracker/tracker.py > "$LOG_DIR/whale-tracker.log" 2>&1 &
WHALE_PID=$!
echo $WHALE_PID > "$LOG_DIR/whale-tracker.pid"
log_ok "Whale Tracker started (PID $WHALE_PID)"

# ── Start SEC Poller ──────────────────────────────────────────────────────────
log_step "Starting SEC EDGAR Poller (public RSS feed)"

pkill -f "sec-poller" 2>/dev/null || true
sleep 0.3

python3 apps/ingestion/sec-poller/poller.py > "$LOG_DIR/sec-poller.log" 2>&1 &
SEC_PID=$!
echo $SEC_PID > "$LOG_DIR/sec-poller.pid"
log_ok "SEC Poller started (PID $SEC_PID)"

# ── Start Reddit Miner ────────────────────────────────────────────────────────
if [ -n "$REDDIT_CLIENT_ID" ] && [ "$REDDIT_CLIENT_ID" != "PASTE_REDDIT_CLIENT_ID_HERE" ]; then
  log_step "Starting Reddit Miner (asyncpraw)"
  pkill -f "reddit-miner" 2>/dev/null || true
  sleep 0.3
  python3 apps/social-mining/reddit-miner/miner.py > "$LOG_DIR/reddit-miner.log" 2>&1 &
  REDDIT_PID=$!
  echo $REDDIT_PID > "$LOG_DIR/reddit-miner.pid"
  log_ok "Reddit Miner started (PID $REDDIT_PID)"
else
  log_warn "Reddit skipped — no API credentials in .env"
fi

# ── Start Audio Worker ────────────────────────────────────────────────────────
log_step "Starting Audio Alert Worker"
pkill -f "audio-worker" 2>/dev/null || true
python3 apps/social-mining/audio-worker/worker.py > "$LOG_DIR/audio-worker.log" 2>&1 &
AUDIO_PID=$!
echo $AUDIO_PID > "$LOG_DIR/audio-worker.pid"
log_ok "Audio Worker started (PID $AUDIO_PID)"

# ── Inject seed events for immediate UI testing ───────────────────────────────
log_step "Injecting seed events (for immediate terminal UI display)"
sleep 2

node -e "
const Redis = require('ioredis');
const r = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');
async function seed() {
  await r.xadd('stream:whale','*','from','0xF977814e90dA44bFA03b6295A0616a897441aceA','to','Binance Hot Wallet','token','USDT','amount_usd','47000000','block_number','19800000','timestamp',Date.now()+'','destination_type','cex_hot_wallet');
  await r.xadd('stream:github','*','repo','ethereum/go-ethereum','eventType','PushEvent','commitMessage','EMERGENCY: patch critical consensus bug in EVM execution','actor','karalabe','timestamp',new Date().toISOString(),'url','https://github.com/ethereum/go-ethereum','semanticScore','9','bias','Bearish');
  await r.xadd('stream:sec','*','accessionNumber','0001193125-24-123456','formType','Form 4','companyName','Coinbase Global Inc','ticker','COIN','filedAt',new Date().toISOString(),'insiderName','Brian Armstrong','transactionType','S','sharesDelta','-150000','url','https://www.sec.gov/cgi-bin/browse-edgar');
  await r.xadd('stream:social','*','platform','twitter','author','VitalikButerin','content','Huge milestone: EIP-4844 cuts L2 fees by 100x. This changes everything for mass adoption.','url','https://twitter.com/VitalikButerin','timestamp',new Date().toISOString(),'engagementScore','9500','keywords','EIP-4844,L2,fees');
  await r.xadd('stream:reddit','*','subreddit','CryptoCurrency','keyword','bitcoin','currentCount','18450','previousCount','3200','spikePercent','476','timestamp',new Date().toISOString(),'topPostUrls','https://reddit.com/r/CryptoCurrency');
  console.log('[Seed] ✅ 5 seed events injected across all streams');
  r.disconnect();
}
seed().catch(e => { console.error('[Seed] Error:', e.message); r.disconnect(); });
" 2>&1

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}${GREEN}════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}${GREEN}  ⚡  ALL SYSTEMS ONLINE — Alpha Intelligence        ${NC}"
echo -e "${BOLD}${GREEN}════════════════════════════════════════════════════${NC}"
echo ""
echo -e "  ${CYAN}🖥️  Terminal UI${NC}     →  ${BOLD}open apps/terminal-ui/index.html${NC}"
echo -e "  ${CYAN}🧠 AI Brain API${NC}    →  ${BOLD}http://localhost:${AI_BRAIN_PORT:-3000}/health${NC}"
echo -e "  ${CYAN}📡 WebSocket${NC}       →  ${BOLD}ws://localhost:${WS_PORT:-3001}${NC}"
echo -e "  ${CYAN}🔍 Health Check${NC}    →  ${BOLD}bash scripts/health-check.sh${NC}"
echo ""
echo -e "  ${DIM}Logs: tail -f logs/ai-brain.log${NC}"
echo -e "  ${DIM}      tail -f logs/scoring-engine.log${NC}"
echo -e "  ${DIM}      tail -f logs/whale-tracker.log${NC}"
echo ""
echo -e "  ${YELLOW}Next scoring tick in 5 minutes. Seed data visible now.${NC}"
echo ""

# ── Open Terminal UI ──────────────────────────────────────────────────────────
sleep 1
open "$ROOT/apps/terminal-ui/index.html" 2>/dev/null || true

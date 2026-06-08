# ⚡ Alpha Intelligence — Crypto Signal Aggregator

[![Node.js](https://img.shields.io/badge/Node.js-20+-339933?style=flat-square&logo=nodedotjs)](https://nodejs.org/)
[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat-square&logo=python)](https://python.org/)
[![Redis](https://img.shields.io/badge/Redis-7.x-DC382D?style=flat-square&logo=redis)](https://redis.io/)
[![Docker](https://img.shields.io/badge/Docker-24+-2496ED?style=flat-square&logo=docker)](https://docker.com/)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.4-3178C6?style=flat-square&logo=typescript)](https://typescriptlang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=flat-square)](./LICENSE)

> **Real-time, AI-powered market intelligence aggregator** for crypto and equity signals. Ingests GitHub commits, SEC filings, on-chain whale flows, and social sentiment — scores them with LLMs — and displays actionable alpha in a stunning terminal UI.

---

## 🏗️ Architecture

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                    ⚡ ALPHA INTELLIGENCE SYSTEM                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐   ║
║  │  🐙 GitHub  │  │  🐋 Whale   │  │  📋 SEC     │  │  📱 Social      │   ║
║  │  Collector  │  │  Tracker    │  │  Collector  │  │  X + Reddit     │   ║
║  │  (Node.js)  │  │  (Node.js)  │  │  (Python)   │  │  (Node.js)      │   ║
║  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └────────┬────────┘   ║
║         │                │                │                   │             ║
║         ▼                ▼                ▼                   ▼             ║
║  ┌───────────────────────────────────────────────────────────────────────┐  ║
║  │                    📨 Redis Streams (5 streams)                       │  ║
║  │  stream:github  stream:whale  stream:sec  stream:social  stream:reddit│  ║
║  └───────────────────────────────────┬───────────────────────────────────┘  ║
║                                      │                                       ║
║                                      ▼                                       ║
║  ┌────────────────────────────────────────────────────────────────────────┐  ║
║  │              🧮 Scoring Engine (packages/scoring-engine)               │  ║
║  │  • 2-hour rolling window (Redis sorted sets)                           │  ║
║  │  • Every 5min: sends to AI Brain → receives SignalEvent[]              │  ║
║  │  • Publishes to stream:signals                                          │  ║
║  │  • WebSocket server (port 3001) → broadcasts to terminal UI            │  ║
║  └─────────────────┬──────────────────────────────┬───────────────────────┘  ║
║                    │                              │                           ║
║                    ▼                              ▼                           ║
║  ┌─────────────────────────┐    ┌─────────────────────────────────────────┐  ║
║  │  🧠 AI Brain Gateway    │    │  🖥️  Terminal UI                        │  ║
║  │  (packages/ai-brain)    │    │  (apps/terminal-ui)                     │  ║
║  │  • Gemini/Groq/Mistral  │    │  • Dark-mode trading terminal           │  ║
║  │  • /v1/chat/completions │    │  • Matrix rain + glassmorphism          │  ║
║  │  • LRU response cache   │    │  • Real-time WebSocket feed             │  ║
║  └─────────────────────────┘    └─────────────────────────────────────────┘  ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

---

## 📦 Modules

| Module | Path | Language | Description |
|--------|------|----------|-------------|
| 🧠 **AI Brain** | `packages/ai-brain` | Node.js/TS | LLM gateway with provider failover (Gemini→Groq→Mistral) |
| 🐙 **GitHub Collector** | `packages/github-collector` | Node.js/TS | Monitors repos for commits, PRs, emergency patches |
| 🐋 **Whale Tracker** | `packages/whale-tracker` | Node.js/TS | On-chain large transfer detection via Etherscan/Moralis |
| 📋 **SEC Collector** | `packages/sec-collector` | Python | EDGAR XBRL + EDGAR RSS parser for Form 4, 8-K, 13F |
| 📱 **Social Scraper** | `packages/social-collector` | Node.js/TS | X/Twitter and Reddit keyword spike detection |
| 🧮 **Scoring Engine** | `packages/scoring-engine` | Node.js/TS | 2-hr window aggregator → AI scoring → WebSocket broadcast |
| 🖥️ **Terminal UI** | `apps/terminal-ui` | Vanilla JS/HTML | Hedge fund-grade trading terminal |
| 📐 **Shared Types** | `packages/shared-types` | TypeScript | Shared interfaces and type definitions |

---

## ✅ Prerequisites

| Tool | Minimum Version | Install |
|------|-----------------|---------|
| Node.js | 20.x LTS | `nvm install 20` |
| npm | 10.x | Bundled with Node.js |
| Python | 3.11+ | `brew install python@3.11` |
| Redis | 7.x | `brew install redis` |
| Docker | 24.x | [docker.com](https://docker.com) |
| Git | 2.x | Bundled with Xcode tools |

---

## 🚀 Quick Start

### Step 1: Clone & Install
```bash
git clone <repo-url> crypto-signal-aggregator
cd crypto-signal-aggregator
npm install
```

### Step 2: Configure Environment
```bash
cp .env.example .env
# Edit .env and fill in your API keys
nano .env
```

### Step 3: Start All Services
```bash
bash scripts/start-all.sh
```

### Step 4: Open Terminal UI
```bash
open apps/terminal-ui/index.html
# Or serve with any static file server:
npx serve apps/terminal-ui -p 8080
```

### Step 5: Verify Health
```bash
bash scripts/health-check.sh
```

---

## 🐳 Docker Compose (Production)

```bash
# Start entire stack
docker compose up -d

# View logs
docker compose logs -f scoring-engine

# Scale collectors
docker compose up -d --scale github-collector=2

# Stop all
docker compose down
```

---

## ⚙️ Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `REDIS_URL` | ✅ | `redis://localhost:6379` | Redis connection string |
| `REDIS_HOST` | — | `localhost` | Redis host (for health check) |
| `REDIS_PORT` | — | `6379` | Redis port (for health check) |
| `AI_BRAIN_URL` | ✅ | `http://localhost:3000` | AI Brain gateway base URL |
| `AI_BRAIN_API_KEY` | — | — | Optional auth header for AI Brain |
| `GEMINI_API_KEY` | ✅ | — | Google Gemini API key |
| `GROQ_API_KEY` | — | — | Groq API key (failover) |
| `MISTRAL_API_KEY` | — | — | Mistral API key (failover) |
| `GITHUB_TOKEN` | ✅ | — | GitHub PAT for API access |
| `ETHERSCAN_API_KEY` | ✅ | — | Etherscan API key for whale tracking |
| `SEC_API_KEY` | — | — | SEC EDGAR API key (optional) |
| `TWITTER_BEARER_TOKEN` | — | — | X/Twitter Bearer Token |
| `REDDIT_CLIENT_ID` | — | — | Reddit OAuth client ID |
| `REDDIT_CLIENT_SECRET` | — | — | Reddit OAuth secret |
| `WS_PORT` | — | `3001` | WebSocket server port |
| `SCORING_INTERVAL_MS` | — | `300000` | Scoring tick interval (5 min) |
| `WINDOW_DURATION_MS` | — | `7200000` | Event window duration (2 hours) |
| `SCORING_MODEL` | — | `gpt-4o-mini` | AI model for scoring |
| `NODE_ENV` | — | `development` | Environment flag |
| `LOG_LEVEL` | — | `info` | Log verbosity |

---

## 📡 AI Brain API Reference

The AI Brain gateway exposes an OpenAI-compatible API:

### `GET /health`
Returns service health status.

**Response:**
```json
{
  "status": "healthy",
  "providers": [
    { "name": "gemini", "status": "active", "latency_ms": 145 },
    { "name": "groq", "status": "standby" },
    { "name": "mistral", "status": "standby" }
  ],
  "uptime_seconds": 3600
}
```

### `GET /v1/models`
Lists available models.

### `POST /v1/chat/completions`
OpenAI-compatible chat completion endpoint.

**Request:**
```json
{
  "model": "gemini-1.5-flash",
  "messages": [
    { "role": "system", "content": "You are a quant analyst..." },
    { "role": "user", "content": "Analyze these events..." }
  ],
  "temperature": 0.1,
  "max_tokens": 4096,
  "response_format": { "type": "json_object" }
}
```

**Response:**
```json
{
  "id": "chatcmpl-abc123",
  "object": "chat.completion",
  "model": "gemini-1.5-flash",
  "choices": [{
    "index": 0,
    "message": { "role": "assistant", "content": "[{...}]" },
    "finish_reason": "stop"
  }],
  "usage": { "prompt_tokens": 512, "completion_tokens": 256, "total_tokens": 768 }
}
```

---

## 📨 WebSocket Message Formats

Connect to `ws://localhost:3001` from any WebSocket client.

### Signal Message
Emitted when the scoring engine produces new signals (every ~5 minutes).

```json
{
  "type": "signal",
  "data": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "timestamp": "2024-06-01T12:34:56.789Z",
    "Data_Source": "Whale_Flow",
    "Raw_Intel_Summary": "Wallet moved 48M USD ETH to Binance hot wallet — liquidation risk.",
    "Impact_Rating": 9,
    "Directional_Bias": "Bearish"
  }
}
```

#### Field Reference

| Field | Type | Values |
|-------|------|--------|
| `id` | `string` | UUID v4 |
| `timestamp` | `string` | ISO 8601 UTC |
| `Data_Source` | `string` | `GitHub_Commit` \| `SEC_Filing` \| `Whale_Flow` \| `X_Scrape` \| `Reddit_Spike` |
| `Raw_Intel_Summary` | `string` | Max 25 words |
| `Impact_Rating` | `integer` | 1–10 (10 = immediate market mover) |
| `Directional_Bias` | `string` | `Bullish` \| `Bearish` \| `Volatility-Driven` \| `Neutral` |

#### Impact Rating Scale

| Score | Meaning | Examples |
|-------|---------|---------|
| 9–10 | 🔴 Immediate market mover | Whale dump to CEX, critical exploit, massive insider sale |
| 7–8 | 🟠 High significance | Emergency GitHub hotfix, large Form 4 purchase |
| 5–6 | 🟡 Medium signal | Keyword spike >500%, routine insider buy, protocol upgrade |
| 3–4 | 🔵 Low signal | Small commit, minor keyword uptick, routine filing |
| 1–2 | ⚪ Noise | Unrelated commit, tiny transfer, background chatter |

### Health Message
Emitted every 10 seconds to all connected clients.

```json
{
  "type": "health",
  "data": {
    "providers": [
      {
        "name": "AI-Brain-Gateway",
        "status": "healthy",
        "latency_ms": 42,
        "last_check": "2024-06-01T12:34:56.789Z"
      },
      {
        "name": "Redis",
        "status": "healthy",
        "latency_ms": 2,
        "last_check": "2024-06-01T12:34:56.789Z"
      }
    ],
    "streams": {
      "stream:github":  { "events_in_window": 14, "last_event_ts": "2024-06-01T12:34:00Z" },
      "stream:whale":   { "events_in_window": 7,  "last_event_ts": "2024-06-01T12:33:00Z" },
      "stream:sec":     { "events_in_window": 3,  "last_event_ts": "2024-06-01T12:30:00Z" },
      "stream:social":  { "events_in_window": 22, "last_event_ts": "2024-06-01T12:34:55Z" },
      "stream:reddit":  { "events_in_window": 18, "last_event_ts": "2024-06-01T12:34:50Z" }
    },
    "uptime_seconds": 3847
  }
}
```

---

## 🚀 Deployment Guide

### Docker Compose (Recommended)

```yaml
# docker-compose.yml
services:
  redis:
    image: redis:7-alpine
    container_name: csa-redis
    ports: ["6379:6379"]
    volumes: ["redis_data:/data"]
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 3s
      retries: 5

  ai-brain:
    build: ./packages/ai-brain
    container_name: csa-ai-brain
    ports: ["3000:3000"]
    env_file: .env
    depends_on:
      redis: { condition: service_healthy }

  scoring-engine:
    build: ./packages/scoring-engine
    container_name: csa-scoring-engine
    ports: ["3001:3001"]
    env_file: .env
    depends_on:
      redis: { condition: service_healthy }
      ai-brain: { condition: service_healthy }

volumes:
  redis_data:
```

### Manual Production

```bash
# Build all packages
npm run build --workspaces

# Start with PM2
npm install -g pm2

pm2 start packages/ai-brain/dist/index.js      --name "ai-brain"
pm2 start packages/scoring-engine/dist/aggregator.js --name "scoring-engine"
pm2 start packages/github-collector/dist/index.js    --name "github-collector"
pm2 start packages/whale-tracker/dist/index.js       --name "whale-tracker"

pm2 save
pm2 startup
```

### Kubernetes

```bash
# Apply manifests
kubectl apply -f k8s/

# Check pods
kubectl get pods -n csa

# Stream logs
kubectl logs -f deployment/scoring-engine -n csa
```

---

## 🔧 Banglish Debugging Guide

> Banglish log messages are designed for **instant debug identification** in mixed-language teams. All critical events log in Romanized Bengali + English.

| Log Message | Meaning | Action |
|-------------|---------|--------|
| `Signal Aggregator shuru holo!` | Aggregator started | Normal startup |
| `5 stream theke data collect korchi` | Reading all 5 streams | Normal |
| `Scoring engine: Xta event AI e pathacchi` | Sending X events to AI | Check count is reasonable |
| `AI Brain theke Xta signal aslo` | Got X signals back | Verify signal quality |
| `Invalid signal format, skip korchi` | Bad AI output | Check model response |
| `Provider cholche na, failover kori` | Provider down, using backup | Check provider API keys |
| `Whale detect hoise` | Large on-chain transfer | Immediate attention |
| `SEC data ashche` | SEC filing ingested | Check form type |
| `Aggregator gracefully shutdown hoise` | Clean shutdown | Normal |
| `Redis e connect korchi` | Redis connection attempt | Check Redis status |
| `Scoring tick: Xta event window e ache` | Window event count | Should be > 0 |
| `Xta UI client ke signal pathalam` | Broadcast count | Check WS connections |
| `AI Brain call fail hoise` | AI Brain unreachable | Check health-check.sh |
| `Window khali, scoring skip korchi` | No events in 2h window | Check collectors |

### Common Issues

**No signals appearing in UI:**
```bash
# Check Redis streams
redis-cli XLEN stream:github
redis-cli XLEN stream:whale

# Check aggregator logs
tail -f logs/scoring-engine.log | grep -E "(Scoring tick|event window)"

# Manual health check
bash scripts/health-check.sh
```

**AI Brain returning empty signals:**
```bash
# Test AI Brain directly
curl -X POST http://localhost:3000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"gemini-1.5-flash","messages":[{"role":"user","content":"Return [{}]"}]}'
```

**WebSocket disconnects:**
```bash
# Check port
lsof -i :3001

# Check aggregator
ps aux | grep aggregator
```

---

## 📁 Project Structure

```
crypto-signal-aggregator/
├── apps/
│   └── terminal-ui/          # Hedge fund-grade terminal UI
│       ├── index.html         # Full dark-mode UI
│       └── main.js            # Vanilla JS WebSocket client
├── packages/
│   ├── shared-types/          # TypeScript interfaces
│   ├── ai-brain/              # LLM gateway (Gemini/Groq/Mistral)
│   ├── github-collector/      # GitHub commit/PR monitor
│   ├── whale-tracker/         # On-chain whale detection
│   ├── sec-collector/         # SEC EDGAR filing parser
│   ├── social-collector/      # X + Reddit scraper
│   └── scoring-engine/        # ← This module (Module 4)
│       ├── src/
│       │   ├── scorer.ts      # ScoringEngine class
│       │   └── aggregator.ts  # Main service
│       ├── Dockerfile
│       ├── package.json
│       └── tsconfig.json
├── scripts/
│   ├── health-check.sh        # System health verifier
│   └── start-all.sh           # Dev mode launcher
├── docker-compose.yml
├── .env.example
├── package.json               # Workspace root
└── README.md
```

---

## 🔌 Redis Stream Schema

All collectors write to Redis Streams using `XADD`:

```bash
# GitHub events
XADD stream:github * repo uniswap/v3-core message "EMERGENCY: patch reentrancy" author "hayden.eth"

# Whale events
XADD stream:whale * from 0xAbc to BinanceHot amount_usd 47000000 token ETH

# SEC events
XADD stream:sec * form_type "Form 4" company "Coinbase" insider "Brian Armstrong" action Sale

# Social events
XADD stream:social * platform X keyword bitcoin spike_pct 680 post_count 12450

# Reddit events
XADD stream:reddit * subreddit CryptoCurrency keyword BTC post_count 1890 spike_pct 520
```

Scoring engine reads all 5 streams with `XREAD COUNT 100 BLOCK 1000`.

---

## 🧪 Testing

```bash
# Unit tests (when implemented)
npm test --workspaces

# Type check all packages
npm run typecheck --workspaces

# Test scorer directly
cd packages/scoring-engine
tsx src/scorer.ts

# Simulate signals to Redis for UI testing
redis-cli XADD stream:whale '*' from '0xWhale' to 'BinanceHot' amount_usd '50000000' token 'BTC'
```

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feat/my-feature`
3. Commit with conventional commits: `git commit -m "feat(scoring): add momentum indicator"`
4. Open a PR targeting `main`

---

## 📄 License

MIT License © 2024 Alpha Intelligence Contributors

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

---

*Built with ❤️ and Banglish debug logs.*

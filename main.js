/**
 * main.js — Terminal UI JavaScript
 *
 * Vanilla JS, zero frameworks.
 * - WebSocket with auto-reconnect (exponential backoff)
 * - Signal rendering with syntax highlighting
 * - Event rate calculator (sliding 60-second window)
 * - UTC clock
 * - Left panel: last 200 entries (virtualized trimming)
 * - Right panel: top 10 high-impact signals (impact >= 7)
 * - Health message handler: updates provider dots
 */

'use strict';

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

const WS_URL           = 'ws://localhost:3001';
const RECONNECT_BASE   = 1000;    // ms
const RECONNECT_MAX    = 30000;   // ms
const MAX_LEFT_ENTRIES = 200;
const MAX_RIGHT_CARDS  = 10;
const HIGH_IMPACT_MIN  = 7;
const RATE_WINDOW_MS   = 60_000; // 1 minute sliding window

// ─────────────────────────────────────────────────────────────────────────────
// State
// ─────────────────────────────────────────────────────────────────────────────

let ws                 = null;
let wsReconnectTimer   = null;
let reconnectAttempts  = 0;
let isConnected        = false;

const leftEntries      = [];  // circular buffer of DOM nodes
const rightSignals     = [];  // Array<SignalEvent>
const eventTimestamps  = [];  // timestamps for rate calculation (ms)

let totalReceived      = 0;
let highImpactToday    = 0;
let lastUpdateTs       = null;

// ─────────────────────────────────────────────────────────────────────────────
// DOM references
// ─────────────────────────────────────────────────────────────────────────────

const $intelStream         = document.getElementById('intel-stream');
const $intelEmpty          = document.getElementById('intel-empty');
const $alphaList           = document.getElementById('alpha-list');
const $alphaEmpty          = document.getElementById('alpha-empty');
const $totalEventsBadge    = document.getElementById('total-events-badge');
const $signalsToday        = document.getElementById('signals-today');
const $wsDot               = document.getElementById('ws-dot');
const $wsStatusText        = document.getElementById('ws-status-text');
const $disconnectBanner    = document.getElementById('disconnect-banner');
const $reconnectInfo       = document.getElementById('reconnect-info');
const $eventsPerMin        = document.getElementById('events-per-min');
const $totalReceived       = document.getElementById('total-received');
const $lastUpdate          = document.getElementById('last-update');
const $utcClock            = document.getElementById('utc-clock');
const $streamsConnected    = document.getElementById('streams-connected');
const $uptimeBadge         = document.getElementById('uptime-badge');

// ─────────────────────────────────────────────────────────────────────────────
// Matrix Rain Canvas
// ─────────────────────────────────────────────────────────────────────────────

(function initMatrixRain() {
  const canvas = document.getElementById('matrix-bg');
  const ctx    = canvas.getContext('2d');

  function resize() {
    canvas.width  = window.innerWidth;
    canvas.height = window.innerHeight;
  }
  resize();
  window.addEventListener('resize', resize, { passive: true });

  const chars      = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789@#$%^&*()_+-=[]{}|;:,./<>?';
  const fontSize   = 14;
  let columns      = Math.floor(canvas.width / fontSize);
  const drops      = [];

  function resetDrops() {
    columns = Math.floor(canvas.width / fontSize);
    drops.length = 0;
    for (let i = 0; i < columns; i++) {
      drops[i] = Math.random() * -100;
    }
  }
  resetDrops();
  window.addEventListener('resize', resetDrops, { passive: true });

  function draw() {
    ctx.fillStyle = 'rgba(10, 10, 15, 0.06)';
    ctx.fillRect(0, 0, canvas.width, canvas.height);
    ctx.fillStyle = '#00ff88';
    ctx.font      = fontSize + 'px JetBrains Mono, monospace';

    for (let i = 0; i < drops.length; i++) {
      const char = chars[Math.floor(Math.random() * chars.length)];
      ctx.fillText(char, i * fontSize, drops[i] * fontSize);
      if (drops[i] * fontSize > canvas.height && Math.random() > 0.975) {
        drops[i] = 0;
      }
      drops[i]++;
    }
  }

  setInterval(draw, 50);
})();

// ─────────────────────────────────────────────────────────────────────────────
// UTC Clock
// ─────────────────────────────────────────────────────────────────────────────

function updateClock() {
  const now  = new Date();
  const hh   = String(now.getUTCHours()).padStart(2, '0');
  const mm   = String(now.getUTCMinutes()).padStart(2, '0');
  const ss   = String(now.getUTCSeconds()).padStart(2, '0');
  const dd   = String(now.getUTCDate()).padStart(2, '0');
  const mo   = String(now.getUTCMonth() + 1).padStart(2, '0');
  const yyyy = now.getUTCFullYear();
  $utcClock.textContent = `${yyyy}-${mo}-${dd} ${hh}:${mm}:${ss} UTC`;
}
updateClock();
setInterval(updateClock, 1000);

// ─────────────────────────────────────────────────────────────────────────────
// WebSocket Management
// ─────────────────────────────────────────────────────────────────────────────

function connectWebSocket() {
  setWsStatus('connecting');

  try {
    ws = new WebSocket(WS_URL);
  } catch (err) {
    console.error('[UI] WebSocket construction error:', err);
    scheduleReconnect();
    return;
  }

  ws.onopen = () => {
    isConnected       = true;
    reconnectAttempts = 0;
    setWsStatus('connected');
    $disconnectBanner.classList.remove('visible');
    $reconnectInfo.classList.remove('visible');
    console.log('[UI] WebSocket connected to', WS_URL);
  };

  ws.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data);
      handleMessage(msg);
    } catch (err) {
      console.warn('[UI] Failed to parse WS message:', err);
    }
  };

  ws.onclose = (event) => {
    isConnected = false;
    setWsStatus('disconnected');
    $disconnectBanner.classList.add('visible');
    console.warn('[UI] WebSocket closed:', event.code, event.reason);
    scheduleReconnect();
  };

  ws.onerror = (err) => {
    console.error('[UI] WebSocket error:', err);
    // onclose will be called next
  };
}

function scheduleReconnect() {
  if (wsReconnectTimer) clearTimeout(wsReconnectTimer);

  const backoff = Math.min(
    RECONNECT_BASE * Math.pow(1.6, reconnectAttempts),
    RECONNECT_MAX,
  );
  reconnectAttempts++;

  $reconnectInfo.textContent = `Retry #${reconnectAttempts} in ${(backoff / 1000).toFixed(1)}s`;
  $reconnectInfo.classList.add('visible');

  console.log(`[UI] Reconnecting in ${backoff}ms (attempt ${reconnectAttempts})`);

  wsReconnectTimer = setTimeout(connectWebSocket, backoff);
}

function setWsStatus(status) {
  $wsDot.className = status;
  const labels = {
    connected:    '● Connected',
    connecting:   '◎ Connecting...',
    disconnected: '✗ Disconnected',
  };
  $wsStatusText.textContent = labels[status] ?? status;
}

// ─────────────────────────────────────────────────────────────────────────────
// Message Handlers
// ─────────────────────────────────────────────────────────────────────────────

function handleMessage(msg) {
  if (!msg || typeof msg !== 'object') return;

  switch (msg.type) {
    case 'signal':
      handleSignal(msg.data);
      break;
    case 'health':
      handleHealth(msg.data);
      break;
    default:
      console.warn('[UI] Unknown message type:', msg.type);
  }
}

function handleSignal(signal) {
  if (!signal || typeof signal !== 'object') return;

  totalReceived++;
  lastUpdateTs = new Date();

  // Record for rate calculation
  eventTimestamps.push(Date.now());

  // Update counters
  animateCounter($totalEventsBadge, totalReceived);
  $totalReceived.textContent = totalReceived;
  $lastUpdate.textContent = 'Last update: ' + formatTimeAgo(lastUpdateTs);

  // Add to left panel
  addToIntelStream(signal);

  // Check if high-impact
  const impact = parseInt(signal.Impact_Rating, 10);
  if (!isNaN(impact) && impact >= HIGH_IMPACT_MIN) {
    highImpactToday++;
    animateCounter($signalsToday, highImpactToday);
    addToAlphaPriority(signal);
    
    // Play audio alert for critical signals (impact >= 9)
    if (impact >= 9) {
      playSynthesizedSound('impact');
    }
  }
}

function handleHealth(data) {
  if (!data) return;

  // Update provider dots
  if (Array.isArray(data.providers)) {
    data.providers.forEach((p) => {
      const dot = document.getElementById('dot-' + p.name);
      if (dot) {
        dot.className = 'provider-dot ' + (p.status || 'down');
        dot.title     = `${p.name}: ${p.status}${p.latency_ms != null ? ' (' + p.latency_ms + 'ms)' : ''}`;
      }
    });
  }

  // Update stream dots based on events_in_window
  if (data.streams && typeof data.streams === 'object') {
    let activeCount = 0;
    Object.entries(data.streams).forEach(([streamKey, stats]) => {
      // Convert "stream:github" → "stream-github"
      const dotId = 'dot-' + streamKey.replace(':', '-');
      const dot   = document.getElementById(dotId);
      if (dot) {
        const hasData = stats.events_in_window > 0;
        if (hasData) {
          dot.className = 'provider-dot healthy';
          dot.title     = `${streamKey}: ${stats.events_in_window} events in window`;
          activeCount++;
        } else {
          dot.className = 'provider-dot degraded';
          dot.title     = `${streamKey}: No events in window`;
        }
      }
    });
    $streamsConnected.textContent = activeCount;
  }

  // Uptime
  if (typeof data.uptime_seconds === 'number') {
    $uptimeBadge.textContent = formatUptime(data.uptime_seconds);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Left Panel: Intel Stream
// ─────────────────────────────────────────────────────────────────────────────

function addToIntelStream(signal) {
  // Hide empty state
  $intelEmpty.style.display = 'none';

  const impact = parseInt(signal.Impact_Rating, 10) || 0;
  const bias   = signal.Directional_Bias || 'Neutral';
  const source = signal.Data_Source || 'Unknown';

  const biasClass    = getBiasClass(bias);
  const impactClass  = getImpactClass(impact);
  const isAlert      = impact >= 8;

  const entry = document.createElement('div');
  entry.className = [
    'intel-entry',
    biasClass,
    impactClass,
    isAlert ? 'alert-flash' : '',
  ].filter(Boolean).join(' ');

  const dotsHtml = buildImpactDots(impact);
  const ts       = signal.timestamp ? formatTs(signal.timestamp) : formatTs(new Date().toISOString());
  const impactPct = (impact / 10) * 100;
  const barColor  = getImpactBarColor(impact);

  entry.innerHTML = `
    <div class="entry-bias-bar"></div>
    <div class="entry-meta">
      <div class="entry-top-row">
        <span class="entry-ts">${escapeHtml(ts)}</span>
        <span class="entry-source source-${escapeHtml(source)}">${escapeHtml(source.replace(/_/g, ' '))}</span>
        <span class="entry-bias-badge bias-${escapeHtml(bias)}">${escapeHtml(bias)}</span>
        <div class="impact-dots">${dotsHtml}</div>
      </div>
      <div class="entry-summary">${escapeHtml(signal.Raw_Intel_Summary || '—')}</div>
      <div class="impact-bar-wrap">
        <div class="impact-bar-track">
          <div class="impact-bar-fill" style="width:${impactPct}%;background:${barColor}"></div>
        </div>
        <span class="impact-num">${impact}</span>
      </div>
    </div>
  `;

  // Prepend (newest on top)
  $intelStream.insertBefore(entry, $intelStream.firstChild);
  leftEntries.unshift(entry);

  // Trim to MAX_LEFT_ENTRIES
  while (leftEntries.length > MAX_LEFT_ENTRIES) {
    const old = leftEntries.pop();
    if (old && old.parentNode === $intelStream) {
      $intelStream.removeChild(old);
    }
  }

  // Update total badge
  $totalEventsBadge.textContent = leftEntries.length;
}

function buildImpactDots(impact) {
  let html = '';
  for (let i = 1; i <= 10; i++) {
    html += `<div class="impact-dot${i <= impact ? ' active' : ''}"></div>`;
  }
  return html;
}

// ─────────────────────────────────────────────────────────────────────────────
// Right Panel: Alpha Priority
// ─────────────────────────────────────────────────────────────────────────────

function addToAlphaPriority(signal) {
  $alphaEmpty.style.display = 'none';

  // Insert signal into sorted list (descending impact, then timestamp desc)
  rightSignals.unshift(signal);
  rightSignals.sort((a, b) => {
    const aI = parseInt(a.Impact_Rating, 10) || 0;
    const bI = parseInt(b.Impact_Rating, 10) || 0;
    if (bI !== aI) return bI - aI;
    // Same impact: newer first
    const aT = new Date(a.timestamp).getTime() || 0;
    const bT = new Date(b.timestamp).getTime() || 0;
    return bT - aT;
  });

  // Keep top 10
  if (rightSignals.length > MAX_RIGHT_CARDS) {
    rightSignals.splice(MAX_RIGHT_CARDS);
  }

  // Re-render the entire alpha list
  renderAlphaList();
}

function renderAlphaList() {
  // Clear existing cards (not the empty state)
  const existing = $alphaList.querySelectorAll('.alpha-card');
  existing.forEach((el) => el.remove());

  rightSignals.forEach((signal, idx) => {
    const card = buildAlphaCard(signal, idx === 0);
    $alphaList.appendChild(card);
  });
}

function buildAlphaCard(signal, isNewest) {
  const impact   = parseInt(signal.Impact_Rating, 10) || 0;
  const bias     = signal.Directional_Bias || 'Neutral';
  const source   = signal.Data_Source || 'Unknown';
  const biasClass = getBiasClass(bias);
  const isCritical = impact >= 9;
  const isHighImpact = impact >= 7;
  const isUltraHigh = impact >= 8;

  const ringClass  = isCritical ? 'impact-critical' : isHighImpact ? 'impact-high' : '';
  const ts         = signal.timestamp ? formatTs(signal.timestamp) : '—';
  const shortId    = signal.id ? signal.id.slice(0, 8) : '????????';

  const card = document.createElement('div');
  card.className = [
    'alpha-card',
    biasClass,
    isNewest ? 'new-card' : '',
    isUltraHigh ? 'ultra-high' : '',
  ].filter(Boolean).join(' ');

  card.innerHTML = `
    <div class="card-impact-ring ${ringClass}">${impact}</div>
    <div class="card-row1">
      <span class="card-source source-${escapeHtml(source)}">${escapeHtml(source.replace(/_/g, ' '))}</span>
      <span class="card-bias entry-bias-badge bias-${escapeHtml(bias)}">${escapeHtml(bias)}</span>
    </div>
    <div class="card-summary">${escapeHtml(signal.Raw_Intel_Summary || '—')}</div>
    <div class="card-footer">
      <span class="card-ts">${escapeHtml(ts)}</span>
      <span class="card-id">#${escapeHtml(shortId)}</span>
    </div>
  `;

  return card;
}

// ─────────────────────────────────────────────────────────────────────────────
// Event Rate Calculator
// ─────────────────────────────────────────────────────────────────────────────

function updateEventRate() {
  const now      = Date.now();
  const cutoff   = now - RATE_WINDOW_MS;

  // Remove timestamps older than 60s
  while (eventTimestamps.length > 0 && eventTimestamps[0] < cutoff) {
    eventTimestamps.shift();
  }

  $eventsPerMin.textContent = eventTimestamps.length;

  // Also update "last update" display
  if (lastUpdateTs) {
    $lastUpdate.textContent = 'Last update: ' + formatTimeAgo(lastUpdateTs);
  }
}

setInterval(updateEventRate, 2000);

// ─────────────────────────────────────────────────────────────────────────────
// Counter Animation
// ─────────────────────────────────────────────────────────────────────────────

function animateCounter(el, target) {
  if (!el) return;
  const start   = parseInt(el.textContent, 10) || 0;
  const delta   = target - start;
  if (delta === 0) return;

  const steps    = 12;
  const stepMs   = 20;
  let  step      = 0;

  const timer = setInterval(() => {
    step++;
    const pct = step / steps;
    const eased = 1 - Math.pow(1 - pct, 3); // ease-out cubic
    el.textContent = Math.round(start + delta * eased);
    if (step >= steps) {
      clearInterval(timer);
      el.textContent = target;
    }
  }, stepMs);
}

// ─────────────────────────────────────────────────────────────────────────────
// JSON Syntax Highlighting
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Converts a JS value to syntax-highlighted HTML string.
 * Used for displaying raw payloads in a JSON-like format.
 */
function syntaxHighlightJson(value, indent = 0) {
  const pad = '  '.repeat(indent);

  if (value === null) {
    return '<span class="json-null">null</span>';
  }

  if (typeof value === 'boolean') {
    return `<span class="json-bool">${value}</span>`;
  }

  if (typeof value === 'number') {
    return `<span class="json-num">${value}</span>`;
  }

  if (typeof value === 'string') {
    return `<span class="json-str">"${escapeHtml(value)}"</span>`;
  }

  if (Array.isArray(value)) {
    if (value.length === 0) return '<span class="json-punct">[]</span>';
    const items = value.map(
      (v) => `${pad}  ${syntaxHighlightJson(v, indent + 1)}`
    ).join('<span class="json-punct">,</span>\n');
    return `<span class="json-punct">[</span>\n${items}\n${pad}<span class="json-punct">]</span>`;
  }

  if (typeof value === 'object') {
    const keys = Object.keys(value);
    if (keys.length === 0) return '<span class="json-punct">{}</span>';
    const pairs = keys.map((k) => {
      const kHtml = `<span class="json-key">"${escapeHtml(k)}"</span>`;
      const vHtml = syntaxHighlightJson(value[k], indent + 1);
      return `${pad}  ${kHtml}<span class="json-punct">: </span>${vHtml}`;
    }).join('<span class="json-punct">,</span>\n');
    return `<span class="json-punct">{</span>\n${pairs}\n${pad}<span class="json-punct">}</span>`;
  }

  return escapeHtml(String(value));
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

function escapeHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

function formatTs(isoString) {
  try {
    const d   = new Date(isoString);
    const hh  = String(d.getUTCHours()).padStart(2, '0');
    const mm  = String(d.getUTCMinutes()).padStart(2, '0');
    const ss  = String(d.getUTCSeconds()).padStart(2, '0');
    return `${hh}:${mm}:${ss}`;
  } catch {
    return '??:??:??';
  }
}

function formatTimeAgo(date) {
  if (!date) return '—';
  const secs = Math.floor((Date.now() - date.getTime()) / 1000);
  if (secs < 5)  return 'just now';
  if (secs < 60) return `${secs}s ago`;
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins}m ago`;
  return `${Math.floor(mins / 60)}h ago`;
}

function formatUptime(seconds) {
  if (seconds < 60)   return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  return `${h}h ${m}m`;
}

function getBiasClass(bias) {
  const map = {
    'Bullish':           'bullish',
    'Bearish':           'bearish',
    'Volatility-Driven': 'volatility',
    'Neutral':           'neutral',
  };
  return map[bias] || 'neutral';
}

function getImpactClass(impact) {
  if (impact >= 9) return 'impact-high'; // critical/high same class, card handles diff
  if (impact >= 7) return 'impact-med';
  if (impact >= 4) return 'impact-low';
  return 'impact-noise';
}

function getImpactBarColor(impact) {
  if (impact >= 9) return '#ff3366';
  if (impact >= 7) return '#00ff88';
  if (impact >= 5) return '#ffcc00';
  if (impact >= 3) return '#4a4a6a';
  return '#2a2a4a';
}

// ─────────────────────────────────────────────────────────────────────────────
// Demo / Simulation Mode
// Activate when no WS is available to show how the UI looks
// Remove or comment out in production
// ─────────────────────────────────────────────────────────────────────────────

const DEMO_SIGNALS = [
  {
    id: 'demo-0001', timestamp: new Date().toISOString(),
    Data_Source: 'Whale_Flow', Directional_Bias: 'Bearish',
    Impact_Rating: 9,
    Raw_Intel_Summary: 'Wallet moved 48M USD ETH to Binance hot wallet — large liquidation imminent.',
  },
  {
    id: 'demo-0002', timestamp: new Date().toISOString(),
    Data_Source: 'GitHub_Commit', Directional_Bias: 'Bearish',
    Impact_Rating: 8,
    Raw_Intel_Summary: 'EMERGENCY reentrancy patch pushed to Uniswap v3-core — possible exploit disclosed.',
  },
  {
    id: 'demo-0003', timestamp: new Date().toISOString(),
    Data_Source: 'SEC_Filing', Directional_Bias: 'Bearish',
    Impact_Rating: 7,
    Raw_Intel_Summary: 'Coinbase CEO Brian Armstrong sold 31.8M USD worth of COIN shares today.',
  },
  {
    id: 'demo-0004', timestamp: new Date().toISOString(),
    Data_Source: 'X_Scrape', Directional_Bias: 'Bullish',
    Impact_Rating: 6,
    Raw_Intel_Summary: 'Bitcoin keyword spike 680% on X — retail sentiment turning strongly positive.',
  },
  {
    id: 'demo-0005', timestamp: new Date().toISOString(),
    Data_Source: 'Reddit_Spike', Directional_Bias: 'Volatility-Driven',
    Impact_Rating: 5,
    Raw_Intel_Summary: 'r/CryptoCurrency post volume up 520% in last 2 hours — heightened community activity.',
  },
  {
    id: 'demo-0006', timestamp: new Date().toISOString(),
    Data_Source: 'Whale_Flow', Directional_Bias: 'Bullish',
    Impact_Rating: 10,
    Raw_Intel_Summary: 'Blackrock BTC ETF received 220M USD inflow today — massive institutional buying confirmed.',
  },
  {
    id: 'demo-0007', timestamp: new Date().toISOString(),
    Data_Source: 'GitHub_Commit', Directional_Bias: 'Neutral',
    Impact_Rating: 3,
    Raw_Intel_Summary: 'Minor documentation update to Ethereum EIP-4844 specs repository.',
  },
  {
    id: 'demo-0008', timestamp: new Date().toISOString(),
    Data_Source: 'SEC_Filing', Directional_Bias: 'Bullish',
    Impact_Rating: 8,
    Raw_Intel_Summary: 'Michael Saylor purchased additional 25,000 BTC via MicroStrategy convertible notes.',
  },
];

// Inject demo data into the UI with slight delays to simulate live streaming
let demoIdx = 0;
function runDemo() {
  if (demoIdx < DEMO_SIGNALS.length) {
    const sig = { ...DEMO_SIGNALS[demoIdx], timestamp: new Date().toISOString() };
    handleSignal(sig);
    demoIdx++;
    setTimeout(runDemo, 800 + Math.random() * 1200);
  } else {
    // Cycle through demo signals again
    demoIdx = 0;
    setTimeout(runDemo, 5000);
  }
}

// Simulate health updates
function runDemoHealth() {
  handleHealth({
    providers: [
      { name: 'AI-Brain-Gateway', status: 'healthy', latency_ms: 42 },
      { name: 'Redis', status: 'healthy', latency_ms: 2 },
    ],
    streams: {
      'stream:github':  { events_in_window: 14, last_event_ts: new Date().toISOString() },
      'stream:whale':   { events_in_window: 7,  last_event_ts: new Date().toISOString() },
      'stream:sec':     { events_in_window: 3,  last_event_ts: new Date().toISOString() },
      'stream:social':  { events_in_window: 22, last_event_ts: new Date().toISOString() },
      'stream:reddit':  { events_in_window: 18, last_event_ts: new Date().toISOString() },
    },
    uptime_seconds: 3847,
  });
  setTimeout(runDemoHealth, 10_000);
}

// ─────────────────────────────────────────────────────────────────────────────
// Init: Connect WebSocket; fall back to demo after 4s if no WS
// ─────────────────────────────────────────────────────────────────────────────

let demoStarted = false;
let wsConnectedEver = false;

function startDemoIfNeeded() {
  if (!wsConnectedEver && !demoStarted) {
    demoStarted = true;
    console.log('[UI] No WS connection — running demo mode');
    setWsStatus('disconnected');
    $wsStatusText.textContent = '⚠ Demo Mode (WS unavailable)';
    runDemo();
    runDemoHealth();
  }
}

// Patch connectWebSocket to track if we ever connected
const _origOnOpen = (openWs) => {
  openWs.onopen = (ev) => {
    wsConnectedEver = true;
    isConnected       = true;
    reconnectAttempts = 0;
    setWsStatus('connected');
    $disconnectBanner.classList.remove('visible');
    $reconnectInfo.classList.remove('visible');
    console.log('[UI] WebSocket connected to', WS_URL);
  };
};

// Start connection
connectWebSocket();

// After 4 seconds, if still not connected, run demo
setTimeout(startDemoIfNeeded, 4000);

// ─────────────────────────────────────────────────────────────────────────────
// SMC Liquidity & Order Flow Engine Live Tracking
// ─────────────────────────────────────────────────────────────────────────────

// State
let selectedAsset = 'LTC';
let selectedTimeframe = '1m';
let currentPrice = 0.0;
let prevPrice = 0.0;
let change24h = 0.0;
let volume24h = 0.0;
let binanceSocket = null;
let isBinanceSocketActive = false;
let simulatedOrderFlowInterval = null;
let currentCandle = null;
let isAudioMuted = true;
let audioCtx = null;

const DEFAULT_PRICES = {
  BTC:  { price: 68500.0, change: 1.25,  volume: 28500.0 },
  ETH:  { price: 3850.0,  change: -0.85, volume: 185000.0 },
  SOL:  { price: 165.0,   change: 4.20,  volume: 3800000.0 },
  LTC:  { price: 82.0,    change: -1.10, volume: 450000.0 },
  XRP:  { price: 0.582,   change: 0.45,  volume: 12500000.0 },
  ADA:  { price: 0.485,   change: -2.30, volume: 16000000.0 },
  DOT:  { price: 6.20,    change: 1.85,  volume: 1900000.0 },
  LINK: { price: 15.50,   change: 3.10,  volume: 2400000.0 }
};

// TradingView Lightweight Charts State
let chart = null;
let candleSeries = null;
let trendlineSeries = null;
const priceLines = {};
let currentChartData = [];
let chartMarkers = [];

// DOM Cache
const $assetSelector = document.getElementById('asset-selector');
const $tickerSymbol = document.getElementById('ticker-symbol');
const $tickerPrice = document.getElementById('ticker-price');
const $priceDirectionArrow = document.getElementById('price-direction-arrow');
const $tickerChange = document.getElementById('ticker-change');
const $tickerVolume = document.getElementById('ticker-volume');

// Grids
const $bslRange = document.getElementById('bsl-range');
const $sslRange = document.getElementById('ssl-range');
const $bslBar = document.getElementById('bsl-bar');
const $sslBar = document.getElementById('ssl-bar');

const $supplyBlockRange = document.getElementById('supply-block-range');
const $demandBlockRange = document.getElementById('demand-block-range');
const $supplyWallDepth = document.getElementById('supply-wall-depth');
const $demandWallDepth = document.getElementById('demand-wall-depth');

const $trendStatusContainer = document.getElementById('trend-status-container');
const $trendStatusBadge = document.getElementById('trend-status-badge');
const $trapWarningContainer = document.getElementById('trap-warning-container');
const $trapWarningText = document.getElementById('trap-warning-text');

const $sweepFeedLog = document.getElementById('sweep-feed-log');

// Tabs DOM Cache
const $tabBtnMetrics = document.getElementById('tab-btn-metrics');
const $tabBtnChart = document.getElementById('tab-btn-chart');
const $tabContentMetrics = document.getElementById('tab-content-metrics');
const $tabContentChart = document.getElementById('tab-content-chart');

// Setup dropdown listener
if ($assetSelector) {
  $assetSelector.addEventListener('change', (e) => {
    switchAsset(e.target.value);
  });
}

// Setup sound toggle click listener
const $soundToggle = document.getElementById('sound-toggle');
if ($soundToggle) {
  $soundToggle.addEventListener('click', () => {
    isAudioMuted = !isAudioMuted;
    if (isAudioMuted) {
      $soundToggle.textContent = '🔇';
      $soundToggle.style.color = 'var(--clr-muted)';
      $soundToggle.style.textShadow = 'none';
      addLogEntry('🔇 Audio alerts muted.', 'purple');
    } else {
      $soundToggle.textContent = '🔊';
      $soundToggle.style.color = 'var(--clr-green)';
      $soundToggle.style.textShadow = '0 0 6px rgba(0, 255, 136, 0.4)';
      
      // Initialize/resume Web Audio API context
      initAudioContext();
      
      addLogEntry('🔊 Audio alerts active. Cyber synthesizer online.', 'green');
      // Play a short chirp to confirm audio is working
      playSynthesizedSound('sweep');
    }
  });
}

// Setup Timeframe Selector listeners
const $$tfBtns = document.querySelectorAll('.tf-btn');
$$tfBtns.forEach(($btn) => {
  $btn.addEventListener('click', (e) => {
    const newTf = e.target.getAttribute('data-tf');
    if (newTf === selectedTimeframe) return;
    
    // Toggle active class visually
    $$tfBtns.forEach((b) => {
      b.classList.remove('active');
      b.style.color = 'var(--clr-muted)';
      b.style.background = 'transparent';
      b.style.borderColor = 'transparent';
      b.style.fontWeight = '700';
    });
    
    e.target.classList.add('active');
    e.target.style.color = 'var(--clr-green)';
    e.target.style.background = 'rgba(0, 255, 136, 0.1)';
    e.target.style.borderColor = 'rgba(0, 255, 136, 0.3)';
    e.target.style.fontWeight = '800';
    
    selectedTimeframe = newTf;
    addLogEntry(`🕒 Chart timeframe changed to ${newTf.toUpperCase()}. Reconnecting feeds...`, 'purple');
    
    // Switch asset to trigger full reload with new timeframe
    switchAsset(selectedAsset);
  });
});

// Setup Tabs Event Listeners
if ($tabBtnMetrics && $tabBtnChart) {
  $tabBtnMetrics.addEventListener('click', () => {
    // Toggle buttons
    $tabBtnMetrics.classList.add('active');
    $tabBtnMetrics.style.color = 'var(--clr-green)';
    $tabBtnMetrics.style.borderBottomColor = 'var(--clr-green)';
    $tabBtnMetrics.style.textShadow = '0 0 6px rgba(0, 255, 136, 0.4)';
    
    $tabBtnChart.classList.remove('active');
    $tabBtnChart.style.color = 'var(--clr-muted)';
    $tabBtnChart.style.borderBottomColor = 'transparent';
    $tabBtnChart.style.textShadow = 'none';
    
    // Toggle contents
    $tabContentMetrics.style.display = 'flex';
    $tabContentChart.style.display = 'none';
  });

  $tabBtnChart.addEventListener('click', () => {
    // Toggle buttons
    $tabBtnChart.classList.add('active');
    $tabBtnChart.style.color = 'var(--clr-green)';
    $tabBtnChart.style.borderBottomColor = 'var(--clr-green)';
    $tabBtnChart.style.textShadow = '0 0 6px rgba(0, 255, 136, 0.4)';
    
    $tabBtnMetrics.classList.remove('active');
    $tabBtnMetrics.style.color = 'var(--clr-muted)';
    $tabBtnMetrics.style.borderBottomColor = 'transparent';
    $tabBtnMetrics.style.textShadow = 'none';
    
    // Toggle contents
    $tabContentMetrics.style.display = 'none';
    $tabContentChart.style.display = 'block';
    
    // Force chart resize immediately
    if (chart && $tabContentChart) {
      const width = $tabContentChart.clientWidth || 400;
      const height = $tabContentChart.clientHeight || 300;
      chart.resize(width, height);
    }
  });
}

// Global error handler to catch browser issues and print them in the feed log
window.onerror = function (message, source, lineno, colno, error) {
  addLogEntry(`❌ JS Error: ${message} at line ${lineno}`, 'red');
  console.error(error);
  return false;
};

// Initialize Lightweight Charts
let initChartRetries = 0;
function initChart() {
  if (typeof LightweightCharts === 'undefined') {
    if (initChartRetries < 20) { // Retry for up to 4 seconds
      initChartRetries++;
      setTimeout(initChart, 200);
    } else {
      console.error('[SMC Engine] TradingView Lightweight Charts library failed to load.');
      const container = document.getElementById('tab-content-chart');
      if (container) {
        container.innerHTML = `<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--clr-red);font-family:var(--font-mono);font-size:11px;font-weight:700;">⚠ CHART OFFLINE (LOCAL SCRIPT LOAD FAILED)</div>`;
      }
    }
    return;
  }

  const container = document.getElementById('tab-content-chart');
  if (!container) return;

  container.innerHTML = ''; // Clear container

  // Use container width/height or default fallbacks
  const initialWidth = container.clientWidth || 400;
  const initialHeight = container.clientHeight || 400;

  chart = LightweightCharts.createChart(container, {
    width: initialWidth,
    height: initialHeight,
    layout: {
      background: { type: 'solid', color: '#0a0a0f' },
      textColor: '#c8c8e8',
      fontFamily: 'JetBrains Mono, monospace',
    },
    grid: {
      vertLines: { color: 'rgba(255, 255, 255, 0.03)' },
      horzLines: { color: 'rgba(255, 255, 255, 0.03)' },
    },
    crosshair: {
      mode: LightweightCharts.CrosshairMode.Normal,
      vertLine: {
        color: 'rgba(0, 255, 136, 0.25)',
        width: 1,
        style: LightweightCharts.LineStyle.Solid,
      },
      horzLine: {
        color: 'rgba(0, 255, 136, 0.25)',
        width: 1,
        style: LightweightCharts.LineStyle.Solid,
      },
    },
    rightPriceScale: {
      borderColor: 'rgba(255, 255, 255, 0.08)',
      alignLabels: true,
    },
    timeScale: {
      borderColor: 'rgba(255, 255, 255, 0.08)',
      timeVisible: true,
      secondsVisible: false,
    },
  });

  candleSeries = chart.addSeries(LightweightCharts.CandlestickSeries, {
    upColor: '#00ff88',
    downColor: '#ff3366',
    borderUpColor: '#00ff88',
    borderDownColor: '#ff3366',
    wickUpColor: '#00ff88',
    wickDownColor: '#ff3366',
  });

  // Create Trendline Line Series
  trendlineSeries = chart.addSeries(LightweightCharts.LineSeries, {
    color: 'rgba(255, 204, 0, 0.7)',
    lineWidth: 1.5,
    lineStyle: LightweightCharts.LineStyle.Dashed,
    title: 'RETAIL TRENDLINE SUPPORT',
    lastValueVisible: false,
    priceLineVisible: false,
  });

  // Create Horizontal SMC Price Lines
  priceLines.bslMin = candleSeries.createPriceLine({
    price: 0,
    color: 'rgba(255, 51, 102, 0.65)',
    lineWidth: 1,
    lineStyle: LightweightCharts.LineStyle.Dashed,
    axisLabelVisible: true,
    title: 'BSL MIN (STOP TARGET)',
  });

  priceLines.bslMax = candleSeries.createPriceLine({
    price: 0,
    color: 'rgba(255, 51, 102, 0.65)',
    lineWidth: 1,
    lineStyle: LightweightCharts.LineStyle.Dashed,
    axisLabelVisible: true,
    title: 'BSL MAX (PRICE MAGNET)',
  });

  priceLines.sslMin = candleSeries.createPriceLine({
    price: 0,
    color: 'rgba(0, 255, 136, 0.65)',
    lineWidth: 1,
    lineStyle: LightweightCharts.LineStyle.Dashed,
    axisLabelVisible: true,
    title: 'SSL MIN (HUNT ZONE)',
  });

  priceLines.sslMax = candleSeries.createPriceLine({
    price: 0,
    color: 'rgba(0, 255, 136, 0.65)',
    lineWidth: 1,
    lineStyle: LightweightCharts.LineStyle.Dashed,
    axisLabelVisible: true,
    title: 'SSL MAX (RETAIL SL)',
  });

  priceLines.whaleSupply = candleSeries.createPriceLine({
    price: 0,
    color: '#ff3366',
    lineWidth: 1.5,
    lineStyle: LightweightCharts.LineStyle.Solid,
    axisLabelVisible: true,
    title: 'INSTITUTIONAL DISTRIBUTION WALL',
  });

  priceLines.whaleDemand = candleSeries.createPriceLine({
    price: 0,
    color: '#00ff88',
    lineWidth: 1.5,
    lineStyle: LightweightCharts.LineStyle.Solid,
    axisLabelVisible: true,
    title: 'INSTITUTIONAL ACCUMULATION WALL',
  });

  priceLines.trappedLiq = candleSeries.createPriceLine({
    price: 0,
    color: '#ffcc00',
    lineWidth: 1,
    lineStyle: LightweightCharts.LineStyle.Dotted,
    axisLabelVisible: true,
    title: 'TRAPPED LIQUIDITY LEVEL',
  });

  // Set up ResizeObserver to handle element size changes (including first render layout and tab switches)
  const resizeObserver = new ResizeObserver((entries) => {
    for (let entry of entries) {
      const { width, height } = entry.contentRect;
      if (chart && width > 0 && height > 0) {
        chart.resize(width, height);
      }
    }
  });
  resizeObserver.observe(container);
  
  // Call switchAsset now that candleSeries is fully initialized
  switchAsset(selectedAsset);
}

function switchAsset(asset) {
  selectedAsset = asset;
  $tickerSymbol.textContent = `${asset}/USDT`;
  
  // Reset prices & candle state to prevent bleeding between assets
  currentPrice = 0.0;
  prevPrice = 0.0;
  change24h = 0.0;
  volume24h = 0.0;
  currentCandle = null;
  currentChartData = [];
  chartMarkers = [];
  if (candleSeries) {
    try {
      candleSeries.setMarkers([]);
    } catch (e) {}
  }
  
  // Clear feed log
  $sweepFeedLog.innerHTML = `<div style="color: var(--clr-muted); font-style: italic;">[SYSTEM] Switching stream to ${asset}...</div>`;
  
  // Fetch initial REST data (24h stats + 100 historical candles) immediately, then connect socket
  fetchBinanceInitialData(asset);
  initBinanceStream(asset);
}

// REST Fetch for immediate data
async function fetchBinanceInitialData(asset) {
  const symbol = `${asset}USDT`;
  const hosts = [
    'https://api.binance.com',
    'https://api1.binance.com',
    'https://api2.binance.com',
    'https://api3.binance.com',
    'https://api.binance.us'
  ];

  let success = false;
  let price = 0.0;
  let change = 0.0;
  let volume = 0.0;
  let chartData = [];

  for (const host of hosts) {
    try {
      console.log(`[SMC Engine] Trying initial fetch via: ${host}`);
      // 1. Fetch 24h ticker info
      const tickerRes = await fetch(`${host}/api/v3/ticker/24hr?symbol=${symbol}`);
      if (!tickerRes.ok) throw new Error(`HTTP error ${tickerRes.status}`);
      const tickerData = await tickerRes.json();
      
      price = parseFloat(tickerData.lastPrice);
      change = parseFloat(tickerData.priceChangePercent);
      volume = parseFloat(tickerData.volume);

      // 2. Fetch historical K-lines (100 candles, selected interval)
      const klinesRes = await fetch(`${host}/api/v3/klines?symbol=${symbol}&interval=${selectedTimeframe}&limit=100`);
      if (!klinesRes.ok) throw new Error(`HTTP error ${klinesRes.status} on klines`);
      
      const klinesData = await klinesRes.json();
      chartData = klinesData.map(d => ({
        time: d[0] / 1000, // ms to seconds
        open: parseFloat(d[1]),
        high: parseFloat(d[2]),
        low: parseFloat(d[3]),
        close: parseFloat(d[4])
      }));

      success = true;
      console.log(`[SMC Engine] Successfully loaded data from: ${host}`);
      addLogEntry(`✅ Market data populated from ${host.replace('https://', '')}.`, 'green');
      break; // Stop iterating hosts
    } catch (err) {
      console.warn(`[SMC Engine] Host ${host} failed:`, err);
    }
  }

  if (success) {
    updateTickerData(price, change, volume);
    if (candleSeries && chartData.length > 0) {
      currentChartData = chartData;
      candleSeries.setData(chartData);
      drawTrendlineOnChart(chartData);
    }
  } else {
    console.error(`[SMC Engine] All REST endpoints failed. Falling back to local offline simulation.`);
    addLogEntry(`⚠️ REST API Offline: All feeds blocked. Initiating local mathematical simulation...`, 'yellow');

    // Fall back to default prices
    const def = DEFAULT_PRICES[asset] || { price: 100, change: 0, volume: 10000 };
    price = def.price;
    change = def.change;
    volume = def.volume;

    updateTickerData(price, change, volume);

    // Generate simulated K-lines
    chartData = generateSimulatedKlines(price);
    if (candleSeries) {
      currentChartData = chartData;
      candleSeries.setData(chartData);
      drawTrendlineOnChart(chartData);
    }
  }
}

// Generate 100 historical candlesticks when API is offline (respecting selectedTimeframe)
function generateSimulatedKlines(basePrice) {
  const data = [];
  const intervalMs = getTimeframeMs(selectedTimeframe);
  const nowAligned = Math.floor(Date.now() / intervalMs) * intervalMs;
  let nextOpen = basePrice;
  
  for (let i = 99; i >= 0; i--) {
    const timeSec = Math.floor((nowAligned - (99 - i) * intervalMs) / 1000);
    const change = (Math.random() - 0.5) * 0.001; // random walk
    const close = nextOpen;
    const open = close * (1 - change);
    const high = Math.max(open, close) * (1 + Math.random() * 0.0005);
    const low = Math.min(open, close) * (1 - Math.random() * 0.0005);
    
    data.unshift({
      time: timeSec,
      open: open,
      high: high,
      low: low,
      close: close
    });
    nextOpen = open;
  }
  return data;
}

// Draw retail trendline support line by connecting early and middle candlestick lows
function drawTrendlineOnChart(chartData) {
  if (!trendlineSeries || chartData.length < 50) return;
  
  const p1 = chartData[Math.floor(chartData.length * 0.15)];
  const p2 = chartData[Math.floor(chartData.length * 0.70)];
  
  const t1 = p1.time;
  const y1 = p1.low;
  const t2 = p2.time;
  const y2 = p2.low;
  
  const slope = (y2 - y1) / (t2 - t1);
  
  const trendPoints = [];
  for (let i = 0; i < chartData.length; i++) {
    const t = chartData[i].time;
    if (t >= t1) {
      const y = y1 + slope * (t - t1);
      trendPoints.push({ time: t, value: y });
    }
  }
  
  trendlineSeries.setData(trendPoints);
}

// Add annotation markers to the candlestick chart
function addChartMarker(time, text, position = 'aboveBar', color = '#ffcc00', shape = 'arrowDown') {
  if (!candleSeries) return;
  
  chartMarkers.push({
    time: time,
    position: position,
    color: color,
    shape: shape,
    text: text,
  });
  
  // Limit to 15 active markers to prevent clutter
  if (chartMarkers.length > 15) {
    chartMarkers.shift();
  }
  
  try {
    candleSeries.setMarkers(chartMarkers);
  } catch (e) {
    console.error('[SMC Chart] Failed to set markers:', e);
  }
}

// WebSocket Stream (Combined Stream for Kline updates and 24h Ticker data)
function initBinanceStream(asset) {
  if (binanceSocket) {
    try {
      binanceSocket.close();
    } catch (e) {}
  }
  
  const symbol = `${asset.toLowerCase()}usdt`;
  // Combined stream: kline + ticker
  const socketUrl = `wss://stream.binance.com:9443/stream?streams=${symbol}@kline_${selectedTimeframe}/${symbol}@ticker`;
  
  binanceSocket = new WebSocket(socketUrl);
  isBinanceSocketActive = false;
  
  binanceSocket.onopen = () => {
    console.log(`[SMC Engine] Binance stream connected for ${asset} (${selectedTimeframe})`);
    addLogEntry(`🔌 Stream established: ${asset}/USDT combined ${selectedTimeframe} market feed active.`, 'cyan');
    isBinanceSocketActive = true;
  };
  
  binanceSocket.onmessage = (event) => {
    try {
      const payload = JSON.parse(event.data);
      const stream = payload.stream;
      const data = payload.data;
      
      isBinanceSocketActive = true;
      
      if (stream.endsWith(`@kline_${selectedTimeframe}`)) {
        const k = data.k;
        if (candleSeries) {
          candleSeries.update({
            time: k.t / 1000,
            open: parseFloat(k.o),
            high: parseFloat(k.h),
            low: parseFloat(k.l),
            close: parseFloat(k.c)
          });
        }
      } else if (stream.endsWith('@ticker')) {
        // 'c' is last price, 'P' is price change percent, 'v' is base asset volume
        const price = parseFloat(data.c);
        const change = parseFloat(data.P);
        const volume = parseFloat(data.v);
        
        updateTickerData(price, change, volume);
      }
    } catch (err) {
      console.warn('[SMC Engine] Failed to parse message:', err);
    }
  };
  
  binanceSocket.onclose = () => {
    console.warn(`[SMC Engine] Binance stream closed for ${asset}`);
    isBinanceSocketActive = false;
  };
  
  binanceSocket.onerror = (err) => {
    console.error('[SMC Engine] WebSocket error:', err);
    isBinanceSocketActive = false;
  };
}

// Update DOM ticker and trigger recalculations
function updateTickerData(price, change, volume) {
  if (isNaN(price)) return;
  
  prevPrice = currentPrice || price;
  currentPrice = price;
  change24h = change;
  volume24h = volume;
  
  // Update Price
  $tickerPrice.textContent = `$${formatPrice(price)}`;
  
  // Flash animation & Arrow direction
  $tickerPrice.classList.remove('ticker-flash-green', 'ticker-flash-red');
  if (price > prevPrice) {
    $tickerPrice.classList.add('ticker-flash-green');
    $priceDirectionArrow.textContent = '▲';
    $priceDirectionArrow.style.color = 'var(--clr-green)';
    $priceDirectionArrow.style.transform = 'translateY(-1px)';
  } else if (price < prevPrice) {
    $tickerPrice.classList.add('ticker-flash-red');
    $priceDirectionArrow.textContent = '▼';
    $priceDirectionArrow.style.color = 'var(--clr-red)';
    $priceDirectionArrow.style.transform = 'translateY(1px)';
  }
  
  // Update 24h Change
  const isPositive = change >= 0;
  $tickerChange.textContent = `${isPositive ? '+' : ''}${change.toFixed(2)}%`;
  $tickerChange.style.color = isPositive ? 'var(--clr-green)' : 'var(--clr-red)';
  
  // Update 24h Volume
  $tickerVolume.textContent = formatVolume(volume);
  
  // Run Calculations
  recalculateSMC(price, change);
}

// SMC Mathematical Model
function recalculateSMC(price, change) {
  // 1. Liquidity Pools Matrix (BSL/SSL Tracking)
  // BSL: premium range (Live Price * 1.010 to 1.018)
  const bslMin = price * 1.010;
  const bslMax = price * 1.018;
  $bslRange.textContent = `$${formatPrice(bslMin)} - $${formatPrice(bslMax)}`;
  
  // SSL: discount range (Live Price * 0.982 to 0.990)
  const sslMin = price * 0.982;
  const sslMax = price * 0.990;
  $sslRange.textContent = `$${formatPrice(sslMin)} - $${formatPrice(sslMax)}`;
  
  // Calculate dynamic proximity bars (visual representation/rich aesthetics)
  const bslPercent = Math.max(5, Math.min(100, ((price - (price * 0.99)) / (bslMin - (price * 0.99))) * 100));
  const sslPercent = Math.max(5, Math.min(100, (((price * 1.01) - price) / ((price * 1.01) - sslMax)) * 100));
  
  $bslBar.style.width = `${bslPercent}%`;
  $sslBar.style.width = `${sslPercent}%`;
  
  // 2. Institutional Order Blocks
  // Whale Supply Zone: project distribution block just below BSL zone (Live Price * 1.004 to 1.008)
  const supplyMin = price * 1.004;
  const supplyMax = price * 1.008;
  $supplyBlockRange.textContent = `$${formatPrice(supplyMin)} - $${formatPrice(supplyMax)}`;
  
  // Whale Demand Zone: project accumulation block just above SSL zone (Live Price * 0.992 to 0.996)
  const demandMin = price * 0.992;
  const demandMax = price * 0.996;
  $demandBlockRange.textContent = `$${formatPrice(demandMin)} - $${formatPrice(demandMax)}`;
  
  // Dynamic whale depth values (fluctuate slightly to feel alive)
  const dynamicFactor = 1 + (Math.sin(Date.now() / 5000) * 0.08); // +/- 8% oscillation
  const supplyWallDepthVal = (volume24h * 0.005) * dynamicFactor;
  const demandWallDepthVal = (volume24h * 0.006) * dynamicFactor;
  $supplyWallDepth.textContent = `${formatVolume(supplyWallDepthVal)} ${selectedAsset}`;
  $demandWallDepth.textContent = `${formatVolume(demandWallDepthVal)} ${selectedAsset}`;
  
  // 3. Trendline Trap Detector
  const isUpTrend = change >= 0;
  if (isUpTrend) {
    $trendStatusBadge.textContent = 'Active Buy-Trend (Up-Trend)';
    $trendStatusBadge.style.color = 'var(--clr-green)';
    $trendStatusBadge.style.background = 'rgba(0, 255, 136, 0.12)';
    $trendStatusBadge.style.border = '1px solid rgba(0, 255, 136, 0.25)';
    $trendStatusContainer.style.borderLeftColor = 'var(--clr-green)';
  } else {
    $trendStatusBadge.textContent = 'Active Sale-Trend (Down-Trend)';
    $trendStatusBadge.style.color = 'var(--clr-red)';
    $trendStatusBadge.style.background = 'rgba(255, 51, 102, 0.12)';
    $trendStatusBadge.style.border = '1px solid rgba(255, 51, 102, 0.25)';
    $trendStatusContainer.style.borderLeftColor = 'var(--clr-red)';
  }
  
  const trapPrice = price * 0.987;
  $trapWarningText.innerHTML = `Retail Liquidity Trapped Below <span style="color:var(--clr-yellow);font-weight:800;">$${formatPrice(trapPrice)}</span> - Anticipate institutional fakeout prior to reversal.`;
  
  // Dynamic warning borders
  if (change < -3) {
    $trapWarningContainer.style.borderColor = 'rgba(255, 51, 102, 0.4)';
    $trapWarningContainer.style.background = 'rgba(255, 51, 102, 0.05)';
  } else {
    $trapWarningContainer.style.borderColor = 'rgba(255, 204, 0, 0.25)';
    $trapWarningContainer.style.background = 'rgba(255, 204, 0, 0.05)';
  }
  
  // Update Price Lines on Chart
  if (priceLines.bslMin) priceLines.bslMin.applyOptions({ price: bslMin });
  if (priceLines.bslMax) priceLines.bslMax.applyOptions({ price: bslMax });
  if (priceLines.sslMin) priceLines.sslMin.applyOptions({ price: sslMin });
  if (priceLines.sslMax) priceLines.sslMax.applyOptions({ price: sslMax });
  if (priceLines.whaleSupply) priceLines.whaleSupply.applyOptions({ price: price * 1.006 });
  if (priceLines.whaleDemand) priceLines.whaleDemand.applyOptions({ price: price * 0.994 });
  if (priceLines.trappedLiq) priceLines.trappedLiq.applyOptions({ price: trapPrice });

  // 4. Alert trigger logic on tick
  checkAlertThresholds(price, bslMin, bslMax, sslMin, sslMax);
}

// Alert Threshold Checker
let lastAlertTime = 0;
let lastAlertState = '';

function checkAlertThresholds(price, bslMin, bslMax, sslMin, sslMax) {
  const now = Date.now();
  if (now - lastAlertTime < 8000) return; // limit alerts to every 8s to prevent spamming
  
  // Approaching SSL (within 0.8% of SSL upper bound)
  if (price <= sslMax * 1.008 && price >= sslMin) {
    if (lastAlertState !== 'ssl') {
      lastAlertState = 'ssl';
      lastAlertTime = now;
      addLogEntry(`🚨 Live Alert: Price approaching SSL at $${formatPrice(sslMax)}! Whales building liquidity.`, 'red');
      playSynthesizedSound('sweep');
    }
  }
  // BSL Hunt active (within 0.8% of BSL lower bound)
  else if (price >= bslMin * 0.992 && price <= bslMax) {
    if (lastAlertState !== 'bsl') {
      lastAlertState = 'bsl';
      lastAlertTime = now;
      addLogEntry(`🚨 Live Alert: BSL Hunt active near $${formatPrice(bslMin)}! Expect short-term whale distribution.`, 'yellow');
      playSynthesizedSound('sweep');
    }
  }
}

// Log utility for sweep feed
function addLogEntry(message, colorClass = '') {
  const time = new Date();
  const hh = String(time.getHours()).padStart(2, '0');
  const mm = String(time.getMinutes()).padStart(2, '0');
  const ss = String(time.getSeconds()).padStart(2, '0');
  const timestamp = `[${hh}:${mm}:${ss}]`;
  
  let colorStyle = 'color: var(--clr-text);';
  if (colorClass === 'green' || colorClass === 'bullish') colorStyle = 'color: var(--clr-green); font-weight: 700;';
  else if (colorClass === 'red' || colorClass === 'bearish') colorStyle = 'color: var(--clr-red); font-weight: 700;';
  else if (colorClass === 'yellow' || colorClass === 'volatility') colorStyle = 'color: var(--clr-yellow); font-weight: 700;';
  else if (colorClass === 'cyan') colorStyle = 'color: var(--clr-cyan); font-weight: 700;';
  else if (colorClass === 'purple') colorStyle = 'color: var(--clr-purple);';
  
  const div = document.createElement('div');
  div.style.marginBottom = '3px';
  div.innerHTML = `<span style="color: var(--clr-muted); margin-right: 6px;">${timestamp}</span><span style="${colorStyle}">${message}</span>`;
  
  $sweepFeedLog.appendChild(div);
  
  // Trim to 100 entries to prevent memory growth
  while ($sweepFeedLog.children.length > 100) {
    $sweepFeedLog.removeChild($sweepFeedLog.firstChild);
  }
  
  // Scroll to bottom
  $sweepFeedLog.scrollTop = $sweepFeedLog.scrollHeight;
}

// Simulate Order Flow micro-ticks (ticks that occur every 1.5 - 3 seconds to keep feed alive)
function startSimulatedOrderFlow() {
  if (simulatedOrderFlowInterval) clearTimeout(simulatedOrderFlowInterval);
  
  const actions = [
    { text: 'limit bid filled at', type: 'green' },
    { text: 'limit ask filled at', type: 'red' },
    { text: 'market sell swept to', type: 'red' },
    { text: 'market buy swept to', type: 'green' },
    { text: 'stop-loss triggered at', type: 'yellow' },
    { text: 'whale resting order placed at', type: 'cyan' }
  ];
  
  function tick() {
    if (!currentPrice) {
      const def = DEFAULT_PRICES[selectedAsset] || { price: 100, change: 0, volume: 10000 };
      currentPrice = def.price;
      change24h = def.change;
      volume24h = def.volume;
    }
    
    // Choose random action
    const action = actions[Math.floor(Math.random() * actions.length)];
    // Perturb price slightly for simulated tick
    const tickPrice = currentPrice * (1 + ((Math.random() - 0.5) * 0.0005));
    // Random size
    const size = (volume24h * 0.00002 * (Math.random() + 0.1)).toFixed(selectedAsset === 'BTC' ? 3 : 1);
    
    let msg = '';
    if (action.text.includes('whale')) {
      msg = `🐋 Whale block: ${size} ${selectedAsset} ${action.text} $${formatPrice(tickPrice)}`;
      playSynthesizedSound('whale');
    } else {
      msg = `⚡ Order flow: ${size} ${selectedAsset} ${action.text} $${formatPrice(tickPrice)}`;
    }
    
    addLogEntry(msg, action.type);
    
    // If Binance socket is offline/inactive, update the ticker data and candlestick chart!
    if (!isBinanceSocketActive) {
      // 1. Update ticker stats
      const volumeGrowth = parseFloat(size) || 0.0;
      updateTickerData(tickPrice, change24h, volume24h + volumeGrowth);
      
      // 2. Update candlestick chart
      if (candleSeries) {
        const intervalMs = getTimeframeMs(selectedTimeframe);
        const candleTime = Math.floor(Date.now() / intervalMs) * (intervalMs / 1000); // start of current timeframe candle in seconds
        
        if (!currentCandle || currentCandle.time !== candleTime) {
          if (currentCandle && currentChartData) {
            currentChartData.push(currentCandle);
            if (currentChartData.length > 150) currentChartData.shift();
            drawTrendlineOnChart(currentChartData);
          }
          
          currentCandle = {
            time: candleTime,
            open: currentPrice,
            high: Math.max(currentPrice, tickPrice),
            low: Math.min(currentPrice, tickPrice),
            close: tickPrice
          };
        } else {
          currentCandle.close = tickPrice;
          currentCandle.high = Math.max(currentCandle.high, tickPrice);
          currentCandle.low = Math.min(currentCandle.low, tickPrice);
        }
        
        candleSeries.update(currentCandle);
        
        // 3. Add visual markers dynamically based on simulated action
        if (Math.random() < 0.15) { // 15% chance per tick to place a marker to avoid overcrowding
          if (action.text.includes('whale')) {
            if (action.type === 'green') {
              addChartMarker(candleTime, '🐋 WHALE BUY', 'belowBar', '#00ff88', 'arrowUp');
            } else {
              addChartMarker(candleTime, '🐋 WHALE SELL', 'aboveBar', '#ff3366', 'arrowDown');
            }
          } else if (action.text.includes('stop-loss')) {
            addChartMarker(candleTime, '🚨 SL SWEEP', 'belowBar', '#ffcc00', 'arrowUp');
          }
        }
      }
    }
    
    // Schedule next tick
    const delay = 1500 + Math.random() * 2500;
    simulatedOrderFlowInterval = setTimeout(tick, delay);
  }
  
  simulatedOrderFlowInterval = setTimeout(tick, 2000);
}

// Helpers
function formatPrice(val) {
  if (val === 0) return '0.00';
  if (val > 1000) return val.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  if (val > 1) return val.toFixed(2);
  return val.toFixed(4);
}

// Timeframe / Volume Formatter
function formatVolume(val) {
  if (val >= 1e6) return (val / 1e6).toFixed(2) + 'M';
  if (val >= 1e3) return (val / 1e3).toFixed(2) + 'K';
  return val.toFixed(2);
}

function getTimeframeMs(tf) {
  if (tf === '5m') return 5 * 60 * 1000;
  if (tf === '15m') return 15 * 60 * 1000;
  if (tf === '1h') return 60 * 60 * 1000;
  return 60 * 1000; // default '1m'
}

function initAudioContext() {
  if (!audioCtx) {
    audioCtx = new (window.AudioContext || window.webkitAudioContext)();
  }
  if (audioCtx.state === 'suspended') {
    audioCtx.resume();
  }
}

function playSynthesizedSound(type) {
  if (isAudioMuted) return;
  try {
    initAudioContext();
    if (!audioCtx) return;

    const now = audioCtx.currentTime;
    
    if (type === 'impact') {
      // High impact cyber alert: double beep
      playTone(880, 0.08, 'sine', now);
      playTone(1100, 0.12, 'sine', now + 0.1);
    } 
    else if (type === 'sweep') {
      // Sweep alert: laser chirp
      const osc = audioCtx.createOscillator();
      const gain = audioCtx.createGain();
      
      osc.type = 'triangle';
      osc.frequency.setValueAtTime(800, now);
      osc.frequency.exponentialRampToValueAtTime(1400, now + 0.15);
      
      gain.gain.setValueAtTime(0.12, now);
      gain.gain.exponentialRampToValueAtTime(0.01, now + 0.15);
      
      osc.connect(gain);
      gain.connect(audioCtx.destination);
      
      osc.start(now);
      osc.stop(now + 0.15);
    } 
    else if (type === 'whale') {
      // Whale Order Fill: low deep sub-sonar ping
      const osc = audioCtx.createOscillator();
      const gain = audioCtx.createGain();
      
      osc.type = 'sine';
      osc.frequency.setValueAtTime(120, now);
      osc.frequency.exponentialRampToValueAtTime(80, now + 0.4);
      
      gain.gain.setValueAtTime(0.2, now);
      gain.gain.exponentialRampToValueAtTime(0.01, now + 0.4);
      
      osc.connect(gain);
      gain.connect(audioCtx.destination);
      
      osc.start(now);
      osc.stop(now + 0.4);
    }
  } catch (err) {
    console.warn('[Audio Alerts] Web Audio failed:', err);
  }
}

function playTone(freq, duration, type = 'sine', startTime = null) {
  if (!audioCtx) return;
  const t = startTime || audioCtx.currentTime;
  const osc = audioCtx.createOscillator();
  const gain = audioCtx.createGain();
  
  osc.type = type;
  osc.frequency.setValueAtTime(freq, t);
  
  gain.gain.setValueAtTime(0.1, t);
  gain.gain.exponentialRampToValueAtTime(0.01, t + duration);
  
  osc.connect(gain);
  gain.connect(audioCtx.destination);
  
  osc.start(t);
  osc.stop(t + duration);
}


// Trigger initial setup
initChart();
startSimulatedOrderFlow();

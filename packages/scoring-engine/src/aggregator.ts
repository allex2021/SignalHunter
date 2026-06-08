/**
 * aggregator.ts — Signal Aggregator Main Service
 *
 * Architecture:
 *  - Connects to Redis, reads from 5 streams
 *  - Maintains a 2-hour rolling window in Redis sorted sets
 *  - Every 5 minutes: triggers ScoringEngine
 *  - Publishes scored signals to stream:signals
 *  - WebSocket server broadcasts signals + health to connected UIs
 *
 * Banglish logs:
 *   "Signal Aggregator shuru holo!"
 *   "5 stream theke data collect korchi"
 *   "Scoring tick: Xta event window e ache"
 *   "Xta UI client ke signal pathalam"
 *   "Aggregator gracefully shutdown hoise"
 */

import 'dotenv/config';
import { IncomingMessage } from 'http';
import Redis, { ChainableCommander } from 'ioredis';
import { WebSocketServer, WebSocket } from 'ws';
import { ScoringEngine, RawWindowEvent, SignalEvent } from './scorer';

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

const REDIS_URL = process.env['REDIS_URL'] ?? 'redis://localhost:6379';
const WS_PORT = parseInt(process.env['WS_PORT'] ?? '3001', 10);
const SCORING_INTERVAL_MS = parseInt(
  process.env['SCORING_INTERVAL_MS'] ?? '300000', // 5 minutes
  10,
);
const WINDOW_DURATION_MS = parseInt(
  process.env['WINDOW_DURATION_MS'] ?? '7200000', // 2 hours
  10,
);
const HEALTH_BROADCAST_INTERVAL_MS = 10_000; // 10 seconds
const STREAM_BLOCK_MS = 1_000; // XREAD block timeout
const STREAM_COUNT = 100; // events per XREAD call

const STREAMS = [
  'stream:github',
  'stream:whale',
  'stream:sec',
  'stream:social',
  'stream:reddit',
] as const;

type StreamName = (typeof STREAMS)[number];

// Redis sorted set keys for windowed event storage
const windowKey = (stream: StreamName) => `window:${stream}`;

// Redis hash key for persisting last-read stream IDs
const CURSOR_KEY = 'aggregator:cursors';

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

interface ProviderHealth {
  name: string;
  status: 'healthy' | 'degraded' | 'down';
  latency_ms: number | null;
  last_check: string;
}

interface StreamStats {
  [stream: string]: {
    events_in_window: number;
    last_event_ts: string | null;
  };
}

interface HealthBroadcast {
  type: 'health';
  data: {
    providers: ProviderHealth[];
    streams: StreamStats;
    uptime_seconds: number;
  };
}

interface SignalBroadcast {
  type: 'signal';
  data: SignalEvent;
}

type WsBroadcast = HealthBroadcast | SignalBroadcast;

// ─────────────────────────────────────────────────────────────────────────────
// Aggregator
// ─────────────────────────────────────────────────────────────────────────────

class SignalAggregator {
  private readonly redis: Redis;
  private readonly redisSub: Redis; // dedicated client for blocking reads
  private readonly scoringEngine: ScoringEngine;
  private readonly wss: WebSocketServer;
  private readonly wsClients: Set<WebSocket> = new Set();

  private readonly startTime = Date.now();
  private isShuttingDown = false;

  private scoringTimer: ReturnType<typeof setInterval> | null = null;
  private healthTimer: ReturnType<typeof setInterval> | null = null;

  // Track stream cursors (last read IDs) in memory for the read loop
  private readonly streamCursors: Map<StreamName, string> = new Map();

  // Stream read loop handles (one per stream)
  private readonly streamLoops: Map<StreamName, boolean> = new Map();

  constructor() {
    console.log('[Aggregator] Signal Aggregator shuru holo!');

    this.redis = new Redis(REDIS_URL, {
      maxRetriesPerRequest: 3,
      retryStrategy: (times) => Math.min(times * 200, 5000),
      lazyConnect: true,
    });

    this.redisSub = new Redis(REDIS_URL, {
      maxRetriesPerRequest: null, // keep retrying for blocking reads
      retryStrategy: (times) => Math.min(times * 200, 5000),
      lazyConnect: true,
    });

    this.scoringEngine = new ScoringEngine();

    this.wss = new WebSocketServer({ port: WS_PORT }, () => {
      console.log(`[Aggregator] WebSocket server port ${WS_PORT} e ready`);
    });

    this.setupWebSocket();
    this.setupSignals();
  }

  // ── Bootstrap ────────────────────────────────────────────────────────────

  async start(): Promise<void> {
    console.log('[Aggregator] Redis e connect korchi...');
    await this.redis.connect();
    await this.redisSub.connect();
    console.log('[Aggregator] Redis connected!');

    // Load persisted cursors from Redis
    await this.loadCursors();

    console.log('[Aggregator] 5 stream theke data collect korchi');

    // Start one read loop per stream
    for (const stream of STREAMS) {
      this.streamLoops.set(stream, true);
      this.startStreamReadLoop(stream).catch((err) => {
        console.error(`[Aggregator] Stream ${stream} loop error:`, err);
      });
    }

    // Scoring tick every 5 minutes
    this.scoringTimer = setInterval(() => {
      this.scoringTick().catch((err) => {
        console.error('[Aggregator] Scoring tick error:', err);
      });
    }, SCORING_INTERVAL_MS);

    // Health broadcast every 10 seconds
    this.healthTimer = setInterval(() => {
      this.broadcastHealth().catch((err) => {
        console.error('[Aggregator] Health broadcast error:', err);
      });
    }, HEALTH_BROADCAST_INTERVAL_MS);

    // Run first scoring tick immediately after 10s to have initial data
    setTimeout(() => {
      this.scoringTick().catch(() => undefined);
    }, 10_000);

    console.log('[Aggregator] Aggregator fully running!');
  }

  // ── Redis Stream Reading ─────────────────────────────────────────────────

  private async loadCursors(): Promise<void> {
    const stored = await this.redis.hgetall(CURSOR_KEY);
    for (const stream of STREAMS) {
      // Default to '$' to read only new messages from startup
      this.streamCursors.set(stream, stored[stream] ?? '$');
    }
    console.log('[Aggregator] Stream cursors loaded:', Object.fromEntries(this.streamCursors));
  }

  private async persistCursors(): Promise<void> {
    const pipeline = this.redis.pipeline() as ChainableCommander;
    for (const [stream, cursor] of this.streamCursors.entries()) {
      pipeline.hset(CURSOR_KEY, stream, cursor);
    }
    await pipeline.exec();
  }

  private async startStreamReadLoop(stream: StreamName): Promise<void> {
    console.log(`[Aggregator] ${stream} stream loop shuru holo`);

    while (this.streamLoops.get(stream) && !this.isShuttingDown) {
      try {
        const cursor = this.streamCursors.get(stream) ?? '$';

        // XREAD blocks for up to STREAM_BLOCK_MS milliseconds
        const results = await this.redisSub.xread(
          'COUNT',
          STREAM_COUNT,
          'BLOCK',
          STREAM_BLOCK_MS,
          'STREAMS',
          stream,
          cursor,
        );

        if (!results || results.length === 0) {
          // Timeout — no new events
          continue;
        }

        const [_streamName, entries] = results[0] as [
          string,
          Array<[string, string[]]>,
        ];

        if (!entries || entries.length === 0) continue;

        const nowMs = Date.now();
        const pipeline = this.redis.pipeline() as ChainableCommander;

        for (const [entryId, fields] of entries) {
          // Parse field-value pairs into an object
          const payload: Record<string, unknown> = {};
          for (let i = 0; i < fields.length - 1; i += 2) {
            const key = fields[i];
            const val = fields[i + 1];
            if (key !== undefined && val !== undefined) {
              try {
                payload[key] = JSON.parse(val);
              } catch {
                payload[key as string] = val;
              }
            }
          }

          // Get timestamp from stream entry ID (format: <ms>-<seq>)
          const tsPart = entryId.split('-')[0];
          const eventTs = tsPart ? parseInt(tsPart, 10) : nowMs;

          const rawEvent: RawWindowEvent = {
            stream,
            event_id: entryId,
            timestamp: eventTs,
            payload,
          };

          // Store in Redis sorted set with timestamp as score
          pipeline.zadd(
            windowKey(stream),
            eventTs,
            JSON.stringify(rawEvent),
          );

          // Trim events older than window duration
          pipeline.zremrangebyscore(
            windowKey(stream),
            '-inf',
            nowMs - WINDOW_DURATION_MS,
          );
        }

        await pipeline.exec();

        // Update cursor to last seen entry ID
        const lastEntry = entries[entries.length - 1];
        if (lastEntry) {
          this.streamCursors.set(stream, lastEntry[0]);
        }

        // Persist cursors every batch
        await this.persistCursors();

        const batchSize = entries.length;
        if (batchSize > 0) {
          console.log(
            `[Aggregator] ${stream}: ${batchSize}ta naya event window e add kora hoise`,
          );
        }
      } catch (err) {
        if (this.isShuttingDown) break;
        console.error(`[Aggregator] ${stream} read error:`, err);
        // Brief pause before retry
        await new Promise((r) => setTimeout(r, 2000));
      }
    }

    console.log(`[Aggregator] ${stream} stream loop bondo hoise`);
  }

  // ── Scoring Tick ─────────────────────────────────────────────────────────

  private async scoringTick(): Promise<void> {
    try {
      const nowMs = Date.now();
      const windowStart = nowMs - WINDOW_DURATION_MS;

      // Collect all events from all stream windows
      const allEvents: RawWindowEvent[] = [];

      for (const stream of STREAMS) {
        const raw = await this.redis.zrangebyscore(
          windowKey(stream),
          windowStart,
          nowMs,
        );

        for (const item of raw) {
          try {
            const parsed = JSON.parse(item) as RawWindowEvent;
            allEvents.push(parsed);
          } catch {
            console.warn(`[Aggregator] Window item parse fail: ${item.slice(0, 100)}`);
          }
        }
      }

      const total = allEvents.length;
      console.log(`[Aggregator] Scoring tick: ${total}ta event window e ache`);

      if (total === 0) {
        console.log('[Aggregator] Window khali, scoring skip korchi');
        return;
      }

      // Sort by timestamp ascending before sending
      allEvents.sort((a, b) => a.timestamp - b.timestamp);

      // Score via AI Brain
      const signals = await this.scoringEngine.scoreWindowEvents(allEvents);

      if (signals.length === 0) {
        console.log('[Aggregator] Kono valid signal aseni, publish skip korchi');
        return;
      }

      // Publish each signal to stream:signals
      const pipeline = this.redis.pipeline() as ChainableCommander;
      for (const signal of signals) {
        pipeline.xadd(
          'stream:signals',
          '*',
          'id', signal.id,
          'timestamp', signal.timestamp,
          'Data_Source', signal.Data_Source,
          'Raw_Intel_Summary', signal.Raw_Intel_Summary,
          'Impact_Rating', String(signal.Impact_Rating),
          'Directional_Bias', signal.Directional_Bias,
        );
      }
      await pipeline.exec();

      // Broadcast to WebSocket clients
      for (const signal of signals) {
        this.broadcastToClients({ type: 'signal', data: signal });
      }

      console.log(
        `[Aggregator] ${this.wsClients.size}ta UI client ke signal pathalam`,
      );
    } catch (err) {
      console.error('[Aggregator] Scoring tick exception:', err);
    }
  }

  // ── WebSocket ────────────────────────────────────────────────────────────

  private setupWebSocket(): void {
    this.wss.on('connection', (ws: WebSocket, req: IncomingMessage) => {
      const ip = req.socket.remoteAddress ?? 'unknown';
      console.log(`[Aggregator] Naya WebSocket client connect: ${ip} (total: ${this.wsClients.size + 1})`);

      this.wsClients.add(ws);

      // Send welcome / immediate health on connect
      this.broadcastHealth()
        .then(() => undefined)
        .catch(() => undefined);

      ws.on('close', () => {
        this.wsClients.delete(ws);
        console.log(
          `[Aggregator] Client disconnect. Remaining: ${this.wsClients.size}`,
        );
      });

      ws.on('error', (err: Error) => {
        console.error('[Aggregator] WebSocket client error:', err.message);
        this.wsClients.delete(ws);
      });

      // Keep-alive ping
      ws.on('pong', () => {
        (ws as WebSocket & { isAlive?: boolean }).isAlive = true;
      });
    });

    // Ping all clients every 30s to detect stale connections
    const pingInterval = setInterval(() => {
      this.wsClients.forEach((ws) => {
        const extWs = ws as WebSocket & { isAlive?: boolean };
        if (extWs.isAlive === false) {
          extWs.terminate();
          this.wsClients.delete(ws);
          return;
        }
        extWs.isAlive = false;
        ws.ping();
      });
    }, 30_000);

    this.wss.on('close', () => clearInterval(pingInterval));
  }

  private broadcastToClients(message: WsBroadcast): void {
    const payload = JSON.stringify(message);
    for (const ws of this.wsClients) {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(payload, (err?: Error) => {
          if (err) {
            console.warn('[Aggregator] WS send error:', err.message);
            this.wsClients.delete(ws);
          }
        });
      } else {
        this.wsClients.delete(ws);
      }
    }
  }

  // ── Health Broadcast ─────────────────────────────────────────────────────

  private async broadcastHealth(): Promise<void> {
    try {
      const providers = await this.checkProviderHealth();
      const streams = await this.getStreamStats();

      const msg: HealthBroadcast = {
        type: 'health',
        data: {
          providers,
          streams,
          uptime_seconds: Math.floor((Date.now() - this.startTime) / 1000),
        },
      };

      this.broadcastToClients(msg);
    } catch (err) {
      console.error('[Aggregator] Health broadcast error:', err);
    }
  }

  private async checkProviderHealth(): Promise<ProviderHealth[]> {
    const aiBrainUrl =
      process.env['AI_BRAIN_URL'] ?? 'http://localhost:3000';

    const providers: ProviderHealth[] = [];

    // Check AI Brain gateway
    const start = Date.now();
    try {
      const { default: axios } = await import('axios');
      await axios.get(`${aiBrainUrl}/health`, { timeout: 3000 });
      providers.push({
        name: 'AI-Brain-Gateway',
        status: 'healthy',
        latency_ms: Date.now() - start,
        last_check: new Date().toISOString(),
      });
    } catch {
      providers.push({
        name: 'AI-Brain-Gateway',
        status: 'down',
        latency_ms: null,
        last_check: new Date().toISOString(),
      });
    }

    // Check Redis
    const redisStart = Date.now();
    try {
      const pong = await this.redis.ping();
      providers.push({
        name: 'Redis',
        status: pong === 'PONG' ? 'healthy' : 'degraded',
        latency_ms: Date.now() - redisStart,
        last_check: new Date().toISOString(),
      });
    } catch {
      providers.push({
        name: 'Redis',
        status: 'down',
        latency_ms: null,
        last_check: new Date().toISOString(),
      });
    }

    return providers;
  }

  private async getStreamStats(): Promise<StreamStats> {
    const stats: StreamStats = {};
    const nowMs = Date.now();
    const windowStart = nowMs - WINDOW_DURATION_MS;

    for (const stream of STREAMS) {
      const count = await this.redis.zcount(
        windowKey(stream),
        windowStart,
        nowMs,
      );

      // Get newest entry
      const newest = await this.redis.zrange(windowKey(stream), -1, -1, 'WITHSCORES');
      let lastTs: string | null = null;
      if (newest.length >= 2) {
        const score = newest[1];
        if (score !== undefined) {
          lastTs = new Date(parseInt(score, 10)).toISOString();
        }
      }

      stats[stream] = {
        events_in_window: count,
        last_event_ts: lastTs,
      };
    }

    return stats;
  }

  // ── Graceful Shutdown ────────────────────────────────────────────────────

  private setupSignals(): void {
    const shutdown = async (signal: string) => {
      if (this.isShuttingDown) return;
      this.isShuttingDown = true;

      console.log(`[Aggregator] ${signal} signal pailam, graceful shutdown shuru...`);

      // Stop stream loops
      for (const stream of STREAMS) {
        this.streamLoops.set(stream, false);
      }

      // Stop timers
      if (this.scoringTimer) clearInterval(this.scoringTimer);
      if (this.healthTimer) clearInterval(this.healthTimer);

      // Close WebSocket server
      await new Promise<void>((resolve) => {
        this.wss.close(() => resolve());
      });

      // Disconnect Redis
      await this.persistCursors();
      this.redis.disconnect();
      this.redisSub.disconnect();

      console.log('[Aggregator] Aggregator gracefully shutdown hoise');
      process.exit(0);
    };

    process.on('SIGTERM', () => shutdown('SIGTERM').catch(console.error));
    process.on('SIGINT', () => shutdown('SIGINT').catch(console.error));

    process.on('uncaughtException', (err) => {
      console.error('[Aggregator] Uncaught exception:', err);
      shutdown('uncaughtException').catch(() => process.exit(1));
    });

    process.on('unhandledRejection', (reason) => {
      console.error('[Aggregator] Unhandled rejection:', reason);
    });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Entry point
// ─────────────────────────────────────────────────────────────────────────────

const aggregator = new SignalAggregator();
aggregator.start().catch((err) => {
  console.error('[Aggregator] Fatal startup error:', err);
  process.exit(1);
});

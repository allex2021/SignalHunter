// =============================================================================
// HealthMonitor — Tracks live health of all AI providers
// Pings every 30s, persists state to Redis, emits status-change events
// =============================================================================

import { EventEmitter } from 'events';
import Redis from 'ioredis';
import {
  ProviderName,
  ProviderHealth,
  ProviderStatus,
  REDIS_KEYS,
  RateLimitError,
} from '@csa/shared-types';
import { pingGemini } from './providers/gemini';
import { pingGroq } from './providers/groq';
import { pingMistral } from './providers/mistral';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface ProviderConfig {
  name: ProviderName;
  apiKey: string;
  pingFn: (apiKey: string) => Promise<number>;
  priority: number; // Lower = higher priority
}

export interface HealthChangeEvent {
  provider: ProviderName;
  previousStatus: ProviderStatus;
  newStatus: ProviderStatus;
  health: ProviderHealth;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PING_INTERVAL_MS = 30_000;
const LATENCY_SMOOTHING_FACTOR = 0.2; // EMA alpha
const REDIS_TTL_SECONDS = 300;

// ---------------------------------------------------------------------------
// HealthMonitor class
// ---------------------------------------------------------------------------

export class HealthMonitor extends EventEmitter {
  private readonly healthMap: Map<ProviderName, ProviderHealth> = new Map();
  private readonly providers: ProviderConfig[];
  private pingIntervalId: ReturnType<typeof setInterval> | null = null;
  private readonly redis: Redis;
  private isRunning = false;

  constructor(redis: Redis) {
    super();
    this.redis = redis;

    const geminiKey = process.env.GEMINI_API_KEY ?? '';
    const groqKey = process.env.GROQ_API_KEY ?? '';
    const mistralKey = process.env.MISTRAL_API_KEY ?? '';

    this.providers = (
      [
        {
          name: 'gemini' as ProviderName,
          apiKey: geminiKey,
          pingFn: pingGemini,
          priority: 1,
        },
        {
          name: 'groq' as ProviderName,
          apiKey: groqKey,
          pingFn: pingGroq,
          priority: 2,
        },
        {
          name: 'mistral' as ProviderName,
          apiKey: mistralKey,
          pingFn: pingMistral,
          priority: 3,
        },
      ] as ProviderConfig[]
    ).filter((p) => p.apiKey.length > 0);

    // Initialize all configured providers as 'initializing'
    for (const p of this.providers) {
      this.healthMap.set(p.name, {
        name: p.name,
        status: 'initializing',
        lastChecked: new Date(),
        successCount: 0,
        failureCount: 0,
        averageLatencyMs: 0,
      });
    }

    console.log(
      `[HealthMonitor] Initialized — configured providers: ${this.providers.map((p) => p.name).join(', ')}`,
    );
  }

  // -------------------------------------------------------------------------
  // Start monitoring loop
  // -------------------------------------------------------------------------

  async start(): Promise<void> {
    if (this.isRunning) {
      console.warn('[HealthMonitor] Already running, duplicate start ignored');
      return;
    }

    this.isRunning = true;
    console.log('[HealthMonitor] Provider status check shuru hocche...');

    // Immediate first ping — don't wait 30s for initial health data
    await this.pingAllProviders();

    this.pingIntervalId = setInterval(async () => {
      await this.pingAllProviders();
    }, PING_INTERVAL_MS);
  }

  // -------------------------------------------------------------------------
  // Stop monitoring loop
  // -------------------------------------------------------------------------

  stop(): void {
    if (this.pingIntervalId) {
      clearInterval(this.pingIntervalId);
      this.pingIntervalId = null;
    }
    this.isRunning = false;
    console.log('[HealthMonitor] Stopped. Provider monitoring band hoise.');
  }

  // -------------------------------------------------------------------------
  // Ping all configured providers concurrently
  // -------------------------------------------------------------------------

  private async pingAllProviders(): Promise<void> {
    console.log(
      `[HealthMonitor] Provider status check hocche — ${new Date().toISOString()}`,
    );

    await Promise.allSettled(
      this.providers.map((p) => this.pingProvider(p)),
    );

    await this.persistToRedis();
  }

  // -------------------------------------------------------------------------
  // Ping a single provider and update health state
  // -------------------------------------------------------------------------

  private async pingProvider(config: ProviderConfig): Promise<void> {
    if (!config.apiKey) {
      console.warn(
        `[HealthMonitor] ${config.name} er API key nai, skip korchi`,
      );
      return;
    }

    const current = this.healthMap.get(config.name);
    const previousStatus = current?.status ?? 'initializing';

    try {
      const latencyMs = await config.pingFn(config.apiKey);

      const previous = this.healthMap.get(config.name) ?? {
        name: config.name,
        status: 'initializing' as ProviderStatus,
        lastChecked: new Date(),
        successCount: 0,
        failureCount: 0,
        averageLatencyMs: latencyMs,
      };

      // Exponential Moving Average for latency
      const newAvgLatency =
        previous.averageLatencyMs === 0
          ? latencyMs
          : LATENCY_SMOOTHING_FACTOR * latencyMs +
            (1 - LATENCY_SMOOTHING_FACTOR) * previous.averageLatencyMs;

      const updated: ProviderHealth = {
        ...previous,
        status: 'healthy',
        lastChecked: new Date(),
        successCount: previous.successCount + 1,
        averageLatencyMs: Math.round(newAvgLatency),
        rateLimitResetAt: undefined, // clear any previous rate limit
      };

      this.healthMap.set(config.name, updated);

      if (previousStatus !== 'healthy') {
        console.log(
          `[HealthMonitor] ${config.name} healthy hoye gese! Latency: ${latencyMs}ms`,
        );
        this.emitStatusChange(config.name, previousStatus, 'healthy', updated);
      } else {
        console.log(
          `[HealthMonitor] ${config.name} OK — latency: ${latencyMs}ms (avg: ${updated.averageLatencyMs}ms)`,
        );
      }
    } catch (err) {
      const previous = this.healthMap.get(config.name) ?? {
        name: config.name,
        status: 'initializing' as ProviderStatus,
        lastChecked: new Date(),
        successCount: 0,
        failureCount: 0,
        averageLatencyMs: 0,
      };

      let newStatus: ProviderStatus;
      let rateLimitResetAt: Date | undefined;

      if (err instanceof RateLimitError) {
        newStatus = 'rate_limited';
        rateLimitResetAt = new Date(Date.now() + err.retryAfterMs);
        console.warn(
          `[HealthMonitor] ${config.name} rate limited, ${err.retryAfterMs / 1000}s pore retry`,
        );
      } else {
        newStatus = 'error';
        console.error(
          `[HealthMonitor] ${config.name} error — ${String(err)}`,
        );
      }

      const updated: ProviderHealth = {
        ...previous,
        status: newStatus,
        lastChecked: new Date(),
        failureCount: previous.failureCount + 1,
        ...(rateLimitResetAt && { rateLimitResetAt }),
      };

      this.healthMap.set(config.name, updated);

      if (previousStatus !== newStatus) {
        this.emitStatusChange(config.name, previousStatus, newStatus, updated);
      }
    }
  }

  // -------------------------------------------------------------------------
  // Emit a status-change event
  // -------------------------------------------------------------------------

  private emitStatusChange(
    provider: ProviderName,
    previousStatus: ProviderStatus,
    newStatus: ProviderStatus,
    health: ProviderHealth,
  ): void {
    const event: HealthChangeEvent = {
      provider,
      previousStatus,
      newStatus,
      health,
    };
    this.emit('statusChange', event);

    if (newStatus === 'rate_limited') {
      console.warn(
        `[HealthMonitor] ${provider} rate limited, ${provider} theke switch korchi`,
      );
    } else if (newStatus === 'error') {
      console.error(
        `[HealthMonitor] ${provider} down! Failover provider e jao`,
      );
    } else if (newStatus === 'healthy' && previousStatus !== 'initializing') {
      console.log(
        `[HealthMonitor] ${provider} fire ashse — ${previousStatus} theke healthy`,
      );
    }
  }

  // -------------------------------------------------------------------------
  // Persist current health state to Redis
  // -------------------------------------------------------------------------

  private async persistToRedis(): Promise<void> {
    try {
      const healthData = Object.fromEntries(
        [...this.healthMap.entries()].map(([name, health]) => [
          name,
          JSON.stringify({
            ...health,
            lastChecked: health.lastChecked.toISOString(),
            rateLimitResetAt: health.rateLimitResetAt?.toISOString(),
          }),
        ]),
      );

      const pipeline = this.redis.pipeline();
      pipeline.hset(REDIS_KEYS.PROVIDER_HEALTH, healthData);
      pipeline.expire(REDIS_KEYS.PROVIDER_HEALTH, REDIS_TTL_SECONDS);
      await pipeline.exec();
    } catch (err) {
      console.error(
        `[HealthMonitor] Redis e persist korte parini — ${String(err)}`,
      );
    }
  }

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  /**
   * Returns all healthy providers, sorted by average latency (fastest first).
   */
  getHealthyProviders(): ProviderHealth[] {
    return [...this.healthMap.values()]
      .filter((h) => h.status === 'healthy')
      .sort((a, b) => a.averageLatencyMs - b.averageLatencyMs);
  }

  /**
   * Returns all providers and their health, sorted by priority.
   */
  getAllProviders(): ProviderHealth[] {
    return [...this.healthMap.values()].sort((a, b) => {
      const priorityMap: Record<ProviderName, number> = {
        gemini: 1,
        groq: 2,
        mistral: 3,
      };
      return priorityMap[a.name] - priorityMap[b.name];
    });
  }

  /**
   * Returns the health of a specific provider.
   */
  getProviderHealth(name: ProviderName): ProviderHealth | undefined {
    return this.healthMap.get(name);
  }

  /**
   * Manually marks a provider as rate-limited (called by FailoverRouter).
   */
  markRateLimited(name: ProviderName, retryAfterMs: number): void {
    const current = this.healthMap.get(name);
    if (!current) return;

    const updated: ProviderHealth = {
      ...current,
      status: 'rate_limited',
      rateLimitResetAt: new Date(Date.now() + retryAfterMs),
      lastChecked: new Date(),
    };

    this.healthMap.set(name, updated);
    console.warn(
      `[HealthMonitor] ${name} manually rate-limited, ${retryAfterMs / 1000}s band thakbe`,
    );

    this.emitStatusChange(name, current.status, 'rate_limited', updated);

    // Schedule automatic recovery after rate limit expires
    setTimeout(() => {
      const provider = this.providers.find((p) => p.name === name);
      if (provider) {
        void this.pingProvider(provider);
      }
    }, retryAfterMs + 1000);
  }

  /**
   * Returns true if at least one provider is healthy.
   */
  hasHealthyProvider(): boolean {
    return this.getHealthyProviders().length > 0;
  }

  /**
   * Returns the API key for a given provider name.
   */
  getApiKey(name: ProviderName): string {
    const provider = this.providers.find((p) => p.name === name);
    return provider?.apiKey ?? '';
  }
}

// =============================================================================
// FailoverRouter — Routes completions across AI providers with exponential backoff
// Priority: lowest latency first → rate-limit aware → exponential backoff retries
// =============================================================================

import {
  ChatCompletionRequest,
  ChatCompletionResponse,
  ProviderName,
  RateLimitError,
  ProviderError,
  AllProvidersDownError,
} from '@csa/shared-types';
import { HealthMonitor } from './health-monitor';
import { callGemini } from './providers/gemini';
import { callGroq } from './providers/groq';
import { callMistral } from './providers/mistral';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface RouteResult {
  response: ChatCompletionResponse;
  usedProvider: ProviderName;
  attemptCount: number;
  totalLatencyMs: number;
}

interface ProviderCallFn {
  (request: ChatCompletionRequest, apiKey: string): Promise<ChatCompletionResponse>;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const MAX_RETRIES_PER_PROVIDER = 2;
const BASE_BACKOFF_MS = 200;
const MAX_BACKOFF_MS = 8_000;
const JITTER_RANGE_MS = 100;

// ---------------------------------------------------------------------------
// Provider call registry
// ---------------------------------------------------------------------------

const PROVIDER_CALL_MAP: Record<ProviderName, ProviderCallFn> = {
  gemini: callGemini,
  groq: callGroq,
  mistral: callMistral,
};

// ---------------------------------------------------------------------------
// Helper — exponential backoff with jitter
// ---------------------------------------------------------------------------

async function backoffDelay(attempt: number): Promise<void> {
  const exponential = Math.min(
    BASE_BACKOFF_MS * Math.pow(2, attempt),
    MAX_BACKOFF_MS,
  );
  const jitter = Math.random() * JITTER_RANGE_MS;
  const delay = Math.round(exponential + jitter);
  console.log(`[FailoverRouter] Backoff: ${delay}ms wait korchi (attempt ${attempt + 1})`);
  return new Promise((resolve) => setTimeout(resolve, delay));
}

// ---------------------------------------------------------------------------
// FailoverRouter class
// ---------------------------------------------------------------------------

export class FailoverRouter {
  private readonly healthMonitor: HealthMonitor;

  constructor(healthMonitor: HealthMonitor) {
    this.healthMonitor = healthMonitor;
  }

  // -------------------------------------------------------------------------
  // Main routing method
  // -------------------------------------------------------------------------

  async route(request: ChatCompletionRequest): Promise<RouteResult> {
    const startTime = Date.now();
    let attemptCount = 0;

    // Get providers sorted by latency (fastest = highest priority)
    const healthyProviders = this.healthMonitor.getHealthyProviders();

    if (healthyProviders.length === 0) {
      console.error(
        '[FailoverRouter] Sob provider down! Emergency backup mode e achi — 503 dibo',
      );
      throw new AllProvidersDownError();
    }

    const providerOrder = healthyProviders.map((h) => h.name);
    console.log(
      `[FailoverRouter] Provider order: [${providerOrder.join(' → ')}]`,
    );

    const errors: Array<{ provider: ProviderName; error: string }> = [];

    // Try each provider in order
    for (const providerName of providerOrder) {
      const apiKey = this.healthMonitor.getApiKey(providerName);

      if (!apiKey) {
        console.warn(
          `[FailoverRouter] ${providerName} er key nai, skip`,
        );
        continue;
      }

      const callFn = PROVIDER_CALL_MAP[providerName];

      // Retry with exponential backoff for transient errors
      for (let attempt = 0; attempt < MAX_RETRIES_PER_PROVIDER; attempt++) {
        attemptCount++;

        if (attempt > 0) {
          await backoffDelay(attempt);
        }

        try {
          console.log(
            `[FailoverRouter] Failover kori, provider: ${providerName} (attempt ${attempt + 1}/${MAX_RETRIES_PER_PROVIDER})`,
          );

          const response = await callFn(request, apiKey);
          const totalLatencyMs = Date.now() - startTime;

          console.log(
            `[FailoverRouter] Success! Provider: ${providerName}, total latency: ${totalLatencyMs}ms, attempts: ${attemptCount}`,
          );

          return {
            response,
            usedProvider: providerName,
            attemptCount,
            totalLatencyMs,
          };
        } catch (err) {
          if (err instanceof RateLimitError) {
            // Rate limit — mark provider and immediately move to next provider
            console.warn(
              `[FailoverRouter] ${providerName} rate limited! Next provider e jao. RetryAfter: ${err.retryAfterMs}ms`,
            );

            this.healthMonitor.markRateLimited(providerName, err.retryAfterMs);

            errors.push({
              provider: providerName,
              error: `RateLimited (retry after ${err.retryAfterMs}ms)`,
            });

            // Break inner loop — don't retry same provider when rate limited
            break;
          } else if (err instanceof ProviderError) {
            const isLastAttempt = attempt === MAX_RETRIES_PER_PROVIDER - 1;

            console.error(
              `[FailoverRouter] ${providerName} provider error: ${err.message}, status: ${err.statusCode ?? 'N/A'} (attempt ${attempt + 1})`,
            );

            errors.push({
              provider: providerName,
              error: `ProviderError[${err.statusCode ?? '?'}]: ${err.message}`,
            });

            // 4xx errors are not retryable — move to next provider immediately
            if (err.statusCode && err.statusCode >= 400 && err.statusCode < 500) {
              console.warn(
                `[FailoverRouter] ${providerName} 4xx error, retry nai — next provider`,
              );
              break;
            }

            if (isLastAttempt) break;
          } else {
            // Unknown error
            console.error(
              `[FailoverRouter] ${providerName} unknown error: ${String(err)} (attempt ${attempt + 1})`,
            );

            errors.push({
              provider: providerName,
              error: `UnknownError: ${String(err)}`,
            });

            if (attempt === MAX_RETRIES_PER_PROVIDER - 1) break;
          }
        }
      }
    }

    // All providers exhausted
    const totalLatencyMs = Date.now() - startTime;
    console.error(
      `[FailoverRouter] Sob provider fail korlo! ${attemptCount} attempts, ${totalLatencyMs}ms. Errors: ${JSON.stringify(errors)}`,
    );

    throw new AllProvidersDownError();
  }

  // -------------------------------------------------------------------------
  // Convenience wrapper — route with specific provider override
  // -------------------------------------------------------------------------

  async routeToProvider(
    request: ChatCompletionRequest,
    preferredProvider: ProviderName,
  ): Promise<RouteResult> {
    const startTime = Date.now();
    const apiKey = this.healthMonitor.getApiKey(preferredProvider);

    if (!apiKey) {
      throw new ProviderError(
        preferredProvider,
        `API key not configured for ${preferredProvider}`,
      );
    }

    const health = this.healthMonitor.getProviderHealth(preferredProvider);

    if (!health || health.status !== 'healthy') {
      console.warn(
        `[FailoverRouter] Preferred provider ${preferredProvider} healthy na (status: ${health?.status}), generic route e jabo`,
      );
      return this.route(request);
    }

    const callFn = PROVIDER_CALL_MAP[preferredProvider];

    try {
      const response = await callFn(request, apiKey);
      return {
        response,
        usedProvider: preferredProvider,
        attemptCount: 1,
        totalLatencyMs: Date.now() - startTime,
      };
    } catch (err) {
      console.warn(
        `[FailoverRouter] Preferred provider ${preferredProvider} failed, generic failover e jabo`,
      );
      return this.route(request);
    }
  }
}

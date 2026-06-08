// =============================================================================
// AI Brain Gateway — Express server exposing OpenAI-compatible API
// Routes: POST /v1/chat/completions | GET /health | GET /providers
// =============================================================================

import 'dotenv/config';
import express, {
  Request,
  Response,
  NextFunction,
  RequestHandler,
} from 'express';
import Redis from 'ioredis';
import { v4 as uuidv4 } from 'uuid';
import {
  ChatCompletionRequest,
  AllProvidersDownError,
  RateLimitError,
  ProviderError,
} from '@csa/shared-types';
import { HealthMonitor } from './health-monitor';
import { FailoverRouter } from './failover-router';

// ---------------------------------------------------------------------------
// Environment config
// ---------------------------------------------------------------------------

const PORT = parseInt(process.env.PORT ?? '3000', 10);
const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379';
const NODE_ENV = process.env.NODE_ENV ?? 'development';
const API_SECRET = process.env.AI_BRAIN_API_SECRET ?? '';

// ---------------------------------------------------------------------------
// Validate required environment variables
// ---------------------------------------------------------------------------

function validateEnv(): void {
  const required = ['GEMINI_API_KEY', 'GROQ_API_KEY', 'MISTRAL_API_KEY'];
  const atLeastOne = required.filter((k) => process.env[k]?.trim());

  if (atLeastOne.length === 0) {
    throw new Error(
      `[Gateway] Kono AI API key nai! Set at least one of: ${required.join(', ')}`,
    );
  }

  console.log(
    `[Gateway] Configured AI providers: ${atLeastOne.map((k) => k.replace('_API_KEY', '').toLowerCase()).join(', ')}`,
  );
}

// ---------------------------------------------------------------------------
// Redis factory with reconnection logic
// ---------------------------------------------------------------------------

function createRedis(): Redis {
  const redis = new Redis(REDIS_URL, {
    maxRetriesPerRequest: 3,
    enableReadyCheck: true,
    retryStrategy(times) {
      if (times > 10) {
        console.error(
          '[Gateway] Redis connection 10+ bar fail — give up kori',
        );
        return null; // Stop retrying
      }
      const delay = Math.min(times * 500, 5000);
      console.warn(`[Gateway] Redis reconnect korchi — attempt ${times}, delay: ${delay}ms`);
      return delay;
    },
  });

  redis.on('connect', () => {
    console.log('[Gateway] Redis connected — data pipeline ready!');
  });

  redis.on('error', (err: Error) => {
    console.error(`[Gateway] Redis error: ${err.message}`);
  });

  redis.on('close', () => {
    console.warn('[Gateway] Redis connection closed');
  });

  return redis;
}

// ---------------------------------------------------------------------------
// Request logging middleware
// ---------------------------------------------------------------------------

const requestLogger: RequestHandler = (req, res, next) => {
  const requestId = uuidv4().slice(0, 8);
  const start = Date.now();

  (req as Request & { requestId: string }).requestId = requestId;

  res.on('finish', () => {
    const latencyMs = Date.now() - start;
    const colorCode =
      res.statusCode >= 500
        ? '\x1b[31m' // red
        : res.statusCode >= 400
          ? '\x1b[33m' // yellow
          : '\x1b[32m'; // green
    const reset = '\x1b[0m';

    console.log(
      `${colorCode}[${requestId}] ${req.method} ${req.path} → ${res.statusCode} (${latencyMs}ms)${reset}`,
    );
  });

  next();
};

// ---------------------------------------------------------------------------
// Optional API key authentication middleware
// ---------------------------------------------------------------------------

const authMiddleware: RequestHandler = (req, res, next) => {
  // Skip auth if no secret is configured
  if (!API_SECRET) {
    next();
    return;
  }

  const authHeader = req.headers.authorization ?? '';
  const token = authHeader.startsWith('Bearer ')
    ? authHeader.slice(7)
    : authHeader;

  if (token !== API_SECRET) {
    res.status(401).json({
      error: {
        type: 'authentication_error',
        message: 'Invalid API key',
        code: 401,
      },
    });
    return;
  }

  next();
};

// ---------------------------------------------------------------------------
// Bootstrap and start the gateway
// ---------------------------------------------------------------------------

async function bootstrap(): Promise<void> {
  validateEnv();

  const redis = createRedis();
  const healthMonitor = new HealthMonitor(redis);
  const failoverRouter = new FailoverRouter(healthMonitor);

  // Listen for health status changes and log them
  healthMonitor.on('statusChange', (event) => {
    const { provider, previousStatus, newStatus } = event;

    if (newStatus === 'rate_limited') {
      console.warn(
        `[Gateway] ${provider} rate limited hoise — ${previousStatus} → ${newStatus}`,
      );
    } else if (newStatus === 'error') {
      console.error(
        `[Gateway] ${provider} error state e gese — ${previousStatus} → ${newStatus}`,
      );
    } else if (newStatus === 'healthy') {
      console.log(
        `[Gateway] ${provider} healthy hoilo — ${previousStatus} → ${newStatus}`,
      );
    }
  });

  // Start health monitoring
  await healthMonitor.start();

  // ---------------------------------------------------------------------------
  // Express app setup
  // ---------------------------------------------------------------------------

  const app = express();

  app.use(express.json({ limit: '10mb' }));
  app.use(requestLogger);

  // ---------------------------------------------------------------------------
  // GET /health — lightweight liveness check
  // ---------------------------------------------------------------------------

  app.get('/health', (_req: Request, res: Response) => {
    const healthyProviders = healthMonitor.getHealthyProviders();
    const isHealthy = healthyProviders.length > 0;

    res.status(isHealthy ? 200 : 503).json({
      status: isHealthy ? 'ok' : 'degraded',
      timestamp: new Date().toISOString(),
      healthyProviders: healthyProviders.map((h) => h.name),
      totalProviders: healthMonitor.getAllProviders().length,
    });
  });

  // ---------------------------------------------------------------------------
  // GET /providers — detailed provider health dashboard
  // ---------------------------------------------------------------------------

  app.get('/providers', (_req: Request, res: Response) => {
    const allProviders = healthMonitor.getAllProviders();

    res.json({
      timestamp: new Date().toISOString(),
      providers: allProviders.map((h) => ({
        name: h.name,
        status: h.status,
        lastChecked: h.lastChecked,
        successCount: h.successCount,
        failureCount: h.failureCount,
        successRate:
          h.successCount + h.failureCount > 0
            ? (
                (h.successCount / (h.successCount + h.failureCount)) *
                100
              ).toFixed(1) + '%'
            : 'N/A',
        averageLatencyMs: h.averageLatencyMs,
        rateLimitResetAt: h.rateLimitResetAt ?? null,
      })),
    });
  });

  // ---------------------------------------------------------------------------
  // POST /v1/chat/completions — OpenAI-compatible completions endpoint
  // ---------------------------------------------------------------------------

  app.post(
    '/v1/chat/completions',
    authMiddleware,
    async (req: Request, res: Response) => {
      const body = req.body as ChatCompletionRequest;

      // Validate request body
      if (!body || !Array.isArray(body.messages) || body.messages.length === 0) {
        res.status(400).json({
          error: {
            type: 'invalid_request_error',
            message: 'messages field is required and must be a non-empty array',
            code: 400,
          },
        });
        return;
      }

      // Validate message structure
      for (const msg of body.messages) {
        if (!msg.role || !msg.content) {
          res.status(400).json({
            error: {
              type: 'invalid_request_error',
              message: 'Each message must have role and content fields',
              code: 400,
            },
          });
          return;
        }
      }

      try {
        const result = await failoverRouter.route(body);

        // Add routing metadata to response headers for observability
        res.setHeader('X-Provider-Used', result.usedProvider);
        res.setHeader('X-Attempt-Count', String(result.attemptCount));
        res.setHeader('X-Total-Latency-Ms', String(result.totalLatencyMs));

        res.json(result.response);
      } catch (err) {
        if (err instanceof AllProvidersDownError) {
          res.status(503).json({
            error: {
              type: 'service_unavailable',
              message:
                'All AI providers are currently unavailable. Please retry shortly.',
              code: 503,
              providers: healthMonitor.getAllProviders().map((h) => ({
                name: h.name,
                status: h.status,
              })),
            },
          });
          return;
        }

        if (err instanceof RateLimitError) {
          res.status(429).json({
            error: {
              type: 'rate_limit_error',
              message: `Provider ${err.provider} is rate limited`,
              code: 429,
              retryAfterMs: err.retryAfterMs,
            },
          });
          return;
        }

        if (err instanceof ProviderError) {
          res.status(502).json({
            error: {
              type: 'provider_error',
              message: err.message,
              code: 502,
              provider: err.provider,
            },
          });
          return;
        }

        // Unknown error
        console.error(`[Gateway] Unexpected error: ${String(err)}`);
        res.status(500).json({
          error: {
            type: 'internal_error',
            message: 'An unexpected error occurred',
            code: 500,
          },
        });
      }
    },
  );

  // ---------------------------------------------------------------------------
  // 404 handler
  // ---------------------------------------------------------------------------

  app.use((_req: Request, res: Response) => {
    res.status(404).json({
      error: {
        type: 'not_found',
        message: 'Endpoint khuje pailam na',
        code: 404,
      },
    });
  });

  // ---------------------------------------------------------------------------
  // Global error handler
  // ---------------------------------------------------------------------------

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
    console.error(`[Gateway] Unhandled error: ${err.message}\n${err.stack ?? ''}`);
    res.status(500).json({
      error: {
        type: 'internal_error',
        message: NODE_ENV === 'production' ? 'Internal server error' : err.message,
        code: 500,
      },
    });
  });

  // ---------------------------------------------------------------------------
  // Start listening
  // ---------------------------------------------------------------------------

  const server = app.listen(PORT, () => {
    console.log(
      `\n🧠 AI Brain Gateway chalu holo! Port: ${PORT}`,
    );
    console.log(`   Environment: ${NODE_ENV}`);
    console.log(`   Redis: ${REDIS_URL}`);
    console.log(`   Auth: ${API_SECRET ? 'enabled' : 'disabled (set AI_BRAIN_API_SECRET)'}`);
    console.log(`   Endpoints:`);
    console.log(`     POST http://localhost:${PORT}/v1/chat/completions`);
    console.log(`     GET  http://localhost:${PORT}/health`);
    console.log(`     GET  http://localhost:${PORT}/providers\n`);
  });

  // ---------------------------------------------------------------------------
  // Graceful shutdown
  // ---------------------------------------------------------------------------

  const gracefulShutdown = async (signal: string): Promise<void> => {
    console.log(
      `\n[Gateway] ${signal} signal pailam — graceful shutdown shuru hocche...`,
    );

    healthMonitor.stop();

    await new Promise<void>((resolve, reject) => {
      server.close((err) => {
        if (err) {
          console.error(`[Gateway] Server close error: ${err.message}`);
          reject(err);
        } else {
          console.log('[Gateway] HTTP server closed');
          resolve();
        }
      });
    });

    try {
      await redis.quit();
      console.log('[Gateway] Redis connection closed');
    } catch (err) {
      console.error(`[Gateway] Redis quit error: ${String(err)}`);
    }

    console.log('[Gateway] Shutdown complete. Allah hafez!');
    process.exit(0);
  };

  process.on('SIGTERM', () => void gracefulShutdown('SIGTERM'));
  process.on('SIGINT', () => void gracefulShutdown('SIGINT'));

  // Catch unhandled promise rejections
  process.on('unhandledRejection', (reason, promise) => {
    console.error(
      `[Gateway] Unhandled Rejection at: ${String(promise)}, reason: ${String(reason)}`,
    );
  });

  // Catch uncaught exceptions
  process.on('uncaughtException', (err) => {
    console.error(
      `[Gateway] Uncaught Exception: ${err.message}\n${err.stack ?? ''}`,
    );
    void gracefulShutdown('uncaughtException');
  });
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

bootstrap().catch((err: Error) => {
  console.error(`[Gateway] Bootstrap fail! ${err.message}\n${err.stack ?? ''}`);
  process.exit(1);
});

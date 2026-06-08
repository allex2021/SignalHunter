import 'dotenv/config';
import axios, { AxiosResponse, AxiosError } from 'axios';
import Redis from 'ioredis';
import { v4 as uuidv4 } from 'uuid';
import * as fs from 'fs';
import * as path from 'path';

// ─────────────────────────────────────────────────────────────────────────────
// Environment & Config
// ─────────────────────────────────────────────────────────────────────────────

const GITHUB_TOKEN = process.env.GITHUB_TOKEN || '';
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS || '60000', 10); // 60s default
const STAGGER_DELAY_MS = parseInt(process.env.STAGGER_DELAY_MS || '500', 10); // 500ms between repos
const WHALE_THRESHOLD_COMMITS = parseInt(process.env.WHALE_THRESHOLD_COMMITS || '5', 10);
const RATE_LIMIT_BUFFER = 50; // Pause when remaining < 50
const REDIS_STREAM_KEY = 'stream:github';
const MAX_STREAM_LENGTH = 100_000; // MAXLEN for XADD
const REPOS_FILE = path.join(__dirname, 'web3-repos.json');

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

type SemanticBias = 'bullish' | 'bearish' | 'volatile' | 'neutral';

interface GitHubEvent {
  id: string;
  type: string;
  actor: { login: string; display_login?: string };
  repo: { name: string; url: string };
  payload: Record<string, unknown>;
  created_at: string;
  public: boolean;
}

interface GitHubRateLimit {
  remaining: number;
  limit: number;
  resetAt: number; // Unix timestamp
}

interface GitHubSignalEntry {
  id: string;
  repo: string;
  eventType: string;
  commitMessage: string;
  actor: string;
  timestamp: string;
  url: string;
  semanticScore: number;
  bias: SemanticBias;
  eventId: string;
}

interface ETagCache {
  [repo: string]: string;
}

// ─────────────────────────────────────────────────────────────────────────────
// Semantic Classification Engine
// ─────────────────────────────────────────────────────────────────────────────

const BULLISH_KEYWORDS = [
  'mainnet',
  'launch',
  'v2',
  'v3',
  'upgrade',
  'optimization',
  'optimized',
  'audit passed',
  'audit complete',
  'integration',
  'partnership',
  'deploy',
  'release',
  'performance',
  'feature complete',
  'milestone',
  'testnet',
  'live',
  'production',
  'stable',
  'merge',
  'ship',
  'shipped',
  'new feature',
  'improvement',
  'enhanced',
];

const BEARISH_KEYWORDS = [
  'vulnerability',
  'critical',
  'exploit',
  'exploited',
  'pause',
  'paused',
  'emergency',
  'hotfix',
  'revert',
  'reverted',
  'deprecat',
  'deprecated',
  'remove',
  'removed',
  'security patch',
  'security fix',
  'incident',
  'breach',
  'hack',
  'rug',
  'drain',
  'attack',
  'bug fix',
  'regression',
];

const VOLATILITY_KEYWORDS = [
  'breaking change',
  'breaking-change',
  'migration',
  'fork',
  'hard fork',
  'major update',
  'overhaul',
  'redesign',
  'restructure',
  'refactor',
  'rearchitect',
  'breaking',
  'incompatible',
  'deprecated api',
  'sunset',
];

/**
 * Classifies a text string semantically and returns score + bias.
 * Score: positive = bullish, negative = bearish, magnitude = confidence.
 */
function classifySemantics(text: string): { score: number; bias: SemanticBias } {
  const lower = text.toLowerCase();
  let score = 0;

  for (const kw of BULLISH_KEYWORDS) {
    if (lower.includes(kw)) {
      score += 1;
    }
  }
  for (const kw of BEARISH_KEYWORDS) {
    if (lower.includes(kw)) {
      score -= 2; // Bearish signals weighted heavier
    }
  }

  let volatilityHit = false;
  for (const kw of VOLATILITY_KEYWORDS) {
    if (lower.includes(kw)) {
      volatilityHit = true;
      score += 0; // Volatility is neutral on direction
    }
  }

  let bias: SemanticBias;
  if (volatilityHit && score === 0) {
    bias = 'volatile';
  } else if (score > 0) {
    bias = 'bullish';
  } else if (score < 0) {
    bias = 'bearish';
  } else {
    bias = 'neutral';
  }

  return { score, bias };
}

// ─────────────────────────────────────────────────────────────────────────────
// Rate Limit Tracker
// ─────────────────────────────────────────────────────────────────────────────

class RateLimitTracker {
  private remaining: number = 5000;
  private limit: number = 5000;
  private resetAt: number = Date.now() + 3600_000;

  update(headers: Record<string, string | undefined>): void {
    const remaining = headers['x-ratelimit-remaining'];
    const limit = headers['x-ratelimit-limit'];
    const reset = headers['x-ratelimit-reset'];

    if (remaining !== undefined) this.remaining = parseInt(remaining, 10);
    if (limit !== undefined) this.limit = parseInt(limit, 10);
    if (reset !== undefined) this.resetAt = parseInt(reset, 10) * 1000;
  }

  get isExhausted(): boolean {
    return this.remaining < RATE_LIMIT_BUFFER;
  }

  get msUntilReset(): number {
    return Math.max(0, this.resetAt - Date.now());
  }

  get status(): GitHubRateLimit {
    return { remaining: this.remaining, limit: this.limit, resetAt: this.resetAt };
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Exponential Backoff Utility
// ─────────────────────────────────────────────────────────────────────────────

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function withExponentialBackoff<T>(
  fn: () => Promise<T>,
  maxRetries: number = 5,
  baseDelayMs: number = 1000
): Promise<T> {
  let lastError: Error = new Error('Unknown error');
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastError = err as Error;
      const delay = baseDelayMs * Math.pow(2, attempt) + Math.random() * 500;
      console.warn(
        `[BACKOFF] Attempt ${attempt + 1}/${maxRetries} fail hoise, ${Math.round(delay)}ms wait korchi... Error: ${lastError.message}`
      );
      await sleep(delay);
    }
  }
  throw lastError;
}

// ─────────────────────────────────────────────────────────────────────────────
// GitHub API Client
// ─────────────────────────────────────────────────────────────────────────────

class GitHubApiClient {
  private readonly baseUrl = 'https://api.github.com';
  private readonly headers: Record<string, string>;
  private readonly rateLimit: RateLimitTracker;

  constructor(token: string) {
    this.headers = {
      Accept: 'application/vnd.github+json',
      'X-GitHub-Api-Version': '2022-11-28',
      'User-Agent': 'CSA-GitHubRecon/1.0.0',
    };
    if (token) {
      this.headers['Authorization'] = `Bearer ${token}`;
      console.log('[AUTH] GitHub token diye authenticated request pathano hobe (5000/hr rate limit)');
    } else {
      console.warn('[AUTH] GitHub token nai! Anonymous mode - matro 60/hr rate limit paabo');
    }
    this.rateLimit = new RateLimitTracker();
  }

  async fetchEvents(
    repo: string,
    etag?: string
  ): Promise<{ events: GitHubEvent[]; etag?: string; notModified: boolean }> {
    if (this.rateLimit.isExhausted) {
      const waitMs = this.rateLimit.msUntilReset;
      console.warn(
        `[RATE_LIMIT] Rate limit exhausted! ${Math.ceil(waitMs / 60000)} minute wait korchi reset er jonno...`
      );
      await sleep(waitMs + 1000);
    }

    const url = `${this.baseUrl}/repos/${repo}/events?per_page=30`;
    const requestHeaders = { ...this.headers };
    if (etag) {
      requestHeaders['If-None-Match'] = etag;
    }

    const response: AxiosResponse<GitHubEvent[]> = await withExponentialBackoff(
      async () => {
        try {
          return await axios.get<GitHubEvent[]>(url, {
            headers: requestHeaders,
            timeout: 15_000,
            validateStatus: (status) => status === 200 || status === 304,
          });
        } catch (err) {
          const axiosErr = err as AxiosError;
          if (axiosErr.response?.status === 403 || axiosErr.response?.status === 429) {
            const retryAfter = axiosErr.response.headers['retry-after'];
            const waitMs = retryAfter ? parseInt(retryAfter, 10) * 1000 : 60_000;
            console.warn(`[RATE_LIMIT] 403/429 paisi! ${Math.ceil(waitMs / 1000)}s wait korchi`);
            await sleep(waitMs);
          }
          throw err;
        }
      },
      5,
      2000
    );

    // Update rate limit state from response headers
    this.rateLimit.update({
      'x-ratelimit-remaining': response.headers['x-ratelimit-remaining'],
      'x-ratelimit-limit': response.headers['x-ratelimit-limit'],
      'x-ratelimit-reset': response.headers['x-ratelimit-reset'],
    });

    if (response.status === 304) {
      return { events: [], etag, notModified: true };
    }

    const newEtag = response.headers['etag'] as string | undefined;
    return { events: response.data || [], etag: newEtag, notModified: false };
  }

  getRateLimitStatus(): GitHubRateLimit {
    return this.rateLimit.status;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Event Processors
// ─────────────────────────────────────────────────────────────────────────────

function processPushEvent(
  event: GitHubEvent,
  repo: string
): GitHubSignalEntry[] {
  const payload = event.payload as {
    commits?: Array<{ message: string; url: string; sha: string }>;
    ref?: string;
  };

  const commits = payload.commits || [];
  const entries: GitHubSignalEntry[] = [];

  for (const commit of commits) {
    const message = commit.message.split('\n')[0].trim(); // First line only
    const { score, bias } = classifySemantics(message);

    if (bias === 'bearish') {
      console.warn(`[BEARISH] ${repo} te bearish signal detect hoise: "${message}"`);
    } else if (bias === 'bullish') {
      console.info(`[BULLISH] ${repo} te bullish signal: "${message}"`);
    }

    entries.push({
      id: uuidv4(),
      repo,
      eventType: 'PushEvent',
      commitMessage: message,
      actor: event.actor.login,
      timestamp: event.created_at,
      url: `https://github.com/${repo}/commit/${commit.sha}`,
      semanticScore: score,
      bias,
      eventId: event.id,
    });

    console.info(`[PUSH] ${repo} te naya commit: "${message}" by ${event.actor.login} [${bias}]`);
  }

  return entries;
}

function processReleaseEvent(event: GitHubEvent, repo: string): GitHubSignalEntry {
  const payload = event.payload as {
    action?: string;
    release?: {
      tag_name?: string;
      name?: string;
      html_url?: string;
      body?: string;
      prerelease?: boolean;
    };
  };

  const release = payload.release || {};
  const isPrerelease = release.prerelease || false;
  const releaseName = release.name || release.tag_name || 'unknown';
  const description = release.body || '';
  const { score, bias } = classifySemantics(`${releaseName} ${description}`);

  console.info(
    `[RELEASE] ${repo} te naya release: ${releaseName} (prerelease=${isPrerelease}) [${bias}]`
  );

  return {
    id: uuidv4(),
    repo,
    eventType: 'ReleaseEvent',
    commitMessage: `Release: ${releaseName}${isPrerelease ? ' (prerelease)' : ''}`,
    actor: event.actor.login,
    timestamp: event.created_at,
    url: release.html_url || `https://github.com/${repo}/releases`,
    semanticScore: isPrerelease ? score : score + 2, // Official releases get bonus score
    bias: isPrerelease ? bias : 'bullish',
    eventId: event.id,
  };
}

function processCreateEvent(event: GitHubEvent, repo: string): GitHubSignalEntry {
  const payload = event.payload as {
    ref_type?: string;
    ref?: string;
    description?: string;
  };

  const refType = payload.ref_type || 'unknown';
  const ref = payload.ref || 'unknown';
  const message = `Created ${refType}: ${ref}`;
  const { score, bias } = classifySemantics(`${ref} ${payload.description || ''}`);

  console.info(`[CREATE] ${repo} te naya ${refType} create hoise: ${ref}`);

  return {
    id: uuidv4(),
    repo,
    eventType: 'CreateEvent',
    commitMessage: message,
    actor: event.actor.login,
    timestamp: event.created_at,
    url: `https://github.com/${repo}/tree/${ref}`,
    semanticScore: score,
    bias,
    eventId: event.id,
  };
}

function processDeleteEvent(event: GitHubEvent, repo: string): GitHubSignalEntry {
  const payload = event.payload as { ref_type?: string; ref?: string };
  const refType = payload.ref_type || 'unknown';
  const ref = payload.ref || 'unknown';
  const message = `Deleted ${refType}: ${ref}`;

  // Deletion is inherently slightly bearish — removing features/branches
  const { score, bias } = classifySemantics(ref);
  const adjustedBias: SemanticBias = score >= 0 ? 'neutral' : 'bearish';

  console.warn(`[DELETE] ${repo} te ${refType} delete hoise: ${ref} — possible bearish signal`);

  return {
    id: uuidv4(),
    repo,
    eventType: 'DeleteEvent',
    commitMessage: message,
    actor: event.actor.login,
    timestamp: event.created_at,
    url: `https://github.com/${repo}`,
    semanticScore: score - 1, // Slight bearish penalty for deletions
    bias: adjustedBias,
    eventId: event.id,
  };
}

function processPullRequestEvent(event: GitHubEvent, repo: string): GitHubSignalEntry | null {
  const payload = event.payload as {
    action?: string;
    pull_request?: {
      title?: string;
      body?: string;
      html_url?: string;
      merged?: boolean;
      number?: number;
    };
  };

  // Only care about merged PRs
  if (payload.action !== 'closed' || !payload.pull_request?.merged) {
    return null;
  }

  const pr = payload.pull_request;
  const title = pr.title || 'Unknown PR';
  const body = pr.body || '';
  const { score, bias } = classifySemantics(`${title} ${body}`);

  console.info(`[PR_MERGED] ${repo} te PR #${pr.number} merge hoise: "${title}" [${bias}]`);

  return {
    id: uuidv4(),
    repo,
    eventType: 'PullRequestMergedEvent',
    commitMessage: `Merged PR #${pr.number}: ${title}`,
    actor: event.actor.login,
    timestamp: event.created_at,
    url: pr.html_url || `https://github.com/${repo}/pulls`,
    semanticScore: score + 1, // Merges are positive signals
    bias: score >= 0 ? 'bullish' : bias,
    eventId: event.id,
  };
}

function processWatchEvent(event: GitHubEvent, repo: string): GitHubSignalEntry {
  return {
    id: uuidv4(),
    repo,
    eventType: 'WatchEvent',
    commitMessage: `${event.actor.login} starred ${repo}`,
    actor: event.actor.login,
    timestamp: event.created_at,
    url: `https://github.com/${repo}`,
    semanticScore: 0,
    bias: 'neutral',
    eventId: event.id,
  };
}

function processEvent(event: GitHubEvent, repo: string): GitHubSignalEntry[] {
  try {
    switch (event.type) {
      case 'PushEvent':
        return processPushEvent(event, repo);
      case 'ReleaseEvent':
        return [processReleaseEvent(event, repo)];
      case 'CreateEvent':
        return [processCreateEvent(event, repo)];
      case 'DeleteEvent':
        return [processDeleteEvent(event, repo)];
      case 'PullRequestEvent': {
        const entry = processPullRequestEvent(event, repo);
        return entry ? [entry] : [];
      }
      case 'WatchEvent':
        return [processWatchEvent(event, repo)];
      default:
        return [];
    }
  } catch (err) {
    console.error(`[ERROR] Event process korte problem hoise (${event.type} on ${repo}): ${(err as Error).message}`);
    return [];
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Redis Publisher
// ─────────────────────────────────────────────────────────────────────────────

class RedisPublisher {
  private readonly client: Redis;
  private readonly streamKey: string;
  private isConnected: boolean = false;

  constructor(url: string, streamKey: string) {
    this.client = new Redis(url, {
      maxRetriesPerRequest: 3,
      enableReadyCheck: true,
      lazyConnect: true,
      retryStrategy: (times: number) => {
        const delay = Math.min(times * 500, 10_000);
        console.warn(`[REDIS] Reconnect attempt ${times}, ${delay}ms delay`);
        return delay;
      },
    });
    this.streamKey = streamKey;

    this.client.on('connect', () => {
      this.isConnected = true;
      console.info('[REDIS] Redis connection established — stream publisher ready');
    });
    this.client.on('error', (err) => {
      this.isConnected = false;
      console.error(`[REDIS] Redis error: ${err.message}`);
    });
    this.client.on('close', () => {
      this.isConnected = false;
      console.warn('[REDIS] Redis connection closed');
    });
  }

  async connect(): Promise<void> {
    await this.client.connect();
  }

  async publishSignal(entry: GitHubSignalEntry): Promise<string> {
    if (!this.isConnected) {
      throw new Error('Redis not connected');
    }

    const streamId = await this.client.xadd(
      this.streamKey,
      'MAXLEN',
      '~',
      MAX_STREAM_LENGTH.toString(),
      '*',
      'id', entry.id,
      'repo', entry.repo,
      'eventType', entry.eventType,
      'commitMessage', entry.commitMessage.substring(0, 500), // Cap message length
      'actor', entry.actor,
      'timestamp', entry.timestamp,
      'url', entry.url,
      'semanticScore', entry.semanticScore.toString(),
      'bias', entry.bias,
      'eventId', entry.eventId,
      'ingestedAt', new Date().toISOString()
    );

    return streamId || 'unknown';
  }

  async publishBatch(entries: GitHubSignalEntry[]): Promise<void> {
    if (entries.length === 0) return;

    const pipeline = this.client.pipeline();
    for (const entry of entries) {
      pipeline.xadd(
        this.streamKey,
        'MAXLEN',
        '~',
        MAX_STREAM_LENGTH.toString(),
        '*',
        'id', entry.id,
        'repo', entry.repo,
        'eventType', entry.eventType,
        'commitMessage', entry.commitMessage.substring(0, 500),
        'actor', entry.actor,
        'timestamp', entry.timestamp,
        'url', entry.url,
        'semanticScore', entry.semanticScore.toString(),
        'bias', entry.bias,
        'eventId', entry.eventId,
        'ingestedAt', new Date().toISOString()
      );
    }

    await pipeline.exec();
    console.info(`[REDIS] ${entries.length}ta signal Redis stream e publish hoise`);
  }

  async getStreamLength(): Promise<number> {
    return await this.client.xlen(this.streamKey);
  }

  async disconnect(): Promise<void> {
    await this.client.quit();
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// State Tracker (seen events de-duplication)
// ─────────────────────────────────────────────────────────────────────────────

class SeenEventTracker {
  // In-memory set — fine for single-process; Redis Set could be used for distributed
  private readonly seen: Set<string> = new Set();
  private readonly maxSize: number = 50_000;

  has(eventId: string): boolean {
    return this.seen.has(eventId);
  }

  add(eventId: string): void {
    if (this.seen.size >= this.maxSize) {
      // Evict oldest 10% when full (approximate via iteration)
      const evictCount = Math.floor(this.maxSize * 0.1);
      const iter = this.seen.values();
      for (let i = 0; i < evictCount; i++) {
        const next = iter.next();
        if (!next.done) this.seen.delete(next.value);
      }
      console.info(`[DEDUP] Seen set full, ${evictCount}ta purano entry evict korchi`);
    }
    this.seen.add(eventId);
  }

  get size(): number {
    return this.seen.size;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Main GitHub Recon Agent
// ─────────────────────────────────────────────────────────────────────────────

class GitHubReconAgent {
  private readonly repos: string[];
  private readonly github: GitHubApiClient;
  private readonly publisher: RedisPublisher;
  private readonly seenEvents: SeenEventTracker;
  private readonly etagCache: ETagCache = {};
  private isRunning: boolean = false;
  private totalSignalsPublished: number = 0;

  constructor(repos: string[]) {
    this.repos = repos;
    this.github = new GitHubApiClient(GITHUB_TOKEN);
    this.publisher = new RedisPublisher(REDIS_URL, REDIS_STREAM_KEY);
    this.seenEvents = new SeenEventTracker();
  }

  async initialize(): Promise<void> {
    console.info('[INIT] GitHub recon agent shuru holo...');
    console.info(`[INIT] ${this.repos.length}ta Web3 repo monitor korbo`);
    await this.publisher.connect();
    console.info('[INIT] Redis connection ready');
    console.info(`[INIT] Poll interval: ${POLL_INTERVAL_MS}ms, Stagger: ${STAGGER_DELAY_MS}ms`);
  }

  private async pollRepo(repo: string): Promise<GitHubSignalEntry[]> {
    const etag = this.etagCache[repo];
    let result: { events: GitHubEvent[]; etag?: string; notModified: boolean };

    try {
      result = await this.github.fetchEvents(repo, etag);
    } catch (err) {
      console.error(`[ERROR] ${repo} er events fetch korte problem: ${(err as Error).message}`);
      return [];
    }

    if (result.notModified) {
      return []; // ETag matched — no new events
    }

    if (result.etag) {
      this.etagCache[repo] = result.etag;
    }

    const newEvents = result.events.filter((e) => !this.seenEvents.has(e.id));

    if (newEvents.length === 0) return [];

    const entries: GitHubSignalEntry[] = [];
    for (const event of newEvents) {
      this.seenEvents.add(event.id);
      const processed = processEvent(event, repo);
      entries.push(...processed);
    }

    return entries;
  }

  private async runOnePollCycle(): Promise<void> {
    const startTime = Date.now();
    const allEntries: GitHubSignalEntry[] = [];
    let reposPolled = 0;
    let reposSkipped = 0;

    for (let i = 0; i < this.repos.length; i++) {
      const repo = this.repos[i];

      // Stagger requests to avoid burst limiting
      if (i > 0) {
        await sleep(STAGGER_DELAY_MS);
      }

      // Check rate limit before each request
      const rateLimitStatus = this.github.getRateLimitStatus();
      if (rateLimitStatus.remaining < RATE_LIMIT_BUFFER) {
        const msUntilReset = Math.max(0, rateLimitStatus.resetAt - Date.now());
        console.warn(
          `[RATE_LIMIT] Remaining ${rateLimitStatus.remaining} < ${RATE_LIMIT_BUFFER}, ` +
          `${Math.ceil(msUntilReset / 60000)} minute pause korchi...`
        );
        await sleep(msUntilReset + 2000);
        reposSkipped += this.repos.length - i;
        break;
      }

      const entries = await this.pollRepo(repo);
      allEntries.push(...entries);
      reposPolled++;
    }

    if (allEntries.length > 0) {
      try {
        await this.publisher.publishBatch(allEntries);
        this.totalSignalsPublished += allEntries.length;

        // Log notable signals
        const bearish = allEntries.filter((e) => e.bias === 'bearish');
        const bullish = allEntries.filter((e) => e.bias === 'bullish');
        if (bearish.length > 0) {
          console.warn(`[SUMMARY] ${bearish.length}ta bearish signal detect hoise is cycle e!`);
          for (const b of bearish.slice(0, 3)) {
            console.warn(`  → [${b.repo}] ${b.commitMessage.substring(0, 80)}`);
          }
        }
        if (bullish.length > 0) {
          console.info(`[SUMMARY] ${bullish.length}ta bullish signal detect hoise!`);
        }
      } catch (err) {
        console.error(`[ERROR] Redis publish fail: ${(err as Error).message}`);
      }
    }

    const elapsed = Date.now() - startTime;
    const streamLen = await this.publisher.getStreamLength().catch(() => -1);
    const rateLimitStatus = this.github.getRateLimitStatus();

    console.info(
      `[CYCLE] Poll cycle complete: ${reposPolled} repos polled, ` +
      `${reposSkipped} skipped, ${allEntries.length} new signals, ` +
      `${elapsed}ms elapsed. Total published: ${this.totalSignalsPublished}. ` +
      `Rate limit remaining: ${rateLimitStatus.remaining}/${rateLimitStatus.limit}. ` +
      `Stream length: ${streamLen}`
    );
  }

  async start(): Promise<void> {
    this.isRunning = true;
    await this.initialize();

    console.info('[START] GitHub recon polling loop shuru holo!');

    // Graceful shutdown
    process.on('SIGTERM', async () => {
      console.info('[SHUTDOWN] SIGTERM received, gracefully shutting down...');
      this.isRunning = false;
      await this.publisher.disconnect();
      console.info('[SHUTDOWN] GitHub recon agent thama gelo. Goodbye!');
      process.exit(0);
    });

    process.on('SIGINT', async () => {
      console.info('[SHUTDOWN] SIGINT received, gracefully shutting down...');
      this.isRunning = false;
      await this.publisher.disconnect();
      process.exit(0);
    });

    while (this.isRunning) {
      const cycleStart = Date.now();
      try {
        await this.runOnePollCycle();
      } catch (err) {
        console.error(`[ERROR] Poll cycle e unhandled error: ${(err as Error).message}`);
        console.error((err as Error).stack);
      }

      const elapsed = Date.now() - cycleStart;
      const remaining = Math.max(0, POLL_INTERVAL_MS - elapsed);

      if (remaining > 0 && this.isRunning) {
        console.info(`[WAIT] Porer cycle er jonno ${Math.ceil(remaining / 1000)}s wait korchi...`);
        await sleep(remaining);
      }
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Bootstrap
// ─────────────────────────────────────────────────────────────────────────────

async function loadRepos(): Promise<string[]> {
  try {
    const raw = fs.readFileSync(REPOS_FILE, 'utf-8');
    const repos = JSON.parse(raw) as string[];
    if (!Array.isArray(repos) || repos.length === 0) {
      throw new Error('web3-repos.json empty or invalid');
    }
    console.info(`[LOAD] ${repos.length}ta Web3 repo load hoise from web3-repos.json`);
    return repos;
  } catch (err) {
    console.error(`[ERROR] web3-repos.json load korte problem: ${(err as Error).message}`);
    process.exit(1);
  }
}

async function main(): Promise<void> {
  console.info('╔══════════════════════════════════════════════════════════╗');
  console.info('║       CSA GitHub Recon Agent v1.0.0 - Module 2          ║');
  console.info('╚══════════════════════════════════════════════════════════╝');
  console.info(`[ENV] REDIS_URL=${REDIS_URL}`);
  console.info(`[ENV] POLL_INTERVAL=${POLL_INTERVAL_MS}ms`);
  console.info(`[ENV] GITHUB_TOKEN=${GITHUB_TOKEN ? 'set (authenticated)' : 'NOT SET (anonymous)'}`);

  const repos = await loadRepos();
  const agent = new GitHubReconAgent(repos);
  await agent.start();
}

main().catch((err) => {
  console.error(`[FATAL] GitHub recon agent crash hoise: ${err.message}`);
  console.error(err.stack);
  process.exit(1);
});

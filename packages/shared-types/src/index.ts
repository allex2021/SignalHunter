// =============================================================================
// @csa/shared-types — Central type definitions for crypto-signal-aggregator
// =============================================================================

// ---------------------------------------------------------------------------
// Provider Types
// ---------------------------------------------------------------------------

export type ProviderStatus = 'healthy' | 'rate_limited' | 'error' | 'initializing';
export type ProviderName = 'gemini' | 'groq' | 'mistral';

export interface ProviderHealth {
  name: ProviderName;
  status: ProviderStatus;
  lastChecked: Date;
  successCount: number;
  failureCount: number;
  rateLimitResetAt?: Date;
  averageLatencyMs: number;
}

// ---------------------------------------------------------------------------
// Chat / LLM Types
// ---------------------------------------------------------------------------

export interface ChatMessage {
  role: 'system' | 'user' | 'assistant';
  content: string;
}

export interface ChatCompletionRequest {
  model?: string;
  messages: ChatMessage[];
  temperature?: number;
  max_tokens?: number;
  stream?: boolean;
}

export interface ChatCompletionResponse {
  id: string;
  object: 'chat.completion';
  created: number;
  model: string;
  provider: ProviderName;
  choices: Array<{
    index: number;
    message: ChatMessage;
    finish_reason: string;
  }>;
  usage: {
    prompt_tokens: number;
    completion_tokens: number;
    total_tokens: number;
  };
}

// ---------------------------------------------------------------------------
// Signal Types
// ---------------------------------------------------------------------------

export type DataSource =
  | 'GitHub_Commit'
  | 'SEC_Filing'
  | 'Whale_Flow'
  | 'X_Scrape'
  | 'Reddit_Spike';

export type DirectionalBias =
  | 'Bullish'
  | 'Bearish'
  | 'Volatility-Driven'
  | 'Neutral';

export interface SignalEvent {
  id: string;
  timestamp: Date;
  Data_Source: DataSource;
  Raw_Intel_Summary: string;
  /** Numeric impact 1 (noise) – 10 (market-moving) */
  Impact_Rating: number;
  Directional_Bias: DirectionalBias;
  metadata?: Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// Stream Event Types — raw data from each ingestion source
// ---------------------------------------------------------------------------

export interface GitHubReconEvent {
  repo: string;
  eventType: string;
  commitMessage?: string;
  actor: string;
  timestamp: string;
  url: string;
  /** AI semantic relevance 0-1 */
  semanticScore: number;
  bias: DirectionalBias;
}

export interface WhaleEvent {
  txHash: string;
  fromAddress: string;
  toAddress: string;
  token: 'USDT' | 'USDC';
  amountUSD: number;
  blockNumber: number;
  timestamp: string;
  destinationType: 'cex_hot_wallet' | 'unknown';
}

export interface SecFilingEvent {
  accessionNumber: string;
  formType: 'Form 4' | 'Schedule 13D' | 'Schedule 13G';
  companyName: string;
  ticker?: string;
  filedAt: string;
  insiderName?: string;
  transactionType?: string;
  sharesDelta?: number;
  url: string;
}

export interface SocialEvent {
  platform: 'twitter' | 'reddit';
  author: string;
  content: string;
  url: string;
  timestamp: string;
  engagementScore: number;
  keywords: string[];
}

export interface RedditSpikeEvent {
  subreddit: string;
  keyword: string;
  currentCount: number;
  previousCount: number;
  spikePercent: number;
  timestamp: string;
  topPostUrls: string[];
}

// ---------------------------------------------------------------------------
// Redis Stream / Key Constants
// ---------------------------------------------------------------------------

export const REDIS_STREAMS = {
  GITHUB: 'stream:github',
  WHALE: 'stream:whale',
  SEC: 'stream:sec',
  SOCIAL: 'stream:social',
  REDDIT: 'stream:reddit',
  SIGNALS: 'stream:signals',
} as const;

export const REDIS_KEYS = {
  PROVIDER_HEALTH: 'health:providers',
  SIGNAL_WINDOW: 'window:signals',
} as const;

// ---------------------------------------------------------------------------
// Utility / Error types used across packages
// ---------------------------------------------------------------------------

export class RateLimitError extends Error {
  public readonly provider: ProviderName;
  public readonly retryAfterMs: number;

  constructor(provider: ProviderName, retryAfterMs = 60_000) {
    super(`Rate limit exceeded for provider: ${provider}`);
    this.name = 'RateLimitError';
    this.provider = provider;
    this.retryAfterMs = retryAfterMs;
  }
}

export class ProviderError extends Error {
  public readonly provider: ProviderName;
  public readonly statusCode?: number;

  constructor(provider: ProviderName, message: string, statusCode?: number) {
    super(`[${provider}] ${message}`);
    this.name = 'ProviderError';
    this.provider = provider;
    this.statusCode = statusCode;
  }
}

export class AllProvidersDownError extends Error {
  constructor() {
    super('All AI providers are currently unavailable');
    this.name = 'AllProvidersDownError';
  }
}

// ---------------------------------------------------------------------------
// Raw event union — used by scoring-prompt builder
// ---------------------------------------------------------------------------

export type RawEvent =
  | GitHubReconEvent
  | WhaleEvent
  | SecFilingEvent
  | SocialEvent
  | RedditSpikeEvent;

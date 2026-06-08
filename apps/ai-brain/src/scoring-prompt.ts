// =============================================================================
// Scoring Prompt — AI system prompt for market signal classification
// Converts raw intel events into structured SignalEvent JSON arrays
// =============================================================================

import {
  ChatCompletionRequest,
  RawEvent,
  GitHubReconEvent,
  WhaleEvent,
  SecFilingEvent,
  SocialEvent,
  RedditSpikeEvent,
} from '@csa/shared-types';

// ---------------------------------------------------------------------------
// System Prompt
// ---------------------------------------------------------------------------

export const MARKET_SCORING_SYSTEM_PROMPT: string = `You are a hyper-precise quantitative market intelligence classifier for a crypto/stock signal aggregator.

## YOUR ROLE
You receive raw event data from 5 intelligence sources and must classify each event into a structured SignalEvent.

## STRICT OUTPUT RULES
1. Return ONLY a valid JSON array. No markdown, no prose, no explanation, no code fences.
2. If no events warrant a signal (Impact_Rating < 3), return an empty array: []
3. Every string in Raw_Intel_Summary must be ONE sentence, maximum 20 words.
4. Never hallucinate data not present in the input.

## SignalEvent Schema (EXACT format required)
[
  {
    "id": "<uuid-v4>",
    "timestamp": "<ISO-8601>",
    "Data_Source": "<GitHub_Commit|SEC_Filing|Whale_Flow|X_Scrape|Reddit_Spike>",
    "Raw_Intel_Summary": "<one sentence, max 20 words>",
    "Impact_Rating": <integer 1-10>,
    "Directional_Bias": "<Bullish|Bearish|Volatility-Driven|Neutral>",
    "metadata": {}
  }
]

## IMPACT RATING GUIDE
- 9-10: Market-moving event (massive whale move, CEO resignation Form 4, exploit commit)
- 7-8: Significant signal (large whale flow, insider buying >$5M, viral tweet from KOL)
- 5-6: Notable signal (medium whale, SEC 13G filing, coordinated Reddit spike)
- 3-4: Weak signal (small transaction, minor commit, low-engagement post)
- 1-2: Noise — omit these from output

## DIRECTIONAL BIAS RULES
- Bullish: Token/equity likely to rise (buying pressure, positive dev activity, accumulation)
- Bearish: Token/equity likely to fall (selling pressure, exchange deposits, negative news)
- Volatility-Driven: Unclear direction but high expected movement (regulatory news, exploits)
- Neutral: Market-neutral information

## DATA SOURCE MAPPING
- GitHubReconEvent → Data_Source: "GitHub_Commit"
- WhaleEvent → Data_Source: "Whale_Flow"
- SecFilingEvent → Data_Source: "SEC_Filing"
- SocialEvent (twitter) → Data_Source: "X_Scrape"
- RedditSpikeEvent → Data_Source: "Reddit_Spike"

## WHALE FLOW SPECIFIC RULES
- amountUSD > 10,000,000 AND destinationType = "cex_hot_wallet" → Bearish (exchange deposit = likely sell)
- amountUSD > 10,000,000 AND destinationType = "unknown" → Volatility-Driven
- token = "USDT" or "USDC" moving to CEX → Potential buy signal (dry powder incoming) → Bullish

## GITHUB SPECIFIC RULES
- Use the provided semanticScore (0-1) as a multiplier for Impact_Rating
- Commits mentioning "vulnerability", "exploit", "critical", "emergency" → Volatility-Driven, high impact
- Commits mentioning "launch", "mainnet", "v2", "upgrade" → Bullish

## SEC FILING SPECIFIC RULES
- Form 4 with transactionType "P" (purchase) and sharesDelta > 0 → Bullish
- Form 4 with transactionType "S" (sale) and sharesDelta < 0 → Bearish
- Schedule 13D → new activist investor → Bullish or Volatility-Driven
- Schedule 13G → passive accumulation → mildly Bullish

Analyze the following events and return the classified JSON array now:`;

// ---------------------------------------------------------------------------
// Raw event type guards
// ---------------------------------------------------------------------------

function isWhaleEvent(e: RawEvent): e is WhaleEvent {
  return 'txHash' in e && 'amountUSD' in e;
}

function isGitHubEvent(e: RawEvent): e is GitHubReconEvent {
  return 'repo' in e && 'semanticScore' in e;
}

function isSecEvent(e: RawEvent): e is SecFilingEvent {
  return 'accessionNumber' in e && 'formType' in e;
}

function isSocialEvent(e: RawEvent): e is SocialEvent {
  return 'platform' in e && 'engagementScore' in e;
}

function isRedditEvent(e: RawEvent): e is RedditSpikeEvent {
  return 'subreddit' in e && 'spikePercent' in e;
}

// ---------------------------------------------------------------------------
// Build human-readable summary of each event for the prompt
// ---------------------------------------------------------------------------

function formatEvent(event: RawEvent, index: number): string {
  const header = `--- Event ${index + 1} ---`;

  if (isWhaleEvent(event)) {
    return `${header}
Type: WhaleEvent
txHash: ${event.txHash}
token: ${event.token}
amountUSD: $${event.amountUSD.toLocaleString()}
from: ${event.fromAddress}
to: ${event.toAddress}
destinationType: ${event.destinationType}
blockNumber: ${event.blockNumber}
timestamp: ${event.timestamp}`;
  }

  if (isGitHubEvent(event)) {
    return `${header}
Type: GitHubReconEvent
repo: ${event.repo}
eventType: ${event.eventType}
actor: ${event.actor}
commitMessage: ${event.commitMessage ?? 'N/A'}
semanticScore: ${event.semanticScore}
bias: ${event.bias}
timestamp: ${event.timestamp}
url: ${event.url}`;
  }

  if (isSecEvent(event)) {
    return `${header}
Type: SecFilingEvent
formType: ${event.formType}
companyName: ${event.companyName}
ticker: ${event.ticker ?? 'N/A'}
insiderName: ${event.insiderName ?? 'N/A'}
transactionType: ${event.transactionType ?? 'N/A'}
sharesDelta: ${event.sharesDelta ?? 'N/A'}
filedAt: ${event.filedAt}
accessionNumber: ${event.accessionNumber}
url: ${event.url}`;
  }

  if (isSocialEvent(event)) {
    return `${header}
Type: SocialEvent
platform: ${event.platform}
author: ${event.author}
content: ${event.content}
engagementScore: ${event.engagementScore}
keywords: ${event.keywords.join(', ')}
timestamp: ${event.timestamp}
url: ${event.url}`;
  }

  if (isRedditEvent(event)) {
    return `${header}
Type: RedditSpikeEvent
subreddit: r/${event.subreddit}
keyword: "${event.keyword}"
currentCount: ${event.currentCount}
previousCount: ${event.previousCount}
spikePercent: +${event.spikePercent.toFixed(1)}%
timestamp: ${event.timestamp}
topPostUrls: ${event.topPostUrls.slice(0, 3).join(', ')}`;
  }

  return `${header}\n${JSON.stringify(event, null, 2)}`;
}

// ---------------------------------------------------------------------------
// Build the full ChatCompletionRequest for the scoring engine
// ---------------------------------------------------------------------------

export function buildScoringPayload(events: RawEvent[]): ChatCompletionRequest {
  if (events.length === 0) {
    console.warn('[ScoringPrompt] No events provided, returning minimal payload');
  }

  const formattedEvents = events.map((e, i) => formatEvent(e, i)).join('\n\n');

  const userContent = events.length > 0
    ? `Total events to classify: ${events.length}\n\n${formattedEvents}\n\nReturn the classified SignalEvent JSON array:`
    : 'No events to classify. Return: []';

  console.log(
    `[ScoringPrompt] Scoring payload banano holo — ${events.length} events`,
  );

  return {
    messages: [
      {
        role: 'system',
        content: MARKET_SCORING_SYSTEM_PROMPT,
      },
      {
        role: 'user',
        content: userContent,
      },
    ],
    temperature: 0.1,   // Low temperature for consistent classification
    max_tokens: 4096,   // Generous token budget for large event batches
  };
}

// ---------------------------------------------------------------------------
// Parse and validate the AI response into SignalEvents
// ---------------------------------------------------------------------------

export interface ParsedSignal {
  id: string;
  timestamp: string;
  Data_Source: string;
  Raw_Intel_Summary: string;
  Impact_Rating: number;
  Directional_Bias: string;
  metadata: Record<string, unknown>;
}

export function parseScoringResponse(rawContent: string): ParsedSignal[] {
  // Strip any accidental markdown code fences
  const cleaned = rawContent
    .trim()
    .replace(/^```(?:json)?\s*/i, '')
    .replace(/\s*```$/, '')
    .trim();

  let parsed: unknown;

  try {
    parsed = JSON.parse(cleaned);
  } catch (e) {
    console.error(
      `[ScoringPrompt] JSON parse fail — content snippet: ${cleaned.slice(0, 200)}`,
    );
    throw new Error(`Failed to parse AI scoring response as JSON: ${String(e)}`);
  }

  if (!Array.isArray(parsed)) {
    console.error(
      '[ScoringPrompt] AI response JSON array na — type:', typeof parsed,
    );
    throw new Error('AI scoring response is not a JSON array');
  }

  const validated: ParsedSignal[] = [];

  for (const item of parsed) {
    if (typeof item !== 'object' || item === null) continue;

    const signal = item as Record<string, unknown>;

    // Validate required fields
    if (
      typeof signal.id !== 'string' ||
      typeof signal.Data_Source !== 'string' ||
      typeof signal.Raw_Intel_Summary !== 'string' ||
      typeof signal.Impact_Rating !== 'number' ||
      typeof signal.Directional_Bias !== 'string'
    ) {
      console.warn('[ScoringPrompt] Invalid signal skipped:', signal);
      continue;
    }

    // Clamp impact rating to valid range
    const clampedRating = Math.max(1, Math.min(10, Math.round(signal.Impact_Rating)));

    validated.push({
      id: signal.id,
      timestamp: typeof signal.timestamp === 'string'
        ? signal.timestamp
        : new Date().toISOString(),
      Data_Source: signal.Data_Source,
      Raw_Intel_Summary: signal.Raw_Intel_Summary,
      Impact_Rating: clampedRating,
      Directional_Bias: signal.Directional_Bias,
      metadata: (typeof signal.metadata === 'object' && signal.metadata !== null)
        ? signal.metadata as Record<string, unknown>
        : {},
    });
  }

  console.log(
    `[ScoringPrompt] ${validated.length} valid signal parse hoise out of ${parsed.length} returned`,
  );

  return validated;
}

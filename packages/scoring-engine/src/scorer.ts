/**
 * scorer.ts — Quantitative Scoring Engine
 *
 * Takes raw events from all 4 Redis streams, compiles them into a
 * 2-hour window payload, and sends to AI Brain for scoring.
 *
 * Banglish logs for instant debug identification:
 *   "Scoring engine: Xta event AI e pathacchi"
 *   "AI Brain theke Xta signal aslo"
 *   "Invalid signal format, skip korchi: ..."
 */

import axios, { AxiosError, AxiosInstance } from 'axios';
import { v4 as uuidv4 } from 'uuid';

// ─────────────────────────────────────────────────────────────────────────────
// Types (inline fallback if @csa/shared-types not yet resolved)
// ─────────────────────────────────────────────────────────────────────────────

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
  timestamp: string; // ISO 8601 UTC
  Data_Source: DataSource;
  Raw_Intel_Summary: string; // max 25 words
  Impact_Rating: number; // integer 1–10
  Directional_Bias: DirectionalBias;
}

export interface RawWindowEvent {
  stream: string;
  event_id: string;
  timestamp: number; // Unix ms
  payload: Record<string, unknown>;
}

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

const VALID_DATA_SOURCES: Set<DataSource> = new Set([
  'GitHub_Commit',
  'SEC_Filing',
  'Whale_Flow',
  'X_Scrape',
  'Reddit_Spike',
]);

const VALID_BIASES: Set<DirectionalBias> = new Set([
  'Bullish',
  'Bearish',
  'Volatility-Driven',
  'Neutral',
]);

const SYSTEM_PROMPT_TEMPLATE = `You are a ruthless quantitative analyst. Analyze the following raw market intelligence events from the last 2 hours.
Return ONLY a valid JSON array of signal objects. No prose, no explanation, only JSON.

Each object must follow exactly this schema:
{
  "Data_Source": "GitHub_Commit" | "SEC_Filing" | "Whale_Flow" | "X_Scrape" | "Reddit_Spike",
  "Raw_Intel_Summary": "<one sentence, max 25 words>",
  "Impact_Rating": <integer 1-10, where 1=noise and 10=immediate market mover>,
  "Directional_Bias": "Bullish" | "Bearish" | "Volatility-Driven" | "Neutral"
}

Rating guidance:
- 9-10: Immediate price-moving event (massive insider sell, whale dump to CEX, critical exploit)
- 7-8: High significance (large Form 4 purchase, GitHub emergency hotfix, coordinated whale move)
- 5-6: Medium signal (keyword spike >500%, routine insider purchase, protocol upgrade commit)
- 3-4: Low signal (small commit, minor keyword uptick, routine filing)
- 1-2: Noise (unrelated commit, tiny transfer, background chatter)

Raw Events:
{{EVENTS_JSON}}`;

// ─────────────────────────────────────────────────────────────────────────────
// ScoringEngine
// ─────────────────────────────────────────────────────────────────────────────

export class ScoringEngine {
  private readonly http: AxiosInstance;
  private readonly model: string;
  private readonly maxRetries: number;

  constructor(options?: {
    aiBrainUrl?: string;
    model?: string;
    maxRetries?: number;
    timeoutMs?: number;
  }) {
    const aiBrainUrl =
      options?.aiBrainUrl ??
      process.env['AI_BRAIN_URL'] ??
      'http://localhost:3000';

    this.model =
      options?.model ??
      process.env['SCORING_MODEL'] ??
      'gpt-4o-mini';

    this.maxRetries = options?.maxRetries ?? 3;

    this.http = axios.create({
      baseURL: aiBrainUrl,
      timeout: options?.timeoutMs ?? 60_000,
      headers: {
        'Content-Type': 'application/json',
        ...(process.env['AI_BRAIN_API_KEY']
          ? { Authorization: `Bearer ${process.env['AI_BRAIN_API_KEY']}` }
          : {}),
      },
    });
  }

  // ── Public API ────────────────────────────────────────────────────────────

  /**
   * Score a window of raw events using the AI Brain.
   * Returns validated SignalEvent[] with assigned UUIDs and timestamps.
   */
  async scoreWindowEvents(events: RawWindowEvent[]): Promise<SignalEvent[]> {
    if (events.length === 0) {
      console.log('[ScoringEngine] Kono event nei, scoring skip korchi');
      return [];
    }

    console.log(
      `[ScoringEngine] Scoring engine: ${events.length}ta event AI e pathacchi`,
    );

    const prompt = this.buildPrompt(events);

    let rawResponse: string;
    try {
      rawResponse = await this.callAIBrainWithRetry(prompt);
    } catch (err) {
      console.error(
        '[ScoringEngine] AI Brain call fail hoise:',
        err instanceof Error ? err.message : err,
      );
      return [];
    }

    const parsed = this.parseAIResponse(rawResponse);

    console.log(
      `[ScoringEngine] AI Brain theke ${parsed.length}ta signal aslo`,
    );

    const validated = this.validateAndEnrich(parsed);

    console.log(
      `[ScoringEngine] ${validated.length}ta valid signal final list e ache`,
    );

    return validated;
  }

  // ── Private helpers ───────────────────────────────────────────────────────

  /**
   * Build the user message containing serialized raw events.
   */
  private buildPrompt(events: RawWindowEvent[]): string {
    // Summarize each raw event into a compact object to keep token count low
    const compactEvents = events.map((e) => ({
      stream: e.stream,
      event_id: e.event_id,
      ts: new Date(e.timestamp).toISOString(),
      payload: e.payload,
    }));

    const eventsJson = JSON.stringify(compactEvents, null, 2);

    return SYSTEM_PROMPT_TEMPLATE.replace('{{EVENTS_JSON}}', eventsJson);
  }

  /**
   * POST to /v1/chat/completions with retry + exponential backoff.
   */
  private async callAIBrainWithRetry(prompt: string): Promise<string> {
    let lastError: unknown;

    for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
      try {
        const response = await this.http.post<{
          choices: Array<{
            message: { content: string };
            finish_reason: string;
          }>;
          usage?: { total_tokens: number };
        }>('/v1/chat/completions', {
          model: this.model,
          messages: [
            {
              role: 'system',
              content:
                'You are a ruthless quantitative analyst. Return ONLY valid JSON arrays. Never add prose or markdown.',
            },
            {
              role: 'user',
              content: prompt,
            },
          ],
          temperature: 0.1,
          max_tokens: 4096,
          response_format: { type: 'json_object' },
        });

        const content = response.data.choices[0]?.message?.content ?? '';

        if (!content) {
          throw new Error('AI Brain empty response dilо');
        }

        if (response.data.usage) {
          console.log(
            `[ScoringEngine] Token usage: ${response.data.usage.total_tokens} (attempt ${attempt})`,
          );
        }

        return content;
      } catch (err) {
        lastError = err;
        const isAxios = err instanceof AxiosError;
        const status = isAxios ? err.response?.status : undefined;

        console.warn(
          `[ScoringEngine] AI Brain attempt ${attempt} fail hoise (status=${status ?? 'network'}): ${
            err instanceof Error ? err.message : String(err)
          }`,
        );

        // Don't retry on 4xx (bad request, auth failures)
        if (status !== undefined && status >= 400 && status < 500) {
          break;
        }

        if (attempt < this.maxRetries) {
          const backoffMs = Math.min(1000 * Math.pow(2, attempt - 1), 16_000);
          console.log(
            `[ScoringEngine] ${backoffMs}ms por retry korbo (attempt ${attempt + 1}/${this.maxRetries})`,
          );
          await new Promise((r) => setTimeout(r, backoffMs));
        }
      }
    }

    throw lastError;
  }

  /**
   * Parse the raw string from AI Brain into a plain JS array.
   * Handles both array-as-root and object-with-signals-key patterns.
   */
  private parseAIResponse(raw: string): Array<Record<string, unknown>> {
    // Strip markdown code fences if present
    const cleaned = raw
      .replace(/^```(?:json)?\s*/i, '')
      .replace(/\s*```$/i, '')
      .trim();

    let parsed: unknown;
    try {
      parsed = JSON.parse(cleaned);
    } catch {
      console.error(
        '[ScoringEngine] AI response JSON parse fail:',
        cleaned.slice(0, 500),
      );
      return [];
    }

    // Handle { "signals": [...] } wrapper
    if (
      typeof parsed === 'object' &&
      parsed !== null &&
      !Array.isArray(parsed)
    ) {
      const obj = parsed as Record<string, unknown>;
      for (const key of ['signals', 'results', 'events', 'data']) {
        if (Array.isArray(obj[key])) {
          return obj[key] as Array<Record<string, unknown>>;
        }
      }
      // If it's a single object, wrap it
      return [obj];
    }

    if (Array.isArray(parsed)) {
      return parsed as Array<Record<string, unknown>>;
    }

    console.error('[ScoringEngine] Unexpected AI response shape:', typeof parsed);
    return [];
  }

  /**
   * Validate each candidate signal, skip invalid ones, enrich with UUID + timestamp.
   */
  private validateAndEnrich(
    candidates: Array<Record<string, unknown>>,
  ): SignalEvent[] {
    const validated: SignalEvent[] = [];

    for (const s of candidates) {
      if (!this.isValidSignalCandidate(s)) {
        console.warn(
          `[ScoringEngine] Invalid signal format, skip korchi: ${JSON.stringify(s)}`,
        );
        continue;
      }

      const impact = Number(s['Impact_Rating']);
      if (!Number.isInteger(impact) || impact < 1 || impact > 10) {
        console.warn(
          `[ScoringEngine] Impact_Rating out of range, skip korchi: ${JSON.stringify(s)}`,
        );
        continue;
      }

      const summary = String(s['Raw_Intel_Summary']).trim();
      const wordCount = summary.split(/\s+/).length;
      if (wordCount > 30) {
        // Allow slight overflow, truncate at 30 words
        const truncated = summary.split(/\s+/).slice(0, 25).join(' ');
        console.warn(
          `[ScoringEngine] Summary too long (${wordCount} words), truncating`,
        );
        validated.push({
          id: uuidv4(),
          timestamp: new Date().toISOString(),
          Data_Source: s['Data_Source'] as DataSource,
          Raw_Intel_Summary: truncated,
          Impact_Rating: impact,
          Directional_Bias: s['Directional_Bias'] as DirectionalBias,
        });
        continue;
      }

      validated.push({
        id: uuidv4(),
        timestamp: new Date().toISOString(),
        Data_Source: s['Data_Source'] as DataSource,
        Raw_Intel_Summary: summary,
        Impact_Rating: impact,
        Directional_Bias: s['Directional_Bias'] as DirectionalBias,
      });
    }

    return validated;
  }

  /**
   * Type guard: checks all required fields exist with correct types.
   */
  private isValidSignalCandidate(s: Record<string, unknown>): boolean {
    if (typeof s !== 'object' || s === null) return false;

    if (!VALID_DATA_SOURCES.has(s['Data_Source'] as DataSource)) {
      return false;
    }

    if (
      typeof s['Raw_Intel_Summary'] !== 'string' ||
      s['Raw_Intel_Summary'].trim() === ''
    ) {
      return false;
    }

    const impact = Number(s['Impact_Rating']);
    if (isNaN(impact)) return false;

    if (!VALID_BIASES.has(s['Directional_Bias'] as DirectionalBias)) {
      return false;
    }

    return true;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Standalone test harness (run directly: tsx src/scorer.ts)
// ─────────────────────────────────────────────────────────────────────────────

if (require.main === module) {
  void (async () => {
    const engine = new ScoringEngine();

    const testEvents: RawWindowEvent[] = [
      {
        stream: 'stream:whale',
        event_id: '1717200000000-0',
        timestamp: Date.now() - 300_000,
        payload: {
          from: '0xAbc123',
          to: 'Binance Hot Wallet',
          amount_usd: 47_000_000,
          token: 'ETH',
          tx_hash: '0xdeadbeef',
        },
      },
      {
        stream: 'stream:github',
        event_id: '1717200001000-0',
        timestamp: Date.now() - 600_000,
        payload: {
          repo: 'uniswap/v3-core',
          message: 'EMERGENCY: patch critical reentrancy vulnerability',
          author: 'hayden.eth',
          files_changed: 3,
        },
      },
      {
        stream: 'stream:sec',
        event_id: '1717200002000-0',
        timestamp: Date.now() - 900_000,
        payload: {
          form_type: 'Form 4',
          company: 'Coinbase Global Inc',
          insider: 'Brian Armstrong',
          transaction: 'Sale',
          shares: 150_000,
          price_usd: 212.5,
          total_usd: 31_875_000,
        },
      },
    ];

    console.log('[Test] Scoring test events...');
    const signals = await engine.scoreWindowEvents(testEvents);
    console.log('[Test] Scored signals:', JSON.stringify(signals, null, 2));
  })();
}

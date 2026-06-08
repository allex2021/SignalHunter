// =============================================================================
// Groq Provider — OpenAI-compatible inference API
// Ultra-fast LPU-based inference — primary failover from Gemini
// =============================================================================

import axios, { AxiosError } from 'axios';
import { v4 as uuidv4 } from 'uuid';
import {
  ChatCompletionRequest,
  ChatCompletionResponse,
  ProviderName,
  RateLimitError,
  ProviderError,
} from '@csa/shared-types';

// ---------------------------------------------------------------------------
// Internal Groq types (mirrors OpenAI wire format)
// ---------------------------------------------------------------------------

interface GroqChatMessage {
  role: 'system' | 'user' | 'assistant';
  content: string;
}

interface GroqChatRequest {
  model: string;
  messages: GroqChatMessage[];
  temperature?: number;
  max_tokens?: number;
  stream?: boolean;
}

interface GroqChoice {
  index: number;
  message: GroqChatMessage;
  finish_reason: string;
}

interface GroqUsage {
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
}

interface GroqChatResponse {
  id: string;
  object: string;
  created: number;
  model: string;
  choices: GroqChoice[];
  usage: GroqUsage;
  system_fingerprint?: string;
  x_groq?: { id: string };
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PROVIDER_NAME: ProviderName = 'groq';
const BASE_URL = 'https://api.groq.com/openai/v1/chat/completions';
// Best free-tier models on Groq — llama3 70b for quality, mixtral as fallback
const DEFAULT_MODEL = 'llama3-70b-8192';
const REQUEST_TIMEOUT_MS = 25_000;

// ---------------------------------------------------------------------------
// Main client function
// ---------------------------------------------------------------------------

export async function callGroq(
  request: ChatCompletionRequest,
  apiKey: string,
): Promise<ChatCompletionResponse> {
  const model = request.model ?? DEFAULT_MODEL;

  const payload: GroqChatRequest = {
    model,
    messages: request.messages as GroqChatMessage[],
    ...(request.temperature !== undefined && { temperature: request.temperature }),
    ...(request.max_tokens !== undefined && { max_tokens: request.max_tokens }),
    stream: false, // streaming handled at gateway level
  };

  const startTime = Date.now();

  console.log(
    `[Groq] Cholche, fast inference hobe — model: ${model}, messages: ${request.messages.length}`,
  );

  try {
    const response = await axios.post<GroqChatResponse>(BASE_URL, payload, {
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${apiKey}`,
      },
      timeout: REQUEST_TIMEOUT_MS,
    });

    const latencyMs = Date.now() - startTime;
    const data = response.data;

    console.log(
      `[Groq] Response ashlo — latency: ${latencyMs}ms, tokens: ${data.usage?.total_tokens ?? 0}, id: ${data.id}`,
    );

    const normalized: ChatCompletionResponse = {
      id: data.id || `groq-${uuidv4()}`,
      object: 'chat.completion',
      created: data.created || Math.floor(Date.now() / 1000),
      model: data.model || model,
      provider: PROVIDER_NAME,
      choices: data.choices.map((c) => ({
        index: c.index,
        message: {
          role: c.message.role,
          content: c.message.content,
        },
        finish_reason: c.finish_reason,
      })),
      usage: {
        prompt_tokens: data.usage?.prompt_tokens ?? 0,
        completion_tokens: data.usage?.completion_tokens ?? 0,
        total_tokens: data.usage?.total_tokens ?? 0,
      },
    };

    return normalized;
  } catch (err) {
    const latencyMs = Date.now() - startTime;

    if (axios.isAxiosError(err)) {
      const axiosErr = err as AxiosError<{
        error?: { message?: string; type?: string };
      }>;
      const statusCode = axiosErr.response?.status;
      const errorMessage =
        axiosErr.response?.data?.error?.message ?? axiosErr.message;
      const errorType = axiosErr.response?.data?.error?.type;

      if (statusCode === 429) {
        const retryAfter = axiosErr.response?.headers?.['retry-after'];
        const retryAfterMs = retryAfter ? parseInt(retryAfter, 10) * 1000 : 60_000;

        console.warn(
          `[Groq] Rate limit dhora khaise — type: ${errorType}, retry after: ${retryAfterMs}ms, latency: ${latencyMs}ms`,
        );
        throw new RateLimitError(PROVIDER_NAME, retryAfterMs);
      }

      console.error(
        `[Groq] API error — status: ${statusCode}, message: ${errorMessage}, latency: ${latencyMs}ms`,
      );
      throw new ProviderError(PROVIDER_NAME, errorMessage, statusCode);
    }

    console.error(
      `[Groq] Unknown error — ${String(err)}, latency: ${latencyMs}ms`,
    );
    throw new ProviderError(PROVIDER_NAME, `Unknown error: ${String(err)}`);
  }
}

// ---------------------------------------------------------------------------
// Lightweight ping — used by HealthMonitor
// ---------------------------------------------------------------------------

export async function pingGroq(apiKey: string): Promise<number> {
  const start = Date.now();
  await callGroq(
    {
      messages: [{ role: 'user', content: 'Reply with the single word: pong' }],
      max_tokens: 5,
      temperature: 0,
    },
    apiKey,
  );
  return Date.now() - start;
}

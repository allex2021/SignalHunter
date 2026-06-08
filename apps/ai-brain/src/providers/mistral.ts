// =============================================================================
// Mistral Provider — Mistral AI API
// OpenAI-compatible format — tertiary backup in the failover chain
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
// Internal Mistral types (mirrors OpenAI wire format)
// ---------------------------------------------------------------------------

interface MistralChatMessage {
  role: 'system' | 'user' | 'assistant';
  content: string;
}

interface MistralChatRequest {
  model: string;
  messages: MistralChatMessage[];
  temperature?: number;
  max_tokens?: number;
  stream?: boolean;
  safe_prompt?: boolean;
}

interface MistralChoice {
  index: number;
  message: MistralChatMessage;
  finish_reason: string;
}

interface MistralUsage {
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
}

interface MistralChatResponse {
  id: string;
  object: string;
  created: number;
  model: string;
  choices: MistralChoice[];
  usage: MistralUsage;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PROVIDER_NAME: ProviderName = 'mistral';
const BASE_URL = 'https://api.mistral.ai/v1/chat/completions';
// mistral-small is the free-tier workhorse; mistral-tiny as ultra-cheap alt
const DEFAULT_MODEL = 'mistral-small-latest';
const REQUEST_TIMEOUT_MS = 35_000;

// ---------------------------------------------------------------------------
// Main client function
// ---------------------------------------------------------------------------

export async function callMistral(
  request: ChatCompletionRequest,
  apiKey: string,
): Promise<ChatCompletionResponse> {
  const model = request.model ?? DEFAULT_MODEL;

  const payload: MistralChatRequest = {
    model,
    messages: request.messages as MistralChatMessage[],
    ...(request.temperature !== undefined && { temperature: request.temperature }),
    ...(request.max_tokens !== undefined && { max_tokens: request.max_tokens }),
    stream: false,
    safe_prompt: false,
  };

  const startTime = Date.now();

  console.log(
    `[Mistral] Use korchi backup hisebe — model: ${model}, messages: ${request.messages.length}`,
  );

  try {
    const response = await axios.post<MistralChatResponse>(BASE_URL, payload, {
      headers: {
        'Content-Type': 'application/json',
        Accept: 'application/json',
        Authorization: `Bearer ${apiKey}`,
      },
      timeout: REQUEST_TIMEOUT_MS,
    });

    const latencyMs = Date.now() - startTime;
    const data = response.data;

    console.log(
      `[Mistral] Response pailam — latency: ${latencyMs}ms, tokens: ${data.usage?.total_tokens ?? 0}, model: ${data.model}`,
    );

    const normalized: ChatCompletionResponse = {
      id: data.id || `mistral-${uuidv4()}`,
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
        message?: string;
        detail?: string;
      }>;
      const statusCode = axiosErr.response?.status;
      const errorMessage =
        axiosErr.response?.data?.message ??
        axiosErr.response?.data?.detail ??
        axiosErr.message;

      if (statusCode === 429) {
        const retryAfter = axiosErr.response?.headers?.['retry-after'];
        const retryAfterMs = retryAfter ? parseInt(retryAfter, 10) * 1000 : 60_000;

        console.warn(
          `[Mistral] Rate limit lagse — retry after: ${retryAfterMs}ms, latency: ${latencyMs}ms`,
        );
        throw new RateLimitError(PROVIDER_NAME, retryAfterMs);
      }

      // Handle Mistral-specific auth error
      if (statusCode === 401) {
        console.error('[Mistral] Authentication failed — API key check koro!');
        throw new ProviderError(PROVIDER_NAME, 'Invalid Mistral API key', 401);
      }

      console.error(
        `[Mistral] API error — status: ${statusCode}, message: ${errorMessage}, latency: ${latencyMs}ms`,
      );
      throw new ProviderError(PROVIDER_NAME, errorMessage, statusCode);
    }

    console.error(
      `[Mistral] Unknown error — ${String(err)}, latency: ${latencyMs}ms`,
    );
    throw new ProviderError(PROVIDER_NAME, `Unknown error: ${String(err)}`);
  }
}

// ---------------------------------------------------------------------------
// Lightweight ping — used by HealthMonitor
// ---------------------------------------------------------------------------

export async function pingMistral(apiKey: string): Promise<number> {
  const start = Date.now();
  await callMistral(
    {
      messages: [{ role: 'user', content: 'Reply with the single word: pong' }],
      max_tokens: 5,
      temperature: 0,
    },
    apiKey,
  );
  return Date.now() - start;
}

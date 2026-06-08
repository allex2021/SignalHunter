// =============================================================================
// Gemini Provider — Google Generative Language API
// Converts OpenAI-style requests → Gemini format → normalized response
// =============================================================================

import axios, { AxiosError } from 'axios';
import { v4 as uuidv4 } from 'uuid';
import {
  ChatCompletionRequest,
  ChatCompletionResponse,
  ChatMessage,
  ProviderName,
  RateLimitError,
  ProviderError,
} from '@csa/shared-types';

// ---------------------------------------------------------------------------
// Internal Gemini API types
// ---------------------------------------------------------------------------

interface GeminiPart {
  text: string;
}

interface GeminiContent {
  role: 'user' | 'model';
  parts: GeminiPart[];
}

interface GeminiGenerateRequest {
  contents: GeminiContent[];
  systemInstruction?: {
    parts: GeminiPart[];
  };
  generationConfig?: {
    temperature?: number;
    maxOutputTokens?: number;
  };
}

interface GeminiCandidate {
  content: GeminiContent;
  finishReason: string;
  index: number;
}

interface GeminiUsageMetadata {
  promptTokenCount: number;
  candidatesTokenCount: number;
  totalTokenCount: number;
}

interface GeminiGenerateResponse {
  candidates: GeminiCandidate[];
  usageMetadata: GeminiUsageMetadata;
  modelVersion?: string;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PROVIDER_NAME: ProviderName = 'gemini';
const BASE_URL = 'https://generativelanguage.googleapis.com/v1beta/models';
const DEFAULT_MODEL = 'gemini-1.5-flash-latest';
const REQUEST_TIMEOUT_MS = 30_000;

// ---------------------------------------------------------------------------
// Helper — Convert OpenAI messages to Gemini contents format
// ---------------------------------------------------------------------------

function convertMessagesToGeminiFormat(messages: ChatMessage[]): {
  systemInstruction?: GeminiGenerateRequest['systemInstruction'];
  contents: GeminiContent[];
} {
  const systemMessages = messages.filter((m) => m.role === 'system');
  const conversationMessages = messages.filter((m) => m.role !== 'system');

  // Combine all system messages into a single system instruction
  const systemInstruction =
    systemMessages.length > 0
      ? {
          parts: [{ text: systemMessages.map((m) => m.content).join('\n\n') }],
        }
      : undefined;

  // Convert user/assistant messages — Gemini uses 'model' for assistant
  const contents: GeminiContent[] = conversationMessages.map((msg) => ({
    role: msg.role === 'assistant' ? 'model' : 'user',
    parts: [{ text: msg.content }],
  }));

  // Gemini requires conversation to start with 'user' — inject empty if needed
  if (contents.length === 0 || contents[0].role !== 'user') {
    contents.unshift({ role: 'user', parts: [{ text: 'Begin.' }] });
  }

  return { systemInstruction, contents };
}

// ---------------------------------------------------------------------------
// Main client function
// ---------------------------------------------------------------------------

export async function callGemini(
  request: ChatCompletionRequest,
  apiKey: string,
): Promise<ChatCompletionResponse> {
  const model = request.model ?? DEFAULT_MODEL;
  const url = `${BASE_URL}/${model}:generateContent?key=${apiKey}`;

  const { systemInstruction, contents } = convertMessagesToGeminiFormat(
    request.messages,
  );

  const payload: GeminiGenerateRequest = {
    contents,
    ...(systemInstruction && { systemInstruction }),
    generationConfig: {
      ...(request.temperature !== undefined && {
        temperature: request.temperature,
      }),
      ...(request.max_tokens !== undefined && {
        maxOutputTokens: request.max_tokens,
      }),
    },
  };

  const startTime = Date.now();

  console.log(
    `[Gemini] Request pathano hocche — model: ${model}, messages: ${request.messages.length}`,
  );

  try {
    const response = await axios.post<GeminiGenerateResponse>(url, payload, {
      headers: {
        'Content-Type': 'application/json',
      },
      timeout: REQUEST_TIMEOUT_MS,
    });

    const latencyMs = Date.now() - startTime;
    console.log(
      `[Gemini] Theke response ashlo — latency: ${latencyMs}ms, finish: ${response.data.candidates[0]?.finishReason ?? 'unknown'}`,
    );

    const candidate = response.data.candidates[0];

    if (!candidate) {
      throw new ProviderError(PROVIDER_NAME, 'No candidates returned from Gemini API');
    }

    const assistantText =
      candidate.content.parts.map((p) => p.text).join('') || '';

    const usage = response.data.usageMetadata;

    const normalized: ChatCompletionResponse = {
      id: `gemini-${uuidv4()}`,
      object: 'chat.completion',
      created: Math.floor(Date.now() / 1000),
      model: model,
      provider: PROVIDER_NAME,
      choices: [
        {
          index: 0,
          message: {
            role: 'assistant',
            content: assistantText,
          },
          finish_reason: candidate.finishReason?.toLowerCase() ?? 'stop',
        },
      ],
      usage: {
        prompt_tokens: usage?.promptTokenCount ?? 0,
        completion_tokens: usage?.candidatesTokenCount ?? 0,
        total_tokens: usage?.totalTokenCount ?? 0,
      },
    };

    return normalized;
  } catch (err) {
    const latencyMs = Date.now() - startTime;

    if (axios.isAxiosError(err)) {
      const axiosErr = err as AxiosError<{ error?: { message?: string } }>;
      const statusCode = axiosErr.response?.status;
      const errorMessage =
        axiosErr.response?.data?.error?.message ?? axiosErr.message;

      if (statusCode === 429) {
        // Parse Retry-After header if present
        const retryAfter = axiosErr.response?.headers?.['retry-after'];
        const retryAfterMs = retryAfter
          ? parseInt(retryAfter, 10) * 1000
          : 60_000;

        console.warn(
          `[Gemini] Rate limit e porlam — ${latencyMs}ms theke, retry after: ${retryAfterMs}ms`,
        );
        throw new RateLimitError(PROVIDER_NAME, retryAfterMs);
      }

      console.error(
        `[Gemini] API error — status: ${statusCode}, message: ${errorMessage}, latency: ${latencyMs}ms`,
      );
      throw new ProviderError(PROVIDER_NAME, errorMessage, statusCode);
    }

    console.error(`[Gemini] Unknown error — ${String(err)}, latency: ${latencyMs}ms`);
    throw new ProviderError(PROVIDER_NAME, `Unknown error: ${String(err)}`);
  }
}

// ---------------------------------------------------------------------------
// Lightweight ping — used by HealthMonitor for latency checks
// ---------------------------------------------------------------------------

export async function pingGemini(apiKey: string): Promise<number> {
  const start = Date.now();
  await callGemini(
    {
      messages: [{ role: 'user', content: 'Reply with the single word: pong' }],
      max_tokens: 5,
      temperature: 0,
    },
    apiKey,
  );
  return Date.now() - start;
}

// Shared streaming harness — the 8-step pipeline described in INFERENCE.md.
//
// The harness:
//   1. Opens an HTTP connection with the adapter's built request
//   2. Parses the SSE byte stream into data lines
//   3. Passes each data line to the adapter's response parser
//   4. Accumulates partial message state from parser output
//   5. Emits events on the common event protocol
//   6. Checks AbortSignal between chunks
//   7. On error: classifies, emits inference.error, cleans up
//   8. On completion: emits inference.usage + inference.done
//
// Provider adapters never touch SSE parsing, connection lifecycle, abort
// handling, or event emission. They translate request/response shapes.

import { type } from "arktype";

import type {
  ConversationMessage,
  InferenceEvent,
  InferenceOptions,
  PartialMessage,
  ProviderConfig,
  TokenUsage,
  AssistantMessage,
  ContentBlock,
} from "@interchange/types/runtime";

import { parseSSE } from "./sse";
import { lookupProvider } from "./providers/registry";
import {
  classifyHTTPError,
  classifyNetworkError,
  classifyAbortError,
  classifyStreamError,
} from "./errors";

export type InferenceHarnessOptions = {
  messages: ConversationMessage[];
  model: string;
  providerConfig: ProviderConfig;
  inferenceOptions?: InferenceOptions;
  signal?: AbortSignal;
  // Sequence number allocator — called once per event to get the next seq.
  nextSeq: () => number;
};

export async function* runInference(
  opts: InferenceHarnessOptions,
): AsyncIterable<InferenceEvent> {
  const {
    messages,
    model,
    providerConfig,
    inferenceOptions = {},
    signal,
    nextSeq,
  } = opts;

  // Emit inference.start immediately.
  yield { type: "inference.start", seq: nextSeq(), data: { model } };

  // Mutable partial state — the harness owns this.
  const partial: PartialMessage = { text: "" };
  let thinkingBuffer = "";
  let usageSeen: TokenUsage | null = null;

  // Tool call state: keyed by callId (or index for OpenAI).
  type ToolCallState = {
    callId: string;
    name: string;
    argsBuffer: string;
  };
  const openToolCalls = new Map<string, ToolCallState>();
  // OpenAI uses index-based tracking before we have a real callId.
  const indexToCallId = new Map<string, string>();

  if (signal?.aborted) {
    yield {
      type: "inference.error",
      seq: nextSeq(),
      data: { error: classifyAbortError(), partial: snapshotPartial(partial) },
    };
    return;
  }

  let adapter;
  try {
    adapter = lookupProvider(providerConfig.provider);
  } catch (cause) {
    yield {
      type: "inference.error",
      seq: nextSeq(),
      data: {
        error: {
          category: "fatal",
          message:
            cause instanceof Error
              ? cause.message
              : `Unknown provider: ${providerConfig.provider}`,
        },
        partial: snapshotPartial(partial),
      },
    };
    return;
  }

  let builtRequest;
  try {
    builtRequest = adapter.buildRequest(messages, model, inferenceOptions);
  } catch (cause) {
    yield {
      type: "inference.error",
      seq: nextSeq(),
      data: {
        error: classifyNetworkError(cause),
        partial: snapshotPartial(partial),
      },
    };
    return;
  }

  // Resolve the full URL and inject credentials.
  const url = resolveURL(builtRequest.url, providerConfig.baseURL);
  const headers = injectCredentials(builtRequest.headers, providerConfig);

  let response: Response;
  try {
    response = await fetch(url, {
      method: "POST",
      headers,
      body: builtRequest.body,
      ...(signal !== undefined ? { signal } : {}),
    });
  } catch (cause) {
    if (signal?.aborted) {
      yield {
        type: "inference.error",
        seq: nextSeq(),
        data: {
          error: classifyAbortError(),
          partial: snapshotPartial(partial),
        },
      };
      return;
    }
    yield {
      type: "inference.error",
      seq: nextSeq(),
      data: {
        error: classifyNetworkError(cause),
        partial: snapshotPartial(partial),
      },
    };
    return;
  }

  if (!response.ok) {
    let errorBody: unknown;
    try {
      errorBody = await response.json();
    } catch {
      try {
        errorBody = await response.text();
      } catch {
        errorBody = undefined;
      }
    }
    const errorMessage = extractErrorMessage(errorBody) ?? response.statusText;
    yield {
      type: "inference.error",
      seq: nextSeq(),
      data: {
        error: classifyHTTPError(response.status, errorMessage, errorBody),
        partial: snapshotPartial(partial),
      },
    };
    return;
  }

  if (response.body === null) {
    yield {
      type: "inference.error",
      seq: nextSeq(),
      data: {
        error: classifyNetworkError(new Error("Response body is null")),
        partial: snapshotPartial(partial),
      },
    };
    return;
  }

  try {
    for await (const sseData of parseSSE(response.body)) {
      if (signal?.aborted) {
        yield {
          type: "inference.error",
          seq: nextSeq(),
          data: {
            error: classifyAbortError(),
            partial: snapshotPartial(partial),
          },
        };
        return;
      }

      const rawEvents = adapter.parseResponse(sseData);

      for (const raw of rawEvents) {
        switch (raw.type) {
          case "inference.text.delta": {
            partial.text += raw.data.token;
            yield {
              type: "inference.text.delta",
              seq: nextSeq(),
              data: {
                token: raw.data.token,
                partial: snapshotPartial(partial),
              },
            };
            break;
          }

          case "inference.thinking.delta": {
            thinkingBuffer += raw.data.token;
            partial.thinking = thinkingBuffer;
            yield {
              type: "inference.thinking.delta",
              seq: nextSeq(),
              data: {
                token: raw.data.token,
                partial: snapshotPartial(partial),
              },
            };
            break;
          }

          case "inference.tool_call.start": {
            const { callId, name } = raw.data;
            openToolCalls.set(callId, { callId, name, argsBuffer: "" });
            // OpenAI sends deltas with a string index ("0", "1", ...) as the
            // callId. Map each index to the real callId so deltas resolve.
            // The index is the position of this tool call in the current batch.
            indexToCallId.set(String(openToolCalls.size - 1), callId);
            partial.toolCalls = [
              ...(partial.toolCalls ?? []),
              {
                id: callId,
                name,
                partialArguments: "",
              },
            ];
            yield {
              type: "inference.tool_call.start",
              seq: nextSeq(),
              data: { callId, name, partial: snapshotPartial(partial) },
            };
            break;
          }

          case "inference.tool_call.delta": {
            const { callId, argumentFragment } = raw.data;

            // Resolve index-based callId to real callId if we have a mapping.
            const resolvedId = indexToCallId.get(callId) ?? callId;
            const tc = openToolCalls.get(resolvedId);
            if (tc !== undefined) {
              tc.argsBuffer += argumentFragment;
              // Update partial.toolCalls entry.
              if (partial.toolCalls !== undefined) {
                for (const ptc of partial.toolCalls) {
                  if (ptc.id === resolvedId) {
                    ptc.partialArguments = tc.argsBuffer;
                    break;
                  }
                }
              }
              yield {
                type: "inference.tool_call.delta",
                seq: nextSeq(),
                data: {
                  callId: resolvedId,
                  argumentFragment,
                  partial: snapshotPartial(partial),
                },
              };
            }
            break;
          }

          case "inference.usage": {
            // Accumulate usage — providers may send multiple usage events
            // (e.g., Anthropic sends one at message_start, one at message_delta).
            usageSeen = mergeUsage(usageSeen, raw.data.usage);
            yield {
              type: "inference.usage",
              seq: nextSeq(),
              data: { usage: raw.data.usage },
            };
            break;
          }

          // inference.done and inference.error from adapters are unexpected —
          // the harness emits those itself. Ignore them.
          default:
            break;
        }
      }
    }
  } catch (cause) {
    if (signal?.aborted) {
      yield {
        type: "inference.error",
        seq: nextSeq(),
        data: {
          error: classifyAbortError(),
          partial: snapshotPartial(partial),
        },
      };
      return;
    }
    yield {
      type: "inference.error",
      seq: nextSeq(),
      data: {
        error: classifyStreamError(cause),
        partial: snapshotPartial(partial),
      },
    };
    return;
  }

  // Finalize any open tool calls that never received an explicit end event.
  const completedToolCalls: ContentBlock[] = [];
  for (const tc of openToolCalls.values()) {
    let parsedArgs: Record<string, unknown>;
    try {
      const raw = tc.argsBuffer.trim() === "" ? "{}" : tc.argsBuffer;
      const parsed = JSON.parse(raw);
      const validated = ParsedToolArgs(parsed);
      parsedArgs = validated instanceof type.errors ? {} : validated;
    } catch {
      parsedArgs = { _raw: tc.argsBuffer };
    }

    completedToolCalls.push({
      type: "tool_call",
      id: tc.callId,
      name: tc.name,
      arguments: parsedArgs,
    });

    yield {
      type: "inference.tool_call.end",
      seq: nextSeq(),
      data: {
        callId: tc.callId,
        name: tc.name,
        arguments: parsedArgs,
        partial: snapshotPartial(partial),
      },
    };
  }

  const finalUsage: TokenUsage = usageSeen ?? {
    input: 0,
    output: 0,
    cacheRead: 0,
    cacheWrite: 0,
    thinking: 0,
  };

  // Emit inference.usage before inference.done per the protocol spec.
  if (usageSeen === null) {
    yield {
      type: "inference.usage",
      seq: nextSeq(),
      data: { usage: finalUsage },
    };
  }

  // Build the final assistant message.
  const contentBlocks: ContentBlock[] = [];
  if (thinkingBuffer.length > 0) {
    contentBlocks.push({ type: "thinking", thinking: thinkingBuffer });
  }
  if (partial.text.length > 0) {
    contentBlocks.push({ type: "text", text: partial.text });
  }
  contentBlocks.push(...completedToolCalls);

  const finalMessage: AssistantMessage = {
    role: "assistant",
    content: contentBlocks,
    model,
  };

  yield {
    type: "inference.done",
    seq: nextSeq(),
    data: { message: finalMessage, usage: finalUsage },
  };
}

function snapshotPartial(partial: PartialMessage): PartialMessage {
  return {
    text: partial.text,
    ...(partial.thinking !== undefined ? { thinking: partial.thinking } : {}),
    ...(partial.toolCalls !== undefined
      ? {
          toolCalls: partial.toolCalls.map((tc) => ({
            id: tc.id,
            name: tc.name,
            partialArguments: tc.partialArguments,
          })),
        }
      : {}),
  };
}

function mergeUsage(
  existing: TokenUsage | null,
  incoming: TokenUsage,
): TokenUsage {
  if (existing === null) return incoming;
  return {
    input: existing.input + incoming.input,
    output: existing.output + incoming.output,
    cacheRead: existing.cacheRead + incoming.cacheRead,
    cacheWrite: existing.cacheWrite + incoming.cacheWrite,
    thinking: existing.thinking + incoming.thinking,
  };
}

function resolveURL(path: string, baseURL: string): string {
  if (path.startsWith("http://") || path.startsWith("https://")) {
    return path;
  }
  const base = baseURL.endsWith("/") ? baseURL.slice(0, -1) : baseURL;
  return base + path;
}

function injectCredentials(
  headers: Record<string, string>,
  config: ProviderConfig,
): Record<string, string> {
  const result = { ...headers };

  if ("x-api-key" in result) {
    result["x-api-key"] = config.apiKey;
  }

  if ("authorization" in result) {
    result["authorization"] = `Bearer ${config.apiKey}`;
  }

  return result;
}

const ParsedToolArgs = type("Record<string, unknown>");

const ErrorBody = type({ error: { message: "string" } });
const DirectMessageBody = type({ message: "string" });

function extractErrorMessage(body: unknown): string | null {
  // Anthropic/OpenAI: { error: { message: "..." } }
  const errorBody = ErrorBody(body);
  if (!(errorBody instanceof type.errors)) {
    return errorBody.error.message;
  }

  // Direct message field as fallback.
  const directBody = DirectMessageBody(body);
  if (!(directBody instanceof type.errors)) {
    return directBody.message;
  }

  return null;
}

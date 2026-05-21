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
  ConversationTurn,
  InferenceEvent,
  InferenceOptions,
  PartialMessage,
  ProviderConfig,
  TokenUsage,
  AssistantTurn,
  ContentBlock,
} from "@intx/types/runtime";

import { parseSSE } from "./sse";
import { lookupProvider } from "./providers/registry";
import {
  classifyHTTPError,
  classifyNetworkError,
  classifyAbortError,
  classifyStreamError,
  classifyTimeoutError,
} from "./errors";

/**
 * Default per-call inactivity timeout (ms). Two minutes is conservative
 * for reasoning-heavy models that emit `inference.thinking.delta` tokens
 * regularly when actually working — sustained silence past this means
 * the provider stream has genuinely stalled, not that the model is
 * thinking. Operators can tune via `InferenceOptions.inactivityTimeoutMs`.
 */
export const DEFAULT_INACTIVITY_TIMEOUT_MS = 120_000;

/**
 * Default per-call total wall-clock cap (ms). Matches Anthropic's
 * documented per-call recommendation and fits within typical CI
 * timeouts. Operators can tune via `InferenceOptions.totalTimeoutMs`.
 */
export const DEFAULT_TOTAL_TIMEOUT_MS = 600_000;

export const HarnessId: unique symbol = Symbol("HarnessId");

/**
 * Runtime dependencies injected into `runInference`. Code-only — not part of
 * any persisted schema. Test harnesses substitute `fetch` (and stamp the
 * `[HarnessId]` tag for per-harness identity) so production `runInference`
 * never reaches `globalThis.fetch`.
 *
 * `fetch` is intentionally typed as a plain function rather than
 * `typeof globalThis.fetch` — the latter is augmented per-runtime (Bun adds
 * `preconnect`; Node and the DOM lib do not) and `runInference` only ever
 * invokes the call signature.
 *
 * The `[HarnessId]` tag is enumerable via `Object.getOwnPropertySymbols`
 * (and `Reflect.ownKeys`, which is the superset). Do not pass `Dependencies`
 * instances through reflective serializers or expose them across trust
 * boundaries. (`JSON.stringify` is safe — it walks string keys only.)
 */
export type Dependencies = {
  readonly fetch: (
    input: string | URL | Request,
    init?: RequestInit,
  ) => Promise<Response>;
  /**
   * Time-based scheduler used by the harness's per-call timeouts (see
   * `InferenceOptions.inactivityTimeoutMs` / `totalTimeoutMs`). Production
   * passes the default wrapper around `setTimeout` / `clearTimeout`; the
   * deterministic test harness injects a scheduler that wraps its virtual
   * clock so timeout tests fire at virtual-time-N without sleeping real
   * wall-clock. Optional for backward compatibility — when omitted the
   * harness substitutes the production default.
   */
  readonly scheduler?: Scheduler;
  readonly [HarnessId]?: symbol;
};

/**
 * Minimal scheduling abstraction. `setTimeout` returns a canceller; the
 * canceller is idempotent (multiple calls are safe). The harness uses
 * this for both the inactivity timer (which is re-armed on every event)
 * and the total wall-clock cap.
 */
export type Scheduler = {
  setTimeout(callback: () => void, delayMs: number): () => void;
};

export function createDefaultScheduler(): Scheduler {
  return {
    setTimeout(callback, delayMs) {
      const handle = setTimeout(callback, delayMs);
      return () => {
        clearTimeout(handle);
      };
    },
  };
}

export function createDefaultDependencies(): Dependencies {
  return {
    fetch: globalThis.fetch.bind(globalThis),
    scheduler: createDefaultScheduler(),
  };
}

export type InferenceHarnessOptions = {
  turns: ConversationTurn[];
  model: string;
  providerConfig: ProviderConfig;
  inferenceOptions?: InferenceOptions;
  signal?: AbortSignal;
  // Sequence number allocator — called once per event to get the next seq.
  nextSeq: () => number;
  deps: Dependencies;
};

export async function* runInference(
  opts: InferenceHarnessOptions,
): AsyncIterable<InferenceEvent> {
  const {
    turns,
    model,
    providerConfig,
    inferenceOptions = {},
    signal,
    nextSeq,
    deps,
  } = opts;

  // Defensive guard for callers that bypass the type system (JS, `any`,
  // unchecked casts). Missing or malformed `deps.fetch` is a programmer
  // bug — surface it as an unhandled throw before any `yield`, so it does
  // not get caught by the network try/catch below and misclassified as a
  // retryable transport failure that callers might paper over with retries.
  if (typeof deps?.fetch !== "function") {
    throw new Error(
      `runInference: deps.fetch must be a function (got ${typeof deps?.fetch}); pass createDefaultDependencies() or a test harness Dependencies object`,
    );
  }

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
    builtRequest = adapter.buildRequest(turns, model, inferenceOptions);
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

  // Per-call timeouts. The inactivity timer fires when the harness
  // hasn't yielded an event for `inactivityTimeoutMs`; the total timer
  // is a wall-clock cap from fetch onwards. We own one AbortController,
  // combine its signal with the caller's, and attribute the abort to
  // whichever timer fired by checking `timeoutReason` at the catch site.
  const inactivityTimeoutMs =
    inferenceOptions.inactivityTimeoutMs ?? DEFAULT_INACTIVITY_TIMEOUT_MS;
  const totalTimeoutMs =
    inferenceOptions.totalTimeoutMs ?? DEFAULT_TOTAL_TIMEOUT_MS;
  const scheduler = deps.scheduler ?? createDefaultScheduler();
  const timeoutAbort = new AbortController();
  let timeoutReason: "inactivity" | "total" | null = null;
  let cancelInactivity: (() => void) | null = null;
  const armInactivity = () => {
    cancelInactivity?.();
    cancelInactivity = scheduler.setTimeout(() => {
      timeoutReason = "inactivity";
      timeoutAbort.abort();
    }, inactivityTimeoutMs);
  };
  const cancelTotal = scheduler.setTimeout(() => {
    timeoutReason = "total";
    timeoutAbort.abort();
  }, totalTimeoutMs);
  const cleanupTimers = () => {
    cancelTotal();
    cancelInactivity?.();
    cancelInactivity = null;
  };
  // Combined signal: the production code's existing caller-signal +
  // our timeout controller, so a fetch implementation that respects
  // AbortSignal sees both.
  const fetchSignal = combineSignals(signal, timeoutAbort.signal);

  let response: Response;
  try {
    response = await deps.fetch(url, {
      method: "POST",
      headers,
      body: builtRequest.body,
      signal: fetchSignal,
    });
  } catch (cause) {
    cleanupTimers();
    if (timeoutReason !== null) {
      const thresholdMs =
        timeoutReason === "inactivity" ? inactivityTimeoutMs : totalTimeoutMs;
      yield {
        type: "inference.error",
        seq: nextSeq(),
        data: {
          error: classifyTimeoutError(timeoutReason, thresholdMs),
          partial: snapshotPartial(partial),
        },
      };
      return;
    }
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
    const retryAfterMs = adapter.extractRetryAfterMs?.(response.headers);
    yield {
      type: "inference.error",
      seq: nextSeq(),
      data: {
        error: classifyHTTPError(
          response.status,
          errorMessage,
          errorBody,
          retryAfterMs,
        ),
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

  // Arm the inactivity timer now that the SSE stream is open. Every
  // event we yield below resets it; sustained silence past
  // `inactivityTimeoutMs` aborts the controller and the loop's catch
  // surfaces the timeout error.
  armInactivity();

  try {
    for await (const sseData of parseSSE(response.body)) {
      if (timeoutReason !== null) {
        // The timeout aborted the stream; bubble up the right error
        // shape rather than letting the abort masquerade as a
        // caller-initiated cancellation.
        const thresholdMs =
          timeoutReason === "inactivity" ? inactivityTimeoutMs : totalTimeoutMs;
        yield {
          type: "inference.error",
          seq: nextSeq(),
          data: {
            error: classifyTimeoutError(timeoutReason, thresholdMs),
            partial: snapshotPartial(partial),
          },
        };
        cleanupTimers();
        return;
      }
      if (signal?.aborted) {
        yield {
          type: "inference.error",
          seq: nextSeq(),
          data: {
            error: classifyAbortError(),
            partial: snapshotPartial(partial),
          },
        };
        cleanupTimers();
        return;
      }

      // Reset inactivity timer — we just got something from the wire.
      armInactivity();

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
    if (timeoutReason !== null) {
      const thresholdMs =
        timeoutReason === "inactivity" ? inactivityTimeoutMs : totalTimeoutMs;
      yield {
        type: "inference.error",
        seq: nextSeq(),
        data: {
          error: classifyTimeoutError(timeoutReason, thresholdMs),
          partial: snapshotPartial(partial),
        },
      };
      cleanupTimers();
      return;
    }
    if (signal?.aborted) {
      yield {
        type: "inference.error",
        seq: nextSeq(),
        data: {
          error: classifyAbortError(),
          partial: snapshotPartial(partial),
        },
      };
      cleanupTimers();
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
    cleanupTimers();
    return;
  }

  // SSE loop exited normally — disarm timers before the finalization
  // path emits any further events. (Both timers are no-op-safe after
  // cleanup, but stopping them avoids spurious firings during a slow
  // finalization step.)
  cleanupTimers();

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

  const finalTurn: AssistantTurn = {
    role: "assistant",
    content: contentBlocks,
    model,
    timestamp: Date.now(),
  };

  const pacingDelayMs = adapter.extractPacingDelayMs?.(response.headers);

  yield {
    type: "inference.done",
    seq: nextSeq(),
    data: {
      turn: finalTurn,
      usage: finalUsage,
      ...(pacingDelayMs !== undefined && pacingDelayMs > 0
        ? { pacingDelayMs }
        : {}),
    },
  };
}

/**
 * Combine an optional caller-supplied `AbortSignal` with the harness's
 * internal timeout-driven controller into a single signal the fetch
 * implementation can observe. Returns the internal controller's signal
 * alone if no caller signal exists; otherwise wires both so that either
 * one firing aborts the combined signal.
 */
function combineSignals(
  caller: AbortSignal | undefined,
  internal: AbortSignal,
): AbortSignal {
  if (caller === undefined) return internal;
  const composite = new AbortController();
  if (caller.aborted) composite.abort(caller.reason);
  else
    caller.addEventListener("abort", () => composite.abort(caller.reason), {
      once: true,
    });
  if (internal.aborted) composite.abort(internal.reason);
  else
    internal.addEventListener("abort", () => composite.abort(internal.reason), {
      once: true,
    });
  return composite.signal;
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

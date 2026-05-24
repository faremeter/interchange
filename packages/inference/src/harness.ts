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
  InferenceSource,
  PartialMessage,
  RedactedThinkingBlock,
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
  ProtocolMismatchError,
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
  source: InferenceSource;
  inferenceOptions?: InferenceOptions;
  signal?: AbortSignal;
  // Sequence number allocator — called once per event to get the next seq.
  nextSeq: () => number;
  deps: Dependencies;
};

export async function* runInference(
  opts: InferenceHarnessOptions,
): AsyncIterable<InferenceEvent> {
  const { turns, source, inferenceOptions, signal, nextSeq, deps } = opts;
  // Per-call options override source-bound defaults. The merge happens
  // here, once, so the adapter and timeout-resolution paths below all
  // see the effective option set without having to remember the
  // precedence rule.
  const effectiveOptions: InferenceOptions = {
    ...(source.defaults ?? {}),
    ...(inferenceOptions ?? {}),
  };
  const model = source.model;

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
  // Cryptographic signature for the thinking block, when the provider
  // emits one. Anthropic requires this signature to be echoed back on
  // any follow-up turn that includes the thinking block in history; see
  // `inference.thinking.signature` in `runtime.ts`.
  let thinkingSignature: string | undefined;
  // Redacted thinking blocks delivered as one-shots inside
  // content_block_start. Each block's opaque `data` blob must echo back
  // verbatim on follow-up turns; the harness preserves insertion order
  // here so the final assistant turn carries them in the same sequence
  // they arrived on the wire.
  const redactedThinkingBlocks: RedactedThinkingBlock[] = [];
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
    adapter = lookupProvider(source.provider);
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
              : `Unknown provider: ${source.provider}`,
        },
        partial: snapshotPartial(partial),
      },
    };
    return;
  }

  let builtRequest;
  try {
    builtRequest = adapter.buildRequest(turns, model, effectiveOptions);
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
  const url = resolveURL(builtRequest.url, source.baseURL);
  const headers = injectCredentials(builtRequest.headers, source);

  // Per-call timeouts. The inactivity timer fires when the harness
  // hasn't yielded an event for `inactivityTimeoutMs`; the total timer
  // is a wall-clock cap from fetch onwards. We own one AbortController,
  // combine its signal with the caller's, and attribute the abort to
  // whichever timer fired by checking `timeoutReason` at the catch site.
  const inactivityTimeoutMs =
    effectiveOptions.inactivityTimeoutMs ?? DEFAULT_INACTIVITY_TIMEOUT_MS;
  const totalTimeoutMs =
    effectiveOptions.totalTimeoutMs ?? DEFAULT_TOTAL_TIMEOUT_MS;
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
  // Per-timer cancellers are idempotent (the production scheduler's
  // canceller wraps `clearTimeout`, which no-ops on a fired timer; the
  // test scheduler's canceller flips a `cancelled` flag). Callers may
  // invoke `cleanupTimers` exactly once; the `try/finally` around the
  // generator body below is the single owner of that lifecycle.
  const cleanupTimers = (): void => {
    cancelTotal();
    cancelInactivity?.();
    cancelInactivity = null;
  };
  // Combined signal: the production code's existing caller-signal +
  // our timeout controller, so a fetch implementation that respects
  // AbortSignal sees both. `cleanupSignal` removes the abort listeners
  // `combineSignals` installs on the caller signal so a long-lived
  // caller signal (e.g., a session-scoped controller) does not
  // accumulate one un-removed listener per call.
  const { signal: fetchSignal, cleanup: cleanupSignal } = combineSignals(
    signal,
    timeoutAbort.signal,
  );

  try {
    let response: Response;
    try {
      response = await deps.fetch(url, {
        method: "POST",
        headers,
        body: builtRequest.body,
        signal: fetchSignal,
      });
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
      // Read the body as text once and then try to parse it as JSON.
      // Calling `.json()` first and falling back to `.text()` on the
      // same response does not work — per WHATWG fetch the body stream
      // is locked/disturbed by the first read attempt, so the fallback
      // throws `TypeError: body already consumed` and `errorBody` ends
      // up `undefined`. Reading text-then-parsing covers both JSON and
      // plain-text error bodies in a single pass.
      //
      // The read is bound to the combined fetch signal so a hostile
      // server returning a 4xx/5xx with a body that never terminates
      // cannot hang the call past the total-timeout horizon.
      let errorBody: unknown;
      try {
        const text = await awaitWithSignal(response.text(), fetchSignal);
        try {
          errorBody = JSON.parse(text);
        } catch {
          errorBody = text;
        }
      } catch {
        errorBody = undefined;
      }
      const errorMessage =
        extractErrorMessage(errorBody) ?? response.statusText;
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
            timeoutReason === "inactivity"
              ? inactivityTimeoutMs
              : totalTimeoutMs;
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

        // Reset inactivity timer — we just got something from the wire.
        armInactivity();

        const rawEvents = adapter.parseResponse(sseData);

        for (const raw of rawEvents) {
          switch (raw.type) {
            case "inference.text.delta": {
              guardNonZeroIndex(raw, "text");
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
              guardNonZeroIndex(raw, "thinking");
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

            case "inference.thinking.signature": {
              // Anthropic emits the signature once per thinking block, after
              // the thinking_delta stream. The harness collapses all thinking
              // content into a single block today, so a single trailing
              // signature is captured here and attached at finalisation.
              guardNonZeroIndex(raw, "signature");
              thinkingSignature = raw.data.signature;
              yield {
                type: "inference.thinking.signature",
                seq: nextSeq(),
                data: { signature: raw.data.signature },
              };
              break;
            }

            case "inference.thinking.redacted": {
              // Redacted thinking blocks arrive as one-shots inside
              // content_block_start (no delta stream). Preserve insertion
              // order so multi-block scenarios reproduce the wire order in
              // the final assistant turn. The block's `data` is opaque
              // base64 that must echo back verbatim on every follow-up
              // turn — passing it through the runtime type rather than
              // re-serializing protects it from any accidental mutation.
              //
              // The conditional spread on `index` is forced by the
              // runtime type's `"index?": "number"`: the field is
              // omit-or-number, not omit-or-number-or-undefined, so
              // passing `raw.data.index` directly when it could be
              // undefined fails TypeScript narrowing. Today's parser
              // always sets it, but the harness accepts other emitters
              // that may not.
              guardNonZeroIndex(raw, "redacted_thinking");
              redactedThinkingBlocks.push(raw.data.redactedThinking);
              yield {
                type: "inference.thinking.redacted",
                seq: nextSeq(),
                data: {
                  redactedThinking: raw.data.redactedThinking,
                  ...(raw.data.index !== undefined
                    ? { index: raw.data.index }
                    : {}),
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
      contentBlocks.push({
        type: "thinking",
        thinking: thinkingBuffer,
        ...(thinkingSignature !== undefined
          ? { signature: thinkingSignature }
          : {}),
      });
    }
    // Redacted thinking blocks land before the assistant text on the
    // wire (Anthropic streams them ahead of any text block); preserving
    // that order is what lets the request builder echo them back in the
    // same position on the next turn.
    contentBlocks.push(...redactedThinkingBlocks);
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
  } finally {
    // Single owner of the timer + signal-listener lifecycle. Runs on
    // every exit including normal completion, early `return`, thrown
    // errors, and consumer abandonment via `for await` `break`
    // (which invokes the generator's `return()` and triggers the
    // finally). Both cleanups are idempotent.
    cleanupTimers();
    cleanupSignal();
  }
}

/**
 * Combine an optional caller-supplied `AbortSignal` with the harness's
 * internal timeout-driven controller into a single signal the fetch
 * implementation can observe. Returns the internal controller's signal
 * alone if no caller signal exists; otherwise wires both so that either
 * one firing aborts the combined signal.
 *
 * Returns a bundle containing the signal AND an explicit cleanup
 * function. `{ once: true }` on the abort listeners only auto-removes
 * after firing, so on the happy path (no abort) the listeners would
 * accumulate against a long-lived caller signal — one un-removed
 * listener per `runInference` call. The caller MUST invoke
 * `cleanup()` exactly once when the call's interest in the signal
 * ends (whether by completion, error, or abandonment); the harness
 * does this from its `try/finally` block. `cleanup()` is idempotent.
 */
type CombinedSignal = {
  readonly signal: AbortSignal;
  readonly cleanup: () => void;
};

function combineSignals(
  caller: AbortSignal | undefined,
  internal: AbortSignal,
): CombinedSignal {
  if (caller === undefined) {
    const noopCleanup = (): void => {
      /* no listener was attached */
    };
    return { signal: internal, cleanup: noopCleanup };
  }
  const composite = new AbortController();
  const onCallerAbort = (): void => {
    composite.abort(caller.reason);
  };
  const onInternalAbort = (): void => {
    composite.abort(internal.reason);
  };
  let cleanedUp = false;
  const cleanup = (): void => {
    if (cleanedUp) return;
    cleanedUp = true;
    caller.removeEventListener("abort", onCallerAbort);
    internal.removeEventListener("abort", onInternalAbort);
  };
  if (caller.aborted) {
    composite.abort(caller.reason);
  } else {
    caller.addEventListener("abort", onCallerAbort, { once: true });
  }
  if (internal.aborted) {
    composite.abort(internal.reason);
  } else {
    internal.addEventListener("abort", onInternalAbort, { once: true });
  }
  return { signal: composite.signal, cleanup };
}

/**
 * Await `promise` but reject early if `signal` aborts in the meantime.
 * Used for non-streaming reads of the error response body so a hostile
 * server cannot hang the call by returning a 4xx/5xx with a body that
 * never terminates. The signal's listener is always removed before
 * settlement so this helper does not itself leak listeners.
 */
async function awaitWithSignal<T>(
  promise: Promise<T>,
  signal: AbortSignal,
): Promise<T> {
  if (signal.aborted) {
    throw new DOMException("aborted", "AbortError");
  }
  return new Promise<T>((resolve, reject) => {
    const onAbort = (): void => {
      reject(new DOMException("aborted", "AbortError"));
    };
    signal.addEventListener("abort", onAbort, { once: true });
    promise.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (err: unknown) => {
        signal.removeEventListener("abort", onAbort);
        reject(err instanceof Error ? err : new Error(String(err)));
      },
    );
  });
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

// Guards the single-buffer text accumulator, the single-buffer thinking
// accumulator, and the single-slot thinking-signature capture against
// non-zero block indices. Per-block routing requires a per-index
// accumulator that does not yet exist in the harness; without it,
// deltas from different blocks would silently concatenate into the
// same buffer (or, for the signature, the later block would overwrite
// the earlier). The guard makes that failure surface loudly.
//
// Thrown as a `ProtocolMismatchError` so the harness's stream-error
// catch routes it through `classifyStreamError` to the
// `"protocol_mismatch"` category — non-retryable — rather than the
// generic-Error `"retryable"` fallback that would invite a retry loop
// to mask a deterministic wiring bug.
function guardNonZeroIndex(
  event: {
    type: string;
    data: { index?: number };
  },
  bufferName: string,
): void {
  const index = event.data.index;
  if (index !== undefined && index !== 0) {
    throw new ProtocolMismatchError(
      `harness received ${event.type} carrying index=${String(index)}; ` +
        `the single-buffer ${bufferName} accumulator cannot route per-index ` +
        `deltas. The per-index harness refactor must precede parsers that ` +
        `emit non-zero indices.`,
      event,
    );
  }
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
  source: InferenceSource,
): Record<string, string> {
  const result = { ...headers };

  if ("x-api-key" in result) {
    result["x-api-key"] = source.apiKey;
  }

  if ("authorization" in result) {
    result["authorization"] = `Bearer ${source.apiKey}`;
  }

  return result;
}

const ParsedToolArgs = type("Record<string, unknown>");

const ErrorBody = type({ error: { message: "string" } });
const DirectMessageBody = type({ message: "string" });

/**
 * Upper bound on the length of a plain-text error body that gets
 * promoted to `InferenceError.message`. Bodies longer than this are
 * truncated with a marker pointing operators at `error.raw`, which
 * always retains the untruncated body. Structured JSON envelopes are
 * not subject to this cap — their `message` fields are server-curated
 * and concise in practice.
 *
 * 500 characters covers a multi-line stack trace or a paragraph of
 * diagnostic text without blowing up the default director's
 * user-facing reply (which concatenates the message into a chat-style
 * string) or the timeline part stored by the hub event collector.
 */
const MAX_PLAIN_TEXT_MESSAGE_CHARS = 500;

function truncatePlainTextMessage(text: string): string {
  if (text.length <= MAX_PLAIN_TEXT_MESSAGE_CHARS) return text;
  return `${text.slice(0, MAX_PLAIN_TEXT_MESSAGE_CHARS)}… (truncated; full body in error.raw)`;
}

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

  // Plain-text error bodies (HTML error pages, raw exception strings,
  // load-balancer diagnostics). The body reaches us via the
  // text-then-parse path in the `!response.ok` branch: when
  // JSON.parse failed, the raw string is stored as errorBody.
  // Surfacing it here means the operator-visible message contains
  // the server's actual diagnostic rather than just `statusText`.
  // `error.raw` always holds the untruncated body for audit-time
  // inspection.
  if (typeof body === "string" && body.length > 0) {
    return truncatePlainTextMessage(body);
  }

  return null;
}

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
  CitationBlock,
  ConversationTurn,
  InferenceEvent,
  InferenceOptions,
  InferenceSource,
  PartialMessage,
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
  // Per-index block tracking. The map preserves insertion order (JS
  // Map guarantee, even for integer keys — unlike plain objects).
  // Each entry records one block's running state; final-turn
  // assembly walks the map in arrival order and emits one ContentBlock
  // per entry. The `tool_use` entries are index markers only — the
  // tool-call state machine lives in `openToolCalls` /
  // `completedToolCalls` and is resolved into the final block at
  // assembly time via the marker's `callId`.
  type BlockState =
    | { kind: "text"; text: string }
    | { kind: "thinking"; text: string; signature?: string }
    | { kind: "redacted_thinking"; data: string }
    | { kind: "tool_use"; callId: string };
  const blockMap = new Map<number, BlockState>();
  // Citations streamed from the provider, in arrival order. The
  // `inference.citation` event today doesn't carry the source content
  // block index (citations attribute to the nearest preceding
  // TextBlock per the CitationBlock docstring), so citations are
  // collected as a flat array and appended after the per-index walk
  // in final-turn assembly.
  const citationBlocks: CitationBlock[] = [];
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
              const idx = requireIndex(raw, "text.delta");
              const existing = blockMap.get(idx);
              if (existing === undefined) {
                blockMap.set(idx, { kind: "text", text: raw.data.token });
              } else if (existing.kind === "text") {
                existing.text += raw.data.token;
              } else {
                throw new ProtocolMismatchError(
                  `harness: text.delta at index ${String(idx)} collides with existing ${existing.kind} block`,
                  raw,
                );
              }
              // Running concat of all text deltas — backwards
              // compatible with consumers that treat `partial.text` as
              // "everything the assistant has typed so far," regardless
              // of which content block it came from.
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
              const idx = requireIndex(raw, "thinking.delta");
              const existing = blockMap.get(idx);
              if (existing === undefined) {
                blockMap.set(idx, { kind: "thinking", text: raw.data.token });
              } else if (existing.kind === "thinking") {
                existing.text += raw.data.token;
              } else {
                throw new ProtocolMismatchError(
                  `harness: thinking.delta at index ${String(idx)} collides with existing ${existing.kind} block`,
                  raw,
                );
              }
              // Running concat of all thinking deltas across every
              // thinking block. Under interleaving (thinking@0 "A",
              // text@1 "X", thinking@2 "B"), `partial.thinking` ends
              // up "AB" — backwards compatible with the pre-per-index
              // single-buffer semantics. Consumers needing per-block
              // structure walk the finalized turn's `content[]`.
              const concat = (partial.thinking ?? "") + raw.data.token;
              partial.thinking = concat;
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
              const idx = requireIndex(raw, "thinking.signature");
              const existing = blockMap.get(idx);
              if (existing === undefined) {
                throw new ProtocolMismatchError(
                  `harness: thinking.signature at index ${String(idx)} has no preceding thinking block at that index`,
                  raw,
                );
              }
              if (existing.kind !== "thinking") {
                throw new ProtocolMismatchError(
                  `harness: thinking.signature at index ${String(idx)} targets an existing ${existing.kind} block, not a thinking block`,
                  raw,
                );
              }
              existing.signature = raw.data.signature;
              yield {
                type: "inference.thinking.signature",
                seq: nextSeq(),
                data: { signature: raw.data.signature },
              };
              break;
            }

            case "inference.citation": {
              // Citations don't carry a content-block index on their
              // event payload today, so they're collected flat and
              // appended at the end of the final turn per the
              // CitationBlock attribution rule. Index-precise
              // attribution is tracked separately.
              citationBlocks.push(raw.data.citation);
              yield {
                type: "inference.citation",
                seq: nextSeq(),
                data: { citation: raw.data.citation },
              };
              break;
            }

            case "inference.thinking.redacted": {
              const idx = requireIndex(raw, "thinking.redacted");
              const existing = blockMap.get(idx);
              if (existing !== undefined) {
                throw new ProtocolMismatchError(
                  `harness: thinking.redacted at index ${String(idx)} collides with existing ${existing.kind} block`,
                  raw,
                );
              }
              blockMap.set(idx, {
                kind: "redacted_thinking",
                data: raw.data.redactedThinking.data,
              });
              yield {
                type: "inference.thinking.redacted",
                seq: nextSeq(),
                data: {
                  redactedThinking: raw.data.redactedThinking,
                  index: idx,
                },
              };
              break;
            }

            case "inference.tool_call.start": {
              const toolIdx = requireIndex(raw, "tool_call.start");
              const { callId, name } = raw.data;
              openToolCalls.set(callId, { callId, name, argsBuffer: "" });
              // OpenAI-flavoured adapters synthesize a placeholder
              // callId on tool_call.delta events (the real id is only
              // present on the start). Key the resolution map on the
              // start event's `data.index` so the placeholder the
              // delta emits (`String(blockIndex)`) maps back to the
              // real id even when `tcDelta.index` is non-zero or
              // non-contiguous.
              indexToCallId.set(String(toolIdx), callId);
              // Anchor the tool_use position in the per-index map.
              // The map walk in final assembly will resolve the marker
              // via `completedToolCalls` so the tool_use block lands
              // in its wire-arrival position relative to text and
              // thinking blocks. Collisions with another kind at the
              // same index throw, matching the discipline of the
              // text/thinking/redacted_thinking branches above —
              // distinct kinds cannot share an index without losing
              // the per-index ordering guarantee.
              const existingAtIdx = blockMap.get(toolIdx);
              if (existingAtIdx === undefined) {
                blockMap.set(toolIdx, { kind: "tool_use", callId });
              } else if (
                existingAtIdx.kind !== "tool_use" ||
                existingAtIdx.callId !== callId
              ) {
                throw new ProtocolMismatchError(
                  `harness: tool_call.start at index ${String(toolIdx)} collides with existing ${existingAtIdx.kind} block`,
                  raw,
                );
              }
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
              // (e.g., Anthropic sends one at message_start with input
              // tokens, then one at message_delta with output tokens
              // and input deliberately set to 0 by the parser to mean
              // "no change to input"). Emit the cumulative
              // post-merge total rather than the raw incoming so
              // downstream consumers and invariants see a monotone
              // non-decreasing stream — the raw incoming would
              // observably "decrease" input from a real count back
              // to 0 between the two events even though no decrease
              // occurred in the underlying counter.
              usageSeen = mergeUsage(usageSeen, raw.data.usage);
              yield {
                type: "inference.usage",
                seq: nextSeq(),
                data: { usage: usageSeen },
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

    // Build the final assistant message by walking the per-index map
    // in insertion order. JS `Map` preserves insertion order for all
    // keys (including integers — distinct from plain object behaviour),
    // so iteration here reproduces the wire-arrival order of content
    // blocks regardless of the numeric values. Tool-call markers are
    // resolved to the finalized ContentBlock from the completedToolCalls
    // array via the marker's callId.
    const completedToolCallsByCallId = new Map<string, ContentBlock>();
    for (const tc of completedToolCalls) {
      if (tc.type === "tool_call") {
        completedToolCallsByCallId.set(tc.id, tc);
      }
    }
    const contentBlocks: ContentBlock[] = [];
    for (const entry of blockMap.values()) {
      if (entry.kind === "text") {
        if (entry.text.length > 0) {
          contentBlocks.push({ type: "text", text: entry.text });
        }
        continue;
      }
      if (entry.kind === "thinking") {
        // Emit thinking blocks even when text is empty if a signature
        // was captured — Anthropic's redacted-adjacent flow can
        // produce a thinking block whose visible text is empty but
        // whose signature must round-trip on follow-up turns.
        if (entry.text.length === 0 && entry.signature === undefined) {
          continue;
        }
        contentBlocks.push({
          type: "thinking",
          thinking: entry.text,
          ...(entry.signature !== undefined
            ? { signature: entry.signature }
            : {}),
        });
        continue;
      }
      if (entry.kind === "redacted_thinking") {
        contentBlocks.push({ type: "redacted_thinking", data: entry.data });
        continue;
      }
      if (entry.kind === "tool_use") {
        const finalized = completedToolCallsByCallId.get(entry.callId);
        if (finalized === undefined) {
          // Every tool_use marker is added in the
          // inference.tool_call.start handler at the same time the
          // entry is inserted into openToolCalls. The finalize loop
          // above turns every openToolCalls entry into a
          // completedToolCalls entry. So a marker whose callId is
          // missing from completedToolCallsByCallId here would mean
          // the start-time bookkeeping diverged from the finalize-
          // time bookkeeping — surface it loudly rather than dropping
          // the tool call from the final turn.
          throw new ProtocolMismatchError(
            `harness: tool_use marker at callId ${entry.callId} has no matching completed tool call`,
            entry,
          );
        }
        contentBlocks.push(finalized);
        continue;
      }
      entry satisfies never;
    }
    // Citations append after the per-index walk. Their event payload
    // doesn't carry the source content-block index today, so they
    // attribute by adjacency to the nearest preceding TextBlock per
    // the CitationBlock docstring. Index-precise attribution is a
    // separately tracked extension.
    contentBlocks.push(...citationBlocks);

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

// The harness's per-index routing is load-bearing on every delta
// carrying an `index`. Provider adapters synthesize a default at the
// adapter boundary if their wire shape doesn't carry one (e.g.
// OpenAI Chat Completions emits `index: 0` explicitly on text and
// thinking deltas because Chat Completions ships a single content
// block per kind per response). A delta arriving at the harness
// without an index is a wiring bug at the adapter, not data the
// harness should silently route to block 0 — surfacing it as a
// ProtocolMismatchError is the load-bearing alternative to corrupt
// state.
function requireIndex(
  event: {
    type: string;
    data: { index?: number };
  },
  variant: string,
): number {
  const index = event.data.index;
  if (index === undefined) {
    throw new ProtocolMismatchError(
      `harness received ${event.type} (${variant}) without an index; ` +
        `provider adapters must synthesize an index at the boundary even ` +
        `when the wire shape doesn't carry one`,
      event,
    );
  }
  return index;
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

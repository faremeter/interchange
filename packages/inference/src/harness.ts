// Shared streaming harness. Provider adapters never touch SSE parsing,
// connection lifecycle, abort handling, or event emission; they translate
// request/response shapes.

import { type } from "arktype";

import type {
  CitationBlock,
  CodeExecutionRequestBlock,
  CodeExecutionResultBlock,
  ConversationTurn,
  ImageBlock,
  InferenceError,
  InferenceEvent,
  InferenceOptions,
  InferenceSource,
  LastCycleSource,
  PartialMessage,
  RetryDecision,
  SafetyRatingBlock,
  TokenUsage,
  AssistantTurn,
  ContentBlock,
} from "@intx/types/runtime";

import type { CredentialMaterialResolver } from "@intx/types";

import { getLogger } from "@intx/log";

import {
  detectResponseKind,
  type ResponseKind,
} from "@intx/types/content-type";

import type { AdapterRegistry } from "./adapter";
import { parseSSE } from "./sse";
import { injectCredentials } from "./auth";
import {
  classifyHTTPError,
  classifyNetworkError,
  classifyAbortError,
  classifyStreamError,
  classifyTimeoutError,
  classifyProtocolMismatch,
  ProtocolMismatchError,
} from "./errors";
import { createDefaultRetryPolicy } from "./retry-policy";

const logger = getLogger(["interchange", "inference", "harness"]);

/**
 * Default per-call inactivity timeout (ms) — sustained silence past this
 * means the provider stream stalled, not that the model is thinking.
 * Tunable via `InferenceOptions.inactivityTimeoutMs`.
 */
export const DEFAULT_INACTIVITY_TIMEOUT_MS = 120_000;

/**
 * Default per-call total wall-clock cap (ms). Matches Anthropic's
 * documented per-call recommendation and fits within typical CI timeouts.
 */
export const DEFAULT_TOTAL_TIMEOUT_MS = 600_000;

export const HarnessId: unique symbol = Symbol("HarnessId");

/**
 * Runtime dependencies injected into `runInference`. Code-only, not part of
 * any persisted schema. Test harnesses substitute `fetch` and stamp the
 * `[HarnessId]` tag so production `runInference` never reaches
 * `globalThis.fetch`.
 *
 * `fetch` is a plain function rather than `typeof globalThis.fetch` — the
 * latter is augmented per-runtime (Bun adds `preconnect`; Node and the DOM
 * lib do not) and `runInference` only ever invokes the call signature.
 *
 * The `[HarnessId]` tag is enumerable via `Object.getOwnPropertySymbols`.
 * Do not pass `Dependencies` through reflective serializers or across trust
 * boundaries. (`JSON.stringify` is safe — it walks string keys only.)
 */
export type Dependencies = {
  readonly fetch: (
    input: string | URL | Request,
    init?: RequestInit,
  ) => Promise<Response>;
  /**
   * Scheduler for the harness's per-call timeouts. Production uses the
   * default wrapper around `setTimeout`; test harnesses inject one backed
   * by a virtual clock. Required — callers must explicitly choose between
   * production and virtual. Use `createDefaultScheduler()` for production.
   */
  readonly scheduler: Scheduler;
  /**
   * Registry resolving an inference source to its provider adapter,
   * consulted on every call via `adapters.resolve`. Required — callers
   * make an explicit choice of provider set. Build with
   * `createDependencies(adapters)` or `createDefaultDependencies()`.
   */
  readonly adapters: AdapterRegistry;
  readonly [HarnessId]?: symbol;
};

/**
 * Scheduling abstraction. `setTimeout` returns an idempotent canceller.
 * `now()` is a monotonic time source in the same `delayMs` units
 * `setTimeout` accepts, so deltas across two reads describe elapsed time.
 */
export type Scheduler = {
  setTimeout(callback: () => void, delayMs: number): () => void;
  now(): number;
};

export function createDefaultScheduler(): Scheduler {
  return {
    setTimeout(callback, delayMs) {
      const handle = setTimeout(callback, delayMs);
      return () => {
        clearTimeout(handle);
      };
    },
    // Monotonic; deltas survive wall-clock adjustments that would make
    // `Date.now()`-based intervals read negative. Consumers only read
    // deltas across two `now()` calls from the same instance.
    now() {
      return performance.now();
    },
  };
}

/**
 * Construct runtime dependencies for `runInference`: `globalThis.fetch`, the
 * production scheduler, and the given adapter registry. The registry is
 * required so the caller makes an explicit choice of provider set;
 * `@intx/inference/providers`' `createDefaultDependencies()` supplies the
 * built-in set.
 */
export function createDependencies(adapters: AdapterRegistry): Dependencies {
  return {
    fetch: globalThis.fetch.bind(globalThis),
    scheduler: createDefaultScheduler(),
    adapters,
  };
}

export type InferenceHarnessOptions = {
  turns: ConversationTurn[];
  source: InferenceSource;
  inferenceOptions?: InferenceOptions;
  signal?: AbortSignal;
  // Sequence number allocator — called once per event to get the next seq.
  nextSeq: () => number;
  // Resolves the source's credential secret by `source.credentialId` at send
  // time. Read live per attempt, so a failover to a source with a different
  // `credentialId` resolves that source's credential. Optional — the harness
  // installs a fail-closed default that throws only if a request actually
  // reaches a credential sentinel without a resolver.
  readMaterial?: CredentialMaterialResolver;
  deps: Dependencies;
};

// Fail-closed default: throws only if a request actually reaches a
// credential sentinel, so a sentinel-free mock harness runs without a
// resolver while a real credentialed request surfaces missing wiring.
const unconfiguredCredentialResolver: CredentialMaterialResolver = (
  credentialId,
) => {
  throw new Error(
    `no credential resolver supplied to the inference harness, but a request needs the secret for credential ${credentialId}`,
  );
};

/**
 * Run one fetch lifecycle and yield its events. Ends on the first
 * `inference.error` or `inference.done`; the outer `runInference` decides
 * retry vs flush per the `RetryPolicy`. Not exported — calling this directly
 * would bypass retry handling.
 */
async function* runSingleAttempt(
  opts: InferenceHarnessOptions,
): AsyncIterable<InferenceEvent> {
  const {
    turns,
    source,
    inferenceOptions,
    signal,
    nextSeq,
    readMaterial,
    deps,
  } = opts;
  // Per-call options override source-bound defaults; merged once here so
  // all downstream paths see the effective set.
  const effectiveOptions: InferenceOptions = {
    ...(source.defaults ?? {}),
    ...(inferenceOptions ?? {}),
  };
  const model = source.model;
  // Snapshot the source identity at call start so a mid-call `setSource`
  // cannot mutate the identity stamped on this call's usage/done events.
  // Identity-only: `resolveURL` reads `source.baseURL` and
  // `injectCredentials` reads `source.apiKey` live, so a swap reroutes
  // the in-flight request while done still carries the pre-swap identity.
  const lastCycleSource: LastCycleSource = {
    sourceId: source.id,
    provider: source.provider,
    model,
  };

  // Emit inference.start immediately.
  yield { type: "inference.start", seq: nextSeq(), data: { model } };

  // Mutable partial state — the harness owns this.
  const partial: PartialMessage = { text: "" };
  // Per-index block tracking. The map preserves insertion order (a JS Map
  // guarantee, even for integer keys). Final-turn assembly walks it in
  // arrival order; `tool_use` entries are index markers resolved to final
  // blocks via `completedToolCalls` at assembly time.
  type BlockState =
    | { kind: "text"; text: string; signature?: string }
    | { kind: "thinking"; text: string; signature?: string }
    | { kind: "redacted_thinking"; data: string }
    | { kind: "refusal"; reason: string }
    | { kind: "tool_use"; callId: string; signature?: string }
    | { kind: "image"; image: ImageBlock; signature?: string }
    | {
        kind: "code_execution_request";
        request: CodeExecutionRequestBlock;
        signature?: string;
      }
    | { kind: "code_execution_result"; result: CodeExecutionResultBlock };
  const blockMap = new Map<number, BlockState>();
  // Indexed citations interleave after the block at their index; unindexed
  // citations append at the end of `content[]`.
  const citationsByIndex = new Map<number, CitationBlock[]>();
  const unindexedCitations: CitationBlock[] = [];
  // Prompt-level safety signals (no candidate index on the first
  // capture). Appended to the finalized turn after indexed blocks.
  const unindexedSafetyRatings: SafetyRatingBlock[] = [];
  let usageSeen: TokenUsage | null = null;

  // Tool call state: keyed by callId (or index for OpenAI).
  type ToolCallState = {
    callId: string;
    name: string;
    argsBuffer: string;
  };
  const openToolCalls = new Map<string, ToolCallState>();
  // OpenAI tracks by index before a real callId exists.
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
    adapter = deps.adapters.resolve(lastCycleSource, source.quirks);
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
              : `Unknown provider: ${lastCycleSource.provider}`,
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
  const headers = injectCredentials(
    builtRequest.headers,
    source,
    readMaterial ?? unconfiguredCredentialResolver,
  );

  // The inactivity timer fires after `inactivityTimeoutMs` without an event;
  // the total timer caps the call from fetch onwards. Both abort one
  // controller; `timeoutReason` attributes the abort at the catch site.
  const inactivityTimeoutMs =
    effectiveOptions.inactivityTimeoutMs ?? DEFAULT_INACTIVITY_TIMEOUT_MS;
  const totalTimeoutMs =
    effectiveOptions.totalTimeoutMs ?? DEFAULT_TOTAL_TIMEOUT_MS;
  const scheduler = deps.scheduler;
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
  // Cancellers are idempotent (production wraps `clearTimeout`; the test
  // scheduler flips a `cancelled` flag). The `try/finally` below is the
  // single owner of this lifecycle.
  const cleanupTimers = (): void => {
    cancelTotal();
    cancelInactivity?.();
    cancelInactivity = null;
  };
  // Combine the caller signal with the timeout controller so the fetch
  // sees both; `cleanupSignal` removes the listeners `combineSignals`
  // installs, so a long-lived caller signal does not accumulate one
  // un-removed listener per call.
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
      // Read the body as text, then try JSON.parse. `.json()`-first with a
      // `.text()` fallback does not work — the first read consumes/locks
      // the body stream, so the fallback throws "body already consumed".
      // The read is bound to the fetch signal so a hostile server with a
      // never-terminating error body cannot hang past the total timeout.
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
    // Const so the non-null narrowing from the guard carries into the
    // closure below (a bare `response.body` re-widens to nullable).
    const responseBody = response.body;

    let responseKind: ResponseKind;
    try {
      responseKind = detectResponseKind(response.headers);
    } catch (cause) {
      // A 2xx whose Content-Type is neither SSE nor JSON is a protocol
      // violation, not a transient failure — surface it loudly.
      yield {
        type: "inference.error",
        seq: nextSeq(),
        data: {
          error: classifyProtocolMismatch(
            cause instanceof Error ? cause.message : String(cause),
          ),
          partial: snapshotPartial(partial),
        },
      };
      return;
    }

    // Arm the inactivity timer now that the SSE stream is open; every event
    // resets it. A JSON body has no inter-event silence, so the timer stays
    // disarmed and the total timer alone bounds the buffered read.
    if (responseKind === "sse") {
      armInactivity();
    }

    // One branch per response kind: SSE yields a batch per wire chunk;
    // JSON buffers the whole body and yields a single batch.
    const rawEventBatches = async function* (): AsyncGenerator<
      InferenceEvent[]
    > {
      if (responseKind === "json") {
        const body = await awaitWithSignal(response.text(), fetchSignal);
        yield adapter.parseJSONResponse(body);
        return;
      }
      for await (const sseData of parseSSE(responseBody)) {
        armInactivity();
        yield adapter.parseResponse(sseData);
      }
    };

    try {
      for await (const rawEvents of rawEventBatches()) {
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
              // Running concat of all text deltas — backwards compatible
              // with consumers that treat `partial.text` as everything the
              // assistant has typed so far, regardless of which block it
              // came from.
              partial.text += raw.data.token;
              yield {
                type: "inference.text.delta",
                seq: nextSeq(),
                data: {
                  token: raw.data.token,
                  partial: snapshotPartial(partial),
                  index: idx,
                },
              };
              break;
            }

            case "inference.refusal.delta": {
              const idx = requireIndex(raw, "refusal.delta");
              const existing = blockMap.get(idx);
              if (existing === undefined) {
                blockMap.set(idx, { kind: "refusal", reason: raw.data.token });
              } else if (existing.kind === "refusal") {
                existing.reason += raw.data.token;
              } else {
                throw new ProtocolMismatchError(
                  `harness: refusal.delta at index ${String(idx)} collides with existing ${existing.kind} block`,
                  raw,
                );
              }
              // Re-yield with a fresh seq; `PartialMessage` has no
              // `refusal` field, so the snapshot reflects surrounding
              // text/thinking state. Consumers needing the running refusal
              // string accumulate the emitted delta tokens themselves.
              yield {
                type: "inference.refusal.delta",
                seq: nextSeq(),
                data: {
                  token: raw.data.token,
                  partial: snapshotPartial(partial),
                  index: idx,
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
              // Running concat of every thinking delta. Under interleaving
              // (thinking@0 "A", text@1 "X", thinking@2 "B"),
              // `partial.thinking` ends up "AB" — backwards compatible
              // with the pre-per-index single-buffer semantics.
              const concat = (partial.thinking ?? "") + raw.data.token;
              partial.thinking = concat;
              yield {
                type: "inference.thinking.delta",
                seq: nextSeq(),
                data: {
                  token: raw.data.token,
                  partial: snapshotPartial(partial),
                  index: idx,
                },
              };
              break;
            }

            case "inference.block.signature": {
              const idx = requireIndex(raw, "block.signature");
              const existing = blockMap.get(idx);
              if (existing === undefined) {
                throw new ProtocolMismatchError(
                  `harness: block.signature at index ${String(idx)} has no preceding block at that index`,
                  raw,
                );
              }
              // A signature authenticates the block whose part it rides on;
              // the kinds without a `signature` field have no place to hold
              // one.
              if (
                existing.kind !== "thinking" &&
                existing.kind !== "text" &&
                existing.kind !== "tool_use" &&
                existing.kind !== "image" &&
                existing.kind !== "code_execution_request"
              ) {
                throw new ProtocolMismatchError(
                  `harness: block.signature at index ${String(idx)} targets an existing ${existing.kind} block, which does not carry a signature`,
                  raw,
                );
              }
              existing.signature = raw.data.signature;
              yield {
                type: "inference.block.signature",
                seq: nextSeq(),
                data: { signature: raw.data.signature, index: idx },
              };
              break;
            }

            case "inference.citation": {
              const citation = raw.data.citation;
              const citationIndex = raw.data.index;
              if (citationIndex !== undefined) {
                let list = citationsByIndex.get(citationIndex);
                if (list === undefined) {
                  list = [];
                  citationsByIndex.set(citationIndex, list);
                }
                list.push(citation);
              } else {
                unindexedCitations.push(citation);
              }
              yield {
                type: "inference.citation",
                seq: nextSeq(),
                data:
                  citationIndex !== undefined
                    ? { citation, index: citationIndex }
                    : { citation },
              };
              break;
            }

            case "inference.safety_rating": {
              const safetyRating = raw.data.safetyRating;
              unindexedSafetyRatings.push(safetyRating);
              yield {
                type: "inference.safety_rating",
                seq: nextSeq(),
                data: { safetyRating },
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
              // OpenAI-flavoured adapters synthesize a placeholder callId on
              // tool_call.delta (the real id is only on the start). Key the
              // resolution map on the start's `data.index` so the placeholder
              // maps back to the real id even when `tcDelta.index` is
              // non-zero or non-contiguous.
              indexToCallId.set(String(toolIdx), callId);
              // Anchor the tool_use position in the per-index map; the final
              // walk resolves the marker via `completedToolCalls` so the
              // block lands in wire-arrival order. Collisions with another
              // kind at the same index throw.
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
                data: {
                  callId,
                  name,
                  partial: snapshotPartial(partial),
                  index: toolIdx,
                },
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

            case "inference.image_output": {
              const imgIdx = requireIndex(raw, "image_output");
              const existing = blockMap.get(imgIdx);
              if (existing === undefined) {
                blockMap.set(imgIdx, { kind: "image", image: raw.data.image });
              } else {
                // Image blocks are atomic per event; a second image_output
                // at the same index, or a collision with another kind, is a
                // protocol violation — there is no coalesce branch.
                throw new ProtocolMismatchError(
                  `harness: image_output at index ${String(imgIdx)} collides with existing ${existing.kind} block`,
                  raw,
                );
              }
              // `partial` is intentionally not updated: images are not
              // streamed, so there is no partial-image concept. The atomic
              // event itself signals arrival; the payload carries the
              // ImageBlock verbatim.
              yield {
                type: "inference.image_output",
                seq: nextSeq(),
                data: { image: raw.data.image, index: imgIdx },
              };
              break;
            }

            case "inference.code_execution.start": {
              const ceIdx = requireIndex(raw, "code_execution.start");
              const existing = blockMap.get(ceIdx);
              if (existing === undefined) {
                blockMap.set(ceIdx, {
                  kind: "code_execution_request",
                  request: raw.data.request,
                });
              } else {
                // Code-execution request blocks are atomic per event in
                // their current form; the start handler never reuses an
                // existing slot. A collision at the same index is a wire bug.
                throw new ProtocolMismatchError(
                  `harness: code_execution.start at index ${String(ceIdx)} collides with existing ${existing.kind} block`,
                  raw,
                );
              }
              yield {
                type: "inference.code_execution.start",
                seq: nextSeq(),
                data: { request: raw.data.request, index: ceIdx },
              };
              break;
            }

            case "inference.code_execution.delta": {
              // Gemini does not emit these (its `executableCode` is atomic),
              // but the type commits to `start -> delta* -> result`, so the
              // handler is wired for providers that do chunk source code.
              // The event's `index` routes to the target block; `requestId`
              // is verified against the block's stored id so a routing bug
              // cannot produce a confidently-wrong concatenation.
              const ceIdx = requireIndex(raw, "code_execution.delta");
              const existing = blockMap.get(ceIdx);
              if (existing === undefined) {
                throw new ProtocolMismatchError(
                  `harness: code_execution.delta at index ${String(ceIdx)} with no preceding code_execution.start`,
                  raw,
                );
              }
              if (existing.kind !== "code_execution_request") {
                throw new ProtocolMismatchError(
                  `harness: code_execution.delta at index ${String(ceIdx)} routed to a ${existing.kind} block`,
                  raw,
                );
              }
              if (existing.request.id !== raw.data.requestId) {
                throw new ProtocolMismatchError(
                  `harness: code_execution.delta requestId ${JSON.stringify(raw.data.requestId)} does not match the block's request id ${JSON.stringify(existing.request.id)} at index ${String(ceIdx)}`,
                  raw,
                );
              }
              existing.request = {
                ...existing.request,
                code: existing.request.code + raw.data.codeFragment,
              };
              yield {
                type: "inference.code_execution.delta",
                seq: nextSeq(),
                data: {
                  requestId: raw.data.requestId,
                  codeFragment: raw.data.codeFragment,
                  index: ceIdx,
                },
              };
              break;
            }

            case "inference.code_execution.result": {
              const ceIdx = requireIndex(raw, "code_execution.result");
              const existing = blockMap.get(ceIdx);
              if (existing === undefined) {
                blockMap.set(ceIdx, {
                  kind: "code_execution_result",
                  result: raw.data.result,
                });
              } else {
                throw new ProtocolMismatchError(
                  `harness: code_execution.result at index ${String(ceIdx)} collides with existing ${existing.kind} block`,
                  raw,
                );
              }
              yield {
                type: "inference.code_execution.result",
                seq: nextSeq(),
                data: { result: raw.data.result, index: ceIdx },
              };
              break;
            }

            case "inference.usage": {
              // Providers may send multiple usage events (Anthropic sends
              // one at message_start, then one at message_delta with input
              // set to 0 to mean "no change"). Emit the cumulative
              // post-merge total so consumers see a monotone stream.
              //
              // `source` uses the call-start `lastCycleSource` snapshot: the
              // harness owns identity attribution, and the adapter's own
              // stamp is replaced here so a future provider synthesizing its
              // own descriptor cannot drift from the call-start identity.
              usageSeen = mergeUsage(usageSeen, raw.data.usage);
              yield {
                type: "inference.usage",
                seq: nextSeq(),
                data: { usage: usageSeen, source: lastCycleSource },
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
        data: { usage: finalUsage, source: lastCycleSource },
      };
    }

    // Build the final assistant message by walking the per-index map in
    // insertion order (a JS Map guarantee, including for integer keys), so
    // iteration reproduces wire-arrival order. Tool-call markers resolve to
    // their finalized block via the marker's callId.
    const completedToolCallsByCallId = new Map<string, ContentBlock>();
    for (const tc of completedToolCalls) {
      if (tc.type === "tool_call") {
        completedToolCallsByCallId.set(tc.id, tc);
      }
    }
    const contentBlocks: ContentBlock[] = [];
    // Each arm of the walk calls `emit(block, idx)`, which appends any
    // citations registered at that index and consumes them, so a new block
    // kind cannot forget the interleave step. Consumed indices are deleted
    // so the post-walk check can surface citations whose index pointed at a
    // block that never emitted.
    const emit = (block: ContentBlock, idx: number) => {
      contentBlocks.push(block);
      const atIdx = citationsByIndex.get(idx);
      if (atIdx !== undefined) {
        contentBlocks.push(...atIdx);
        citationsByIndex.delete(idx);
      }
    };
    for (const [idx, entry] of blockMap.entries()) {
      if (entry.kind === "text") {
        // Emit even with empty text if a signature was captured, so a
        // signature riding on an otherwise-empty text carrier still
        // round-trips (mirrors the thinking-block rule below).
        if (entry.text.length === 0 && entry.signature === undefined) {
          continue;
        }
        emit(
          {
            type: "text",
            text: entry.text,
            ...(entry.signature !== undefined
              ? { signature: entry.signature }
              : {}),
          },
          idx,
        );
        continue;
      }
      if (entry.kind === "thinking") {
        // Anthropic's redacted-adjacent flow can produce a thinking block
        // whose visible text is empty but whose signature must round-trip
        // on follow-up turns.
        if (entry.text.length === 0 && entry.signature === undefined) {
          continue;
        }
        emit(
          {
            type: "thinking",
            thinking: entry.text,
            ...(entry.signature !== undefined
              ? { signature: entry.signature }
              : {}),
          },
          idx,
        );
        continue;
      }
      if (entry.kind === "redacted_thinking") {
        emit({ type: "redacted_thinking", data: entry.data }, idx);
        continue;
      }
      if (entry.kind === "refusal") {
        // Empty-reason refusals were filtered at the adapter's wire boundary
        // (OpenAI skips length-0 refusal chunks), so an empty reason here
        // indicates a synthetic capture or an adapter without that guard.
        // Skip rather than emit a RefusalBlock that fails the type's
        // "human-readable text" contract.
        if (entry.reason.length === 0) continue;
        emit({ type: "refusal", reason: entry.reason }, idx);
        continue;
      }
      if (entry.kind === "tool_use") {
        const finalized = completedToolCallsByCallId.get(entry.callId);
        if (finalized === undefined) {
          // Every tool_use marker is added at tool_call.start alongside the
          // openToolCalls entry, and the finalize loop turns every
          // openToolCalls entry into a completedToolCalls entry. A missing
          // marker here means the two bookkeeping paths diverged — surface
          // it loudly rather than dropping the tool call.
          throw new ProtocolMismatchError(
            `harness: tool_use marker at callId ${entry.callId} has no matching completed tool call`,
            entry,
          );
        }
        if (finalized.type !== "tool_call") {
          throw new ProtocolMismatchError(
            `harness: tool_use marker at callId ${entry.callId} resolved to a ${finalized.type} block, not a tool_call`,
            entry,
          );
        }
        emit(
          entry.signature !== undefined
            ? { ...finalized, signature: entry.signature }
            : finalized,
          idx,
        );
        continue;
      }
      if (entry.kind === "image") {
        // The ImageBlock is stored complete on the entry (images are atomic,
        // not streamed), so the final walk emits it verbatim. Citation
        // interleave applies as for any other block kind.
        emit(
          entry.signature !== undefined
            ? { ...entry.image, signature: entry.signature }
            : entry.image,
          idx,
        );
        continue;
      }
      if (entry.kind === "code_execution_request") {
        // Carries whatever code accumulated across start plus any deltas at
        // this index. Gemini delivers all of it atomically on `start`;
        // streaming providers extend `request.code` via the delta handler
        // before this walk runs.
        emit(
          entry.signature !== undefined
            ? { ...entry.request, signature: entry.signature }
            : entry.request,
          idx,
        );
        continue;
      }
      if (entry.kind === "code_execution_result") {
        emit(entry.result, idx);
        continue;
      }
      entry satisfies never;
    }
    if (citationsByIndex.size > 0) {
      // A citation whose index pointed at a block that never made it into
      // `content[]` (orphan reference, or a block filtered out by the final
      // walk) would otherwise be silently dropped — surface the bookkeeping
      // mismatch loudly.
      const orphanIndices = Array.from(citationsByIndex.keys()).sort(
        (a, b) => a - b,
      );
      throw new ProtocolMismatchError(
        `harness: ${String(citationsByIndex.size)} citation index/indices have no matching emitted block in the final turn: ${orphanIndices.join(", ")}`,
        { orphanIndices },
      );
    }
    contentBlocks.push(...unindexedCitations);
    contentBlocks.push(...unindexedSafetyRatings);

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
        source: lastCycleSource,
        ...(pacingDelayMs !== undefined && pacingDelayMs > 0
          ? { pacingDelayMs }
          : {}),
      },
    };
  } finally {
    // Single owner of the timer + signal-listener lifecycle. Runs on every
    // exit including normal completion, early `return`, thrown errors, and
    // consumer abandonment via `for await` `break`. Both cleanups are
    // idempotent.
    cleanupTimers();
    cleanupSignal();
  }
}

/**
 * Run a single inference call with mechanical retry. Wraps
 * `runSingleAttempt` and consults the configured `RetryPolicy` on every
 * `inference.error`.
 *
 * Events from each attempt are buffered until the attempt terminates; the
 * wrapper flushes them only once it knows whether the attempt resolved or
 * its events should be discarded for a retry. That gives the caller a
 * single clean event stream — exactly one `inference.start`, no orphaned
 * partial deltas, no leaked `inference.error`s from retried attempts. The
 * cost is that nothing reaches the caller until the attempt's terminal
 * shape is known, even on a successful first attempt. The buffer is
 * per-call and bounded by one attempt's event stream.
 *
 * Caller-visible seqs stay contiguous across retries: each attempt runs
 * against a private seq allocator, and on flush the wrapper re-stamps the
 * buffered events with the caller's `nextSeq`, so a discarded attempt
 * leaves no gap in the consumer's stream.
 *
 * Between attempts the wrapper emits one `inference.retry` event carrying
 * the attempt number, the policy-chosen `delayMs`, and the classified
 * error. The delay is awaited via `deps.scheduler`, so virtual-clock test
 * harnesses advance retry delays without sleeping real wall-clock. The
 * caller's `signal` short-circuits the delay: aborting mid-delay wakes the
 * await immediately, and the next attempt surfaces `inference.error` of
 * category `aborted` from its entry-time signal check, which the default
 * policy aborts on.
 *
 * If the policy throws synchronously or rejects, the wrapper treats it as
 * `{ kind: "abort" }`, logs the exception at `warn`, and surfaces the
 * *original* `inference.error` to the caller.
 *
 * Synchronous throws from `runSingleAttempt` (`ProtocolMismatchError` from
 * the streaming parse or the finalization walk, etc.) propagate out of
 * `runInference`; the current attempt's buffered events are discarded with
 * the throw, and the caller's `for await` rejects.
 */
export async function* runInference(
  opts: InferenceHarnessOptions,
): AsyncIterable<InferenceEvent> {
  // Crash-loudly guards: the wrapper touches `deps.fetch`, `deps.adapters`,
  // and `deps.scheduler` before any event yields, so a malformed `deps` from
  // a JS caller would otherwise surface as a confusing undefined-property
  // error. The wrapper is the single public entrypoint; this is the right
  // layer to own the shape check.
  if (typeof opts.deps?.fetch !== "function") {
    throw new Error(
      `runInference: deps.fetch must be a function (got ${typeof opts.deps?.fetch}); pass createDefaultDependencies() or a test harness Dependencies object`,
    );
  }
  if (typeof opts.deps.scheduler?.now !== "function") {
    const schedulerType = typeof opts.deps.scheduler;
    const detail =
      schedulerType === "object"
        ? "scheduler is missing the now() method"
        : `got ${schedulerType}`;
    throw new Error(
      `runInference: deps.scheduler must implement now() (${detail}); pass createDefaultDependencies() or a test harness Dependencies object`,
    );
  }
  if (typeof opts.deps.adapters?.resolve !== "function") {
    const adaptersType = typeof opts.deps.adapters;
    const detail =
      adaptersType === "object"
        ? "adapters is missing the resolve() method"
        : `got ${adaptersType}`;
    throw new Error(
      `runInference: deps.adapters must implement resolve() (${detail}); pass createDependencies(adapters), createDefaultDependencies(), or a test harness Dependencies object`,
    );
  }
  const policy =
    opts.inferenceOptions?.retryPolicy ?? createDefaultRetryPolicy();
  // The guards above proved `opts.deps.scheduler` is well-formed; the
  // rest of the wrapper reads it directly without the `?.` ceremony.
  const scheduler = opts.deps.scheduler;
  const startedAtMs = scheduler.now();
  const signal = opts.signal;

  for (let attempt = 1; ; attempt++) {
    const buffered: InferenceEvent[] = [];
    let terminalError: InferenceError | undefined;

    // Per-attempt private allocator. If a discarded attempt's seqs leaked
    // into the caller-visible stream, a retry would leave gaps — so the
    // wrapper allocates from a private counter and re-stamps the buffer
    // with caller-visible seqs at flush time.
    let attemptSeq = 0;
    const attemptOpts: InferenceHarnessOptions = {
      ...opts,
      nextSeq: () => attemptSeq++,
    };
    for await (const event of runSingleAttempt(attemptOpts)) {
      buffered.push(event);
      if (event.type === "inference.error") {
        terminalError = event.data.error;
        break;
      }
      if (event.type === "inference.done") {
        break;
      }
    }

    if (terminalError === undefined) {
      // Successful attempt. Re-stamp the buffer with caller-visible
      // seqs (the private allocator's values are discarded) and
      // flush in order.
      for (const event of buffered) yield { ...event, seq: opts.nextSeq() };
      return;
    }

    // Consult the policy. Sync throws and rejections both resolve to an
    // abort decision; the original `inference.error` surfaces to the caller.
    // The exception is logged at `warn` so a misbehaving custom policy is
    // not invisible under load.
    let decision: RetryDecision;
    try {
      decision = await Promise.resolve(
        policy({
          error: terminalError,
          attempt,
          elapsedMs: scheduler.now() - startedAtMs,
        }),
      );
    } catch (cause) {
      logger.warn`Retry policy threw at attempt ${String(attempt)}; treating as abort. error=${cause instanceof Error ? cause.message : String(cause)}`;
      decision = { kind: "abort" };
    }

    if (decision.kind === "abort") {
      // Flush the buffer (including the terminal inference.error)
      // with re-stamped caller-visible seqs and return. No
      // `inference.retry` event is emitted on the abort path.
      for (const event of buffered) yield { ...event, seq: opts.nextSeq() };
      return;
    }

    // Discard the failed attempt's events, emit a single inference.retry,
    // await the delay, and re-enter the loop.
    yield {
      type: "inference.retry",
      seq: opts.nextSeq(),
      data: {
        attempt,
        delayMs: decision.delayMs,
        previousError: terminalError,
      },
    };

    const retryDelayMs = decision.delayMs;
    // Wire the caller signal into the delay so an abort mid-wait
    // short-circuits to the next attempt within a single virtual tick
    // instead of pinning the wrapper for the full `retryDelayMs` (a
    // 60-second `retryAfterMs` on a quota error would otherwise block
    // cancellation for a minute). Standard race of a scheduled timeout
    // against an abort listener: one `settled` flag plus a `settle()`
    // that cancels whichever side did not fire and removes the listener
    // so the caller signal does not accumulate stale entries.
    await new Promise<void>((resolve) => {
      let settled = false;
      const settle = (): void => {
        if (settled) return;
        settled = true;
        cancelTimer();
        if (signal !== undefined) {
          signal.removeEventListener("abort", onAbort);
        }
        resolve();
      };
      const onAbort = (): void => {
        settle();
      };
      const cancelTimer = scheduler.setTimeout(() => {
        settle();
      }, retryDelayMs);
      if (signal !== undefined) {
        if (signal.aborted) {
          settle();
        } else {
          signal.addEventListener("abort", onAbort, { once: true });
        }
      }
    });
  }
}

/**
 * Combine an optional caller `AbortSignal` with the harness's internal
 * timeout controller into a single signal the fetch can observe. Returns
 * the internal signal alone if no caller signal exists.
 *
 * The returned bundle includes an explicit `cleanup()`: `{ once: true }`
 * listeners only auto-remove after firing, so on the happy path they would
 * accumulate against a long-lived caller signal — one un-removed listener
 * per call. The caller MUST invoke `cleanup()` exactly once when the call's
 * interest in the signal ends; the harness does this from its `try/finally`.
 * `cleanup()` is idempotent.
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
 * server cannot hang the call with a body that never terminates. The
 * signal listener is always removed before settlement, so the helper
 * does not leak listeners.
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

// Per-index routing is load-bearing on every delta that carries an `index`.
// Provider adapters synthesize a default at the boundary when their wire
// shape lacks one (e.g. OpenAI emits `index: 0` explicitly). A delta arriving
// without an index is a wiring bug at the adapter, not data to silently
// route to block 0 — surfacing it as a ProtocolMismatchError is the
// load-bearing alternative to corrupt state.
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

const ParsedToolArgs = type("Record<string, unknown>");

const ErrorBody = type({ error: { message: "string" } });
const DirectMessageBody = type({ message: "string" });

/**
 * Upper bound on the length of a plain-text error body promoted to
 * `InferenceError.message`. Longer bodies are truncated with a marker
 * pointing operators at `error.raw`, which always retains the untruncated
 * body. Structured JSON envelopes are exempt — their `message` fields are
 * server-curated and concise in practice.
 *
 * 500 characters covers a stack trace or a paragraph of diagnostics
 * without blowing up the default director's user-facing reply or the
 * timeline part stored by the hub event collector.
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

  // Plain-text error bodies (HTML pages, raw exception strings, load-balancer
  // diagnostics) reach us via the text-then-parse path: when JSON.parse
  // failed, the raw string is stored as `errorBody`. Surfacing it here means
  // the operator-visible message carries the server's actual diagnostic, not
  // just `statusText`. `error.raw` always holds the untruncated body.
  if (typeof body === "string" && body.length > 0) {
    return truncatePlainTextMessage(body);
  }

  return null;
}

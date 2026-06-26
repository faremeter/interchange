// Agent reactor: the event-driven dispatch loop.
//
// The reactor processes one event at a time, asks the director for the next
// action, validates the action set, and executes. It manages the streaming
// harness for inference, dispatches tool calls, handles gates and correlation,
// and emits all session events with monotonic sequence numbers.
//
// Suspension semantics: when the director returns a suspend action, the reactor
// registers the gate and continues processing events. Inbound messages during
// suspension reach the director as message.received events (director decides:
// queue, fork, or ignore). When the gate clears, a reactor.gate.cleared event
// is enqueued and the director gets to decide next steps.
//
// (INFERENCE.md § Agent Reactor)

import type {
  InboundMessage,
  InferenceEvent,
  InferenceOptions,
  InferenceSource,
  ReactorDirector,
  ReactorInboundEvent,
  ContextStore,
  ToolRunner,
  TokenUsage,
  ConversationTurn,
  ToolResult,
  ToolCall,
  AbortReason,
  BeforeToolExtension,
  ReactorAction,
  ToolResultTransform,
  ContextTransform,
  Compactor,
  TransformRecord,
  StrategyContext,
  StrategyResult,
} from "@intx/types/runtime";

import { getLogger } from "@intx/log";
import { runInference } from "./harness";
import type { Dependencies, InferenceHarnessOptions } from "./harness";
import { createCapabilities } from "./director";
import { createGateManager } from "./gates";
import { createCorrelationRegistry } from "./correlation";
import { createStateManager } from "./state";
import { validateActions } from "./actions";
import {
  createToolResultTurn,
  createInboundTurn,
  assertWellFormedToolSequence,
} from "./turns";
import type { CorrelationValidator } from "./correlation";

const logger = getLogger(["interchange", "reactor"]);

function buildHarnessOpts(
  turns: ConversationTurn[],
  source: InferenceSource,
  options: InferenceOptions | undefined,
  signal: AbortSignal,
  nextSeq: () => number,
  deps: Dependencies,
): InferenceHarnessOptions {
  if (options !== undefined) {
    return {
      turns,
      source,
      inferenceOptions: options,
      signal,
      nextSeq,
      deps,
    };
  }
  return { turns, source, signal, nextSeq, deps };
}

export type ReactorEmittedEvent =
  | InferenceEvent
  | {
      type: "message.received";
      seq: number;
      data: { message: InboundMessage };
    };

export type ReactorConfig = {
  sessionId: string;
  director: ReactorDirector;
  source: InferenceSource;
  /**
   * Fail over `source` to the next entry in the priority-ordered source
   * list, in place, returning false at the end of the list. When omitted the
   * reactor runs the single active source with no failover.
   */
  failOverToNextSource?: () => boolean;
  /** Reset `source` to the most-preferred source, in place. */
  resetToPreferredSource?: () => void;
  toolRunner: ToolRunner;
  contextStore: ContextStore;
  correlationValidator?: CorrelationValidator;
  onEvent: (event: ReactorEmittedEvent) => void;
  deps: Dependencies;
  inferenceRunner?: (
    opts: InferenceHarnessOptions,
  ) => AsyncGenerator<InferenceEvent>;
  beforeToolExtensions?: BeforeToolExtension[];
  toolResultTransforms?: ToolResultTransform[];
  contextTransforms?: ContextTransform[];
  compactors?: Record<string, Compactor>;
  afterCheckpoint?: () => Promise<void>;
  onShutdown?: () => Promise<void>;
  gateTimeout?: number;
  shutdownTimeoutMs?: number;
};

export type Reactor = {
  /** Begin processing. Emits reactor.start. Must be called exactly once. */
  start(): void;
  /** Inject an inbound message into the reactor. */
  deliver(message: InboundMessage): void;
  /** Initiate graceful shutdown with a reason. */
  abort(reason: AbortReason): void;
};

const DEFAULT_GATE_TIMEOUT_MS = 3_600_000;
const DEFAULT_SHUTDOWN_TIMEOUT_MS = 30_000;

/**
 * Creates a reactor instance bound to the given configuration.
 * Call `start()` to begin the event loop.
 */
export function createReactor(config: ReactorConfig): Reactor {
  const {
    sessionId,
    director,
    toolRunner,
    contextStore,
    correlationValidator,
    onEvent,
    deps,
    inferenceRunner = runInference,
    beforeToolExtensions = [],
    toolResultTransforms = [],
    contextTransforms = [],
    compactors = {},
    afterCheckpoint,
    onShutdown,
    gateTimeout = DEFAULT_GATE_TIMEOUT_MS,
    shutdownTimeoutMs = DEFAULT_SHUTDOWN_TIMEOUT_MS,
    // Resolve the optional failover hooks once here, at the reactor's
    // construction edge. A reactor with no source list fails over to
    // nothing and resets to a no-op, so the inference loop below runs the
    // single active source exactly as before.
    failOverToNextSource = () => false,
    resetToPreferredSource = () => {
      /* single-source: nothing to reset */
    },
  } = config;

  // Monotonic sequence counter, scoped to this session.
  let seq = 0;
  function nextSeq(): number {
    return ++seq;
  }

  function emit(event: ReactorEmittedEvent): void {
    onEvent(event);
  }

  // Inbound event queue. Events are pushed here and drained by the loop.
  const queue: ReactorInboundEvent[] = [];
  let queueResolve: (() => void) | null = null;

  // A tool cycle spans from the moment the reactor dispatches an inference or
  // a tool batch until the director has consumed every completion event that
  // operation produces. While a cycle is in flight, admitting a new inbound
  // message — and the inference it triggers — ahead of the outstanding
  // completion events corrupts the prompt: an assistant tool_call turn must be
  // immediately followed by its tool results, and a new inference would
  // instead interleave fresh turns and re-infer against a half-finished batch,
  // which providers reject.
  //
  // pendingContinuations is the authoritative count of dispatched operations
  // whose completion events have not yet been consumed. Every cycle event is
  // counted as it is enqueued and uncounted as it is dequeued, so the count
  // always equals the number of cycle events waiting in the queue. While it is
  // positive, dequeueNext drains cycle events ahead of inbound mail; at zero
  // the cycle is quiescent and processing reverts to FIFO.
  //
  // An earlier design inferred "mid-cycle" from history shape — whether the
  // last turn was an assistant tool_call turn. That underreports in-flight
  // work: a finished tool batch appends its tool-result turn to history before
  // its tool.done events are consumed, flipping the last turn away from the
  // assistant tool_call turn while completion events are still queued, which
  // let inbound mail start an overlapping inference.
  const CYCLE_EVENT_TYPES = new Set<ReactorInboundEvent["type"]>([
    "inference.done",
    "inference.error",
    "tool.done",
  ]);

  let pendingContinuations = 0;

  function enqueue(event: ReactorInboundEvent): void {
    if (CYCLE_EVENT_TYPES.has(event.type)) {
      pendingContinuations += 1;
    }
    queue.push(event);
    if (queueResolve !== null) {
      const resolve = queueResolve;
      queueResolve = null;
      resolve();
    }
  }

  async function waitForEvent(): Promise<void> {
    if (queue.length > 0) return;
    await new Promise<void>((resolve) => {
      queueResolve = resolve;
    });
  }

  function dequeueNext(): ReactorInboundEvent | undefined {
    if (queue.length === 0) return undefined;

    // Always process abort immediately.
    const abortIdx = queue.findIndex((e) => e.type === "abort");
    if (abortIdx !== -1) {
      return queue.splice(abortIdx, 1)[0];
    }

    // Mid-cycle: drain inference-cycle events before anything else so the
    // outstanding inference or tool batch completes before new mail can start
    // an overlapping inference.
    if (pendingContinuations > 0) {
      const idx = queue.findIndex((e) => CYCLE_EVENT_TYPES.has(e.type));
      if (idx !== -1) {
        return queue.splice(idx, 1)[0];
      }
    }

    return queue.shift();
  }

  const gates = createGateManager();
  const correlations = createCorrelationRegistry();
  const capabilities = createCapabilities();

  let stateManager: ReturnType<typeof createStateManager> | null = null;
  let running = false;
  let done = false;
  let shutdownStarted = false;

  // Per-message run-bracket state. Set when the loop dequeues a
  // message.received and begins per-message work; cleared at the
  // terminal point (wait/reply/done) or at a reactor-fatal abandon.
  // `messageRunId` is reactor-minted per dequeue via crypto.randomUUID
  // so a crash-and-replay that re-delivers the same messageId still
  // produces unambiguous start/end pairs downstream.
  let currentMessageRunId: string | null = null;
  let currentMessageId: string | null = null;

  function openMessageRun(messageId: string): void {
    currentMessageRunId = crypto.randomUUID();
    currentMessageId = messageId;
    emit({
      type: "message.run.started",
      seq: nextSeq(),
      data: {
        messageId,
        messageRunId: currentMessageRunId,
        receivedAt: Date.now(),
      },
    });
  }

  function closeMessageRun(
    status: "completed" | "failed",
    error?: { message: string; kind?: string },
  ): void {
    if (currentMessageRunId === null || currentMessageId === null) return;
    const data: {
      messageRunId: string;
      messageId: string;
      status: "completed" | "failed";
      error?: { message: string; kind?: string };
    } = {
      messageRunId: currentMessageRunId,
      messageId: currentMessageId,
      status,
    };
    if (error !== undefined) data.error = error;
    emit({ type: "message.run.ended", seq: nextSeq(), data });
    currentMessageRunId = null;
    currentMessageId = null;
  }

  // Per-cycle accumulator of TransformRecord entries produced by every
  // transform invocation (tool result, context, compactor). Flushed via
  // contextStore.writeManifest at cycle boundaries.
  let manifestBuffer: TransformRecord[] = [];

  // Tracks how the current cycle should be summarized in the commit message.
  let cycleInferred = false;
  let cycleToolCallsExecuted = 0;
  let cycleCompactorName: string | null = null;

  // Director-supplied checkpoint message override; consumed exactly once.
  let pendingMessage: string | null = null;

  // AbortController for in-flight inference/tool operations.
  let operationController = new AbortController();

  function abortOperations(): void {
    operationController.abort();
    operationController = new AbortController();
  }

  // Track in-flight inference and tool promises for shutdown cleanup.
  const inFlight = new Set<Promise<unknown>>();

  function track<T>(p: Promise<T>): Promise<T> {
    inFlight.add(p);
    p.then(
      () => inFlight.delete(p),
      () => inFlight.delete(p),
    );
    return p;
  }

  // -------------------------------------------------------------------------
  // Correlation helper
  // -------------------------------------------------------------------------

  // Guard against concurrent tryCorrelate calls for the same correlationId.
  // deliver() is fire-and-forget async, so two rapid delivers can interleave
  // across an await boundary in the validator, causing double-correlation.
  const correlatingIds = new Set<string>();

  async function tryCorrelate(message: InboundMessage): Promise<boolean> {
    const correlationId = message.headers.interchangeCorrelationId;
    if (correlationId === undefined) return false;

    if (correlatingIds.has(correlationId)) return false;
    const pending = correlations.lookup(correlationId);
    if (pending === undefined) return false;

    correlatingIds.add(correlationId);

    if (correlationValidator !== undefined) {
      let valid: boolean;
      try {
        valid = await correlationValidator.validate(pending, message);
      } catch (cause) {
        logger.warn`Correlation validator threw for ${correlationId}: ${cause}`;
        correlatingIds.delete(correlationId);
        return false;
      }
      if (!valid) {
        correlatingIds.delete(correlationId);
        return false;
      }
    }

    // Clear the gate associated with this correlation, if any.
    const gate = gates.findByCorrelationId(correlationId);
    if (gate !== undefined) {
      gates.clear(gate.gateId);
    }

    correlations.remove(correlationId);

    if (stateManager !== null) {
      stateManager.removePendingOperation(correlationId);

      // Append the correlated message to conversation history so the model
      // sees the response content when it re-infers after the gate clears.
      const msg = createInboundTurn(message);
      if (msg !== null) {
        stateManager.appendTurn(msg);
      }
    }

    emit({
      type: "message.correlated",
      seq: nextSeq(),
      data: { message, correlationId },
    });

    return true;
  }

  // -------------------------------------------------------------------------
  // Action execution
  // -------------------------------------------------------------------------

  let pendingPacingDelayMs = 0;

  function buildStrategyContext(trigger: string): StrategyContext {
    if (stateManager === null) {
      throw new Error("State manager not initialized");
    }
    return { state: stateManager.snapshot(), trigger };
  }

  async function persistBlobs(
    blobs: StrategyResult<unknown>["blobs"],
  ): Promise<void> {
    if (blobs === undefined) return;
    for (const blob of blobs) {
      await contextStore.writeBlob(blob.key, blob.bytes, blob.contentType);
    }
  }

  async function executeInfer(
    options: InferenceOptions | undefined,
  ): Promise<void> {
    if (stateManager === null) return;

    const signal = operationController.signal;

    // Proactive pacing: if the previous inference response indicated we are
    // at the rate limit, wait before sending the next request.
    if (pendingPacingDelayMs > 0 && !signal.aborted) {
      const delayMs = pendingPacingDelayMs;
      pendingPacingDelayMs = 0;
      logger.info`Pacing: waiting ${String(delayMs)}ms before next inference request`;
      await new Promise<void>((resolve) => {
        const timer = setTimeout(resolve, delayMs);
        const onAbort = () => {
          clearTimeout(timer);
          resolve();
        };
        signal.addEventListener("abort", onAbort, { once: true });
      });
      if (signal.aborted) return;
    }

    // Run the context transform chain to produce the materialized prompt.
    let prompt: ConversationTurn[] = stateManager.getTurns();
    for (const transform of contextTransforms) {
      const ctx = buildStrategyContext("pre-inference");
      const result = await transform.apply(prompt, ctx);
      prompt = result.output;
      manifestBuffer.push(result.record);
      await persistBlobs(result.blobs);
    }

    // Tripwire: a malformed tool sequence is invalid in a coherent tool
    // conversation and would otherwise surface as an opaque provider rejection.
    // Catch it here, before the prompt is persisted or sent, so the corruption
    // fails loud as an internal error at the assembly boundary. Throwing routes
    // through the reactor's fatal-error path.
    assertWellFormedToolSequence(prompt);

    try {
      await contextStore.writePrompt(prompt);
    } catch (cause) {
      logger.error`writePrompt failed: ${cause}`;
      emitError(
        `writePrompt failed: ${cause instanceof Error ? cause.message : String(cause)}`,
        false,
      );
    }

    const p = (async () => {
      // Per-source attempt budget for transient errors (quota/retryable/
      // timeout). Kept small because failover, not flogging one source, is
      // the recovery path: the harness already does its own mechanical
      // retry under each attempt, so this caps reactor-level same-source
      // retries at one before moving to the next source.
      const sameSourceAttempts = 2;
      const defaultRetryMs = 60_000;

      // Each cycle starts at the most-preferred source; a failover in a
      // prior cycle must not leave the agent permanently demoted.
      resetToPreferredSource();

      let attempt = 0;
      for (;;) {
        const harnessOpts = buildHarnessOpts(
          prompt,
          config.source,
          options,
          signal,
          nextSeq,
          deps,
        );

        let lastDone:
          | Extract<InferenceEvent, { type: "inference.done" }>
          | undefined;
        let lastError:
          | Extract<InferenceEvent, { type: "inference.error" }>
          | undefined;

        for await (const event of inferenceRunner(harnessOpts)) {
          emit(event);
          if (event.type === "inference.done") lastDone = event;
          else if (event.type === "inference.error") lastError = event;
        }

        if (lastDone !== undefined) {
          if (stateManager !== null) {
            stateManager.appendTurn(lastDone.data.turn);
            stateManager.accumUsage(lastDone.data.usage);
            stateManager.setLastCycleUsage(lastDone.data.usage);
            stateManager.setLastCycleSource(lastDone.data.source);
          }
          cycleInferred = true;
          try {
            await contextStore.writeResponse(lastDone.data.turn);
          } catch (cause) {
            logger.error`writeResponse failed: ${cause}`;
            emitError(
              `writeResponse failed: ${cause instanceof Error ? cause.message : String(cause)}`,
              false,
            );
          }
          if (lastDone.data.pacingDelayMs !== undefined) {
            pendingPacingDelayMs = lastDone.data.pacingDelayMs;
          }
          const u = lastDone.data.usage;
          logger.info`Inference usage: input=${String(u.input)} output=${String(u.output)} cacheRead=${String(u.cacheRead)} cacheWrite=${String(u.cacheWrite)}${lastDone.data.pacingDelayMs !== undefined ? ` pacing=${String(lastDone.data.pacingDelayMs)}ms` : ""}`;
          enqueue({
            type: "inference.done",
            turn: lastDone.data.turn,
            usage: lastDone.data.usage,
            source: lastDone.data.source,
          });
          return;
        }

        if (lastError === undefined) {
          emitError("Inference runner returned without a terminal event", true);
          enqueue({
            type: "inference.error",
            error: {
              category: "fatal",
              message: "Inference runner returned without a terminal event",
            },
            partial: { text: "" },
          });
          return;
        }

        const err = lastError.data.error;
        const partial = lastError.data.partial;

        // Source-invariant failures: no source can serve this call, so
        // abort the whole cycle rather than waste failover attempts.
        if (
          err.category === "context_overflow" ||
          err.category === "fatal" ||
          err.category === "aborted"
        ) {
          enqueue({ type: "inference.error", error: err, partial });
          return;
        }

        // A rate limit is the one category worth waiting out on the same
        // source: it clears with time, and the reactor's backoff is longer
        // than the harness's own per-call retry. The harness has already
        // exhausted its internal mechanical retries for retryable/timeout
        // by the time the reactor sees them, so those fail over rather than
        // re-running the same source (which would just retry-compound).
        if (err.category === "quota_exhausted") {
          attempt += 1;
          if (attempt < sameSourceAttempts && !signal.aborted) {
            const delayMs = err.retryAfterMs ?? defaultRetryMs;
            logger.warn`Rate limited, retrying same source after ${String(delayMs)}ms`;
            await new Promise<void>((resolve) => {
              const timer = setTimeout(resolve, delayMs);
              const onAbort = () => {
                clearTimeout(timer);
                resolve();
              };
              signal.addEventListener("abort", onAbort, { once: true });
            });
            if (signal.aborted) {
              enqueue({
                type: "inference.error",
                error: {
                  category: "aborted",
                  message: "inference aborted during rate limit backoff",
                },
                partial,
              });
              return;
            }
            continue;
          }
        }

        // Same-source rate-limit budget exhausted, or a source-specific
        // failure (credential, protocol mismatch, retryable, timeout): fail
        // over to the next source. A pacing delay the leaving source asked
        // for must not gate the next source.
        pendingPacingDelayMs = 0;
        if (failOverToNextSource()) {
          logger.warn`Failing over to next inference source after ${err.category}`;
          attempt = 0;
          continue;
        }

        // No further source to fail over to: surface the last error.
        enqueue({ type: "inference.error", error: err, partial });
        return;
      }
    })();

    track(p);
    await p;
  }

  async function executeTools(
    calls: ToolCall[],
    parallel: boolean,
    addToHistory = true,
  ): Promise<void> {
    if (stateManager === null) return;
    const state = stateManager;

    const signal = operationController.signal;

    const runOne = async (call: ToolCall): Promise<ToolResult> => {
      // Run before-tool extensions. First block or throw terminates the chain.
      for (const ext of beforeToolExtensions) {
        let blockReason: string | undefined;
        try {
          blockReason = await ext.beforeTool(call, state.snapshot(), signal);
        } catch (cause) {
          const msg = cause instanceof Error ? cause.message : String(cause);
          emitError(
            `BeforeToolExtension threw for ${call.name}: ${msg}`,
            false,
          );
          blockReason = msg;
        }
        if (blockReason !== undefined) {
          const blocked: ToolResult = {
            callId: call.id,
            content: blockReason,
            isError: true,
          };
          emit({
            type: "tool.done",
            seq: nextSeq(),
            data: { result: blocked },
          });
          return blocked;
        }
      }

      emit({ type: "tool.start", seq: nextSeq(), data: { call } });
      const rawResult = await toolRunner.run(call, signal);
      emit({ type: "tool.done", seq: nextSeq(), data: { result: rawResult } });

      if (rawResult.pendingMarker !== undefined && stateManager !== null) {
        const marker = rawResult.pendingMarker;
        const gateId = `pending-${marker.correlationId}`;
        const op: import("@intx/types/runtime").PendingOperation = {
          correlationId: marker.correlationId,
          registeredAt: Date.now(),
          gateId,
          ...(marker.expectedFrom !== undefined
            ? { expectedFrom: marker.expectedFrom }
            : {}),
        };
        correlations.register(op);
        stateManager.addPendingOperation(op);
      }

      // Apply the tool-result transform chain. Each transform's output is fed
      // into the next; emitted blobs are persisted immediately so downstream
      // transforms can rely on the spill being available.
      let current = rawResult;
      for (const transform of toolResultTransforms) {
        const ctx = buildStrategyContext("tool-result-ingest");
        const tr = await transform.apply({ call, result: current }, ctx);
        manifestBuffer.push(tr.record);
        await persistBlobs(tr.blobs);
        current = tr.output;
      }

      return current;
    };

    let results: ToolResult[];
    if (parallel) {
      const p = Promise.all(calls.map((c) => runOne(c)));
      track(p);
      results = await p;
    } else {
      results = [];
      for (const call of calls) {
        const p = runOne(call);
        track(p);
        results.push(await p);
      }
    }

    cycleToolCallsExecuted += results.length;

    if (addToHistory && stateManager !== null) {
      stateManager.appendTurn(createToolResultTurn(results));
    }

    for (const result of results) {
      enqueue({ type: "tool.done", result });
    }
  }

  async function executeCompact(
    compactorName: string,
    reason: string,
  ): Promise<void> {
    if (stateManager === null) return;
    const compactor = compactors[compactorName];
    if (compactor === undefined) {
      throw new Error(
        `executeCompact: no compactor registered for name ${JSON.stringify(compactorName)}`,
      );
    }

    const ctx: StrategyContext = {
      state: stateManager.snapshot(),
      trigger: `director:${reason}`,
    };
    const result = await compactor.apply(stateManager.getTurns(), ctx);

    stateManager.replaceTurns(result.output);
    await contextStore.writeTurns(result.output);
    await persistBlobs(result.blobs);
    manifestBuffer.push(result.record);
    cycleCompactorName = compactor.name;

    logger.info`Compaction by ${compactor.name} reduced history (reason: ${reason})`;
  }

  // -------------------------------------------------------------------------
  // Cycle boundary commit
  // -------------------------------------------------------------------------

  function buildCycleMessage(): string {
    if (pendingMessage !== null) {
      const msg = pendingMessage;
      pendingMessage = null;
      return msg;
    }

    if (cycleCompactorName !== null) {
      return `Cycle: compaction by ${cycleCompactorName}`;
    }

    const parts: string[] = [];
    if (cycleInferred) parts.push("inferred");
    if (cycleToolCallsExecuted > 0) {
      const noun = cycleToolCallsExecuted === 1 ? "tool call" : "tool calls";
      parts.push(`${String(cycleToolCallsExecuted)} ${noun}`);
    }

    if (parts.length === 0) return "Cycle: no-op";
    return `Cycle: ${parts.join(" + ")}`;
  }

  function resetCycleAccumulators(): void {
    manifestBuffer = [];
    cycleInferred = false;
    cycleToolCallsExecuted = 0;
    cycleCompactorName = null;
  }

  async function commitCycle(): Promise<void> {
    if (stateManager === null) return;

    // Only commit when the cycle did real work or the director set an
    // override message. An empty cycle (no inference, no tools, no compact,
    // no override) commits nothing.
    const hasWork =
      cycleInferred ||
      cycleToolCallsExecuted > 0 ||
      cycleCompactorName !== null;
    const hasOverride = pendingMessage !== null;
    if (!hasWork && !hasOverride) {
      resetCycleAccumulators();
      return;
    }

    const message = buildCycleMessage();

    try {
      await contextStore.writeTurns(stateManager.getTurns());
      await contextStore.writeManifest(manifestBuffer);
      await writeMetadata();
      const commit = await contextStore.commit({ message });
      lastCheckpointHash = commit.hash;
    } catch (cause) {
      logger.error`Cycle commit failed: ${cause}`;
      emitError(
        `Cycle commit failed: ${cause instanceof Error ? cause.message : String(cause)}`,
        false,
      );
      resetCycleAccumulators();
      return;
    }

    resetCycleAccumulators();

    if (afterCheckpoint !== undefined) {
      try {
        await afterCheckpoint();
      } catch (cause) {
        logger.error`afterCheckpoint failed: ${cause}`;
        emitError(
          `afterCheckpoint failed: ${cause instanceof Error ? cause.message : String(cause)}`,
          false,
        );
      }
    }
  }

  async function writeMetadata(): Promise<void> {
    if (stateManager === null) return;
    await contextStore.writeMetadata({
      pendingOperations: stateManager.getPendingOperations(),
      tokenUsage: stateManager.getTokenUsage(),
    });
  }

  // -------------------------------------------------------------------------
  // Main loop
  // -------------------------------------------------------------------------

  async function loop(): Promise<void> {
    if (stateManager === null) {
      throw new Error("State manager not initialized before loop");
    }

    while (!done) {
      await waitForEvent();

      if (done) break;

      const event = dequeueNext();
      if (event === undefined) continue;

      // A dequeued cycle event is one fewer in-flight continuation. Pairs with
      // the increment in enqueue(); both key off CYCLE_EVENT_TYPES so they
      // cannot drift.
      if (CYCLE_EVENT_TYPES.has(event.type)) {
        pendingContinuations -= 1;
      }

      // Handle abort events: initiate shutdown regardless of director.
      if (event.type === "abort") {
        if (!shutdownStarted) {
          done = true;
          await initiateShutdown();
        }
        break;
      }

      // Append inbound messages to conversation history so the provider sees them.
      // Each dequeued message.received opens a fresh per-message run bracket.
      // If a prior bracket is still open (defensive — should not occur given
      // the dequeue priority that drains cycle events before new messages),
      // close it as completed first so the new bracket starts cleanly.
      if (event.type === "message.received") {
        if (stateManager !== null) {
          const msg = createInboundTurn(event.message);
          if (msg !== null) {
            stateManager.appendTurn(msg);
          }
        }
        if (currentMessageRunId !== null) {
          closeMessageRun("completed");
        }
        openMessageRun(event.message.headers.messageId);
      }

      let actions;
      try {
        actions = await director.decide(
          event,
          stateManager.snapshot(),
          capabilities,
        );
      } catch (cause) {
        const msg = cause instanceof Error ? cause.message : String(cause);

        logger.error`Director threw during decide: ${cause}`;
        emitError(`Director exception: ${msg}`, true);
        closeMessageRun("failed", {
          message: `Director exception: ${msg}`,
          kind: "reactor_fatal",
        });
        done = true;
        await initiateShutdown();
        break;
      }

      const validation = validateActions(actions);
      if (!validation.ok) {
        emitError(`Invalid action set: ${validation.error}`, true);
        closeMessageRun("failed", {
          message: `Invalid action set: ${validation.error}`,
          kind: "reactor_fatal",
        });
        done = true;
        await initiateShutdown();
        break;
      }

      const normalized = validation.normalized;

      // Checkpoint sets the next cycle's commit message.
      const checkpointAction = normalized.find(
        (a): a is Extract<ReactorAction, { type: "checkpoint" }> =>
          a.type === "checkpoint",
      );
      if (checkpointAction !== undefined) {
        pendingMessage = checkpointAction.message;
      }

      // Emit custom events (validated type namespace).
      for (const action of normalized) {
        if (action.type === "emit") {
          const reserved = ["inference.", "tool.", "reactor.", "fork."];
          const blocked = reserved.some((p) => action.eventType.startsWith(p));
          if (blocked) {
            emitError(
              `Director tried to emit reserved event type: ${action.eventType}`,
              false,
            );
            continue;
          }
          emit({ type: action.eventType, seq: nextSeq(), data: action.data });
        }
      }

      // Fork is excluded in this build.
      for (const action of normalized) {
        if (action.type === "fork") {
          emitError("Fork action is not supported in this build", false);
        }
      }

      // Handle done.
      if (normalized.some((a) => a.type === "done")) {
        // Flush the cycle (in case the director paired done with checkpoint
        // or other work) before shutting down.
        await commitCycle();
        closeMessageRun("completed");
        done = true;
        await initiateShutdown();
        break;
      }

      // Handle wait: commit the cycle (if work happened) and return to the
      // event loop without shutting down. Wait is a per-message terminal:
      // the reactor has nothing more to do for the message and is returning
      // to idle.
      if (normalized.some((a) => a.type === "wait")) {
        await commitCycle();
        closeMessageRun("completed");
        continue;
      }

      // Handle suspend: register gate and continue the loop (don't block).
      const suspendAction = normalized.find((a) => a.type === "suspend");
      if (suspendAction !== undefined && suspendAction.type === "suspend") {
        const { gate } = suspendAction;
        const effectiveTimeout =
          gate.timeoutMs > 0 ? gate.timeoutMs : gateTimeout;

        emit({
          type: "reactor.gate.blocked",
          seq: nextSeq(),
          data: { reason: gate.type, gateId: gate.gateId },
        });

        if (stateManager !== null) {
          stateManager.setGatesSnapshot(gates.snapshot());
        }

        // Register the gate. The onCleared callback enqueues the cleared event
        // so the loop processes it normally without blocking here.
        void gates.register(
          gate.gateId,
          gate.type,
          effectiveTimeout,
          gate.correlationId,
          (gateId, reason) => {
            if (stateManager !== null) {
              stateManager.setGatesSnapshot(gates.snapshot());
            }
            emit({
              type: "reactor.gate.cleared",
              seq: nextSeq(),
              data: { gateId, reason },
            });
            enqueue({ type: "reactor.gate.cleared", gateId, reason });
          },
        );

        if (stateManager !== null) {
          stateManager.setGatesSnapshot(gates.snapshot());
        }

        // Commit before the loop continues so the suspended-state turns are
        // durable across restart.
        await commitCycle();
        continue;
      }

      // Handle reply — emit the content for the harness/supervisor to send.
      const replyAction = normalized.find((a) => a.type === "reply");
      if (replyAction !== undefined && replyAction.type === "reply") {
        // Flush any pending cycle work before signaling the reply so the
        // emitted checkpointHash matches the visible state.
        await commitCycle();
        emit({
          type: "connector.reply",
          seq: nextSeq(),
          data: {
            content: replyAction.content,
            ...(lastCheckpointHash !== undefined
              ? { checkpointHash: lastCheckpointHash }
              : {}),
          },
        });
        // Reply is a per-message terminal point: close the bracket so the
        // next inbound message opens a fresh run.
        closeMessageRun("completed");
        // After replying, wait for the next inbound message.
        continue;
      }

      // Handle compact (its own cycle; runs before any infer can be requested
      // in the same director invocation — validation forbids that pairing).
      const compactAction = normalized.find((a) => a.type === "compact");
      if (compactAction !== undefined && compactAction.type === "compact") {
        try {
          await executeCompact(compactAction.compactor, compactAction.reason);
        } catch (cause) {
          const msg = cause instanceof Error ? cause.message : String(cause);
          logger.error`Compaction failed: ${cause}`;
          emitError(`Compaction failed: ${msg}`, true);
          closeMessageRun("failed", {
            message: `Compaction failed: ${msg}`,
            kind: "reactor_fatal",
          });
          done = true;
          await initiateShutdown();
          break;
        }
        await commitCycle();
        continue;
      }

      // Handle infer.
      const inferAction = normalized.find((a) => a.type === "infer");
      if (inferAction !== undefined && inferAction.type === "infer") {
        await executeInfer(inferAction.options);
        continue;
      }

      // Handle execute_tools.
      const toolsAction = normalized.find((a) => a.type === "execute_tools");
      if (toolsAction !== undefined && toolsAction.type === "execute_tools") {
        const parallel = toolsAction.parallel !== false;
        const addToHistory = toolsAction.addToHistory !== false;
        await executeTools(toolsAction.calls, parallel, addToHistory);
        continue;
      }

      // No infer/tools/reply/suspend/wait/compact action — if a checkpoint
      // override was set on its own (or alongside emit/fork), the next event
      // will pick it up. Nothing to flush here.
    }
  }

  function emitError(message: string, fatal: boolean): void {
    emit({
      type: "reactor.error",
      seq: nextSeq(),
      data: { error: message, fatal },
    });
  }

  let lastCheckpointHash: string | undefined;

  async function initiateShutdown(): Promise<void> {
    if (shutdownStarted) return;
    shutdownStarted = true;

    abortOperations();
    gates.shutdown();

    if (stateManager !== null) {
      stateManager.setGatesSnapshot([]);
    }

    if (inFlight.size > 0) {
      const deadline = new Promise<void>((resolve) =>
        setTimeout(resolve, shutdownTimeoutMs),
      );
      await Promise.race([Promise.allSettled([...inFlight]), deadline]);
    }

    if (onShutdown !== undefined) {
      try {
        await onShutdown();
      } catch (cause) {
        logger.error`onShutdown failed: ${cause}`;
        emitError(
          `onShutdown failed: ${cause instanceof Error ? cause.message : String(cause)}`,
          false,
        );
      }
    }

    emit({
      type: "reactor.done",
      seq: nextSeq(),
      data: {},
    });
  }

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  function start(): void {
    if (running) {
      throw new Error("Reactor is already running");
    }
    running = true;

    void (async () => {
      let initialTurns: ConversationTurn[];
      let initialOps;
      let initialUsage: TokenUsage;
      try {
        const loaded = await contextStore.load();
        initialTurns = loaded.turns;
        initialOps = loaded.pendingOperations;
        initialUsage = loaded.tokenUsage;
      } catch (cause) {
        logger.error`Context store load failed: ${cause}`;
        emitError(
          `Context store load failed: ${cause instanceof Error ? cause.message : String(cause)}`,
          true,
        );
        emit({ type: "reactor.done", seq: nextSeq(), data: {} });
        return;
      }

      stateManager = createStateManager(
        sessionId,
        initialTurns,
        initialOps,
        initialUsage,
      );
      stateManager.setGatesSnapshot(gates.snapshot());

      emit({ type: "reactor.start", seq: nextSeq(), data: {} });

      try {
        await loop();
      } catch (cause) {
        const msg = cause instanceof Error ? cause.message : String(cause);
        logger.error`Reactor loop threw unexpectedly: ${cause}`;
        emitError(`Internal reactor error: ${msg}`, true);
        closeMessageRun("failed", {
          message: `Internal reactor error: ${msg}`,
          kind: "reactor_fatal",
        });
        if (!shutdownStarted) {
          await initiateShutdown();
        }
      }
    })();
  }

  function deliver(message: InboundMessage): void {
    if (done) return;
    void (async () => {
      const correlated = await tryCorrelate(message);
      if (!correlated) {
        emit({
          type: "message.received",
          seq: nextSeq(),
          data: { message },
        });
        enqueue({ type: "message.received", message });
      }
    })();
  }

  function abort(reason: AbortReason): void {
    enqueue({ type: "abort", reason });
  }

  return { start, deliver, abort };
}

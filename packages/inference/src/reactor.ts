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
  BeforeToolDecision,
  BeforeToolExtension,
  GateType,
  PendingOperation,
  ReactorAction,
  ToolResultTransform,
  ContextTransform,
  Compactor,
  TransformRecord,
  StrategyContext,
  StrategyResult,
} from "@intx/types/runtime";

import { getLogger } from "@intx/log";
import { ApprovalDecision, signalKindToGateType } from "@intx/types";
import { type } from "arktype";
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

// Sentinel returned by a per-call tool run when a before-tool extension parked
// the call on a gate. Distinct from every ToolResult so a suspended call is
// excluded from the tool-result history append and from tool.done continuation.
const SUSPENDED = Symbol("suspended");

// Exhaustiveness guard for the resume-dispatch switch. A newly added
// SignalKind or approval outcome that is not classified fails to type-check
// here, so the switch cannot silently drop an unhandled case.
function assertNever(x: never): never {
  throw new Error(`Unhandled resume case: ${JSON.stringify(x)}`);
}

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
  // A suspension registers a gate and may persist a pending operation. That is
  // a durable state change even when the cycle ran no inference and completed
  // no tool call, so it must force the cycle commit.
  let cycleSuspended = false;

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

  // How the reactor resumes a correlated pending operation.
  //
  //   redispatch — an approved approval re-runs its parked tool call. The
  //     reactor grants a one-shot bypass for the call and re-dispatches it;
  //     the resumed run answers the parked call with a real tool result. The
  //     correlated message body is the decision, not conversation content, so
  //     it is NOT appended to history.
  //   gate-cleared — the async-tool path (a pending marker awaiting an inbound
  //     response). The gate clears normally, driving the director to re-infer,
  //     and the correlated message body IS appended to history so the model
  //     sees the response it was waiting on.
  type ResumeDispatch =
    | { mode: "redispatch"; calls: ToolCall[] }
    | { mode: "gate-cleared" }
    | { mode: "error_result"; result: ToolResult };

  // Decide how a correlated approval-kind pending operation resumes, granting
  // any one-shot bypass synchronously so no delivery can interleave between the
  // grant and the re-dispatch enqueued by the caller. An operation that carries
  // a `suspendedCall` is an ask-flow suspension: the approver's decision routes
  // it down the re-dispatch rail. An operation without one is an async-tool
  // pending marker, which resumes on the normal gate-cleared rail.
  //
  // The nested switch is total: the outer `assertNever(op.kind)` rejects a
  // future SignalKind at compile time, and the inner `assertNever` rejects a
  // future decision outcome. A malformed decision body fails loud at the parse
  // boundary before the switch.
  function resumePendingOperation(
    op: PendingOperation,
    message: InboundMessage,
  ): ResumeDispatch {
    if (op.suspendedCall === undefined) {
      return { mode: "gate-cleared" };
    }
    const suspendedCall = op.suspendedCall;

    if (message.content === undefined) {
      throw new Error(
        `Correlated approval decision for ${op.correlationId} has no body to parse`,
      );
    }
    let raw: unknown;
    try {
      raw = JSON.parse(message.content);
    } catch (cause) {
      throw new Error(
        `Correlated approval decision for ${op.correlationId} is not valid JSON`,
        { cause },
      );
    }
    const decision = ApprovalDecision(raw);
    if (decision instanceof type.errors) {
      throw new Error(
        `Correlated approval decision for ${op.correlationId} is malformed: ${decision.summary}`,
      );
    }

    switch (op.kind) {
      case "approval":
        switch (decision.outcome) {
          case "approved":
            // Authorize the exact parked call to run once, then re-dispatch it.
            // Grant on every before-tool extension: only the authz extension
            // responds, but referencing it directly would re-couple the reactor
            // to authz and break a deployment that runs without it.
            for (const ext of beforeToolExtensions) {
              ext.grantOneShot?.(suspendedCall.id);
            }
            return { mode: "redispatch", calls: [suspendedCall] };
          case "rejected": {
            // The approver denied the call. Answer the parked call with a
            // synthetic error result rather than re-running it — no one-shot
            // bypass is granted, so the tool never executes. The approver's
            // reason, when present, is surfaced to the model verbatim.
            const content =
              "denied by approver" +
              (decision.message !== undefined ? `: ${decision.message}` : "");
            return {
              mode: "error_result",
              result: { callId: suspendedCall.id, content, isError: true },
            };
          }
          default:
            return assertNever(decision.outcome);
        }
      default:
        return assertNever(op.kind);
    }
  }

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

    // Capture the operation before removal so the resume dispatch can read its
    // kind and suspended call. Removal happens only after the dispatch is
    // decided, all inside this correlatingIds-guarded critical section so a
    // double-deliver early-returns rather than double-dispatching.
    const op = pending;

    let dispatch: ResumeDispatch;
    try {
      dispatch = resumePendingOperation(op, message);
    } catch (cause) {
      correlatingIds.delete(correlationId);
      throw cause;
    }

    const gate = gates.findByCorrelationId(correlationId);
    switch (dispatch.mode) {
      case "redispatch": {
        // Clear the gate WITHOUT enqueuing gate.cleared: the re-dispatched call
        // is the resumption, so a gate.cleared-driven re-infer would double the
        // continuation. The re-dispatch's own tool.done drives the re-infer.
        if (gate !== undefined) {
          gates.clearSilently(gate.gateId);
          if (stateManager !== null) {
            stateManager.setGatesSnapshot(gates.snapshot());
          }
        }
        correlations.remove(correlationId);
        if (stateManager !== null) {
          stateManager.removePendingOperation(correlationId);
        }
        // The grant is already recorded (synchronously, in
        // resumePendingOperation) with no await since; enqueue the re-dispatch
        // so it runs on the loop with normal event ordering. The director seeds
        // its outstanding-result count off this event before the call's
        // tool.done arrives.
        enqueue({ type: "resume.execute_tools", calls: dispatch.calls });
        break;
      }
      case "error_result": {
        // The approver denied the call. Clear the gate SILENTLY (like the
        // approved redispatch) so it cannot also trip onGateCleared and enqueue
        // a second continuation. The synthetic error result answers the parked
        // call; the director appends it and re-infers once.
        if (gate !== undefined) {
          gates.clearSilently(gate.gateId);
          if (stateManager !== null) {
            stateManager.setGatesSnapshot(gates.snapshot());
          }
        }
        correlations.remove(correlationId);
        if (stateManager !== null) {
          stateManager.removePendingOperation(correlationId);
        }
        enqueue({ type: "resume.tool_result", result: dispatch.result });
        break;
      }
      case "gate-cleared": {
        // Async-tool resumption: clear the gate normally so the director
        // re-infers, and append the correlated response to history so the model
        // sees the content it was waiting on.
        if (gate !== undefined) {
          gates.clear(gate.gateId);
        }
        correlations.remove(correlationId);
        if (stateManager !== null) {
          stateManager.removePendingOperation(correlationId);
          const msg = createInboundTurn(message);
          if (msg !== null) {
            stateManager.appendTurn(msg);
          }
        }
        break;
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

    void track(p);
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

    const runOne = async (
      call: ToolCall,
    ): Promise<ToolResult | typeof SUSPENDED> => {
      // Run before-tool extensions. The first non-allow decision terminates
      // the chain: `block` answers the call with an error result, `suspend`
      // parks it (no result, no tool.done).
      for (const ext of beforeToolExtensions) {
        let decision: BeforeToolDecision;
        try {
          decision = await ext.beforeTool(call, state.snapshot(), signal);
        } catch (cause) {
          const msg = cause instanceof Error ? cause.message : String(cause);
          emitError(
            `BeforeToolExtension threw for ${call.name}: ${msg}`,
            false,
          );
          decision = { type: "block", reason: msg };
        }

        if (decision.type === "suspend") {
          // Park the call: register the gate, persist the pending operation,
          // snapshot, and commit. The call is neither run nor answered — no
          // tool.start, no tool.done, no tool-result turn. The gate clears
          // when the correlated external decision is delivered.
          await suspendOnGate({
            gateType: decision.gate.type,
            gateId: decision.gate.gateId,
            timeoutMs: Math.max(1, decision.gate.timeoutAt - Date.now()),
            correlationId: decision.gate.correlationId,
            pendingOp: decision.pendingOp,
          });
          return SUSPENDED;
        }

        if (decision.type === "block") {
          const blocked: ToolResult = {
            callId: call.id,
            content: decision.reason,
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
        const op: PendingOperation = {
          correlationId: marker.correlationId,
          // Placeholder: async markers should carry their own SignalKind. The
          // resume switch keys on suspendedCall presence (absent here) as the
          // interim discriminator instead of on kind.
          kind: "approval",
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

    let outcomes: (ToolResult | typeof SUSPENDED)[];
    if (parallel) {
      const p = Promise.all(calls.map((c) => runOne(c)));
      void track(p);
      outcomes = await p;
    } else {
      outcomes = [];
      for (const call of calls) {
        const p = runOne(call);
        void track(p);
        outcomes.push(await p);
      }
    }

    // Suspended calls are parked, not answered: they contribute no tool
    // result to history and no tool.done continuation event.
    const results = outcomes.filter((o): o is ToolResult => o !== SUSPENDED);

    cycleToolCallsExecuted += results.length;

    if (addToHistory && stateManager !== null && results.length > 0) {
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
    cycleSuspended = false;
  }

  async function commitCycle(): Promise<void> {
    if (stateManager === null) return;

    // Only commit when the cycle did real work or the director set an
    // override message. An empty cycle (no inference, no tools, no compact,
    // no override) commits nothing.
    const hasWork =
      cycleInferred ||
      cycleToolCallsExecuted > 0 ||
      cycleCompactorName !== null ||
      cycleSuspended;
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
  // Gate suspension critical section
  // -------------------------------------------------------------------------

  // While a suspend is committing (the `await commitCycle()` in
  // `suspendOnGate`), its gate is already armed but `reactor.gate.blocked` has
  // not been emitted yet. If the gate's timeout timer elapses inside that
  // window, `onGateCleared` would take effect ahead of the `blocked` it belongs
  // to — emitting `reactor.gate.cleared` on the plain path, or enqueuing the
  // synthetic `resume.tool_result` and removing the pending operation on the
  // ask rail — before the suspension has been announced. `deriveStatus` and the
  // send-awaiter both assume a gate's `blocked` precedes any effect of its
  // clearing, so the in-flight suspend is tracked here and such a clear is
  // deferred until `blocked` has fired.
  type InFlightSuspend = {
    gateId: string;
    deferredClear: { reason: "resolved" | "timeout" | "shutdown" } | null;
  };
  let suspendingGate: InFlightSuspend | null = null;

  // Callback the gate manager invokes when a gate resolves, times out, or is
  // shut down. Refreshes the snapshot and drives the loop's next step.
  //
  // A parked ask-flow approval that TIMES OUT ends without running its tool:
  // it must be answered with a synthetic error result rather than left as a
  // dangling tool_use. That path enqueues `resume.tool_result` INSTEAD OF
  // `reactor.gate.cleared` — the two are mutually exclusive, because enqueuing
  // both would drive two re-inferences for one timeout. Every other case (an
  // async-marker pending op with no suspendedCall, no pending op at all, a
  // `resolved`/`shutdown` reason, or a shutting-down reactor) keeps today's
  // behavior: enqueue `reactor.gate.cleared` and let the director re-infer.
  //
  // A delivered `resolved` never reaches here on the ask rail — the redispatch
  // and reject paths clear the gate silently (no onCleared) — so the timeout
  // branch is gated on `reason === "timeout"` and shutdown stays on the plain
  // path: a shutting-down reactor must not manufacture tool results.
  function onGateCleared(
    gateId: string,
    reason: "resolved" | "timeout" | "shutdown",
  ): void {
    // A clear that fires while this gate's suspend is still committing must not
    // take effect before `reactor.gate.blocked` is emitted. Record it and let
    // suspendOnGate replay the full handler once the block is announced.
    if (
      suspendingGate !== null &&
      suspendingGate.gateId === gateId &&
      suspendingGate.deferredClear === null
    ) {
      suspendingGate.deferredClear = { reason };
      return;
    }

    if (stateManager !== null) {
      stateManager.setGatesSnapshot(gates.snapshot());
    }

    if (reason === "timeout") {
      const op = correlations.findByGateId(gateId);
      if (op !== undefined && op.suspendedCall !== undefined) {
        correlations.remove(op.correlationId);
        if (stateManager !== null) {
          stateManager.removePendingOperation(op.correlationId);
        }
        enqueue({
          type: "resume.tool_result",
          result: {
            callId: op.suspendedCall.id,
            content: "approval timed out",
            isError: true,
          },
        });
        return;
      }
    }

    emit({
      type: "reactor.gate.cleared",
      seq: nextSeq(),
      data: { gateId, reason },
    });
    enqueue({ type: "reactor.gate.cleared", gateId, reason });
  }

  // Parks the reactor on a gate. Shared by the director's `suspend` action and
  // the before-tool `suspend` decision so both paths register the gate,
  // durably persist any pending operation, snapshot the active gates, and
  // commit before returning to the loop — a suspended reactor's state must be
  // durable across restart. When `pendingOp` is supplied its correlation is
  // registered and it is persisted; the director path has already persisted
  // its pending operation (via the tool's pending marker), so it passes none.
  async function suspendOnGate(args: {
    gateType: GateType;
    gateId: string;
    timeoutMs: number;
    correlationId: string | undefined;
    pendingOp: PendingOperation | undefined;
  }): Promise<void> {
    const { gateType, gateId, timeoutMs, correlationId, pendingOp } = args;

    if (pendingOp !== undefined) {
      correlations.register(pendingOp);
      if (stateManager !== null) {
        stateManager.addPendingOperation(pendingOp);
      }
    }

    // Track this suspend as in flight so a clear racing the commit below is
    // deferred until `reactor.gate.blocked` has been emitted.
    const inFlightSuspend: InFlightSuspend = { gateId, deferredClear: null };
    suspendingGate = inFlightSuspend;

    // Register the gate. onGateCleared enqueues the cleared event so the loop
    // processes it normally without blocking here.
    void gates.register(
      gateId,
      gateType,
      timeoutMs,
      correlationId,
      onGateCleared,
    );

    if (stateManager !== null) {
      stateManager.setGatesSnapshot(gates.snapshot());
    }

    // Registering the gate (and any pending operation) is a durable state
    // change that must be committed even if this cycle did no other work.
    cycleSuspended = true;

    // Commit before the loop continues so the suspended state is durable
    // across restart.
    await commitCycle();

    // Emit `reactor.gate.blocked` only AFTER the commit. This event resolves
    // the `send()` awaiter as "suspended", and a downstream consumer (the warm
    // agent's run-boundary durability mirror) reads the pending operation back
    // out of the just-committed context store the instant `send()` settles.
    // Emitting before the commit would resolve `send()` first, letting that
    // mirror read a store that has not yet persisted the pending op -- it would
    // durably mirror an empty pending-operation set and lose the approval
    // snapshot, so a parked correlation could not be re-registered after a hub
    // reconnect. This upholds persist-before-settle: the durable commit the
    // header promises before returning to the loop lands before the suspension
    // settles.
    emit({
      type: "reactor.gate.blocked",
      seq: nextSeq(),
      data: {
        reason: gateType,
        gateId,
        ...(correlationId !== undefined ? { correlationId } : {}),
        ...(pendingOp?.approvalSnapshot !== undefined
          ? { approvalSnapshot: pendingOp.approvalSnapshot }
          : {}),
      },
    });

    // The suspension is announced. If the gate cleared while the commit was in
    // flight, its handler was deferred to keep it after `blocked`; replay it
    // now, in order.
    suspendingGate = null;
    if (inFlightSuspend.deferredClear !== null) {
      onGateCleared(gateId, inFlightSuspend.deferredClear.reason);
    }
  }

  // Re-registers a live gate and correlation for each pending operation loaded
  // from the context store on restart. The remaining timeout is computed from
  // the persisted absolute deadline (`timeoutAt`) against the current clock, so
  // the deadline is preserved across the restart rather than restarted; a
  // deadline already in the past clamps to 1ms so the gate fires on the next
  // tick. An operation persisted without a `timeoutAt` (hold-indefinitely) has
  // no deadline to preserve; the gate manager cannot express an indefinite
  // hold, so it is armed with the session-level `gateTimeout` — the same
  // effective timeout the director-suspend fallback uses — rather than a
  // silent zero. This does not run through `suspendOnGate`: rehydration must
  // not re-emit `reactor.gate.blocked` (the suspension already happened before
  // the restart) and must not commit (nothing changed).
  function rehydrateGates(ops: PendingOperation[]): void {
    for (const op of ops) {
      const timeoutMs =
        op.timeoutAt !== undefined
          ? Math.max(1, op.timeoutAt - Date.now())
          : gateTimeout;
      correlations.register(op);
      void gates.register(
        op.gateId,
        signalKindToGateType(op.kind),
        timeoutMs,
        op.correlationId,
        onGateCleared,
      );
    }
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

      // A parked approval that ended without running its tool (rejected or
      // timed out) carries a synthetic error result answering the parked call.
      // Land it in history before the director decides so the tool_result turn
      // closes the dangling tool_use and the re-inference the director returns
      // sees a well-formed sequence. No tool ran, so no tool.done and no
      // counter change accompany it.
      if (event.type === "resume.tool_result") {
        if (stateManager !== null) {
          stateManager.appendTurn(createToolResultTurn([event.result]));
        }
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
        await suspendOnGate({
          gateType: gate.type,
          gateId: gate.gateId,
          timeoutMs: gate.timeoutMs > 0 ? gate.timeoutMs : gateTimeout,
          correlationId: gate.correlationId,
          pendingOp: undefined,
        });
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

      try {
        // Re-arm gates for operations that were suspended before the restart.
        // The state manager holds the loaded pending operations, but a gate is
        // in-memory and does not survive a restart; without this a reloaded
        // suspended agent is wedged (no live gate to clear, no correlation to
        // match). Each op re-registers its correlation and a live gate keyed on
        // the op's own gateId and correlationId, so a delivered signal clears
        // it exactly as the original suspension would have.
        //
        // Rehydration runs inside this try/catch because the pending operations
        // come from the context store — an untrusted external boundary — and
        // correlation/gate registration throws synchronously on a duplicate
        // correlationId or gateId. A throw must surface as reactor.error plus
        // reactor.done (matching the load-failure path), not brick the reactor
        // as a silent unhandled rejection.
        rehydrateGates(initialOps);

        stateManager.setGatesSnapshot(gates.snapshot());

        emit({ type: "reactor.start", seq: nextSeq(), data: {} });

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
      let correlated: boolean;
      try {
        correlated = await tryCorrelate(message);
      } catch (cause) {
        // A correlation-path invariant failed (e.g. a malformed approval
        // decision). Surface it as a fatal reactor error rather than a silent
        // unhandled rejection, and stop the run — the resume cannot proceed on
        // a decision the reactor cannot trust.
        const msg = cause instanceof Error ? cause.message : String(cause);
        logger.error`Correlation dispatch failed: ${cause}`;
        emitError(`Correlation dispatch failed: ${msg}`, true);
        closeMessageRun("failed", {
          message: `Correlation dispatch failed: ${msg}`,
          kind: "reactor_fatal",
        });
        done = true;
        if (!shutdownStarted) {
          await initiateShutdown();
        }
        return;
      }
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

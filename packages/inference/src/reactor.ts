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
import { createToolResultTurn, createInboundTurn } from "./turns";
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

  function enqueue(event: ReactorInboundEvent): void {
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

  // When the last message in history is an assistant message with tool_calls
  // whose results haven't been appended yet, the conversation is in a
  // transient state. Inserting a user text message (from message.received)
  // at this point violates the provider's protocol: an assistant tool_call
  // must be immediately followed by the corresponding tool result messages.
  //
  // To prevent this, we check whether the history has pending tool_calls
  // and, if so, prioritize inference-cycle events (inference.done,
  // inference.error, tool.done) so the cycle completes before inbound
  // messages are interleaved. Once the tool results are in history, we
  // revert to FIFO so inbound messages are processed promptly.
  const CYCLE_EVENT_TYPES = new Set<ReactorInboundEvent["type"]>([
    "inference.done",
    "inference.error",
    "tool.done",
  ]);

  function historyHasPendingToolCalls(): boolean {
    if (stateManager === null) return false;
    const turns = stateManager.getTurns();
    if (turns.length === 0) return false;

    const last = turns.at(-1);
    if (last === undefined || last.role !== "assistant") return false;

    return last.content.some((b) => b.type === "tool_call");
  }

  function dequeueNext(): ReactorInboundEvent | undefined {
    if (queue.length === 0) return undefined;

    // Always process abort immediately.
    const abortIdx = queue.findIndex((e) => e.type === "abort");
    if (abortIdx !== -1) {
      return queue.splice(abortIdx, 1)[0];
    }

    // When mid-cycle (assistant tool_calls without tool results), drain
    // inference-cycle events before anything else.
    if (historyHasPendingToolCalls()) {
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
    // The director-requested model is informational. The active
    // InferenceSource is authoritative: `runInference` reads `source.model`
    // and emits `inference.start` with that model. The agent's capabilities
    // wrapper already rewrites `infer(model)` to use the source's model
    // before the action is built, so the value reaching here matches
    // `source.model` in practice — but we do not rely on that.
    _model: string,
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
      const maxRetries = 3;
      const defaultRetryMs = 60_000;

      for (let attempt = 0; attempt <= maxRetries; attempt++) {
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
          });
          return;
        }

        if (lastError !== undefined) {
          const err = lastError.data.error;
          if (
            err.category === "quota_exhausted" &&
            attempt < maxRetries &&
            !signal.aborted
          ) {
            const delayMs = err.retryAfterMs ?? defaultRetryMs;
            logger.warn`Rate limited (attempt ${String(attempt + 1)}/${String(maxRetries)}), retrying after ${String(delayMs)}ms`;
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
                partial: lastError.data.partial,
              });
              return;
            }
            continue;
          }

          enqueue({
            type: "inference.error",
            error: err,
            partial: lastError.data.partial,
          });
          return;
        }

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

      // Handle abort events: initiate shutdown regardless of director.
      if (event.type === "abort") {
        if (!shutdownStarted) {
          done = true;
          await initiateShutdown();
        }
        break;
      }

      // Append inbound messages to conversation history so the provider sees them.
      if (event.type === "message.received" && stateManager !== null) {
        const msg = createInboundTurn(event.message);
        if (msg !== null) {
          stateManager.appendTurn(msg);
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
        done = true;
        await initiateShutdown();
        break;
      }

      const validation = validateActions(actions);
      if (!validation.ok) {
        emitError(`Invalid action set: ${validation.error}`, true);
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
        done = true;
        await initiateShutdown();
        break;
      }

      // Handle wait: commit the cycle (if work happened) and return to the
      // event loop without shutting down.
      if (normalized.some((a) => a.type === "wait")) {
        await commitCycle();
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
          logger.error`Compaction failed: ${cause}`;
          emitError(
            `Compaction failed: ${cause instanceof Error ? cause.message : String(cause)}`,
            true,
          );
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
        await executeInfer(inferAction.model, inferAction.options);
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
        logger.error`Reactor loop threw unexpectedly: ${cause}`;
        emitError(
          `Internal reactor error: ${cause instanceof Error ? cause.message : String(cause)}`,
          true,
        );
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

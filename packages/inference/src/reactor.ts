// Agent reactor: the event-driven dispatch loop.
//
// The reactor processes one event at a time, asks the plugin for the next
// action, validates the action set, and executes. It manages the streaming
// harness for inference, dispatches tool calls, handles gates and correlation,
// and emits all session events with monotonic sequence numbers.
//
// Suspension semantics: when the plugin returns a suspend action, the reactor
// registers the gate and continues processing events. Inbound messages during
// suspension reach the plugin as message.received events (plugin decides:
// queue, fork, or ignore). When the gate clears, a reactor.gate.cleared event
// is enqueued and the plugin gets to decide next steps.
//
// (INFERENCE.md § Agent Reactor)

import type {
  InboundMessage,
  InferenceEvent,
  ProviderConfig,
  ReactorPlugin,
  ReactorInboundEvent,
  ContextStore,
  ToolRunner,
  TokenUsage,
  ConversationMessage,
  ToolResult,
  ToolCall,
  AbortReason,
  BeforeToolExtension,
  ReactorAction,
} from "@interchange/types/runtime";

import { getLogger } from "@interchange/log";
import { runInference } from "./harness";
import type { InferenceHarnessOptions } from "./harness";
import { createCapabilities } from "./plugin";
import { createGateManager } from "./gates";
import { createCorrelationRegistry } from "./correlation";
import { createStateManager } from "./state";
import { validateActions } from "./actions";
import { createToolResultMessage, createInboundMessage } from "./messages";
import type { CorrelationValidator } from "./correlation";

const logger = getLogger(["interchange", "reactor"]);

function buildHarnessOpts(
  messages: ConversationMessage[],
  model: string,
  providerConfig: ProviderConfig,
  options: Record<string, unknown>,
  signal: AbortSignal,
  nextSeq: () => number,
): InferenceHarnessOptions {
  if (Object.keys(options).length > 0) {
    return {
      messages,
      model,
      providerConfig,
      inferenceOptions:
        options as import("@interchange/types/runtime").InferenceOptions,
      signal,
      nextSeq,
    };
  }
  return { messages, model, providerConfig, signal, nextSeq };
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
  plugin: ReactorPlugin;
  providerConfig: ProviderConfig;
  toolRunner: ToolRunner;
  contextStore: ContextStore;
  correlationValidator?: CorrelationValidator;
  onEvent: (event: ReactorEmittedEvent) => void;
  inferenceRunner?: (
    opts: InferenceHarnessOptions,
  ) => AsyncGenerator<InferenceEvent>;
  beforeToolExtensions?: BeforeToolExtension[];
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
  abort(reason: string): void;
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
    plugin,
    toolRunner,
    contextStore,
    correlationValidator,
    onEvent,
    inferenceRunner = runInference,
    beforeToolExtensions = [],
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
    const messages = stateManager.getMessages();
    if (messages.length === 0) return false;

    const last = messages.at(-1);
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
      const msg = createInboundMessage(message);
      if (msg !== null) {
        stateManager.appendMessage(msg);
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

  async function executeInfer(
    model: string,
    options: Record<string, unknown>,
  ): Promise<void> {
    if (stateManager === null) return;

    const signal = operationController.signal;
    const messages = stateManager.getMessages();

    const p = (async () => {
      const harnessOpts = buildHarnessOpts(
        messages,
        model,
        config.providerConfig,
        options,
        signal,
        nextSeq,
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
          stateManager.appendMessage(lastDone.data.message);
          stateManager.accumUsage(lastDone.data.usage);
        }
        enqueue({
          type: "inference.done",
          message: lastDone.data.message,
          usage: lastDone.data.usage,
        });
      } else if (lastError !== undefined) {
        enqueue({
          type: "inference.error",
          error: lastError.data.error,
          partial: lastError.data.partial,
        });
      } else {
        emitError("Inference runner returned without a terminal event", true);
        enqueue({
          type: "inference.error",
          error: {
            category: "fatal",
            message: "Inference runner returned without a terminal event",
          },
          partial: { text: "" },
        });
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
      const result = await toolRunner.run(call, signal);
      emit({ type: "tool.done", seq: nextSeq(), data: { result } });

      if (result.pendingMarker !== undefined && stateManager !== null) {
        const marker = result.pendingMarker;
        const gateId = `pending-${marker.correlationId}`;
        const op: import("@interchange/types/runtime").PendingOperation = {
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

      return result;
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

    if (addToHistory && stateManager !== null) {
      stateManager.appendMessage(createToolResultMessage(results));
    }

    for (const result of results) {
      enqueue({ type: "tool.done", result });
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

      // Handle abort events: initiate shutdown regardless of plugin.
      if (event.type === "abort") {
        if (!shutdownStarted) {
          done = true;
          await initiateShutdown();
        }
        break;
      }

      // Append inbound messages to conversation history so the provider sees them.
      if (event.type === "message.received" && stateManager !== null) {
        const msg = createInboundMessage(event.message);
        if (msg !== null) {
          stateManager.appendMessage(msg);
        }
      }

      let actions;
      try {
        actions = await plugin.decide(
          event,
          stateManager.snapshot(),
          capabilities,
        );
      } catch (cause) {
        const msg = cause instanceof Error ? cause.message : String(cause);

        logger.error`Plugin threw during decide: ${cause}`;
        emitError(`Plugin exception: ${msg}`, true);
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

      // Checkpoint fires before everything else.
      const checkpointAction = normalized.find(
        (a): a is Extract<ReactorAction, { type: "checkpoint" }> =>
          a.type === "checkpoint",
      );
      if (checkpointAction !== undefined) {
        await executeCheckpoint(checkpointAction.message);
      }

      // Emit custom events (validated type namespace).
      for (const action of normalized) {
        if (action.type === "emit") {
          const reserved = ["inference.", "tool.", "reactor.", "fork."];
          const blocked = reserved.some((p) => action.eventType.startsWith(p));
          if (blocked) {
            emitError(
              `Plugin tried to emit reserved event type: ${action.eventType}`,
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
        done = true;
        await initiateShutdown();
        break;
      }

      // Handle wait: return to the event loop without shutting down.
      if (normalized.some((a) => a.type === "wait")) {
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
        // Loop continues — plugin will receive the gate.cleared event later.
        continue;
      }

      // Handle reply — emit the content for the harness/supervisor to send.
      const replyAction = normalized.find((a) => a.type === "reply");
      if (replyAction !== undefined && replyAction.type === "reply") {
        emit({
          type: "connector.reply",
          seq: nextSeq(),
          data: { content: replyAction.content },
        });
        // After replying, wait for the next inbound message.
        continue;
      }

      // Handle infer.
      const inferAction = normalized.find((a) => a.type === "infer");
      if (inferAction !== undefined && inferAction.type === "infer") {
        await executeInfer(
          inferAction.model,
          (inferAction.options ?? {}) as Record<string, unknown>,
        );
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
    }
  }

  function emitError(message: string, fatal: boolean): void {
    emit({
      type: "reactor.error",
      seq: nextSeq(),
      data: { error: message, fatal },
    });
  }

  async function executeCheckpoint(message: string): Promise<void> {
    if (stateManager === null) return;
    try {
      await contextStore.commit(
        stateManager.getMessages(),
        stateManager.getPendingOperations(),
        stateManager.getTokenUsage(),
        message,
      );
    } catch (cause) {
      logger.error`Checkpoint failed: ${cause}`;
      emitError(
        `Checkpoint failed: ${cause instanceof Error ? cause.message : String(cause)}`,
        false,
      );
      return;
    }
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
      let initialMessages: ConversationMessage[];
      let initialOps;
      let initialUsage: TokenUsage;
      try {
        const loaded = await contextStore.load();
        initialMessages = loaded.messages;
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
        initialMessages,
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

  function abort(reason: string): void {
    enqueue({ type: "abort", reason: reason as AbortReason });
  }

  return { start, deliver, abort };
}

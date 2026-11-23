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
  PersistedPendingOperation,
  ReactorAction,
  ToolResultTransform,
  ContextTransform,
  Compactor,
  TransformRecord,
  StrategyContext,
  StrategyResult,
  TurnEndedData,
  TurnFailureKind,
} from "@intx/types/runtime";

import { getLogger } from "@intx/log";
import { ApprovalDecision, signalKindToGateType } from "@intx/types/signals";
import { PerCallInferenceOptions } from "@intx/types/runtime-core";
import type { CredentialMaterialResolver } from "@intx/types";
import { canonicalJsonStringify } from "@intx/types/wire-definition-hash";
import { type } from "arktype";
import { runInference } from "./harness";
import { classifyAbortError } from "./errors";
import type { Dependencies, InferenceHarnessOptions } from "./harness";
import { createCapabilities } from "./director";
import { createGateManager, type GateRecord } from "./gates";
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

/**
 * Layer director-named options over per-send ones; a director key wins over
 * the per-send value for that same key. The merge is shallow: a director that
 * names `thinking` replaces the whole object, so nested per-send fields such
 * as `budgetTokens` are dropped rather than merged in.
 */
export function mergeInferenceOptions(
  perSend: InferenceOptions | undefined,
  director: InferenceOptions | undefined,
): InferenceOptions | undefined {
  if (perSend === undefined) return director;
  return { ...perSend, ...director };
}

/**
 * Copy only the keys a send may set. A structurally wider bag still
 * type-checks as `PerCallInferenceOptions`; without this copy,
 * `systemPrompt`, `tools`, and `providerOptions` would enter the
 * turn's options and displace the deployed agent definition. The key
 * set is the arktype declaration itself, so a key added there is copied
 * without a matching edit here. The declaration also checks the values,
 * so an out-of-range option is rejected here rather than at the provider.
 */
const StripToPerCallInference =
  PerCallInferenceOptions.onDeepUndeclaredKey("delete");

function perCallInference(
  options: PerCallInferenceOptions,
): PerCallInferenceOptions {
  const stripped = StripToPerCallInference(options);
  if (stripped instanceof type.errors) {
    throw new Error(`Invalid per-call inference options: ${stripped.summary}`);
  }
  return stripped;
}

function buildHarnessOpts(
  turns: ConversationTurn[],
  source: InferenceSource,
  options: InferenceOptions | undefined,
  signal: AbortSignal,
  nextSeq: () => number,
  readMaterial: CredentialMaterialResolver | undefined,
  deps: Dependencies,
): InferenceHarnessOptions {
  // exactOptionalPropertyTypes is on: only set the optional keys when defined.
  return {
    turns,
    source,
    ...(options !== undefined ? { inferenceOptions: options } : {}),
    signal,
    nextSeq,
    ...(readMaterial !== undefined ? { readMaterial } : {}),
    deps,
  };
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
  /**
   * Resolves the active source's credential secret by `credentialId` from the
   * run's credential cell at send time. Read live per attempt, so a failover to
   * a source with a different `credentialId` resolves that source's credential.
   * Optional: the harness installs a fail-closed default when it is omitted.
   */
  readMaterial?: CredentialMaterialResolver;
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
  /**
   * Number of consecutive identical tool batches in one turn that trips
   * doom-loop detection. A batch's identity is its set of executed tool
   * calls; a runaway model repeating the same call burns inference cost with
   * no progress. On the Nth consecutive identical batch the reactor emits a
   * fatal `reactor.error`, ends the turn `failed` with kind `doom_loop`, and
   * shuts down. Must be a positive integer.
   * Pass `false` to disable doom-loop detection entirely. Defaults to
   * `DEFAULT_DOOM_LOOP_THRESHOLD`.
   */
  doomLoopThreshold?: number | false;
};

export type DeliverOptions = {
  inference?: PerCallInferenceOptions;
  /**
   * The id of the turn this delivery belongs to. A turn is one inbound
   * request; its id spans every cycle and every resume of that request.
   * When omitted, the reactor mints a fresh id for the delivery.
   */
  turnId?: string;
};

export type Reactor = {
  /** Begin processing. Emits reactor.start. Must be called exactly once. */
  start(): void;
  /**
   * Inject an inbound message into the reactor. `opts.inference` applies to
   * every later inference call of the turn the delivery starts, or of the
   * parked turn it resumes, beneath any option the director names; it is
   * never retained past that turn. A resume with no options keeps the
   * options the turn has. `opts.turnId` names the delivery's turn;
   * the reactor stamps it on the `inference.start`, `inference.done`,
   * `connector.reply` and `reactor.gate.blocked` events of the cycles the
   * delivery starts. A delivery with no `turnId` gets a fresh one. A
   * correlated delivery that resumes a parked operation continues the
   * operation's turn instead. A director gate on an existing pending
   * operation with no live gate moves that operation to the turn that
   * suspends.
   *
   * Throws when `opts.turnId` names a turn that already ended. A delivery
   * that names a turn whose abandon is still pending is dropped. A delivery
   * after the reactor is done ends its turn `failed` at once, unless that
   * turn is open in this process (started and not ended, parked on a
   * pending operation, or with a queued event): such a turn survives for
   * the restart, and the reactor drops the delivery with no event. A caller
   * that names another live turn as `opts.turnId` misuses the reactor.
   */
  deliver(message: InboundMessage, opts?: DeliverOptions): void;
  /**
   * Abandon a turn: its caller gave up. The reactor follows a `joined` turn
   * to the turn it joined. It aborts the turn's operation in flight, drops
   * the turn's queued events, removes its gates and their operations,
   * answers its open tool calls with an error result, commits, and ends the
   * turn `abandoned`, with no director call. The director never sees the
   * turn again, and a later `connector.reply` or `reactor.gate.blocked` of
   * the turn is suppressed. While the abandon is pending, a delivery that
   * names the turn is dropped; once the turn has ended, `deliver()` that
   * names it throws, as for any ended turn. The work runs on the loop, so
   * this call emits no event. It is idempotent, does nothing for an ended
   * turn or after the reactor is done, and never throws.
   */
  abandonTurn(turnId: string): void;
  /** Initiate graceful shutdown with a reason. */
  abort(reason: AbortReason): void;
};

const DEFAULT_GATE_TIMEOUT_MS = 3_600_000;
const DEFAULT_SHUTDOWN_TIMEOUT_MS = 30_000;
const DEFAULT_DOOM_LOOP_THRESHOLD = 3;

/**
 * The number of gates one turn can hold at a time. The limit counts gates of
 * every type, not only approvals: the single in-flight suspend slot, the one
 * correlation that `send()` reports, the runtime parking of one correlation,
 * and the connector router's map of parked turns by correlation id all hold
 * one gate per turn, whatever its type (INFERENCE.md § Gates).
 */
const MAX_GATES_PER_TURN = 1;

// The error result of a before-tool suspend that the gate limit refuses. It
// names no gate type, because the gate the turn holds can be of any type.
const GATE_LIMIT_REFUSAL =
  "Another call in this turn is waiting on a gate. Request this call again after that decision.";

// The result that answers each open call of an abandoned turn.
const ABANDONED_CALL_RESULT = "turn abandoned";

// The result of a call that did not start because the reactor was aborted.
const ABORTED_CALL_RESULT = "reactor aborted";

/**
 * The turn id of a pending operation that was persisted before turn ids
 * existed. The id is stable across restarts because it derives from the
 * operation's correlation id.
 */
export function legacyTurnId(correlationId: string): string {
  return `legacy:${correlationId}`;
}

// A pending operation as the reactor loads it. Older state can lack two
// fields. An operation persisted before turn ids existed gets a legacy turn
// id. An operation persisted before `gated` existed counts as gated, because
// that state re-armed a gate for every operation.
function loadedOperation(op: PersistedPendingOperation): PendingOperation {
  return {
    ...op,
    turnId: op.turnId ?? legacyTurnId(op.correlationId),
    gated: op.gated ?? true,
  };
}

/**
 * Resolve the caller-facing `doomLoopThreshold` into the reactor's internal
 * form: a positive integer when detection is active, or `null` when it is
 * disabled. `undefined` (omitted) takes the default; `false` disables; a
 * number is validated here — the construction edge is the one place that owns
 * the default and rejects a malformed value loudly, so a stray `0`, negative,
 * or non-integer throws rather than silently disarming the guard. Downstream
 * code compares against the returned `number | null` and never sees the raw
 * `false`, whose numeric coercion would otherwise trip the loop immediately.
 */
function resolveDoomLoopThreshold(
  raw: number | false | undefined,
): number | null {
  if (raw === false) return null;
  if (raw === undefined) return DEFAULT_DOOM_LOOP_THRESHOLD;
  if (!Number.isInteger(raw) || raw < 1) {
    throw new Error(
      `doomLoopThreshold must be a positive integer or false, got ${String(raw)}`,
    );
  }
  return raw;
}

/**
 * Order-independent identity of a batch of executed tool calls. Each call
 * canonicalizes to its name and arguments (the call `id` is excluded, since it
 * differs on every request); sorting makes a parallel batch match regardless
 * of the order the model emitted its calls. Two turns share a signature when
 * they run the same multiset of `(name, arguments)` pairs.
 */
function toolBatchSignature(calls: ToolCall[]): string {
  return calls.map(callSignature).sort().join("\n");
}

// A call's name and arguments in canonical form, without its id.
function callSignature(call: ToolCall): string {
  return canonicalJsonStringify({ name: call.name, arguments: call.arguments });
}

const NO_APPROVALS: ReadonlySet<ToolCall> = new Set();

/**
 * The calls of an `execute_tools` action that a `resume.execute_tools` event
 * approved: each action call that matches an approved call on its id, name
 * and arguments. Each approved call matches at most one action call, so a
 * second action call with the same id gets no approval. A call the director
 * changed gets none either, and hits its ask gate again (fails closed). The
 * set holds the action's call objects, so two calls that share an id in one
 * batch cannot share an approval.
 */
function approvedCallsOf(
  approvedCalls: ToolCall[],
  actionCalls: ToolCall[],
): ReadonlySet<ToolCall> {
  const unmatched = approvedCalls.map((call) => ({
    id: call.id,
    signature: callSignature(call),
  }));
  const approved = new Set<ToolCall>();
  for (const call of actionCalls) {
    const signature = callSignature(call);
    const index = unmatched.findIndex(
      (a) => a.id === call.id && a.signature === signature,
    );
    if (index === -1) continue;
    unmatched.splice(index, 1);
    approved.add(call);
  }
  return approved;
}

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

  // Resolved once at the construction edge: a positive integer while detection
  // is active, or `null` when the caller disabled it with `false`. Every
  // downstream comparison reads this binding, never the raw config value.
  const doomLoopThreshold = resolveDoomLoopThreshold(config.doomLoopThreshold);

  // Monotonic sequence counter, scoped to this session.
  let seq = 0;
  function nextSeq(): number {
    return ++seq;
  }

  // Set once `reactor.done` is emitted.
  let doneEmitted = false;

  // Two rules hold for every event, so the one place that sees every event
  // enforces them:
  //
  // - No event that carries a turn's id follows its `reactor.turn.ended`. A
  //   turn can end while work of it is still in flight: a correlation-
  //   dispatch failure ends the running turn during its inference, its tool
  //   batch, or its suspend commit, and each of those paths would emit for
  //   the turn when it settles. Events that carry no turn id (such as
  //   `tool.start`, `tool.done`, the inference deltas, `inference.error` and
  //   `reactor.gate.cleared`) are not dropped here, so during that fatal
  //   shutdown they can still follow the turn's failed end, up to
  //   `reactor.done`.
  // - Only `reactor.turn.ended` follows `reactor.done`. Work that the
  //   shutdown does not wait for, such as a director suspend whose commit
  //   outlives the shutdown, can still settle after it. A delivery after done
  //   still ends its turn, so that one event passes.
  //
  // - No `connector.reply` or `reactor.gate.blocked` of an abandoned turn is
  //   emitted. A reply commit or a suspend commit can be in flight when the
  //   abandon arrives; the turn's caller gave up, so its reply and park are
  //   suppressed here, the one owner of that rule.
  //
  // A dropped event already took its sequence number, so a drop can leave a
  // gap in the sequence numbers. Apart from the abandon rule, a drop happens
  // only on a fatal-failure path, right before or after `reactor.done`.
  function emit(event: ReactorEmittedEvent): void {
    if (doneEmitted && event.type !== "reactor.turn.ended") return;
    const eventTurnId = turnIdOf(event);
    if (eventTurnId !== undefined && endedTurns.has(eventTurnId)) return;
    if (
      (event.type === "connector.reply" ||
        event.type === "reactor.gate.blocked") &&
      eventTurnId !== undefined &&
      abandonedTurns.has(eventTurnId)
    ) {
      return;
    }
    onEvent(event);
    if (event.type === "reactor.done") doneEmitted = true;
  }

  // The turn id an event carries, except on `reactor.turn.ended`, which is
  // the end itself.
  function turnIdOf(event: ReactorEmittedEvent): string | undefined {
    switch (event.type) {
      case "inference.start":
      case "inference.done":
      case "connector.reply":
      case "reactor.gate.blocked":
      case "reactor.turn.started":
        return event.data.turnId;
      default:
        return undefined;
    }
  }

  // Inbound event queue. Events are pushed here and drained by the loop. Each
  // entry carries the id of the turn its event belongs to. Only `abort`
  // belongs to no turn.
  type AbortEvent = Extract<ReactorInboundEvent, { type: "abort" }>;
  type TurnEvent = Exclude<ReactorInboundEvent, AbortEvent>;
  // An internal event that abandons its turn (see abandonTurn). The loop
  // handles it itself, and the director never sees it.
  type AbandonEvent = { type: "reactor.abandon" };
  type QueuedEvent =
    | { event: AbortEvent; turnId: null }
    | { event: TurnEvent | AbandonEvent; turnId: string };
  const queue: QueuedEvent[] = [];
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
  const CYCLE_EVENT_TYPES = new Set<QueuedEvent["event"]["type"]>([
    "inference.done",
    "inference.error",
    "tool.done",
  ]);

  let pendingContinuations = 0;

  function enqueue(event: AbortEvent, turnId: null): void;
  function enqueue(event: TurnEvent | AbandonEvent, turnId: string): void;
  function enqueue(
    event: ReactorInboundEvent | AbandonEvent,
    turnId: string | null,
  ): void {
    // No event is queued for an ended turn. After done this is routine: in-
    // flight work settles after a fatal path ended its turn. While the
    // reactor runs, delivery admission never queues for an ended turn, so a
    // hit here is a bug; it is reported and the event is dropped.
    if (turnId !== null && endedTurns.has(turnId)) {
      if (!done) {
        logger.error`Event ${event.type} queued for ended turn ${turnId}`;
        emitError(`Event ${event.type} for ended turn ${turnId}`, false);
      }
      return;
    }
    if (CYCLE_EVENT_TYPES.has(event.type)) {
      pendingContinuations += 1;
    }
    if (event.type === "abort") {
      queue.push({ event, turnId: null });
    } else if (turnId !== null) {
      queue.push({ event, turnId });
    } else {
      throw new Error(`Event ${event.type} enqueued without a turn`);
    }
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

  function dequeueNext(): QueuedEvent | undefined {
    if (queue.length === 0) return undefined;

    // Always process abort immediately.
    const abortIdx = queue.findIndex((e) => e.event.type === "abort");
    if (abortIdx !== -1) {
      return queue.splice(abortIdx, 1)[0];
    }

    // Mid-cycle: drain inference-cycle events before anything else so the
    // outstanding inference or tool batch completes before new mail can start
    // an overlapping inference. An abandon of the turn that owns those cycle
    // events goes first: it purges them. An abandon of another turn waits
    // until the cycle is quiescent, so its history append cannot land in the
    // middle of another turn's tool batch.
    if (pendingContinuations > 0) {
      const idx = queue.findIndex((e) => CYCLE_EVENT_TYPES.has(e.event.type));
      if (idx !== -1) {
        const cycleTurn = queue[idx]?.turnId;
        const abandonIdx = queue.findIndex(
          (e) => e.event.type === "reactor.abandon" && e.turnId === cycleTurn,
        );
        return queue.splice(abandonIdx !== -1 ? abandonIdx : idx, 1)[0];
      }
    }

    // Quiescent: the first abandon goes first, else FIFO.
    const abandonIdx = queue.findIndex(
      (e) => e.event.type === "reactor.abandon",
    );
    if (abandonIdx !== -1) return queue.splice(abandonIdx, 1)[0];

    return queue.shift();
  }

  // Gate timeouts run on the injected scheduler, so a test can fire them.
  const gates = createGateManager(deps.scheduler);
  const correlations = createCorrelationRegistry();
  const capabilities = createCapabilities();

  let stateManager: ReturnType<typeof createStateManager> | null = null;
  let running = false;
  let done = false;
  let shutdownStarted = false;
  // Correlation state is empty until context loading and gate rehydration
  // finish. Hold early deliveries so a resumed approval cannot be mistaken
  // for a new conversation message during that startup window.
  let startupDeliveries:
    | {
        message: InboundMessage;
        turnId: string;
        inference: PerCallInferenceOptions | undefined;
      }[]
    | null = [];

  // The turn of the event the loop is handling now. It is set from each
  // dequeued entry and is null while the loop waits for an event, so a read
  // outside the handling of a turn's event fails at once.
  let cycleTurnId: string | null = null;

  function requireCycleTurnId(): string {
    if (cycleTurnId === null) {
      throw new Error("No turn is running in the reactor");
    }
    return cycleTurnId;
  }

  // The state of each live or staged turn. A delivery stages a turn's state
  // before the loop starts it, so the per-send options wait there. A parked
  // turn keeps its state, also its doom-loop counters, until it ends. The
  // counters are not persisted, so a restart resets them to zero.
  type TurnState = {
    started: boolean;
    inference: PerCallInferenceOptions | undefined;
    replied: boolean;
    lastToolBatchSignature: string | null;
    toolBatchRepeatCount: number;
    lastToolBatchNames: string[];
    // The ids of the turn's tool calls that history holds with no result
    // yet: added when an inference appends its assistant turn, removed when
    // a call gets its result. An abandon answers them. Not persisted.
    openCalls: Set<string>;
  };
  const turnStates = new Map<string, TurnState>();
  // The turns that abandonTurn abandoned. A turn stays here after its end,
  // so a delivery that names it later is still discarded. Like endedTurns,
  // the set grows by one id for each abandoned turn for the life of the
  // process.
  const abandonedTurns = new Set<string>();
  // Gates of an abandoned turn whose gate.blocked was dropped because the
  // abandon landed during the suspend's commit. The abandon clears each one
  // with no gate.cleared, and removes it from this set.
  const unannouncedGates = new Set<string>();
  // For each turn that ended `joined`, the turn it joined. abandonTurn
  // follows this chain to the turn that holds the work. Like endedTurns, the
  // map grows by one entry for each join for the life of the process.
  const joinedInto = new Map<string, string>();
  // The turns that ended in this process. The reactor emits at most one
  // `reactor.turn.ended` for each turn, and an event of an ended turn is
  // never handled again. The set grows by one id for each turn for the life
  // of the process.
  const endedTurns = new Set<string>();

  function turnState(turnId: string): TurnState {
    let s = turnStates.get(turnId);
    if (s === undefined) {
      s = {
        started: false,
        inference: undefined,
        replied: false,
        lastToolBatchSignature: null,
        toolBatchRepeatCount: 0,
        lastToolBatchNames: [],
        openCalls: new Set<string>(),
      };
      turnStates.set(turnId, s);
    }
    return s;
  }

  function requireTurnState(turnId: string): TurnState {
    const s = turnStates.get(turnId);
    if (s === undefined) {
      throw new Error(`Turn ${turnId} has no state`);
    }
    return s;
  }

  // Emit `reactor.turn.started` the first time the loop handles an event of
  // a turn in this process. A resume during the first cycle of a turn emits
  // no second start.
  function startTurn(event: TurnEvent, turnId: string): void {
    if (endedTurns.has(turnId)) {
      throw new Error(`Event ${event.type} for ended turn ${turnId}`);
    }
    const s = turnState(turnId);
    if (s.started) return;
    if (CYCLE_EVENT_TYPES.has(event.type)) {
      throw new Error(
        `Event ${event.type} for turn ${turnId} before its start`,
      );
    }
    s.started = true;
    const messageId =
      event.type === "message.received"
        ? event.message.headers.messageId
        : undefined;
    emit({
      type: "reactor.turn.started",
      seq: nextSeq(),
      data: {
        turnId,
        resumed: event.type !== "message.received",
        ...(messageId !== undefined ? { messageId } : {}),
      },
    });
  }

  // End a started turn that has no live gate and no queued event. A turn
  // with either still has work, and a later cycle settles it again.
  function settleTurn(turnId: string): void {
    const s = turnStates.get(turnId);
    if (s === undefined || !s.started) return;
    if (
      gates.countForTurn(turnId) > 0 ||
      queue.some((e) => e.turnId === turnId)
    ) {
      return;
    }
    endTurn(
      s.replied
        ? { turnId, outcome: "replied" }
        : { turnId, outcome: "unanswered" },
    );
  }

  function endTurn(data: TurnEndedData): void {
    if (endedTurns.has(data.turnId)) return;
    endedTurns.add(data.turnId);
    turnStates.delete(data.turnId);
    if (data.outcome === "joined") {
      joinedInto.set(data.turnId, data.joinedTurnId);
    }
    emit({ type: "reactor.turn.ended", seq: nextSeq(), data });
  }

  function failTurn(
    turnId: string,
    message: string,
    kind: TurnFailureKind,
  ): void {
    endTurn({ turnId, outcome: "failed", error: { message, kind } });
  }

  // The turn whose cycle runs now. Between two events the loop holds no
  // turn, but a dispatched operation that settled has queued its cycle event
  // under the turn it runs for.
  function runningTurn(): string | null {
    return (
      cycleTurnId ??
      queue.find((e) => CYCLE_EVENT_TYPES.has(e.event.type))?.turnId ??
      null
    );
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
  // no tool call, so it must force the cycle commit. A refused director
  // suspend forces it too (see `commitRefusedSuspend`).
  let cycleSuspended = false;
  // Set when a pending operation changed outside a suspend (a gate timed out
  // and its operation is no longer gated). The next commit persists it.
  let operationsChanged = false;
  // The turn an abandon removed state of in this cycle. It forces the cycle
  // commit and names the commit.
  let cycleAbandonedTurn: string | null = null;

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

  // The correlation ids whose delivery is in progress. verifyCorrelation
  // adds an id; a rejected validation releases it at once, and a matched id
  // stays until processDelivery's finally, after the admission is applied.
  // deliver() is fire-and-forget async: two deliveries for one correlation
  // can interleave across the validator's await, and with no validator both
  // can verify in the same tick, so the id must stay held until the apply.
  const correlatingIds = new Set<string>();

  // How the reactor resumes a correlated pending operation.
  //
  //   redispatch — an approved approval re-runs its parked tool call. The
  //     queued resume.execute_tools carries the call, and the loop passes the
  //     approval to the before-tool run of that exact call (approvedCallsOf);
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

  // Decide how a correlated approval-kind pending operation resumes. It
  // changes no state; the approval itself travels on the queued event. An
  // operation that carries
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
            // Re-dispatch the exact parked call. The loop passes the approval
            // to its before-tool run.
            return { mode: "redispatch", calls: [suspendedCall] };
          case "rejected": {
            // The approver denied the call. Answer the parked call with a
            // synthetic error result rather than re-running it, so the tool
            // never executes. The approver's
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

  // Delivery admission. A delivery awaits once, in verifyCorrelation, and
  // changes no state before that await, except that it marks its
  // correlation as in progress (correlatingIds). After it, admitDelivery
  // reads the current state and returns one decision, and applyAdmission
  // carries it out, both in the same synchronous step. No other code on the delivery
  // path changes state. A correlation ends the delivered turn only when this
  // process has not seen it: the turn has not ended and this process does
  // not hold it open (see turnPhase).
  type Verdict =
    | { kind: "uncorrelated" }
    | { kind: "matched"; correlationId: string };

  // "ended": the turn ended in this process. "open": not ended, and started,
  // or owns a registered pending operation, or has a queued event (after a
  // restart, a parked turn whose rehydrated gate fired before it started).
  // "unseen": neither.
  type TurnPhase = "unseen" | "open" | "ended";

  type Admission = { deliveredPhase: TurnPhase } & (
    | { kind: "after-done" }
    | { kind: "discard" }
    | { kind: "message"; turnId: string }
    | {
        kind: "consume";
        correlationId: string;
        op: PendingOperation | undefined;
        appendHistory: boolean;
        endDelivered: TurnEndedData | null;
      }
    | {
        kind: "resume";
        correlationId: string;
        op: PendingOperation;
        gate: GateRecord;
        // The turn the resume continues.
        resumedTurnId: string;
        endDelivered: TurnEndedData | null;
      }
  );

  // The only await of the delivery path. Apart from the in-progress guard
  // it reads and never writes: it returns "matched" when an operation is
  // registered for the message's correlation id, no other delivery for that
  // id is in progress, and the validator accepts. The id stays in
  // correlatingIds from here until the matched delivery is applied.
  async function verifyCorrelation(message: InboundMessage): Promise<Verdict> {
    const correlationId = message.headers.interchangeCorrelationId;
    if (correlationId === undefined) return { kind: "uncorrelated" };
    if (correlatingIds.has(correlationId)) return { kind: "uncorrelated" };
    const pending = correlations.lookup(correlationId);
    if (pending === undefined) return { kind: "uncorrelated" };

    correlatingIds.add(correlationId);
    if (correlationValidator !== undefined) {
      let valid: boolean;
      try {
        valid = await correlationValidator.validate(pending, message);
      } catch (cause) {
        logger.warn`Correlation validator threw for ${correlationId}: ${cause}`;
        correlatingIds.delete(correlationId);
        return { kind: "uncorrelated" };
      }
      if (!valid) {
        correlatingIds.delete(correlationId);
        return { kind: "uncorrelated" };
      }
    }
    // A matched verdict keeps the id in correlatingIds. processDelivery
    // releases it after the admission is applied, so a second delivery for
    // the same correlation, even one made in the same tick, finds it in
    // progress and becomes a regular message.
    return { kind: "matched", correlationId };
  }

  function turnPhase(turnId: string): TurnPhase {
    if (endedTurns.has(turnId)) return "ended";
    if (
      turnStates.get(turnId)?.started === true ||
      correlations.all().some((op) => op.turnId === turnId) ||
      queue.some((e) => e.turnId === turnId)
    ) {
      return "open";
    }
    return "unseen";
  }

  // Decide what a delivery does, from the state at this moment. The
  // operation can have timed out, resumed through another path, or moved to
  // the turn that now waits for it while the validator ran; the decision
  // uses the operation registered now, under its current turn.
  function admitDelivery(deliveredTurnId: string, verdict: Verdict): Admission {
    const deliveredPhase = turnPhase(deliveredTurnId);
    if (done) return { deliveredPhase, kind: "after-done" };
    // The caller of an abandoned turn gave up: a delivery that names it is
    // dropped with no event, also one that would resume a parked turn.
    if (abandonedTurns.has(deliveredTurnId)) {
      return { deliveredPhase, kind: "discard" };
    }
    if (verdict.kind === "uncorrelated") {
      // A delivery that does not correlate is a new message under its
      // turn. When that turn ended while the validator ran, the message gets
      // a new turn, as a delivery that names no turn does.
      return {
        deliveredPhase,
        kind: "message",
        turnId:
          deliveredPhase === "ended" ? crypto.randomUUID() : deliveredTurnId,
      };
    }
    const { correlationId } = verdict;
    const unanswered: TurnEndedData | null =
      deliveredPhase === "unseen"
        ? { turnId: deliveredTurnId, outcome: "unanswered" }
        : null;
    const op = correlations.lookup(correlationId);
    if (op === undefined) {
      return {
        deliveredPhase,
        kind: "consume",
        correlationId,
        op: undefined,
        appendHistory: false,
        endDelivered: unanswered,
      };
    }
    const gate = gates.findByCorrelationId(correlationId);
    // An approval resumes on the operation's turn (re-run or denied
    // result). An async response clears the gate, and the gate's
    // gate.cleared resumes the gate's turn. A marker registered while a
    // director gate already waits on its correlation takes that gate's
    // turn, so the operation's turn and the gate's turn are the same.
    const resumedTurnId =
      gate === undefined || op.suspendedCall !== undefined
        ? op.turnId
        : gate.turnId;
    // A turn that is being abandoned resumes nothing: its abandon removes
    // the operation and answers its call.
    if (abandonedTurns.has(resumedTurnId) && !endedTurns.has(resumedTurnId)) {
      return {
        deliveredPhase,
        kind: "consume",
        correlationId,
        op: undefined,
        appendHistory: false,
        endDelivered: unanswered,
      };
    }
    if (gate === undefined) {
      // An approval with no gate timed out during its suspend's commit; the
      // deferred clear answers its call when the suspend completes, so the
      // operation is left to it. An async response with no gate lands in
      // history and resumes nothing.
      const isAsync = op.suspendedCall === undefined;
      return {
        deliveredPhase,
        kind: "consume",
        correlationId,
        op,
        appendHistory: isAsync,
        endDelivered: unanswered,
      };
    }
    return {
      deliveredPhase,
      kind: "resume",
      correlationId,
      op,
      gate,
      resumedTurnId,
      endDelivered:
        deliveredPhase === "unseen"
          ? {
              turnId: deliveredTurnId,
              outcome: "joined",
              joinedTurnId: resumedTurnId,
            }
          : null,
    };
  }

  // Carry out an admission. This is the only code on the delivery path that
  // changes state. A malformed approval decision throws from
  // resumePendingOperation before any change (a correlation-dispatch
  // failure).
  function applyAdmission(
    admission: Admission,
    message: InboundMessage,
    deliveredTurnId: string,
    inference: PerCallInferenceOptions | undefined,
  ): void {
    switch (admission.kind) {
      case "discard":
        return;
      case "after-done":
        // A turn this process holds open survives for the restart, and an
        // ended turn already has its end, so only an unseen turn ends here.
        if (admission.deliveredPhase === "unseen") {
          failTurn(deliveredTurnId, "The reactor is done", "reactor_done");
        }
        return;
      case "message": {
        const turnId = admission.turnId;
        if (turnId !== deliveredTurnId) {
          logger.warn`Delivery named turn ${deliveredTurnId}, which ended while its correlation was validated; it runs under new turn ${turnId}`;
        }
        if (inference !== undefined) turnState(turnId).inference = inference;
        emit({ type: "message.received", seq: nextSeq(), data: { message } });
        enqueue({ type: "message.received", message }, turnId);
        return;
      }
      case "consume": {
        const { correlationId, op } = admission;
        if (op !== undefined && admission.appendHistory) {
          correlations.remove(correlationId);
          if (stateManager !== null) {
            stateManager.removePendingOperation(correlationId);
            const msg = createInboundTurn(message);
            if (msg !== null) stateManager.appendTurn(msg);
          }
        }
        emit({
          type: "message.correlated",
          seq: nextSeq(),
          data: { message, correlationId },
        });
        if (admission.endDelivered !== null) endTurn(admission.endDelivered);
        return;
      }
      case "resume": {
        const { correlationId, op, gate } = admission;
        const dispatch = resumePendingOperation(op, message);
        // A resume's options apply to the turn it continues, and a resume
        // with no options keeps the options that turn has.
        if (inference !== undefined) {
          turnState(admission.resumedTurnId).inference = inference;
        }
        switch (dispatch.mode) {
          case "redispatch":
          case "error_result":
            // Clear the gate WITHOUT enqueuing its gate.cleared: the
            // re-dispatched call, or the synthetic error result of a denied
            // call, is the resumption, so a gate.cleared-driven re-infer would
            // double the continuation. The gate.cleared is still emitted for
            // observers, after the gate's blocked (see InFlightSuspend).
            gates.clearSilently(gate.gateId);
            if (suspendingGate?.gateId === gate.gateId) {
              suspendingGate.clearedOnAdmission = true;
            } else {
              emit({
                type: "reactor.gate.cleared",
                seq: nextSeq(),
                data: { gateId: gate.gateId, reason: "resolved" },
              });
            }
            correlations.remove(correlationId);
            if (stateManager !== null) {
              stateManager.setGatesSnapshot(gates.snapshot());
              stateManager.removePendingOperation(correlationId);
            }
            // A call is open from the inference that asked it, or from the
            // admission that resumes it, until its result. After a restart
            // only this admission records the re-dispatched call, so an
            // abandon before its result still answers it.
            if (dispatch.mode === "redispatch") {
              const open = turnState(op.turnId).openCalls;
              for (const call of dispatch.calls) open.add(call.id);
            }
            // The re-dispatch event carries the approved call. The director
            // sees this event before the re-dispatched call's tool.done.
            enqueue(
              dispatch.mode === "redispatch"
                ? { type: "resume.execute_tools", calls: dispatch.calls }
                : { type: "resume.tool_result", result: dispatch.result },
              op.turnId,
            );
            break;
          case "gate-cleared":
            // Async-tool resumption: clear the gate normally, so its
            // gate.cleared resumes the gate's turn, and append the response
            // to history so the model sees the content it was waiting on.
            gates.clear(gate.gateId);
            correlations.remove(correlationId);
            if (stateManager !== null) {
              stateManager.removePendingOperation(correlationId);
              const msg = createInboundTurn(message);
              if (msg !== null) stateManager.appendTurn(msg);
            }
            break;
        }
        emit({
          type: "message.correlated",
          seq: nextSeq(),
          data: { message, correlationId },
        });
        if (admission.endDelivered !== null) endTurn(admission.endDelivered);
        return;
      }
    }
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

  // Queue the terminal event of an inference that an abort stopped before
  // its provider call.
  function queueAbortedInference(turnId: string): void {
    enqueue(
      {
        type: "inference.error",
        error: classifyAbortError(),
        partial: { text: "" },
      },
      turnId,
    );
  }

  async function executeInfer(
    options: InferenceOptions | undefined,
  ): Promise<void> {
    if (stateManager === null) return;

    const signal = operationController.signal;
    const turnId = requireCycleTurnId();

    // Proactive pacing: if the previous inference response indicated we are
    // at the rate limit, wait before sending the next request.
    // The wait runs on the injected scheduler. The delay is cleared only
    // when the wait completes: an abort (an abandon of this turn) keeps it,
    // so the next inference, of any turn, still waits.
    if (pendingPacingDelayMs > 0 && !signal.aborted) {
      const delayMs = pendingPacingDelayMs;
      logger.info`Pacing: waiting ${String(delayMs)}ms before next inference request`;
      const completed = await new Promise<boolean>((resolve) => {
        const onAbort = () => {
          cancel();
          resolve(false);
        };
        const cancel = deps.scheduler.setTimeout(() => {
          signal.removeEventListener("abort", onAbort);
          resolve(true);
        }, delayMs);
        signal.addEventListener("abort", onAbort, { once: true });
      });
      if (completed) pendingPacingDelayMs = 0;
      // Every dispatched inference queues exactly one terminal event, so an
      // abort finds the turn this inference runs for.
      if (signal.aborted) {
        queueAbortedInference(turnId);
        return;
      }
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

    // An abandon of this turn can land while the context transforms and the
    // prompt write run. No provider call starts after it.
    if (signal.aborted) {
      queueAbortedInference(turnId);
      return;
    }

    const p = (async () => {
      // Each cycle starts at the most-preferred source; a failover in a
      // prior cycle must not leave the agent permanently demoted.
      resetToPreferredSource();

      for (;;) {
        // A correlation-dispatch failure can end the turn while this cycle
        // runs. Its state is then gone, and the aborted call needs no options.
        const harnessOpts = buildHarnessOpts(
          prompt,
          config.source,
          mergeInferenceOptions(turnStates.get(turnId)?.inference, options),
          signal,
          nextSeq,
          config.readMaterial,
          deps,
        );

        let lastDone:
          | Extract<InferenceEvent, { type: "inference.done" }>
          | undefined;
        let lastError:
          | Extract<InferenceEvent, { type: "inference.error" }>
          | undefined;

        for await (const event of inferenceRunner(harnessOpts)) {
          // The harness knows no turns; the reactor stamps the turn id here.
          if (event.type === "inference.start") {
            emit({ ...event, data: { ...event.data, turnId } });
          } else if (event.type === "inference.done") {
            emit({ ...event, data: { ...event.data, turnId } });
          } else {
            emit(event);
          }
          if (event.type === "inference.done") lastDone = event;
          else if (event.type === "inference.error") lastError = event;
        }

        if (lastDone !== undefined) {
          if (stateManager !== null) {
            stateManager.appendTurn(lastDone.data.turn);
            const open = turnStates.get(turnId)?.openCalls;
            for (const block of lastDone.data.turn.content) {
              if (block.type === "tool_call") open?.add(block.id);
            }
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
          enqueue(
            {
              type: "inference.done",
              turn: lastDone.data.turn,
              usage: lastDone.data.usage,
              source: lastDone.data.source,
            },
            turnId,
          );
          return;
        }

        if (lastError === undefined) {
          emitError("Inference runner returned without a terminal event", true);
          enqueue(
            {
              type: "inference.error",
              error: {
                category: "fatal",
                message: "Inference runner returned without a terminal event",
              },
              partial: { text: "" },
            },
            turnId,
          );
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
          enqueue({ type: "inference.error", error: err, partial }, turnId);
          return;
        }

        // Any remaining error (quota, credential, protocol mismatch,
        // retryable, timeout) is source-specific. The harness wrapper owns
        // mechanical retry and has already exhausted it against this source
        // by the time the reactor sees the error, including honoring a
        // provider Retry-After for quota, so re-running the same source
        // would only retry-compound. Fail over to the next source instead.
        // A pacing delay the leaving source asked for must not gate the
        // next source.
        pendingPacingDelayMs = 0;
        if (failOverToNextSource()) {
          logger.warn`Failing over to next inference source after ${err.category}`;
          continue;
        }

        // No further source to fail over to: surface the last error.
        enqueue({ type: "inference.error", error: err, partial }, turnId);
        return;
      }
    })();

    void track(p);
    await p;
  }

  // `approved` holds the calls (by identity) whose suspension an approver
  // approved and that this batch now runs; the first before-tool run of each
  // gets `approved: true`.
  async function executeTools(
    calls: ToolCall[],
    parallel: boolean,
    addToHistory: boolean,
    approved: ReadonlySet<ToolCall>,
  ): Promise<void> {
    if (stateManager === null) return;
    const state = stateManager;

    const signal = operationController.signal;
    const turnId = requireCycleTurnId();
    // Calls whose suspend the gate limit refused. They did not run, so the
    // doom-loop accounting leaves them out.
    const refusedIds = new Set<string>();

    // Answer a call that does not run with an error result: no tool.start and
    // no result transforms.
    const answerWithError = (call: ToolCall, content: string): ToolResult => {
      const result: ToolResult = { callId: call.id, content, isError: true };
      emit({ type: "tool.done", seq: nextSeq(), data: { result } });
      return result;
    };

    // The answer of a call that an abort stopped: an abandon of this turn,
    // or the reactor's abort.
    const abortedAnswer = (call: ToolCall): ToolResult =>
      answerWithError(
        call,
        abandonedTurns.has(turnId)
          ? ABANDONED_CALL_RESULT
          : ABORTED_CALL_RESULT,
      );

    // Each approval covers one run. The same call object can appear twice
    // in a batch, and only its first run takes the approval.
    const unusedApprovals = new Set(approved);

    const runOne = async (
      call: ToolCall,
    ): Promise<ToolResult | typeof SUSPENDED> => {
      // Read before the first await, so the runs take approvals in call
      // order on the parallel path too.
      const isApproved = unusedApprovals.delete(call);
      // Run before-tool extensions. The first non-allow decision terminates
      // the chain: `block` answers the call with an error result, `suspend`
      // parks it (no result, no tool.done). A suspend that the gate limit
      // refuses is answered with an error result like a block: the call gets
      // no gate, no pending operation and no `reactor.gate.blocked`.
      for (const ext of beforeToolExtensions) {
        let decision: BeforeToolDecision;
        try {
          decision = await ext.beforeTool(call, state.snapshot(), signal, {
            approved: isApproved,
          });
        } catch (cause) {
          const msg = cause instanceof Error ? cause.message : String(cause);
          emitError(
            `BeforeToolExtension threw for ${call.name}: ${msg}`,
            false,
          );
          decision = { type: "block", reason: msg };
        }

        // An abandon of this turn can land while the extension decides. Its
        // decision is then not acted on: no gate is registered, no
        // operation is persisted, and the call is answered.
        if (signal.aborted) return abortedAnswer(call);

        if (decision.type === "suspend") {
          // Park the call: register the gate, persist the pending operation,
          // snapshot, and commit. The call is neither run nor answered — no
          // tool.start, no tool.done, no tool-result turn. The gate clears
          // when the correlated external decision is delivered.
          const admitted = await suspendOnGate({
            gateType: decision.gate.type,
            gateId: decision.gate.gateId,
            timeoutMs: Math.max(1, decision.gate.timeoutAt - Date.now()),
            correlationId: decision.gate.correlationId,
            pendingOp: decision.pendingOp,
            takeOver: undefined,
            turnId,
          });
          if (admitted === "parked") return SUSPENDED;
          refusedIds.add(call.id);
          return answerWithError(call, GATE_LIMIT_REFUSAL);
        }

        if (decision.type === "block") {
          return answerWithError(call, decision.reason);
        }
      }

      // No tool starts after an abort, also with no extension or after the
      // last one allowed the call: the call is answered with an error
      // result, as the inference check does for a provider call.
      if (signal.aborted) return abortedAnswer(call);

      emit({ type: "tool.start", seq: nextSeq(), data: { call } });
      const rawResult = await toolRunner.run(call, signal);
      emit({ type: "tool.done", seq: nextSeq(), data: { result: rawResult } });

      if (rawResult.pendingMarker !== undefined && stateManager !== null) {
        const marker = rawResult.pendingMarker;
        // A director can suspend on a correlation before its marker exists.
        // That live gate already waits on the marker, so the operation is
        // gated under that gate and its turn, and a restart re-arms it.
        // Otherwise no turn waits on the marker yet, and a later director
        // suspend on its correlation gives it a gate.
        const waiting = gates.findByCorrelationId(marker.correlationId);
        const op: PendingOperation = {
          correlationId: marker.correlationId,
          // Placeholder: async markers should carry their own SignalKind. The
          // resume switch keys on suspendedCall presence (absent here) as the
          // interim discriminator instead of on kind.
          kind: "approval",
          registeredAt: Date.now(),
          gateId: waiting?.gateId ?? `pending-${marker.correlationId}`,
          turnId: waiting?.turnId ?? turnId,
          gated: waiting !== undefined,
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

    // Doom-loop accounting keys off the calls that actually ran, aligned to
    // their outcome by index (both the parallel and serial paths above keep
    // `outcomes` in `calls` order). A parked call contributes nothing, so a
    // suspend-then-redispatch cycle counts its one real execution once. A
    // call the gate limit refused did not run and contributes nothing too.
    // When a batch holds N identical gated calls and the model asks again
    // for each refused one, the same single call therefore runs N times in
    // a row (park, approve, redispatch), and the default threshold of 3
    // trips on the third run (INFERENCE.md § Gates). The
    // counters belong to the turn, so another turn's batches between two of
    // this turn's batches do not reset them. The loop reads the count after
    // this returns and ends the turn when it reaches the threshold. A `null`
    // threshold means detection is disabled, so the accounting is skipped
    // entirely. A turn that a correlation-dispatch failure ended while the
    // batch ran has no state left to count in.
    const ranCalls = calls.filter(
      (call, i) => outcomes[i] !== SUSPENDED && !refusedIds.has(call.id),
    );
    const doom = turnStates.get(turnId);
    if (
      doomLoopThreshold !== null &&
      ranCalls.length > 0 &&
      doom !== undefined
    ) {
      const signature = toolBatchSignature(ranCalls);
      if (signature === doom.lastToolBatchSignature) {
        doom.toolBatchRepeatCount += 1;
      } else {
        doom.lastToolBatchSignature = signature;
        doom.toolBatchRepeatCount = 1;
      }
      doom.lastToolBatchNames = ranCalls.map((call) => call.name);
    }

    cycleToolCallsExecuted += results.length;

    const open = turnStates.get(turnId)?.openCalls;
    for (const result of results) open?.delete(result.callId);

    if (addToHistory && stateManager !== null && results.length > 0) {
      stateManager.appendTurn(createToolResultTurn(results));
    }

    for (const result of results) {
      enqueue({ type: "tool.done", result }, turnId);
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

    if (cycleAbandonedTurn !== null) {
      return `Cycle: abandoned turn ${cycleAbandonedTurn}`;
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
    operationsChanged = false;
    cycleAbandonedTurn = null;
  }

  // A refused director suspend registers no gate, but an earlier event of
  // the cycle can have registered a pending operation, such as an async
  // marker. The turn can stay parked on its gate across a restart, so the
  // cycle commits as a suspend does, also when it looks like it did no work.
  async function commitRefusedSuspend(): Promise<void> {
    cycleSuspended = true;
    await commitCycle();
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
      cycleSuspended ||
      operationsChanged ||
      cycleAbandonedTurn !== null;
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
  //
  // An approval can also be admitted while its own suspend commits. Its
  // admission clears the gate silently at once, but the `reactor.gate.cleared`
  // it announces is held here (`clearedOnAdmission`) and emitted right after
  // `blocked`.
  type InFlightSuspend = {
    gateId: string;
    deferredClear: { reason: "resolved" | "timeout" | "shutdown" } | null;
    clearedOnAdmission: boolean;
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
  // path: a shutting-down reactor must not manufacture tool results. The
  // timeout branch still emits `reactor.gate.cleared` (emitted only, not
  // enqueued), so an observer sees every gate that `blocked` announced clear.
  //
  // `turnId` is the cleared gate's turn. Each enqueued resume continues the
  // parked turn, not the turn that runs when the gate clears.
  function onGateCleared(
    gateId: string,
    reason: "resolved" | "timeout" | "shutdown",
    turnId: string,
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
        emit({
          type: "reactor.gate.cleared",
          seq: nextSeq(),
          data: { gateId, reason: "timeout" },
        });
        enqueue(
          {
            type: "resume.tool_result",
            result: {
              callId: op.suspendedCall.id,
              content: "approval timed out",
              isError: true,
            },
          },
          op.turnId,
        );
        return;
      }
      // The gate timed out, but its operation (an async marker) stays
      // registered: no live gate parks it any more.
      ungateOperation(gateId);
    }

    emit({
      type: "reactor.gate.cleared",
      seq: nextSeq(),
      data: { gateId, reason },
    });
    enqueue({ type: "reactor.gate.cleared", gateId, reason }, turnId);
  }

  // Keep the rule "gated: a live gate parks the operation": when a gate goes
  // away and its operation stays registered, mark the operation not gated
  // and persist that at the next commit. A restart then re-arms no gate for
  // it, and a later delivery for it resumes nothing. A shutdown keeps the
  // operation gated, so its parked turn resumes after the restart.
  function ungateOperation(gateId: string): void {
    const op = correlations.findByGateId(gateId);
    if (op === undefined || !op.gated) return;
    const ungated: PendingOperation = { ...op, gated: false };
    correlations.remove(op.correlationId);
    correlations.register(ungated);
    if (stateManager !== null) {
      stateManager.removePendingOperation(op.correlationId);
      stateManager.addPendingOperation(ungated);
    }
    operationsChanged = true;
  }

  // Parks the reactor on a gate. Shared by the director's `suspend` action and
  // the before-tool `suspend` decision so both paths register the gate,
  // durably persist any pending operation, snapshot the active gates, and
  // commit before returning to the loop — a suspended reactor's state must be
  // durable across restart. When `pendingOp` is supplied its correlation is
  // registered and it is persisted; the director path has already persisted
  // its pending operation (via the tool's pending marker), so it passes none.
  // `turnId` is the turn that parks. The gate, any pending operation and the
  // blocked event all carry it.
  //
  // Admission: a turn holds at most `MAX_GATES_PER_TURN` gates. The check
  // runs before any state changes, and everything from it to the gate
  // registration is synchronous, so a sibling call of a parallel batch sees
  // this gate while its commit is still in flight. A gate whose timeout fired
  // during that commit is gone from the gate manager, so the in-flight slot
  // also counts: only the running turn suspends. A refused suspend changes
  // nothing and returns "refused".
  //
  // `takeOver` is an operation with no live gate that the running turn now
  // waits for, of this turn or of another one. Only after admission does it
  // move to this turn and take this gate (`gateId`, `gated`), so a refused
  // suspend leaves it unchanged.
  async function suspendOnGate(args: {
    gateType: GateType;
    gateId: string;
    timeoutMs: number;
    correlationId: string | undefined;
    pendingOp: Omit<PendingOperation, "turnId" | "gated"> | undefined;
    takeOver: PendingOperation | undefined;
    turnId: string;
  }): Promise<"parked" | "refused"> {
    const { gateType, gateId, timeoutMs, correlationId, turnId } = args;
    if (
      suspendingGate !== null ||
      gates.countForTurn(turnId) >= MAX_GATES_PER_TURN
    ) {
      return "refused";
    }

    const pendingOp =
      args.pendingOp !== undefined
        ? { ...args.pendingOp, turnId, gated: true }
        : undefined;

    if (args.takeOver !== undefined) {
      // Remove before the register: the registry throws on a duplicate
      // correlation id.
      const moved: PendingOperation = {
        ...args.takeOver,
        turnId,
        gateId,
        gated: true,
      };
      correlations.remove(moved.correlationId);
      correlations.register(moved);
      if (stateManager !== null) {
        stateManager.removePendingOperation(moved.correlationId);
        stateManager.addPendingOperation(moved);
      }
    }

    if (pendingOp !== undefined) {
      correlations.register(pendingOp);
      if (stateManager !== null) {
        stateManager.addPendingOperation(pendingOp);
      }
    }

    // Track this suspend as in flight so a clear racing the commit below is
    // deferred until `reactor.gate.blocked` has been emitted.
    const inFlightSuspend: InFlightSuspend = {
      gateId,
      deferredClear: null,
      clearedOnAdmission: false,
    };
    suspendingGate = inFlightSuspend;

    // Register the gate. onGateCleared enqueues the cleared event so the loop
    // processes it normally without blocking here.
    void gates.register(
      gateId,
      gateType,
      timeoutMs,
      correlationId,
      onGateCleared,
      turnId,
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

    // An abandon of this turn that landed during the commit drops the
    // gate.blocked below. Record the gate, so the abandon does not announce
    // the clear of a gate no observer saw.
    if (abandonedTurns.has(turnId)) unannouncedGates.add(gateId);

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
        turnId,
      },
    });

    // The suspension is announced. If the gate cleared while the commit was in
    // flight, its handler was deferred to keep it after `blocked`; replay it
    // now, in order. The deferred clear belongs to this suspend's gate, so it
    // carries this suspend's turn.
    suspendingGate = null;
    if (inFlightSuspend.clearedOnAdmission) {
      emit({
        type: "reactor.gate.cleared",
        seq: nextSeq(),
        data: { gateId, reason: "resolved" },
      });
    }
    if (inFlightSuspend.deferredClear !== null) {
      onGateCleared(gateId, inFlightSuspend.deferredClear.reason, turnId);
    }
    return "parked";
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
  // the restart) and must not commit (nothing changed). Every operation is
  // registered, so a later director suspend still finds a marker, but only an
  // operation that had a gate gets one back.
  function rehydrateGates(ops: PendingOperation[]): void {
    for (const op of ops) {
      correlations.register(op);
      if (!op.gated) continue;
      const timeoutMs =
        op.timeoutAt !== undefined
          ? Math.max(1, op.timeoutAt - Date.now())
          : gateTimeout;
      void gates.register(
        op.gateId,
        signalKindToGateType(op.kind),
        timeoutMs,
        op.correlationId,
        onGateCleared,
        op.turnId,
      );
    }
  }

  // -------------------------------------------------------------------------
  // Abandon
  // -------------------------------------------------------------------------

  // Abandon a turn on the loop, with no director call. Whatever phase the
  // turn is in, the effect is one rule: drop its queued events, remove its
  // gates and their operations, answer each of its open calls once, commit,
  // and end it `abandoned`. A dropped resume.tool_result answers its call
  // with its own result (a denial or a timeout); every other call gets an
  // error result. A turn this process has not seen has
  // none of these, and only ends. An operation of the turn that has no gate
  // stays; a later delivery for it resumes nothing.
  async function handleAbandon(turnId: string): Promise<void> {
    if (endedTurns.has(turnId)) return;
    // Suspends run only on the loop, and so does this handler.
    if (suspendingGate !== null) {
      throw new Error(`Abandon of turn ${turnId} while a suspend commits`);
    }

    // The union of the turn's open calls and the calls of its parked
    // operations (they cover a restart, since the open calls are not
    // persisted). A re-dispatched call is open from its admission, so it is
    // in the first set, and a dropped re-dispatch takes its approval with it.
    const toAnswer = new Set<string>(turnStates.get(turnId)?.openCalls);
    // The results of the dropped resume.tool_result events, in queue order.
    const ownResults: ToolResult[] = [];
    for (let i = queue.length - 1; i >= 0; i -= 1) {
      const entry = queue[i];
      if (entry?.turnId !== turnId) continue;
      if (CYCLE_EVENT_TYPES.has(entry.event.type)) pendingContinuations -= 1;
      if (entry.event.type === "resume.tool_result") {
        ownResults.unshift(entry.event.result);
      }
      queue.splice(i, 1);
    }

    let changed = false;
    for (const gate of gates.listForTurn(turnId)) {
      gates.clearSilently(gate.gateId);
      changed = true;
      // A gate whose gate.blocked was dropped gets no gate.cleared either.
      if (!unannouncedGates.delete(gate.gateId)) {
        emit({
          type: "reactor.gate.cleared",
          seq: nextSeq(),
          data: { gateId: gate.gateId, reason: "abandoned" },
        });
      }
      const op =
        gate.correlationId !== undefined
          ? correlations.lookup(gate.correlationId)
          : correlations.findByGateId(gate.gateId);
      if (op !== undefined && op.turnId === turnId) {
        correlations.remove(op.correlationId);
        stateManager?.removePendingOperation(op.correlationId);
        if (op.suspendedCall !== undefined) toAnswer.add(op.suspendedCall.id);
      }
    }

    for (const result of ownResults) toAnswer.delete(result.callId);

    if (stateManager !== null) {
      stateManager.setGatesSnapshot(gates.snapshot());
      const results: ToolResult[] = [
        ...ownResults,
        ...Array.from(toAnswer, (callId) => ({
          callId,
          content: ABANDONED_CALL_RESULT,
          isError: true,
        })),
      ];
      if (results.length > 0) {
        stateManager.appendTurn(createToolResultTurn(results));
        changed = true;
      }
    }

    // Persist before the end settles, as a suspend does.
    if (changed) cycleAbandonedTurn = turnId;
    await commitCycle();
    endTurn({ turnId, outcome: "abandoned" });
  }

  // -------------------------------------------------------------------------
  // Main loop
  // -------------------------------------------------------------------------

  async function loop(): Promise<void> {
    if (stateManager === null) {
      throw new Error("State manager not initialized before loop");
    }

    while (!done) {
      // The previous event's actions are dispatched. Its turn ends here when
      // it holds no gate and has no queued event.
      if (cycleTurnId !== null) settleTurn(cycleTurnId);
      cycleTurnId = null;
      await waitForEvent();

      if (done) break;

      const entry = dequeueNext();
      if (entry === undefined) continue;
      const { event } = entry;
      cycleTurnId = entry.turnId;

      // A dequeued cycle event is one fewer in-flight continuation. Pairs with
      // the increment in enqueue(); both key off CYCLE_EVENT_TYPES so they
      // cannot drift.
      if (CYCLE_EVENT_TYPES.has(event.type)) {
        pendingContinuations -= 1;
      }

      // Handle abort events: initiate shutdown regardless of director. The
      // running turn ends failed. A parked or queued turn gets no end: it
      // survives a restart.
      if (event.type === "abort") {
        if (!shutdownStarted) {
          done = true;
          const running = runningTurn();
          if (running !== null) {
            failTurn(running, `Reactor aborted: ${event.reason}`, "aborted");
          }
          await initiateShutdown();
        }
        break;
      }

      // An abandon is handled here, before the turn would start, and the
      // director never sees it.
      if (event.type === "reactor.abandon") {
        await handleAbandon(requireCycleTurnId());
        continue;
      }

      startTurn(event, requireCycleTurnId());

      // Append inbound messages to conversation history so the provider sees them.
      if (event.type === "message.received") {
        if (stateManager !== null) {
          const msg = createInboundTurn(event.message);
          if (msg !== null) {
            stateManager.appendTurn(msg);
          }
        }
      }

      // A parked approval that ended without running its tool (rejected or
      // timed out) carries a synthetic error result answering the parked call.
      // Land it in history before the director decides so the tool_result turn
      // closes the dangling tool_use and the re-inference the director returns
      // sees a well-formed sequence. No tool ran, so no tool.done accompanies
      // it.
      if (event.type === "resume.tool_result") {
        if (stateManager !== null) {
          stateManager.appendTurn(createToolResultTurn([event.result]));
        }
        turnStates
          .get(requireCycleTurnId())
          ?.openCalls.delete(event.result.callId);
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
        failTurn(
          requireCycleTurnId(),
          `Director exception: ${msg}`,
          "reactor_fatal",
        );
        done = true;
        await initiateShutdown();
        break;
      }

      // A correlation-dispatch failure can end the running turn and stop the
      // reactor while the director decides. No action of that turn may
      // follow its end.
      if (done) break;
      // An abandon of this turn can land while the director decides. Every
      // action of that decision is dropped, `done` included; the queued
      // abandon ends the turn.
      if (abandonedTurns.has(requireCycleTurnId())) continue;

      const validation = validateActions(actions);
      if (!validation.ok) {
        emitError(`Invalid action set: ${validation.error}`, true);
        failTurn(
          requireCycleTurnId(),
          `Invalid action set: ${validation.error}`,
          "reactor_fatal",
        );
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
        settleTurn(requireCycleTurnId());
        done = true;
        await initiateShutdown();
        break;
      }

      // Handle wait: commit the cycle (if work happened) and return to the
      // event loop without shutting down. The turn ends at the top of the
      // loop unless it has queued work.
      if (normalized.some((a) => a.type === "wait")) {
        await commitCycle();
        continue;
      }

      // Handle suspend: register gate and continue the loop (don't block).
      const suspendAction = normalized.find((a) => a.type === "suspend");
      if (suspendAction !== undefined && suspendAction.type === "suspend") {
        const { gate } = suspendAction;
        const turnId = requireCycleTurnId();
        // A correlation resumes through one gate. A second gate on a
        // correlation that already has a live gate would never clear, so the
        // reactor refuses the suspend and registers no gate.
        if (
          gate.correlationId !== undefined &&
          gates.findByCorrelationId(gate.correlationId) !== undefined
        ) {
          await commitRefusedSuspend();
          emitError(
            `Director suspend on gate ${gate.gateId} refused: correlation ${gate.correlationId} already has a live gate`,
            false,
          );
          continue;
        }
        // A director may suspend on the correlation of an operation that has
        // no live gate (an async pending marker), of this turn or of an
        // earlier one. The running turn now waits for that operation, so the
        // operation, its gate and its resume belong to the turn that waits.
        // The earlier turn can also have ended, and a gate under an ended
        // turn would resume a turn that cannot end again. suspendOnGate moves
        // the operation and gives it this gate only after admission, and its
        // commit persists the change, so a restart re-arms this gate.
        const waitedOn =
          gate.correlationId !== undefined
            ? correlations.lookup(gate.correlationId)
            : undefined;
        const admitted = await suspendOnGate({
          gateType: gate.type,
          gateId: gate.gateId,
          timeoutMs: gate.timeoutMs > 0 ? gate.timeoutMs : gateTimeout,
          correlationId: gate.correlationId,
          pendingOp: undefined,
          takeOver: waitedOn,
          turnId,
        });
        // A refused director suspend registers no gate. An operation it named
        // stays registered under its own turn with no gate, also after a
        // restart, so a later delivery for it resumes nothing. The turn holds
        // its gate, so it does not end here.
        if (admitted === "refused") {
          await commitRefusedSuspend();
          emitError(
            `Director suspend on gate ${gate.gateId} refused: turn ${turnId} already holds a gate`,
            false,
          );
        }
        continue;
      }

      // Handle reply — emit the content for the harness/supervisor to send.
      const replyAction = normalized.find((a) => a.type === "reply");
      if (replyAction !== undefined && replyAction.type === "reply") {
        // Flush any pending cycle work before signaling the reply so the
        // emitted checkpointHash matches the visible state.
        await commitCycle();
        // A correlation-dispatch failure during the commit ends this turn and
        // stops the reactor; a reply must not follow the turn's end.
        if (done) break;
        emit({
          type: "connector.reply",
          seq: nextSeq(),
          data: {
            content: replyAction.content,
            ...(lastCheckpointHash !== undefined
              ? { checkpointHash: lastCheckpointHash }
              : {}),
            turnId: requireCycleTurnId(),
          },
        });
        requireTurnState(requireCycleTurnId()).replied = true;
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
          failTurn(
            requireCycleTurnId(),
            `Compaction failed: ${msg}`,
            "reactor_fatal",
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
        await executeInfer(inferAction.options);
        continue;
      }

      // Handle execute_tools.
      const toolsAction = normalized.find((a) => a.type === "execute_tools");
      if (toolsAction !== undefined && toolsAction.type === "execute_tools") {
        const parallel = toolsAction.parallel !== false;
        const addToHistory = toolsAction.addToHistory !== false;
        // A re-dispatch's approval applies only to the action the director
        // returns for that event, in this cycle.
        const approved =
          event.type === "resume.execute_tools"
            ? approvedCallsOf(event.calls, toolsAction.calls)
            : NO_APPROVALS;
        await executeTools(toolsAction.calls, parallel, addToHistory, approved);
        const turnId = requireCycleTurnId();
        // A batch that ran while its turn was abandoned cannot trip the doom
        // loop; the queued abandon ends the turn.
        if (abandonedTurns.has(turnId)) continue;
        const doom = turnStates.get(turnId);
        if (
          doomLoopThreshold !== null &&
          doom !== undefined &&
          doom.toolBatchRepeatCount >= doomLoopThreshold
        ) {
          const tools = doom.lastToolBatchNames.join(", ");
          const message =
            `Doom loop detected: an identical tool batch (${tools}) executed ` +
            `${String(doomLoopThreshold)} times consecutively`;
          emitError(message, true);
          failTurn(turnId, message, "doom_loop");
          done = true;
          await initiateShutdown();
          break;
        }
        continue;
      }

      // No infer/tools/reply/suspend/wait/compact action — if a checkpoint
      // override was set on its own (or alongside emit/fork), the next event
      // will pick it up. Nothing to flush here. The turn ends at the top of
      // the loop unless it has queued work.
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
      let initialOps: PendingOperation[];
      let initialUsage: TokenUsage;
      try {
        const loaded = await contextStore.load();
        initialTurns = loaded.turns;
        // An operation from older state gets its missing fields here, so
        // the state manager and the rehydrated gates see one turn id and one
        // `gated` value for each operation.
        initialOps = loaded.pendingOperations.map(loadedOperation);
        initialUsage = loaded.tokenUsage;
      } catch (cause) {
        done = true;
        startupDeliveries = null;
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

        const bufferedDeliveries = startupDeliveries;
        startupDeliveries = null;
        if (bufferedDeliveries !== null) {
          for (const { message, turnId, inference } of bufferedDeliveries) {
            processDelivery(message, turnId, inference);
          }
        }

        await loop();
      } catch (cause) {
        const msg = cause instanceof Error ? cause.message : String(cause);
        done = true;
        startupDeliveries = null;
        logger.error`Reactor loop threw unexpectedly: ${cause}`;
        emitError(`Internal reactor error: ${msg}`, true);
        // A throw from rehydrateGates comes before any turn runs, and the
        // startup deliveries it holds are dropped with no end.
        if (cycleTurnId !== null) {
          failTurn(
            cycleTurnId,
            `Internal reactor error: ${msg}`,
            "reactor_fatal",
          );
        }
        if (!shutdownStarted) {
          await initiateShutdown();
        }
      }
    })();
  }

  function processDelivery(
    message: InboundMessage,
    turnId: string,
    inference: PerCallInferenceOptions | undefined,
  ): void {
    void (async () => {
      const verdict = await verifyCorrelation(message);
      const admission = admitDelivery(turnId, verdict);
      let failed = false;
      try {
        applyAdmission(admission, message, turnId, inference);
      } catch (cause) {
        failed = true;
        // A correlation-path invariant failed (e.g. a malformed approval
        // decision). Surface it as a fatal reactor error rather than a silent
        // unhandled rejection, and stop the run — the resume cannot proceed on
        // a decision the reactor cannot trust. The delivered turn ends failed
        // only when this process had not seen it (a parked or queued turn
        // gets no end on a failure path), and the running turn ends failed.
        const msg = cause instanceof Error ? cause.message : String(cause);
        logger.error`Correlation dispatch failed: ${cause}`;
        emitError(`Correlation dispatch failed: ${msg}`, true);
        const error = `Correlation dispatch failed: ${msg}`;
        if (admission.deliveredPhase === "unseen") {
          failTurn(turnId, error, "reactor_fatal");
        }
        const running = runningTurn();
        if (running !== null) failTurn(running, error, "reactor_fatal");
        done = true;
      } finally {
        // A matched verdict holds its correlation as in progress until the
        // admission is applied, whatever the outcome.
        if (verdict.kind === "matched") {
          correlatingIds.delete(verdict.correlationId);
        }
      }
      if (failed && !shutdownStarted) {
        await initiateShutdown();
      }
    })();
  }

  function deliver(message: InboundMessage, opts?: DeliverOptions): void {
    const inference =
      opts?.inference !== undefined
        ? perCallInference(opts.inference)
        : undefined;
    if (opts?.turnId !== undefined && endedTurns.has(opts.turnId)) {
      throw new Error(`Turn ${opts.turnId} has already ended`);
    }
    const turnId = opts?.turnId ?? crypto.randomUUID();
    if (done) {
      applyAdmission(
        admitDelivery(turnId, { kind: "uncorrelated" }),
        message,
        turnId,
        inference,
      );
      return;
    }
    if (startupDeliveries !== null) {
      startupDeliveries.push({ message, turnId, inference });
      return;
    }
    processDelivery(message, turnId, inference);
  }

  function abandonTurn(turnId: string): void {
    if (done) return;
    let target = turnId;
    for (
      let next = joinedInto.get(target);
      next !== undefined;
      next = joinedInto.get(target)
    ) {
      target = next;
    }
    if (endedTurns.has(target) || abandonedTurns.has(target)) return;
    abandonedTurns.add(target);
    // The running turn's operation in flight is its own: abort it, and give
    // the next turn a fresh signal.
    if (runningTurn() === target) abortOperations();
    enqueue({ type: "reactor.abandon" }, target);
  }

  function abort(reason: AbortReason): void {
    // The loop cannot dequeue the abort event while it is awaiting an active
    // inference or tool batch. Signal that operation immediately so it can
    // settle and return control to the loop, where the queued abort retains
    // its priority over every other event.
    operationController.abort();
    enqueue({ type: "abort", reason }, null);
  }

  return { start, deliver, abandonTurn, abort };
}

// Workflow runtime environment contract.
//
// `WorkflowRuntimeEnv` is the surface the single runtime body
// consumes. `runLocal` and any future child-process entry point
// satisfy this contract; the runtime body switches on env keys, not
// on which process it is running in. The body must not reference
// any `isChildProcess`-shaped discriminator -- an explicit
// source-level test in `run.test.ts` asserts the discipline.

import type { AgentDefinition, BaseEnv, DirectorRegistry } from "@intx/agent";
import type { ApprovalSnapshot, ControlParkKind } from "@intx/types/runtime";

import type {
  AuthorizeContext,
  WorkflowAuthorizeFn,
} from "../authorize-context";
import type { Primitive } from "../definition/index";
import type { WorkflowEvent } from "../state-machine/index";
import type { DrainController } from "./drain";

/**
 * Read/append/tail event log per run. The append-only invariant is
 * the state machine's responsibility; the repo store is the durable
 * substrate the runtime writes to, reads from on resume, and tails
 * from when awaiting an externally-committed event (the event-sourced
 * `waitForTimer` shape).
 */
export interface RepoStore {
  /** Return every committed event in seq order. */
  read(runId: string): Promise<readonly WorkflowEvent[]>;
  /** Append one event; rejects if seq is non-monotonic. */
  append(runId: string, event: WorkflowEvent): Promise<void>;
  /**
   * Append a contiguous run of events in a SINGLE durable commit. The
   * events must carry strictly-monotonic, gap-free seqs continuing the
   * run's prior tip (the first event's seq is `priorLastSeq + 1`).
   * Equivalent in effect to calling `append` once per event, but the
   * durable substrate writes all `events/<seq>.json` blobs under one
   * tree-rewrite + ref-advance instead of one per event. An empty
   * `events` array is a no-op (no commit).
   *
   * This is the batch seam the runtime's commit-chain flushes through
   * at a segment boundary (suspension or completion): the per-event
   * in-memory state-machine validation is unchanged; only the durable
   * write is coalesced.
   */
  appendBatch(runId: string, events: readonly WorkflowEvent[]): Promise<void>;
  /**
   * Tail the run's event log. Returns an async iterator yielding one
   * `{ seq, event }` entry per committed `WorkflowEvent` on the run's
   * ref, in commit order. `seq` is the workflow-event `seq` (the
   * field the state machine assigns), not a substrate-level commit
   * counter.
   *
   * Cancellation: when `opts.signal` aborts, the iterator ends
   * cleanly (no throw from the consumer's `for await`).
   *
   * Replay vs live:
   *   - `from: { seq: number }` enumerates every prior event whose
   *     `seq` is >= the supplied number, then transitions to live
   *     mode and continues with newly-committed events.
   *   - `from: "head"` records the run's last seq at subscribe time
   *     and emits only events committed strictly after.
   *
   * Backpressure: events are buffered in userspace bounded by
   * `bufferLimit` (default 1024). On overrun the iterator throws.
   * Silent drop would corrupt the workflow-runtime's view of the
   * world; consumers that cannot keep up are expected to abort.
   *
   * Production wraps the substrate's `subscribeKind` typed helper
   * under the hood; runLocal serves events from the in-memory log.
   * The runtime body's contract is the same shape across both.
   */
  subscribe(
    runId: string,
    opts: SubscribeOpts,
  ): AsyncIterableIterator<{ seq: number; event: WorkflowEvent }>;
}

export interface SubscribeOpts {
  signal: AbortSignal;
  from: "head" | { seq: number };
  bufferLimit?: number;
}

/**
 * Durable timer scheduler.
 *
 * Event-sourced shape: callers commit `TimerSet` against the run's
 * log themselves; `scheduleIn` registers wall-clock intent against
 * the scheduler so that at `fireAt` the scheduler commits
 * `TimerFired{timerId}` to the run's log. The single-writer-on-
 * `TimerFired` invariant lives here: the production scheduler is
 * the only thing that commits `TimerFired` to the log. Callers
 * await the commit by tailing `repoStore.subscribe`.
 *
 * The returned disposer cancels the pending `TimerFired` commit
 * (the in-process race between the awaiting consumer settling on a
 * sibling event -- a signal arrival under `awaitSignal` -- and the
 * timer's deadline). On dispose the scheduler discards the queued
 * callback; no `TimerFired` lands. The production scheduler honours
 * the dispose by removing the queue entry; restart recovery never
 * re-arms a disposed entry because the run's log already carries a
 * sibling terminal event for that step by the time the runtime
 * settled.
 */
export interface Scheduler {
  scheduleIn(runId: string, timerId: string, fireAt: Date): () => void;
}

/**
 * FIFO single-consumer signal channel. Pre-await delivery is queued
 * under the signal name; an awaiter consumes the next queued signal
 * for its name on subscription.
 *
 * The shape is in-process callback-based (`deliver` / `awaitNext`).
 * A production signal source that delivers via a mail bus has to
 * translate "mail arrives" into "the right awaiter's promise
 * resolves," and on resume the channel's in-memory queue is empty
 * even though the state machine's `unconsumedSignals` carries the
 * queued signals. The choice of resolution -- rehydrate the channel
 * from the log, change `awaitNext` to consult the state-machine
 * queue, or replace this interface with a log-tail subscription
 * shape -- is a substrate-shaped decision that depends on the
 * production mail substrate.
 */
export interface SignalChannel {
  /** Inject a signal. The state machine handles dedup by `signalId`. */
  deliver(name: string, payload: unknown, signalId?: string): Promise<void>;
  /**
   * Wait for the next signal of the given name. Resolves with the
   * payload (and the assigned `signalId`) when the signal arrives.
   * Rejects on `signal.abort()` if `signal` is supplied.
   */
  awaitNext(
    name: string,
    signal?: AbortSignal,
  ): Promise<{ payload: unknown; signalId: string }>;
}

/**
 * Per-call hook that determines the step's `AgentResult`. The runtime
 * delegates the actual reactor invocation to this callback so the same
 * runtime body works against a real agent (production) or a stub
 * (tests). The callback's contract: invoke the agent's send-and-respond
 * loop with the materialized input, and return the captured
 * `AgentResult`. Any thrown error propagates as `StepFailed`.
 *
 * The callback receives the per-step `AuthorizeContext` so it can
 * build an `env.authorize` closure that delegates to the runtime's
 * workflow-typed `WorkflowAuthorizeFn` with the context already
 * embedded.
 */
export type StepInvoker = (
  input: StepInvokeRequest,
) => Promise<StepInvokeResult>;

export interface StepInvokeRequest {
  agent: AgentDefinition<BaseEnv>;
  /** The materialized input the runtime resolved from the step's `input` selector. */
  input: unknown;
  /** Workflow-runtime context for every authz call inside the step. */
  authzContext: AuthorizeContext;
  /** Cancelled when the step is being torn down (timeout, cancellation). */
  signal: AbortSignal;
  /**
   * Present only on a resume re-invocation of a step that previously
   * suspended. `kind` carries the park kind the step suspended on, so the
   * invoker synthesizes the right inbound: an `"approval"` resume delivers a
   * body stamped with `correlationId` so the reactor's `tryCorrelate` matches
   * the rehydrated gate; an `"input"` resume delivers the `decision` as a
   * plain next user turn (there is no gate to match). Absent on the first
   * invocation, where the invoker drives a plain `agent.send`.
   */
  resume?: { correlationId: string; decision: unknown; kind: ControlParkKind };
}

/**
 * The outcome of a single `invokeStep`. A step either produces its `output`
 * (the agent replied) or suspends on a tool/authz gate, handing back the
 * `correlationId` the runtime parks the step on until the correlated decision
 * is delivered. The suspend carries an explicit `kind: "approval"`
 * discriminant and a REQUIRED snapshot (the sidecar->hub co-write treats it
 * as mandatory), so a snapshot-less approval is unrepresentable here rather
 * than resting on the runtime guard alone.
 *
 * An invoker can ONLY suspend as an approval. The `"input"` control-plane
 * park (a long-lived step awaiting its next trigger) is minted exclusively by
 * the RUNTIME's trigger-budget re-arm -- never by an invoker -- which is what
 * keeps the finite-budget respawn seed sound: every input `SignalAwaited` in
 * the durable log is a runtime re-arm, so counting them counts turns
 * serviced. Offering an input arm here would let a host invoker emit input
 * parks that inflate that count.
 */
export type StepInvokeResult =
  | { output: unknown }
  | {
      suspend: {
        correlationId: string;
        kind: "approval";
        approvalSnapshot: ApprovalSnapshot;
      };
    };

/**
 * Per-action deterministic effect handler invocation, the effect analog
 * of `StepInvoker`. The runtime hands the handler ref, materialized
 * input, declared effect capabilities, authz context, and abort signal
 * across the boundary; the host resolves the ref to a handler function,
 * runs it with a capability- and ledger-checked `EffectContext` built
 * from `env.authorize` and `env.effects`, and returns the output. No
 * inference runs. The runtime never resolves the handler ref itself,
 * mirroring how it never reads `agent.toolFactories`.
 */
export type ActionInvoker = (
  input: ActionInvokeRequest,
) => Promise<ActionInvokeResult>;

export interface ActionInvokeRequest {
  /** Ref the host resolves to a deterministic effect handler. */
  handler: string;
  /** The materialized input the runtime resolved from the action's `input` selector. */
  input: unknown;
  /** Capabilities the action declared it may exercise (its `effect.requires`). */
  requires: readonly string[];
  /** Workflow-runtime context for every authz call inside the handler. */
  authzContext: AuthorizeContext;
  /** Cancelled when the action is being torn down (timeout, cancellation). */
  signal: AbortSignal;
}

export interface ActionInvokeResult {
  output: unknown;
}

/**
 * Crash-safe exactly-once substrate for action effects. It is a
 * substrate DISTINCT from the run event log: recording an effect must
 * NOT enter the run-log commit chain or trigger a segment flush, so a
 * dropped run-log buffer never takes the ledger with it.
 *
 * `record` MUST be durable on return (synchronous with respect to
 * durability), independent of the run-log buffer, and MUST NOT be
 * co-located with `StepCompleted` in a shared batch -- the crash-dedup
 * contract depends on the ledger surviving a dropped run-log buffer.
 * The in-memory implementation is trivially durable; a production
 * substrate owns this contract.
 */
export interface EffectLedger {
  /** Return the recorded output for a key, or `undefined` on a miss. */
  lookup(effectKey: string): Promise<{ output: unknown } | undefined>;
  /** Durably record an effect's output under its key (see contract above). */
  record(effectKey: string, output: unknown): Promise<void>;
}

/**
 * Capability- and ledger-checked handle passed to an action handler.
 * Every external effect the handler performs must run through `perform`
 * so it is (a) authorized against the operator-approved effect floor and
 * (b) deduplicated by the effect ledger across a crash re-run. `perform`
 * refuses any `capability` not in the action's declared `requires` set,
 * calls `env.authorize` before the effect, and on a ledger hit returns
 * the recorded result without running `run`.
 */
export interface EffectContext {
  perform(opts: {
    effectId: string;
    capability: string;
    run: () => Promise<unknown>;
  }): Promise<unknown>;
}

/**
 * Blob substrate. The default-threshold (1 MiB) spill is implemented
 * by `recordOutput`; consumers receive the same shape (`{ ref }`)
 * regardless of whether the value spilled to a blob or inlined.
 */
export interface BlobSubstrate {
  /** Returns a `ref` an event log can carry; either inline or a blob URI. */
  recordOutput(
    stepId: string,
    attempt: number,
    value: unknown,
  ): Promise<{ ref: string }>;
  /** Resolve a previously-recorded ref back to its value. */
  resolveRef(ref: string): Promise<unknown>;
  /**
   * `true` for substrates whose storage does not survive instance
   * turnover (the in-memory `runLocal` substrate). The runtime's
   * resume path uses this to emit a targeted error when a seed log
   * references blob: refs against a fresh ephemeral substrate --
   * resume requires the substrate that recorded the refs, and an
   * empty ephemeral substrate cannot serve them.
   */
  readonly ephemeral: boolean;
}

/**
 * Spawn callback for `childWorkflow`. The parent runtime allocates the
 * `childRunId` and commits `ChildSpawned` *before* invoking the
 * callback so the parent's audit log records the spawn before any
 * work begins on the child side. The callback resolves `definitionRef`
 * -- the internal ref the deploy step assigned when it lifted the authored
 * inline child -- to a concrete `WorkflowDefinition` using whatever in-memory
 * lookup the runtime supplies (a lifted-body map in `runLocal`, the parent's
 * re-evaluated closure map in production), constructs the child run against
 * the supplied id, and returns the terminal status.
 *
 * The runtime body does not carry a definition lookup of its own.
 */
export type SpawnChildWorkflow = (input: {
  definitionRef: string;
  childRunId: string;
  input: unknown;
  parentRunId: string;
  parentStepId: string;
  signal: AbortSignal;
  /**
   * The child run's depth in the spawn chain (one deeper than the parent).
   * The runtime checks it against `maxChildSpawnDepth` before this callback
   * is invoked; the spawner threads it into the child run so the child's own
   * spawns keep counting. See `child-depth.ts`.
   */
  depth: number;
  /** The tree-wide spawn-depth ceiling, threaded to the child run. */
  maxChildSpawnDepth: number;
}) => Promise<{
  terminalStatus: "completed" | "failed" | "cancelled";
}>;

/**
 * An approval park surfaced from a suspendable child body: the reserved
 * correlation the child parked on and the approver-facing snapshot, so the
 * parent (runOnTrigger) can proxy the approval up its OWN run's park
 * machinery on the same correlation. A child body's `input` parks are the
 * section's re-arm, not surfaced here.
 */
export type SuspendableChildPark = {
  correlationId: string;
  approvalSnapshot?: ApprovalSnapshot;
};

/**
 * A live handle to a running suspendable child body. `next` resolves each
 * time the child parks on an approval (the caller proxies it up and, once
 * granted, calls `resume`), parks on an author `awaitSignal` gate (surfaced as
 * `signal-park` so the caller proxies it up as a signal-relay await and
 * `deliverSignal`s the resolved signal back), or reaches a terminal. `resume`
 * delivers the granted decision to the child's reserved correlation channel;
 * `deliverSignal` delivers a signal on the body's own author-chosen name.
 * Both unblock the body's awaiter so it continues.
 */
export type SuspendableChildHandle = {
  next(): Promise<
    | { kind: "park"; park: SuspendableChildPark }
    | { kind: "signal-park"; name: string }
    | { kind: "terminal"; terminalStatus: "completed" | "failed" | "cancelled" }
  >;
  resume(correlationId: string, decision: unknown): Promise<void>;
  /**
   * Deliver a signal on the body's author-chosen name (a non-reserved
   * `awaitSignal` gate the body parked on), carrying the ORIGINAL `signalId`
   * so the body's run-lifetime dedup makes a redelivered relay idempotent.
   * Distinct from `resume`, which targets a reserved correlation channel.
   */
  deliverSignal(
    name: string,
    payload: unknown,
    signalId: string,
  ): Promise<void>;
};

/**
 * Spawn a body sub-DAG as a child run that MAY suspend on an approval park.
 * Unlike `spawnChild` (which awaits a terminal), this returns a live handle
 * the caller drives across parks: the child's control-plane approval parks
 * surface via `handle.next()` so the caller proxies them up its own run's
 * park machinery, and `handle.resume` relays the granted decision back into
 * the child. onTrigger runs every event's body through this seam, so a body
 * step's approval is serviced without the body child ever needing to reach a
 * terminal to unblock the parent.
 *
 * `resumeFromEvents` re-adopts a body child that was mid-flight when the
 * process crashed: the seam drives `runtimeRun` from the supplied durable log
 * instead of a fresh `triggerPayload`, so a body step that was parked on an
 * approval re-parks on its reserved channel. A re-park does NOT re-fire
 * `onPark` (the park is already durable), so the caller does not observe the
 * in-flight park via `next()` on resume; it relays the eventual grant via
 * `resume` on the correlation it recovered from its own log, and `next()`
 * resumes surfacing the body's subsequent parks and terminal as normal.
 */
export type SpawnSuspendableChild = (input: {
  definitionRef: string;
  childRunId: string;
  input: unknown;
  parentRunId: string;
  parentStepId: string;
  signal: AbortSignal;
  /**
   * The depth this body run executes AT -- the container step's OWN depth,
   * passed through UNCHANGED. A suspendable body (an onTrigger section or a
   * loop iteration) is inlined rework, not a `childWorkflow` spawn, so it does
   * not consume a depth rung; only a `childWorkflow` inside the body increments
   * (to this depth + 1) via `runChildWorkflow`. Threading it keeps the
   * tree-wide ceiling counting a grandchild from the top-level run rather than
   * resetting at the body boundary. See `child-depth.ts`.
   */
  depth: number;
  /** The tree-wide spawn-depth ceiling, threaded into the body run. */
  maxChildSpawnDepth: number;
  resumeFromEvents?: readonly WorkflowEvent[];
}) => Promise<SuspendableChildHandle>;

/**
 * A loop's `while` predicate or `carry` transform. Deliberately receives
 * only data -- the just-completed iteration's resolved output and the
 * current carry state -- and NO effect context, authorize, or signal.
 * These functions run in the runtime body on every forward pass and
 * every resume, so they must be pure and side-effect free; the type has
 * no effect parameter so an effectful function is not even expressible.
 * `while` coerces its result to a boolean (continue while truthy);
 * `carry` returns the next iteration's input.
 */
export type LoopFn = (childOutput: unknown, carryState: unknown) => unknown;

/**
 * Resolve a loop's `while`/`carry` string ref to a pure `LoopFn`,
 * mirroring how `directors` and action handlers resolve string refs.
 */
export type LoopFnRegistry = (ref: string) => LoopFn;

/**
 * A control-plane suspension the runtime body notifies the host of when a
 * step parks on a reserved `signalName(correlationId)` channel. Distinct
 * from the `awaitNext` signal-channel park: it carries the correlation the
 * eventual resolver routes a decision back on, so the host can register the
 * suspension out-of-band (the sidecar co-writes a routing + approval row at
 * the hub). A plain `awaitSignal` gate parked on an author-chosen name does
 * NOT produce one -- only the reserved control-plane channel does.
 */
export type WorkflowPark = {
  runId: string;
  correlationId: string;
  parkKind: ControlParkKind;
  /**
   * Approver-facing snapshot of the parked tool call, forwarded from the
   * reactor so the host can register it alongside the correlation. Required on
   * every approval park: the runtime throws at the emit site rather than fire a
   * snapshot-less one, because the sidecar->hub co-write treats the snapshot as
   * mandatory (the register frame requires it and the approval columns are NOT
   * NULL). A resume-from-park does not re-fire the notify.
   *
   * Absent for input parks, which carry no snapshot.
   */
  approvalSnapshot?: ApprovalSnapshot;
};

/**
 * The notify a suspendable child body fires when a step parks on an author
 * `awaitSignal` gate -- a NON-reserved, author-chosen `name` (not a reserved
 * `signalName(correlationId)` control channel). Distinct from {@link
 * WorkflowPark}: an author gate carries no correlation and no snapshot, so the
 * host cannot register it at the hub. The suspendable-child seam surfaces it up
 * to `runOnTrigger`, which proxies the body's await as a signal-relay await on
 * its own run and relays the resolved signal back down. A host that is not a
 * suspendable-child body (the container run, runLocal) leaves `onSignalPark`
 * unset, so an author gate outside a section body parks with no notify exactly
 * as before.
 */
export type WorkflowSignalPark = {
  runId: string;
  name: string;
};

/**
 * The minimal durable record of a control-plane approval suspension the resume
 * classifier needs to recover a step that crashed mid-park -- one whose
 * `StepStarted` flushed but whose `SignalAwaited` did not. The reactor commits
 * its pending operation to durable storage before the park flush, so this
 * record survives the crash even when the workflow log does not carry the
 * `SignalAwaited`. Deliberately narrow: the runtime reconstructs the missing
 * `SignalAwaited` from the correlationId (and optional timeout) alone and must
 * not learn the reactor's pending-operation internals (`gateId`,
 * `suspendedCall`, the snapshot), which stay the host's concern.
 */
export type ParkedApprovalOp = {
  correlationId: string;
  /**
   * Absolute deadline in epoch milliseconds, as the reactor's pending operation
   * stores it. Absent for the indefinite-hold approval parks that are the norm.
   * The runtime converts it to the ISO string a `SignalAwaited` carries.
   */
  timeoutAtMs?: number;
};

/**
 * Read-only recovery hook the resume classifier consults: enumerate the durable
 * pending approval operations a step left behind, keyed by
 * `{ runId, stepId, attempt }`. See `WorkflowRuntimeEnv.readParkedApprovalOps`.
 */
export type ReadParkedApprovalOps = (args: {
  runId: string;
  stepId: string;
  attempt: number;
}) => Promise<ParkedApprovalOp[]>;

/**
 * The runtime body's full env surface. The two implementations
 * (`runLocal` and the production child-process entry point) construct
 * differently-flavoured concrete values for each field but the body
 * sees only this interface.
 */
export interface WorkflowRuntimeEnv {
  repoStore: RepoStore;
  scheduler: Scheduler;
  signalChannel: SignalChannel;
  blobs: BlobSubstrate;
  directors: DirectorRegistry;
  /** Workflow-level authorize used by every step's per-call closure. */
  authorize: WorkflowAuthorizeFn;
  /** Per-step reactor invocation. Production wires this through to `createAgent`. */
  invokeStep: StepInvoker;
  /**
   * Per-action deterministic effect handler invocation. Optional: a host
   * that does not wire it does not support `action` primitives, and
   * `runAction` fails loudly if a workflow uses one. runLocal always
   * wires it.
   */
  invokeAction?: ActionInvoker;
  /**
   * Effect ledger for crash-safe exactly-once action effects. On the env
   * only so the host's `invokeAction` can build its `EffectContext`
   * against it; the runtime body never calls it directly (same as
   * `authorize`). Optional for the same reason as `invokeAction`.
   */
  effects?: EffectLedger;
  /** Spawn callback for `childWorkflow`. */
  spawnChild: SpawnChildWorkflow;
  /**
   * Spawn callback for an onTrigger section's per-event body: a child run
   * driven across approval parks via a live handle (see
   * {@link SpawnSuspendableChild}). Optional: a host that does not wire it
   * does not support onTrigger sections, and `runOnTrigger` fails loudly.
   *
   * This is a second child-drive seam alongside the terminal-only
   * `spawnChild` that `childWorkflow` uses, rather than one unified
   * park-aware seam. The two are kept separate because unifying them would
   * require migrating `childWorkflow`'s terminal-only path onto the
   * park-aware drive; that is a deliberate, separate decision, not an
   * oversight.
   */
  spawnSuspendableChild?: SpawnSuspendableChild;
  /**
   * Spawn one loop iteration as a suspendable child. Same
   * {@link SpawnSuspendableChild} contract as `spawnSuspendableChild`, but
   * wired to an executor that runs the body under the parent run's INHERITED
   * env (real tools, action invoker, grants, and the durable shared store) --
   * a loop is the parent's own bounded rework, not a fresh capped section
   * body. `runLoop` drives it across the body's parks exactly as `runOnTrigger`
   * drives a section body. Optional: a host that does not wire it does not
   * support `loop`, and `runLoop` fails loudly. runLocal wires it.
   */
  spawnLoopIteration?: SpawnSuspendableChild;
  /**
   * Resolve a loop's `while`/`carry` refs to pure functions. Optional
   * for the same reason as `spawnLoopIteration`.
   */
  loopFns?: LoopFnRegistry;
  /**
   * Clock for timestamp generation. Tests inject a deterministic
   * implementation; production uses `new Date()`. Keeping the clock on
   * the env keeps the runtime body free of direct `Date` references
   * that would otherwise be the only branching point between local-dev
   * and production.
   */
  clock: () => Date;
  /** Random id generator for run ids, signal ids, timer ids. */
  newId: (prefix: string) => string;
  /**
   * Drain controller the runtime body observes at four sites: main
   * loop entry, retry-between-attempts in `runStep`, `waitForTimer`,
   * and `runAwaitSignal`. The runtime never mutates the controller;
   * the host implements the writing side. runLocal supplies a no-op
   * controller whose signal never fires.
   */
  drain: DrainController;
  /**
   * Optional suspension-notify sink. Fired once each time a step commits a
   * `SignalAwaited` on a reserved `signalName(correlationId)` channel -- the
   * agent-step suspend path, not a plain `awaitSignal` gate. The host uses it
   * to register the correlation out-of-band (production: the sidecar sends a
   * `signal.correlation.register` frame to the hub, which co-writes the run's
   * routing + approval rows so the resolver can route a delivered decision
   * back to the parked run). It fires exactly once per suspension, on the
   * initial park: a resume that finds the step already `awaiting-signal`
   * (including a crash-mid-park step the classifier recovers to
   * `awaiting-signal` via `readParkedApprovalOps`) skips the fresh-emit branch
   * and does not re-fire. Recovering a registration lost across a crash is
   * driven from durable state on a later re-establishment (the supervisor's
   * re-emit), whose hub write is idempotent on the correlationId. A host that
   * does not wire it does not register suspensions -- runLocal leaves it unset.
   */
  onPark?: (park: WorkflowPark) => void;
  /**
   * Optional author-signal park sink, the non-reserved-channel sibling of
   * `onPark`. Fired once each time a park on an author `awaitSignal` gate (a
   * plain, author-chosen `name`, not a reserved `signalName(correlationId)`
   * channel) must surface up for a container to relay, on the fresh park only
   * -- a re-park resume that finds the step already `awaiting-signal` does not
   * re-fire, the same discipline as `onPark`. Two sites fire it: a body's leaf
   * `awaitSignal` gate, and a container's own signal-relay await when that
   * container is itself a suspendable child (a loop iteration or onTrigger
   * section nested inside another), so the park composes up one layer at a time.
   * The suspendable-child seam wires it on every body env (loop iteration or
   * section body); a TOP-LEVEL container run and runLocal leave it unset, so the
   * outermost relay awaits the run's real channel directly and an author gate
   * off any suspendable-child path parks silently as before.
   */
  onSignalPark?: (park: WorkflowSignalPark) => void;
  /**
   * Optional read-only recovery hook: enumerate the durable pending approval
   * operations a step left behind, keyed by `{ runId, stepId, attempt }`. The
   * resume classifier consults it for a step that crashed mid-invocation (a
   * durable `StepStarted` with no `StepCompleted`) to distinguish a step that
   * crashed AFTER the reactor durably recorded an approval suspension but
   * BEFORE the `SignalAwaited` flushed -- resumable, by reconstructing the
   * missing `SignalAwaited` and re-parking -- from a genuine crash mid-agent
   * turn, which stays a terminal failure. Production wires it to the sidecar's
   * durable step store (cold isogit / warm substrate). A host that does not
   * wire it -- runLocal -- leaves every crashed invocation a terminal failure,
   * the pre-recovery behavior.
   */
  readParkedApprovalOps?: ReadParkedApprovalOps;
}

/**
 * Public handle a caller of `runWorkflow` interacts with. The
 * `complete` promise resolves once the run reaches a terminal phase;
 * `cancel` and `signal` are control-plane operations.
 */
export interface WorkflowRun {
  runId: string;
  complete: Promise<RunResult>;
  cancel(origin: "self" | "supervisor-operator", reason: string): Promise<void>;
  signal(name: string, payload: unknown, signalId?: string): Promise<void>;
}

export interface RunResult {
  runId: string;
  terminalStatus: "completed" | "failed" | "cancelled";
  /** Captured outputs of every step that reached `completed`. */
  outputs: Record<string, unknown>;
  /** The full event log as committed. */
  events: readonly WorkflowEvent[];
}

/** Discriminator for the executor's per-primitive dispatch. */
export type PrimitiveKind = Primitive["kind"];

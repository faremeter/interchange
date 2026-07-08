// Workflow runtime environment contract.
//
// `WorkflowRuntimeEnv` is the surface the single runtime body
// consumes. `runLocal` and any future child-process entry point
// satisfy this contract; the runtime body switches on env keys, not
// on which process it is running in. The body must not reference
// any `isChildProcess`-shaped discriminator -- an explicit
// source-level test in `run.test.ts` asserts the discipline.

import type { AgentDefinition, BaseEnv, DirectorRegistry } from "@intx/agent";

import type {
  AuthorizeContext,
  WorkflowAuthorizeFn,
} from "../authorize-context";
import type { Primitive, WorkflowDefinition } from "../definition/index";
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
}

export interface StepInvokeResult {
  output: unknown;
}

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
 * work begins on the child side. The callback resolves
 * `definitionRef` to a concrete `WorkflowDefinition` using whatever
 * lookup the runtime supplies (a `childResolver` function in
 * `runLocal`, a deploy-time resolver in production), constructs the
 * child run against the supplied id, and returns the terminal status.
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
}) => Promise<{
  terminalStatus: "completed" | "failed" | "cancelled";
}>;

/**
 * Run one loop iteration as a child run. Distinct from `spawnChild`:
 * loop iterations run the inline `bodyDefinition` against a SHARED store
 * (the parent's repoStore + blobs + effects) under a caller-supplied
 * DETERMINISTIC `childRunId`, and return the child's RESOLVED step
 * outputs (not just a terminal status) so the loop's while/carry
 * functions can read them without touching blob refs. The host owns
 * idempotency: a `childRunId` whose durable log is already terminal
 * returns its recorded outputs without re-running. Because a loop body
 * may not suspend (no awaitSignal/sleep/childWorkflow), a persisted
 * child log is always terminal -- a mid-iteration crash drops the whole
 * buffered segment, leaving an empty log the host re-runs fresh.
 */
export type RunLoopIteration = (input: {
  bodyDefinition: WorkflowDefinition;
  childRunId: string;
  input: unknown;
  parentRunId: string;
  parentStepId: string;
  signal: AbortSignal;
}) => Promise<{
  terminalStatus: "completed" | "failed" | "cancelled";
  output: Record<string, unknown>;
}>;

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
   * Run one loop iteration as a child run against the shared store.
   * Optional: a host that does not wire it does not support `loop`, and
   * `runLoop` fails loudly. runLocal wires it.
   */
  runLoopIteration?: RunLoopIteration;
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

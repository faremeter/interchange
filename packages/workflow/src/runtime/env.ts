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
import type { Primitive } from "../definition/index";
import type { WorkflowEvent } from "../state-machine/index";

/**
 * Read/append event log per run. The append-only invariant is the
 * state machine's responsibility; the repo store is the durable
 * substrate the runtime writes to and reads from on resume.
 */
export interface RepoStore {
  /** Return every committed event in seq order. */
  read(runId: string): Promise<readonly WorkflowEvent[]>;
  /** Append one event; rejects if seq is non-monotonic. */
  append(runId: string, event: WorkflowEvent): Promise<void>;
}

/**
 * Durable timer scheduler.
 *
 * The shape is callback-based: the runtime body calls `scheduleIn`,
 * commits `TimerSet` in its own event stream, and commits
 * `TimerFired` when the callback resolves. This is the only
 * callback-shaped substrate primitive in `WorkflowRuntimeEnv` --
 * every other primitive (`RepoStore`, `BlobSubstrate`,
 * `SignalChannel`) is in-memory or log-shaped. A future production
 * scheduler that observes `TimerSet` events from a durable log and
 * commits `TimerFired` from outside the workflow process would
 * prefer a "wait for `TimerFired` event in the log" shape; the
 * runtime body's `waitForTimer` helper would change with it. The
 * choice between the two shapes is a substrate-shaped decision that
 * is made when the durable substrate is implemented.
 */
export interface Scheduler {
  /**
   * Schedule `fire` to run after `delayMs` milliseconds. The returned
   * disposer cancels the pending callback.
   */
  scheduleIn(delayMs: number, fire: () => void): () => void;
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
  /** Spawn callback for `childWorkflow`. */
  spawnChild: SpawnChildWorkflow;
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

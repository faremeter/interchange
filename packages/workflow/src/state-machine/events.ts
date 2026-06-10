// Workflow-run event vocabulary.
//
// The event log on the `workflow-run` kind is the audit ledger for a
// run. Each event is the smallest committed unit of progress; events
// monotonically increase their `seq` per run, and the log is
// append-only.
//
// The transition function in `transition.ts` validates which events are
// legal in which states. See the design spec section 4 for the full
// narrative of the cancellation, signal, and timer invariants.

export type RunId = string;
export type StepId = string;
export type AttemptId = number;
export type SequenceNumber = number;
export type SignalId = string;
export type TimerId = string;

/**
 * Cancellation source. The state machine validates shape only; the
 * `workflow-run` kind handler validates that the signing principal is
 * consistent with the declared origin.
 *
 * - `self`               -- the workflow process cancelling its own run.
 * - `supervisor-drain`   -- the sidecar supervisor escalating a drain
 *                           timeout against a `cancel`-behavior step.
 * - `supervisor-operator`-- an operator cancellation routed through the
 *                           supervisor.
 * - `hub-admin`          -- a privileged hub-administration cancellation.
 */
export type CancelOrigin =
  | "self"
  | "supervisor-drain"
  | "supervisor-operator"
  | "hub-admin";

export const CANCEL_ORIGINS: readonly CancelOrigin[] = [
  "self",
  "supervisor-drain",
  "supervisor-operator",
  "hub-admin",
];

interface EventBase {
  /** Monotonically increasing per run, assigned at commit time. */
  seq: SequenceNumber;
  /** ISO-8601 commit timestamp. */
  at: string;
}

export interface RunStarted extends EventBase {
  kind: "RunStarted";
  runId: RunId;
  definitionHash: string;
  trigger: { type: string; payload: unknown };
  /**
   * Message-id of the mail that fired this run, recorded so the
   * workflow-run repo participates in the per-address claim-check dedup
   * on respawn-after-crash. Omitted for non-mail triggers.
   *
   * The state machine enforces dedup against the run-scoped
   * `consumedMessageIds` set (a re-issued `RunStarted` for an
   * already consumed message-id is rejected). The wider per-address
   * FIFO serialization invariant (two `mail` triggers at the same
   * address produce two `RunStarted`s in FIFO order, the second
   * queues until the first completes) lives in the queue substrate;
   * the runtime body has no role in enforcing it.
   */
  consumedMessageId?: string;
}

export interface StepStarted extends EventBase {
  kind: "StepStarted";
  stepId: StepId;
  attempt: AttemptId;
  input: { ref: string };
}

export interface StepCompleted extends EventBase {
  kind: "StepCompleted";
  stepId: StepId;
  attempt: AttemptId;
  output: { ref: string };
}

export interface StepFailed extends EventBase {
  kind: "StepFailed";
  stepId: StepId;
  attempt: AttemptId;
  error: { message: string; code?: string };
  retriesExhausted: boolean;
}

export interface AttemptScheduled extends EventBase {
  kind: "AttemptScheduled";
  stepId: StepId;
  nextAttempt: AttemptId;
  /** Must reference a prior `TimerSet` whose `stepId` is this step. */
  timerId: TimerId;
  fireAt: string;
}

export interface SignalAwaited extends EventBase {
  kind: "SignalAwaited";
  stepId: StepId;
  signalName: string;
  timeoutAt?: string;
}

export interface SignalReceived extends EventBase {
  kind: "SignalReceived";
  signalName: string;
  signalId: SignalId;
  payload: unknown;
}

export interface TimerSet extends EventBase {
  kind: "TimerSet";
  timerId: TimerId;
  fireAt: string;
  stepId?: StepId;
}

export interface TimerFired extends EventBase {
  kind: "TimerFired";
  timerId: TimerId;
}

export interface CancelRequested extends EventBase {
  kind: "CancelRequested";
  reason: string;
  origin: CancelOrigin;
}

export interface CancelPropagated extends EventBase {
  kind: "CancelPropagated";
  stepId: StepId;
}

export interface ChildSpawned extends EventBase {
  kind: "ChildSpawned";
  stepId: StepId;
  childRunId: RunId;
  /**
   * The lookup ref the parent used to resolve the child
   * `WorkflowDefinition`. The child run's own `RunStarted` event
   * carries the content-addressed `definitionHash`; this field is the
   * parent-side breadcrumb that points at which child definition was
   * requested, not the cryptographic identity of what was actually
   * instantiated. Downstream consumers correlating parent and child
   * audit trails should join on `childRunId`, not this ref.
   */
  childDefinitionRef: string;
}

export interface ChildCancelRequested extends EventBase {
  kind: "ChildCancelRequested";
  childRunId: RunId;
}

export interface ChildCompleted extends EventBase {
  kind: "ChildCompleted";
  childRunId: RunId;
  terminalStatus: "completed" | "failed" | "cancelled";
}

export interface RunCompleted extends EventBase {
  kind: "RunCompleted";
}

export interface RunFailed extends EventBase {
  kind: "RunFailed";
  error: { message: string };
}

export interface RunCancelled extends EventBase {
  kind: "RunCancelled";
}

export type WorkflowEvent =
  | RunStarted
  | StepStarted
  | StepCompleted
  | StepFailed
  | AttemptScheduled
  | SignalAwaited
  | SignalReceived
  | TimerSet
  | TimerFired
  | CancelRequested
  | CancelPropagated
  | ChildSpawned
  | ChildCancelRequested
  | ChildCompleted
  | RunCompleted
  | RunFailed
  | RunCancelled;

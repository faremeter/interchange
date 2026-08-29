// Workflow-run state shape.
//
// The state machine's transition function consumes events from the
// append-only log and returns a fresh `RunState`. The runtime
// reconstructs state from the log on resume; it never reads stale
// in-memory state across process restart.

import type { ControlParkKind } from "@intx/types/runtime";

import type {
  RunId,
  SequenceNumber,
  SignalId,
  StepId,
  TimerId,
} from "./events";

export type RunPhase =
  | "pending"
  | "running"
  | "cancelling"
  | "completed"
  | "failed"
  | "cancelled";

export type StepPhase =
  | "in-flight"
  | "awaiting-signal"
  | "awaiting-timer"
  | "completed"
  | "failed"
  | "cancelled";

export interface StepState {
  stepId: StepId;
  phase: StepPhase;
  currentAttempt: number;
  outputRef?: string;
  lastError?: { message: string };
  awaitingSignal?: {
    name: string;
    timeoutAt?: string;
    parkKind?: ControlParkKind;
  };
  awaitingTimerId?: TimerId;
}

/**
 * Resolve a control-plane park's kind to a definite value.
 *
 * An explicit `parkKind` (`"input"` or `"signal-relay"`) is returned verbatim.
 * `parkKind` is ABSENT in two cases: a plain `awaitSignal` gate (not a
 * control-plane park at all), and a reserved-channel park committed before the
 * kind was recorded. Callers reach the absent path only on a reserved
 * `signalName(correlationId)` channel, where an absent kind can only be the
 * latter -- a legacy park, which was an `"approval"` by construction (the
 * `input`/`signal-relay` kinds postdate it). A `"signal-relay"` park is on an
 * author-named (non-reserved) channel and always carries its explicit kind, so
 * it never falls to the absent-means-approval default. This function is the
 * SINGLE point of that legacy interpretation; every other read of `parkKind`
 * goes through it rather than re-deriving the rule.
 */
export function controlParkKindOf(awaitingSignal: {
  parkKind?: ControlParkKind;
}): ControlParkKind {
  if (awaitingSignal.parkKind === "input") return "input";
  if (awaitingSignal.parkKind === "signal-relay") return "signal-relay";
  return "approval";
}

export interface ChildState {
  childRunId: RunId;
  spawnedBy: StepId;
  cancelRequested: boolean;
  terminalStatus?: "completed" | "failed" | "cancelled";
  /**
   * Reduced from `ChildCompleted.abortedTeardown`: a `failed` terminal that is
   * a parent-cascade abort teardown rather than a genuine body failure. The
   * onTrigger resume classifier reads it to end a torn-down `tolerate` section
   * rather than resurrect it. See `ChildCompleted`.
   */
  abortedTeardown?: boolean;
}

export interface PendingTimer {
  timerId: TimerId;
  fireAt: string;
  stepId?: StepId;
}

export interface QueuedSignal {
  id: SignalId;
  payload: unknown;
}

export interface RunState {
  runId: RunId;
  phase: RunPhase;
  definitionHash?: string;
  lastSeq: SequenceNumber;
  steps: Map<StepId, StepState>;
  children: Map<RunId, ChildState>;
  pendingTimers: Map<TimerId, PendingTimer>;
  /** Run-lifetime dedup for `SignalReceived`. */
  observedSignalIds: Set<SignalId>;
  /** Queue per signal name for pre-await delivery. */
  unconsumedSignals: Map<string, QueuedSignal[]>;
  /**
   * Message-ids consumed by this run's `RunStarted` events. The kind
   * handler rejects a re-issued `RunStarted` whose message-id appears
   * here. v1 only ever issues `RunStarted` once per run, so the set has
   * at most one entry, but the invariant is enforced regardless so
   * resume-after-crash with partial commits stays well-defined.
   */
  consumedMessageIds: Set<string>;
  cancelReason?: string;
}

export function emptyState(runId: RunId): RunState {
  return {
    runId,
    phase: "pending",
    lastSeq: 0,
    steps: new Map(),
    children: new Map(),
    pendingTimers: new Map(),
    observedSignalIds: new Set(),
    unconsumedSignals: new Map(),
    consumedMessageIds: new Set(),
  };
}

export function isTerminalRunPhase(phase: RunPhase): boolean {
  return phase === "completed" || phase === "failed" || phase === "cancelled";
}

export function isTerminalStepPhase(phase: StepPhase): boolean {
  return phase === "completed" || phase === "failed" || phase === "cancelled";
}

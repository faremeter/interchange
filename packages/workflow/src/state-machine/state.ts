// Workflow-run state shape.
//
// The state machine's transition function consumes events from the
// append-only log and returns a fresh `RunState`. The runtime
// reconstructs state from the log on resume; it never reads stale
// in-memory state across process restart.

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
  awaitingSignal?: { name: string; timeoutAt?: string };
  awaitingTimerId?: TimerId;
}

export interface ChildState {
  childRunId: RunId;
  spawnedBy: StepId;
  cancelRequested: boolean;
  terminalStatus?: "completed" | "failed" | "cancelled";
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

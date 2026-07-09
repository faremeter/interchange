// The transition function -- the source of truth for legal workflow-run
// transitions. Each handler returns a fresh state; the function never
// mutates the input. Invariants are encoded as assertions that throw
// `TransitionError` with the offending event attached.
//
// Run lifecycle:
//
//   pending -> running -> { completed | failed | cancelling -> cancelled }
//
// Step lifecycle:
//
//   in-flight -> { awaiting-signal | awaiting-timer | completed | failed | cancelled }
//
// Cancellation wins over failure: once `cancelling`, a subsequent
// `RunFailed` is rejected. Cancellation also keeps already-terminal step
// phases intact (a step that completed before cancellation cascaded
// stays completed).
//
// Signal delivery is FIFO single-consumer with run-lifetime dedup by
// `signalId`. A `SignalReceived` whose name has no awaiter is queued
// under that name and consumed by the next `SignalAwaited` for the
// same name.

import { CANCEL_ORIGINS } from "./events";
import type {
  AttemptScheduled,
  CancelPropagated,
  CancelRequested,
  ChildCancelRequested,
  ChildCompleted,
  ChildSpawned,
  RunCancelled,
  RunCompleted,
  RunFailed,
  RunStarted,
  SignalAwaited,
  SignalReceived,
  StepCompleted,
  StepFailed,
  StepId,
  StepStarted,
  TimerFired,
  TimerSet,
  WorkflowEvent,
} from "./events";
import {
  isTerminalRunPhase,
  isTerminalStepPhase,
  type RunPhase,
  type RunState,
  type StepState,
} from "./state";

/**
 * Discriminator for the kinds of invariant violations the transition
 * function can raise. Every `throw new TransitionError(...)` in this
 * file passes one of these codes, so callers that need to react
 * programmatically (e.g. the runtime's `cancel` path tolerating a
 * terminal-phase race) inspect `code` rather than matching against the
 * message string.
 */
export type TransitionErrorCode =
  | "sequence"
  | "terminal-phase"
  | "phase"
  | "unknown-step"
  | "unknown-child"
  | "step-phase"
  | "attempt"
  | "timer"
  | "cancel-origin"
  | "consumed-message"
  | "step-already-started"
  | "child-already-spawned";

export class TransitionError extends Error {
  readonly event: WorkflowEvent;
  readonly code: TransitionErrorCode;
  constructor(
    code: TransitionErrorCode,
    message: string,
    event: WorkflowEvent,
  ) {
    super(
      `transition error at seq ${String(event.seq)} (${event.kind}): ${message}`,
    );
    this.name = "TransitionError";
    this.event = event;
    this.code = code;
  }
}

/**
 * Apply a single event to a state, returning a new state. Throws
 * `TransitionError` if the event violates an invariant for the current
 * state.
 *
 * Invariants enforced:
 *   - Sequence numbers start at 1 and strictly increase.
 *   - No events after a terminal phase.
 *   - Per-event preconditions (step exists, phase is legal, timer
 *     pairing for `AttemptScheduled`).
 */
export function applyEvent(state: RunState, event: WorkflowEvent): RunState {
  if (event.seq < 1) {
    throw new TransitionError("sequence", "sequence must be >= 1", event);
  }
  if (event.seq <= state.lastSeq) {
    throw new TransitionError(
      "sequence",
      `non-monotonic sequence (last=${String(state.lastSeq)}, this=${String(event.seq)})`,
      event,
    );
  }
  if (isTerminalRunPhase(state.phase)) {
    throw new TransitionError(
      "terminal-phase",
      `event after terminal phase ${state.phase}`,
      event,
    );
  }

  switch (event.kind) {
    case "RunStarted":
      return handleRunStarted(state, event);
    case "StepStarted":
      return handleStepStarted(state, event);
    case "StepCompleted":
      return handleStepCompleted(state, event);
    case "StepFailed":
      return handleStepFailed(state, event);
    case "AttemptScheduled":
      return handleAttemptScheduled(state, event);
    case "SignalAwaited":
      return handleSignalAwaited(state, event);
    case "SignalReceived":
      return handleSignalReceived(state, event);
    case "TimerSet":
      return handleTimerSet(state, event);
    case "TimerFired":
      return handleTimerFired(state, event);
    case "CancelRequested":
      return handleCancelRequested(state, event);
    case "CancelPropagated":
      return handleCancelPropagated(state, event);
    case "ChildSpawned":
      return handleChildSpawned(state, event);
    case "ChildCancelRequested":
      return handleChildCancelRequested(state, event);
    case "ChildCompleted":
      return handleChildCompleted(state, event);
    case "RunCompleted":
      return handleRunCompleted(state, event);
    case "RunFailed":
      return handleRunFailed(state, event);
    case "RunCancelled":
      return handleRunCancelled(state, event);
  }
}

/**
 * Drop the awaiter fields from a step. `exactOptionalPropertyTypes`
 * forbids assigning `undefined` to an optional field, so transitioning
 * a step out of `awaiting-signal` or `awaiting-timer` requires
 * structurally removing the awaiter keys.
 */
function withoutAwaiters(
  step: StepState,
): Omit<StepState, "awaitingSignal" | "awaitingTimerId"> {
  const { awaitingSignal: _signal, awaitingTimerId: _timer, ...rest } = step;
  return rest;
}

function handleRunStarted(state: RunState, e: RunStarted): RunState {
  if (state.phase !== "pending") {
    throw new TransitionError("phase", `RunStarted in phase ${state.phase}`, e);
  }
  if (
    e.consumedMessageId !== undefined &&
    state.consumedMessageIds.has(e.consumedMessageId)
  ) {
    throw new TransitionError(
      "consumed-message",
      `RunStarted re-issued for consumed message-id ${e.consumedMessageId}`,
      e,
    );
  }
  const consumedMessageIds = new Set(state.consumedMessageIds);
  if (e.consumedMessageId !== undefined) {
    consumedMessageIds.add(e.consumedMessageId);
  }
  return {
    ...state,
    phase: "running",
    definitionHash: e.definitionHash,
    consumedMessageIds,
    lastSeq: e.seq,
  };
}

function handleStepStarted(state: RunState, e: StepStarted): RunState {
  requireRunPhase(state, e, "running");
  // StepStarted marks the very first time a step enters the run. Later
  // attempts re-enter via AttemptScheduled + TimerFired rather than a
  // fresh StepStarted, so a step that already has state must not see
  // another StepStarted. Without this guard a misbehaving runner
  // could resurrect a terminal step entry or silently reset the
  // attempt counter.
  if (state.steps.has(e.stepId)) {
    throw new TransitionError(
      "step-already-started",
      `StepStarted for step ${e.stepId} which already has state; retries enter via AttemptScheduled + TimerFired`,
      e,
    );
  }
  const steps = new Map(state.steps);
  steps.set(e.stepId, {
    stepId: e.stepId,
    phase: "in-flight",
    currentAttempt: e.attempt,
  });
  return { ...state, steps, lastSeq: e.seq };
}

function handleStepCompleted(state: RunState, e: StepCompleted): RunState {
  const step = requireStep(state, e, e.stepId);
  if (isTerminalStepPhase(step.phase)) {
    throw new TransitionError(
      "step-phase",
      `StepCompleted for step already in terminal phase ${step.phase}`,
      e,
    );
  }
  const steps = new Map(state.steps);
  steps.set(e.stepId, {
    ...withoutAwaiters(step),
    phase: "completed",
    outputRef: e.output.ref,
  });
  return { ...state, steps, lastSeq: e.seq };
}

function handleStepFailed(state: RunState, e: StepFailed): RunState {
  const step = requireStep(state, e, e.stepId);
  if (isTerminalStepPhase(step.phase)) {
    throw new TransitionError(
      "step-phase",
      `StepFailed for step already in terminal phase ${step.phase}`,
      e,
    );
  }
  const steps = new Map(state.steps);
  steps.set(e.stepId, {
    ...withoutAwaiters(step),
    phase: "failed",
    lastError: e.error,
  });
  return { ...state, steps, lastSeq: e.seq };
}

function handleAttemptScheduled(
  state: RunState,
  e: AttemptScheduled,
): RunState {
  const step = requireStep(state, e, e.stepId);
  // AttemptScheduled rejects against a completed or cancelled step:
  // those terminal phases must not be resurrected into awaiting-timer
  // by a stray retry event. The legitimate retry path goes through
  // StepFailed which leaves the step in `failed` (also terminal under
  // `isTerminalStepPhase`) but is explicitly the retry-eligible
  // terminal; the handler permits that transition by name.
  if (step.phase === "completed" || step.phase === "cancelled") {
    throw new TransitionError(
      "step-phase",
      `AttemptScheduled refused to resurrect step ${e.stepId} from ${step.phase}`,
      e,
    );
  }
  if (step.currentAttempt >= e.nextAttempt) {
    throw new TransitionError(
      "attempt",
      `AttemptScheduled with non-increasing attempt (current=${String(step.currentAttempt)}, next=${String(e.nextAttempt)})`,
      e,
    );
  }
  const timer = state.pendingTimers.get(e.timerId);
  if (!timer) {
    throw new TransitionError(
      "timer",
      `AttemptScheduled references unknown timer ${e.timerId}; runtime must emit TimerSet first`,
      e,
    );
  }
  if (timer.stepId !== e.stepId) {
    const boundTo = timer.stepId ?? "<none>";
    throw new TransitionError(
      "timer",
      `AttemptScheduled timer ${e.timerId} is bound to step ${boundTo}, not ${e.stepId}`,
      e,
    );
  }
  const steps = new Map(state.steps);
  steps.set(e.stepId, {
    ...step,
    currentAttempt: e.nextAttempt,
    phase: "awaiting-timer",
    awaitingTimerId: e.timerId,
  });
  return { ...state, steps, lastSeq: e.seq };
}

function handleSignalAwaited(state: RunState, e: SignalAwaited): RunState {
  const step = requireStep(state, e, e.stepId);
  // Pre-await queued signal: consume the first unconsumed signal for
  // this name and keep the step in-flight. Otherwise transition to
  // awaiting-signal.
  const queue = state.unconsumedSignals.get(e.signalName);
  if (queue && queue.length > 0) {
    const remaining = queue.slice(1);
    const unconsumed = new Map(state.unconsumedSignals);
    if (remaining.length === 0) {
      unconsumed.delete(e.signalName);
    } else {
      unconsumed.set(e.signalName, remaining);
    }
    const steps = new Map(state.steps);
    steps.set(e.stepId, {
      ...withoutAwaiters(step),
      phase: "in-flight",
    });
    return {
      ...state,
      steps,
      unconsumedSignals: unconsumed,
      lastSeq: e.seq,
    };
  }
  const steps = new Map(state.steps);
  const awaitingSignal: { name: string; timeoutAt?: string } = {
    name: e.signalName,
    ...(e.timeoutAt !== undefined ? { timeoutAt: e.timeoutAt } : {}),
  };
  steps.set(e.stepId, {
    ...step,
    phase: "awaiting-signal",
    awaitingSignal,
  });
  return { ...state, steps, lastSeq: e.seq };
}

function handleSignalReceived(state: RunState, e: SignalReceived): RunState {
  // At-least-once delivery: duplicate signalIds are no-ops.
  if (state.observedSignalIds.has(e.signalId)) {
    return { ...state, lastSeq: e.seq };
  }
  const observed = new Set(state.observedSignalIds);
  observed.add(e.signalId);

  // FIFO single-consumer: find the first step awaiting this signal name.
  let awaiterId: StepId | undefined;
  for (const [id, step] of state.steps) {
    if (
      step.phase === "awaiting-signal" &&
      step.awaitingSignal?.name === e.signalName
    ) {
      awaiterId = id;
      break;
    }
  }

  if (awaiterId !== undefined) {
    const steps = new Map(state.steps);
    const awaiter = state.steps.get(awaiterId);
    if (!awaiter) {
      throw new TransitionError(
        "unknown-step",
        `internal: awaiter ${awaiterId} disappeared between scan and set`,
        e,
      );
    }
    steps.set(awaiterId, {
      ...withoutAwaiters(awaiter),
      phase: "in-flight",
    });
    return { ...state, steps, observedSignalIds: observed, lastSeq: e.seq };
  }

  // No awaiter present -- queue the signal for a future SignalAwaited.
  const unconsumed = new Map(state.unconsumedSignals);
  const queue = unconsumed.get(e.signalName) ?? [];
  unconsumed.set(e.signalName, [
    ...queue,
    { id: e.signalId, payload: e.payload },
  ]);
  return {
    ...state,
    unconsumedSignals: unconsumed,
    observedSignalIds: observed,
    lastSeq: e.seq,
  };
}

function handleTimerSet(state: RunState, e: TimerSet): RunState {
  const pendingTimers = new Map(state.pendingTimers);
  const pending: { timerId: string; fireAt: string; stepId?: string } = {
    timerId: e.timerId,
    fireAt: e.fireAt,
    ...(e.stepId !== undefined ? { stepId: e.stepId } : {}),
  };
  pendingTimers.set(e.timerId, pending);
  // A `TimerSet` bound to a step that is currently in-flight (the
  // sleep primitive's awaiting transition; the retry path's failed
  // step is not in-flight at this point) transitions the step to
  // `awaiting-timer`. The TimerFired handler reverses the transition.
  // Without this move, a sleeping step is indistinguishable from one
  // doing active compute -- the drain policy and operator UIs cannot
  // tell when a workflow is parked on a timer.
  let steps = state.steps;
  if (e.stepId !== undefined) {
    const step = state.steps.get(e.stepId);
    if (step && step.phase === "in-flight") {
      steps = new Map(steps);
      steps.set(e.stepId, {
        ...step,
        phase: "awaiting-timer",
        awaitingTimerId: e.timerId,
      });
    }
  }
  return { ...state, steps, pendingTimers, lastSeq: e.seq };
}

function handleTimerFired(state: RunState, e: TimerFired): RunState {
  const pending = state.pendingTimers.get(e.timerId);
  if (!pending) {
    // Already-resolved timer firing again: idempotent no-op for state.
    return { ...state, lastSeq: e.seq };
  }
  const pendingTimers = new Map(state.pendingTimers);
  pendingTimers.delete(e.timerId);

  let steps = state.steps;
  if (pending.stepId !== undefined) {
    const step = state.steps.get(pending.stepId);
    if (step?.phase === "awaiting-timer" || step?.phase === "awaiting-signal") {
      steps = new Map(steps);
      steps.set(pending.stepId, {
        ...withoutAwaiters(step),
        phase: "in-flight",
      });
    }
  }
  return { ...state, steps, pendingTimers, lastSeq: e.seq };
}

function handleCancelRequested(state: RunState, e: CancelRequested): RunState {
  if (!CANCEL_ORIGINS.includes(e.origin)) {
    throw new TransitionError(
      "cancel-origin",
      `CancelRequested unknown origin ${e.origin}`,
      e,
    );
  }
  if (state.phase === "cancelling") {
    return { ...state, lastSeq: e.seq };
  }
  // CancelRequested is legal from `pending` (the early-lifecycle
  // window where the runtime has not yet committed `RunStarted`) and
  // from `running`. In both cases the run transitions to `cancelling`
  // and the runtime body's cleanup path emits `RunCancelled`. The
  // pending-side admission lets a caller cancel a run synchronously
  // after `runtimeRun` returns without racing the body's first
  // `RunStarted` commit.
  if (state.phase !== "running" && state.phase !== "pending") {
    throw new TransitionError(
      "phase",
      `CancelRequested in phase ${state.phase}`,
      e,
    );
  }
  return {
    ...state,
    phase: "cancelling",
    cancelReason: e.reason,
    lastSeq: e.seq,
  };
}

function handleCancelPropagated(
  state: RunState,
  e: CancelPropagated,
): RunState {
  if (state.phase !== "cancelling") {
    throw new TransitionError(
      "phase",
      `CancelPropagated in phase ${state.phase}`,
      e,
    );
  }
  const step = state.steps.get(e.stepId);
  if (!step) {
    // Propagating cancel to a step that never started: idempotent.
    return { ...state, lastSeq: e.seq };
  }
  // Do not overwrite an already-terminal step phase. A step that
  // completed before cancellation cascaded keeps its actual terminal
  // status; only in-flight or awaiting steps transition to cancelled.
  if (isTerminalStepPhase(step.phase)) {
    return { ...state, lastSeq: e.seq };
  }
  const steps = new Map(state.steps);
  steps.set(e.stepId, {
    ...withoutAwaiters(step),
    phase: "cancelled",
  });
  return { ...state, steps, lastSeq: e.seq };
}

function handleChildSpawned(state: RunState, e: ChildSpawned): RunState {
  requireRunPhase(state, e, "running");
  // ChildSpawned inserts a child into the keyed children map for the
  // first time, like handleStepStarted inserts into steps. A second
  // ChildSpawned for an existing childRunId is a double-insertion, not a
  // re-observed transition, so it is rejected rather than allowed to
  // clobber the child's terminalStatus/cancelRequested (which would
  // resurrect a finished child into the cancel cascade). The guard is the
  // invariant, not any assumption about freshly minted ids: a re-emitted
  // ChildSpawned for an already-known child -- which a loop's
  // deterministic per-iteration child id produces on resume -- is rejected
  // rather than allowed to overwrite the existing child.
  if (state.children.has(e.childRunId)) {
    throw new TransitionError(
      "child-already-spawned",
      `ChildSpawned for child ${e.childRunId} which already has state`,
      e,
    );
  }
  const children = new Map(state.children);
  children.set(e.childRunId, {
    childRunId: e.childRunId,
    spawnedBy: e.stepId,
    cancelRequested: false,
  });
  return { ...state, children, lastSeq: e.seq };
}

function handleChildCancelRequested(
  state: RunState,
  e: ChildCancelRequested,
): RunState {
  const child = state.children.get(e.childRunId);
  if (!child) {
    throw new TransitionError(
      "unknown-child",
      `ChildCancelRequested for unknown child ${e.childRunId}`,
      e,
    );
  }
  if (child.cancelRequested) {
    return { ...state, lastSeq: e.seq };
  }
  const children = new Map(state.children);
  children.set(e.childRunId, { ...child, cancelRequested: true });
  return { ...state, children, lastSeq: e.seq };
}

function handleChildCompleted(state: RunState, e: ChildCompleted): RunState {
  const child = state.children.get(e.childRunId);
  if (!child) {
    throw new TransitionError(
      "unknown-child",
      `ChildCompleted for unknown child ${e.childRunId}`,
      e,
    );
  }
  if (child.terminalStatus !== undefined) {
    return { ...state, lastSeq: e.seq };
  }
  const children = new Map(state.children);
  children.set(e.childRunId, { ...child, terminalStatus: e.terminalStatus });
  return { ...state, children, lastSeq: e.seq };
}

function handleRunCompleted(state: RunState, e: RunCompleted): RunState {
  requireRunPhase(state, e, "running");
  return { ...state, phase: "completed", lastSeq: e.seq };
}

function handleRunFailed(state: RunState, e: RunFailed): RunState {
  // Cancellation wins: if the run is already cancelling, step failures
  // do not promote the run to "failed".
  if (state.phase !== "running") {
    throw new TransitionError(
      "phase",
      `RunFailed in phase ${state.phase}; cancellation wins`,
      e,
    );
  }
  return { ...state, phase: "failed", lastSeq: e.seq };
}

function handleRunCancelled(state: RunState, e: RunCancelled): RunState {
  if (state.phase !== "cancelling") {
    throw new TransitionError(
      "phase",
      `RunCancelled in phase ${state.phase}`,
      e,
    );
  }
  return { ...state, phase: "cancelled", lastSeq: e.seq };
}

function requireRunPhase(
  state: RunState,
  event: WorkflowEvent,
  expected: RunPhase,
): void {
  if (state.phase !== expected) {
    throw new TransitionError(
      "phase",
      `expected phase ${expected}, was ${state.phase}`,
      event,
    );
  }
}

function requireStep(
  state: RunState,
  event: WorkflowEvent,
  stepId: StepId,
): StepState {
  const step = state.steps.get(stepId);
  if (!step) {
    throw new TransitionError("unknown-step", `unknown step ${stepId}`, event);
  }
  return step;
}

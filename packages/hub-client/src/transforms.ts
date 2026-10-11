import { correlationIdFromSignalName } from "@intx/types/signals";

import type { WorkflowRunEvent } from "./validators";
import type { AwaitingSignal } from "./types";

// The run-event types that seal a run: once one of these is committed the run
// has settled and its event log will not grow.
export const TERMINAL_RUN_EVENT_TYPES: readonly string[] = [
  "RunCompleted",
  "RunFailed",
  "RunCancelled",
];

export function isTerminalRunEvents(events: WorkflowRunEvent[]): boolean {
  return events.some((e) => TERMINAL_RUN_EVENT_TYPES.includes(e.type));
}

const STEP_RETIRED_EVENT_TYPES: readonly string[] = [
  "StepCompleted",
  "StepFailed",
  "CancelPropagated",
];

/**
 * Every live author-signal park a caller can complete, in log order.
 *
 * A park is offered only while the run is still waiting on it. A
 * delivery that is already in the log satisfies the later await, so
 * that await is not offered. A correlated park then stays until a
 * `SignalReceived` or `SignalAwaitAbandoned` for that id and author
 * name. A name-only receive of the same name does not clear it. A
 * name-only park clears on a name-only `SignalReceived` or
 * `SignalAwaitAbandoned` for that name. The park also leaves when its
 * step completes, fails, or is cancelled, when the timer bound to that
 * step fires, or when the run ends. A reserved `__signal__:` park is an
 * input or approval channel and is omitted.
 */
export function findAwaitingSignals(
  events: WorkflowRunEvent[],
): AwaitingSignal[] {
  const live: AwaitingSignal[] = [];
  const seenSignalIds = new Set<string>();
  const pendingByName = new Map<string, number>();
  const pendingByCorrelation = new Map<string, number>();
  const timerStepId = new Map<string, string>();
  for (const event of events) {
    if (TERMINAL_RUN_EVENT_TYPES.includes(event.type)) {
      live.length = 0;
      continue;
    }
    if (STEP_RETIRED_EVENT_TYPES.includes(event.type)) {
      const stepId = readString(event.body, "stepId");
      if (stepId !== undefined) retireStep(live, stepId);
      continue;
    }
    if (event.type === "TimerSet") {
      const timerId = readString(event.body, "timerId");
      const stepId = readString(event.body, "stepId");
      if (timerId !== undefined && stepId !== undefined) {
        timerStepId.set(timerId, stepId);
      }
      continue;
    }
    if (event.type === "TimerFired") {
      const timerId = readString(event.body, "timerId");
      const stepId =
        timerId !== undefined ? timerStepId.get(timerId) : undefined;
      if (stepId !== undefined) retireStep(live, stepId);
      continue;
    }
    if (event.type === "SignalAwaited") {
      const signalName = event.body["signalName"];
      if (typeof signalName !== "string") {
        throw new Error(
          `SignalAwaited event at seq ${String(event.seq)} is missing a string signalName`,
        );
      }
      if (correlationIdFromSignalName(signalName) !== undefined) continue;
      const stepId = readString(event.body, "stepId");
      const correlationId = readString(event.body, "correlationId");
      const alreadySatisfied =
        correlationId !== undefined
          ? takePending(
              pendingByCorrelation,
              correlationKey(correlationId, signalName),
            )
          : takePending(pendingByName, signalName);
      if (alreadySatisfied) continue;
      live.push({
        seq: event.seq,
        signalName,
        ...(stepId !== undefined ? { stepId } : {}),
        ...(correlationId !== undefined ? { correlationId } : {}),
      });
      continue;
    }
    if (
      event.type === "SignalReceived" ||
      event.type === "SignalAwaitAbandoned"
    ) {
      const signalName = readString(event.body, "signalName");
      if (signalName === undefined) continue;
      const correlationId = readString(event.body, "correlationId");
      if (event.type === "SignalReceived") {
        const signalId = readString(event.body, "signalId");
        if (signalId !== undefined) {
          if (seenSignalIds.has(signalId)) continue;
          seenSignalIds.add(signalId);
        }
      }
      if (retireSlot(live, signalName, correlationId)) continue;
      // An abandon retires a park. It is not a delivery, so it must not
      // satisfy a later await of the same name.
      if (event.type !== "SignalReceived") continue;
      if (correlationId !== undefined) {
        const heldByOtherName = live.some(
          (park) =>
            park.correlationId === correlationId &&
            park.signalName !== signalName,
        );
        if (!heldByOtherName) {
          addPending(
            pendingByCorrelation,
            correlationKey(correlationId, signalName),
          );
        }
      } else {
        addPending(pendingByName, signalName);
      }
    }
  }
  return live;
}

/**
 * The latest live author-signal park, or null when the run has none.
 * Callers that must address every park use {@link findAwaitingSignals}.
 */
export function findAwaitingSignal(
  events: WorkflowRunEvent[],
): AwaitingSignal | null {
  const live = findAwaitingSignals(events);
  return live[live.length - 1] ?? null;
}

function readString(
  body: Record<string, unknown>,
  key: string,
): string | undefined {
  const value = body[key];
  return typeof value === "string" ? value : undefined;
}

function correlationKey(correlationId: string, signalName: string): string {
  return `${correlationId}\0${signalName}`;
}

function addPending(pending: Map<string, number>, key: string): void {
  const count = pending.get(key);
  pending.set(key, count === undefined ? 1 : count + 1);
}

function takePending(pending: Map<string, number>, key: string): boolean {
  const count = pending.get(key);
  if (count === undefined) return false;
  if (count === 1) pending.delete(key);
  else pending.set(key, count - 1);
  return true;
}

function retireStep(live: AwaitingSignal[], stepId: string): void {
  for (let i = live.length - 1; i >= 0; i -= 1) {
    if (live[i]?.stepId === stepId) live.splice(i, 1);
  }
}

function retireSlot(
  live: AwaitingSignal[],
  signalName: string,
  correlationId: string | undefined,
): boolean {
  let removed = false;
  for (let i = live.length - 1; i >= 0; i -= 1) {
    const park = live[i];
    if (park === undefined || park.signalName !== signalName) continue;
    const sameSlot =
      correlationId === undefined
        ? park.correlationId === undefined
        : park.correlationId === correlationId;
    if (!sameSlot) continue;
    live.splice(i, 1);
    removed = true;
  }
  return removed;
}

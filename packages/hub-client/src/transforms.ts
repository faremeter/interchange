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

/**
 * Returns the awaited signal from the latest unresolved `SignalAwaited`
 * event, or null when the run is not currently parked on a signal. A later
 * `SignalReceived` carrying the same `signalName`, or a terminal event, clears
 * the await.
 */
export function findAwaitingSignal(
  events: WorkflowRunEvent[],
): AwaitingSignal | null {
  let awaiting: AwaitingSignal | null = null;
  for (const event of events) {
    if (event.type === "SignalAwaited") {
      const signalName = event.body["signalName"];
      if (typeof signalName !== "string") {
        throw new Error(
          `SignalAwaited event at seq ${String(event.seq)} is missing a string signalName`,
        );
      }
      awaiting = { seq: event.seq, signalName };
      continue;
    }
    if (awaiting === null) {
      continue;
    }
    if (
      event.type === "SignalReceived" &&
      event.body["signalName"] === awaiting.signalName
    ) {
      awaiting = null;
      continue;
    }
    if (TERMINAL_RUN_EVENT_TYPES.includes(event.type)) {
      awaiting = null;
    }
  }
  return awaiting;
}

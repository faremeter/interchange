import type { WorkflowRunEvent } from "./validators";

export type { WorkflowRunEvent };

// A run parked on a signal: seq of the latest unresolved `SignalAwaited`
// event and the signal name it awaits.
export type AwaitingSignal = {
  seq: number;
  signalName: string;
};

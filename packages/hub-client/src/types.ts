import type { WorkflowRunEvent } from "./validators";

export type { WorkflowRunEvent };

// A run parked on a signal it has not yet received: the seq of the latest
// unresolved `SignalAwaited` event and the signal it awaits. Null when the run
// is not currently waiting.
export type AwaitingSignal = {
  seq: number;
  signalName: string;
};

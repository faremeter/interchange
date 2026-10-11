import type { WorkflowRunEvent } from "./validators";

export type { WorkflowRunEvent };

// One live signal park a caller can complete. `correlationId` is set on an
// onTrigger relay park; a name-only gate omits it. `stepId` is the step
// that parked, when the event recorded one.
export type AwaitingSignal = {
  seq: number;
  signalName: string;
  stepId?: string;
  correlationId?: string;
};

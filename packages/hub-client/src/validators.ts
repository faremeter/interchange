import { type } from "arktype";

// A single committed run-event: `type` is the discriminator; `body` is the
// verbatim per-type payload (the hub validates it at push time).
export const WorkflowRunEvent = type({
  seq: "number",
  type: "string",
  body: "Record<string, unknown>",
});
export type WorkflowRunEvent = typeof WorkflowRunEvent.infer;

// The run-event log read response: the run id and its seq-ordered events.
export const WorkflowRunEvents = type({
  runId: "string",
  events: WorkflowRunEvent.array(),
});
export type WorkflowRunEvents = typeof WorkflowRunEvents.infer;

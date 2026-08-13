import { type } from "arktype";

// A single committed workflow-run event as the run-event log records it.
// `type` is the discriminator; `body` carries the full per-type payload
// verbatim (the hub validates the shape at push time, so the client narrows
// on the discriminator it cares about).
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

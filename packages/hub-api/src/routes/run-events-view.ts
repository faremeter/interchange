// The wire shape for a workflow run's committed event log -- one schema, one
// shaper -- shared by the deploy/observe surface (`workflows.ts`) and the
// run-observe surface (`runs.ts`) so the two routes cannot drift their
// contract.

import { type } from "arktype";

import type { WorkflowRunEvent } from "@intx/hub-sessions";

// A single committed workflow-run event. `type` is the discriminator; `body`
// carries the full per-type payload verbatim (the workflow-run kind handler
// validates the shape at push time).
const WorkflowRunEventResponse = type({
  seq: "number",
  type: "string",
  body: "Record<string, unknown>",
});

export const WorkflowRunEventsResponse = type({
  runId: "string",
  events: WorkflowRunEventResponse.array(),
});

export function formatRunEvent(event: WorkflowRunEvent) {
  return { seq: event.seq, type: event.type, body: event.body };
}

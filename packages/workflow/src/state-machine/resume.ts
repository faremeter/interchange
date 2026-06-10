// Reconstruct run state from an event log.
//
// Used on engine restart and on every scheduler tick for a given run.
// The transition function in `transition.ts` validates each event in
// turn; an invalid log surfaces as a `TransitionError` thrown out of
// this function.
//
// If the log shows `CancelRequested` without a matching `RunCancelled`,
// the resulting state's phase is `cancelling` -- the runtime should
// re-issue `CancelPropagated` for any non-terminal steps and
// `ChildCancelRequested` for any tracked children whose
// `cancelRequested` flag is still false, then write `RunCancelled` once
// the cascade is acknowledged.

import type { RunId, WorkflowEvent } from "./events";
import { emptyState, type RunState } from "./state";
import { applyEvent } from "./transition";

export function resumeFromLog(
  runId: RunId,
  events: readonly WorkflowEvent[],
): RunState {
  let state = emptyState(runId);
  for (const event of events) {
    state = applyEvent(state, event);
  }
  return state;
}

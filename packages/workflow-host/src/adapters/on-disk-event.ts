// The flat shape of a workflow event on disk. The supervisor writes events too,
// and the sidecar that loads it must not load the workflow runtime, so this
// stays apart from the runtime's store adapter.

import type { WorkflowEvent } from "@intx/workflow";

/**
 * Translate a state-machine `WorkflowEvent` (using `kind` as the
 * discriminator) into the on-disk envelope shape (`{seq, type,
 * ...rest}`) the workflow-run kind handler validates and the
 * substrate's `subscribeKind` helper filters on. Exported so the
 * supervisor's terminal-commit path encodes a supervisor-authored
 * `RunFailed` through the same single source of the on-disk shape.
 */
export function workflowEventToOnDisk(
  event: WorkflowEvent,
  seq: number,
): Record<string, unknown> {
  const { kind, seq: _eventSeq, ...rest } = event;
  return { seq, type: kind, ...rest };
}

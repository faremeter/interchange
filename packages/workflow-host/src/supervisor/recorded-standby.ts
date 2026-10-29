import { type } from "arktype";

import { WorkflowProjectionDefinition } from "@intx/types/sidecar";

import { ParkedCorrelation } from "../ipc/control-payloads";

/**
 * A child in standby as its host records it. A restart restores the standby
 * from this, without the child, while the run's history is still at `tip`:
 * `parked` is what the stopped child's runs were parked on, `wakeAt` the
 * earliest timer it had queued, `tip` the run's history tip, read before the
 * child reported its parks, and `definition` the one the first child verified,
 * which lays the steps out again with no child to report it.
 */
export const RecordedStandby = type({
  parked: ParkedCorrelation.array(),
  "wakeAt?": "string.date.iso",
  tip: "string | null",
  definition: WorkflowProjectionDefinition,
});
export type RecordedStandby = typeof RecordedStandby.infer;

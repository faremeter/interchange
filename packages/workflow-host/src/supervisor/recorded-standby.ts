import { type } from "arktype";

import { ParkedCorrelation } from "../ipc/control-payloads";

/**
 * A child in standby as its host records it. A restart restores the standby
 * from this, without the child, while the run's history is still at `tip`:
 * `parked` is what the stopped child's runs were parked on, `wakeAt` the
 * earliest timer it had queued, and `tip` the run's history tip, read before
 * the child reported its parks.
 */
export const RecordedStandby = type({
  parked: ParkedCorrelation.array(),
  "wakeAt?": "string.date.iso",
  tip: "string | null",
});
export type RecordedStandby = typeof RecordedStandby.infer;

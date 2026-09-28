import { getLogger } from "@intx/log";
import type { InferenceEffort } from "@intx/types/runtime";

const logger = getLogger(["interchange", "inference", "adapter"]);

// A step routing to a model that cannot carry its effort re-issues the same
// drop on every turn and every retry, so each distinct drop is reported once
// for the life of the process.
const reported = new Set<string>();

/** Forget every reported drop so tests can assert on the next one. */
export function resetDroppedEffortReports(): void {
  reported.clear();
}

/**
 * A named `effort` the adapter cannot put on the wire. The selector accepts
 * the field with no model awareness, so the drop is warn-logged rather than
 * a silent no-op.
 */
export function warnDroppedEffort(
  model: string,
  effort: InferenceEffort,
  reason: string,
): void {
  const key = JSON.stringify([model, effort, reason]);
  if (reported.has(key)) return;
  reported.add(key);
  logger.warn`Dropping set effort ${effort} for model ${model}: ${reason}`;
}

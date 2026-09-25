import { getLogger } from "@intx/log";
import type { InferenceEffort } from "@intx/types/runtime";

const logger = getLogger(["interchange", "inference", "adapter"]);

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
  logger.warn`Dropping set effort ${effort} for model ${model}: ${reason}`;
}

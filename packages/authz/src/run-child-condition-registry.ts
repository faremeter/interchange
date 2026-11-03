import { timeWindowEvaluator } from "./time-window";
import type { ConditionRegistry } from "./types";

/**
 * The conditions a workflow run's child process can evaluate without a
 * request: only the clock. The child evaluates its step grants with this
 * registry, and any other condition key throws there, so the authorization
 * fails closed.
 */
export const RUN_CHILD_CONDITION_REGISTRY: Readonly<ConditionRegistry> =
  Object.freeze({
    time_window: timeWindowEvaluator,
  });

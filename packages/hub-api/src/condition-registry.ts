import { timeWindowEvaluator } from "@intx/authz";
import type { ConditionRegistry } from "@intx/types/authz";

/**
 * The hub's standard condition registry: every condition evaluator a hub grant
 * may name. Every hub component that evaluates grants uses this constant, so an
 * evaluator added here applies to all of them.
 */
export const HUB_CONDITION_REGISTRY: Readonly<ConditionRegistry> =
  Object.freeze({
    time_window: timeWindowEvaluator,
  });

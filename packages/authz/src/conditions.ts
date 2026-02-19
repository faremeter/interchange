import type { ConditionContext, ConditionRegistry } from "./types";

/**
 * Evaluate whether a grant's conditions are satisfied.
 *
 * Each key in the conditions object is looked up in the registry.
 * All conditions must be met for the grant to apply. If any
 * condition key has no registered evaluator, an error is thrown
 * to surface misconfiguration immediately.
 *
 * Null or empty conditions are always met.
 */
export async function evaluateConditions(
  conditions: Record<string, unknown> | null,
  ctx: ConditionContext,
  registry: ConditionRegistry = {},
): Promise<boolean> {
  if (!conditions) return true;

  const keys = Object.keys(conditions);
  if (keys.length === 0) return true;

  for (const key of keys) {
    const evaluator = registry[key];
    if (!evaluator) {
      throw new Error(
        `Unknown condition: "${key}". Register an evaluator in the condition registry.`,
      );
    }

    const result = await evaluator(conditions[key], ctx);
    if (!result) return false;
  }

  return true;
}

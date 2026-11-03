import type {
  ConditionContext,
  ConditionEvaluator,
  ConditionRegistry,
} from "./types";

function unknownConditionMessage(key: string): string {
  return `Unknown condition: "${key}"`;
}

// Own properties only: a registry is a plain object, so a key such as
// "constructor" or "__proto__" would otherwise resolve to an inherited value
// that is not an evaluator.
function lookupEvaluator(
  registry: ConditionRegistry,
  key: string,
): ConditionEvaluator | undefined {
  return Object.hasOwn(registry, key) ? registry[key] : undefined;
}

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
  registry: ConditionRegistry,
): Promise<boolean> {
  if (!conditions) return true;

  const keys = Object.keys(conditions);
  if (keys.length === 0) return true;

  for (const key of keys) {
    const evaluator = lookupEvaluator(registry, key);
    if (!evaluator) {
      throw new Error(
        `${unknownConditionMessage(key)}. Register an evaluator in the condition registry.`,
      );
    }

    const result = await evaluator.evaluate(conditions[key], ctx);
    if (!result) return false;
  }

  return true;
}

/**
 * Check a grant's conditions object against a registry without evaluating
 * it. Returns one problem per bad entry, in key order: an unregistered key,
 * or a body the key's evaluator rejects. An empty result means
 * `evaluateConditions` can evaluate every entry against the same registry.
 *
 * Each evaluator's message already names its condition, so it passes
 * through unchanged.
 */
export function validateConditions(
  conditions: Record<string, unknown> | null,
  registry: ConditionRegistry,
): string[] {
  if (!conditions) return [];

  const problems: string[] = [];
  for (const [key, value] of Object.entries(conditions)) {
    const evaluator = lookupEvaluator(registry, key);
    if (!evaluator) {
      problems.push(unknownConditionMessage(key));
      continue;
    }
    const problem = evaluator.validate(value);
    if (problem !== null) problems.push(problem);
  }
  return problems;
}

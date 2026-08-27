// Stack-safety bound on childWorkflow spawn nesting.
//
// A childWorkflow runs its child in-process (runLocal recurses on itself;
// the unified host recurses through the same rung-0 child), so a deeply
// nested authored chain -- parent spawns child spawns child ... -- pushes a
// fresh runtimeRun frame per rung onto one shared stack. Nothing in the
// definition surface bounds that nesting: an authored cross-definition cycle
// is impossible (inline vendoring lifts every child to a strict-descendant
// `{ ref }`), but a legitimately deep chain still overflows. This ceiling is
// the backstop that fails such a chain loud instead of crashing the process
// (and, in a multiplexed child, every co-tenant run with it).

export const MAX_CHILD_SPAWN_DEPTH = 32;

/**
 * Resolve the effective ceiling at a `runtimeRun` edge. A caller (a test)
 * may request a LOWER ceiling, but never a higher one: production can only
 * be made stricter, never looser, so an injected value cannot defeat the
 * backstop. Absent request defaults to the constant. A non-finite request
 * (`NaN`/`Infinity`) is rejected rather than clamped, because `Math.min`
 * would let `NaN` through and `childDepth > NaN` is always false -- a
 * silently disabled guard is exactly what this ceiling exists to prevent.
 */
export function resolveMaxChildSpawnDepth(
  requested: number | undefined,
): number {
  if (requested === undefined) return MAX_CHILD_SPAWN_DEPTH;
  if (!Number.isFinite(requested)) {
    throw new Error(
      `maxChildSpawnDepth must be a finite number; got ${String(requested)}`,
    );
  }
  return Math.min(requested, MAX_CHILD_SPAWN_DEPTH);
}

export class ChildSpawnDepthExceededError extends Error {
  readonly depth: number;
  readonly parentStepId: string;
  readonly maxDepth: number;
  constructor(depth: number, parentStepId: string, maxDepth: number) {
    super(
      `childWorkflow spawn depth ${String(depth)} exceeds the maximum ` +
        `${String(maxDepth)} at step ${JSON.stringify(parentStepId)}; ` +
        `the child chain is too deeply nested to run without risking a ` +
        `rung-0 stack overflow`,
    );
    this.name = "ChildSpawnDepthExceededError";
    this.depth = depth;
    this.parentStepId = parentStepId;
    this.maxDepth = maxDepth;
  }
}

/**
 * Guard a spawn decision. Called at the spawn site (before any
 * `StepStarted`/`ChildSpawned` is committed) so the throw becomes a clean
 * `StepFailed` on the parent step and no phantom child-run log is ever
 * created for the rejected spawn.
 */
export function assertSpawnDepthWithinLimit(
  childDepth: number,
  parentStepId: string,
  maxDepth: number,
): void {
  if (childDepth > maxDepth) {
    throw new ChildSpawnDepthExceededError(childDepth, parentStepId, maxDepth);
  }
}

// Single owner of the scoped step-id format for fan-out iterations.
//
// A `map` iteration runs its inner step in place, and a `loop` iteration
// tracks its step state, under a per-index scoped step id
// `<baseStepId>[<index>]`. The base id is an author-declared `stepId`,
// constrained by `STEP_ID_PATTERN` (`../definition/workflow`) to
// `[a-zA-Z0-9_-]+`, so a base id never contains a bracket. The trailing
// `[<index>]` is therefore an unambiguous scope marker.
//
// Every site that mints a scoped id calls `scopedStepId`; every site that
// recovers the base id for a definition or deploy-asset lookup calls
// `baseStepId`. Keeping the encode/decode pair here means the format has one
// owner rather than a hand-rolled template and several divergent strip
// regexes scattered across the runtime and the sidecar.
//
// This module also owns the related loop-iteration body RUN id format
// (`loopBodyRunId`): a cross-run store key rather than an in-run step id, so it
// carries the container run id and is documented separately below.

/**
 * Encode a fan-out iteration's scoped step id from its base step id and
 * zero-based iteration index.
 */
export function scopedStepId(base: string, index: number): string {
  return `${base}[${String(index)}]`;
}

/**
 * Recover the base step id from a scoped iteration id, stripping a single
 * trailing `[<digits>]`. Identity on an already-unscoped id. A single strip
 * is correct because iterations do not nest: a `MapPrimitive.step` is a
 * `StepPrimitive`, so `<base>[<i>][<j>]` cannot arise.
 */
export function baseStepId(stepId: string): string {
  return stepId.replace(/\[\d+\]$/, "");
}

/**
 * Encode a loop iteration's body-child run id from the loop's own run id, the
 * loop step id, and the zero-based iteration index. Unlike the scoped STEP id
 * (which lives inside a single run's step namespace), the body run id is a
 * cross-run store key, so it carries the container run id as an ancestry
 * prefix: a loop nested in an outer iteration runs under that iteration's body
 * run id, so `<runId>__<loopId>__<index>` re-roots per nesting level and an
 * inner loop under two outer iterations gets distinct ids. Deterministic --
 * crash-resume re-derives the same string rather than reversing it.
 *
 * Injectivity does NOT require a `__`-free run id (a nested loop's own run id
 * contains `__`, and a caller-supplied top-level run id may too). It holds
 * because `loopId` contains no `__` (rejected at definition time in
 * `normalize`) and `index` is always digits: the final two `__` in the output
 * are therefore always the two separators, so the string decomposes to exactly
 * one (runId, loopId, index) regardless of what the run id contains. The run
 * id is separately constrained by `RUN_ID_PATTERN` for store-path and
 * mail-address safety, not for injectivity.
 */
export function loopBodyRunId(
  runId: string,
  loopId: string,
  index: number,
): string {
  return `${runId}__${loopId}__${String(index)}`;
}

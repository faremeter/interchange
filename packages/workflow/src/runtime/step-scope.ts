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
// This module also owns the related body RUN id formats (`loopBodyRunId`,
// `sectionBodyRunId`): cross-run store keys rather than in-run step ids, so
// each carries the container run id and is documented separately below.

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
 * Unique for one fixed run id. `loopId` contains no `__` (rejected at
 * definition time in `normalize`) and `index` is decimal digits, so two
 * different `(loopId, index)` pairs under that same run id cannot encode the
 * same string. It is not a unique decoding of every string `RUN_ID_PATTERN`
 * allows: `loopBodyRunId("a_", "_b", 0)` and `loopBodyRunId("a__", "b", 0)`
 * are both `a____b__0`. `RUN_ID_PATTERN` constrains the run id for store-path
 * and mail-address safety.
 */
export function loopBodyRunId(
  runId: string,
  loopId: string,
  index: number,
): string {
  return `${runId}__${loopId}__${String(index)}`;
}

/**
 * Encode an onTrigger section body's run id from the parent run id, the
 * section step id, and the zero-based event index. The body run id is a
 * cross-run store key, so it carries the parent:
 * `<parentRunId>__<sectionId>__<index>`.
 *
 * Unique for one fixed parent run id. `sectionId` contains no `__` (rejected
 * at definition time in `normalize`) and `index` is decimal digits, so two
 * different `(sectionId, index)` pairs under that parent cannot encode the
 * same string. It is not a unique decoding across every parent
 * `RUN_ID_PATTERN` allows.
 */
export function sectionBodyRunId(
  parentRunId: string,
  sectionId: string,
  eventIndex: number,
): string {
  return `${parentRunId}__${sectionId}__${String(eventIndex)}`;
}

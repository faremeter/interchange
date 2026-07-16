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

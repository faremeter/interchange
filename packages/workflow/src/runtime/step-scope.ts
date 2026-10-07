// Single owner of the scoped step-id format for fan-out iterations.
//
// A `map` iteration runs its inner step in place, and a `loop` iteration
// tracks its step state, under a per-index scoped step id
// `<baseStepId>[<index>]`. Base ids never contain brackets
// (`STEP_ID_PATTERN` is `[a-zA-Z0-9_-]+`), so the trailing `[<index>]`
// is an unambiguous scope marker. One owner here instead of hand-rolled
// templates and strip regexes scattered across the runtime and the sidecar.
//
// This module also owns the loop-iteration body run id format
// (`loopBodyRunId`), documented below.

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
 * Encode a loop iteration's body-child run id: `<runId>__<loopId>__<index>`.
 * Unlike the scoped step id (in-run), this is a cross-run store key, so it
 * carries the container run id as an ancestry prefix and re-roots per nesting
 * level. Deterministic -- crash-resume re-derives the same string.
 *
 * Injectivity does not require a `__`-free run id: `loopId` contains no `__`
 * (rejected in `normalize`) and `index` is always digits, so the final two
 * `__` are always the separators and the string decomposes to exactly one
 * (runId, loopId, index). The run id is separately constrained by
 * `RUN_ID_PATTERN` for store-path and mail-address safety.
 */
export function loopBodyRunId(
  runId: string,
  loopId: string,
  index: number,
): string {
  return `${runId}__${loopId}__${String(index)}`;
}

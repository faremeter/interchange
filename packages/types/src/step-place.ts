// A workflow step's place in its deployment. The workflow runtime gives it to
// each step invocation and to the park hooks.

/**
 * Where a step sits in its deployment's run tree. `stepPath` holds base step
 * ids, never scoped `<id>[<index>]` ids, root first: the top-level step id,
 * then the id of each container step (loop, onTrigger, childWorkflow) down to
 * the step itself. A map item's path ends in the map's own id, and
 * `mapIndex` is the item's index. `mapIndex` is absent for a step that is not
 * a map item.
 */
export type StepPlace = { stepPath: readonly string[]; mapIndex?: number };

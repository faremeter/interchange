// Path-selector DSL.
//
// `input`, `reads`, and `writes` on a workflow step are *data*, not
// code. The DAG must be statically inspectable so the deploy-time
// capability-surface walker can compute the grant union without
// executing user code. The four selector shapes below are the entire
// vocabulary.

/**
 * Reference a dot-separated path inside the per-run context.
 *
 * The runtime evaluator (`runtime/selectors.ts`) resolves the path
 * against a root that exposes `trigger.payload` and
 * `steps.<id>.output`. Paths use `.` as the separator; array indices
 * use `[n]` syntax (e.g. `steps.plan.output.tasks[0].title`).
 */
export interface FromSelector {
  from: string;
}

/**
 * Project a subset of fields from another selector's result. Used to
 * shape an upstream step's output into the input the downstream step
 * expects.
 */
export interface ProjectSelector {
  project: Selector;
  fields: readonly string[];
}

/**
 * Merge several selector results into one object. Later entries
 * override earlier ones for overlapping keys.
 */
export interface MergeSelector {
  merge: readonly Selector[];
}

/**
 * Embed a constant value into the input. Used for fan-out parameters
 * the workflow author wants to commit to at definition time.
 */
export interface LiteralSelector {
  literal: unknown;
}

export type Selector =
  | FromSelector
  | ProjectSelector
  | MergeSelector
  | LiteralSelector;

export function isFromSelector(s: Selector): s is FromSelector {
  return "from" in s;
}
export function isProjectSelector(s: Selector): s is ProjectSelector {
  return "project" in s;
}
export function isMergeSelector(s: Selector): s is MergeSelector {
  return "merge" in s;
}
export function isLiteralSelector(s: Selector): s is LiteralSelector {
  return "literal" in s;
}

/**
 * Walk every selector in a tree, calling `visit` on each. Used by the
 * capability-surface walker and by definition-validation passes that
 * confirm the selector tree is statically resolvable.
 */
export function walkSelectors(
  selector: Selector,
  visit: (s: Selector) => void,
): void {
  visit(selector);
  if (isProjectSelector(selector)) {
    walkSelectors(selector.project, visit);
  } else if (isMergeSelector(selector)) {
    for (const inner of selector.merge) {
      walkSelectors(inner, visit);
    }
  }
}

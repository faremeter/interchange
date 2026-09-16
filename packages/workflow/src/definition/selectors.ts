// Path-selector DSL.
//
// A step's `input` is *data*, not code: the four selector shapes below are the
// entire vocabulary a step may use to name where its input comes from, so the
// wiring between steps can be read off the definition without executing any
// author code. Two readers consume it -- the runtime evaluator in
// `runtime/selectors.ts`, which resolves a selector against the per-run
// context, and the definition-validation passes in `workflow.ts`, which walk
// the `input` tree to check that every path it names is statically resolvable.
//
// The `reads` and `writes` fields a step may also carry use the same shapes,
// but nothing reads them: they are projected verbatim onto the wire and
// otherwise inert. In particular the deploy-time capability-surface walker
// never inspects a selector -- it derives the grant union from agent
// capabilities and tools, not from selector paths -- so the vocabulary carries
// no grant guarantee.

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
 * Walk every selector in a tree, calling `visit` on each.
 *
 * Exported from `@intx/workflow` for consumers that need to inspect a selector
 * tree generically. Nothing in this repository calls it: the runtime evaluator
 * and the definition-validation passes each dispatch on the selector shape
 * directly, because both do work per shape rather than one uniform visit.
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

export type PathSegment =
  | { kind: "key"; key: string }
  | { kind: "index"; index: number };

/**
 * Tokenize a `FromSelector` path into key/index segments, the single grammar
 * the DSL uses. The runtime resolver walks these segments; definition-time
 * validators reuse the same tokenizer so their judgment about a path's shape
 * (e.g. whether it is a whole-object read) matches what the resolver does.
 */
export function splitPath(path: string): readonly PathSegment[] {
  const out: PathSegment[] = [];
  const parts = path.split(".");
  for (const part of parts) {
    if (part === "") {
      throw new Error(`empty path segment in ${path}`);
    }
    // Detect inline index syntax: foo[2] -> "foo" then "[2]"
    const match = /^([^[\]]+)((?:\[\d+\])*)$/.exec(part);
    if (!match) {
      throw new Error(`invalid path segment ${part} in ${path}`);
    }
    const keyPart = match[1];
    const indexPart = match[2];
    if (keyPart === undefined) {
      throw new Error(`invalid path segment ${part} in ${path}`);
    }
    out.push({ kind: "key", key: keyPart });
    if (indexPart !== undefined && indexPart !== "") {
      for (const idxMatch of indexPart.matchAll(/\[(\d+)\]/g)) {
        const raw = idxMatch[1];
        if (raw === undefined) continue;
        out.push({ kind: "index", index: Number(raw) });
      }
    }
  }
  return out;
}

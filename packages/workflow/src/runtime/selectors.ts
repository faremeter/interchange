// Runtime evaluator for the path-selector DSL.
//
// Resolves a `Selector` against a `SelectorContext` rooted at the
// run's trigger payload and the captured outputs of completed steps.
// Used by the step executor to materialize a step's `input` and to
// resolve declared `reads` against the run-state subtree.

import {
  isFromSelector,
  isLiteralSelector,
  isMergeSelector,
  isProjectSelector,
  type Selector,
} from "../definition/selectors";

export interface SelectorContext {
  trigger: { payload: unknown };
  steps: Record<string, { output: unknown }>;
}

export class SelectorError extends Error {
  readonly selector: Selector;
  constructor(message: string, selector: Selector) {
    super(message);
    this.name = "SelectorError";
    this.selector = selector;
  }
}

export function evaluate(selector: Selector, ctx: SelectorContext): unknown {
  if (isLiteralSelector(selector)) {
    return selector.literal;
  }
  if (isFromSelector(selector)) {
    return resolvePath(selector.from, ctx, selector);
  }
  if (isProjectSelector(selector)) {
    const source = evaluate(selector.project, ctx);
    if (!isRecord(source)) {
      throw new SelectorError(
        "project selector requires the source to be an object",
        selector,
      );
    }
    const projected: Record<string, unknown> = {};
    for (const field of selector.fields) {
      projected[field] = source[field];
    }
    return projected;
  }
  if (isMergeSelector(selector)) {
    const merged: Record<string, unknown> = {};
    for (const inner of selector.merge) {
      const value = evaluate(inner, ctx);
      if (!isRecord(value)) {
        throw new SelectorError(
          "merge selector requires each operand to be an object",
          selector,
        );
      }
      Object.assign(merged, value);
    }
    return merged;
  }
  throw new SelectorError("unknown selector shape", selector);
}

function resolvePath(
  path: string,
  ctx: SelectorContext,
  selector: Selector,
): unknown {
  if (path === "") {
    throw new SelectorError(
      "from selector requires a non-empty path",
      selector,
    );
  }
  const segments = splitPath(path);
  let cursor: unknown = ctx;
  for (const segment of segments) {
    if (segment.kind === "index") {
      if (!Array.isArray(cursor)) {
        throw new SelectorError(
          `cannot index into non-array at segment [${String(segment.index)}] of ${path}`,
          selector,
        );
      }
      cursor = cursor[segment.index];
    } else {
      if (cursor === null || cursor === undefined) {
        throw new SelectorError(
          `cannot read ${segment.key} from ${cursor === null ? "null" : "undefined"} in path ${path}`,
          selector,
        );
      }
      if (!isRecord(cursor)) {
        throw new SelectorError(
          `cannot read ${segment.key} from non-object in path ${path}`,
          selector,
        );
      }
      // Distinguish a missing key from a key whose value is `null` or
      // `undefined`. `in` checks the key's presence on the object;
      // bracket-indexing alone would silently return `undefined` for
      // a typo and let the runtime feed `undefined` into a step as
      // though no input were supplied. Surface the missing key with a
      // path so the author can spot the typo.
      if (!(segment.key in cursor)) {
        throw new SelectorError(
          `missing key ${segment.key} in path ${path}`,
          selector,
        );
      }
      cursor = cursor[segment.key];
    }
  }
  return cursor;
}

type PathSegment =
  | { kind: "key"; key: string }
  | { kind: "index"; index: number };

function splitPath(path: string): readonly PathSegment[] {
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

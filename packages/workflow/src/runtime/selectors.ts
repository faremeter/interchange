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
  splitPath,
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
    const entries: [string, unknown][] = [];
    for (const field of selector.fields) {
      // Own keys only: `source[field]` reads through the prototype chain, so
      // a field named `toString` resolves on any JSON-derived object.
      if (!Object.hasOwn(source, field)) {
        throw new SelectorError(
          `missing field ${field} in project selector source`,
          selector,
        );
      }
      const value = source[field];
      // `Object.hasOwn` accepts a key whose value is `undefined`, and `fields`
      // claims a value rather than a key. A hole admitted here would win the
      // key in a `merge`, erasing the value an earlier operand supplied.
      if (value === undefined) {
        throw new SelectorError(
          `field ${field} is undefined in project selector source`,
          selector,
        );
      }
      entries.push([field, value]);
    }
    // `Object.fromEntries` defines; assigning onto an accumulator object would
    // hit the inherited `__proto__` setter for a field of that name.
    return Object.fromEntries(entries);
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
      for (const [key, entry] of Object.entries(value)) {
        // Define, never `[[Set]]`: assignment sends an operand's own
        // `__proto__` key to the accumulator's prototype setter. Spread is
        // equally safe and was rejected -- it reads as a cosmetic rewrite of
        // the `Object.assign` this replaces, and invites reverting.
        Object.defineProperty(merged, key, {
          value: entry,
          writable: true,
          enumerable: true,
          configurable: true,
        });
      }
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
      // An out-of-range index silently returning `undefined` would feed a
      // step as though the author had supplied a hole.
      if (segment.index < 0 || segment.index >= cursor.length) {
        throw new SelectorError(
          `index [${String(segment.index)}] out of range (length ${String(cursor.length)}) in path ${path}`,
          selector,
        );
      }
      // Own elements only, matching the key branch below: an index a sparse
      // array leaves unfilled is in range and is not an own property, so it
      // yields the same hole the range guard above refuses.
      if (!Object.hasOwn(cursor, segment.index)) {
        throw new SelectorError(
          `missing index [${String(segment.index)}] in path ${path}`,
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
      // Own keys only: `in` walks the prototype chain, so a path ending in
      // `toString` or `constructor` resolves on any JSON-derived object.
      if (!Object.hasOwn(cursor, segment.key)) {
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

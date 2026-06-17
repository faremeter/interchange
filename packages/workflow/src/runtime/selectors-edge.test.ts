// Selector edge cases: array index out-of-range and trailing-index
// resolution must surface a `SelectorError` rather than silently
// returning `undefined`. The H-R2 fix covered missing keys via the
// `in` check on the key branch; the index branch needs the same
// guard so an out-of-bounds index does not feed `undefined` into a
// step as though no input were supplied.

import { describe, test, expect } from "bun:test";

import { evaluate, SelectorError, type SelectorContext } from "./selectors";

const ctx: SelectorContext = {
  trigger: { payload: { goal: "ship it", tasks: ["a", "b"] } },
  steps: { plan: { output: { items: [{ id: 1 }] } } },
};

describe("selectors edge cases", () => {
  test("missing intermediate key throws", () => {
    expect(() => evaluate({ from: "trigger.payload.nope.deep" }, ctx)).toThrow(
      SelectorError,
    );
  });

  test("array index out of range throws when followed by a key", () => {
    expect(() =>
      evaluate({ from: "steps.plan.output.items[42].id" }, ctx),
    ).toThrow(SelectorError);
  });

  test("array index out of range with no further segments throws", () => {
    expect(() =>
      evaluate({ from: "steps.plan.output.items[42]" }, ctx),
    ).toThrow(SelectorError);
  });

  test("trailing index on a non-array throws", () => {
    expect(() => evaluate({ from: "trigger.payload.goal[0]" }, ctx)).toThrow(
      SelectorError,
    );
  });
});

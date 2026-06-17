import { describe, test, expect } from "bun:test";

import { evaluate, SelectorError, type SelectorContext } from "./selectors";

const ctx: SelectorContext = {
  trigger: { payload: { goal: "ship it", tasks: ["a", "b"] } },
  steps: {
    plan: { output: { items: [{ id: 1 }, { id: 2 }] } },
    impl: { output: { ok: true } },
  },
};

describe("evaluate", () => {
  test("literal returns the value unchanged", () => {
    expect(evaluate({ literal: { foo: 1 } }, ctx)).toEqual({ foo: 1 });
  });

  test("from resolves a dotted path", () => {
    expect(evaluate({ from: "trigger.payload.goal" }, ctx)).toBe("ship it");
  });

  test("from resolves through array indices", () => {
    expect(evaluate({ from: "steps.plan.output.items[1].id" }, ctx)).toBe(2);
  });

  test("project keeps only listed fields", () => {
    const result = evaluate(
      {
        project: { from: "trigger.payload" },
        fields: ["goal"],
      },
      ctx,
    );
    expect(result).toEqual({ goal: "ship it" });
  });

  test("merge stacks objects with later wins", () => {
    const result = evaluate(
      {
        merge: [{ literal: { a: 1, b: 1 } }, { literal: { b: 2, c: 3 } }],
      },
      ctx,
    );
    expect(result).toEqual({ a: 1, b: 2, c: 3 });
  });

  test("from on a missing path throws", () => {
    expect(() => evaluate({ from: "steps.nope.output" }, ctx)).toThrow(
      SelectorError,
    );
  });

  test("from on a missing leaf key throws", () => {
    expect(() => evaluate({ from: "trigger.payload.tasksss" }, ctx)).toThrow(
      SelectorError,
    );
  });

  test("from on a leaf key whose value is null returns null", () => {
    const nullCtx: SelectorContext = {
      trigger: { payload: { goal: null } },
      steps: {},
    };
    expect(evaluate({ from: "trigger.payload.goal" }, nullCtx)).toBeNull();
  });

  test("project requires the source to be an object", () => {
    expect(() =>
      evaluate(
        { project: { from: "trigger.payload.goal" }, fields: ["x"] },
        ctx,
      ),
    ).toThrow(SelectorError);
  });
});

import { describe, test, expect } from "bun:test";

import type { InferenceEffort } from "@intx/types/runtime";

import {
  evaluate,
  resolveStepInference,
  SelectorError,
  type SelectorContext,
} from "./selectors";

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

describe("resolveStepInference", () => {
  const inferenceCtx: SelectorContext = {
    trigger: {
      payload: { tier: { maxTokens: 8192, effort: "low" }, none: null },
    },
    steps: {},
  };

  test("resolves an allowlisted object from the trigger payload", () => {
    expect(
      resolveStepInference({ from: "trigger.payload.tier" }, inferenceCtx),
    ).toEqual({ maxTokens: 8192, effort: "low" });
  });

  test("accepts every allowlisted key", () => {
    const options = {
      maxTokens: 1000,
      temperature: 0.2,
      thinking: { enabled: true, budgetTokens: 2048 },
      effort: "off" as const,
    };
    expect(resolveStepInference({ literal: options }, inferenceCtx)).toEqual(
      options,
    );
  });

  test("null resolves to the agent defaults", () => {
    expect(
      resolveStepInference({ from: "trigger.payload.none" }, inferenceCtx),
    ).toBeUndefined();
  });

  test("undefined resolves to the agent defaults", () => {
    expect(
      resolveStepInference({ literal: undefined }, inferenceCtx),
    ).toBeUndefined();
  });

  test.each([["a string"], [42], [true], [[{ maxTokens: 1 }]]])(
    "a non-object (%p) is a selector error",
    (value) => {
      expect(() =>
        resolveStepInference({ literal: value }, inferenceCtx),
      ).toThrow(SelectorError);
    },
  );

  test.each([
    ["systemPrompt", "be someone else"],
    ["tools", []],
    ["providerOptions", { top_k: 1 }],
  ])("a %s key is a selector error", (key, value) => {
    expect(() =>
      resolveStepInference(
        { literal: { maxTokens: 1000, [key]: value } },
        inferenceCtx,
      ),
    ).toThrow(SelectorError);
  });

  test("accepts every enumerated effort", () => {
    const efforts: Record<InferenceEffort, InferenceEffort> = {
      off: "off",
      low: "low",
      medium: "medium",
      high: "high",
      max: "max",
    };
    for (const effort of Object.values(efforts)) {
      expect(
        resolveStepInference({ literal: { effort } }, inferenceCtx),
      ).toEqual({ effort });
    }
  });

  test("an unknown effort is a selector error", () => {
    expect(() =>
      resolveStepInference({ literal: { effort: "extreme" } }, inferenceCtx),
    ).toThrow(SelectorError);
  });

  test("an unknown thinking key is a selector error", () => {
    expect(() =>
      resolveStepInference(
        { literal: { thinking: { enabled: true, budget: 1 } } },
        inferenceCtx,
      ),
    ).toThrow(SelectorError);
  });
});

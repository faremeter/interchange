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

function assertRecord(
  value: unknown,
): asserts value is Record<string, unknown> {
  if (typeof value !== "object" || value === null) {
    throw new Error(`expected an object result, got ${typeof value}`);
  }
}

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

  test("from on an inherited member throws", () => {
    // Every payload arrives through `JSON.parse`, so it carries
    // `Object.prototype`; a path segment naming one of its members is a typo,
    // not a read.
    const payload: unknown = JSON.parse('{"goal":"ship it"}');
    const protoCtx: SelectorContext = {
      trigger: { payload },
      steps: {},
    };
    for (const member of [
      "toString",
      "constructor",
      "hasOwnProperty",
      "__proto__",
    ]) {
      expect(() =>
        evaluate({ from: `trigger.payload.${member}` }, protoCtx),
      ).toThrow(SelectorError);
    }
    expect(evaluate({ from: "trigger.payload.goal" }, protoCtx)).toBe(
      "ship it",
    );
  });

  test("from resolves an own key that shadows an inherited member", () => {
    // `__proto__` is a legal mail header field name (RFC 5322 section 3.6.8),
    // and `JSON.parse` keeps it as an own data property.
    const payload: unknown = JSON.parse(
      '{"rawHeaders":{"__proto__":["injected"],"subject":["hi"]}}',
    );
    const ownCtx: SelectorContext = { trigger: { payload }, steps: {} };
    expect(
      evaluate({ from: "trigger.payload.rawHeaders.__proto__" }, ownCtx),
    ).toEqual(["injected"]);
    expect(
      evaluate({ from: "trigger.payload.rawHeaders.subject" }, ownCtx),
    ).toEqual(["hi"]);
  });

  test("project requires the source to be an object", () => {
    expect(() =>
      evaluate(
        { project: { from: "trigger.payload.goal" }, fields: ["x"] },
        ctx,
      ),
    ).toThrow(SelectorError);
  });

  test("project on an inherited member throws", () => {
    const payload: unknown = JSON.parse('{"goal":"ship it"}');
    const protoCtx: SelectorContext = { trigger: { payload }, steps: {} };
    for (const member of ["toString", "constructor", "hasOwnProperty"]) {
      expect(() =>
        evaluate(
          { project: { from: "trigger.payload" }, fields: [member] },
          protoCtx,
        ),
      ).toThrow(SelectorError);
    }
  });

  test("project on an absent own field throws", () => {
    expect(() =>
      evaluate(
        { project: { from: "trigger.payload" }, fields: ["goal", "nope"] },
        ctx,
      ),
    ).toThrow(SelectorError);
  });

  test("project defines an own key that shadows an inherited member", () => {
    const payload: unknown = JSON.parse(
      '{"rawHeaders":{"__proto__":["injected"],"subject":["hi"]}}',
    );
    const ownCtx: SelectorContext = { trigger: { payload }, steps: {} };
    const result = evaluate(
      {
        project: { from: "trigger.payload.rawHeaders" },
        fields: ["__proto__", "subject"],
      },
      ownCtx,
    );
    assertRecord(result);
    expect(Object.keys(result)).toEqual(["__proto__", "subject"]);
    expect(Object.getPrototypeOf(result)).toBe(Object.prototype);
    expect(Object.getOwnPropertyDescriptor(result, "__proto__")).toEqual({
      value: ["injected"],
      writable: true,
      enumerable: true,
      configurable: true,
    });
  });

  test("merge defines an operand key that names an inherited member", () => {
    const payload: unknown = JSON.parse(
      '{"rawHeaders":{"__proto__":["injected"]}}',
    );
    const ownCtx: SelectorContext = { trigger: { payload }, steps: {} };
    const result = evaluate(
      {
        merge: [{ literal: { a: 1 } }, { from: "trigger.payload.rawHeaders" }],
      },
      ownCtx,
    );
    assertRecord(result);
    expect(Object.keys(result)).toEqual(["a", "__proto__"]);
    expect(Object.getPrototypeOf(result)).toBe(Object.prototype);
  });

  test("merge cannot clobber an earlier operand with a missing field", () => {
    expect(() =>
      evaluate(
        {
          merge: [
            { literal: { goal: "keep me" } },
            { project: { from: "steps.impl.output" }, fields: ["goal"] },
          ],
        },
        ctx,
      ),
    ).toThrow(SelectorError);
  });

  test("project on an own field whose value is undefined throws", () => {
    const holeCtx: SelectorContext = {
      trigger: { payload: { goal: undefined } },
      steps: {},
    };
    expect(() =>
      evaluate(
        { project: { from: "trigger.payload" }, fields: ["goal"] },
        holeCtx,
      ),
    ).toThrow(SelectorError);
  });

  test("merge cannot clobber an earlier operand with an own undefined field", () => {
    // The projection is the later operand, so a hole it yielded would win the
    // overlapping key and erase the earlier operand's value instead of failing.
    const holeCtx: SelectorContext = {
      trigger: { payload: { goal: undefined } },
      steps: {},
    };
    expect(() =>
      evaluate(
        {
          merge: [
            { literal: { goal: "keep me" } },
            { project: { from: "trigger.payload" }, fields: ["goal"] },
          ],
        },
        holeCtx,
      ),
    ).toThrow(SelectorError);
  });

  test("from on an in-range index a sparse array leaves unfilled throws", () => {
    // eslint-disable-next-line no-sparse-arrays -- the hole is the subject
    const items = [, "b"];
    const sparseCtx: SelectorContext = {
      trigger: { payload: { items } },
      steps: {},
    };
    expect(Object.hasOwn(items, 0)).toBe(false);
    expect(() =>
      evaluate({ from: "trigger.payload.items[0]" }, sparseCtx),
    ).toThrow(SelectorError);
    expect(evaluate({ from: "trigger.payload.items[1]" }, sparseCtx)).toBe("b");
  });

  test("from resolves an own member whose value is undefined", () => {
    // The index guard refuses an absent own property, not a stored value, so a
    // filled element holding `undefined` resolves exactly as the key branch
    // resolves an own key holding one. `runStep` canonicalizes a step input of
    // `undefined` to `null` for both the audit blob and the invoker, which is
    // why `from` admits the hole that `project` refuses.
    const holeCtx: SelectorContext = {
      trigger: { payload: { goal: undefined, items: [undefined] } },
      steps: {},
    };
    expect(evaluate({ from: "trigger.payload.goal" }, holeCtx)).toBeUndefined();
    expect(
      evaluate({ from: "trigger.payload.items[0]" }, holeCtx),
    ).toBeUndefined();
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
      effort: "high" as const,
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

  test.each([["extreme"], ["off"]])(
    "an unknown effort (%p) is a selector error",
    (effort) => {
      expect(() =>
        resolveStepInference({ literal: { effort } }, inferenceCtx),
      ).toThrow(SelectorError);
    },
  );

  test("an unknown thinking key is a selector error", () => {
    expect(() =>
      resolveStepInference(
        { literal: { thinking: { enabled: true, budget: 1 } } },
        inferenceCtx,
      ),
    ).toThrow(SelectorError);
  });
});

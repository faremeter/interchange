import { describe, test, expect } from "bun:test";

import { evaluateConditions, validateConditions } from "./conditions";
import { timeWindowEvaluator } from "./time-window";
import type {
  ConditionContext,
  ConditionEvaluator,
  ConditionRegistry,
} from "./types";

function evaluator(
  evaluate: ConditionEvaluator["evaluate"],
): ConditionEvaluator {
  return { validate: () => null, evaluate };
}

const PROTOTYPE_KEYS = ["constructor", "toString", "__proto__"];

// An own enumerable key, as a JSON request body carries it. An object
// literal cannot make "__proto__" an own key.
function ownKey(key: string): Record<string, unknown> {
  const conditions: Record<string, unknown> = {};
  Object.defineProperty(conditions, key, { value: 1, enumerable: true });
  return conditions;
}

function ctx(overrides?: Partial<ConditionContext>): ConditionContext {
  return {
    now: new Date("2025-06-15T14:30:00Z"),
    resource: "agent:agt_abc",
    action: "read",
    principalId: "prn_1",
    tenantId: "tnt_1",
    consumer: "",
    ...overrides,
  };
}

describe("evaluateConditions", () => {
  test("null conditions are always met", async () => {
    expect(await evaluateConditions(null, ctx())).toBe(true);
  });

  test("empty object conditions are always met", async () => {
    expect(await evaluateConditions({}, ctx())).toBe(true);
  });

  test("unknown condition key throws an error", async () => {
    expect(
      evaluateConditions({ bogus_condition: true }, ctx(), {}),
    ).rejects.toThrow('Unknown condition: "bogus_condition"');
  });

  test("single condition evaluated against registry", async () => {
    const registry: ConditionRegistry = {
      is_tuesday: evaluator(() => true),
    };

    expect(
      await evaluateConditions({ is_tuesday: null }, ctx(), registry),
    ).toBe(true);
  });

  test("condition returning false rejects the grant", async () => {
    const registry: ConditionRegistry = {
      always_fail: evaluator(() => false),
    };

    expect(
      await evaluateConditions({ always_fail: null }, ctx(), registry),
    ).toBe(false);
  });

  test("all conditions must pass (AND semantics)", async () => {
    const registry: ConditionRegistry = {
      cond_a: evaluator(() => true),
      cond_b: evaluator(() => true),
      cond_c: evaluator(() => false),
    };

    expect(
      await evaluateConditions(
        { cond_a: null, cond_b: null, cond_c: null },
        ctx(),
        registry,
      ),
    ).toBe(false);
  });

  test("all conditions passing returns true", async () => {
    const registry: ConditionRegistry = {
      cond_a: evaluator(() => true),
      cond_b: evaluator(() => true),
    };

    expect(
      await evaluateConditions({ cond_a: null, cond_b: null }, ctx(), registry),
    ).toBe(true);
  });

  test("evaluator receives the condition value", async () => {
    let receivedValue: unknown;
    const registry: ConditionRegistry = {
      threshold: evaluator((value) => {
        receivedValue = value;
        return true;
      }),
    };

    await evaluateConditions({ threshold: 42 }, ctx(), registry);

    expect(receivedValue).toBe(42);
  });

  test("evaluator receives the full context", async () => {
    let receivedCtx: ConditionContext | undefined;
    const registry: ConditionRegistry = {
      spy: evaluator((_value, c) => {
        receivedCtx = c;
        return true;
      }),
    };

    const c = ctx({ principalId: "prn_spy", tenantId: "tnt_spy" });
    await evaluateConditions({ spy: null }, c, registry);

    expect(receivedCtx?.principalId).toBe("prn_spy");
    expect(receivedCtx?.tenantId).toBe("tnt_spy");
    expect(receivedCtx?.resource).toBe("agent:agt_abc");
    expect(receivedCtx?.action).toBe("read");
  });

  test("async evaluators are supported", async () => {
    const registry: ConditionRegistry = {
      async_check: evaluator(async () => {
        await new Promise((r) => setTimeout(r, 1));
        return true;
      }),
    };

    expect(
      await evaluateConditions({ async_check: null }, ctx(), registry),
    ).toBe(true);
  });

  test("async evaluator returning false rejects", async () => {
    const registry: ConditionRegistry = {
      async_fail: evaluator(async () => {
        await new Promise((r) => setTimeout(r, 1));
        return false;
      }),
    };

    expect(
      await evaluateConditions({ async_fail: null }, ctx(), registry),
    ).toBe(false);
  });

  test("short-circuits on first failing condition", async () => {
    let secondCalled = false;
    const registry: ConditionRegistry = {
      first: evaluator(() => false),
      second: evaluator(() => {
        secondCalled = true;
        return true;
      }),
    };

    await evaluateConditions({ first: null, second: null }, ctx(), registry);

    expect(secondCalled).toBe(false);
  });

  test("a key that names an Object prototype property is unknown", async () => {
    for (const key of PROTOTYPE_KEYS) {
      expect(evaluateConditions(ownKey(key), ctx(), {})).rejects.toThrow(
        `Unknown condition: "${key}"`,
      );
    }
  });

  test("no registry provided with non-null conditions throws", async () => {
    // No registry argument at all -- defaults to empty registry
    expect(evaluateConditions({ some_condition: true }, ctx())).rejects.toThrow(
      'Unknown condition: "some_condition"',
    );
  });
});

describe("validateConditions", () => {
  function rejecting(problem: string): ConditionEvaluator {
    return { validate: () => problem, evaluate: () => true };
  }

  test("null and empty conditions have no problems", () => {
    expect(validateConditions(null, {})).toEqual([]);
    expect(validateConditions({}, {})).toEqual([]);
  });

  test("entries every evaluator accepts have no problems", () => {
    const registry: ConditionRegistry = {
      cond_a: evaluator(() => true),
      cond_b: evaluator(() => false),
    };
    expect(validateConditions({ cond_a: 1, cond_b: 2 }, registry)).toEqual([]);
  });

  test("an unregistered key is named", () => {
    expect(validateConditions({ bogus: true }, {})).toEqual([
      'Unknown condition: "bogus"',
    ]);
  });

  test("a key that names an Object prototype property is unknown", () => {
    for (const key of PROTOTYPE_KEYS) {
      expect(validateConditions(ownKey(key), {})).toEqual([
        `Unknown condition: "${key}"`,
      ]);
    }
  });

  test("an evaluator's problem passes through unchanged", () => {
    const registry: ConditionRegistry = {
      limit: rejecting("limit: max must be a number"),
    };
    expect(validateConditions({ limit: {} }, registry)).toEqual([
      "limit: max must be a number",
    ]);
  });

  test("the evaluator receives the condition value", () => {
    let received: unknown;
    const registry: ConditionRegistry = {
      limit: {
        validate: (value) => {
          received = value;
          return null;
        },
        evaluate: () => true,
      },
    };
    validateConditions({ limit: 42 }, registry);
    expect(received).toBe(42);
  });

  test("every bad entry is reported in key order", () => {
    const registry: ConditionRegistry = {
      ok: evaluator(() => true),
      bad: rejecting("bad: rejected"),
    };
    expect(validateConditions({ unknown: 1, ok: 2, bad: 3 }, registry)).toEqual(
      ['Unknown condition: "unknown"', "bad: rejected"],
    );
  });

  test("validation does not evaluate", () => {
    let evaluated = false;
    const registry: ConditionRegistry = {
      spy: evaluator(() => {
        evaluated = true;
        return true;
      }),
    };
    validateConditions({ spy: null }, registry);
    expect(evaluated).toBe(false);
  });

  test("reports a time_window body that is missing its timezone", () => {
    expect(
      validateConditions(
        { time_window: { after: "09:00", before: "17:00" } },
        { time_window: timeWindowEvaluator },
      ),
    ).toEqual(["time_window: timezone must be a string (was missing)"]);
  });
});

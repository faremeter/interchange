import { describe, test, expect } from "bun:test";

import { evaluateConditions } from "./conditions";
import type { ConditionContext, ConditionRegistry } from "./types";

function ctx(overrides?: Partial<ConditionContext>): ConditionContext {
  return {
    now: new Date("2025-06-15T14:30:00Z"),
    resource: "agent:agt_abc",
    action: "read",
    principalId: "prn_1",
    tenantId: "tnt_1",
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
      is_tuesday: () => true,
    };

    expect(
      await evaluateConditions({ is_tuesday: null }, ctx(), registry),
    ).toBe(true);
  });

  test("condition returning false rejects the grant", async () => {
    const registry: ConditionRegistry = {
      always_fail: () => false,
    };

    expect(
      await evaluateConditions({ always_fail: null }, ctx(), registry),
    ).toBe(false);
  });

  test("all conditions must pass (AND semantics)", async () => {
    const registry: ConditionRegistry = {
      cond_a: () => true,
      cond_b: () => true,
      cond_c: () => false,
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
      cond_a: () => true,
      cond_b: () => true,
    };

    expect(
      await evaluateConditions({ cond_a: null, cond_b: null }, ctx(), registry),
    ).toBe(true);
  });

  test("evaluator receives the condition value", async () => {
    let receivedValue: unknown;
    const registry: ConditionRegistry = {
      threshold: (value) => {
        receivedValue = value;
        return true;
      },
    };

    await evaluateConditions({ threshold: 42 }, ctx(), registry);

    expect(receivedValue).toBe(42);
  });

  test("evaluator receives the full context", async () => {
    let receivedCtx: ConditionContext | undefined;
    const registry: ConditionRegistry = {
      spy: (_value, c) => {
        receivedCtx = c;
        return true;
      },
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
      async_check: async () => {
        await new Promise((r) => setTimeout(r, 1));
        return true;
      },
    };

    expect(
      await evaluateConditions({ async_check: null }, ctx(), registry),
    ).toBe(true);
  });

  test("async evaluator returning false rejects", async () => {
    const registry: ConditionRegistry = {
      async_fail: async () => {
        await new Promise((r) => setTimeout(r, 1));
        return false;
      },
    };

    expect(
      await evaluateConditions({ async_fail: null }, ctx(), registry),
    ).toBe(false);
  });

  test("short-circuits on first failing condition", async () => {
    let secondCalled = false;
    const registry: ConditionRegistry = {
      first: () => false,
      second: () => {
        secondCalled = true;
        return true;
      },
    };

    await evaluateConditions({ first: null, second: null }, ctx(), registry);

    expect(secondCalled).toBe(false);
  });

  test("no registry provided with non-null conditions throws", async () => {
    // No registry argument at all -- defaults to empty registry
    expect(evaluateConditions({ some_condition: true }, ctx())).rejects.toThrow(
      'Unknown condition: "some_condition"',
    );
  });
});

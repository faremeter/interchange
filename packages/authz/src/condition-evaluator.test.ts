import { describe, test, expect } from "bun:test";
import { type } from "arktype";

import { createConditionEvaluator } from "./condition-evaluator";
import type { ConditionContext } from "./types";

const ctx: ConditionContext = {
  now: new Date("2026-06-15T14:30:00Z"),
  resource: "agent:agt_abc",
  action: "read",
  principalId: "prn_1",
  tenantId: "tnt_1",
  consumer: "",
};

describe("createConditionEvaluator", () => {
  const Limit = type({ max: "number" }).pipe(({ max }) => max * 2);

  test("validate returns null for a value the schema accepts", () => {
    const evaluator = createConditionEvaluator("limit", Limit, () => true);
    expect(evaluator.validate({ max: 1 })).toBeNull();
  });

  test("validate names the condition and joins problems on one line", () => {
    const evaluator = createConditionEvaluator(
      "limit",
      type({ "+": "reject", max: "number" }),
      () => true,
    );
    expect(evaluator.validate({ cap: 1 })).toBe(
      "limit: max must be a number (was missing); cap must be removed",
    );
  });

  test("evaluate passes the parsed value to the callback", async () => {
    let received: unknown;
    const evaluator = createConditionEvaluator("limit", Limit, (value) => {
      received = value;
      return true;
    });
    expect(await evaluator.evaluate({ max: 3 }, ctx)).toBe(true);
    expect(received).toBe(6);
  });

  test("evaluate throws the validate message without calling back", () => {
    let called = false;
    const evaluator = createConditionEvaluator("limit", Limit, () => {
      called = true;
      return true;
    });
    expect(() => evaluator.evaluate({ max: "x" }, ctx)).toThrow(
      "limit: max must be a number (was a string)",
    );
    expect(called).toBe(false);
  });
});

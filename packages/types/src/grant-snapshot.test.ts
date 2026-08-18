import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import { GrantWalkSnapshot } from "./grant-snapshot";

function representativeSnapshot() {
  return {
    perStep: [
      {
        stepId: "root",
        grants: [
          "tool:bash",
          "director:default",
          "inference.source:anthropic:claude",
          "mail.address:agent@example.com",
        ],
        grantEffects: {
          "tool:bash": "ask",
          "tool:read": "allow",
        },
      },
      {
        stepId: "child",
        grants: [],
        grantEffects: {},
      },
    ],
    grantRequirements: [
      {
        resource: "tool:bash",
        action: "invoke",
        source: "creator",
      },
      {
        resource: "credential:crd_stripe",
        action: "use",
        effect: "ask",
        source: "invoker",
        conditions: { max_spend_per_day: 100 },
      },
    ],
  };
}

describe("GrantWalkSnapshot validator", () => {
  test("accepts a representative snapshot after a JSON round-trip", () => {
    const roundTripped: unknown = JSON.parse(
      JSON.stringify(representativeSnapshot()),
    );
    const result = GrantWalkSnapshot(roundTripped);
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts an empty walk", () => {
    const result = GrantWalkSnapshot({ perStep: [], grantRequirements: [] });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects an unknown grant effect", () => {
    const result = GrantWalkSnapshot({
      perStep: [
        {
          stepId: "root",
          grants: ["tool:bash"],
          grantEffects: { "tool:bash": "maybe" },
        },
      ],
      grantRequirements: [],
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a non-string grant entry", () => {
    const result = GrantWalkSnapshot({
      perStep: [{ stepId: "root", grants: [42], grantEffects: {} }],
      grantRequirements: [],
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a malformed grant requirement", () => {
    const result = GrantWalkSnapshot({
      perStep: [],
      grantRequirements: [
        { resource: "tool:bash", action: "invoke", source: "system" },
      ],
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a missing perStep field", () => {
    const result = GrantWalkSnapshot({ grantRequirements: [] });
    expect(result instanceof type.errors).toBe(true);
  });
});

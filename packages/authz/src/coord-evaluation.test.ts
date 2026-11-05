import { describe, test, expect } from "bun:test";

import { evaluateCoordinateGrants } from "./coord-evaluation";
import { coordTypeTier, defineCoordGrammar, type Coordinate } from "./coord";
import { grantSpecificity } from "./specificity";
import type { GrantRule } from "./types";

function grant(
  resource: string,
  effect: GrantRule["effect"],
  action = "use",
): GrantRule {
  return {
    id: `grt_${Math.random().toString(36).slice(2, 10)}`,
    resource,
    action,
    effect,
    origin: "role",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: null,
  };
}

const LOW_HIGH = defineCoordGrammar({
  namespace: "test.ns",
  action: "use",
  coordTypes: ["low", "high"],
});

const HIGH_LOW = defineCoordGrammar({
  namespace: "test.ns",
  action: "use",
  coordTypes: ["high", "low"],
});

// The low coordinate has the longer id, so pattern specificity alone would
// pick it; a high-tier winner proves the tier ordering.
const COORDS: Coordinate<"low" | "high">[] = [
  { coordType: "low", id: "low_target_with_a_longer_id" },
  { coordType: "high", id: "hi" },
];

describe("evaluateCoordinateGrants", () => {
  test("reads the tier order from the grammar", async () => {
    const grants = [
      grant("test.ns:low:low_target_with_a_longer_id", "deny"),
      grant("test.ns:high:hi", "allow"),
    ];

    const lowFirst = await evaluateCoordinateGrants({
      grammar: LOW_HIGH,
      coordinates: COORDS,
      grants,
      registry: undefined,
    });
    const highFirst = await evaluateCoordinateGrants({
      grammar: HIGH_LOW,
      coordinates: COORDS,
      grants,
      registry: undefined,
    });

    expect(lowFirst?.effect).toBe("allow");
    expect(highFirst?.effect).toBe("deny");
  });

  test("ignores grants outside the grammar namespace", async () => {
    const winner = await evaluateCoordinateGrants({
      grammar: LOW_HIGH,
      coordinates: COORDS,
      grants: [grant("other.ns:high:hi", "allow"), grant("*", "allow", "*")],
      registry: undefined,
    });

    expect(winner).toBeNull();
  });

  test("ranks a wildcard type segment below a named type", async () => {
    // Equal specificity: only the tier rule can pick the named-type grant.
    expect(grantSpecificity("test.ns:high*", "use")).toBe(
      grantSpecificity("test.ns:low:*", "use"),
    );

    const winner = await evaluateCoordinateGrants({
      grammar: LOW_HIGH,
      coordinates: COORDS,
      grants: [grant("test.ns:high*", "deny"), grant("test.ns:low:*", "allow")],
      registry: undefined,
    });

    expect(winner?.resource).toBe("test.ns:low:*");
    expect(winner?.effect).toBe("allow");
  });
});

describe("coord grammar", () => {
  test("gives tier 0 to a type segment that is not a coord type", () => {
    expect(coordTypeTier(LOW_HIGH, "constructor")).toBe(0);
    expect(coordTypeTier(LOW_HIGH, "toString")).toBe(0);
    expect(coordTypeTier(LOW_HIGH, "*")).toBe(0);
    expect(coordTypeTier(LOW_HIGH, "low")).toBe(1);
    expect(coordTypeTier(LOW_HIGH, "high")).toBe(2);
  });

  test("rejects a namespace with a colon and duplicate coord types", () => {
    expect(() =>
      defineCoordGrammar({ namespace: "a:b", action: "use", coordTypes: [] }),
    ).toThrow();
    expect(() =>
      defineCoordGrammar({
        namespace: "test.ns",
        action: "use",
        coordTypes: ["low", "low"],
      }),
    ).toThrow();
  });
});

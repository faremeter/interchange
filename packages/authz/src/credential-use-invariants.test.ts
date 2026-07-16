import { describe, test, expect } from "bun:test";

import { evaluateGrants } from "./evaluate";
import { matchPattern } from "./patterns";
import type { GrantRule } from "./types";

const CRED = "credential:crd_abc123";
const USE = "use";

function grant(
  overrides: Partial<GrantRule> &
    Pick<GrantRule, "resource" | "action" | "effect">,
): GrantRule {
  return {
    id: `grt_${Math.random().toString(36).slice(2, 10)}`,
    origin: "system",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: null,
    ...overrides,
  };
}

describe("wildcard matcher reaches credential:{id}/use", () => {
  test("matchPattern('*', ...) matches both resource and action", () => {
    expect(matchPattern("*", CRED)).toBe(true);
    expect(matchPattern("*", USE)).toBe(true);
  });

  test("owner */* grant resolves to allow for credential:{id}/use", async () => {
    const result = await evaluateGrants(
      [grant({ resource: "*", action: "*", effect: "allow" })],
      CRED,
      USE,
    );
    expect(result.effect).toBe("allow");
  });

  test("credential:* / use also resolves to allow", async () => {
    const result = await evaluateGrants(
      [grant({ resource: "credential:*", action: "use", effect: "allow" })],
      CRED,
      USE,
    );
    expect(result.effect).toBe("allow");
  });
});

describe("admin/member default grants lack use on credentials", () => {
  test("admin read/create/manage do NOT satisfy use", async () => {
    const adminGrants = [
      grant({ resource: "*", action: "read", effect: "allow" }),
      grant({ resource: "*", action: "create", effect: "allow" }),
      grant({ resource: "*", action: "manage", effect: "allow" }),
    ];
    const result = await evaluateGrants(adminGrants, CRED, USE);
    expect(result.effect).toBe(null);
    expect(result.effect === "allow").toBe(false);
  });

  test("member read does NOT satisfy use", async () => {
    const result = await evaluateGrants(
      [grant({ resource: "*", action: "read", effect: "allow" })],
      CRED,
      USE,
    );
    expect(result.effect).toBe(null);
  });
});

describe("fail-closed: anything other than allow is denial", () => {
  test("no matching grant resolves to effect null", async () => {
    const result = await evaluateGrants([], CRED, USE);
    expect(result.effect).toBe(null);
    expect(result.effect !== "allow").toBe(true);
  });

  test("ask effect is not allow", async () => {
    const result = await evaluateGrants(
      [grant({ resource: "credential:*", action: "use", effect: "ask" })],
      CRED,
      USE,
    );
    expect(result.effect).toBe("ask");
    expect(result.effect !== "allow").toBe(true);
  });

  test("deny at equal specificity beats allow", async () => {
    const allow = grant({
      resource: "credential:*",
      action: "use",
      effect: "allow",
    });
    const deny = grant({
      resource: "credential:*",
      action: "use",
      effect: "deny",
    });
    const result = await evaluateGrants([allow, deny], CRED, USE);
    expect(result.effect).toBe("deny");
    expect(result.effect !== "allow").toBe(true);
  });

  test("conditioned grant with no registry is skipped (fail closed)", async () => {
    const result = await evaluateGrants(
      [
        grant({
          resource: "credential:*",
          action: "use",
          effect: "allow",
          conditions: { some_condition: true },
        }),
      ],
      CRED,
      USE,
    );
    expect(result.effect).toBe(null);
  });
});

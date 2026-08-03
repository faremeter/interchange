import { describe, test, expect } from "bun:test";

import { authorizeAction } from "./authorize-action";
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

describe("authorizeAction collapses the fail-closed rule", () => {
  test("an allow grant authorizes", async () => {
    const decision = await authorizeAction(
      [grant({ resource: CRED, action: USE, effect: "allow" })],
      CRED,
      USE,
    );
    expect(decision).toEqual({ ok: true });
  });

  test("a deny grant withholds with reason deny", async () => {
    const decision = await authorizeAction(
      [grant({ resource: CRED, action: USE, effect: "deny" })],
      CRED,
      USE,
    );
    expect(decision).toEqual({ ok: false, reason: "deny" });
  });

  test("an ask grant withholds with reason ask", async () => {
    const decision = await authorizeAction(
      [grant({ resource: CRED, action: USE, effect: "ask" })],
      CRED,
      USE,
    );
    expect(decision).toEqual({ ok: false, reason: "ask" });
  });

  test("no matching grant withholds with reason no_matching_grant", async () => {
    const decision = await authorizeAction([], CRED, USE);
    expect(decision).toEqual({ ok: false, reason: "no_matching_grant" });
  });

  test("a conditioned grant is withheld when no registry is passed", async () => {
    // The verb never defaults a registry; evaluateGrants skips conditioned
    // grants fail-closed, so this resolves to no matching grant.
    const decision = await authorizeAction(
      [
        grant({
          resource: CRED,
          action: USE,
          effect: "allow",
          conditions: { tool: "builtin:reactor" },
        }),
      ],
      CRED,
      USE,
    );
    expect(decision).toEqual({ ok: false, reason: "no_matching_grant" });
  });
});

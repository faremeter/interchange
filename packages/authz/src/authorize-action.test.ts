import { describe, test, expect } from "bun:test";

import { authorizeAction, authorizeActionUntil } from "./authorize-action";
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
      { conditions: "pessimistic" },
    );
    expect(decision).toEqual({ ok: true });
  });

  test("a deny grant withholds with reason deny", async () => {
    const decision = await authorizeAction(
      [grant({ resource: CRED, action: USE, effect: "deny" })],
      CRED,
      USE,
      { conditions: "pessimistic" },
    );
    expect(decision).toEqual({ ok: false, reason: "deny" });
  });

  test("an ask grant withholds with reason ask", async () => {
    const decision = await authorizeAction(
      [grant({ resource: CRED, action: USE, effect: "ask" })],
      CRED,
      USE,
      { conditions: "pessimistic" },
    );
    expect(decision).toEqual({ ok: false, reason: "ask" });
  });

  test("no matching grant withholds with reason no_matching_grant", async () => {
    const decision = await authorizeAction([], CRED, USE, {
      conditions: "pessimistic",
    });
    expect(decision).toEqual({ ok: false, reason: "no_matching_grant" });
  });

  test("a conditioned allow is withheld in pessimistic mode", async () => {
    // The verb passes the caller's mode through; pessimistic mode never
    // applies a conditioned allow, so this resolves to no matching grant.
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
      { conditions: "pessimistic" },
    );
    expect(decision).toEqual({ ok: false, reason: "no_matching_grant" });
  });
});

describe("authorizeActionUntil", () => {
  const PESSIMISTIC = { conditions: "pessimistic" } as const;
  const start = Date.now();
  const hour = (n: number) => new Date(start + n * 3_600_000);
  const allow = (expiresAt: Date | null, resource = CRED) =>
    grant({ resource, action: USE, effect: "allow", expiresAt });

  test("an allow from one expiring grant ends at that grant's expiry", async () => {
    const decision = await authorizeActionUntil(
      [allow(hour(2))],
      CRED,
      USE,
      PESSIMISTIC,
    );
    expect(decision).toEqual({ ok: true, expiresAt: hour(2) });
  });

  test("two covering grants: the allow ends at the later expiry", async () => {
    const decision = await authorizeActionUntil(
      [allow(hour(1)), allow(hour(3), "credential:*")],
      CRED,
      USE,
      PESSIMISTIC,
    );
    expect(decision).toEqual({ ok: true, expiresAt: hour(3) });
  });

  test("a covering grant with no expiry gives an allow with no end", async () => {
    const decision = await authorizeActionUntil(
      [allow(hour(1)), allow(null, "credential:*")],
      CRED,
      USE,
      PESSIMISTIC,
    );
    expect(decision).toEqual({ ok: true, expiresAt: null });
  });

  test("a specific allow over a broader deny ends with the specific allow", async () => {
    const decision = await authorizeActionUntil(
      [
        allow(hour(1)),
        grant({ resource: "credential:*", action: USE, effect: "deny" }),
        allow(null, "credential:*"),
      ],
      CRED,
      USE,
      PESSIMISTIC,
    );
    expect(decision).toEqual({ ok: true, expiresAt: hour(1) });
  });

  test("an allow that ends and later comes back ends at the first change", async () => {
    // At 1h the specific allow ends and the broad deny decides; at 2h the
    // deny ends and the broad allow decides again.
    const decision = await authorizeActionUntil(
      [
        allow(hour(1)),
        grant({
          resource: "credential:*",
          action: USE,
          effect: "deny",
          expiresAt: hour(2),
        }),
        allow(hour(3), "credential:*"),
      ],
      CRED,
      USE,
      PESSIMISTIC,
    );
    expect(decision).toEqual({ ok: true, expiresAt: hour(1) });
  });

  test("a conditioned allow does not extend the allow in pessimistic mode", async () => {
    const decision = await authorizeActionUntil(
      [
        allow(hour(1)),
        grant({
          resource: "credential:*",
          action: USE,
          effect: "allow",
          conditions: { tool: "builtin:reactor" },
        }),
      ],
      CRED,
      USE,
      PESSIMISTIC,
    );
    expect(decision).toEqual({ ok: true, expiresAt: hour(1) });
  });

  test("an expired allow in the input does not end a live allow", async () => {
    const decision = await authorizeActionUntil(
      [allow(hour(-1)), allow(hour(4), "credential:*")],
      CRED,
      USE,
      PESSIMISTIC,
    );
    expect(decision).toEqual({ ok: true, expiresAt: hour(4) });
  });

  test("a specific allow and a broad deny that end together leave the broad allow", async () => {
    const decision = await authorizeActionUntil(
      [
        allow(hour(1)),
        grant({
          resource: "credential:*",
          action: USE,
          effect: "deny",
          expiresAt: hour(1),
        }),
        allow(null, "credential:*"),
      ],
      CRED,
      USE,
      PESSIMISTIC,
    );
    expect(decision).toEqual({ ok: true, expiresAt: null });
  });

  test("a conditioned broad deny ends the allow when the specific allow ends", async () => {
    const decision = await authorizeActionUntil(
      [
        allow(hour(2)),
        grant({
          resource: "credential:*",
          action: USE,
          effect: "deny",
          conditions: { tool: "builtin:reactor" },
        }),
        grant({ resource: "*", action: "*", effect: "allow" }),
      ],
      CRED,
      USE,
      PESSIMISTIC,
    );
    expect(decision).toEqual({ ok: true, expiresAt: hour(2) });
  });

  test("an allow that passes from one grant to the next ends at the last one", async () => {
    const decision = await authorizeActionUntil(
      [
        allow(hour(1)),
        allow(hour(2), "credential:crd_*"),
        grant({ resource: "credential:*", action: USE, effect: "deny" }),
      ],
      CRED,
      USE,
      PESSIMISTIC,
    );
    expect(decision).toEqual({ ok: true, expiresAt: hour(2) });
  });

  test("a withheld action carries the reason", async () => {
    const decision = await authorizeActionUntil(
      [grant({ resource: CRED, action: USE, effect: "ask" })],
      CRED,
      USE,
      PESSIMISTIC,
    );
    expect(decision).toEqual({ ok: false, reason: "ask" });
  });
});

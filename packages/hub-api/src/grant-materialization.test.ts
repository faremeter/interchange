import { describe, test, expect } from "bun:test";
import type { GrantRule } from "@intx/types/authz";
import {
  resolveGrantMaterialization,
  INVOKER_GRANT_TTL_MS,
} from "./grant-materialization";

function grant(
  partial: Partial<GrantRule> &
    Pick<GrantRule, "resource" | "action" | "origin">,
): GrantRule {
  return {
    id: "g_" + Math.random().toString(36).slice(2),
    effect: "allow",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: null,
    ...partial,
  };
}

function first<T>(rows: readonly T[]): T {
  const row = rows[0];
  if (row === undefined) throw new Error("expected at least one grant row");
  return row;
}

const NOW = new Date("2026-07-21T00:00:00.000Z");

const base = {
  tenantId: "t_1",
  targetPrincipalId: "p_target",
  now: NOW,
};

describe("resolveGrantMaterialization", () => {
  test("creator-sourced requirement resolves against creatorGrants, origin creator, no expiry", async () => {
    const res = await resolveGrantMaterialization({
      ...base,
      grantRequirements: [
        { resource: "wallet:w1", action: "spend", source: "creator" },
      ],
      invokerGrants: [],
      creatorGrants: [
        grant({ resource: "wallet:w1", action: "spend", origin: "creator" }),
      ],
    });
    expect(res.ok).toBe(true);
    if (!res.ok) return;
    expect(res.grantRows).toHaveLength(1);
    const r = first(res.grantRows);
    expect(r.origin).toBe("creator");
    expect(r.expiresAt).toBeNull();
    expect(r.effect).toBe("allow");
    expect(r.tenantId).toBe("t_1");
    expect(r.principalId).toBe("p_target");
    expect(r.resource).toBe("wallet:w1");
    expect(r.createdAt).toBe(NOW);
    expect(r.updatedAt).toBe(NOW);
  });

  test("invoker-sourced requirement resolves against invokerGrants, origin invoker, TTL expiry", async () => {
    const res = await resolveGrantMaterialization({
      ...base,
      grantRequirements: [
        { resource: "wallet:w1", action: "spend", source: "invoker" },
      ],
      invokerGrants: [
        grant({ resource: "wallet:w1", action: "spend", origin: "role" }),
      ],
      creatorGrants: [],
    });
    expect(res.ok).toBe(true);
    if (!res.ok) return;
    const r = first(res.grantRows);
    expect(r.origin).toBe("invoker");
    expect(r.expiresAt).toEqual(new Date(NOW.getTime() + INVOKER_GRANT_TTL_MS));
  });

  test("invoker-origin grants are NOT delegatable (filtered out before resolution)", async () => {
    // invoker holds the capability but only via an invoker-origin grant.
    const res = await resolveGrantMaterialization({
      ...base,
      grantRequirements: [
        { resource: "wallet:w1", action: "spend", source: "invoker" },
      ],
      invokerGrants: [
        grant({ resource: "wallet:w1", action: "spend", origin: "invoker" }),
      ],
      creatorGrants: [],
    });
    expect(res.ok).toBe(false);
    if (res.ok) return;
    expect(res.rejection).toEqual({
      status: 403,
      code: "insufficient_grants",
      message:
        "Invoker lacks authority for wallet:w1/spend (no_matching_grant)",
    });
  });

  for (const effect of ["deny", "ask"] as const) {
    test(`an invoker-origin ${effect} still blocks delegation`, async () => {
      // A run that deploys a workflow holds invoker-origin rows. Its allow
      // cannot be re-delegated, but its deny or ask still restricts it.
      const res = await resolveGrantMaterialization({
        ...base,
        grantRequirements: [
          { resource: "asset:a1", action: "read", source: "invoker" },
        ],
        invokerGrants: [
          grant({ resource: "asset:a1", action: "read", origin: "creator" }),
          grant({
            resource: "asset:a1",
            action: "read",
            origin: "invoker",
            effect,
          }),
        ],
        creatorGrants: [],
      });
      expect(res.ok).toBe(false);
      if (res.ok) return;
      expect(res.rejection.message).toEndWith(`(${effect})`);
    });
  }

  test("creator lacking authority => 403 insufficient_grants with creator message", async () => {
    const res = await resolveGrantMaterialization({
      ...base,
      grantRequirements: [
        { resource: "wallet:w1", action: "spend", source: "creator" },
      ],
      invokerGrants: [],
      creatorGrants: [],
    });
    expect(res.ok).toBe(false);
    if (res.ok) return;
    expect(res.rejection).toEqual({
      status: 403,
      code: "insufficient_grants",
      message:
        "Creator lacks authority to delegate wallet:w1/spend (no_matching_grant)",
    });
  });

  // A delegation check has no request, so it cannot tell whether a
  // delegator's conditioned grant holds. A user's time-window grant and a
  // run's consumer-scoped grant are both conditioned grants.
  const CONDITIONS = [
    { time_window: { after: "09:00", before: "17:00" } },
    { tool: "tool:@acme/wallet" },
  ];

  for (const source of ["creator", "invoker"] as const) {
    const delegate = (delegatorGrants: GrantRule[]) =>
      resolveGrantMaterialization({
        ...base,
        grantRequirements: [{ resource: "wallet:w1", action: "spend", source }],
        invokerGrants: source === "invoker" ? delegatorGrants : [],
        creatorGrants: source === "creator" ? delegatorGrants : [],
      });

    for (const conditions of CONDITIONS) {
      const kind = Object.keys(conditions).join();

      for (const effect of ["deny", "ask"] as const) {
        test(`a ${source}'s ${kind} ${effect} blocks delegation over its unconditioned allow`, async () => {
          const res = await delegate([
            grant({ resource: "wallet:*", action: "spend", origin: "role" }),
            grant({
              resource: "wallet:*",
              action: "spend",
              origin: "role",
              effect,
              conditions,
            }),
          ]);
          expect(res.ok).toBe(false);
          if (res.ok) return;
          expect(res.rejection.code).toBe("insufficient_grants");
          expect(res.rejection.message).toEndWith(`(${effect})`);
        });
      }

      test(`a ${source}'s ${kind} allow alone does not delegate`, async () => {
        const res = await delegate([
          grant({
            resource: "wallet:*",
            action: "spend",
            origin: "role",
            conditions,
          }),
        ]);
        expect(res.ok).toBe(false);
        if (res.ok) return;
        expect(res.rejection.message).toEndWith("(no_matching_grant)");
      });
    }
  }

  test("unknown source => 409 not_launchable", async () => {
    const res = await resolveGrantMaterialization({
      ...base,
      grantRequirements: [
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- deliberately bypass the validated type to reach the defensive unknown-source branch
        { resource: "x", action: "y", source: "bogus" as unknown as "creator" },
      ],
      invokerGrants: [],
      creatorGrants: [],
    });
    expect(res.ok).toBe(false);
    if (res.ok) return;
    expect(res.rejection).toEqual({
      status: 409,
      code: "not_launchable",
      message: "Unknown grant requirement source: bogus",
    });
  });

  test("effect defaults to allow, and explicit effect is preserved", async () => {
    const res = await resolveGrantMaterialization({
      ...base,
      grantRequirements: [
        {
          resource: "wallet:w1",
          action: "spend",
          source: "creator",
          effect: "deny",
        },
      ],
      invokerGrants: [],
      // creator holds an allow that authorizes delegation; the materialized
      // row still carries the requirement's own effect (deny).
      creatorGrants: [
        grant({ resource: "wallet:w1", action: "spend", origin: "creator" }),
      ],
    });
    expect(res.ok).toBe(true);
    if (!res.ok) return;
    expect(first(res.grantRows).effect).toBe("deny");
  });

  test("short-circuits on first failed requirement (no rows accumulated past failure)", async () => {
    const res = await resolveGrantMaterialization({
      ...base,
      grantRequirements: [
        // first fails -> should return immediately
        { resource: "wallet:w1", action: "spend", source: "creator" },
        { resource: "wallet:w2", action: "spend", source: "creator" },
      ],
      invokerGrants: [],
      creatorGrants: [
        grant({ resource: "wallet:w2", action: "spend", origin: "creator" }),
      ],
    });
    expect(res.ok).toBe(false);
    if (res.ok) return;
    expect(res.rejection.message).toBe(
      "Creator lacks authority to delegate wallet:w1/spend (no_matching_grant)",
    );
  });

  test("requirements resolve in order", async () => {
    const res = await resolveGrantMaterialization({
      ...base,
      grantRequirements: [
        { resource: "wallet:w1", action: "spend", source: "creator" },
        { resource: "wallet:w2", action: "read", source: "invoker" },
      ],
      invokerGrants: [
        grant({ resource: "wallet:*", action: "*", origin: "role" }),
      ],
      creatorGrants: [
        grant({ resource: "wallet:w1", action: "spend", origin: "creator" }),
      ],
    });
    expect(res.ok).toBe(true);
    if (!res.ok) return;
    expect(res.grantRows.map((r) => [r.resource, r.origin])).toEqual([
      ["wallet:w1", "creator"],
      ["wallet:w2", "invoker"],
    ]);
  });
});

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
      adHocInvokerGrants: [],
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
      adHocInvokerGrants: [],
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
      adHocInvokerGrants: [],
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
      message: "Invoker lacks authority for wallet:w1/spend",
    });
  });

  test("creator lacking authority => 403 insufficient_grants with creator message", async () => {
    const res = await resolveGrantMaterialization({
      ...base,
      grantRequirements: [
        { resource: "wallet:w1", action: "spend", source: "creator" },
      ],
      adHocInvokerGrants: [],
      invokerGrants: [],
      creatorGrants: [],
    });
    expect(res.ok).toBe(false);
    if (res.ok) return;
    expect(res.rejection).toEqual({
      status: 403,
      code: "insufficient_grants",
      message: "Creator lacks authority to delegate wallet:w1/spend",
    });
  });

  test("unknown source => 409 not_launchable", async () => {
    const res = await resolveGrantMaterialization({
      ...base,
      grantRequirements: [
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- deliberately bypass the validated type to reach the defensive unknown-source branch
        { resource: "x", action: "y", source: "bogus" as unknown as "creator" },
      ],
      adHocInvokerGrants: [],
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

  test("ad-hoc invoker grants resolve against delegatable invoker grants, origin invoker + TTL", async () => {
    const res = await resolveGrantMaterialization({
      ...base,
      grantRequirements: [],
      adHocInvokerGrants: [{ resource: "wallet:w1", action: "spend" }],
      invokerGrants: [
        grant({ resource: "wallet:w1", action: "spend", origin: "system" }),
      ],
      creatorGrants: [],
    });
    expect(res.ok).toBe(true);
    if (!res.ok) return;
    const r = first(res.grantRows);
    expect(r.origin).toBe("invoker");
    expect(r.expiresAt).toEqual(new Date(NOW.getTime() + INVOKER_GRANT_TTL_MS));
  });

  test("ad-hoc invoker grant lacking authority => 403", async () => {
    const res = await resolveGrantMaterialization({
      ...base,
      grantRequirements: [],
      adHocInvokerGrants: [{ resource: "wallet:w9", action: "spend" }],
      invokerGrants: [],
      creatorGrants: [],
    });
    expect(res.ok).toBe(false);
    if (res.ok) return;
    expect(res.rejection).toEqual({
      status: 403,
      code: "insufficient_grants",
      message: "Invoker lacks authority for wallet:w9/spend",
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
      adHocInvokerGrants: [],
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
      adHocInvokerGrants: [],
      invokerGrants: [],
      creatorGrants: [
        grant({ resource: "wallet:w2", action: "spend", origin: "creator" }),
      ],
    });
    expect(res.ok).toBe(false);
    if (res.ok) return;
    expect(res.rejection.message).toBe(
      "Creator lacks authority to delegate wallet:w1/spend",
    );
  });

  test("requirements resolve in order, then ad-hoc grants appended", async () => {
    const res = await resolveGrantMaterialization({
      ...base,
      grantRequirements: [
        { resource: "wallet:w1", action: "spend", source: "creator" },
        { resource: "wallet:w2", action: "read", source: "invoker" },
      ],
      adHocInvokerGrants: [{ resource: "wallet:w3", action: "read" }],
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
      ["wallet:w3", "invoker"],
    ]);
  });
});

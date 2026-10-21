import { describe, expect, test } from "bun:test";

import type { DB } from "@intx/db";

import type { PrincipalRow, TenantRow } from "./context";
import { resolveTenantPrincipal } from "./principal-lookup";

const NOW = new Date("2025-01-15T00:00:00Z");

const TENANT: TenantRow = {
  id: "ten_a",
  name: "Tenant A",
  slug: "ten-a",
  domain: "ten-a.example.com",
  parentId: null,
  config: null,
  createdAt: NOW,
  updatedAt: NOW,
};

function makePrincipal(
  kind: PrincipalRow["kind"],
  status: PrincipalRow["status"],
): PrincipalRow {
  return {
    id: "prn_a",
    tenantId: TENANT.id,
    kind,
    refId: kind === "user" ? "user_alice" : "run_a",
    status,
    createdAt: NOW,
    updatedAt: NOW,
  };
}

function createMockDB(opts: {
  tenant: TenantRow | undefined;
  principal: PrincipalRow | undefined;
}): DB["db"] {
  const mock = {
    query: {
      tenant: { findFirst: async () => opts.tenant },
      principal: { findFirst: async () => opts.principal },
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase type cannot be structurally satisfied in tests
  return mock as unknown as DB["db"];
}

describe("resolveTenantPrincipal", () => {
  test("reports a missing tenant before looking up the principal", async () => {
    const db = createMockDB({
      tenant: undefined,
      principal: makePrincipal("user", "active"),
    });
    const result = await resolveTenantPrincipal(db, {
      tenantId: TENANT.id,
      select: { userId: "user_alice" },
    });
    expect(result).toEqual({ kind: "tenant_not_found" });
  });

  test("reports a missing principal with the tenant it searched", async () => {
    const db = createMockDB({ tenant: TENANT, principal: undefined });
    const result = await resolveTenantPrincipal(db, {
      tenantId: TENANT.id,
      select: { principalId: "prn_missing" },
    });
    expect(result).toEqual({ kind: "principal_not_found", tenant: TENANT });
  });

  test("reports an inactive principal instead of resolving it", async () => {
    const suspended = makePrincipal("user", "suspended");
    const db = createMockDB({ tenant: TENANT, principal: suspended });
    const result = await resolveTenantPrincipal(db, {
      tenantId: TENANT.id,
      select: { userId: "user_alice" },
    });
    expect(result).toEqual({
      kind: "principal_inactive",
      tenant: TENANT,
      principal: suspended,
    });
  });

  test("resolves an active user principal", async () => {
    const user = makePrincipal("user", "active");
    const db = createMockDB({ tenant: TENANT, principal: user });
    const result = await resolveTenantPrincipal(db, {
      tenantId: TENANT.id,
      select: { userId: "user_alice" },
    });
    expect(result).toEqual({ kind: "ok", tenant: TENANT, principal: user });
  });

  test("resolves a principal of any kind named by id", async () => {
    const run = makePrincipal("workflow", "active");
    const db = createMockDB({ tenant: TENANT, principal: run });
    const result = await resolveTenantPrincipal(db, {
      tenantId: TENANT.id,
      select: { principalId: run.id },
    });
    expect(result).toEqual({ kind: "ok", tenant: TENANT, principal: run });
  });
});

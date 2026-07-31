import { describe, expect, test } from "bun:test";
import { Hono } from "hono";

import { createInMemoryGrantStore } from "@intx/authz";
import type { GrantRule } from "@intx/types/authz";

import type { PrincipalRow, TenantEnv, TenantRow } from "../context";
import { createRequireGrant } from "../middleware/grant";
import { createRoleAssignRoutes } from "./roles";

const TENANT_A = "tnt_a";
const CALLER_PRINCIPAL = "prn_caller";
const TARGET_PRINCIPAL = "prn_target";
const ROLE_ID = "rol_target";

const callerTenant: TenantRow = {
  id: TENANT_A,
  name: "Tenant A",
  slug: "a",
  domain: "a.example.com",
  parentId: null,
  config: null,
  createdAt: new Date("2025-01-01"),
  updatedAt: new Date("2025-01-01"),
};

const callerPrincipal: PrincipalRow = {
  id: CALLER_PRINCIPAL,
  tenantId: TENANT_A,
  kind: "user",
  refId: "usr_a",
  status: "active",
  createdAt: new Date("2025-01-01"),
  updatedAt: new Date("2025-01-01"),
};

function manageGrant(overrides: Partial<GrantRule> = {}): GrantRule {
  return {
    id: "grant-role-manage",
    resource: "role:*",
    action: "manage",
    effect: "allow",
    origin: "system",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: CALLER_PRINCIPAL,
    ...overrides,
  };
}

type MockRow = Record<string, unknown>;
type MockDBOpts = {
  // db.query.principal.findFirst — the handler's tenant-scoped target load;
  // undefined stands for a principal that is not in the caller's tenant.
  targetPrincipal?: MockRow | undefined;
  // db.query.role.findFirst — the handler's tenant-scoped role load.
  role?: MockRow | undefined;
  // Rows db.delete(...).returning() yields when the delete is issued.
  deleted?: MockRow[];
  // Fires when the join-row delete is issued.
  onDelete?: () => void;
};

function createMockDB(opts: MockDBOpts) {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase cannot be structurally satisfied in tests
  return {
    query: {
      principal: { findFirst: async () => opts.targetPrincipal },
      role: { findFirst: async () => opts.role },
    },
    delete: () => ({
      where: () => ({
        returning: async () => {
          opts.onDelete?.();
          return opts.deleted ?? [];
        },
      }),
    }),
  } as unknown as Parameters<typeof createRoleAssignRoutes>[0]["db"];
}

function createTestApp(opts: { db: MockDBOpts; grants?: GrantRule[] }) {
  const grantStore = createInMemoryGrantStore(opts.grants ?? [manageGrant()]);
  const requireGrant = createRequireGrant({
    grantStore,
    conditionRegistry: {},
  });

  const app = new Hono<TenantEnv>();
  // Inject the caller's already-resolved tenant + principal, standing in for
  // the resolveTenant middleware the full app mounts ahead of these routes.
  app.use("*", async (c, next) => {
    c.set("user", null);
    c.set("session", null);
    c.set("tenant", callerTenant);
    c.set("principal", callerPrincipal);
    await next();
  });
  app.route(
    "/:principalId/roles",
    createRoleAssignRoutes({ db: createMockDB(opts.db), requireGrant }),
  );
  return app;
}

const unassignURL = `/${TARGET_PRINCIPAL}/roles/${ROLE_ID}`;

describe("DELETE /:principalId/roles/:roleId — tenant scoping", () => {
  test("in-tenant unassign deletes the join row and returns 204", async () => {
    let deleteIssued = false;
    const app = createTestApp({
      db: {
        targetPrincipal: { id: TARGET_PRINCIPAL, tenantId: TENANT_A },
        role: { id: ROLE_ID, tenantId: TENANT_A },
        deleted: [{ principalId: TARGET_PRINCIPAL, roleId: ROLE_ID }],
        onDelete: () => {
          deleteIssued = true;
        },
      },
    });
    const res = await app.request(unassignURL, { method: "DELETE" });
    expect(res.status).toBe(204);
    expect(deleteIssued).toBe(true);
  });

  test("cross-tenant unassign 404s and never deletes the join row", async () => {
    // The caller holds a role:* manage grant in tenant A, but the target
    // principal lives in another tenant, so the tenant-scoped lookup misses.
    // The handler must 404 before issuing the delete, so the victim tenant's
    // role assignment survives.
    let deleteIssued = false;
    const app = createTestApp({
      db: {
        targetPrincipal: undefined,
        role: { id: ROLE_ID, tenantId: TENANT_A },
        deleted: [{ principalId: TARGET_PRINCIPAL, roleId: ROLE_ID }],
        onDelete: () => {
          deleteIssued = true;
        },
      },
    });
    const res = await app.request(unassignURL, { method: "DELETE" });
    expect(res.status).toBe(404);
    expect(deleteIssued).toBe(false);
  });

  test("403 without a manage grant", async () => {
    const app = createTestApp({
      db: { targetPrincipal: undefined, role: undefined },
      grants: [],
    });
    const res = await app.request(unassignURL, { method: "DELETE" });
    expect(res.status).toBe(403);
  });
});

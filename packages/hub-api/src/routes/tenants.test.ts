import { describe, expect, test } from "bun:test";
import { Hono } from "hono";

import { createInMemoryGrantStore } from "@intx/authz";
import type { PrincipalKeyStore } from "@intx/db";
import type { GrantRule } from "@intx/types/authz";

import type { AppEnv, PrincipalRow, TenantRow } from "../context";
import { createAuthorizeGrant, createRequireGrant } from "../middleware/grant";
import { createResolveTenant } from "../middleware/tenant";
import { createTenantRoutes } from "./tenants";

// The routes are mounted without the app's `/api/tenants/:tenantId/*`
// middleware, so these tests prove that detail and update carry their own
// membership and grant gates.

const TENANT_ID = "tnt_a";
const PRINCIPAL_ID = "prn_caller";
const USER_ID = "usr_caller";
const NOW = new Date("2025-01-01");

const tenantRow: TenantRow = {
  id: TENANT_ID,
  name: "Tenant A",
  slug: "a",
  domain: "a.localhost",
  parentId: null,
  config: null,
  createdAt: NOW,
  updatedAt: NOW,
};

function principalWith(status: PrincipalRow["status"]): PrincipalRow {
  return {
    id: PRINCIPAL_ID,
    tenantId: TENANT_ID,
    kind: "user",
    refId: USER_ID,
    status,
    createdAt: NOW,
    updatedAt: NOW,
  };
}

function manageGrant(): GrantRule {
  return {
    id: "grant-tenant-manage",
    resource: `tenant:${TENANT_ID}`,
    action: "manage",
    effect: "allow",
    origin: "system",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: PRINCIPAL_ID,
  };
}

function createMockDB(principal: PrincipalRow | undefined) {
  const notImplemented = (path: string) => () => {
    throw new Error(`mock: ${path} not implemented`);
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase cannot be structurally satisfied in tests
  return {
    query: {
      tenant: { findFirst: async () => tenantRow },
      principal: { findFirst: async () => principal },
    },
    transaction: notImplemented("db.transaction"),
  } as unknown as Parameters<typeof createTenantRoutes>[0]["db"];
}

function createTestApp(opts: {
  principal: PrincipalRow | undefined;
  grants?: GrantRule[];
}) {
  const db = createMockDB(opts.principal);
  const grantStore = createInMemoryGrantStore(opts.grants ?? []);
  const conditionRegistry = {};
  const app = new Hono<AppEnv>();
  app.use("*", async (c, next) => {
    c.set("user", {
      id: USER_ID,
      email: "caller@example.com",
      emailVerified: true,
      name: "Caller",
      createdAt: NOW,
      updatedAt: NOW,
    });
    c.set("session", null);
    await next();
  });
  app.route(
    "/api/tenants",
    createTenantRoutes({
      db,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- only tenant creation without a parent uses the key store
      principalKeyStore: {} as PrincipalKeyStore,
      authorizeGrant: createAuthorizeGrant({ grantStore, conditionRegistry }),
      resolveTenant: createResolveTenant({ db }),
      requireGrant: createRequireGrant({ grantStore, conditionRegistry }),
    }),
  );
  return app;
}

function patchTenant(app: Hono<AppEnv>, body: unknown) {
  return app.request(`/api/tenants/${TENANT_ID}`, {
    method: "PATCH",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

function patchName(app: Hono<AppEnv>) {
  return patchTenant(app, { name: "Renamed" });
}

describe("tenant detail and update gates", () => {
  test("an active member reads the tenant", async () => {
    const app = createTestApp({ principal: principalWith("active") });
    const res = await app.request(`/api/tenants/${TENANT_ID}`);
    expect(res.status).toBe(200);
  });

  test.each(["suspended", "invited", "deactivated"] as const)(
    "a principal with status %s cannot read the tenant",
    async (status) => {
      const app = createTestApp({ principal: principalWith(status) });
      const res = await app.request(`/api/tenants/${TENANT_ID}`);
      expect(res.status).toBe(403);
    },
  );

  test("a user outside the tenant cannot read it", async () => {
    const app = createTestApp({ principal: undefined });
    const res = await app.request(`/api/tenants/${TENANT_ID}`);
    expect(res.status).toBe(403);
  });

  test.each(["suspended", "invited", "deactivated"] as const)(
    "a principal with status %s holding manage cannot update the tenant",
    async (status) => {
      const app = createTestApp({
        principal: principalWith(status),
        grants: [manageGrant()],
      });
      expect((await patchName(app)).status).toBe(403);
    },
  );

  test("an active member without manage cannot update the tenant", async () => {
    const app = createTestApp({ principal: principalWith("active") });
    expect((await patchName(app)).status).toBe(403);
  });

  // The body validator runs after both gates, so an invalid body tells a
  // caller that passed them (400) from one that did not (403).
  test("a user outside the tenant is refused before an invalid update body is checked", async () => {
    const app = createTestApp({ principal: undefined });
    expect((await patchTenant(app, { name: 42 })).status).toBe(403);
  });

  test("an active member holding manage passes both gates to the body check", async () => {
    const app = createTestApp({
      principal: principalWith("active"),
      grants: [manageGrant()],
    });
    expect((await patchTenant(app, { name: 42 })).status).toBe(400);
  });

  test("creating a tenant under a parent does not run the tenant middleware", async () => {
    // The middleware answers a request with no :tenantId as a 400, so the
    // parent refusal proves it did not run.
    const app = createTestApp({ principal: undefined });
    const res = await app.request("/api/tenants", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        name: "Child",
        slug: "child",
        parentId: TENANT_ID,
      }),
    });
    expect(res.status).toBe(403);
    expect(await res.json()).toEqual({
      error: {
        code: "forbidden",
        message:
          "You do not have permission to create a tenant under this parent",
      },
    });
  });
});

import { describe, expect, test } from "bun:test";
import { Hono } from "hono";
import type { MiddlewareHandler } from "hono";

import type { DB } from "@intx/db";

import type { PrincipalRow, TenantEnv, TenantRow } from "../context";
import { createResolveTenant } from "./tenant";

const NOW = new Date("2025-01-15T00:00:00Z");

function makeTenant(id: string): TenantRow {
  return {
    id,
    name: `Tenant ${id}`,
    slug: id,
    domain: `${id}.example.com`,
    parentId: null,
    config: null,
    createdAt: NOW,
    updatedAt: NOW,
  };
}

function makePrincipal(
  id: string,
  tenantId: string,
  status: PrincipalRow["status"] = "active",
): PrincipalRow {
  return {
    id,
    tenantId,
    kind: "user",
    refId: "user_alice",
    status,
    createdAt: NOW,
    updatedAt: NOW,
  };
}

function notImplemented(path: string) {
  return () => {
    throw new Error(`mock: ${path} not implemented`);
  };
}

type MockDBOpts = {
  tenant?: TenantRow | undefined;
  principal?: PrincipalRow | undefined;
};

function createMockDB(opts: MockDBOpts): DB["db"] {
  const mock = {
    query: {
      tenant: {
        findFirst: async () => opts.tenant,
        findMany: notImplemented("db.query.tenant.findMany"),
      },
      principal: {
        findFirst: async () => opts.principal,
        findMany: notImplemented("db.query.principal.findMany"),
      },
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase type cannot be structurally satisfied in tests
  return mock as unknown as DB["db"];
}

describe("createResolveTenant", () => {
  test("short-circuits when principal and tenant are already set on context", async () => {
    const dbMock = {
      query: {
        tenant: { findFirst: notImplemented("tenant.findFirst") },
        principal: { findFirst: notImplemented("principal.findFirst") },
      },
    };
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase type cannot be structurally satisfied in tests
    const db = dbMock as unknown as DB["db"];

    const tenantRow = makeTenant("ten_a");
    const principalRow = makePrincipal("prin_1", "ten_a");

    const app = new Hono<TenantEnv>();
    const preset: MiddlewareHandler<TenantEnv> = async (c, next) => {
      c.set("tenant", tenantRow);
      c.set("principal", principalRow);
      await next();
    };
    app.get(
      "/tenants/:tenantId/probe",
      preset,
      createResolveTenant({ db }),
      (c) => {
        const t = c.get("tenant");
        const p = c.get("principal");
        return c.json({ tenantId: t.id, principalId: p.id });
      },
    );

    const res = await app.request("/tenants/ten_a/probe");
    expect(res.status).toBe(200);
    const body: unknown = await res.json();
    expect(body).toEqual({ tenantId: "ten_a", principalId: "prin_1" });
  });

  test("normal path runs when context is not pre-populated", async () => {
    const tenantRow = makeTenant("ten_a");
    const principalRow = makePrincipal("prin_alice", "ten_a");
    const db = createMockDB({
      tenant: tenantRow,
      principal: principalRow,
    });

    const app = new Hono<TenantEnv>();
    const setUser: MiddlewareHandler<TenantEnv> = async (c, next) => {
      c.set("user", {
        id: "user_alice",
        createdAt: NOW,
        updatedAt: NOW,
        email: "alice@example.com",
        emailVerified: true,
        name: "Alice",
      });
      c.set("session", null);
      await next();
    };
    app.get(
      "/tenants/:tenantId/probe",
      setUser,
      createResolveTenant({ db }),
      (c) => {
        const t = c.get("tenant");
        const p = c.get("principal");
        return c.json({ tenantId: t.id, principalId: p.id });
      },
    );

    const res = await app.request("/tenants/ten_a/probe");
    expect(res.status).toBe(200);
    const body: unknown = await res.json();
    expect(body).toEqual({ tenantId: "ten_a", principalId: "prin_alice" });
  });

  test("normal path returns 401 when there is no user", async () => {
    const db = createMockDB({});
    const app = new Hono<TenantEnv>();
    const setNoUser: MiddlewareHandler<TenantEnv> = async (c, next) => {
      c.set("user", null);
      c.set("session", null);
      await next();
    };
    app.get(
      "/tenants/:tenantId/probe",
      setNoUser,
      createResolveTenant({ db }),
      (c) => c.text("ok"),
    );

    const res = await app.request("/tenants/ten_a/probe");
    expect(res.status).toBe(401);
  });

  test("normal path returns 403 when the principal is suspended", async () => {
    const db = createMockDB({
      tenant: makeTenant("ten_a"),
      principal: makePrincipal("prin_alice", "ten_a", "suspended"),
    });

    const app = new Hono<TenantEnv>();
    const setUser: MiddlewareHandler<TenantEnv> = async (c, next) => {
      c.set("user", {
        id: "user_alice",
        createdAt: NOW,
        updatedAt: NOW,
        email: "alice@example.com",
        emailVerified: true,
        name: "Alice",
      });
      c.set("session", null);
      await next();
    };
    app.get(
      "/tenants/:tenantId/probe",
      setUser,
      createResolveTenant({ db }),
      (c) => c.text("ok"),
    );

    const res = await app.request("/tenants/ten_a/probe");
    expect(res.status).toBe(403);
  });
});

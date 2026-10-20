import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { Hono } from "hono";

import { createInMemoryGrantStore, toolConsumerEvaluator } from "@intx/authz";
import { configureSync, getConfig } from "@intx/log";
import type { GrantRule, GrantStore } from "@intx/types/authz";

import type { PrincipalRow, TenantEnv, TenantRow } from "../context";
import { createAuthorizeGrant, createRequireGrant } from "./grant";

const PRINCIPAL_ID = "prn_caller";
const TENANT_ID = "tnt_a";
const NOW = new Date("2025-01-01");

function grant(effect: GrantRule["effect"]): GrantRule {
  return {
    id: `grant-${effect}`,
    resource: "widget:*",
    action: "read",
    effect,
    origin: "system",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: PRINCIPAL_ID,
  };
}

// The in-memory store ignores the tenant, so record what the check asks for.
function authorizeGrantWith(grants: GrantRule[]) {
  const inner = createInMemoryGrantStore(grants);
  const tenantsAsked: string[] = [];
  const grantStore: GrantStore = {
    ...inner,
    collectGrants: (principalId, tenantId) => {
      tenantsAsked.push(tenantId);
      return inner.collectGrants(principalId, tenantId);
    },
  };
  const authorizeGrant = createAuthorizeGrant({
    grantStore,
    conditionRegistry: {},
  });
  return Object.assign(authorizeGrant, { tenantsAsked });
}

const captured: {
  category: readonly string[];
  properties: Record<string, unknown>;
}[] = [];
const savedConfig = getConfig();

beforeAll(() => {
  configureSync({
    reset: true,
    sinks: {
      capture: (record) => {
        captured.push({
          category: record.category,
          properties: record.properties,
        });
      },
    },
    loggers: [{ category: [], lowestLevel: "debug", sinks: ["capture"] }],
  });
});

afterAll(() => {
  if (!savedConfig) {
    throw new Error(
      "no logging configuration was captured before this suite replaced it",
    );
  }
  configureSync({ reset: true, ...savedConfig });
});

beforeEach(() => {
  captured.length = 0;
});

function denialLogs() {
  return captured.filter(
    (r) => r.category.join(".") === "hub.middleware.grant",
  );
}

const CHECK = {
  principalId: PRINCIPAL_ID,
  tenantId: TENANT_ID,
  resource: "widget:*",
  action: "read",
  consumer: undefined,
};

describe("createAuthorizeGrant", () => {
  test("allows an action an allow grant covers, without a denial log", async () => {
    const authorizeGrant = authorizeGrantWith([grant("allow")]);
    expect(await authorizeGrant(CHECK)).toBe(true);
    expect(authorizeGrant.tenantsAsked).toEqual([TENANT_ID]);
    expect(denialLogs()).toEqual([]);
  });

  test("refuses an action a deny grant covers and logs the denial", async () => {
    const authorizeGrant = authorizeGrantWith([grant("deny")]);
    expect(await authorizeGrant(CHECK)).toBe(false);
    expect(authorizeGrant.tenantsAsked).toEqual([TENANT_ID]);
    expect(denialLogs().map((r) => r.properties)).toEqual([
      {
        principalId: PRINCIPAL_ID,
        tenantId: TENANT_ID,
        resource: "widget:*",
        action: "read",
        consumer: null,
        effect: "deny",
        resolvedBy: "grant-deny",
      },
    ]);
  });

  test("refuses an action no grant covers and logs it as unmatched", async () => {
    expect(await authorizeGrantWith([])(CHECK)).toBe(false);
    expect(denialLogs().map((r) => r.properties)).toMatchObject([
      { tenantId: TENANT_ID, effect: "no_match", resolvedBy: null },
    ]);
  });
});

describe("createRequireGrant", () => {
  const tenant: TenantRow = {
    id: TENANT_ID,
    name: "Tenant A",
    slug: "a",
    domain: "a.localhost",
    parentId: null,
    config: null,
    createdAt: NOW,
    updatedAt: NOW,
  };
  const principal: PrincipalRow = {
    id: PRINCIPAL_ID,
    tenantId: TENANT_ID,
    kind: "user",
    refId: "usr_caller",
    status: "active",
    createdAt: NOW,
    updatedAt: NOW,
  };

  function appWith(grants: GrantRule[]) {
    const requireGrant = createRequireGrant({
      grantStore: createInMemoryGrantStore(grants),
      conditionRegistry: {},
    });
    const app = new Hono<TenantEnv>();
    app.use("*", async (c, next) => {
      c.set("tenant", tenant);
      c.set("principal", principal);
      await next();
    });
    app.get("/", requireGrant("widget:*", "read"), (c) => c.text("ok"));
    return app;
  }

  test("passes a request the context principal's grants allow", async () => {
    expect((await appWith([grant("allow")]).request("/")).status).toBe(200);
  });

  test("refuses a request the context principal's grants do not allow", async () => {
    const res = await appWith([]).request("/");
    expect(res.status).toBe(403);
    expect(await res.json()).toEqual({
      error: {
        code: "forbidden",
        message: "You do not have permission to perform this action",
      },
    });
  });
});

describe("createRequireGrant with a consumer", () => {
  const tenant: TenantRow = {
    id: TENANT_ID,
    name: "Tenant A",
    slug: "a",
    domain: "a.localhost",
    parentId: null,
    config: null,
    createdAt: NOW,
    updatedAt: NOW,
  };
  const principal: PrincipalRow = {
    id: PRINCIPAL_ID,
    tenantId: TENANT_ID,
    kind: "workflow",
    refId: "run_caller",
    status: "active",
    createdAt: NOW,
    updatedAt: NOW,
  };

  function scopedGrant(tool: string | null): GrantRule {
    return {
      ...grant("allow"),
      conditions: tool === null ? null : { tool },
    };
  }

  function appWith(grants: GrantRule[], consumer: string | undefined) {
    const requireGrant = createRequireGrant({
      grantStore: createInMemoryGrantStore(grants),
      conditionRegistry: { tool: toolConsumerEvaluator },
    });
    const app = new Hono<TenantEnv>();
    app.use("*", async (c, next) => {
      c.set("tenant", tenant);
      c.set("principal", principal);
      if (consumer !== undefined) c.set("consumer", consumer);
      await next();
    });
    app.get("/", requireGrant("widget:*", "read"), (c) => c.text("ok"));
    return app;
  }

  test("a grant scoped to the request's consumer allows it", async () => {
    const app = appWith([scopedGrant("action:a")], "action:a");
    expect((await app.request("/")).status).toBe(200);
  });

  test("a grant scoped to another consumer refuses it", async () => {
    const app = appWith([scopedGrant("action:b")], "action:a");
    expect((await app.request("/")).status).toBe(403);
  });

  test("a consumer-scoped grant refuses a request with no consumer", async () => {
    const app = appWith([scopedGrant("action:a")], undefined);
    expect((await app.request("/")).status).toBe(403);
  });

  test("a deny scoped to the request's consumer overrides an unscoped allow", async () => {
    const deny: GrantRule = {
      ...grant("deny"),
      conditions: { tool: "action:a" },
    };
    expect(
      (await appWith([scopedGrant(null), deny], "action:a").request("/"))
        .status,
    ).toBe(403);
    expect(
      (await appWith([scopedGrant(null), deny], "action:b").request("/"))
        .status,
    ).toBe(200);
  });

  test("a grant with no conditions allows every consumer", async () => {
    const app = appWith([scopedGrant(null)], "action:a");
    expect((await app.request("/")).status).toBe(200);
  });
});

import { describe, test, expect } from "bun:test";

import { createInMemoryGrantStore } from "@intx/authz";
import type { GrantRule } from "@intx/types/authz";
import {
  createSidecarEmitter,
  type EventCollectorRegistry,
  type SessionService,
  type SidecarRouter,
} from "@intx/hub-sessions";

import { createApp } from "../app";
import type { GetSession } from "../session";

const TENANT_ID = "tnt_test";
const PRINCIPAL_ID = "prn_test";
const USER_ID = "usr_test";

const testTenant = {
  id: TENANT_ID,
  name: "Test",
  slug: "test",
  domain: "test.example.com",
  parentId: null,
  config: null,
  createdAt: new Date("2025-01-01"),
  updatedAt: new Date("2025-01-01"),
};

const testPrincipal = {
  id: PRINCIPAL_ID,
  tenantId: TENANT_ID,
  kind: "user" as const,
  refId: USER_ID,
  status: "active" as const,
  createdAt: new Date("2025-01-01"),
  updatedAt: new Date("2025-01-01"),
};

function grant(resource: string, action: string): GrantRule {
  return {
    id: `grant-${resource}-${action}`,
    resource,
    action,
    effect: "allow",
    origin: "system",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: PRINCIPAL_ID,
  };
}

const ALL_CATALOG_GRANTS = [
  grant("model:*", "create"),
  grant("model:*", "manage"),
  grant("model-provider:*", "create"),
  grant("model-provider:*", "manage"),
  grant("model-offering:*", "create"),
  grant("model-offering:*", "manage"),
  grant("model-pricing:*", "create"),
];

type InsertCapture = { table: string; rows: Record<string, unknown>[] };

function tableName(table: unknown): string {
  if (table && typeof table === "object") {
    const sym = Object.getOwnPropertySymbols(table).find(
      (s) => s.description === "drizzle:Name",
    );
    if (sym) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle stores the table name keyed by a documented symbol
      const value = (table as Record<symbol, unknown>)[sym];
      if (typeof value === "string") return value;
    }
  }
  return "unknown";
}

type CatalogDBOpts = {
  model?: Record<string, unknown>;
  modelProvider?: Record<string, unknown>;
  modelOffering?: Record<string, unknown>;
  modelPricing?: Record<string, unknown>;
  inserts: InsertCapture[];
};

function notImplemented(path: string) {
  return () => {
    throw new Error(`mock: ${path} not implemented`);
  };
}

function createMockDB(opts: CatalogDBOpts) {
  function insertChain(table: unknown) {
    const name = tableName(table);
    return {
      values: (
        rowsOrRow: Record<string, unknown> | Record<string, unknown>[],
      ) => {
        const rows = Array.isArray(rowsOrRow) ? rowsOrRow : [rowsOrRow];
        opts.inserts.push({ table: name, rows });
        return { returning: () => Promise.resolve(rows) };
      },
    };
  }

  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase type cannot be structurally satisfied in tests
  return {
    query: {
      tenant: {
        findFirst: async () => testTenant,
        // getDescendantTenants (via the post-mutation source push) walks the
        // subtree; no children in tests.
        findMany: async () => [],
      },
      // The post-mutation source push scans running instances; none in tests.
      agentInstance: {
        findFirst: notImplemented("db.query.agentInstance.findFirst"),
        findMany: async () => [],
      },
      principal: {
        findFirst: async () => testPrincipal,
        findMany: notImplemented("db.query.principal.findMany"),
      },
      role: {
        findFirst: notImplemented("db.query.role.findFirst"),
        findMany: async () => [],
      },
      model: {
        findFirst: async () => opts.model,
        findMany: async () => [],
      },
      modelProvider: {
        findFirst: async () => opts.modelProvider,
        findMany: async () => [],
      },
      modelOffering: {
        findFirst: async () => opts.modelOffering,
        findMany: async () => [],
      },
      modelPricing: {
        findFirst: async () => opts.modelPricing,
        findMany: async () => [],
      },
    },
    insert: insertChain,
  } as unknown as Parameters<typeof createApp>[0]["db"];
}

function createMockGetSession(): GetSession {
  const now = new Date("2025-01-01");
  return async () => ({
    user: {
      id: USER_ID,
      email: "test@example.com",
      emailVerified: true,
      name: "Test User",
      createdAt: now,
      updatedAt: now,
    },
    session: {
      id: "session_test",
      userId: USER_ID,
      token: "tok_test",
      expiresAt: new Date("2999-01-01"),
      createdAt: now,
      updatedAt: now,
    },
  });
}

function createMockSidecarRouter(): SidecarRouter {
  function notImpl(name: string): never {
    throw new Error(`mock: sidecarRouter.${name} not implemented`);
  }
  return {
    handleOpen: () => notImpl("handleOpen"),
    handleMessage: () => notImpl("handleMessage"),
    handleClose: () => notImpl("handleClose"),
    routeMail: () => notImpl("routeMail"),
    sendAgentDeploy: () => notImpl("sendAgentDeploy"),
    sendAgentUndeploy: () => notImpl("sendAgentUndeploy"),
    sendSessionStart: () => notImpl("sendSessionStart"),
    sendSessionAbort: () => notImpl("sendSessionAbort"),
    sendGrantsUpdate: () => notImpl("sendGrantsUpdate"),
    // Mutations fire a fire-and-forget source push; resolve so it is a no-op.
    sendSourcesUpdate: () => Promise.resolve(),
    sendPack: () => notImpl("sendPack"),
    sendSyncRequest: () => notImpl("sendSyncRequest"),
    sendSignalDeliver: () => notImpl("sendSignalDeliver"),
    sendDrain: () => notImpl("sendDrain"),
    subscribeAgent: () => notImpl("subscribeAgent"),
    dispatchAgentEvent: () => undefined,
    getConnectedSidecars: () => [],
    getRoutableAddresses: () => [],
    getConnectorState: () => null,
    events: createSidecarEmitter(),
  };
}

function createMockSessionService(): SessionService {
  return {
    launchSession: () => {
      throw new Error("mock: sessionService.launchSession not implemented");
    },
    sendUserMessage: () => {
      throw new Error("mock: sessionService.sendUserMessage not implemented");
    },
    endSession: () => {
      throw new Error("mock: sessionService.endSession not implemented");
    },
  };
}

function createMockEventCollectors(): EventCollectorRegistry {
  return {
    create: notImplemented("eventCollectors.create"),
    dispatch: notImplemented("eventCollectors.dispatch"),
    abandon: notImplemented("eventCollectors.abandon"),
    has: () => false,
    getStatus: () => undefined,
    getAccumulatedText: () => undefined,
    getCurrentTurnId: () => undefined,
    getLastTurnId: () => undefined,
  };
}

type TestAppOpts = {
  db: CatalogDBOpts;
  grants?: GrantRule[];
};

function createTestApp(opts: TestAppOpts) {
  return createApp({
    getSession: createMockGetSession(),
    authHandler: () => new Response("", { status: 404 }),
    db: createMockDB(opts.db),
    grantStore: createInMemoryGrantStore(opts.grants ?? ALL_CATALOG_GRANTS),
    sidecarRouter: createMockSidecarRouter(),
    sessionService: createMockSessionService(),
    eventCollectors: createMockEventCollectors(),
    assetService: null,
    repoStore: null,
    maxTarballBytes: 10_000_000,
  });
}

const providersURL = `/api/tenants/${TENANT_ID}/catalog/providers`;
const modelsURL = `/api/tenants/${TENANT_ID}/catalog/models`;
const offeringsURL = `/api/tenants/${TENANT_ID}/catalog/offerings`;

function postJSON(
  app: ReturnType<typeof createTestApp>,
  url: string,
  body: unknown,
) {
  return app.request(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

function patchJSON(
  app: ReturnType<typeof createTestApp>,
  url: string,
  body: unknown,
) {
  return app.request(url, {
    method: "PATCH",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

describe("POST /catalog/providers", () => {
  test("rejects a body that sets both credentialId and walletId", async () => {
    const inserts: InsertCapture[] = [];
    const app = createTestApp({ db: { inserts } });
    const res = await postJSON(app, providersURL, {
      name: "anthropic",
      plugin: "anthropic",
      baseURL: "https://api.anthropic.com",
      credentialId: "crd_1",
      walletId: "wlt_1",
    });
    expect(res.status).toBe(400);
    expect(inserts.filter((i) => i.table === "model_provider")).toHaveLength(0);
  });

  test("rejects a body that sets neither credentialId nor walletId", async () => {
    const inserts: InsertCapture[] = [];
    const app = createTestApp({ db: { inserts } });
    const res = await postJSON(app, providersURL, {
      name: "anthropic",
      plugin: "anthropic",
      baseURL: "https://api.anthropic.com",
    });
    expect(res.status).toBe(400);
    expect(inserts.filter((i) => i.table === "model_provider")).toHaveLength(0);
  });

  test("creates a provider with exactly one auth binding", async () => {
    const inserts: InsertCapture[] = [];
    const app = createTestApp({ db: { inserts } });
    const res = await postJSON(app, providersURL, {
      name: "anthropic",
      plugin: "anthropic",
      baseURL: "https://api.anthropic.com",
      credentialId: "crd_1",
    });
    expect(res.status).toBe(201);
    const captured = inserts.filter((i) => i.table === "model_provider");
    expect(captured).toHaveLength(1);
    const row = captured[0]?.rows[0];
    expect(row).toMatchObject({
      tenantId: TENANT_ID,
      credentialId: "crd_1",
      walletId: null,
    });
  });

  test("returns 409 when the provider name already exists", async () => {
    const inserts: InsertCapture[] = [];
    const app = createTestApp({
      db: { inserts, modelProvider: { id: "mpv_existing" } },
    });
    const res = await postJSON(app, providersURL, {
      name: "anthropic",
      plugin: "anthropic",
      baseURL: "https://api.anthropic.com",
      credentialId: "crd_1",
    });
    expect(res.status).toBe(409);
    expect(inserts.filter((i) => i.table === "model_provider")).toHaveLength(0);
  });
});

describe("POST /catalog/models", () => {
  test("returns 409 when the canonical name already exists", async () => {
    const inserts: InsertCapture[] = [];
    const app = createTestApp({
      db: { inserts, model: { id: "mdl_existing" } },
    });
    const res = await postJSON(app, modelsURL, { canonicalName: "claude" });
    expect(res.status).toBe(409);
    expect(inserts.filter((i) => i.table === "model")).toHaveLength(0);
  });

  test("creates a tenant-local model", async () => {
    const inserts: InsertCapture[] = [];
    const app = createTestApp({ db: { inserts } });
    const res = await postJSON(app, modelsURL, {
      canonicalName: "claude",
      displayName: "Claude",
    });
    expect(res.status).toBe(201);
    const captured = inserts.filter((i) => i.table === "model");
    expect(captured).toHaveLength(1);
    expect(captured[0]?.rows[0]).toMatchObject({
      tenantId: TENANT_ID,
      canonicalName: "claude",
    });
  });
});

describe("POST /catalog/offerings", () => {
  test("returns 404 when the model is not owned by the tenant", async () => {
    const inserts: InsertCapture[] = [];
    // model.findFirst → undefined (not owned); provider present.
    const app = createTestApp({
      db: { inserts, modelProvider: { id: "mpv_1" } },
    });
    const res = await postJSON(app, offeringsURL, {
      modelId: "mdl_x",
      providerId: "mpv_1",
    });
    expect(res.status).toBe(404);
    expect(inserts.filter((i) => i.table === "model_offering")).toHaveLength(0);
  });

  test("returns 404 when the provider is not owned by the tenant", async () => {
    const inserts: InsertCapture[] = [];
    // model present; provider.findFirst → undefined.
    const app = createTestApp({
      db: { inserts, model: { id: "mdl_1" } },
    });
    const res = await postJSON(app, offeringsURL, {
      modelId: "mdl_1",
      providerId: "mpv_x",
    });
    expect(res.status).toBe(404);
    expect(inserts.filter((i) => i.table === "model_offering")).toHaveLength(0);
  });

  test("returns 409 when the model and provider are already paired", async () => {
    const inserts: InsertCapture[] = [];
    const app = createTestApp({
      db: {
        inserts,
        model: { id: "mdl_1" },
        modelProvider: { id: "mpv_1" },
        modelOffering: { id: "mof_existing" },
      },
    });
    const res = await postJSON(app, offeringsURL, {
      modelId: "mdl_1",
      providerId: "mpv_1",
    });
    expect(res.status).toBe(409);
    expect(inserts.filter((i) => i.table === "model_offering")).toHaveLength(0);
  });
});

describe("PATCH /catalog/providers/:id", () => {
  test("returns 409 when renaming to a name another provider holds", async () => {
    const inserts: InsertCapture[] = [];
    // The conflict pre-check finds a different provider with the target name.
    const app = createTestApp({
      db: { inserts, modelProvider: { id: "mpv_other" } },
    });
    const res = await patchJSON(app, `${providersURL}/mpv_self`, {
      name: "taken",
    });
    expect(res.status).toBe(409);
  });
});

describe("POST /catalog/offerings/:id/pricing", () => {
  const pricingURL = `${offeringsURL}/mof_1/pricing`;

  test("appends a pricing row to an owned offering", async () => {
    const inserts: InsertCapture[] = [];
    const app = createTestApp({
      db: { inserts, modelOffering: { id: "mof_1" } },
    });
    const res = await postJSON(app, pricingURL, {
      currency: "USD",
      inputTokenPrice: "0.000003",
    });
    expect(res.status).toBe(201);
    const captured = inserts.filter((i) => i.table === "model_pricing");
    expect(captured).toHaveLength(1);
    expect(captured[0]?.rows[0]).toMatchObject({
      tenantId: TENANT_ID,
      offeringId: "mof_1",
      currency: "USD",
      inputTokenPrice: "0.000003",
    });
  });

  test("returns 404 when the offering is not owned by the tenant", async () => {
    const inserts: InsertCapture[] = [];
    const app = createTestApp({ db: { inserts } });
    const res = await postJSON(app, pricingURL, { currency: "USD" });
    expect(res.status).toBe(404);
    expect(inserts.filter((i) => i.table === "model_pricing")).toHaveLength(0);
  });

  test("returns 409 for a duplicate currency and effective-from", async () => {
    const inserts: InsertCapture[] = [];
    const app = createTestApp({
      db: {
        inserts,
        modelOffering: { id: "mof_1" },
        modelPricing: { id: "mpr_existing" },
      },
    });
    const res = await postJSON(app, pricingURL, {
      currency: "USD",
      effectiveFrom: "2025-01-01T00:00:00Z",
    });
    expect(res.status).toBe(409);
    expect(inserts.filter((i) => i.table === "model_pricing")).toHaveLength(0);
  });

  test("returns 400 when effectiveFrom is not a valid timestamp", async () => {
    const inserts: InsertCapture[] = [];
    const app = createTestApp({
      db: { inserts, modelOffering: { id: "mof_1" } },
    });
    const res = await postJSON(app, pricingURL, {
      currency: "USD",
      effectiveFrom: "not-a-date",
    });
    expect(res.status).toBe(400);
    expect(inserts.filter((i) => i.table === "model_pricing")).toHaveLength(0);
  });
});

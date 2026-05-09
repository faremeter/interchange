import { describe, test, expect } from "bun:test";

import { createInMemoryGrantStore } from "@interchange/authz";
import type { GrantRule } from "@interchange/types/authz";
import type { SessionStatus } from "@interchange/types";

import { createApp } from "../app";
import type { Auth } from "../auth";
import type { EventCollectorRegistry } from "../event-collector-registry";
import type { SessionService } from "../session-service";
import type { SidecarRouter } from "../ws/sidecar-handler";

// ---------------------------------------------------------------------------
// Test data constants
// ---------------------------------------------------------------------------

const TENANT_ID = "tnt_test";
const PRINCIPAL_ID = "prn_test";
const USER_ID = "usr_test";
const INSTANCE_ID = "ins_test";
const AGENT_ID = "agt_test";
const ADDRESS = "ins_test@test.example.com";

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

const testInstance = {
  id: INSTANCE_ID,
  agentId: AGENT_ID,
  tenantId: TENANT_ID,
  address: ADDRESS,
  status: "running" as const,
  principalId: "prn_agent",
  kernelId: null,
  sidecarId: null,
  sessionId: "ses_test",
  publicKey: null,
  createdAt: new Date("2025-01-01"),
  updatedAt: new Date("2025-01-01"),
  endedAt: null,
};

const testAgent = { id: AGENT_ID, name: "Test Agent" };

function makeGrant(overrides: Partial<GrantRule> = {}): GrantRule {
  return {
    id: "grant-test",
    resource: "instance:*",
    action: "read",
    effect: "allow",
    origin: "system",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: PRINCIPAL_ID,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Mock factories
//
// Each test sets up exactly the canned data it expects. The mock DB does NOT
// evaluate drizzle where-clauses — it returns the canned data as-is. This
// is intentional: we're testing route behavior, not drizzle's query builder.
// If a test wants a 404, it omits the relevant data from the mock.
// ---------------------------------------------------------------------------

type TestInstance = Omit<typeof testInstance, "status" | "endedAt"> & {
  status: string;
  endedAt: Date | null;
};

type MockDBOpts = {
  tenant?: typeof testTenant | undefined;
  principal?: typeof testPrincipal | undefined;
  instance?: TestInstance | undefined;
  agent?: typeof testAgent | undefined;
  offerings?: Record<string, unknown>[] | undefined;
};

function notImplemented(path: string) {
  return () => {
    throw new Error(`mock: ${path} not implemented`);
  };
}

function createMockDB(opts: MockDBOpts) {
  // Builder chain for db.select().from().innerJoin().where().limit()
  // Simulates the instance+agent join used by the offerings handler.
  function selectChain() {
    const joinedRows =
      opts.instance && opts.agent
        ? [{ instance: opts.instance, agentName: opts.agent.name }]
        : [];

    return {
      from: () => ({
        innerJoin: () => ({
          where: () => ({
            limit: () => Promise.resolve(joinedRows),
            orderBy: (..._args: unknown[]) => ({
              limit: () => Promise.resolve(joinedRows),
            }),
          }),
        }),
      }),
    };
  }

  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase type cannot be structurally satisfied in tests
  return {
    query: {
      tenant: {
        findFirst: async () => opts.tenant,
        findMany: notImplemented("db.query.tenant.findMany"),
      },
      principal: {
        findFirst: async () => opts.principal,
        findMany: notImplemented("db.query.principal.findMany"),
      },
      agentInstance: {
        findFirst: async () => opts.instance,
        findMany: notImplemented("db.query.agentInstance.findMany"),
      },
      offering: {
        findFirst: notImplemented("db.query.offering.findFirst"),
        findMany: async () => opts.offerings ?? [],
      },
    },
    select: selectChain,
  } as unknown as Parameters<typeof createApp>[0]["db"];
}

function createMockAuth(userId: string) {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- betterAuth type cannot be structurally satisfied in tests
  return {
    api: {
      getSession: async () => ({
        user: { id: userId, email: "test@example.com", name: "Test User" },
        session: { id: "session_test" },
      }),
    },
    handler: async () => new Response("", { status: 404 }),
  } as unknown as Auth;
}

function createMockSidecarRouter(
  routableAddresses: string[] = [],
): SidecarRouter {
  function notImpl(name: string): never {
    throw new Error(`mock: sidecarRouter.${name} not implemented`);
  }
  return {
    handleOpen(_ws) {
      notImpl("handleOpen");
    },
    handleMessage(_ws, _data) {
      notImpl("handleMessage");
    },
    handleClose(_ws) {
      notImpl("handleClose");
    },
    routeMail(_addr, _msg) {
      return notImpl("routeMail");
    },
    sendAgentDeploy(_addr, _config) {
      return notImpl("sendAgentDeploy");
    },
    sendAgentUndeploy(_addr, _reason) {
      return notImpl("sendAgentUndeploy");
    },
    sendSessionStart(_addr) {
      return notImpl("sendSessionStart");
    },
    sendSessionAbort(_addr, _reason) {
      return notImpl("sendSessionAbort");
    },
    sendGrantsUpdate(_addr, _grants) {
      return notImpl("sendGrantsUpdate");
    },
    sendProvidersUpdate(_addr, _providers) {
      return notImpl("sendProvidersUpdate");
    },
    sendPack(_addr, _pack, _ref, _sha) {
      return notImpl("sendPack");
    },
    sendSyncRequest(_addr) {
      notImpl("sendSyncRequest");
    },
    subscribeAgent(_addr, _callback) {
      return notImpl("subscribeAgent");
    },
    dispatchAgentEvent(_addr, _event) {
      notImpl("dispatchAgentEvent");
    },
    getConnectedSidecars: () => [],
    getRoutableAddresses: () => routableAddresses,
  };
}

function createMockSessionService(): SessionService {
  function notImpl(name: string): never {
    throw new Error(`mock: sessionService.${name} not implemented`);
  }
  return {
    launchSession(_params) {
      return notImpl("launchSession");
    },
    sendUserMessage(_params) {
      return notImpl("sendUserMessage");
    },
    endSession(_addr, _reason) {
      return notImpl("endSession");
    },
  };
}

function createMockEventCollectors(
  statuses = new Map<string, SessionStatus>(),
): EventCollectorRegistry {
  return {
    create: notImplemented("eventCollectors.create"),
    dispatch: notImplemented("eventCollectors.dispatch"),
    abandon: notImplemented("eventCollectors.abandon"),
    has: (address) => statuses.has(address),
    getStatus: (address) => statuses.get(address),
    getAccumulatedText: () => undefined,
  };
}

type TestAppOpts = {
  db?: MockDBOpts;
  grants?: GrantRule[];
  routableAddresses?: string[];
  collectorStatuses?: Map<string, SessionStatus>;
};

function createTestApp(opts: TestAppOpts = {}) {
  const db = createMockDB(
    opts.db ?? {
      tenant: testTenant,
      principal: testPrincipal,
      instance: testInstance,
      agent: testAgent,
    },
  );

  return createApp({
    auth: createMockAuth(USER_ID),
    db,
    grantStore: createInMemoryGrantStore(opts.grants ?? [makeGrant()]),
    sidecarRouter: createMockSidecarRouter(opts.routableAddresses),
    sessionService: createMockSessionService(),
    eventCollectors: createMockEventCollectors(opts.collectorStatuses),
  });
}

function instanceURL(tenantId = TENANT_ID, instanceId = INSTANCE_ID): string {
  return `/api/tenants/${tenantId}/agents/instances/${instanceId}`;
}

// ---------------------------------------------------------------------------
// Smoke test — verifies the mock infrastructure satisfies the middleware chain
// ---------------------------------------------------------------------------

describe("instance route test infrastructure", () => {
  test("authenticated request reaches the route handler", async () => {
    const app = createTestApp();
    const res = await app.request(`${instanceURL()}/health`);
    expect(res.status).toBe(200);
  });

  test("missing grant returns 403", async () => {
    const app = createTestApp({ grants: [] });
    const res = await app.request(`${instanceURL()}/health`);
    expect(res.status).toBe(403);
  });
});

// ---------------------------------------------------------------------------
// Health endpoint tests
// ---------------------------------------------------------------------------

describe("GET /agents/instances/:instanceId/health", () => {
  test("returns ok/ok when address is routable and collector exists", async () => {
    const app = createTestApp({
      routableAddresses: [ADDRESS],
      collectorStatuses: new Map([[ADDRESS, { status: "idle" }]]),
    });

    const res = await app.request(`${instanceURL()}/health`);
    expect(res.status).toBe(200);

    const body = await res.json();
    expect(body).toEqual({
      liveness: "ok",
      readiness: "ok",
      lastCheckedAt: null,
    });
  });

  test("returns unhealthy/not_ready when not routable and no collector", async () => {
    const app = createTestApp({
      routableAddresses: [],
      collectorStatuses: new Map(),
    });

    const res = await app.request(`${instanceURL()}/health`);
    expect(res.status).toBe(200);

    const body = await res.json();
    expect(body).toEqual({
      liveness: "unhealthy",
      readiness: "not_ready",
      lastCheckedAt: null,
    });
  });

  test("returns ok/not_ready when routable but no collector", async () => {
    const app = createTestApp({
      routableAddresses: [ADDRESS],
      collectorStatuses: new Map(),
    });

    const res = await app.request(`${instanceURL()}/health`);
    expect(res.status).toBe(200);

    const body = await res.json();
    expect(body).toEqual({
      liveness: "ok",
      readiness: "not_ready",
      lastCheckedAt: null,
    });
  });

  test("returns unhealthy/ok when not routable but collector exists", async () => {
    const app = createTestApp({
      routableAddresses: [],
      collectorStatuses: new Map([[ADDRESS, { status: "busy" }]]),
    });

    const res = await app.request(`${instanceURL()}/health`);
    expect(res.status).toBe(200);

    const body = await res.json();
    expect(body).toEqual({
      liveness: "unhealthy",
      readiness: "ok",
      lastCheckedAt: null,
    });
  });

  test("returns 404 when instance does not exist", async () => {
    const app = createTestApp({
      db: {
        tenant: testTenant,
        principal: testPrincipal,
        instance: undefined,
        agent: testAgent,
      },
    });

    const res = await app.request(`${instanceURL()}/health`);
    expect(res.status).toBe(404);

    const body: unknown = await res.json();
    expect(body).toMatchObject({ error: { code: "not_found" } });
  });

  test("returns 410 when instance is stopped", async () => {
    const stoppedInstance = {
      ...testInstance,
      status: "stopped" as const,
      endedAt: new Date("2025-06-01"),
    };

    const app = createTestApp({
      db: {
        tenant: testTenant,
        principal: testPrincipal,
        instance: stoppedInstance,
        agent: testAgent,
      },
    });

    const res = await app.request(`${instanceURL()}/health`);
    expect(res.status).toBe(410);

    const body: unknown = await res.json();
    expect(body).toMatchObject({ error: { code: "gone" } });
  });
});

// ---------------------------------------------------------------------------
// Offerings endpoint tests
// ---------------------------------------------------------------------------

describe("GET /agents/instances/:instanceId/offerings", () => {
  test("returns offerings for the instance's agent definition", async () => {
    const offerings = [
      {
        id: "off_1",
        agentId: AGENT_ID,
        tenantId: TENANT_ID,
        name: "Translation",
        description: "Translate text",
        pricing: { base: { amount: "10", currency: "USD" } },
        schema: null,
        createdAt: new Date("2025-01-01"),
        updatedAt: new Date("2025-01-01"),
      },
      {
        id: "off_2",
        agentId: AGENT_ID,
        tenantId: TENANT_ID,
        name: "Summarization",
        description: null,
        pricing: null,
        schema: null,
        createdAt: new Date("2025-01-02"),
        updatedAt: new Date("2025-01-02"),
      },
    ];

    const app = createTestApp({
      db: {
        tenant: testTenant,
        principal: testPrincipal,
        instance: testInstance,
        agent: testAgent,
        offerings,
      },
    });

    const res = await app.request(`${instanceURL()}/offerings`);
    expect(res.status).toBe(200);

    const body: unknown = await res.json();
    expect(body).toHaveLength(2);
    expect(body).toMatchObject([
      { id: "off_1", agentName: "Test Agent", name: "Translation" },
      { id: "off_2", name: "Summarization" },
    ]);
  });

  test("returns empty array when no offerings exist", async () => {
    const app = createTestApp({
      db: {
        tenant: testTenant,
        principal: testPrincipal,
        instance: testInstance,
        agent: testAgent,
        offerings: [],
      },
    });

    const res = await app.request(`${instanceURL()}/offerings`);
    expect(res.status).toBe(200);

    const body = await res.json();
    expect(body).toEqual([]);
  });

  test("returns 404 when instance does not exist", async () => {
    const app = createTestApp({
      db: {
        tenant: testTenant,
        principal: testPrincipal,
        instance: undefined,
        agent: undefined,
      },
    });

    const res = await app.request(`${instanceURL()}/offerings`);
    expect(res.status).toBe(404);

    const body: unknown = await res.json();
    expect(body).toMatchObject({ error: { code: "not_found" } });
  });

  test("returns offerings for stopped instances", async () => {
    const stoppedInstance = {
      ...testInstance,
      status: "stopped" as const,
      endedAt: new Date("2025-06-01"),
    };

    const offerings = [
      {
        id: "off_1",
        agentId: AGENT_ID,
        tenantId: TENANT_ID,
        name: "Translation",
        description: "Translate text",
        pricing: null,
        schema: null,
        createdAt: new Date("2025-01-01"),
        updatedAt: new Date("2025-01-01"),
      },
    ];

    const app = createTestApp({
      db: {
        tenant: testTenant,
        principal: testPrincipal,
        instance: stoppedInstance,
        agent: testAgent,
        offerings,
      },
    });

    const res = await app.request(`${instanceURL()}/offerings`);
    expect(res.status).toBe(200);

    const body: unknown = await res.json();
    expect(body).toHaveLength(1);
    expect(body).toMatchObject([{ id: "off_1", agentName: "Test Agent" }]);
  });
});

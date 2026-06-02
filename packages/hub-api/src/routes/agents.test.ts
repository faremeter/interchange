import { describe, test, expect } from "bun:test";

import { createInMemoryGrantStore } from "@intx/authz";
import type { GrantRule } from "@intx/types/authz";

import { createApp } from "../app";
import {
  createSidecarEmitter,
  type EventCollectorRegistry,
  type SessionService,
  type SidecarRouter,
} from "@intx/hub-sessions";
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

function makeGrant(overrides: Partial<GrantRule> = {}): GrantRule {
  return {
    id: "grant-test",
    resource: "agent:*",
    action: "create",
    effect: "allow",
    origin: "system",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: PRINCIPAL_ID,
    ...overrides,
  };
}

type InsertCapture = {
  table: string;
  rows: Record<string, unknown>[];
};

function tableName(table: unknown): string {
  // Drizzle PgTable objects carry their name on a Symbol; the test only
  // needs a stable string label, so we cooperate with the Drizzle API by
  // reading a known own property when present and falling back to a
  // marker that lets a misconfigured test fail loudly.
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

type MockDBOpts = {
  tenant?: typeof testTenant;
  principal?: typeof testPrincipal;
  roles?: { id: string; name: string; tenantId: string }[];
  inserts: InsertCapture[];
};

function notImplemented(path: string) {
  return () => {
    throw new Error(`mock: ${path} not implemented`);
  };
}

function createMockDB(opts: MockDBOpts) {
  const roles = opts.roles ?? [];

  function insertChain(table: unknown) {
    const name = tableName(table);
    return {
      values: (
        rowsOrRow: Record<string, unknown> | Record<string, unknown>[],
      ) => {
        const rows = Array.isArray(rowsOrRow) ? rowsOrRow : [rowsOrRow];
        opts.inserts.push({ table: name, rows });
        return {
          returning: () => Promise.resolve(rows),
          then: (resolve: (v: undefined) => unknown) => resolve(undefined),
        };
      },
    };
  }

  const txLike = {
    insert: insertChain,
  };

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
      role: {
        findFirst: notImplemented("db.query.role.findFirst"),
        findMany: async () => roles,
      },
      agent: {
        findFirst: notImplemented("db.query.agent.findFirst"),
        findMany: notImplemented("db.query.agent.findMany"),
      },
      agentRole: {
        findFirst: notImplemented("db.query.agentRole.findFirst"),
        findMany: notImplemented("db.query.agentRole.findMany"),
      },
    },
    transaction: async (fn: (tx: typeof txLike) => Promise<unknown>) =>
      fn(txLike),
    insert: insertChain,
  } as unknown as Parameters<typeof createApp>[0]["db"];
}

function createMockGetSession(userId: string): GetSession {
  const now = new Date("2025-01-01");
  return async () => ({
    user: {
      id: userId,
      email: "test@example.com",
      emailVerified: true,
      name: "Test User",
      createdAt: now,
      updatedAt: now,
    },
    session: {
      id: "session_test",
      userId,
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
    sendSourcesUpdate: () => notImpl("sendSourcesUpdate"),
    sendPack: () => notImpl("sendPack"),
    sendSyncRequest: () => notImpl("sendSyncRequest"),
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
  db: MockDBOpts;
  grants?: GrantRule[];
};

function createTestApp(opts: TestAppOpts) {
  const db = createMockDB(opts.db);

  return createApp({
    getSession: createMockGetSession(USER_ID),
    authHandler: () => new Response("", { status: 404 }),
    db,
    grantStore: createInMemoryGrantStore(opts.grants ?? [makeGrant()]),
    sidecarRouter: createMockSidecarRouter(),
    sessionService: createMockSessionService(),
    eventCollectors: createMockEventCollectors(),
    assetService: null,
    repoStore: null,
  });
}

const agentsURL = `/api/tenants/${TENANT_ID}/agents/definitions`;

describe("POST /agents/definitions", () => {
  test("seeds a creator-level agent-state read grant on the new agent", async () => {
    const inserts: InsertCapture[] = [];

    const app = createTestApp({
      db: {
        tenant: testTenant,
        principal: testPrincipal,
        inserts,
      },
    });

    const res = await app.request(agentsURL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ name: "Demo" }),
    });

    expect(res.status).toBe(201);

    const grantInserts = inserts.filter((i) => i.table === "grant");
    expect(grantInserts).toHaveLength(1);

    const insertedRows = grantInserts[0]?.rows ?? [];
    expect(insertedRows).toHaveLength(1);

    const agentInserts = inserts.filter((i) => i.table === "agent");
    expect(agentInserts).toHaveLength(1);
    const agentRow = agentInserts[0]?.rows[0];
    expect(agentRow).toBeDefined();
    const agentId = agentRow?.["id"];
    if (typeof agentId !== "string") {
      throw new Error("expected captured agent insert to carry a string id");
    }

    const grantRow = insertedRows[0];
    expect(grantRow).toMatchObject({
      tenantId: TENANT_ID,
      principalId: PRINCIPAL_ID,
      resource: `agent-state:${agentId}`,
      action: "read",
      effect: "allow",
      origin: "creator",
    });
  });

  test("grant insert is ordered relative to the agent insert", async () => {
    const inserts: InsertCapture[] = [];

    const app = createTestApp({
      db: {
        tenant: testTenant,
        principal: testPrincipal,
        inserts,
      },
    });

    const res = await app.request(agentsURL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ name: "Demo" }),
    });

    expect(res.status).toBe(201);

    const tables = inserts.map((i) => i.table);
    const agentIdx = tables.indexOf("agent");
    const grantIdx = tables.indexOf("grant");
    expect(agentIdx).toBeGreaterThanOrEqual(0);
    expect(grantIdx).toBeGreaterThan(agentIdx);
  });

  test("missing create grant rejects the request before any insert runs", async () => {
    const inserts: InsertCapture[] = [];

    const app = createTestApp({
      db: {
        tenant: testTenant,
        principal: testPrincipal,
        inserts,
      },
      grants: [],
    });

    const res = await app.request(agentsURL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ name: "Demo" }),
    });

    expect(res.status).toBe(403);

    const grantInserts = inserts.filter((i) => i.table === "grant");
    expect(grantInserts).toHaveLength(0);
  });
});

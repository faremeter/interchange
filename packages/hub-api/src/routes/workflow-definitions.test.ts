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
const DEF_ID = "wfd_test";

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
    resource: "workflow-definition:*",
    action: "manage",
    effect: "allow",
    origin: "system",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: PRINCIPAL_ID,
    ...overrides,
  };
}

function notImpl(path: string): never {
  throw new Error(`mock: ${path} not implemented`);
}

type Def = {
  id: string;
  tenantId: string;
  name: string;
  description: string | null;
  currentVersion: string;
  status: "deployed" | "stopped";
  grantRequirements: null;
  modelRequirements: null;
  createdAt: Date;
  updatedAt: Date;
};

type MockDBOpts = {
  // Definition returned inside rollback's transaction (undefined -> 404).
  definition?: Def | undefined;
  // Target version row found inside the transaction (undefined -> 400).
  targetVersion?: { version: string } | undefined;
  // Rows the versions list returns.
  versions?: Record<string, unknown>[] | undefined;
};

function makeDef(overrides: Partial<Def> = {}): Def {
  return {
    id: DEF_ID,
    tenantId: TENANT_ID,
    name: "My Definition",
    description: null,
    currentVersion: "2",
    status: "deployed",
    grantRequirements: null,
    modelRequirements: null,
    createdAt: new Date("2025-01-01"),
    updatedAt: new Date("2025-01-02"),
    ...overrides,
  };
}

function createMockDB(opts: MockDBOpts) {
  function updateChain() {
    return {
      set: () => ({
        where: () => {
          const result = Promise.resolve();
          return Object.assign(result, {
            returning: () =>
              Promise.resolve([makeDef({ currentVersion: "1" })]),
          });
        },
      }),
    };
  }

  const txLike = {
    query: {
      workflowDefinition: { findFirst: async () => opts.definition },
      workflowDefinitionVersion: { findFirst: async () => opts.targetVersion },
    },
    update: updateChain,
  };

  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase type cannot be structurally satisfied in tests
  return {
    query: {
      tenant: {
        findFirst: async () => testTenant,
        findMany: () => notImpl("db.query.tenant.findMany"),
      },
      principal: {
        findFirst: async () => testPrincipal,
        findMany: () => notImpl("db.query.principal.findMany"),
      },
      workflowDefinition: {
        // The versions handler's tenant-scoped existence check; undefined
        // stands for a definition that is not in the URL tenant (another
        // tenant's, or nonexistent).
        findFirst: async () => opts.definition,
        findMany: () => notImpl("db.query.workflowDefinition.findMany"),
      },
      workflowDefinitionVersion: {
        findFirst: () =>
          notImpl("db.query.workflowDefinitionVersion.findFirst"),
        findMany: async () => opts.versions ?? [],
      },
    },
    transaction: async (fn: (tx: typeof txLike) => Promise<unknown>) =>
      fn(txLike),
  } as unknown as Parameters<typeof createApp>[0]["db"];
}

function createMockGetSession(): GetSession {
  const now = new Date("2025-01-01");
  return async () => ({
    user: {
      id: USER_ID,
      email: "t@example.com",
      emailVerified: true,
      name: "Test",
      createdAt: now,
      updatedAt: now,
    },
    session: {
      id: "session_test",
      userId: USER_ID,
      token: "tok",
      expiresAt: new Date("2999-01-01"),
      createdAt: now,
      updatedAt: now,
    },
  });
}

function createMockSidecarRouter(): SidecarRouter {
  return {
    handleOpen: () => notImpl("handleOpen"),
    handleMessage: () => notImpl("handleMessage"),
    handleClose: () => notImpl("handleClose"),
    routeMail: () => notImpl("routeMail"),
    sendRunGrants: () => notImpl("sendRunGrants"),
    sendAgentDeploy: () => notImpl("sendAgentDeploy"),
    sendAgentUndeploy: () => notImpl("sendAgentUndeploy"),
    sendSourcesUpdate: () => notImpl("sendSourcesUpdate"),
    sendCredentialsUpdate: () => notImpl("sendCredentialsUpdate"),
    sendPack: () => notImpl("sendPack"),
    sendProvisionStep: () => notImpl("sendProvisionStep"),
    bindStepRoute: () => notImpl("bindStepRoute"),
    unbindStepRoute: () => notImpl("unbindStepRoute"),
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
    stageWorkflowStep: () => notImpl("stageWorkflowStep"),
    deployWorkflowFromSource: () => notImpl("deployWorkflowFromSource"),
    sendUserMessage: () => notImpl("sendUserMessage"),
    endSession: () => notImpl("endSession"),
  };
}

function createMockEventCollectors(): EventCollectorRegistry {
  return {
    create: () => notImpl("eventCollectors.create"),
    dispatch: () => notImpl("eventCollectors.dispatch"),
    abandon: () => notImpl("eventCollectors.abandon"),
    has: () => false,
    getStatus: () => undefined,
    getAccumulatedText: () => undefined,
    getCurrentTurnId: () => undefined,
    getLastTurnId: () => undefined,
  };
}

function createTestApp(opts: { db: MockDBOpts; grants?: GrantRule[] }) {
  return createApp({
    getSession: createMockGetSession(),
    authHandler: () => new Response("", { status: 404 }),
    db: createMockDB(opts.db),
    grantStore: createInMemoryGrantStore(opts.grants ?? [makeGrant()]),
    sidecarRouter: createMockSidecarRouter(),
    sessionService: createMockSessionService(),
    eventCollectors: createMockEventCollectors(),
    assetService: null,
    repoStore: null,
    maxTarballBytes: 10_000_000,
  });
}

const defsURL = `/api/tenants/${TENANT_ID}/workflows/definitions`;

describe("GET /workflows/definitions/:definitionId/versions", () => {
  test("lists a definition's versions in order", async () => {
    const app = createTestApp({
      db: {
        definition: makeDef(),
        versions: [
          {
            id: "wdv_1",
            definitionId: DEF_ID,
            version: "1",
            status: "inactive",
            createdAt: new Date("2025-01-01"),
          },
          {
            id: "wdv_2",
            definitionId: DEF_ID,
            version: "2",
            status: "active",
            createdAt: new Date("2025-01-02"),
          },
        ],
      },
      grants: [makeGrant({ action: "read" })],
    });

    const res = await app.request(`${defsURL}/${DEF_ID}/versions`);
    expect(res.status).toBe(200);
    const body: unknown = await res.json();
    expect(body).toMatchObject({
      data: [
        { version: "1", status: "inactive" },
        { version: "2", status: "active" },
      ],
    });
  });

  test("404 without leaking versions when the definition is another tenant's", async () => {
    // The tenant-scoped definition lookup misses (the definition belongs to
    // another tenant), so the handler must 404 and never read, let alone
    // return, that definition's version history -- even though rows exist
    // for the id.
    const app = createTestApp({
      db: {
        definition: undefined,
        versions: [
          {
            id: "wdv_leak",
            definitionId: DEF_ID,
            version: "cross-tenant-secret",
            status: "active",
            createdAt: new Date("2025-01-02"),
          },
        ],
      },
      grants: [makeGrant({ action: "read" })],
    });

    const res = await app.request(`${defsURL}/${DEF_ID}/versions`);
    expect(res.status).toBe(404);
    const body: unknown = await res.json();
    expect(JSON.stringify(body)).not.toContain("cross-tenant-secret");
  });

  test("404 when the definition does not exist", async () => {
    const app = createTestApp({
      db: { definition: undefined, versions: [] },
      grants: [makeGrant({ action: "read" })],
    });
    const res = await app.request(`${defsURL}/${DEF_ID}/versions`);
    expect(res.status).toBe(404);
  });

  test("403 without a read grant", async () => {
    const app = createTestApp({ db: { versions: [] }, grants: [] });
    const res = await app.request(`${defsURL}/${DEF_ID}/versions`);
    expect(res.status).toBe(403);
  });
});

describe("POST /workflows/definitions/:definitionId/rollback", () => {
  async function rollback(app: ReturnType<typeof createTestApp>) {
    return app.request(`${defsURL}/${DEF_ID}/rollback`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ version: "1" }),
    });
  }

  test("rolls back and returns the updated definition", async () => {
    const app = createTestApp({
      db: { definition: makeDef(), targetVersion: { version: "1" } },
    });
    const res = await rollback(app);
    expect(res.status).toBe(200);
    const body: unknown = await res.json();
    expect(body).toMatchObject({ id: DEF_ID, currentVersion: "1" });
  });

  test("404 when the definition does not exist", async () => {
    const app = createTestApp({
      db: { definition: undefined, targetVersion: { version: "1" } },
    });
    const res = await rollback(app);
    expect(res.status).toBe(404);
  });

  test("400 when the target version does not exist", async () => {
    const app = createTestApp({
      db: { definition: makeDef(), targetVersion: undefined },
    });
    const res = await rollback(app);
    expect(res.status).toBe(400);
  });

  test("403 without a manage grant", async () => {
    const app = createTestApp({
      db: { definition: makeDef(), targetVersion: { version: "1" } },
      grants: [makeGrant({ action: "read" })],
    });
    const res = await rollback(app);
    expect(res.status).toBe(403);
  });
});

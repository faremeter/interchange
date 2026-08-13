import { describe, test, expect } from "bun:test";

import { createInMemoryGrantStore } from "@intx/authz";
import type { GrantRule } from "@intx/types/authz";
import type { SessionStatus } from "@intx/types";
import type { ConnectorThreadState } from "@intx/types/runtime";

import { createApp } from "../app";
import {
  agentSession,
  inferenceTurn,
  turnPart,
  workflowRun,
} from "@intx/db/schema";
import {
  createSidecarEmitter,
  type EventCollectorRegistry,
  type SessionService,
  type SidecarRouter,
} from "@intx/hub-sessions";
import type { GetSession } from "../session";

// ---------------------------------------------------------------------------
// Test data constants
// ---------------------------------------------------------------------------

const TENANT_ID = "tnt_test";
const PRINCIPAL_ID = "prn_test";
const USER_ID = "usr_test";
const INSTANCE_ID = "run_test";
const ADDRESS = "run_test@test.example.com";

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

// The definition the read routes key offerings and names on.
const testDefinition = {
  id: "wfd_test",
  name: "Test Agent",
  tenantId: TENANT_ID,
};

// A `workflow_run` as `findRoutableById`'s run query projects it. Its status is
// the run enum, which the read routes map onto the instance vocabulary.
function makeTestRun(overrides: Record<string, unknown> = {}) {
  return {
    id: INSTANCE_ID,
    anchorRunId: INSTANCE_ID,
    tenantId: TENANT_ID,
    address: ADDRESS,
    publicKey: null,
    status: "running",
    createdAt: new Date("2025-01-01"),
    endedAt: null,
    principalId: null,
    kernelId: null,
    sidecarId: null,
    definitionId: "wfd_test",
    ...overrides,
  };
}

function makeGrant(overrides: Partial<GrantRule> = {}): GrantRule {
  return {
    id: "grant-test",
    resource: "workflow-run:*",
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

type MockDBOpts = {
  tenant?: typeof testTenant | undefined;
  principal?: typeof testPrincipal | undefined;
  definition?: typeof testDefinition | undefined;
  /** A folded workflow_run row `findRoutableById`'s run query returns (with a
   * `definitionKind`). */
  run?: Record<string, unknown> | undefined;
  /** The session id `resolveRunSessionId` finds for a run's principal (used by
   * the mail routes). */
  runSessionId?: string | undefined;
  /** inference_turn rows the turns route returns for a run. */
  turns?: Record<string, unknown>[] | undefined;
  /** turn_part rows joined to those turns. */
  turnParts?: Record<string, unknown>[] | undefined;
  offerings?: Record<string, unknown>[] | undefined;
  /** Rows returned for the priorMail query used by POST /mail.
   * Defaults to `[]` (no prior session mail). */
  sessionMail?: { id: string }[];
  /** Captured rows passed to db.insert(sessionMail).values(...). */
  inserts?: Record<string, unknown>[];
};

function notImplemented(path: string) {
  return () => {
    throw new Error(`mock: ${path} not implemented`);
  };
}

function createMockDB(opts: MockDBOpts) {
  const sessionMailRows = opts.sessionMail ?? [];

  // Builder chain, distinguished by the target table `t`:
  //   - workflowRun: `findRoutableById`'s run query (.where().limit() -> the
  //     seeded run, or empty when none is seeded).
  //   - anything else (sessionMail): the priorMail query used by POST mail
  //     (.where().orderBy().limit()).
  function selectChain() {
    const runRows = opts.run ? [opts.run] : [];

    return {
      from: (t: unknown) => {
        if (t === workflowRun) {
          return {
            where: () => ({ limit: () => Promise.resolve(runRows) }),
          };
        }
        if (t === agentSession) {
          // resolveRunSessionId: .where().orderBy().limit()
          const sessionRows = opts.runSessionId
            ? [{ id: opts.runSessionId }]
            : [];
          return {
            where: () => ({
              orderBy: (..._args: unknown[]) => ({
                limit: () => Promise.resolve(sessionRows),
              }),
            }),
          };
        }
        if (t === inferenceTurn) {
          // turns route: .where().orderBy().limit()
          return {
            where: () => ({
              orderBy: (..._args: unknown[]) => ({
                limit: () => Promise.resolve(opts.turns ?? []),
              }),
            }),
          };
        }
        if (t === turnPart) {
          // turns route: .where().orderBy() (no limit)
          return {
            where: () => ({
              orderBy: (..._args: unknown[]) =>
                Promise.resolve(opts.turnParts ?? []),
            }),
          };
        }
        return {
          // priorMail (sessionMail): .where().orderBy().limit().
          where: () => ({
            orderBy: (..._args: unknown[]) => ({
              limit: () => Promise.resolve(sessionMailRows),
            }),
            limit: () => Promise.resolve(sessionMailRows),
          }),
        };
      },
    };
  }

  const insertCapture = opts.inserts;

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
      workflowDefinition: {
        findFirst: async () => opts.definition,
        findMany: notImplemented("db.query.workflowDefinition.findMany"),
      },
      offering: {
        findFirst: notImplemented("db.query.offering.findFirst"),
        findMany: async () => opts.offerings ?? [],
      },
    },
    select: selectChain,
    insert: () => ({
      values: (row: Record<string, unknown>) => {
        if (insertCapture !== undefined) {
          insertCapture.push(row);
        }
        return Promise.resolve();
      },
    }),
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

function createMockSidecarRouter(
  routableAddresses: string[] = [],
  connectorStates = new Map<string, ConnectorThreadState | null>(),
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
    sendRunGrants(_addr, _runId, _stepGrants) {
      return notImpl("sendRunGrants");
    },
    sendAgentDeploy(_addr, _config) {
      return notImpl("sendAgentDeploy");
    },
    sendAgentUndeploy(_addr, _reason) {
      return notImpl("sendAgentUndeploy");
    },
    sendSourcesUpdate(_addr, _sources, _defaultSource) {
      return notImpl("sendSourcesUpdate");
    },
    sendCredentialsUpdate(_addr, _delivery) {
      return notImpl("sendCredentialsUpdate");
    },
    sendPack(_addr, _pack, _ref, _sha) {
      return notImpl("sendPack");
    },
    sendProvisionStep(_agentAddress, _config) {
      return notImpl("sendProvisionStep");
    },
    bindStepRoute(_stepAddress) {
      notImpl("bindStepRoute");
    },
    unbindStepRoute(_stepAddress) {
      notImpl("unbindStepRoute");
    },
    sendSyncRequest(_addr) {
      notImpl("sendSyncRequest");
    },
    sendSignalDeliver(_opts) {
      notImpl("sendSignalDeliver");
    },
    sendDrain(_opts) {
      notImpl("sendDrain");
    },
    subscribeAgent(_addr, _callback) {
      return notImpl("subscribeAgent");
    },
    dispatchAgentEvent(_addr, _event) {
      // No-op default: many routes dispatch events but the tests don't
      // assert on them. Override at the test boundary if assertion is
      // needed.
    },
    getConnectedSidecars: () => [],
    getRoutableAddresses: () => routableAddresses,
    getConnectorState: (addr) => connectorStates.get(addr) ?? null,
    events: createSidecarEmitter(),
  };
}

function createMockSessionService(): SessionService {
  function notImpl(name: string): never {
    throw new Error(`mock: sessionService.${name} not implemented`);
  }
  return {
    stageWorkflowStep(_params) {
      return notImpl("stageWorkflowStep");
    },
    deployInstanceAtHead(_params) {
      return notImpl("deployInstanceAtHead");
    },
    deployWorkflowDefinition(_params) {
      return notImpl("deployWorkflowDefinition");
    },
    deploySingleStepAtHead(_params) {
      return notImpl("deploySingleStepAtHead");
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
    getCurrentTurnId: () => undefined,
    getLastTurnId: () => undefined,
  };
}

type TestAppOpts = {
  db?: MockDBOpts;
  grants?: GrantRule[];
  routableAddresses?: string[];
  connectorStates?: Map<string, ConnectorThreadState | null>;
  sessionService?: SessionService;
  collectorStatuses?: Map<string, SessionStatus>;
};

function createTestApp(opts: TestAppOpts = {}) {
  const db = createMockDB(
    opts.db ?? {
      tenant: testTenant,
      principal: testPrincipal,
      run: makeTestRun({ principalId: "prn_agent" }),
      runSessionId: "ses_test",
      definition: testDefinition,
    },
  );

  return createApp({
    getSession: createMockGetSession(USER_ID),
    authHandler: () => new Response("", { status: 404 }),
    db,
    grantStore: createInMemoryGrantStore(opts.grants ?? [makeGrant()]),
    sidecarRouter: createMockSidecarRouter(
      opts.routableAddresses,
      opts.connectorStates,
    ),
    sessionService: opts.sessionService ?? createMockSessionService(),
    eventCollectors: createMockEventCollectors(opts.collectorStatuses),
    assetService: null,
    repoStore: null,
    maxTarballBytes: 10_000_000,
  });
}

function instanceURL(tenantId = TENANT_ID, runId = INSTANCE_ID): string {
  return `/api/tenants/${tenantId}/workflows/runs/${runId}`;
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

describe("GET /workflows/runs/:runId/health", () => {
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

  test("returns 404 when the run does not exist", async () => {
    const app = createTestApp({
      db: {
        tenant: testTenant,
        principal: testPrincipal,
      },
    });

    const res = await app.request(`${instanceURL()}/health`);
    expect(res.status).toBe(404);

    const body: unknown = await res.json();
    expect(body).toMatchObject({ error: { code: "not_found" } });
  });
});

// ---------------------------------------------------------------------------
// Offerings endpoint tests
// ---------------------------------------------------------------------------

describe("GET /workflows/runs/:runId/offerings", () => {
  test("returns offerings for the instance's agent definition", async () => {
    const offerings = [
      {
        id: "off_1",
        definitionId: "wfd_test",
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
        definitionId: "wfd_test",
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
        run: makeTestRun(),
        definition: testDefinition,
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
        run: makeTestRun(),
        definition: testDefinition,
        offerings: [],
      },
    });

    const res = await app.request(`${instanceURL()}/offerings`);
    expect(res.status).toBe(200);

    const body = await res.json();
    expect(body).toEqual([]);
  });

  test("returns 404 when the run does not exist", async () => {
    const app = createTestApp({
      db: {
        tenant: testTenant,
        principal: testPrincipal,
      },
    });

    const res = await app.request(`${instanceURL()}/offerings`);
    expect(res.status).toBe(404);

    const body: unknown = await res.json();
    expect(body).toMatchObject({ error: { code: "not_found" } });
  });

  test("returns offerings for a stopped run", async () => {
    const offerings = [
      {
        id: "off_1",
        definitionId: "wfd_test",
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
        run: makeTestRun({
          status: "completed",
          endedAt: new Date("2025-06-01"),
        }),
        definition: testDefinition,
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

// ---------------------------------------------------------------------------
// Folded run read routes — a workflow_run served through the instance surface
// ---------------------------------------------------------------------------

describe("read routes serve a folded run", () => {
  // No agent_instance row; the run backs the address instead.
  function foldedApp(run: Record<string, unknown>) {
    return createTestApp({
      db: {
        tenant: testTenant,
        principal: testPrincipal,
        definition: testDefinition,
        run,
      },
    });
  }

  test("detail shapes a running run as a running instance", async () => {
    const res = await foldedApp(makeTestRun()).request(instanceURL());
    expect(res.status).toBe(200);
    const body: unknown = await res.json();
    expect(body).toMatchObject({
      id: INSTANCE_ID,
      definitionId: "wfd_test",
      definitionName: "Test Agent",
      address: ADDRESS,
      status: "running",
    });
  });

  test("detail maps a terminal run's status onto the instance vocabulary", async () => {
    const res = await foldedApp(makeTestRun({ status: "completed" })).request(
      instanceURL(),
    );
    expect(res.status).toBe(200);
    const body: unknown = await res.json();
    expect(body).toMatchObject({ status: "stopped" });
  });

  test("health 410s a terminal run, as it would a stopped instance", async () => {
    const res = await foldedApp(makeTestRun({ status: "cancelled" })).request(
      `${instanceURL()}/health`,
    );
    expect(res.status).toBe(410);
    const body: unknown = await res.json();
    expect(body).toMatchObject({ error: { code: "gone" } });
  });

  test("health serves a live run", async () => {
    const app = createTestApp({
      db: {
        tenant: testTenant,
        principal: testPrincipal,
        run: makeTestRun(),
      },
      routableAddresses: [ADDRESS],
    });
    const res = await app.request(`${instanceURL()}/health`);
    expect(res.status).toBe(200);
    const body: unknown = await res.json();
    expect(body).toMatchObject({ liveness: "ok" });
  });

  test("offerings resolve through the run's origin agent", async () => {
    const app = createTestApp({
      db: {
        tenant: testTenant,
        principal: testPrincipal,
        definition: testDefinition,
        run: makeTestRun(),
        offerings: [
          {
            id: "off_1",
            definitionId: "wfd_test",
            tenantId: TENANT_ID,
            name: "Translation",
            description: "Translate text",
            pricing: null,
            schema: null,
            createdAt: new Date("2025-01-01"),
            updatedAt: new Date("2025-01-01"),
          },
        ],
      },
    });
    const res = await app.request(`${instanceURL()}/offerings`);
    expect(res.status).toBe(200);
    const body: unknown = await res.json();
    expect(body).toMatchObject([
      { id: "off_1", agentName: "Test Agent", name: "Translation" },
    ]);
  });
});

// ---------------------------------------------------------------------------
// Folded run interact routes — mail send/list and turns for a workflow_run
// ---------------------------------------------------------------------------

describe("interactive routes are gated not-implemented for workflow runs", () => {
  function writeGrant(): GrantRule {
    return makeGrant({ resource: "workflow-run:*", action: "write" });
  }
  function readGrant(): GrantRule {
    return makeGrant({ resource: "workflow-run:*", action: "read" });
  }

  // Mail send/history, turns, the event stream, and stop were built for the
  // retired folded-launch surface. A workflow (anchor) run carries none of the
  // per-run session, collector, or terminal machinery they read, so each route
  // answers 501 not_implemented until its mapping lands (stop and mail history
  // are tracked as INTR-454). The routes stay mounted, not 404, so the admin UI
  // gets a clean answer.
  test("POST mail answers 501 not_implemented", async () => {
    const app = createTestApp({ grants: [writeGrant()] });
    const res = await app.request(`${instanceURL()}/mail`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ content: "hello run" }),
    });
    expect(res.status).toBe(501);
    expect(await res.json()).toMatchObject({
      error: { code: "not_implemented" },
    });
  });

  test("GET mail answers 501 not_implemented", async () => {
    const app = createTestApp({ grants: [readGrant()] });
    const res = await app.request(`${instanceURL()}/mail`);
    expect(res.status).toBe(501);
    expect(await res.json()).toMatchObject({
      error: { code: "not_implemented" },
    });
  });

  test("GET turns answers 501 not_implemented", async () => {
    const app = createTestApp({ grants: [readGrant()] });
    const res = await app.request(`${instanceURL()}/turns`);
    expect(res.status).toBe(501);
    expect(await res.json()).toMatchObject({
      error: { code: "not_implemented" },
    });
  });

  test("GET events answers 501 not_implemented", async () => {
    const app = createTestApp({ grants: [readGrant()] });
    const res = await app.request(`${instanceURL()}/events`);
    expect(res.status).toBe(501);
    expect(await res.json()).toMatchObject({
      error: { code: "not_implemented" },
    });
  });

  test("DELETE stop answers 501 not_implemented", async () => {
    const app = createTestApp({
      grants: [makeGrant({ resource: "workflow-run:*", action: "manage" })],
    });
    const res = await app.request(instanceURL(), { method: "DELETE" });
    expect(res.status).toBe(501);
    expect(await res.json()).toMatchObject({
      error: { code: "not_implemented" },
    });
  });
});

// ---------------------------------------------------------------------------
// Blob endpoint routing test
// ---------------------------------------------------------------------------

describe("GET /workflows/runs/blobs/:blobId", () => {
  test("blob route is reachable and not shadowed by /:runId", async () => {
    const app = createTestApp();
    const url = `/api/tenants/${TENANT_ID}/workflows/runs/blobs/bad-format`;
    const res = await app.request(url);

    // The blob handler rejects malformed IDs with 400.
    // If /:runId shadowed this route, we'd get 404 (no instance "blobs").
    expect(res.status).toBe(400);
    const body: unknown = await res.json();
    expect(body).toMatchObject({ error: { code: "bad_request" } });
  });
});

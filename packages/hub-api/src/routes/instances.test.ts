import { describe, test, expect } from "bun:test";

import { createInMemoryGrantStore } from "@intx/authz";
import {
  assembleSignedContent,
  assembleMessage,
  type MessageHeaders,
} from "@intx/mime";
import type { GrantRule } from "@intx/types/authz";
import { base64Encode } from "@intx/types";
import type { SessionStatus } from "@intx/types";
import type {
  ConnectorThreadState,
  MessageAttachment,
} from "@intx/types/runtime";

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
const INSTANCE_ID = "ins_test";
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

// Recovers the SQL table name a mock's `.from(table)` / `.update(table)` was
// called with, so a mock db can branch on which table a query targets. Drizzle
// stores the name under a documented symbol; there is no plain `.name`.
function drizzleTableName(table: unknown): string {
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

describe("interact routes serve a folded run", () => {
  function writeGrant(): GrantRule {
    return makeGrant({ resource: "workflow-run:*", action: "write" });
  }
  function readGrant(): GrantRule {
    return makeGrant({ resource: "workflow-run:*", action: "read" });
  }
  function sendingService(): SessionService {
    return {
      stageWorkflowStep() {
        throw new Error("not implemented");
      },
      deployInstanceAtHead() {
        throw new Error("not implemented");
      },
      deployWorkflowDefinition() {
        throw new Error("not implemented");
      },
      deploySingleStepAtHead() {
        throw new Error("not implemented");
      },
      endSession() {
        throw new Error("not implemented");
      },
      sendUserMessage() {
        return Promise.resolve(new Uint8Array([1, 2, 3]));
      },
    };
  }

  test("POST mail on a running run persists on the run session with a null runId", async () => {
    const inserts: Record<string, unknown>[] = [];
    const app = createTestApp({
      grants: [writeGrant()],
      sessionService: sendingService(),
      db: {
        tenant: testTenant,
        principal: testPrincipal,
        run: makeTestRun({ principalId: "prn_run" }),
        runSessionId: "ses_run",
        inserts,
      },
    });

    const res = await app.request(`${instanceURL()}/mail`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ content: "hello run" }),
    });

    expect(res.status).toBe(201);
    const body: unknown = await res.json();
    // A run's mail anchors on its session and records no instance.
    expect(body).toMatchObject({ sessionId: "ses_run", runId: null });
    const mailInsert = inserts.find((r) => r["direction"] === "inbound");
    expect(mailInsert).toMatchObject({
      sessionId: "ses_run",
      runId: null,
    });
  });

  test("POST mail 409s a terminal run", async () => {
    const app = createTestApp({
      grants: [writeGrant()],
      sessionService: sendingService(),
      db: {
        tenant: testTenant,
        principal: testPrincipal,
        run: makeTestRun({ status: "completed", principalId: "prn_run" }),
        runSessionId: "ses_run",
      },
    });

    const res = await app.request(`${instanceURL()}/mail`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ content: "too late" }),
    });
    expect(res.status).toBe(409);
  });

  test("GET mail serves a terminated run's history via its ended session", async () => {
    const app = createTestApp({
      grants: [readGrant()],
      db: {
        tenant: testTenant,
        principal: testPrincipal,
        run: makeTestRun({
          status: "cancelled",
          endedAt: new Date("2025-02-01"),
          principalId: "prn_run",
        }),
        // The ended session is still resolvable by the run's principal.
        runSessionId: "ses_run",
      },
    });

    // A terminal run is served (not 404); with no seeded mail the page is empty
    // -- the point is that it resolves the ended session rather than 404ing.
    const res = await app.request(`${instanceURL()}/mail`);
    expect(res.status).toBe(200);
    const body: unknown = await res.json();
    expect(body).toMatchObject({ data: [] });
  });

  test("GET turns returns a run's inference turns keyed by the run id", async () => {
    const app = createTestApp({
      grants: [readGrant()],
      db: {
        tenant: testTenant,
        principal: testPrincipal,
        run: makeTestRun({ principalId: "prn_run" }),
        turns: [
          {
            id: "turn_1",
            sessionId: "ses_run",
            runId: INSTANCE_ID,
            model: "test-model",
            status: "completed",
            startedAt: new Date("2025-02-01"),
            endedAt: new Date("2025-02-01"),
          },
        ],
        turnParts: [],
      },
    });

    const res = await app.request(`${instanceURL()}/turns`);
    expect(res.status).toBe(200);
    const body: unknown = await res.json();
    expect(body).toMatchObject({
      data: [{ id: "turn_1", runId: INSTANCE_ID, model: "test-model" }],
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

// ---------------------------------------------------------------------------
// POST /:runId/mail — threading-header policy
// ---------------------------------------------------------------------------

describe("POST /workflows/runs/:runId/mail", () => {
  // The user's bare addr-spec is `${principal.refId}@${tenant.domain}`.
  const USER_ADDR = `${USER_ID}@${testTenant.domain}`;

  function makeMailGrant(): GrantRule {
    return makeGrant({ resource: "workflow-run:*", action: "write" });
  }

  type CapturedSendArgs = {
    inReplyTo?: string;
    references?: string[];
  };

  function captureSendUserMessage(): {
    service: SessionService;
    captured: CapturedSendArgs[];
  } {
    const captured: CapturedSendArgs[] = [];
    const service: SessionService = {
      stageWorkflowStep() {
        throw new Error("not implemented");
      },
      deployInstanceAtHead() {
        throw new Error("not implemented");
      },
      deployWorkflowDefinition() {
        throw new Error("not implemented");
      },
      deploySingleStepAtHead() {
        throw new Error("not implemented");
      },
      endSession() {
        throw new Error("not implemented");
      },
      sendUserMessage(params) {
        captured.push({
          ...(params.inReplyTo !== undefined
            ? { inReplyTo: params.inReplyTo }
            : {}),
          ...(params.references !== undefined
            ? { references: params.references }
            : {}),
        });
        return Promise.resolve(new Uint8Array([1, 2, 3]));
      },
    };
    return { service, captured };
  }

  async function postMail(app: ReturnType<typeof createTestApp>) {
    return app.request(`${instanceURL()}/mail`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ content: "hello agent" }),
    });
  }

  test("no active connector → no threading headers", async () => {
    const { service, captured } = captureSendUserMessage();
    const app = createTestApp({
      grants: [makeMailGrant()],
      sessionService: service,
      // connectorStates default empty → getConnectorState returns null
    });

    const res = await postMail(app);
    expect(res.status).toBe(201);
    expect(captured).toHaveLength(1);
    expect(captured[0]?.inReplyTo).toBeUndefined();
    expect(captured[0]?.references).toBeUndefined();
  });

  test("active connector started by the same user → user continues the thread", async () => {
    const { service, captured } = captureSendUserMessage();
    const connectorStates = new Map<string, ConnectorThreadState | null>();
    connectorStates.set(ADDRESS, {
      threadRoot: "<root@example.com>",
      lastMessageId: "<last@example.com>",
      replyTo: USER_ADDR,
      cc: [],
    });

    const app = createTestApp({
      grants: [makeMailGrant()],
      sessionService: service,
      connectorStates,
    });

    const res = await postMail(app);
    expect(res.status).toBe(201);
    expect(captured).toHaveLength(1);
    expect(captured[0]?.inReplyTo).toBe("<last@example.com>");
    expect(captured[0]?.references).toEqual(["<root@example.com>"]);
  });

  test("active connector started by another peer → user joins the same thread", async () => {
    // The connector is one durable shared thread per agent. A user
    // opening a session against an agent whose active thread was
    // started by another peer (a parent agent that launched this one,
    // a peer agent, a prior session by anyone else) joins that thread
    // — the agent's next connector.reply will then CC the prior
    // speaker alongside the user.
    const { service, captured } = captureSendUserMessage();
    const connectorStates = new Map<string, ConnectorThreadState | null>();
    connectorStates.set(ADDRESS, {
      threadRoot: "<root@example.com>",
      lastMessageId: "<last@example.com>",
      replyTo: "someone-else@example.com",
      cc: [],
    });

    const app = createTestApp({
      grants: [makeMailGrant()],
      sessionService: service,
      connectorStates,
    });

    const res = await postMail(app);
    expect(res.status).toBe(201);
    expect(captured).toHaveLength(1);
    expect(captured[0]?.inReplyTo).toBe("<last@example.com>");
    expect(captured[0]?.references).toEqual(["<root@example.com>"]);
  });

  test("session history takes precedence over the connector cache", async () => {
    const { service, captured } = captureSendUserMessage();
    const connectorStates = new Map<string, ConnectorThreadState | null>();
    connectorStates.set(ADDRESS, {
      threadRoot: "<root@example.com>",
      lastMessageId: "<connector-last@example.com>",
      replyTo: USER_ADDR,
      cc: [],
    });

    const app = createTestApp({
      grants: [makeMailGrant()],
      sessionService: service,
      connectorStates,
      db: {
        tenant: testTenant,
        principal: testPrincipal,
        run: makeTestRun({ principalId: "prn_agent" }),
        runSessionId: "ses_test",
        sessionMail: [{ id: "prior-1" }],
      },
    });

    const res = await postMail(app);
    expect(res.status).toBe(201);
    expect(captured).toHaveLength(1);
    expect(captured[0]?.inReplyTo).toBe(`<prior-1@${testTenant.domain}>`);
    expect(captured[0]?.references).toEqual([`<prior-1@${testTenant.domain}>`]);
  });
});

// ---------------------------------------------------------------------------
// POST /:runId/mail — attachment validation
// ---------------------------------------------------------------------------

describe("POST /workflows/runs/:runId/mail attachments", () => {
  function makeMailGrant(): GrantRule {
    return makeGrant({ resource: "workflow-run:*", action: "write" });
  }

  // A session service whose sendUserMessage assembles a real conversation
  // MIME from the params, so the route's response echoes the parsed
  // attachment metadata exactly as production does.
  function captureAttachmentSend(): {
    service: SessionService;
    captured: (MessageAttachment[] | undefined)[];
  } {
    const captured: (MessageAttachment[] | undefined)[] = [];
    const service: SessionService = {
      stageWorkflowStep() {
        throw new Error("not implemented");
      },
      deployInstanceAtHead() {
        throw new Error("not implemented");
      },
      deployWorkflowDefinition() {
        throw new Error("not implemented");
      },
      deploySingleStepAtHead() {
        throw new Error("not implemented");
      },
      endSession() {
        throw new Error("not implemented");
      },
      sendUserMessage(params) {
        captured.push(params.attachments);
        const content = assembleSignedContent({
          kind: "conversation",
          text: params.content,
          ...(params.attachments !== undefined
            ? { attachments: params.attachments }
            : {}),
        });
        const headers: MessageHeaders = {
          from: params.from,
          to: [params.agentAddress],
          cc: undefined,
          date: params.date,
          messageId: params.messageId,
          subject: undefined,
          inReplyTo: params.inReplyTo,
          references: params.references,
          mimeVersion: "1.0",
          interchangeType: "conversation.message",
          interchangeCorrelationId: undefined,
          interchangeTenantId: params.tenantId,
          interchangeAgentId: undefined,
          interchangeSessionId: params.sessionId,
          interchangeOfferingId: undefined,
          interchangeSchemaVersion: undefined,
          traceparent: undefined,
          tracestate: undefined,
        };
        const raw = assembleMessage(
          headers,
          content,
          new TextEncoder().encode("FAKE-SIG"),
        );
        return Promise.resolve(raw);
      },
    };
    return { service, captured };
  }

  function postMailWith(
    app: ReturnType<typeof createTestApp>,
    attachments: unknown[],
  ) {
    return app.request(`${instanceURL()}/mail`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ content: "hello agent", attachments }),
    });
  }

  test("valid attachment is decoded, forwarded, and echoed in the response", async () => {
    const { service, captured } = captureAttachmentSend();
    const app = createTestApp({
      grants: [makeMailGrant()],
      sessionService: service,
    });

    const data = base64Encode(
      new Uint8Array([0x89, 0x50, 0x4e, 0x47, 1, 2, 3]),
    );
    const res = await postMailWith(app, [
      { mimeType: "image/png", data, name: "shot.png" },
    ]);

    expect(res.status).toBe(201);
    expect(captured).toEqual([
      [
        {
          name: "shot.png",
          contentType: "image/png",
          data: new Uint8Array([0x89, 0x50, 0x4e, 0x47, 1, 2, 3]),
        },
      ],
    ]);

    const json = await res.json();
    expect(json).toMatchObject({
      attachments: [{ name: "shot.png", type: "image/png" }],
    });
  });

  test("disallowed mimeType yields structured disallowed_mime_type", async () => {
    const { service } = captureAttachmentSend();
    const app = createTestApp({
      grants: [makeMailGrant()],
      sessionService: service,
    });

    const data = base64Encode(new Uint8Array([1, 2, 3]));
    const res = await postMailWith(app, [{ mimeType: "image/tiff", data }]);

    expect(res.status).toBe(400);
    expect(await res.json()).toMatchObject({
      error: {
        code: "disallowed_mime_type",
        attachmentIndex: 0,
        mimeType: "image/tiff",
      },
    });
  });

  test("malformed base64 yields structured malformed_base64", async () => {
    const { service } = captureAttachmentSend();
    const app = createTestApp({
      grants: [makeMailGrant()],
      sessionService: service,
    });

    const res = await postMailWith(app, [
      { mimeType: "image/png", data: "@@@not-valid-base64@@@" },
    ]);

    expect(res.status).toBe(400);
    expect(await res.json()).toMatchObject({
      error: { code: "malformed_base64", attachmentIndex: 0 },
    });
  });

  test("an unsafe filename yields a structured 400, not a 502", async () => {
    const { service, captured } = captureAttachmentSend();
    const app = createTestApp({
      grants: [makeMailGrant()],
      sessionService: service,
    });

    const data = base64Encode(new Uint8Array([1, 2, 3]));
    const res = await postMailWith(app, [
      { mimeType: "image/png", data, name: 'a"b.png' },
    ]);

    // Rejected at the boundary before the message is ever assembled, so the
    // client sees a 400 with the structured code rather than a 502 from the
    // MIME assembler's header-safety guard.
    expect(res.status).toBe(400);
    expect(await res.json()).toMatchObject({
      error: { code: "invalid_attachment_name", attachmentIndex: 0 },
    });
    expect(captured).toHaveLength(0);
  });

  test("oversize attachment wins over total, reporting the offending index", async () => {
    const { service } = captureAttachmentSend();
    const app = createTestApp({
      grants: [makeMailGrant()],
      sessionService: service,
    });

    const small = base64Encode(new Uint8Array([1, 2, 3]));
    const oversize = base64Encode(new Uint8Array(11 * 1024 * 1024).fill(0x61));
    const res = await postMailWith(app, [
      { mimeType: "image/png", data: small },
      { mimeType: "image/png", data: oversize },
      { mimeType: "image/png", data: small },
    ]);

    expect(res.status).toBe(400);
    expect(await res.json()).toMatchObject({
      error: { code: "oversize_attachment", attachmentIndex: 1 },
    });
  });

  test("auth runs before attachment validation", async () => {
    const { service, captured } = captureAttachmentSend();
    const app = createTestApp({
      grants: [],
      sessionService: service,
    });

    // A disallowed attachment would be a 400 if validation ran first; with
    // no write grant the route must reject with its auth failure instead.
    const data = base64Encode(new Uint8Array([1, 2, 3]));
    const res = await postMailWith(app, [{ mimeType: "image/tiff", data }]);

    expect(res.status).toBe(403);
    expect(captured).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// DELETE /:runId — folded run (workflow_run) stop path
// ---------------------------------------------------------------------------

describe("DELETE /workflows/runs/:runId (folded run)", () => {
  type Update = { table: string; set: Record<string, unknown> };
  type EndCall = { address: string; reason: string };

  const RUN_ID = "ins_folded_run";
  const RUN_PRINCIPAL = "prn_run";
  const RUN_ADDRESS = `${RUN_ID}@${testTenant.domain}`;

  function makeRun(overrides: Record<string, unknown> = {}) {
    return {
      id: RUN_ID,
      tenantId: TENANT_ID,
      anchorRunId: null,
      definitionId: "wfd_folded",
      principalId: RUN_PRINCIPAL,
      address: RUN_ADDRESS,
      status: "running",
      publicKey: "pk-run",
      endedAt: null,
      ...overrides,
    };
  }

  // A db whose `select(workflow_run)` returns the seeded run and whose
  // `update(...)` records the (table, set) of every write, so the test can
  // assert the run, its principal, and its session are all flipped terminal.
  function createFoldedDeleteDB(opts: {
    run: Record<string, unknown> | undefined;
    updates: Update[];
  }) {
    function updateChain(table: unknown) {
      return {
        set: (values: Record<string, unknown>) => ({
          where: () => {
            opts.updates.push({
              table: drizzleTableName(table),
              set: values,
            });
            return Promise.resolve();
          },
        }),
      };
    }

    // The teardown writes run inside db.transaction; the tx exposes the same
    // capturing update as the db.
    const txLike = { update: updateChain };

    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase type cannot be structurally satisfied in tests
    return {
      query: {
        // The tenant/principal middleware resolves these before the route runs.
        tenant: {
          findFirst: async () => testTenant,
          findMany: notImplemented("db.query.tenant.findMany"),
        },
        principal: {
          findFirst: async () => testPrincipal,
          findMany: notImplemented("db.query.principal.findMany"),
        },
      },
      select: () => ({
        from: (table: unknown) => ({
          where: () => ({
            limit: () =>
              Promise.resolve(
                drizzleTableName(table) === "workflow_run" && opts.run
                  ? [opts.run]
                  : [],
              ),
          }),
        }),
      }),
      update: updateChain,
      transaction: async (fn: (tx: typeof txLike) => Promise<unknown>) =>
        fn(txLike),
    } as unknown as Parameters<typeof createApp>[0]["db"];
  }

  function createStopSessionService(calls: EndCall[]): SessionService {
    return {
      stageWorkflowStep: () => {
        throw new Error("mock: stageWorkflowStep not implemented");
      },
      deployInstanceAtHead: () => {
        throw new Error("mock: deployInstanceAtHead not implemented");
      },
      deployWorkflowDefinition: () => {
        throw new Error("mock: deployWorkflowDefinition not implemented");
      },
      deploySingleStepAtHead: () => {
        throw new Error("mock: deploySingleStepAtHead not implemented");
      },
      sendUserMessage: () => {
        throw new Error("mock: sendUserMessage not implemented");
      },
      endSession: (address, reason) => {
        calls.push({ address, reason });
        return Promise.resolve();
      },
    };
  }

  function stopApp(
    db: ReturnType<typeof createFoldedDeleteDB>,
    sessionService: SessionService,
    abandoned: string[],
  ) {
    return createApp({
      getSession: createMockGetSession(USER_ID),
      authHandler: () => new Response("", { status: 404 }),
      db,
      grantStore: createInMemoryGrantStore([
        makeGrant({ resource: "workflow-run:*", action: "manage" }),
      ]),
      sidecarRouter: createMockSidecarRouter(),
      sessionService,
      eventCollectors: {
        create: () => undefined,
        dispatch: notImplemented("eventCollectors.dispatch"),
        abandon: (address) => {
          abandoned.push(address);
        },
        has: () => false,
        getStatus: () => undefined,
        getAccumulatedText: () => undefined,
        getCurrentTurnId: () => undefined,
        getLastTurnId: () => undefined,
      },
      assetService: null,
      repoStore: null,
      maxTarballBytes: 10_000_000,
    });
  }

  async function stop(app: ReturnType<typeof stopApp>) {
    return app.request(`/api/tenants/${TENANT_ID}/workflows/runs/${RUN_ID}`, {
      method: "DELETE",
    });
  }

  test("stopping a running folded run flips it, its principal, and its session terminal", async () => {
    const updates: Update[] = [];
    const endCalls: EndCall[] = [];
    const abandoned: string[] = [];
    const app = stopApp(
      createFoldedDeleteDB({ run: makeRun(), updates }),
      createStopSessionService(endCalls),
      abandoned,
    );

    const res = await stop(app);

    expect(res.status).toBe(204);
    expect(endCalls).toEqual([
      { address: RUN_ADDRESS, reason: "instance_stopped" },
    ]);
    expect(abandoned).toEqual([RUN_ADDRESS]);

    const runUpdate = updates.find((u) => u.table === "workflow_run");
    expect(runUpdate?.set).toMatchObject({ status: "cancelled" });
    expect(runUpdate?.set["endedAt"]).toBeInstanceOf(Date);

    // The run's own principal is deactivated and its transitional session,
    // keyed by that principal, is ended.
    const principalUpdate = updates.find((u) => u.table === "principal");
    expect(principalUpdate?.set).toMatchObject({ status: "deactivated" });

    const sessionUpdate = updates.find((u) => u.table === "agent_session");
    expect(sessionUpdate?.set).toMatchObject({ status: "ended" });
  });

  test("stopping an already-terminal folded run is a 409 with no writes", async () => {
    const updates: Update[] = [];
    const endCalls: EndCall[] = [];
    const abandoned: string[] = [];
    const app = stopApp(
      createFoldedDeleteDB({
        run: makeRun({ status: "cancelled", endedAt: new Date(0) }),
        updates,
      }),
      createStopSessionService(endCalls),
      abandoned,
    );

    const res = await stop(app);

    expect(res.status).toBe(409);
    expect(endCalls).toEqual([]);
    expect(updates).toEqual([]);
    expect(abandoned).toEqual([]);
  });

  test("a run with no address is not an instance the stop route owns (404)", async () => {
    const updates: Update[] = [];
    const endCalls: EndCall[] = [];
    const abandoned: string[] = [];
    const app = stopApp(
      createFoldedDeleteDB({
        run: makeRun({ address: null }),
        updates,
      }),
      createStopSessionService(endCalls),
      abandoned,
    );

    const res = await stop(app);

    expect(res.status).toBe(404);
    expect(endCalls).toEqual([]);
    expect(updates).toEqual([]);
    expect(abandoned).toEqual([]);
  });

  test("a deployment anchor run (workflow-derived address) is not stoppable here (404, no undeploy)", async () => {
    const updates: Update[] = [];
    const endCalls: EndCall[] = [];
    const abandoned: string[] = [];
    // The anchor run shares the deployment id and owns a workflow-derived
    // address. The stop route must report it absent rather than tear down the
    // live deployment via endSession.
    const app = stopApp(
      createFoldedDeleteDB({
        run: makeRun({ address: `ins_dep_anchor@${testTenant.domain}` }),
        updates,
      }),
      createStopSessionService(endCalls),
      abandoned,
    );

    const res = await stop(app);

    expect(res.status).toBe(404);
    expect(endCalls).toEqual([]);
    expect(updates).toEqual([]);
    expect(abandoned).toEqual([]);
  });

  test("a sidecar teardown failure returns 502 before any run write", async () => {
    const updates: Update[] = [];
    const abandoned: string[] = [];
    // endSession rejects: the sidecar-first ordering must surface 502 and
    // leave the run non-terminal (no writes, no abandon) so a retry re-drives.
    const throwingService: SessionService = {
      stageWorkflowStep: () => {
        throw new Error("mock: stageWorkflowStep not implemented");
      },
      deployInstanceAtHead: () => {
        throw new Error("mock: deployInstanceAtHead not implemented");
      },
      deployWorkflowDefinition: () => {
        throw new Error("mock: deployWorkflowDefinition not implemented");
      },
      deploySingleStepAtHead: () => {
        throw new Error("mock: deploySingleStepAtHead not implemented");
      },
      sendUserMessage: () => {
        throw new Error("mock: sendUserMessage not implemented");
      },
      endSession: () => Promise.reject(new Error("sidecar down")),
    };
    const app = stopApp(
      createFoldedDeleteDB({ run: makeRun(), updates }),
      throwingService,
      abandoned,
    );

    const res = await stop(app);

    expect(res.status).toBe(502);
    expect(updates).toEqual([]);
    expect(abandoned).toEqual([]);
  });

  test("stopping a folded run with no principal skips the principal and session writes", async () => {
    const updates: Update[] = [];
    const endCalls: EndCall[] = [];
    const abandoned: string[] = [];
    const app = stopApp(
      createFoldedDeleteDB({ run: makeRun({ principalId: null }), updates }),
      createStopSessionService(endCalls),
      abandoned,
    );

    const res = await stop(app);

    expect(res.status).toBe(204);
    // Only the run is settled; there is no own principal or session to end.
    expect(updates.map((u) => u.table)).toEqual(["workflow_run"]);
    expect(abandoned).toEqual([RUN_ADDRESS]);
  });
});

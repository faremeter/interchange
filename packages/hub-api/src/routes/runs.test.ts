import { describe, test, expect } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";

import { createInMemoryGrantStore } from "@intx/authz";
import type { GrantRule } from "@intx/types/authz";
import type { SessionStatus } from "@intx/types";
import type { ConnectorThreadState } from "@intx/types/runtime";

import { createApp } from "../app";
import { agentSession, workflowRun } from "@intx/db/schema";
import {
  createSidecarEmitter,
  type AssetService,
  type EventCollectorRegistry,
  type RepoStore,
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
const RUN_ID = "run_test";
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

// A top-level `workflow_run` (self-anchored: `anchorRunId === id`, with a
// routing address) as `findRoutableById`'s run query projects it. Its status is
// the run enum, which the read routes map onto the run-view vocabulary.
function makeTestRun(overrides: Record<string, unknown> = {}) {
  return {
    id: RUN_ID,
    anchorRunId: RUN_ID,
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
  /** A top-level workflow_run row `findRoutableById`'s run query returns. */
  run?: Record<string, unknown> | undefined;
  /** The session id `resolveRunSessionId` finds for a run's principal (used by
   * the mail routes). */
  runSessionId?: string | undefined;
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

// ---------------------------------------------------------------------------
// Run-event log fixtures
//
// The turns/events routes read the durable, git-backed run-event log through
// the workflow-run reader, which consults only `RepoStore.getRepoDir`. These
// helpers seed a temp git repo with a run's committed events and a RepoStore
// stub that points the reader at it; every other RepoStore method throws so a
// read path that drifts onto an unwired method fails loudly.
// ---------------------------------------------------------------------------

async function seedRunEventsRepo(
  runId: string,
  events: { seq: number; type: string; body?: Record<string, unknown> }[],
): Promise<string> {
  const dir = await fs.promises.mkdtemp(path.join(os.tmpdir(), "runs-route-"));
  // The reader resolves refs/heads/main, so the seed must commit on main;
  // isomorphic-git otherwise initializes on master and the reader reads nothing.
  await git.init({ fs, dir, defaultBranch: "main" });
  for (const e of events) {
    const rel = `runs/${runId}/events/${e.seq}.json`;
    const full = path.join(dir, rel);
    await fs.promises.mkdir(path.dirname(full), { recursive: true });
    await fs.promises.writeFile(
      full,
      JSON.stringify({ seq: e.seq, type: e.type, ...(e.body ?? {}) }),
    );
    await git.add({ fs, dir, filepath: rel });
  }
  await git.commit({
    fs,
    dir,
    message: "seed run events",
    author: { name: "test", email: "test@example.com" },
  });
  return dir;
}

function createReaderRepoStore(dir: string): RepoStore {
  const unused = () =>
    Promise.reject(new Error("runs route test: RepoStore method not wired"));
  return {
    initRepo: unused,
    writeTree: unused,
    writeTreePreservingPrefix: unused,
    writeTreeDelta: unused,
    receivePack: unused,
    createPack: unused,
    commitPackedTip: () => {
      throw new Error("runs route test: RepoStore method not wired");
    },
    resolveRef: unused,
    listRefs: unused,
    resolveHead: unused,
    getRepoDir: () => dir,
    openCommittedReads: unused,
    openCommittedReadsAtCommit: unused,
    subscribe: () => {
      throw new Error("runs route test: RepoStore.subscribe not wired");
    },
  };
}

// The turns/events routes never call the asset service; it exists only to
// satisfy the app.ts XOR that keeps assetService and repoStore a unit.
function createThrowingAssetService(): AssetService {
  const notWired = (name: string) => (): never => {
    throw new Error(`runs route test: AssetService.${name} not wired`);
  };
  return {
    createAsset: notWired("createAsset"),
    populateAsset: notWired("populateAsset"),
    readAssetBlob: notWired("readAssetBlob"),
    listAssetBlobs: notWired("listAssetBlobs"),
  };
}

type TestAppOpts = {
  db?: MockDBOpts;
  grants?: GrantRule[];
  routableAddresses?: string[];
  connectorStates?: Map<string, ConnectorThreadState | null>;
  sessionService?: SessionService;
  collectorStatuses?: Map<string, SessionStatus>;
  /** When set, the run routes read the durable run-event log through this
   * store; app.ts also gets a throwing asset service to satisfy its XOR. */
  repoStore?: RepoStore;
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

  const repoStore = opts.repoStore ?? null;
  // The app.ts XOR keeps assetService and repoStore moving as a unit, so a
  // seeded run-event store comes with a throwing asset service.
  const assetService = repoStore !== null ? createThrowingAssetService() : null;

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
    assetService,
    repoStore,
    maxTarballBytes: 10_000_000,
  });
}

function runURL(tenantId = TENANT_ID, runId = RUN_ID): string {
  return `/api/tenants/${tenantId}/workflows/runs/${runId}`;
}

// ---------------------------------------------------------------------------
// Smoke test — verifies the mock infrastructure satisfies the middleware chain
// ---------------------------------------------------------------------------

describe("run route test infrastructure", () => {
  test("authenticated request reaches the route handler", async () => {
    const app = createTestApp();
    const res = await app.request(`${runURL()}/health`);
    expect(res.status).toBe(200);
  });

  test("missing grant returns 403", async () => {
    const app = createTestApp({ grants: [] });
    const res = await app.request(`${runURL()}/health`);
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

    const res = await app.request(`${runURL()}/health`);
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

    const res = await app.request(`${runURL()}/health`);
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

    const res = await app.request(`${runURL()}/health`);
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

    const res = await app.request(`${runURL()}/health`);
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

    const res = await app.request(`${runURL()}/health`);
    expect(res.status).toBe(404);

    const body: unknown = await res.json();
    expect(body).toMatchObject({ error: { code: "not_found" } });
  });
});

// ---------------------------------------------------------------------------
// Offerings endpoint tests
// ---------------------------------------------------------------------------

describe("GET /workflows/runs/:runId/offerings", () => {
  test("returns offerings for the run's workflow definition", async () => {
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

    const res = await app.request(`${runURL()}/offerings`);
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

    const res = await app.request(`${runURL()}/offerings`);
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

    const res = await app.request(`${runURL()}/offerings`);
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

    const res = await app.request(`${runURL()}/offerings`);
    expect(res.status).toBe(200);

    const body: unknown = await res.json();
    expect(body).toHaveLength(1);
    expect(body).toMatchObject([{ id: "off_1", agentName: "Test Agent" }]);
  });
});

// ---------------------------------------------------------------------------
// Read routes serve a top-level workflow run
// ---------------------------------------------------------------------------

describe("read routes serve a workflow run", () => {
  function runApp(run: Record<string, unknown>) {
    return createTestApp({
      db: {
        tenant: testTenant,
        principal: testPrincipal,
        definition: testDefinition,
        run,
      },
    });
  }

  test("detail shapes a running run as a running run view", async () => {
    const res = await runApp(makeTestRun()).request(runURL());
    expect(res.status).toBe(200);
    const body: unknown = await res.json();
    expect(body).toMatchObject({
      id: RUN_ID,
      definitionId: "wfd_test",
      definitionName: "Test Agent",
      address: ADDRESS,
      status: "running",
    });
  });

  test("detail maps a terminal run's status onto the run-view vocabulary", async () => {
    const res = await runApp(makeTestRun({ status: "completed" })).request(
      runURL(),
    );
    expect(res.status).toBe(200);
    const body: unknown = await res.json();
    expect(body).toMatchObject({ status: "stopped" });
  });

  test("health 410s a terminal run, as it would a stopped run", async () => {
    const res = await runApp(makeTestRun({ status: "cancelled" })).request(
      `${runURL()}/health`,
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
    const res = await app.request(`${runURL()}/health`);
    expect(res.status).toBe(200);
    const body: unknown = await res.json();
    expect(body).toMatchObject({ liveness: "ok" });
  });

  test("offerings resolve through the run's workflow definition", async () => {
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
    const res = await app.request(`${runURL()}/offerings`);
    expect(res.status).toBe(200);
    const body: unknown = await res.json();
    expect(body).toMatchObject([
      { id: "off_1", agentName: "Test Agent", name: "Translation" },
    ]);
  });
});

// ---------------------------------------------------------------------------
// Run interact routes — mail send routes through the Trigger path; stop and
// mail history stay gated (INTR-454)
// ---------------------------------------------------------------------------

describe("mail send delegates to the trigger; stop and mail history stay gated", () => {
  function manageGrant(): GrantRule {
    return makeGrant({ resource: "workflow-run:*", action: "manage" });
  }
  function readGrant(): GrantRule {
    return makeGrant({ resource: "workflow-run:*", action: "read" });
  }

  // Mail send is wired: it resolves the run (a tenant-scoped 404 for an unknown
  // id) and fires it through the run's workflow-native Trigger path. When the
  // hub runs without the workflow substrate (the default test app has no
  // repoStore/assetService), the trigger is unavailable and the route answers
  // 503 -- honestly unavailable, never the old 501 stub and never a silent
  // no-op.
  test("POST mail 404s for an unknown run", async () => {
    const app = createTestApp({
      grants: [manageGrant()],
      db: { tenant: testTenant, principal: testPrincipal },
    });
    const res = await app.request(`${runURL()}/mail`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ content: "hello run" }),
    });
    expect(res.status).toBe(404);
    expect(await res.json()).toMatchObject({ error: { code: "not_found" } });
  });

  test("POST mail resolves the run and delegates to the trigger", async () => {
    const app = createTestApp({ grants: [manageGrant()] });
    const res = await app.request(`${runURL()}/mail`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ content: "hello run" }),
    });
    // No longer 501: the run resolves, and the trigger is consulted. Without the
    // workflow substrate the default app cannot fire, so it answers 503.
    expect(res.status).toBe(503);
    expect(await res.json()).toMatchObject({ error: { code: "unavailable" } });
  });

  test("GET mail answers 501 not_implemented", async () => {
    const app = createTestApp({ grants: [readGrant()] });
    const res = await app.request(`${runURL()}/mail`);
    expect(res.status).toBe(501);
    expect(await res.json()).toMatchObject({
      error: { code: "not_implemented" },
    });
  });

  test("DELETE stop answers 501 not_implemented", async () => {
    const app = createTestApp({
      grants: [manageGrant()],
    });
    const res = await app.request(runURL(), { method: "DELETE" });
    expect(res.status).toBe(501);
    expect(await res.json()).toMatchObject({
      error: { code: "not_implemented" },
    });
  });
});

// ---------------------------------------------------------------------------
// Turns + events serve the run's durable event log
// ---------------------------------------------------------------------------

describe("turns and events serve the run's committed event log", () => {
  function readGrant(): GrantRule {
    return makeGrant({ resource: "workflow-run:*", action: "read" });
  }

  const seededEvents = [
    { seq: 0, type: "RunStarted", body: { consumedMessageId: "m1" } },
    { seq: 1, type: "StepStarted", body: { stepId: "step1" } },
    { seq: 2, type: "RunCompleted" as const, body: {} },
  ];

  async function seededApp() {
    const dir = await seedRunEventsRepo(RUN_ID, seededEvents);
    return createTestApp({
      grants: [readGrant()],
      db: {
        tenant: testTenant,
        principal: testPrincipal,
        definition: testDefinition,
        run: makeTestRun(),
      },
      repoStore: createReaderRepoStore(dir),
    });
  }

  test("GET events returns the seq-ordered run event log", async () => {
    const app = await seededApp();
    const res = await app.request(`${runURL()}/events`);
    expect(res.status).toBe(200);
    const body: unknown = await res.json();
    expect(body).toMatchObject({
      runId: RUN_ID,
      events: [
        { seq: 0, type: "RunStarted", body: { consumedMessageId: "m1" } },
        { seq: 1, type: "StepStarted", body: { stepId: "step1" } },
        { seq: 2, type: "RunCompleted" },
      ],
    });
  });

  test("GET turns returns the same committed event log", async () => {
    const app = await seededApp();
    const res = await app.request(`${runURL()}/turns`);
    expect(res.status).toBe(200);
    const body: unknown = await res.json();
    expect(body).toMatchObject({
      runId: RUN_ID,
      events: [
        { seq: 0, type: "RunStarted" },
        { seq: 1, type: "StepStarted" },
        { seq: 2, type: "RunCompleted" },
      ],
    });
  });

  test("GET events returns an empty log for a not-yet-triggered run", async () => {
    // The run exists but has committed no events, so the reader finds no
    // runs/<runId>/ tree -- an honestly empty timeline, not a 404 or 503.
    const dir = await seedRunEventsRepo("run_other", seededEvents);
    const app = createTestApp({
      grants: [readGrant()],
      db: {
        tenant: testTenant,
        principal: testPrincipal,
        definition: testDefinition,
        run: makeTestRun(),
      },
      repoStore: createReaderRepoStore(dir),
    });
    const res = await app.request(`${runURL()}/events`);
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ runId: RUN_ID, events: [] });
  });

  test("GET events 404s an unknown run", async () => {
    const dir = await seedRunEventsRepo(RUN_ID, seededEvents);
    const app = createTestApp({
      grants: [readGrant()],
      db: { tenant: testTenant, principal: testPrincipal },
      repoStore: createReaderRepoStore(dir),
    });
    const res = await app.request(`${runURL()}/events`);
    expect(res.status).toBe(404);
    expect(await res.json()).toMatchObject({ error: { code: "not_found" } });
  });

  test("GET events 503s when the run-event substrate is absent", async () => {
    // createTestApp defaults repoStore to null (no deploy surface), so the
    // reader is unavailable and the route errors clearly rather than serving an
    // empty log as if it were real state.
    const app = createTestApp({ grants: [readGrant()] });
    const res = await app.request(`${runURL()}/events`);
    expect(res.status).toBe(503);
    expect(await res.json()).toMatchObject({ error: { code: "unavailable" } });
  });

  test("GET turns 503s when the run-event substrate is absent", async () => {
    const app = createTestApp({ grants: [readGrant()] });
    const res = await app.request(`${runURL()}/turns`);
    expect(res.status).toBe(503);
    expect(await res.json()).toMatchObject({ error: { code: "unavailable" } });
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
    // If /:runId shadowed this route, we'd get 404 (no run "blobs").
    expect(res.status).toBe(400);
    const body: unknown = await res.json();
    expect(body).toMatchObject({ error: { code: "bad_request" } });
  });
});

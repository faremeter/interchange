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

  // Builder chain that handles two shapes:
  //   1. .from().innerJoin().where().{limit | orderBy().limit()} — the
  //      instance+agent join used by the offerings handler.
  //   2. .from().where().orderBy().limit() — the priorMail query used by
  //      POST /:instanceId/mail.
  // The mock distinguishes them by whether innerJoin is on the path.
  function selectChain() {
    const joinedRows =
      opts.instance && opts.agent
        ? [{ instance: opts.instance, agentName: opts.agent.name }]
        : [];

    return {
      from: () => ({
        // join-shaped chain
        innerJoin: () => ({
          where: () => ({
            limit: () => Promise.resolve(joinedRows),
            orderBy: (..._args: unknown[]) => ({
              limit: () => Promise.resolve(joinedRows),
            }),
          }),
        }),
        // non-join chain (priorMail)
        where: () => ({
          orderBy: (..._args: unknown[]) => ({
            limit: () => Promise.resolve(sessionMailRows),
          }),
          limit: () => Promise.resolve(sessionMailRows),
        }),
      }),
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
    sendAgentDeploy(_addr, _config) {
      return notImpl("sendAgentDeploy");
    },
    sendAgentUndeploy(_addr, _reason) {
      return notImpl("sendAgentUndeploy");
    },
    sendSourcesUpdate(_addr, _sources, _defaultSource) {
      return notImpl("sendSourcesUpdate");
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
      instance: testInstance,
      agent: testAgent,
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

// ---------------------------------------------------------------------------
// Blob endpoint routing test
// ---------------------------------------------------------------------------

describe("GET /agents/instances/blobs/:blobId", () => {
  test("blob route is reachable and not shadowed by /:instanceId", async () => {
    const app = createTestApp();
    const url = `/api/tenants/${TENANT_ID}/agents/instances/blobs/bad-format`;
    const res = await app.request(url);

    // The blob handler rejects malformed IDs with 400.
    // If /:instanceId shadowed this route, we'd get 404 (no instance "blobs").
    expect(res.status).toBe(400);
    const body: unknown = await res.json();
    expect(body).toMatchObject({ error: { code: "bad_request" } });
  });
});

// ---------------------------------------------------------------------------
// POST /:instanceId/mail — threading-header policy
// ---------------------------------------------------------------------------

describe("POST /agents/instances/:instanceId/mail", () => {
  // The user's bare addr-spec is `${principal.refId}@${tenant.domain}`.
  const USER_ADDR = `${USER_ID}@${testTenant.domain}`;

  function makeMailGrant(): GrantRule {
    return makeGrant({ resource: "instance:*", action: "write" });
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
        instance: testInstance,
        agent: testAgent,
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
// POST /:instanceId/mail — attachment validation
// ---------------------------------------------------------------------------

describe("POST /agents/instances/:instanceId/mail attachments", () => {
  function makeMailGrant(): GrantRule {
    return makeGrant({ resource: "instance:*", action: "write" });
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
// POST /agents/instances — creator-grant seed on launch
//
// These tests exercise the launch transaction directly. The mock DB below is
// independent of the smaller mock used by the other suites in this file: it
// supports db.transaction, captures insert calls per table, and stubs the
// surface area of credential resolution (providers, credentials, ancestor
// chain) that the launch path traverses before reaching the transaction
// block. The single instance launched per test is the canonical fixture; we
// assert on the grant row written for resource `agent-state:<instanceId>`.
// ---------------------------------------------------------------------------

describe("POST /agents/instances seeds creator agent-state grant", () => {
  const CREATOR_ID = "prn_creator";
  const PROVIDER_ID = "prv_test";
  const CREDENTIAL_ID = "cred_test";
  const AGENT_DEF_ID = "agt_def";

  type TableInsert = { table: string; rows: Record<string, unknown>[] };

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

  type LaunchMockOpts = {
    agent: Record<string, unknown> | undefined;
    inserts: TableInsert[];
    provider?: Record<string, unknown> | undefined;
    credential?: Record<string, unknown> | undefined;
    model?: Record<string, unknown> | undefined;
    modelProvider?: Record<string, unknown> | undefined;
    modelOffering?: Record<string, unknown> | undefined;
  };

  function createLaunchMockDB(opts: LaunchMockOpts) {
    function insertChain(table: unknown) {
      const name = drizzleTableName(table);
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

    const txLike = { insert: insertChain };

    function updateChain() {
      return {
        set: () => ({
          where: () => {
            const result = Promise.resolve();
            return Object.assign(result, {
              returning: () =>
                Promise.resolve([
                  {
                    id: "ins_new",
                    agentId: AGENT_DEF_ID,
                    tenantId: TENANT_ID,
                    address: "ins_new@test.example.com",
                    status: "running",
                    principalId: "prn_instance",
                    kernelId: null,
                    sidecarId: null,
                    sessionId: "ses_new",
                    publicKey: null,
                    createdAt: new Date("2025-01-01"),
                    updatedAt: new Date("2025-01-01"),
                    endedAt: null,
                  },
                ]),
            });
          },
        }),
      };
    }

    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase type cannot be structurally satisfied in tests
    return {
      query: {
        tenant: {
          findFirst: async () => testTenant,
          findMany: notImplemented("db.query.tenant.findMany"),
        },
        principal: {
          findFirst: async () => testPrincipal,
          findMany: notImplemented("db.query.principal.findMany"),
        },
        agent: {
          findFirst: async () => opts.agent,
          findMany: notImplemented("db.query.agent.findMany"),
        },
        agentRole: {
          findFirst: notImplemented("db.query.agentRole.findFirst"),
          findMany: async () => [],
        },
        role: {
          findFirst: notImplemented("db.query.role.findFirst"),
          findMany: async () => [],
        },
        provider: {
          findFirst: async () => opts.provider,
          findMany: async () => (opts.provider ? [opts.provider] : []),
        },
        credential: {
          findFirst: async () => opts.credential,
          findMany: async () => (opts.credential ? [opts.credential] : []),
        },
        model: {
          findFirst: notImplemented("db.query.model.findFirst"),
          findMany: async () => (opts.model ? [opts.model] : []),
        },
        modelProvider: {
          findFirst: notImplemented("db.query.modelProvider.findFirst"),
          findMany: async () =>
            opts.modelProvider ? [opts.modelProvider] : [],
        },
        modelOffering: {
          findFirst: notImplemented("db.query.modelOffering.findFirst"),
          findMany: async () =>
            opts.modelOffering ? [opts.modelOffering] : [],
        },
      },
      transaction: async (fn: (tx: typeof txLike) => Promise<unknown>) =>
        fn(txLike),
      insert: insertChain,
      update: updateChain,
    } as unknown as Parameters<typeof createApp>[0]["db"];
  }

  function createLaunchGrantStore(): ReturnType<
    typeof createInMemoryGrantStore
  > {
    return createInMemoryGrantStore([
      // The invoking user holds an instance:* create grant.
      makeGrant({
        id: "g-instance-create",
        resource: "instance:*",
        action: "create",
      }),
    ]);
  }

  function makeAgentDef(): Record<string, unknown> {
    return {
      id: AGENT_DEF_ID,
      tenantId: TENANT_ID,
      creatorPrincipalId: CREATOR_ID,
      name: "Test Agent",
      description: null,
      systemPrompt: "You are a test agent.",
      contextConfig: null,
      initialState: null,
      modelConfig: { defaultModel: "test-model" },
      capabilities: null,
      credentialRequirements: null,
      modelRequirements: [{ model: "test-model" }],
      grantRequirements: null,
      toolPackages: [],
      currentVersion: "1",
      status: "deployed",
      createdAt: new Date("2025-01-01"),
      updatedAt: new Date("2025-01-01"),
    };
  }

  const MODEL_ID = "mdl_test";
  const MODEL_PROVIDER_ID = "mpv_test";
  const OFFERING_ID = "mof_test";

  function makeCatalogModel(): Record<string, unknown> {
    return {
      id: MODEL_ID,
      tenantId: TENANT_ID,
      canonicalName: "test-model",
      displayName: null,
      description: null,
      disabled: false,
      createdAt: new Date("2025-01-01"),
      updatedAt: new Date("2025-01-01"),
    };
  }

  function makeCatalogProvider(): Record<string, unknown> {
    return {
      id: MODEL_PROVIDER_ID,
      tenantId: TENANT_ID,
      name: "test-provider",
      plugin: "openai",
      baseURL: "https://api.test.example.com",
      credentialId: CREDENTIAL_ID,
      walletId: null,
      disabled: false,
      createdAt: new Date("2025-01-01"),
      updatedAt: new Date("2025-01-01"),
    };
  }

  function makeCatalogOffering(): Record<string, unknown> {
    return {
      id: OFFERING_ID,
      tenantId: TENANT_ID,
      modelId: MODEL_ID,
      providerId: MODEL_PROVIDER_ID,
      priority: 0,
      deploymentTags: [],
      capabilities: [],
      disabled: false,
      createdAt: new Date("2025-01-01"),
      updatedAt: new Date("2025-01-01"),
    };
  }

  function makeCredential(): Record<string, unknown> {
    return {
      id: CREDENTIAL_ID,
      tenantId: TENANT_ID,
      providerId: PROVIDER_ID,
      principalId: null,
      name: "test-cred",
      status: "active",
      scopes: null,
      secret: "sk-test",
    };
  }

  function createCapturingSessionService(): SessionService {
    return {
      stageWorkflowStep: async () => undefined,
      deployInstanceAtHead: async () => ({ publicKey: "pk-instance-mock" }),
      deployWorkflowDefinition: () => {
        throw new Error("mock: deployWorkflowDefinition not implemented");
      },
      deploySingleStepAtHead: () => {
        throw new Error("mock: deploySingleStepAtHead not implemented");
      },
      sendUserMessage: () => {
        throw new Error("mock: sendUserMessage not implemented");
      },
      endSession: () => {
        throw new Error("mock: endSession not implemented");
      },
    };
  }

  function createCapturingEventCollectors(): EventCollectorRegistry {
    return {
      create: () => undefined,
      dispatch: notImplemented("eventCollectors.dispatch"),
      abandon: () => undefined,
      has: () => false,
      getStatus: () => undefined,
      getAccumulatedText: () => undefined,
      getCurrentTurnId: () => undefined,
      getLastTurnId: () => undefined,
    };
  }

  test("launch transaction inserts agent-state read grant on creator", async () => {
    const inserts: TableInsert[] = [];

    const db = createLaunchMockDB({
      agent: makeAgentDef(),
      credential: makeCredential(),
      model: makeCatalogModel(),
      modelProvider: makeCatalogProvider(),
      modelOffering: makeCatalogOffering(),
      inserts,
    });

    const app = createApp({
      getSession: createMockGetSession(USER_ID),
      authHandler: () => new Response("", { status: 404 }),
      db,
      grantStore: createLaunchGrantStore(),
      sidecarRouter: createMockSidecarRouter(),
      sessionService: createCapturingSessionService(),
      eventCollectors: createCapturingEventCollectors(),
      assetService: null,
      repoStore: null,
      maxTarballBytes: 10_000_000,
    });

    const res = await app.request(
      `/api/tenants/${TENANT_ID}/agents/instances`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ agentId: AGENT_DEF_ID }),
      },
    );

    expect(res.status).toBe(201);

    const grantInserts = inserts.filter((i) => i.table === "grant");
    expect(grantInserts.length).toBeGreaterThan(0);

    const allGrantRows = grantInserts.flatMap((g) => g.rows);
    const stateGrant = allGrantRows.find(
      (g) =>
        typeof g["resource"] === "string" &&
        (g["resource"] as string).startsWith("agent-state:"),
    );

    expect(stateGrant).toBeDefined();
    expect(stateGrant).toMatchObject({
      tenantId: TENANT_ID,
      principalId: CREATOR_ID,
      action: "read",
      effect: "allow",
      origin: "creator",
    });

    const instanceInserts = inserts.filter((i) => i.table === "agent_instance");
    expect(instanceInserts).toHaveLength(1);
    const instanceRow = instanceInserts[0]?.rows[0];
    expect(instanceRow).toBeDefined();
    const instanceId = instanceRow?.["id"];
    if (typeof instanceId !== "string") {
      throw new Error(
        "expected captured agent_instance insert to carry a string id",
      );
    }
    expect(stateGrant?.["resource"]).toBe(`agent-state:${instanceId}`);
  });

  test("agent-state grant insert is ordered after the agent_instance insert", async () => {
    const inserts: TableInsert[] = [];

    const db = createLaunchMockDB({
      agent: makeAgentDef(),
      credential: makeCredential(),
      model: makeCatalogModel(),
      modelProvider: makeCatalogProvider(),
      modelOffering: makeCatalogOffering(),
      inserts,
    });

    const app = createApp({
      getSession: createMockGetSession(USER_ID),
      authHandler: () => new Response("", { status: 404 }),
      db,
      grantStore: createLaunchGrantStore(),
      sidecarRouter: createMockSidecarRouter(),
      sessionService: createCapturingSessionService(),
      eventCollectors: createCapturingEventCollectors(),
      assetService: null,
      repoStore: null,
      maxTarballBytes: 10_000_000,
    });

    const res = await app.request(
      `/api/tenants/${TENANT_ID}/agents/instances`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ agentId: AGENT_DEF_ID }),
      },
    );

    expect(res.status).toBe(201);

    // Walk the insert log: find the agent_instance row first, then the
    // first agent-state grant after it.
    let sawInstance = false;
    let sawStateGrantAfterInstance = false;
    for (const ins of inserts) {
      if (ins.table === "agent_instance") {
        sawInstance = true;
        continue;
      }
      if (!sawInstance) continue;
      if (ins.table === "grant") {
        for (const row of ins.rows) {
          if (
            typeof row["resource"] === "string" &&
            (row["resource"] as string).startsWith("agent-state:")
          ) {
            sawStateGrantAfterInstance = true;
            break;
          }
        }
      }
      if (sawStateGrantAfterInstance) break;
    }

    expect(sawInstance).toBe(true);
    expect(sawStateGrantAfterInstance).toBe(true);
  });

  function launchApp(db: ReturnType<typeof createLaunchMockDB>) {
    return createApp({
      getSession: createMockGetSession(USER_ID),
      authHandler: () => new Response("", { status: 404 }),
      db,
      grantStore: createLaunchGrantStore(),
      sidecarRouter: createMockSidecarRouter(),
      sessionService: createCapturingSessionService(),
      eventCollectors: createCapturingEventCollectors(),
      assetService: null,
      repoStore: null,
      maxTarballBytes: 10_000_000,
    });
  }

  async function launch(db: ReturnType<typeof createLaunchMockDB>) {
    return launchApp(db).request(`/api/tenants/${TENANT_ID}/agents/instances`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ agentId: AGENT_DEF_ID }),
    });
  }

  test("rejects launch when the agent declares no model requirements", async () => {
    const agent = makeAgentDef();
    agent["modelRequirements"] = null;
    const res = await launch(
      createLaunchMockDB({ agent, credential: makeCredential(), inserts: [] }),
    );
    expect(res.status).toBe(409);
    expect(JSON.stringify(await res.json())).toContain("no model requirements");
  });

  test("rejects launch when the only provider is wallet-backed", async () => {
    const walletProvider = makeCatalogProvider();
    walletProvider["credentialId"] = null;
    walletProvider["walletId"] = "wal_test";
    const res = await launch(
      createLaunchMockDB({
        agent: makeAgentDef(),
        model: makeCatalogModel(),
        modelProvider: walletProvider,
        modelOffering: makeCatalogOffering(),
        inserts: [],
      }),
    );
    expect(res.status).toBe(409);
    expect(JSON.stringify(await res.json())).toContain("wallet_backed");
  });

  test("passes catalog-ordered sources to deployInstanceAtHead and persists modelPreferences", async () => {
    const inserts: TableInsert[] = [];
    let launchedSources: unknown;
    let launchedDefaultSource: unknown;
    const sessionService: SessionService = {
      stageWorkflowStep: () => {
        throw new Error("mock: stageWorkflowStep not implemented");
      },
      deployInstanceAtHead: async (params) => {
        launchedSources = params.config.sources;
        launchedDefaultSource = params.config.defaultSource;
        return { publicKey: "pk-instance-mock" };
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
      endSession: () => {
        throw new Error("mock: endSession not implemented");
      },
    };

    const db = createLaunchMockDB({
      agent: makeAgentDef(),
      credential: makeCredential(),
      model: makeCatalogModel(),
      modelProvider: makeCatalogProvider(),
      modelOffering: makeCatalogOffering(),
      inserts,
    });

    const app = createApp({
      getSession: createMockGetSession(USER_ID),
      authHandler: () => new Response("", { status: 404 }),
      db,
      grantStore: createLaunchGrantStore(),
      sidecarRouter: createMockSidecarRouter(),
      sessionService,
      eventCollectors: createCapturingEventCollectors(),
      assetService: null,
      repoStore: null,
      maxTarballBytes: 10_000_000,
    });

    const preferences = [
      {
        model: "test-model",
        providers: { mode: "prefer", order: ["test-provider"] },
      },
    ];
    const res = await app.request(
      `/api/tenants/${TENANT_ID}/agents/instances`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          agentId: AGENT_DEF_ID,
          modelPreferences: preferences,
        }),
      },
    );

    expect(res.status).toBe(201);
    // The catalog-resolved source reaches the harness config verbatim, and
    // the head of the priority-ordered list is the default.
    expect(launchedSources).toEqual([
      {
        id: OFFERING_ID,
        provider: "openai",
        baseURL: "https://api.test.example.com",
        apiKey: "sk-test",
        model: "test-model",
        capabilities: [],
      },
    ]);
    expect(launchedDefaultSource).toBe(OFFERING_ID);

    // The invoker preference is persisted on the instance for re-resolution.
    const instanceRow = inserts.find((i) => i.table === "agent_instance")
      ?.rows[0];
    expect(instanceRow?.["modelPreferences"]).toEqual(preferences);
  });
});

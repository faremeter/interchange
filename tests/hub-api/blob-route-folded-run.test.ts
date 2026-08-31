import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { createInMemoryGrantStore } from "@intx/authz";
import { createApp, type GetSession } from "@intx/hub-api";
import {
  createSidecarEmitter,
  type EventCollectorRegistry,
  type SessionService,
  type SidecarRouter,
} from "@intx/hub-sessions";
import type { GrantRule } from "@intx/types/authz";
import { agentSession, sessionMail, workflowDefinition } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedPrincipal,
  seedTenants,
  seedWorkflowRun,
} from "@intx/test-harness/seed";

// Exercises the blob endpoint against a real migrated schema. The blob route
// derives its authorization subject from the mail's owning routable: an
// instance's mail carries its indexed runId, while a folded run's mail
// carries a null runId and keys on its session, so the run id is recovered
// by joining workflow_run to agent_session on the shared principal. That join
// and the authorization it feeds only run against a real database -- the
// mock-capture harness in instances.test.ts stubs the sessionMail writes and
// cannot exercise them.

const TENANT_ID = "tnt_blob";
const ACTOR_PRINCIPAL_ID = "prn_actor";
const ACTOR_USER_ID = "usr_actor";
const DEFINITION_ID = "wfd_blob";

const RUN_PRINCIPAL_ID = "prn_run";
const RUN_ID = "run_blob";
const RUN_SESSION_ID = "ses_run";

const INSTANCE_PRINCIPAL_ID = "prn_instance";
const INSTANCE_ID = "run_blob";
const INSTANCE_SESSION_ID = "ses_instance";

const ORPHAN_PRINCIPAL_ID = "prn_orphan";
const ORPHAN_SESSION_ID = "ses_orphan";

const enc = new TextEncoder();

// A minimal multipart/mixed message whose first part is fetchable at path "1".
// extractPartByPath returns that part's bytes verbatim (headers and body), so a
// distinctive body lets a test confirm the right bytes came back.
function multipartRaw(partOneBody: string): Uint8Array {
  return enc.encode(
    [
      "Content-Type: multipart/mixed; boundary=bnd",
      "",
      "--bnd",
      "Content-Type: text/plain",
      "",
      partOneBody,
      "--bnd--",
      "",
    ].join("\r\n"),
  );
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

function notImpl(name: string): never {
  throw new Error(`mock: ${name} not implemented`);
}

function createMockSidecarRouter(): SidecarRouter {
  return {
    handleOpen: () => notImpl("handleOpen"),
    handleMessage: () => notImpl("handleMessage"),
    handleClose: () => notImpl("handleClose"),
    routeMail: () => notImpl("routeMail"),
    sendRunGrants: () => notImpl("sendRunGrants"),
    sendAgentUndeploy: () => notImpl("sendAgentUndeploy"),
    sendSourcesUpdate: () => notImpl("sendSourcesUpdate"),
    sendCredentialsUpdate: () => notImpl("sendCredentialsUpdate"),
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
    sendUserMessage: () => notImpl("sendUserMessage"),
    endSession: () => notImpl("endSession"),
  };
}

function createMockEventCollectors(): EventCollectorRegistry {
  return {
    create: () => notImpl("create"),
    dispatch: () => notImpl("dispatch"),
    abandon: () => notImpl("abandon"),
    has: () => false,
    getStatus: () => undefined,
    getAccumulatedText: () => undefined,
    getCurrentTurnId: () => undefined,
    getLastTurnId: () => undefined,
  };
}

function runReadGrant(subjectId: string): GrantRule {
  return {
    id: `grant-read-${subjectId}`,
    resource: `workflow-run:${subjectId}`,
    action: "read",
    effect: "allow",
    origin: "system",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: ACTOR_PRINCIPAL_ID,
  };
}

let h: TestDb;

beforeAll(async () => {
  if (!harnessDbEnvAvailable()) return;
  h = await createTestDb();
});

afterAll(async () => {
  if (!harnessDbEnvAvailable()) return;
  await h.close();
});

beforeEach(async () => {
  if (!harnessDbEnvAvailable()) return;
  await h.reset();
  await seedTenants(h.db, [{ id: TENANT_ID }]);
  await seedPrincipal(h.db, {
    id: ACTOR_PRINCIPAL_ID,
    tenantId: TENANT_ID,
    kind: "user",
    refId: ACTOR_USER_ID,
  });
  await h.db.insert(workflowDefinition).values({
    id: DEFINITION_ID,
    tenantId: TENANT_ID,
    name: "blob-agent",
  });
});

function buildApp(grants: GrantRule[]): ReturnType<typeof createApp> {
  return createApp({
    getSession: createMockGetSession(ACTOR_USER_ID),
    authHandler: () => new Response("", { status: 404 }),
    db: h.db,
    grantStore: createInMemoryGrantStore(grants),
    sidecarRouter: createMockSidecarRouter(),
    sessionService: createMockSessionService(),
    eventCollectors: createMockEventCollectors(),
    assetService: null,
    repoStore: null,
    maxTarballBytes: 10_000_000,
  });
}

// A folded run: its own workflow principal, an agent_session keyed on that
// principal, and a workflow_run with no deployment -- all sharing the run
// principal, which is how the blob route recovers the run from the mail's
// session. When `ended` is set the run is stopped and its session ended, so a
// test can assert the resolution does not filter on endedAt.
async function seedFoldedRun(opts: { ended?: boolean } = {}): Promise<void> {
  const endedAt = opts.ended === true ? new Date("2025-06-01") : null;
  await seedPrincipal(h.db, {
    id: RUN_PRINCIPAL_ID,
    tenantId: TENANT_ID,
    kind: "workflow",
    refId: RUN_ID,
  });
  await h.db.insert(agentSession).values({
    id: RUN_SESSION_ID,
    tenantId: TENANT_ID,
    agentId: DEFINITION_ID,
    principalId: RUN_PRINCIPAL_ID,
    status: opts.ended === true ? "ended" : "active",
    endedAt,
  });
  await seedWorkflowRun(h.db, {
    id: RUN_ID,
    tenantId: TENANT_ID,
    definitionId: DEFINITION_ID,
    principalId: RUN_PRINCIPAL_ID,
    status: opts.ended === true ? "completed" : "running",
    endedAt,
  });
}

async function insertMail(opts: {
  id: string;
  sessionId: string;
  runId: string | null;
  body: string;
}): Promise<void> {
  await h.db.insert(sessionMail).values({
    id: opts.id,
    sessionId: opts.sessionId,
    runId: opts.runId,
    tenantId: TENANT_ID,
    direction: "inbound",
    status: "delivered",
    raw: multipartRaw(opts.body),
  });
}

function blobUrl(mailId: string): string {
  return `/api/tenants/${TENANT_ID}/workflows/runs/blobs/blob_${mailId}_1`;
}

describe.skipIf(!harnessDbEnvAvailable())(
  "GET /workflows/runs/blobs/:blobId (folded run)",
  () => {
    test("serves a folded run's mail blob when the caller holds the run grant", async () => {
      await seedFoldedRun();
      await insertMail({
        id: "mail_folded_ok",
        sessionId: RUN_SESSION_ID,
        runId: null,
        body: "folded run part body",
      });
      const app = buildApp([runReadGrant(RUN_ID)]);

      const res = await app.request(blobUrl("mail_folded_ok"));
      expect(res.status).toBe(200);
      const bytes = new Uint8Array(await res.arrayBuffer());
      expect(new TextDecoder().decode(bytes)).toContain("folded run part body");
    });

    test("serves a stopped folded run's mail blob (session resolution ignores endedAt)", async () => {
      await seedFoldedRun({ ended: true });
      await insertMail({
        id: "mail_folded_stopped",
        sessionId: RUN_SESSION_ID,
        runId: null,
        body: "stopped run part body",
      });
      const app = buildApp([runReadGrant(RUN_ID)]);

      const res = await app.request(blobUrl("mail_folded_stopped"));
      expect(res.status).toBe(200);
      const bytes = new Uint8Array(await res.arrayBuffer());
      expect(new TextDecoder().decode(bytes)).toContain(
        "stopped run part body",
      );
    });

    test("forbids a folded run's mail blob when the caller lacks the run grant", async () => {
      await seedFoldedRun();
      await insertMail({
        id: "mail_folded_forbidden",
        sessionId: RUN_SESSION_ID,
        runId: null,
        body: "secret",
      });
      // The caller holds a grant for an unrelated run, so authorization is
      // enforced on the resolved run id, not merely on holding any grant.
      const app = buildApp([runReadGrant("run_other")]);

      const res = await app.request(blobUrl("mail_folded_forbidden"));
      expect(res.status).toBe(403);
    });

    test("serves a legacy instance's mail blob through the unchanged branch", async () => {
      await seedPrincipal(h.db, {
        id: INSTANCE_PRINCIPAL_ID,
        tenantId: TENANT_ID,
        kind: "workflow",
        refId: INSTANCE_ID,
      });
      await h.db.insert(agentSession).values({
        id: INSTANCE_SESSION_ID,
        tenantId: TENANT_ID,
        agentId: DEFINITION_ID,
        principalId: INSTANCE_PRINCIPAL_ID,
        status: "active",
      });
      await insertMail({
        id: "mail_instance_ok",
        sessionId: INSTANCE_SESSION_ID,
        runId: INSTANCE_ID,
        body: "legacy instance part body",
      });
      const app = buildApp([runReadGrant(INSTANCE_ID)]);

      const res = await app.request(blobUrl("mail_instance_ok"));
      expect(res.status).toBe(200);
      const bytes = new Uint8Array(await res.arrayBuffer());
      expect(new TextDecoder().decode(bytes)).toContain(
        "legacy instance part body",
      );
    });

    test("returns 404 when a null-instance mail's session names no run", async () => {
      // A session owned by a non-run principal: the mail carries a null
      // runId, so the run resolution runs and must fail closed rather than
      // authorize a fabricated subject.
      await seedPrincipal(h.db, {
        id: ORPHAN_PRINCIPAL_ID,
        tenantId: TENANT_ID,
        kind: "user",
        refId: "usr_orphan",
      });
      await h.db.insert(agentSession).values({
        id: ORPHAN_SESSION_ID,
        tenantId: TENANT_ID,
        agentId: DEFINITION_ID,
        principalId: ORPHAN_PRINCIPAL_ID,
        status: "active",
      });
      await insertMail({
        id: "mail_orphan",
        sessionId: ORPHAN_SESSION_ID,
        runId: null,
        body: "unreachable",
      });
      // Even a caller holding a broad grant cannot reach it: resolution fails
      // before authorization.
      const app = buildApp([runReadGrant("run_other")]);

      const res = await app.request(blobUrl("mail_orphan"));
      expect(res.status).toBe(404);
    });
  },
);

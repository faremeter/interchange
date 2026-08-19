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
import { workflowDefinition } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedPrincipal, seedTenants } from "@intx/test-harness/seed";

// The offering foreign key now references workflow_definition (its agent_id
// column holds a definition id, the API field name kept for contract
// stability). This drives the re-pointed route against a real migrated schema:
// create validates a definition, the response carries the definition's name as
// agentName, and the new per-tenant definitions list backs the picker the admin
// create form uses.

const TENANT_ID = "tnt_off";
const ACTOR_PRINCIPAL_ID = "prn_actor";
const ACTOR_USER_ID = "usr_actor";
const DEFINITION_ID = "wfd_demo";

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
    name: "Demo Definition",
  });
});

function buildApp(): ReturnType<typeof createApp> {
  return createApp({
    getSession: createMockGetSession(ACTOR_USER_ID),
    authHandler: () => new Response("", { status: 404 }),
    db: h.db,
    grantStore: createInMemoryGrantStore([
      grant("offering:*", "create"),
      grant("offering:*", "read"),
      grant("workflow-definition:*", "read"),
    ]),
    sidecarRouter: createMockSidecarRouter(),
    sessionService: createMockSessionService(),
    eventCollectors: createMockEventCollectors(),
    assetService: null,
    repoStore: null,
    maxTarballBytes: 10_000_000,
  });
}

function base(): string {
  return `/api/tenants/${TENANT_ID}`;
}

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

function asObject(v: unknown): Record<string, unknown> {
  if (!isObject(v)) {
    throw new Error("expected an object body");
  }
  return v;
}

function dataRows(v: unknown): Record<string, unknown>[] {
  const data = asObject(v)["data"];
  if (!Array.isArray(data)) {
    throw new Error("expected a data array");
  }
  return data.map(asObject);
}

async function createOffering(
  app: ReturnType<typeof createApp>,
  agentId: string,
): Promise<Response> {
  return app.request(`${base()}/offerings`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ agentId, name: "Translation" }),
  });
}

describe.skipIf(!harnessDbEnvAvailable())(
  "offering re-pointed onto workflow_definition (real DB)",
  () => {
    test("creates an offering against a definition and returns its name as agentName", async () => {
      const app = buildApp();
      const res = await createOffering(app, DEFINITION_ID);
      expect(res.status).toBe(201);
      const body = asObject(await res.json());
      expect(body["agentId"]).toBe(DEFINITION_ID);
      expect(body["agentName"]).toBe("Demo Definition");
    });

    test("rejects creation against a definition that does not exist", async () => {
      const app = buildApp();
      const res = await createOffering(app, "wfd_missing");
      expect(res.status).toBe(404);
    });

    test("lists offerings with the definition's name", async () => {
      const app = buildApp();
      expect((await createOffering(app, DEFINITION_ID)).status).toBe(201);

      const res = await app.request(`${base()}/offerings`);
      expect(res.status).toBe(200);
      const rows = dataRows(await res.json());
      expect(rows).toHaveLength(1);
      expect(rows[0]?.["agentId"]).toBe(DEFINITION_ID);
      expect(rows[0]?.["agentName"]).toBe("Demo Definition");
    });

    test("lists the tenant's workflow definitions", async () => {
      const app = buildApp();
      const res = await app.request(`${base()}/workflows/definitions`);
      expect(res.status).toBe(200);
      const rows = dataRows(await res.json());
      expect(rows).toHaveLength(1);
      expect(rows[0]?.["id"]).toBe(DEFINITION_ID);
      expect(rows[0]?.["name"]).toBe("Demo Definition");
    });
  },
);

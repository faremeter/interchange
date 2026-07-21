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
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedPrincipal,
  seedTenants,
  seedWorkflowDeployment,
} from "@intx/test-harness/seed";

// Exercises the principals resolver against a real migrated schema so the
// `workflow`-kind display-name second pass -- which reads the deployment's
// `address` off the `workflow_deployment` row a workflow principal's refId
// points at -- runs end to end rather than against a mock.

const TENANT_ID = "tnt_principals";
const ACTOR_PRINCIPAL_ID = "prn_actor";
const ACTOR_USER_ID = "usr_actor";
const ASSET_ID = "ast_wf";
const DEPLOYMENT_ID = "dep_wf";
const WORKFLOW_PRINCIPAL_ID = "prn_workflow";
const DEPLOYMENT_ADDRESS = "ins_dep_wf@principals.test";

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
    deployInstanceAtHead: () => notImpl("deployInstanceAtHead"),
    deployWorkflowDefinition: () => notImpl("deployWorkflowDefinition"),
    deploySingleStepAtHead: () => notImpl("deploySingleStepAtHead"),
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

function readPrincipalsGrant(): GrantRule {
  return {
    id: "grant-actor-principal-read",
    resource: "principal:*",
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
});

async function setup() {
  await seedTenants(h.db, [{ id: TENANT_ID }]);
  await seedPrincipal(h.db, {
    id: ACTOR_PRINCIPAL_ID,
    tenantId: TENANT_ID,
    kind: "user",
    refId: ACTOR_USER_ID,
  });
  await seedAsset(h.db, {
    id: ASSET_ID,
    tenantId: TENANT_ID,
    kind: "workflow",
    name: "wf",
  });
  // The workflow principal's refId is the deployment id, so name resolution
  // can reach the deployment row and derive the display name from its address.
  await seedPrincipal(h.db, {
    id: WORKFLOW_PRINCIPAL_ID,
    tenantId: TENANT_ID,
    kind: "workflow",
    refId: DEPLOYMENT_ID,
  });
  await seedWorkflowDeployment(h.db, {
    id: DEPLOYMENT_ID,
    tenantId: TENANT_ID,
    definitionAssetId: ASSET_ID,
    address: DEPLOYMENT_ADDRESS,
  });

  return createApp({
    getSession: createMockGetSession(ACTOR_USER_ID),
    authHandler: () => new Response("", { status: 404 }),
    db: h.db,
    grantStore: createInMemoryGrantStore([readPrincipalsGrant()]),
    sidecarRouter: createMockSidecarRouter(),
    sessionService: createMockSessionService(),
    eventCollectors: createMockEventCollectors(),
    assetService: null,
    repoStore: null,
    maxTarballBytes: 10_000_000,
  });
}

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

describe.skipIf(!harnessDbEnvAvailable())(
  "GET /api/tenants/:tenantId/principals",
  () => {
    test("resolves a workflow principal's display name from its deployment address", async () => {
      const app = await setup();
      const res = await app.request(
        `/api/tenants/${TENANT_ID}/principals?kind=workflow`,
      );
      expect(res.status).toBe(200);

      const body: unknown = await res.json();
      if (!isObject(body)) throw new Error("expected object body");
      const data = body["data"];
      if (!Array.isArray(data)) throw new Error("expected data array");
      expect(data).toHaveLength(1);

      const row = data[0];
      if (!isObject(row)) throw new Error("expected principal row");
      expect(row["kind"]).toBe("workflow");
      expect(row["refId"]).toBe(DEPLOYMENT_ID);
      expect(row["displayName"]).toBe(`Workflow (${DEPLOYMENT_ADDRESS})`);
    });
  },
);

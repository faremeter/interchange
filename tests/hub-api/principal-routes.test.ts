import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { eq } from "drizzle-orm";

import { createInMemoryGrantStore } from "@intx/authz";
import { credential, principal } from "@intx/db/schema";
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
  seedCredential,
  seedPrincipal,
  seedProvider,
  seedTenants,
  seedWorkflowRun,
} from "@intx/test-harness/seed";

// Exercises the principals resolver against a real migrated schema so the
// `workflow`-kind display-name second pass -- which reads the deployment's
// `address` by joining a workflow principal's refId (its run id) through
// `workflow_run` to its anchor run -- runs end to end rather than
// against a mock.

const TENANT_ID = "tnt_principals";
const ACTOR_PRINCIPAL_ID = "prn_actor";
const ACTOR_USER_ID = "usr_actor";
const ASSET_ID = "ast_wf";
const DEPLOYMENT_ID = "run_wf";
const RUN_ID = "run_wf_child";
const WORKFLOW_PRINCIPAL_ID = "prn_workflow";
const DEPLOYMENT_ADDRESS = "run_wf@principals.test";
const TARGET_PRINCIPAL_ID = "prn_target";
const PROVIDER_ID = "prv_principals";
const PERSONAL_CREDENTIAL_ID = "crd_personal";
const ORG_CREDENTIAL_ID = "crd_org";

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
    noteSenderDeployStarted: () => notImpl("noteSenderDeployStarted"),
    noteSenderDeploySettled: () => notImpl("noteSenderDeploySettled"),
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

function managePrincipalsGrant(): GrantRule {
  return {
    id: "grant-actor-principal-manage",
    resource: "principal:*",
    action: "manage",
    effect: "allow",
    origin: "system",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: ACTOR_PRINCIPAL_ID,
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
  // The workflow principal's refId is its run id; name resolution self-joins
  // the run to its anchor run (on the deployment id) to derive the display
  // name from the anchor run's address.
  await seedPrincipal(h.db, {
    id: WORKFLOW_PRINCIPAL_ID,
    tenantId: TENANT_ID,
    kind: "workflow",
    refId: RUN_ID,
  });
  // The deployment's anchor run carries the routing address the display name
  // resolves to; its id is the deployment id and the child run below self-joins
  // to it on that id. It is inserted first so the child run's deployment_id FK
  // resolves.
  await seedWorkflowRun(h.db, {
    id: DEPLOYMENT_ID,
    tenantId: TENANT_ID,
    anchorRunId: DEPLOYMENT_ID,
    address: DEPLOYMENT_ADDRESS,
    status: "running",
  });
  await seedWorkflowRun(h.db, {
    id: RUN_ID,
    anchorRunId: DEPLOYMENT_ID,
    tenantId: TENANT_ID,
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

async function setupDelete() {
  await seedTenants(h.db, [{ id: TENANT_ID }]);
  await seedPrincipal(h.db, {
    id: ACTOR_PRINCIPAL_ID,
    tenantId: TENANT_ID,
    kind: "user",
    refId: ACTOR_USER_ID,
  });
  await seedPrincipal(h.db, {
    id: TARGET_PRINCIPAL_ID,
    tenantId: TENANT_ID,
    kind: "user",
    refId: "usr_target",
  });
  await seedProvider(h.db, {
    id: PROVIDER_ID,
    tenantId: TENANT_ID,
    name: "openai",
  });

  return createApp({
    getSession: createMockGetSession(ACTOR_USER_ID),
    authHandler: () => new Response("", { status: 404 }),
    db: h.db,
    grantStore: createInMemoryGrantStore([managePrincipalsGrant()]),
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
      expect(row["refId"]).toBe(RUN_ID);
      expect(row["displayName"]).toBe(`Workflow (${DEPLOYMENT_ADDRESS})`);
    });
  },
);

describe.skipIf(!harnessDbEnvAvailable())(
  "DELETE /api/tenants/:tenantId/principals/:principalId",
  () => {
    test("refuses to remove a principal who owns a credential", async () => {
      const app = await setupDelete();
      await seedCredential(h.db, {
        id: PERSONAL_CREDENTIAL_ID,
        tenantId: TENANT_ID,
        providerId: PROVIDER_ID,
        name: "personal",
        principalId: TARGET_PRINCIPAL_ID,
      });

      const res = await app.request(
        `/api/tenants/${TENANT_ID}/principals/${TARGET_PRINCIPAL_ID}`,
        { method: "DELETE" },
      );
      expect(res.status).toBe(409);
      const body: unknown = await res.json();
      if (!isObject(body)) throw new Error("expected object body");
      const error = body["error"];
      if (!isObject(error)) throw new Error("expected error body");
      expect(error["code"]).toBe("conflict");
      expect(error["message"]).toBe("Principal is still referenced");

      const [credentialRow] = await h.db
        .select()
        .from(credential)
        .where(eq(credential.id, PERSONAL_CREDENTIAL_ID));
      expect(credentialRow?.principalId).toBe(TARGET_PRINCIPAL_ID);

      const [principalRow] = await h.db
        .select()
        .from(principal)
        .where(eq(principal.id, TARGET_PRINCIPAL_ID));
      expect(principalRow?.id).toBe(TARGET_PRINCIPAL_ID);
    });

    test("removes a principal who owns no credential", async () => {
      const app = await setupDelete();
      await seedCredential(h.db, {
        id: ORG_CREDENTIAL_ID,
        tenantId: TENANT_ID,
        providerId: PROVIDER_ID,
        name: "org",
        principalId: null,
      });

      const res = await app.request(
        `/api/tenants/${TENANT_ID}/principals/${TARGET_PRINCIPAL_ID}`,
        { method: "DELETE" },
      );
      expect(res.status).toBe(204);

      const principals = await h.db
        .select()
        .from(principal)
        .where(eq(principal.id, TARGET_PRINCIPAL_ID));
      expect(principals).toHaveLength(0);

      const [orgCredential] = await h.db
        .select()
        .from(credential)
        .where(eq(credential.id, ORG_CREDENTIAL_ID));
      expect(orgCredential?.principalId).toBeNull();
    });

    test("returns not found for an unknown principal", async () => {
      const app = await setupDelete();
      const res = await app.request(
        `/api/tenants/${TENANT_ID}/principals/prn_missing`,
        { method: "DELETE" },
      );
      expect(res.status).toBe(404);
    });
  },
);

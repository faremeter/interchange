import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { and, eq } from "drizzle-orm";

import { createInMemoryGrantStore } from "@intx/authz";
import { createApp, type GetSession } from "@intx/hub-api";
import {
  createSidecarEmitter,
  type EventCollectorRegistry,
  type SessionService,
  type SidecarRouter,
} from "@intx/hub-sessions";
import { grant as grantTable } from "@intx/db/schema";
import type { GrantRule } from "@intx/types/authz";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedPrincipal,
  seedProvider,
  seedTenants,
} from "@intx/test-harness/seed";

// These route tests exercise credential creation against a real migrated
// schema so the auto-grant insert runs inside the same transaction as the
// credential insert and is asserted directly against the grant table.

const TENANT_ID = "tnt_cred";
const ACTOR_PRINCIPAL_ID = "prn_actor";
const OWNER_PRINCIPAL_ID = "prn_owner";
const ACTOR_USER_ID = "usr_actor";
const PROVIDER_ID = "prv_test";

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

function createGrant(action: string): GrantRule {
  return {
    id: `grant-actor-${action}`,
    resource: "credential:*",
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
});

async function setup() {
  await seedTenants(h.db, [{ id: TENANT_ID }]);
  await seedPrincipal(h.db, {
    id: ACTOR_PRINCIPAL_ID,
    tenantId: TENANT_ID,
    refId: ACTOR_USER_ID,
  });
  await seedPrincipal(h.db, {
    id: OWNER_PRINCIPAL_ID,
    tenantId: TENANT_ID,
    refId: "usr_owner",
  });
  await seedProvider(h.db, {
    id: PROVIDER_ID,
    tenantId: TENANT_ID,
    name: "openai",
  });

  const app = createApp({
    getSession: createMockGetSession(ACTOR_USER_ID),
    authHandler: () => new Response("", { status: 404 }),
    db: h.db,
    grantStore: createInMemoryGrantStore([createGrant("create")]),
    sidecarRouter: createMockSidecarRouter(),
    sessionService: createMockSessionService(),
    eventCollectors: createMockEventCollectors(),
    assetService: null,
    repoStore: null,
    maxTarballBytes: 10_000_000,
  });
  return app;
}

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

async function useGrantsFor(credentialId: string, principalId: string) {
  return h.db
    .select()
    .from(grantTable)
    .where(
      and(
        eq(grantTable.principalId, principalId),
        eq(grantTable.resource, `credential:${credentialId}`),
        eq(grantTable.action, "use"),
      ),
    );
}

describe.skipIf(!harnessDbEnvAvailable())(
  "POST /api/tenants/:tenantId/credentials",
  () => {
    test("mints a durable use-grant for the owner of a personal credential", async () => {
      const app = await setup();
      const res = await app.request(`/api/tenants/${TENANT_ID}/credentials`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          providerId: PROVIDER_ID,
          name: "my-key",
          type: "api_key",
          secret: "sk-personal",
          principalId: OWNER_PRINCIPAL_ID,
        }),
      });
      expect(res.status).toBe(201);
      const body: unknown = await res.json();
      if (!isObject(body)) throw new Error("expected object body");
      const credentialId = body["id"];
      if (typeof credentialId !== "string") {
        throw new Error("expected credential id");
      }

      const grants = await useGrantsFor(credentialId, OWNER_PRINCIPAL_ID);
      expect(grants).toHaveLength(1);
      const g = grants[0];
      if (g === undefined) throw new Error("expected grant row");
      expect(g.effect).toBe("allow");
      expect(g.origin).toBe("creator");
      expect(g.expiresAt).toBeNull();
      expect(g.tenantId).toBe(TENANT_ID);
    });

    test("mints no use-grant for an organizational credential", async () => {
      const app = await setup();
      const res = await app.request(`/api/tenants/${TENANT_ID}/credentials`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          providerId: PROVIDER_ID,
          name: "org-key",
          type: "api_key",
          secret: "sk-org",
        }),
      });
      expect(res.status).toBe(201);
      const body: unknown = await res.json();
      if (!isObject(body)) throw new Error("expected object body");
      const credentialId = body["id"];
      if (typeof credentialId !== "string") {
        throw new Error("expected credential id");
      }
      expect(body["principalId"]).toBeNull();

      const allUseGrants = await h.db
        .select()
        .from(grantTable)
        .where(
          and(
            eq(grantTable.resource, `credential:${credentialId}`),
            eq(grantTable.action, "use"),
          ),
        );
      expect(allUseGrants).toHaveLength(0);
    });
  },
);

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
import { federationTrust } from "@intx/db/schema";
import type { GrantRule } from "@intx/types/authz";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { createTestCredentialCipher } from "@intx/test-harness/crypto";
import { seedPrincipal, seedTenants } from "@intx/test-harness/seed";

// Federation trusts let another tenant's runs interact with this tenant, so
// changing them is tenant administration. These tests drive the routes with
// the system member and admin grant shapes against a real migrated schema.

const TENANT_ID = "tnt_federation";
const TARGET_TENANT_ID = "tnt_federation_target";
const ACTOR_PRINCIPAL_ID = "prn_actor";
const ACTOR_USER_ID = "usr_actor";

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

function grantsFor(actions: readonly string[], resource = "*"): GrantRule[] {
  return actions.map((action) => ({
    id: `grant-actor-${action}`,
    resource,
    action,
    effect: "allow",
    origin: "system",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: ACTOR_PRINCIPAL_ID,
  }));
}

const MEMBER = grantsFor(["read"]);
const ADMIN = grantsFor(["read", "create", "manage"]);

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

async function setup(grants: GrantRule[]) {
  await seedTenants(h.db, [{ id: TENANT_ID }, { id: TARGET_TENANT_ID }]);
  await seedPrincipal(h.db, {
    id: ACTOR_PRINCIPAL_ID,
    tenantId: TENANT_ID,
    refId: ACTOR_USER_ID,
  });

  return createApp({
    getSession: createMockGetSession(ACTOR_USER_ID),
    authHandler: () => new Response("", { status: 404 }),
    db: h.db,
    grantStore: createInMemoryGrantStore(grants),
    sidecarRouter: createMockSidecarRouter(),
    sessionService: createMockSessionService(),
    eventCollectors: createMockEventCollectors(),
    credentialCipher: createTestCredentialCipher(),
    assetService: null,
    repoStore: null,
    maxTarballBytes: 10_000_000,
  });
}

async function seedTrust(): Promise<void> {
  await h.db.insert(federationTrust).values({
    id: "ftr_existing",
    tenantId: TENANT_ID,
    targetTenantId: TARGET_TENANT_ID,
    direction: "bilateral",
    createdAt: new Date("2025-01-01"),
  });
}

async function trustRows() {
  return await h.db
    .select()
    .from(federationTrust)
    .where(
      and(
        eq(federationTrust.tenantId, TENANT_ID),
        eq(federationTrust.targetTenantId, TARGET_TENANT_ID),
      ),
    );
}

function createTrust(app: Awaited<ReturnType<typeof setup>>) {
  return app.request(`/api/tenants/${TENANT_ID}/federation`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      targetTenantId: TARGET_TENANT_ID,
      direction: "bilateral",
    }),
  });
}

function revokeTrust(app: Awaited<ReturnType<typeof setup>>) {
  return app.request(
    `/api/tenants/${TENANT_ID}/federation/${TARGET_TENANT_ID}`,
    { method: "DELETE" },
  );
}

describe.skipIf(!harnessDbEnvAvailable())(
  "/api/tenants/:tenantId/federation",
  () => {
    test("a member can list trusts", async () => {
      const app = await setup(MEMBER);
      await seedTrust();
      const res = await app.request(`/api/tenants/${TENANT_ID}/federation`);
      expect(res.status).toBe(200);
    });

    test("a member cannot establish a trust", async () => {
      const app = await setup(MEMBER);
      const res = await createTrust(app);
      expect(res.status).toBe(403);
      expect(await trustRows()).toHaveLength(0);
    });

    test("a member cannot revoke a trust", async () => {
      const app = await setup(MEMBER);
      await seedTrust();
      const res = await revokeTrust(app);
      expect(res.status).toBe(403);
      expect(await trustRows()).toHaveLength(1);
    });

    test("a member gets 403, not 404, for a trust that does not exist", async () => {
      const app = await setup(MEMBER);
      const res = await revokeTrust(app);
      expect(res.status).toBe(403);
    });

    test("a principal with no grants cannot list trusts", async () => {
      const app = await setup([]);
      const res = await app.request(`/api/tenants/${TENANT_ID}/federation`);
      expect(res.status).toBe(403);
    });

    test("a create grant keyed on another target cannot establish the trust", async () => {
      const app = await setup(grantsFor(["create"], "federation:tnt_other"));
      const res = await createTrust(app);
      expect(res.status).toBe(403);
      expect(await trustRows()).toHaveLength(0);
    });

    test("a create grant keyed on the target tenant establishes the trust", async () => {
      const app = await setup(
        grantsFor(["create"], `federation:${TARGET_TENANT_ID}`),
      );
      expect((await createTrust(app)).status).toBe(201);
      expect(await trustRows()).toHaveLength(1);
    });

    test("a deny keyed on the target tenant overrides a create grant on every target", async () => {
      const [deny] = grantsFor(["create"], `federation:${TARGET_TENANT_ID}`);
      if (deny === undefined) throw new Error("expected one grant");
      const app = await setup([
        ...grantsFor(["create"], "federation:*"),
        { ...deny, id: "grant-actor-deny", effect: "deny" },
      ]);
      const res = await createTrust(app);
      expect(res.status).toBe(403);
      expect(await trustRows()).toHaveLength(0);
    });

    test("a manage grant keyed on another target cannot revoke the trust", async () => {
      const app = await setup(grantsFor(["manage"], "federation:tnt_other"));
      await seedTrust();
      const res = await revokeTrust(app);
      expect(res.status).toBe(403);
      expect(await trustRows()).toHaveLength(1);
    });

    test("a manage grant keyed on the target tenant revokes the trust", async () => {
      const app = await setup(
        grantsFor(["manage"], `federation:${TARGET_TENANT_ID}`),
      );
      await seedTrust();
      const res = await revokeTrust(app);
      expect(res.status).toBe(204);
      expect(await trustRows()).toHaveLength(0);
    });

    test("a grant scoped to a run consumer does not authorize a session", async () => {
      const [scoped] = grantsFor(["read"]);
      if (scoped === undefined) throw new Error("expected one grant");
      const app = await setup([
        { ...scoped, conditions: { tool: "action:x" } },
      ]);
      const res = await app.request(`/api/tenants/${TENANT_ID}/federation`);
      expect(res.status).toBe(403);
    });

    test("an admin can establish and revoke a trust", async () => {
      const app = await setup(ADMIN);
      expect((await createTrust(app)).status).toBe(201);
      expect(await trustRows()).toHaveLength(1);
      expect((await revokeTrust(app)).status).toBe(204);
      expect(await trustRows()).toHaveLength(0);
    });
  },
);

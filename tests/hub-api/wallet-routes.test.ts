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
import { createApp, type GetSession } from "@intx/hub-api";
import {
  createSidecarEmitter,
  type EventCollectorRegistry,
  type SessionService,
  type SidecarRouter,
} from "@intx/hub-sessions";
import { wallet } from "@intx/db/schema";
import type { GrantRule } from "@intx/types/authz";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { createTestCredentialCipher } from "@intx/test-harness/crypto";
import {
  seedModelProvider,
  seedPrincipal,
  seedTenants,
  seedWallet,
} from "@intx/test-harness/seed";

// These route tests exercise wallet deletion against a real migrated schema so
// the model_provider.wallet_id restrict foreign key is enforced for real.

const TENANT_ID = "tnt_wallet";
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

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

function createGrant(action: string): GrantRule {
  return {
    id: `grant-actor-${action}`,
    resource: "wallet:*",
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

  const app = createApp({
    getSession: createMockGetSession(ACTOR_USER_ID),
    authHandler: () => new Response("", { status: 404 }),
    db: h.db,
    grantStore: createInMemoryGrantStore([createGrant("manage")]),
    sidecarRouter: createMockSidecarRouter(),
    sessionService: createMockSessionService(),
    eventCollectors: createMockEventCollectors(),
    credentialCipher: createTestCredentialCipher(),
    assetService: null,
    repoStore: null,
    maxTarballBytes: 10_000_000,
  });
  return app;
}

describe.skipIf(!harnessDbEnvAvailable())(
  "DELETE /api/tenants/:tenantId/wallets/:walletId",
  () => {
    test("returns 409 and preserves the wallet when a model provider references it", async () => {
      const app = await setup();
      await seedWallet(h.db, { id: "wlt_bound", tenantId: TENANT_ID });
      await seedModelProvider(h.db, {
        id: "mp_wallet",
        tenantId: TENANT_ID,
        name: "wallet-provider",
        plugin: "openai",
        baseURL: "https://api.openai.com",
        walletId: "wlt_bound",
      });

      const res = await app.request(
        `/api/tenants/${TENANT_ID}/wallets/wlt_bound`,
        { method: "DELETE" },
      );
      expect(res.status).toBe(409);
      const body: unknown = await res.json();
      if (!isObject(body)) throw new Error("expected object body");
      const error = body["error"];
      if (!isObject(error)) throw new Error("expected error object");
      expect(error["code"]).toBe("conflict");

      const [row] = await h.db
        .select()
        .from(wallet)
        .where(eq(wallet.id, "wlt_bound"));
      expect(row).toBeDefined();
    });

    test("deletes a wallet that no model provider references", async () => {
      const app = await setup();
      await seedWallet(h.db, { id: "wlt_free", tenantId: TENANT_ID });

      const res = await app.request(
        `/api/tenants/${TENANT_ID}/wallets/wlt_free`,
        { method: "DELETE" },
      );
      expect(res.status).toBe(204);

      const rows = await h.db
        .select()
        .from(wallet)
        .where(eq(wallet.id, "wlt_free"));
      expect(rows).toHaveLength(0);
    });

    test("returns 404 when the wallet does not exist", async () => {
      const app = await setup();
      const res = await app.request(
        `/api/tenants/${TENANT_ID}/wallets/wlt_missing`,
        { method: "DELETE" },
      );
      expect(res.status).toBe(404);
    });

    test("returns 404 without disclosing a referenced wallet in another tenant", async () => {
      const app = await setup();

      // A wallet in a different tenant, referenced by a model provider there.
      // The acting principal holds a wildcard `wallet:*` grant, so requireGrant
      // admits the request; tenant isolation is owned by the delete's WHERE
      // clause. A pre-check that queried the referencing row globally would leak
      // the wallet's existence as a 409.
      const OTHER_TENANT_ID = "tnt_other";
      await seedTenants(h.db, [{ id: OTHER_TENANT_ID }]);
      await seedWallet(h.db, { id: "wlt_foreign", tenantId: OTHER_TENANT_ID });
      await seedModelProvider(h.db, {
        id: "mp_foreign",
        tenantId: OTHER_TENANT_ID,
        name: "foreign-provider",
        plugin: "openai",
        baseURL: "https://api.openai.com",
        walletId: "wlt_foreign",
      });

      const res = await app.request(
        `/api/tenants/${TENANT_ID}/wallets/wlt_foreign`,
        { method: "DELETE" },
      );
      expect(res.status).toBe(404);

      const rows = await h.db
        .select()
        .from(wallet)
        .where(eq(wallet.id, "wlt_foreign"));
      expect(rows).toHaveLength(1);
    });
  },
);

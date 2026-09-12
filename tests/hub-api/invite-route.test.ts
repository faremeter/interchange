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
import { createPrincipalKeyStore } from "@intx/db";
import { principal, principalKey, user } from "@intx/db/schema";
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
import { createTestCredentialCipher } from "@intx/test-harness/crypto";
import { seedPrincipal, seedTenants } from "@intx/test-harness/seed";

// Exercises the invite route end to end through the in-process hub app with a
// real key store, proving the invited principal is both created and minted an
// active signing key on the write path.

const TENANT_ID = "tnt_invite";
const ACTOR_PRINCIPAL_ID = "prn_invite_actor";
const ACTOR_USER_ID = "usr_invite_actor";
const INVITEE_USER_ID = "usr_invitee";
const INVITEE_EMAIL = "invitee@invite.test";
const cipher = createTestCredentialCipher();

function createMockGetSession(userId: string): GetSession {
  const now = new Date("2025-01-01");
  return async () => ({
    user: {
      id: userId,
      email: "actor@invite.test",
      emailVerified: true,
      name: "Actor",
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

function inviteCreateGrant(): GrantRule {
  return {
    id: "grant-invite-create",
    resource: "principal:*",
    action: "create",
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
  // The acting user's active principal makes it a tenant member; the grant
  // authorizes the invite.
  await seedPrincipal(h.db, {
    id: ACTOR_PRINCIPAL_ID,
    tenantId: TENANT_ID,
    kind: "user",
    refId: ACTOR_USER_ID,
  });
  // The invite resolves the target by email, so the user row must exist.
  const now = new Date();
  await h.db.insert(user).values({
    id: INVITEE_USER_ID,
    name: "Invitee",
    email: INVITEE_EMAIL,
    emailVerified: true,
    createdAt: now,
    updatedAt: now,
  });

  return createApp({
    getSession: createMockGetSession(ACTOR_USER_ID),
    authHandler: () => new Response("", { status: 404 }),
    db: h.db,
    grantStore: createInMemoryGrantStore([inviteCreateGrant()]),
    principalKeyStore: createPrincipalKeyStore({ db: h.db, cipher }),
    sidecarRouter: createMockSidecarRouter(),
    sessionService: createMockSessionService(),
    eventCollectors: createMockEventCollectors(),
    assetService: null,
    repoStore: null,
    maxTarballBytes: 10_000_000,
  });
}

describe.skipIf(!harnessDbEnvAvailable())(
  "POST /api/tenants/:tenantId/members/invite",
  () => {
    test("creates the invited principal and mints its active signing key", async () => {
      const app = await setup();

      const res = await app.request(
        `/api/tenants/${TENANT_ID}/members/invite`,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ email: INVITEE_EMAIL }),
        },
      );
      expect(res.status).toBe(201);

      const [invited] = await h.db
        .select()
        .from(principal)
        .where(
          and(
            eq(principal.tenantId, TENANT_ID),
            eq(principal.kind, "user"),
            eq(principal.refId, INVITEE_USER_ID),
          ),
        );
      if (invited === undefined) {
        throw new Error("invited principal was not created");
      }
      expect(invited.status).toBe("invited");

      const [key] = await h.db
        .select()
        .from(principalKey)
        .where(
          and(
            eq(principalKey.principalId, invited.id),
            eq(principalKey.status, "active"),
          ),
        );
      expect(key).toBeDefined();
    });
  },
);

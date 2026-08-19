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
import { pgErrorCode } from "@intx/db";
import { credential, grant as grantTable } from "@intx/db/schema";
import type { GrantRule } from "@intx/types/authz";
import { credentialAad } from "@intx/types";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { createTestCredentialCipher } from "@intx/test-harness/crypto";
import {
  seedCredential,
  seedModelProvider,
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
    grantStore: createInMemoryGrantStore([
      createGrant("create"),
      createGrant("manage"),
    ]),
    sidecarRouter: createMockSidecarRouter(),
    sessionService: createMockSessionService(),
    eventCollectors: createMockEventCollectors(),
    // A real cipher, so the write-path encryption is exercised end to end.
    credentialCipher: createTestCredentialCipher(),
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

    test("stores the secret and refreshSecret encrypted at rest", async () => {
      const app = await setup();
      const res = await app.request(`/api/tenants/${TENANT_ID}/credentials`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          providerId: PROVIDER_ID,
          name: "enc-key",
          type: "api_key",
          secret: "sk-should-be-encrypted",
          refreshSecret: "rk-should-be-encrypted",
        }),
      });
      expect(res.status).toBe(201);
      const body: unknown = await res.json();
      if (!isObject(body)) throw new Error("expected object body");
      const credentialId = body["id"];
      if (typeof credentialId !== "string") {
        throw new Error("expected credential id");
      }
      // The response never carries the secret back out.
      expect(body["secret"]).toBeUndefined();

      // The raw columns hold enc:aead ciphertext, not the plaintext -- the proof
      // that the write path never stores a secret in the clear.
      const [row] = await h.db
        .select()
        .from(credential)
        .where(eq(credential.id, credentialId));
      if (row === undefined) throw new Error("credential row not found");
      if (row.refreshSecret === null) {
        throw new Error("expected an encrypted refreshSecret");
      }
      expect(row.secret).toStartWith("enc:aead:");
      expect(row.secret).not.toBe("sk-should-be-encrypted");
      expect(row.refreshSecret).toStartWith("enc:aead:");
      expect(row.refreshSecret).not.toBe("rk-should-be-encrypted");

      // And each decrypts back through the same cipher, bound to (id, column).
      const cipher = createTestCredentialCipher();
      expect(
        await cipher.decrypt(row.secret, credentialAad(credentialId, "secret")),
      ).toBe("sk-should-be-encrypted");
      expect(
        await cipher.decrypt(
          row.refreshSecret,
          credentialAad(credentialId, "refreshSecret"),
        ),
      ).toBe("rk-should-be-encrypted");
    });

    test("re-encrypts a rotated secret at rest on update", async () => {
      const app = await setup();
      const createRes = await app.request(
        `/api/tenants/${TENANT_ID}/credentials`,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            providerId: PROVIDER_ID,
            name: "rotate-me",
            type: "api_key",
            secret: "sk-original",
          }),
        },
      );
      expect(createRes.status).toBe(201);
      const created: unknown = await createRes.json();
      if (!isObject(created)) throw new Error("expected object body");
      const credentialId = created["id"];
      if (typeof credentialId !== "string") {
        throw new Error("expected credential id");
      }

      const patchRes = await app.request(
        `/api/tenants/${TENANT_ID}/credentials/${credentialId}`,
        {
          method: "PATCH",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ secret: "sk-rotated" }),
        },
      );
      expect(patchRes.status).toBe(200);

      // The rotated secret is stored as ciphertext of the NEW value, not the
      // plaintext -- the update path encrypts just like the insert path.
      const [row] = await h.db
        .select()
        .from(credential)
        .where(eq(credential.id, credentialId));
      if (row === undefined) throw new Error("credential row not found");
      expect(row.secret).toStartWith("enc:aead:");
      expect(row.secret).not.toBe("sk-rotated");
      const cipher = createTestCredentialCipher();
      expect(
        await cipher.decrypt(row.secret, credentialAad(credentialId, "secret")),
      ).toBe("sk-rotated");
    });
  },
);

describe.skipIf(!harnessDbEnvAvailable())(
  "DELETE /api/tenants/:tenantId/credentials/:credentialId",
  () => {
    test("removes the owner's per-credential use-grant when a personal credential is deleted", async () => {
      const app = await setup();
      const createRes = await app.request(
        `/api/tenants/${TENANT_ID}/credentials`,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            providerId: PROVIDER_ID,
            name: "delete-me",
            type: "api_key",
            secret: "sk-personal",
            principalId: OWNER_PRINCIPAL_ID,
          }),
        },
      );
      expect(createRes.status).toBe(201);
      const created: unknown = await createRes.json();
      if (!isObject(created)) throw new Error("expected object body");
      const credentialId = created["id"];
      if (typeof credentialId !== "string") {
        throw new Error("expected credential id");
      }

      expect(await useGrantsFor(credentialId, OWNER_PRINCIPAL_ID)).toHaveLength(
        1,
      );

      const deleteRes = await app.request(
        `/api/tenants/${TENANT_ID}/credentials/${credentialId}`,
        { method: "DELETE" },
      );
      expect(deleteRes.status).toBe(204);

      expect(await useGrantsFor(credentialId, OWNER_PRINCIPAL_ID)).toHaveLength(
        0,
      );
    });

    test("deletes an organizational credential that has no use-grant", async () => {
      const app = await setup();
      const createRes = await app.request(
        `/api/tenants/${TENANT_ID}/credentials`,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            providerId: PROVIDER_ID,
            name: "org-delete-me",
            type: "api_key",
            secret: "sk-org",
          }),
        },
      );
      expect(createRes.status).toBe(201);
      const created: unknown = await createRes.json();
      if (!isObject(created)) throw new Error("expected object body");
      const credentialId = created["id"];
      if (typeof credentialId !== "string") {
        throw new Error("expected credential id");
      }

      const deleteRes = await app.request(
        `/api/tenants/${TENANT_ID}/credentials/${credentialId}`,
        { method: "DELETE" },
      );
      expect(deleteRes.status).toBe(204);
    });

    test("returns 404 when the credential does not exist", async () => {
      const app = await setup();
      const deleteRes = await app.request(
        `/api/tenants/${TENANT_ID}/credentials/crd_missing`,
        { method: "DELETE" },
      );
      expect(deleteRes.status).toBe(404);
    });

    test("returns 409 and preserves the credential when a model provider references it", async () => {
      const app = await setup();
      const createRes = await app.request(
        `/api/tenants/${TENANT_ID}/credentials`,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            providerId: PROVIDER_ID,
            name: "bound",
            type: "api_key",
            secret: "sk-personal",
            principalId: OWNER_PRINCIPAL_ID,
          }),
        },
      );
      expect(createRes.status).toBe(201);
      const created: unknown = await createRes.json();
      if (!isObject(created)) throw new Error("expected object body");
      const credentialId = created["id"];
      if (typeof credentialId !== "string") {
        throw new Error("expected credential id");
      }

      await seedModelProvider(h.db, {
        id: "mp_bound",
        tenantId: TENANT_ID,
        name: "bound-provider",
        plugin: "openai",
        baseURL: "https://api.openai.com",
        credentialId,
      });

      const deleteRes = await app.request(
        `/api/tenants/${TENANT_ID}/credentials/${credentialId}`,
        { method: "DELETE" },
      );
      expect(deleteRes.status).toBe(409);
      const body: unknown = await deleteRes.json();
      if (!isObject(body)) throw new Error("expected object body");
      const error = body["error"];
      if (!isObject(error)) throw new Error("expected error object");
      expect(error["code"]).toBe("conflict");

      // The rejected delete leaves the credential and its use-grant intact.
      const [row] = await h.db
        .select()
        .from(credential)
        .where(eq(credential.id, credentialId));
      expect(row).toBeDefined();
      expect(await useGrantsFor(credentialId, OWNER_PRINCIPAL_ID)).toHaveLength(
        1,
      );
    });

    test("returns 404 without disclosing a referenced credential in another tenant", async () => {
      const app = await setup();

      // A credential in a different tenant, referenced by a model provider
      // there. The acting principal holds a wildcard `credential:*` grant, so
      // requireGrant admits the request; tenant isolation is owned by the
      // delete's WHERE clause. A pre-check that queried the referencing row
      // globally would leak the credential's existence as a 409.
      const OTHER_TENANT_ID = "tnt_other";
      await seedTenants(h.db, [{ id: OTHER_TENANT_ID }]);
      await seedProvider(h.db, {
        id: "prv_other",
        tenantId: OTHER_TENANT_ID,
        name: "openai",
      });
      await seedCredential(h.db, {
        id: "crd_foreign",
        tenantId: OTHER_TENANT_ID,
        providerId: "prv_other",
        name: "foreign",
        type: "api_key",
        secret: "sk",
      });
      await seedModelProvider(h.db, {
        id: "mp_foreign",
        tenantId: OTHER_TENANT_ID,
        name: "foreign-provider",
        plugin: "openai",
        baseURL: "https://api.openai.com",
        credentialId: "crd_foreign",
      });

      const deleteRes = await app.request(
        `/api/tenants/${TENANT_ID}/credentials/crd_foreign`,
        { method: "DELETE" },
      );
      expect(deleteRes.status).toBe(404);

      // The foreign credential is untouched -- the delete matched no row in the
      // caller's tenant and never reached the foreign referencing row.
      const creds = await h.db
        .select({ id: credential.id })
        .from(credential)
        .where(eq(credential.id, "crd_foreign"));
      expect(creds).toHaveLength(1);
    });

    test("removes every grant naming the exact resource but spares the wildcard and prefix siblings", async () => {
      const app = await setup();
      const createRes = await app.request(
        `/api/tenants/${TENANT_ID}/credentials`,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            providerId: PROVIDER_ID,
            name: "guarded",
            type: "api_key",
            secret: "sk-personal",
            principalId: OWNER_PRINCIPAL_ID,
          }),
        },
      );
      expect(createRes.status).toBe(201);
      const created: unknown = await createRes.json();
      if (!isObject(created)) throw new Error("expected object body");
      const credentialId = created["id"];
      if (typeof credentialId !== "string") {
        throw new Error("expected credential id");
      }

      // Alongside the auto-minted `credential:{id}` / `use` grant, seed a
      // same-resource `manage` grant (must also be removed), a coarse
      // `credential:*` wildcard grant (must survive), and a prefix-sibling
      // `credential:{id}-other` grant (must survive). Together these guard the
      // two design decisions: exact-match never touches the wildcard, and the
      // delete is action-agnostic.
      await h.db.insert(grantTable).values([
        {
          id: "grn_extra_manage",
          tenantId: TENANT_ID,
          principalId: OWNER_PRINCIPAL_ID,
          resource: `credential:${credentialId}`,
          action: "manage",
          effect: "allow",
          origin: "invoker",
        },
        {
          id: "grn_wildcard",
          tenantId: TENANT_ID,
          principalId: OWNER_PRINCIPAL_ID,
          resource: "credential:*",
          action: "use",
          effect: "allow",
          origin: "invoker",
        },
        {
          id: "grn_sibling",
          tenantId: TENANT_ID,
          principalId: OWNER_PRINCIPAL_ID,
          resource: `credential:${credentialId}-other`,
          action: "use",
          effect: "allow",
          origin: "invoker",
        },
      ]);

      const deleteRes = await app.request(
        `/api/tenants/${TENANT_ID}/credentials/${credentialId}`,
        { method: "DELETE" },
      );
      expect(deleteRes.status).toBe(204);

      const remaining = await h.db
        .select()
        .from(grantTable)
        .where(eq(grantTable.tenantId, TENANT_ID));
      const remainingResources = remaining.map(
        (g) => `${g.resource}/${g.action}`,
      );

      // Both grants naming the exact resource are gone (use + manage).
      expect(remainingResources).not.toContain(
        `credential:${credentialId}/use`,
      );
      expect(remainingResources).not.toContain(
        `credential:${credentialId}/manage`,
      );
      // The wildcard grant and the prefix sibling are untouched.
      expect(remainingResources).toContain("credential:*/use");
      expect(remainingResources).toContain(
        `credential:${credentialId}-other/use`,
      );
    });
  },
);

describe.skipIf(!harnessDbEnvAvailable())(
  "credential delete foreign-key error shape",
  () => {
    test("a restrict violation on credential delete is recognized as a referenced-row violation", async () => {
      // The DELETE handler's catch maps a model_provider.credential_id restrict
      // violation to a 409; its isReferencedRowViolation check reads the
      // SQLSTATE via pgErrorCode. Pin the empirical fact this rests on: a real
      // restrict-foreign-key delete through drizzle surfaces a referenced-row
      // violation on the error cause chain -- 23001 (restrict_violation) or
      // 23503 (foreign_key_violation), depending on the Postgres version. The
      // route 409 test drives the catch end to end; a driver-wrapping change is
      // caught here.
      await seedTenants(h.db, [{ id: TENANT_ID }]);
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
      await seedCredential(h.db, {
        id: "crd_bound",
        tenantId: TENANT_ID,
        providerId: PROVIDER_ID,
        principalId: OWNER_PRINCIPAL_ID,
        name: "bound",
        type: "api_key",
        secret: "sk",
      });
      await seedModelProvider(h.db, {
        id: "mp_ref",
        tenantId: TENANT_ID,
        name: "ref",
        plugin: "openai",
        baseURL: "https://api.openai.com",
        credentialId: "crd_bound",
      });

      let caught: unknown = undefined;
      let deletedWithoutError = false;
      try {
        await h.db.transaction(async (tx) => {
          await tx
            .delete(credential)
            .where(eq(credential.id, "crd_bound"))
            .returning();
        });
        deletedWithoutError = true;
      } catch (err) {
        caught = err;
      }

      expect(deletedWithoutError).toBe(false);
      const code = pgErrorCode(caught);
      if (code === undefined) {
        throw new Error("expected a SQLSTATE on the caught error");
      }
      expect(["23001", "23503"]).toContain(code);
    });
  },
);

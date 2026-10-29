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
import {
  credential,
  grant as grantTable,
  workflowDefinition,
} from "@intx/db/schema";
import type { GrantRule } from "@intx/types/authz";
import { credentialAad } from "@intx/types";
import { waitUntil } from "@intx/types/testing";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { createTestCredentialCipher } from "@intx/test-harness/crypto";
import {
  seedCredential,
  seedModel,
  seedModelOffering,
  seedModelProvider,
  seedPrincipal,
  seedProvider,
  seedTenants,
  seedWorkflowRun,
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

// A sidecar router that records `sendCredentialsUpdate` calls so a test can
// observe the fire-and-forget revoke broadcast a route triggers.
function capturingCredentialsRouter(): {
  router: SidecarRouter;
  calls: {
    address: string;
    revoke: string[] | undefined;
    materialsCount: number;
    secretVersions: (number | undefined)[];
  }[];
} {
  const base = createMockSidecarRouter();
  const calls: {
    address: string;
    revoke: string[] | undefined;
    materialsCount: number;
    secretVersions: (number | undefined)[];
  }[] = [];
  return {
    calls,
    router: {
      ...base,
      sendCredentialsUpdate: async (address, delivery, revoke) => {
        calls.push({
          address,
          revoke,
          materialsCount: delivery.materials.length,
          secretVersions: delivery.materials.map((m) => m.secretVersion),
        });
      },
    },
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

async function setup(opts?: { sidecarRouter?: SidecarRouter }) {
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
    sidecarRouter: opts?.sidecarRouter ?? createMockSidecarRouter(),
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

    test("advances the secret version only when the secret changes", async () => {
      const app = await setup();
      const createRes = await app.request(
        `/api/tenants/${TENANT_ID}/credentials`,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            providerId: PROVIDER_ID,
            name: "versioned",
            type: "api_key",
            secret: "sk-1",
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
      const patch = async (body: Record<string, unknown>) => {
        const res = await app.request(
          `/api/tenants/${TENANT_ID}/credentials/${credentialId}`,
          {
            method: "PATCH",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(body),
          },
        );
        expect(res.status).toBe(200);
      };
      const secretVersion = async () => {
        const [row] = await h.db
          .select({ secretVersion: credential.secretVersion })
          .from(credential)
          .where(eq(credential.id, credentialId));
        if (row === undefined) throw new Error("credential row not found");
        return row.secretVersion;
      };

      expect(await secretVersion()).toBe(1);
      await patch({ secret: "sk-2" });
      expect(await secretVersion()).toBe(2);
      await patch({ description: "renamed" });
      expect(await secretVersion()).toBe(2);
      await patch({ secret: "sk-3" });
      expect(await secretVersion()).toBe(3);
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

describe.skipIf(!harnessDbEnvAvailable())(
  "credential revoke broadcast to live deployments",
  () => {
    async function createCredential(
      app: Awaited<ReturnType<typeof setup>>,
    ): Promise<string> {
      const res = await app.request(`/api/tenants/${TENANT_ID}/credentials`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          providerId: PROVIDER_ID,
          name: "revoke-me",
          type: "api_key",
          secret: "sk-revoke",
        }),
      });
      expect(res.status).toBe(201);
      const body: unknown = await res.json();
      if (!isObject(body) || typeof body["id"] !== "string") {
        throw new Error("expected a credential id");
      }
      return body["id"];
    }

    // One running instance run (anchorRunId null), one running deployment-anchor
    // run (anchorRunId non-null -- the case the source-update push excludes), one
    // deployment anchor still "deployed" (never triggered, but already holding
    // its deploy-time credentials), and one completed run. A revoke must reach
    // the three live addresses and skip the completed run.
    async function seedRuns(): Promise<void> {
      await seedWorkflowRun(h.db, {
        id: "run_instance",
        tenantId: TENANT_ID,
        address: "instance@test.interchange",
        status: "running",
      });
      await seedWorkflowRun(h.db, {
        id: "run_anchor",
        tenantId: TENANT_ID,
        anchorRunId: "run_instance",
        address: "anchor@test.interchange",
        status: "running",
      });
      await seedWorkflowRun(h.db, {
        id: "run_deployed",
        tenantId: TENANT_ID,
        anchorRunId: "run_deployed",
        address: "deployed@test.interchange",
        status: "deployed",
      });
      await seedWorkflowRun(h.db, {
        id: "run_done",
        tenantId: TENANT_ID,
        address: "done@test.interchange",
        status: "completed",
      });
    }

    test("DELETE broadcasts a flat revoke to every live run, untriggered anchor included, and skips a completed run", async () => {
      const cap = capturingCredentialsRouter();
      const app = await setup({ sidecarRouter: cap.router });
      const credentialId = await createCredential(app);
      await seedRuns();

      const res = await app.request(
        `/api/tenants/${TENANT_ID}/credentials/${credentialId}`,
        { method: "DELETE" },
      );
      expect(res.status).toBe(204);

      // pushCredentialRevoke enumerates its address set in one query, then
      // calls sendCredentialsUpdate for every address in one synchronous map,
      // so the call list is complete the moment it reaches three. A broadcast to
      // the completed run would already be recorded here, which is why the
      // exact-length assertion below needs no settling delay ahead of it.
      await waitUntil(() => cap.calls.length >= 3);

      expect(cap.calls).toHaveLength(3);
      expect(new Set(cap.calls.map((c) => c.address))).toEqual(
        new Set([
          "instance@test.interchange",
          "anchor@test.interchange",
          "deployed@test.interchange",
        ]),
      );
      for (const call of cap.calls) {
        // A pure revoke: empty delivery, the deleted id named in revoke.
        expect(call.materialsCount).toBe(0);
        expect(call.revoke).toEqual([credentialId]);
      }
    });

    test("PATCH status:revoked broadcasts a flat revoke", async () => {
      const cap = capturingCredentialsRouter();
      const app = await setup({ sidecarRouter: cap.router });
      const credentialId = await createCredential(app);
      await seedRuns();

      const res = await app.request(
        `/api/tenants/${TENANT_ID}/credentials/${credentialId}`,
        {
          method: "PATCH",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ status: "revoked" }),
        },
      );
      expect(res.status).toBe(200);

      // One enumerate-then-fan-out, as in the DELETE case above: reaching three
      // calls means the revoke pushed to exactly the addresses it resolved.
      await waitUntil(() => cap.calls.length >= 3);

      expect(cap.calls).toHaveLength(3);
      expect(new Set(cap.calls.map((c) => c.address))).toEqual(
        new Set([
          "instance@test.interchange",
          "anchor@test.interchange",
          "deployed@test.interchange",
        ]),
      );
      for (const call of cap.calls) {
        expect(call.materialsCount).toBe(0);
        expect(call.revoke).toEqual([credentialId]);
      }
    });

    test("a PATCH that does not revoke sends no credential broadcast", async () => {
      const cap = capturingCredentialsRouter();
      const app = await setup({ sidecarRouter: cap.router });
      const credentialId = await createCredential(app);
      await seedRuns();

      const res = await app.request(
        `/api/tenants/${TENANT_ID}/credentials/${credentialId}`,
        {
          method: "PATCH",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ name: "renamed" }),
        },
      );
      expect(res.status).toBe(200);

      // No secret rotation and no revocation, so the route starts no push at
      // all: there is no promise to await and no state that a predicate could
      // poll, because the awaited outcome is the absence of a call. Elapsed
      // time is the only evidence available for this negative, so the sleep
      // stays. It is safe in the one direction that matters -- a loaded machine
      // makes the window longer in real terms, so the sleep can only
      // under-detect a stray push, never fail a correct run.
      await new Promise((r) => setTimeout(r, 30));
      expect(cap.calls).toHaveLength(0);
    });
  },
);

describe.skipIf(!harnessDbEnvAvailable())(
  "credential rotation push to live deployments",
  () => {
    const ROTATION_PROVIDER_ID = "prv_rotation";

    // A deployment anchor that has not been triggered yet and holds the
    // credential in its delivered refs, and a completed one that also held it.
    async function seedRotation(
      app: Awaited<ReturnType<typeof setup>>,
    ): Promise<string> {
      // The rotation re-resolves the secret's origin from its provider, so the
      // provider needs an API base URL.
      await seedProvider(h.db, {
        id: ROTATION_PROVIDER_ID,
        tenantId: TENANT_ID,
        name: "rotation",
        apiBaseUrl: "https://api.rotation.test",
      });
      const res = await app.request(`/api/tenants/${TENANT_ID}/credentials`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          providerId: ROTATION_PROVIDER_ID,
          name: "rotate-live",
          type: "api_key",
          secret: "sk-1",
        }),
      });
      expect(res.status).toBe(201);
      const body: unknown = await res.json();
      if (!isObject(body) || typeof body["id"] !== "string") {
        throw new Error("expected a credential id");
      }
      const credentialId = body["id"];
      for (const [id, status] of [
        ["run_live", "deployed"],
        ["run_ended", "completed"],
      ] as const) {
        await seedWorkflowRun(h.db, {
          id,
          tenantId: TENANT_ID,
          anchorRunId: id,
          address: `${id}@test.interchange`,
          status,
          credentialRefs: { credentialIds: [credentialId], bindings: [] },
        });
      }
      return credentialId;
    }

    test("a secret PATCH pushes the rotated secret to a live deployment that holds it", async () => {
      const cap = capturingCredentialsRouter();
      const app = await setup({ sidecarRouter: cap.router });
      const credentialId = await seedRotation(app);

      const res = await app.request(
        `/api/tenants/${TENANT_ID}/credentials/${credentialId}`,
        {
          method: "PATCH",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ secret: "sk-2" }),
        },
      );
      expect(res.status).toBe(200);

      await waitUntil(() => cap.calls.length >= 1);
      expect(cap.calls).toEqual([
        {
          address: "run_live@test.interchange",
          revoke: undefined,
          materialsCount: 1,
          secretVersions: [2],
        },
      ]);
    });

    test("a PATCH that rotates and revokes in one request sends only the revoke", async () => {
      const cap = capturingCredentialsRouter();
      const sources: string[] = [];
      const app = await setup({
        sidecarRouter: {
          ...cap.router,
          sendSourcesUpdate: async (address) => {
            sources.push(address);
          },
        },
      });
      const credentialId = await seedRotation(app);
      // A running instance whose model has a second offering behind another
      // credential. A rotation push would re-push its sources onto that
      // offering.
      await seedCredential(h.db, {
        id: "crd_other",
        tenantId: TENANT_ID,
        providerId: ROTATION_PROVIDER_ID,
        name: "other",
        type: "api_key",
        secret: await createTestCredentialCipher().encrypt(
          "sk-other",
          credentialAad("crd_other", "secret"),
        ),
      });
      await seedModel(h.db, {
        id: "mdl_opus",
        tenantId: TENANT_ID,
        canonicalName: "opus",
      });
      for (const [index, credId] of [credentialId, "crd_other"].entries()) {
        await seedModelProvider(h.db, {
          id: `mpv_${String(index)}`,
          tenantId: TENANT_ID,
          name: `mpv-${String(index)}`,
          credentialId: credId,
        });
        await seedModelOffering(h.db, {
          id: `mof_${String(index)}`,
          tenantId: TENANT_ID,
          modelId: "mdl_opus",
          providerId: `mpv_${String(index)}`,
          priority: index,
          capabilities: [],
        });
      }
      await h.db.insert(workflowDefinition).values({
        id: "wfd_instance",
        tenantId: TENANT_ID,
        creatorPrincipalId: null,
        name: "instance",
        modelRequirements: [{ model: "opus" }],
      });
      await seedWorkflowRun(h.db, {
        id: "run_instance",
        tenantId: TENANT_ID,
        definitionId: "wfd_instance",
        address: "instance@test.interchange",
        status: "running",
      });

      const res = await app.request(
        `/api/tenants/${TENANT_ID}/credentials/${credentialId}`,
        {
          method: "PATCH",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ secret: "sk-2", status: "revoked" }),
        },
      );
      expect(res.status).toBe(200);

      await waitUntil(() => cap.calls.length >= 2);
      // A rotation push would arrive after the revoke's single fan-out; the
      // same elapsed-time negative as the no-revoke PATCH case above.
      await new Promise((r) => setTimeout(r, 30));
      expect(new Set(cap.calls.map((c) => c.address))).toEqual(
        new Set(["run_live@test.interchange", "instance@test.interchange"]),
      );
      for (const call of cap.calls) {
        expect(call.materialsCount).toBe(0);
        expect(call.revoke).toEqual([credentialId]);
      }
      expect(cap.calls).toHaveLength(2);
      expect(sources).toEqual([]);
    });
  },
);

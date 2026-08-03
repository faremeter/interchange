import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { eq, and } from "drizzle-orm";
import { type } from "arktype";

import { createApp, type GetSession } from "@intx/hub-api";
import { createGrantStore } from "@intx/db";
import { grant, workflowDefinition, workflowRun } from "@intx/db/schema";
import {
  createSidecarRouter,
  type AssetService,
  type EventCollectorRegistry,
  type RepoStore,
  type SessionService,
  type SidecarAuthenticator,
} from "@intx/hub-sessions";
import { synthesizeFoldedWorkflow } from "@intx/workflow-deploy/testing";
import type { CredentialBinding } from "@intx/types";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedCredential,
  seedGrant,
  seedModel,
  seedModelOffering,
  seedModelProvider,
  seedPrincipal,
  seedProvider,
  seedTenants,
} from "@intx/test-harness/seed";

// Exercises the launch path's credential-binding resolution end to end against
// a real database: POST /api/tenants/:tenantId/workflows/runs reads the
// definition's credential bindings from the folded asset body, resolves each to
// a tenant-owned credential, and materializes a consumer-scoped `credential:{id}`
// / `use` grant on the instance principal. A tenant-owned credential's use is
// authorized by ownership within the tenant hierarchy (the same walk-up the
// resolution runs), not by a personal grant on the creator -- so the grant
// materializes with origin `system` and no creator grant is consulted. Nothing
// else in the suite drives a real credential resolution through the launch
// route, so these tests pin that the binding resolves, authorizes by ownership,
// stamps the consumer condition, and fails closed when it does not resolve.

const TENANT = "tnt_cb";
const USER = "usr_caller";
const CALLER = "prn_caller";
const CREATOR = "prn_creator";
const DEF = "wfd_cb";
const ASSET = "ast_cb";

// The inference path runs BEFORE binding resolution and emits a
// credential-backed model provider's source only when the tenant owns that
// model credential (a wallet-backed provider is non-launchable, so it cannot
// stand in). The model credential is tenant-owned in every test so the launch
// reaches the binding code; the binding uses a SEPARATE credential per test.
const MODEL_CRED = "crd_model";
const MODEL_PROVIDER = "prv_model";
const CATALOG_PROVIDER = "mpv_cb";
const MODEL = "mdl_cb";
const OFFERING = "mof_cb";

// The binding's provider (matched by name) and the credential it resolves.
const BINDING_PROVIDER = "prv_gh";
const BINDING_PROVIDER_NAME = "github";
const BINDING_CRED = "crd_gh";
const BINDING_PACKAGE = "@acme/tools";

function mockGetSession(userId: string): GetSession {
  const now = new Date("2026-01-01");
  return async () => ({
    user: {
      id: userId,
      email: "caller@example.com",
      emailVerified: true,
      name: "Caller",
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

const acceptAnySidecar: SidecarAuthenticator = async ({ sidecarId }) => ({
  kind: "shared",
  sidecarId,
});

// The launch calls deployInstanceAtHead AFTER the DB transaction commits; it
// must resolve (a throw unwinds the run and yields 502, not the grant rows).
// It captures the deploy params so a test can assert the credential delivery
// the hub builds onto the deploy frame.
let capturedDeployParams:
  | Parameters<SessionService["deployInstanceAtHead"]>[0]
  | undefined;

function capturingSessionService(): SessionService {
  const notImpl = (name: string) => (): never => {
    throw new Error(`mock: sessionService.${name} not implemented`);
  };
  return {
    stageWorkflowStep: async () => undefined,
    deployInstanceAtHead: async (params) => {
      capturedDeployParams = params;
      return { publicKey: "pk-instance-mock" };
    },
    deployWorkflowDefinition: notImpl("deployWorkflowDefinition"),
    deploySingleStepAtHead: notImpl("deploySingleStepAtHead"),
    sendUserMessage: notImpl("sendUserMessage"),
    endSession: notImpl("endSession"),
  };
}

function capturingEventCollectors(): EventCollectorRegistry {
  const notImpl = (name: string) => (): never => {
    throw new Error(`mock: eventCollectors.${name} not implemented`);
  };
  return {
    create: () => undefined,
    dispatch: notImpl("dispatch"),
    abandon: () => undefined,
    has: () => false,
    getStatus: () => undefined,
    getAccumulatedText: () => undefined,
    getCurrentTurnId: () => undefined,
    getLastTurnId: () => undefined,
  };
}

// The launch only reads the folded body back through readAssetBlob; the assetId
// is ignored by the stub. A repo store stub is required alongside it because
// createApp enforces the asset/repo XOR.
function mockAssetService(json: string): AssetService {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the launch only exercises readAssetBlob
  return {
    readAssetBlob: async () => new TextEncoder().encode(json),
  } as unknown as AssetService;
}
// eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- unused stub, present only to satisfy the asset/repo XOR
const STUB_REPO_STORE = {} as unknown as RepoStore;

function foldedJson(bindings: readonly CredentialBinding[]): string {
  return JSON.stringify(
    synthesizeFoldedWorkflow({
      workflowId: "wf_cb",
      mailAddress: "cb@test.example",
      systemPrompt: "You are a test agent.",
      description: null,
      inferencePreferences: [],
      toolPackagePins: [],
      credentialBindings: bindings,
    }),
  );
}

const CREATOR_BINDING: CredentialBinding = {
  package: BINDING_PACKAGE,
  handle: "gh",
  provider: BINDING_PROVIDER_NAME,
  locator: "tenant",
};

describe.skipIf(!harnessDbEnvAvailable())(
  "POST /api/tenants/:tenantId/workflows/runs resolves credential bindings (real DB)",
  () => {
    let h: TestDb;

    beforeAll(async () => {
      h = await createTestDb();
    });

    afterAll(async () => {
      await h.close();
    });

    beforeEach(async () => {
      capturedDeployParams = undefined;
      await h.reset();
      await seedTenants(h.db, [{ id: TENANT }]);
      // The launcher (session user) and its route-guard grant.
      await seedPrincipal(h.db, {
        id: CALLER,
        tenantId: TENANT,
        kind: "user",
        refId: USER,
      });
      await seedGrant(h.db, {
        id: "grt_caller_run_create",
        tenantId: TENANT,
        principalId: CALLER,
        resource: "workflow-run:*",
        action: "create",
      });
      // The definition's creator.
      await seedPrincipal(h.db, {
        id: CREATOR,
        tenantId: TENANT,
        kind: "user",
        refId: "creator-user",
      });
      // Inference catalog: a credential-backed model provider for "test-model".
      await seedProvider(h.db, {
        id: MODEL_PROVIDER,
        tenantId: TENANT,
        name: "anthropic-model",
      });
      await seedCredential(h.db, {
        id: MODEL_CRED,
        tenantId: TENANT,
        providerId: MODEL_PROVIDER,
        name: "model-key",
      });
      await seedModel(h.db, {
        id: MODEL,
        tenantId: TENANT,
        canonicalName: "test-model",
      });
      await seedModelProvider(h.db, {
        id: CATALOG_PROVIDER,
        tenantId: TENANT,
        name: "anthropic",
        credentialId: MODEL_CRED,
      });
      await seedModelOffering(h.db, {
        id: OFFERING,
        tenantId: TENANT,
        modelId: MODEL,
        providerId: CATALOG_PROVIDER,
      });
      // The model credential is tenant-owned, so the inference source is
      // emitted by ownership -- no grant is seeded -- and the launch always
      // reaches the binding code rather than 409-ing on the model source first.
      // The asset the launch hydrates the folded body from, and the definition
      // row pointing at it.
      await seedAsset(h.db, {
        id: ASSET,
        tenantId: TENANT,
        kind: "workflow",
        name: ASSET,
        creatorPrincipalId: CREATOR,
      });
      await h.db.insert(workflowDefinition).values({
        id: DEF,
        tenantId: TENANT,
        creatorPrincipalId: CREATOR,
        assetId: ASSET,
        name: "cb-def",
        modelRequirements: [{ model: "test-model" }],
        status: "deployed",
      });
      // The binding's provider always exists; whether a credential resolves it
      // varies per test.
      await seedProvider(h.db, {
        id: BINDING_PROVIDER,
        tenantId: TENANT,
        name: BINDING_PROVIDER_NAME,
        // The bound credential is delivered as an origin-pinned handle, so its
        // provider must declare an API origin.
        apiBaseUrl: "https://api.example.test",
      });
    });

    function launch(bindings: readonly CredentialBinding[]) {
      const app = createApp({
        getSession: mockGetSession(USER),
        authHandler: () => new Response("", { status: 404 }),
        db: h.db,
        grantStore: createGrantStore(h.db),
        sidecarRouter: createSidecarRouter({
          authenticateSidecar: acceptAnySidecar,
        }),
        sessionService: capturingSessionService(),
        eventCollectors: capturingEventCollectors(),
        assetService: mockAssetService(foldedJson(bindings)),
        repoStore: STUB_REPO_STORE,
        maxTarballBytes: 10_000_000,
      });
      return app.request(`/api/tenants/${TENANT}/workflows/runs`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ definitionId: DEF }),
      });
    }

    async function instancePrincipalId(): Promise<string> {
      const runs = await h.db
        .select()
        .from(workflowRun)
        .where(eq(workflowRun.definitionId, DEF));
      const principalId = runs[0]?.principalId;
      if (typeof principalId !== "string") {
        throw new Error("launched run carries no instance principal id");
      }
      return principalId;
    }

    test("materializes a consumer-scoped credential:{id}/use grant by tenant ownership, no personal grant", async () => {
      // Tenant-owned credential (no principalId). No creator grant is seeded:
      // authority comes purely from ownership within the tenant hierarchy.
      await seedCredential(h.db, {
        id: BINDING_CRED,
        tenantId: TENANT,
        providerId: BINDING_PROVIDER,
        name: "gh-key",
      });

      const res = await launch([CREATOR_BINDING]);
      expect(res.status).toBe(201);

      const principalId = await instancePrincipalId();
      const grants = await h.db
        .select()
        .from(grant)
        .where(
          and(
            eq(grant.principalId, principalId),
            eq(grant.resource, `credential:${BINDING_CRED}`),
            eq(grant.action, "use"),
          ),
        );
      expect(grants).toHaveLength(1);
      expect(grants[0]).toMatchObject({
        effect: "allow",
        // Ownership-derived tenant authority materializes as a system-origin
        // grant, not a delegated creator grant.
        origin: "system",
        conditions: { tool: `tool:${BINDING_PACKAGE}` },
      });

      // The hub also builds the credential delivery onto the deploy frame: the
      // per-handle descriptor plus the decrypted material (secret + provider
      // facts), keyed by credentialId. The test app runs the noop cipher, so
      // the delivered secret is the stored value verbatim.
      expect(capturedDeployParams?.credentials).toEqual({
        bindings: [
          {
            handle: "gh",
            credentialId: BINDING_CRED,
            consumer: `tool:${BINDING_PACKAGE}`,
          },
        ],
        materials: [
          {
            credentialId: BINDING_CRED,
            providerKey: "test-plugin",
            origin: "https://api.example.test",
            secret: `${BINDING_CRED}-secret`,
          },
        ],
      });
    });

    test("rejects the launch 409 when no credential resolves the binding", async () => {
      // The provider exists but carries no credential, so resolution returns
      // null -- a launch-blocking configuration failure, not an authority one.
      const res = await launch([CREATOR_BINDING]);
      expect(res.status).toBe(409);
      const body = type({
        error: { code: "string", "message?": "string" },
      }).assert(await res.json());
      expect(body.error.code).toBe("not_launchable");
    });

    test("rejects the launch 409 when the binding is ambiguous", async () => {
      // Two active tenant-owned credentials on the same provider with no name
      // to disambiguate: resolution throws, which the handler catches and maps
      // to the same fail-closed 409 rather than a 500.
      await seedCredential(h.db, {
        id: "crd_gh_a",
        tenantId: TENANT,
        providerId: BINDING_PROVIDER,
        name: "gh-a",
      });
      await seedCredential(h.db, {
        id: "crd_gh_b",
        tenantId: TENANT,
        providerId: BINDING_PROVIDER,
        name: "gh-b",
      });

      const res = await launch([CREATOR_BINDING]);
      expect(res.status).toBe(409);
      const body = type({
        error: { code: "string", "message?": "string" },
      }).assert(await res.json());
      expect(body.error.code).toBe("not_launchable");
    });
  },
);

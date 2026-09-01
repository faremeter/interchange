// Route-to-resolution coverage for a credential-bound workflow deploy.
//
// A definition that carries a `credentialBindings` entry is deployed through the
// PRODUCTION `POST /workflows/deployments` route on the shared (non-exclusive)
// path, against a REAL session service backed by real Postgres and a live
// sidecar subprocess. The route threads the app's credential cipher into
// `deployWorkflowFromSource`, which resolves the binding against the DB-seeded
// provider + credential and DECRYPTS the stored secret through that cipher
// before it emits the deploy frame. The deploy returns only after the sidecar
// acks the frame, so a 201 proves the binding resolved and decrypted -- the
// resolution runs strictly before the frame send.
//
// The existing coverage leaves this exact path unguarded: the route unit test
// drives a MOCKED session service (the real deploy never runs), and the two
// real-credential-decryption tests call the deploy composition directly,
// bypassing both the route and the shared deploy method. The cipher forward is
// an optional field at every hop, so dropping it compiles clean; only a deploy
// that resolves a real binding through the route catches a regression.
//
// The tests exercise the same seeded ciphertext through the route:
//   * Positive: the app holds the SAME key the secret was sealed under, so the
//     binding resolves and decrypts -- 201. If the route dropped the cipher or
//     the shared deploy stopped forwarding it, resolution fails closed on the
//     "no credentialCipher was supplied" guard and the deploy is 502; the
//     positive assertion catches that.
//   * Wrong key: the app holds a DIFFERENT key over the same ciphertext, so the
//     AEAD decrypt refuses the key-id mismatch and the deploy fails closed
//     (502). If the cipher stopped being invoked (a pass-through regression),
//     the wrong key would 201; this assertion catches that.
//   * Keyless: the app is built with NO cipher, so it resolves the noop cipher
//     (a hub booted without CREDENTIAL_ENCRYPTION_KEY). The noop refuses the
//     real ciphertext rather than passing it through, so the deploy fails
//     closed (502) instead of delivering an un-decrypted secret -- the
//     misconfigured-production case.
//
// The credential delivery is not observable as a return value or a DB row (it
// rides the sidecar frame), and a real AES-256-GCM decrypt authenticates the
// plaintext against key and AAD -- so a 201 through the matched cipher already
// proves the delivered bytes are the authentic seeded secret. The run rail that
// carries that secret to a tool is covered by `single-step-credential-tool`.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { createEnvKeyCredentialCipher } from "@intx/crypto";
import { createGrantStore } from "@intx/db";
import { tenant as tenantTable } from "@intx/db/schema";
import { createApp, type GetSession } from "@intx/hub-api";
import {
  createAssetService,
  createSessionService,
  DEFAULT_ASSET_REF,
  type EventCollectorRegistry,
  type SessionService,
} from "@intx/hub-sessions";
import { credentialAad } from "@intx/types";
import type { InferenceSource } from "@intx/types/runtime";
import type { WorkflowDefinitionAssetSource } from "@intx/types/workflow-sources";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { createTestCredentialCipher } from "@intx/test-harness/crypto";
import {
  seedAsset,
  seedCredential,
  seedGrant,
  seedPrincipal,
  seedProvider,
} from "@intx/test-harness/seed";

import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  startDeployFlowEnv,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { bundleWorkflowEntry } from "../hub-agent/lib/bundle-workflow-entry";
import { CREDENTIAL_HANDLE } from "./fixtures/credential-tool-bundle";
import { credentialToolWorkflowEntry } from "./fixtures/credential-tool-workflow";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const TENANT_ID = "tnt_credential_route";
const CALLER_USER_ID = "usr_credential_route_caller";
const CALLER_PRINCIPAL_ID = "prn_credential_route_caller";
const PROVIDER_ID = "prv_credential_route";
const PROVIDER_NAME = "credential-route-provider";
const CREDENTIAL_ID = "cred_credential_route";
const CREDENTIAL_NAME = "credential-route-cred";
const SECRET = "sk-route-probe-4f7c";

// The binding's trigger address never routes: the test asserts deploy-time
// resolution and never fires a run. The route mints the real deployment address
// server-side, so the source (frozen before the POST) cannot carry it; a fixed
// placeholder in the tenant domain is approved by the probe gate and left inert.
const PLACEHOLDER_ADDRESS = `placeholder@${DEPLOYMENT_DOMAIN}`;

const SOURCE_FIXTURE_PACKAGE_NAME = "@wf/credential-route-fixture";
const SOURCE_FIXTURE_PACKAGE_VERSION = "1.0.0";
const WORKFLOW_ENTRY = "./workflow.mjs";

let env: DeployFlowEnv;
let h: TestDb;
let sessionService: SessionService;

function createMockGetSession(userId: string): GetSession {
  const now = new Date("2025-01-01");
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
      id: "session_credential_route",
      userId,
      token: "tok_credential_route",
      expiresAt: new Date("2999-01-01"),
      createdAt: now,
      updatedAt: now,
    },
  });
}

function notImpl(name: string): never {
  throw new Error(`credential-route mock: ${name} not implemented`);
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

// Seed a `workflow`-kind git source asset carrying the credential-binding
// fixture, returning the asset-source ref the route deploys from. Mirrors the
// source-seeding `deployWorkflowSourceForTest` runs before its direct deploy;
// here the same source is handed to the HTTP route instead. The route uses one
// asset id for both the definition asset row and the git source repo, so the
// caller seeds the asset row under the same id.
async function seedBindingSource(assetId: string): Promise<{
  source: WorkflowDefinitionAssetSource;
}> {
  const entryModule = credentialToolWorkflowEntry({
    systemPrompt: "You are the credential-route probe agent.",
    address: PLACEHOLDER_ADDRESS,
    binding: {
      handle: CREDENTIAL_HANDLE,
      provider: PROVIDER_NAME,
      name: CREDENTIAL_NAME,
    },
  });

  const scratchDir = await fs.mkdtemp(
    path.join(os.tmpdir(), "credential-route-fixture-"),
  );
  let workflowJs: string;
  try {
    workflowJs = await bundleWorkflowEntry(scratchDir, entryModule);
  } finally {
    await fs.rm(scratchDir, { recursive: true, force: true });
  }

  const repoId = { kind: "workflow", id: assetId } as const;
  await env.hub.agentRepoStore.repoStore.initRepo(repoId);
  const { commitSha } = await env.hub.agentRepoStore.repoStore.writeTree(
    { kind: "hub" },
    repoId,
    DEFAULT_ASSET_REF,
    {
      files: {
        "package.json": JSON.stringify({
          name: SOURCE_FIXTURE_PACKAGE_NAME,
          version: SOURCE_FIXTURE_PACKAGE_VERSION,
          interchange: { workflow: WORKFLOW_ENTRY },
        }),
        "workflow.mjs": workflowJs,
      },
      message: `seed credential-route source for ${assetId}`,
    },
  );

  return {
    source: {
      kind: "asset",
      assetId,
      package: { format: "source", commitSha },
    },
  };
}

// Deploy the credential-bound definition through the real route with `appCipher`
// as the app's credential cipher, returning the raw response for assertion. When
// `appCipher` is omitted the app is built with no cipher, so it resolves the
// noop cipher -- the keyless-composition case.
async function deployBindingThroughRoute(opts: {
  assetId: string;
  appCipher?: ReturnType<typeof createTestCredentialCipher>;
}): Promise<Response> {
  await seedAsset(h.db, {
    id: opts.assetId,
    tenantId: TENANT_ID,
    kind: "workflow",
    name: opts.assetId,
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });
  const { source } = await seedBindingSource(opts.assetId);

  const inferenceSource: InferenceSource = {
    id: "anthropic:mock-model",
    provider: "anthropic",
    baseURL: `http://localhost:${String(env.inference.server.port)}`,
    apiKey: "sk-mock",
    model: "mock-model",
  };

  const app = createApp({
    getSession: createMockGetSession(CALLER_USER_ID),
    authHandler: () => new Response("", { status: 404 }),
    db: h.db,
    grantStore: createGrantStore(h.db),
    sidecarRouter: env.hub.router,
    sessionService,
    ...(opts.appCipher !== undefined
      ? { credentialCipher: opts.appCipher }
      : {}),
    eventCollectors: createMockEventCollectors(),
    assetService: createAssetService({
      db: h.db,
      repoStore: env.hub.agentRepoStore.repoStore,
    }),
    repoStore: env.hub.agentRepoStore.repoStore,
    maxTarballBytes: 10_000_000,
  });

  return app.request(`/api/tenants/${TENANT_ID}/workflows/deployments`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      source,
      entry: WORKFLOW_ENTRY,
      sources: [inferenceSource],
      defaultSource: inferenceSource.id,
    }),
  });
}

describe.skipIf(!harnessDbEnvAvailable())(
  "a credential-bound workflow deploy through the real route resolves its binding",
  () => {
    beforeAll(async () => {
      h = await createTestDb();
      await h.db.insert(tenantTable).values({
        id: TENANT_ID,
        name: TENANT_ID,
        slug: TENANT_ID,
        domain: DEPLOYMENT_DOMAIN,
        parentId: null,
      });
      await seedPrincipal(h.db, {
        id: CALLER_PRINCIPAL_ID,
        tenantId: TENANT_ID,
        kind: "user",
        refId: CALLER_USER_ID,
        status: "active",
      });
      // The deploy route gates on `workflow:*` / `create` for the caller.
      await seedGrant(h.db, {
        id: "grant-credential-route-create",
        tenantId: TENANT_ID,
        resource: "workflow:*",
        action: "create",
        effect: "allow",
        origin: "system",
        principalId: CALLER_PRINCIPAL_ID,
      });
      // The provider backs an origin-pinned credential: a non-empty `apiBaseUrl`
      // is required for the binding to resolve (an un-pinnable secret fails
      // closed). The origin is never contacted -- no run is fired.
      await seedProvider(h.db, {
        id: PROVIDER_ID,
        tenantId: TENANT_ID,
        name: PROVIDER_NAME,
        plugin: "http",
        apiBaseUrl: "https://credential-route-origin.test",
      });
      // Seed the secret as CIPHERTEXT sealed under the test key, so the positive
      // deploy proves a real decrypt (not a no-op pass-through). `seedCredential`
      // otherwise stores the secret verbatim. Tenant-owned (principalId null) so
      // the `tenant` locator resolves it.
      await seedCredential(h.db, {
        id: CREDENTIAL_ID,
        tenantId: TENANT_ID,
        providerId: PROVIDER_ID,
        name: CREDENTIAL_NAME,
        secret: await createTestCredentialCipher().encrypt(
          SECRET,
          credentialAad(CREDENTIAL_ID, "secret"),
        ),
      });

      env = await startDeployFlowEnv({});

      // The route resolves credentials off the session service's
      // construction-time db, so it must be bound to real Postgres -- the
      // harness's own session service uses an in-memory stub. Reuse the harness's
      // real sidecar-connected router and repo store.
      sessionService = createSessionService({
        sidecarRouter: env.hub.router,
        agentRepoStore: env.hub.agentRepoStore,
        assetService: createAssetService({
          db: h.db,
          repoStore: env.hub.agentRepoStore.repoStore,
        }),
        db: h.db,
        toolPackageRegistries: {
          httpRegistries: new Map([
            ["npmjs", { url: "https://registry.test" }],
          ]),
          defaultRegistry: "npmjs",
        },
      });
    });

    afterAll(async () => {
      if (env !== undefined) await env.teardown();
      if (h !== undefined) await h.close();
    });

    test("resolves and decrypts the binding when the cipher matches", async () => {
      const res = await deployBindingThroughRoute({
        assetId: "ast_credential_route_pos",
        appCipher: createTestCredentialCipher(),
      });
      if (res.status !== 201) {
        const body: unknown = await res.json();
        throw new Error(
          `expected 201 from the deploy route, got ${String(res.status)}: ${JSON.stringify(body)}\n${env.sidecarDiagnostics()}`,
        );
      }
      expect(res.status).toBe(201);
    });

    test("fails closed when the app cipher cannot decrypt the binding", async () => {
      // A real cipher on a DIFFERENT key: the AEAD decrypt refuses the key-id
      // mismatch, so resolution throws and the route maps it to 502.
      const wrongKeyCipher = createEnvKeyCredentialCipher(
        new Uint8Array(32).fill(9),
      );
      const res = await deployBindingThroughRoute({
        assetId: "ast_credential_route_neg",
        appCipher: wrongKeyCipher,
      });
      expect(res.status).toBe(502);
      const body: unknown = await res.json();
      // Assert the decrypt-origin message so a genuine sidecar outage (also 502)
      // cannot pass this test for the wrong reason.
      const message =
        typeof body === "object" && body !== null && "error" in body
          ? JSON.stringify(body)
          : String(body);
      expect(message).toMatch(/key id .* does not match/);
    });

    test("fails closed when the app has no cipher and the binding is encrypted", async () => {
      // A keyless app (no CREDENTIAL_ENCRYPTION_KEY) resolves the noop cipher.
      // The binding's credential is stored as real ciphertext, which the noop
      // refuses rather than passing through, so the deploy fails closed (502)
      // instead of delivering an un-decrypted secret.
      const res = await deployBindingThroughRoute({
        assetId: "ast_credential_route_keyless",
      });
      expect(res.status).toBe(502);
      const body: unknown = await res.json();
      // Assert the noop-rejection message so a genuine sidecar outage (also 502)
      // cannot pass this test for the wrong reason.
      const message =
        typeof body === "object" && body !== null && "error" in body
          ? JSON.stringify(body)
          : String(body);
      expect(message).toMatch(/refusing to pass an enc: ciphertext/);
    });
  },
);

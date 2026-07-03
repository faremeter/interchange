import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { type } from "arktype";

import { createInMemoryGrantStore } from "@intx/authz";
import { createSSHSignature, generateKeyPair } from "@intx/crypto";
import { createApp, type GetSession } from "@intx/hub-api";
import {
  createAssetService,
  createRepoStore,
  createSidecarEmitter,
  packageRegistryAuthorize,
  packageRegistryKindHandler,
  skillAuthorize,
  skillKindHandler,
  type AssetService,
  type AuthorizeFn,
  type EventCollectorRegistry,
  type RepoStore,
  type SessionService,
  type SidecarRouter,
} from "@intx/hub-sessions";
import type { GrantRule } from "@intx/types/authz";
import type { KeyPair } from "@intx/types/runtime";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedPrincipal, seedTenants } from "@intx/test-harness/seed";

// These route tests exercise the inherited-asset GET endpoints and the
// package-registry tarball endpoints against a real database. The asset
// resolver and the tenant/principal middleware issue real drizzle queries,
// which previously had to be served by a mock that walked drizzle's internal
// `queryChunks`; here they run against a migrated postgres schema.

const ROOT_TENANT_ID = "tnt_root";
const CHILD_TENANT_ID = "tnt_child";
const SIBLING_TENANT_ID = "tnt_sibling";
const ROOT_PRINCIPAL_ID = "prn_root";
const CHILD_PRINCIPAL_ID = "prn_child";
const ROOT_USER_ID = "usr_root";
const CHILD_USER_ID = "usr_child";

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

// ---------------------------------------------------------------------------
// Filesystem fixtures and signer
// ---------------------------------------------------------------------------

const tempDirs: string[] = [];

async function makeTempDir(prefix: string): Promise<string> {
  const d = await fs.promises.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(d);
  return d;
}

let signingKey: KeyPair;

// ---------------------------------------------------------------------------
// Substrate (real RepoStore, real signer) and in-process app mocks
// ---------------------------------------------------------------------------

async function createWiredSubstrate(): Promise<{
  dataDir: string;
  repoStore: RepoStore;
}> {
  const dataDir = await makeTempDir("asset-routes-");
  const signer = async (payload: string) =>
    createSSHSignature(payload, signingKey.privateKey, signingKey.publicKey);
  const authorize: AuthorizeFn = (principal, repoId, ref, action) => {
    if (repoId.kind === "skill") {
      return skillAuthorize(principal, repoId, ref, action);
    }
    if (repoId.kind === "package-registry") {
      return packageRegistryAuthorize(principal, repoId, ref, action);
    }
    return { allowed: false, reason: `no authorize for ${repoId.kind}` };
  };
  const repoStore = createRepoStore({
    dataDir,
    signingKey,
    handlers: {
      skill: skillKindHandler,
      "package-registry": packageRegistryKindHandler,
    },
    authorize,
    signingCallback: () => signer,
  });
  return { dataDir, repoStore };
}

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

function notImplemented(name: string) {
  return () => {
    throw new Error(`mock: ${name} not implemented`);
  };
}

function createMockSidecarRouter(): SidecarRouter {
  function notImpl(name: string): never {
    throw new Error(`mock: sidecarRouter.${name} not implemented`);
  }
  return {
    handleOpen: () => notImpl("handleOpen"),
    handleMessage: () => notImpl("handleMessage"),
    handleClose: () => notImpl("handleClose"),
    routeMail: () => notImpl("routeMail"),
    sendAgentDeploy: () => notImpl("sendAgentDeploy"),
    sendAgentUndeploy: () => notImpl("sendAgentUndeploy"),
    sendSessionStart: () => notImpl("sendSessionStart"),
    sendSessionAbort: () => notImpl("sendSessionAbort"),
    sendGrantsUpdate: () => notImpl("sendGrantsUpdate"),
    sendSourcesUpdate: () => notImpl("sendSourcesUpdate"),
    sendPack: () => notImpl("sendPack"),
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
    launchSession: notImplemented("sessionService.launchSession"),
    deployInstanceAtHead: notImplemented("sessionService.deployInstanceAtHead"),
    deployWorkflowDefinition: notImplemented(
      "sessionService.deployWorkflowDefinition",
    ),
    deploySingleStepAtHead: notImplemented(
      "sessionService.deploySingleStepAtHead",
    ),
    sendUserMessage: notImplemented("sessionService.sendUserMessage"),
    endSession: notImplemented("sessionService.endSession"),
  };
}

function createMockEventCollectors(): EventCollectorRegistry {
  return {
    create: notImplemented("eventCollectors.create"),
    dispatch: notImplemented("eventCollectors.dispatch"),
    abandon: notImplemented("eventCollectors.abandon"),
    has: () => false,
    getStatus: () => undefined,
    getAccumulatedText: () => undefined,
    getCurrentTurnId: () => undefined,
    getLastTurnId: () => undefined,
  };
}

// ---------------------------------------------------------------------------
// Multi-tenant harness against a real database
// ---------------------------------------------------------------------------

// One migrated schema is shared across every suite below. These hooks
// run at file scope so they cover all the suites, so they self-gate on
// the same env check the suites use: without database env the suites
// skip and the hooks must no-op rather than throw in beforeAll.
let h: TestDb;

beforeAll(async () => {
  if (!harnessDbEnvAvailable()) return;
  signingKey = await generateKeyPair();
  h = await createTestDb();
});

afterAll(async () => {
  if (!harnessDbEnvAvailable()) return;
  await h.close();
  for (const d of tempDirs.splice(0)) {
    await fs.promises.rm(d, { recursive: true, force: true }).catch(() => {
      /* best effort */
    });
  }
});

beforeEach(async () => {
  if (!harnessDbEnvAvailable()) return;
  await h.reset();
});

type AssetSpec = {
  id: string;
  tenantId: string;
  kind: string;
  name: string;
};

type MultiTenantHarness = {
  app: ReturnType<typeof createApp>;
  assetService: AssetService;
};

function readGrant(principalId: string): GrantRule {
  return {
    id: `grant-${principalId}-read`,
    resource: "asset:*",
    action: "read",
    effect: "allow",
    origin: "system",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId,
  };
}

async function setupMultiTenant(opts: {
  userId: string;
  grants?: GrantRule[];
  assets?: AssetSpec[];
}): Promise<MultiTenantHarness> {
  await seedTenants(h.db, [
    { id: ROOT_TENANT_ID },
    { id: CHILD_TENANT_ID, parentId: ROOT_TENANT_ID },
    { id: SIBLING_TENANT_ID, parentId: ROOT_TENANT_ID },
  ]);
  await seedPrincipal(h.db, {
    id: ROOT_PRINCIPAL_ID,
    tenantId: ROOT_TENANT_ID,
    refId: ROOT_USER_ID,
  });
  await seedPrincipal(h.db, {
    id: CHILD_PRINCIPAL_ID,
    tenantId: CHILD_TENANT_ID,
    refId: CHILD_USER_ID,
  });
  for (const a of opts.assets ?? []) {
    await seedAsset(h.db, a);
  }

  const { repoStore } = await createWiredSubstrate();
  const assetService = createAssetService({ db: h.db, repoStore });
  const grants = opts.grants ?? [
    readGrant(ROOT_PRINCIPAL_ID),
    readGrant(CHILD_PRINCIPAL_ID),
  ];
  const app = createApp({
    getSession: createMockGetSession(opts.userId),
    authHandler: () => new Response("", { status: 404 }),
    db: h.db,
    grantStore: createInMemoryGrantStore(grants),
    sidecarRouter: createMockSidecarRouter(),
    sessionService: createMockSessionService(),
    eventCollectors: createMockEventCollectors(),
    assetService,
    repoStore,
    maxTarballBytes: 10_000_000,
  });
  return { app, assetService };
}

const AssetWithOriginShape = type({
  id: "string",
  tenantId: "string",
  kind: "string",
  name: "string",
  displayName: "string | null",
  creatorPrincipalId: "string | null",
  createdAt: "string",
  updatedAt: "string",
  origin: {
    tenantId: "string",
    direct: "boolean",
  },
});
const AssetListShape = AssetWithOriginShape.array();

async function parseAssetList(res: Response) {
  const raw: unknown = await res.json();
  const parsed = AssetListShape(raw);
  if (parsed instanceof type.errors) {
    throw new Error(`asset list did not validate: ${parsed.summary}`);
  }
  return parsed;
}

const ErrorResponseShape = type({
  error: { code: "string", message: "string" },
});

async function parseErrorResponse(res: Response) {
  const raw: unknown = await res.json();
  const parsed = ErrorResponseShape(raw);
  if (parsed instanceof type.errors) {
    throw new Error(`error response did not validate: ${parsed.summary}`);
  }
  return parsed;
}

// ---------------------------------------------------------------------------
// GET /assets (inherited list)
// ---------------------------------------------------------------------------

describe.skipIf(!harnessDbEnvAvailable())(
  "GET /api/tenants/:tenantId/assets",
  () => {
    test("returns a root asset with origin.direct = false when listed from a child tenant", async () => {
      const h2 = await setupMultiTenant({
        userId: CHILD_USER_ID,
        assets: [
          {
            id: "ast_root_greet",
            tenantId: ROOT_TENANT_ID,
            kind: "skill",
            name: "greet",
          },
        ],
      });
      const res = await h2.app.request(
        `/api/tenants/${CHILD_TENANT_ID}/assets`,
        { method: "GET" },
      );
      expect(res.status).toBe(200);
      const list = await parseAssetList(res);
      expect(list).toHaveLength(1);
      expect(list[0]?.id).toBe("ast_root_greet");
      expect(list[0]?.origin).toEqual({
        tenantId: ROOT_TENANT_ID,
        direct: false,
      });
    });

    test("child asset shadows the inherited root asset of the same (kind, name)", async () => {
      const h2 = await setupMultiTenant({
        userId: CHILD_USER_ID,
        assets: [
          {
            id: "ast_root_greet",
            tenantId: ROOT_TENANT_ID,
            kind: "skill",
            name: "greet",
          },
          {
            id: "ast_child_greet",
            tenantId: CHILD_TENANT_ID,
            kind: "skill",
            name: "greet",
          },
        ],
      });
      const res = await h2.app.request(
        `/api/tenants/${CHILD_TENANT_ID}/assets`,
        { method: "GET" },
      );
      expect(res.status).toBe(200);
      const list = await parseAssetList(res);
      expect(list).toHaveLength(1);
      expect(list[0]?.id).toBe("ast_child_greet");
      expect(list[0]?.origin).toEqual({
        tenantId: CHILD_TENANT_ID,
        direct: true,
      });
    });

    test("filters by kind when ?kind= is supplied", async () => {
      const h2 = await setupMultiTenant({
        userId: ROOT_USER_ID,
        assets: [
          {
            id: "ast_skill",
            tenantId: ROOT_TENANT_ID,
            kind: "skill",
            name: "greet",
          },
          {
            id: "ast_pkg",
            tenantId: ROOT_TENANT_ID,
            kind: "package-registry",
            name: "main",
          },
        ],
      });
      const res = await h2.app.request(
        `/api/tenants/${ROOT_TENANT_ID}/assets?kind=package-registry`,
        { method: "GET" },
      );
      expect(res.status).toBe(200);
      const list = await parseAssetList(res);
      expect(list).toHaveLength(1);
      expect(list[0]?.id).toBe("ast_pkg");
    });

    test("inherited=false suppresses ancestor rows", async () => {
      const h2 = await setupMultiTenant({
        userId: CHILD_USER_ID,
        assets: [
          {
            id: "ast_root_greet",
            tenantId: ROOT_TENANT_ID,
            kind: "skill",
            name: "greet",
          },
          {
            id: "ast_child_search",
            tenantId: CHILD_TENANT_ID,
            kind: "skill",
            name: "search",
          },
        ],
      });
      const res = await h2.app.request(
        `/api/tenants/${CHILD_TENANT_ID}/assets?inherited=false`,
        { method: "GET" },
      );
      expect(res.status).toBe(200);
      const list = await parseAssetList(res);
      expect(list).toHaveLength(1);
      expect(list[0]?.id).toBe("ast_child_search");
    });

    test("rejects inherited values other than true or false with 400", async () => {
      const h2 = await setupMultiTenant({ userId: CHILD_USER_ID });
      const res = await h2.app.request(
        `/api/tenants/${CHILD_TENANT_ID}/assets?inherited=please`,
        { method: "GET" },
      );
      expect(res.status).toBe(400);
      const body = await parseErrorResponse(res);
      expect(body.error.code).toBe("bad_request");
    });
  },
);

// ---------------------------------------------------------------------------
// GET /assets/:assetId (chain-validated)
// ---------------------------------------------------------------------------

describe.skipIf(!harnessDbEnvAvailable())(
  "GET /api/tenants/:tenantId/assets/:assetId",
  () => {
    test("returns the asset when it belongs to an ancestor tenant", async () => {
      const h2 = await setupMultiTenant({
        userId: CHILD_USER_ID,
        assets: [
          {
            id: "ast_root_greet",
            tenantId: ROOT_TENANT_ID,
            kind: "skill",
            name: "greet",
          },
        ],
      });
      const res = await h2.app.request(
        `/api/tenants/${CHILD_TENANT_ID}/assets/ast_root_greet`,
        { method: "GET" },
      );
      expect(res.status).toBe(200);
      const body: unknown = await res.json();
      if (!isObject(body)) throw new Error("expected object body");
      expect(body["id"]).toBe("ast_root_greet");
      expect(body["tenantId"]).toBe(ROOT_TENANT_ID);
    });

    test("returns 404 when the asset belongs to a sibling tenant", async () => {
      const h2 = await setupMultiTenant({
        userId: CHILD_USER_ID,
        assets: [
          {
            id: "ast_sibling_secret",
            tenantId: SIBLING_TENANT_ID,
            kind: "package-registry",
            name: "private",
          },
        ],
      });
      const res = await h2.app.request(
        `/api/tenants/${CHILD_TENANT_ID}/assets/ast_sibling_secret`,
        { method: "GET" },
      );
      expect(res.status).toBe(404);
    });

    test("returns 404 when the asset does not exist", async () => {
      const h2 = await setupMultiTenant({ userId: ROOT_USER_ID });
      const res = await h2.app.request(
        `/api/tenants/${ROOT_TENANT_ID}/assets/ast_missing`,
        { method: "GET" },
      );
      expect(res.status).toBe(404);
    });
  },
);

// ---------------------------------------------------------------------------
// Tarball PUT / GET / DELETE
// ---------------------------------------------------------------------------

const PKR_GRANTS: GrantRule[] = ["create", "read", "write"].map(
  (action): GrantRule => ({
    id: `grant-root-${action}`,
    resource: "asset:*",
    action,
    effect: "allow",
    origin: "system",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: ROOT_PRINCIPAL_ID,
  }),
);

async function makeNpmTarball(pkg: {
  name: string;
  version: string;
}): Promise<Uint8Array> {
  const tar = await import("tar");
  const stagingRoot = await makeTempDir("pkr-routes-");
  const pkgDir = path.join(stagingRoot, "package");
  await fs.promises.mkdir(pkgDir, { recursive: true });
  await fs.promises.writeFile(
    path.join(pkgDir, "package.json"),
    JSON.stringify(pkg, null, 2),
    "utf-8",
  );
  await fs.promises.writeFile(
    path.join(pkgDir, "index.js"),
    "module.exports = {};\n",
  );
  const out = path.join(stagingRoot, "out.tgz");
  await tar.create(
    { cwd: stagingRoot, gzip: true, file: out, portable: true },
    ["package"],
  );
  return new Uint8Array(await fs.promises.readFile(out));
}

async function setupRegistry(): Promise<{
  h: MultiTenantHarness;
  assetId: string;
}> {
  const harness = await setupMultiTenant({
    userId: ROOT_USER_ID,
    grants: PKR_GRANTS,
  });
  const asset = await harness.assetService.createAsset({
    tenantId: ROOT_TENANT_ID,
    kind: "package-registry",
    name: "builtins",
  });
  return { h: harness, assetId: asset.id };
}

describe.skipIf(!harnessDbEnvAvailable())(
  "PUT /api/tenants/:tenantId/assets/:assetId/tarballs/:filename",
  () => {
    test("uploads a tarball and returns commit + integrity", async () => {
      const { h: harness, assetId } = await setupRegistry();
      const bytes = await makeNpmTarball({ name: "tool-a", version: "1.0.0" });
      const res = await harness.app.request(
        `/api/tenants/${ROOT_TENANT_ID}/assets/${assetId}/tarballs/tool-a-1.0.0.tgz`,
        {
          method: "PUT",
          headers: { "content-type": "application/octet-stream" },
          body: bytes,
        },
      );
      expect(res.status).toBe(200);
      const body: unknown = await res.json();
      if (!isObject(body)) throw new Error("expected object");
      expect(typeof body["commit"]).toBe("string");
      const integrity = body["integrity"];
      if (typeof integrity !== "string")
        throw new Error("integrity not string");
      expect(integrity.startsWith("sha512-")).toBe(true);
    });

    test("rejects an unsafe filename with 400", async () => {
      const { h: harness, assetId } = await setupRegistry();
      const res = await harness.app.request(
        `/api/tenants/${ROOT_TENANT_ID}/assets/${assetId}/tarballs/has spaces.tgz`,
        { method: "PUT", body: new Uint8Array() },
      );
      expect(res.status).toBe(400);
    });

    test("rejects a filename without .tgz with 400", async () => {
      const { h: harness, assetId } = await setupRegistry();
      const res = await harness.app.request(
        `/api/tenants/${ROOT_TENANT_ID}/assets/${assetId}/tarballs/tool-a-1.0.0.tar`,
        { method: "PUT", body: new Uint8Array() },
      );
      expect(res.status).toBe(400);
    });

    test("rejects with 404 when the asset is not a package-registry", async () => {
      const harness = await setupMultiTenant({
        userId: ROOT_USER_ID,
        grants: PKR_GRANTS,
      });
      const skillAsset = await harness.assetService.createAsset({
        tenantId: ROOT_TENANT_ID,
        kind: "skill",
        name: "greet",
      });
      const res = await harness.app.request(
        `/api/tenants/${ROOT_TENANT_ID}/assets/${skillAsset.id}/tarballs/tool-a-1.0.0.tgz`,
        {
          method: "PUT",
          body: await makeNpmTarball({ name: "tool-a", version: "1.0.0" }),
        },
      );
      expect(res.status).toBe(404);
    });

    test("rejects with 404 when the asset id does not exist", async () => {
      const harness = await setupMultiTenant({
        userId: ROOT_USER_ID,
        grants: PKR_GRANTS,
      });
      const res = await harness.app.request(
        `/api/tenants/${ROOT_TENANT_ID}/assets/ast_missing/tarballs/tool-a-1.0.0.tgz`,
        {
          method: "PUT",
          body: await makeNpmTarball({ name: "tool-a", version: "1.0.0" }),
        },
      );
      expect(res.status).toBe(404);
    });

    test("rejects with 400 when the tarball's package.json fails validation", async () => {
      const { h: harness, assetId } = await setupRegistry();
      const tar = await import("tar");
      const stagingRoot = await makeTempDir("pkr-bad-");
      const pkgDir = path.join(stagingRoot, "package");
      await fs.promises.mkdir(pkgDir, { recursive: true });
      await fs.promises.writeFile(
        path.join(pkgDir, "package.json"),
        JSON.stringify({ version: "0.0.1" }),
        "utf-8",
      );
      const out = path.join(stagingRoot, "out.tgz");
      await tar.create(
        { cwd: stagingRoot, gzip: true, file: out, portable: true },
        ["package"],
      );
      const bytes = new Uint8Array(await fs.promises.readFile(out));
      const res = await harness.app.request(
        `/api/tenants/${ROOT_TENANT_ID}/assets/${assetId}/tarballs/bad-0.0.1.tgz`,
        { method: "PUT", body: bytes },
      );
      expect(res.status).toBe(400);
    });
  },
);

describe.skipIf(!harnessDbEnvAvailable())(
  "GET /api/tenants/:tenantId/assets/:assetId/tarballs",
  () => {
    test("lists uploaded tarballs with size and integrity", async () => {
      const { h: harness, assetId } = await setupRegistry();
      const bytes = await makeNpmTarball({ name: "tool-a", version: "1.0.0" });
      const putRes = await harness.app.request(
        `/api/tenants/${ROOT_TENANT_ID}/assets/${assetId}/tarballs/tool-a-1.0.0.tgz`,
        { method: "PUT", body: bytes },
      );
      expect(putRes.status).toBe(200);

      const listRes = await harness.app.request(
        `/api/tenants/${ROOT_TENANT_ID}/assets/${assetId}/tarballs`,
        { method: "GET" },
      );
      expect(listRes.status).toBe(200);
      const list: unknown = await listRes.json();
      if (!Array.isArray(list)) throw new Error("expected array");
      expect(list).toHaveLength(1);
      const first = list[0];
      if (!isObject(first)) throw new Error("expected object");
      expect(first["filename"]).toBe("tool-a-1.0.0.tgz");
      expect(typeof first["size"]).toBe("number");
      expect(typeof first["integrity"]).toBe("string");
    });

    test("returns an empty list on a fresh registry", async () => {
      const { h: harness, assetId } = await setupRegistry();
      const res = await harness.app.request(
        `/api/tenants/${ROOT_TENANT_ID}/assets/${assetId}/tarballs`,
        { method: "GET" },
      );
      expect(res.status).toBe(200);
      const list: unknown = await res.json();
      expect(list).toEqual([]);
    });

    test("returns 404 when the asset is not a package-registry", async () => {
      const harness = await setupMultiTenant({
        userId: ROOT_USER_ID,
        grants: PKR_GRANTS,
      });
      const skillAsset = await harness.assetService.createAsset({
        tenantId: ROOT_TENANT_ID,
        kind: "skill",
        name: "greet",
      });
      const res = await harness.app.request(
        `/api/tenants/${ROOT_TENANT_ID}/assets/${skillAsset.id}/tarballs`,
        { method: "GET" },
      );
      expect(res.status).toBe(404);
    });
  },
);

describe.skipIf(!harnessDbEnvAvailable())(
  "DELETE /api/tenants/:tenantId/assets/:assetId/tarballs/:filename",
  () => {
    test("removes the tarball and commits", async () => {
      const { h: harness, assetId } = await setupRegistry();
      const bytes = await makeNpmTarball({ name: "tool-a", version: "1.0.0" });
      const putRes = await harness.app.request(
        `/api/tenants/${ROOT_TENANT_ID}/assets/${assetId}/tarballs/tool-a-1.0.0.tgz`,
        { method: "PUT", body: bytes },
      );
      expect(putRes.status).toBe(200);

      const delRes = await harness.app.request(
        `/api/tenants/${ROOT_TENANT_ID}/assets/${assetId}/tarballs/tool-a-1.0.0.tgz`,
        { method: "DELETE" },
      );
      expect(delRes.status).toBe(200);
      const body: unknown = await delRes.json();
      if (!isObject(body)) throw new Error("expected object");
      expect(typeof body["commit"]).toBe("string");

      const listRes = await harness.app.request(
        `/api/tenants/${ROOT_TENANT_ID}/assets/${assetId}/tarballs`,
        { method: "GET" },
      );
      const list: unknown = await listRes.json();
      expect(list).toEqual([]);
    });

    test("returns 404 when the filename is not present", async () => {
      const { h: harness, assetId } = await setupRegistry();
      const res = await harness.app.request(
        `/api/tenants/${ROOT_TENANT_ID}/assets/${assetId}/tarballs/missing-1.0.0.tgz`,
        { method: "DELETE" },
      );
      expect(res.status).toBe(404);
    });
  },
);

describe.skipIf(!harnessDbEnvAvailable())(
  "PUT tarballs: concurrent writes against the same asset",
  () => {
    test("two concurrent PUTs to different filenames both survive", async () => {
      // The route reads existing tarball entries and rewrites the prefix
      // inside the per-repo lock, so two concurrent PUTs to different
      // filenames serialize rather than racing on a shared pre-image;
      // both files must end up in the final tree.
      const { h: harness, assetId } = await setupRegistry();
      const a = await makeNpmTarball({ name: "tool-a", version: "1.0.0" });
      const b = await makeNpmTarball({ name: "tool-b", version: "1.0.0" });
      const [resA, resB] = await Promise.all([
        harness.app.request(
          `/api/tenants/${ROOT_TENANT_ID}/assets/${assetId}/tarballs/tool-a-1.0.0.tgz`,
          { method: "PUT", body: a },
        ),
        harness.app.request(
          `/api/tenants/${ROOT_TENANT_ID}/assets/${assetId}/tarballs/tool-b-1.0.0.tgz`,
          { method: "PUT", body: b },
        ),
      ]);
      expect(resA.status).toBe(200);
      expect(resB.status).toBe(200);

      const listRes = await harness.app.request(
        `/api/tenants/${ROOT_TENANT_ID}/assets/${assetId}/tarballs`,
        { method: "GET" },
      );
      expect(listRes.status).toBe(200);
      const list: unknown = await listRes.json();
      if (!Array.isArray(list)) throw new Error("expected array");
      const filenames = list
        .map((row: unknown) => (isObject(row) ? row["filename"] : null))
        .filter((n): n is string => typeof n === "string")
        .sort();
      expect(filenames).toEqual(["tool-a-1.0.0.tgz", "tool-b-1.0.0.tgz"]);
    });
  },
);

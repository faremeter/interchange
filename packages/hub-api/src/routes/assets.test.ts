import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";
import { type } from "arktype";

import { createInMemoryGrantStore } from "@intx/authz";
import {
  generateKeyPair,
  createSSHSignature,
  verifySSHSignature,
} from "@intx/crypto-node";
import type { KeyPair } from "@intx/types/runtime";
import type { GrantRule } from "@intx/types/authz";
import type { AssetRow as DBAssetRow, DB } from "@intx/db";
import {
  asset as assetTable,
  agentAsset as agentAssetTable,
} from "@intx/db/schema";
import {
  createAssetService,
  createRepoStore,
  createSidecarEmitter,
  packageRegistryAuthorize,
  packageRegistryKindHandler,
  skillKindHandler,
  skillAuthorize,
  workflowAuthorize,
  workflowKindHandler,
  WORKFLOW_JSON_PATH,
  type AssetService,
  type AuthorizeFn,
  type EventCollectorRegistry,
  type KindHandler,
  type Principal,
  type RepoId,
  type RepoStore,
  type SessionService,
  type SidecarRouter,
  type ValidatePushResult,
} from "@intx/hub-sessions";
import { collectReachableObjects } from "@intx/storage-isogit";

import { createApp } from "../app";
import type { GetSession } from "../session";
import { SANE_GITIGNORE } from "./assets";

// ---------------------------------------------------------------------------
// IDs and base fixtures
// ---------------------------------------------------------------------------

const TENANT_ID = "tnt_test";
const PRINCIPAL_ID = "prn_test";
const USER_ID = "usr_test";

const testTenant = {
  id: TENANT_ID,
  name: "Test",
  slug: "test",
  domain: "test.example.com",
  parentId: null,
  config: null,
  createdAt: new Date("2025-01-01"),
  updatedAt: new Date("2025-01-01"),
};

const testPrincipal = {
  id: PRINCIPAL_ID,
  tenantId: TENANT_ID,
  kind: "user" as const,
  refId: USER_ID,
  status: "active" as const,
  createdAt: new Date("2025-01-01"),
  updatedAt: new Date("2025-01-01"),
};

// ---------------------------------------------------------------------------
// Filesystem fixtures
// ---------------------------------------------------------------------------

const tempDirs: string[] = [];

async function makeTempDir(prefix: string): Promise<string> {
  const d = await fs.promises.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(d);
  return d;
}

let signingKey: KeyPair;

beforeAll(async () => {
  signingKey = await generateKeyPair();
});

afterAll(async () => {
  for (const d of tempDirs.splice(0)) {
    await fs.promises.rm(d, { recursive: true, force: true }).catch(() => {
      /* best effort */
    });
  }
});

// ---------------------------------------------------------------------------
// DB stub
// ---------------------------------------------------------------------------

type AssetRow = {
  id: string;
  tenantId: string;
  kind: "agent-state" | "skill" | "package-registry" | "workflow";
  name: string;
  displayName: string | null;
  creatorPrincipalId: string | null;
  createdAt: Date;
  updatedAt: Date;
};

type DBState = {
  assets: AssetRow[];
  /** When set, the next `findFirst(asset)` returns the row whose id
   * matches this value. The asset-routes smart-HTTP handler looks up
   * by `(tenantId, kind, name)`, but the stub only needs to honour
   * the route's narrow query: we pre-seed which row to return. */
  assetLookupHint: { tenantId: string; kind: string; name: string } | null;
};

function makeMockDB(state: DBState): DB["db"] {
  function findFirstAsset(): Promise<AssetRow | undefined> {
    const hint = state.assetLookupHint;
    if (hint === null) return Promise.resolve(undefined);
    const match = state.assets.find(
      (a) =>
        a.tenantId === hint.tenantId &&
        a.kind === hint.kind &&
        a.name === hint.name,
    );
    return Promise.resolve(match);
  }

  function insertChain(table: unknown) {
    return {
      values(rows: AssetRow | AssetRow[]) {
        const list = Array.isArray(rows) ? rows : [rows];
        if (table === assetTable) {
          const inserted: AssetRow[] = [];
          for (const r of list) {
            if (
              state.assets.some(
                (existing) =>
                  existing.tenantId === r.tenantId &&
                  existing.kind === r.kind &&
                  existing.name === r.name,
              )
            ) {
              const driverErr = new Error(
                `duplicate key value violates unique constraint`,
              ) as Error & { code?: string };
              driverErr.code = "23505";
              // Mirror Drizzle: the driver error is wrapped as `cause`.
              const err = new Error("Failed query", { cause: driverErr });
              return {
                returning: () => Promise.reject(err),
                then: (_resolve: (v: undefined) => unknown) =>
                  Promise.reject(err),
              };
            }
            state.assets.push(r);
            inserted.push(r);
          }
          return {
            returning: () => Promise.resolve(inserted),
            then: (resolve: (v: undefined) => unknown) => resolve(undefined),
          };
        }
        if (table === agentAssetTable) {
          // Unused in these tests; the asset-routes layer never
          // inserts agent_asset rows.
          return {
            returning: () => Promise.resolve(list),
            then: (resolve: (v: undefined) => unknown) => resolve(undefined),
          };
        }
        return {
          returning: () => Promise.resolve(list),
          then: (resolve: (v: undefined) => unknown) => resolve(undefined),
        };
      },
    };
  }

  const mock = {
    query: {
      tenant: {
        findFirst: async () => testTenant,
        findMany: async () => [testTenant],
      },
      principal: {
        findFirst: async () => testPrincipal,
        findMany: async () => [testPrincipal],
      },
      asset: {
        findFirst: () => findFirstAsset(),
        findMany: async () => state.assets,
      },
      gitToken: {
        findFirst: async () => undefined,
        findMany: async () => [],
      },
    },
    transaction: async (fn: (tx: unknown) => Promise<unknown>) =>
      fn({ insert: insertChain }),
    insert: insertChain,
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase type cannot be structurally satisfied in tests
  return mock as unknown as DB["db"];
}

// ---------------------------------------------------------------------------
// Substrate (real RepoStore, real signer) and AssetService
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
    if (repoId.kind === "workflow") {
      return workflowAuthorize(principal, repoId, ref, action);
    }
    return { allowed: false, reason: `no authorize for ${repoId.kind}` };
  };
  const repoStore = createRepoStore({
    dataDir,
    signingKey,
    handlers: {
      skill: skillKindHandler,
      "package-registry": packageRegistryKindHandler,
      workflow: workflowKindHandler,
    },
    authorize,
    signingCallback: () => signer,
  });
  return { dataDir, repoStore };
}

// ---------------------------------------------------------------------------
// Other mocks
// ---------------------------------------------------------------------------

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
    launchSession: () => {
      throw new Error("mock: sessionService.launchSession not implemented");
    },
    deployWorkflowDefinition: () => {
      throw new Error(
        "mock: sessionService.deployWorkflowDefinition not implemented",
      );
    },
    sendUserMessage: () => {
      throw new Error("mock: sessionService.sendUserMessage not implemented");
    },
    endSession: () => {
      throw new Error("mock: sessionService.endSession not implemented");
    },
  };
}

function notImplemented(name: string) {
  return () => {
    throw new Error(`mock: ${name} not implemented`);
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
// App harness
// ---------------------------------------------------------------------------

function makeCreateGrant(overrides: Partial<GrantRule> = {}): GrantRule {
  return {
    id: "grant-create",
    resource: "asset:*",
    action: "create",
    effect: "allow",
    origin: "system",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: PRINCIPAL_ID,
    ...overrides,
  };
}

type Harness = {
  app: ReturnType<typeof createApp>;
  state: DBState;
  repoStore: RepoStore;
  assetService: AssetService;
  dataDir: string;
};

async function setup(
  grants: GrantRule[] = [makeCreateGrant()],
): Promise<Harness> {
  const state: DBState = { assets: [], assetLookupHint: null };
  const db = makeMockDB(state);
  const { dataDir, repoStore } = await createWiredSubstrate();
  const assetService = createAssetService({ db, repoStore });
  const app = createApp({
    getSession: createMockGetSession(USER_ID),
    authHandler: () => new Response("", { status: 404 }),
    db,
    grantStore: createInMemoryGrantStore(grants),
    sidecarRouter: createMockSidecarRouter(),
    sessionService: createMockSessionService(),
    eventCollectors: createMockEventCollectors(),
    assetService,
    repoStore,
    maxTarballBytes: 10_000_000,
  });
  return { app, state, repoStore, assetService, dataDir };
}

const createURL = `/api/tenants/${TENANT_ID}/assets`;

const AssetResponseShape = type({
  id: "string",
  tenantId: "string",
  kind: "string",
  name: "string",
  "displayName?": "string | null",
  "creatorPrincipalId?": "string | null",
  "createdAt?": "string",
  "updatedAt?": "string",
});

const ErrorResponseShape = type({
  error: {
    code: "string",
    message: "string",
  },
});

async function parseAssetResponse(res: Response) {
  const raw: unknown = await res.json();
  const parsed = AssetResponseShape(raw);
  if (parsed instanceof type.errors) {
    throw new Error(`asset response did not validate: ${parsed.summary}`);
  }
  return parsed;
}

async function parseErrorResponse(res: Response) {
  const raw: unknown = await res.json();
  const parsed = ErrorResponseShape(raw);
  if (parsed instanceof type.errors) {
    throw new Error(`error response did not validate: ${parsed.summary}`);
  }
  return parsed;
}

// ---------------------------------------------------------------------------
// REST POST /assets
// ---------------------------------------------------------------------------

describe("POST /api/tenants/:tenantId/assets", () => {
  test("creates a skill asset and returns the row", async () => {
    const h = await setup();
    const res = await h.app.request(createURL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        kind: "skill",
        name: "greet",
        displayName: "Greeting skill",
      }),
    });
    expect(res.status).toBe(201);
    const body = await parseAssetResponse(res);
    expect(body.kind).toBe("skill");
    expect(body.name).toBe("greet");
    expect(body.tenantId).toBe(TENANT_ID);
    expect(body.id.startsWith("ast_")).toBe(true);
    expect(h.state.assets).toHaveLength(1);
  });

  test("rejects an unknown kind with 400", async () => {
    const h = await setup();
    const res = await h.app.request(createURL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ kind: "nonsense", name: "greet" }),
    });
    expect(res.status).toBe(400);
    expect(h.state.assets).toHaveLength(0);
  });

  test("rejects a malformed name with 400 invalid_name", async () => {
    const h = await setup();
    const res = await h.app.request(createURL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ kind: "skill", name: "Bad Name!" }),
    });
    expect(res.status).toBe(400);
    const body = await parseErrorResponse(res);
    expect(body.error.code).toBe("invalid_name");
    expect(h.state.assets).toHaveLength(0);
  });

  test("rejects creation when the principal has no asset:* create grant", async () => {
    const h = await setup([]);
    const res = await h.app.request(createURL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ kind: "skill", name: "greet" }),
    });
    expect(res.status).toBe(403);
    expect(h.state.assets).toHaveLength(0);
  });

  test("the genesis commit ships the asset-route gitignore body", async () => {
    const h = await setup();
    const res = await h.app.request(createURL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ kind: "skill", name: "greet" }),
    });
    expect(res.status).toBe(201);
    const body = await parseAssetResponse(res);
    const dir = h.repoStore.getRepoDir({ kind: "skill", id: body.id });
    const onDisk = await fs.promises.readFile(
      path.join(dir, ".gitignore"),
      "utf-8",
    );
    expect(onDisk).toBe(SANE_GITIGNORE);
    expect(onDisk).toContain(".DS_Store");
    expect(onDisk).toContain("keys/");
  });

  test("the genesis commit is signed and verifies against the hub public key", async () => {
    const h = await setup();
    const res = await h.app.request(createURL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ kind: "skill", name: "greet" }),
    });
    expect(res.status).toBe(201);
    const body = await parseAssetResponse(res);
    const dir = h.repoStore.getRepoDir({ kind: "skill", id: body.id });

    const [entry] = await git.log({ fs, dir, depth: 1 });
    if (entry === undefined) throw new Error("no commit in log");
    expect(entry.commit.author.name).toBe("interchange-hub");
    const signature = entry.commit.gpgsig;
    if (signature === undefined) throw new Error("commit was not signed");

    const { object } = await git.readObject({
      fs,
      dir,
      oid: entry.oid,
      format: "content",
    });
    if (!(object instanceof Uint8Array)) {
      throw new Error("expected raw commit content as Uint8Array");
    }
    const content = new TextDecoder().decode(object);
    const gpgsigIdx = content.indexOf("\ngpgsig ");
    let endIdx = gpgsigIdx + 1;
    while (endIdx < content.length) {
      const nlIdx = content.indexOf("\n", endIdx);
      if (nlIdx === -1) break;
      endIdx = nlIdx + 1;
      if (endIdx < content.length && content[endIdx] !== " ") break;
    }
    const payload =
      content.substring(0, gpgsigIdx) + "\n" + content.substring(endIdx);
    expect(
      await verifySSHSignature(payload, signature, signingKey.publicKey),
    ).toBe(true);
  });

  test("HEAD points at refs/heads/main after init", async () => {
    const h = await setup();
    const res = await h.app.request(createURL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ kind: "skill", name: "greet" }),
    });
    expect(res.status).toBe(201);
    const body = await parseAssetResponse(res);
    const dir = h.repoStore.getRepoDir({ kind: "skill", id: body.id });
    const head = await fs.promises.readFile(
      path.join(dir, ".git", "HEAD"),
      "utf-8",
    );
    expect(head.trim()).toBe("ref: refs/heads/main");
  });
});

// ---------------------------------------------------------------------------
// Smart-HTTP routes -- bearer enforcement
// ---------------------------------------------------------------------------

describe("smart-HTTP asset routes", () => {
  test("info/refs without bearer credentials responds 401 with WWW-Authenticate", async () => {
    const h = await setup();
    const url =
      `/api/tenants/${TENANT_ID}/assets/skill/missing.git/info/refs?` +
      `service=git-upload-pack`;
    const res = await h.app.request(url, { method: "GET" });
    expect(res.status).toBe(401);
    const challenge = res.headers.get("WWW-Authenticate");
    expect(challenge).not.toBeNull();
    expect(challenge ?? "").toMatch(/Basic/);
  });

  test("git-upload-pack without bearer credentials responds 401", async () => {
    const h = await setup();
    const url = `/api/tenants/${TENANT_ID}/assets/skill/missing.git/git-upload-pack`;
    const res = await h.app.request(url, { method: "POST" });
    expect(res.status).toBe(401);
  });

  test("git-receive-pack without bearer credentials responds 401", async () => {
    const h = await setup();
    const url = `/api/tenants/${TENANT_ID}/assets/skill/missing.git/git-receive-pack`;
    const res = await h.app.request(url, { method: "POST" });
    expect(res.status).toBe(401);
  });
});

// ---------------------------------------------------------------------------
// Workflow-kind assets: REST create + smart-HTTP push validation
// ---------------------------------------------------------------------------

const HUB_PRINCIPAL: Principal = { kind: "hub" };

function validWorkflowJSON(): string {
  return JSON.stringify({
    id: "my-workflow",
    triggers: [{ type: "manual" }],
    steps: { first: { kind: "step", id: "first" } },
    stepOrder: ["first"],
  });
}

// Permissive source handler: the workflow allowlist is enforced by the
// *target* repo's receivePack (the path the smart-HTTP route drives), so
// the pack must be staged in a source that does not pre-reject the tree
// at write time. This mirrors the source/target split used by the
// workflow-kind substrate tests.
const permissiveWorkflowHandler: KindHandler = {
  kind: "workflow",
  directoryPrefix: "assets/workflow",
  validatePush(): ValidatePushResult {
    return { ok: true };
  },
  onRefUpdated() {
    /* no-op */
  },
};

/**
 * Build a packfile that introduces `files` as a single commit on `ref`
 * of `repoId`, using a permissive throwaway substrate as the source.
 * Returns the bytes plus the tip SHA so the caller can drive
 * `receivePack` against the strict asset repo the way the smart-HTTP
 * `git-receive-pack` route does.
 */
async function buildWorkflowPack(
  repoId: RepoId,
  ref: string,
  files: Record<string, string>,
): Promise<{ pack: Uint8Array; commitSha: string }> {
  const sourceDataDir = await makeTempDir("workflow-pack-src-");
  const signer = async (payload: string) =>
    createSSHSignature(payload, signingKey.privateKey, signingKey.publicKey);
  const source = createRepoStore({
    dataDir: sourceDataDir,
    signingKey,
    handlers: { workflow: permissiveWorkflowHandler },
    authorize: () => ({ allowed: true }),
    signingCallback: () => signer,
  });
  await source.initRepo(repoId);
  const { commitSha } = await source.writeTree(HUB_PRINCIPAL, repoId, ref, {
    files,
    message: "workflow push",
  });
  const sourceDir = source.getRepoDir(repoId);
  const oids = await collectReachableObjects(sourceDir, commitSha);
  const packResult = await git.packObjects({
    fs,
    dir: sourceDir,
    oids,
    write: false,
  });
  if (packResult.packfile === undefined) {
    throw new Error("git.packObjects returned no packfile");
  }
  return { pack: packResult.packfile, commitSha };
}

describe("workflow-kind asset routes", () => {
  test("POST creates a workflow asset and returns the row", async () => {
    const h = await setup();
    const res = await h.app.request(createURL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        kind: "workflow",
        name: "nightly-report",
        displayName: "Nightly report",
      }),
    });
    expect(res.status).toBe(201);
    const body = await parseAssetResponse(res);
    expect(body.kind).toBe("workflow");
    expect(body.name).toBe("nightly-report");
    expect(body.tenantId).toBe(TENANT_ID);
    expect(body.id.startsWith("ast_")).toBe(true);
    expect(h.state.assets).toHaveLength(1);
    expect(h.state.assets[0]?.kind).toBe("workflow");
  });

  test("a workflow.json pushed to the created repo is accepted by workflowKindHandler", async () => {
    const h = await setup();
    const createRes = await h.app.request(createURL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ kind: "workflow", name: "nightly-report" }),
    });
    expect(createRes.status).toBe(201);
    const created = await parseAssetResponse(createRes);

    // The smart-HTTP git-receive-pack route resolves the URL to this
    // RepoId and hands the pack to repoStore.receivePack, which runs
    // workflowKindHandler.validatePush on every new commit. Drive that
    // substrate call directly against the REST-created repo. The pack is
    // a parentless commit, so push it to a fresh ref (createAsset's
    // genesis owns refs/heads/main); the workflow handler does not gate
    // on ref name.
    const repoId: RepoId = { kind: "workflow", id: created.id };
    const ref = "refs/heads/deploy";
    const { pack, commitSha } = await buildWorkflowPack(repoId, ref, {
      [WORKFLOW_JSON_PATH]: validWorkflowJSON(),
    });

    await h.repoStore.receivePack(
      HUB_PRINCIPAL,
      repoId,
      ref,
      pack,
      commitSha,
      null,
    );

    expect(await h.repoStore.resolveRef(HUB_PRINCIPAL, repoId, ref)).toBe(
      commitSha,
    );
  });

  test("a push carrying a disallowed top-level path is rejected by workflowKindHandler", async () => {
    const h = await setup();
    const createRes = await h.app.request(createURL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ kind: "workflow", name: "nightly-report" }),
    });
    expect(createRes.status).toBe(201);
    const created = await parseAssetResponse(createRes);

    const repoId: RepoId = { kind: "workflow", id: created.id };
    const ref = "refs/heads/deploy";
    const { pack, commitSha } = await buildWorkflowPack(repoId, ref, {
      [WORKFLOW_JSON_PATH]: validWorkflowJSON(),
      "stray-file.txt": "not in the workflow allowlist",
    });

    await expect(
      h.repoStore.receivePack(
        HUB_PRINCIPAL,
        repoId,
        ref,
        pack,
        commitSha,
        null,
      ),
    ).rejects.toThrow(
      /path_violation:.*unexpected top-level entry "stray-file\.txt"/,
    );
  });
});

// ---------------------------------------------------------------------------
// Multi-tenant fixtures for the inherited GET endpoints
// ---------------------------------------------------------------------------

const ROOT_TENANT_ID = "tnt_root";
const CHILD_TENANT_ID = "tnt_child";
const SIBLING_TENANT_ID = "tnt_sibling";
const ROOT_PRINCIPAL_ID = "prn_root";
const CHILD_PRINCIPAL_ID = "prn_child";
const ROOT_USER_ID = "usr_root";
const CHILD_USER_ID = "usr_child";

type TenantStub = {
  id: string;
  name: string;
  slug: string;
  domain: string;
  parentId: string | null;
  config: null;
  createdAt: Date;
  updatedAt: Date;
};

type PrincipalStub = {
  id: string;
  tenantId: string;
  kind: "user";
  refId: string;
  status: "active";
  createdAt: Date;
  updatedAt: Date;
};

const SQL_TO_JS_COLUMN: Record<string, string> = {
  id: "id",
  tenant_id: "tenantId",
  parent_id: "parentId",
  kind: "kind",
  name: "name",
  ref_id: "refId",
};

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

function getString(v: unknown, key: string): string | undefined {
  if (!isObject(v)) return undefined;
  const candidate = v[key];
  return typeof candidate === "string" ? candidate : undefined;
}

function getArray(v: unknown, key: string): unknown[] | undefined {
  if (!isObject(v)) return undefined;
  const candidate = v[key];
  return Array.isArray(candidate) ? candidate : undefined;
}

/**
 * Walks a drizzle `where` expression and extracts the `(column = value)`
 * bindings it imposes. Mirrors the helper in
 * `packages/db/src/asset-resolution.test.ts`; covers the queries the
 * tenant/principal middleware and asset walker issue.
 */
function extractEqualities(
  predicate: unknown,
  into: Record<string, unknown>,
): void {
  const chunks = getArray(predicate, "queryChunks");
  if (chunks === undefined) return;
  for (let i = 0; i < chunks.length; i++) {
    const c = chunks[i];
    const colName = getString(c, "name");
    if (colName !== undefined) {
      const sepValue = getArray(chunks[i + 1], "value");
      if (sepValue !== undefined && sepValue[0] === " = ") {
        const valChunk = chunks[i + 2];
        if (isObject(valChunk) && "value" in valChunk) {
          const jsName = SQL_TO_JS_COLUMN[colName];
          if (jsName === undefined) {
            throw new Error(`unmapped SQL column in test mock: ${colName}`);
          }
          into[jsName] = valChunk["value"];
        }
      }
    } else if (getArray(c, "queryChunks") !== undefined) {
      extractEqualities(c, into);
    }
  }
}

function rowMatches(row: object, filter: Record<string, unknown>): boolean {
  const map = new Map<string, unknown>(Object.entries(row));
  for (const [k, v] of Object.entries(filter)) {
    if (map.get(k) !== v) return false;
  }
  return true;
}

type MultiTenantState = {
  tenants: TenantStub[];
  principals: PrincipalStub[];
  assets: DBAssetRow[];
};

function makeMultiTenantMockDB(state: MultiTenantState): DB["db"] {
  function findByPredicate<T extends object>(
    rows: T[],
    where: unknown,
  ): T | undefined {
    const filter: Record<string, unknown> = {};
    extractEqualities(where, filter);
    return rows.find((r) => rowMatches(r, filter));
  }
  function filterByPredicate<T extends object>(rows: T[], where: unknown): T[] {
    const filter: Record<string, unknown> = {};
    extractEqualities(where, filter);
    return rows.filter((r) => rowMatches(r, filter));
  }
  function insertAsset(row: DBAssetRow): Promise<DBAssetRow[]> {
    const duplicate = state.assets.some(
      (existing) =>
        existing.tenantId === row.tenantId &&
        existing.kind === row.kind &&
        existing.name === row.name,
    );
    if (duplicate) {
      const driverErr = new Error(
        `duplicate key value violates unique constraint`,
      ) as Error & { code?: string };
      driverErr.code = "23505";
      // Mirror Drizzle: the driver error is wrapped as `cause`.
      const err = new Error("Failed query", { cause: driverErr });
      return Promise.reject(err);
    }
    state.assets.push(row);
    return Promise.resolve([row]);
  }
  const mock = {
    query: {
      tenant: {
        findFirst: (opts: { where?: unknown }) =>
          Promise.resolve(findByPredicate(state.tenants, opts.where)),
        findMany: (opts: { where?: unknown }) =>
          Promise.resolve(filterByPredicate(state.tenants, opts.where)),
      },
      principal: {
        findFirst: (opts: { where?: unknown }) =>
          Promise.resolve(findByPredicate(state.principals, opts.where)),
        findMany: (opts: { where?: unknown }) =>
          Promise.resolve(filterByPredicate(state.principals, opts.where)),
      },
      asset: {
        findFirst: (opts: { where?: unknown }) =>
          Promise.resolve(findByPredicate(state.assets, opts.where)),
        findMany: (opts: { where?: unknown }) =>
          Promise.resolve(filterByPredicate(state.assets, opts.where)),
      },
      gitToken: {
        findFirst: async () => undefined,
        findMany: async () => [],
      },
    },
    insert(table: unknown) {
      if (table !== assetTable) {
        throw new Error("multi-tenant mock only handles asset inserts");
      }
      return {
        values(row: DBAssetRow) {
          return {
            returning: () => insertAsset(row),
          };
        },
      };
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase type cannot be structurally satisfied in tests
  return mock as unknown as DB["db"];
}

function makeTenantStub(
  overrides: { id: string; slug: string; parentId: string | null } & Partial<
    Omit<TenantStub, "id" | "slug" | "parentId">
  >,
): TenantStub {
  return {
    name: overrides.slug,
    domain: `${overrides.slug}.example.com`,
    config: null,
    createdAt: new Date("2025-01-01"),
    updatedAt: new Date("2025-01-01"),
    ...overrides,
  };
}

function makePrincipalStub(opts: {
  id: string;
  tenantId: string;
  refId: string;
}): PrincipalStub {
  return {
    kind: "user",
    status: "active",
    createdAt: new Date("2025-01-01"),
    updatedAt: new Date("2025-01-01"),
    ...opts,
  };
}

function makeAssetRow(opts: {
  id: string;
  tenantId: string;
  kind: string;
  name: string;
  displayName?: string;
}): DBAssetRow {
  return {
    id: opts.id,
    tenantId: opts.tenantId,
    kind: opts.kind,
    name: opts.name,
    displayName: opts.displayName ?? null,
    creatorPrincipalId: null,
    createdAt: new Date("2025-01-01"),
    updatedAt: new Date("2025-01-01"),
  };
}

type MultiTenantHarness = {
  app: ReturnType<typeof createApp>;
  state: MultiTenantState;
  assetService: AssetService;
};

async function setupMultiTenant(opts: {
  userId: string;
  grants?: GrantRule[];
  extraAssets?: DBAssetRow[];
}): Promise<MultiTenantHarness> {
  const state: MultiTenantState = {
    tenants: [
      makeTenantStub({ id: ROOT_TENANT_ID, slug: "root", parentId: null }),
      makeTenantStub({
        id: CHILD_TENANT_ID,
        slug: "child",
        parentId: ROOT_TENANT_ID,
      }),
      makeTenantStub({
        id: SIBLING_TENANT_ID,
        slug: "sibling",
        parentId: ROOT_TENANT_ID,
      }),
    ],
    principals: [
      makePrincipalStub({
        id: ROOT_PRINCIPAL_ID,
        tenantId: ROOT_TENANT_ID,
        refId: ROOT_USER_ID,
      }),
      makePrincipalStub({
        id: CHILD_PRINCIPAL_ID,
        tenantId: CHILD_TENANT_ID,
        refId: CHILD_USER_ID,
      }),
    ],
    assets: opts.extraAssets ?? [],
  };
  const db = makeMultiTenantMockDB(state);
  const { dataDir: _dataDir, repoStore } = await createWiredSubstrate();
  const assetService = createAssetService({ db, repoStore });
  const defaultGrants: GrantRule[] = [
    {
      id: "grant-root-read",
      resource: "asset:*",
      action: "read",
      effect: "allow",
      origin: "system",
      conditions: null,
      expiresAt: null,
      roleId: null,
      principalId: ROOT_PRINCIPAL_ID,
    },
    {
      id: "grant-child-read",
      resource: "asset:*",
      action: "read",
      effect: "allow",
      origin: "system",
      conditions: null,
      expiresAt: null,
      roleId: null,
      principalId: CHILD_PRINCIPAL_ID,
    },
  ];
  const app = createApp({
    getSession: createMockGetSession(opts.userId),
    authHandler: () => new Response("", { status: 404 }),
    db,
    grantStore: createInMemoryGrantStore(opts.grants ?? defaultGrants),
    sidecarRouter: createMockSidecarRouter(),
    sessionService: createMockSessionService(),
    eventCollectors: createMockEventCollectors(),
    assetService,
    repoStore,
    maxTarballBytes: 10_000_000,
  });
  return { app, state, assetService };
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

// ---------------------------------------------------------------------------
// GET /assets (inherited list)
// ---------------------------------------------------------------------------

describe("GET /api/tenants/:tenantId/assets", () => {
  test("returns a root asset with origin.direct = false when listed from a child tenant", async () => {
    const h = await setupMultiTenant({
      userId: CHILD_USER_ID,
      extraAssets: [
        makeAssetRow({
          id: "ast_root_greet",
          tenantId: ROOT_TENANT_ID,
          kind: "skill",
          name: "greet",
        }),
      ],
    });
    const res = await h.app.request(`/api/tenants/${CHILD_TENANT_ID}/assets`, {
      method: "GET",
    });
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
    const h = await setupMultiTenant({
      userId: CHILD_USER_ID,
      extraAssets: [
        makeAssetRow({
          id: "ast_root_greet",
          tenantId: ROOT_TENANT_ID,
          kind: "skill",
          name: "greet",
        }),
        makeAssetRow({
          id: "ast_child_greet",
          tenantId: CHILD_TENANT_ID,
          kind: "skill",
          name: "greet",
        }),
      ],
    });
    const res = await h.app.request(`/api/tenants/${CHILD_TENANT_ID}/assets`, {
      method: "GET",
    });
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
    const h = await setupMultiTenant({
      userId: ROOT_USER_ID,
      extraAssets: [
        makeAssetRow({
          id: "ast_skill",
          tenantId: ROOT_TENANT_ID,
          kind: "skill",
          name: "greet",
        }),
        makeAssetRow({
          id: "ast_pkg",
          tenantId: ROOT_TENANT_ID,
          kind: "package-registry",
          name: "main",
        }),
      ],
    });
    const res = await h.app.request(
      `/api/tenants/${ROOT_TENANT_ID}/assets?kind=package-registry`,
      { method: "GET" },
    );
    expect(res.status).toBe(200);
    const list = await parseAssetList(res);
    expect(list).toHaveLength(1);
    expect(list[0]?.id).toBe("ast_pkg");
  });

  test("inherited=false suppresses ancestor rows", async () => {
    const h = await setupMultiTenant({
      userId: CHILD_USER_ID,
      extraAssets: [
        makeAssetRow({
          id: "ast_root_greet",
          tenantId: ROOT_TENANT_ID,
          kind: "skill",
          name: "greet",
        }),
        makeAssetRow({
          id: "ast_child_search",
          tenantId: CHILD_TENANT_ID,
          kind: "skill",
          name: "search",
        }),
      ],
    });
    const res = await h.app.request(
      `/api/tenants/${CHILD_TENANT_ID}/assets?inherited=false`,
      { method: "GET" },
    );
    expect(res.status).toBe(200);
    const list = await parseAssetList(res);
    expect(list).toHaveLength(1);
    expect(list[0]?.id).toBe("ast_child_search");
  });

  test("rejects inherited values other than true or false with 400", async () => {
    const h = await setupMultiTenant({ userId: CHILD_USER_ID });
    const res = await h.app.request(
      `/api/tenants/${CHILD_TENANT_ID}/assets?inherited=please`,
      { method: "GET" },
    );
    expect(res.status).toBe(400);
    const body = await parseErrorResponse(res);
    expect(body.error.code).toBe("bad_request");
  });
});

// ---------------------------------------------------------------------------
// GET /assets/:assetId (chain-validated)
// ---------------------------------------------------------------------------

describe("GET /api/tenants/:tenantId/assets/:assetId", () => {
  test("returns the asset when it belongs to an ancestor tenant", async () => {
    const h = await setupMultiTenant({
      userId: CHILD_USER_ID,
      extraAssets: [
        makeAssetRow({
          id: "ast_root_greet",
          tenantId: ROOT_TENANT_ID,
          kind: "skill",
          name: "greet",
        }),
      ],
    });
    const res = await h.app.request(
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
    const h = await setupMultiTenant({
      userId: CHILD_USER_ID,
      extraAssets: [
        makeAssetRow({
          id: "ast_sibling_secret",
          tenantId: SIBLING_TENANT_ID,
          kind: "package-registry",
          name: "private",
        }),
      ],
    });
    const res = await h.app.request(
      `/api/tenants/${CHILD_TENANT_ID}/assets/ast_sibling_secret`,
      { method: "GET" },
    );
    expect(res.status).toBe(404);
  });

  test("returns 404 when the asset does not exist", async () => {
    const h = await setupMultiTenant({ userId: ROOT_USER_ID });
    const res = await h.app.request(
      `/api/tenants/${ROOT_TENANT_ID}/assets/ast_missing`,
      { method: "GET" },
    );
    expect(res.status).toBe(404);
  });
});

// ---------------------------------------------------------------------------
// Tarball PUT / GET / DELETE
// ---------------------------------------------------------------------------

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
    {
      cwd: stagingRoot,
      gzip: true,
      file: out,
      portable: true,
    },
    ["package"],
  );
  return new Uint8Array(await fs.promises.readFile(out));
}

const PKR_GRANTS: GrantRule[] = [
  {
    id: "grant-root-create",
    resource: "asset:*",
    action: "create",
    effect: "allow",
    origin: "system",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: ROOT_PRINCIPAL_ID,
  },
  {
    id: "grant-root-read",
    resource: "asset:*",
    action: "read",
    effect: "allow",
    origin: "system",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: ROOT_PRINCIPAL_ID,
  },
  {
    id: "grant-root-write",
    resource: "asset:*",
    action: "write",
    effect: "allow",
    origin: "system",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: ROOT_PRINCIPAL_ID,
  },
];

async function setupRegistry(): Promise<{
  h: MultiTenantHarness;
  assetId: string;
}> {
  const h = await setupMultiTenant({
    userId: ROOT_USER_ID,
    grants: PKR_GRANTS,
  });
  const asset = await h.assetService.createAsset({
    tenantId: ROOT_TENANT_ID,
    kind: "package-registry",
    name: "builtins",
    initOpts: { gitignore: SANE_GITIGNORE },
  });
  return { h, assetId: asset.id };
}

describe("PUT /api/tenants/:tenantId/assets/:assetId/tarballs/:filename", () => {
  test("uploads a tarball and returns commit + integrity", async () => {
    const { h, assetId } = await setupRegistry();
    const bytes = await makeNpmTarball({
      name: "tool-a",
      version: "1.0.0",
    });
    const res = await h.app.request(
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
    expect(typeof body["integrity"]).toBe("string");
    const integrity = body["integrity"];
    if (typeof integrity !== "string") throw new Error("integrity not string");
    expect(integrity.startsWith("sha512-")).toBe(true);
  });

  test("rejects an unsafe filename with 400", async () => {
    const { h, assetId } = await setupRegistry();
    const res = await h.app.request(
      `/api/tenants/${ROOT_TENANT_ID}/assets/${assetId}/tarballs/has spaces.tgz`,
      { method: "PUT", body: new Uint8Array() },
    );
    // Hono encodes spaces; the route layer rejects on the
    // SAFE_PATH_SEGMENT-style pattern check.
    expect(res.status).toBe(400);
  });

  test("rejects a filename without .tgz with 400", async () => {
    const { h, assetId } = await setupRegistry();
    const res = await h.app.request(
      `/api/tenants/${ROOT_TENANT_ID}/assets/${assetId}/tarballs/tool-a-1.0.0.tar`,
      { method: "PUT", body: new Uint8Array() },
    );
    expect(res.status).toBe(400);
  });

  test("rejects with 404 when the asset is not a package-registry", async () => {
    const h = await setupMultiTenant({
      userId: ROOT_USER_ID,
      grants: PKR_GRANTS,
    });
    const skillAsset = await h.assetService.createAsset({
      tenantId: ROOT_TENANT_ID,
      kind: "skill",
      name: "greet",
      initOpts: { gitignore: SANE_GITIGNORE },
    });
    const res = await h.app.request(
      `/api/tenants/${ROOT_TENANT_ID}/assets/${skillAsset.id}/tarballs/tool-a-1.0.0.tgz`,
      {
        method: "PUT",
        body: await makeNpmTarball({ name: "tool-a", version: "1.0.0" }),
      },
    );
    expect(res.status).toBe(404);
  });

  test("rejects with 404 when the asset id does not exist", async () => {
    const h = await setupMultiTenant({
      userId: ROOT_USER_ID,
      grants: PKR_GRANTS,
    });
    const res = await h.app.request(
      `/api/tenants/${ROOT_TENANT_ID}/assets/ast_missing/tarballs/tool-a-1.0.0.tgz`,
      {
        method: "PUT",
        body: await makeNpmTarball({ name: "tool-a", version: "1.0.0" }),
      },
    );
    expect(res.status).toBe(404);
  });

  test("rejects with 400 when the tarball's package.json fails validation", async () => {
    const { h, assetId } = await setupRegistry();
    // Build a tarball whose package.json is missing `name`.
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
    const res = await h.app.request(
      `/api/tenants/${ROOT_TENANT_ID}/assets/${assetId}/tarballs/bad-0.0.1.tgz`,
      { method: "PUT", body: bytes },
    );
    expect(res.status).toBe(400);
  });
});

describe("GET /api/tenants/:tenantId/assets/:assetId/tarballs", () => {
  test("lists uploaded tarballs with size and integrity", async () => {
    const { h, assetId } = await setupRegistry();
    const bytes = await makeNpmTarball({ name: "tool-a", version: "1.0.0" });
    const putRes = await h.app.request(
      `/api/tenants/${ROOT_TENANT_ID}/assets/${assetId}/tarballs/tool-a-1.0.0.tgz`,
      { method: "PUT", body: bytes },
    );
    expect(putRes.status).toBe(200);

    const listRes = await h.app.request(
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
    const { h, assetId } = await setupRegistry();
    const res = await h.app.request(
      `/api/tenants/${ROOT_TENANT_ID}/assets/${assetId}/tarballs`,
      { method: "GET" },
    );
    expect(res.status).toBe(200);
    const list: unknown = await res.json();
    expect(list).toEqual([]);
  });

  test("returns 404 when the asset is not a package-registry", async () => {
    const h = await setupMultiTenant({
      userId: ROOT_USER_ID,
      grants: PKR_GRANTS,
    });
    const skillAsset = await h.assetService.createAsset({
      tenantId: ROOT_TENANT_ID,
      kind: "skill",
      name: "greet",
      initOpts: { gitignore: SANE_GITIGNORE },
    });
    const res = await h.app.request(
      `/api/tenants/${ROOT_TENANT_ID}/assets/${skillAsset.id}/tarballs`,
      { method: "GET" },
    );
    expect(res.status).toBe(404);
  });
});

describe("DELETE /api/tenants/:tenantId/assets/:assetId/tarballs/:filename", () => {
  test("removes the tarball and commits", async () => {
    const { h, assetId } = await setupRegistry();
    const bytes = await makeNpmTarball({ name: "tool-a", version: "1.0.0" });
    const putRes = await h.app.request(
      `/api/tenants/${ROOT_TENANT_ID}/assets/${assetId}/tarballs/tool-a-1.0.0.tgz`,
      { method: "PUT", body: bytes },
    );
    expect(putRes.status).toBe(200);

    const delRes = await h.app.request(
      `/api/tenants/${ROOT_TENANT_ID}/assets/${assetId}/tarballs/tool-a-1.0.0.tgz`,
      { method: "DELETE" },
    );
    expect(delRes.status).toBe(200);
    const body: unknown = await delRes.json();
    if (!isObject(body)) throw new Error("expected object");
    expect(typeof body["commit"]).toBe("string");

    const listRes = await h.app.request(
      `/api/tenants/${ROOT_TENANT_ID}/assets/${assetId}/tarballs`,
      { method: "GET" },
    );
    const list: unknown = await listRes.json();
    expect(list).toEqual([]);
  });

  test("returns 404 when the filename is not present", async () => {
    const { h, assetId } = await setupRegistry();
    const res = await h.app.request(
      `/api/tenants/${ROOT_TENANT_ID}/assets/${assetId}/tarballs/missing-1.0.0.tgz`,
      { method: "DELETE" },
    );
    expect(res.status).toBe(404);
  });
});

describe("PUT tarballs: concurrent writes against the same asset", () => {
  test("two concurrent PUTs to different filenames both survive", async () => {
    // Regression: the pre-fix route read existing entries OUTSIDE the
    // per-repo lock and then issued writeTree(clearPrefix:"tarballs/")
    // under it. Two concurrent PUTs would both pre-image the same
    // empty (or stale) tip, and whichever write committed second would
    // clobber the first. The fix moves the read inside the lock via
    // repoStore.writeTreePreservingPrefix; both files must end up in
    // the final tree.
    const { h, assetId } = await setupRegistry();
    const a = await makeNpmTarball({ name: "tool-a", version: "1.0.0" });
    const b = await makeNpmTarball({ name: "tool-b", version: "1.0.0" });
    const [resA, resB] = await Promise.all([
      h.app.request(
        `/api/tenants/${ROOT_TENANT_ID}/assets/${assetId}/tarballs/tool-a-1.0.0.tgz`,
        { method: "PUT", body: a },
      ),
      h.app.request(
        `/api/tenants/${ROOT_TENANT_ID}/assets/${assetId}/tarballs/tool-b-1.0.0.tgz`,
        { method: "PUT", body: b },
      ),
    ]);
    expect(resA.status).toBe(200);
    expect(resB.status).toBe(200);

    const listRes = await h.app.request(
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
});

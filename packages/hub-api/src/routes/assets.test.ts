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
} from "@intx/crypto";
import type { KeyPair } from "@intx/types/runtime";
import type { GrantRule } from "@intx/types/authz";
import type { DB } from "@intx/db";
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
    deploySingleStepAtHead: () => {
      throw new Error(
        "mock: sessionService.deploySingleStepAtHead not implemented",
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

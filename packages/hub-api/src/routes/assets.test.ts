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
import type { DB } from "@intx/db";
import {
  asset as assetTable,
  agentAsset as agentAssetTable,
} from "@intx/db/schema";
import {
  createAssetService,
  createRepoStore,
  createSidecarEmitter,
  skillKindHandler,
  skillAuthorize,
  type AssetService,
  type EventCollectorRegistry,
  type RepoStore,
  type SessionService,
  type SidecarRouter,
} from "@intx/hub-sessions";

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
  kind: "agent-state" | "skill";
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
              const err = new Error(
                `duplicate key value violates unique constraint`,
              ) as Error & { code?: string };
              err.code = "23505";
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
  const repoStore = createRepoStore({
    dataDir,
    signingKey,
    handlers: { skill: skillKindHandler },
    authorize: skillAuthorize,
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
    expect(verifySSHSignature(payload, signature, signingKey.publicKey)).toBe(
      true,
    );
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

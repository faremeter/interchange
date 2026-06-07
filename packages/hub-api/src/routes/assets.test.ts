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
  });
  return { app, state };
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

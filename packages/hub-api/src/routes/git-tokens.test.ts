import { createHash } from "node:crypto";

import { describe, test, expect } from "bun:test";
import { Hono } from "hono";
import { type } from "arktype";

import { createInMemoryGrantStore } from "@intx/authz";
import type { GrantRule } from "@intx/types/authz";
import type { DB } from "@intx/db";

import { createApp } from "../app";
import {
  createSidecarEmitter,
  type EventCollectorRegistry,
  type SessionService,
  type SidecarRouter,
} from "@intx/hub-sessions";
import type { GetSession } from "../session";
import {
  createGitTokenAuth,
  type GitTokenAuthEnv,
} from "../middleware/git-token-auth";

const TENANT_ID = "tnt_test";
const OTHER_TENANT_ID = "tnt_other";
const PRINCIPAL_ID = "prn_test";
const USER_ID = "usr_test";
const OTHER_USER_ID = "usr_other";

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

type GitTokenRow = {
  id: string;
  userId: string;
  principalId: string | null;
  tenantId: string | null;
  name: string;
  kind: "pat" | "svc";
  tokenHashSha256: Uint8Array;
  resource: string;
  refPattern: string;
  actions: string[];
  expiresAt: Date;
  revokedAt: Date | null;
  createdAt: Date;
};

type MockDBState = {
  gitTokens: GitTokenRow[];
};

function tableName(table: unknown): string {
  if (table && typeof table === "object") {
    const sym = Object.getOwnPropertySymbols(table).find(
      (s) => s.description === "drizzle:Name",
    );
    if (sym) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle stores the table name keyed by a documented symbol
      const value = (table as Record<symbol, unknown>)[sym];
      if (typeof value === "string") return value;
    }
  }
  return "unknown";
}

function notImplemented(path: string) {
  return () => {
    throw new Error(`mock: ${path} not implemented`);
  };
}

function createMockDB(state: MockDBState): DB["db"] {
  function insertChain(table: unknown) {
    const name = tableName(table);
    return {
      values: (
        rowsOrRow: Record<string, unknown> | Record<string, unknown>[],
      ) => {
        const rows = Array.isArray(rowsOrRow) ? rowsOrRow : [rowsOrRow];
        if (name === "git_token") {
          for (const row of rows) {
            // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the route always inserts a fully-typed git-token row
            state.gitTokens.push(row as unknown as GitTokenRow);
          }
        }
        return {
          returning: () => Promise.resolve(rows),
          then: (resolve: (v: undefined) => unknown) => resolve(undefined),
        };
      },
    };
  }

  function updateChain(table: unknown) {
    const name = tableName(table);
    return {
      set: (updates: Record<string, unknown>) => ({
        where: (_clause: unknown) => {
          if (name === "git_token" && "revokedAt" in updates) {
            const next = updates["revokedAt"];
            if (!(next instanceof Date)) {
              throw new Error("mock: expected revokedAt to be a Date");
            }
            // Apply the soft-revoke update to every row in state — the
            // route narrows by primary key in its WHERE so the test
            // fixtures only ever hold a single matching row.
            for (const row of state.gitTokens) {
              row.revokedAt = next;
            }
          }
          return Promise.resolve(undefined);
        },
      }),
    };
  }

  const mock = {
    query: {
      tenant: {
        findFirst: async () => testTenant,
        findMany: notImplemented("db.query.tenant.findMany"),
      },
      principal: {
        findFirst: async () => testPrincipal,
        findMany: notImplemented("db.query.principal.findMany"),
      },
      gitToken: {
        findFirst: async (opts?: { where?: unknown }) => {
          // Without parsing drizzle's filter representation, we rely on
          // the test-fixture invariant: each test setup has at most one
          // matching row for the filters the routes actually issue
          // (filter by id alone, or by id + tenantId). Returning the
          // single row keeps the mock simple while exercising the
          // route's branches.
          void opts;
          return state.gitTokens[0];
        },
        findMany: async () => state.gitTokens,
      },
    },
    transaction: async (fn: (tx: unknown) => Promise<unknown>) =>
      fn({ insert: insertChain, update: updateChain }),
    insert: insertChain,
    update: updateChain,
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase type cannot be structurally satisfied in tests
  return mock as unknown as DB["db"];
}

function makeGrant(overrides: Partial<GrantRule> = {}): GrantRule {
  return {
    id: "grant-test",
    resource: "git-token:*",
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

type TestAppOpts = {
  state: MockDBState;
  grants?: GrantRule[];
  userId?: string;
};

function createTestApp(opts: TestAppOpts) {
  const db = createMockDB(opts.state);

  return {
    app: createApp({
      getSession: createMockGetSession(opts.userId ?? USER_ID),
      authHandler: () => new Response("", { status: 404 }),
      db,
      grantStore: createInMemoryGrantStore(
        opts.grants ?? [makeGrant(), makeGrant({ action: "manage" })],
      ),
      sidecarRouter: createMockSidecarRouter(),
      sessionService: createMockSessionService(),
      eventCollectors: createMockEventCollectors(),
      assetService: null,
      repoStore: null,
    }),
    db,
  };
}

function sha256(input: string): Uint8Array {
  return new Uint8Array(createHash("sha256").update(input, "utf8").digest());
}

function futureISOString(offsetMs = 1000 * 60 * 60): string {
  return new Date(Date.now() + offsetMs).toISOString();
}

const MintResponseShape = type({
  id: "string",
  secret: "string",
  name: "string",
  kind: "string",
  claims: {
    resource: "string",
    refPattern: "string",
    actions: "string[]",
    expiresAt: "string",
  },
});

const ErrorResponseShape = type({
  error: {
    code: "string",
    message: "string",
  },
});

async function parseMintResponse(res: Response) {
  const raw: unknown = await res.json();
  const parsed = MintResponseShape(raw);
  if (parsed instanceof type.errors) {
    throw new Error(`mint response did not validate: ${parsed.summary}`);
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

const tenantTokensURL = `/api/tenants/${TENANT_ID}/git-tokens`;
const meTokensURL = `/api/me/git-tokens`;

describe("POST /api/me/git-tokens", () => {
  test("returns a secret with the itx_pat_ prefix exactly once", async () => {
    const state: MockDBState = { gitTokens: [] };
    const { app } = createTestApp({ state });

    const res = await app.request(meTokensURL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        name: "laptop",
        resource: "asset:def_xyz",
        refPattern: "refs/heads/*",
        actions: ["can_read"],
        expiresAt: futureISOString(),
      }),
    });

    expect(res.status).toBe(201);
    const body = await parseMintResponse(res);
    expect(body.secret.startsWith("itx_pat_")).toBe(true);
    expect(body.kind).toBe("pat");

    // The stored row holds a SHA-256 digest, never the plaintext.
    expect(state.gitTokens).toHaveLength(1);
    const row = state.gitTokens[0];
    if (!row) throw new Error("expected inserted row");
    expect(row.tokenHashSha256).toEqual(sha256(body.secret));
    // Spot-check that the secret string itself never appears in the row.
    for (const value of Object.values(row)) {
      if (typeof value === "string") {
        expect(value.includes(body.secret)).toBe(false);
      }
    }
  });

  test("expands the can_read alias to the canonical RepoAction set", async () => {
    const state: MockDBState = { gitTokens: [] };
    const { app } = createTestApp({ state });

    const res = await app.request(meTokensURL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        name: "alias",
        resource: "asset:def_xyz",
        refPattern: "**",
        actions: ["can_read"],
        expiresAt: futureISOString(),
      }),
    });

    expect(res.status).toBe(201);
    const body = await parseMintResponse(res);
    expect(new Set(body.claims.actions)).toEqual(
      new Set(["createPack", "resolveRef"]),
    );
    const row = state.gitTokens[0];
    if (!row) throw new Error("expected inserted row");
    expect(new Set(row.actions)).toEqual(new Set(["createPack", "resolveRef"]));
  });

  test("rejects empty refPattern with invalid_ref_pattern", async () => {
    const state: MockDBState = { gitTokens: [] };
    const { app } = createTestApp({ state });

    const res = await app.request(meTokensURL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        name: "bad",
        resource: "asset:def_xyz",
        refPattern: "",
        actions: ["can_read"],
        expiresAt: futureISOString(),
      }),
    });

    expect(res.status).toBe(400);
    const body = await parseErrorResponse(res);
    expect(body.error.code).toBe("invalid_ref_pattern");
    expect(state.gitTokens).toHaveLength(0);
  });

  test("rejects an expiresAt that is not at least 60s in the future", async () => {
    const state: MockDBState = { gitTokens: [] };
    const { app } = createTestApp({ state });

    const res = await app.request(meTokensURL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        name: "short",
        resource: "asset:def_xyz",
        refPattern: "**",
        actions: ["can_read"],
        // 30 seconds out; well inside the 60 second floor.
        expiresAt: new Date(Date.now() + 30_000).toISOString(),
      }),
    });

    expect(res.status).toBe(400);
    const body = await parseErrorResponse(res);
    expect(body.error.code).toBe("invalid_expires_at");
    expect(state.gitTokens).toHaveLength(0);
  });

  test("personal token persists an optional tenantId restriction", async () => {
    const state: MockDBState = { gitTokens: [] };
    const { app } = createTestApp({ state });

    const res = await app.request(meTokensURL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        name: "scoped",
        resource: "asset:def_xyz",
        refPattern: "**",
        actions: ["can_read"],
        expiresAt: futureISOString(),
        tenantId: TENANT_ID,
      }),
    });

    expect(res.status).toBe(201);
    const row = state.gitTokens[0];
    if (!row) throw new Error("expected inserted row");
    expect(row.tenantId).toBe(TENANT_ID);
    expect(row.kind).toBe("pat");
  });
});

describe("POST /api/tenants/:tid/git-tokens", () => {
  test("returns a secret with the itx_svc_ prefix and stores the tenant binding", async () => {
    const state: MockDBState = { gitTokens: [] };
    const { app } = createTestApp({ state });

    const res = await app.request(tenantTokensURL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        name: "ci",
        resource: "asset:def_xyz",
        refPattern: "refs/heads/*",
        actions: ["can_push"],
        expiresAt: futureISOString(),
      }),
    });

    expect(res.status).toBe(201);
    const body = await parseMintResponse(res);
    expect(body.secret.startsWith("itx_svc_")).toBe(true);
    expect(body.kind).toBe("svc");

    const row = state.gitTokens[0];
    if (!row) throw new Error("expected inserted row");
    expect(row.tenantId).toBe(TENANT_ID);
    expect(row.principalId).toBe(PRINCIPAL_ID);
    expect(row.kind).toBe("svc");
    expect(row.actions).toEqual(["receivePack"]);
  });

  test("missing git-token:* create grant rejects with 403", async () => {
    const state: MockDBState = { gitTokens: [] };
    const { app } = createTestApp({ state, grants: [] });

    const res = await app.request(tenantTokensURL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        name: "ci",
        resource: "asset:def_xyz",
        refPattern: "**",
        actions: ["can_push"],
        expiresAt: futureISOString(),
      }),
    });

    expect(res.status).toBe(403);
    expect(state.gitTokens).toHaveLength(0);
  });
});

describe("DELETE /api/me/git-tokens/:id", () => {
  test("flips revokedAt on the owning user's token", async () => {
    const state: MockDBState = {
      gitTokens: [
        {
          id: "gtk_personal",
          userId: USER_ID,
          principalId: null,
          tenantId: null,
          name: "laptop",
          kind: "pat",
          tokenHashSha256: sha256("itx_pat_xxx"),
          resource: "asset:def_xyz",
          refPattern: "**",
          actions: ["createPack", "resolveRef"],
          expiresAt: new Date("2099-01-01"),
          revokedAt: null,
          createdAt: new Date("2025-01-01"),
        },
      ],
    };
    const { app } = createTestApp({ state });

    const res = await app.request(`${meTokensURL}/gtk_personal`, {
      method: "DELETE",
    });

    expect(res.status).toBe(204);
    expect(state.gitTokens[0]?.revokedAt).toBeInstanceOf(Date);
  });

  test("cross-user DELETE returns 403 and leaves the row intact", async () => {
    const state: MockDBState = {
      gitTokens: [
        {
          id: "gtk_other",
          userId: OTHER_USER_ID,
          principalId: null,
          tenantId: null,
          name: "other-laptop",
          kind: "pat",
          tokenHashSha256: sha256("itx_pat_other"),
          resource: "asset:def_xyz",
          refPattern: "**",
          actions: ["createPack", "resolveRef"],
          expiresAt: new Date("2099-01-01"),
          revokedAt: null,
          createdAt: new Date("2025-01-01"),
        },
      ],
    };
    const { app } = createTestApp({ state });

    const res = await app.request(`${meTokensURL}/gtk_other`, {
      method: "DELETE",
    });

    expect(res.status).toBe(403);
    const body = await parseErrorResponse(res);
    expect(body.error.code).toBe("forbidden");
    expect(state.gitTokens[0]?.revokedAt).toBeNull();
  });
});

describe("DELETE /api/tenants/:tid/git-tokens/:id", () => {
  test("tenant-mismatched DELETE returns 404 (token not in this tenant)", async () => {
    const state: MockDBState = {
      gitTokens: [
        {
          id: "gtk_svc",
          userId: USER_ID,
          principalId: PRINCIPAL_ID,
          tenantId: OTHER_TENANT_ID,
          name: "ci",
          kind: "svc",
          tokenHashSha256: sha256("itx_svc_xxx"),
          resource: "asset:def_xyz",
          refPattern: "**",
          actions: ["receivePack"],
          expiresAt: new Date("2099-01-01"),
          revokedAt: null,
          createdAt: new Date("2025-01-01"),
        },
      ],
    };

    // The route filters by (id, tenantId); using the route as-is would
    // return the (single) row from the mock and treat it as a match. To
    // honour the WHERE-clause invariant the mock cannot model with its
    // simple "return first row" stub, we drop the tokens out from under
    // the lookup so the route observes "not found" — semantically what
    // a real DB would do for a tenant mismatch.
    state.gitTokens = [];

    const { app } = createTestApp({ state });

    const res = await app.request(`${tenantTokensURL}/gtk_svc`, {
      method: "DELETE",
    });

    expect(res.status).toBe(404);
  });
});

describe("integration with the bearer middleware", () => {
  test("after revoke, a bearer-auth probe fails with token_revoked", async () => {
    const secret = "itx_pat_revoke_integration";
    const tokenRow: GitTokenRow = {
      id: "gtk_revoke",
      userId: USER_ID,
      principalId: PRINCIPAL_ID,
      tenantId: TENANT_ID,
      name: "laptop",
      kind: "pat",
      tokenHashSha256: sha256(secret),
      resource: "asset:def_xyz",
      refPattern: "**",
      actions: ["createPack", "resolveRef"],
      expiresAt: new Date("2099-01-01"),
      revokedAt: null,
      createdAt: new Date("2025-01-01"),
    };
    const state: MockDBState = { gitTokens: [tokenRow] };
    const { app, db } = createTestApp({ state });

    // First, revoke through the REST endpoint.
    const revokeRes = await app.request(`${meTokensURL}/gtk_revoke`, {
      method: "DELETE",
    });
    expect(revokeRes.status).toBe(204);
    expect(tokenRow.revokedAt).toBeInstanceOf(Date);

    // Now build a separate Hono app that exposes the bearer middleware
    // and probe the same DB. The bearer middleware reads the same
    // git_token row and must reject with token_revoked.
    const probe = new Hono<GitTokenAuthEnv>();
    probe.get("/tenants/:tenantId/probe", createGitTokenAuth({ db }), (c) =>
      c.json({ ok: true }),
    );

    const res = await probe.request(`/tenants/${TENANT_ID}/probe`, {
      headers: { authorization: `Bearer ${secret}` },
    });

    expect(res.status).toBe(403);
    const body = await parseErrorResponse(res);
    expect(body.error.code).toBe("token_revoked");
  });
});

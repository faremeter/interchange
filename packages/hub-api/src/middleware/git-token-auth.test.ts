import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { Hono } from "hono";

import type { DB } from "@intx/db";
import { configureSync, getConfig, resetSync } from "@intx/log";

import { createGitTokenAuth, type GitTokenAuthEnv } from "./git-token-auth";

type GitTokenRow = {
  id: string;
  userId: string;
  principalId: string | null;
  tenantId: string | null;
  name: string;
  kind: string;
  tokenHashSha256: Uint8Array;
  resource: string;
  refPattern: string;
  actions: string[];
  expiresAt: Date;
  revokedAt: Date | null;
  createdAt: Date;
};

type TenantRow = {
  id: string;
  name: string;
  slug: string;
  domain: string;
  parentId: string | null;
  config: unknown;
  createdAt: Date;
  updatedAt: Date;
};

type PrincipalRow = {
  id: string;
  tenantId: string;
  kind: "user" | "agent";
  refId: string;
  status: "active" | "suspended" | "invited" | "deactivated";
  createdAt: Date;
  updatedAt: Date;
};

type MockDBOpts = {
  gitToken?: GitTokenRow | null;
  tenant?: TenantRow | null;
  principal?: PrincipalRow | null;
  principalByRef?: PrincipalRow | null;
};

function notImplemented(path: string) {
  return () => {
    throw new Error(`mock: ${path} not implemented`);
  };
}

function createMockDB(opts: MockDBOpts): DB["db"] {
  const mock = {
    query: {
      gitToken: {
        findFirst: async () =>
          opts.gitToken !== null && opts.gitToken !== undefined
            ? opts.gitToken
            : undefined,
        findMany: notImplemented("db.query.gitToken.findMany"),
      },
      tenant: {
        findFirst: async () =>
          opts.tenant !== null && opts.tenant !== undefined
            ? opts.tenant
            : undefined,
        findMany: notImplemented("db.query.tenant.findMany"),
      },
      principal: {
        findFirst: async () => {
          if (
            opts.gitToken !== null &&
            opts.gitToken !== undefined &&
            opts.gitToken.principalId !== null
          ) {
            return opts.principal !== null && opts.principal !== undefined
              ? opts.principal
              : undefined;
          }
          if (opts.principalByRef !== null && opts.principalByRef !== undefined)
            return opts.principalByRef;
          if (opts.principal !== null && opts.principal !== undefined)
            return opts.principal;
          return undefined;
        },
        findMany: notImplemented("db.query.principal.findMany"),
      },
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase type cannot be structurally satisfied in tests
  return mock as unknown as DB["db"];
}

async function sha256(input: string): Promise<Uint8Array> {
  return new Uint8Array(
    await crypto.subtle.digest("SHA-256", new TextEncoder().encode(input)),
  );
}

const NOW = new Date("2025-01-15T00:00:00Z");
const FUTURE = new Date("2099-01-01T00:00:00Z");
const PAST = new Date("2024-01-01T00:00:00Z");

function makeTenant(id: string): TenantRow {
  return {
    id,
    name: `Tenant ${id}`,
    slug: id,
    domain: `${id}.example.com`,
    parentId: null,
    config: null,
    createdAt: NOW,
    updatedAt: NOW,
  };
}

function makePrincipal(
  id: string,
  tenantId: string,
  status: PrincipalRow["status"] = "active",
): PrincipalRow {
  return {
    id,
    tenantId,
    kind: "user",
    refId: "user_alice",
    status,
    createdAt: NOW,
    updatedAt: NOW,
  };
}

async function makeToken(
  overrides: Partial<GitTokenRow> = {},
): Promise<GitTokenRow> {
  const secret = overrides.id ? `${overrides.id}_secret` : "default_secret";
  return {
    id: "tok_1",
    userId: "user_alice",
    principalId: "prin_1",
    tenantId: "ten_a",
    name: "laptop",
    kind: "pat",
    tokenHashSha256: await sha256(`itx_pat_${secret}`),
    resource: "asset:def_xyz",
    refPattern: "**",
    actions: ["createPack", "resolveRef"],
    expiresAt: FUTURE,
    revokedAt: null,
    createdAt: NOW,
    ...overrides,
  };
}

function buildApp(db: DB["db"]) {
  const app = new Hono<GitTokenAuthEnv>();
  const auth = createGitTokenAuth({ db });
  app.get("/tenants/:tenantId/probe", auth, (c) => {
    const principal = c.get("principal");
    const tenant = c.get("tenant");
    const claims = c.get("git-token-claims");
    return c.json({
      principalId: principal.id,
      tenantId: tenant.id,
      claims: {
        resource: claims.resource,
        refPattern: claims.refPattern,
        actions: claims.actions,
        expiresAt: claims.expiresAt.toISOString(),
      },
    });
  });
  return app;
}

function basicAuthHeader(username: string, password: string): string {
  return (
    "Basic " + Buffer.from(`${username}:${password}`, "utf8").toString("base64")
  );
}

const captured: {
  category: readonly string[];
  level: string;
  message: string;
}[] = [];

const savedConfig = getConfig();

beforeAll(() => {
  configureSync({
    reset: true,
    sinks: {
      capture: (record) => {
        const message = Array.isArray(record.message)
          ? record.message
              .map((part) =>
                typeof part === "string" ? part : JSON.stringify(part),
              )
              .join("")
          : String(record.message);
        captured.push({
          category: record.category,
          level: record.level,
          message,
        });
      },
    },
    loggers: [
      { category: [], lowestLevel: "debug", sinks: ["capture"] },
      {
        category: ["logtape", "meta"],
        lowestLevel: "warning",
        sinks: ["capture"],
      },
    ],
  });
});

afterAll(() => {
  if (savedConfig) {
    configureSync({ reset: true, ...savedConfig });
  } else {
    resetSync();
  }
});

beforeEach(() => {
  captured.length = 0;
});

function hasGitTokenLog(): boolean {
  return captured.some(
    (r) =>
      r.category.length >= 2 &&
      r.category[0] === "hub" &&
      r.category[1] === "git-token",
  );
}

describe("createGitTokenAuth", () => {
  describe("missing or unparseable Authorization header", () => {
    test("no header returns 401 with WWW-Authenticate", async () => {
      const app = buildApp(createMockDB({}));
      const res = await app.request("/tenants/ten_a/probe");
      expect(res.status).toBe(401);
      expect(res.headers.get("www-authenticate")).toBe(
        'Basic realm="Interchange"',
      );
      const body = await res.json();
      expect(body).toEqual({
        error: { code: "unauthorized", message: "Authentication required" },
      });
      expect(hasGitTokenLog()).toBe(true);
    });

    test("malformed header (no scheme/space) returns 401", async () => {
      const app = buildApp(createMockDB({}));
      const res = await app.request("/tenants/ten_a/probe", {
        headers: { authorization: "garbage" },
      });
      expect(res.status).toBe(401);
      expect(res.headers.get("www-authenticate")).toBe(
        'Basic realm="Interchange"',
      );
    });

    test("unsupported scheme returns 401", async () => {
      const app = buildApp(createMockDB({}));
      const res = await app.request("/tenants/ten_a/probe", {
        headers: { authorization: "Digest abc" },
      });
      expect(res.status).toBe(401);
    });

    test("Basic with no password returns 401", async () => {
      const app = buildApp(createMockDB({}));
      const res = await app.request("/tenants/ten_a/probe", {
        headers: { authorization: basicAuthHeader("alice", "") },
      });
      expect(res.status).toBe(401);
    });
  });

  describe("malformed prefix is rejected before DB hit", () => {
    test("Bearer token without itx_ prefix returns 401 and does not query DB", async () => {
      let queried = false;
      const dbMock = {
        query: {
          gitToken: {
            findFirst: async () => {
              queried = true;
              return undefined;
            },
          },
          tenant: { findFirst: notImplemented("tenant.findFirst") },
          principal: { findFirst: notImplemented("principal.findFirst") },
        },
      };
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase type cannot be structurally satisfied in tests
      const db = dbMock as unknown as DB["db"];
      const app = buildApp(db);
      const res = await app.request("/tenants/ten_a/probe", {
        headers: { authorization: "Bearer not_a_valid_token" },
      });
      expect(res.status).toBe(401);
      expect(queried).toBe(false);
    });
  });

  describe("unknown token", () => {
    test("returns 401 with WWW-Authenticate (does not leak validity)", async () => {
      const app = buildApp(createMockDB({ gitToken: null }));
      const res = await app.request("/tenants/ten_a/probe", {
        headers: { authorization: "Bearer itx_pat_nonexistent" },
      });
      expect(res.status).toBe(401);
      expect(res.headers.get("www-authenticate")).toBe(
        'Basic realm="Interchange"',
      );
    });
  });

  describe("revoked token", () => {
    test("returns 403 token_revoked", async () => {
      const token = await makeToken({
        id: "tok_revoked",
        tokenHashSha256: await sha256("itx_pat_revoked_secret"),
        revokedAt: PAST,
      });
      const app = buildApp(
        createMockDB({
          gitToken: token,
          tenant: makeTenant("ten_a"),
          principal: makePrincipal("prin_1", "ten_a"),
        }),
      );
      const res = await app.request("/tenants/ten_a/probe", {
        headers: { authorization: "Bearer itx_pat_revoked_secret" },
      });
      expect(res.status).toBe(403);
      const body: unknown = await res.json();
      expect(body).toMatchObject({ error: { code: "token_revoked" } });
    });
  });

  describe("expired token", () => {
    test("returns 403 token_expired", async () => {
      const token = await makeToken({
        id: "tok_expired",
        tokenHashSha256: await sha256("itx_pat_expired_secret"),
        expiresAt: PAST,
      });
      const app = buildApp(
        createMockDB({
          gitToken: token,
          tenant: makeTenant("ten_a"),
          principal: makePrincipal("prin_1", "ten_a"),
        }),
      );
      const res = await app.request("/tenants/ten_a/probe", {
        headers: { authorization: "Bearer itx_pat_expired_secret" },
      });
      expect(res.status).toBe(403);
      const body: unknown = await res.json();
      expect(body).toMatchObject({ error: { code: "token_expired" } });
    });
  });

  describe("tenant mismatch", () => {
    test("tenant-bound token with mismatched URL tenant returns 403 tenant_mismatch", async () => {
      const token = await makeToken({
        id: "tok_bound_a",
        tokenHashSha256: await sha256("itx_svc_bound_secret"),
        kind: "svc",
        tenantId: "ten_a",
      });
      const app = buildApp(
        createMockDB({
          gitToken: token,
          tenant: makeTenant("ten_a"),
          principal: makePrincipal("prin_1", "ten_a"),
        }),
      );
      const res = await app.request("/tenants/ten_b/probe", {
        headers: { authorization: "Bearer itx_svc_bound_secret" },
      });
      expect(res.status).toBe(403);
      const body: unknown = await res.json();
      expect(body).toMatchObject({ error: { code: "tenant_mismatch" } });
    });
  });

  describe("personal token principal resolution from (userId, :tid)", () => {
    test("returns 403 principal_not_found when no principal exists for (userId, :tid)", async () => {
      const token = await makeToken({
        id: "tok_personal",
        tokenHashSha256: await sha256("itx_pat_personal_secret"),
        tenantId: null,
        principalId: null,
      });
      const app = buildApp(
        createMockDB({
          gitToken: token,
          tenant: makeTenant("ten_a"),
          principal: null,
          principalByRef: null,
        }),
      );
      const res = await app.request("/tenants/ten_a/probe", {
        headers: { authorization: "Bearer itx_pat_personal_secret" },
      });
      expect(res.status).toBe(403);
      const body: unknown = await res.json();
      expect(body).toMatchObject({ error: { code: "principal_not_found" } });
    });

    test("succeeds when principalByRef resolves", async () => {
      const token = await makeToken({
        id: "tok_personal",
        tokenHashSha256: await sha256("itx_pat_personal_ok"),
        tenantId: null,
        principalId: null,
      });
      const app = buildApp(
        createMockDB({
          gitToken: token,
          tenant: makeTenant("ten_a"),
          principalByRef: makePrincipal("prin_alice_a", "ten_a"),
        }),
      );
      const res = await app.request("/tenants/ten_a/probe", {
        headers: { authorization: "Bearer itx_pat_personal_ok" },
      });
      expect(res.status).toBe(200);
      const body: unknown = await res.json();
      expect(body).toMatchObject({
        principalId: "prin_alice_a",
        tenantId: "ten_a",
      });
    });
  });

  describe("suspended principal", () => {
    test("returns 403 principal_suspended", async () => {
      const token = await makeToken({
        id: "tok_susp",
        tokenHashSha256: await sha256("itx_pat_susp_secret"),
      });
      const app = buildApp(
        createMockDB({
          gitToken: token,
          tenant: makeTenant("ten_a"),
          principal: makePrincipal("prin_1", "ten_a", "suspended"),
        }),
      );
      const res = await app.request("/tenants/ten_a/probe", {
        headers: { authorization: "Bearer itx_pat_susp_secret" },
      });
      expect(res.status).toBe(403);
      const body: unknown = await res.json();
      expect(body).toMatchObject({ error: { code: "principal_suspended" } });
    });
  });

  describe("success path", () => {
    test("sets principal, tenant, and git-token-claims; logs success", async () => {
      const token = await makeToken({
        id: "tok_ok",
        tokenHashSha256: await sha256("itx_pat_ok_secret"),
        resource: "asset:def_xyz",
        refPattern: "refs/heads/main",
        actions: ["createPack", "resolveRef"],
      });
      const app = buildApp(
        createMockDB({
          gitToken: token,
          tenant: makeTenant("ten_a"),
          principal: makePrincipal("prin_1", "ten_a"),
        }),
      );
      const res = await app.request("/tenants/ten_a/probe", {
        headers: { authorization: "Bearer itx_pat_ok_secret" },
      });
      expect(res.status).toBe(200);
      const body: unknown = await res.json();
      expect(body).toMatchObject({
        principalId: "prin_1",
        tenantId: "ten_a",
        claims: {
          resource: "asset:def_xyz",
          refPattern: "refs/heads/main",
          actions: ["createPack", "resolveRef"],
          expiresAt: FUTURE.toISOString(),
        },
      });
      expect(hasGitTokenLog()).toBe(true);
      expect(
        captured.some(
          (r) => r.category[1] === "git-token" && r.message.includes("success"),
        ),
      ).toBe(true);
    });

    test("Basic auth: username variance does not affect gating", async () => {
      const token = await makeToken({
        id: "tok_basic",
        tokenHashSha256: await sha256("itx_pat_basic_secret"),
      });
      const db = createMockDB({
        gitToken: token,
        tenant: makeTenant("ten_a"),
        principal: makePrincipal("prin_1", "ten_a"),
      });
      const app = buildApp(db);

      for (const username of ["alice", "bob", "", "x"]) {
        const res = await app.request("/tenants/ten_a/probe", {
          headers: {
            authorization: basicAuthHeader(username, "itx_pat_basic_secret"),
          },
        });
        expect(res.status).toBe(200);
      }
    });

    test("Basic auth: username is logged for forensics", async () => {
      const token = await makeToken({
        id: "tok_log_user",
        tokenHashSha256: await sha256("itx_pat_log_user_secret"),
      });
      const app = buildApp(
        createMockDB({
          gitToken: token,
          tenant: makeTenant("ten_a"),
          principal: makePrincipal("prin_1", "ten_a"),
        }),
      );
      const res = await app.request("/tenants/ten_a/probe", {
        headers: {
          authorization: basicAuthHeader(
            "forensic-username",
            "itx_pat_log_user_secret",
          ),
        },
      });
      expect(res.status).toBe(200);
      expect(
        captured.some((r) => r.message.includes("forensic-username")),
      ).toBe(true);
    });
  });

  describe("SHA-256 hash round-trip", () => {
    test("the secret stored as SHA-256 matches the secret on the wire", async () => {
      const secret = "itx_pat_roundtrip_secret";
      const hash = await sha256(secret);
      const token = await makeToken({
        id: "tok_rt",
        tokenHashSha256: hash,
      });
      const app = buildApp(
        createMockDB({
          gitToken: token,
          tenant: makeTenant("ten_a"),
          principal: makePrincipal("prin_1", "ten_a"),
        }),
      );
      const res = await app.request("/tenants/ten_a/probe", {
        headers: { authorization: `Bearer ${secret}` },
      });
      expect(res.status).toBe(200);
    });
  });
});

import { describe, expect, test } from "bun:test";
import { Hono } from "hono";

import type { DB, RunTokenStore } from "@intx/db";

import type { PrincipalRow, TenantEnv, TenantRow } from "../context";
import {
  createRunTokenAuth,
  isRunTokenRoute,
  matchesRunTokenRoute,
} from "./run-token-auth";

const RUN = "/api/tenants/tnt_a/workflows/runs";
const NOW = new Date("2025-01-15T00:00:00Z");

const tenantRow: TenantRow = {
  id: "tnt_a",
  name: "Tenant A",
  slug: "tnt_a",
  domain: "tnt_a.example.com",
  parentId: null,
  config: null,
  createdAt: NOW,
  updatedAt: NOW,
};

const runPrincipal: PrincipalRow = {
  id: "prn_run",
  tenantId: "tnt_a",
  kind: "workflow",
  refId: "run_a",
  status: "active",
  createdAt: NOW,
  updatedAt: NOW,
};

function notImplemented(path: string) {
  return () => {
    throw new Error(`mock: ${path} not implemented`);
  };
}

function createMockDB(): DB["db"] {
  const mock = {
    query: {
      tenant: { findFirst: async () => tenantRow },
      principal: { findFirst: async () => runPrincipal },
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase type cannot be structurally satisfied in tests
  return mock as unknown as DB["db"];
}

function createMockRunTokenStore(): RunTokenStore {
  return {
    mintIfAbsent: notImplemented("runTokenStore.mintIfAbsent"),
    findByHash: async () => ({
      id: "rtk_a",
      principalId: runPrincipal.id,
      consumer: "action:caller",
    }),
    loadDelivery: notImplemented("runTokenStore.loadDelivery"),
  };
}

describe("createRunTokenAuth", () => {
  test("a run token clears the session identity a cookie set", async () => {
    const app = new Hono<TenantEnv>();
    app.use("*", async (c, next) => {
      c.set("user", {
        id: "usr_cookie",
        email: "cookie@example.com",
        emailVerified: true,
        name: "Cookie",
        createdAt: NOW,
        updatedAt: NOW,
      });
      c.set("session", {
        id: "ses_cookie",
        userId: "usr_cookie",
        token: "tok_cookie",
        expiresAt: new Date("2999-01-01"),
        createdAt: NOW,
        updatedAt: NOW,
      });
      await next();
    });
    app.use(
      "/api/tenants/:tenantId/*",
      createRunTokenAuth({
        db: createMockDB(),
        runTokenStore: createMockRunTokenStore(),
      }),
    );
    app.get("/api/tenants/:tenantId/workflows/runs", (c) =>
      c.json({
        user: c.get("user"),
        session: c.get("session"),
        principalId: c.get("principal").id,
      }),
    );

    const res = await app.request(RUN, {
      headers: { authorization: "Bearer itx_run_secret" },
    });

    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({
      user: null,
      session: null,
      principalId: runPrincipal.id,
    });
  });
});

describe("matchesRunTokenRoute", () => {
  test("a parameter matches one non-empty segment", () => {
    const pattern = "/api/tenants/:tenantId/workflows/runs/:runId";
    expect(matchesRunTokenRoute(pattern, `${RUN}/run_1`)).toBe(true);
    expect(matchesRunTokenRoute(pattern, `${RUN}/`)).toBe(false);
    expect(matchesRunTokenRoute(pattern, `${RUN}/run_1/events`)).toBe(false);
  });

  test("an encoded slash stays inside its segment", () => {
    const pattern = "/api/tenants/:tenantId/workflows/runs/:runId/events";
    expect(matchesRunTokenRoute(pattern, `${RUN}/a%2Fb/events`)).toBe(true);
  });

  test("a trailing slash is a different path", () => {
    expect(
      matchesRunTokenRoute("/api/tenants/:tenantId/workflows/runs", `${RUN}/`),
    ).toBe(false);
  });
});

describe("isRunTokenRoute", () => {
  test("allows a listed method and path", () => {
    expect(isRunTokenRoute("GET", RUN)).toBe(true);
    expect(isRunTokenRoute("DELETE", `${RUN}/run_1`)).toBe(true);
  });

  test("refuses a listed path with another method", () => {
    expect(isRunTokenRoute("DELETE", `${RUN}/run_1/mail`)).toBe(false);
    expect(isRunTokenRoute("HEAD", RUN)).toBe(false);
  });

  test("refuses a path that is not listed", () => {
    expect(isRunTokenRoute("GET", "/api/tenants/tnt_a/grants")).toBe(false);
    expect(isRunTokenRoute("POST", "/api/tenants/tnt_a/federation")).toBe(
      false,
    );
  });

  test("a run id that reads `runs` matches the run routes", () => {
    // `/workflows/runs/runs` matches the run-detail pattern; both patterns it
    // could match are read routes the router sends to the run router.
    expect(isRunTokenRoute("GET", `${RUN}/runs`)).toBe(true);
  });
});

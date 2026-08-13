import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import { Hono } from "hono";
import type { DB } from "@intx/db";
import { createInMemoryGrantStore } from "@intx/authz";
import type { GrantRule } from "@intx/types/authz";
import { createApp, createHubContextMiddleware, mountHubRoutes } from "./app";
import type { AppEnv } from "./context";
import {
  createEventCollectorRegistry,
  createSidecarRouter,
  type SessionService,
  type SidecarAuthenticator,
} from "@intx/hub-sessions";
import type { GetSession } from "./session";

const OpenAPISpec = type({
  info: { title: "string", version: "string" },
  paths: "Record<string, Record<string, unknown>>",
});

// eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase type cannot be structurally satisfied in tests
const mockDb = {} as unknown as DB["db"];
const acceptAnySidecar: SidecarAuthenticator = async ({ sidecarId }) => ({
  kind: "shared",
  sidecarId,
});
const sidecarRouter = createSidecarRouter({
  authenticateSidecar: acceptAnySidecar,
});
const sessionService: SessionService = {
  stageWorkflowStep(_params) {
    throw new Error("mock: sessionService.stageWorkflowStep not implemented");
  },
  deployInstanceAtHead(_params) {
    throw new Error(
      "mock: sessionService.deployInstanceAtHead not implemented",
    );
  },
  deployWorkflowDefinition(_params) {
    throw new Error(
      "mock: sessionService.deployWorkflowDefinition not implemented",
    );
  },
  deploySingleStepAtHead(_params) {
    throw new Error(
      "mock: sessionService.deploySingleStepAtHead not implemented",
    );
  },
  sendUserMessage(_params) {
    throw new Error("mock: sessionService.sendUserMessage not implemented");
  },
  endSession(_addr, _reason) {
    throw new Error("mock: sessionService.endSession not implemented");
  },
};
const eventCollectors = createEventCollectorRegistry({ db: mockDb });

const app = createApp({
  getSession: async () => null,
  authHandler: () => new Response("", { status: 404 }),
  db: mockDb,
  sidecarRouter,
  sessionService,
  eventCollectors,
  assetService: null,
  repoStore: null,
  maxTarballBytes: 10_000_000,
});

describe("app", () => {
  test("GET /status returns ok", async () => {
    const res = await app.request("/status");
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body).toEqual({ status: "ok" });
  });

  test("GET /openapi.json returns a valid spec with expected tags", async () => {
    const res = await app.request("/openapi.json");
    expect(res.status).toBe(200);

    const spec = OpenAPISpec.assert(await res.json());

    expect(spec.info.title).toBe("Interchange Hub");
    expect(spec.info.version).toBe("0.0.0");

    const paths = Object.keys(spec.paths);
    expect(paths.length).toBeGreaterThan(50);

    const tags = new Set<string>();
    for (const methods of Object.values(spec.paths)) {
      for (const op of Object.values(methods)) {
        if (typeof op !== "object" || op === null) continue;
        if (!("tags" in op)) continue;
        const { tags: opTags } = op;
        if (!Array.isArray(opTags)) continue;
        for (const tag of opTags) {
          if (typeof tag === "string") tags.add(tag);
        }
      }
    }

    const expectedTags = [
      "User",
      "Tenants",
      "Principals",
      "Roles",
      "Grants",
      "Runs",
      "Approvals",
      "Wallets",
      "Credentials",
      "Discovery",
      "Observability",
      "Agent Data",
    ];

    for (const tag of expectedTags) {
      expect(tags.has(tag)).toBe(true);
    }
  });

  test("federation routes do not double the /federation path segment", async () => {
    const res = await app.request("/openapi.json");
    const spec = OpenAPISpec.assert(await res.json());
    const federationPaths = Object.keys(spec.paths).filter((p) =>
      p.includes("federation"),
    );

    expect(federationPaths.length).toBeGreaterThan(0);
    for (const p of federationPaths) {
      expect(p).not.toContain("federation/federation");
    }
  });

  test("POST with invalid body returns 400", async () => {
    const res = await app.request("/api/tenants", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    expect(res.status).toBe(400);
  });

  test("opting out of asset+repo-store hides the git-tokens mint surface", async () => {
    // Token minting is only useful when at least one smart-HTTP route
    // consumes the tokens. With `assetService: null` and
    // `repoStore: null` there is no consumer, so the OpenAPI spec
    // must not advertise the `Git Tokens` tag and the mint paths
    // must not appear. The HTTP-request shape would test the same
    // invariant but is noisier — the auth middleware fronts the
    // `/api/me/*` tree and short-circuits with 401 before the router
    // matches, masking whether the route exists.
    const res = await app.request("/openapi.json");
    const spec = OpenAPISpec.assert(await res.json());

    const tags = new Set<string>();
    for (const methods of Object.values(spec.paths)) {
      for (const op of Object.values(methods)) {
        if (typeof op !== "object" || op === null) continue;
        if (!("tags" in op)) continue;
        const { tags: opTags } = op;
        if (!Array.isArray(opTags)) continue;
        for (const tag of opTags) {
          if (typeof tag === "string") tags.add(tag);
        }
      }
    }
    expect(tags.has("Git Tokens")).toBe(false);

    const paths = Object.keys(spec.paths);
    const gitTokenPaths = paths.filter((p) => p.includes("git-tokens"));
    expect(gitTokenPaths).toEqual([]);
  });
});

describe("mountHubRoutes composition", () => {
  const stubUser = {
    id: "usr_compose",
    email: "compose@example.com",
    emailVerified: true,
    name: "Compose Test",
    image: null,
    createdAt: new Date("2025-01-01"),
    updatedAt: new Date("2025-01-01"),
  };
  const stubSession = {
    id: "ses_compose",
    userId: stubUser.id,
    token: "tok_compose",
    expiresAt: new Date("2999-01-01"),
    createdAt: stubUser.createdAt,
    updatedAt: stubUser.updatedAt,
  };
  const getSession: GetSession = async () => ({
    user: stubUser,
    session: stubSession,
  });

  test("third-party Hono app shares the hub's request context", async () => {
    const thirdParty = new Hono<AppEnv>();
    thirdParty.use(createHubContextMiddleware({ getSession }));

    // A sibling route owned by the third party reads from the same
    // request context the hub routes use.
    thirdParty.get("/sibling/whoami", (c) => {
      const user = c.get("user");
      return c.json({ userId: user?.id ?? null });
    });

    mountHubRoutes(thirdParty, {
      db: mockDb,
      sidecarRouter,
      sessionService,
      eventCollectors,
      assetService: null,
      repoStore: null,
      maxTarballBytes: 10_000_000,
    });

    const siblingRes = await thirdParty.request("/sibling/whoami");
    expect(siblingRes.status).toBe(200);
    expect(await siblingRes.json()).toEqual({ userId: stubUser.id });

    // Hub routes mounted on the same app respond as expected.
    const statusRes = await thirdParty.request("/status");
    expect(statusRes.status).toBe(200);
    expect(await statusRes.json()).toEqual({ status: "ok" });
  });

  test("mountHubRoutes does not mount an auth handler at /api/auth/*", async () => {
    const thirdParty = new Hono<AppEnv>();
    thirdParty.use(createHubContextMiddleware({ getSession }));
    mountHubRoutes(thirdParty, {
      db: mockDb,
      sidecarRouter,
      sessionService,
      eventCollectors,
      assetService: null,
      repoStore: null,
      maxTarballBytes: 10_000_000,
    });

    // Without an auth handler mounted by the caller, /api/auth/* is
    // free for the third party to wire as they choose.
    const res = await thirdParty.request("/api/auth/anything");
    expect(res.status).toBe(404);
  });
});

describe("stub-router default-deny", () => {
  const TENANT = "tnt_stub";
  const stubUser = {
    id: "usr_stub",
    email: "stub@example.com",
    emailVerified: true,
    name: "Stub",
    image: null,
    createdAt: new Date("2025-01-01"),
    updatedAt: new Date("2025-01-01"),
  };
  const stubSession = {
    id: "ses_stub",
    userId: stubUser.id,
    token: "tok_stub",
    expiresAt: new Date("2999-01-01"),
    createdAt: stubUser.createdAt,
    updatedAt: stubUser.updatedAt,
  };
  const stubTenant = {
    id: TENANT,
    name: "Stub",
    slug: "stub",
    domain: "stub.example.com",
    parentId: null,
    config: null,
    createdAt: new Date("2025-01-01"),
    updatedAt: new Date("2025-01-01"),
  };
  const stubMember = {
    id: "prn_stub",
    tenantId: TENANT,
    kind: "user" as const,
    refId: stubUser.id,
    status: "active" as const,
    createdAt: new Date("2025-01-01"),
    updatedAt: new Date("2025-01-01"),
  };

  // resolveTenant only reads the tenant and the caller's membership principal;
  // the stub routes are 501 and touch no DB, so those two lookups are all the
  // default-deny path exercises.
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- only resolveTenant's tenant+principal lookups are exercised here
  const memberDb = {
    query: {
      tenant: { findFirst: async () => stubTenant },
      principal: { findFirst: async () => stubMember },
    },
  } as unknown as DB["db"];

  function grant(resource: string): GrantRule {
    return {
      id: `grant-${resource}`,
      resource,
      action: "read",
      effect: "allow",
      origin: "system",
      conditions: null,
      expiresAt: null,
      roleId: null,
      principalId: stubMember.id,
    };
  }

  function stubApp(grants: GrantRule[]) {
    return createApp({
      getSession: async () => ({ user: stubUser, session: stubSession }),
      authHandler: () => new Response("", { status: 404 }),
      db: memberDb,
      grantStore: createInMemoryGrantStore(grants),
      sidecarRouter,
      sessionService,
      eventCollectors,
      assetService: null,
      repoStore: null,
      maxTarballBytes: 10_000_000,
    });
  }

  const agentDataPath = `/api/tenants/${TENANT}/agents/agt_x/history/ref_x`;
  const tracesPath = `/api/tenants/${TENANT}/traces/trc_x`;

  test("an active member without a grant is denied the agent-data stub (403, not 501)", async () => {
    const res = await stubApp([]).request(agentDataPath);
    expect(res.status).toBe(403);
  });

  test("an active member without a grant is denied the observability stub (403, not 501)", async () => {
    const res = await stubApp([]).request(tracesPath);
    expect(res.status).toBe(403);
  });

  test("the agent-data grant passes the mount and reaches the 501 stub", async () => {
    const res = await stubApp([grant("agent-data:*")]).request(agentDataPath);
    expect(res.status).toBe(501);
  });

  test("the observability grant passes the mount and reaches the 501 stub", async () => {
    const res = await stubApp([grant("observability:*")]).request(tracesPath);
    expect(res.status).toBe(501);
  });
});

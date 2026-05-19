import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import { Hono } from "hono";
import type { DB } from "@interchange/db";
import { createApp, createHubContextMiddleware, mountHubRoutes } from "./app";
import type { AppEnv } from "./context";
import { createEventCollectorRegistry } from "./event-collector-registry";
import type { GetSession } from "./session";
import type { SessionService } from "./session-service";
import { createSidecarRouter } from "./ws/sidecar-handler";

const OpenAPISpec = type({
  info: { title: "string", version: "string" },
  paths: "Record<string, Record<string, unknown>>",
});

// eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase type cannot be structurally satisfied in tests
const mockDb = {} as unknown as DB["db"];
const sidecarRouter = createSidecarRouter({});
const sessionService: SessionService = {
  launchSession(_params) {
    throw new Error("mock: sessionService.launchSession not implemented");
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
      "Agents",
      "Instances",
      "Approvals",
      "Wallets",
      "Credentials",
      "Discovery",
      "Observability",
      "Agent Data",
      "Sidecars",
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
    });

    // Without an auth handler mounted by the caller, /api/auth/* is
    // free for the third party to wire as they choose.
    const res = await thirdParty.request("/api/auth/anything");
    expect(res.status).toBe(404);
  });
});

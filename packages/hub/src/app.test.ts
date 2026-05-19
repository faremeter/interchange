import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import type { DB } from "@interchange/db";
import { createApp } from "./app";
import { createEventCollectorRegistry } from "./event-collector-registry";
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

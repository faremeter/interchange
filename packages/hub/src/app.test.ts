import { describe, test, expect } from "bun:test";
import type { DB } from "@interchange/db";
import { createApp } from "./app";
import type { Auth } from "./auth";
import { createEventCollectorRegistry } from "./event-collector-registry";
import type { SessionService } from "./session-service";
import { createSidecarRouter } from "./ws/sidecar-handler";

const mockAuth = {
  api: {
    getSession: async () => null,
  },
  handler: async () => new Response("", { status: 404 }),
} as unknown as Auth;

const mockDb = {} as unknown as DB["db"];
const sidecarRouter = createSidecarRouter({});
const sessionService = {} as unknown as SessionService;
const eventCollectors = createEventCollectorRegistry(mockDb);

const app = createApp({
  auth: mockAuth,
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

    const spec = (await res.json()) as {
      info: { title: string; version: string };
      paths: Record<string, unknown>;
    };

    expect(spec.info.title).toBe("Interchange Hub");
    expect(spec.info.version).toBe("0.0.0");

    const paths = Object.keys(spec.paths);
    expect(paths.length).toBeGreaterThan(50);

    const tags = new Set<string>();
    for (const methods of Object.values(spec.paths) as Record<
      string,
      { tags?: string[] }
    >[]) {
      for (const op of Object.values(methods)) {
        if (op.tags) {
          for (const tag of op.tags) {
            tags.add(tag);
          }
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
    ];

    for (const tag of expectedTags) {
      expect(tags.has(tag)).toBe(true);
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

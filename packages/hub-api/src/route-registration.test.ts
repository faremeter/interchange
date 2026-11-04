import { describe, expect, test } from "bun:test";

import type { DB } from "@intx/db";
import {
  createEventCollectorRegistry,
  createSidecarRouter,
  type AssetService,
  type RepoStore,
  type SessionService,
} from "@intx/hub-sessions";

import { createApp } from "./app";
import { RUN_TOKEN_ROUTES } from "./middleware/run-token-auth";

// eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase type cannot be structurally satisfied in tests
const mockDb = {} as unknown as DB["db"];

const sessionService: SessionService = {
  stageWorkflowStep(_params) {
    throw new Error("mock: sessionService.stageWorkflowStep not implemented");
  },
  endSession(_addr, _reason) {
    throw new Error("mock: sessionService.endSession not implemented");
  },
};

function createRegistrationApp() {
  // The deploy, signal and asset route groups mount only with a repo
  // store and an asset service. Registration never calls either, so
  // placeholders are enough to list the routes.
  return createApp({
    getSession: async () => null,
    authHandler: () => new Response("", { status: 404 }),
    db: mockDb,
    sidecarRouter: createSidecarRouter({
      withExecutableWorkflowRun: async (_target, send) => send(),
      authenticateSidecar: async ({ sidecarId }) => ({ sidecarId }),
      resolveSidecarBindings: async () => [],
      validateSidecarIdentity: async () => true,
    }),
    sessionService,
    eventCollectors: createEventCollectorRegistry({ db: mockDb }),
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- registration-only placeholder
    assetService: {} as AssetService,
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- registration-only placeholder
    repoStore: {} as RepoStore,
    maxTarballBytes: 10_000_000,
  });
}

describe("run-token allow-list", () => {
  test("every allow-listed route is a registered tenant route", () => {
    const app = createRegistrationApp();
    const shape = (path: string) => path.replace(/:[^/]+/g, ":");
    const registered = new Set(
      app.routes.map((r) => `${r.method} ${shape(r.path)}`),
    );
    const unregistered = RUN_TOKEN_ROUTES.filter(
      (route) => !registered.has(`${route.method} ${shape(route.path)}`),
    );
    expect(unregistered).toEqual([]);

    // The guard admits paths, not handlers. Where a listed pattern and an
    // off-list route can match the same path, the router must send that
    // path to the listed route, so the listed route must be registered
    // first.
    const handlers = app.routes.filter((r) => r.method !== "ALL");
    const isListed = (r: { method: string; path: string }) =>
      RUN_TOKEN_ROUTES.some(
        (l) => l.method === r.method && shape(l.path) === shape(r.path),
      );
    const overlap = (a: string, b: string) => {
      const x = a.split("/");
      const y = b.split("/");
      return (
        x.length === y.length &&
        x.every((segment, i) => {
          const other = y[i] ?? "";
          return (
            segment.startsWith(":") ||
            other.startsWith(":") ||
            segment === other
          );
        })
      );
    };
    const shadowing: string[] = [];
    handlers.forEach((offList, offIndex) => {
      if (isListed(offList)) return;
      for (const listed of RUN_TOKEN_ROUTES) {
        if (listed.method !== offList.method) continue;
        if (!overlap(listed.path, offList.path)) continue;
        const listedIndex = handlers.findIndex(
          (r) =>
            r.method === listed.method && shape(r.path) === shape(listed.path),
        );
        if (listedIndex > offIndex) {
          shadowing.push(`${offList.method} ${offList.path}`);
        }
      }
    });
    expect(shadowing).toEqual([]);
  });
});

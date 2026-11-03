import { describe, expect, test } from "bun:test";
import { Hono, type Env } from "hono";
import type { RouterRoute } from "hono/types";

import { createInMemoryGrantStore } from "@intx/authz";
import type { DB } from "@intx/db";
import {
  createEventCollectorRegistry,
  createSidecarRouter,
  type AssetService,
  type RepoStore,
  type SessionService,
} from "@intx/hub-sessions";

import { createApp } from "./app";
import type { TenantEnv } from "./context";
import { createRequireGrant, isGrantCheck } from "./middleware/grant";
import { RUN_TOKEN_ROUTES } from "./middleware/run-token-auth";

// eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase type cannot be structurally satisfied in tests
const mockDb = {} as unknown as DB["db"];

const sessionService: SessionService = {
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

const TENANT_PREFIX = "/api/tenants/:tenantId";

// Any name for the tenant parameter, so a route that spells it differently
// is still checked.
function isTenantPath(path: string): boolean {
  return /^\/api\/tenants\/:[^/]+(\/|$)/.test(path);
}

/**
 * A tenant route that checks its grant in the handler instead of in
 * `requireGrant`, because the resource comes from data that the handler
 * reads. `path` is relative to `TENANT_PREFIX`. `reason` names the function
 * that checks the grant, the resource and action, and where the resource
 * comes from.
 */
type GrantExemption = { method: string; path: string; reason: string };

type SampleVerdict = "checked" | "unchecked" | { fault: string };

// The value for each parameter and wildcard in a sample request. It must not
// match a static path segment, or a guard on that segment could seem to
// cover a route with a parameter in the same place.
const SAMPLE_SEGMENT = "__sample__";

const GRANT_EXEMPTIONS: GrantExemption[] = [
  {
    method: "GET",
    path: "/workflows/runs/blobs/:blobId",
    reason:
      "blob handler: workflow-run:<run> / read; run from the session mail row",
  },
  {
    method: "GET",
    path: "/approvals/:approvalId",
    reason:
      "approval detail handler: approval:<anchorRunId> / resolve; run from the approval row",
  },
  {
    method: "POST",
    path: "/approvals/:approvalId/approve",
    reason:
      "resolveApproval: approval:<anchorRunId> / resolve; run from the approval row",
  },
  {
    method: "POST",
    path: "/approvals/:approvalId/reject",
    reason:
      "resolveApproval: approval:<anchorRunId> / resolve; run from the approval row",
  },
  {
    method: "POST",
    path: "/federation",
    reason:
      "denyUnlessBodyGrant: federation:<targetTenantId> / create; target from the body",
  },
  {
    method: "GET",
    path: "/assets/:kind/:nameDotGit/info/refs",
    reason:
      "resolveSmartHttp (assets): git token, asset:<id> verdict after the asset lookup",
  },
  {
    method: "POST",
    path: "/assets/:kind/:nameDotGit/git-upload-pack",
    reason:
      "resolveSmartHttp (assets): git token, asset:<id> verdict after the asset lookup",
  },
  {
    method: "POST",
    path: "/assets/:kind/:nameDotGit/git-receive-pack",
    reason:
      "resolveSmartHttp (assets): git token, asset:<id> verdict after the asset lookup",
  },
  {
    method: "GET",
    path: "/workflows/runs/:runId/state.git/info/refs",
    reason:
      "resolveSmartHttp (agent-state-git): git token, agent-state:<id> verdict after the run lookup",
  },
  {
    method: "POST",
    path: "/workflows/runs/:runId/state.git/git-upload-pack",
    reason:
      "resolveSmartHttp (agent-state-git): git token, agent-state:<id> verdict after the run lookup",
  },
];

/**
 * Lists every tenant route that runs without a grant check and is not in
 * `exemptions`, plus every exemption that is stale, repeated or not needed.
 *
 * The check asks the app's own router for the handler chain of a sample
 * request to each route, and requires a `requireGrant` middleware before the
 * route's handler. It proves that a grant check is present, not that its
 * resource and action are right. A middleware that wraps `requireGrant`
 * carries no tag, so a route behind one reports as unchecked. That includes
 * every route of a sub-app with its own `onError`, because `app.route()`
 * wraps each handler of such a sub-app.
 *
 * The route table does not say which entries are middleware. An `ALL` entry
 * on a path that ends in `*` counts as middleware, so the check does not see
 * a handler mounted with `app.all(".../*", ...)`, and middleware mounted with
 * `app.use()` on an exact path reads as a route; mount tenant middleware on a
 * `/*` path. A route registered twice in a row, the second time with a grant
 * check and a handler, is also out of reach; a route registered twice with
 * other routes between reports as registered more than once. A grant check
 * counts if it comes before the route's last handler, so the check does not
 * see a handler earlier in the same registration that answers first; put
 * `requireGrant` ahead of every handler that can answer.
 *
 * A route for every method is sent as GET, so the check does not see a route
 * that answers another method on the path first. Reject such a tenant route
 * outright if one is ever needed; a list of methods cannot cover custom ones.
 */
function findGrantViolations<E extends Env>(
  app: Hono<E>,
  exemptions: GrantExemption[],
): string[] {
  const violations: string[] = [];
  // Each handler of a route has its own entry, and the router hands back
  // the same entry objects, so entries identify a route even when routes
  // share a handler function. One registration adds its entries in a row,
  // and its last entry is the one that answers.
  const entriesOf = new Map<string, { entries: RouterRoute[]; last: number }>();
  app.routes.forEach((r, i) => {
    if (r.method === "ALL" && r.path.endsWith("*")) return;
    const key = `${r.method} ${r.path}`;
    const seen = entriesOf.get(key);
    if (seen !== undefined && seen.last !== i - 1 && isTenantPath(r.path)) {
      violations.push(`${key}: registered more than once`);
    }
    entriesOf.set(key, { entries: [...(seen?.entries ?? []), r], last: i });
  });
  // Routes outside the tenant prefix stay in the map, because one of them can
  // answer a tenant path first.
  const routes = new Map<string, RouterRoute>();
  for (const [key, { entries }] of entriesOf) {
    const answering = entries.at(-1);
    if (answering === undefined) continue;
    // Grant checks with no handler on a wildcard path guard a prefix, such as
    // one mounted with `app.get(".../*", requireGrant(...))`.
    const guard = entries.every((e) => isGrantCheck(e.handler));
    if (answering.path.endsWith("*") && guard) continue;
    routes.set(key, answering);
  }
  const routeOfEntry = new Map<RouterRoute, string>();
  for (const [key, r] of routes) routeOfEntry.set(r, key);

  const exempt = new Set<string>();
  for (const e of exemptions) {
    const key = `${e.method} ${TENANT_PREFIX}${e.path}`;
    if (exempt.has(key)) violations.push(`${key}: exempted twice`);
    exempt.add(key);
    if (!routes.has(key)) violations.push(`${key}: exemption matches no route`);
  }

  for (const [key, r] of routes) {
    if (!isTenantPath(r.path)) continue;
    if (r.path.includes("{")) {
      violations.push(`${key}: no sample value for a regex parameter`);
      continue;
    }
    // A `/*` route also answers its bare prefix, so it gets a sample of each,
    // unless the bare prefix has its own route, which the check covers.
    const stem = r.path.replace(/:\w+/g, SAMPLE_SEGMENT);
    const bare = `${r.method} ${r.path.slice(0, -2)}`;
    const filled = stem.replace(/\*$/, SAMPLE_SEGMENT);
    const samples =
      stem.endsWith("/*") && !routes.has(bare)
        ? [filled, stem.slice(0, -2)]
        : [filled];
    const results = samples.map((sample) => ({
      sample,
      verdict: verdictAt(r, sample),
    }));
    const uniform = results.every(
      ({ verdict }) =>
        JSON.stringify(verdict) === JSON.stringify(results[0]?.verdict),
    );
    for (const [i, { sample, verdict }] of results.entries()) {
      if (uniform && i > 0) break;
      const at = uniform ? key : `${key} at ${sample}`;
      if (typeof verdict === "object") {
        violations.push(`${at}: ${verdict.fault}`);
      } else if (verdict === "unchecked" && !exempt.has(key)) {
        violations.push(`${at}: no grant check`);
      }
    }
    const checked = results.every(({ verdict }) => verdict === "checked");
    if (checked && exempt.has(key)) {
      violations.push(`${key}: has a grant check, so needs no exemption`);
    }
  }
  return violations.sort();

  function verdictAt(r: RouterRoute, sample: string): SampleVerdict {
    const [matches] = app.router.match(
      r.method === "ALL" ? "GET" : r.method,
      sample,
    );
    const chain = matches.map(([[handler, entry]]) => ({ handler, entry }));
    const own = chain.findIndex((link) => link.entry === r);
    if (own < 0) return { fault: `${sample} does not reach the route` };
    const before = chain.slice(0, own);
    const shadow = before.find((link) => routeOfEntry.has(link.entry));
    if (shadow !== undefined) {
      return { fault: `shadowed by ${routeOfEntry.get(shadow.entry)}` };
    }
    // A route for every method needs a guard for every method; a guard for
    // some methods leaves the others open.
    const checked = before.some(
      (link) =>
        isGrantCheck(link.handler) &&
        (r.method !== "ALL" || link.entry.method === "ALL"),
    );
    return checked ? "checked" : "unchecked";
  }
}

describe("tenant route grant checks", () => {
  test("every tenant route checks a grant or is exempt", () => {
    expect(
      findGrantViolations(createRegistrationApp(), GRANT_EXEMPTIONS),
    ).toEqual([]);
  });

  test("the check sees the route groups that mount only with a repo store", () => {
    const registered = new Set(
      createRegistrationApp().routes.map((r) => `${r.method} ${r.path}`),
    );
    const expected = [
      "POST /workflows/deployments",
      "GET /git-tokens",
      "GET /assets",
      "GET /workflows/runs/:runId/state.git/info/refs",
    ].map((route) => {
      const [method, path] = route.split(" ");
      return `${method} ${TENANT_PREFIX}${path}`;
    });
    expect(expected.filter((route) => !registered.has(route))).toEqual([]);
  });

  const requireGrant = createRequireGrant({
    grantStore: createInMemoryGrantStore([]),
    conditionRegistry: {},
  });
  const route = `${TENANT_PREFIX}/x`;
  const answer = () => new Response("ok");

  test("accepts a route that mounts a grant check", () => {
    const app = new Hono<TenantEnv>();
    app.get(route, requireGrant("widget:*", "read"), answer);
    expect(findGrantViolations(app, [])).toEqual([]);
  });

  test("accepts a route under a prefix that mounts a grant check", () => {
    const app = new Hono<TenantEnv>();
    app.use(`${route}/*`, requireGrant("widget:*", "read"));
    app.get(`${route}/y`, answer);
    expect(findGrantViolations(app, [])).toEqual([]);
  });

  test("reports a route without a grant check", () => {
    const app = new Hono<TenantEnv>();
    app.get(route, answer);
    expect(findGrantViolations(app, [])).toEqual([
      `GET ${route}: no grant check`,
    ]);
  });

  test("accepts an exempt route without a grant check", () => {
    const app = new Hono<TenantEnv>();
    app.get(route, answer);
    expect(
      findGrantViolations(app, [{ method: "GET", path: "/x", reason: "r" }]),
    ).toEqual([]);
  });

  test("reports stale, repeated and unneeded exemptions", () => {
    const app = new Hono<TenantEnv>();
    app.get(route, requireGrant("widget:*", "read"), answer);
    expect(
      findGrantViolations(app, [
        { method: "GET", path: "/x", reason: "r" },
        { method: "GET", path: "/x", reason: "r" },
        { method: "GET", path: "/gone", reason: "r" },
      ]),
    ).toEqual([
      `GET ${TENANT_PREFIX}/gone: exemption matches no route`,
      `GET ${route}: exempted twice`,
      `GET ${route}: has a grant check, so needs no exemption`,
    ]);
  });

  test("reports a route that an earlier route answers first", () => {
    const app = new Hono<TenantEnv>();
    app.get(`${route}/:id`, requireGrant("widget:*", "read"), answer);
    app.get(`${route}/special`, answer);
    expect(findGrantViolations(app, [])).toEqual([
      `GET ${route}/special: shadowed by GET ${route}/:id`,
    ]);
  });

  test("reports a route without a grant check on the real app", () => {
    const app = createRegistrationApp();
    app.get(route, answer);
    expect(findGrantViolations(app, GRANT_EXEMPTIONS)).toEqual([
      `GET ${route}: no grant check`,
    ]);
  });

  test("reports a route for every method without a grant check", () => {
    const app = new Hono<TenantEnv>();
    app.all(route, answer);
    expect(findGrantViolations(app, [])).toEqual([
      `ALL ${route}: no grant check`,
    ]);
  });

  test("reports a route for every method behind a guard for one method", () => {
    const app = new Hono<TenantEnv>();
    app.get(`${route}/*`, requireGrant("widget:*", "read"));
    app.all(`${route}/y`, answer);
    expect(findGrantViolations(app, [])).toEqual([
      `ALL ${route}/y: no grant check`,
    ]);
  });

  test("reports a route whose grant check comes after its handler", () => {
    const app = new Hono<TenantEnv>();
    app.get(route, answer, requireGrant("widget:*", "read"));
    expect(findGrantViolations(app, [])).toEqual([
      `GET ${route}: no grant check`,
    ]);
  });

  test("reports a route for every method behind a guard for some methods", () => {
    const app = new Hono<TenantEnv>();
    app.on(
      ["GET", "POST", "PUT", "PATCH", "DELETE"],
      `${route}/*`,
      requireGrant("widget:*", "read"),
    );
    app.all(`${route}/y`, answer);
    expect(findGrantViolations(app, [])).toEqual([
      `ALL ${route}/y: no grant check`,
    ]);
  });

  test("reports a wildcard route whose grant check comes after its handler", () => {
    const app = new Hono<TenantEnv>();
    app.get(`${route}/*`, answer, requireGrant("widget:*", "read"));
    expect(findGrantViolations(app, [])).toEqual([
      `GET ${route}/*: no grant check`,
    ]);
  });

  test("reports a tenant route that a route outside the prefix answers first", () => {
    const app = new Hono<TenantEnv>();
    app.get("/api/tenants/*", answer);
    app.get(route, requireGrant("widget:*", "read"), answer);
    expect(findGrantViolations(app, [])).toEqual([
      `GET ${route}: shadowed by GET /api/tenants/*`,
    ]);
  });

  test("reports a wildcard route whose bare prefix skips the guard", () => {
    const app = new Hono<TenantEnv>();
    app.use(`${route}/:seg/*`, requireGrant("widget:*", "read"));
    app.get(`${route}/*`, answer);
    expect(findGrantViolations(app, [])).toEqual([
      `GET ${route}/* at /api/tenants/${SAMPLE_SEGMENT}/x: no grant check`,
    ]);
  });

  test("reports a route that only a guard on a static segment seems to cover", () => {
    const app = new Hono<TenantEnv>();
    app.use(`${route}/*`, requireGrant("widget:*", "read"));
    app.get(`${TENANT_PREFIX}/:thing/y`, answer);
    expect(findGrantViolations(app, [])).toEqual([
      `GET ${TENANT_PREFIX}/:thing/y: no grant check`,
    ]);
  });

  test("leaves the bare prefix of a wildcard route to its own route", () => {
    const app = new Hono<TenantEnv>();
    app.get(route, requireGrant("widget:*", "read"), answer);
    app.get(`${route}/*`, requireGrant("widget:*", "read"), answer);
    expect(findGrantViolations(app, [])).toEqual([]);
  });

  test("reports a tenant route with another name for the tenant parameter", () => {
    const app = new Hono<TenantEnv>();
    app.get("/api/tenants/:tid/y", answer);
    expect(findGrantViolations(app, [])).toEqual([
      "GET /api/tenants/:tid/y: no grant check",
    ]);
  });

  test("accepts a route under a method prefix that mounts a grant check", () => {
    const app = new Hono<TenantEnv>();
    app.get(`${route}/*`, requireGrant("widget:*", "read"));
    app.get(`${route}/y`, answer);
    expect(findGrantViolations(app, [])).toEqual([]);
  });

  test("reports a route registered again by a later route group", () => {
    const app = new Hono<TenantEnv>();
    app.get(route, answer);
    app.get(`${route}/other`, requireGrant("widget:*", "read"), answer);
    app.get(route, requireGrant("widget:*", "read"), answer);
    expect(findGrantViolations(app, [])).toEqual([
      `GET ${route}: registered more than once`,
    ]);
  });
});

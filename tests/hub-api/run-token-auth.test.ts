import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { eq } from "drizzle-orm";

import { createInMemoryGrantStore } from "@intx/authz";
import { createRunTokenStore } from "@intx/db";
import { principal, workflowDefinition, workflowRun } from "@intx/db/schema";
import { createApp, RUN_TOKEN_ROUTES, type GetSession } from "@intx/hub-api";
import {
  createEventCollectorRegistry,
  createSidecarRouter,
  type AssetService,
  type RepoStore,
  type SessionService,
  type WorkflowAllocationService,
} from "@intx/hub-sessions";
import type { GrantRule } from "@intx/types/authz";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { createTestCredentialCipher } from "@intx/test-harness/crypto";
import { seedAsset, seedPrincipal, seedTenants } from "@intx/test-harness/seed";

// A workflow run token authenticates as its run principal on the tenant
// routes, carries its consumer into authorization, and reaches only the
// allow-listed routes. A request without one keeps the session flow.

const TENANT_ID = "tnt_runtoken";
const OTHER_TENANT_ID = "tnt_runtoken_other";
const RUN_PRINCIPAL_ID = "prn_run";
const USER_PRINCIPAL_ID = "prn_user";
const USER_ID = "usr_runtoken";
const CONSUMER = "action:caller";
const RUNS = `/api/tenants/${TENANT_ID}/workflows/runs`;

const cipher = createTestCredentialCipher();

function grant(
  id: string,
  principalId: string,
  resource: string,
  action: string,
  extra: Partial<GrantRule> = {},
): GrantRule {
  return {
    id,
    resource,
    action,
    effect: "allow",
    origin: "system",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId,
    ...extra,
  };
}

const USER_OWNER = grant("grt_user", USER_PRINCIPAL_ID, "*", "*");
const RUN_EVERYTHING = grant("grt_run_all", RUN_PRINCIPAL_ID, "*", "*");

function mockGetSession(userId: string | null): GetSession {
  const now = new Date("2026-01-01");
  return async () =>
    userId === null
      ? null
      : {
          user: {
            id: userId,
            email: "user@example.com",
            emailVerified: true,
            name: "User",
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
        };
}

function mockSessionService(): SessionService {
  const notImpl = (name: string) => (): never => {
    throw new Error(`mock: sessionService.${name} not implemented`);
  };
  return {
    stageWorkflowStep: notImpl("stageWorkflowStep"),
    endSession: notImpl("endSession"),
  };
}

describe.skipIf(!harnessDbEnvAvailable())(
  "workflow run token authentication (real DB)",
  () => {
    let h: TestDb;
    let token: string;

    beforeAll(async () => {
      h = await createTestDb();
    });

    afterAll(async () => {
      await h.close();
    });

    beforeEach(async () => {
      await h.reset();
      await seedTenants(h.db, [{ id: TENANT_ID }, { id: OTHER_TENANT_ID }]);
      await seedPrincipal(h.db, {
        id: USER_PRINCIPAL_ID,
        tenantId: TENANT_ID,
        refId: USER_ID,
      });
      await seedPrincipal(h.db, {
        id: RUN_PRINCIPAL_ID,
        tenantId: TENANT_ID,
        kind: "workflow",
        refId: "run_caller",
      });
      const store = createRunTokenStore({ db: h.db, cipher });
      await store.mintIfAbsent(
        { principalId: RUN_PRINCIPAL_ID, consumers: [CONSUMER] },
        h.db,
      );
      const [delivered] = await store.loadDelivery(RUN_PRINCIPAL_ID);
      if (delivered === undefined) throw new Error("expected a run token");
      token = delivered.token;
    });

    function makeApp(
      grants: GrantRule[],
      sessionUser: string | null = null,
      workflowAllocationService?: WorkflowAllocationService,
    ) {
      return createApp({
        // The deploy route mounts only with the stores. A stub allocation
        // service never reaches them.
        ...(workflowAllocationService !== undefined
          ? {
              workflowAllocationService,
              // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- unused by the stubbed deploy
              assetService: {} as AssetService,
              // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- unused by the stubbed deploy
              repoStore: {} as RepoStore,
            }
          : { assetService: null, repoStore: null }),
        getSession: mockGetSession(sessionUser),
        authHandler: () => new Response("", { status: 404 }),
        db: h.db,
        grantStore: createInMemoryGrantStore(grants),
        credentialCipher: cipher,
        sidecarRouter: createSidecarRouter({
          withExecutableWorkflowRun: async (_target, send) => send(),
          authenticateSidecar: async ({ sidecarId }) => ({ sidecarId }),
          resolveSidecarBindings: async () => [],
          validateSidecarIdentity: async () => true,
        }),
        sessionService: mockSessionService(),
        eventCollectors: createEventCollectorRegistry({ db: h.db }),
        maxTarballBytes: 10_000_000,
      });
    }

    function asRun(
      app: ReturnType<typeof makeApp>,
      path: string,
      init: RequestInit = {},
    ) {
      return app.request(path, {
        ...init,
        headers: { authorization: `Bearer ${token}` },
      });
    }

    test("a request without a run token keeps the session flow", async () => {
      const app = makeApp([USER_OWNER], USER_ID);
      expect((await app.request(RUNS)).status).toBe(200);
    });

    test("a run token authorizes as its run principal", async () => {
      const app = makeApp([
        grant("grt_run_read", RUN_PRINCIPAL_ID, "workflow-run:*", "read"),
      ]);
      expect((await asRun(app, RUNS)).status).toBe(200);
    });

    test("a run token without a grant is refused", async () => {
      expect((await asRun(makeApp([]), RUNS)).status).toBe(403);
    });

    test("an unknown run token is unauthenticated", async () => {
      const res = await makeApp([RUN_EVERYTHING]).request(RUNS, {
        headers: { authorization: "Bearer itx_run_unknown" },
      });
      expect(res.status).toBe(401);
    });

    test("a run token is not valid in another tenant", async () => {
      const app = makeApp([RUN_EVERYTHING]);
      const res = await asRun(
        app,
        `/api/tenants/${OTHER_TENANT_ID}/workflows/runs`,
      );
      expect(res.status).toBe(403);
    });

    test("a run token stops working when its run principal is deactivated", async () => {
      await h.db
        .update(principal)
        .set({ status: "deactivated" })
        .where(eq(principal.id, RUN_PRINCIPAL_ID));
      expect((await asRun(makeApp([RUN_EVERYTHING]), RUNS)).status).toBe(401);
    });

    test("a run token held by a principal that is not a workflow is unauthenticated", async () => {
      const store = createRunTokenStore({ db: h.db, cipher });
      await store.mintIfAbsent(
        { principalId: USER_PRINCIPAL_ID, consumers: [CONSUMER] },
        h.db,
      );
      const [userToken] = await store.loadDelivery(USER_PRINCIPAL_ID);
      if (userToken === undefined) throw new Error("expected a run token");
      const res = await makeApp([RUN_EVERYTHING]).request(RUNS, {
        headers: { authorization: `Bearer ${userToken.token}` },
      });
      expect(res.status).toBe(401);
    });

    test("a run that deploys a workflow is recorded as its deployer", async () => {
      // The deployer is the invoker of record, so the child's invoker
      // requirements resolve against the run that deployed it.
      await seedAsset(h.db, {
        id: "ast_child",
        tenantId: TENANT_ID,
        kind: "workflow",
        name: "child",
        creatorPrincipalId: USER_PRINCIPAL_ID,
      });
      const prepared: Parameters<
        WorkflowAllocationService["prepareProvisionedDeployment"]
      >[0][] = [];
      const app = makeApp([RUN_EVERYTHING], null, {
        prepareProvisionedDeployment: async (args) => {
          prepared.push(args);
          await h.db.insert(workflowDefinition).values({
            id: "wfd_child",
            tenantId: TENANT_ID,
            name: "child",
            assetId: args.definitionAssetId,
          });
          await h.db.insert(workflowRun).values({
            id: args.anchorRunId,
            tenantId: TENANT_ID,
            anchorRunId: args.anchorRunId,
            definitionId: "wfd_child",
            address: `${args.anchorRunId}@${args.deploymentDomain}`,
            status: "running",
          });
          return {
            anchorRunId: args.anchorRunId,
            deploymentAddress: `${args.anchorRunId}@${args.deploymentDomain}`,
            allocationId: "sal_child",
            status: "pending",
          };
        },
        deployReadyAllocation: async () => null,
      });

      const res = await app.request(
        `/api/tenants/${TENANT_ID}/workflows/deployments`,
        {
          method: "POST",
          headers: {
            authorization: `Bearer ${token}`,
            "content-type": "application/json",
          },
          body: JSON.stringify({
            source: {
              kind: "asset",
              assetId: "ast_child",
              package: { format: "source", commitSha: "a".repeat(40) },
            },
            entry: "./workflow.ts",
            sourceOfferingIds: ["ofr_primary"],
            defaultSourceOfferingId: "ofr_primary",
          }),
        },
      );
      expect(res.status).toBe(201);
      expect(prepared.map((p) => p.sourceAuthorityPrincipalId)).toEqual([
        RUN_PRINCIPAL_ID,
      ]);
    });

    test("a run token cannot reach a route off the allow-list, whatever its grants", async () => {
      const app = makeApp([RUN_EVERYTHING]);
      const refused: [string, string][] = [
        ["GET", `/api/tenants/${TENANT_ID}/grants`],
        ["GET", `/api/tenants/${TENANT_ID}/approvals`],
        ["POST", `/api/tenants/${TENANT_ID}/federation`],
        ["POST", `${RUNS}/run_x/mail`],
        ["HEAD", RUNS],
        ["GET", `/api/tenants/${TENANT_ID}`],
        ["PATCH", `/api/tenants/${TENANT_ID}`],
        ["GET", `${RUNS}/run_x/state.git/info/refs`],
      ];
      for (const [method, path] of refused) {
        const res = await asRun(app, path, { method });
        expect({ method, path, status: res.status }).toEqual({
          method,
          path,
          status: 403,
        });
      }
    });

    test("the allow-list matches the decoded path the router routes on", async () => {
      // `%73` decodes to `s`, so the guard and the router both see `runs`.
      const app = makeApp([RUN_EVERYTHING]);
      const res = await asRun(
        app,
        `/api/tenants/${TENANT_ID}/workflows/run%73`,
      );
      expect(res.status).toBe(200);
    });

    test("a grant scoped to another consumer does not authorize the token", async () => {
      const app = makeApp([
        grant("grt_run_read", RUN_PRINCIPAL_ID, "workflow-run:*", "read", {
          conditions: { tool: "action:other" },
        }),
      ]);
      expect((await asRun(app, RUNS)).status).toBe(403);
    });

    test("a grant scoped to the token's consumer authorizes it", async () => {
      const app = makeApp([
        grant("grt_run_read", RUN_PRINCIPAL_ID, "workflow-run:*", "read", {
          conditions: { tool: CONSUMER },
        }),
      ]);
      expect((await asRun(app, RUNS)).status).toBe(200);
    });

    test("a deny scoped to the token's consumer overrides an unscoped allow", async () => {
      const app = makeApp([
        grant("grt_run_read", RUN_PRINCIPAL_ID, "workflow-run:*", "read"),
        grant("grt_run_deny", RUN_PRINCIPAL_ID, "workflow-run:*", "read", {
          effect: "deny",
          conditions: { tool: CONSUMER },
        }),
      ]);
      expect((await asRun(app, RUNS)).status).toBe(403);
    });

    test("a git token bearer passes through to the session flow", async () => {
      const app = makeApp([USER_OWNER], USER_ID);
      const res = await app.request(RUNS, {
        headers: { authorization: "Bearer itx_pat_not_a_run_token" },
      });
      expect(res.status).toBe(200);
    });

    test("every allow-listed route is a registered tenant route", () => {
      // The deploy, signal and asset route groups mount only with a repo
      // store and an asset service. Registration never calls either, so
      // placeholders are enough to list the routes.
      const app = createApp({
        getSession: mockGetSession(null),
        authHandler: () => new Response("", { status: 404 }),
        db: h.db,
        sidecarRouter: createSidecarRouter({
          withExecutableWorkflowRun: async (_target, send) => send(),
          authenticateSidecar: async ({ sidecarId }) => ({ sidecarId }),
          resolveSidecarBindings: async () => [],
          validateSidecarIdentity: async () => true,
        }),
        sessionService: mockSessionService(),
        eventCollectors: createEventCollectorRegistry({ db: h.db }),
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- registration-only placeholder
        assetService: {} as AssetService,
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- registration-only placeholder
        repoStore: {} as RepoStore,
        maxTarballBytes: 10_000_000,
      });
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
              r.method === listed.method &&
              shape(r.path) === shape(listed.path),
          );
          if (listedIndex > offIndex) {
            shadowing.push(`${offList.method} ${offList.path}`);
          }
        }
      });
      expect(shadowing).toEqual([]);
    });
  },
);

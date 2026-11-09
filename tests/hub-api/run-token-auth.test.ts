import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { type } from "arktype";
import { eq } from "drizzle-orm";
import type { RouterRoute } from "hono/types";

import { createInMemoryGrantStore } from "@intx/authz";
import { generateKeyPair } from "@intx/crypto";
import {
  createApprovalStore,
  createRunTokenStore,
  createSignalCorrelationStore,
} from "@intx/db";
import {
  gitToken,
  grant as grantTable,
  principal,
  signalCorrelation,
  workflowDefinition,
  workflowRun,
} from "@intx/db/schema";
import { createApp, type GetSession } from "@intx/hub-api";
import {
  createAssetService,
  createEventCollectorRegistry,
  createSidecarRouter,
  type AssetService,
  type RepoStore,
  type SessionService,
  type SidecarRouter,
  type WorkflowAllocationService,
} from "@intx/hub-sessions";
import type { GrantRule } from "@intx/types/authz";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { createTestCredentialCipher } from "@intx/test-harness/crypto";
import {
  seedAsset,
  seedPrincipal,
  seedTenants,
  seedWorkflowRun,
  tenantSlugFromId,
} from "@intx/test-harness/seed";
import { base64Encode, signalName } from "@intx/types";

import { createTestRepoStore } from "./lib/repo-store";

// A workflow run token authenticates as its run principal on the tenant
// routes, carries its consumer into authorization, and reaches every tenant
// route, where the route's grant check decides. A request without one keeps
// the session flow.

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
      opts: {
        triggerStores?: boolean;
        sidecarRouter?: Partial<SidecarRouter>;
        stores?: { assetService: AssetService; repoStore: RepoStore };
      } = {},
    ) {
      return createApp({
        // The deploy route and the run mail trigger mount only with the
        // stores. A stub allocation service never reaches them, and a mail
        // request these tests send is refused before the trigger uses them.
        ...(opts.stores !== undefined
          ? opts.stores
          : workflowAllocationService !== undefined ||
              opts.triggerStores === true
            ? {
                // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- unused by the stubbed deploy
                assetService: {} as AssetService,
                // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- unused by the stubbed deploy
                repoStore: {} as RepoStore,
              }
            : { assetService: null, repoStore: null }),
        ...(workflowAllocationService !== undefined
          ? { workflowAllocationService }
          : {}),
        getSession: mockGetSession(sessionUser),
        authHandler: () => new Response("", { status: 404 }),
        db: h.db,
        grantStore: createInMemoryGrantStore(grants),
        credentialCipher: cipher,
        sidecarRouter: {
          ...createSidecarRouter({
            withExecutableWorkflowRun: async (_target, send) => send(),
            authenticateSidecar: async ({ sidecarId }) => ({ sidecarId }),
            resolveSidecarBindings: async () => [],
            validateSidecarIdentity: async () => true,
          }),
          ...opts.sidecarRouter,
        },
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
        headers: { ...init.headers, authorization: `Bearer ${token}` },
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

    test("a run token with a session cookie cannot mint a git token", async () => {
      const app = makeApp([RUN_EVERYTHING], USER_ID, undefined, {
        triggerStores: true,
      });
      const res = await asRun(app, `/api/tenants/${TENANT_ID}/git-tokens`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          name: "ci",
          resource: "asset:ast_any",
          refPattern: "**",
          actions: ["can_push"],
          expiresAt: new Date(Date.now() + 60 * 60 * 1000).toISOString(),
        }),
      });
      expect(res.status).toBe(403);
      expect(await res.json()).toEqual({
        error: {
          code: "forbidden",
          message: "Only a user principal can mint a git token",
        },
      });
      expect(await h.db.select().from(gitToken)).toEqual([]);
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

    describe("the route's grant check decides", () => {
      const json = (body: unknown): RequestInit => ({
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      });
      const routes: {
        method: string;
        path: string;
        init?: RequestInit;
        granted: number;
      }[] = [
        {
          method: "GET",
          path: `/api/tenants/${TENANT_ID}/grants`,
          granted: 200,
        },
        {
          method: "GET",
          path: `/api/tenants/${TENANT_ID}/approvals`,
          granted: 200,
        },
        {
          method: "POST",
          path: `/api/tenants/${TENANT_ID}/federation`,
          init: json({
            targetTenantId: OTHER_TENANT_ID,
            direction: "outbound",
          }),
          granted: 201,
        },
        {
          method: "POST",
          path: `/api/tenants/${TENANT_ID}/workflows/run_x/mail`,
          // A run signs its own mail, so the route refuses a hub-signed body
          // with `signed_mail_required`.
          init: json({ content: "hi" }),
          granted: 400,
        },
        { method: "HEAD", path: RUNS, granted: 200 },
        { method: "GET", path: `/api/tenants/${TENANT_ID}`, granted: 200 },
        {
          method: "PATCH",
          path: `/api/tenants/${TENANT_ID}`,
          init: json({ config: {} }),
          granted: 200,
        },
      ];

      async function statuses(grants: GrantRule[]) {
        const app = makeApp(grants, null, undefined, { triggerStores: true });
        const seen: {
          method: string;
          path: string;
          status: number;
          refusal: string | null;
        }[] = [];
        for (const { method, path, init } of routes) {
          const res = await asRun(app, path, { ...init, method });
          // A HEAD response has no body to carry the refusal.
          const refusal =
            res.status === 403 && method !== "HEAD"
              ? type({ error: { message: "string" } }).assert(await res.json())
                  .error.message
              : null;
          seen.push({ method, path, status: res.status, refusal });
        }
        return seen;
      }

      test("a run with the grant reaches each route", async () => {
        expect(await statuses([RUN_EVERYTHING])).toEqual(
          routes.map(({ method, path, granted }) => ({
            method,
            path,
            status: granted,
            refusal: null,
          })),
        );
      });

      test("a run without the grant is refused by each route's grant check", async () => {
        expect(await statuses([])).toEqual(
          routes.map(({ method, path }) => ({
            method,
            path,
            status: 403,
            refusal:
              method === "HEAD"
                ? null
                : "You do not have permission to perform this action",
          })),
        );
      });
    });

    describe("approval resolution", () => {
      // The approval belongs to another run, as for a run that supervises
      // another deployment.
      const TARGET = "run_approval_target";
      const TARGET_ADDRESS = `${TARGET}@${tenantSlugFromId(TENANT_ID).toLowerCase()}.example.test`;
      const APPROVAL_ID = "apr_target";
      const CORRELATION = "cor_target";
      const APPROVE = `/api/tenants/${TENANT_ID}/approvals/${APPROVAL_ID}/approve`;

      beforeEach(async () => {
        await seedWorkflowRun(h.db, {
          id: TARGET,
          tenantId: TENANT_ID,
          anchorRunId: TARGET,
          address: TARGET_ADDRESS,
        });
        await createApprovalStore(h.db).create({
          id: APPROVAL_ID,
          tenantId: TENANT_ID,
          anchorRunId: TARGET,
          runId: TARGET,
          agentAddress: TARGET_ADDRESS,
          correlationId: CORRELATION,
          toolDefinition: {
            name: "search",
            description: "search",
            inputSchema: { type: "object" },
          },
          toolArguments: {},
          timeoutAt: new Date(Date.now() + 3_600_000),
        });
        await createSignalCorrelationStore(h.db).register({
          correlationId: CORRELATION,
          tenantId: TENANT_ID,
          anchorRunId: TARGET,
          agentAddress: TARGET_ADDRESS,
          runId: TARGET,
          signalName: signalName(CORRELATION),
          kind: "approval",
        });
      });

      async function approve(grants: GrantRule[]) {
        const delivered: Parameters<SidecarRouter["sendSignalDeliver"]>[0][] =
          [];
        const app = makeApp(grants, null, undefined, {
          sidecarRouter: {
            sendSignalDeliver: async (frame) => {
              delivered.push(frame);
            },
          },
        });
        const res = await asRun(app, APPROVE, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ scope: "once" }),
        });
        const stored = await createApprovalStore(h.db).findById(APPROVAL_ID);
        const correlation = await h.db.query.signalCorrelation.findFirst({
          where: eq(signalCorrelation.correlationId, CORRELATION),
        });
        return { res, delivered, stored, correlation };
      }

      test("a run with resolve on approvals resolves one", async () => {
        const { res, delivered, stored, correlation } = await approve([
          grant("grt_run_resolve", RUN_PRINCIPAL_ID, "approval:*", "resolve"),
        ]);
        expect(res.status).toBe(200);
        expect(await res.json()).toMatchObject({
          id: APPROVAL_ID,
          status: "approved",
          scope: "once",
          resolvedAt: expect.any(String),
        });
        expect(stored?.status).toBe("approved");
        expect(correlation?.resolvedAt).toBeInstanceOf(Date);
        expect(delivered).toHaveLength(1);
        expect(delivered[0]).toMatchObject({
          agentAddress: TARGET_ADDRESS,
          runId: TARGET,
          signalName: signalName(CORRELATION),
          payload: { outcome: "approved" },
        });
      });

      test.each([
        ["manage on approvals", "approval:*", "manage"],
        ["resolve on another run's approvals", "approval:run_other", "resolve"],
      ])(
        "a run with %s cannot resolve one",
        async (_name, resource, action) => {
          const { res, delivered, stored, correlation } = await approve([
            grant("grt_run_approval", RUN_PRINCIPAL_ID, resource, action),
          ]);
          expect(res.status).toBe(403);
          expect(await res.json()).toEqual({
            error: {
              code: "forbidden",
              message: "You do not have permission to resolve this approval",
            },
          });
          expect(stored?.status).toBe("pending");
          expect(correlation?.resolvedAt).toBeNull();
          expect(delivered).toEqual([]);
        },
      );
    });

    describe("grant creation", () => {
      // The run gives itself a grant, the widening that docs/AUTH.md warns an
      // operator about.
      function createGrant(grants: GrantRule[]) {
        return asRun(makeApp(grants), `/api/tenants/${TENANT_ID}/grants`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            principalId: RUN_PRINCIPAL_ID,
            resource: "workflow-run:*",
            action: "manage",
            effect: "allow",
            origin: "system",
          }),
        });
      }

      async function storedGrants() {
        return await h.db
          .select({
            principalId: grantTable.principalId,
            resource: grantTable.resource,
            action: grantTable.action,
          })
          .from(grantTable)
          .where(eq(grantTable.tenantId, TENANT_ID));
      }

      test("a run with create on grants gives itself a grant", async () => {
        const res = await createGrant([
          grant("grt_run_grant", RUN_PRINCIPAL_ID, "grant:*", "create"),
        ]);
        expect(res.status).toBe(201);
        expect(await res.json()).toMatchObject({
          principalId: RUN_PRINCIPAL_ID,
          resource: "workflow-run:*",
          action: "manage",
        });
        expect(await storedGrants()).toEqual([
          {
            principalId: RUN_PRINCIPAL_ID,
            resource: "workflow-run:*",
            action: "manage",
          },
        ]);
      });

      test("a run without create on grants cannot create one", async () => {
        const res = await createGrant([
          grant("grt_run_grant", RUN_PRINCIPAL_ID, "grant:*", "read"),
        ]);
        expect(res.status).toBe(403);
        expect(await res.json()).toEqual({
          error: {
            code: "forbidden",
            message: "You do not have permission to perform this action",
          },
        });
        expect(await storedGrants()).toEqual([]);
      });
    });

    describe("the run mail route", () => {
      const TARGET = "run_mail_target";
      const TARGET_ADDRESS = `${TARGET}@${tenantSlugFromId(TENANT_ID).toLowerCase()}.example.test`;
      const MANAGE_TARGET = grant(
        "grt_run_manage",
        RUN_PRINCIPAL_ID,
        `workflow-run:${TARGET}`,
        "manage",
      );

      beforeEach(async () => {
        // A routable run is its own anchor and has an address.
        await seedWorkflowRun(h.db, {
          id: TARGET,
          tenantId: TENANT_ID,
          anchorRunId: TARGET,
          address: TARGET_ADDRESS,
        });
      });

      function postMail(grants: GrantRule[], body: unknown) {
        const app = makeApp(grants, null, undefined, { triggerStores: true });
        return asRun(app, `${RUNS}/${TARGET}/mail`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(body),
        });
      }

      test("a run token with manage on the target reaches it and must sign its own mail", async () => {
        const res = await postMail([MANAGE_TARGET], { content: "hi" });
        expect(res.status).toBe(400);
        expect(await res.json()).toMatchObject({
          error: { code: "signed_mail_required" },
        });
      });

      test("a caller-signed message from another sender is refused", async () => {
        // Every routing header is right except From, so only the From check
        // refuses it.
        const raw = new TextEncoder().encode(
          [
            "From: someone@else.test",
            `To: ${TARGET_ADDRESS}`,
            "Message-ID: <m1@else.test>",
            "Interchange-Type: conversation.message",
            "",
            "hi",
          ].join("\r\n"),
        );
        const res = await postMail([MANAGE_TARGET], {
          rawMessage: base64Encode(raw),
        });
        expect(res.status).toBe(400);
        const body = type({
          error: { code: "string", message: "string" },
        }).assert(await res.json());
        expect(body.error.code).toBe("mail_header_invalid");
        expect(body.error.message).toContain(
          "From must be the caller's address",
        );
      });

      test("a run token without manage on the target is refused", async () => {
        const res = await postMail(
          [
            grant(
              "grt_run_read",
              RUN_PRINCIPAL_ID,
              `workflow-run:${TARGET}`,
              "read",
            ),
          ],
          { content: "hi" },
        );
        expect(res.status).toBe(403);
        const body = type({ error: { message: "string" } }).assert(
          await res.json(),
        );
        expect(body.error.message).toBe(
          "You do not have permission to perform this action",
        );
      });
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

    // Every registered tenant route, called by a run that holds every grant,
    // with unknown ids and an empty JSON body. The run holds every grant, so
    // a 401 or 403 can only come from a block on the run itself. Most
    // handlers stop at their lookup or their body validation, so this shows
    // that no route blocks or fails a run before that point. A refusal
    // after validation is out of its reach, such as the git-token mint's
    // refusal of every principal that is not a user; the tests above cover
    // the paths past that point. route-registration.test.ts enumerates and
    // samples the routes the same way.
    test("no tenant route blocks a run with every grant before validation or fails it with a server error", async () => {
      const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), "run-sweep-"));
      try {
        const repoStore = createTestRepoStore({
          dataDir,
          signingKey: await generateKeyPair(),
        });
        const app = makeApp([RUN_EVERYTHING], null, undefined, {
          stores: {
            repoStore,
            assetService: createAssetService({ db: h.db, repoStore }),
          },
        });

        const entriesOf = new Map<string, RouterRoute[]>();
        for (const r of app.routes) {
          if (r.method === "ALL" && r.path.endsWith("*")) continue;
          if (!TENANT_PATH.test(r.path)) continue;
          const key = `${r.method} ${r.path}`;
          entriesOf.set(key, [...(entriesOf.get(key) ?? []), r]);
        }

        const keyOf = new Map<RouterRoute, string>();
        for (const [key, entries] of entriesOf) {
          for (const e of entries) keyOf.set(e, key);
        }

        const offenders: string[] = [];
        const unreached: string[] = [];
        const unsampled: string[] = [];
        // Pins for routes the app does not mount.
        const unmounted = new Set(SWEEP_PINNED.keys());
        for (const [key, entries] of entriesOf) {
          const [first] = entries;
          if (first === undefined) continue;
          const method = first.method === "ALL" ? "GET" : first.method;
          const sample = sampleTenantPath(first.path);
          if (sample.unsampled.length > 0) {
            unsampled.push(key);
            continue;
          }
          // The first route the sample matches answers it, so it must be this
          // one; the wildcard guards before it are not in `keyOf`.
          const [matches] = app.router.match(method, sample.url);
          const answering = matches
            .map(([[, entry]]) => keyOf.get(entry))
            .find((k) => k !== undefined);
          if (answering !== key) {
            unreached.push(
              `${key} at ${sample.url}: answered by ${answering ?? "none"}`,
            );
            continue;
          }

          const withBody = ["POST", "PUT", "PATCH"].includes(method);
          const res = await asRun(app, sample.url, {
            method,
            ...(withBody
              ? {
                  headers: { "content-type": "application/json" },
                  body: "{}",
                }
              : {}),
          });
          const body = await res.text();
          const answer = `${key} -> ${res.status} ${body.slice(0, 200)}`;
          const pinned = SWEEP_PINNED.get(key);
          if (pinned !== undefined) {
            unmounted.delete(key);
            const challenged = res.headers.has("www-authenticate");
            if (
              res.status !== pinned.status ||
              !body.includes(pinned.marker) ||
              challenged !== (pinned.challenge ?? false)
            ) {
              offenders.push(`${answer} (pinned to ${pinned.status})`);
            }
            continue;
          }
          if (res.status === 401 || res.status === 403 || res.status >= 500) {
            offenders.push(answer);
          }
        }

        for (const route of SWEEP_REQUIRED) {
          if (!entriesOf.has(route)) unreached.push(`${route}: not mounted`);
        }
        expect({
          offenders,
          unreached,
          unsampled,
          unmounted: [...unmounted],
        }).toEqual({
          offenders: [],
          unreached: [],
          unsampled: [],
          unmounted: [],
        });
      } finally {
        await fs.rm(dataDir, { recursive: true, force: true });
      }
    });
  },
);

const TENANT_PATH = /^\/api\/tenants\/:[^/]+(\/|$)/;
const TENANT_ROUTE = "/api/tenants/:tenantId";
// The repository segment is SMART_HTTP_REPO_SEGMENT in hub-api's asset routes.
const SMART_HTTP_REPO = `${TENANT_ROUTE}/assets/:kind/:nameDotGit{[^/]+\\.git}`;

// The value for each parameter and wildcard in a sample request. It must not
// match a static path segment, or the sample could reach another route.
const SWEEP_SAMPLE = "__sample__";

// The value for a parameter constrained by a pattern, keyed by the pattern.
const SWEEP_PATTERN_SAMPLES: Record<string, string> = {
  "[^/]+\\.git": `${SWEEP_SAMPLE}.git`,
};

// A route whose answer the sweep pins: its status, a text its body carries,
// and whether it carries the git-token middleware's Basic challenge, so the
// pin fails when the answer comes from somewhere else.
type SweepPin = {
  status: number;
  marker: string;
  challenge?: true;
  reason: string;
};

// A stub that answers 501 to every caller.
const NOT_IMPLEMENTED: SweepPin = {
  status: 501,
  marker: '"not_implemented"',
  reason: "not implemented for any caller",
};
const NO_LIFECYCLE: SweepPin = {
  status: 503,
  marker: "Workflow lifecycle service unavailable",
  reason: "the test app has no workflow lifecycle service",
};
const GIT_TOKEN_ONLY: SweepPin = {
  status: 401,
  marker: '"unauthorized"',
  challenge: true,
  reason: "smart-HTTP takes only a git token",
};

const SWEEP_PINNED = new Map<string, SweepPin>([
  ...[
    "/agents/:agentId/logs",
    "/agents/:agentId/metrics",
    "/traces",
    "/traces/:traceId",
    "/agents/:agentId/data",
    "/agents/:agentId/data/*",
    "/agents/:agentId/history",
    "/agents/:agentId/history/:ref",
    "/agents/:agentId/branches",
  ].map((p): [string, SweepPin] => [
    `GET ${TENANT_ROUTE}${p}`,
    NOT_IMPLEMENTED,
  ]),
  [
    `POST ${TENANT_ROUTE}/agents/:agentId/history/:ref/restore`,
    NOT_IMPLEMENTED,
  ],
  [`GET ${TENANT_ROUTE}/workflows/runs/:runId/mail`, NOT_IMPLEMENTED],
  [`GET ${TENANT_ROUTE}/workflows/runs/:runId/lifecycle`, NO_LIFECYCLE],
  [`POST ${TENANT_ROUTE}/workflows/runs/:runId/capacity/release`, NO_LIFECYCLE],
  [`DELETE ${TENANT_ROUTE}/workflows/runs/:runId`, NO_LIFECYCLE],
  [`GET ${SMART_HTTP_REPO}/info/refs`, GIT_TOKEN_ONLY],
  [`POST ${SMART_HTTP_REPO}/git-upload-pack`, GIT_TOKEN_ONLY],
  [`POST ${SMART_HTTP_REPO}/git-receive-pack`, GIT_TOKEN_ONLY],
]);

// Route groups that mount only with a repo store, so a change to the app
// wiring cannot drop them from the sweep unseen.
const SWEEP_REQUIRED = [
  `POST ${TENANT_ROUTE}/workflows/deployments`,
  `GET ${TENANT_ROUTE}/git-tokens`,
  `GET ${TENANT_ROUTE}/assets`,
  `GET ${SMART_HTTP_REPO}/info/refs`,
];

function sampleTenantPath(routePath: string) {
  const unsampled: string[] = [];
  const url = routePath
    .replace(/^\/api\/tenants\/:[^/]+/, `/api/tenants/${TENANT_ID}`)
    .replace(/:\w+(?:\{([^}]+)\})?/g, (param, pattern?: string) => {
      if (pattern === undefined) return SWEEP_SAMPLE;
      const sample = SWEEP_PATTERN_SAMPLES[pattern];
      if (sample !== undefined) return sample;
      unsampled.push(param);
      return param;
    })
    .replace(/\*$/, SWEEP_SAMPLE);
  return { url, unsampled };
}

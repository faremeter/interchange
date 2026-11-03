import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { type } from "arktype";

import { createApp, type GetSession } from "@intx/hub-api";
import {
  createEventCollectorRegistry,
  createSidecarRouter,
  type SessionService,
  type SidecarAuthIdentity,
} from "@intx/hub-sessions";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedPrincipal, seedWorkflowRun } from "@intx/test-harness/seed";

// Pins request-body validation at the grant route layer, against a real DB.
//
// Exactly one target: CreateGrant carries a `.narrow` (unit-tested in
// packages/types) requiring a grant to target exactly one of a role or a
// principal, mirroring the grant_target_exactly_one DB CHECK. This proves the
// validator is actually MOUNTED on the route: a both/neither body is a 400
// here, not a database 500 from tripping the CHECK past an unguarded insert.
//
// Conditions: create and update check `conditions` against the hub condition
// registry (unit-tested in packages/authz), so a grant that authorization
// could not evaluate is a 400 naming the problem, and is never stored.

function mockGetSession(userId: string): GetSession {
  const now = new Date("2026-01-01");
  return async () => ({
    user: {
      id: userId,
      email: "creator@example.com",
      emailVerified: true,
      name: "Creator",
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
  });
}

function bindingFor(sidecarId: string): SidecarAuthIdentity {
  return {
    kind: "allocated",
    sidecarId,
    allocationId: "allocation-test",
    tenantId: "tenant-test",
    anchorRunId: "run-test",
    workflowRunAddress: "workflow-test@example.test",
    generation: 1,
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
  "grant routes validate the request body (real DB)",
  () => {
    let h: TestDb;

    beforeAll(async () => {
      h = await createTestDb();
    });

    afterAll(async () => {
      await h.close();
    });

    beforeEach(async () => {
      await h.reset();
    });

    function makeApp() {
      return createApp({
        getSession: mockGetSession("usr_creator"),
        authHandler: () => new Response("", { status: 404 }),
        db: h.db,
        sidecarRouter: createSidecarRouter({
          withExecutableWorkflowRun: async (_target, send) => send(),
          authenticateSidecar: async ({ sidecarId }) => ({ sidecarId }),
          resolveSidecarBindings: async (sidecarId) => [bindingFor(sidecarId)],
          validateSidecarIdentity: async () => true,
        }),
        sessionService: mockSessionService(),
        eventCollectors: createEventCollectorRegistry({ db: h.db }),
        assetService: null,
        repoStore: null,
        maxTarballBytes: 10_000_000,
      });
    }

    // Creating a tenant assigns the creator the `owner` system role, which
    // holds a `*`/`*` allow grant. That authorizes requireGrant("grant:*",
    // "create") on the POST grants route, so the same mock session is an
    // authorized grant-creator with no manual principal/grant seeding. The
    // 201 assertion fails loudly at the bootstrap line if that seeding path
    // ever changes, rather than surfacing later as a confusing 403.
    //
    // The returned `roleId` is the `member` role -- an arbitrary real role to
    // hand the exactly-one 201 case as a grant TARGET (it satisfies the
    // roleId foreign key). It is unrelated to the owner role that authorizes
    // the CALLER above.
    async function bootstrapTenant(
      app: ReturnType<typeof makeApp>,
      slug: string,
    ): Promise<{ tenantId: string; roleId: string }> {
      const res = await app.request("/api/tenants", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name: "Acme", slug }),
      });
      expect(res.status).toBe(201);
      const { id: tenantId } = type({ id: "string" }).assert(await res.json());

      const roles = await h.db.query.role.findMany({
        where: (r, { eq }) => eq(r.tenantId, tenantId),
      });
      const roleId = new Map(roles.map((r) => [r.name, r.id])).get("member");
      if (roleId === undefined) {
        throw new Error(
          "bootstrapped tenant is missing the member system role",
        );
      }
      return { tenantId, roleId };
    }

    function postGrant(
      app: ReturnType<typeof makeApp>,
      tenantId: string,
      body: Record<string, unknown>,
    ) {
      return app.request(`/api/tenants/${tenantId}/grants`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      });
    }

    function patchGrant(
      app: ReturnType<typeof makeApp>,
      tenantId: string,
      grantId: string,
      body: Record<string, unknown>,
    ) {
      return app.request(`/api/tenants/${tenantId}/grants/${grantId}`, {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      });
    }

    const ErrorBody = type({ error: { code: "string", message: "string" } });

    async function storedConditions(grantId: string) {
      const row = await h.db.query.grant.findFirst({
        where: (g, { eq }) => eq(g.id, grantId),
      });
      if (row === undefined) throw new Error(`grant ${grantId} not stored`);
      return row.conditions;
    }

    const WINDOW = { after: "09:00", before: "17:00", timezone: "UTC" };

    test("rejects a body with both targets as a 400", async () => {
      const app = makeApp();
      const { tenantId, roleId } = await bootstrapTenant(app, "grant-both");

      const res = await postGrant(app, tenantId, {
        roleId,
        principalId: "prn_x",
        resource: "agent:*",
        action: "read",
        effect: "allow",
        origin: "creator",
      });

      expect(res.status).toBe(400);
      // Pin that the 400 is the exactly-one-target narrow, not some other
      // validation failure. Every other field is individually valid (the 201
      // case proves it), so today only the narrow can reject this body -- but
      // if a future field tightening (e.g. a principalId format) shifted the
      // 400 to a different cause, a status-only assertion would keep passing
      // while silently ceasing to test the two-target rule. Scanning the
      // serialized body for the narrow's own marker keeps the guard honest
      // without coupling to the validator's error-envelope shape.
      expect(JSON.stringify(await res.json())).toContain("exactly one target");
    });

    test("rejects a body with neither target as a 400", async () => {
      const app = makeApp();
      const { tenantId } = await bootstrapTenant(app, "grant-neither");

      const res = await postGrant(app, tenantId, {
        resource: "agent:*",
        action: "read",
        effect: "allow",
        origin: "creator",
      });

      expect(res.status).toBe(400);
      // As in the both-target case: confirm the narrow is what rejected this,
      // not an unrelated validation error.
      expect(JSON.stringify(await res.json())).toContain("exactly one target");
    });

    test("accepts a body with exactly one target as a 201", async () => {
      const app = makeApp();
      const { tenantId, roleId } = await bootstrapTenant(app, "grant-valid");

      const res = await postGrant(app, tenantId, {
        roleId,
        resource: "agent:*",
        action: "read",
        effect: "allow",
        origin: "creator",
      });

      expect(res.status).toBe(201);
      const created = type({ id: "string" }).assert(await res.json());
      expect(created.id).toStartWith("grt_");

      // Confirm the grant actually persisted (not just that a 201 came back)
      // and that it landed with the single role target the body asked for.
      const persisted = await h.db.query.grant.findFirst({
        where: (g, { eq }) => eq(g.id, created.id),
      });
      expect(persisted).toMatchObject({
        roleId,
        principalId: null,
        resource: "agent:*",
        action: "read",
      });
    });
    describe("conditions", () => {
      async function setup(slug: string) {
        const app = makeApp();
        const { tenantId, roleId } = await bootstrapTenant(app, slug);
        const grantBody = (conditions: unknown) => ({
          roleId,
          resource: "agent:*",
          action: "read",
          effect: "allow",
          origin: "creator",
          conditions,
        });
        return { app, tenantId, grantBody };
      }

      async function createWindowGrant(slug: string) {
        const ctx = await setup(slug);
        const res = await postGrant(
          ctx.app,
          ctx.tenantId,
          ctx.grantBody({ time_window: WINDOW }),
        );
        expect(res.status).toBe(201);
        const { id } = type({ id: "string" }).assert(await res.json());
        return { ...ctx, grantId: id };
      }

      async function grantCount() {
        return (await h.db.query.grant.findMany()).length;
      }

      test("POST rejects a time_window missing its timezone", async () => {
        const { app, tenantId, grantBody } = await setup("cond-no-tz");
        const before = await grantCount();

        const res = await postGrant(
          app,
          tenantId,
          grantBody({ time_window: { after: "09:00", before: "17:00" } }),
        );

        expect(res.status).toBe(400);
        expect(ErrorBody.assert(await res.json()).error).toEqual({
          code: "bad_request",
          message:
            'invalid conditions: ["time_window: timezone must be a string (was missing)"]',
        });
        expect(await grantCount()).toBe(before);
      });

      test("POST rejects a time_window with the wrong keys", async () => {
        const { app, tenantId, grantBody } = await setup("cond-keys");

        const res = await postGrant(
          app,
          tenantId,
          grantBody({ time_window: { start: "09:00", end: "17:00" } }),
        );

        expect(res.status).toBe(400);
        const { message } = ErrorBody.assert(await res.json()).error;
        expect(message).toContain("after must be a string (was missing)");
        expect(message).toContain("start must be removed");
      });

      test("POST rejects an unregistered condition", async () => {
        const { app, tenantId, grantBody } = await setup("cond-unknown");

        const res = await postGrant(
          app,
          tenantId,
          grantBody({ max_spend_per_day: 100 }),
        );

        expect(res.status).toBe(400);
        expect(ErrorBody.assert(await res.json()).error.message).toBe(
          'invalid conditions: ["Unknown condition: \\"max_spend_per_day\\""]',
        );
      });

      test("POST stores a valid time_window", async () => {
        const { grantId } = await createWindowGrant("cond-valid");
        expect(await storedConditions(grantId)).toEqual({
          time_window: WINDOW,
        });
      });

      test("PATCH rejects malformed conditions and keeps the stored ones", async () => {
        const { app, tenantId, grantId } =
          await createWindowGrant("cond-patch-bad");

        const res = await patchGrant(app, tenantId, grantId, {
          conditions: { time_window: { ...WINDOW, timezone: "Mars/Olympus" } },
        });

        expect(res.status).toBe(400);
        expect(ErrorBody.assert(await res.json()).error.message).toContain(
          "time_window: timezone must be a time zone name or UTC offset",
        );
        expect(await storedConditions(grantId)).toEqual({
          time_window: WINDOW,
        });
      });

      test("PATCH stores valid conditions", async () => {
        const { app, tenantId, grantId } =
          await createWindowGrant("cond-patch-ok");
        const next = { ...WINDOW, after: "08:00" };

        const res = await patchGrant(app, tenantId, grantId, {
          conditions: { time_window: next },
        });

        expect(res.status).toBe(200);
        expect(await storedConditions(grantId)).toEqual({ time_window: next });
      });

      test("PATCH with null conditions clears them", async () => {
        const { app, tenantId, grantId } =
          await createWindowGrant("cond-patch-null");

        const res = await patchGrant(app, tenantId, grantId, {
          conditions: null,
        });

        expect(res.status).toBe(200);
        expect(await storedConditions(grantId)).toBeNull();
      });

      describe("on a principal without a run", () => {
        // A `tool` condition on a non-credential resource is evaluable on the
        // hub, and only a run's own principal also faces the run's narrower
        // registries. These targets take the hub check alone.
        async function postToolGrant(slug: string, kind: "user" | "workflow") {
          const app = makeApp();
          const { tenantId } = await bootstrapTenant(app, slug);
          const principalId = `prn_${slug}`;
          await seedPrincipal(h.db, {
            id: principalId,
            tenantId,
            kind,
            refId: `ref_${slug}`,
          });
          return postGrant(app, tenantId, {
            principalId,
            resource: "tool:search",
            action: "invoke",
            effect: "allow",
            origin: "creator",
            conditions: { tool: "tool:pkg" },
          });
        }

        test("POST accepts a tool condition on a user principal", async () => {
          const res = await postToolGrant("user-tool", "user");
          expect(res.status).toBe(201);
        });

        test("POST accepts a tool condition on a workflow principal with no run", async () => {
          // A workflow principal can name a definition rather than a run.
          // The run check keys on a run that names the principal, not on
          // the principal's kind.
          const res = await postToolGrant("definition-tool", "workflow");
          expect(res.status).toBe(201);
        });
      });

      describe("on a workflow run's principal", () => {
        const TOOL = { tool: "tool:pkg" };

        async function setupRun(slug: string) {
          const app = makeApp();
          const { tenantId, roleId } = await bootstrapTenant(app, slug);
          const principalId = `prn_run_${slug}`;
          await seedPrincipal(h.db, {
            id: principalId,
            tenantId,
            kind: "workflow",
            refId: `run_${slug}`,
          });
          await seedWorkflowRun(h.db, {
            id: `run_${slug}`,
            tenantId,
            principalId,
          });
          const runGrant = (
            resource: string,
            action: string,
            conditions: unknown,
          ) => ({
            principalId,
            resource,
            action,
            effect: "allow",
            origin: "creator",
            conditions,
          });
          return { app, tenantId, roleId, runGrant };
        }

        test("POST refuses a tool condition on a non-credential resource", async () => {
          const { app, tenantId, runGrant } = await setupRun("run-tool");

          const res = await postGrant(
            app,
            tenantId,
            runGrant("tool:search", "invoke", TOOL),
          );

          expect(res.status).toBe(400);
          expect(ErrorBody.assert(await res.json()).error.message).toContain(
            "a workflow run cannot evaluate this grant",
          );
        });

        test("POST accepts a tool condition on credential use", async () => {
          const { app, tenantId, runGrant } = await setupRun("run-cred");

          const res = await postGrant(
            app,
            tenantId,
            runGrant("credential:crd_1", "use", TOOL),
          );

          expect(res.status).toBe(201);
        });

        test("POST refuses a time window on credential use", async () => {
          const { app, tenantId, runGrant } = await setupRun("run-cred-tw");

          const res = await postGrant(
            app,
            tenantId,
            runGrant("credential:crd_1", "use", { time_window: WINDOW }),
          );

          expect(res.status).toBe(400);
          expect(ErrorBody.assert(await res.json()).error.message).toContain(
            "credential use cannot evaluate this grant",
          );
        });

        test("POST accepts a tool condition on a role grant", async () => {
          const { app, tenantId, roleId } = await setupRun("run-role");

          const res = await postGrant(app, tenantId, {
            roleId,
            resource: "tool:search",
            action: "invoke",
            effect: "allow",
            origin: "creator",
            conditions: TOOL,
          });

          expect(res.status).toBe(201);
        });

        test("PATCH refuses a tool condition and keeps the stored one", async () => {
          const { app, tenantId, runGrant } = await setupRun("run-patch");
          const created = await postGrant(
            app,
            tenantId,
            runGrant("tool:search", "invoke", { time_window: WINDOW }),
          );
          expect(created.status).toBe(201);
          const { id } = type({ id: "string" }).assert(await created.json());

          const res = await patchGrant(app, tenantId, id, {
            conditions: TOOL,
          });

          expect(res.status).toBe(400);
          expect(await storedConditions(id)).toEqual({ time_window: WINDOW });
        });

        test("PATCH of a missing grant's conditions is a 404", async () => {
          const { app, tenantId } = await setupRun("run-missing");

          const res = await patchGrant(app, tenantId, "grt_missing", {
            conditions: TOOL,
          });

          expect(res.status).toBe(404);
        });
      });

      test("PATCH without conditions leaves them unchanged", async () => {
        const { app, tenantId, grantId } =
          await createWindowGrant("cond-patch-omit");

        const res = await patchGrant(app, tenantId, grantId, {
          effect: "deny",
        });

        expect(res.status).toBe(200);
        expect(await storedConditions(grantId)).toEqual({
          time_window: WINDOW,
        });
      });
    });
  },
);

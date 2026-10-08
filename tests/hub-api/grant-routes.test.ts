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
  type SidecarAuthenticator,
} from "@intx/hub-sessions";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";

// Pins the exactly-one-target invariant at the POST /api/grants route
// layer: CreateGrant's `.narrow` requires a grant to target exactly one of
// a role or a principal (mirroring the grant_target_exactly_one DB CHECK).
// A both/neither body is a 400 here, proving the validator is mounted on
// the route rather than tripping the CHECK as a database 500.

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

const acceptAnySidecar: SidecarAuthenticator = async ({ sidecarId }) => ({
  kind: "allocated",
  sidecarId,
  allocationId: "allocation-test",
  tenantId: "tenant-test",
  anchorRunId: "run-test",
  workflowRunAddress: "workflow-test@example.test",
  generation: 1,
});

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
  "POST /api/grants enforces exactly one target (real DB)",
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
          authenticateSidecar: acceptAnySidecar,
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
    // holds a `*`/`*` allow grant, authorizing requireGrant("grant:*",
    // "create") with no manual seeding. The returned `roleId` is the
    // `member` role, an arbitrary real role to hand the 201 case as a grant
    // target (it satisfies the roleId FK); it is unrelated to the owner
    // role that authorizes the caller.
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
      // Pin the 400 to the exactly-one-target narrow, not some other
      // validation failure: every other field is valid (the 201 case proves
      // it), so scanning for the narrow's own marker keeps the guard honest
      // if a future field tightening shifts the cause.
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
      // As in the both-target case: confirm the narrow rejected this, not
      // an unrelated validation error.
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

      // Confirm the grant persisted with the single role target requested.
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
  },
);

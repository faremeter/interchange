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

// Pins the exactly-one-target invariant at the POST /api/grants route layer.
// CreateGrant carries a `.narrow` (unit-tested in packages/types) requiring a
// grant to target exactly one of a role or a principal, mirroring the
// grant_target_exactly_one DB CHECK. This proves the validator is actually
// MOUNTED on the route: a both/neither body is a 400 here, not a database 500
// from tripping the CHECK past an unguarded insert. Nothing else in the suite
// pins that the validator converts a malformed body into a 400 on this route.

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
  kind: "shared",
  sidecarId,
});

function mockSessionService(): SessionService {
  const notImpl = (name: string) => (): never => {
    throw new Error(`mock: sessionService.${name} not implemented`);
  };
  return {
    stageWorkflowStep: notImpl("stageWorkflowStep"),
    deployInstanceAtHead: notImpl("deployInstanceAtHead"),
    deployWorkflowDefinition: notImpl("deployWorkflowDefinition"),
    deployWorkflowFromSource: notImpl("deployWorkflowFromSource"),
    deploySingleStepAtHead: notImpl("deploySingleStepAtHead"),
    sendUserMessage: notImpl("sendUserMessage"),
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
          authenticateSidecar: acceptAnySidecar,
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
  },
);

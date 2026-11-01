import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { type } from "arktype";
import { eq } from "drizzle-orm";

import { createApp, type GetSession } from "@intx/hub-api";
import {
  createEventCollectorRegistry,
  createSidecarRouter,
  type SessionService,
  type SidecarAuthIdentity,
} from "@intx/hub-sessions";
import {
  grant,
  principal,
  principalRole,
  role,
  workflowDefinition,
  workflowRunLaunchSpec,
} from "@intx/db/schema";
import type { GrantRequirement } from "@intx/types";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedGrant,
  seedPrincipal,
  seedWorkflowRun,
  seedWorkflowRunLaunchSpec,
} from "@intx/test-harness/seed";

// The grant, role and principal routes re-derive the running runs that
// delegate from the principals they change, and send those runs their
// grants after the change commits.

function mockGetSession(userId: string): GetSession {
  const now = new Date("2026-01-01");
  return async () => ({
    user: {
      id: userId,
      email: "owner@example.com",
      emailVerified: true,
      name: "Owner",
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

const CREATOR = "prn_creator";
const DEPLOYER = "prn_deployer";
const ANCHOR = "run_live";
const ADDRESS = "run_live@tenant.example";
const RUN_PRINCIPAL = "prn_run_live";
const VAULT = { resource: "secret:vault", action: "use" } as const;

describe.skipIf(!harnessDbEnvAvailable())(
  "delegator grant routes re-derive running runs (real DB)",
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

    // A sidecar router whose grants sends record the address when the send
    // starts and the run's rows as the send reads them, so a test sees what
    // reached the run. The route starts its sends before it responds.
    function makeApp() {
      const started: { address: string; resources: Promise<string[]> }[] = [];
      const sends = async () =>
        Promise.all(
          started.map(async ({ address, resources }) => ({
            address,
            resources: await resources,
          })),
        );
      const router = createSidecarRouter({
        withExecutableWorkflowRun: async (_target, send) => send(),
        authenticateSidecar: async ({ sidecarId }) => ({ sidecarId }),
        resolveSidecarBindings: async (sidecarId) => [bindingFor(sidecarId)],
        validateSidecarIdentity: async () => true,
      });
      const app = createApp({
        getSession: mockGetSession("usr_owner"),
        authHandler: () => new Response("", { status: 404 }),
        db: h.db,
        sidecarRouter: {
          ...router,
          sendRunGrants: async (address) => {
            const resources = runResources();
            started.push({ address, resources });
            await resources;
            return true;
          },
        },
        sessionService: mockSessionService(),
        eventCollectors: createEventCollectorRegistry({ db: h.db }),
        assetService: null,
        repoStore: null,
        maxTarballBytes: 10_000_000,
      });
      return { app, sends };
    }

    async function runResources(): Promise<string[]> {
      const rows = await h.db
        .select()
        .from(grant)
        .where(eq(grant.principalId, RUN_PRINCIPAL));
      return rows.map((r) => `${r.origin} ${r.resource} ${r.effect}`).sort();
    }

    // Create the tenant through the route, so the caller is its owner, then
    // seed a live deployment whose run holds the rows its requirements
    // produced at first trigger. The definition names no creator, so the
    // asset's creator can be deleted.
    async function setup(requirements: GrantRequirement[]) {
      const { app, sends } = makeApp();
      const res = await app.request("/api/tenants", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name: "Acme", slug: "acme" }),
      });
      expect(res.status).toBe(201);
      const { id: tenantId } = type({ id: "string" }).assert(await res.json());

      for (const id of [CREATOR, DEPLOYER]) {
        await seedPrincipal(h.db, {
          id,
          tenantId,
          kind: "user",
          refId: `${id}-user`,
        });
      }
      await seedAsset(h.db, {
        id: "ast_live",
        tenantId,
        kind: "workflow",
        name: "ast_live",
        creatorPrincipalId: CREATOR,
      });
      await h.db.insert(workflowDefinition).values({
        id: "wfd_live",
        tenantId,
        name: "wfd_live",
        assetId: "ast_live",
      });
      await h.db.insert(principal).values({
        id: RUN_PRINCIPAL,
        tenantId,
        kind: "workflow",
        refId: ANCHOR,
        status: "active",
      });
      await seedWorkflowRun(h.db, {
        id: ANCHOR,
        tenantId,
        anchorRunId: ANCHOR,
        definitionId: "wfd_live",
        principalId: RUN_PRINCIPAL,
        address: ADDRESS,
      });
      await seedWorkflowRunLaunchSpec(h.db, {
        anchorRunId: ANCHOR,
        sourceAuthorityPrincipalId: DEPLOYER,
        grantSnapshot: { perStep: [], grantRequirements: requirements },
      });
      for (const req of requirements) {
        await seedGrant(h.db, {
          id: `grt_run_${req.source}`,
          tenantId,
          principalId: RUN_PRINCIPAL,
          ...VAULT,
          origin: req.source,
          expiresAt:
            req.source === "invoker" ? new Date(Date.now() + 60_000) : null,
        });
      }
      return { app, sends, tenantId };
    }

    const CREATOR_VAULT: GrantRequirement = { ...VAULT, source: "creator" };

    function request(
      app: ReturnType<typeof makeApp>["app"],
      method: string,
      path: string,
      body?: Record<string, unknown>,
    ) {
      return app.request(path, {
        method,
        headers: { "content-type": "application/json" },
        ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
      });
    }

    test("deleting the creator's grant removes the run's row and sends the committed rows", async () => {
      const { app, sends, tenantId } = await setup([CREATOR_VAULT]);
      await seedGrant(h.db, {
        id: "grt_creator_vault",
        tenantId,
        principalId: CREATOR,
        ...VAULT,
      });

      const res = await request(
        app,
        "DELETE",
        `/api/tenants/${tenantId}/grants/grt_creator_vault`,
      );

      expect(res.status).toBe(204);
      expect(await runResources()).toEqual([]);
      expect(await sends()).toEqual([{ address: ADDRESS, resources: [] }]);
    });

    test("patching the creator's grant to deny removes the run's row", async () => {
      const { app, sends, tenantId } = await setup([CREATOR_VAULT]);
      await seedGrant(h.db, {
        id: "grt_creator_vault",
        tenantId,
        principalId: CREATOR,
        ...VAULT,
      });

      const res = await request(
        app,
        "PATCH",
        `/api/tenants/${tenantId}/grants/grt_creator_vault`,
        { effect: "deny" },
      );

      expect(res.status).toBe(200);
      expect(await runResources()).toEqual([]);
      expect((await sends()).map((s) => s.address)).toEqual([ADDRESS]);
    });

    test("granting the creator the authority again adds the run's row back", async () => {
      const { app, sends, tenantId } = await setup([CREATOR_VAULT]);
      await h.db.delete(grant).where(eq(grant.principalId, RUN_PRINCIPAL));

      const res = await request(
        app,
        "POST",
        `/api/tenants/${tenantId}/grants`,
        {
          principalId: CREATOR,
          ...VAULT,
          effect: "allow",
          origin: "creator",
        },
      );

      expect(res.status).toBe(201);
      expect(await runResources()).toEqual(["creator secret:vault allow"]);
      expect(await sends()).toEqual([
        { address: ADDRESS, resources: ["creator secret:vault allow"] },
      ]);
    });

    test("a role change removes the run's row", async () => {
      const { app, sends, tenantId } = await setup([CREATOR_VAULT]);
      await h.db
        .insert(role)
        .values({ id: "rol_vault", tenantId, name: "vault" });
      await h.db
        .insert(principalRole)
        .values({ principalId: CREATOR, roleId: "rol_vault" });
      await seedGrant(h.db, {
        id: "grt_role_vault",
        tenantId,
        roleId: "rol_vault",
        ...VAULT,
        origin: "role",
      });

      const res = await request(
        app,
        "DELETE",
        `/api/tenants/${tenantId}/grants/grt_role_vault`,
      );

      expect(res.status).toBe(204);
      expect(await runResources()).toEqual([]);
      expect((await sends()).map((s) => s.address)).toEqual([ADDRESS]);
    });

    test("removing the creator's role removes the run's row", async () => {
      const { app, sends, tenantId } = await setup([CREATOR_VAULT]);
      await h.db
        .insert(role)
        .values({ id: "rol_vault", tenantId, name: "vault" });
      await h.db
        .insert(principalRole)
        .values({ principalId: CREATOR, roleId: "rol_vault" });
      await seedGrant(h.db, {
        id: "grt_role_vault",
        tenantId,
        roleId: "rol_vault",
        ...VAULT,
        origin: "role",
      });

      const res = await request(
        app,
        "DELETE",
        `/api/tenants/${tenantId}/principals/${CREATOR}/roles/rol_vault`,
      );

      expect(res.status).toBe(204);
      expect(await runResources()).toEqual([]);
      expect((await sends()).map((s) => s.address)).toEqual([ADDRESS]);
    });

    test("deactivating the deployer removes the run's invoker row", async () => {
      const { app, sends, tenantId } = await setup([
        { ...VAULT, source: "invoker" },
      ]);
      await seedGrant(h.db, {
        id: "grt_deployer_vault",
        tenantId,
        principalId: DEPLOYER,
        ...VAULT,
      });

      const res = await request(
        app,
        "PATCH",
        `/api/tenants/${tenantId}/principals/${DEPLOYER}`,
        { status: "deactivated" },
      );

      expect(res.status).toBe(200);
      expect(await runResources()).toEqual([]);
      expect((await sends()).map((s) => s.address)).toEqual([ADDRESS]);
    });

    test("deleting the asset's creator removes the run's row", async () => {
      const { app, sends, tenantId } = await setup([CREATOR_VAULT]);
      await seedGrant(h.db, {
        id: "grt_creator_vault",
        tenantId,
        principalId: CREATOR,
        ...VAULT,
      });

      const res = await request(
        app,
        "DELETE",
        `/api/tenants/${tenantId}/principals/${CREATOR}`,
      );

      expect(res.status).toBe(204);
      expect(await runResources()).toEqual([]);
      expect((await sends()).map((s) => s.address)).toEqual([ADDRESS]);
    });

    test("deleting a deployer is refused as a conflict and changes nothing", async () => {
      const { app, sends, tenantId } = await setup([CREATOR_VAULT]);

      const res = await request(
        app,
        "DELETE",
        `/api/tenants/${tenantId}/principals/${DEPLOYER}`,
      );

      expect(res.status).toBe(409);
      expect(await runResources()).toEqual(["creator secret:vault allow"]);
      expect(await sends()).toEqual([]);
    });

    test("a change whose re-derivation fails is rolled back and sends nothing", async () => {
      const { app, sends, tenantId } = await setup([CREATOR_VAULT]);
      await seedGrant(h.db, {
        id: "grt_creator_vault",
        tenantId,
        principalId: CREATOR,
        ...VAULT,
      });
      // A run that holds rows and has no snapshot cannot be re-derived.
      await h.db
        .update(workflowRunLaunchSpec)
        .set({ grantSnapshot: null })
        .where(eq(workflowRunLaunchSpec.anchorRunId, ANCHOR));

      const res = await request(
        app,
        "DELETE",
        `/api/tenants/${tenantId}/grants/grt_creator_vault`,
      );

      expect(res.status).toBe(500);
      const kept = await h.db
        .select({ id: grant.id })
        .from(grant)
        .where(eq(grant.id, "grt_creator_vault"));
      expect(kept).toHaveLength(1);
      expect(await runResources()).toEqual(["creator secret:vault allow"]);
      expect(await sends()).toEqual([]);
    });

    test("a change to an unknown grant sends nothing", async () => {
      const { app, sends, tenantId } = await setup([CREATOR_VAULT]);

      const patched = await request(
        app,
        "PATCH",
        `/api/tenants/${tenantId}/grants/grt_missing`,
        { effect: "deny" },
      );
      const deleted = await request(
        app,
        "DELETE",
        `/api/tenants/${tenantId}/grants/grt_missing`,
      );

      expect([patched.status, deleted.status]).toEqual([404, 404]);
      expect(await sends()).toEqual([]);
    });

    test("a change to the run's own rows sends the run its grants", async () => {
      const { app, sends, tenantId } = await setup([CREATOR_VAULT]);

      const res = await request(
        app,
        "DELETE",
        `/api/tenants/${tenantId}/grants/grt_run_creator`,
      );

      expect(res.status).toBe(204);
      expect(await sends()).toEqual([{ address: ADDRESS, resources: [] }]);
    });

    test("assigning the creator a role that holds the authority adds the run's row back", async () => {
      const { app, sends, tenantId } = await setup([CREATOR_VAULT]);
      await h.db.delete(grant).where(eq(grant.principalId, RUN_PRINCIPAL));
      await h.db
        .insert(role)
        .values({ id: "rol_vault", tenantId, name: "vault" });
      await seedGrant(h.db, {
        id: "grt_role_vault",
        tenantId,
        roleId: "rol_vault",
        ...VAULT,
        origin: "role",
      });
      const res = await request(
        app,
        "POST",
        `/api/tenants/${tenantId}/principals/${CREATOR}/roles/rol_vault`,
      );
      expect(res.status).toBe(204);
      expect(await runResources()).toEqual(["creator secret:vault allow"]);
      expect((await sends()).map((s) => s.address)).toEqual([ADDRESS]);
      const again = await request(
        app,
        "POST",
        `/api/tenants/${tenantId}/principals/${CREATOR}/roles/rol_vault`,
      );
      expect(again.status).toBe(204);
      expect((await sends()).length).toBe(1);
    });

    test("granting a role the authority re-derives its members' runs", async () => {
      const { app, sends, tenantId } = await setup([CREATOR_VAULT]);
      await h.db.delete(grant).where(eq(grant.principalId, RUN_PRINCIPAL));
      await h.db
        .insert(role)
        .values({ id: "rol_vault", tenantId, name: "vault" });
      await h.db
        .insert(principalRole)
        .values({ principalId: CREATOR, roleId: "rol_vault" });
      const res = await request(
        app,
        "POST",
        `/api/tenants/${tenantId}/grants`,
        { roleId: "rol_vault", ...VAULT, effect: "allow", origin: "role" },
      );
      expect(res.status).toBe(201);
      expect(await runResources()).toEqual(["creator secret:vault allow"]);
      expect((await sends()).map((s) => s.address)).toEqual([ADDRESS]);
    });

    test("a role with members is not deleted, and one without is", async () => {
      const { app, sends, tenantId } = await setup([CREATOR_VAULT]);
      await h.db
        .insert(role)
        .values({ id: "rol_vault", tenantId, name: "vault" });
      await h.db
        .insert(principalRole)
        .values({ principalId: CREATOR, roleId: "rol_vault" });
      const busy = await request(
        app,
        "DELETE",
        `/api/tenants/${tenantId}/roles/rol_vault`,
      );
      expect(busy.status).toBe(400);
      await h.db
        .delete(principalRole)
        .where(eq(principalRole.roleId, "rol_vault"));
      const ok = await request(
        app,
        "DELETE",
        `/api/tenants/${tenantId}/roles/rol_vault`,
      );
      expect(ok.status).toBe(204);
      expect(
        (await h.db.select().from(role).where(eq(role.id, "rol_vault"))).length,
      ).toBe(0);
      expect(await sends()).toEqual([]);
    });

    test("reactivating the deployer adds the invoker row back", async () => {
      const { app, tenantId } = await setup([{ ...VAULT, source: "invoker" }]);
      await seedGrant(h.db, {
        id: "grt_deployer_vault",
        tenantId,
        principalId: DEPLOYER,
        ...VAULT,
      });
      const off = await request(
        app,
        "PATCH",
        `/api/tenants/${tenantId}/principals/${DEPLOYER}`,
        { status: "deactivated" },
      );
      expect(off.status).toBe(200);
      expect(await runResources()).toEqual([]);
      const on = await request(
        app,
        "PATCH",
        `/api/tenants/${tenantId}/principals/${DEPLOYER}`,
        { status: "active" },
      );
      expect(on.status).toBe(200);
      expect(await runResources()).toEqual(["invoker secret:vault allow"]);
    });

    test("a principal or role change that finds nothing sends nothing", async () => {
      const { app, sends, tenantId } = await setup([CREATOR_VAULT]);
      const p = await request(
        app,
        "PATCH",
        `/api/tenants/${tenantId}/principals/prn_missing`,
        { status: "deactivated" },
      );
      const d = await request(
        app,
        "DELETE",
        `/api/tenants/${tenantId}/principals/prn_missing`,
      );
      await h.db
        .insert(role)
        .values({ id: "rol_vault", tenantId, name: "vault" });
      const u = await request(
        app,
        "DELETE",
        `/api/tenants/${tenantId}/principals/${CREATOR}/roles/rol_vault`,
      );
      expect([p.status, d.status, u.status]).toEqual([404, 404, 404]);
      expect(await sends()).toEqual([]);
      expect(await runResources()).toEqual(["creator secret:vault allow"]);
    });
  },
);

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
import { createApp, type GetSession } from "@intx/hub-api";
import {
  createEventCollectorRegistry,
  createSidecarRouter,
  ensureWorkflowDefinitionForAsset,
  type SessionService,
  type SidecarAuthenticator,
} from "@intx/hub-sessions";
import { tenant, workflowDefinition } from "@intx/db/schema";
import type { GrantRule, GrantStore } from "@intx/types/authz";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedPrincipal, seedTenants } from "@intx/test-harness/seed";

const ROOT_TENANT_ID = "tnt_lifecycle_root";
const TENANT_ID = "tnt_lifecycle_child";
const OTHER_TENANT_ID = "tnt_lifecycle_other";
const ACTOR_PRINCIPAL_ID = "prn_lifecycle_actor";
const ACTOR_USER_ID = "usr_lifecycle_actor";
const DEFINITION_ID = "wfd_lifecycle";
const OTHER_DEFINITION_ID = "wfd_lifecycle_other";

function mockGetSession(userId: string): GetSession {
  const now = new Date("2026-01-01");
  return async () => ({
    user: {
      id: userId,
      email: "actor@example.com",
      emailVerified: true,
      name: "Actor",
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
  tenantId: TENANT_ID,
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

function allow(resource: string, action: string): GrantRule {
  return {
    id: `grant-${resource}-${action}`,
    resource,
    action,
    effect: "allow",
    origin: "system",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: ACTOR_PRINCIPAL_ID,
  };
}

describe.skipIf(!harnessDbEnvAvailable())(
  "workflow lifecycle policy write routes (real DB)",
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
      await seedTenants(h.db, [
        { id: ROOT_TENANT_ID },
        { id: TENANT_ID, parentId: ROOT_TENANT_ID },
        { id: OTHER_TENANT_ID },
      ]);
      await h.db
        .update(tenant)
        .set({ config: { lifecycle: { maxLifetime: "2h" } } })
        .where(eq(tenant.id, ROOT_TENANT_ID));
      await seedPrincipal(h.db, {
        id: ACTOR_PRINCIPAL_ID,
        tenantId: TENANT_ID,
        kind: "user",
        refId: ACTOR_USER_ID,
      });
      await h.db.insert(workflowDefinition).values([
        { id: DEFINITION_ID, tenantId: TENANT_ID, name: "lifecycle-def" },
        {
          id: OTHER_DEFINITION_ID,
          tenantId: OTHER_TENANT_ID,
          name: "other-def",
        },
      ]);
    });

    function makeApp(grantStore: GrantStore) {
      return createApp({
        getSession: mockGetSession(ACTOR_USER_ID),
        authHandler: () => new Response("", { status: 404 }),
        db: h.db,
        grantStore,
        sidecarRouter: createSidecarRouter({
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

    function updateTenant(grants: GrantRule[], body: unknown) {
      return makeApp(createInMemoryGrantStore(grants)).request(
        `/api/tenants/${TENANT_ID}`,
        {
          method: "PATCH",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(body),
        },
      );
    }

    function patchTenant(grants: GrantRule[], config: unknown) {
      return updateTenant(grants, { config });
    }

    function patchDefinitionLifecycle(
      definitionId: string,
      body: unknown,
      grantStore = createInMemoryGrantStore([
        allow("workflow-definition:*", "manage"),
      ]),
    ) {
      return makeApp(grantStore).request(
        `/api/tenants/${TENANT_ID}/workflows/definitions/${definitionId}/lifecycle`,
        {
          method: "PATCH",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(body),
        },
      );
    }

    async function savedTenantConfig(): Promise<unknown> {
      const row = await h.db.query.tenant.findFirst({
        where: eq(tenant.id, TENANT_ID),
        columns: { config: true },
      });
      return row?.config;
    }

    async function savedDefinitionPolicy(): Promise<unknown> {
      const row = await h.db.query.workflowDefinition.findFirst({
        where: eq(workflowDefinition.id, DEFINITION_ID),
        columns: { lifecyclePolicy: true },
      });
      return row?.lifecyclePolicy;
    }

    test("a manage grant on the tenant allows tightening an inherited limit", async () => {
      const config = { lifecycle: { maxLifetime: "1h" } };
      const res = await patchTenant(
        [allow(`tenant:${TENANT_ID}`, "manage")],
        config,
      );
      expect(res.status).toBe(200);
      expect(await savedTenantConfig()).toEqual(config);
    });

    test("a tenant cannot extend a limit inherited from its parent", async () => {
      const res = await patchTenant([allow(`tenant:${TENANT_ID}`, "manage")], {
        lifecycle: { maxLifetime: "3h" },
      });
      expect(res.status).toBe(400);
      expect(await res.json()).toEqual({
        error: {
          code: "bad_request",
          message: "maxLifetime exceeds inherited limit",
        },
      });
      expect(await savedTenantConfig()).toBeNull();
    });

    test("tenant membership without a manage grant cannot update the tenant", async () => {
      const res = await patchTenant([allow(`tenant:${TENANT_ID}`, "read")], {
        lifecycle: { maxLifetime: "1h" },
      });
      expect(res.status).toBe(403);
      expect(await savedTenantConfig()).toBeNull();
    });

    const PLACEMENT = {
      capabilities: [{ capability: "network:outbound", effect: "block" }],
    };
    const MANAGE_TENANT = [allow(`tenant:${TENANT_ID}`, "manage")];

    async function setTenantConfig(config: unknown): Promise<void> {
      await h.db.update(tenant).set({ config }).where(eq(tenant.id, TENANT_ID));
    }

    async function setRootConfig(config: unknown): Promise<void> {
      await h.db
        .update(tenant)
        .set({ config })
        .where(eq(tenant.id, ROOT_TENANT_ID));
    }

    async function savedTenantName(): Promise<string | undefined> {
      const row = await h.db.query.tenant.findFirst({
        where: eq(tenant.id, TENANT_ID),
        columns: { name: true },
      });
      return row?.name;
    }

    // Saved before lifecycle policies were validated; "w" is not a unit.
    const INVALID_LIFECYCLE = { lifecycle: { maxLifetime: "1w" } };
    const INHERITED_INVALID = {
      error: {
        code: "invalid_tenant_config",
        message: "The tenant or an ancestor has invalid configuration",
      },
    };

    test("a lifecycle update keeps the tenant's other config keys", async () => {
      await setTenantConfig({ sidecarPlacement: PLACEMENT, note: "kept" });
      const res = await patchTenant(MANAGE_TENANT, {
        lifecycle: { maxLifetime: "1h" },
      });
      expect(res.status).toBe(200);
      const expected = {
        sidecarPlacement: PLACEMENT,
        note: "kept",
        lifecycle: { maxLifetime: "1h" },
      };
      expect(await res.json()).toMatchObject({ config: expected });
      expect(await savedTenantConfig()).toEqual(expected);
    });

    test("a placement update keeps a lifecycle above a since-tightened limit", async () => {
      // Above the parent's 2h limit, as if saved before the parent tightened.
      await setTenantConfig({ lifecycle: { maxLifetime: "3h" } });
      const res = await patchTenant(MANAGE_TENANT, {
        sidecarPlacement: PLACEMENT,
      });
      expect(res.status).toBe(200);
      expect(await savedTenantConfig()).toEqual({
        lifecycle: { maxLifetime: "3h" },
        sidecarPlacement: PLACEMENT,
      });
    });

    test("a null config value removes that key", async () => {
      await setTenantConfig({
        lifecycle: { maxLifetime: "1h" },
        sidecarPlacement: PLACEMENT,
      });
      const res = await patchTenant(MANAGE_TENANT, { lifecycle: null });
      expect(res.status).toBe(200);
      expect(await savedTenantConfig()).toEqual({
        sidecarPlacement: PLACEMENT,
      });
    });

    test("concurrent updates to different config keys both persist", async () => {
      const [lifecycleRes, placementRes] = await Promise.all([
        patchTenant(MANAGE_TENANT, { lifecycle: { maxLifetime: "1h" } }),
        patchTenant(MANAGE_TENANT, { sidecarPlacement: PLACEMENT }),
      ]);
      expect(lifecycleRes.status).toBe(200);
      expect(placementRes.status).toBe(200);
      expect(await savedTenantConfig()).toEqual({
        lifecycle: { maxLifetime: "1h" },
        sidecarPlacement: PLACEMENT,
      });
    });

    test("an invalid stored key must be replaced or removed before other keys change", async () => {
      await setTenantConfig(INVALID_LIFECYCLE);
      const refused = await patchTenant(MANAGE_TENANT, {
        sidecarPlacement: PLACEMENT,
      });
      expect(refused.status).toBe(400);
      expect(await refused.text()).toContain("lifecycle.maxLifetime");
      expect(await savedTenantConfig()).toEqual({
        lifecycle: { maxLifetime: "1w" },
      });

      const repaired = await patchTenant(MANAGE_TENANT, {
        lifecycle: null,
        sidecarPlacement: PLACEMENT,
      });
      expect(repaired.status).toBe(200);
      expect(await savedTenantConfig()).toEqual({
        sidecarPlacement: PLACEMENT,
      });
    });

    test("a rename is refused until an invalid stored key is repaired", async () => {
      await setTenantConfig(INVALID_LIFECYCLE);
      const refused = await updateTenant(MANAGE_TENANT, { name: "Renamed" });
      expect(refused.status).toBe(400);
      expect(await refused.text()).toContain("lifecycle.maxLifetime");
      expect(await savedTenantName()).not.toBe("Renamed");

      const repaired = await updateTenant(MANAGE_TENANT, {
        name: "Renamed",
        config: { lifecycle: null },
      });
      expect(repaired.status).toBe(200);
      expect(await savedTenantName()).toBe("Renamed");
    });

    test("reading a tenant reports its invalid stored config", async () => {
      await setTenantConfig(INVALID_LIFECYCLE);
      const res = await makeApp(createInMemoryGrantStore([])).request(
        `/api/tenants/${TENANT_ID}`,
      );
      expect(res.status).toBe(409);
      expect(await res.json()).toMatchObject({
        error: {
          code: "invalid_tenant_config",
          message: expect.stringContaining(
            `Tenant ${TENANT_ID} has invalid configuration: lifecycle.maxLifetime`,
          ),
        },
      });
    });

    test("a lifecycle update hides invalid inherited config values", async () => {
      await setRootConfig(INVALID_LIFECYCLE);
      const res = await patchTenant(MANAGE_TENANT, {
        lifecycle: { maxLifetime: "1h" },
      });
      expect(res.status).toBe(409);
      expect(await res.json()).toEqual(INHERITED_INVALID);
      expect(await savedTenantConfig()).toBeNull();
    });

    test("a definition policy update hides invalid inherited config values", async () => {
      await setRootConfig(INVALID_LIFECYCLE);
      const res = await patchDefinitionLifecycle(DEFINITION_ID, {
        lifecycle: { maxLifetime: "1h" },
      });
      expect(res.status).toBe(409);
      expect(await res.json()).toEqual(INHERITED_INVALID);
      expect(await savedDefinitionPolicy()).toBeNull();
    });

    test("a definition policy within the tenant's limits is saved", async () => {
      const lifecycle = {
        maxLifetime: "90m",
        capacityRetention: { completed: "0s" },
      };
      const res = await patchDefinitionLifecycle(DEFINITION_ID, { lifecycle });
      expect(res.status).toBe(200);
      expect(await res.json()).toMatchObject({ id: DEFINITION_ID, lifecycle });
      expect(await savedDefinitionPolicy()).toEqual(lifecycle);
    });

    test("a definition policy cannot exceed a limit inherited by its tenant", async () => {
      const res = await patchDefinitionLifecycle(DEFINITION_ID, {
        lifecycle: { maxLifetime: "3h" },
      });
      expect(res.status).toBe(400);
      expect(await res.json()).toEqual({
        error: {
          code: "bad_request",
          message: "maxLifetime exceeds inherited limit",
        },
      });
      expect(await savedDefinitionPolicy()).toBeNull();
    });

    test("another tenant's definition is not found", async () => {
      const res = await patchDefinitionLifecycle(OTHER_DEFINITION_ID, {
        lifecycle: { maxLifetime: "1h" },
      });
      expect(res.status).toBe(404);
    });

    async function seedRevisions(): Promise<void> {
      await seedAsset(h.db, {
        id: "ast_lifecycle",
        tenantId: TENANT_ID,
        kind: "workflow",
        name: "lifecycle-workflow",
      });
      await seedAsset(h.db, {
        id: "ast_lifecycle_unrelated",
        tenantId: TENANT_ID,
        kind: "workflow",
        name: "unrelated-workflow",
      });
      await h.db.insert(workflowDefinition).values([
        {
          id: "wfd_revision_1",
          tenantId: TENANT_ID,
          name: "lifecycle-workflow",
          assetId: "ast_lifecycle",
          wireHash: "revision-1",
        },
        {
          id: "wfd_revision_2",
          tenantId: TENANT_ID,
          name: "lifecycle-workflow",
          assetId: "ast_lifecycle",
          wireHash: "revision-2",
        },
        {
          id: "wfd_unrelated",
          tenantId: TENANT_ID,
          name: "unrelated-workflow",
          assetId: "ast_lifecycle_unrelated",
          wireHash: "revision-1",
        },
      ]);
    }

    async function definitionPolicies(): Promise<Record<string, unknown>> {
      const rows = await h.db
        .select({
          id: workflowDefinition.id,
          lifecyclePolicy: workflowDefinition.lifecyclePolicy,
        })
        .from(workflowDefinition)
        .where(eq(workflowDefinition.tenantId, TENANT_ID));
      return Object.fromEntries(
        rows.map((row) => [row.id, row.lifecyclePolicy]),
      );
    }

    test("a definition policy covers every revision of its workflow asset", async () => {
      await seedRevisions();
      const lifecycle = { maxLifetime: "90m" };

      const res = await patchDefinitionLifecycle("wfd_revision_1", {
        lifecycle,
      });
      expect(res.status).toBe(200);
      expect(await definitionPolicies()).toEqual({
        [DEFINITION_ID]: null,
        wfd_revision_1: lifecycle,
        wfd_revision_2: lifecycle,
        wfd_unrelated: null,
      });

      const revision = await h.db.transaction((tx) =>
        ensureWorkflowDefinitionForAsset(tx, {
          assetId: "ast_lifecycle",
          wireHash: "revision-3",
        }),
      );
      const created = await h.db.query.workflowDefinition.findFirst({
        where: eq(workflowDefinition.id, revision.definitionId),
        columns: { lifecyclePolicy: true },
      });
      expect(created?.lifecyclePolicy).toEqual(lifecycle);
    });

    test("a grant on one revision cannot change the others", async () => {
      await seedRevisions();

      const res = await patchDefinitionLifecycle(
        "wfd_revision_1",
        { lifecycle: { maxLifetime: "90m" } },
        createInMemoryGrantStore([
          allow("workflow-definition:wfd_revision_1", "manage"),
        ]),
      );
      expect(res.status).toBe(403);
      expect(await definitionPolicies()).toEqual({
        [DEFINITION_ID]: null,
        wfd_revision_1: null,
        wfd_revision_2: null,
        wfd_unrelated: null,
      });
    });

    test("grants on every revision allow the override", async () => {
      await seedRevisions();
      const lifecycle = { maxLifetime: "90m" };

      const res = await patchDefinitionLifecycle(
        "wfd_revision_1",
        { lifecycle },
        createInMemoryGrantStore([
          allow("workflow-definition:wfd_revision_1", "manage"),
          allow("workflow-definition:wfd_revision_2", "manage"),
        ]),
      );
      expect(res.status).toBe(200);
      expect(await definitionPolicies()).toEqual({
        [DEFINITION_ID]: null,
        wfd_revision_1: lifecycle,
        wfd_revision_2: lifecycle,
        wfd_unrelated: null,
      });
    });

    test("a revision deployed after the permission check refuses the override", async () => {
      await seedRevisions();
      const grants = createInMemoryGrantStore([
        allow("workflow-definition:*", "manage"),
      ]);
      // Every grant lookup deploys a new revision, so one lands after the
      // route has read the revisions it authorizes.
      let deployed = 0;
      const deployingGrants: GrantStore = {
        ...grants,
        async collectGrants(principalId, tenantId) {
          deployed += 1;
          await h.db.transaction((tx) =>
            ensureWorkflowDefinitionForAsset(tx, {
              assetId: "ast_lifecycle",
              wireHash: `deployed-${deployed}`,
            }),
          );
          return grants.collectGrants(principalId, tenantId);
        },
      };

      const res = await patchDefinitionLifecycle(
        "wfd_revision_1",
        { lifecycle: { maxLifetime: "90m" } },
        deployingGrants,
      );
      expect(res.status).toBe(409);
      expect(
        Object.values(await definitionPolicies()).every(
          (policy) => policy === null,
        ),
      ).toBe(true);
    });

    test("an undeclared policy key is rejected", async () => {
      const res = await patchDefinitionLifecycle(DEFINITION_ID, {
        lifecycle: { maxlifetime: "1h" },
      });
      expect(res.status).toBe(400);
      expect(await savedDefinitionPolicy()).toBeNull();
    });
  },
);

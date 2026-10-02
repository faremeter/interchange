import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { eq } from "drizzle-orm";

import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedTenants } from "@intx/test-harness/seed";
import {
  resolveDeploymentLifecyclePolicy,
  TenantConfigInvalidError,
  validateLifecyclePolicyEdit,
} from "@intx/db";
import { tenant, workflowDefinition } from "@intx/db/schema";
import type { ResolvedWorkflowLifecyclePolicy } from "@intx/types";

const DEFAULTS: ResolvedWorkflowLifecyclePolicy = {
  maxLifetime: "5d",
  capacityRetention: { completed: "10m", failed: "12h", cancelled: "2h" },
};

describe.skipIf(!harnessDbEnvAvailable())(
  "deployment lifecycle policy resolution",
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
        { id: "root" },
        { id: "child", parentId: "root" },
      ]);
      await h.db
        .update(tenant)
        .set({
          config: {
            lifecycle: {
              maxLifetime: "24h",
              capacityRetention: { completed: "0s", failed: "1h" },
            },
          },
        })
        .where(eq(tenant.id, "root"));
      await h.db
        .update(tenant)
        .set({ config: { lifecycle: { maxLifetime: "12h" } } })
        .where(eq(tenant.id, "child"));
      await h.db.insert(workflowDefinition).values({
        id: "definition",
        tenantId: "child",
        name: "Example",
        lifecyclePolicy: {
          maxLifetime: "2h",
          capacityRetention: { failed: "15m" },
        },
      });
    });

    test("resolves tenant ancestry before installed-workflow overrides", async () => {
      expect(
        await resolveDeploymentLifecyclePolicy(
          h.db,
          "child",
          "definition",
          DEFAULTS,
        ),
      ).toEqual({
        maxLifetime: "2h",
        capacityRetention: { completed: "0s", failed: "15m", cancelled: "2h" },
      });
    });

    test("fills fields no level sets with the given defaults", async () => {
      await h.db.update(tenant).set({ config: null });
      await h.db
        .update(workflowDefinition)
        .set({ lifecyclePolicy: null })
        .where(eq(workflowDefinition.id, "definition"));
      expect(
        await resolveDeploymentLifecyclePolicy(
          h.db,
          "child",
          "definition",
          DEFAULTS,
        ),
      ).toEqual({
        maxLifetime: "5d",
        capacityRetention: { completed: "10m", failed: "12h", cancelled: "2h" },
      });
    });

    test("the defaults are not a ceiling", async () => {
      await h.db.update(tenant).set({ config: null });
      await h.db
        .update(workflowDefinition)
        .set({
          lifecyclePolicy: {
            maxLifetime: "30d",
            capacityRetention: { completed: "2h" },
          },
        })
        .where(eq(workflowDefinition.id, "definition"));
      expect(
        await resolveDeploymentLifecyclePolicy(
          h.db,
          "child",
          "definition",
          DEFAULTS,
        ),
      ).toEqual({
        maxLifetime: "30d",
        capacityRetention: { completed: "2h", failed: "12h", cancelled: "2h" },
      });
    });

    test("caps overrides a tighter ancestor invalidated after they were saved", async () => {
      await h.db
        .update(tenant)
        .set({
          config: {
            lifecycle: {
              maxLifetime: "1h",
              capacityRetention: { failed: "5m" },
            },
          },
        })
        .where(eq(tenant.id, "root"));
      expect(
        await resolveDeploymentLifecyclePolicy(
          h.db,
          "child",
          "definition",
          DEFAULTS,
        ),
      ).toEqual({
        maxLifetime: "1h",
        capacityRetention: { completed: "10m", failed: "5m", cancelled: "2h" },
      });
    });

    test("a stale ancestor value cannot reject a valid edit below it", async () => {
      await h.db
        .update(tenant)
        .set({ config: { lifecycle: { maxLifetime: "6h" } } })
        .where(eq(tenant.id, "root"));

      expect(
        await validateLifecyclePolicyEdit(h.db, "child", { maxLifetime: "2h" }),
      ).toEqual({ ok: true, policy: { maxLifetime: "2h" } });
      expect(
        await validateLifecyclePolicyEdit(h.db, "child", {
          maxLifetime: "8h",
        }),
      ).toEqual({
        ok: false,
        field: "maxLifetime",
        requested: "8h",
        limit: "6h",
      });
    });

    test("an edit is validated against its own ancestors only", async () => {
      expect(
        await validateLifecyclePolicyEdit(h.db, null, { maxLifetime: "48h" }),
      ).toEqual({ ok: true, policy: { maxLifetime: "48h" } });
      expect(
        await validateLifecyclePolicyEdit(h.db, "root", {
          maxLifetime: "20h",
        }),
      ).toMatchObject({ ok: true, policy: { maxLifetime: "20h" } });
      expect(
        await validateLifecyclePolicyEdit(h.db, "root", {
          capacityRetention: { failed: "2h" },
        }),
      ).toMatchObject({
        ok: false,
        field: "capacityRetention.failed",
        limit: "1h",
      });
    });

    test("reports a stored config that fails validation with its tenant", async () => {
      await h.db
        .update(tenant)
        .set({ config: { lifecycle: { maxLifetime: "1w" } } })
        .where(eq(tenant.id, "root"));
      await expect(
        resolveDeploymentLifecyclePolicy(h.db, "child", "definition", DEFAULTS),
      ).rejects.toThrow(TenantConfigInvalidError);
      await expect(
        validateLifecyclePolicyEdit(h.db, "child", {}),
      ).rejects.toThrow(
        /^Tenant root has invalid configuration: lifecycle\.maxLifetime/,
      );
    });

    test("rejects a cross-tenant definition", async () => {
      await expect(
        resolveDeploymentLifecyclePolicy(h.db, "root", "definition", DEFAULTS),
      ).rejects.toThrow("does not belong");
    });
  },
);

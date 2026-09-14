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
import { resolveDeploymentLifecyclePolicy } from "@intx/db";
import { tenant, workflowDefinition } from "@intx/db/schema";

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
        await resolveDeploymentLifecyclePolicy(h.db, "child", "definition"),
      ).toEqual({
        ok: true,
        policy: {
          maxLifetime: "2h",
          capacityRetention: { completed: "0s", failed: "15m" },
        },
      });
    });

    test("rejects an override invalidated by a tighter ancestor and a cross-tenant definition", async () => {
      await h.db
        .update(tenant)
        .set({ config: { lifecycle: { maxLifetime: "1h" } } })
        .where(eq(tenant.id, "root"));
      expect(
        await resolveDeploymentLifecyclePolicy(h.db, "child", "definition"),
      ).toMatchObject({ ok: false, field: "maxLifetime", limit: "1h" });
      await expect(
        resolveDeploymentLifecyclePolicy(h.db, "root", "definition"),
      ).rejects.toThrow("does not belong");
    });
  },
);

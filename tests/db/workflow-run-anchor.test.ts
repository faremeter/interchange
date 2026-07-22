import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { eq } from "drizzle-orm";

import { createWorkflowRunStore } from "@intx/db";
import { workflowRun } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedPrincipal,
  seedTenants,
  seedWorkflowDeployment,
  seedWorkflowRun,
} from "@intx/test-harness/seed";

const TENANT = "tnt";
const ASSET = "ast";
const DEPLOYMENT = "dep";

describe.skipIf(!harnessDbEnvAvailable())(
  "workflowRunStore.anchorWithPrincipal (real DB)",
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
      await seedTenants(h.db, [{ id: TENANT }]);
      await seedAsset(h.db, {
        id: ASSET,
        tenantId: TENANT,
        kind: "workflow",
        name: ASSET,
      });
      await seedWorkflowDeployment(h.db, {
        id: DEPLOYMENT,
        tenantId: TENANT,
        definitionAssetId: ASSET,
      });
      await seedPrincipal(h.db, {
        id: "prn-run",
        tenantId: TENANT,
        kind: "workflow",
        refId: "run-1",
        status: "active",
      });
    });

    test("inserts a fresh run row carrying the principal", async () => {
      const store = createWorkflowRunStore(h.db);
      await store.anchorWithPrincipal({
        id: "run-1",
        deploymentId: DEPLOYMENT,
        tenantId: TENANT,
        principalId: "prn-run",
        status: "running",
      });

      const [row] = await h.db
        .select()
        .from(workflowRun)
        .where(eq(workflowRun.id, "run-1"));
      expect(row?.principalId).toBe("prn-run");
      expect(row?.status).toBe("running");
    });

    test("attaches the principal to a co-write's null-principal row", async () => {
      // The lazy anchor in signal-correlation registration inserted the row
      // first with a null principal (the run parked before the trigger route
      // committed). The trigger path must attach its principal without
      // throwing on the id conflict, and must not disturb the row's status.
      await seedWorkflowRun(h.db, {
        id: "run-1",
        deploymentId: DEPLOYMENT,
        tenantId: TENANT,
        principalId: null,
      });

      const store = createWorkflowRunStore(h.db);
      await store.anchorWithPrincipal({
        id: "run-1",
        deploymentId: DEPLOYMENT,
        tenantId: TENANT,
        principalId: "prn-run",
        status: "running",
      });

      const rows = await h.db
        .select()
        .from(workflowRun)
        .where(eq(workflowRun.id, "run-1"));
      // Exactly one row survives the conflict, now carrying the principal.
      expect(rows).toHaveLength(1);
      expect(rows[0]?.principalId).toBe("prn-run");
      expect(rows[0]?.status).toBe("running");
    });

    test("does not overwrite a principal already attached", async () => {
      // A second principal already owns the run row (a concurrent winner
      // attached it). The null-guarded reconcile must leave it untouched.
      await seedPrincipal(h.db, {
        id: "prn-existing",
        tenantId: TENANT,
        kind: "workflow",
        refId: "run-other",
        status: "active",
      });
      await seedWorkflowRun(h.db, {
        id: "run-1",
        deploymentId: DEPLOYMENT,
        tenantId: TENANT,
        principalId: "prn-existing",
      });

      const store = createWorkflowRunStore(h.db);
      await store.anchorWithPrincipal({
        id: "run-1",
        deploymentId: DEPLOYMENT,
        tenantId: TENANT,
        principalId: "prn-run",
        status: "running",
      });

      const [row] = await h.db
        .select()
        .from(workflowRun)
        .where(eq(workflowRun.id, "run-1"));
      expect(row?.principalId).toBe("prn-existing");
    });
  },
);

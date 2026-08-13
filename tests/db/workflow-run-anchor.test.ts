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
import { workflowDefinition, workflowRun } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedPrincipal,
  seedTenants,
  seedWorkflowRun,
} from "@intx/test-harness/seed";

const TENANT = "tnt";
const ASSET = "ast";
const DEPLOYMENT = "dep";
const DEFINITION = "wfd";

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
      await h.db.insert(workflowDefinition).values({
        id: DEFINITION,
        tenantId: TENANT,
        name: DEFINITION,
      });
      await seedWorkflowRun(h.db, {
        id: DEPLOYMENT,
        anchorRunId: DEPLOYMENT,
        tenantId: TENANT,
        definitionId: DEFINITION,
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
        anchorRunId: DEPLOYMENT,
        tenantId: TENANT,
        definitionId: DEFINITION,
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

    test("flips a deployed null-principal anchor to running and attaches the principal", async () => {
      // Deploy created the anchor first, born "deployed" with a null principal
      // (its pre-trigger window). The first trigger must attach its principal
      // without throwing on the id conflict, and flip "deployed" -> "running" in
      // the same reconcile.
      await seedWorkflowRun(h.db, {
        id: "run-1",
        anchorRunId: DEPLOYMENT,
        tenantId: TENANT,
        definitionId: DEFINITION,
        principalId: null,
        status: "deployed",
      });

      const store = createWorkflowRunStore(h.db);
      await store.anchorWithPrincipal({
        id: "run-1",
        anchorRunId: DEPLOYMENT,
        tenantId: TENANT,
        definitionId: DEFINITION,
        principalId: "prn-run",
        status: "running",
      });

      const rows = await h.db
        .select()
        .from(workflowRun)
        .where(eq(workflowRun.id, "run-1"));
      // Exactly one row survives the conflict, now carrying the principal and
      // flipped live.
      expect(rows).toHaveLength(1);
      expect(rows[0]?.principalId).toBe("prn-run");
      expect(rows[0]?.status).toBe("running");
    });

    test("does not resurrect a terminal null-principal run", async () => {
      // A concurrent teardown settled the run terminal before the trigger's
      // reconcile ran, leaving it "cancelled" with a null principal. The
      // `status = 'deployed'` guard must block the reconcile: the two-part
      // `principalId IS NULL` guard alone would flip a terminal row back to
      // running and attach a principal, resurrecting a dead run.
      await seedWorkflowRun(h.db, {
        id: "run-1",
        anchorRunId: DEPLOYMENT,
        tenantId: TENANT,
        definitionId: DEFINITION,
        principalId: null,
        status: "cancelled",
      });

      const store = createWorkflowRunStore(h.db);
      await store.anchorWithPrincipal({
        id: "run-1",
        anchorRunId: DEPLOYMENT,
        tenantId: TENANT,
        definitionId: DEFINITION,
        principalId: "prn-run",
        status: "running",
      });

      const [row] = await h.db
        .select()
        .from(workflowRun)
        .where(eq(workflowRun.id, "run-1"));
      expect(row?.status).toBe("cancelled");
      expect(row?.principalId).toBeNull();
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
        anchorRunId: DEPLOYMENT,
        tenantId: TENANT,
        definitionId: DEFINITION,
        principalId: "prn-existing",
      });

      const store = createWorkflowRunStore(h.db);
      await store.anchorWithPrincipal({
        id: "run-1",
        anchorRunId: DEPLOYMENT,
        tenantId: TENANT,
        definitionId: DEFINITION,
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

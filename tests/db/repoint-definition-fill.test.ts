import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { readFileSync } from "node:fs";
import { eq, sql } from "drizzle-orm";

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
  seedWorkflowDeployment,
} from "@intx/test-harness/seed";

// Migration 0056 re-points the deployment foreign keys onto the anchor run and
// fills definition_id on the runs it still anchors, so every run carries its
// definition. Migration 0055 fills the anchor runs it inserts, but a
// pre-existing non-anchor run (a folded or child run created before this branch
// added the column) has a null definition_id that only the deployment -> asset
// -> definition join resolves. This drives that fill statement -- lifted from
// the migration so the test cannot drift -- against seeded pre-existing runs.
const MIGRATION_SQL = readFileSync(
  new URL(
    "../../packages/db/migrations/0056_repoint_deployment_fks_to_anchor_run.sql",
    import.meta.url,
  ),
  "utf-8",
);
const FILL_STATEMENT = MIGRATION_SQL.split("--> statement-breakpoint")
  .map((s) => s.trim())
  .find((s) => s.includes('UPDATE "workflow_run"'));
if (FILL_STATEMENT === undefined) {
  throw new Error("migration 0056 has no definition_id fill UPDATE");
}
const FILL = sql.raw(FILL_STATEMENT);

const TENANT = "tnt";
const ASSET = "ast_wf";
const DEFINITION = "wfd_wf";

describe.skipIf(!harnessDbEnvAvailable())(
  "0056 definition_id fill (migration)",
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
      await seedPrincipal(h.db, {
        id: "prn",
        tenantId: TENANT,
        kind: "user",
        refId: "creator",
      });
      await seedAsset(h.db, {
        id: ASSET,
        tenantId: TENANT,
        kind: "workflow",
        name: "wf",
        creatorPrincipalId: "prn",
      });
      await h.db.insert(workflowDefinition).values({
        id: DEFINITION,
        tenantId: TENANT,
        name: "def",
        assetId: ASSET,
      });
    });

    async function run(id: string) {
      return (
        await h.db.select().from(workflowRun).where(eq(workflowRun.id, id))
      )[0];
    }

    test("fills a pre-existing non-anchor run from its deployment's definition", async () => {
      await seedWorkflowDeployment(h.db, {
        id: "dep",
        tenantId: TENANT,
        definitionAssetId: ASSET,
      });
      // The anchor run already carries its definition (migration 0055 / the
      // deploy path set it); the fill must leave it untouched.
      await h.db.insert(workflowRun).values({
        id: "dep",
        tenantId: TENANT,
        deploymentId: "dep",
        definitionId: DEFINITION,
        status: "running",
      });
      // A pre-existing child/folded run of that deployment, born before the
      // column existed, with a null definition_id the fill must resolve.
      await h.db.insert(workflowRun).values({
        id: "run-child",
        tenantId: TENANT,
        deploymentId: "dep",
        definitionId: null,
        status: "running",
      });

      await h.db.execute(FILL);

      expect((await run("run-child"))?.definitionId).toBe(DEFINITION);
      expect((await run("dep"))?.definitionId).toBe(DEFINITION);
    });

    test("leaves a run null when its deployment's asset was never folded", async () => {
      // A deployment whose workflow asset has no folded definition: the fill's
      // inner join finds no definition, so the run keeps its null definition_id.
      await seedAsset(h.db, {
        id: "ast_unfolded",
        tenantId: TENANT,
        kind: "workflow",
        name: "unfolded",
        creatorPrincipalId: "prn",
      });
      await seedWorkflowDeployment(h.db, {
        id: "dep_unfolded",
        tenantId: TENANT,
        definitionAssetId: "ast_unfolded",
      });
      await h.db.insert(workflowRun).values({
        id: "dep_unfolded",
        tenantId: TENANT,
        deploymentId: "dep_unfolded",
        definitionId: null,
        status: "running",
      });

      await h.db.execute(FILL);

      expect((await run("dep_unfolded"))?.definitionId).toBeNull();
    });
  },
);

import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { and, eq } from "drizzle-orm";

import { sessionAsset, workflowDefinition, workflowRun } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedPrincipal, seedTenants } from "@intx/test-harness/seed";

// A folded run IS the launched instance and writes its run id into
// session_asset.instance_id when it attaches an asset. That id is a workflow_run
// id, not an agent_instance id, so the old agent_instance foreign key rejected
// it. With the FK dropped (instance_id is now a bare polymorphic column), the
// materialization row inserts against a folded run.

const TENANT = "tnt";
const DEFINITION = "wfd_demo";
const RUN = "run_folded";

describe.skipIf(!harnessDbEnvAvailable())(
  "session_asset materialization for a folded run (real DB)",
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
      await h.db.insert(workflowDefinition).values({
        id: DEFINITION,
        tenantId: TENANT,
        name: "demo",
      });
      // A folded run: its id is a workflow_run id, absent from agent_instance.
      await h.db.insert(workflowRun).values({
        id: RUN,
        tenantId: TENANT,
        definitionId: DEFINITION,
        address: "ins_run_folded@wf.example",
        status: "running",
      });
    });

    test("records a materialization keyed on the folded run's id", async () => {
      await h.db.insert(sessionAsset).values({
        instanceId: RUN,
        mountPath: "/skills/demo",
        assetPackSha: "sha-pack",
        sourceCommitSha: "sha-commit",
      });

      const rows = await h.db
        .select()
        .from(sessionAsset)
        .where(
          and(
            eq(sessionAsset.instanceId, RUN),
            eq(sessionAsset.mountPath, "/skills/demo"),
          ),
        );
      expect(rows).toHaveLength(1);
    });
  },
);

import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { resolveWorkflowPrincipalNames } from "@intx/hub-api";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedTenants,
  seedWorkflowDeployment,
  seedWorkflowRun,
} from "@intx/test-harness/seed";

const TENANT = "tnt";
const ASSET = "ast";
const DEPLOYMENT = "dep";
const ADDRESS = "ins_dep@wf.example";

describe.skipIf(!harnessDbEnvAvailable())(
  "resolveWorkflowPrincipalNames (real DB)",
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
        address: ADDRESS,
      });
    });

    test("resolves a run-principal refId to Workflow (<address>)", async () => {
      // The principal's refId is the run id, not the deployment id. The helper
      // must join the runId through workflow_run to workflow_deployment to
      // reach the address.
      await seedWorkflowRun(h.db, {
        id: "run-1",
        deploymentId: DEPLOYMENT,
        tenantId: TENANT,
      });

      const names = await resolveWorkflowPrincipalNames(h.db, ["run-1"]);
      expect(names.get("run-1")).toBe(`Workflow (${ADDRESS})`);
    });

    test("omits a refId with no run row", async () => {
      // A refId that names no run row resolves to nothing, so the caller falls
      // back to the raw refId rather than a wrong label.
      const names = await resolveWorkflowPrincipalNames(h.db, ["run-missing"]);
      expect(names.has("run-missing")).toBe(false);
    });
  },
);

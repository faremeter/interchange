import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { resolveWorkflowPrincipalNames } from "@intx/hub-api";
import { workflowRun } from "@intx/db/schema";
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
      // The deployment's anchor run carries the routing address the display
      // name comes from; its id is the deployment id.
      await h.db.insert(workflowRun).values({
        id: DEPLOYMENT,
        tenantId: TENANT,
        deploymentId: DEPLOYMENT,
        address: ADDRESS,
        status: "running",
      });
    });

    test("resolves a child run's refId to its anchor run's address", async () => {
      // The principal's refId is the child run id, not the deployment id. The
      // helper self-joins the run to its anchor run (on the deployment id) to
      // reach the routing address the anchor carries.
      await seedWorkflowRun(h.db, {
        id: "run-1",
        deploymentId: DEPLOYMENT,
        tenantId: TENANT,
      });

      const names = await resolveWorkflowPrincipalNames(h.db, ["run-1"]);
      expect(names.get("run-1")).toBe(`Workflow (${ADDRESS})`);
    });

    test("resolves the anchor run's own refId to its address", async () => {
      // The anchor run's deploymentId is its own id, so the self-join resolves
      // it to itself and it names its own address.
      const names = await resolveWorkflowPrincipalNames(h.db, [DEPLOYMENT]);
      expect(names.get(DEPLOYMENT)).toBe(`Workflow (${ADDRESS})`);
    });

    test("omits a refId with no run row", async () => {
      // A refId that names no run row resolves to nothing, so the caller falls
      // back to the raw refId rather than a wrong label.
      const names = await resolveWorkflowPrincipalNames(h.db, ["run-missing"]);
      expect(names.has("run-missing")).toBe(false);
    });

    test("resolves a folded run via its own address (no deployment)", async () => {
      // A folded launch's run carries its address directly and has no
      // deployment, so the label comes from workflow_run.address.
      await h.db.insert(workflowRun).values({
        id: "run-folded",
        tenantId: TENANT,
        deploymentId: null,
        definitionId: null,
        address: "ins_folded@wf.example",
        status: "running",
      });

      const names = await resolveWorkflowPrincipalNames(h.db, ["run-folded"]);
      expect(names.get("run-folded")).toBe("Workflow (ins_folded@wf.example)");
    });

    test("omits a run with neither a deployment nor its own address", async () => {
      await h.db.insert(workflowRun).values({
        id: "run-nameless",
        tenantId: TENANT,
        deploymentId: null,
        definitionId: null,
        address: null,
        status: "running",
      });

      const names = await resolveWorkflowPrincipalNames(h.db, ["run-nameless"]);
      expect(names.has("run-nameless")).toBe(false);
    });
  },
);

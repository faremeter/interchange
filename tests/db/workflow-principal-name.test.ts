import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import {
  resolveWorkflowPrincipalLabels,
  resolveWorkflowPrincipalNames,
} from "@intx/hub-api";
import { workflowDefinition } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedTenants,
  seedWorkflowRun,
} from "@intx/test-harness/seed";

const TENANT = "tnt";
const ASSET = "ast";
const DEPLOYMENT = "dep";
const ADDRESS = "run_dep@wf.example";

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
      // The deployment's anchor run carries the routing address the display
      // name comes from; its id is the deployment id.
      await seedWorkflowRun(h.db, {
        id: DEPLOYMENT,
        tenantId: TENANT,
        anchorRunId: DEPLOYMENT,
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
        anchorRunId: DEPLOYMENT,
        tenantId: TENANT,
      });

      const names = await resolveWorkflowPrincipalNames(h.db, ["run-1"]);
      expect(names.get("run-1")).toBe(`Workflow (${ADDRESS})`);
    });

    test("resolves the anchor run's own refId to its address", async () => {
      // The anchor run's anchorRunId is its own id, so the self-join resolves
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
      await seedWorkflowRun(h.db, {
        id: "run-folded",
        tenantId: TENANT,
        anchorRunId: null,
        address: "run_folded@wf.example",
        status: "running",
      });

      const names = await resolveWorkflowPrincipalNames(h.db, ["run-folded"]);
      expect(names.get("run-folded")).toBe("Workflow (run_folded@wf.example)");
    });

    test("omits a run with neither a deployment nor its own address", async () => {
      await seedWorkflowRun(h.db, {
        id: "run-nameless",
        tenantId: TENANT,
        anchorRunId: null,
        address: null,
        status: "running",
      });

      const names = await resolveWorkflowPrincipalNames(h.db, ["run-nameless"]);
      expect(names.has("run-nameless")).toBe(false);
    });
  },
);

describe.skipIf(!harnessDbEnvAvailable())(
  "resolveWorkflowPrincipalLabels (real DB)",
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
    });

    test("resolves a run refId to its address label", async () => {
      await seedWorkflowRun(h.db, {
        id: "run-1",
        tenantId: TENANT,
        anchorRunId: null,
        address: "run_@wf.example",
        status: "running",
      });

      const labels = await resolveWorkflowPrincipalLabels(h.db, ["run-1"]);
      expect(labels.get("run-1")).toBe("Workflow (run_@wf.example)");
    });

    test("falls a definition refId through to the definition name", async () => {
      // A re-keyed definition principal's refId names no run, so it falls
      // through the run resolver to the definition resolver.
      await h.db.insert(workflowDefinition).values({
        id: "wfd-1",
        tenantId: TENANT,
        name: "my-workflow",
      });

      const labels = await resolveWorkflowPrincipalLabels(h.db, ["wfd-1"]);
      expect(labels.get("wfd-1")).toBe("my-workflow");
    });

    test("resolves run and definition refIds in one batch", async () => {
      // Proves the fallthrough handles a mixed batch: the run resolves on the
      // first pass, the definition only on the second.
      await seedWorkflowRun(h.db, {
        id: "run-1",
        tenantId: TENANT,
        anchorRunId: null,
        address: "run_@wf.example",
        status: "running",
      });
      await h.db.insert(workflowDefinition).values({
        id: "wfd-1",
        tenantId: TENANT,
        name: "my-workflow",
      });

      const labels = await resolveWorkflowPrincipalLabels(h.db, [
        "run-1",
        "wfd-1",
      ]);
      expect(labels.get("run-1")).toBe("Workflow (run_@wf.example)");
      expect(labels.get("wfd-1")).toBe("my-workflow");
    });

    test("omits a refId that matches neither a run nor a definition", async () => {
      const labels = await resolveWorkflowPrincipalLabels(h.db, ["nope"]);
      expect(labels.has("nope")).toBe(false);
    });
  },
);

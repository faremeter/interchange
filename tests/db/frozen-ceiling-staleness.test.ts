// The mail materializer caches a per-deployment grant basis for the process
// lifetime. The childWorkflow runtime ceiling is version-scoped and
// `currentVersion` is mutable (a rollback repoints it), so the ceiling must be
// read fresh each run, never frozen with the content-pure basis. This test
// pins that: a later run of the same definition, after a rollback to a narrower
// version, must materialize under the CURRENT version's surface -- not the
// ceiling that a prior run froze at the superseded version.

import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { eq } from "drizzle-orm";

import { createGrantStore, writeApprovedGrantSurface } from "@intx/db";
import {
  grant,
  principal,
  workflowDefinition,
  workflowDefinitionVersion,
  workflowRun,
} from "@intx/db/schema";
import type { ApprovedGrantSurface } from "@intx/types";
import type { AssetService } from "@intx/hub-sessions";
import { createMailTriggeredRunGrantsMaterializer } from "@intx/hub-api";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedGrant,
  seedPrincipal,
  seedTenants,
} from "@intx/test-harness/seed";

const TENANT = "tnt";
const ASSET = "ast";
const DEFINITION = "wfd_real";
const DEPLOYMENT = "run_real";
const WORKFLOW_ADDRESS = "run_real@tenant.example";
const CREATOR = "prn_creator";
const RUN_A = "<mail-run-A@tenant.example>";
const RUN_B = "<mail-run-B@tenant.example>";

function childWorkflowParentJson(): string {
  return JSON.stringify({
    id: "wf_child_parent",
    triggers: [{ type: "mail", to: WORKFLOW_ADDRESS }],
    stepOrder: ["sub"],
    steps: {
      sub: {
        kind: "childWorkflow",
        id: "sub",
        definitionRef: "ast_child",
        drainBehavior: "cancel",
      },
    },
  });
}

function mockAssetService(json: string): AssetService {
  function notImpl(name: string): never {
    throw new Error(`mock: assetService.${name} not implemented`);
  }
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- only readAssetBlob is exercised by the materializer
  return {
    createAsset: () => notImpl("createAsset"),
    populateAsset: () => notImpl("populateAsset"),
    readAssetBlob: async () => new TextEncoder().encode(json),
  } as unknown as AssetService;
}

describe.skipIf(!harnessDbEnvAvailable())(
  "childWorkflow ceiling is read per run, not frozen across a rollback",
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
        id: CREATOR,
        tenantId: TENANT,
        kind: "user",
        refId: "creator-user",
      });
      await seedAsset(h.db, {
        id: ASSET,
        tenantId: TENANT,
        kind: "workflow",
        name: ASSET,
        creatorPrincipalId: CREATOR,
      });
      await h.db.insert(workflowDefinition).values({
        id: DEFINITION,
        tenantId: TENANT,
        name: DEFINITION,
        assetId: ASSET,
        currentVersion: "1",
      });
      await h.db.insert(workflowRun).values({
        id: DEPLOYMENT,
        tenantId: TENANT,
        anchorRunId: DEPLOYMENT,
        definitionId: DEFINITION,
        address: WORKFLOW_ADDRESS,
        status: "running",
      });
      await seedGrant(h.db, {
        id: "grt_creator_vault",
        tenantId: TENANT,
        principalId: CREATOR,
        resource: "secret:vault",
        action: "use",
        effect: "allow",
        origin: "creator",
      });
    });

    test("a run after a rollback uses the current version's surface, not the frozen one", async () => {
      // Version 1 carries broad child authority; version 2 (the current version
      // after an operator rollback) carries narrow.
      const broad: ApprovedGrantSurface = {
        grants: ["tool:broad_tool"],
        grantEffects: { "tool:broad_tool": "allow" },
      };
      const narrow: ApprovedGrantSurface = {
        grants: ["tool:narrow_tool"],
        grantEffects: { "tool:narrow_tool": "allow" },
      };
      await h.db.insert(workflowDefinitionVersion).values([
        { id: "wdv_v1", definitionId: DEFINITION, version: "1" },
        { id: "wdv_v2", definitionId: DEFINITION, version: "2" },
      ]);
      await writeApprovedGrantSurface(h.db, DEFINITION, "1", broad);
      await writeApprovedGrantSurface(h.db, DEFINITION, "2", narrow);

      // One materializer: its per-definition basis cache lives for the whole
      // process, as the hub's long-lived sidecar router holds it.
      const materialize = createMailTriggeredRunGrantsMaterializer({
        db: h.db,
        assetService: mockAssetService(childWorkflowParentJson()),
        grantStore: createGrantStore(h.db),
      });

      // Run A materializes while currentVersion = 1.
      const resultA = await materialize({
        agentAddress: WORKFLOW_ADDRESS,
        runId: RUN_A,
      });
      expect(resultA.outcome).toBe("materialized");

      // An operator rolls the definition's current version forward to 2.
      await h.db
        .update(workflowDefinition)
        .set({ currentVersion: "2" })
        .where(eq(workflowDefinition.id, DEFINITION));

      // Run B is a brand-new top-level run of the same deployment.
      const resultB = await materialize({
        agentAddress: WORKFLOW_ADDRESS,
        runId: RUN_B,
      });
      expect(resultB.outcome).toBe("materialized");

      const [principalB] = await h.db
        .select()
        .from(principal)
        .where(eq(principal.refId, RUN_B));
      const grantsB = await h.db
        .select()
        .from(grant)
        .where(eq(grant.principalId, principalB?.id ?? ""));
      const resourcesB = grantsB.map((row) => row.resource).sort();

      // Run B must carry version 2's narrow authority, never version 1's broad
      // ceiling. A stale frozen ceiling here would be a fail-open over-grant.
      expect(resourcesB).toEqual(["tool:narrow_tool"]);
      expect(resourcesB).not.toContain("tool:broad_tool");
    });
  },
);

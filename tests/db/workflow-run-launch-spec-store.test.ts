import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { createWorkflowRunLaunchSpecStore } from "@intx/db";
import { workflowDefinition } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedPrincipal,
  seedTenants,
  seedWorkflowRun,
} from "@intx/test-harness/seed";

const TENANT_ID = "tnt-launch-spec";
const DEFINITION_ID = "wfd-launch-spec";
const ANCHOR_RUN_ID = "dep-launch-spec";
const PRINCIPAL_ID = "prn-launch-spec";

// A minimal, valid frozen approval bundle -- the recovery input for a provisioned
// deploy. It names the source and carries the inert projection/closure/hash; it
// holds no resolved inference source, so no credential secret can be persisted.
const FROZEN_APPROVAL_BUNDLE = {
  source: {
    kind: "asset" as const,
    assetId: "ast-launch-spec",
    package: { format: "source" as const, commitSha: "c0ffee".padEnd(40, "0") },
  },
  entry: "./workflow.mjs",
  projection: {
    id: "workflow",
    triggers: [] as unknown[],
    stepOrder: [] as string[],
    steps: {},
  },
  closure: { schemaVersion: "1" as const, topLevel: [], entries: [] },
  approvedWireHash: "a".repeat(64),
  approvedGrants: [] as string[],
};

describe.skipIf(!harnessDbEnvAvailable())(
  "workflowRunLaunchSpecStore (real DB)",
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
      await seedTenants(h.db, [{ id: TENANT_ID }]);
      await seedPrincipal(h.db, {
        id: PRINCIPAL_ID,
        tenantId: TENANT_ID,
        kind: "user",
        refId: "user-launch-spec",
        status: "active",
      });
      await h.db.insert(workflowDefinition).values({
        id: DEFINITION_ID,
        tenantId: TENANT_ID,
        name: DEFINITION_ID,
      });
      await seedWorkflowRun(h.db, {
        id: ANCHOR_RUN_ID,
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        definitionId: DEFINITION_ID,
      });
    });

    test("round-trips immutable recovery inputs without source secrets", async () => {
      const store = createWorkflowRunLaunchSpecStore(h.db);
      await store.create({
        anchorRunId: ANCHOR_RUN_ID,
        sessionId: "ses-launch-spec",
        deploymentDomain: "tenant.example",
        sourceAuthorityPrincipalId: PRINCIPAL_ID,
        frozenApprovalBundle: FROZEN_APPROVAL_BUNDLE,
        sourceOfferingIds: ["offering-primary", "offering-fallback"],
        defaultSourceOfferingId: "offering-primary",
        deployContent: { systemPrompt: "" },
        toolPackagePins: [{ name: "@intx/search", version: "^1.2.0" }],
      });

      const stored = await store.get(ANCHOR_RUN_ID);
      expect(stored).not.toBeNull();
      expect(stored?.schemaVersion).toBe(1);
      expect(stored?.sessionId).toBe("ses-launch-spec");
      expect(stored?.sourceOfferingIds).toEqual([
        "offering-primary",
        "offering-fallback",
      ]);
      expect(stored?.defaultSourceOfferingId).toBe("offering-primary");
      expect(stored?.frozenApprovalBundle).toEqual(FROZEN_APPROVAL_BUNDLE);
      expect(stored).not.toHaveProperty("sources");
      expect(stored).not.toHaveProperty("apiKey");
    });

    test("returns null for an anchor without a launch specification", async () => {
      const store = createWorkflowRunLaunchSpecStore(h.db);
      expect(await store.get("missing-anchor")).toBeNull();
    });

    test("rejects a default source outside the pinned offering ids", async () => {
      const store = createWorkflowRunLaunchSpecStore(h.db);
      await expect(
        store.create({
          anchorRunId: ANCHOR_RUN_ID,
          sessionId: "ses-launch-spec",
          deploymentDomain: "tenant.example",
          sourceAuthorityPrincipalId: PRINCIPAL_ID,
          frozenApprovalBundle: FROZEN_APPROVAL_BUNDLE,
          sourceOfferingIds: ["offering-primary"],
          defaultSourceOfferingId: "offering-missing",
          deployContent: { systemPrompt: "" },
        }),
      ).rejects.toThrow(/default source.*is not in/);
      expect(await store.get(ANCHOR_RUN_ID)).toBeNull();
    });
  },
);

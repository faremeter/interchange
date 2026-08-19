import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { createEnvKeyCredentialCipher } from "@intx/crypto";
import {
  createSidecarAllocationStore,
  createWorkflowRunLaunchSpecStore,
} from "@intx/db";
import { workflowDefinition, workflowRun } from "@intx/db/schema";
import { eq } from "drizzle-orm";
import {
  createWorkflowAllocationService,
  SessionLaunchError,
  type DeployPreparedCodeSourcedWorkflowParams,
  type InstallAndApproveResult,
  type SidecarProvisioner,
} from "@intx/hub-sessions";
import { credentialAad } from "@intx/types";
import type { WorkflowDefinitionSource } from "@intx/types/workflow-sources";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedCredential,
  seedModel,
  seedModelOffering,
  seedModelProvider,
  seedPrincipal,
  seedProvider,
  seedTenants,
} from "@intx/test-harness/seed";

const TENANT_ID = "tnt-workflow-allocation";
const PRINCIPAL_ID = "prn-workflow-allocation";
const ASSET_ID = "ast-workflow-allocation";
const DEFINITION_ID = "wfd-workflow-allocation";
const OFFERING_ID = "mof-workflow-allocation";
const CREDENTIAL_ID = "cred-workflow-allocation";
const CREDENTIAL_SECRET = "secret-only-resolved-at-launch";
const CREDENTIAL_CIPHER = createEnvKeyCredentialCipher(
  new Uint8Array(32).fill(7),
);
const DEPLOY_COMMIT = "c0ffee".padEnd(40, "0");

const provisioner: SidecarProvisioner = {
  id: "test-provisioner",
  apiVersion: 1,
  bindingFingerprint: "test-provisioner:v1",
  async ensure() {
    return { kind: "accepted" };
  },
  async destroy() {
    return { kind: "destroyed" };
  },
};

// A code-sourced deploy: the workflow asset's own git subtree at a pinned
// commit, evaluated at `entry`. The exclusive prepare freezes the probe of THIS
// source on shared capacity, then deploys the frozen bundle to the allocation.
const SOURCE: WorkflowDefinitionSource = {
  kind: "asset",
  assetId: ASSET_ID,
  package: { format: "source", commitSha: DEPLOY_COMMIT },
};
const ENTRY = "./workflow.mjs";

// A minimal, valid frozen approval bundle: the projection and closure are the
// smallest shapes their arktype schemas accept, and the grant set + wire hash
// are inert strings. No credential secret ever enters the bundle -- sources are
// re-resolved from the launch spec's offering ids at deploy time.
function frozenApproval(): InstallAndApproveResult {
  return {
    approval: {
      ok: true,
      definitionId: DEFINITION_ID,
      approvedWireHash: "a".repeat(64),
      approvedGrants: new Set<string>(),
      projection: { id: "wf-x", triggers: [], stepOrder: [], steps: {} },
    },
    projection: { id: "wf-x", triggers: [], stepOrder: [], steps: {} },
    closure: { schemaVersion: "1", topLevel: [], entries: [] },
  };
}

describe.skipIf(!harnessDbEnvAvailable())(
  "workflow allocation service (real DB)",
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
      });
      await seedAsset(h.db, {
        id: ASSET_ID,
        tenantId: TENANT_ID,
        kind: "workflow",
        name: "workflow-allocation",
        creatorPrincipalId: PRINCIPAL_ID,
      });
      await seedProvider(h.db, {
        id: "prv-workflow-allocation",
        tenantId: TENANT_ID,
        name: "credential-provider",
      });
      await seedCredential(h.db, {
        id: CREDENTIAL_ID,
        tenantId: TENANT_ID,
        providerId: "prv-workflow-allocation",
        name: "credential",
        secret: await CREDENTIAL_CIPHER.encrypt(
          CREDENTIAL_SECRET,
          credentialAad(CREDENTIAL_ID, "secret"),
        ),
      });
      await seedModel(h.db, {
        id: "mdl-workflow-allocation",
        tenantId: TENANT_ID,
        canonicalName: "opus",
      });
      await seedModelProvider(h.db, {
        id: "mpv-workflow-allocation",
        tenantId: TENANT_ID,
        name: "anthropic",
        credentialId: CREDENTIAL_ID,
      });
      await seedModelOffering(h.db, {
        id: OFFERING_ID,
        tenantId: TENANT_ID,
        modelId: "mdl-workflow-allocation",
        providerId: "mpv-workflow-allocation",
      });
    });

    test("freezes a secret-free bundle at prepare and deploys it for the accepted generation", async () => {
      const installCalls: {
        source: WorkflowDefinitionSource;
        entry: string;
      }[] = [];
      const deployCalls: DeployPreparedCodeSourcedWorkflowParams[] = [];
      let workflowActive = false;
      const service = createWorkflowAllocationService({
        db: h.db,
        plugins: {
          getDefaultProvisioner: () => provisioner,
          getProvisioner: (id) => (id === provisioner.id ? provisioner : null),
        },
        preparedDeployer: {
          // The exclusive prepare freezes the definition on shared capacity: the
          // real path persists a workflow_definition row and returns its id, so
          // the mock does the same (the anchor row FKs to it).
          installAndApproveWorkflowSource: async (params) => {
            installCalls.push({ source: params.source, entry: params.entry });
            await h.db
              .insert(workflowDefinition)
              .values({
                id: DEFINITION_ID,
                tenantId: TENANT_ID,
                name: "workflow-allocation-def",
              })
              .onConflictDoNothing({ target: workflowDefinition.id });
            return frozenApproval();
          },
          deployPreparedCodeSourcedWorkflow: async (params) => {
            deployCalls.push(params);
            const result = {
              anchorRunId: params.anchorRunId,
              deploymentAddress: params.agentAddress,
              publicKey: "supervisor-public-key",
            };
            await h.db
              .update(workflowRun)
              .set({ publicKey: result.publicKey })
              .where(eq(workflowRun.id, params.anchorRunId));
            return result;
          },
          deployPreparedWorkflowDefinition: () => {
            throw new Error(
              "deployPreparedWorkflowDefinition must not be called on the code-sourced exclusive path",
            );
          },
        },
        credentialCipher: CREDENTIAL_CIPHER,
        allocationRouter: {
          isAllocatedWorkflowActive: async () => workflowActive,
        },
        createAllocationId: () => "sal-workflow-allocation",
        now: () => new Date("2026-08-03T12:00:00.000Z"),
      });

      const prepared = await service.prepareExclusiveDeployment({
        tenantId: TENANT_ID,
        anchorRunId: "dep-workflow-allocation",
        deploymentDomain: `${TENANT_ID}.example.test`,
        source: SOURCE,
        entry: ENTRY,
        definitionAssetId: ASSET_ID,
        placement: { sharing: "exclusive", reuse: "same-deployment" },
        sessionId: "ses-workflow-allocation",
        sourceAuthorityPrincipalId: PRINCIPAL_ID,
        sourceOfferingIds: [OFFERING_ID],
        defaultSourceOfferingId: OFFERING_ID,
        deployContent: { systemPrompt: "" },
      });

      expect(prepared.status).toBe("pending");
      // The probe/freeze rode shared capacity at prepare time, carrying the
      // caller's source + entry.
      expect(installCalls).toEqual([{ source: SOURCE, entry: ENTRY }]);

      const spec = await createWorkflowRunLaunchSpecStore(h.db).get(
        prepared.anchorRunId,
      );
      expect(spec?.sourceOfferingIds).toEqual([OFFERING_ID]);
      // The frozen bundle is the recovery input; it names the source but holds
      // no resolved inference source, so no credential secret is persisted.
      expect(spec?.frozenApprovalBundle.source).toEqual(SOURCE);
      expect(spec?.frozenApprovalBundle.approvedWireHash).toBe("a".repeat(64));
      expect(JSON.stringify(spec)).not.toContain(CREDENTIAL_SECRET);

      const allocations = createSidecarAllocationStore(h.db);
      const bound = await allocations.bindInitialSidecar({
        allocationId: prepared.allocationId,
        expectedGeneration: 0,
        sidecarId: "sc-workflow-allocation",
        tokenHashSha256: new Uint8Array([1, 2, 3]),
        connectDeadline: new Date("2026-08-03T12:02:00.000Z"),
      });
      expect(bound?.generation).toBe(1);
      const allocated = await allocations.markAllocated({
        allocationId: prepared.allocationId,
        generation: 1,
      });
      if (allocated === null) throw new Error("allocation was not accepted");

      await service.deployReadyAllocation(allocated);

      expect(deployCalls).toHaveLength(1);
      expect(deployCalls[0]?.allocationTarget).toEqual({
        allocationId: prepared.allocationId,
        generation: 1,
      });
      expect(deployCalls[0]?.source).toEqual(SOURCE);
      // The inference chain is re-resolved from the launch spec's offering ids
      // at deploy time, decrypting the credential secret only now.
      expect(deployCalls[0]?.config).toMatchObject({
        sessionId: "ses-workflow-allocation",
        principalId: PRINCIPAL_ID,
        defaultSource: OFFERING_ID,
        sources: [
          {
            id: OFFERING_ID,
            apiKey: CREDENTIAL_SECRET,
            model: "opus",
          },
        ],
      });

      workflowActive = true;
      await expect(
        service.deployReadyAllocation(allocated),
      ).resolves.toBeNull();
      expect(deployCalls).toHaveLength(1);

      await h.db
        .update(workflowRun)
        .set({ publicKey: null })
        .where(eq(workflowRun.id, prepared.anchorRunId));
      const error = await service
        .deployReadyAllocation(allocated)
        .catch((cause: unknown) => cause);
      expect(error).toBeInstanceOf(SessionLaunchError);
      if (!(error instanceof SessionLaunchError)) {
        throw new Error("unreachable");
      }
      expect(error.leakedAgent).toBe(true);
      expect(deployCalls).toHaveLength(1);
    });
  },
);

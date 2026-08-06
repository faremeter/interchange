import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { defineAgent } from "@intx/agent";
import { createEnvKeyCredentialCipher } from "@intx/crypto";
import {
  createSidecarAllocationStore,
  createWorkflowRunLaunchSpecStore,
} from "@intx/db";
import { workflowRun } from "@intx/db/schema";
import { eq } from "drizzle-orm";
import {
  createWorkflowAllocationService,
  SessionLaunchError,
  type DeployPreparedWorkflowDefinitionParams,
  type SidecarProvisioner,
} from "@intx/hub-sessions";
import { credentialAad } from "@intx/types";
import { defineWorkflow, step } from "@intx/workflow/definition";
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
const OFFERING_ID = "mof-workflow-allocation";
const CREDENTIAL_ID = "cred-workflow-allocation";
const CREDENTIAL_SECRET = "secret-only-resolved-at-launch";
const CREDENTIAL_CIPHER = createEnvKeyCredentialCipher(
  new Uint8Array(32).fill(7),
);

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

const agent = defineAgent({
  id: "worker",
  systemPrompt: "Do the work",
  tools: [],
  capabilities: [],
  inference: { sources: [{ provider: "anthropic", model: "opus" }] },
});

const definition = defineWorkflow({
  id: "wf-workflow-allocation",
  trigger: { type: "manual" },
  sidecarPlacement: { sharing: "exclusive", reuse: "same-deployment" },
  steps: { work: step({ agent, after: [] }) },
});

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

    test("persists a secret-free launch spec and reconstructs it for the accepted generation", async () => {
      const deployCalls: DeployPreparedWorkflowDefinitionParams[] = [];
      let workflowActive = false;
      const service = createWorkflowAllocationService({
        db: h.db,
        plugins: {
          getDefaultProvisioner: () => provisioner,
          getProvisioner: (id) => (id === provisioner.id ? provisioner : null),
        },
        preparedDeployer: {
          deployPreparedWorkflowDefinition: async (params) => {
            deployCalls.push(params);
            const result = {
              deploymentId: params.deploymentId,
              deploymentAddress: params.config.agentAddress,
              publicKey: "supervisor-public-key",
            };
            await h.db
              .update(workflowRun)
              .set({ publicKey: result.publicKey })
              .where(eq(workflowRun.id, params.deploymentId));
            return result;
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
        deploymentId: "dep-workflow-allocation",
        deploymentDomain: `${TENANT_ID}.example.test`,
        definition,
        definitionAssetId: ASSET_ID,
        placement: { sharing: "exclusive", reuse: "same-deployment" },
        sessionId: "ses-workflow-allocation",
        sourceAuthorityPrincipalId: PRINCIPAL_ID,
        sourceOfferingIds: [OFFERING_ID],
        defaultSourceOfferingId: OFFERING_ID,
        deployContent: { systemPrompt: "" },
      });

      expect(prepared.status).toBe("pending");
      const spec = await createWorkflowRunLaunchSpecStore(h.db).get(
        prepared.deploymentId,
      );
      expect(spec?.sourceOfferingIds).toEqual([OFFERING_ID]);
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
        .where(eq(workflowRun.id, prepared.deploymentId));
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

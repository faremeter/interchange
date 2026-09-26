import {
  DEFAULT_ASSET_REF,
  deployCodeSourcedWorkflow,
  type PreparedWorkflowDeployer,
  type RepoId,
  type SessionService,
  type WorkflowAllocationService,
} from "@intx/hub-sessions";
import type { CredentialCipher } from "@intx/types";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import {
  buildInertProjectionStepSources,
  deriveRunAddress,
} from "@intx/workflow-deploy";
import type { TestDb } from "@intx/test-harness/db-harness";

import {
  seedInferenceCredentials,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";

export type ProvisionedDeploymentObservation = {
  anchorRunId: string;
  agentAddress: string;
  projectionId: string;
};

export function createProvisionedDeploymentStandIn(opts: {
  db: TestDb["db"];
  env: DeployFlowEnv;
  sessionService: SessionService & PreparedWorkflowDeployer;
  inferenceSource: InferenceSource;
  sourceRepoKind: RepoId["kind"];
  credentialCipher: CredentialCipher;
  seedInferenceCredentials: boolean;
  onDeployed?: (deployment: ProvisionedDeploymentObservation) => void;
}): WorkflowAllocationService {
  return {
    async prepareProvisionedDeployment(args) {
      if (args.source.kind !== "asset") {
        throw new Error("route-deploy test requires an asset source");
      }
      const source = args.source;
      const agentAddress = deriveRunAddress({
        runId: args.anchorRunId,
        domain: args.deploymentDomain,
      });
      const allocationTarget = opts.env.hub.prepareAllocationIdentity(
        args.anchorRunId,
        agentAddress,
      );
      const approved =
        await opts.sessionService.installAndApproveWorkflowSource({
          source,
          entry: args.entry,
          ...(args.pin !== undefined ? { pin: args.pin } : {}),
          definitionAssetId: args.definitionAssetId,
          allocationTarget,
        });
      if (!approved.approval.ok) {
        throw new Error(
          `route-deploy probe was not approved: ${approved.approval.reason}`,
        );
      }
      const config: HarnessConfig = {
        sessionId: args.sessionId,
        agentId: args.anchorRunId,
        tenantId: args.tenantId,
        principalId: args.sourceAuthorityPrincipalId,
        agentAddress,
        systemPrompt: "",
        tools: [],
        grants: [],
        sources: [opts.inferenceSource],
        defaultSource: opts.inferenceSource.id,
      };
      const sources = buildInertProjectionStepSources({
        projection: approved.projection,
        config,
        operatorApprovals: approved.approval.approvedSurface,
      });
      if (opts.seedInferenceCredentials) {
        await seedInferenceCredentials(opts.db, args.tenantId, sources, config);
      }

      const repoId: RepoId = {
        kind: opts.sourceRepoKind,
        id: source.assetId,
      };
      const resolveAttachment = async (requestedAssetId: string) => {
        if (requestedAssetId !== source.assetId) {
          throw new Error(
            `route-deploy test received unexpected asset ${requestedAssetId}`,
          );
        }
        const commitSha =
          await opts.env.hub.agentRepoStore.repoStore.resolveRef(
            { kind: "hub" },
            repoId,
            DEFAULT_ASSET_REF,
          );
        if (commitSha === null) {
          throw new Error(`route-deploy source ${requestedAssetId} is empty`);
        }
        const { pack, ref } =
          await opts.env.hub.agentRepoStore.repoStore.createPack(
            { kind: "hub" },
            repoId,
            DEFAULT_ASSET_REF,
          );
        return { pack, ref, commitSha };
      };
      await deployCodeSourcedWorkflow({
        approved,
        source,
        resolveAttachment,
        sidecarAllocationRouter: opts.env.hub.router,
        allocationTarget,
        agentAddress,
        config,
        sources,
        db: opts.db,
        tenantId: args.tenantId,
        anchorRunId: args.anchorRunId,
        deploymentDomain: args.deploymentDomain,
        credentialCipher: opts.credentialCipher,
      });
      opts.onDeployed?.({
        anchorRunId: args.anchorRunId,
        agentAddress,
        projectionId: approved.projection.id,
      });
      return {
        anchorRunId: args.anchorRunId,
        deploymentAddress: agentAddress,
        allocationId: allocationTarget.allocationId,
        status: "pending",
      };
    },
    async deployReadyAllocation() {
      throw new Error("route-deploy test does not reconcile allocations");
    },
  };
}

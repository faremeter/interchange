import {
  createSidecarAllocationStore,
  createWorkflowRunLaunchSpecStore,
  resolveTenantSidecarCapabilityPolicies,
  resolveSourcesByOfferingIds,
  type DB,
  type SidecarAllocation,
} from "@intx/db";
import { grant, workflowRun } from "@intx/db/schema";
import { eq } from "drizzle-orm";
import { generateId } from "@intx/hub-common";
import { hexEncode, type CredentialCipher } from "@intx/types";
import type { FrozenApprovalBundle } from "@intx/types/sidecar";
import type { HarnessConfig } from "@intx/types/runtime";
import type { ToolPackagePin } from "@intx/types/tool-packages";
import type { WorkflowDefinitionSource } from "@intx/types/workflow-sources";
import { deriveRunAddress, deriveRunAgentId } from "@intx/workflow-deploy";

import type { DeployContent } from "./agent-repo";
import type {
  SidecarCapabilityMismatch,
  SidecarPluginRegistry,
} from "./sidecar-allocation";
import {
  SessionLaunchError,
  type DeployWorkflowDefinitionResult,
  type PreparedWorkflowDeployer,
} from "./session-service";
import type { SidecarAllocationRouter } from "./ws/sidecar-handler";
import type { InstallAndApproveResult } from "./workflow-probe-gate";

export class WorkflowProvisioningError extends Error {
  readonly code: string;

  constructor(code: string, message: string) {
    super(message);
    this.name = "WorkflowProvisioningError";
    this.code = code;
  }
}

export type PrepareProvisionedWorkflowDeploymentArgs = {
  readonly tenantId: string;
  readonly anchorRunId: string;
  readonly deploymentDomain: string;
  /** Where the definition's bytes come from at probe time. */
  readonly source: WorkflowDefinitionSource;
  /** The `interchange.workflow` entry-module path the sidecar evaluates. */
  readonly entry: string;
  /**
   * A `name@range` spec for the definition package. REQUIRED for the `registry`
   * and asset-`tarball` variants; omitted for the asset-`source` variant.
   */
  readonly pin?: string;
  readonly definitionAssetId: string;
  readonly sessionId: string;
  readonly sourceAuthorityPrincipalId: string;
  readonly sourceOfferingIds: readonly string[];
  readonly defaultSourceOfferingId: string;
  readonly deployContent: DeployContent;
  readonly toolPackagePins?: readonly ToolPackagePin[];
};

export type PreparedProvisionedWorkflowDeployment = {
  readonly anchorRunId: string;
  readonly deploymentAddress: string;
  readonly allocationId: string;
  readonly status: "pending";
};

export type WorkflowAllocationService = {
  prepareProvisionedDeployment(
    args: PrepareProvisionedWorkflowDeploymentArgs,
  ): Promise<PreparedProvisionedWorkflowDeployment>;
  deployReadyAllocation(
    allocation: SidecarAllocation,
  ): Promise<DeployWorkflowDefinitionResult | null>;
};

export type WorkflowAllocationServiceDeps = {
  readonly db: DB["db"];
  readonly plugins: SidecarPluginRegistry;
  readonly preparedDeployer: PreparedWorkflowDeployer;
  readonly credentialCipher: CredentialCipher;
  readonly allocationRouter: Pick<
    SidecarAllocationRouter,
    "isAllocatedWorkflowActive"
  >;
  readonly createAllocationId?: () => string;
  readonly now?: () => Date;
};

function randomAllocationId(): string {
  return `sal_${hexEncode(crypto.getRandomValues(new Uint8Array(16)))}`;
}

export function createWorkflowAllocationService({
  db,
  plugins,
  preparedDeployer,
  credentialCipher,
  allocationRouter,
  createAllocationId = randomAllocationId,
  now = () => new Date(),
}: WorkflowAllocationServiceDeps): WorkflowAllocationService {
  const allocationStore = createSidecarAllocationStore(db);
  const launchSpecStore = createWorkflowRunLaunchSpecStore(db);

  async function prepareProvisionedDeployment(
    args: PrepareProvisionedWorkflowDeploymentArgs,
  ): Promise<PreparedProvisionedWorkflowDeployment> {
    if (args.sourceOfferingIds.length === 0) {
      throw new WorkflowProvisioningError(
        "invalid_source_offerings",
        "Provisioned workflow deployment requires at least one catalog offering",
      );
    }
    if (!args.sourceOfferingIds.includes(args.defaultSourceOfferingId)) {
      throw new WorkflowProvisioningError(
        "invalid_source_offerings",
        "The default source offering must be present in the source offering chain",
      );
    }

    const sourceCheck = await resolveSourcesByOfferingIds(
      db,
      args.tenantId,
      args.sourceOfferingIds,
      credentialCipher,
    );
    if (!sourceCheck.ok) {
      throw new WorkflowProvisioningError(
        "source_offering_unavailable",
        `Catalog offering ${sourceCheck.offeringId} cannot be used by the deployment authority`,
      );
    }

    // Probe + gate + freeze the code-sourced definition once at request time.
    // The freeze is sidecar-agnostic (a wire hash
    // over the inert projection plus the resolved closure), so the frozen bundle
    // deploys later to the dedicated allocation with no re-probe. A non-approval
    // surfaces as a `WorkflowDefinitionInvalidError` from the deployer, which the
    // route maps to a 409.
    const [approved, tenantPolicies] = await Promise.all([
      preparedDeployer.installAndApproveWorkflowSource({
        source: args.source,
        entry: args.entry,
        ...(args.pin !== undefined ? { pin: args.pin } : {}),
        definitionAssetId: args.definitionAssetId,
      }),
      resolveTenantSidecarCapabilityPolicies(db, args.tenantId),
    ]);
    if (!approved.approval.ok) {
      // `installAndApproveWorkflowSource` already fails closed on a non-approval;
      // restate the narrowing so the frozen bundle below reads the ok arm.
      throw new Error(
        "prepareProvisionedDeployment: install did not yield an approved definition",
      );
    }
    // Capture the narrowed values before the transaction: TS drops the
    // `approval.ok` narrowing inside the async callback below.
    const definitionId = approved.approval.definitionId;
    const selection = plugins.selectProvisioner({
      tenantPolicies,
      workflowRules: approved.projection.sidecarPlacement?.capabilities ?? [],
    });
    if (!selection.ok) {
      if (selection.reason === "ambiguous") {
        throw new WorkflowProvisioningError(
          "provisioner_ambiguous",
          `Multiple sidecar provisioners satisfy the deployment: ${selection.provisionerIds.join(", ")}`,
        );
      }
      throw new WorkflowProvisioningError(
        "provisioner_no_match",
        formatProvisionerMismatches(selection.mismatches),
      );
    }
    const provisioner = selection.provisioner;
    const frozenApprovalBundle: FrozenApprovalBundle = {
      source: args.source,
      entry: args.entry,
      projection: approved.projection,
      closure: approved.closure,
      approvedWireHash: approved.approval.approvedWireHash,
      approvedGrants: [...approved.approval.approvedGrants],
    };

    const allocationId = createAllocationId();
    const createdAt = now();
    const deploymentAddress = deriveRunAddress({
      runId: args.anchorRunId,
      domain: args.deploymentDomain,
    });
    await db.transaction(async (tx) => {
      // The freeze already ensured (create-if-absent) the definition row keyed by
      // the approved wire hash and returned its id; anchor to THAT row rather
      // than re-ensuring, so the anchor's definition is exactly the one approved.
      await tx.insert(workflowRun).values({
        id: args.anchorRunId,
        tenantId: args.tenantId,
        anchorRunId: args.anchorRunId,
        definitionId,
        address: deploymentAddress,
        // Born "deployed": the anchor is live but pre-trigger. The first trigger
        // flips it to "running" (see `anchorWithPrincipal`).
        status: "deployed",
        createdAt,
      });
      await tx.insert(grant).values({
        id: generateId("grant"),
        tenantId: args.tenantId,
        principalId: args.sourceAuthorityPrincipalId,
        resource: `workflow-run:${args.anchorRunId}`,
        action: "read",
        effect: "allow",
        origin: "creator",
        createdAt,
        updatedAt: createdAt,
      });
      await launchSpecStore.create(
        {
          anchorRunId: args.anchorRunId,
          sessionId: args.sessionId,
          deploymentDomain: args.deploymentDomain,
          sourceAuthorityPrincipalId: args.sourceAuthorityPrincipalId,
          frozenApprovalBundle,
          sourceOfferingIds: [...args.sourceOfferingIds],
          defaultSourceOfferingId: args.defaultSourceOfferingId,
          deployContent: args.deployContent,
          ...(args.toolPackagePins !== undefined
            ? { toolPackagePins: [...args.toolPackagePins] }
            : {}),
          createdAt,
        },
        tx,
      );
      await allocationStore.createPending(
        {
          id: allocationId,
          anchorRunId: args.anchorRunId,
          tenantId: args.tenantId,
          provisionerId: provisioner.id,
          provisionerApiVersion: provisioner.apiVersion,
          provisionerBindingFingerprint: provisioner.bindingFingerprint,
          now: createdAt,
        },
        tx,
      );
    });

    return {
      anchorRunId: args.anchorRunId,
      deploymentAddress,
      allocationId,
      status: "pending",
    };
  }

  async function deployReadyAllocation(
    allocation: SidecarAllocation,
  ): Promise<DeployWorkflowDefinitionResult | null> {
    if (
      allocation.status !== "allocated" ||
      allocation.ensureAcceptedGeneration !== allocation.generation
    ) {
      throw new Error(
        `Allocation ${allocation.id} generation ${String(allocation.generation)} is not accepted for deployment`,
      );
    }
    const allocationTarget = {
      allocationId: allocation.id,
      generation: allocation.generation,
    };
    const anchor = await db.query.workflowRun.findFirst({
      where: eq(workflowRun.id, allocation.anchorRunId),
      columns: { publicKey: true, definitionId: true },
    });
    if (anchor === undefined) {
      throw new Error(`Allocation ${allocation.id} has no workflow anchor run`);
    }
    if (await allocationRouter.isAllocatedWorkflowActive(allocationTarget)) {
      if (anchor.publicKey !== null) return null;
      throw new SessionLaunchError(
        "provision",
        new Error(
          `Allocation ${allocation.id} has an active workflow without a completed initialization key`,
        ),
        true,
      );
    }
    if (anchor.definitionId === null) {
      throw new Error(
        `Allocation ${allocation.id} anchor run has no frozen workflow definition`,
      );
    }
    const spec = await launchSpecStore.get(allocation.anchorRunId);
    if (spec === null) {
      throw new Error(
        `Allocation ${allocation.id} has no workflow launch specification`,
      );
    }
    // The frozen bundle deploys verbatim -- no re-probe. Rehydrate the approval
    // hand-off from it: the approved grant set becomes a `Set`, and the frozen
    // definition id is the anchor's own (set at prepare time from this freeze).
    const bundle = spec.frozenApprovalBundle;
    const approved: InstallAndApproveResult = {
      approval: {
        ok: true,
        definitionId: anchor.definitionId,
        approvedWireHash: bundle.approvedWireHash,
        approvedGrants: new Set(bundle.approvedGrants),
        projection: bundle.projection,
      },
      projection: bundle.projection,
      closure: bundle.closure,
    };
    // Re-resolve the inference chain from the catalog at launch time -- the
    // launch spec stores offering ids, never resolved sources, so a rotated
    // credential is picked up here and no secret was ever persisted.
    const resolved = await resolveSourcesByOfferingIds(
      db,
      allocation.tenantId,
      spec.sourceOfferingIds,
      credentialCipher,
    );
    if (!resolved.ok) {
      throw new Error(
        `Catalog offering ${resolved.offeringId} is unavailable while recovering allocation ${allocation.id}`,
      );
    }
    const defaultSource = resolved.sources.find(
      (source) => source.id === spec.defaultSourceOfferingId,
    );
    if (defaultSource === undefined) {
      throw new Error(
        `Default offering ${spec.defaultSourceOfferingId} was not resolved for allocation ${allocation.id}`,
      );
    }
    const deploymentAddress = deriveRunAddress({
      runId: allocation.anchorRunId,
      domain: spec.deploymentDomain,
    });
    const config: HarnessConfig = {
      sessionId: spec.sessionId,
      agentId: deriveRunAgentId({ runId: allocation.anchorRunId }),
      tenantId: allocation.tenantId,
      principalId: spec.sourceAuthorityPrincipalId,
      agentAddress: deploymentAddress,
      systemPrompt: "",
      tools: [],
      grants: [],
      sources: resolved.sources,
      defaultSource: defaultSource.id,
    };

    return preparedDeployer.deployPreparedCodeSourcedWorkflow({
      tenantId: allocation.tenantId,
      anchorRunId: allocation.anchorRunId,
      deploymentDomain: spec.deploymentDomain,
      agentAddress: deploymentAddress,
      source: bundle.source,
      approved,
      config,
      allocationTarget,
      credentialCipher,
    });
  }

  return { prepareProvisionedDeployment, deployReadyAllocation };
}

function formatProvisionerMismatches(
  mismatches: Readonly<Record<string, readonly SidecarCapabilityMismatch[]>>,
): string {
  const details = Object.entries(mismatches)
    .sort(([left], [right]) => left.localeCompare(right))
    .flatMap(([provisionerId, failures]) =>
      failures.map(
        ({ capability, expected, actual }) =>
          `${provisionerId}: ${capability} must be ${expected}, was ${actual}`,
      ),
    );
  return details.length === 0
    ? "No sidecar provisioners are registered"
    : `No sidecar provisioner satisfies the deployment (${details.join("; ")})`;
}

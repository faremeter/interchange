import { type } from "arktype";

import {
  createSidecarAllocationStore,
  createWorkflowRunLaunchSpecStore,
  getAncestorChain,
  resolveSourcesByOfferingIds,
  type DB,
  type SidecarAllocation,
} from "@intx/db";
import { grant, tenant as tenantTable, workflowRun } from "@intx/db/schema";
import { eq, inArray } from "drizzle-orm";
import { generateId } from "@intx/hub-common";
import {
  hexEncode,
  TenantConfig,
  type CredentialCipher,
  type SidecarPlacementRequirement,
} from "@intx/types";
import type { HarnessConfig } from "@intx/types/runtime";
import type { ToolPackagePin } from "@intx/types/tool-packages";
import {
  hashDefinition,
  type WorkflowDefinition,
} from "@intx/workflow/definition";
import { computeLiveDefinitionHash } from "@intx/workflow";
import { deriveDeploymentAddress } from "@intx/workflow-deploy";

import type { DeployContent } from "./agent-repo";
import {
  resolveEffectiveSidecarPlacement,
  type SidecarPluginRegistry,
} from "./sidecar-allocation";
import {
  SessionLaunchError,
  type DeployWorkflowDefinitionResult,
  type PreparedWorkflowDeployer,
} from "./session-service";
import type { SidecarAllocationRouter } from "./ws/sidecar-handler";
import { ensureWorkflowDefinitionForAsset } from "./workflow-definition-ensure";
import { workflowDefinitionEnvelopeSchema } from "./workflow-kind";

export class ExclusiveWorkflowPlacementError extends Error {
  readonly code: string;

  constructor(code: string, message: string) {
    super(message);
    this.name = "ExclusiveWorkflowPlacementError";
    this.code = code;
  }
}

export type PrepareExclusiveWorkflowDeploymentArgs = {
  readonly tenantId: string;
  readonly deploymentId: string;
  readonly deploymentDomain: string;
  readonly definition: WorkflowDefinition;
  readonly definitionAssetId: string;
  readonly placement: SidecarPlacementRequirement & {
    readonly sharing: "exclusive";
  };
  readonly sessionId: string;
  readonly sourceAuthorityPrincipalId: string;
  readonly sourceOfferingIds: readonly string[];
  readonly defaultSourceOfferingId: string;
  readonly deployContent: DeployContent;
  readonly toolPackagePins?: readonly ToolPackagePin[];
};

export type PreparedExclusiveWorkflowDeployment = {
  readonly deploymentId: string;
  readonly deploymentAddress: string;
  readonly allocationId: string;
  readonly status: "pending";
};

export type WorkflowAllocationService = {
  prepareExclusiveDeployment(
    args: PrepareExclusiveWorkflowDeploymentArgs,
  ): Promise<PreparedExclusiveWorkflowDeployment>;
  deployReadyAllocation(
    allocation: SidecarAllocation,
  ): Promise<DeployWorkflowDefinitionResult | null>;
};

export type WorkflowAllocationServiceDeps = {
  readonly db: DB["db"];
  readonly plugins: SidecarPluginRegistry;
  readonly preparedDeployer: PreparedWorkflowDeployer;
  readonly credentialCipher?: CredentialCipher;
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

function parseDefinitionSnapshot(snapshot: Record<string, unknown>) {
  const validated = workflowDefinitionEnvelopeSchema(snapshot);
  if (validated instanceof type.errors) {
    throw new Error(
      `Persisted workflow definition failed validation: ${validated.summary}`,
    );
  }
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the persisted snapshot was written from a WorkflowDefinition and the same deployment-envelope schema validates it again at this DB-to-runtime boundary
  return validated as unknown as WorkflowDefinition;
}

export function resolveDeclaredWorkflowSidecarPlacement(
  definition: WorkflowDefinition,
): SidecarPlacementRequirement | undefined {
  const placements: SidecarPlacementRequirement[] = [];

  function visit(current: WorkflowDefinition): void {
    if (current.sidecarPlacement !== undefined) {
      placements.push(current.sidecarPlacement);
    }
    for (const primitive of Object.values(current.steps)) {
      if (primitive.kind === "loop") {
        visit(primitive.body);
      } else if (primitive.kind === "onTrigger" && "inline" in primitive.body) {
        visit(primitive.body.inline);
      }
    }
  }

  visit(definition);
  if (placements.length === 0) return undefined;
  return {
    sharing: "exclusive",
    reuse: placements.every(
      (placement) => placement.reuse === "same-deployment",
    )
      ? "same-deployment"
      : "never",
  };
}

/** Resolve workflow and inherited tenant placement into one launch decision. */
export async function resolveWorkflowSidecarPlacement(
  db: DB["db"],
  tenantId: string,
  definition: WorkflowDefinition,
): Promise<SidecarPlacementRequirement | null> {
  const tenantIds = await getAncestorChain(db, tenantId);
  const rows = await db.query.tenant.findMany({
    where: inArray(tenantTable.id, tenantIds),
    columns: { id: true, config: true },
  });
  const configsByTenantId = new Map(
    rows.map((row) => [
      row.id,
      row.config === null
        ? TenantConfig.assert({})
        : TenantConfig.assert(row.config),
    ]),
  );
  const tenantConfigs = tenantIds.map((id) => {
    const config = configsByTenantId.get(id);
    if (config === undefined) {
      throw new Error(
        `Tenant ${id} disappeared while resolving sidecar placement`,
      );
    }
    return config;
  });
  const workflowPlacement = resolveDeclaredWorkflowSidecarPlacement(definition);
  return resolveEffectiveSidecarPlacement({
    tenantConfigs,
    ...(workflowPlacement !== undefined ? { workflowPlacement } : {}),
  });
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

  async function prepareExclusiveDeployment(
    args: PrepareExclusiveWorkflowDeploymentArgs,
  ): Promise<PreparedExclusiveWorkflowDeployment> {
    const provisioner = plugins.getDefaultProvisioner();
    if (provisioner === null) {
      throw new ExclusiveWorkflowPlacementError(
        "exclusive_provisioner_unavailable",
        "No default sidecar provisioner is configured for exclusive workflows",
      );
    }
    if (args.sourceOfferingIds.length === 0) {
      throw new ExclusiveWorkflowPlacementError(
        "invalid_source_offerings",
        "Exclusive workflow deployment requires at least one catalog offering",
      );
    }
    if (!args.sourceOfferingIds.includes(args.defaultSourceOfferingId)) {
      throw new ExclusiveWorkflowPlacementError(
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
      throw new ExclusiveWorkflowPlacementError(
        "source_offering_unavailable",
        `Catalog offering ${sourceCheck.offeringId} cannot be used by the deployment authority`,
      );
    }

    const allocationId = createAllocationId();
    const createdAt = now();
    const deploymentAddress = deriveDeploymentAddress({
      deploymentId: args.deploymentId,
      deploymentDomain: args.deploymentDomain,
    });
    // Key the definition by its wire-projection content hash, matching
    // `deployWorkflowDefinition`: the same definition resolves to the same
    // `(assetId, wireHash)` row whether it deploys through the normal or the
    // exclusive-allocation path, so the two never mint divergent definitions.
    const wireHash = await computeLiveDefinitionHash(args.definition);
    await db.transaction(async (tx) => {
      const { definitionId } = await ensureWorkflowDefinitionForAsset(tx, {
        assetId: args.definitionAssetId,
        wireHash,
      });
      await tx.insert(workflowRun).values({
        id: args.deploymentId,
        tenantId: args.tenantId,
        deploymentId: args.deploymentId,
        definitionId,
        address: deploymentAddress,
        status: "running",
        createdAt,
      });
      await tx.insert(grant).values({
        id: generateId("grant"),
        tenantId: args.tenantId,
        principalId: args.sourceAuthorityPrincipalId,
        resource: `workflow-run:${args.deploymentId}`,
        action: "read",
        effect: "allow",
        origin: "creator",
        createdAt,
        updatedAt: createdAt,
      });
      await launchSpecStore.create(
        {
          anchorRunId: args.deploymentId,
          sessionId: args.sessionId,
          deploymentDomain: args.deploymentDomain,
          sourceAuthorityPrincipalId: args.sourceAuthorityPrincipalId,
          definitionSnapshot: args.definition,
          definitionHash: hexEncode(hashDefinition(args.definition)),
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
          anchorRunId: args.deploymentId,
          tenantId: args.tenantId,
          provisionerId: provisioner.id,
          provisionerApiVersion: provisioner.apiVersion,
          provisionerBindingFingerprint: provisioner.bindingFingerprint,
          placement: args.placement,
          now: createdAt,
        },
        tx,
      );
    });

    return {
      deploymentId: args.deploymentId,
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
      columns: { publicKey: true },
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
    const spec = await launchSpecStore.get(allocation.anchorRunId);
    if (spec === null) {
      throw new Error(
        `Allocation ${allocation.id} has no workflow launch specification`,
      );
    }
    const definition = parseDefinitionSnapshot(spec.definitionSnapshot);
    if (hexEncode(hashDefinition(definition)) !== spec.definitionHash) {
      throw new Error(
        `Allocation ${allocation.id} workflow definition hash does not match its launch specification`,
      );
    }
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
    const deploymentAddress = deriveDeploymentAddress({
      deploymentId: allocation.anchorRunId,
      deploymentDomain: spec.deploymentDomain,
    });
    const config: HarnessConfig = {
      sessionId: spec.sessionId,
      agentId: `ins_${allocation.anchorRunId}`,
      tenantId: allocation.tenantId,
      principalId: spec.sourceAuthorityPrincipalId,
      agentAddress: deploymentAddress,
      systemPrompt: "",
      tools: [],
      grants: [],
      sources: resolved.sources,
      defaultSource: defaultSource.id,
    };
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- parseWorkflowRunLaunchSpecRow validates the persisted value is a JSON object; DeployContent's optional fields are validated again by the deploy-tree writers that consume them
    const deployContent = spec.deployContent as DeployContent;

    return preparedDeployer.deployPreparedWorkflowDefinition({
      tenantId: allocation.tenantId,
      deploymentId: allocation.anchorRunId,
      deploymentDomain: spec.deploymentDomain,
      definition,
      config,
      deployContent,
      allocationTarget,
      ...(spec.toolPackagePins !== null
        ? { toolPackagePins: spec.toolPackagePins }
        : {}),
    });
  }

  return { prepareExclusiveDeployment, deployReadyAllocation };
}

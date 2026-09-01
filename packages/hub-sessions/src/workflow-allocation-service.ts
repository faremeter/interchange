import { type } from "arktype";

import { sha256 } from "@intx/crypto";
import {
  createSidecarAllocationStore,
  createWorkflowProbeStore,
  createWorkflowRunLaunchSpecStore,
  resolveTenantSidecarCapabilityPolicies,
  resolveSourcesByOfferingIds,
  type DB,
  type SidecarAllocation,
  type WorkflowProbe,
} from "@intx/db";
import { grant, workflowRun } from "@intx/db/schema";
import { eq } from "drizzle-orm";
import { generateId } from "@intx/hub-common";
import { getLogger } from "@intx/log";
import {
  hexEncode,
  SidecarCapabilityRule,
  type CredentialCipher,
} from "@intx/types";
import type { FrozenApprovalBundle } from "@intx/types/sidecar";
import type { HarnessConfig } from "@intx/types/runtime";
import type { ToolPackagePin } from "@intx/types/tool-packages";
import type { WorkflowDefinitionSource } from "@intx/types/workflow-sources";
import {
  buildInertProjectionStepSources,
  deriveRunAddress,
  deriveRunAgentId,
} from "@intx/workflow-deploy";

import type { DeployContent } from "./agent-repo";
import {
  DestroySidecarResult,
  EnsureSidecarResult,
  type SidecarCapabilityMismatch,
  type SidecarPluginRegistry,
  type SidecarProvisioner,
  type SidecarProvisionerSelection,
} from "./sidecar-allocation";
import {
  SessionLaunchError,
  type DeployWorkflowDefinitionResult,
  type PreparedWorkflowDeployer,
} from "./session-service";
import type {
  AllocatedSidecarTarget,
  SidecarAllocationRouter,
} from "./ws/sidecar-handler";
import type { InstallAndApproveResult } from "./workflow-probe-gate";
import { buildReferencedWorkflowSourcePins } from "./workflow-source-pins";

const logger = getLogger(["hub", "workflow-allocation"]);

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
  initialize?(): Promise<void>;
  reconcileReleasingProbes?(): Promise<void>;
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
  /** Decrypts tenant-owned credential bindings for provisioned deployments. */
  readonly credentialCipher: CredentialCipher;
  readonly probeCapabilityRules?: readonly SidecarCapabilityRule[];
  readonly allocationRouter: Pick<
    SidecarAllocationRouter,
    | "disconnectAllocation"
    | "fenceAllocation"
    | "isAllocatedWorkflowActive"
    | "sendProbeToAllocation"
    | "waitForAllocatedSidecar"
  >;
  readonly hubWebSocketUrl: string;
  readonly createAllocationId?: () => string;
  readonly createSidecarId?: () => string;
  readonly createToken?: () => string;
  readonly connectTimeoutMs?: number;
  readonly now?: () => Date;
};

function randomAllocationId(): string {
  return `sal_${hexEncode(crypto.getRandomValues(new Uint8Array(16)))}`;
}

function randomSidecarId(): string {
  return `sc_${hexEncode(crypto.getRandomValues(new Uint8Array(16)))}`;
}

function randomToken(): string {
  return `intx_sc_${hexEncode(crypto.getRandomValues(new Uint8Array(32)))}`;
}

function selectProvisioner(
  selection: SidecarProvisionerSelection,
): SidecarProvisioner {
  if (selection.ok) return selection.provisioner;
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

function parseEnsureResult(value: unknown): EnsureSidecarResult {
  const result = EnsureSidecarResult(value);
  if (result instanceof type.errors) {
    throw new Error(
      `Sidecar provisioner returned an invalid ensure result: ${result.summary}`,
    );
  }
  return result;
}

function parseDestroyResult(value: unknown): DestroySidecarResult {
  const result = DestroySidecarResult(value);
  if (result instanceof type.errors) {
    throw new Error(
      `Sidecar provisioner returned an invalid destroy result: ${result.summary}`,
    );
  }
  return result;
}

function createProvisionedHarnessConfig(args: {
  readonly tenantId: string;
  readonly anchorRunId: string;
  readonly deploymentDomain: string;
  readonly sessionId: string;
  readonly sourceAuthorityPrincipalId: string;
  readonly sources: HarnessConfig["sources"];
  readonly defaultSource: string;
}): HarnessConfig {
  const deploymentAddress = deriveRunAddress({
    runId: args.anchorRunId,
    domain: args.deploymentDomain,
  });
  return {
    sessionId: args.sessionId,
    agentId: deriveRunAgentId({ runId: args.anchorRunId }),
    tenantId: args.tenantId,
    principalId: args.sourceAuthorityPrincipalId,
    agentAddress: deploymentAddress,
    systemPrompt: "",
    tools: [],
    grants: [],
    sources: args.sources,
    defaultSource: args.defaultSource,
  };
}

export function createWorkflowAllocationService({
  db,
  plugins,
  preparedDeployer,
  credentialCipher,
  probeCapabilityRules = [],
  allocationRouter,
  hubWebSocketUrl,
  createAllocationId = randomAllocationId,
  createSidecarId = randomSidecarId,
  createToken = randomToken,
  connectTimeoutMs = 120_000,
  now = () => new Date(),
}: WorkflowAllocationServiceDeps): WorkflowAllocationService {
  const allocationStore = createSidecarAllocationStore(db);
  const probeStore = createWorkflowProbeStore(db);
  const launchSpecStore = createWorkflowRunLaunchSpecStore(db);
  const validatedProbeCapabilityRules =
    SidecarCapabilityRule.array()(probeCapabilityRules);

  if (validatedProbeCapabilityRules instanceof type.errors) {
    throw new Error(
      `Invalid workflow probe capability rules: ${validatedProbeCapabilityRules.summary}`,
    );
  }
  const configuredProbeCapabilityRules = validatedProbeCapabilityRules.map(
    (rule) => ({ ...rule }),
  );

  if (connectTimeoutMs <= 0) {
    throw new Error("connectTimeoutMs must be positive");
  }

  function matchingProvisioner(
    probe: WorkflowProbe,
  ): SidecarProvisioner | null {
    const provisioner = plugins.getProvisioner(probe.provisionerId);
    return provisioner !== null &&
      provisioner.apiVersion === probe.provisionerApiVersion &&
      provisioner.bindingFingerprint === probe.provisionerBindingFingerprint
      ? provisioner
      : null;
  }

  async function beginProbeRelease(
    probe: WorkflowProbe,
    failure?: { code: string; message: string },
  ): Promise<WorkflowProbe | null> {
    if (probe.status === "releasing") return probe;
    return probeStore.transition(
      probe.id,
      ["pending", "provisioning", "probing"],
      "releasing",
      {
        ...(failure !== undefined
          ? {
              failureCode: failure.code,
              failureMessage: failure.message,
            }
          : {}),
        now: now(),
      },
    );
  }

  async function finishProbeRelease(
    releasing: WorkflowProbe,
    finalStatus: "succeeded" | "failed",
  ): Promise<void> {
    if (releasing.sidecarId !== null) {
      const provisioner = matchingProvisioner(releasing);
      if (provisioner === null) {
        throw new Error(
          `Cannot clean up workflow probe ${releasing.id}: provisioner binding ${releasing.provisionerId} is unavailable`,
        );
      }
      const destroyed = parseDestroyResult(
        await provisioner.destroy({
          allocationId: releasing.id,
          generation: releasing.generation,
          sidecarId: releasing.sidecarId,
          ...(releasing.externalRef !== null
            ? { externalRef: releasing.externalRef }
            : {}),
        }),
      );
      if (destroyed.kind === "rejected") {
        throw new Error(
          `Provisioner ${provisioner.id} rejected probe cleanup: ${destroyed.message}`,
        );
      }
      allocationRouter.disconnectAllocation({
        allocationId: releasing.id,
        generation: releasing.generation,
      });
    }
    await probeStore.transition(releasing.id, ["releasing"], finalStatus, {
      now: now(),
    });
  }

  async function releaseProbe(
    probe: WorkflowProbe,
    finalStatus: "succeeded" | "failed",
    failure?: { code: string; message: string },
  ): Promise<void> {
    const releasing = await beginProbeRelease(probe, failure);
    if (releasing === null) return;
    await finishProbeRelease(releasing, finalStatus);
  }

  async function createDeployment(args: {
    request: PrepareProvisionedWorkflowDeploymentArgs;
    approved: InstallAndApproveResult & {
      approval: Extract<InstallAndApproveResult["approval"], { ok: true }>;
    };
    provisioner: SidecarProvisioner;
    probe: WorkflowProbe;
    adoptProbe: boolean;
  }): Promise<PreparedProvisionedWorkflowDeployment> {
    const { request, approved, provisioner, probe, adoptProbe } = args;
    const probeSidecarId = probe.sidecarId;
    if (adoptProbe && probeSidecarId === null) {
      throw new Error(`Workflow probe ${probe.id} has no sidecar to adopt`);
    }
    const allocationId = adoptProbe ? probe.id : createAllocationId();
    const createdAt = now();
    const deploymentAddress = deriveRunAddress({
      runId: request.anchorRunId,
      domain: request.deploymentDomain,
    });
    const frozenApprovalBundle: FrozenApprovalBundle = {
      source: request.source,
      entry: request.entry,
      projection: approved.projection,
      closure: approved.closure,
      approvedWireHash: approved.approval.approvedWireHash,
      approvedGrants: [...approved.approval.approvedGrants],
    };

    await db.transaction(async (tx) => {
      await tx.insert(workflowRun).values({
        id: request.anchorRunId,
        tenantId: request.tenantId,
        anchorRunId: request.anchorRunId,
        definitionId: approved.approval.definitionId,
        address: deploymentAddress,
        status: "deployed",
        createdAt,
      });
      await tx.insert(grant).values({
        id: generateId("grant"),
        tenantId: request.tenantId,
        principalId: request.sourceAuthorityPrincipalId,
        resource: `workflow-run:${request.anchorRunId}`,
        action: "read",
        effect: "allow",
        origin: "creator",
        createdAt,
        updatedAt: createdAt,
      });
      await launchSpecStore.create(
        {
          anchorRunId: request.anchorRunId,
          sessionId: request.sessionId,
          deploymentDomain: request.deploymentDomain,
          sourceAuthorityPrincipalId: request.sourceAuthorityPrincipalId,
          frozenApprovalBundle,
          sourceOfferingIds: [...request.sourceOfferingIds],
          defaultSourceOfferingId: request.defaultSourceOfferingId,
          deployContent: request.deployContent,
          ...(request.toolPackagePins !== undefined
            ? { toolPackagePins: [...request.toolPackagePins] }
            : {}),
          createdAt,
        },
        tx,
      );
      if (adoptProbe) {
        if (probeSidecarId === null) {
          throw new Error(`Workflow probe ${probe.id} lost its sidecar`);
        }
        await allocationStore.createAdopted(
          {
            id: allocationId,
            anchorRunId: request.anchorRunId,
            tenantId: request.tenantId,
            provisionerId: provisioner.id,
            provisionerApiVersion: provisioner.apiVersion,
            provisionerBindingFingerprint: provisioner.bindingFingerprint,
            sidecarId: probeSidecarId,
            generation: probe.generation,
            ...(probe.externalRef !== null
              ? { externalRef: probe.externalRef }
              : {}),
            connectDeadline: new Date(createdAt.getTime() + connectTimeoutMs),
            now: createdAt,
          },
          tx,
        );
        const completed = await probeStore.transition(
          probe.id,
          ["probing"],
          "succeeded",
          { now: createdAt, tx },
        );
        if (completed === null) {
          throw new Error(`Workflow probe ${probe.id} changed before adoption`);
        }
      } else {
        await allocationStore.createPending(
          {
            id: allocationId,
            anchorRunId: request.anchorRunId,
            tenantId: request.tenantId,
            provisionerId: provisioner.id,
            provisionerApiVersion: provisioner.apiVersion,
            provisionerBindingFingerprint: provisioner.bindingFingerprint,
            now: createdAt,
          },
          tx,
        );
      }
    });

    if (adoptProbe) {
      allocationRouter.disconnectAllocation({
        allocationId: probe.id,
        generation: probe.generation,
      });
    }
    return {
      anchorRunId: request.anchorRunId,
      deploymentAddress,
      allocationId,
      status: "pending",
    };
  }

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

    const [sourceCheck, tenantPolicies] = await Promise.all([
      resolveSourcesByOfferingIds(
        db,
        args.tenantId,
        args.sourceOfferingIds,
        credentialCipher,
      ),
      resolveTenantSidecarCapabilityPolicies(db, args.tenantId),
    ]);
    if (!sourceCheck.ok) {
      throw new WorkflowProvisioningError(
        "source_offering_unavailable",
        `Catalog offering ${sourceCheck.offeringId} cannot be used by the deployment authority`,
      );
    }

    const defaultSource = sourceCheck.sources.find(
      (source) => source.id === args.defaultSourceOfferingId,
    );
    if (defaultSource === undefined) {
      throw new WorkflowProvisioningError(
        "invalid_source_offerings",
        `Default offering ${args.defaultSourceOfferingId} was not resolved for deployment ${args.anchorRunId}`,
      );
    }
    const probeProvisioner = selectProvisioner(
      plugins.selectProvisioner({
        tenantPolicies,
        probeRules: configuredProbeCapabilityRules,
        workflowRules: [],
      }),
    );
    const probeId = createAllocationId();
    let probe = await probeStore.create({
      id: probeId,
      tenantId: args.tenantId,
      definitionAssetId: args.definitionAssetId,
      source: args.source,
      entry: args.entry,
      ...(args.pin !== undefined ? { pin: args.pin } : {}),
      provisionerId: probeProvisioner.id,
      provisionerApiVersion: probeProvisioner.apiVersion,
      provisionerBindingFingerprint: probeProvisioner.bindingFingerprint,
      now: now(),
    });

    try {
      const token = createToken();
      const sidecarId = createSidecarId();
      const bound = await probeStore.bindSidecar({
        probeId,
        sidecarId,
        tokenHashSha256: await sha256(token),
        now: now(),
      });
      if (bound === null) {
        throw new Error(
          `Workflow probe ${probeId} changed before provisioning`,
        );
      }
      probe = bound;
      const allocationTarget: AllocatedSidecarTarget = {
        allocationId: probe.id,
        generation: probe.generation,
      };
      allocationRouter.fenceAllocation(probe.id, probe.generation);
      const ensured = parseEnsureResult(
        await probeProvisioner.ensure({
          allocationId: probe.id,
          generation: probe.generation,
          tenantId: probe.tenantId,
          // The provisioner contract treats this as an opaque owner id. A
          // probe has no workflow run, so its own id is the honest owner.
          anchorRunId: probe.id,
          sidecarId,
          token,
          hubWebSocketUrl,
        }),
      );
      if (ensured.kind === "rejected") {
        throw new WorkflowProvisioningError(ensured.code, ensured.message);
      }
      const probing = await probeStore.markProbing({
        probeId,
        ...(ensured.externalRef !== undefined
          ? { externalRef: ensured.externalRef }
          : {}),
        now: now(),
      });
      if (probing === null) {
        throw new Error(`Workflow probe ${probeId} changed after provisioning`);
      }
      probe = probing;
      await allocationRouter.waitForAllocatedSidecar(
        allocationTarget,
        connectTimeoutMs,
      );

      let resultRecorded = false;
      const approved = await preparedDeployer.installAndApproveWorkflowSource({
        source: args.source,
        entry: args.entry,
        ...(args.pin !== undefined ? { pin: args.pin } : {}),
        definitionAssetId: args.definitionAssetId,
        allocationTarget,
        onProbeResult: async (result) => {
          if (
            (await probeStore.recordResult(probeId, result, now())) === null
          ) {
            throw new Error(
              `Workflow probe ${probeId} changed before result persistence`,
            );
          }
          resultRecorded = true;
        },
      });
      if (!resultRecorded) {
        throw new Error(
          `Workflow probe ${probeId} completed without persisting its result`,
        );
      }
      if (!approved.approval.ok) {
        throw new Error(
          "prepareProvisionedDeployment: install did not yield an approved definition",
        );
      }
      const narrowed = {
        ...approved,
        approval: approved.approval,
      };
      const config = createProvisionedHarnessConfig({
        tenantId: args.tenantId,
        anchorRunId: args.anchorRunId,
        deploymentDomain: args.deploymentDomain,
        sessionId: args.sessionId,
        sourceAuthorityPrincipalId: args.sourceAuthorityPrincipalId,
        sources: sourceCheck.sources,
        defaultSource: defaultSource.id,
      });
      buildInertProjectionStepSources({
        projection: approved.projection,
        config,
        operatorApprovals: approved.approval.approvedGrants,
      });
      await buildReferencedWorkflowSourcePins({
        projection: approved.projection,
        config,
        operatorApprovals: approved.approval.approvedGrants,
      });
      const deploymentProvisioner = selectProvisioner(
        plugins.selectProvisioner({
          tenantPolicies,
          workflowRules:
            approved.projection.sidecarPlacement?.capabilities ?? [],
        }),
      );
      const adoptProbe =
        deploymentProvisioner.id === probeProvisioner.id &&
        deploymentProvisioner.apiVersion === probeProvisioner.apiVersion &&
        deploymentProvisioner.bindingFingerprint ===
          probeProvisioner.bindingFingerprint;
      if (!adoptProbe) {
        await releaseProbe(probe, "succeeded");
      }
      return await createDeployment({
        request: args,
        approved: narrowed,
        provisioner: deploymentProvisioner,
        probe,
        adoptProbe,
      });
    } catch (error) {
      try {
        await releaseProbe(probe, "failed", {
          code: "probe_failed",
          message: error instanceof Error ? error.message : String(error),
        });
      } catch {
        // Preserve the primary probe/provisioning error. The durable
        // `releasing` row remains discoverable for periodic cleanup.
      }
      throw error;
    }
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
    const config = createProvisionedHarnessConfig({
      tenantId: allocation.tenantId,
      anchorRunId: allocation.anchorRunId,
      deploymentDomain: spec.deploymentDomain,
      sessionId: spec.sessionId,
      sourceAuthorityPrincipalId: spec.sourceAuthorityPrincipalId,
      sources: resolved.sources,
      defaultSource: defaultSource.id,
    });

    return preparedDeployer.deployPreparedCodeSourcedWorkflow({
      tenantId: allocation.tenantId,
      anchorRunId: allocation.anchorRunId,
      deploymentDomain: spec.deploymentDomain,
      agentAddress: config.agentAddress,
      source: bundle.source,
      approved,
      config,
      allocationTarget,
      credentialCipher,
    });
  }

  function cleanupFinalStatus(probe: WorkflowProbe): "succeeded" | "failed" {
    return probe.failureCode === null ? "succeeded" : "failed";
  }

  async function reconcileReleasingProbes(): Promise<void> {
    const failures: unknown[] = [];
    for (const probe of await probeStore.listReleasing()) {
      try {
        await releaseProbe(probe, cleanupFinalStatus(probe));
      } catch (error) {
        failures.push(error);
      }
    }
    if (failures.length > 0) {
      throw new AggregateError(
        failures,
        "Failed to clean up releasing workflow probes",
      );
    }
  }

  async function initialize(): Promise<void> {
    const failures: unknown[] = [];
    for (const probe of await probeStore.listActive()) {
      let releasing: WorkflowProbe | null;
      try {
        releasing = await beginProbeRelease(
          probe,
          probe.status === "releasing"
            ? undefined
            : {
                code: "probe_interrupted",
                message: "Hub restarted before the workflow probe completed",
              },
        );
      } catch (error) {
        failures.push(error);
        continue;
      }
      if (releasing === null) continue;
      try {
        await finishProbeRelease(releasing, cleanupFinalStatus(releasing));
      } catch (error) {
        logger.warn`Workflow probe ${releasing.id} cleanup remains pending after startup: ${error instanceof Error ? error.message : String(error)}`;
      }
    }
    if (failures.length > 0) {
      throw new AggregateError(
        failures,
        "Failed to mark interrupted workflow probes for cleanup",
      );
    }
  }

  return {
    initialize,
    reconcileReleasingProbes,
    prepareProvisionedDeployment,
    deployReadyAllocation,
  };
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

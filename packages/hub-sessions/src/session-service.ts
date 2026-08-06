import { type } from "arktype";
import { and, eq } from "drizzle-orm";

import {
  createDefaultDirectorRegistry,
  type DirectorRegistry,
} from "@intx/agent";
import { getLogger } from "@intx/log";
import {
  assembleMessage,
  assembleSignedContent,
  createDetachedSignatureFromProvider,
  type MessageHeaders,
} from "@intx/mime";
import { listAssetsForTenant, type DB } from "@intx/db";
import {
  grant as grantTable,
  sidecarAllocation as sidecarAllocationTable,
  workflowRun as workflowRunTable,
} from "@intx/db/schema";
import { base64Encode, hexEncode } from "@intx/types";
import type { CredentialDelivery } from "@intx/types/sidecar";
import { generateId } from "@intx/hub-common";
import { ensureWorkflowDefinitionForAsset } from "./workflow-definition-ensure";
import { sessionAsset as sessionAssetTable } from "@intx/db/schema";
import type {
  CryptoProvider,
  HarnessConfig,
  InferenceSource,
  MessageAttachment,
} from "@intx/types/runtime";
import {
  type RegistryConfig,
  type RegistrySource,
  type ScopeRoute,
  AssetRegistrySource,
  HttpRegistrySource,
  ManifestInvalidError,
  createClosureResolver,
} from "@intx/tool-packaging";
import {
  ToolPackageManifest,
  type ToolPackagePin,
} from "@intx/types/tool-packages";
import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";
import type { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import type { WorkflowDefinitionSource } from "@intx/types/workflow-sources";
import {
  defineWorkflow,
  type WorkflowDefinition,
} from "@intx/workflow/definition";
import {
  assertChainHeadIsDefault,
  computeLiveDefinitionHash,
  createWorkflowDeployOrchestrator,
  deriveDeploymentAddress,
  walkCapabilities,
  wrapHarnessAsSingleStepWorkflow,
  type ApprovalSet,
  type DeployContent as OrchestratorDeployContent,
  type DeployWorkflowArgs,
  type DeployWorkflowResult,
  type DeploySingleStepFn,
  type LaunchSessionFn,
  type ReferencedBodyDefinition,
  type SendMultiStepDeployFn,
  type WorkflowRepoWriter,
} from "@intx/workflow-deploy";

import type { AgentRepoStore, DeployContent } from "./agent-repo";
import {
  DEFAULT_ASSET_REF,
  type Asset,
  type AssetService,
} from "./asset-service";
import type {
  AllocatedSidecarTarget,
  SidecarAllocationRouter,
  SidecarRouter,
} from "./ws/sidecar-handler";
import type { Principal, RepoId } from "./repo-store";
import { restoreWorkflowRunToAllocation } from "./workflow-run-restore";
import { materializeDeployGrantsFromFrozen } from "./workflow-probe-gate";

const logger = getLogger(["interchange", "hub", "session-service"]);

export class SessionLaunchError extends Error {
  /** Which phase failed: "write", "provision", "pack", or "start". */
  readonly phase: string;
  /** True if the sidecar has a provisioned agent that could not be cleaned up. */
  readonly leakedAgent: boolean;

  constructor(phase: string, cause: unknown, leakedAgent: boolean) {
    const msg =
      cause instanceof Error ? cause.message : "Session launch failed";
    super(msg, { cause });
    this.name = "SessionLaunchError";
    this.phase = phase;
    this.leakedAgent = leakedAgent;
  }
}

export type SessionService = {
  /**
   * Stage one step of a multi-step workflow deploy: bind a transient route
   * for the step address, fire a no-spawn provision frame (init the step's
   * agent-state repo and record the hub key), deliver the deploy + asset
   * packs, and unbind the route -- no warm harness. The multi-step branch
   * stages every step this way before firing the deployment-level workflow
   * frame that spawns the supervised child; the child reads each staged step
   * tree from disk and runs the step itself.
   */
  stageWorkflowStep(params: {
    agentAddress: string;
    agentId: string;
    instanceId: string;
    config: HarnessConfig;
    deployContent: DeployContent;
    toolPackagePins?: readonly ToolPackagePin[];
    allocationTarget?: AllocatedSidecarTarget;
  }): Promise<void>;

  /**
   * Deploy a single-agent instance through the single-step-at-head path,
   * wrapping the harness as a one-step workflow and routing it through the
   * deploy core with the instance's real identity. Replaces `launchSession`
   * as the production instance-deploy entry point: the instance runs as a
   * supervised workflow-process child. Records no deployment anchor run.
   * Returns the head's agent-key ack (the key the head signs its
   * reconnect challenges with).
   */
  deployInstanceAtHead(params: {
    agentAddress: string;
    agentId: string;
    instanceId: string;
    config: HarnessConfig;
    deployContent: DeployContent;
    toolPackagePins?: readonly ToolPackagePin[];
    credentials?: CredentialDelivery;
  }): Promise<{ publicKey: string }>;

  /**
   * Deploy a one-step workflow once at the head through the deploy core,
   * without a DB-backed deployment anchor run. Stages the
   * head's deploy tree (deploy-tree write, pack, asset fan-out), fires the
   * deployment `agent.deploy` frame carrying the workflow definition +
   * source pin (the sidecar initializes the head repo and spawns the
   * workflow-process child), then delivers the pack to the head. Returns
   * the sidecar supervisor's principal public key. See `DeploySingleStepFn`.
   */
  deploySingleStepAtHead: DeploySingleStepFn;

  /**
   * Deploy a multi-step `WorkflowDefinition` through the workflow-deploy
   * orchestrator's multi-step branch. This is the general workflow
   * deploy entry point: it is not coupled to a single agent's
   * credential/session model the way `launchSession` is. The
   * orchestrator derives every per-step address
   * from `deploymentId` + `deploymentDomain`, provisions each step's
   * agent-state repo via the shared per-agent deploy phases, writes the
   * workflow repo, and fires the deployment-level `agent.deploy` frame.
   *
   * Persists the deployment's anchor run -- the `workflow_run` whose id is
   * `deploymentId` -- carrying its routing identity and definition, so the
   * deployment is listable per tenant off its runs; the RepoStore substrate
   * has no by-kind listing API of its own.
   *
   * Returns the supervisor's principal public key surfaced by the
   * sidecar's `agent.deploy.ack`.
   */
  deployWorkflowDefinition(
    params: DeployWorkflowDefinitionParams,
  ): Promise<DeployWorkflowDefinitionResult>;

  /**
   * Compose a signed RFC 2822 message from the user and deliver it to the
   * agent via the mail transport. Throws if the agent is unreachable.
   * Returns the raw MIME bytes of the assembled message.
   */
  sendUserMessage(params: UserMessageParams): Promise<Uint8Array>;

  /**
   * Undeploy an agent and wait for the sidecar to acknowledge.
   */
  endSession(agentAddress: string, reason: string): Promise<void>;
};

export type DeployWorkflowDefinitionParams = {
  /** Owning tenant; recorded on the deployment's anchor run. */
  tenantId: string;
  /**
   * Stable deployment identifier. The orchestrator concatenates it into
   * every derived per-step address and the deployment-level address, and
   * it is the deployment's anchor-run id. The caller owns its generation.
   */
  deploymentId: string;
  /**
   * Mail domain the deployment's derived addresses live under. The
   * orchestrator derives `ins_<deploymentId>-<stepId>@<deploymentDomain>`
   * per step and `ins_<deploymentId>@<deploymentDomain>` for the
   * deployment-level supervisor address.
   */
  deploymentDomain: string;
  /** The hydrated workflow definition to deploy. */
  definition: WorkflowDefinition;
  /**
   * The `workflow`-kind asset the definition was hydrated from. Recorded
   * on the projection row so the listing surface can join back to the
   * source asset.
   */
  definitionAssetId: string;
  /**
   * Harness configuration shared across every step's launch. The
   * orchestrator overrides `agentAddress`, `agentId`, and `systemPrompt`
   * per step.
   */
  config: HarnessConfig;
  /** Deploy-tree content shared across every step's launch. */
  deployContent: DeployContent;
  /** Tool-package pins to ship with every step's deploy. */
  toolPackagePins?: readonly ToolPackagePin[];
};

export type DeployPreparedWorkflowDefinitionParams = Omit<
  DeployWorkflowDefinitionParams,
  "definitionAssetId"
> & {
  allocationTarget: AllocatedSidecarTarget;
};

export type DeployWorkflowDefinitionResult = {
  /** Echoes the deployment id recorded on the projection row. */
  deploymentId: string;
  /** Deployment-level mail address the supervisor registers on the bus. */
  deploymentAddress: string;
  /** Supervisor principal public key from the sidecar's deploy ack. */
  publicKey: string;
};

export type PreparedWorkflowDeployer = {
  /** Deploy an anchor that was durably prepared before capacity was requested. */
  deployPreparedWorkflowDefinition(
    params: DeployPreparedWorkflowDefinitionParams,
  ): Promise<DeployWorkflowDefinitionResult>;
};

export type UserMessageParams = {
  agentAddress: string;
  from: string;
  messageId: string;
  date: Date;
  content: string;
  attachments?: MessageAttachment[];
  inReplyTo?: string;
  references?: string[];
  sessionId: string;
  tenantId: string;
  cryptoProvider: CryptoProvider;
};

export type SessionServiceDeps = {
  sidecarRouter: SidecarRouter;
  /** Present when this Hub can route deploy phases to exclusive allocations. */
  sidecarAllocationRouter?: SidecarAllocationRouter;
  agentRepoStore: AgentRepoStore;
  /**
   * Optional asset attachment integration. When set, the deploy flow
   * fans out per-attachment packs after the deploy pack lands and
   * inserts a `session_asset` row per attachment. When unset, only
   * the deploy pack is sent — the single-pack path is preserved
   * bit-for-bit.
   */
  assetService?: AssetService;
  /** DB handle used for `session_asset` manifest inserts. Required
   * iff `assetService` is set. */
  db?: DB["db"];
  /**
   * Tool-package registry configuration. Required iff any agent the
   * service launches has non-empty `toolPackagePins`. When set, the
   * service builds a per-agent `ClosureResolver` at launch time: the
   * registry map combines (a) every `package-registry` asset visible
   * to the agent's tenant via the INTR-178 walker — keyed by
   * `asset.name` — and (b) the statically-configured HTTP registries
   * in `httpRegistries`.
   *
   * **Name-collision policy.** When an asset and an HTTP registry
   * both claim the same registry name, the asset wins. This mirrors
   * the inner-shadows-outer rule the tenancy walker already applies
   * to asset resolution and gives operators a single mental model:
   * closer-scope shadows wider-scope. The rule is a contract this
   * service guarantees, not an iteration-order accident — consumers
   * may rely on it to override a wider-scope HTTP registry by
   * publishing an asset at a closer tenancy.
   *
   * `defaultRegistry` names the entry the resolver consults for any
   * package whose scope does not match `scopeRouting`. The name must
   * resolve in the combined map for the given agent — if no asset and
   * no HTTP entry carries that name, launch fails at the
   * registry-resolution step.
   */
  toolPackageRegistries?: {
    /**
     * Registry identifier → registry config. The key is the
     * identifier `scopeRouting` entries and manifest `registry`
     * references point at; the value carries url plus optional auth.
     */
    readonly httpRegistries: ReadonlyMap<string, RegistryConfig>;
    readonly defaultRegistry: string;
    readonly scopeRouting?: readonly ScopeRoute[];
  };
};

// Hub-side principal for reading asset repos. Assets are signed by the
// hub itself, and the launch fan-out reads them on the hub to assemble
// packs for delivery to a sidecar -- so the hub principal is correct.
const HUB_PRINCIPAL: Principal = { kind: "hub" };

type ResolvedAttachment = {
  mountPath: string;
  sourceCommitSha: string;
  repoId: RepoId;
  pack: Uint8Array;
  ref: string;
};

type SessionAssetRecord = {
  instanceId: string;
  mountPath: string;
  assetPackSha: string;
  sourceCommitSha: string;
};

async function createPackSha(pack: Uint8Array): Promise<string> {
  const digest = await crypto.subtle.digest(
    "SHA-256",
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- ArrayBuffer-backed at the call site; Web Crypto's BufferSource type rejects Uint8Array<ArrayBufferLike> under TS 5.9 (microsoft/TypeScript#62240)
    pack as Uint8Array<ArrayBuffer>,
  );
  return hexEncode(new Uint8Array(digest));
}

/**
 * Walk a resolved tool-package manifest and return every distinct
 * `assetId` referenced by a `kind: "asset"` entry. Order is the
 * resolver's BFS order so the fan-out below is deterministic for
 * tests; a `Set` would be wrong here because tests assert specific
 * orderings.
 */
function collectDistinctAssetIds(manifest: ToolPackageManifest): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const entry of manifest.entries) {
    if (entry.source.kind !== "asset") continue;
    if (seen.has(entry.source.assetId)) continue;
    seen.add(entry.source.assetId);
    out.push(entry.source.assetId);
  }
  return out;
}

/**
 * Translate the orchestrator's structural `DeployContent` (which types
 * `toolPackageManifest` as `unknown`) back into the hub-sessions
 * `DeployContent` shape. The orchestrator round-trips whatever the
 * caller supplied, but the surface type widens `toolPackageManifest` to
 * `unknown`; the validator narrows it back to the canonical shape
 * `agentRepoStore.writeDeployTree` consumes.
 *
 * Exported so a test fixture that forwards orchestrator-shaped deploy
 * content into `launchSession` narrows it the same validated way the
 * production multi-step callback does, rather than casting `unknown`.
 */
export function bridgeOrchestratorDeployContent(
  content: OrchestratorDeployContent,
): DeployContent {
  const bridged: DeployContent = { systemPrompt: content.systemPrompt };
  if (content.toolPackageManifest !== undefined) {
    const validated = ToolPackageManifest(content.toolPackageManifest);
    if (validated instanceof type.errors) {
      throw new Error(
        `orchestrator deploy content carries an invalid toolPackageManifest: ${validated.summary}`,
      );
    }
    bridged.toolPackageManifest = validated;
  }
  if (content.assetMounts !== undefined) {
    bridged.assetMounts = content.assetMounts;
  }
  return bridged;
}

/**
 * Project a `WorkflowDefinition` onto the wire envelope the sidecar deploy
 * router serializes verbatim into `workflow.json` and the workflow-process
 * child re-validates against `workflowDefinitionEnvelopeSchema`: `id`,
 * `triggers`, `steps`, `stepOrder`, optional `state`. The projection widens
 * the `readonly` arrays at the boundary (the serializer never mutates them); a
 * missing envelope-required field would round-trip into the child's envelope
 * rejection on disk.
 */
function toWireWorkflowDefinition(definition: WorkflowDefinition): {
  id: string;
  triggers: unknown[];
  stepOrder: string[];
  steps: Record<string, unknown>;
  state?: Record<string, unknown>;
  grantRequirements?: unknown[];
  sidecarPlacement?: {
    sharing: "exclusive";
    reuse?: "never" | "same-deployment";
  };
} {
  return {
    id: definition.id,
    triggers: [...definition.triggers],
    stepOrder: [...definition.stepOrder],
    steps: definition.steps as Record<string, unknown>,
    ...(definition.state !== undefined ? { state: definition.state } : {}),
    ...(definition.grantRequirements !== undefined
      ? { grantRequirements: [...definition.grantRequirements] }
      : {}),
    ...(definition.sidecarPlacement !== undefined
      ? { sidecarPlacement: definition.sidecarPlacement }
      : {}),
  };
}

/** Fields both deploy-frame arms carry onto `sendAgentDeploy`, independent of
 * whether the definition is live-authored or code-sourced. */
type DeployFrameCommonArgs = {
  sidecarRouter: SidecarRouter;
  sidecarAllocationRouter?: SidecarAllocationRouter;
  allocationTarget?: AllocatedSidecarTarget;
  agentAddress: string;
  config: HarnessConfig;
  sources: Record<string, InferenceSource[]>;
};

/**
 * Live-authored arm: the hub holds the live `WorkflowDefinition` and is the
 * authority for the deployment's content hash. It projects the definition onto
 * the wire envelope and recomputes the wire hash the frame carries.
 */
export type LiveAuthoredDeployFrameArgs = DeployFrameCommonArgs & {
  lineage: "live";
  definition: WorkflowDefinition;
  /**
   * Extracted onTrigger section bodies to carry inline so the sidecar
   * materializes each as its own `assets/workflow/<bodyRef>/workflow.json`
   * plus a co-located `sources.json`; a body child then resolves both the ref
   * and its inference sources off disk without a hub round-trip.
   */
  referencedDefinitions?: readonly ReferencedBodyDefinition[];
  credentials?: CredentialDelivery;
};

/**
 * Source-ref arm: for a code-sourced (npm) deploy the hub never holds the live
 * `WorkflowDefinition` -- it lives only in the airlocked child. The gate/freeze
 * layer already projected the definition to its inert `WorkflowProjectionDefinition`
 * and hashed THAT; this arm carries both verbatim. The content hash is owned by
 * the gate, so this arm never recomputes it -- recomputing over the live wire
 * lineage would diverge from the inert projection the child re-verifies against.
 */
export type SourceRefDeployFrameArgs = DeployFrameCommonArgs & {
  lineage: "source-ref";
  /**
   * The inert wire projection the gate froze -- the same closed
   * `WorkflowProjectionDefinition` a `workflow.probe.result` carries. Placed on
   * the frame's `definition` field verbatim; it is already that field's type,
   * so no coercion is needed.
   */
  projection: WorkflowProjectionDefinition;
  /**
   * The gate-frozen wire hash of `projection` -- stamped onto the frame VERBATIM.
   * This arm does not recompute it: the freeze layer owns the content hash, and
   * the child re-verifies its recompute over the inert projection against this
   * exact value.
   */
  approvedWireHash: string;
  /**
   * Where the definition's bytes come from -- the source-ref that names the
   * npm registry publishing the definition package. Carried on the frame so
   * the sidecar can re-materialize the definition from source.
   */
  source: WorkflowDefinitionSource;
  /**
   * The frozen dependency closure the hub resolved for the definition's pin.
   * Carried alongside `source` so the sidecar materializes the exact tree the
   * hub pinned.
   */
  closure?: ToolPackageManifest;
  /**
   * The hub's frozen approved grant set (grant-shape strings) established by
   * the workflow gate/freeze. When supplied, deploy grants are materialized as
   * a SUBSET of it -- never a fresh walk -- and carried on the frame as
   * `approvedDeployGrants`. `deployGrantCandidates` is the candidate grant-shape
   * set gated against it (defaults to the frozen set itself, i.e. the full
   * approved set).
   */
  frozenApprovedGrants?: ReadonlySet<string>;
  deployGrantCandidates?: Iterable<string>;
};

export type SendMultiStepDeployFrameArgs =
  | LiveAuthoredDeployFrameArgs
  | SourceRefDeployFrameArgs;

/**
 * Wire the workflow-deploy orchestrator's `sendMultiStepDeploy`
 * dependency against `SidecarRouter.sendAgentDeploy`. The router
 * accepts an optional `workflow` projection on the deploy frame; the
 * sidecar's deploy router uses field presence to route the frame to
 * the workflow deploy path. The supervisor public key returned by the
 * sidecar's `agent.deploy.ack` is threaded back as the
 * `MultiStepDeployResult.publicKey`.
 *
 * The `lineage` discriminant selects who owns the content hash. On the
 * `source-ref` arm the gate/freeze layer already hashed the inert projection,
 * so the frozen hash and the inert projection ride the frame verbatim. On the
 * `live` arm the hub holds the live definition and recomputes the wire hash.
 * The two arms are mutually exclusive at the type level: a source-ref deploy
 * cannot pass a live definition and cannot omit its frozen hash.
 *
 * Exported so the co-located caller-site test can assert that the
 * closure constructed in `launchSession` reaches the wire surface via
 * `sendAgentDeploy` with a `workflow` field structurally matching the
 * `AgentDeployFrame.workflow` schema.
 */
export async function sendMultiStepDeployFrame(
  args: SendMultiStepDeployFrameArgs,
): Promise<{ publicKey: string }> {
  if (args.lineage === "source-ref") {
    // Materialize deploy grants as a subset of the frozen approved set when the
    // caller supplies it, so deploy grants trace to the freeze rather than a
    // fresh capability walk. Absent: the frame carries no `approvedDeployGrants`.
    const approvedDeployGrants =
      args.frozenApprovedGrants !== undefined
        ? materializeDeployGrantsFromFrozen(
            args.frozenApprovedGrants,
            args.deployGrantCandidates ?? args.frozenApprovedGrants,
          )
        : undefined;
    return args.sidecarRouter.sendAgentDeploy(args.agentAddress, args.config, {
      // The inert projection and its gate-frozen hash ride the frame verbatim;
      // neither is re-derived here. `projection` is already the frame's
      // `definition` type, so it is assigned with no coercion.
      definition: args.projection,
      sources: args.sources,
      approvedWireHash: args.approvedWireHash,
      source: args.source,
      ...(args.closure !== undefined ? { closure: args.closure } : {}),
      ...(approvedDeployGrants !== undefined ? { approvedDeployGrants } : {}),
    });
  }

  const wireDefinition = toWireWorkflowDefinition(args.definition);
  // The hub is the authority for the deployment's content hash: recompute the
  // wire hash here so the frame carries the hub-approved value the sidecar
  // feeds the child as `DEFINITION_HASH`. The freeze stored exactly this hash,
  // so recomputing it at the hub reproduces the frozen approval's anchor; the
  // sidecar never recomputes.
  const approvedWireHash = await computeWireDefinitionHash(wireDefinition);
  const workflow = {
    definition: wireDefinition,
    sources: args.sources,
    approvedWireHash,
    ...(args.referencedDefinitions !== undefined &&
    args.referencedDefinitions.length > 0
      ? {
          referencedDefinitions: await Promise.all(
            args.referencedDefinitions.map(async (body) => {
              const bodyWire = toWireWorkflowDefinition(body.definition);
              return {
                definition: bodyWire,
                sources: body.sources,
                // Per-body freeze anchor: the hub recomputes each referenced
                // body's wire hash so a body child re-verifies its recompute
                // against the hub authority.
                approvedWireHash: await computeWireDefinitionHash(bodyWire),
              };
            }),
          ),
        }
      : {}),
    ...(args.credentials !== undefined
      ? { credentials: args.credentials }
      : {}),
  };
  if (args.allocationTarget !== undefined) {
    if (args.sidecarAllocationRouter === undefined) {
      throw new Error("Exclusive deployment routing is not configured");
    }
    return args.sidecarAllocationRouter.sendAgentDeployToAllocation(
      args.allocationTarget,
      args.agentAddress,
      args.config,
      workflow,
    );
  }
  return args.sidecarRouter.sendAgentDeploy(
    args.agentAddress,
    args.config,
    workflow,
  );
}

/**
 * `WorkflowRepoWriter` backed by the hub's repo substrate. Writes the
 * orchestrator-produced workflow tree (`workflow.json`,
 * `capability-declarations.json`, `.gitignore`) into a `workflow`-kind
 * repo keyed by the workflow definition id, committing on the published
 * asset ref. The hub principal is the only writer of the workflow repo,
 * matching `workflowAuthorize`'s hub-writes / sidecar-reads split.
 */
function createHubWorkflowRepoWriter(
  agentRepoStore: AgentRepoStore,
): WorkflowRepoWriter {
  return {
    async writeWorkflowRepo(args) {
      const repoId: RepoId = { kind: "workflow", id: args.workflowRepoId };
      const files: Record<string, string> = {};
      for (const [path, contents] of args.files) {
        files[path] = contents;
      }
      await agentRepoStore.repoStore.writeTree(
        HUB_PRINCIPAL,
        repoId,
        DEFAULT_ASSET_REF,
        { files, message: "Write workflow deploy tree" },
      );
    },
  };
}

export function createSessionService(
  deps: SessionServiceDeps,
): SessionService & PreparedWorkflowDeployer {
  const {
    sidecarRouter,
    sidecarAllocationRouter,
    agentRepoStore,
    assetService,
    db,
    toolPackageRegistries,
  } = deps;

  if (assetService !== undefined && db === undefined) {
    throw new Error(
      "createSessionService: db is required when assetService is set",
    );
  }
  if (toolPackageRegistries !== undefined && db === undefined) {
    throw new Error(
      "createSessionService: db is required when toolPackageRegistries is set",
    );
  }

  function requireAllocationRouter(): SidecarAllocationRouter {
    if (sidecarAllocationRouter === undefined) {
      throw new Error("Exclusive deployment routing is not configured");
    }
    return sidecarAllocationRouter;
  }

  /**
   * Stage a deploy on the sidecar: resolve assets and tool packages, write
   * the deploy tree, provision the agent, and deliver the deploy + asset
   * packs (Phases 0-2b). Phase 1's provision has two shapes:
   *   - `workflowFrame` set: the single-step head hand-off fires the
   *     deployment `agent.deploy` frame that spawns the workflow-process
   *     child. Returns the supervisor public key.
   *   - `stageOnly` set: a multi-step per-step stage binds a transient route
   *     for the step address, fires a no-spawn provision frame (init repo +
   *     record hub key), and unbinds the route once the packs land. No
   *     child.
   * A call with neither is rejected -- the legacy warm-harness path
   * is gone.
   */
  async function executeLaunchPhases(params: {
    agentAddress: string;
    agentId: string;
    instanceId: string;
    config: HarnessConfig;
    deployContent: DeployContent;
    toolPackagePins?: readonly ToolPackagePin[];
    /**
     * Single-step workflow deploy. When present, Phase 1 fires the
     * deployment `agent.deploy` frame carrying the workflow definition +
     * source pins (the sidecar initializes the head repo on receipt and
     * spawns the workflow-process child) instead of the plain provision
     * frame. The returned supervisor public key comes from that frame's
     * ack.
     *
     * Mutually exclusive with `stageOnly`.
     */
    workflowFrame?: {
      definition: WorkflowDefinition;
      sources: Record<string, InferenceSource[]>;
      referencedDefinitions?: readonly ReferencedBodyDefinition[];
      credentials?: CredentialDelivery;
    };
    /**
     * Multi-step per-step stage. When true, Phase 1 binds a transient route
     * for the step address, fires a no-spawn provision frame (the sidecar
     * inits the step's agent-state repo and records the hub key), delivers
     * the deploy + asset packs, and unbinds the route -- no provision of a
     * warm harness and no child. The deployment-level workflow frame, sent
     * once after every step is staged, spawns the child. Returns no ack.
     * Mutually exclusive with `workflowFrame`.
     */
    stageOnly?: boolean;
    allocationTarget?: AllocatedSidecarTarget;
  }): Promise<{ publicKey: string } | undefined> {
    const { agentAddress, agentId, instanceId, config, deployContent } = params;
    const toolPackagePins = params.toolPackagePins ?? [];
    const stageOnly = params.stageOnly ?? false;
    if (params.workflowFrame !== undefined && stageOnly) {
      throw new Error(
        "executeLaunchPhases: workflowFrame and stageOnly are mutually exclusive",
      );
    }
    const workflowFrame = params.workflowFrame;

    let effectiveDeployContent: DeployContent = deployContent;

    // Phase 0a-bis: Resolve the agent's tool-package pins into a full
    // closure manifest. Empty pins skip the resolver entirely. A
    // ManifestInvalidError (e.g. unsatisfied peer dependency) is a
    // launch-time failure — the deploy never ships and the sidecar
    // is not touched.
    //
    // The resolver runs once per launch with no cross-launch caching;
    // the packument cache scopes only within a single closure walk.
    // Acceptable at the current N (handful of agents, small pin sets
    // per agent) — a tenant-scoped packument cache or a per-pin set
    // resolved-manifest cache would be the obvious scaling lever
    // when launch latency becomes the bottleneck.
    const manifestAssetAttachments: ResolvedAttachment[] = [];
    if (toolPackagePins.length > 0) {
      if (toolPackageRegistries === undefined) {
        throw new SessionLaunchError(
          "write",
          new Error(
            `agent ${agentId} has ${String(toolPackagePins.length)} pinned tool package(s) but the session service has no toolPackageRegistries configured`,
          ),
          false,
        );
      }
      if (assetService === undefined) {
        throw new SessionLaunchError(
          "write",
          new Error(
            `agent ${agentId} has pinned tool packages but the session service has no assetService configured for asset-backed registries`,
          ),
          false,
        );
      }
      let manifest: ToolPackageManifest;
      let assetIndex: Map<string, Asset>;
      try {
        const built = await buildAndResolve({
          agentId,
          tenantId: config.tenantId,
          pins: toolPackagePins,
          registries: toolPackageRegistries,
          assetService,
        });
        manifest = built.manifest;
        assetIndex = built.assetIndex;
      } catch (err) {
        if (err instanceof ManifestInvalidError) {
          logger.warn`tool-package manifest validation failed for agent ${agentId}: ${err.message}`;
        }
        throw new SessionLaunchError("write", err, false);
      }

      const assetMounts = new Map<string, string>();
      try {
        for (const assetId of collectDistinctAssetIds(manifest)) {
          const asset = assetIndex.get(assetId);
          if (asset === undefined) {
            // The asset id appears in the manifest but is not in the
            // tenant-visible asset set. This can only happen if the
            // resolver's registry map and the asset index disagree —
            // the same scan populated both, so reaching this branch
            // would indicate an upstream invariant violation.
            throw new Error(
              `resolved tool-package manifest references asset ${assetId} which is not visible to tenant ${config.tenantId}`,
            );
          }
          const mountPath = `package-registries/${asset.name}/`;
          assetMounts.set(assetId, mountPath);
          manifestAssetAttachments.push(
            await resolveAssetAttachment({
              asset,
              mountPath,
            }),
          );
        }
      } catch (err) {
        throw new SessionLaunchError("write", err, false);
      }

      effectiveDeployContent = {
        ...effectiveDeployContent,
        toolPackageManifest: manifest,
        ...(assetMounts.size > 0 ? { assetMounts } : {}),
      };
    }

    // Phase 0b: Write deploy tree and produce packfile (hub-local, no
    // sidecar state to clean up if this fails).
    let pack: Uint8Array;
    let commitSha: string;
    let ref: string;
    try {
      await agentRepoStore.writeDeployTree(agentId, effectiveDeployContent);
      ({ pack, commitSha, ref } =
        await agentRepoStore.createDeployPack(agentId));
    } catch (err) {
      throw new SessionLaunchError("write", err, false);
    }

    // A stage-only per-step deploy binds a transient route for the step
    // address so the packs below route to the deployment's sidecar; the
    // route is held only for the pack window and dropped in the `finally`.
    if (stageOnly) {
      try {
        if (params.allocationTarget === undefined) {
          sidecarRouter.bindStepRoute(agentAddress);
        } else {
          await requireAllocationRouter().bindAllocatedStepRoute(
            params.allocationTarget,
            agentAddress,
          );
        }
      } catch (err) {
        throw new SessionLaunchError("provision", err, false);
      }
    }
    try {
      // Phase 1: Provision on sidecar. A single-step workflow deploy sends
      // the deployment `agent.deploy` frame carrying the workflow definition
      // + source pins: the sidecar's deploy router initializes the head repo
      // on receipt (so the Phase 2 pack has a repo to apply into) and spawns
      // the workflow-process child. A stage-only per-step deploy sends a
      // no-spawn provision frame: the sidecar inits the step's agent-state
      // repo and records the hub key, but spawns nothing. Firing the frame
      // before the Phase 2 pack is the ordering barrier -- the repo must
      // exist before the pack applies. A workflow frame's ack surfaces the
      // supervisor public key to the caller.
      let deployAckPublicKey: string | undefined;
      try {
        if (workflowFrame !== undefined) {
          const ack = await sendMultiStepDeployFrame({
            lineage: "live",
            sidecarRouter,
            ...(sidecarAllocationRouter !== undefined
              ? { sidecarAllocationRouter }
              : {}),
            ...(params.allocationTarget !== undefined
              ? { allocationTarget: params.allocationTarget }
              : {}),
            agentAddress,
            config,
            definition: workflowFrame.definition,
            sources: workflowFrame.sources,
            ...(workflowFrame.referencedDefinitions !== undefined
              ? { referencedDefinitions: workflowFrame.referencedDefinitions }
              : {}),
            ...(workflowFrame.credentials !== undefined
              ? { credentials: workflowFrame.credentials }
              : {}),
          });
          deployAckPublicKey = ack.publicKey;
        } else if (stageOnly) {
          if (params.allocationTarget === undefined) {
            await sidecarRouter.sendProvisionStep(agentAddress, config);
          } else {
            await requireAllocationRouter().sendProvisionStepToAllocation(
              params.allocationTarget,
              agentAddress,
              config,
            );
          }
        } else {
          // Every caller supplies `workflowFrame` (single-step head) or
          // `stageOnly` (multi-step per-step). A deploy with neither has no
          // provisioning shape -- the legacy warm-harness path is gone -- so
          // fail loud rather than ship a deploy pack the sidecar never
          // provisioned a repo for.
          throw new Error(
            "executeLaunchPhases: a deploy requires either workflowFrame or stageOnly",
          );
        }
      } catch (err) {
        throw new SessionLaunchError("provision", err, false);
      }

      // Phase 2: Pack delivery. On failure, the warm/workflow paths tear the
      // sidecar deployment down; a stage-only step has no supervisor to
      // undeploy, so it only drops its transient route (in the `finally`).
      // The step's inited agent-state repo is left on the sidecar: the
      // orchestrator aborts the whole deploy before the deployment frame is
      // sent, so there is nothing to undeploy, and a redeploy of the same
      // deployment overwrites the orphaned repo. This is an acceptable minor
      // leak on the exceptional staging-failure path, not a live-path cost.
      try {
        if (params.allocationTarget === undefined) {
          await sidecarRouter.sendPack(agentAddress, pack, ref, commitSha);
        } else {
          await requireAllocationRouter().sendPackToAllocation(
            params.allocationTarget,
            agentAddress,
            pack,
            ref,
            commitSha,
          );
        }
      } catch (err) {
        if (!stageOnly && params.allocationTarget === undefined) {
          await attemptCleanup(agentAddress, "pack", err);
        }
        throw new SessionLaunchError(
          "pack",
          err,
          !stageOnly && params.allocationTarget !== undefined,
        );
      }

      // Phase 2b: Asset-pack fan-out. For each attached asset, build a
      // pack, reserve the manifest row, then send the pack. The manifest
      // reservation MUST happen before the pack send: if the sidecar acks
      // but the row is missing, the session has materialization without
      // a recorded manifest. An allocated replacement may reuse the exact
      // row its predecessor recorded; ordinary launches still require a new
      // row. If reservation fails, the pack send must not happen.
      //
      // The fan-out materializes the package-registry assets the
      // tool-package resolver picked. They live behind tenant
      // inheritance rather than a per-agent attachment row, so the
      // session service synthesizes the attachment view in
      // `manifestAssetAttachments`.
      const fanOut: ResolvedAttachment[] = manifestAssetAttachments;
      if (assetService !== undefined && fanOut.length > 0) {
        // Track the rows this attempt owns so a later fan-out failure can roll
        // them back in lockstep with the sidecar undeploy. Allocated rows are
        // durable recovery intent, not attempt-owned materialization state, so
        // replacement failures must leave them in place for the next worker.
        const committed: SessionAssetRecord[] = [];
        for (const att of fanOut) {
          try {
            const committedRecord = await sendAttachmentPack(
              instanceId,
              agentAddress,
              att,
              params.allocationTarget,
            );
            if (committedRecord !== null) committed.push(committedRecord);
          } catch (err) {
            await rollbackCommittedAttachments(committed);
            if (!stageOnly && params.allocationTarget === undefined) {
              await attemptCleanup(agentAddress, "pack", err);
            }
            throw new SessionLaunchError(
              "pack",
              err,
              !stageOnly && params.allocationTarget !== undefined,
            );
          }
        }
      }

      return deployAckPublicKey === undefined
        ? undefined
        : { publicKey: deployAckPublicKey };
    } finally {
      if (stageOnly) {
        if (params.allocationTarget === undefined) {
          sidecarRouter.unbindStepRoute(agentAddress);
        } else {
          requireAllocationRouter().unbindAllocatedStepRoute(
            params.allocationTarget,
            agentAddress,
          );
        }
      }
    }
  }

  /**
   * Deploy a one-step workflow once at the head. Reuses the full
   * launch-phase machinery (deploy-tree write, pack, asset fan-out) via
   * `executeLaunchPhases`, swapping the Phase 1 provision frame for the
   * workflow frame. The workflow frame makes the sidecar initialize the
   * head repo and spawn the workflow-process child; the follow-up pack
   * lands the head's deploy tree. Returns the supervisor's principal
   * public key from the frame's ack. A workflow-frame launch always
   * yields a deploy-ack key; its absence is a wiring bug, not a
   * tolerable case.
   */
  async function deploySingleStepAtHeadForRoute(
    deployParams: Parameters<DeploySingleStepFn>[0],
    allocationTarget?: AllocatedSidecarTarget,
  ): Promise<{ publicKey: string }> {
    const result = await executeLaunchPhases({
      agentAddress: deployParams.agentAddress,
      agentId: deployParams.agentId,
      instanceId: deployParams.instanceId,
      config: deployParams.config,
      deployContent: bridgeOrchestratorDeployContent(
        deployParams.deployContent,
      ),
      workflowFrame: {
        definition: deployParams.definition,
        sources: deployParams.sources,
        ...(deployParams.referencedDefinitions !== undefined
          ? { referencedDefinitions: deployParams.referencedDefinitions }
          : {}),
        ...(deployParams.credentials !== undefined
          ? { credentials: deployParams.credentials }
          : {}),
      },
      ...(deployParams.toolPackagePins !== undefined
        ? { toolPackagePins: deployParams.toolPackagePins }
        : {}),
      ...(allocationTarget !== undefined ? { allocationTarget } : {}),
    });
    if (result === undefined) {
      throw new Error(
        "single-step deploy at head: executeLaunchPhases returned no deploy-ack public key for a workflow-frame deploy",
      );
    }
    return result;
  }

  const deploySingleStepAtHead: DeploySingleStepFn = (deployParams) =>
    deploySingleStepAtHeadForRoute(deployParams);

  /**
   * Build the workflow-deploy orchestrator (with its launch-session and
   * multi-step callbacks) and run one deploy. Shared by `launchSession`
   * and `deployWorkflowDefinition`, which differ only in the workflow
   * repo writer, the director registry, and the deploy args.
   */
  async function runWorkflowDeploy(args: {
    workflowRepo: WorkflowRepoWriter;
    directorRegistry: DirectorRegistry;
    deployArgs: DeployWorkflowArgs;
    allocationTarget?: AllocatedSidecarTarget;
  }): Promise<DeployWorkflowResult> {
    // The per-step launcher: stage each step's deploy tree WITHOUT a warm
    // harness (the supervised child runs the step), with the orchestrator's
    // structural `DeployContent` narrowed back to the hub-sessions shape
    // first.
    const launchSessionCallback: LaunchSessionFn = (orchestratorParams) =>
      stageWorkflowStep({
        agentAddress: orchestratorParams.agentAddress,
        agentId: orchestratorParams.agentId,
        instanceId: orchestratorParams.instanceId,
        config: orchestratorParams.config,
        deployContent: bridgeOrchestratorDeployContent(
          orchestratorParams.deployContent,
        ),
        ...(orchestratorParams.toolPackagePins !== undefined
          ? { toolPackagePins: orchestratorParams.toolPackagePins }
          : {}),
        ...(args.allocationTarget !== undefined
          ? { allocationTarget: args.allocationTarget }
          : {}),
      });

    const sendMultiStepDeployCallback: SendMultiStepDeployFn = (deployParams) =>
      sendMultiStepDeployFrame({
        lineage: "live",
        sidecarRouter,
        ...(sidecarAllocationRouter !== undefined
          ? { sidecarAllocationRouter }
          : {}),
        ...(args.allocationTarget !== undefined
          ? { allocationTarget: args.allocationTarget }
          : {}),
        agentAddress: deployParams.agentAddress,
        config: deployParams.config,
        definition: deployParams.definition,
        sources: deployParams.sources,
        ...(deployParams.referencedDefinitions !== undefined
          ? { referencedDefinitions: deployParams.referencedDefinitions }
          : {}),
      });

    const orchestrator = createWorkflowDeployOrchestrator({
      directorRegistry: args.directorRegistry,
      workflowRepo: args.workflowRepo,
      launchSession: launchSessionCallback,
      sendMultiStepDeploy: sendMultiStepDeployCallback,
      deploySingleStepAtHead: (deployParams) =>
        deploySingleStepAtHeadForRoute(deployParams, args.allocationTarget),
    });

    return orchestrator.deployWorkflow(args.deployArgs);
  }

  /**
   * Stage one step of a multi-step workflow deploy: bind a transient route
   * for the step address, fire a no-spawn provision frame (the sidecar inits
   * the step's agent-state repo and records the hub key), deliver the deploy
   * and asset packs, and unbind the route -- no warm harness. The multi-step
   * branch stages every step this way, then fires ONE deployment-level
   * workflow frame that writes the step grants and spawns the supervised
   * workflow-process child; the child reads each step's staged deploy tree
   * from disk and runs the step itself.
   */
  async function stageWorkflowStep(params: {
    agentAddress: string;
    agentId: string;
    instanceId: string;
    config: HarnessConfig;
    deployContent: DeployContent;
    toolPackagePins?: readonly ToolPackagePin[];
    allocationTarget?: AllocatedSidecarTarget;
  }): Promise<void> {
    await executeLaunchPhases({
      agentAddress: params.agentAddress,
      agentId: params.agentId,
      instanceId: params.instanceId,
      config: params.config,
      deployContent: params.deployContent,
      stageOnly: true,
      ...(params.toolPackagePins !== undefined
        ? { toolPackagePins: params.toolPackagePins }
        : {}),
      ...(params.allocationTarget !== undefined
        ? { allocationTarget: params.allocationTarget }
        : {}),
    });
  }

  /**
   * Deploy a single-agent instance through the single-step-at-head path: wrap
   * the harness as a one-step workflow (the same wrap `launchSession` uses) and
   * route it through `deploySingleStepAtHead` with the instance's REAL identity
   * -- so the head address IS the instance address and the deploy runs as a
   * supervised workflow-process child.
   *
   * Unlike the orchestrator's `runSingleStepAtHead` (which derives its deploy
   * key from the deployment), this passes the instance id as the `agentId`
   * deploy key -- the id the head address encodes and every deploy-ref reader
   * resolves by, so the hub-written deploy tree and the sidecar's state
   * writeback share one repo. The child resolves its skills and tool-package
   * pins by mailbox address, not by this key. It records no deployment anchor
   * run (a plain instance has no workflow asset). Returns the head's agent-key
   * ack.
   */
  async function deployInstanceAtHead(params: {
    agentAddress: string;
    agentId: string;
    instanceId: string;
    config: HarnessConfig;
    deployContent: DeployContent;
    toolPackagePins?: readonly ToolPackagePin[];
    credentials?: CredentialDelivery;
  }): Promise<{ publicKey: string }> {
    const { agentAddress, agentId, instanceId, config, deployContent } = params;

    const singleStepAgent = wrapHarnessAsSingleStepWorkflow({
      config,
      deployContent,
    });
    const workflow = defineWorkflow({
      id: `wf_${agentId}`,
      agent: singleStepAgent,
      trigger: { type: "mail", to: agentAddress },
    });

    // The sole step's id, read off the built definition.
    const stepId = workflow.stepOrder[0];
    if (stepId === undefined) {
      throw new Error(
        `instance deploy for ${agentAddress}: the wrapped single-step workflow has an empty stepOrder`,
      );
    }

    // Pin the step's inference sources to the instance's FULL ordered source
    // chain so the workflow-process child's reactor fails over across it at
    // runtime. The route already resolved and authorized `config.sources`
    // against the tenant catalog, so the chain is pinned directly with NO
    // operator-approval sweep: the operator-approval gate does not apply on
    // the pre-authorized instance path (unlike the workflow deploy path,
    // which gates every source in the chain). Only the reactor's
    // head-is-default invariant is enforced here.
    assertChainHeadIsDefault({
      sources: config.sources,
      defaultSource: config.defaultSource,
      workflowId: workflow.id,
    });

    return deploySingleStepAtHead({
      agentAddress,
      agentId,
      instanceId,
      config,
      deployContent,
      definition: workflow,
      sources: { [stepId]: config.sources },
      hubPublicKey: hexEncode(agentRepoStore.getSigningPublicKey()),
      ...(params.toolPackagePins !== undefined
        ? { toolPackagePins: params.toolPackagePins }
        : {}),
      ...(params.credentials !== undefined
        ? { credentials: params.credentials }
        : {}),
    });
  }

  async function executeWorkflowDefinitionDeploy(
    params: Omit<DeployWorkflowDefinitionParams, "definitionAssetId"> & {
      allocationTarget?: AllocatedSidecarTarget;
    },
  ): Promise<DeployWorkflowDefinitionResult> {
    // The deploy is initiated by an authorized tenant operator against a
    // workflow asset they authored; approve exactly the grant surface the
    // definition declares. The same director registry feeds both this
    // approval-set derivation and the orchestrator's gate so the walk the
    // route approves and the walk the orchestrator enforces are identical.
    const directorRegistry = createDefaultDirectorRegistry();
    const walk = walkCapabilities(params.definition, directorRegistry);
    const operatorApprovals: ApprovalSet = new Set<string>(
      [...walk.perStep.values()].flatMap((declarations) => [
        ...declarations.grants,
      ]),
    );

    const result = await runWorkflowDeploy({
      workflowRepo: createHubWorkflowRepoWriter(agentRepoStore),
      directorRegistry,
      deployArgs: {
        workflow: params.definition,
        deploymentId: params.deploymentId,
        deploymentDomain: params.deploymentDomain,
        config: params.config,
        deployContent: params.deployContent,
        operatorApprovals,
        hubPublicKey: hexEncode(agentRepoStore.getSigningPublicKey()),
        ...(params.toolPackagePins !== undefined
          ? { toolPackagePins: params.toolPackagePins }
          : {}),
      },
      ...(params.allocationTarget !== undefined
        ? { allocationTarget: params.allocationTarget }
        : {}),
    });

    return {
      deploymentId: params.deploymentId,
      deploymentAddress: deriveDeploymentAddress({
        deploymentId: params.deploymentId,
        deploymentDomain: params.deploymentDomain,
      }),
      publicKey: result.publicKey,
    };
  }

  async function deployWorkflowDefinition(
    params: DeployWorkflowDefinitionParams,
  ): Promise<DeployWorkflowDefinitionResult> {
    const {
      tenantId,
      deploymentId,
      deploymentDomain,
      definitionAssetId,
      config,
    } = params;
    const result = await executeWorkflowDefinitionDeploy(params);

    if (db === undefined) {
      throw new Error(
        "deployWorkflowDefinition requires a db handle to record the deployment's anchor run",
      );
    }
    // The wire-projection hash keys the definition's selector-keyed identity:
    // one asset backs many definitions, distinguished by this content handle.
    const wireHash = await computeLiveDefinitionHash(definition);
    const now = new Date();
    await db.transaction(async (tx) => {
      // Project the workflow asset into a first-class definition (create-if-
      // absent) so the anchor run can carry it. A native workflow's definition
      // is otherwise born only in the one-time backfill; creating it here makes
      // every deploy yield a definition, so the run's `definitionId` is
      // populated at birth rather than only for the rows the backfill reached.
      const { definitionId } = await ensureWorkflowDefinitionForAsset(tx, {
        assetId: definitionAssetId,
        wireHash,
      });

      // The deployment's anchor run: the one workflow_run that carries the
      // deployment's routing identity, 1:1 with the deployment (id and address
      // both derived from `deploymentId`). It is the deployment's sole
      // first-class record -- the row that owns the address and public key the
      // reconnect ownership challenge verifies: deploy-ack writes the key here
      // and the key lookup reads it off this row. It is born running with no key
      // yet (deploy-ack fills it), carrying its definition. Its `deploymentId`
      // equals its own id, so the anchor row references itself. Child runs of
      // this deployment are separate address-less rows. `principalId` is null --
      // the workflow-derived key path reads `publicKey` directly and never
      // consults it, and the `workflow-run:<deploymentId>` grant seeded below
      // already covers reads.
      await tx.insert(workflowRunTable).values({
        id: deploymentId,
        tenantId,
        deploymentId,
        definitionId,
        address: deriveDeploymentAddress({ deploymentId, deploymentDomain }),
        publicKey: result.publicKey,
        status: "running",
        createdAt: now,
      });

      // Seed a read grant on the deployment's workflow-run resource for the
      // deploying principal so they can observe run events out of the box,
      // mirroring the per-instance agent-state read grant the agent deploy
      // path seeds for the creator. Without this a non-owner deployer would
      // deploy a workflow they cannot read the runs of.
      await tx.insert(grantTable).values({
        id: generateId("grant"),
        tenantId,
        principalId: config.principalId,
        resource: `workflow-run:${deploymentId}`,
        action: "read",
        effect: "allow",
        origin: "creator",
        createdAt: now,
        updatedAt: now,
      });
    });

    return result;
  }

  async function deployPreparedWorkflowDefinition(
    params: DeployPreparedWorkflowDefinitionParams,
  ): Promise<DeployWorkflowDefinitionResult> {
    if (db === undefined) {
      throw new Error(
        "deployPreparedWorkflowDefinition requires a db handle to update the prepared anchor run",
      );
    }
    await restoreWorkflowRunToAllocation({
      agentRepoStore,
      allocationRouter: requireAllocationRouter(),
      allocationTarget: params.allocationTarget,
      agentAddress: deriveDeploymentAddress({
        deploymentId: params.deploymentId,
        deploymentDomain: params.deploymentDomain,
      }),
    });
    const result = await executeWorkflowDefinitionDeploy(params);
    try {
      const updated = await db.transaction(async (tx) => {
        const [allocation] = await tx
          .select({
            id: sidecarAllocationTable.id,
            anchorRunId: sidecarAllocationTable.anchorRunId,
            status: sidecarAllocationTable.status,
            generation: sidecarAllocationTable.generation,
            ensureAcceptedGeneration:
              sidecarAllocationTable.ensureAcceptedGeneration,
          })
          .from(sidecarAllocationTable)
          .where(
            eq(sidecarAllocationTable.id, params.allocationTarget.allocationId),
          )
          .limit(1)
          .for("update");
        if (
          allocation === undefined ||
          allocation.anchorRunId !== params.deploymentId ||
          allocation.status !== "allocated" ||
          allocation.generation !== params.allocationTarget.generation ||
          allocation.ensureAcceptedGeneration !==
            params.allocationTarget.generation
        ) {
          return null;
        }
        const [anchor] = await tx
          .update(workflowRunTable)
          .set({ publicKey: result.publicKey })
          .where(
            and(
              eq(workflowRunTable.id, params.deploymentId),
              eq(workflowRunTable.deploymentId, params.deploymentId),
              eq(workflowRunTable.tenantId, params.tenantId),
            ),
          )
          .returning({ id: workflowRunTable.id });
        return anchor ?? null;
      });
      if (updated === null) {
        throw new Error(
          `Prepared anchor run ${params.deploymentId} lost allocation ownership before initialization completed`,
        );
      }
    } catch (error) {
      throw new SessionLaunchError("start", error, true);
    }
    return result;
  }

  async function rollbackCommittedAttachments(
    committed: readonly SessionAssetRecord[],
  ): Promise<void> {
    if (db === undefined) return;
    if (committed.length === 0) return;
    // Per-row try/catch so a single rollback failure does not stop the
    // sweep — every committed row needs to come off the books before
    // the caller emits the original sendPack error.
    for (const record of committed) {
      try {
        await db
          .delete(sessionAssetTable)
          .where(
            and(
              eq(sessionAssetTable.instanceId, record.instanceId),
              eq(sessionAssetTable.mountPath, record.mountPath),
              eq(sessionAssetTable.assetPackSha, record.assetPackSha),
              eq(sessionAssetTable.sourceCommitSha, record.sourceCommitSha),
            ),
          );
      } catch (err) {
        logger.warn`session_asset rollback failed for earlier-committed instance=${record.instanceId} mountPath=${record.mountPath}: ${err instanceof Error ? err.message : String(err)}`;
      }
    }
  }

  async function sendAttachmentPack(
    instanceId: string,
    agentAddress: string,
    attachment: ResolvedAttachment,
    allocationTarget?: AllocatedSidecarTarget,
  ): Promise<SessionAssetRecord | null> {
    if (db === undefined) {
      // Guarded at construction; reassert defensively so the
      // narrowing is visible to readers and a future refactor cannot
      // accidentally invoke this without a db.
      throw new Error("sendAttachmentPack invoked without a db handle");
    }

    const { mountPath, sourceCommitSha, repoId, pack, ref } = attachment;

    const assetPackSha = await createPackSha(pack);
    const record: SessionAssetRecord = {
      instanceId,
      mountPath,
      assetPackSha,
      sourceCommitSha,
    };

    // Reserve the manifest row before the pack send so we never end up in the
    // materialized-without-manifest state. Only an allocated launch may reuse
    // an identical row: replacement workers keep the stable instance id and
    // mount path, while the shared path retains its duplicate-launch guard.
    const rollbackRecord = allocationTarget === undefined ? record : null;
    if (allocationTarget === undefined) {
      await db
        .insert(sessionAssetTable)
        .values({ ...record, materializedAt: new Date() });
    } else {
      const inserted = await db
        .insert(sessionAssetTable)
        .values({ ...record, materializedAt: new Date() })
        .onConflictDoNothing({
          target: [sessionAssetTable.instanceId, sessionAssetTable.mountPath],
        })
        .returning({ instanceId: sessionAssetTable.instanceId });
      if (inserted.length === 0) {
        const existing = await db.query.sessionAsset.findFirst({
          where: and(
            eq(sessionAssetTable.instanceId, instanceId),
            eq(sessionAssetTable.mountPath, mountPath),
          ),
          columns: {
            assetPackSha: true,
            sourceCommitSha: true,
          },
        });
        if (existing === undefined) {
          throw new Error(
            `session_asset ${instanceId}/${mountPath} disappeared after its insert conflicted`,
          );
        }
        if (
          existing.assetPackSha !== assetPackSha ||
          existing.sourceCommitSha !== sourceCommitSha
        ) {
          throw new Error(
            `session_asset ${instanceId}/${mountPath} conflicts with the allocated workflow's restored asset`,
          );
        }
      }
    }

    try {
      const options = { mountPath, repoId };
      if (allocationTarget === undefined) {
        await sidecarRouter.sendPack(
          agentAddress,
          pack,
          ref,
          sourceCommitSha,
          options,
        );
      } else {
        await requireAllocationRouter().sendPackToAllocation(
          allocationTarget,
          agentAddress,
          pack,
          ref,
          sourceCommitSha,
          options,
        );
      }
    } catch (err) {
      // Shared launches own the row they just created and roll it back when
      // the send fails. Allocated rows are durable recovery intent: even a row
      // first inserted by this attempt can already be reused by another
      // reconciler, so no replacement attempt may delete it.
      // The forensic value of a manifest-without-materialization row is
      // negligible because no agent will read against it. Wrap the
      // rollback in its own try/catch so a rollback failure (DB gone,
      // connection killed mid-launch) is logged rather than masking the
      // primary sendPack error — the caller needs to see the original
      // failure, not the secondary one.
      if (rollbackRecord !== null) {
        try {
          await db
            .delete(sessionAssetTable)
            .where(
              and(
                eq(sessionAssetTable.instanceId, rollbackRecord.instanceId),
                eq(sessionAssetTable.mountPath, rollbackRecord.mountPath),
                eq(sessionAssetTable.assetPackSha, rollbackRecord.assetPackSha),
                eq(
                  sessionAssetTable.sourceCommitSha,
                  rollbackRecord.sourceCommitSha,
                ),
              ),
            );
        } catch (rollbackErr) {
          const msg =
            rollbackErr instanceof Error
              ? rollbackErr.message
              : String(rollbackErr);
          logger.warn`session_asset rollback failed for instance=${instanceId} mountPath=${mountPath}: ${msg}`;
        }
      }
      throw err;
    }
    return rollbackRecord;
  }

  /**
   * Build a per-agent `ClosureResolver` from the tenant's visible
   * package-registry assets plus the statically-configured HTTP
   * registries, then run the closure resolution against `pins`.
   *
   * Returns the resolved manifest and an asset-id-keyed index of the
   * package-registry assets the resolver knew about, so the caller can
   * derive mount paths from the asset name without a second DB hit.
   */
  async function buildAndResolve(args: {
    agentId: string;
    tenantId: string;
    pins: readonly ToolPackagePin[];
    registries: NonNullable<SessionServiceDeps["toolPackageRegistries"]>;
    assetService: AssetService;
  }): Promise<{
    manifest: ToolPackageManifest;
    assetIndex: Map<string, Asset>;
  }> {
    if (db === undefined) {
      // Guarded at construction; restate for the narrowing.
      throw new Error("buildAndResolve invoked without a db handle");
    }
    const visibleAssets = await listAssetsForTenant(
      db,
      args.tenantId,
      "package-registry",
    );
    const registryMap = new Map<string, RegistrySource>();
    // `assetIndex` carries only the assets the resolver might have
    // read from — i.e. one row per registry name, the one that won
    // its `(kind, name)` slot. Shadowed assets that lost the
    // collision are deliberately excluded: the resolver can never
    // reach them, so the fan-out path must never see them in the
    // index either. The walker walks leaf-to-root inside
    // `listAssetsForTenant`, so the first occurrence of any
    // `(kind, name)` wins — we replay the same shadowing here.
    // Shadowed assets — those that lose the `(kind, name)` collision
    // contest at a lower tenancy level — are dropped entirely from
    // the per-launch registry map. They never appear in `assetIndex`
    // either, so the fan-out that translates `kind: "asset"` manifest
    // entries back to asset rows cannot reach them. This matches the
    // resolver's view: a closure built from this map sees exactly
    // the assets the resolver would have read from, and shadowed
    // tarballs are invisible to both layers.
    const assetIndex = new Map<string, Asset>();
    for (const row of visibleAssets) {
      if (registryMap.has(row.name)) continue;
      const asset: Asset = {
        id: row.id,
        tenantId: row.tenantId,
        kind: "package-registry",
        name: row.name,
        displayName: row.displayName,
        creatorPrincipalId: row.creatorPrincipalId,
        createdAt: row.createdAt,
        updatedAt: row.updatedAt,
      };
      assetIndex.set(asset.id, asset);
      registryMap.set(
        asset.name,
        new AssetRegistrySource({
          name: asset.name,
          assetId: asset.id,
          readBlob: (path) =>
            args.assetService.readAssetBlob({
              assetId: asset.id,
              path,
            }),
          listBlobs: (dir) =>
            args.assetService.listAssetBlobs({
              assetId: asset.id,
              dir,
            }),
        }),
      );
    }
    for (const [name, cfg] of args.registries.httpRegistries) {
      // Asset wins on collision with an HTTP registry of the same
      // name; symmetric with the inner-shadows-outer rule that
      // governs the tenant walker.
      if (registryMap.has(name)) continue;
      registryMap.set(name, new HttpRegistrySource({ name, config: cfg }));
    }
    if (!registryMap.has(args.registries.defaultRegistry)) {
      throw new Error(
        `agent ${args.agentId}: defaultRegistry "${args.registries.defaultRegistry}" is neither a tenant-visible package-registry asset nor a configured HTTP registry`,
      );
    }
    const resolver = createClosureResolver({
      registries: registryMap,
      defaultRegistry: args.registries.defaultRegistry,
      ...(args.registries.scopeRouting !== undefined
        ? { scopeRouting: args.registries.scopeRouting }
        : {}),
    });
    const manifest = await resolver.resolveClosure(args.pins);
    return { manifest, assetIndex };
  }

  /**
   * Build a `ResolvedAttachment` for an asset the tool-package resolver
   * picked from. The pack is read from the asset's main ref (the same
   * ref the resolver consumed tarballs from).
   */
  async function resolveAssetAttachment(args: {
    asset: Asset;
    mountPath: string;
  }): Promise<ResolvedAttachment> {
    const repoId: RepoId = { kind: args.asset.kind, id: args.asset.id };
    const sourceCommitSha = await agentRepoStore.repoStore.resolveRef(
      HUB_PRINCIPAL,
      repoId,
      DEFAULT_ASSET_REF,
    );
    if (sourceCommitSha === null) {
      throw new Error(
        `tool-package asset ${args.asset.kind}/${args.asset.id} has no commit on ${DEFAULT_ASSET_REF}`,
      );
    }
    const { pack, ref: returnedRef } =
      await agentRepoStore.repoStore.createPack(
        HUB_PRINCIPAL,
        repoId,
        DEFAULT_ASSET_REF,
      );
    return {
      mountPath: args.mountPath,
      sourceCommitSha,
      repoId,
      pack,
      ref: returnedRef,
    };
  }

  async function attemptCleanup(
    agentAddress: string,
    failedPhase: string,
    originalErr: unknown,
  ): Promise<void> {
    try {
      await sidecarRouter.sendAgentUndeploy(agentAddress, failedPhase);
    } catch (cleanupErr) {
      logger.error`Failed to clean up agent ${agentAddress} after ${failedPhase} failure: ${String(cleanupErr)}`;
      // Preserve the original error as cause so the root cause is not
      // lost when the cleanup also fails.
      throw new SessionLaunchError(failedPhase, originalErr, true);
    }
  }

  async function sendUserMessage(
    params: UserMessageParams,
  ): Promise<Uint8Array> {
    const {
      agentAddress,
      from,
      messageId,
      date,
      content,
      attachments,
      inReplyTo,
      references,
      sessionId,
      tenantId,
      cryptoProvider,
    } = params;

    const headers: MessageHeaders = {
      from,
      to: [agentAddress],
      cc: undefined,
      date,
      messageId,
      subject: undefined,
      inReplyTo,
      references,
      mimeVersion: "1.0",
      interchangeType: "conversation.message",
      interchangeCorrelationId: undefined,
      interchangeTenantId: tenantId,
      interchangeAgentId: undefined,
      interchangeSessionId: sessionId,
      interchangeOfferingId: undefined,
      interchangeSchemaVersion: undefined,
      traceparent: undefined,
      tracestate: undefined,
    };

    const signedContent = assembleSignedContent({
      kind: "conversation",
      text: content,
      ...(attachments !== undefined ? { attachments } : {}),
    });
    const signature = await createDetachedSignatureFromProvider(
      signedContent,
      cryptoProvider,
    );
    const rawMessage = assembleMessage(headers, signedContent, signature);
    const base64 = base64Encode(rawMessage);

    const delivered = sidecarRouter.routeMail(agentAddress, base64, messageId);
    if (!delivered) {
      throw new Error(
        `Failed to deliver message to ${agentAddress}: agent is unreachable`,
      );
    }

    return rawMessage;
  }

  async function endSession(
    agentAddress: string,
    reason: string,
  ): Promise<void> {
    await sidecarRouter.sendAgentUndeploy(agentAddress, reason);
  }

  return {
    stageWorkflowStep,
    deployInstanceAtHead,
    deploySingleStepAtHead,
    deployWorkflowDefinition,
    deployPreparedWorkflowDefinition,
    sendUserMessage,
    endSession,
  };
}

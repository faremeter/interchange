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
  workflowDeployment as workflowDeploymentTable,
} from "@intx/db/schema";
import { base64Encode, hexEncode } from "@intx/types";
import { generateId } from "@intx/hub-common";
import {
  sessionAsset as sessionAssetTable,
  type SessionAssetSource,
} from "@intx/db/schema";
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
import {
  defineWorkflow,
  type WorkflowDefinition,
} from "@intx/workflow/definition";
import {
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
  type SendMultiStepDeployFn,
  type WorkflowRepoWriter,
} from "@intx/workflow-deploy";

import type { AgentRepoStore, DeployContent } from "./agent-repo";
import {
  DEFAULT_ASSET_REF,
  type AgentAssetWithAsset,
  type Asset,
  type AssetService,
} from "./asset-service";
import {
  buildAvailableSkillsStanza,
  type AvailableSkillEntry,
} from "./available-skills-stanza";
import { getSkillIndex } from "./skill-kind";
import type { SidecarRouter } from "./ws/sidecar-handler";
import type { Principal, RepoId } from "./repo-store";

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
  }): Promise<void>;

  /**
   * Deploy a single-agent instance through the single-step-at-head path,
   * wrapping the harness as a one-step workflow and routing it through the
   * deploy core with the instance's real identity. Replaces `launchSession`
   * as the production instance-deploy entry point: the instance runs as a
   * supervised workflow-process child rather than the legacy trivial
   * in-process path. Writes no `workflow_deployment` row. Returns the head's
   * agent-key ack (the key the head signs its reconnect challenges with).
   */
  deployInstanceAtHead(params: {
    agentAddress: string;
    agentId: string;
    instanceId: string;
    config: HarnessConfig;
    deployContent: DeployContent;
    toolPackagePins?: readonly ToolPackagePin[];
  }): Promise<{ publicKey: string }>;

  /**
   * Deploy a one-step workflow once at the head through the deploy core,
   * without the DB-backed `workflow_deployment` projection row. Stages the
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
   * Persists one `workflow_deployment` projection row keyed by
   * `deploymentId` so the deployment is listable per tenant; the
   * RepoStore substrate has no by-kind listing API of its own.
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
  /** Owning tenant; recorded on the projection row. */
  tenantId: string;
  /**
   * Stable deployment identifier. The orchestrator concatenates it into
   * every derived per-step address and the deployment-level address, and
   * it is the `workflow_deployment` row's primary key. The caller owns
   * its generation.
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

export type DeployWorkflowDefinitionResult = {
  /** Echoes the deployment id recorded on the projection row. */
  deploymentId: string;
  /** Deployment-level mail address the supervisor registers on the bus. */
  deploymentAddress: string;
  /** Supervisor principal public key from the sidecar's deploy ack. */
  publicKey: string;
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

// Hub-side principal for reading skill repos. Skills are signed by the
// hub itself, and listAgentAssets is being called on the hub to assemble
// packs for delivery to a sidecar — so the hub principal is correct.
const HUB_PRINCIPAL: Principal = { kind: "hub" };

type ResolvedAttachment = {
  /** The `agent_asset` row id when this attachment came from an
   * explicit `agent_asset` attachment; `null` when it came from a
   * resolver-derived package-registry asset that has no per-agent
   * attachment row. The `session_asset.source` column distinguishes
   * the two paths in the audit record. */
  agentAssetId: string | null;
  /** Which materialization path produced this attachment.
   * `"direct"` mirrors `agentAssetId !== null`; `"resolved"` mirrors
   * `agentAssetId === null`. The session-asset row carries the
   * resolved value so audit queries can filter without joining. */
  source: SessionAssetSource;
  /** Asset `name` column. Used to build the qualified `<asset.name>/<skill-name>`
   * prefix in the `<available_skills>` stanza. */
  assetName: string;
  /** Asset `kind` column, used to gate skill-index lookups. */
  assetKind: AgentAssetWithAsset["asset"]["kind"];
  mountPath: string;
  sourceCommitSha: string;
  repoId: RepoId;
  pack: Uint8Array;
  ref: string;
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
 * Dedup the union of `direct` and `resolved` attachments by asset id
 * (taken from `repoId.id`), with `direct` taking precedence whenever
 * both name the same asset.
 *
 * The package-registry "both name the same asset" case is refused
 * upstream at the resolver block (a direct attachment plus a resolver
 * pin for the same package-registry asset would emit assetMounts at
 * the resolver's ref while the direct attachment materializes at the
 * operator's chosen ref, leaving the loader to resolve manifest
 * entries against tarballs that do not exist at the materialized
 * mount). Skill attachments cannot collide via the resolver path —
 * the resolver only emits package-registry entries — so the dedup
 * still has to handle skill self-collisions defensively and to fall
 * through cleanly when both sources happen to name an asset the
 * upstream check has not flagged.
 *
 * The function takes the two sources as named parameters rather than a
 * pre-merged list so the precedence rule is structural: a future
 * refactor cannot accidentally swap the order by re-arranging an
 * intermediate spread.
 */
function dedupAttachmentsByAssetId(args: {
  direct: readonly ResolvedAttachment[];
  resolved: readonly ResolvedAttachment[];
}): ResolvedAttachment[] {
  const seen = new Set<string>();
  const out: ResolvedAttachment[] = [];
  for (const att of args.direct) {
    if (seen.has(att.repoId.id)) continue;
    seen.add(att.repoId.id);
    out.push(att);
  }
  for (const att of args.resolved) {
    if (seen.has(att.repoId.id)) continue;
    seen.add(att.repoId.id);
    out.push(att);
  }
  return out;
}

/**
 * Compute the materialization path for an attachment from the asset's
 * kind and name. v1 does not let users override the path — the path is
 * a function of the asset, full stop. Today only `skill` has a defined
 * mapping (`skills/<asset.name>/`); other kinds reach this code path
 * via the `never` branch and throw, per the defensive-coding rule that
 * we never silently invent a default for an unhandled kind.
 *
 * Asset names are validated lowercase-kebab at `createAsset`, which is
 * the only entry path into this function, so the resulting path is
 * safe under `applyAssetPack`'s per-segment validator.
 */
function resolveMountPath(row: AgentAssetWithAsset): string {
  switch (row.asset.kind) {
    case "skill":
      return `skills/${row.asset.name}/`;
    case "package-registry":
      return `package-registries/${row.asset.name}/`;
    case "agent-state":
      throw new Error(
        `mount_path_required: agent_asset row ${row.id} references agent-state asset ${row.asset.id}; agent-state attachments are not supported`,
      );
    case "workflow":
      throw new Error("kind handler not yet registered: workflow");
    case "workflow-run":
      throw new Error("kind handler not yet registered: workflow-run");
    default: {
      const exhaustive: never = row.asset.kind;
      throw new Error(
        `mount_path_required: no default mountPath for asset kind ${String(exhaustive)} on row ${row.id}`,
      );
    }
  }
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
 * Wire the workflow-deploy orchestrator's `sendMultiStepDeploy`
 * dependency against `SidecarRouter.sendAgentDeploy`. The router
 * accepts an optional `workflow` projection on the deploy frame; the
 * sidecar's deploy router uses field presence to discriminate the
 * multi-step branch from the trivial branch. The supervisor public key
 * returned by the sidecar's `agent.deploy.ack` is threaded back as the
 * `MultiStepDeployResult.publicKey`.
 *
 * Exported so the co-located caller-site test can assert that the
 * closure constructed in `launchSession` reaches the wire surface via
 * `sendAgentDeploy` with a `workflow` field structurally matching the
 * `AgentDeployFrame.workflow` schema.
 */
export async function sendMultiStepDeployFrame(args: {
  sidecarRouter: SidecarRouter;
  agentAddress: string;
  config: HarnessConfig;
  definition: WorkflowDefinition;
  sources: Record<string, InferenceSource[]>;
}): Promise<{ publicKey: string }> {
  // The wire validator's projection types `stepOrder` and `triggers`
  // as mutable arrays while `WorkflowDefinition` declares them as
  // `readonly`. The wire serializer never mutates the arrays; the
  // shallow copies pay the readonly-widen at the boundary. Every
  // field listed here must match the structural envelope the
  // workflow-process child re-validates against on materialization
  // (`workflowDefinitionEnvelopeSchema`): `id`, `triggers`, `steps`,
  // `stepOrder`, optional `state`. The sidecar deploy router
  // serializes this object verbatim into `workflow.json`; a missing
  // envelope-required field here would round-trip into the child's
  // envelope rejection on disk.
  const wireDefinition = {
    id: args.definition.id,
    triggers: [...args.definition.triggers],
    stepOrder: [...args.definition.stepOrder],
    steps: args.definition.steps as Record<string, unknown>,
    ...(args.definition.state !== undefined
      ? { state: args.definition.state }
      : {}),
  };
  return args.sidecarRouter.sendAgentDeploy(args.agentAddress, args.config, {
    definition: wireDefinition,
    sources: args.sources,
  });
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

export function createSessionService(deps: SessionServiceDeps): SessionService {
  const {
    sidecarRouter,
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

  /**
   * Stage a deploy on the sidecar: resolve assets and tool packages, write
   * the deploy tree, provision the agent, and deliver the deploy + asset
   * packs (Phases 0-2b). Does NOT start the warm harness -- callers that
   * want one (the legacy agent-deploy path) invoke `startWarmSession`
   * afterward. Phase 1's provision has three shapes:
   *   - `workflowFrame` set: the single-step head hand-off fires the
   *     deployment `agent.deploy` frame that spawns the workflow-process
   *     child. Returns the supervisor public key.
   *   - `stageOnly` set: a multi-step per-step stage binds a transient route
   *     for the step address, fires a no-spawn provision frame (init repo +
   *     record hub key), and unbinds the route once the packs land. No warm
   *     harness, no child.
   *   - neither: the legacy plain provision frame (warm harness).
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
     * ack; the caller skips `startWarmSession` for a workflow deploy.
     *
     * Mutually exclusive with `stageOnly`.
     */
    workflowFrame?: {
      definition: WorkflowDefinition;
      sources: Record<string, InferenceSource[]>;
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

    // Phase 0: Resolve attached assets first so the skill index is in
    // hand before the deploy tree is written. The `<available_skills>`
    // stanza describing every attached skill must land in
    // `deploy/prompt.md`, so it has to be composed before
    // `writeDeployTree` produces the on-disk tree.
    let attachments: ResolvedAttachment[] = [];
    let availableSkills: AvailableSkillEntry[] = [];
    if (assetService !== undefined) {
      try {
        attachments = await resolveAttachments(assetService, agentId);
        availableSkills = collectAvailableSkills(attachments);
      } catch (err) {
        throw new SessionLaunchError("write", err, false);
      }
    }

    const stanza = buildAvailableSkillsStanza(availableSkills);
    let effectiveDeployContent: DeployContent =
      stanza.length === 0
        ? deployContent
        : {
            ...deployContent,
            systemPrompt: `${deployContent.systemPrompt}\n\n${stanza}\n`,
          };

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
        // Refuse to mix a direct package-registry attachment with a
        // resolver-driven pin against the same asset id. The resolver
        // path emits an `assetMounts` entry pointing at the asset's
        // DEFAULT_ASSET_REF tip, but a direct attachment may carry any
        // ref the operator chose at attach time. The downstream dedup
        // in `dedupAttachmentsByAssetId` lets the direct attachment win
        // — its bytes would materialize at the operator's chosen ref
        // while `assetMounts` still names the resolver's ref, leaving
        // the loader to resolve manifest entries against tarballs that
        // do not exist at the materialized mount. Surface the conflict
        // at launch as a manifest-shaped violation rather than letting
        // the integrity mismatch surface deep inside the sidecar apply.
        const directPackageRegistryAttachments = attachments.filter(
          (att) => att.assetKind === "package-registry",
        );
        for (const assetId of collectDistinctAssetIds(manifest)) {
          const conflict = directPackageRegistryAttachments.find(
            (att) => att.repoId.id === assetId,
          );
          if (conflict !== undefined) {
            throw new ManifestInvalidError(
              `package-registry asset ${conflict.assetKind}/${conflict.assetName} (${assetId}) is both directly attached to the agent and selected by the tool-package resolver; attach OR pin via tenancy, not both`,
            );
          }
        }
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
            await resolveDirectAssetAttachment({
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
        sidecarRouter.bindStepRoute(agentAddress);
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
      // repo and records the hub key, but spawns nothing. The plain-provision
      // frame stays for the legacy agent-deploy passthrough. Firing the frame
      // before the Phase 2 pack is the ordering barrier -- the repo must
      // exist before the pack applies. A workflow frame's ack surfaces the
      // supervisor public key to the caller.
      let deployAckPublicKey: string | undefined;
      try {
        if (workflowFrame !== undefined) {
          const ack = await sendMultiStepDeployFrame({
            sidecarRouter,
            agentAddress,
            config,
            definition: workflowFrame.definition,
            sources: workflowFrame.sources,
          });
          deployAckPublicKey = ack.publicKey;
        } else if (stageOnly) {
          await sidecarRouter.sendProvisionStep(agentAddress, config);
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
        await sidecarRouter.sendPack(agentAddress, pack, ref, commitSha);
      } catch (err) {
        if (!stageOnly) await attemptCleanup(agentAddress, "pack", err);
        throw new SessionLaunchError("pack", err, false);
      }

      // Phase 2b: Asset-pack fan-out. For each attached asset, build a
      // pack, insert the manifest row, then send the pack. The manifest
      // insert MUST happen before the pack send: if the sidecar acks
      // but the row is missing, the session has materialization without
      // a recorded manifest. If the row insert fails, the pack send
      // must not happen.
      //
      // The fan-out covers two sources: the agent's direct attachments
      // (skills, today) and the package-registry assets the tool-package
      // resolver picked from. The latter live behind tenant inheritance
      // rather than a per-agent attachment row, so the session service
      // synthesizes the attachment view in `manifestAssetAttachments`.
      //
      // Both sources can name the same `package-registry` asset — a
      // direct attachment and a resolver pin would each compute
      // `mountPath = "package-registries/<asset.name>/"` and collide on
      // the `(instanceId, mountPath)` PK in `session_asset`. Dedup by
      // asset id BEFORE the inserts and let the direct attachment win:
      // it is an explicit operator action and carries an `agentAssetId`
      // the audit query joins against. The resolver-derived row would
      // produce the same materialized contents, so dropping it is
      // semantically lossless.
      const fanOut: ResolvedAttachment[] = dedupAttachmentsByAssetId({
        direct: attachments,
        resolved: manifestAssetAttachments,
      });
      if (assetService !== undefined && fanOut.length > 0) {
        // Track every successfully committed attachment so a later
        // fan-out failure can roll back the earlier rows in lockstep
        // with the sidecar undeploy. Without this, fan-out[0] succeeds,
        // fan-out[1] fails, attemptCleanup tears down the sidecar — but
        // fan-out[0]'s session_asset row survives and a future
        // materialization query reads a manifest the sidecar no longer
        // honors.
        const committed: ResolvedAttachment[] = [];
        for (const att of fanOut) {
          try {
            await sendAttachmentPack(instanceId, agentAddress, att);
            committed.push(att);
          } catch (err) {
            await rollbackCommittedAttachments(instanceId, committed);
            if (!stageOnly) await attemptCleanup(agentAddress, "pack", err);
            throw new SessionLaunchError("pack", err, false);
          }
        }
      }

      return deployAckPublicKey === undefined
        ? undefined
        : { publicKey: deployAckPublicKey };
    } finally {
      if (stageOnly) {
        sidecarRouter.unbindStepRoute(agentAddress);
      }
    }
  }

  /**
   * Deploy a one-step workflow once at the head. Reuses the full
   * launch-phase machinery (deploy-tree write, pack, asset fan-out) via
   * `executeLaunchPhases`, swapping the Phase 1 provision frame for the
   * workflow frame; it never calls `startWarmSession`, so no warm harness
   * starts. The workflow frame makes the sidecar initialize the head repo
   * and spawn the workflow-process child; the follow-up pack lands the
   * head's deploy
   * tree. Returns the supervisor's principal public key from the frame's
   * ack. A workflow-frame launch always yields a deploy-ack key; its
   * absence is a wiring bug, not a tolerable case.
   */
  const deploySingleStepAtHead: DeploySingleStepFn = async (deployParams) => {
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
      },
      ...(deployParams.toolPackagePins !== undefined
        ? { toolPackagePins: deployParams.toolPackagePins }
        : {}),
    });
    if (result === undefined) {
      throw new Error(
        "single-step deploy at head: executeLaunchPhases returned no deploy-ack public key for a workflow-frame deploy",
      );
    }
    return result;
  };

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
      });

    const sendMultiStepDeployCallback: SendMultiStepDeployFn = (deployParams) =>
      sendMultiStepDeployFrame({
        sidecarRouter,
        agentAddress: deployParams.agentAddress,
        config: deployParams.config,
        definition: deployParams.definition,
        sources: deployParams.sources,
      });

    const orchestrator = createWorkflowDeployOrchestrator({
      directorRegistry: args.directorRegistry,
      workflowRepo: args.workflowRepo,
      launchSession: launchSessionCallback,
      sendMultiStepDeploy: sendMultiStepDeployCallback,
      deploySingleStepAtHead,
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
    });
  }

  /**
   * Deploy a single-agent instance through the single-step-at-head path: wrap
   * the harness as a one-step workflow (the same wrap `launchSession` uses) and
   * route it through `deploySingleStepAtHead` with the instance's REAL identity
   * -- so the head address IS the instance address and the deploy runs as a
   * supervised workflow-process child, not the legacy trivial in-process path.
   *
   * Unlike the orchestrator's `runSingleStepAtHead`, this calls
   * `deploySingleStepAtHead` directly with the route's real `agentId`
   * (`row.id`), NOT a `deriveDeploymentAgentId(deploymentId)` -- the child
   * resolves skills, deploy tree, and tool-package pins by `agentId`, so
   * collapsing it to the deployment id would strip the instance's attachments.
   * It writes no `workflow_deployment` row (a plain instance has no workflow
   * asset). Returns the head's agent-key ack.
   */
  async function deployInstanceAtHead(params: {
    agentAddress: string;
    agentId: string;
    instanceId: string;
    config: HarnessConfig;
    deployContent: DeployContent;
    toolPackagePins?: readonly ToolPackagePin[];
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
    // runtime, matching the legacy in-process harness. The route already
    // resolved and authorized `config.sources` against the tenant catalog, so
    // it is pinned directly rather than re-run through the orchestrator's
    // operator-approval gate.
    //
    // Fail loud on the invariant the reactor depends on: the reactor resolves
    // its initial source by id (`defaultSource`) and fails over FORWARD-ONLY
    // with no wrap, so the default must be element 0 or part of the chain is
    // unreachable -- and if the default were last, failover would silently
    // no-op. The route guarantees `config.sources[0].id === config.defaultSource`
    // (head = active); assert it here so a future reordering fails loudly
    // rather than silently disabling failover.
    if (config.sources.length === 0) {
      throw new Error(
        `instance deploy for ${agentAddress}: config.sources is empty; at least the default source is required`,
      );
    }
    if (config.sources[0]?.id !== config.defaultSource) {
      throw new Error(
        `instance deploy for ${agentAddress}: config.sources[0] (${JSON.stringify(config.sources[0]?.id)}) must be the default source ${JSON.stringify(config.defaultSource)}; the reactor fails over forward from the default and would otherwise skip the head`,
      );
    }

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
    });
  }

  async function deployWorkflowDefinition(
    params: DeployWorkflowDefinitionParams,
  ): Promise<DeployWorkflowDefinitionResult> {
    const {
      tenantId,
      deploymentId,
      deploymentDomain,
      definition,
      definitionAssetId,
      config,
      deployContent,
    } = params;

    // The deploy is initiated by an authorized tenant operator against a
    // workflow asset they authored; approve exactly the grant surface the
    // definition declares. The same director registry feeds both this
    // approval-set derivation and the orchestrator's gate so the walk the
    // route approves and the walk the orchestrator enforces are identical.
    const directorRegistry = createDefaultDirectorRegistry();
    const walk = walkCapabilities(definition, directorRegistry);
    const operatorApprovals: ApprovalSet = new Set<string>(
      [...walk.perStep.values()].flatMap((declarations) => [
        ...declarations.grants,
      ]),
    );

    const result = await runWorkflowDeploy({
      workflowRepo: createHubWorkflowRepoWriter(agentRepoStore),
      directorRegistry,
      deployArgs: {
        workflow: definition,
        deploymentId,
        deploymentDomain,
        config,
        deployContent,
        operatorApprovals,
        hubPublicKey: hexEncode(agentRepoStore.getSigningPublicKey()),
        ...(params.toolPackagePins !== undefined
          ? { toolPackagePins: params.toolPackagePins }
          : {}),
      },
    });

    if (db === undefined) {
      throw new Error(
        "deployWorkflowDefinition requires a db handle to record the workflow_deployment projection row",
      );
    }
    const now = new Date();
    await db.transaction(async (tx) => {
      await tx.insert(workflowDeploymentTable).values({
        id: deploymentId,
        tenantId,
        definitionAssetId,
        status: "deployed",
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

    return {
      deploymentId,
      deploymentAddress: deriveDeploymentAddress({
        deploymentId,
        deploymentDomain,
      }),
      publicKey: result.publicKey,
    };
  }

  async function rollbackCommittedAttachments(
    instanceId: string,
    committed: readonly ResolvedAttachment[],
  ): Promise<void> {
    if (db === undefined) return;
    if (committed.length === 0) return;
    // Per-row try/catch so a single rollback failure does not stop the
    // sweep — every committed row needs to come off the books before
    // the caller emits the original sendPack error.
    for (const att of committed) {
      try {
        await db
          .delete(sessionAssetTable)
          .where(
            and(
              eq(sessionAssetTable.instanceId, instanceId),
              eq(sessionAssetTable.mountPath, att.mountPath),
            ),
          );
      } catch (err) {
        logger.warn`session_asset rollback failed for earlier-committed instance=${instanceId} mountPath=${att.mountPath}: ${err instanceof Error ? err.message : String(err)}`;
      }
    }
  }

  async function sendAttachmentPack(
    instanceId: string,
    agentAddress: string,
    attachment: ResolvedAttachment,
  ): Promise<void> {
    if (db === undefined) {
      // Guarded at construction; reassert defensively so the
      // narrowing is visible to readers and a future refactor cannot
      // accidentally invoke this without a db.
      throw new Error("sendAttachmentPack invoked without a db handle");
    }

    const {
      agentAssetId,
      source,
      mountPath,
      sourceCommitSha,
      repoId,
      pack,
      ref,
    } = attachment;

    const assetPackSha = await createPackSha(pack);

    // Insert manifest row before the pack send so we never end up in
    // the materialized-without-manifest state. Both direct and
    // resolver-derived materializations write a row; the `source`
    // column records which path produced it, and `agentAssetId` is
    // null for resolver-derived rows.
    await db.insert(sessionAssetTable).values({
      instanceId,
      agentAssetId,
      mountPath,
      assetPackSha,
      sourceCommitSha,
      source,
      materializedAt: new Date(),
    });

    try {
      await sidecarRouter.sendPack(agentAddress, pack, ref, sourceCommitSha, {
        mountPath,
        repoId,
      });
    } catch (err) {
      // Roll back the manifest row when the send fails so the manifest
      // and the materialized state on the sidecar can never disagree.
      // The forensic value of a manifest-without-materialization row is
      // negligible because no agent will read against it. Wrap the
      // rollback in its own try/catch so a rollback failure (DB gone,
      // connection killed mid-launch) is logged rather than masking the
      // primary sendPack error — the caller needs to see the original
      // failure, not the secondary one.
      try {
        await db
          .delete(sessionAssetTable)
          .where(
            and(
              eq(sessionAssetTable.instanceId, instanceId),
              eq(sessionAssetTable.mountPath, mountPath),
            ),
          );
      } catch (rollbackErr) {
        const msg =
          rollbackErr instanceof Error
            ? rollbackErr.message
            : String(rollbackErr);
        logger.warn`session_asset rollback failed for instance=${instanceId} mountPath=${mountPath}: ${msg}`;
      }
      throw err;
    }
  }

  async function resolveAttachments(
    service: AssetService,
    agentId: string,
  ): Promise<ResolvedAttachment[]> {
    const rows = await service.listAgentAssets(agentId);
    const resolved: ResolvedAttachment[] = [];
    for (const row of rows) {
      resolved.push(await resolveAttachment(row));
    }
    return resolved;
  }

  async function resolveAttachment(
    row: AgentAssetWithAsset,
  ): Promise<ResolvedAttachment> {
    const mountPath = resolveMountPath(row);
    const repoId: RepoId = { kind: row.asset.kind, id: row.asset.id };

    const sourceCommitSha = await agentRepoStore.repoStore.resolveRef(
      HUB_PRINCIPAL,
      repoId,
      row.ref,
    );
    if (sourceCommitSha === null) {
      throw new Error(
        `attachment_ref_unresolved: ${row.asset.kind}/${row.asset.id} has no commit on ${row.ref}`,
      );
    }

    const { pack, ref: returnedRef } =
      await agentRepoStore.repoStore.createPack(HUB_PRINCIPAL, repoId, row.ref);

    return {
      agentAssetId: row.id,
      source: "direct",
      assetName: row.asset.name,
      assetKind: row.asset.kind,
      mountPath,
      sourceCommitSha,
      repoId,
      pack,
      ref: returnedRef,
    };
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
   * Build a `ResolvedAttachment` for an asset the resolver picked
   * from but which has no per-agent attachment row. The pack is read
   * from the asset's main ref (the same ref the resolver consumed
   * tarballs from), and `agentAssetId` is `null` so the fan-out path
   * knows to skip the `session_asset` insert.
   */
  async function resolveDirectAssetAttachment(args: {
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
      agentAssetId: null,
      source: "resolved",
      assetName: args.asset.name,
      assetKind: args.asset.kind,
      mountPath: args.mountPath,
      sourceCommitSha,
      repoId,
      pack,
      ref: returnedRef,
    };
  }

  function collectAvailableSkills(
    resolved: ResolvedAttachment[],
  ): AvailableSkillEntry[] {
    const entries: AvailableSkillEntry[] = [];
    for (const att of resolved) {
      if (att.assetKind !== "skill") continue;
      const index = getSkillIndex(att.repoId.id, att.ref);
      for (const entry of index) {
        entries.push({
          qualifiedName: `${att.assetName}/${entry.name}`,
          description: entry.description,
          workspacePath: `workspace/${att.mountPath}${entry.workspaceSubpath}`,
        });
      }
    }
    return entries;
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

    const delivered = sidecarRouter.routeMail(agentAddress, base64);
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
    sendUserMessage,
    endSession,
  };
}

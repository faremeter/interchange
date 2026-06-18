import { createHash } from "node:crypto";

import { type } from "arktype";
import { and, eq } from "drizzle-orm";

import {
  createDefaultDirectorRegistry,
  defaultDirectorFactory,
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
  wrapHarnessAsTrivialAgent,
  type ApprovalSet,
  type DeployContent as OrchestratorDeployContent,
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
   * Orchestrate the full deploy lifecycle:
   *   1. Write deploy tree to hub repo and produce packfile
   *   2. Provision agent on sidecar (sendAgentDeploy)
   *   3. Deliver deploy packfile (sendPack)
   *   4. Fan-out attached asset packs: for each row returned by
   *      `assetService.listAgentAssets(agentId)`, resolve the
   *      mountPath, resolve the ref to a source commit SHA, build a
   *      pack, insert a `session_asset` row, and send the pack to the
   *      sidecar with `mountPath` set on the `repo.pack.done` frame.
   *      The manifest insert MUST precede the pack send.
   *   5. Start session (sendSessionStart)
   *
   * On partial failure after provision, attempts cleanup via
   * sendAgentUndeploy before re-throwing.
   */
  launchSession(params: {
    agentAddress: string;
    agentId: string;
    instanceId: string;
    config: HarnessConfig;
    deployContent: DeployContent;
    /**
     * The agent definition's pinned tool packages. When non-empty, the
     * service uses its injected `toolPackageResolver` to compute the
     * full pinned closure and ships the resulting manifest as part of
     * the deploy tree. An empty array (or absent value) means no
     * tool-package manifest is materialized; the sidecar's loader is
     * a no-op for this deploy.
     */
    toolPackagePins?: readonly ToolPackagePin[];
  }): Promise<void>;

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
   * Optional asset attachment integration. When set, `launchSession`
   * fans out per-attachment packs after the deploy pack lands and
   * inserts a `session_asset` row per attachment. When unset, only
   * the deploy pack is sent — the legacy single-pack path is
   * preserved bit-for-bit.
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

function createPackSha(pack: Uint8Array): string {
  return createHash("sha256").update(pack).digest("hex");
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
 * Compute the operator-approval set the workflow-deploy orchestrator
 * needs for a trivial-wrap deploy. The wrapped agent carries no
 * capability list and no director ref (see `wrapHarnessAsTrivialAgent`),
 * but `HarnessConfig.tools` projects onto the synthesized agent's
 * `toolFactories` so the walk emits `tool:<name>` grants the gate
 * checks. The approval set mirrors that projection: every tool the
 * `HarnessConfig` already names gets the matching `tool:` approval,
 * the per-source inference grants land alongside the default-director
 * grant, and the trigger-derived mail grants close the set out.
 *
 * The legacy agent-deploy path has already authorized the deployment
 * in toto -- the harness's `tools` array is the operator-supplied
 * surface the hub ships to the sidecar -- and re-running deploy
 * through the workflow surface must not synthesize a fresh approval
 * prompt for grants the legacy path implicitly approved. The shape
 * here keeps the gate honest (an unapproved tool fails the deploy)
 * while the legacy passthrough remains bit-for-bit on the wire.
 */
function buildTrivialApprovalSet(args: {
  agentAddress: string;
  config: HarnessConfig;
}): ApprovalSet {
  const approvals = new Set<string>();
  for (const tool of args.config.tools) {
    approvals.add(`tool:${tool.name}`);
  }
  for (const source of args.config.sources) {
    approvals.add(`inference.source:${source.provider}:${source.model}`);
  }
  approvals.add(`director:${defaultDirectorFactory.id}`);
  approvals.add(`mail.address:${args.agentAddress}`);
  const at = args.agentAddress.lastIndexOf("@");
  if (at >= 0 && at < args.agentAddress.length - 1) {
    approvals.add(`mail.send:${args.agentAddress.slice(at + 1)}`);
  }
  return approvals;
}

/**
 * Translate the orchestrator's structural `DeployContent` (which types
 * `toolPackageManifest` as `unknown`) back into the hub-sessions
 * `DeployContent` shape. The orchestrator round-trips whatever the
 * caller supplied, but the surface type widens `toolPackageManifest` to
 * `unknown`; the validator narrows it back to the canonical shape
 * `agentRepoStore.writeDeployTree` consumes.
 */
function bridgeOrchestratorDeployContent(
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
  sources: Record<string, InferenceSource>;
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
 * No-op `WorkflowRepoWriter` used by the legacy `launchSession`
 * delegate. The workflow-deploy orchestrator writes a workflow repo
 * tree before per-step launch; the legacy agent-deploy path never
 * materialized a workflow repo and the integration-test surface does
 * not exercise one, so the trivial wrap skips the write rather than
 * inventing a phantom repo.
 */
function createNoopWorkflowRepoWriter(): WorkflowRepoWriter {
  return {
    async writeWorkflowRepo(_args) {
      return;
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
   * Drive the per-agent deploy + session-start phases. Factored out of
   * `launchSession` so the workflow-deploy orchestrator's trivial branch
   * can call back into the exact phases the legacy agent-deploy path
   * owns. The body here is the legacy `launchSession` body verbatim;
   * `launchSession` itself now wraps the call in a single-step workflow
   * and routes through the orchestrator.
   */
  async function executeLaunchPhases(params: {
    agentAddress: string;
    agentId: string;
    instanceId: string;
    config: HarnessConfig;
    deployContent: DeployContent;
    toolPackagePins?: readonly ToolPackagePin[];
  }): Promise<void> {
    const { agentAddress, agentId, instanceId, config, deployContent } = params;
    const toolPackagePins = params.toolPackagePins ?? [];

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

    // Phase 1: Provision on sidecar.
    try {
      await sidecarRouter.sendAgentDeploy(agentAddress, config);
    } catch (err) {
      throw new SessionLaunchError("provision", err, false);
    }

    // Phases 2-3: Pack delivery and session start. If either fails,
    // attempt cleanup so the sidecar doesn't retain a zombie agent.
    try {
      await sidecarRouter.sendPack(agentAddress, pack, ref, commitSha);
    } catch (err) {
      await attemptCleanup(agentAddress, "pack", err);
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
          await attemptCleanup(agentAddress, "pack", err);
          throw new SessionLaunchError("pack", err, false);
        }
      }
    }

    try {
      await sidecarRouter.sendSessionStart(agentAddress);
    } catch (err) {
      await attemptCleanup(agentAddress, "start", err);
      throw new SessionLaunchError("start", err, false);
    }
  }

  /**
   * Legacy agent-deploy entry point preserved bit-for-bit at its wire
   * shape. The body now constructs a single-step trivial workflow from
   * the deploy's `HarnessConfig` + `DeployContent`, synthesizes the
   * matching operator-approval set, and delegates to the workflow-deploy
   * orchestrator. The orchestrator's trivial branch round-trips back
   * into `executeLaunchPhases` with the original `trivialBindings`,
   * which preserves every on-disk and wire-level surface the legacy
   * agent-deploy path exposed.
   */
  async function launchSession(params: {
    agentAddress: string;
    agentId: string;
    instanceId: string;
    config: HarnessConfig;
    deployContent: DeployContent;
    toolPackagePins?: readonly ToolPackagePin[];
  }): Promise<void> {
    const { agentAddress, agentId, instanceId, config, deployContent } = params;

    const trivialAgent = wrapHarnessAsTrivialAgent({
      config,
      deployContent,
    });
    const workflow = defineWorkflow({
      id: `wf_${agentId}`,
      agent: trivialAgent,
      trigger: { type: "mail", to: agentAddress },
    });
    const operatorApprovals = buildTrivialApprovalSet({
      agentAddress,
      config,
    });

    const launchSessionCallback: LaunchSessionFn = async (orchestratorParams) =>
      executeLaunchPhases({
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

    const sendMultiStepDeployCallback: SendMultiStepDeployFn = (params) =>
      sendMultiStepDeployFrame({
        sidecarRouter,
        agentAddress: params.agentAddress,
        config: params.config,
        definition: params.definition,
        sources: params.sources,
      });

    const orchestrator = createWorkflowDeployOrchestrator({
      directorRegistry: createDefaultDirectorRegistry(),
      workflowRepo: createNoopWorkflowRepoWriter(),
      launchSession: launchSessionCallback,
      sendMultiStepDeploy: sendMultiStepDeployCallback,
    });

    await orchestrator.deployWorkflow({
      workflow,
      trivialBindings: { agentAddress, agentId, instanceId },
      config,
      deployContent,
      ...(params.toolPackagePins !== undefined
        ? { toolPackagePins: params.toolPackagePins }
        : {}),
      operatorApprovals,
    });
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

    const assetPackSha = createPackSha(pack);

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
    const base64 = Buffer.from(rawMessage).toString("base64");

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

  return { launchSession, sendUserMessage, endSession };
}

import { type } from "arktype";
import { and, eq, isNull } from "drizzle-orm";

import { getLogger } from "@intx/log";
import {
  assembleMessage,
  assembleSignedContent,
  createDetachedSignatureFromProvider,
  type MessageHeaders,
} from "@intx/mime";
import {
  buildCredentialDelivery,
  listAssetsForTenant,
  resolveInferenceMaterials,
  type DB,
} from "@intx/db";
import {
  grant as grantTable,
  sidecarAllocation as sidecarAllocationTable,
  workflowDefinition as workflowDefinitionTable,
  workflowRun as workflowRunTable,
  type WorkflowRunCredentialRefs,
} from "@intx/db/schema";
import { base64Encode, hexEncode } from "@intx/types";
import type {
  CredentialDelivery,
  CredentialMaterialEntry,
} from "@intx/types/sidecar";
import type { CredentialCipher } from "@intx/types";
import { generateId } from "@intx/hub-common";
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
import type {
  SourceRefPin,
  WorkflowProjectionWithSources,
  WorkflowSourceAssetMount,
} from "@intx/types/sidecar";
import type {
  WorkflowDefinitionAssetSource,
  WorkflowDefinitionRegistrySource,
  WorkflowDefinitionSource,
} from "@intx/types/workflow-sources";
import {
  buildInertProjectionStepSources,
  deriveRunAddress,
  enumerateInertBodies,
  pickStepInferenceSource,
  pinInertStepSources,
  WorkflowDefinitionInvalidError,
  type DeployContent as OrchestratorDeployContent,
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
import { isDeployFrameFailure } from "./ws/sidecar-handler";
import type { Principal, RepoId, RepoKind } from "./repo-store";
import {
  buildSourceAssetMounts,
  type ResolveAssetAttachmentFn,
} from "./workflow-closure-resolution";
import { restoreWorkflowRunToAllocation } from "./workflow-run-restore";
import { committedReadsToSourceTree } from "./committed-source-tree";
import {
  installAndApproveWorkflowDefinition,
  type InstallAndApproveArgs,
  type InstallAndApproveResult,
} from "./workflow-probe-gate";

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
    runId: string;
    config: HarnessConfig;
    deployContent: DeployContent;
    toolPackagePins?: readonly ToolPackagePin[];
    allocationTarget?: AllocatedSidecarTarget;
  }): Promise<void>;

  /**
   * Deploy a CODE-SOURCED workflow definition end to end: install + probe +
   * gate + freeze (`approve-probed`), then deploy the frozen definition by
   * source-ref. This is the general workflow deploy entry point the
   * `POST /deployments` route drives; it never hydrates a live definition from a
   * static `workflow.json`.
   *
   * The service owns the source-read wiring (`repoStore` committed reads and
   * asset pack fan-out) and the registry configuration, so the caller passes
   * only the deploy intent: where the definition's bytes come from
   * (`source`/`entry`/`pin`), the `workflow`-kind asset the definition projects
   * over (`definitionAssetId`), and the shared harness config. The method
   * dispatches on `source.kind`/`source.package.format` to build the install
   * args, pins every top-level step's inference source under the frozen
   * approval, and persists the deployment's anchor run.
   *
   * Persists the deployment's anchor `workflow_run` (id = `anchorRunId`) via
   * `deployCodeSourcedWorkflow`, so the deployment is listable per tenant.
   * Returns the supervisor's principal public key from the sidecar deploy ack.
   */
  deployWorkflowFromSource(
    params: DeployWorkflowFromSourceParams,
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

export type DeployWorkflowDefinitionResult = {
  /** Echoes the deployment id recorded on the projection row. */
  anchorRunId: string;
  /** Deployment-level mail address the supervisor registers on the bus. */
  deploymentAddress: string;
  /** Supervisor principal public key from the sidecar's deploy ack. */
  publicKey: string;
};

export type DeployWorkflowFromSourceParams = {
  /** Owning tenant; recorded on the deployment's anchor run. */
  tenantId: string;
  /**
   * Stable deployment identifier and anchor-run id. The deployment-level
   * address derives from it; the caller owns its generation.
   */
  anchorRunId: string;
  /** Mail domain the deployment's derived addresses live under. */
  deploymentDomain: string;
  /**
   * The deployment-level mail address, derived by the caller from `anchorRunId`
   * + `deploymentDomain`. Re-derived and asserted coherent inside
   * `deployCodeSourcedWorkflow`.
   */
  agentAddress: string;
  /** Where the definition's bytes come from at apply time. */
  source: WorkflowDefinitionSource;
  /** The `interchange.workflow` entry-module path the sidecar evaluates. */
  entry: string;
  /**
   * A `name@range` spec for the definition package. REQUIRED for the `registry`
   * and asset-`tarball` variants (the pin selects the member); omitted for the
   * asset-`source` variant, whose member is selected by `package.packageName`.
   */
  pin?: string;
  /**
   * The `workflow`-kind asset the frozen definition projects a
   * `workflow_definition` over. Distinct from a `source.kind === "asset"`
   * source's `assetId`, which names where the bytes live.
   */
  definitionAssetId: string;
  /**
   * Harness config shared across the deployment. Its `sources`/`defaultSource`
   * are the operator-supplied inference chain; the method pins each top-level
   * step to one approved source from it.
   */
  config: HarnessConfig;
  /** Cipher for the definition's tenant-owned credential bindings, if any. */
  credentialCipher?: CredentialCipher;
};

/**
 * Install/probe/gate/freeze inputs for a code-sourced workflow, DECOUPLED from
 * deploy. The provisioned prepare path calls this before allocating capacity,
 * persists the frozen bundle, and deploys it to the allocation later with no
 * re-probe.
 */
export type InstallAndApproveWorkflowSourceParams = {
  /** Where the definition's bytes come from at probe time. */
  source: WorkflowDefinitionSource;
  /** The `interchange.workflow` entry-module path the sidecar evaluates. */
  entry: string;
  /**
   * A `name@range` spec for the definition package. REQUIRED for the `registry`
   * and asset-`tarball` variants; omitted for the asset-`source` variant.
   */
  pin?: string;
  /** The `workflow`-kind asset the frozen definition projects a definition over. */
  definitionAssetId: string;
};

/**
 * Inputs to deploy a previously-frozen code-sourced approval bundle to a
 * dedicated allocation. Mirrors `DeployPreparedWorkflowDefinitionParams` for the
 * source-ref lineage: the anchor `workflow_run` row already exists from prepare
 * time, so the deploy UPDATES it under the allocation-ownership lock rather than
 * inserting a fresh one.
 */
export type DeployPreparedCodeSourcedWorkflowParams = {
  /** Owning tenant; the definition's own tenant for credential resolution. */
  tenantId: string;
  /** The pre-inserted anchor run id, fixed at prepare time. */
  anchorRunId: string;
  /** Mail domain the deployment's derived addresses live under. */
  deploymentDomain: string;
  /** The deployment-level mail address; re-derived and asserted coherent. */
  agentAddress: string;
  /** Where the definition's bytes come from, rehydrated from the frozen bundle. */
  source: WorkflowDefinitionSource;
  /** The frozen approval bundle rehydrated from the launch spec. */
  approved: InstallAndApproveResult;
  /** Harness config carrying the re-resolved per-step inference chain. */
  config: HarnessConfig;
  /** The exact allocation generation to deploy onto. */
  allocationTarget: AllocatedSidecarTarget;
  /** Cipher for the definition's tenant-owned credential bindings, if any. */
  credentialCipher?: CredentialCipher;
};

export type PreparedWorkflowDeployer = {
  /**
   * Install + probe + gate + freeze a code-sourced definition, returning the
   * frozen bundle WITHOUT deploying it. The provisioned
   * prepare path persists the bundle and deploys it later via
   * `deployPreparedCodeSourcedWorkflow`.
   */
  installAndApproveWorkflowSource(
    params: InstallAndApproveWorkflowSourceParams,
  ): Promise<InstallAndApproveResult>;
  /**
   * Deploy a previously-frozen code-sourced approval bundle to a dedicated
   * allocation, updating the pre-existing anchor run under the
   * allocation-ownership lock. No re-probe: the frozen projection/hash/closure
   * ride verbatim.
   */
  deployPreparedCodeSourcedWorkflow(
    params: DeployPreparedCodeSourcedWorkflowParams,
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
  /** Present when this Hub can route deploy phases to provisioned allocations. */
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
  runId: string;
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

/** Fields the deploy frame carries onto `sendAgentDeploy`. */
type DeployFrameCommonArgs = {
  sidecarRouter: SidecarRouter;
  sidecarAllocationRouter?: SidecarAllocationRouter;
  allocationTarget?: AllocatedSidecarTarget;
  agentAddress: string;
  config: HarnessConfig;
  sources: Record<string, InferenceSource[]>;
};

/**
 * For a code-sourced (npm) deploy the hub never holds the live
 * `WorkflowDefinition` -- it lives only in the airlocked child. The gate/freeze
 * layer hashed the inert projection; the deploy frame carries that hash and the
 * source-ref pin, and the sidecar re-materializes and evaluates the pinned code
 * from the pin, so no inline definition rides the frame. The content hash is
 * owned by the gate, so this frame never recomputes it -- recomputing over a
 * live wire lineage would diverge from the inert projection the child
 * re-verifies against.
 */
export type SourceRefDeployFrameArgs = DeployFrameCommonArgs & {
  lineage: "source-ref";
  /**
   * The gate-frozen wire hash of the approved projection -- stamped onto the
   * frame VERBATIM. This arm does not recompute it: the freeze layer owns the
   * content hash, and the child re-verifies its closure evaluation against this
   * exact value.
   */
  approvedWireHash: string;
  /**
   * The source-ref pin: where the definition's bytes come from plus the frozen
   * dependency closure the hub resolved for it. The two co-travel, so they are
   * one required object on this arm (see `SourceRefPin`) -- the sidecar
   * re-materializes the exact tree from the pin at apply time.
   */
  sourceRef: SourceRefPin;
  /**
   * Resolved credential material for the definition's credential bindings,
   * delivered to the child on the frame. The hub resolves + decrypts here; the
   * source-ref child decrypts nothing. The grant that AUTHORIZES a credential's
   * use is minted per-run by run-grant materialization, not carried on this
   * frame.
   */
  credentials?: CredentialDelivery;
  /**
   * The projection's inline onTrigger section bodies, each already in inert wire
   * form with its per-step inference sources pinned and its own wire hash --
   * built by `deployCodeSourcedWorkflow` from the frozen projection. The sidecar
   * seals each body's sources into the run record and re-verifies against its
   * wire hash. Absent when the projection has no inline onTrigger body.
   */
  referencedDefinitions?: readonly WorkflowProjectionWithSources[];
  /**
   * Source assets the pin's `kind:"asset"` closure entries read from, delivered
   * inline on the frame so the sidecar checks them out into its durable
   * per-deployment source store. Absent for a registry-sourced pin (its tarballs
   * are fetched over HTTP).
   */
  assets?: readonly WorkflowSourceAssetMount[];
};

export type SendMultiStepDeployFrameArgs = SourceRefDeployFrameArgs;

/**
 * Emit the source-ref deploy frame onto `SidecarRouter.sendAgentDeploy`. The
 * router accepts an optional `workflow` projection on the deploy frame; the
 * sidecar's deploy router uses field presence to route the frame to the
 * workflow deploy path, and returns the supervisor public key on the
 * `agent.deploy.ack`.
 *
 * The gate/freeze layer already hashed the inert projection, so the frozen hash
 * and the inert projection ride the frame verbatim -- this never recomputes the
 * content hash. Recomputing over a live wire lineage would diverge from the
 * inert projection the child re-verifies against.
 *
 * Exported so the co-located caller-site test can assert that the constructed
 * closure reaches the wire surface via `sendAgentDeploy` with a `workflow`
 * field structurally matching the `AgentDeployFrame.workflow` schema.
 */
export async function sendMultiStepDeployFrame(
  args: SendMultiStepDeployFrameArgs,
): Promise<{ publicKey: string }> {
  const workflow = {
    // The deploy frame carries no inline definition: the sidecar evaluates the
    // pinned code closure from `sourceRef` and re-verifies it against
    // `approvedWireHash`. Only the gate-frozen hash and the pin ride the frame.
    sources: args.sources,
    approvedWireHash: args.approvedWireHash,
    sourceRef: args.sourceRef,
    ...(args.credentials !== undefined
      ? { credentials: args.credentials }
      : {}),
    ...(args.referencedDefinitions !== undefined &&
    args.referencedDefinitions.length > 0
      ? { referencedDefinitions: [...args.referencedDefinitions] }
      : {}),
    ...(args.assets !== undefined && args.assets.length > 0
      ? { assets: [...args.assets] }
      : {}),
  };
  // A prepared provisioned deploy routes its frame to the allocation; an
  // ordinary deploy sends it on the ordinary router. The frozen projection/hash/pin
  // ride verbatim in both cases -- only the transport differs.
  if (args.allocationTarget !== undefined) {
    if (args.sidecarAllocationRouter === undefined) {
      throw new Error("Provisioned deployment routing is not configured");
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
 * Arguments for `deployCodeSourcedWorkflow`. The `approved` bundle is the
 * `installAndApproveWorkflowDefinition` output verbatim -- the frozen hash,
 * inert projection, and closure travel together inside it so no caller can pair
 * a hash with a mismatched projection or closure. The remaining fields are the
 * operator/asset config the approve step never sees: the per-step inference
 * `sources`, the deploy `config`, the target `agentAddress`, and the `source`
 * ref that names where the definition's bytes are published.
 */
type DeployCodeSourcedCommonArgs = DeployFrameCommonArgs & {
  approved: InstallAndApproveResult;
  /**
   * The hub DB handle, the definition's OWN tenant, the deployment's anchor run
   * id, and the mail domain its run address lives under. REQUIRED: this function
   * writes the deployment's anchor `workflow_run` row, and run-grant
   * materialization keys off it. `tenantId` is the definition's own tenant
   * (tenant-owned credential resolution walks up from it); do not pass a
   * request/config tenant that may differ. `anchorRunId` is caller-supplied: the
   * deployment mail address is frozen into the approved package bytes at
   * authoring time, so the run id it derives from is fixed before this runs and
   * cannot be minted here. `deploymentDomain` pairs with `anchorRunId` to
   * re-derive the run address and assert it matches `agentAddress`, failing
   * closed on an incoherent pair.
   */
  db: DB["db"];
  tenantId: string;
  anchorRunId: string;
  deploymentDomain: string;
  /**
   * Credential cipher, REQUIRED whenever the definition carries credential
   * bindings OR any pinned inference source (top-level or inline body) references
   * a credential; resolution fails closed without it. Omit only for a deploy with
   * neither.
   */
  credentialCipher?: CredentialCipher;
  /**
   * Present only for a prepared provisioned deploy: route the source-ref frame
   * to this allocation instead of the ordinary router. `sidecarAllocationRouter`
   * carries the allocation transport and is REQUIRED whenever `allocationTarget`
   * is set. An ordinary deploy omits both.
   */
  allocationTarget?: AllocatedSidecarTarget;
  sidecarAllocationRouter?: SidecarAllocationRouter;
};

/** Deploy a definition published to an npm registry: the sidecar fetches its
 * tarballs over HTTP, so no source asset is delivered. */
export type DeployCodeSourcedRegistryArgs = DeployCodeSourcedCommonArgs & {
  source: WorkflowDefinitionRegistrySource;
};

/** Deploy a definition sourced from a hub `package-registry` asset: the caller
 * mints `resolveAttachment` so this glue delivers the asset packs the sidecar
 * checks out, without importing the asset service. */
export type DeployCodeSourcedAssetArgs = DeployCodeSourcedCommonArgs & {
  source: WorkflowDefinitionAssetSource;
  resolveAttachment: ResolveAssetAttachmentFn;
};

export type DeployCodeSourcedWorkflowArgs =
  | DeployCodeSourcedRegistryArgs
  | DeployCodeSourcedAssetArgs;

function isAssetDeployArgs(
  args: DeployCodeSourcedWorkflowArgs,
): args is DeployCodeSourcedAssetArgs {
  return args.source.kind === "asset";
}

/**
 * The single public composition entrypoint for a code-sourced (npm) deploy. It
 * consumes the approve output and builds the source-ref deploy frame internally,
 * so the security-load-bearing hand-off -- frozen wire hash, inert projection,
 * frozen closure -- is assembled in one place from one cohesive object rather
 * than reassembled by each caller. The frozen approval's hash and projection
 * ride the frame verbatim: nothing here recomputes the hash or re-resolves the
 * closure, so the child re-verify over the inert projection matches the gate's
 * freeze.
 *
 * Credential MATERIAL rides ONE `CredentialDelivery` delivered to the child on
 * the frame, unioned from three rails and deduped by credentialId: tool bindings
 * (grant-scoped, resolved here via `buildCredentialDelivery`); every top-level
 * inference source; and every inline body step's inference source. Both inference
 * rails are resolved HERE from the DB under the tenant-ownership authority
 * (`resolveInferenceMaterials`), so the deploy is self-contained -- no caller
 * pre-supplies material, and a spawned body child finds its secret in the cell.
 * The merge is a post-authz union of already-cleared material (tool material is
 * grant-scoped, inference material is tenant-ownership-scoped), never a shared
 * authz check.
 * Credential GRANT enforcement is a SEPARATE layer: the `credential:{id}` /
 * `use` grant the runtime gate checks is minted per-run by run-grant
 * materialization into `runs/<runId>/grants.json`, not carried on this frame --
 * the deploy-time `config.grants` spawn-time snapshot is suppressed once the
 * sidecar wires per-run grant pushes, so it is not the enforcement transport.
 *
 * A gate outcome that did not approve cannot deploy: an unapproved `approval`
 * fails closed here rather than shipping an unfrozen definition.
 *
 * This does the READ-ONLY preparation ONLY: it runs the guards, resolves
 * credential material, pins the body sources, and builds the asset mounts, then
 * returns the frozen definition id and the assembled send args. It emits NO
 * frame and writes NO row, so it has no side effect to unwind. The ordinary path
 * (`deployCodeSourcedWorkflow`) sequences prepare -> INSERT anchor -> emit so
 * the anchor is visible before the frame spawns the child; `emitSourceRefDeployFrame`
 * composes prepare -> emit for the prepared provisioned path, whose anchor row
 * already exists from prepare time. It returns the frozen definition id so each
 * caller writes the same content-addressed identity the gate persisted.
 */
async function prepareSourceRefDeploy(
  args: DeployCodeSourcedWorkflowArgs & {
    allocationTarget?: AllocatedSidecarTarget;
    sidecarAllocationRouter?: SidecarAllocationRouter;
  },
): Promise<{ definitionId: string; sendArgs: SendMultiStepDeployFrameArgs }> {
  const { approval, projection, closure } = args.approved;
  if (!approval.ok) {
    throw new Error(
      `deployCodeSourcedWorkflow: refusing to deploy an unapproved workflow (gate reason: ${approval.reason})`,
    );
  }

  // Fail-closed persisted-definition guard. The anchor row this writes carries
  // an FK to `workflow_definition`, so a phantom `definitionId` would otherwise
  // reach the INSERT and fail with a raw constraint violation. A mis-wired
  // caller -- or a test double that skips the approve step's DB writer -- could
  // pass an approval whose definition was never persisted; verify it exists and
  // fail with a domain error before deploying, rather than deploying and then
  // failing the anchor insert into a deployed-but-unanchored state.
  const persistedDefinition = await args.db.query.workflowDefinition.findFirst({
    where: eq(workflowDefinitionTable.id, approval.definitionId),
    columns: { id: true },
  });
  if (persistedDefinition === undefined) {
    throw new Error(
      `deployCodeSourcedWorkflow: approval.definitionId ${approval.definitionId} does not reference a persisted workflow_definition row`,
    );
  }

  // Coherence guard, run BEFORE the deploy frame: the anchor row's id and its
  // routing address must name the same run. The deployment mail address is
  // frozen into the approved package bytes at authoring time, so its run id is
  // fixed before this runs and the caller owns `anchorRunId`. A mismatched
  // (anchorRunId, agentAddress) pair would let run-grant materialization find
  // the anchor by `address` while `deriveRunAddress` from `anchorRunId` names a
  // different run -- a silent grant-identity split. Fail closed here, before the
  // frame is sent or any row is persisted, rather than deploying an incoherent
  // pair.
  const derivedAddress = deriveRunAddress({
    runId: args.anchorRunId,
    domain: args.deploymentDomain,
  });
  if (derivedAddress !== args.agentAddress) {
    throw new Error(
      `deployCodeSourcedWorkflow: anchorRunId ${args.anchorRunId} derives address ${derivedAddress} but agentAddress is ${args.agentAddress}`,
    );
  }

  // Resolve the operator-approved credential bindings into delivered material.
  // Tenant-owned resolution keys off the definition's tenant and walks up the
  // hierarchy; it does not consult creator/invoker (the only locator today is
  // `tenant`). A code-sourced deployment has no single authenticated invoker,
  // so invoker is null; when principal-owned locators arrive, the asset creator
  // must be resolved and passed here. A resolution failure is fail-closed.
  const bindings = projection.credentialBindings ?? [];
  let credentials: CredentialDelivery | undefined;
  if (bindings.length > 0) {
    if (args.credentialCipher === undefined) {
      throw new Error(
        "deployCodeSourcedWorkflow: definition carries credential bindings but " +
          "no credentialCipher was supplied; cannot resolve credential material",
      );
    }
    const delivery = await buildCredentialDelivery({
      db: args.db,
      tenantId: args.tenantId,
      bindings,
      creatorPrincipalId: null,
      invokerPrincipalId: null,
      credentialCipher: args.credentialCipher,
    });
    if (!delivery.ok) {
      throw new Error(
        `deployCodeSourcedWorkflow: credential binding resolution failed: ${delivery.reason.message}`,
      );
    }
    credentials = delivery.delivery;
  }

  // Pin per-step inference sources for the projection's inline trigger bodies
  // -- onTrigger sections and childWorkflow children, enumerated transitively.
  // The hub holds only the frozen inert projection, so it enumerates the inline
  // bodies from the wire form and resolves each body step's source through the
  // same resolver + operator-approval gate the top-level steps use
  // (`pickStepInferenceSource` against `approval.approvedGrants`). Each body's
  // wire hash is recomputed from the inert body verbatim, so a body child's
  // re-verify over the re-evaluated closure clears the same barrier a top-level
  // re-verify does. The pinned sources ride OUTSIDE the hash; their trust comes
  // from being resolved here under the approval gate, which is why the pin stays
  // hub-side and is never caller-supplied.
  //
  // These entries ride the `referencedDefinitions` wire field. Each entry's
  // `definition` is the approved inert body def straight from the frozen,
  // hash-covered projection (id set to the ref); the sidecar reads that id to
  // key the per-body approved hash and to stage the body's `sources.json`, which
  // the body child reads to pin its steps. The body child resolves the body
  // DEFINITION itself in-memory from the re-verified closure and hard-fails
  // rather than reading it off disk, so no body workflow.json is staged (see the
  // staging loop in workflow-host-wiring.ts and the anti-fallback guard in
  // workflow-host run-child.ts).
  const referencedDefinitions: WorkflowProjectionWithSources[] =
    await Promise.all(
      enumerateInertBodies(projection).map(async (body) => {
        // Pin every body step's source, recursing into a loop nested inside the
        // body (`pinInertStepSources` owns the walk, the loop recursion, and the
        // flat-map collision rule). The per-step LEAF policy is the body's own:
        //
        //   - A genuine non-agent step (sleep, awaitSignal, or the loop
        //     container itself) declares no preference and runs no inference, so
        //     it advertises no `inference.source` grant the gate could approve.
        //     The deploy frame's coverage contract still requires a source entry
        //     for every body step, so pin the deploy's default source as an inert
        //     placeholder: the body child resolves a step's source only when that
        //     step invokes inference, so this entry is never read, which is why
        //     it needs no operator approval.
        //   - An agent step -- including one that declares no preference (an
        //     empty `modelSources`), which still resolves a source at runtime --
        //     runs inference and gets a source pinned through the approval gate.
        const sources = pinInertStepSources({
          definition: body.definition,
          workflowId: body.ref,
          context: `deployCodeSourcedWorkflow body ${body.ref}: `,
          resolveLeafSource: ({ stepId, isAgent, preference }) => {
            if (!isAgent) {
              const placeholder = args.config.sources.find(
                (s) => s.id === args.config.defaultSource,
              );
              if (placeholder === undefined) {
                throw new WorkflowDefinitionInvalidError(
                  body.ref,
                  `non-agent body step ${stepId} needs an inert placeholder source, but the deploy config carries no defaultSource entry to pin`,
                );
              }
              return placeholder;
            }
            return pickStepInferenceSource({
              preferred: preference,
              stepId,
              workflowId: body.ref,
              config: args.config,
              operatorApprovals: approval.approvedGrants,
            });
          },
        });
        return {
          definition: body.definition,
          sources,
          approvedWireHash: await computeWireDefinitionHash(body.definition),
        };
      }),
    );

  // Assemble the ONE credential delivery. Its `materials` cover three rails, each
  // authorized upstream on its own terms, deduped by credentialId into one cell:
  //   - tool bindings, grant-scoped through `buildCredentialDelivery` above;
  //   - EVERY top-level inference source (`args.sources`), tenant-owned;
  //   - EVERY inline body step's inference source (onTrigger/childWorkflow bodies
  //     pinned above), tenant-owned.
  // The inference rails are resolved HERE from the DB under the tenant-ownership
  // authority, so this deploy is self-contained: a direct deploy (a test) that
  // seeds the credentials in the DB -- rather than pre-supplying material -- still
  // fills the cell, and a spawned body finds its secret rather than failing closed
  // at resolve time. Precedence on a shared credentialId is tool material first
  // (grant-scoped), then the inference material (tenant-ownership-scoped): the
  // first material for an id wins. Inference sources carry NO binding descriptor
  // -- they reference their credential by id directly.
  const materials = new Map<string, CredentialMaterialEntry>();
  for (const material of credentials?.materials ?? []) {
    materials.set(material.credentialId, material);
  }
  const inferenceCredentialIds = new Set<string>();
  for (const stepSources of Object.values(args.sources)) {
    for (const source of stepSources) {
      inferenceCredentialIds.add(source.credentialId);
    }
  }
  for (const body of referencedDefinitions) {
    for (const stepSources of Object.values(body.sources)) {
      for (const source of stepSources) {
        inferenceCredentialIds.add(source.credentialId);
      }
    }
  }
  if (inferenceCredentialIds.size > 0) {
    if (args.credentialCipher === undefined) {
      throw new Error(
        "deployCodeSourcedWorkflow: pinned inference sources reference credentials " +
          "but no credentialCipher was supplied to resolve them",
      );
    }
    const inferenceMaterials = await resolveInferenceMaterials(
      args.db,
      args.tenantId,
      inferenceCredentialIds,
      args.credentialCipher,
    );
    for (const material of inferenceMaterials) {
      if (!materials.has(material.credentialId)) {
        materials.set(material.credentialId, material);
      }
    }
  }
  // A delivery with tool bindings always carries their material, so an empty map
  // means no rail contributed anything -- send no delivery. (Tool bindings never
  // produce a descriptor without a material, so bindings-without-materials cannot
  // occur.)
  const credentialDelivery: CredentialDelivery | undefined =
    materials.size > 0
      ? {
          bindings: credentials?.bindings ?? [],
          materials: [...materials.values()],
        }
      : undefined;

  // An asset-sourced pin's `kind:"asset"` closure entries read from source
  // assets the sidecar cannot fetch itself; deliver them inline on the frame so
  // the sidecar checks them out into its durable per-deployment source store. A
  // registry pin fetches its tarballs over HTTP and delivers none.
  const assets: WorkflowSourceAssetMount[] = isAssetDeployArgs(args)
    ? await buildSourceAssetMounts(closure, args.resolveAttachment)
    : [];

  const sendArgs: SendMultiStepDeployFrameArgs = {
    lineage: "source-ref",
    sidecarRouter: args.sidecarRouter,
    ...(args.sidecarAllocationRouter !== undefined
      ? { sidecarAllocationRouter: args.sidecarAllocationRouter }
      : {}),
    ...(args.allocationTarget !== undefined
      ? { allocationTarget: args.allocationTarget }
      : {}),
    agentAddress: args.agentAddress,
    config: args.config,
    sources: args.sources,
    approvedWireHash: approval.approvedWireHash,
    sourceRef: { source: args.source, closure },
    ...(credentialDelivery !== undefined
      ? { credentials: credentialDelivery }
      : {}),
    ...(referencedDefinitions.length > 0 ? { referencedDefinitions } : {}),
    ...(assets.length > 0 ? { assets } : {}),
  };

  return { definitionId: approval.definitionId, sendArgs };
}

/**
 * Prepare then emit the source-ref deploy frame, for the prepared provisioned
 * path whose anchor `workflow_run` row already exists (inserted at prepare
 * time). It emits the frame but does NOT touch the anchor row: the caller
 * (`deployPreparedCodeSourcedWorkflow`) stamps the acked key under the
 * allocation-ownership lock. A tagged `DeployFrameFailure` is converted to the
 * `SessionLaunchError` disposition the allocation reconciler consumes, while
 * untagged preparation errors remain safe same-generation retries. The ordinary
 * path does NOT use this wrapper -- it must interleave the anchor INSERT between
 * prepare and emit, so it drives `prepareSourceRefDeploy` and
 * `sendMultiStepDeployFrame` directly.
 */
// The non-secret projection of a delivery, persisted on the anchor run so the
// reconnect resync can re-resolve current materials. Secrets never land here.
function credentialRefsFromDelivery(
  delivery: CredentialDelivery,
): WorkflowRunCredentialRefs {
  return {
    credentialIds: delivery.materials.map((material) => material.credentialId),
    bindings: delivery.bindings,
  };
}

async function emitSourceRefDeployFrame(
  args: DeployCodeSourcedWorkflowArgs & {
    allocationTarget?: AllocatedSidecarTarget;
    sidecarAllocationRouter?: SidecarAllocationRouter;
  },
): Promise<{
  publicKey: string;
  definitionId: string;
  credentialRefs?: WorkflowRunCredentialRefs;
}> {
  const { definitionId, sendArgs } = await prepareSourceRefDeploy(args);
  try {
    const result = await sendMultiStepDeployFrame(sendArgs);
    return {
      publicKey: result.publicKey,
      definitionId,
      ...(sendArgs.credentials !== undefined
        ? { credentialRefs: credentialRefsFromDelivery(sendArgs.credentials) }
        : {}),
    };
  } catch (cause) {
    if (!isDeployFrameFailure(cause)) throw cause;
    throw new SessionLaunchError("start", cause, cause.frameSent);
  }
}

/**
 * The single public composition entrypoint for a SHARED code-sourced (npm)
 * deploy: prepare, INSERT the deployment's anchor `workflow_run` row, THEN emit
 * the source-ref frame. The anchor row is the deployment's first-class record
 * that owns its routing address and public key. Run-grant materialization keys
 * off this row (address + live status), so WITHOUT it no per-run grants (tool,
 * capability, OR credential) ever materialize for a source-ref deployment. Born
 * "deployed" (live but pre-trigger) with a null public key: the first trigger's
 * materialization flips it to "running" via `anchorWithPrincipal`'s guarded
 * update, which a row born "running" would skip. Its `anchorRunId` equals its
 * own id, so the anchor references itself. The deployer read grant is deferred
 * to the production route, which carries the authenticated deployer principal.
 *
 * ORDERING IS LOAD-BEARING. The anchor row must be committed and visible to the
 * pack-receipt connection BEFORE the frame reaches the wire: the frame spawns
 * the child, whose first events pack races the ack back, and
 * `receiveWorkflowRunPack` fails closed on a missing live anchor. Emitting first
 * (the previous order) rejected that first pack and never bootstrapped the log.
 * This works because `args.db` is the autocommit handle (`DB["db"]`, which the
 * type forbids from being a transaction) and the INSERT is NOT wrapped in a
 * transaction with the emit -- so the row is durably visible the instant the
 * INSERT statement returns. Do NOT relax `db` to a transaction executor or wrap
 * anchor+emit in one transaction to make them atomic: that reopens the race.
 *
 * On emit failure the anchor row is rolled back or fenced by the `frameSent`
 * evidence from the transport. `leakedAgent: false` (safe to fully roll back) is
 * the STRONG claim and is made only on positive proof the frame never reached
 * the wire (`isDeployFrameFailure && frameSent === false`); every other failure
 * -- a sent-but-unacked frame OR any untagged error -- is treated as
 * possibly-live: the anchor is fenced `deployed` -> `failed` and the error is
 * `leakedAgent: true`.
 *
 * The prepared provisioned path does NOT use this composition: its anchor row
 * already exists from prepare time, so it drives `emitSourceRefDeployFrame` and
 * an UPDATE-under-allocation-lock instead.
 */
export async function deployCodeSourcedWorkflow(
  args: DeployCodeSourcedWorkflowArgs,
): Promise<{ publicKey: string }> {
  const { definitionId, sendArgs } = await prepareSourceRefDeploy(args);

  // INSERT the anchor before the frame. A collision or DB error here spawned
  // nothing (no frame went out), so it is a clean, non-leaking failure.
  try {
    await args.db.insert(workflowRunTable).values({
      id: args.anchorRunId,
      tenantId: args.tenantId,
      anchorRunId: args.anchorRunId,
      definitionId,
      address: args.agentAddress,
      publicKey: null,
      status: "deployed",
      createdAt: new Date(),
      // Persist the non-secret shape of the delivery so the reconnect resync
      // can re-resolve current materials for these ids. Secrets never land here.
      ...(sendArgs.credentials !== undefined
        ? { credentialRefs: credentialRefsFromDelivery(sendArgs.credentials) }
        : {}),
    });
  } catch (cause) {
    throw new SessionLaunchError("start", cause, false);
  }

  let publicKey: string;
  try {
    const result = await sendMultiStepDeployFrame(sendArgs);
    publicKey = result.publicKey;
  } catch (cause) {
    if (isDeployFrameFailure(cause) && cause.frameSent === false) {
      // Positive proof the frame never reached the wire: nothing spawned, so
      // fully roll the anchor back. The guard (`deployed`, null key) is a
      // tripwire on the `frameSent: false` contract -- a 0-row delete means the
      // row advanced or vanished, so the contract lied and a child may be live;
      // surface that loudly and refuse to claim it is safe to roll back.
      const deleted = await args.db
        .delete(workflowRunTable)
        .where(
          and(
            eq(workflowRunTable.id, args.anchorRunId),
            eq(workflowRunTable.anchorRunId, args.anchorRunId),
            eq(workflowRunTable.tenantId, args.tenantId),
            eq(workflowRunTable.status, "deployed"),
            isNull(workflowRunTable.publicKey),
          ),
        )
        .returning({ id: workflowRunTable.id });
      if (deleted.length === 0) {
        logger.error`anchor-before-frame rollback found no deployed/null-key row for ${args.anchorRunId} after a frameSent:false failure; the never-sent contract was violated and a child may be live`;
        throw new SessionLaunchError("start", cause, true);
      }
      throw new SessionLaunchError("start", cause, false);
    }
    // A sent-but-unacked frame, OR any untagged/unexpected error: no positive
    // proof of a clean send, so treat the agent as possibly-live. Fence the
    // anchor `deployed` -> `failed` (guarded so a self-flip to "running" by a
    // trigger that already landed is left alone). Do NOT delete: a live child
    // needs the anchor to bootstrap.
    const flipped = await args.db
      .update(workflowRunTable)
      .set({ status: "failed" })
      .where(
        and(
          eq(workflowRunTable.id, args.anchorRunId),
          eq(workflowRunTable.anchorRunId, args.anchorRunId),
          eq(workflowRunTable.tenantId, args.tenantId),
          eq(workflowRunTable.status, "deployed"),
          isNull(workflowRunTable.publicKey),
        ),
      )
      .returning({ id: workflowRunTable.id });
    if (flipped.length === 0) {
      // The anchor already advanced past deployed -- a trigger flipped it to
      // "running", so the deploy actually succeeded and the run is progressing
      // despite the ack failure. Leave it; the leaked-agent disposition still
      // holds because the frame was (or may have been) sent.
      logger.warn`anchor-before-frame: anchor ${args.anchorRunId} already advanced past deployed on an unacked/failed emit; the agent is live and the run is progressing despite the ack failure`;
    } else {
      logger.warn`anchor-before-frame: fenced anchor ${args.anchorRunId} deployed->failed on an unacked/failed emit; the agent may be leaked but the run is dead`;
    }
    throw new SessionLaunchError("start", cause, true);
  }

  // Emit succeeded: stamp the acked key. No status guard -- the key is a fact
  // regardless of whether the pack-ack race already flipped the row to
  // "running", and skipping the stamp there would strand a live run with a null
  // key. A 0-row update is an anomaly (nothing should remove a deployed anchor
  // on the success path), but the deploy succeeded, so log it rather than
  // failing a live run.
  const stamped = await args.db
    .update(workflowRunTable)
    .set({ publicKey })
    .where(
      and(
        eq(workflowRunTable.id, args.anchorRunId),
        eq(workflowRunTable.anchorRunId, args.anchorRunId),
        eq(workflowRunTable.tenantId, args.tenantId),
      ),
    )
    .returning({ id: workflowRunTable.id });
  if (stamped.length === 0) {
    logger.error`anchor-before-frame: anchor ${args.anchorRunId} vanished before its public key could be stamped on a successful deploy`;
  }

  return { publicKey };
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
      throw new Error("Provisioned deployment routing is not configured");
    }
    return sidecarAllocationRouter;
  }

  /**
   * Stage one per-step deploy on the sidecar: resolve assets and tool
   * packages, write the deploy tree, provision the step, and deliver the
   * deploy + asset packs (Phases 0-2b). Phase 1 binds a transient route for
   * the step address, fires a no-spawn provision frame (init repo + record
   * hub key), and unbinds the route once the packs land -- no warm harness and
   * no child. The deployment-level workflow frame, sent once after every step
   * is staged, spawns the child. A call without `stageOnly` is rejected -- the
   * legacy warm-harness and single-step-head paths are gone.
   */
  async function executeLaunchPhases(params: {
    agentAddress: string;
    agentId: string;
    runId: string;
    config: HarnessConfig;
    deployContent: DeployContent;
    toolPackagePins?: readonly ToolPackagePin[];
    /**
     * Per-step stage. When true, Phase 1 binds a transient route for the step
     * address, fires a no-spawn provision frame (the sidecar inits the step's
     * agent-state repo and records the hub key), delivers the deploy + asset
     * packs, and unbinds the route -- no warm harness and no child. The
     * deployment-level workflow frame, sent once after every step is staged,
     * spawns the child.
     */
    stageOnly?: boolean;
    allocationTarget?: AllocatedSidecarTarget;
  }): Promise<void> {
    const { agentAddress, agentId, runId, config, deployContent } = params;
    const toolPackagePins = params.toolPackagePins ?? [];
    const stageOnly = params.stageOnly ?? false;

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
      // Phase 1: Provision on sidecar. A stage-only per-step deploy sends a
      // no-spawn provision frame: the sidecar inits the step's agent-state
      // repo and records the hub key, but spawns nothing. Firing the frame
      // before the Phase 2 pack is the ordering barrier -- the repo must
      // exist before the pack applies.
      try {
        if (stageOnly) {
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
          // Every caller supplies `stageOnly`. A deploy without it has no
          // provisioning shape -- the legacy warm-harness and single-step-head
          // paths are gone -- so fail loud rather than ship a deploy pack the
          // sidecar never provisioned a repo for.
          throw new Error("executeLaunchPhases: a deploy requires stageOnly");
        }
      } catch (err) {
        throw new SessionLaunchError("provision", err, false);
      }

      // Phase 2: Pack delivery. A stage-only step has no supervisor to
      // undeploy, so on failure it only drops its transient route (in the
      // `finally`). The step's inited agent-state repo is left on the sidecar:
      // the deploy aborts before the deployment frame is sent, so there is
      // nothing to undeploy, and a redeploy of the same deployment overwrites
      // the orphaned repo. This is an acceptable minor leak on the exceptional
      // staging-failure path, not a live-path cost.
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
              runId,
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
    runId: string;
    config: HarnessConfig;
    deployContent: DeployContent;
    toolPackagePins?: readonly ToolPackagePin[];
    allocationTarget?: AllocatedSidecarTarget;
  }): Promise<void> {
    await executeLaunchPhases({
      agentAddress: params.agentAddress,
      agentId: params.agentId,
      runId: params.runId,
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

  // Resolve the npm registry config a code-sourced install resolves external
  // deps against, by the registry name. A code-sourced deploy needs the
  // registry map configured; a hub that mounts the deploy surface without it is
  // mis-wired, so this fails loud rather than defaulting a registry URL.
  function requireRegistryConfig(registryName: string): RegistryConfig {
    if (toolPackageRegistries === undefined) {
      throw new Error(
        "deployWorkflowFromSource: the session service has no toolPackageRegistries configured; a code-sourced deploy cannot resolve its dependency closure",
      );
    }
    const config = toolPackageRegistries.httpRegistries.get(registryName);
    if (config === undefined) {
      throw new Error(
        `deployWorkflowFromSource: no HTTP registry named ${JSON.stringify(registryName)} is configured`,
      );
    }
    return config;
  }

  // Build the git-pack resolver a source/tarball asset arm delivers inline. The
  // pin names one backing asset, so the resolver binds that asset's repo (its
  // kind fixed by the arm) and its default ref; a request for any OTHER asset id
  // is a closure that reaches beyond its single backing asset and fails loud
  // rather than silently packing the wrong repo.
  function bindAssetAttachmentResolver(
    assetId: string,
    repoKind: RepoKind,
  ): ResolveAssetAttachmentFn {
    return async (requestedAssetId) => {
      if (requestedAssetId !== assetId) {
        throw new Error(
          `deployWorkflowFromSource: closure references asset ${requestedAssetId}, but only the pinned source asset ${assetId} is deliverable`,
        );
      }
      const repoId: RepoId = { kind: repoKind, id: assetId };
      const commitSha = await agentRepoStore.repoStore.resolveRef(
        HUB_PRINCIPAL,
        repoId,
        DEFAULT_ASSET_REF,
      );
      if (commitSha === null) {
        throw new Error(
          `deployWorkflowFromSource: source asset ${assetId} has no commit on ${DEFAULT_ASSET_REF}`,
        );
      }
      const { pack, ref } = await agentRepoStore.repoStore.createPack(
        HUB_PRINCIPAL,
        repoId,
        DEFAULT_ASSET_REF,
      );
      return { pack, ref, commitSha };
    };
  }

  // Assemble the install args for the concrete source arm. Mirrors the
  // `isAssetSourceInstallArgs`/`isAssetTarballInstallArgs` guards the probe gate
  // narrows on: an asset-`source` arm binds committed reads at the pinned commit
  // plus the npm registry for external deps; an asset-`tarball` arm binds the
  // asset's blob reads and a pin; a `registry` arm carries only its registry
  // config and a pin. A `pin` missing where the arm requires it fails closed.
  async function buildInstallArgs(
    params: InstallAndApproveWorkflowSourceParams,
    resolveAttachment: ResolveAssetAttachmentFn | null,
  ): Promise<InstallAndApproveArgs> {
    if (db === undefined) {
      throw new Error(
        "deployWorkflowFromSource requires a db handle to freeze the approval",
      );
    }
    const dbHandle = db;
    const common = {
      entry: params.entry,
      assetId: params.definitionAssetId,
      approvals: { mode: "approve-probed" } as const,
      router: sidecarRouter,
      db: dbHandle,
    };
    const source = params.source;

    if (source.kind === "asset") {
      if (resolveAttachment === null) {
        throw new Error(
          "deployWorkflowFromSource: an asset-sourced deploy requires an attachment resolver",
        );
      }
      if (source.package.format === "source") {
        const committed =
          await agentRepoStore.repoStore.openCommittedReadsAtCommit(
            HUB_PRINCIPAL,
            { kind: "workflow", id: source.assetId },
            source.package.commitSha,
          );
        if (committed === null) {
          throw new Error(
            `deployWorkflowFromSource: source asset ${source.assetId} has no commit ${source.package.commitSha}`,
          );
        }
        const registryName = requireDefaultRegistryName();
        return {
          ...common,
          source,
          reads: committedReadsToSourceTree(committed),
          registryName,
          registryConfig: requireRegistryConfig(registryName),
          resolveAttachment,
        };
      }
      if (params.pin === undefined) {
        throw new Error(
          "deployWorkflowFromSource: an asset-tarball deploy requires a name@range pin",
        );
      }
      if (assetService === undefined) {
        throw new Error(
          "deployWorkflowFromSource: an asset-tarball deploy requires an asset service to read the package blobs",
        );
      }
      const tarballAssetId = source.assetId;
      const tarballService = assetService;
      return {
        ...common,
        source,
        pin: params.pin,
        readBlob: (path) =>
          tarballService.readAssetBlob({ assetId: tarballAssetId, path }),
        listBlobs: (dir) =>
          tarballService.listAssetBlobs({ assetId: tarballAssetId, dir }),
        resolveAttachment,
      };
    }
    if (params.pin === undefined) {
      throw new Error(
        "deployWorkflowFromSource: a registry deploy requires a name@range pin",
      );
    }
    return {
      ...common,
      source,
      pin: params.pin,
      registryConfig: requireRegistryConfig(source.registry),
    };
  }

  function requireDefaultRegistryName(): string {
    if (toolPackageRegistries === undefined) {
      throw new Error(
        "deployWorkflowFromSource: the session service has no toolPackageRegistries configured; a code-sourced deploy cannot resolve its dependency closure",
      );
    }
    return toolPackageRegistries.defaultRegistry;
  }

  // Bind the pack resolver an asset arm delivers inline. An asset arm delivers
  // its backing repo (its kind fixed by `package.format`); a registry arm
  // fetches its tarballs over HTTP and delivers no asset, so it binds nothing.
  // Both the install (probe) and the deploy rebind the SAME resolver from the
  // source, so a prepared deploy reconstructs it from the frozen `source`.
  function bindSourceAttachmentResolver(
    source: WorkflowDefinitionSource,
  ): ResolveAssetAttachmentFn | null {
    return source.kind === "asset"
      ? bindAssetAttachmentResolver(
          source.assetId,
          source.package.format === "source" ? "workflow" : "package-registry",
        )
      : null;
  }

  // Install + probe + gate + freeze a code-sourced definition, returning the
  // frozen bundle and the (asset-only) attachment resolver. The gate outcome is
  // NOT asserted here: `deployWorkflowFromSource` and `installAndApproveWorkflowSource`
  // each surface a non-approval as their own domain error. This is the common
  // freeze used by both ordinary deploys and provisioned prepare runs.
  async function prepareCodeSourcedApproval(
    params: InstallAndApproveWorkflowSourceParams,
  ): Promise<{
    approved: InstallAndApproveResult;
    resolveAttachment: ResolveAssetAttachmentFn | null;
  }> {
    const resolveAttachment = bindSourceAttachmentResolver(params.source);
    const installArgs = await buildInstallArgs(params, resolveAttachment);
    const approved = await installAndApproveWorkflowDefinition(installArgs);
    return { approved, resolveAttachment };
  }

  // Freeze a code-sourced approval WITHOUT deploying it. The provisioned
  // prepare path persists the returned bundle and deploys it to an allocation
  // later. A non-approval fails closed as an invalid
  // definition.
  async function installAndApproveWorkflowSource(
    params: InstallAndApproveWorkflowSourceParams,
  ): Promise<InstallAndApproveResult> {
    const { approved } = await prepareCodeSourcedApproval(params);
    if (!approved.approval.ok) {
      throw new WorkflowDefinitionInvalidError(
        approved.projection.id,
        `code-sourced workflow install did not approve (reason: ${approved.approval.reason})`,
      );
    }
    return approved;
  }

  async function deployWorkflowFromSource(
    params: DeployWorkflowFromSourceParams,
  ): Promise<DeployWorkflowDefinitionResult> {
    if (db === undefined) {
      throw new Error(
        "deployWorkflowFromSource requires a db handle to record the deployment's anchor run",
      );
    }
    const source = params.source;
    const { approved, resolveAttachment } =
      await prepareCodeSourcedApproval(params);
    if (!approved.approval.ok) {
      throw new WorkflowDefinitionInvalidError(
        approved.projection.id,
        `code-sourced workflow install did not approve (reason: ${approved.approval.reason})`,
      );
    }

    // Pin every top-level step's inference source under the frozen approval,
    // then hand the frozen bundle to the source-ref deploy.
    const sources = buildInertProjectionStepSources({
      projection: approved.projection,
      config: params.config,
      operatorApprovals: approved.approval.approvedGrants,
    });

    const commonDeploy = {
      approved,
      sidecarRouter,
      agentAddress: params.agentAddress,
      config: params.config,
      sources,
      db,
      tenantId: params.tenantId,
      anchorRunId: params.anchorRunId,
      deploymentDomain: params.deploymentDomain,
      ...(params.credentialCipher !== undefined
        ? { credentialCipher: params.credentialCipher }
        : {}),
    };
    // Branch on the source discriminant so the deploy args match the
    // asset/registry arms of `DeployCodeSourcedWorkflowArgs`: an asset arm
    // carries the attachment resolver (asserted non-null here to satisfy the
    // union and fail loud on a mis-wired caller), a registry arm carries none.
    let result: { publicKey: string };
    if (source.kind === "asset") {
      if (resolveAttachment === null) {
        throw new Error(
          "deployWorkflowFromSource: asset source deploy is missing its attachment resolver",
        );
      }
      result = await deployCodeSourcedWorkflow({
        ...commonDeploy,
        source,
        resolveAttachment,
      });
    } else {
      result = await deployCodeSourcedWorkflow({ ...commonDeploy, source });
    }

    // Seed the deploying principal's read grant on the deployment's workflow-run
    // resource. `deployCodeSourcedWorkflow` wrote the anchor row but deliberately
    // leaves this grant to the route, which carries the authenticated deployer
    // principal.
    const now = new Date();
    await db.insert(grantTable).values({
      id: generateId("grant"),
      tenantId: params.tenantId,
      principalId: params.config.principalId,
      resource: `workflow-run:${params.anchorRunId}`,
      action: "read",
      effect: "allow",
      origin: "creator",
      createdAt: now,
      updatedAt: now,
    });

    return {
      anchorRunId: params.anchorRunId,
      deploymentAddress: params.agentAddress,
      publicKey: result.publicKey,
    };
  }

  /**
   * Update a prepared anchor run's `publicKey` under the allocation-ownership
   * lock. The anchor row was inserted at prepare time; this stamps the
   * supervisor key returned by the deploy ack, but only while the allocation
   * still names this exact accepted generation for this anchor. A lost lock (the
   * allocation moved on, another worker took the generation) fails closed as a
   * leaked-agent `SessionLaunchError` -- the deploy already reached the sidecar,
   * so the caller must treat the sidecar agent as possibly live. Used by the
   * `deployPreparedCodeSourcedWorkflow` prepared path.
   */
  async function updateAnchorPublicKeyUnderAllocationLock(args: {
    tenantId: string;
    anchorRunId: string;
    allocationTarget: AllocatedSidecarTarget;
    publicKey: string;
    credentialRefs?: WorkflowRunCredentialRefs;
  }): Promise<void> {
    if (db === undefined) {
      throw new Error(
        "updateAnchorPublicKeyUnderAllocationLock requires a db handle",
      );
    }
    const dbHandle = db;
    try {
      const updated = await dbHandle.transaction(async (tx) => {
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
            eq(sidecarAllocationTable.id, args.allocationTarget.allocationId),
          )
          .limit(1)
          .for("update");
        if (
          allocation === undefined ||
          allocation.anchorRunId !== args.anchorRunId ||
          allocation.status !== "allocated" ||
          allocation.generation !== args.allocationTarget.generation ||
          allocation.ensureAcceptedGeneration !==
            args.allocationTarget.generation
        ) {
          return null;
        }
        const [anchor] = await tx
          .update(workflowRunTable)
          .set({
            publicKey: args.publicKey,
            ...(args.credentialRefs !== undefined
              ? { credentialRefs: args.credentialRefs }
              : {}),
          })
          .where(
            and(
              eq(workflowRunTable.id, args.anchorRunId),
              eq(workflowRunTable.anchorRunId, args.anchorRunId),
              eq(workflowRunTable.tenantId, args.tenantId),
            ),
          )
          .returning({ id: workflowRunTable.id });
        return anchor ?? null;
      });
      if (updated === null) {
        throw new Error(
          `Prepared anchor run ${args.anchorRunId} lost allocation ownership before initialization completed`,
        );
      }
    } catch (error) {
      throw new SessionLaunchError("start", error, true);
    }
  }

  /**
   * Deploy a previously-frozen code-sourced approval bundle to a dedicated
   * allocation. The anchor `workflow_run` row already exists from prepare time
   * (with its `definitionId` set), so this UPDATES it under the
   * allocation-ownership lock
   * rather than inserting. No re-probe: the frozen projection/hash/closure ride
   * verbatim from `params.approved`, and the per-step inference sources are
   * re-pinned from the re-resolved chain (deliberately NOT frozen, since a
   * resolved source carries a credential secret).
   */
  async function deployPreparedCodeSourcedWorkflow(
    params: DeployPreparedCodeSourcedWorkflowParams,
  ): Promise<DeployWorkflowDefinitionResult> {
    if (db === undefined) {
      throw new Error(
        "deployPreparedCodeSourcedWorkflow requires a db handle to update the prepared anchor run",
      );
    }
    const dbHandle = db;
    const approval = params.approved.approval;
    if (!approval.ok) {
      throw new Error(
        "deployPreparedCodeSourcedWorkflow: refusing to deploy an unapproved workflow bundle",
      );
    }
    const allocationRouter = requireAllocationRouter();
    const source = params.source;
    const resolveAttachment = bindSourceAttachmentResolver(source);

    // Re-pin every top-level step's inference source from the re-resolved chain
    // under the frozen approval -- the same pin the shared deploy computes.
    const sources = buildInertProjectionStepSources({
      projection: params.approved.projection,
      config: params.config,
      operatorApprovals: approval.approvedGrants,
    });

    // Restore the Hub-authoritative run ref onto the exact allocation generation
    // before its address is routed.
    await restoreWorkflowRunToAllocation({
      agentRepoStore,
      allocationRouter,
      allocationTarget: params.allocationTarget,
      agentAddress: params.agentAddress,
    });

    const commonEmit = {
      approved: params.approved,
      sidecarRouter,
      sidecarAllocationRouter: allocationRouter,
      allocationTarget: params.allocationTarget,
      agentAddress: params.agentAddress,
      config: params.config,
      sources,
      db: dbHandle,
      tenantId: params.tenantId,
      anchorRunId: params.anchorRunId,
      deploymentDomain: params.deploymentDomain,
      ...(params.credentialCipher !== undefined
        ? { credentialCipher: params.credentialCipher }
        : {}),
    };
    // Branch on the source discriminant so the emit args match the asset/registry
    // arms: an asset arm carries the rebuilt attachment resolver (asserted
    // non-null to satisfy the union), a registry arm carries none.
    let result: {
      publicKey: string;
      definitionId: string;
      credentialRefs?: WorkflowRunCredentialRefs;
    };
    if (source.kind === "asset") {
      if (resolveAttachment === null) {
        throw new Error(
          "deployPreparedCodeSourcedWorkflow: asset source deploy is missing its attachment resolver",
        );
      }
      result = await emitSourceRefDeployFrame({
        ...commonEmit,
        source,
        resolveAttachment,
      });
    } else {
      result = await emitSourceRefDeployFrame({ ...commonEmit, source });
    }

    await updateAnchorPublicKeyUnderAllocationLock({
      tenantId: params.tenantId,
      anchorRunId: params.anchorRunId,
      allocationTarget: params.allocationTarget,
      publicKey: result.publicKey,
      ...(result.credentialRefs !== undefined
        ? { credentialRefs: result.credentialRefs }
        : {}),
    });

    return {
      anchorRunId: params.anchorRunId,
      deploymentAddress: params.agentAddress,
      publicKey: result.publicKey,
    };
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
              eq(sessionAssetTable.runId, record.runId),
              eq(sessionAssetTable.mountPath, record.mountPath),
              eq(sessionAssetTable.assetPackSha, record.assetPackSha),
              eq(sessionAssetTable.sourceCommitSha, record.sourceCommitSha),
            ),
          );
      } catch (err) {
        logger.warn`session_asset rollback failed for earlier-committed instance=${record.runId} mountPath=${record.mountPath}: ${err instanceof Error ? err.message : String(err)}`;
      }
    }
  }

  async function sendAttachmentPack(
    runId: string,
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
      runId,
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
          target: [sessionAssetTable.runId, sessionAssetTable.mountPath],
        })
        .returning({ runId: sessionAssetTable.runId });
      if (inserted.length === 0) {
        const existing = await db.query.sessionAsset.findFirst({
          where: and(
            eq(sessionAssetTable.runId, runId),
            eq(sessionAssetTable.mountPath, mountPath),
          ),
          columns: {
            assetPackSha: true,
            sourceCommitSha: true,
          },
        });
        if (existing === undefined) {
          throw new Error(
            `session_asset ${runId}/${mountPath} disappeared after its insert conflicted`,
          );
        }
        if (
          existing.assetPackSha !== assetPackSha ||
          existing.sourceCommitSha !== sourceCommitSha
        ) {
          throw new Error(
            `session_asset ${runId}/${mountPath} conflicts with the allocated workflow's restored asset`,
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
                eq(sessionAssetTable.runId, rollbackRecord.runId),
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
          logger.warn`session_asset rollback failed for instance=${runId} mountPath=${mountPath}: ${msg}`;
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
    deployWorkflowFromSource,
    installAndApproveWorkflowSource,
    deployPreparedCodeSourcedWorkflow,
    sendUserMessage,
    endSession,
  };
}

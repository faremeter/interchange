// Sidecar deploy program for the workflow host. Stages a workflow deployment
// onto a supervised workflow-process child: materializes the frozen closure,
// records the run, and registers the deployment's mail, signal, drain, and
// grants routes.

import { rm, stat } from "node:fs/promises";
import { join as pathJoin } from "node:path";

import { type } from "arktype";

import { derivePublicKeyBytes, signEd25519 } from "@intx/crypto";
import { getLogger } from "@intx/log";
import type { HubTransport } from "@intx/mail-memory";
import type {
  Principal,
  RepoId,
  RepoStore,
  WorkflowRunSupervisorPrincipal,
} from "@intx/hub-sessions/substrate";
import {
  hexDecode,
  hexEncode,
  type CredentialCipher,
  type SignalKind,
} from "@intx/types";
import {
  parseInferenceEvent,
  type ApprovalSnapshot,
  type CryptoProvider,
  type InferenceEvent,
  type InferenceSource,
  type InboundMailOutcome,
  type InboundMailPolicy,
  type KeyPair,
} from "@intx/types/runtime";
import {
  WORKFLOW_CONTROL_INITIALIZING_ERROR,
  WorkflowProjectionDefinition,
  type AgentDeployFrame,
  type AgentUndeployFrame,
  type CredentialDelivery,
  type SourceRefPin,
  type WorkflowControlFrame,
  type WorkflowRunRefTips,
  type WorkflowSourceAssetMount,
} from "@intx/types/sidecar";
import {
  STEP_ID_PATTERN,
  projectLiveToInert,
  type WorkflowDefinition,
} from "@intx/workflow";

import { loadWorkflowDefinitionFromClosure } from "../workflow-definition-loader";
import { readRunGrants, runGrantsPath } from "../run-grants";
import {
  wrapHubTransportAsMailBus,
  type HubTransportMailBusAdapter,
} from "../mail-bus/hub-transport-adapter";
import {
  hashGrants,
  STEP_GRANTS_PATH,
  STEP_GRANTS_REF,
  type CredentialsSnapshot,
  type CredentialsSnapshotStep,
  type DeriveStepAddress,
  type DeriveStepRepoId,
} from "../supervisor/credentials";
import {
  createWorkflowSupervisor,
  type SpawnOpts,
  type WorkflowSupervisor,
} from "../supervisor/supervisor";
import type {
  DispatchTimingMark,
  SubprocessSpawner,
  SuspensionRegistration,
} from "../supervisor/types";
import type {
  MultistepDrainRouter,
  MultistepGrantsRouter,
  MultistepMailRouter,
  MultistepSignalRouter,
  MultistepSourcesRouter,
  MultistepCredentialsRouter,
} from "./workflow-run-pack-client";
import {
  deleteWorkflowRunRecord,
  scanWorkflowRunRecords,
  writeWorkflowRunRecord,
  type WorkflowRunRecord,
} from "./workflow-run-record";

const logger = getLogger(["interchange", "sidecar", "workflow-host-wiring"]);

// A raw Ed25519 public key is 32 bytes.
const ED25519_PUBLIC_KEY_BYTES = 32;

/**
 * Durable per-deployment store for a source-ref deployment's checked-out
 * source assets. Sibling of the closure instance dir: the closure dir is
 * reclaimed on every apply/restore, this store is not, so the assets survive
 * restart without re-delivery. Reclaimed on redeploy and undeploy.
 */
function deploymentSourceAssetRoot(
  dataDir: string,
  deploymentId: string,
): string {
  return pathJoin(dataDir, "workflow-definition-sources", deploymentId);
}

/**
 * Durable indexed-`.git` store root a pinned deployment's source-format
 * asset entries are checked out from. Sibling of the plain-file source store;
 * both survive restart so re-materialization needs no re-delivery.
 */
function deploymentSourceGitRoot(
  dataDir: string,
  deploymentId: string,
): string {
  return pathJoin(dataDir, "workflow-definition-source-gits", deploymentId);
}

/**
 * `assetId -> mountPath` map a pinned closure's TARBALL `kind:"asset"` entries
 * resolve against, derived purely from the pin so deploy and restore agree
 * without the frame's delivered assets. Source-format entries resolve through
 * `deriveSourceGitDirs` instead.
 */
function deriveSourceAssetMounts(
  pin: SourceRefPin,
  workflowSourceAssetMountPath: (assetId: string) => string,
): Map<string, string> {
  const mounts = new Map<string, string>();
  for (const entry of pin.closure.entries) {
    if (
      entry.source.kind === "asset" &&
      entry.source.package.format === "tarball"
    ) {
      mounts.set(
        entry.source.assetId,
        workflowSourceAssetMountPath(entry.source.assetId),
      );
    }
  }
  return mounts;
}

/**
 * `assetId -> gitDir` map a pinned closure's SOURCE `kind:"asset"` entries
 * check subtrees out of, derived purely from the pin so deploy and restore
 * agree without re-delivery.
 */
function deriveSourceGitDirs(
  pin: SourceRefPin,
  gitRoot: string,
  sourceAssetGitDir: (gitDirRoot: string, assetId: string) => string,
): Map<string, string> {
  const gitDirs = new Map<string, string>();
  for (const entry of pin.closure.entries) {
    if (
      entry.source.kind === "asset" &&
      entry.source.package.format === "source"
    ) {
      gitDirs.set(
        entry.source.assetId,
        sourceAssetGitDir(gitRoot, entry.source.assetId),
      );
    }
  }
  return gitDirs;
}

async function isExistingDir(dir: string): Promise<boolean> {
  try {
    return (await stat(dir)).isDirectory();
  } catch (err) {
    if (err instanceof Error && "code" in err && err.code === "ENOENT") {
      return false;
    }
    throw err;
  }
}

/**
 * Resolve the durable source-asset store root and `assetId -> mountPath` map a
 * pinned deployment's `kind:"asset"` closure entries materialize from,
 * asserting every referenced asset's mount dir exists on disk. Derived purely
 * from the pin so deploy and restore resolve identical mounts. A missing
 * mount is a broken deployment the hub must re-drive; the loader still
 * SRI-verifies each tarball's bytes at materialization.
 */
export async function resolveDeploymentAssetMounts(
  dataDir: string,
  deploymentId: string,
  pin: SourceRefPin,
  workflowSourceAssetMountPath: (assetId: string) => string,
  sourceAssetGitDir: (gitDirRoot: string, assetId: string) => string,
): Promise<{
  assetRoot: string;
  assetMounts: ReadonlyMap<string, string>;
  gitDirs: ReadonlyMap<string, string>;
}> {
  const assetRoot = deploymentSourceAssetRoot(dataDir, deploymentId);
  const assetMounts = deriveSourceAssetMounts(
    pin,
    workflowSourceAssetMountPath,
  );
  for (const [assetId, mountPath] of assetMounts) {
    const mountDir = pathJoin(assetRoot, mountPath);
    if (!(await isExistingDir(mountDir))) {
      throw new Error(
        `resolveDeploymentAssetMounts: source asset ${JSON.stringify(assetId)} for deployment ${deploymentId} is not present in the durable store at ${mountDir}; the deployment must be re-driven from the hub`,
      );
    }
  }
  const gitRoot = deploymentSourceGitRoot(dataDir, deploymentId);
  const gitDirs = deriveSourceGitDirs(pin, gitRoot, sourceAssetGitDir);
  for (const [assetId, gitDir] of gitDirs) {
    if (!(await isExistingDir(gitDir))) {
      throw new Error(
        `resolveDeploymentAssetMounts: source asset ${JSON.stringify(assetId)} for deployment ${deploymentId} has no indexed git store at ${gitDir}; the deployment must be re-driven from the hub`,
      );
    }
  }
  return { assetRoot, assetMounts, gitDirs };
}

/**
 * Parse a substrate-config byte cap (`SIDECAR_CACHE_MAX_BYTES` /
 * `SIDECAR_REGISTRY_MAX_TARBALL_BYTES`) from the multi-step substrate env to a
 * positive finite number, sizing the tarball cache and per-fetch cap at
 * closure materialization. The boot edge threads these values through; a
 * missing or non-numeric value is a boot-edge wiring bug, so it fails loud.
 */
function requireSubstrateByteCap(
  env: Record<string, string>,
  key: string,
): number {
  const raw = env[key];
  if (raw === undefined) {
    throw new Error(
      `sidecar deploy router: ${key} must be present in the multi-step substrate env to materialize a frozen workflow closure`,
    );
  }
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(
      `sidecar deploy router: ${key} must be a positive finite number, got ${JSON.stringify(raw)}`,
    );
  }
  return parsed;
}

/**
 * Principal the deploy router presents when writing a step's grants into the
 * agent-state repo. The kind handler gates `writeTree` as hub-only; the router
 * is the hub's local stand-in on the sidecar's disk, so it claims the hub
 * principal for this single bookkeeping write. The child reads the same repo
 * via the working-tree path (`getRepoDir`), which is not authorize-gated.
 */
const GRANTS_WRITE_PRINCIPAL: Principal = { kind: "hub" };

/**
 * Per-deploy address/repo strategy. The single-step launched-agent deploy and
 * the derived multi-step deploy compute the per-step mail address and
 * agent-state repo id differently; `deriveStepAddress` and `deriveStepRepoId`
 * must agree on the choice, so they are minted together.
 */
type StepStrategy = {
  deriveStepAddress: DeriveStepAddress;
  deriveStepRepoId: DeriveStepRepoId;
};

/**
 * Decide the per-step address/repo strategy from the projection's step count.
 * One step keeps the deploy's own mail address and grants repo keyed by
 * `parseAgentId(address)`; more derive `<runId>-<stepId>` per step.
 *
 * NOTE: `deriveStepAddress` feeds the credentials snapshot and grants-repo
 * derivation, NOT the child's on-disk tool read (`stepDeployTreeDir`), which
 * re-derives the step address from the deployment mailbox address on its own.
 */
function createStepStrategy(args: {
  legacyAddress: string;
  stepOrder: readonly string[];
  multistepDeriveStepAddress: DeriveStepAddress;
  parseAgentId: (agentAddress: string) => string;
}): StepStrategy {
  if (args.stepOrder.length === 1) {
    return {
      deriveStepAddress: () => args.legacyAddress,
      // Defer `parseAgentId` into the closure: the supervisor invokes
      // `deriveStepRepoId` only while assembling the credentialsSnapshot inside
      // `spawn()`, so a malformed address surfaces there, at the same point the
      // rest of the spawn path would fault.
      deriveStepRepoId: () => ({
        kind: "agent-state",
        id: args.parseAgentId(args.legacyAddress),
      }),
    };
  }
  return {
    deriveStepAddress: args.multistepDeriveStepAddress,
    deriveStepRepoId: ({ runId, stepId }) => ({
      kind: "agent-state",
      id: `${runId}-${stepId}`,
    }),
  };
}

/**
 * Write a grant set as the `{ grants: WireGrantRule[] }` envelope the snapshot
 * validator accepts. No `runId`: one `agent-state` repo per step at
 * `STEP_GRANTS_PATH`, keyed by `deriveStepRepoId` -- the whole flat step-id
 * namespace, loop-body steps included; a failure rejects the deploy rather
 * than spawning a child that authorizes closed against an empty set. With
 * `runId`: one `runs/<runId>/grants.json` in the deployment's `workflow-run`
 * repo. Both use the same hub principal and `refs/heads/main` ref.
 */
async function writeStepGrants(args: {
  repoStore: RepoStore;
  anchorRunId: string;
  stepOrder: readonly string[];
  deriveStepRepoId: DeriveStepRepoId;
  grants: readonly unknown[] | undefined;
  runId?: string;
}): Promise<void> {
  // Coerce an absent array to `[]` so the on-disk envelope is always a valid
  // `{ grants: [] }`, never `{}` (which the snapshot's validator rejects).
  const grants = args.grants ?? [];
  const serialized = JSON.stringify({ grants }, null, 2);
  if (args.runId !== undefined) {
    await args.repoStore.writeTree(
      GRANTS_WRITE_PRINCIPAL,
      { kind: "workflow-run", id: args.anchorRunId },
      STEP_GRANTS_REF,
      {
        files: { [runGrantsPath(args.runId)]: serialized },
        message: `Write run grants for ${args.runId}`,
      },
    );
    return;
  }
  for (const stepId of args.stepOrder) {
    const repoId = args.deriveStepRepoId({
      runId: args.anchorRunId,
      stepId,
    });
    await args.repoStore.writeTree(
      GRANTS_WRITE_PRINCIPAL,
      repoId,
      STEP_GRANTS_REF,
      {
        files: { [STEP_GRANTS_PATH]: serialized },
        message: `Write step grants for ${stepId}`,
      },
    );
  }
}

export type AssembleRunCredentialsSnapshotOpts = {
  /** Substrate handle the sink reads the per-run grants file from. */
  repoStore: RepoStore;
  /** Anchor run id keying the workflow-run repo the grants file lives in. */
  anchorRunId: string;
  /** Run whose per-run grants file is read. */
  runId: string;
  /** The deployment's flat step-id namespace, loop-body ids included. */
  stepOrder: readonly string[];
  /** Per-step mail-address derivation. */
  deriveStepAddress: DeriveStepAddress;
};

/**
 * Resolve a run's credentials snapshot at the `onRunStart` grants barrier from
 * its per-run grants file. Every legitimate birth path writes the file before
 * dispatch (external/mail-triggered runs ship a `run.grants` frame; spawned
 * children inherit their parent's grants at spawn). A missing file is a run
 * that reached the barrier with no grants written -- it FAILS CLOSED rather
 * than running under-authorized. A malformed file also throws
 * (`readRunGrants`): its presence implies a grants frame was delivered, so a
 * structural failure is a boundary bug.
 */
export async function assembleRunCredentialsSnapshot(
  opts: AssembleRunCredentialsSnapshotOpts,
): Promise<CredentialsSnapshot> {
  const runGrants = await readRunGrants({
    repoStore: opts.repoStore,
    anchorRunId: opts.anchorRunId,
    runId: opts.runId,
  });
  if (runGrants === undefined) {
    throw new Error(
      `sidecar onRunStart: run ${opts.runId} has no grants file at ${runGrantsPath(opts.runId)}; refusing to start the run under-authorized`,
    );
  }
  const contentHash = await hashGrants(runGrants);
  const steps: CredentialsSnapshotStep[] = opts.stepOrder.map((stepId) => ({
    stepId,
    address: opts.deriveStepAddress({
      runId: opts.anchorRunId,
      stepId,
    }),
    grants: runGrants,
    contentHash,
  }));
  return { steps };
}

export type CreateSidecarWorkflowSupervisorOpts = {
  /** Sidecar's hub mail transport. */
  transport: HubTransport;
  /** Substrate-shaped RepoStore the workflow-host's supervisor reads from. */
  repoStore: RepoStore;
  /** Sidecar's 32-byte Ed25519 private key seed for principal signing. */
  signingKeySeed: Uint8Array;
  /** Workflow-run repo identity for the deployment. */
  workflowRunRepoId: RepoId;
  /** Workflow-run repo ref the supervisor commits events to. */
  workflowRunRef: string;
  /** Deployment id baked into principal claims and address derivation. */
  runId: string;
  /**
   * Decrypted credential material for the deployment's tools (from the deploy
   * frame), delivered to the child on the pre-trigger barrier. Absent when the
   * deployment binds no credentials.
   */
  credentialDelivery?: CredentialDelivery;
  /**
   * Step count of the deployed `WorkflowDefinition` (`stepOrder.length`).
   * Threaded into the child's spawn-time env so its deploy-tree read
   * collapses onto the head for a single-step deployment.
   */
  stepCount: number;
  /**
   * The deployment's flat step-id namespace: `stepOrder` plus the step ids of
   * every `loop` body. The `onRunStart` grants sink walks these to assemble
   * the per-run snapshot; a loop iteration inherits the parent run's env and
   * authorizes against this same snapshot.
   */
  stepOrder: readonly string[];
  /** Deployment's mail address. */
  deploymentMailAddress: string;
  /** Per-step mail-address derivation. */
  deriveStepAddress: DeriveStepAddress;
  /**
   * Optional override of the per-step `agent-state` repo identity the
   * supervisor reads grants from while assembling the credentialsSnapshot.
   * Defaults to `<runId>-<stepId>`; the single-step launched-agent deploy
   * supplies a derivation returning the legacy agent-state repo so the child
   * reads grants from the same repo the legacy agent identity keys.
   */
  deriveStepRepoId?: DeriveStepRepoId;
  /** Substrate-config keys propagated to the child via spawn-time env. */
  substrateEnv: Record<string, string>;
  /**
   * Dynamic spawn-env fragment recomputed on every spawn and recycle respawn
   * (e.g. a live-rotated inference-source list); its keys layer over
   * `substrateEnv`.
   */
  dynamicSpawnEnv: () => Record<string, string>;
  /**
   * Subprocess spawner. The booting process passes the Bun-backed
   * spawner; tests pass a deterministic mock.
   */
  subprocessSpawner: SubprocessSpawner;
  /** Path of the workflow-child binary the spawner launches. */
  binaryPath: string;
  /**
   * Optional per-message dispatch-timing observer, forwarded verbatim to the
   * supervisor's `onDispatchTiming` binding. Wired only for the Phase 4.7
   * latency gate, which needs the per-message round-trip from inside the
   * sidecar subprocess. Absent in production.
   */
  onDispatchTiming?: (mark: DispatchTimingMark) => void;
  /** D2 §10c forced-repack A/B toggle, forwarded to `repackEveryMessages`. */
  repackEveryMessages?: { everyMessages: number };
  /**
   * Consumed-dedup retention horizon (ms), forwarded to `consumedRetentionMs`.
   * The boot edge resolves `CONSUMED_RETENTION_MS`; absent, the supervisor
   * applies `DEFAULT_CONSUMED_RETENTION_MS` (24h).
   */
  consumedRetentionMs?: number;
  /**
   * Spawn ready-handshake timeout (ms), forwarded to `readyTimeoutMs`. The
   * boot edge resolves `CHILD_READY_TIMEOUT_MS`; absent, the supervisor
   * applies `DEFAULT_READY_TIMEOUT_MS` (30s).
   */
  readyTimeoutMs?: number;
  /**
   * Control-plane suspension sink forwarded to the supervisor's
   * `onSuspensionRegister` binding. The supervisor stamps `runId` +
   * `agentAddress` and invokes this when a workflow-process child reports a
   * `park.notify`; production wiring routes it to the hub link so a
   * `signal.correlation.register` frame reaches the hub. Absent means the
   * deployment registers no suspensions.
   */
  onSuspensionRegister?: (registration: SuspensionRegistration) => void;
  /**
   * Self-termination sink forwarded to the supervisor's `onSelfTerminate`
   * binding. The supervisor invokes it when it drives itself to a terminal
   * phase (crash-loop latch, channel crash, recycle failure); production
   * wiring routes it to the deploy router's address reclaim. Absent means a
   * self-termination is not surfaced to the host.
   */
  onSelfTerminate?: (info: {
    phase: "stopped" | "crash-looping";
    reason: string;
  }) => void;
  /**
   * Predicate consulted at the `onRunStart` grants barrier: returns `true` for
   * a runId whose `run.grants` write was attempted and FAILED, so the per-run
   * grants file the run needs never landed. The barrier fails such a run loudly
   * rather than starting it under-authorized. This names the known failed
   * write specifically, instead of the generic missing-file error the barrier
   * would otherwise produce. Absent means no run is poisoned.
   */
  isRunPoisoned?: (runId: string) => boolean;
};

export type SidecarWorkflowSupervisor = {
  supervisor: WorkflowSupervisor;
  /**
   * Hand a delivered inbound message off to the supervisor's mail
   * subscription. The returned promise resolves once the message is durably
   * accepted and rejects when it was not, so the hub-link can send a
   * `mail.inbound.ack` only on resolution (resolve = ack, reject = withhold).
   */
  routeInbound(message: Uint8Array): Promise<void>;
  /** Snapshot accessor that proxies the supervisor's credentials view. */
  getCredentialsSnapshot(): CredentialsSnapshot | null;
  /**
   * The per-run grants barrier the supervisor awaits before firing a run's
   * trigger. Rejects for a poisoned run (its `run.grants` write failed) and
   * otherwise resolves the run's credentials snapshot. Exposed so the barrier
   * can be exercised without driving a full spawn.
   */
  onRunStart(args: {
    runId: string;
    anchorRunId: string;
  }): Promise<CredentialsSnapshot>;
};

/**
 * Env key the multi-step branch uses to carry each step's ordered
 * inference-source failover chain from `frame.workflow.sources` down to the
 * workflow-process child. The substrate factory's `buildEnv` reads it at step
 * invocation; the supervisor passes the value through verbatim. Listed here so
 * the router and the substrate-factory consumer spell it the same way.
 */
export const STEP_INFERENCE_SOURCES_ENV_KEY = "STEP_INFERENCE_SOURCES";

/**
 * Spawn-env key carrying every spawned body's per-step inference-source pins
 * as a JSON `{ [definitionId]: { [stepId]: InferenceSource[] } }` map. The
 * sidecar decrypts the sealed body sources from the run record and serializes
 * the plaintext here so the run child resolves a body's sources without
 * holding the sidecar's cipher key. Mirrors `STEP_INFERENCE_SOURCES` for the
 * top level.
 */
export const WORKFLOW_BODY_SOURCES_ENV_KEY = "WORKFLOW_BODY_SOURCES";

/**
 * Upper bound on the serialized `WORKFLOW_BODY_SOURCES` env value. A single
 * env value cannot exceed the OS argument-string ceiling (`MAX_ARG_STRLEN`,
 * 128 KiB on Linux) or the child `execve` fails opaquely at spawn; the key
 * sums over every body at every depth, so the deploy path validates the size
 * here and rejects an over-large deployment at deploy time rather than at a
 * much-later spawn. Held below the ceiling with headroom for the rest of the
 * env block.
 */
const WORKFLOW_BODY_SOURCES_MAX_BYTES = 96 * 1024;

/**
 * Build the record's `bodySources` map from the deploy frame's referenced body
 * definitions (the flat set of onTrigger sections and childWorkflow children,
 * each already source-pinned by the hub). `undefined` when the deployment
 * references no bodies, so the record omits the field.
 */
function buildBodySourcesMap(
  referenced: NonNullable<
    AgentDeployFrame["workflow"]
  >["referencedDefinitions"],
): WorkflowRunRecord["bodySources"] {
  if (referenced === undefined || referenced.length === 0) {
    return undefined;
  }
  const map: NonNullable<WorkflowRunRecord["bodySources"]> = {};
  for (const ref of referenced) {
    map[ref.definition.id] = ref.sources;
  }
  return map;
}

/**
 * Validate the wire-projected workflow definition at the deploy-router
 * boundary. The arktype `AgentDeployFrame` validator enforces the wire shape;
 * this function takes `unknown`-typed inputs so it can also gate callers that
 * bypass the wire boundary, and enforces the invariants the router and the
 * downstream supervisor rely on: non-empty `id`, non-empty `stepOrder`
 * (a zero-step workflow has no semantics here), every `stepOrder` entry
 * matching `STEP_ID_PATTERN` (so per-step address derivation never needs
 * escaping), backed by a `steps[id]` entry (the child resolves each step's
 * primitive at run time), and a non-empty `sources[id]` failover chain (an
 * empty chain leaves the reactor with no initial source; rejected here rather
 * than deferred to a deep-stack child failure).
 *
 * A rejection surfaces as a thrown `Error` the link's deploy frame caller
 * converts into a structured failure reply.
 */
export function validateWorkflowProjection(projection: {
  definition: { id: unknown; stepOrder: unknown; steps: unknown };
  sources: unknown;
}): void {
  const def = projection.definition;
  if (typeof def.id !== "string" || def.id.length === 0) {
    throw new Error(
      "sidecar deploy router: workflow.definition.id must be a non-empty string",
    );
  }
  if (!Array.isArray(def.stepOrder) || def.stepOrder.length === 0) {
    throw new Error(
      "sidecar deploy router: workflow.definition.stepOrder must be a non-empty array",
    );
  }
  if (typeof def.steps !== "object" || def.steps === null) {
    throw new Error(
      "sidecar deploy router: workflow.definition.steps must be an object",
    );
  }
  if (typeof projection.sources !== "object" || projection.sources === null) {
    throw new Error(
      "sidecar deploy router: workflow.sources must be an object",
    );
  }
  const steps = def.steps;
  const sources = projection.sources;
  for (const stepId of def.stepOrder) {
    if (typeof stepId !== "string" || stepId.length === 0) {
      throw new Error(
        "sidecar deploy router: workflow.definition.stepOrder entries must be non-empty strings",
      );
    }
    if (!STEP_ID_PATTERN.test(stepId)) {
      throw new Error(
        `sidecar deploy router: stepId ${JSON.stringify(stepId)} must match ${STEP_ID_PATTERN.source}`,
      );
    }
    if (!Object.prototype.hasOwnProperty.call(steps, stepId)) {
      throw new Error(
        `sidecar deploy router: workflow.definition.steps is missing entry for stepId ${JSON.stringify(stepId)}`,
      );
    }
    if (!Object.prototype.hasOwnProperty.call(sources, stepId)) {
      throw new Error(
        `sidecar deploy router: workflow.sources is missing entry for stepId ${JSON.stringify(stepId)}`,
      );
    }
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- sources is checked to be a non-null object above; this reads a value to re-check its array shape
    const stepSources = (sources as Record<string, unknown>)[stepId];
    if (!Array.isArray(stepSources) || stepSources.length === 0) {
      throw new Error(
        `sidecar deploy router: workflow.sources[${JSON.stringify(stepId)}] must be a non-empty array (the step's ordered inference-source failover chain)`,
      );
    }
  }
}

/**
 * Derive the supervisor's principal public key from the sidecar's Ed25519
 * signing seed. The supervisor signs every workflow-run event with this key;
 * the multi-step branch surfaces it to the link so the hub records the
 * verifying key for the deployment's signed events.
 */
async function derivePrincipalPublicKeyHex(
  signingKeySeed: Uint8Array,
): Promise<string> {
  return hexEncode(await derivePublicKeyBytes(signingKeySeed));
}

/** Values the link folds into the outbound deploy ack. */
type DeployRouterResult = {
  publicKey: string;
};

/** What a handled workflow control reports back to the hub. */
type WorkflowControlOutcome = {
  refTips?: WorkflowRunRefTips;
};

type ResolvedInboundMailPolicy = Record<InboundMailOutcome, "reject" | "admit">;

/**
 * The link routes `agent.deploy`, `agent.undeploy`, and `workflow.control`
 * through `deploy`, `undeploy`, and `control`. The boot edge also calls
 * `restoreWorkflowRuns` once before connecting to the hub, reads
 * `activeAddresses` on each reconnect, and asks `reEmitParkedCorrelations`
 * to recover correlations dropped during an outage.
 */
export interface SidecarDeployRouter {
  deploy(frame: AgentDeployFrame): Promise<DeployRouterResult>;
  undeploy(frame: AgentUndeployFrame): Promise<void>;
  control(frame: WorkflowControlFrame): Promise<WorkflowControlOutcome>;
  /**
   * Re-establish every persisted workflow deployment on this sidecar's local
   * substrate. Runs once at boot, before `hubLink.connect()`, so a single-step
   * head's mailbox/transport registration is live before the hub routes to it.
   * Soft-fails per deployment: a record that cannot be restored is logged and
   * left on disk for a later boot to retry -- never deleted here.
   */
  restoreWorkflowRuns(): Promise<void>;
  /**
   * The workflow-substrate deployment addresses (`run_<hex>@domain`) this
   * router hosts a live supervisor for -- the set this sidecar can route mail
   * to. The boot edge announces them to the hub on (re)connect; they are
   * hub-minted and carry no per-address key, so the allocation-authenticated
   * announcement is what re-establishes their routes after a WS reconnect.
   */
  activeAddresses(): string[];
  /**
   * Re-register every correlation the deployment at `address` is parked on, by
   * asking its live supervisor to re-emit them to the hub. The boot edge calls
   * this per address the hub link re-routes on an authenticated reconnect, so
   * a register frame the hub dropped from its bounded send queue during the
   * outage is recovered. Fire-and-forget: the driver is best-effort and
   * watchdog-bounded inside the supervisor.
   */
  reEmitParkedCorrelations(address: string): void;
}

export function createSidecarDeployRouter<THost, TRegistries>(deps: {
  sessions: {
    initRepo(address: string): Promise<void>;
  };
  keyStore: {
    loadOrGenerateKey(
      address: string,
    ): Promise<{ keyPair: KeyPair; isNew: boolean }>;
    recordHubKey(address: string, hexHubPublicKey: string): void;
    forgetAgent(address: string): void;
  };
  /**
   * Cache of hub-vouched sender public keys. The grants handler writes each
   * co-delivered `senderIdentities` entry here before the run's grants land,
   * so a durable grant is never missing the key its recipient needs to verify
   * the sender's inbound mail.
   */
  senderKeyCache: {
    put(address: string, publicKey: Uint8Array): Promise<void>;
  };
  transport: HubTransport;
  repoStore: RepoStore;
  signingKeySeed: Uint8Array;
  /**
   * The sidecar cipher that seals credential material at rest. Used to seal
   * each run record's inference-source apiKeys on persist and unseal them on
   * the boot scan, so the durable record carries ciphertext rather than
   * plaintext secrets.
   */
  credentialCipher: CredentialCipher;
  /**
   * Per-agent crypto factory: takes the agent's raw key pair and returns a
   * `CryptoProvider` bound to it (production wires `createEd25519Crypto`). The
   * multi-step branch registers the spawned single-step agent's signing key on
   * the host transport before `spawn()`, the OUTBOUND half of mailbox ownership
   * (§3a); without it the spawned address has no provider on the transport and
   * an outbound send throws "address is not registered".
   */
  createAgentCrypto: (keyPair: KeyPair) => CryptoProvider;
  /**
   * Source-admission gate: throws if a step's pinned inference source names a
   * provider this sidecar cannot build. The buildable set is sidecar config
   * (the boot edge's adapter registry), so admission lives at the sidecar --
   * the hub cannot know a given sidecar's providers. Production wires the
   * default builder's `canBuildSource` verbatim. Distinct from the
   * orchestrator's operator-approval check: that gates on whether the operator
   * approved a `provider:model`; this gates on buildability at all.
   */
  assertSourceBuildable: (source: InferenceSource) => void;
  /**
   * Record a `(runId -> agentAddress)` mapping the boot edge's workflow-run
   * pack push facade consults when it must address an outbound pack frame.
   * Fires once per inbound `agent.deploy` frame before the deployment's
   * supervisor spawns, so the first pack push the child triggers sees the
   * mapping.
   */
  registerDeployment: (entry: { runId: string; agentAddress: string }) => void;
  /**
   * Symmetric removal hook for `registerDeployment`, fired from the link's
   * `agent.undeploy` path. A subsequent stale `writeTreePreservingPrefix`
   * against the dead deployment's workflow-run ref surfaces structurally
   * (`registry.resolve` returns `null`) rather than resolving to the prior
   * address.
   */
  unregisterDeployment: (entry: {
    runId: string;
    agentAddress: string;
  }) => void;
  /**
   * Read the tips of a stopped deployment's workflow-run refs and schedule a
   * push of any commit the hub has not acknowledged; the hub confirms the stop
   * only once it holds these tips.
   */
  reportDeploymentRefTips: (
    agentAddress: string,
  ) => Promise<WorkflowRunRefTips>;
  /**
   * Substrate-config env keys the multi-step branch propagates into the
   * workflow-process child's spawn-time env (see `SIDECAR_SUBSTRATE_CONFIG_KEYS`
   * in `substrate-factory.ts`), with `STEP_INFERENCE_SOURCES` merged on top per
   * multi-step frame. Defaults to an empty record so a router built without
   * substrate config needs no boot-edge threading.
   */
  multistepSubstrateEnv?: Record<string, string>;
  /**
   * Subprocess spawner the multi-step branch hands to the supervisor.
   * The booting process passes the Bun-backed spawner; tests pass a
   * deterministic mock.
   */
  multistepSubprocessSpawner: SubprocessSpawner;
  /**
   * Path of the workflow-child binary the spawner launches. Empty is
   * rejected at construction.
   */
  multistepBinaryPath: string;
  /**
   * Sink the supervisor invokes for every verified InferenceEvent the child
   * publishes. Threads the deployment's run address plus the deploy's session
   * id so a downstream fan-out can route each event to the right hub timeline
   * (`InferenceEvent` itself is sessionless). Defaults to a no-op.
   */
  publishWorkflowInferenceEvent?: (
    agentAddress: string,
    event: InferenceEvent,
    sessionId: string | undefined,
  ) => void;
  /**
   * Sink the supervisor invokes for every control-plane suspension a child
   * reports (`park.notify`); threaded into `onSuspensionRegister` so a parked
   * run's correlation registers at the hub (routing + approval rows). The
   * supervisor stamps `runId` + `agentAddress` first. Defaults to a no-op.
   */
  publishWorkflowSuspension?: (registration: {
    correlationId: string;
    runId: string;
    anchorRunId: string;
    agentAddress: string;
    kind: SignalKind;
    approvalSnapshot?: ApprovalSnapshot;
  }) => void;
  /**
   * Optional override for the multi-step branch's per-step mail-address
   * derivation. Defaults to `${runId}-${stepId}@<deploymentDomain>` derived
   * from the frame's run address.
   */
  multistepDeriveStepAddress?: DeriveStepAddress;
  /**
   * Per-deployment-address mail handler registry the hub-link's `mail.inbound`
   * path consults before the legacy session-routed delivery. The multi-step
   * branch registers `wired.routeInbound` against the deployment address once
   * spawn succeeds. Optional for tests without an end-to-end mail loop.
   */
  multistepMailRouter?: MultistepMailRouter;
  /**
   * Per-recipient-address registry of resolved inbound-mail admission policies
   * the hub-link's `mail.inbound` seam enforces. The multi-step branch
   * resolves the deployment's authored `inboundMailPolicy` into it once spawn
   * succeeds and removes the entry in the same teardown, so a reused address
   * never inherits a stale policy. Absent registry: inbound frames for the
   * address are rejected (fully-closed default) until wired. Optional for
   * tests without the hub-link seam.
   */
  inboundMailPolicyRegistry?: {
    register(address: string, policy: ResolvedInboundMailPolicy): void;
    unregister(address: string): void;
  };
  /**
   * Per-deployment-address signal handler registry the hub-link's
   * `signal.deliver` path consults. The multi-step branch registers
   * `wired.supervisor.deliverSignal` against the deployment address once spawn
   * succeeds; the child commits the resulting `SignalReceived` through its own
   * substrate, preserving the workflow-run repo's single-writer invariant.
   * Optional for tests without an end-to-end signal loop.
   */
  multistepSignalRouter?: MultistepSignalRouter;
  /**
   * Per-deployment-address drain handler registry the hub-link's `drain.deliver`
   * path consults. The multi-step branch registers `wired.supervisor.drain`
   * against the deployment address once spawn succeeds; the supervisor's
   * per-run `drainTimeout` accumulators commit a signed
   * `CancelRequested{origin: "supervisor-drain"}` when the deadline expires
   * (cancel-mode steps abort on the child side; wait-mode steps continue).
   * Optional for tests without an end-to-end drain loop.
   */
  multistepDrainRouter?: MultistepDrainRouter;
  /**
   * Per-deployment-address grants handler registry the hub-link's `run.grants`
   * path consults. The deploy router registers a handler against the
   * deployment address once spawn succeeds (single- and multi-step alike) so a
   * hub-side frame writes the run's grants to `runs/<runId>/grants.json` in
   * the workflow-run repo; `tryRoute` awaits the write, so the frame's FIFO
   * completion means the grants are durable. Optional for tests without an
   * end-to-end grants loop.
   */
  multistepGrantsRouter?: MultistepGrantsRouter;
  /**
   * Per-deployment-address sources-rotation handler registry. Only a
   * single-step warm deployment registers one (once spawn succeeds) so a
   * rotation flows into `wired.supervisor.deliverSources` and on to the
   * child's warm agent; a multi-step deployment has no single warm agent, so
   * `tryRoute` reports its address as unrouted. Optional for tests without a
   * rotation loop.
   */
  multistepSourcesRouter?: MultistepSourcesRouter;
  /**
   * Optional per-deployment credential-delivery handler registry. Every
   * deployment with a supervisor registers a handler after `spawn` (the
   * material cell is per-child and read by every step's tool capabilities); an
   * inbound `credentials.update` for a torn-down address is unrouted. Optional
   * for tests without a credential-delivery loop.
   */
  multistepCredentialsRouter?: MultistepCredentialsRouter;
  /**
   * Optional per-message dispatch-timing observer the multi-step branch
   * forwards to each supervisor it constructs; resolved at the sidecar boot
   * edge from the Phase 4.7 latency-gate env, absent in ordinary production.
   * The supervisor runs in this sidecar subprocess, so the observer sees both
   * ends of the IPC round-trip in one process.
   */
  onDispatchTiming?: (mark: DispatchTimingMark) => void;
  /** D2 §10c forced-repack A/B toggle forwarded to each supervisor. */
  repackEveryMessages?: { everyMessages: number };
  /**
   * Consumed-dedup retention horizon (ms) forwarded to every supervisor the
   * router constructs. The sidecar boot edge resolves the operator's
   * `CONSUMED_RETENTION_MS` config; absent, the supervisor applies
   * `DEFAULT_CONSUMED_RETENTION_MS` (24h).
   */
  consumedRetentionMs?: number;
  /**
   * Spawn ready-handshake timeout (ms) forwarded to every supervisor the
   * router constructs. The sidecar boot edge resolves the operator's
   * `CHILD_READY_TIMEOUT_MS` config; absent, the supervisor applies
   * `DEFAULT_READY_TIMEOUT_MS` (30s). A child that spawns but never signals
   * ready is killed and its spawn rejected rather than hanging the deploy or
   * boot-time restore.
   */
  readyTimeoutMs?: number;
  /**
   * Run-record writer, injectable so a test can block or fail the persist at a
   * controlled point -- the natural seam for exercising a recycle that
   * interleaves the source-rotation persist window. Defaults to the real
   * `writeWorkflowRunRecord`; production never overrides it.
   */
  writeWorkflowRunRecord?: typeof writeWorkflowRunRecord;
  /**
   * Materialize a source-ref deployment's frozen closure. The booting process
   * passes the real apply; a test passes a stub so the deploy/restore path can
   * run without a live registry.
   */
  applyFrozenWorkflowClosure: (args: {
    readonly source: SourceRefPin["source"];
    readonly closure: SourceRefPin["closure"];
    readonly instanceDir: string;
    readonly cacheRoot: string;
    readonly cacheMaxBytes: number;
    readonly registryMaxTarballBytes: number;
    readonly registries: TRegistries;
    readonly host: THost;
    readonly loadDefinition: (loadArgs: {
      readonly packageDir: string;
      readonly importCacheKey?: string;
    }) => Promise<WorkflowDefinition>;
    readonly assetRoot: string;
    readonly assetMounts: ReadonlyMap<string, string>;
    readonly gitDirs: ReadonlyMap<string, string>;
  }) => Promise<{
    readonly definition: WorkflowDefinition;
    readonly packageDir: string;
    readonly deployDir: string;
  }>;
  /** Registry table the closure apply selects optional dependencies against. */
  readRegistries: () => TRegistries;
  /** Platform pair the closure apply selects optional dependencies with. */
  resolveHostPlatform: () => THost;
  /**
   * Resolve a deployment's authored inbound-mail policy into the total
   * decision map stored beside the mail-router registration.
   */
  resolveInboundMailPolicy: (
    authored: InboundMailPolicy | undefined,
  ) => ResolvedInboundMailPolicy;
  /**
   * Check a frame's inline source assets out into the durable stores the
   * closure materializes from.
   */
  materializeWorkflowAssets: (args: {
    assets: readonly WorkflowSourceAssetMount[];
    closure: SourceRefPin["closure"];
    assetRoot: string;
    gitDirRoot: string;
    maxAssetPayloadBytes: number;
  }) => Promise<unknown>;
  /** Cap on the total inline source-asset payload of one frame. */
  maxInlineAssetPayloadBytes: number;
  /** Lossy address-to-repo-id substitution the workflow-run repo is keyed by. */
  deriveWorkflowRunRepoId: (agentAddress: string) => string;
  /**
   * Every step id in a frozen projection's flat step-id namespace. The
   * credentials snapshot and the grants bridge both walk this set.
   */
  inertFlatNamespaceStepIds: (args: {
    definition: WorkflowProjectionDefinition;
    context: string;
  }) => readonly string[];
  /** Workspace-relative mount path a tarball source asset is delivered under. */
  workflowSourceAssetMountPath: (assetId: string) => string;
  /** Absolute git directory a source-format asset is indexed into. */
  sourceAssetGitDir: (gitDirRoot: string, assetId: string) => string;
  /** Run id of an `<runId>@<domain>` address. */
  parseAgentId: (agentAddress: string) => string;
}): SidecarDeployRouter {
  // Validate the signing seed at construction so a malformed key fails
  // sidecar boot rather than the first multi-step deploy, where the public
  // key is derived from it (`derivePrincipalPublicKeyHex`). The seed also
  // signs every workflow-run event via the supervisor.
  if (deps.signingKeySeed.length !== 32) {
    throw new Error(
      `sidecar deploy router: Ed25519 signing seed must be 32 bytes, got ${deps.signingKeySeed.length}`,
    );
  }
  const publishInferenceEvent =
    deps.publishWorkflowInferenceEvent ??
    ((
      _address: string,
      _event: InferenceEvent,
      _sessionId: string | undefined,
    ): void => {
      /* no-op default: tests and production-without-a-publisher
         deployments do not consume events. */
    });
  const publishSuspension =
    deps.publishWorkflowSuspension ??
    ((_registration: {
      correlationId: string;
      runId: string;
      anchorRunId: string;
      agentAddress: string;
      kind: SignalKind;
      approvalSnapshot?: ApprovalSnapshot;
    }): void => {
      /* no-op default: tests and production-without-a-publisher
         deployments do not register suspensions. */
    });
  const multistepSubstrateEnv = deps.multistepSubstrateEnv ?? {};
  // Sidecar data dir the deployment's per-step scratch roots under
  // (`<dataDir>/workflow-step-state/<runId>/...`), resolved once so the
  // undeploy hook can reclaim the whole subtree. Absent only when the router
  // is wired without substrate config (a test that never spawns a child), so
  // the undeploy reclaim is correctly skipped.
  const stepStateDataDir = multistepSubstrateEnv.SIDECAR_DATA_DIR;
  if (deps.multistepBinaryPath.length === 0) {
    throw new Error(
      "sidecar deploy router: multistepBinaryPath must name the workflow child binary",
    );
  }
  const persistWorkflowRunRecord =
    deps.writeWorkflowRunRecord ?? writeWorkflowRunRecord;
  const applyClosure = deps.applyFrozenWorkflowClosure;
  const multistepSpawner = deps.multistepSubprocessSpawner;
  const multistepDeriveStepAddress: DeriveStepAddress =
    deps.multistepDeriveStepAddress ??
    (({ runId, stepId }) => `${runId}-${stepId}`);

  // Per-deployment supervisor tracking. The multi-step branch constructs one
  // `SidecarWorkflowSupervisor` per `agent.deploy` frame; the supervisor owns
  // the workflow-process child, its IPC pipes, and its event-channel fd. The
  // undeploy hook consults this map to call `supervisor.shutdown()` so the
  // child's lifetime ends with the deployment.
  const activeSupervisors = new Map<string, SidecarWorkflowSupervisor>();
  const workflowCancellationTasks = new WeakMap<
    SidecarWorkflowSupervisor,
    Promise<void>
  >();
  const workflowStopTasks = new Map<string, Promise<void>>();

  // Read after the supervisor has shut down, so the tips cover every run
  // event and consumption record the stopped worker committed; later inbox
  // enqueues, grants writes, and mailbox replays record no outcome.
  async function reportControlOutcome(
    frame: WorkflowControlFrame,
  ): Promise<WorkflowControlOutcome> {
    return frame.action === "stop"
      ? { refTips: await deps.reportDeploymentRefTips(frame.agentAddress) }
      : {};
  }

  // Synchronous single-flight guard for the deploy path. `deployMultiStep`
  // cannot reserve its `activeSupervisors` slot up front (the supervisor does
  // not exist until inside `spawnWorkflowRun`), so it records the address here
  // synchronously, before its first await, and clears it in a finally once the
  // deploy settles. `activeSupervisors` fills only after `spawn` succeeds, so
  // the has-check alone would let two same-address frames both pass and the
  // loser's unwind delete the winner's live run record; this set closes that
  // window by rejecting a second frame mid-deploy before it touches any
  // durable state. Only the live deploy path reserves; boot restore is serial
  // and relies on the `activeSupervisors` backstop instead.
  const reservingDeployAddresses = new Set<string>();

  // Slug-collision tracking. `deriveWorkflowRunRepoId` substitutes disallowed
  // characters with `-`, deterministic but lossy: two distinct run addresses
  // can collapse to the same slug, and a collision would let the second
  // deploy silently overwrite the first's workflow-run repo state (the slug IS
  // the repoId). The map records the first-claimer; a later deploy producing
  // the same slug from a different address is rejected before any supervisor
  // or repo state is touched.
  const slugClaims = new Map<string, string>();

  function claimSlug(runId: string, agentAddress: string): void {
    const existing = slugClaims.get(runId);
    if (existing !== undefined && existing !== agentAddress) {
      throw new Error(
        `workflow-run repo id collision: run addresses ${JSON.stringify(existing)} and ${JSON.stringify(agentAddress)} both project to runId ${JSON.stringify(runId)}`,
      );
    }
    // A same-address re-claim is a defensive no-op: a live re-deploy is
    // rejected by the `activeSupervisors` guard first, and a failed or
    // undeployed deploy releases the slug, so `existing` is only ever
    // undefined or a different address here.
    slugClaims.set(runId, agentAddress);
  }

  function releaseSlug(runId: string, agentAddress: string): void {
    const existing = slugClaims.get(runId);
    if (existing === agentAddress) slugClaims.delete(runId);
  }

  function unregisterWorkflowRoutes(agentAddress: string): void {
    deps.multistepMailRouter?.unregister(agentAddress);
    deps.inboundMailPolicyRegistry?.unregister(agentAddress);
    deps.multistepSignalRouter?.unregister(agentAddress);
    deps.multistepDrainRouter?.unregister(agentAddress);
    deps.multistepGrantsRouter?.unregister(agentAddress);
    deps.multistepSourcesRouter?.unregister(agentAddress);
    deps.multistepCredentialsRouter?.unregister(agentAddress);
  }

  // Reclaim a deployment address whose supervisor drove ITSELF to a terminal
  // phase (crash-loop latch, channel crash, recycle failure) without an
  // operator undeploy, so a redeploy succeeds without a manual undeploy: the
  // `has`-guarded `activeSupervisors` entry is the redeploy gate and
  // `transport.register` throws on a double-register, so both are released
  // here, along with the routers and slug so a frame racing the reclaim is
  // rejected at the boundary.
  //
  // Three deliberate invariants:
  //   - Never calls back into `wired.supervisor.*`: it runs as the
  //     supervisor's own `onSelfTerminate` sink, re-entrantly during terminal
  //     teardown; a call back in would re-enter that teardown. The supervisor
  //     has already terminated, so there is nothing to shut down.
  //   - Does NOT delete the durable run record or on-disk state (unlike the
  //     `undeploy` hook): a self-termination is not a hub-initiated undeploy --
  //     the hub still believes the address is deployed -- so leaving the record
  //     lets a boot restore re-spawn the address. The reclaim only drops
  //     in-memory routing state.
  //   - Does NOT call `unregisterDeployment`, so the deployment-address mapping
  //     is RETAINED. Only the OUTBOUND pack push (`registry.resolve`) consults
  //     it, and the supervisor itself drives that; the crash-loop latch
  //     commits its `RunFailed` tombstone, resolving the mapping, before this
  //     sink fires, and a stale entry never blocks a redeploy.
  //
  // Fully synchronous between the guard and the mutations, so it is idempotent
  // and cannot interleave with a concurrent operator `undeploy`.
  function reclaimSelfTerminatedSupervisor(args: {
    runId: string;
    agentAddress: string;
  }): void {
    if (!activeSupervisors.has(args.agentAddress)) return;
    // Drop racing frames at the router boundary first, then unwind the
    // underlying registrations -- the same ordering the undeploy hook uses.
    unregisterWorkflowRoutes(args.agentAddress);
    activeSupervisors.delete(args.agentAddress);
    deps.transport.unregister(args.agentAddress);
    releaseSlug(args.runId, args.agentAddress);
  }

  /**
   * Remove the legacy `${dataDir}/assets/workflow/<bodyRef>/` staging a
   * pre-sealed-record deploy wrote for a body (a world-readable plaintext
   * `sources.json`, INTR-310). Body sources now ride sealed in the run record
   * and reach the child through the spawn env, so the staging is retired; a
   * redeploy of an address that predates the change reclaims its orphaned
   * file here. A never-staged body is a no-op (`force`).
   */
  async function sweepLegacyBodySources(
    sidecarDataDir: string | undefined,
    definitionId: string,
  ): Promise<void> {
    if (typeof sidecarDataDir !== "string" || sidecarDataDir.length === 0) {
      throw new Error(
        "sidecar deploy router: SIDECAR_DATA_DIR must be present in the multi-step substrate env; the legacy body-source staging is reclaimed against this data dir",
      );
    }
    const bodyAssetDir = pathJoin(
      sidecarDataDir,
      "assets",
      "workflow",
      definitionId,
    );
    await rm(bodyAssetDir, { recursive: true, force: true });
  }

  /**
   * The per-deployment inputs the shared spawn core needs to stand up a
   * workflow deployment, independent of the live deploy frame. The live deploy
   * path builds this from `frame`/`projection`; a boot-time restore path
   * builds the same shape from the persisted run record.
   */
  interface WorkflowDeploySpec {
    agentAddress: string;
    /**
     * The runnable definition, projected to its inert wire shape. Source-ref
     * is the only deploy lineage, so this is always the closure evaluation
     * (`projectLiveToInert(applied.definition)`), never a frame-carried inline
     * definition. The deploy path and boot-time restore both derive it from
     * the materialized closure.
     */
    definition: WorkflowProjectionDefinition;
    sources: NonNullable<AgentDeployFrame["workflow"]>["sources"];
    /**
     * Per spawned-body inference-source pins, keyed by the body's definition
     * id (the flat set of onTrigger sections and childWorkflow children).
     * Persisted in the record and delivered to the run child as plaintext
     * through the spawn env, so the child resolves a body's sources without
     * the sidecar's cipher key. `undefined` when the deployment spawns no
     * bodies.
     */
    bodySources: WorkflowRunRecord["bodySources"];
    /**
     * The run's unified credential-material cell (inference + tool secrets,
     * plus tool bindings). Persisted sealed in the record and delivered to the
     * child on the pre-trigger barrier; the child resolves its inference
     * sources and tools from it by `credentialId`. `undefined` when the
     * deployment binds no credentials.
     */
    credentials: CredentialDelivery | undefined;
    /**
     * The hub-approved wire hash the deploy frame carried. The child's
     * `DEFINITION_HASH` is sourced from this hub authority, NOT a sidecar
     * recompute; the shared spawn core fails closed rather than substitute one.
     */
    approvedWireHash: string | undefined;
    /** Correlates the child's inference events to the deploy's session. */
    sessionId: string | undefined;
    /**
     * Hub public key recorded at the head for deploy-pack and inbound
     * hub-frame verification. Required for a single-step deployment (whose
     * head IS the agent identity); undefined for a multi-step deployment,
     * which derives per-step addresses and records no head key.
     */
    hubPublicKey: string | undefined;
    /**
     * Sidecar-local directory of the materialized closure, threaded into the
     * child's spawn env so the run child evaluates the pinned code to a live
     * definition. Never travels on the hub frame and is not persisted to the
     * record. Always present -- source-ref is the only deploy lineage.
     */
    closurePackageDir: string;
    /**
     * The source-ref pin, persisted by `buildWorkflowRunRecord` so a boot-time
     * restore can re-run `applyFrozenWorkflowClosure` to re-materialize the
     * pinned code. Its `source` carries no secret (the registry token resolves
     * from env at apply time); its `closure` is frozen versions + SRIs.
     */
    sourceRef: NonNullable<AgentDeployFrame["workflow"]>["sourceRef"];
  }

  /**
   * Build the durable run record from a spec and a source table. The table is
   * a parameter (not `spec.sources`) so the rotation handler can write
   * live-rotated sources through the same shape a boot-time restore reseeds
   * from.
   */
  function buildWorkflowRunRecord(
    spec: WorkflowDeploySpec,
    sources: WorkflowRunRecord["sources"],
  ): WorkflowRunRecord {
    // A source-ref record -- the only lineage -- must carry the hash; a spec
    // missing it is a wiring defect. Fail loudly here rather than persist a
    // record the boot scan would reject as corrupt.
    if (spec.approvedWireHash === undefined) {
      throw new Error(
        `buildWorkflowRunRecord: a source-ref deployment (${spec.agentAddress}) must carry approvedWireHash`,
      );
    }
    return {
      version: 2 as const,
      agentAddress: spec.agentAddress,
      definitionId: spec.definition.id,
      sources,
      ...(spec.bodySources !== undefined
        ? { bodySources: spec.bodySources }
        : {}),
      ...(spec.credentials !== undefined
        ? { credentials: spec.credentials }
        : {}),
      ...(spec.sessionId !== undefined ? { sessionId: spec.sessionId } : {}),
      ...(spec.hubPublicKey !== undefined
        ? { hubPublicKey: spec.hubPublicKey }
        : {}),
      lineage: "source-ref",
      // Feeds the restored child's DEFINITION_HASH so it re-verifies the
      // evaluated closure against the hub-approved pin, and the source-ref pin a
      // restore re-runs applyFrozenWorkflowClosure with.
      approvedWireHash: spec.approvedWireHash,
      sourceRef: spec.sourceRef,
    };
  }

  /**
   * Materialize a source-ref deployment's frozen closure to its per-deployment
   * instance dir and return the applied result. Owns the plumbing the deploy
   * path and the boot-time restore path share: the deterministic instance dir
   * under `<dataDir>/workflow-definition-closures/<deploymentId>`, the
   * content-addressed cache root, the two substrate byte caps, and the
   * registry table. Callers consume `applied.definition`/`packageDir`.
   *
   * The instance dir is force-reclaimed before the apply: `deploymentId` is
   * deterministic per address, so a redeploy or boot-restore reuses the same
   * dir, and a prior soft-failed deploy or dead process can leave it
   * half-materialized. Safe because no live reader holds the dir when this
   * runs -- a precondition each caller establishes at its call site.
   */
  async function materializeDeploymentClosure(
    dataDir: string,
    deploymentId: string,
    pin: SourceRefPin,
  ) {
    const instanceDir = pathJoin(
      dataDir,
      "workflow-definition-closures",
      deploymentId,
    );
    await rm(instanceDir, { recursive: true, force: true });

    // Tarball `kind:"asset"` entries read from the durable plain-file store; a
    // source-format entry checks its subtree out of the durable indexed git
    // store. Deriving and asserting both from the pin alone is what makes this
    // symmetric on deploy and restore.
    const { assetRoot, assetMounts, gitDirs } =
      await resolveDeploymentAssetMounts(
        dataDir,
        deploymentId,
        pin,
        deps.workflowSourceAssetMountPath,
        deps.sourceAssetGitDir,
      );

    return applyClosure({
      source: pin.source,
      closure: pin.closure,
      instanceDir,
      cacheRoot: pathJoin(dataDir, "workflow-definition-closure-cache"),
      cacheMaxBytes: requireSubstrateByteCap(
        multistepSubstrateEnv,
        "SIDECAR_CACHE_MAX_BYTES",
      ),
      registryMaxTarballBytes: requireSubstrateByteCap(
        multistepSubstrateEnv,
        "SIDECAR_REGISTRY_MAX_TARBALL_BYTES",
      ),
      registries: deps.readRegistries(),
      host: deps.resolveHostPlatform(),
      loadDefinition: loadWorkflowDefinitionFromClosure,
      assetRoot,
      assetMounts,
      gitDirs,
    });
  }

  /**
   * Single owner of the workflow-deployment spawn sequence: construct the
   * supervisor, register the single-step agent's outbound key + head repo +
   * hub key, spawn the workflow-process child, then register the live
   * deployment (supervisor, mail/signal/drain routers, address mapping). A
   * `try/finally` unwinds every piece of partial state on a throw, so a failed
   * spawn leaks nothing. Both the live deploy path and the boot-time restore
   * path route through here so the two can never diverge on how a deployment
   * is stood up. Callers materialize the deploy-only durable state (the source
   * closure, step grants) before calling.
   */
  async function spawnWorkflowRun(
    spec: WorkflowDeploySpec,
  ): Promise<DeployRouterResult> {
    // The run's credential material rides on `spec.credentials`: the sidecar
    // seals it into the record, so both the live deploy and the boot-restore
    // path carry it here (restore unseals it from the record), and it is
    // delivered to the child on the pre-trigger barrier.
    const credentialDelivery = spec.credentials;
    // Fail loud if this address already has a live supervisor: the `has()`
    // check is the primary early guard, giving a clean error before the
    // `transport.register` duplicate-throw backstop and before the
    // `activeSupervisors.set` below could clobber the running deployment's
    // handle. Both the deploy path and boot restore route through here, so
    // this is the single transition guard against a double-spawn -- notably a
    // boot restore racing a legacy restore for the same address.
    if (activeSupervisors.has(spec.agentAddress)) {
      throw new Error(
        `sidecar deploy router: a supervisor is already active for ${spec.agentAddress}; refusing to spawn a second`,
      );
    }
    const runId = deps.deriveWorkflowRunRepoId(spec.agentAddress);

    // Single-step launched-agent deploy vs. derived multi-step deploy: one
    // step keeps the deployment's own (legacy) mail address and grants repo
    // keyed by the legacy instance id; more derive `<runId>-<stepId>` per step
    // for both the mail address and the agent-state repo id.
    const stepStrategy = createStepStrategy({
      legacyAddress: spec.agentAddress,
      stepOrder: spec.definition.stepOrder,
      multistepDeriveStepAddress,
      parseAgentId: deps.parseAgentId,
    });

    // Every step id the deployment's credentials snapshot must cover. NOT
    // `stepOrder`: a `loop` body runs in-process as a child run inheriting the
    // parent's env, so a body step authorizes against the SAME snapshot as the
    // top-level steps, keyed by its own plain step id; a snapshot built from
    // `stepOrder` alone carries no entry for it and the child's authorize
    // throws on the body's first tool call. The address/repo strategy stays on
    // `stepOrder`: the head/step collapse is a property of the deployment's
    // own step count, not of what a body can run.
    const credentialStepIds = deps.inertFlatNamespaceStepIds({
      definition: spec.definition,
      context: "sidecar deploy router credentials snapshot: ",
    });

    // Unwind every piece of spawn state if any step in this block throws, so a
    // failed spawn leaks no freshly-spawned workflow-process child,
    // `activeSupervisors` entry, transport registration, or multistep router
    // registration (the deployment-address registration happens before spawn
    // and is unwound by its own guard). Ordering in the finally is the reverse
    // of the success-path registration order. The caller owns the deployment
    // slug -- claimed before any durable write, released on failure -- so it
    // is not touched here.
    let succeeded = false;
    let wiredForUnwind: SidecarWorkflowSupervisor | undefined;
    let supervisorRegistered = false;
    let routersRegistered = false;
    let agentTransportRegistered = false;
    let hubKeyRecorded = false;
    let deploymentRegistered = false;
    try {
      // The child's `DEFINITION_HASH` is the HUB-APPROVED wire hash the deploy
      // frame carried -- the hub is the authority, so the child re-verifies its
      // own recompute against it. Both feeds into this core carry it: the
      // production deploy builder always stamps it, and boot restore
      // re-attaches it from the persisted record. A missing hash is a wiring
      // bug, not a legacy case: a sidecar recompute would collapse the child's
      // re-verify into a self-check that gives false assurance. Fail loud
      // instead.
      if (spec.approvedWireHash === undefined) {
        throw new Error(
          `workflow deploy spawn (${spec.agentAddress}): the deploy spec carries no approvedWireHash. The hub deploy builder must stamp the hub-approved wire hash and a restore must re-attach it from the persisted record; the sidecar will not recompute it, which would collapse the child's re-verify to a self-check.`,
        );
      }
      const definitionHash = spec.approvedWireHash;

      // Spawned bodies' plaintext inference sources, serialized once for the
      // deployment's lifetime (bodies do not live-rotate, unlike the top-level
      // sources below). The child looks each body up by definition id and never
      // holds the sidecar's cipher key. Size-guarded at deploy against the OS
      // argument-string ceiling so an over-large deployment fails loudly
      // rather than at a later child `execve`.
      const bodySourcesEnv = JSON.stringify(spec.bodySources ?? {});
      const bodySourcesBytes = Buffer.byteLength(bodySourcesEnv, "utf8");
      if (bodySourcesBytes > WORKFLOW_BODY_SOURCES_MAX_BYTES) {
        throw new Error(
          `sidecar deploy router: serialized ${WORKFLOW_BODY_SOURCES_ENV_KEY} is ${String(bodySourcesBytes)} bytes, over the ${String(WORKFLOW_BODY_SOURCES_MAX_BYTES)}-byte spawn-env limit; the deployment has too many spawned-body sources to deliver through the child env`,
        );
      }

      const substrateEnv: Record<string, string> = {
        ...multistepSubstrateEnv,
        WORKFLOW_RUN_REPO_ID: runId,
        WORKFLOW_RUN_REF: "refs/heads/main",
        // Thread the materialized closure's sidecar-local package dir so the
        // run child evaluates the pinned code to a live definition and
        // re-verifies by project-then-hash against `DEFINITION_HASH`. Fixed
        // for the deployment's lifetime, so it rides the frozen substrate env.
        CLOSURE_PACKAGE_DIR: spec.closurePackageDir,
        [WORKFLOW_BODY_SOURCES_ENV_KEY]: bodySourcesEnv,
      };
      // Live-rotatable per-step inference sources, seeded from the deploy spec
      // and revised in place by the single-step sources-rotation handler below.
      // `STEP_INFERENCE_SOURCES` is NOT in the frozen `substrateEnv`: it is
      // recomputed on every spawn and recycle respawn via `dynamicSpawnEnv`,
      // so a rotation survives a recycle instead of reverting to the
      // deploy-time list.
      let currentSources = spec.sources;

      // RunIds whose `run.grants` write was attempted and failed. The grants
      // handler records a runId here on a write failure; the grants barrier
      // reads it through `isRunPoisoned` and fails the run rather than starting
      // it under the empty deploy-time grant set. Scoped to this deployment's
      // supervisor, so a run that never had a `run.grants` frame is never
      // added and internal runs inherit the deployment's grants normally.
      const poisonedRunIds = new Set<string>();

      const wired = createSidecarWorkflowSupervisor({
        transport: deps.transport,
        repoStore: deps.repoStore,
        signingKeySeed: deps.signingKeySeed,
        workflowRunRepoId: {
          kind: "workflow-run",
          id: runId,
        },
        workflowRunRef: "refs/heads/main",
        runId,
        stepCount: spec.definition.stepOrder.length,
        stepOrder: credentialStepIds,
        deploymentMailAddress: spec.agentAddress,
        ...(credentialDelivery !== undefined ? { credentialDelivery } : {}),
        deriveStepAddress: stepStrategy.deriveStepAddress,
        deriveStepRepoId: stepStrategy.deriveStepRepoId,
        isRunPoisoned: (runId) => poisonedRunIds.has(runId),
        // The supervisor stamps `runId` + `agentAddress` first; forward the
        // fully-stamped registration so a `signal.correlation.register` frame
        // reaches the hub for the parked run.
        onSuspensionRegister: publishSuspension,
        // Drop the supervisor from `activeSupervisors` when it self-terminates
        // (crash-loop latch, channel crash, recycle failure) so the address is
        // redeployable without a manual undeploy.
        onSelfTerminate: () =>
          reclaimSelfTerminatedSupervisor({
            runId,
            agentAddress: spec.agentAddress,
          }),
        substrateEnv,
        // Recomputed on every spawn AND recycle respawn so a respawn carries
        // the current (possibly rotated) list, not the frozen deploy-time one.
        dynamicSpawnEnv: () => ({
          [STEP_INFERENCE_SOURCES_ENV_KEY]: JSON.stringify(currentSources),
        }),
        subprocessSpawner: multistepSpawner,
        binaryPath: deps.multistepBinaryPath,
        ...(deps.onDispatchTiming !== undefined
          ? { onDispatchTiming: deps.onDispatchTiming }
          : {}),
        ...(deps.repackEveryMessages !== undefined
          ? { repackEveryMessages: deps.repackEveryMessages }
          : {}),
        ...(deps.consumedRetentionMs !== undefined
          ? { consumedRetentionMs: deps.consumedRetentionMs }
          : {}),
        ...(deps.readyTimeoutMs !== undefined
          ? { readyTimeoutMs: deps.readyTimeoutMs }
          : {}),
      });

      // OUTBOUND half of mailbox ownership (§3a): register the deployment
      // mail address's signing key on the host transport before `spawn()`.
      // Every step signs outbound sends as `spec.agentAddress`, so the
      // transport must hold a `CryptoProvider` for it or `send` throws
      // "not registered".
      const { keyPair } = await deps.keyStore.loadOrGenerateKey(
        spec.agentAddress,
      );
      deps.transport.register(
        spec.agentAddress,
        deps.createAgentCrypto(keyPair),
      );
      agentTransportRegistered = true;

      // The ack surfaces the deployment address's Ed25519 public key (minted
      // above) so the Hub can publish the same identity for every deployment.
      const deploymentPublicKey = hexEncode(keyPair.publicKey);
      if (spec.definition.stepOrder.length === 1) {
        // Initialize the head's deploy-tree repo (idempotent) so the hub's
        // deploy-pack push has a repo to apply into. `initRepo`, not
        // `provisionAgent`: the child mints its own keypair and persists no
        // hub-agent config.
        await deps.sessions.initRepo(spec.agentAddress);

        // Record the hub's public key so the deploy-pack apply (and any
        // inbound hub-signed frame) can verify against it.
        if (spec.hubPublicKey === undefined) {
          throw new Error(
            "sidecar deploy router: a single-step workflow deployment requires a hubPublicKey to record at the head; none was supplied",
          );
        }
        deps.keyStore.recordHubKey(spec.agentAddress, spec.hubPublicKey);
        hubKeyRecorded = true;
      }

      // Warm-keep is the single-step launched-agent deploy (the sole step IS
      // the long-lived agent); the signal is carried down through the spawn
      // env.
      const warmKeep = spec.definition.stepOrder.length === 1;
      const spawnOpts: SpawnOpts = {
        stepOrder: [...credentialStepIds],
        definitionHash,
        warmKeep,
        onInferenceEvent: (event) => {
          // Re-narrow the HMAC-verified event to the hub's `InferenceEvent`
          // union; a parse failure is upstream corruption, so drop it loudly.
          const validated = parseInferenceEvent(event);
          if (validated instanceof type.errors) {
            logger.warn`dropping workflow inference event for ${spec.agentAddress}: ${validated.summary}`;
            return;
          }
          publishInferenceEvent(spec.agentAddress, validated, spec.sessionId);
        },
      };

      // Record the deployment-address mapping BEFORE `spawn`: the spawn's
      // `replayProcessingToInbox` writes through the pack-pushing facade,
      // which resolves this mapping; recording after `spawn` loses the race
      // and the replay's write throws "no run address registered". The finally
      // unwinds it on any failure.
      deps.registerDeployment({
        runId,
        agentAddress: spec.agentAddress,
      });
      deploymentRegistered = true;

      // A spawn-time rejection surfaces structurally and leaves the registry
      // untouched: the supervisor is registered only after spawn succeeds.
      await wired.supervisor.spawn(spawnOpts);
      wiredForUnwind = wired;
      activeSupervisors.set(spec.agentAddress, wired);
      supervisorRegistered = true;

      // Bind the deployment's mail address to the supervisor's `routeInbound`
      // so the hub-link dispatches inbound mail into its mail-bus
      // subscription.
      deps.multistepMailRouter?.register(spec.agentAddress, (message) =>
        wired.routeInbound(message),
      );
      // Resolve the authored inbound-mail policy ONCE, beside the mail-router
      // registration, so the hub-link seam has it before any inbound frame
      // routes.
      deps.inboundMailPolicyRegistry?.register(
        spec.agentAddress,
        deps.resolveInboundMailPolicy(spec.definition.inboundMailPolicy),
      );
      // Register the signal-delivery handler so a hub `signal.deliver` frame
      // dispatches through the supervisor's `deliverSignal`.
      deps.multistepSignalRouter?.register(spec.agentAddress, async (args) => {
        await wired.supervisor.deliverSignal({
          runId: args.runId,
          signalName: args.signalName,
          signalId: args.signalId,
          payload: args.payload,
        });
      });
      // Register the drain handler so a hub `drain.deliver` frame dispatches
      // through the supervisor's `drain`.
      deps.multistepDrainRouter?.register(spec.agentAddress, async (args) => {
        await wired.supervisor.drain({ deadlineMs: args.deadlineMs });
      });
      // Register the grants handler so a hub `run.grants` frame writes the
      // run's grants to `runs/<runId>/grants.json` in the workflow-run repo.
      deps.multistepGrantsRouter?.register(spec.agentAddress, async (args) => {
        try {
          // Cache the sender keys BEFORE the grants file lands, so "grant
          // durable" implies "key durable": a cache fault falls into the catch
          // below and poisons the run. A malformed key is a hub-side defect:
          // skip it, log at ERROR, and cache the valid ones rather than wedge
          // the run on every replay.
          for (const identity of args.senderIdentities ?? []) {
            let publicKey: Uint8Array;
            try {
              publicKey = hexDecode(identity.publicKey);
            } catch (cause) {
              const message =
                cause instanceof Error ? cause.message : String(cause);
              logger.error`Skipping unparseable sender key for ${identity.address} on run ${args.runId}: ${message}`;
              continue;
            }
            if (publicKey.length !== ED25519_PUBLIC_KEY_BYTES) {
              logger.error`Skipping wrong-length sender key for ${identity.address} on run ${args.runId}: got ${String(publicKey.length)} bytes`;
              continue;
            }
            await deps.senderKeyCache.put(identity.address, publicKey);
          }
          await writeStepGrants({
            repoStore: deps.repoStore,
            anchorRunId: runId,
            stepOrder: spec.definition.stepOrder,
            deriveStepRepoId: stepStrategy.deriveStepRepoId,
            grants: args.stepGrants,
            runId: args.runId,
          });
        } catch (cause) {
          // A sender-key or grants write did not land: poison the runId so the
          // barrier fails the run, then re-throw so the hub-link logs the
          // failure loudly.
          poisonedRunIds.add(args.runId);
          throw cause;
        }
        // Refresh a live child so a standing approval resolved mid-run lowers
        // its floor immediately. Best-effort and non-fatal: the durable file
        // governs the next barrier/respawn.
        await wired.supervisor.deliverGrants(args.runId);
      });
      // Register the sources-rotation handler ONLY for a single-step warm
      // deployment: only it has one long-lived agent whose sources can be
      // swapped in place.
      if (warmKeep) {
        // A single-step source table has exactly one entry, keyed by the head
        // step; derive that key once here.
        const rotationStepId = spec.definition.stepOrder[0];
        if (rotationStepId === undefined) {
          throw new Error(
            "single-step deploy has no step id for sources rotation",
          );
        }
        deps.multistepSourcesRouter?.register(
          spec.agentAddress,
          async (args) => {
            const rotated = { [rotationStepId]: args.sources };
            // Swap `currentSources` synchronously BEFORE the durable persist:
            // it is the process-local respawn hint, so a recycle interleaving
            // the persist must respawn on the SAME sources being persisted;
            // swapping first leaves only child-ahead-of-durable on a failed
            // persist, which the next recycle heals.
            const prevSources = currentSources;
            currentSources = rotated;
            // The durable write still precedes the LIVE swap, so
            // persist-before-externally-visible holds for state that outlives
            // the process. On a failed persist, roll the hint back so
            // `currentSources` and the record stay in agreement -- the
            // invariant restart consistency depends on. Skipped when no data
            // dir was wired (a test router that never persists).
            if (stepStateDataDir !== undefined) {
              try {
                await persistWorkflowRunRecord(
                  stepStateDataDir,
                  runId,
                  buildWorkflowRunRecord(spec, rotated),
                  deps.credentialCipher,
                );
              } catch (cause) {
                // Safe to roll back unconditionally: the sidecar's
                // per-connection inbound-frame queue serializes handler runs,
                // so no second rotation is in flight whose committed table
                // this could clobber.
                currentSources = prevSources;
                throw cause;
              }
            }
            await wired.supervisor.deliverSources({
              sources: args.sources,
              defaultSource: args.defaultSource,
            });
          },
        );
      }

      // Register the credential-delivery handler for EVERY deployment: the
      // material cell is per-child and read by every step's tool capabilities.
      // No durable persist -- credential material never touches disk.
      deps.multistepCredentialsRouter?.register(
        spec.agentAddress,
        async (args) => {
          await wired.supervisor.deliverCredentials({
            delivery: args.delivery,
            ...(args.revoke !== undefined ? { revoke: args.revoke } : {}),
          });
        },
      );
      routersRegistered = true;

      succeeded = true;
      return { publicKey: deploymentPublicKey };
    } finally {
      if (!succeeded) {
        // Unwind in reverse registration order so each step undoes state the
        // success path confirmed; ordering matches the `undeploy` hook.
        if (routersRegistered) {
          deps.multistepMailRouter?.unregister(spec.agentAddress);
          deps.inboundMailPolicyRegistry?.unregister(spec.agentAddress);
          deps.multistepSignalRouter?.unregister(spec.agentAddress);
          deps.multistepDrainRouter?.unregister(spec.agentAddress);
          deps.multistepGrantsRouter?.unregister(spec.agentAddress);
          // Safe unconditionally: `unregister` is a no-op for an address that
          // never registered a sources handler.
          deps.multistepSourcesRouter?.unregister(spec.agentAddress);
          deps.multistepCredentialsRouter?.unregister(spec.agentAddress);
        }
        if (supervisorRegistered) {
          activeSupervisors.delete(spec.agentAddress);
        }
        if (wiredForUnwind !== undefined) {
          await wiredForUnwind.supervisor.shutdown().catch((cause) => {
            const message =
              cause instanceof Error ? cause.message : String(cause);
            logger.warn`multi-step deploy unwind: supervisor.shutdown failed: ${message}`;
          });
        }
        if (agentTransportRegistered) {
          // Drop the agent's transport registration so a failed deploy does
          // not leave the address live with a dangling `CryptoProvider`.
          deps.transport.unregister(spec.agentAddress);
        }
        if (hubKeyRecorded) {
          // Reverse `recordHubKey` so a failed deploy leaves no in-memory hub
          // key or keypair cache behind (a redeploy reloads the keypair from
          // disk). The deploy-tree repo `initRepo` created is deliberately NOT
          // reversed: the durable identity keypair lives inside it, so
          // removing it would destroy an identity a rerouted head must keep.
          deps.keyStore.forgetAgent(spec.agentAddress);
        }
        if (deploymentRegistered) {
          // Reverse the pre-spawn `registerDeployment`: drop the address
          // mapping so a failed spawn leaves the registry as it found it
          // (registered first, unwound last); a stale write then fails
          // structurally (`registry.resolve` returns null) instead of routing
          // to a deployment that never came up.
          deps.unregisterDeployment({
            runId,
            agentAddress: spec.agentAddress,
          });
        }
      }
    }
  }

  /**
   * Provision one step of a multi-step deploy WITHOUT spawning: init the
   * step's agent-state repo and record the hub key so a later full-closure
   * deploy pack has a repo to apply into and a key to verify the commit
   * signature against. Constructs no supervisor or child.
   *
   * Returns the sidecar's principal public key so the ack carries one; the
   * per-step address is workflow-derived and records no `agent_instance` key,
   * so the hub discards the value.
   */
  async function provisionStep(
    frame: AgentDeployFrame,
  ): Promise<DeployRouterResult> {
    await deps.sessions.initRepo(frame.agentAddress);
    deps.keyStore.recordHubKey(frame.agentAddress, frame.hubPublicKey);
    return {
      publicKey: await derivePrincipalPublicKeyHex(deps.signingKeySeed),
    };
  }

  async function deployMultiStep(
    frame: AgentDeployFrame,
    projection: NonNullable<AgentDeployFrame["workflow"]>,
  ): Promise<DeployRouterResult> {
    // Reject a re-deploy of an already-live OR mid-deploy address BEFORE any
    // durable write: the writes below destructively overwrite state owned by
    // the current holder. `activeSupervisors` catches completed deploys;
    // `reservingDeployAddresses` closes the window before the map is
    // populated (it fills only after spawn succeeds, so the has-check alone
    // would let a loser delete the winner's record).
    if (
      activeSupervisors.has(frame.agentAddress) ||
      reservingDeployAddresses.has(frame.agentAddress)
    ) {
      throw new Error(
        `sidecar deploy router: ${frame.agentAddress} is already deployed; undeploy it before redeploying`,
      );
    }

    const runId = deps.deriveWorkflowRunRepoId(frame.agentAddress);

    // The run record, materialized closure, and per-step scratch all root
    // under the sidecar data dir.
    const dataDir = stepStateDataDir;
    if (typeof dataDir !== "string" || dataDir.length === 0) {
      throw new Error(
        "sidecar deploy router: SIDECAR_DATA_DIR must be present in the multi-step substrate env; the run record and workflow-process child root under it",
      );
    }

    // Claim the deployment slug BEFORE any durable write so a colliding runId
    // (two addresses projecting to the same slug) is rejected before anything
    // touches disk -- the "no repo state touched before rejection" guarantee.
    // Released on any failure below; a successful deploy keeps it until the
    // undeploy hook releases it.
    claimSlug(runId, frame.agentAddress);
    // Hold the single-flight reservation across the async body; everything
    // above is synchronous, so the reservation is only needed from the first
    // await onward.
    reservingDeployAddresses.add(frame.agentAddress);
    try {
      // Source-ref apply -- the only deploy lineage: materialize the hub's
      // frozen `closure` and evaluate the PINNED CODE to the workflow
      // definition; the frame carries no inline definition to trust. The
      // child's load-boundary re-verify recomputes the wire hash and fails
      // closed on divergence from the hub-approved hash.
      //
      // Check the frame's inline source assets into the durable
      // per-deployment store, reclaiming it first so a redeploy drops
      // unreferenced assets. Deploy-only: restore re-reads the persisted store
      // with no re-delivery.
      const assetStore = deploymentSourceAssetRoot(dataDir, runId);
      const gitStore = deploymentSourceGitRoot(dataDir, runId);
      await rm(assetStore, { recursive: true, force: true });
      await rm(gitStore, { recursive: true, force: true });
      if (projection.assets !== undefined && projection.assets.length > 0) {
        await deps.materializeWorkflowAssets({
          assets: projection.assets,
          closure: projection.sourceRef.closure,
          assetRoot: assetStore,
          gitDirRoot: gitStore,
          maxAssetPayloadBytes: deps.maxInlineAssetPayloadBytes,
        });
      }
      // Safe to reclaim: single-flight-guarded and the child is not yet
      // spawned, so no live reader holds it.
      const applied = await materializeDeploymentClosure(
        dataDir,
        runId,
        projection.sourceRef,
      );
      const validatedDefinition = WorkflowProjectionDefinition(
        projectLiveToInert(applied.definition),
      );
      if (validatedDefinition instanceof type.errors) {
        throw new Error(
          `sidecar deploy router: workflow definition loaded from the frozen closure failed projection validation: ${validatedDefinition.summary}`,
        );
      }
      const effectiveDefinition = validatedDefinition;

      // Structural invariants arktype does not cover (non-empty stepOrder,
      // every entry backed by `steps` and `sources`), checked against the
      // closure-derived definition. Mirrors the restore path.
      validateWorkflowProjection({
        definition: effectiveDefinition,
        sources: projection.sources,
      });

      // Source-admission gate: reject a deploy where any step pins a provider
      // this sidecar cannot build. Every source in a failover chain must be
      // buildable (an unbuildable tail would fail only after failover onto
      // it), so the whole list is checked.
      for (const stepId of effectiveDefinition.stepOrder) {
        const chain = projection.sources[stepId];
        if (chain !== undefined) {
          for (const source of chain) deps.assertSourceBuildable(source);
        }
      }

      // A one-step definition keeps the deploy's own mail address and grants
      // repo; a multi-step one derives `<runId>-<stepId>` per step, isolating
      // each step's grants in its own repo.
      const stepStrategy = createStepStrategy({
        legacyAddress: frame.agentAddress,
        stepOrder: effectiveDefinition.stepOrder,
        multistepDeriveStepAddress,
        parseAgentId: deps.parseAgentId,
      });

      // The spec the shared spawn core consumes and the durable record that
      // lets a restore rebuild the SAME spec (definition re-evaluated from the
      // pinned closure; sources, session id, and hub key from the record).
      const spec: WorkflowDeploySpec = {
        agentAddress: frame.agentAddress,
        definition: effectiveDefinition,
        sources: projection.sources,
        bodySources: buildBodySourcesMap(projection.referencedDefinitions),
        // The hub's unified credential-material cell (inference + tool secrets);
        // persisted sealed in the record and delivered on the barrier.
        credentials: projection.credentials,
        approvedWireHash: projection.approvedWireHash,
        sessionId: frame.config.sessionId,
        hubPublicKey:
          effectiveDefinition.stepOrder.length === 1
            ? frame.hubPublicKey
            : undefined,
        // Sidecar-local dir of the just-materialized closure, threaded into the
        // child's env so it re-evaluates the pinned code.
        closurePackageDir: applied.packageDir,
        // The source-ref pin the record persists so a restore can
        // re-materialize the closure.
        sourceRef: projection.sourceRef,
      };
      const record = buildWorkflowRunRecord(spec, spec.sources);

      // Persist the run record BEFORE the spawn so a crash mid-spawn leaves a
      // record the boot scan re-drives. A soft-failed deploy deletes it below,
      // so only a crash-interrupted deploy leaves one.
      await persistWorkflowRunRecord(
        dataDir,
        runId,
        record,
        deps.credentialCipher,
      );

      // Sweep any legacy plaintext body-source file: body sources now ride
      // sealed in the run record and reach the child through the spawn env, so
      // the old `assets/workflow/<bodyRef>/` staging is retired. A no-op for
      // deployments first seen on this build.
      for (const referenced of projection.referencedDefinitions ?? []) {
        await sweepLegacyBodySources(dataDir, referenced.definition.id);
      }

      // Grants bridge: the child reads each step's grants from
      // `state/grants.json` in the step's agent-state repo while the
      // supervisor assembles the credentialsSnapshot. Write the operator-
      // approved `frame.config.grants` to the same repo via `deriveStepRepoId`
      // before the spawn core.
      //
      // The write covers the whole flat step-id namespace, loop body steps
      // included: an unwritten repo yields an empty grant set, denying the
      // body every approved resource.
      await writeStepGrants({
        repoStore: deps.repoStore,
        anchorRunId: runId,
        stepOrder: deps.inertFlatNamespaceStepIds({
          definition: effectiveDefinition,
          context: "sidecar deploy router grants bridge: ",
        }),
        deriveStepRepoId: stepStrategy.deriveStepRepoId,
        grants: frame.config.grants,
      });

      // Hand off to the shared spawn core.
      return await spawnWorkflowRun(spec);
    } catch (cause) {
      // Soft failure (this process survived, the deploy threw): drop the record
      // and release the slug so the failed deploy is neither restored nor leaks
      // its slug. A rejecting delete is logged but `cause` still propagates and
      // the slug is still released.
      try {
        await deleteWorkflowRunRecord(dataDir, runId);
      } catch (cleanupError) {
        const message =
          cleanupError instanceof Error
            ? cleanupError.message
            : String(cleanupError);
        logger.error`deploy cleanup: deleteWorkflowRunRecord failed for ${runId}: ${message}`;
      }
      releaseSlug(runId, frame.agentAddress);
      throw cause;
    } finally {
      // Release the reservation either way; on success `activeSupervisors`
      // keeps the guard rejecting re-deploys.
      reservingDeployAddresses.delete(frame.agentAddress);
    }
  }

  return {
    async deploy(frame): Promise<DeployRouterResult> {
      if (workflowStopTasks.has(frame.agentAddress)) {
        throw new Error("Workflow deployment is still stopping");
      }
      if (frame.provisionStep === true) {
        return await provisionStep(frame);
      }
      if (frame.workflow !== undefined) {
        return await deployMultiStep(frame, frame.workflow);
      }
      // A provision-step frame primes the per-step repo; a workflow frame
      // spawns the supervised child. A frame carrying neither is unsupported.
      throw new Error(
        `sidecar deploy router: unsupported deploy frame for ${frame.agentAddress}; a deploy must carry provisionStep or a workflow definition`,
      );
    },
    async control(frame): Promise<WorkflowControlOutcome> {
      if (deps.parseAgentId(frame.agentAddress) !== frame.runId) {
        throw new Error(
          "Workflow control run does not match the deployment address",
        );
      }
      const stopping = workflowStopTasks.get(frame.agentAddress);
      if (stopping !== undefined) {
        await stopping;
        return reportControlOutcome(frame);
      }
      if (reservingDeployAddresses.has(frame.agentAddress)) {
        throw new Error(WORKFLOW_CONTROL_INITIALIZING_ERROR);
      }
      const wired = activeSupervisors.get(frame.agentAddress);
      if (frame.action === "cancel" && wired !== undefined) {
        const cancelling = workflowCancellationTasks.get(wired);
        if (cancelling !== undefined) {
          await cancelling;
          return {};
        }
        const pending = wired.supervisor
          .requestCancel({
            runId: frame.runId,
            origin: "supervisor-operator",
            reason: frame.reason,
            at: new Date().toISOString(),
          })
          .then(() => undefined);
        // The Hub resends cancel until the run is terminal; keep the committed
        // cancellation so a resend reuses it, and retry only a failed one.
        workflowCancellationTasks.set(wired, pending);
        try {
          await pending;
        } catch (error) {
          workflowCancellationTasks.delete(wired);
          throw error;
        }
        return {};
      }
      // Cancellation without a supervisor must still retire its restart record.
      // Publish stop before its first await; it fences new commands and deploys
      // while shutdown kills the child.
      const pending = Promise.resolve().then(async () => {
        unregisterWorkflowRoutes(frame.agentAddress);
        if (wired !== undefined) await wired.supervisor.shutdown();
        if (activeSupervisors.get(frame.agentAddress) === wired) {
          reclaimSelfTerminatedSupervisor({
            runId: deps.deriveWorkflowRunRepoId(frame.agentAddress),
            agentAddress: frame.agentAddress,
          });
        }
        // A stopped terminal deployment must not respawn on restart; keep its
        // scratch and source material for the allocation's retention period.
        if (stepStateDataDir !== undefined) {
          await deleteWorkflowRunRecord(
            stepStateDataDir,
            deps.deriveWorkflowRunRepoId(frame.agentAddress),
          );
        }
      });
      workflowStopTasks.set(frame.agentAddress, pending);
      try {
        await pending;
      } finally {
        if (workflowStopTasks.get(frame.agentAddress) === pending)
          workflowStopTasks.delete(frame.agentAddress);
      }
      return reportControlOutcome(frame);
    },
    async undeploy(frame): Promise<void> {
      const stopping = workflowStopTasks.get(frame.agentAddress);
      if (stopping !== undefined) await stopping;
      // Release the per-deployment routing state both branches install, so a
      // stale frame aimed at the dead address is rejected at the router rather
      // than dispatched into an orphan supervisor handler. The unregister
      // calls are idempotent.
      //
      // Routers come down BEFORE the supervisor's `shutdown()` so a frame
      // racing the undeploy is dropped at the boundary, not dispatched into a
      // supervisor mid-teardown.
      const runId = deps.deriveWorkflowRunRepoId(frame.agentAddress);
      unregisterWorkflowRoutes(frame.agentAddress);
      // Shut the supervisor down so the child, its IPC pipes, and its
      // event-channel fd are released. `shutdown()` is idempotent and handles
      // the kill + `exited` await internally. The map entry is removed before
      // the await so a re-deploy cannot observe a stale handle if it rejects.
      const wired = activeSupervisors.get(frame.agentAddress);
      if (wired !== undefined) {
        activeSupervisors.delete(frame.agentAddress);
        await wired.supervisor.shutdown();
        // Drop the address's transport registration (OUTBOUND half, §3a);
        // `unregister` is a no-op only if spawn failed before registering.
        deps.transport.unregister(frame.agentAddress);
      }
      // Reclaim warm and cold scratch after teardown; durable conversations
      // live under a separate root and survive undeploy.
      if (stepStateDataDir !== undefined) {
        await rm(pathJoin(stepStateDataDir, "workflow-step-state", runId), {
          recursive: true,
          force: true,
        });
      }
      // Drop the run record so a restore does not re-spawn a torn-down
      // deployment, and reclaim the materialized closure tree and source
      // stores. Runs on every undeploy so crash-interrupted or
      // failed-to-spawn state is reclaimed too; a registry-sourced deployment
      // never creates the source store, so its `force` remove is a no-op.
      if (stepStateDataDir !== undefined) {
        await deleteWorkflowRunRecord(stepStateDataDir, runId);
        await rm(
          pathJoin(stepStateDataDir, "workflow-definition-closures", runId),
          { recursive: true, force: true },
        );
        await rm(deploymentSourceAssetRoot(stepStateDataDir, runId), {
          recursive: true,
          force: true,
        });
        await rm(deploymentSourceGitRoot(stepStateDataDir, runId), {
          recursive: true,
          force: true,
        });
      }
      releaseSlug(runId, frame.agentAddress);
      deps.unregisterDeployment({
        runId,
        agentAddress: frame.agentAddress,
      });
    },
    async restoreWorkflowRuns(): Promise<void> {
      const dataDir = stepStateDataDir;
      if (dataDir === undefined) {
        // No data dir wired (a test router that never spawns): nothing was
        // ever persisted, so nothing to restore.
        return;
      }

      const scanned = await scanWorkflowRunRecords(
        dataDir,
        deps.credentialCipher,
      );
      // Restore serially: deterministic boot-log ordering, one isolable
      // warning per failed record, and no concurrent child-spawn /
      // transport-register storm. Each failure is caught so one bad
      // deployment cannot strand the rest.
      for (const { runId, record } of scanned) {
        try {
          // Integrity: the stored address must re-derive to its own directory
          // name; a mismatch means a corrupt or misplaced record, so skip it
          // rather than restore under the wrong slug. Schema-invalid source-ref
          // records are already rejected at the scan boundary.
          const derived = deps.deriveWorkflowRunRepoId(record.agentAddress);
          if (derived !== runId) {
            logger.warn`skipping workflow deployment restore: ${record.agentAddress} derives slug ${derived}, not its directory ${runId}`;
            continue;
          }

          // Reconstruct the runnable definition the SAME way the deploy path
          // does: re-materialize the pinned closure, evaluate the pinned code,
          // and project it to the inert wire shape. The closure IS the source
          // of truth; no on-disk definition is read. The helper reclaims the
          // instance dir first, safe because the prior process is dead and
          // restore is serial. Registry-sourced entries hit the content-
          // addressed closure cache; asset-sourced entries read the durable
          // source store -- both SRI-verified. A miss soft-fails the record
          // (kept for the next boot).
          const applied = await materializeDeploymentClosure(
            dataDir,
            runId,
            record.sourceRef,
          );
          const validatedDefinition = WorkflowProjectionDefinition(
            projectLiveToInert(applied.definition),
          );
          if (validatedDefinition instanceof type.errors) {
            logger.warn`skipping workflow deployment restore for ${record.agentAddress}: workflow definition loaded from the frozen closure failed projection validation: ${validatedDefinition.summary}`;
            continue;
          }
          const definition: WorkflowProjectionDefinition = validatedDefinition;
          const closurePackageDir = applied.packageDir;

          // Structural invariants arktype does not cover (non-empty stepOrder,
          // every entry backed by `steps` and `sources`). The closure eval
          // skips the deploy frame's coverage narrow, so this is where
          // definition-vs-sources coverage is checked.
          validateWorkflowProjection({ definition, sources: record.sources });

          // Re-run the source-admission gate: refuse to restore a deployment
          // whose pinned provider this sidecar can no longer build. The record
          // is KEPT so a later boot with the provider restored retries it.
          for (const stepId of definition.stepOrder) {
            const chain = record.sources[stepId];
            if (chain !== undefined) {
              for (const source of chain) deps.assertSourceBuildable(source);
            }
          }

          const spec: WorkflowDeploySpec = {
            agentAddress: record.agentAddress,
            definition,
            sources: record.sources,
            // The record's unsealed body sources, re-delivered through the
            // spawn env. `undefined` for a legacy record; the child then falls
            // back to the on-disk plaintext file.
            bodySources: record.bodySources,
            // The record's credential-material cell, unsealed by the boot scan
            // and re-delivered on the pre-trigger barrier so an offline restart
            // restores credentials without the hub.
            credentials: record.credentials,
            // The hub-approved wire hash the original deploy persisted, so the
            // restore re-spawn carries the same `DEFINITION_HASH`, never a
            // recompute.
            approvedWireHash: record.approvedWireHash,
            sessionId: record.sessionId,
            hubPublicKey: record.hubPublicKey,
            // Sidecar-local dir of the just-materialized closure, threaded into
            // the child's env so it re-evaluates the pinned code.
            closurePackageDir,
            // Carry the source-ref pin so a post-restore rotation -- which
            // rebuilds the record from the spec -- re-persists it; dropping it
            // would wedge the NEXT restart.
            sourceRef: record.sourceRef,
          };

          // Claim the slug before the spawn, release on failure. Unlike
          // deploy's soft-fail, restore does NOT delete the record or
          // re-materialize the step grants / body sources -- both are already
          // on disk. A failed restore warns and leaves the record for the next
          // boot.
          //
          // Release only a slug THIS pass newly claimed: if the address is
          // already live, the double-spawn guard throws, and freeing the slug
          // then would strand a live deployment's collision guard.
          const slugNewlyClaimed =
            slugClaims.get(runId) !== record.agentAddress;
          claimSlug(runId, record.agentAddress);
          try {
            await spawnWorkflowRun(spec);
            logger.info`Restored workflow deployment for ${record.agentAddress}`;
          } catch (cause) {
            if (slugNewlyClaimed) {
              releaseSlug(runId, record.agentAddress);
            }
            throw cause;
          }
        } catch (cause) {
          const reason = cause instanceof Error ? cause.message : String(cause);
          logger.warn`Failed to restore workflow deployment ${runId}: ${reason}`;
        }
      }
    },
    activeAddresses(): string[] {
      // `activeSupervisors` keys are exactly the addresses with a live
      // supervisor, i.e. the addresses this sidecar can route mail to.
      return [...activeSupervisors.keys()];
    },
    reEmitParkedCorrelations(address: string): void {
      const wired = activeSupervisors.get(address);
      if (wired === undefined) {
        // No live supervisor owns a hub-reported-routable address (torn down
        // or not yet respawned); re-registering would be wrong, so skip.
        // Debug, not warn: the ordinary torn-down case should not cry wolf.
        logger.debug`re-emit on reconnect: no active supervisor for ${address}; skipping`;
        return;
      }
      // Fire-and-forget: the driver is best-effort and watchdog-bounded, so
      // the reconnect fan-out never awaits it. The `.catch` is
      // defense-in-depth; the driver already swallows its own failures.
      void wired.supervisor.reEmitParkedCorrelations().catch((cause) => {
        const message = cause instanceof Error ? cause.message : String(cause);
        logger.warn`re-emit of parked correlations on hub reconnect failed for ${address}: ${message}`;
      });
    },
  };
}

/**
 * Logical mail-audit reference the supervisor stamps onto every
 * inbox/processing/consumed envelope for sidecar-hosted deployments. The
 * substrate does not dereference the value; it is a host-side pointer the
 * audit consumer joins on, keyed by deployment id plus messageId.
 */
export function deriveSidecarMailAuditRef(runId: string): (
  messageId: string,
  rawMessage: Uint8Array,
) => {
  store: string;
  path: string;
} {
  return (messageId, _rawMessage) => ({
    store: "sidecar-mail-audit",
    path: `${runId}/${messageId}`,
  });
}

/**
 * Construct a per-deployment supervisor with the sidecar's bindings
 * pre-wired. Called once per multi-step `agent.deploy` frame to stand
 * up the workflow-process child.
 */
export function createSidecarWorkflowSupervisor(
  opts: CreateSidecarWorkflowSupervisorOpts,
): SidecarWorkflowSupervisor {
  const mailBus: HubTransportMailBusAdapter = wrapHubTransportAsMailBus(
    opts.transport,
  );
  const supervisorPrincipal: WorkflowRunSupervisorPrincipal = {
    kind: "supervisor",
    anchorRunId: opts.runId,
  };
  // Per-run grants sink the supervisor awaits before the run's `trigger.fire`.
  // A throw fails the run at the dispatch barrier rather than firing the
  // trigger against absent grants; the snapshot is the run's own per-run
  // grants file, written before dispatch by every legitimate birth path.
  const onRunStart = (args: {
    runId: string;
    anchorRunId: string;
  }): Promise<CredentialsSnapshot> => {
    // Fail closed on a run whose `run.grants` write was recorded as failed
    // (its file never landed). Distinct from the absent-file backstop in
    // `assembleRunCredentialsSnapshot`: this names the recorded write failure
    // so a poisoned run's diagnostics point at the failed push.
    if (opts.isRunPoisoned?.(args.runId) === true) {
      return Promise.reject(
        new Error(
          `sidecar onRunStart: run ${args.runId} grants write failed; refusing to start the run under-authorized`,
        ),
      );
    }
    return assembleRunCredentialsSnapshot({
      repoStore: opts.repoStore,
      anchorRunId: args.anchorRunId,
      runId: args.runId,
      stepOrder: opts.stepOrder,
      deriveStepAddress: opts.deriveStepAddress,
    });
  };
  const supervisor = createWorkflowSupervisor({
    repoStore: opts.repoStore,
    signAsPrincipal: async (kind, payload) => {
      const sig = await signEd25519(opts.signingKeySeed, payload);
      return { sig, principalKind: kind };
    },
    mailBus,
    subprocessSpawner: opts.subprocessSpawner,
    binaryPath: opts.binaryPath,
    substrateEnv: opts.substrateEnv,
    dynamicSpawnEnv: opts.dynamicSpawnEnv,
    workflowRunRepoId: opts.workflowRunRepoId,
    workflowRunRef: opts.workflowRunRef,
    anchorRunId: opts.runId,
    stepCount: opts.stepCount,
    deploymentMailAddress: opts.deploymentMailAddress,
    readPrincipal: supervisorPrincipal,
    deriveStepAddress: opts.deriveStepAddress,
    onRunStart,
    ...(opts.credentialDelivery !== undefined
      ? { credentialDelivery: opts.credentialDelivery }
      : {}),
    ...(opts.onSuspensionRegister !== undefined
      ? { onSuspensionRegister: opts.onSuspensionRegister }
      : {}),
    ...(opts.onSelfTerminate !== undefined
      ? { onSelfTerminate: opts.onSelfTerminate }
      : {}),
    ...(opts.deriveStepRepoId !== undefined
      ? { deriveStepRepoId: opts.deriveStepRepoId }
      : {}),
    deriveMailAuditRef: deriveSidecarMailAuditRef(opts.runId),
    ...(opts.onDispatchTiming !== undefined
      ? { onDispatchTiming: opts.onDispatchTiming }
      : {}),
    ...(opts.repackEveryMessages !== undefined
      ? { repackEveryMessages: opts.repackEveryMessages }
      : {}),
    ...(opts.consumedRetentionMs !== undefined
      ? { consumedRetentionMs: opts.consumedRetentionMs }
      : {}),
    ...(opts.readyTimeoutMs !== undefined
      ? { readyTimeoutMs: opts.readyTimeoutMs }
      : {}),
  });
  return {
    supervisor,
    routeInbound(message) {
      return mailBus.routeInbound(opts.deploymentMailAddress, message);
    },
    getCredentialsSnapshot: () => supervisor.getCredentialsSnapshot(),
    onRunStart,
  };
}

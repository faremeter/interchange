// Thin wiring module that constructs `createWorkflowSupervisor` with
// this sidecar's host-specific bindings: the existing mail-bus
// instance, the sidecar's Ed25519 signing keypair, the substrate
// RepoStore handle, and `Bun.spawn` as the subprocess spawner. Any
// logic that would benefit a future alternative-sidecar
// implementation lives inside `@intx/workflow-host`, not here.

import { mkdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { dirname, join as pathJoin } from "node:path";
import { fileURLToPath } from "node:url";

import { type } from "arktype";

import { derivePublicKeyBytes, signEd25519 } from "@intx/crypto";
import { getLogger } from "@intx/log";
import type { HubTransport } from "@intx/mail-memory";
import {
  parseAgentId,
  workflowSourceAssetMountPath,
  type Principal,
  type RepoId,
  type RepoStore,
  type WorkflowRunSupervisorPrincipal,
} from "@intx/hub-sessions";
import type {
  AgentKeyStore,
  DeployRouter,
  DeployRouterResult,
  SessionManager,
} from "@intx/hub-agent";
import {
  createWorkflowSupervisor,
  hashGrants,
  STEP_GRANTS_PATH,
  STEP_GRANTS_REF,
  wrapHubTransportAsMailBus,
  type CredentialsSnapshot,
  type CredentialsSnapshotStep,
  type DeriveStepAddress,
  type DeriveStepRepoId,
  type DispatchTimingMark,
  type FrameReader,
  type HubTransportMailBusAdapter,
  type NdjsonReader,
  type NdjsonWriter,
  type SpawnOpts,
  type SubprocessHandle,
  type SubprocessSpawner,
  type SuspensionRegistration,
  type WorkflowSupervisor,
} from "@intx/workflow-host";
import { hexEncode, type SignalKind } from "@intx/types";
import {
  parseInferenceEvent,
  type ApprovalSnapshot,
  type CryptoProvider,
  type InferenceEvent,
  type InferenceSource,
  type KeyPair,
} from "@intx/types/runtime";
import {
  WorkflowProjectionDefinition,
  type AgentDeployFrame,
  type CredentialDelivery,
  type SourceRefPin,
} from "@intx/types/sidecar";
import { STEP_ID_PATTERN, projectLiveToInert } from "@intx/workflow";
import { deriveWorkflowRunRepoId } from "@intx/workflow-deploy";

import {
  applyFrozenWorkflowClosure,
  type AppliedWorkflowClosure,
} from "./workflow-closure-apply";
import {
  MAX_INLINE_ASSET_PAYLOAD_BYTES,
  materializeWorkflowAssets,
  sourceAssetGitDir,
} from "./source-asset-delivery";
import { readRegistries } from "./sidecar-materialization-config";

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
import { readRunGrants, runGrantsPath } from "./run-grants";

const logger = getLogger(["interchange", "sidecar", "workflow-host-wiring"]);

/**
 * Project an run address into the substrate-safe id of its
 * workflow-run repo. Both deploy branches key `{ kind: "workflow-run",
 * id }` by this slug, and the supervisor principal's `anchorRunId`
 * must equal that id for the workflow-run kind handler's authz check to
 * pass. The derivation is owned by `@intx/workflow-deploy` so the hub's
 * read routes reconstruct the identical id; this thin delegator keeps
 * the sidecar's call sites readable while the rationale and the
 * substrate `SAFE_REPO_ID` contract live with the shared function.
 *
 * The name keeps "Deployment" deliberately: it derives the deploy-phase
 * routing slug (the workflow-run repo id an address projects to), not a
 * run identity, so it survives the run-first identity sweep.
 */
export function deriveDeploymentId(agentAddress: string): string {
  return deriveWorkflowRunRepoId(agentAddress);
}

/**
 * The durable per-deployment store the sidecar checks a source-ref deployment's
 * source assets out into. A SIBLING of the closure instance dir, not a child:
 * `materializeDeploymentClosure` reclaims the closure dir on every apply and
 * restore, but never this store, so the checked-out assets survive a restart
 * and re-materialization needs no re-delivery. The store is reclaimed on
 * redeploy (at the deploy call site) and on undeploy.
 */
function deploymentSourceAssetRoot(
  dataDir: string,
  deploymentId: string,
): string {
  return pathJoin(dataDir, "workflow-definition-sources", deploymentId);
}

/**
 * The durable indexed-`.git` store root a pinned deployment's source-format
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
 * The `assetId -> mountPath` map a pinned closure's TARBALL `kind:"asset"`
 * entries resolve against, derived purely from the pin (via the shared
 * mount-path helper) so deploy and restore agree without the frame's delivered
 * assets. Source-format entries resolve through `deriveSourceGitDirs` instead.
 */
function deriveSourceAssetMounts(pin: SourceRefPin): Map<string, string> {
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
 * The `assetId -> gitDir` map a pinned closure's SOURCE `kind:"asset"` entries
 * check subtrees out of, derived purely from the pin so deploy and restore
 * agree without re-delivery.
 */
function deriveSourceGitDirs(
  pin: SourceRefPin,
  gitRoot: string,
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
 * pinned deployment materializes its `kind:"asset"` closure entries from,
 * asserting every referenced asset's mount directory is present on disk. This
 * is a cheap early gate for a missing checkout; the loader still SRI-verifies
 * each tarball's bytes at materialization. Derived purely from the pin, so the
 * deploy path and the boot-time restore path (which has only the pin, no
 * re-delivery) resolve the identical mounts. A missing mount is a broken
 * deployment the hub must re-drive, so it fails loud rather than materializing
 * against an absent store.
 */
export async function resolveDeploymentAssetMounts(
  dataDir: string,
  deploymentId: string,
  pin: SourceRefPin,
): Promise<{
  assetRoot: string;
  assetMounts: ReadonlyMap<string, string>;
  gitDirs: ReadonlyMap<string, string>;
}> {
  const assetRoot = deploymentSourceAssetRoot(dataDir, deploymentId);
  const assetMounts = deriveSourceAssetMounts(pin);
  for (const [assetId, mountPath] of assetMounts) {
    const mountDir = pathJoin(assetRoot, mountPath);
    if (!(await isExistingDir(mountDir))) {
      throw new Error(
        `resolveDeploymentAssetMounts: source asset ${JSON.stringify(assetId)} for deployment ${deploymentId} is not present in the durable store at ${mountDir}; the deployment must be re-driven from the hub`,
      );
    }
  }
  const gitRoot = deploymentSourceGitRoot(dataDir, deploymentId);
  const gitDirs = deriveSourceGitDirs(pin, gitRoot);
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
 * Read a substrate-config byte cap (`SIDECAR_CACHE_MAX_BYTES` /
 * `SIDECAR_REGISTRY_MAX_TARBALL_BYTES`) from the multi-step substrate env and
 * parse it to a positive finite number. The boot edge resolves these once and
 * threads them through the substrate env; a source-ref deploy needs them to
 * size the tarball cache and per-fetch cap when it materializes the frozen
 * workflow closure. A missing or non-numeric value is a boot-edge wiring bug,
 * so it fails loud rather than defaulting.
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
 * Hub principal the deploy router presents when it writes a step's
 * grants into the agent-state repo on the sidecar's substrate. The
 * agent-state kind handler gates `writeTree` as hub-only; the deploy
 * router is the local stand-in for the hub on the sidecar's disk, so it
 * claims the hub principal for this single bookkeeping write. The child
 * reads the same repo via the working-tree path (`getRepoDir`), which is
 * not authorize-gated.
 */
const GRANTS_WRITE_PRINCIPAL: Principal = { kind: "hub" };

/**
 * Per-deploy address/repo strategy. The single-step launched-agent
 * deploy and the derived multi-step deploy disagree on how the per-step
 * mail address and agent-state repo id are computed; both
 * `deriveStepAddress` (consumed by the supervisor's credentialsSnapshot
 * assembly for the step's mail address) and `deriveStepRepoId` (consumed
 * by the same assembly to locate each step's grants) must agree on the
 * choice, so they are minted together.
 */
type StepStrategy = {
  deriveStepAddress: DeriveStepAddress;
  deriveStepRepoId: DeriveStepRepoId;
};

/**
 * Decide the per-step address/repo strategy from the projection's step
 * count.
 *
 * `stepOrder.length === 1` is the single-agent deploy: the sole
 * step keeps the deploy's own mail address, so its grants live in the
 * agent-state repo keyed by `parseAgentId(address)`. The spawned child
 * reads the agent's grants from where the run's identity already
 * lives, and the deploy frame's run address is preserved (the
 * workflow-run repo stays keyed by `deriveWorkflowRunRepoId(address)`).
 *
 * Any other step count is a derived multi-step deploy: each step gets a
 * derived `<runId>-<stepId>` mail address (via the router's
 * `multistepDeriveStepAddress`) and a derived agent-state repo under the
 * default `<runId>-<stepId>` convention.
 *
 * NOTE: the supervisor's `deriveStepAddress` feeds the credentials
 * snapshot's per-step mail `address` and the grants-repo derivation. It
 * does NOT feed the child's on-disk tool read (`stepDeployTreeDir` in
 * `step-agent-tools.ts`), which re-derives the step address from the
 * deployment mailbox address independently. The deploy tree must
 * therefore be staged at the address `stepDeployTreeDir` computes,
 * regardless of this strategy's address choice.
 */
function createStepStrategy(args: {
  legacyAddress: string;
  stepOrder: readonly string[];
  multistepDeriveStepAddress: DeriveStepAddress;
}): StepStrategy {
  if (args.stepOrder.length === 1) {
    return {
      deriveStepAddress: () => args.legacyAddress,
      // `parseAgentId` is deferred into the closure rather than computed
      // eagerly: the supervisor only invokes `deriveStepRepoId` while
      // assembling the credentialsSnapshot inside `spawn()`, so a
      // malformed address surfaces at the same point the rest of the
      // spawn path would fault rather than ahead of the deploy router's
      // other boundary checks.
      deriveStepRepoId: () => ({
        kind: "agent-state",
        id: parseAgentId(args.legacyAddress),
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
 * Write a grant set into the sidecar's substrate as the canonical
 * `{ grants: WireGrantRule[] }` envelope -- the shape
 * `assembleCredentialsSnapshot` validates (`{ grants: unknown[] }`) and
 * the child's `evaluateGrants` adapter narrows to `GrantRule[]`.
 *
 * Two destinations share this write machinery, selected by `runId`:
 *
 *   - `runId` absent (deploy time): write every step's grants into its
 *     own `agent-state` repo at `STEP_GRANTS_PATH`, keyed by the same
 *     `deriveStepRepoId` the supervisor reads with, so read and write
 *     address the same repo. The write is on the spawn critical path: a
 *     failure rejects the deploy (the caller's `finally` unwinds the
 *     partial state) rather than spawning a child that would fail every
 *     authorize closed against an empty grant set.
 *
 *   - `runId` present (per-run delivery): write a single
 *     `runs/<runId>/grants.json` into the deployment's `workflow-run`
 *     repo, sibling to that run's `runs/<runId>/events/` subtree. The
 *     `stepOrder` / `deriveStepRepoId` fields are unused in this mode --
 *     the destination is the one workflow-run repo keyed by
 *     `runId`, not a per-step fan-out.
 *
 * Both destinations use the same hub principal and `refs/heads/main`
 * ref: the agent-state kind handler gates `writeTree` as hub-only, and
 * the workflow-run repo pins its run subtree under the same moving ref.
 */
async function writeStepGrants(args: {
  repoStore: RepoStore;
  anchorRunId: string;
  stepOrder: readonly string[];
  deriveStepRepoId: DeriveStepRepoId;
  grants: readonly unknown[] | undefined;
  runId?: string;
}): Promise<void> {
  // The deploy frame's validated HarnessConfig always carries a `grants`
  // array (possibly empty); an absent array means "no grants", which
  // serializes to the same fail-closed empty file the snapshot expects.
  // Coerce here so the on-disk envelope is always a valid `{ grants: [] }`
  // rather than `{}` (which the snapshot's validator rejects).
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
  /** Step ids in `stepOrder`; the per-run grants apply uniformly across them. */
  stepOrder: readonly string[];
  /** Per-step mail-address derivation. */
  deriveStepAddress: DeriveStepAddress;
};

/**
 * Resolve a run's credentials snapshot for the `onRunStart` grants
 * barrier from its per-run grants file.
 *
 * Every legitimate run birth path writes `runs/<runId>/grants.json` in
 * the deployment's workflow-run repo before the run dispatches -- the
 * external trigger route and the mail-triggered path both ship a
 * `run.grants` frame the sidecar writes, and a spawned child inherits its
 * parent's grants directly at spawn without reaching this barrier. The
 * per-run file IS the run's snapshot: the run's single flat grant set is
 * applied uniformly across every step, keyed on each step's address.
 *
 * A missing file is therefore not an internal run inheriting deploy-time
 * grants -- it is a run that reached its barrier with no grants written,
 * so it FAILS CLOSED here rather than running under-authorized. A file
 * that exists but is malformed also throws (via `readRunGrants`), for the
 * same reason: the file's presence implies a grants frame was delivered,
 * so a structural failure is a boundary bug, not a default.
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

// The supervisor's `binaryPath` binding resolves to the sidecar's
// own `bin/workflow-child` script via `import.meta.resolve` against
// the `@intx/sidecar-app` package. The script lives next to this
// wiring module (`../bin/workflow-child`); resolving it statically
// at wiring-module load time keeps the production spawn surface
// independent of any runtime env override. Tests inject a sentinel
// path via the `binaryPath` opts override; production wiring
// closes over this constant.
const SIDECAR_WORKFLOW_CHILD_BINARY: string = (() => {
  const url = import.meta.resolve("../bin/workflow-child");
  return fileURLToPath(url);
})();

/**
 * Child fd the supervisor inherits the event-channel pipe on. The
 * supervisor's spawn-time convention is:
 *
 *   fd 0 stdin  -- downstream control channel (supervisor -> child)
 *   fd 1 stdout -- upstream control channel (child -> supervisor)
 *   fd 2 stderr -- inherited so child diagnostics land on the
 *                  sidecar's stderr
 *   fd 3        -- event-channel write side (child writes
 *                  HMAC-authenticated InferenceEvent frames here;
 *                  the supervisor reads the parent end as a
 *                  `FrameReader`)
 *
 * The child opens fd 3 via `EVENT_CHANNEL_FD` in
 * `@intx/workflow-host`'s `from-process-env`. The two ends of the
 * pipe are provisioned by `Bun.spawn`'s `stdio` slot: setting
 * `stdio[3] = "pipe"` makes Bun mint a pipe pair where the child
 * inherits the write half at fd 3 and the parent receives the read
 * half as a numeric fd at `proc.stdio[3]` in its own address space.
 */
const CHILD_EVENT_CHANNEL_FD = 3;

/**
 * Wrap a Bun `FileSink` as the supervisor's `NdjsonWriter`. The
 * supervisor's control-channel sender writes one JSON line per
 * frame (already including the trailing newline); the writer is
 * responsible for passing the bytes through to the child's stdin
 * without buffering across frames so each frame surfaces on the
 * far side as soon as `write()` resolves.
 */
function ndjsonWriterFromFileSink(sink: Bun.FileSink): NdjsonWriter {
  return {
    async write(line: string): Promise<void> {
      const result = sink.write(line);
      if (typeof result !== "number") await result;
      const flushed = sink.flush();
      if (typeof flushed !== "number") await flushed;
    },
  };
}

/**
 * Wrap a Bun stdout `ReadableStream` as the supervisor's
 * `NdjsonReader`. The pipe is a byte stream; this reader buffers
 * partial chunks and yields one complete line per iteration. The
 * receiver's iterator finalises only on EOF, which mirrors the
 * `defaultControlReader` shape the child wires for `process.stdin`.
 */
function ndjsonReaderFromReadableStream(
  stream: ReadableStream<Uint8Array>,
): NdjsonReader {
  return {
    read(): AsyncIterableIterator<string> {
      return (async function* () {
        const decoder = new TextDecoder("utf-8");
        let pending = "";
        const reader = stream.getReader();
        try {
          while (true) {
            const { value, done } = await reader.read();
            if (value !== undefined) {
              pending += decoder.decode(value, { stream: true });
              let nl = pending.indexOf("\n");
              while (nl >= 0) {
                const line = pending.slice(0, nl).replace(/\r$/, "");
                pending = pending.slice(nl + 1);
                if (line.length > 0) yield line;
                nl = pending.indexOf("\n");
              }
            }
            if (done) break;
          }
          if (pending.length > 0) yield pending;
        } finally {
          reader.releaseLock();
        }
      })();
    },
  };
}

/**
 * Wrap the parent-side read fd of the event-channel pipe as the
 * supervisor's `FrameReader`. The child publishes one HMAC-
 * authenticated envelope per `FileSink.write()` and the supervisor's
 * `receiveEventChannel` parses each yielded `Uint8Array` as one
 * complete envelope. The pipe is a byte stream; this reader yields
 * each raw chunk the kernel delivers and trusts the sender's
 * one-write-per-envelope discipline. The buffer-overflow / framing
 * discipline lives in `receiveEventChannel`'s parser.
 */
function frameReaderFromFd(fd: number): FrameReader {
  const stream = Bun.file(fd).stream();
  return {
    read(): AsyncIterableIterator<Uint8Array> {
      return (async function* () {
        const reader = stream.getReader();
        try {
          while (true) {
            const { value, done } = await reader.read();
            if (value !== undefined && value.byteLength > 0) yield value;
            if (done) break;
          }
        } finally {
          reader.releaseLock();
        }
      })();
    },
  };
}

/**
 * Real `Bun.spawn`-backed subprocess spawner. Constructs a fresh env
 * carrying exactly the trust anchors and substrate-config keys the
 * supervisor passed in (no inheritance of the sidecar's process env);
 * inherits stdio 0/1/2 as control + stderr; pipes fd 3 for the event
 * channel and surfaces the parent-side read fd as the supervisor's
 * `FrameReader`.
 *
 * Failure modes flow through the returned handle's `exited` promise.
 * A `Bun.spawn` that fails to launch (binary missing, env malformed,
 * `EXEC` error) settles `exited` with a non-zero code; the
 * supervisor's `wireChild` races `exited` against `readyPromise`
 * inside `spawn()` so a spawn-time crash surfaces as a rejected
 * spawn rather than a wedged `starting` state.
 */
export const defaultSubprocessSpawner: SubprocessSpawner = ({
  binaryPath,
  env,
}): SubprocessHandle => {
  const proc = Bun.spawn([binaryPath], {
    stdio: ["pipe", "pipe", "inherit", "pipe"],
    env,
  });
  const eventFd = proc.stdio[CHILD_EVENT_CHANNEL_FD];
  if (typeof eventFd !== "number") {
    throw new Error(
      `workflow-host-wiring: Bun.spawn did not return a numeric fd at stdio[${String(CHILD_EVENT_CHANNEL_FD)}] for the event channel; got ${typeof eventFd}`,
    );
  }
  return {
    pid: proc.pid,
    controlWriter: ndjsonWriterFromFileSink(proc.stdin),
    controlReader: ndjsonReaderFromReadableStream(proc.stdout),
    eventReader: frameReaderFromFd(eventFd),
    kill(signal?: number | string): void {
      // The supervisor's `SubprocessHandle.kill` widens the signal
      // to `number | string`; Bun's `Subprocess.kill` accepts
      // `number | NodeJS.Signals`. The supervisor's call sites pass
      // `"SIGTERM"` / `"SIGKILL"` (recycle path) or no argument
      // (shutdown path), which Bun handles directly. Cast at the
      // boundary so the inner call matches Bun's narrower type
      // without coercing valid input.
      if (signal === undefined) {
        proc.kill();
        return;
      }
      if (typeof signal === "number") {
        proc.kill(signal);
        return;
      }
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- supervisor's kill widens to `string`; Bun's runtime accepts the same `"SIG*"` strings, narrowed back at the boundary.
      proc.kill(signal as NodeJS.Signals);
    },
    exited: proc.exited,
  };
};

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
   * frame). Delivered to the child on the pre-trigger barrier. Absent when the
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
   * Step ids in the deployed `WorkflowDefinition`'s `stepOrder`. The
   * `onRunStart` grants sink walks these to assemble the per-run
   * credentialsSnapshot from each step's `agent-state` repo, so the sink
   * needs the ordered ids rather than the bare count.
   */
  stepOrder: readonly string[];
  /** Deployment's mail address. */
  deploymentMailAddress: string;
  /** Per-step mail-address derivation. */
  deriveStepAddress: DeriveStepAddress;
  /**
   * Optional override of the per-step `agent-state` repo identity the
   * supervisor reads grants from while assembling the
   * credentialsSnapshot. Defaults to the `<runId>-<stepId>`
   * convention; the single-step launched-agent deploy supplies a
   * derivation that returns the legacy agent-state repo so the spawned
   * child reads grants from the same repo the legacy agent identity
   * keys.
   */
  deriveStepRepoId?: DeriveStepRepoId;
  /** Substrate-config keys propagated to the child via spawn-time env. */
  substrateEnv: Record<string, string>;
  /**
   * Dynamic spawn-env fragment the supervisor recomputes on every spawn and
   * recycle respawn (e.g. a live-rotated inference-source list). Its keys
   * layer over `substrateEnv`. See the `dynamicSpawnEnv` supervisor binding.
   */
  dynamicSpawnEnv: () => Record<string, string>;
  /**
   * Override the subprocess spawner. Tests inject a deterministic
   * mock; production defaults to the `Bun.spawn`-backed
   * `defaultSubprocessSpawner`.
   */
  subprocessSpawner?: SubprocessSpawner;
  /** Override the `bin/workflow-child` path. */
  binaryPath?: string;
  /**
   * Optional per-message dispatch-timing observer, forwarded verbatim to
   * the supervisor's `onDispatchTiming` binding. Absent in production;
   * the deploy router wires it (off a benchmark env gate) only for the
   * Phase 4.7 latency gate, which needs the supervisor to emit the
   * per-message infra round-trip from inside the sidecar subprocess.
   */
  onDispatchTiming?: (mark: DispatchTimingMark) => void;
  /**
   * D2 §10c forced-repack A/B toggle, forwarded verbatim to the
   * supervisor's `repackEveryMessages` binding. Absent in production;
   * the deploy router wires it (off the same benchmark env gate) only
   * for the D2 attribution run.
   */
  repackEveryMessages?: { everyMessages: number };
  /**
   * Consumed-dedup retention horizon (ms), forwarded to the
   * supervisor's `consumedRetentionMs` binding. The boot edge resolves
   * the operator's `CONSUMED_RETENTION_MS` config; absent, the
   * supervisor applies `DEFAULT_CONSUMED_RETENTION_MS` (24h).
   */
  consumedRetentionMs?: number;
  /**
   * Spawn ready-handshake timeout (ms), forwarded to the supervisor's
   * `readyTimeoutMs` binding. The boot edge resolves the operator's
   * `CHILD_READY_TIMEOUT_MS` config; absent, the supervisor applies
   * `DEFAULT_READY_TIMEOUT_MS` (30s).
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
   * Predicate consulted at the `onRunStart` grants barrier: returns `true`
   * for a runId whose `run.grants` write was attempted and FAILED, so the
   * per-run grants file the run needs never landed. The barrier fails such a
   * run loudly rather than starting it under-authorized. This is a
   * fast-fail with a precise cause: an absent grants file already fails the
   * barrier closed on its own (every run birth path writes the file before
   * dispatch, so its absence is a defect), and this predicate lets a run
   * whose write is KNOWN to have failed reject with that specific reason
   * instead of a generic missing-file error. Absent means no run is
   * poisoned.
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
 * inference-source failover chain from `frame.workflow.sources` down to
 * the workflow-process child. The substrate factory's `buildEnv` reads
 * this and resolves a step's chain at step invocation, feeding it to the
 * reactor for forward-only failover; the supervisor itself is opaque to
 * the value (it is plumbed through `bindings.substrateEnv` verbatim).
 *
 * Listed here so the router and the future substrate-factory consumer
 * spell the key the same way without a magic-string trip hazard.
 */
export const STEP_INFERENCE_SOURCES_ENV_KEY = "STEP_INFERENCE_SOURCES";

/**
 * Validate the wire-projected workflow definition at the deploy-router
 * boundary. The arktype `AgentDeployFrame` validator enforces the
 * wire shape (`id` is non-empty, `stepOrder` is `string[]`, `steps`
 * is an object, `sources` covers every `stepOrder` entry); this
 * function takes `unknown`-typed inputs so it can also gate callers
 * that bypass the wire boundary, and it enforces the invariants the
 * router and the downstream supervisor rely on:
 *
 *   - `definition.id` is a non-empty string. The arktype shape
 *     already enforces this on the wire; the re-check here protects
 *     bypass callers and keeps the failure shape consistent with the
 *     other invariants this function owns.
 *   - `definition.stepOrder` is non-empty. The wire shape admits
 *     `[]`; a zero-step workflow has no semantics here.
 *   - Every `stepOrder` entry matches `STEP_ID_PATTERN` so per-step
 *     mail-address derivation never needs escaping at the substrate
 *     boundary.
 *   - Every `stepOrder` entry has a corresponding `steps[id]` entry.
 *     The wire shape lets `steps[id]` be `unknown` and lets the
 *     entry be absent; presence is required so the workflow-process
 *     child can resolve each step's primitive at run time.
 *   - Every `stepOrder` entry has a corresponding `sources[id]`
 *     entry, and that entry is a non-empty array (the step's ordered
 *     failover chain). The arktype narrow already enforces both; the
 *     re-check here surfaces a structured router-side error instead of
 *     an arktype validation failure at the wire boundary, which keeps
 *     the failure shape consistent with the rest of the validations
 *     this function owns. An empty chain would leave the reactor with
 *     no initial source, so it is rejected here rather than deferred to
 *     a deep-stack child failure.
 *
 * A rejection here surfaces as a thrown `Error` the link's deploy
 * frame caller converts into a structured failure reply.
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
 * Derive the supervisor's principal public key from the sidecar's
 * Ed25519 signing seed. The supervisor signs every workflow-run event
 * with this key; the multi-step branch surfaces it to the link so the
 * hub records the verifying key for the deployment's signed events.
 */
async function derivePrincipalPublicKeyHex(
  signingKeySeed: Uint8Array,
): Promise<string> {
  return hexEncode(await derivePublicKeyBytes(signingKeySeed));
}

/**
 * The sidecar's `DeployRouter` plus the boot-time restore driver. The link
 * routes `agent.deploy`/`agent.undeploy` through the `DeployRouter` surface;
 * the sidecar boot edge additionally calls `restoreWorkflowRuns` once,
 * before connecting to the hub, to re-establish the deployments a prior
 * process persisted. The extra method is sidecar-app-only, so it rides on the
 * concrete router type rather than the shared `DeployRouter` contract.
 */
export interface SidecarDeployRouter extends DeployRouter {
  /**
   * Re-establish every persisted workflow deployment on this sidecar's local
   * substrate. Runs once at boot, before `hubLink.connect()`, so a single-step
   * head's mailbox/transport registration is live before the hub routes to it.
   * Soft-fails per deployment: a record that cannot be restored (unbuildable
   * provider, corrupt `workflow.json`, spawn failure) is logged and left on
   * disk for a later boot to retry -- it is never deleted here.
   */
  restoreWorkflowRuns(): Promise<void>;
  /**
   * The workflow-substrate deployment addresses (`run_<hex>@domain`) this router
   * currently hosts a live supervisor for -- the set of addresses this
   * sidecar can route mail to. The boot edge announces these to the hub on
   * (re)connect so the hub re-registers them for routing: they are hub-minted
   * and carry no per-address key, so unlike single-agent sessions they are
   * not re-established by the challenge flow, and without this announcement
   * the hub drops their route on a WS reconnect. Reflects `deploy`/`undeploy`
   * and boot-time restore live, so a caller re-reads it per connect.
   */
  activeAddresses(): string[];
  /**
   * Trigger B: re-register every correlation the deployment at `address` is
   * currently parked on, by asking its live supervisor to re-emit them to the
   * hub. The boot edge calls this per address the hub link re-routes on a
   * reconnect challenge (`onWorkflowAddressesRoutable`), so a register frame
   * the hub dropped from its bounded send queue during the outage is recovered.
   * The address-dispatch wrapper around the supervisor's own no-arg
   * `reEmitParkedCorrelations`; fire-and-forget (the driver is best-effort and
   * watchdog-bounded inside the supervisor).
   */
  reEmitParkedCorrelations(address: string): void;
}

export function createSidecarDeployRouter(deps: {
  sessions: SessionManager;
  keyStore: AgentKeyStore;
  transport: HubTransport;
  repoStore: RepoStore;
  signingKeySeed: Uint8Array;
  /**
   * Per-agent crypto factory. Receives the agent's raw key pair and
   * returns a `CryptoProvider` bound to it (production wires
   * `@intx/crypto`'s `createEd25519Crypto`). The multi-step branch
   * uses this to register the spawned single-step agent's signing key on
   * the host transport before `spawn()`, so the supervisor's outbound
   * mail path (`MailBusBindings.sendOutbound`) signs the agent's replies
   * with the AGENT's identity -- the OUTBOUND half of mailbox ownership
   * (§3a). Without this registration the spawned agent's address has no
   * `CryptoProvider` on the transport (nothing else registers one for
   * it), and an outbound send would throw "address is not registered"
   * rather than emit unsigned mail.
   */
  createAgentCrypto: (keyPair: KeyPair) => CryptoProvider;
  /**
   * Source-admission gate: throws if a step's pinned inference source
   * names a provider this sidecar cannot build. The buildable-provider
   * set is sidecar config (the boot edge's adapter registry), so this
   * admission control lives at the sidecar -- the hub is a different
   * process and cannot know a given sidecar's providers. Production wires
   * the default harness builder's `canBuildSource` verbatim, so a rejected
   * provider carries the same `"... is not registered"` message.
   *
   * Distinct from the orchestrator's operator-approval check
   * (`pickStepInferenceSource`): that gates on whether the operator
   * approved a `provider:model`; this gates on whether the provider is
   * buildable at all. A source can be approved yet unbuildable.
   */
  assertSourceBuildable: (source: InferenceSource) => void;
  /**
   * Record a `(runId -> agentAddress)` mapping the boot edge's
   * workflow-run pack push facade consults when it must address an
   * outbound pack frame. Fires once per inbound `agent.deploy` frame
   * before the deployment's supervisor spawns, so the first pack push
   * the child triggers sees the mapping. Tests that do not exercise
   * the pack push path may pass a no-op.
   */
  registerDeployment: (entry: { runId: string; agentAddress: string }) => void;
  /**
   * Symmetric removal hook for `registerDeployment`. Fires from the
   * link's `agent.undeploy` path so the boot edge's
   * `DeploymentAddressRegistry` drops the mapping when the deployment
   * is torn down. A subsequent stale `writeTreePreservingPrefix`
   * against the dead deployment's workflow-run ref surfaces
   * structurally (`registry.resolve` returns `null`) rather than
   * silently resolving to the prior address. Tests that do not
   * exercise the pack push path may pass a no-op.
   */
  unregisterDeployment: (entry: {
    runId: string;
    agentAddress: string;
  }) => void;
  /**
   * Substrate-config env keys the multi-step branch propagates into
   * the workflow-process child's spawn-time env (see
   * `SIDECAR_SUBSTRATE_CONFIG_KEYS` in `workflow-substrate-factory.ts`).
   * The router merges `STEP_INFERENCE_SOURCES` on top per multi-step
   * frame. Defaults to an empty record so a router built without
   * substrate config (e.g. a test) needs no boot-edge threading.
   */
  multistepSubstrateEnv?: Record<string, string>;
  /**
   * Subprocess spawner the multi-step branch hands to the supervisor.
   * Defaults to the production `Bun.spawn`-backed
   * `defaultSubprocessSpawner`; tests inject a deterministic mock.
   */
  multistepSubprocessSpawner?: SubprocessSpawner;
  /**
   * Optional override for the resolved `bin/workflow-child` path the
   * multi-step branch hands to the supervisor. Production wiring uses
   * the package-local default; tests inject a sentinel value so the
   * mock spawner can assert on it.
   */
  multistepBinaryPath?: string;
  /**
   * Callback the supervisor invokes for every verified InferenceEvent
   * the workflow-process child publishes. The router threads the
   * deployment's run address plus the deploy's session id through to
   * the callback so a downstream fan-out can route each event to the
   * hub timeline keyed to the right session. The `InferenceEvent` itself
   * is sessionless; the session id rides alongside it, sourced from the
   * deploy frame's `HarnessConfig.sessionId` per deployment. It is
   * optional because a deploy frame need not carry a session id (a
   * headless deployment with no hub-side session); the sink decides what
   * an absent session id means. Defaults to a no-op; production wiring
   * supplies the event publisher.
   */
  publishWorkflowInferenceEvent?: (
    agentAddress: string,
    event: InferenceEvent,
    sessionId: string | undefined,
  ) => void;
  /**
   * Callback the supervisor invokes for every control-plane suspension a
   * workflow-process child reports (`park.notify`). The multi-step branch
   * threads it into the supervisor's `onSuspensionRegister` binding so a
   * parked run's correlation is registered at the hub (routing + approval
   * rows). The supervisor stamps `runId` + `agentAddress` before
   * invoking it. Defaults to a no-op; production wiring supplies the
   * hub-link-backed publisher.
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
   * derivation. Defaults to `${runId}-${stepId}@<deploymentDomain>`
   * derived from the frame's run address. Tests inject a deterministic
   * factory.
   */
  multistepDeriveStepAddress?: DeriveStepAddress;
  /**
   * Per-deployment-address mail handler registry the hub-link's
   * `mail.inbound` path consults before falling back to the legacy
   * session-routed delivery. The multi-step branch registers
   * `wired.routeInbound` against the deployment's mail address once
   * `supervisor.spawn` succeeds so inbound mail aimed at the
   * deployment address flows into the supervisor's mail-bus
   * subscription.
   *
   * Optional so tests that exercise the multi-step branch without an
   * end-to-end mail loop can omit the binding; an absent registry
   * simply means multi-step inbound mail cannot route through the
   * hub-link until the wiring is plumbed.
   */
  multistepMailRouter?: MultistepMailRouter;
  /**
   * Per-deployment-address signal handler registry the sidecar
   * hub-link's `signal.deliver` path consults. The multi-step branch
   * registers `wired.supervisor.deliverSignal` against the deployment's
   * mail address once `supervisor.spawn` succeeds so a hub-side
   * `signal.deliver` frame flows into the workflow-process child via
   * the IPC's `signal.deliver` payload. The child commits the
   * resulting `SignalReceived` event through its own substrate,
   * preserving the workflow-run repo's single-writer invariant on the
   * sidecar side.
   *
   * Optional so tests that exercise the multi-step branch without an
   * end-to-end signal loop can omit the binding; an absent registry
   * means hub-side signals cannot route through the hub-link until the
   * wiring is plumbed.
   */
  multistepSignalRouter?: MultistepSignalRouter;
  /**
   * Per-deployment-address drain handler registry the sidecar
   * hub-link's `drain.deliver` path consults. The multi-step branch
   * registers `wired.supervisor.drain` against the deployment's mail
   * address once `supervisor.spawn` succeeds so a hub-side
   * `drain.deliver` frame flows into the workflow-process child via
   * the IPC's `drain` payload and arms the supervisor's per-run
   * `drainTimeout` accumulators. Cancel-mode in-flight steps abort on
   * the child side; wait-mode steps continue. Accumulators commit a
   * signed `CancelRequested{origin: "supervisor-drain"}` against the
   * workflow-run repo when the deadline expires.
   *
   * Optional so tests that exercise the multi-step branch without an
   * end-to-end drain loop can omit the binding; an absent registry
   * means hub-side drain frames cannot route through the hub-link until
   * the wiring is plumbed.
   */
  multistepDrainRouter?: MultistepDrainRouter;
  /**
   * Per-deployment-address grants handler registry the sidecar
   * hub-link's `run.grants` path consults. The deploy router registers a
   * handler against the deployment's mail address once `supervisor.spawn`
   * succeeds, for single-step and multi-step deployments alike, so a
   * hub-side `run.grants` frame writes the run's grants to
   * `runs/<runId>/grants.json` inside the deployment's `workflow-run`
   * repo. The write is awaited in `tryRoute` so the frame's FIFO
   * completion means the grants are durable on disk.
   *
   * Optional so tests that exercise the multi-step branch without an
   * end-to-end grants loop can omit the binding; an absent registry
   * means hub-side grants frames cannot route through the hub-link until
   * the wiring is plumbed.
   */
  multistepGrantsRouter?: MultistepGrantsRouter;
  /**
   * Per-deployment-address sources-rotation handler registry. Only a
   * single-step warm deployment registers a handler (against the
   * deployment's mail address once `supervisor.spawn` succeeds) so a
   * rotation resolved for its address flows into
   * `wired.supervisor.deliverSources` and on to the child's warm agent. A
   * multi-step deployment registers none -- it has no single warm agent to
   * rotate -- so `tryRoute` reports its address as unrouted.
   *
   * Optional so tests that exercise deploys without a rotation loop can
   * omit the binding; an absent registry means no rotation handler is
   * installed for any deployment.
   */
  multistepSourcesRouter?: MultistepSourcesRouter;
  /**
   * Optional per-deployment credential-delivery handler registry. Every
   * deployment with a supervisor registers a handler after `spawn` (not only
   * warm single-step ones -- the material cell is per-child and read by every
   * step's tool capabilities), and an inbound `credentials.update` for an
   * unregistered (torn-down) address is unrouted. Optional so tests without a
   * credential-delivery loop can omit the binding.
   */
  multistepCredentialsRouter?: MultistepCredentialsRouter;
  /**
   * Optional per-message dispatch-timing observer the multi-step branch
   * forwards to each supervisor it constructs. Resolved at the sidecar
   * boot edge from the Phase 4.7 latency-gate env gate; absent in
   * ordinary production. The supervisor runs in this sidecar subprocess,
   * so the observer sees both ends of the per-message IPC round-trip in
   * one process and can emit a parseable timing line the benchmark
   * harness reads off the subprocess's output stream.
   */
  onDispatchTiming?: (mark: DispatchTimingMark) => void;
  /**
   * D2 §10c forced-repack A/B toggle the multi-step branch forwards to
   * each supervisor it constructs. Resolved at the sidecar boot edge from
   * the same benchmark env gate; absent in ordinary production.
   */
  repackEveryMessages?: { everyMessages: number };
  /**
   * Consumed-dedup retention horizon (ms) forwarded to every supervisor
   * the router constructs. The sidecar boot edge resolves the operator's
   * `CONSUMED_RETENTION_MS` config; absent, the supervisor applies
   * `DEFAULT_CONSUMED_RETENTION_MS` (24h). See the workflow-run kind
   * handler for the operator-owned horizon invariant.
   */
  consumedRetentionMs?: number;
  /**
   * Spawn ready-handshake timeout (ms) forwarded to every supervisor the
   * router constructs. The sidecar boot edge resolves the operator's
   * `CHILD_READY_TIMEOUT_MS` config; absent, the supervisor applies
   * `DEFAULT_READY_TIMEOUT_MS` (30s). A child that spawns but never
   * signals ready is killed and its spawn rejected rather than hanging
   * the deploy or boot-time restore.
   */
  readyTimeoutMs?: number;
  /**
   * Run-record writer, injectable so a test can block or fail the
   * persist at a controlled point -- the natural seam for exercising a
   * recycle that interleaves the source-rotation persist window. Defaults
   * to the real `writeWorkflowRunRecord`; production never overrides
   * it.
   */
  writeWorkflowRunRecord?: typeof writeWorkflowRunRecord;
  /**
   * Materialize a source-ref deployment's frozen closure. Defaults to the real
   * `applyFrozenWorkflowClosure` (registry fetch + SRI verify + layout);
   * production never overrides it. A test seam so a unit test can drive the
   * deploy/restore source-ref path without a live registry.
   */
  applyFrozenWorkflowClosure?: typeof applyFrozenWorkflowClosure;
}): SidecarDeployRouter {
  // Validate the signing seed at construction so a malformed key fails
  // sidecar boot rather than the first multi-step deploy, where the
  // public key is derived from it (`derivePrincipalPublicKeyHex`). The
  // seed also signs every workflow-run event via the supervisor.
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
  // Sidecar data dir the deployment's per-step scratch is rooted under
  // (`<dataDir>/workflow-step-state/<runId>/...`). Resolved once
  // from the boot-edge substrate env so the undeploy hook can reclaim
  // the whole subtree. Absent only when the router is wired without
  // substrate config (a test that never spawns a child), in which case
  // no child ever rooted scratch and the undeploy reclaim is correctly
  // skipped.
  const stepStateDataDir = multistepSubstrateEnv.SIDECAR_DATA_DIR;
  const persistWorkflowRunRecord =
    deps.writeWorkflowRunRecord ?? writeWorkflowRunRecord;
  const applyClosure =
    deps.applyFrozenWorkflowClosure ?? applyFrozenWorkflowClosure;
  const multistepSpawner =
    deps.multistepSubprocessSpawner ?? defaultSubprocessSpawner;
  const multistepDeriveStepAddress: DeriveStepAddress =
    deps.multistepDeriveStepAddress ??
    (({ runId, stepId }) => `${runId}-${stepId}`);

  // Per-deployment supervisor tracking. The multi-step branch
  // constructs one `SidecarWorkflowSupervisor` per `agent.deploy`
  // frame; the supervisor owns the workflow-process child, its IPC
  // pipes, and its event-channel fd. The undeploy hook consults this
  // map to call `supervisor.shutdown()` so the child's lifetime ends
  // with the deployment.
  const activeSupervisors = new Map<string, SidecarWorkflowSupervisor>();

  // Synchronous single-flight guard for the deploy path. The real supervisor
  // does not exist until inside `spawnWorkflowRun`, so `deployMultiStep`
  // cannot reserve its `activeSupervisors` slot up front; instead it records
  // the address here synchronously, before its first await, and clears it in a
  // finally once the deploy settles. `activeSupervisors` is populated only
  // after `spawn` succeeds, so the has-check alone leaves a window in which two
  // same-address frames both pass and the loser's unwind deletes the winner's
  // live run record. This set closes that window: a second frame that
  // arrives while the first is mid-deploy is rejected before it touches any
  // durable state. Only the live deploy path reserves; the boot restore path
  // is serial and relies on the `activeSupervisors` backstop instead.
  const reservingDeployAddresses = new Set<string>();

  // Slug-collision tracking. `deriveDeploymentId` substitutes
  // disallowed characters with `-`, which is deterministic but lossy:
  // two distinct run addresses can collapse to the same slug, and
  // a collision would let the second deploy silently overwrite the
  // first deploy's workflow-run repo state (the slug IS the repoId).
  // This map records the first-claimer; a subsequent deploy that
  // produces the same slug from a different address is rejected at
  // the router before any supervisor or repo state is touched.
  const slugClaims = new Map<string, string>();

  function claimSlug(runId: string, agentAddress: string): void {
    const existing = slugClaims.get(runId);
    if (existing !== undefined && existing !== agentAddress) {
      throw new Error(
        `deriveDeploymentId collision: run addresses ${JSON.stringify(existing)} and ${JSON.stringify(agentAddress)} both project to runId ${JSON.stringify(runId)}`,
      );
    }
    // A same-address re-claim is a defensive no-op: the `activeSupervisors`
    // guard rejects a live re-deploy before claimSlug is re-invoked, and a
    // failed or undeployed deploy releases the slug first, so in practice
    // `existing` is only ever undefined or a different address here.
    slugClaims.set(runId, agentAddress);
  }

  function releaseSlug(runId: string, agentAddress: string): void {
    const existing = slugClaims.get(runId);
    if (existing === agentAddress) slugClaims.delete(runId);
  }

  /**
   * Materialize an extracted onTrigger body's per-step inference-source pins to
   * `${dataDir}/assets/workflow/<bodyRef>/sources.json`. A body child runs
   * in-process with no process env and loses its env across a restart, so its
   * sources must be durable on disk; the body invoker reads this file to build
   * the body's inference-source resolver (INTR-310). The body DEFINITION is not
   * staged: source-ref is the only lineage, so the run child resolves each body
   * in-memory from the parent's re-verified closure. Idempotent
   * content-compare write.
   */
  async function materializeWorkflowSources(
    sidecarDataDir: string | undefined,
    definitionId: string,
    sources: NonNullable<AgentDeployFrame["workflow"]>["sources"],
  ): Promise<void> {
    if (typeof sidecarDataDir !== "string" || sidecarDataDir.length === 0) {
      throw new Error(
        "sidecar deploy router: SIDECAR_DATA_DIR must be present in the multi-step substrate env; the workflow-process child resolves the workflow-asset repo dir against this data dir",
      );
    }
    const sourcesAssetPath = pathJoin(
      sidecarDataDir,
      "assets",
      "workflow",
      definitionId,
      "sources.json",
    );
    const sourcesAssetBytes = JSON.stringify(sources, null, 2);
    try {
      await mkdir(dirname(sourcesAssetPath), { recursive: true });
      // Idempotent: only rewrite when the on-disk content differs. Treats a
      // missing file as different.
      let existing: string | null = null;
      try {
        existing = await readFile(sourcesAssetPath, "utf8");
      } catch (cause) {
        if (
          !(
            cause instanceof Error &&
            "code" in cause &&
            (cause as { code: unknown }).code === "ENOENT"
          )
        ) {
          throw cause;
        }
      }
      if (existing !== sourcesAssetBytes) {
        await writeFile(sourcesAssetPath, sourcesAssetBytes, "utf8");
      }
    } catch (cause) {
      const reason = cause instanceof Error ? cause.message : String(cause);
      throw new Error(
        `sidecar deploy router: failed to materialize sources.json at ${sourcesAssetPath}: ${reason}`,
        { cause },
      );
    }
  }

  /**
   * The per-deployment inputs the shared spawn core needs to stand up a
   * workflow deployment, independent of the live deploy frame. The live
   * deploy path builds this from `frame`/`projection`; a boot-time restore
   * path builds the same shape from the persisted run record.
   */
  interface WorkflowDeploySpec {
    agentAddress: string;
    /**
     * The runnable definition, projected to its inert wire shape. Source-ref is
     * the only deploy lineage, so this is always the closure evaluation
     * (`projectLiveToInert(applied.definition)`), NOT a frame-carried inline
     * definition (the deploy frame carries none). Both the deploy path and the
     * boot-time restore derive it the same way, from the materialized closure.
     */
    definition: WorkflowProjectionDefinition;
    sources: NonNullable<AgentDeployFrame["workflow"]>["sources"];
    /**
     * The hub-approved wire hash the deploy frame carried
     * (`AgentDeployWorkflow.approvedWireHash`). The child's `DEFINITION_HASH`
     * is sourced from this hub authority, NOT a sidecar recompute. Undefined
     * only for a frame that carried no approved hash on the wire; the shared
     * spawn core fails closed rather than substitute a recompute.
     */
    approvedWireHash: string | undefined;
    /**
     * Hub-approved wire hash per referenced onTrigger body id, threaded to the
     * child (via the substrate env) so a body child re-verifies its recompute
     * against the hub authority. Empty when the deploy carried no referenced
     * bodies with approved hashes.
     */
    referencedDefinitionHashes: Record<string, string>;
    /** Correlates the child's inference events to the deploy's session. */
    sessionId: string | undefined;
    /**
     * Hub public key recorded at the head for deploy-pack verification and
     * inbound hub-frame verification. Required for a single-step
     * deployment (whose head IS the agent identity); undefined for a
     * genuine multi-step deployment, which derives per-step addresses and
     * records no head key.
     */
    hubPublicKey: string | undefined;
    /**
     * Sidecar-local directory of the materialized workflow-definition closure,
     * set after the frozen closure is applied. The spawn core threads it into
     * the child's spawn env so the run child evaluates the pinned code to a live
     * definition. Sidecar-local: it never travels on the hub deploy frame and is
     * not persisted to the deployment record. Always present -- source-ref is
     * the only deploy lineage.
     */
    closurePackageDir: string;
    /**
     * The source-ref pin, carried so `buildWorkflowRunRecord` can persist it and
     * a boot-time restore can re-run `applyFrozenWorkflowClosure` to
     * re-materialize the pinned code. Its `source` carries no secret (the
     * registry token is resolved from env at apply time); its `closure` is
     * frozen versions + SRIs.
     */
    sourceRef: NonNullable<AgentDeployFrame["workflow"]>["sourceRef"];
  }

  /**
   * Build the durable run record from a spec and a source table. The
   * table is a parameter (not `spec.sources`) so the deploy path writes the
   * deploy-time sources while the rotation handler writes the live-rotated
   * ones -- both through one shape, so a rotation persists the same record a
   * boot-time restore reseeds from.
   */
  function buildWorkflowRunRecord(
    spec: WorkflowDeploySpec,
    sources: WorkflowRunRecord["sources"],
  ): WorkflowRunRecord {
    // The record schema requires both for a source-ref record -- the only
    // lineage -- so a spec missing either is a wiring defect. Fail loudly here
    // rather than persist a record the boot scan would then reject as corrupt.
    if (spec.approvedWireHash === undefined) {
      throw new Error(
        `buildWorkflowRunRecord: a source-ref deployment (${spec.agentAddress}) must carry approvedWireHash`,
      );
    }
    // The per-body approved hashes are re-threaded into the child's spawn env on
    // restore so the onTrigger-body re-verify barrier survives a restart;
    // omitted when the deployment has no bodies, matching the "no null entry"
    // shape the deploy-time populate uses.
    return {
      version: 1 as const,
      agentAddress: spec.agentAddress,
      definitionId: spec.definition.id,
      sources,
      ...(spec.sessionId !== undefined ? { sessionId: spec.sessionId } : {}),
      ...(spec.hubPublicKey !== undefined
        ? { hubPublicKey: spec.hubPublicKey }
        : {}),
      ...(Object.keys(spec.referencedDefinitionHashes).length > 0
        ? { referencedDefinitionHashes: spec.referencedDefinitionHashes }
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
   * instance dir and return the applied result. Owns the plumbing both the
   * deploy path and the boot-time restore path share: the deterministic
   * instance dir under `<dataDir>/workflow-definition-closures/<deploymentId>`,
   * the content-addressed cache root, the two substrate byte caps, and the
   * registry table. The caller consumes the result: deploy validates
   * `applied.definition` and takes `packageDir`; restore takes `packageDir` and
   * re-carries the pin on its spec.
   *
   * The instance dir is force-reclaimed before the apply. `deploymentId` is
   * deterministic per agent address, so a redeploy or a boot-restore reuses the
   * same dir; a prior soft-failed deploy or a dead prior process can leave it
   * half-materialized. The rm makes the apply write into a clean dir either
   * way (a no-op on a never-deployed address). It is safe to reclaim only
   * because no live reader holds the dir when this runs -- a precondition each
   * caller establishes and notes at its call site.
   */
  async function materializeDeploymentClosure(
    dataDir: string,
    deploymentId: string,
    pin: SourceRefPin,
  ): Promise<AppliedWorkflowClosure> {
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
      await resolveDeploymentAssetMounts(dataDir, deploymentId, pin);

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
      registries: readRegistries(),
      assetRoot,
      assetMounts,
      gitDirs,
    });
  }

  /**
   * The single owner of the workflow-deployment spawn sequence: construct
   * the supervisor, register the single-step agent's outbound key + head
   * repo + hub key, spawn the workflow-process child, then register the
   * live deployment (supervisor, mail/signal/drain routers, address
   * mapping). Its `try/finally` unwinds every piece of partial state if any
   * step throws, so a failed spawn leaks nothing. Both the live deploy path
   * and the boot-time restore path route through here so the two can never
   * diverge on how a deployment is stood up. Callers materialize the
   * deploy-only durable state (`workflow.json`, step grants) before calling.
   */
  async function spawnWorkflowRun(
    spec: WorkflowDeploySpec,
    // Decrypted credential material from the deploy frame, delivered to the
    // child on the pre-trigger barrier. Threaded as a separate arg rather than
    // on `spec` so it never reaches the on-disk run record (the sidecar
    // holds no cipher; a persisted credential would be plaintext at rest). The
    // boot-restore path passes none -- a restored in-flight deployment gets its
    // material from the hub's reconnect re-push, not off disk.
    credentialDelivery?: CredentialDelivery,
  ): Promise<DeployRouterResult> {
    // Fail loud if this address already has a live supervisor. Both single-
    // and multi-step now register on the transport, so both carry the
    // `transport.register` duplicate-throw backstop; this `has()` check is the
    // primary early guard that gives a clean error before that lower-level
    // throw and before the `activeSupervisors.set` below could clobber the
    // running deployment's handle. Both the deploy path and the boot restore
    // path route through here, so this is the single transition guard against
    // a double-spawn -- notably a boot restore racing a legacy restore for the
    // same address (the B-reroute follow-up relies on it).
    if (activeSupervisors.has(spec.agentAddress)) {
      throw new Error(
        `sidecar deploy router: a supervisor is already active for ${spec.agentAddress}; refusing to spawn a second`,
      );
    }
    const runId = deriveDeploymentId(spec.agentAddress);

    // Single-step launched-agent deploy vs. derived multi-step deploy. A
    // one-step deployment keeps the deployment's own (legacy) mail address
    // and its grants in the legacy agent-state repo keyed by the legacy
    // instance id. A multi-step deployment derives `<runId>-<stepId>`
    // per step for both the mail address and the agent-state repo id.
    const stepStrategy = createStepStrategy({
      legacyAddress: spec.agentAddress,
      stepOrder: spec.definition.stepOrder,
      multistepDeriveStepAddress,
    });

    // Unwind every piece of spawn state if any step in this block throws,
    // so a failed spawn leaks no freshly-spawned workflow-process child,
    // `activeSupervisors` entry, transport registration, or multistep
    // router registration. (The deployment-address registration happens
    // before spawn and is unwound by its own guard.) The ordering inside
    // the finally is the reverse of the success-path registration order.
    // The caller owns the deployment slug: it must
    // claim the collision guard before any durable write and release it on
    // failure, so the slug is not touched here.
    let succeeded = false;
    let wiredForUnwind: SidecarWorkflowSupervisor | undefined;
    let supervisorRegistered = false;
    let routersRegistered = false;
    let agentTransportRegistered = false;
    let hubKeyRecorded = false;
    let deploymentRegistered = false;
    try {
      // The child's `DEFINITION_HASH` is the HUB-APPROVED wire hash the deploy
      // frame carried (`spec.approvedWireHash`) -- the hub is the authority, so
      // the child re-verifies its own recompute against it. Both feeds into this
      // core carry it: the production hub deploy builder always stamps it, and
      // the boot restore re-attaches it from the persisted record. A missing
      // hash here is therefore a wiring bug, not a legacy case to paper over:
      // substituting a sidecar recompute would make the child re-verify against
      // the sidecar's own hash rather than the hub authority -- a circular check
      // that gives false assurance. Fail loud instead.
      if (spec.approvedWireHash === undefined) {
        throw new Error(
          `workflow deploy spawn (${spec.agentAddress}): the deploy spec carries no approvedWireHash. The hub deploy builder must stamp the hub-approved wire hash and a restore must re-attach it from the persisted record; the sidecar will not recompute it, which would collapse the child's re-verify to a self-check.`,
        );
      }
      const definitionHash = spec.approvedWireHash;

      // Per-deployment substrate-config keys the workflow-substrate-factory
      // validator requires. The boot edge's `multistepSubstrateEnv` carries
      // the boot-edge constants; the four workflow-definition / workflow-run
      // identity keys are derived per-deploy here.
      const substrateEnv: Record<string, string> = {
        ...multistepSubstrateEnv,
        WORKFLOW_DEFINITION_REPO_ID: spec.definition.id,
        WORKFLOW_DEFINITION_REF: "refs/heads/main",
        WORKFLOW_RUN_REPO_ID: runId,
        WORKFLOW_RUN_REF: "refs/heads/main",
        // Thread the hub-approved per-body wire hashes to the child so a body
        // child re-verifies its recompute against the hub authority. Carried on
        // the frozen substrate env (not the dynamic fragment) because the map
        // is fixed for the deployment's lifetime. Omitted when empty so a
        // deployment with no referenced bodies ships no key.
        ...(Object.keys(spec.referencedDefinitionHashes).length > 0
          ? {
              REFERENCED_DEFINITION_HASHES: JSON.stringify(
                spec.referencedDefinitionHashes,
              ),
            }
          : {}),
        // Thread the materialized closure's sidecar-local package dir so the
        // run child EVALUATES the pinned code to a live definition and
        // re-verifies by project-then-hash against `DEFINITION_HASH`. Source-ref
        // is the only deploy lineage, so this is always present. Sidecar-local;
        // carried on the frozen substrate env because the value is fixed for the
        // deployment's lifetime.
        CLOSURE_PACKAGE_DIR: spec.closurePackageDir,
      };
      // Live-rotatable per-step inference sources. Seeded from the deploy
      // spec, then revised in place by the single-step sources-rotation
      // handler below. `STEP_INFERENCE_SOURCES` is NOT in the frozen
      // `substrateEnv`: it is recomputed on every spawn and recycle respawn
      // via `dynamicSpawnEnv`, so a rotation survives a recycle instead of
      // reverting to the deploy-time list.
      let currentSources = spec.sources;

      // RunIds whose `run.grants` write was attempted and failed. The grants
      // handler below records a runId here on a write failure; the grants
      // barrier reads it through `isRunPoisoned` and fails the run rather than
      // starting it under the empty deploy-time grant set. Scoped to this
      // deployment's supervisor; a run that never had a `run.grants` frame is
      // never added, so internal runs inherit the deployment's grants normally.
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
        stepOrder: spec.definition.stepOrder,
        deploymentMailAddress: spec.agentAddress,
        ...(credentialDelivery !== undefined ? { credentialDelivery } : {}),
        deriveStepAddress: stepStrategy.deriveStepAddress,
        deriveStepRepoId: stepStrategy.deriveStepRepoId,
        isRunPoisoned: (runId) => poisonedRunIds.has(runId),
        // The supervisor stamps `runId` + `agentAddress` before
        // invoking this; forward the fully-stamped registration to the
        // hub-link-backed publisher so a `signal.correlation.register` frame
        // reaches the hub for the parked run.
        onSuspensionRegister: publishSuspension,
        substrateEnv,
        // Recomputed on every spawn AND recycle respawn. The rotation
        // handler below revises `currentSources` in place, so a respawn
        // re-serializes the current (possibly rotated) list rather than the
        // frozen deploy-time value.
        dynamicSpawnEnv: () => ({
          [STEP_INFERENCE_SOURCES_ENV_KEY]: JSON.stringify(currentSources),
        }),
        subprocessSpawner: multistepSpawner,
        ...(deps.multistepBinaryPath !== undefined
          ? { binaryPath: deps.multistepBinaryPath }
          : {}),
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

      // OUTBOUND half of mailbox ownership (§3a): register a signing key for
      // the deployment mail address on the host transport so the supervisor
      // signs the deployment's outbound mail. Every step -- single- or
      // multi-step -- signs its outbound sends as `spec.agentAddress` (the
      // one deployment mail address; no per-step sender reaches the host
      // transport), so the transport MUST hold a `CryptoProvider` for it or
      // `getTransportFor(senderAddress).send` throws "not registered".
      // Registration happens before `spawn()` so the address is live the
      // instant the first reply routes outbound.
      const { keyPair } = await deps.keyStore.loadOrGenerateKey(
        spec.agentAddress,
      );
      deps.transport.register(
        spec.agentAddress,
        deps.createAgentCrypto(keyPair),
      );
      agentTransportRegistered = true;

      // The public key the deploy ack surfaces to the hub is the deployment
      // address's own Ed25519 key -- the one `loadOrGenerateKey` minted above,
      // which `AgentKeyStore.signChallenge(spec.agentAddress)` also signs
      // reconnect challenges with. EVERY deployment acks it, single- and
      // multi-step alike, so the hub can verify the reconnect ownership
      // challenge for both: a single-step head records it into
      // `agent_instance.publicKey`; a workflow-derived deployment records it on
      // its `workflow_deployment` row. A multi-step deployment previously acked
      // the supervisor principal key -- which the hub discarded and which does
      // NOT match what `signChallenge` signs with -- so its address could be
      // re-claimed on reconnect without proof; carrying the deployment key
      // closes that.
      const deploymentPublicKey = hexEncode(keyPair.publicKey);
      if (spec.definition.stepOrder.length === 1) {
        // A single-step workflow stages its deploy tree at the head (the
        // lone step IS the head). Initialize the head's on-disk deploy-tree
        // repo (idempotent) so the hub's deploy-pack push has a repo to
        // apply into. The narrow `initRepo` (not `provisionAgent`) is
        // deliberate: the supervised child mints its own keypair and
        // persists no hub-agent config.
        await deps.sessions.initRepo(spec.agentAddress);

        // Record the hub's public key at the head so the deploy-pack apply
        // (and any inbound hub-signed frame) verifies against it. The
        // verifier resolves the key from the in-memory key store's
        // `recordHubKey` map, so a single-step deployment cannot stand up
        // without it.
        if (spec.hubPublicKey === undefined) {
          throw new Error(
            "sidecar deploy router: a single-step workflow deployment requires a hubPublicKey to record at the head; none was supplied",
          );
        }
        deps.keyStore.recordHubKey(spec.agentAddress, spec.hubPublicKey);
        hubKeyRecorded = true;
      }

      const stepOrder = [...spec.definition.stepOrder];
      // Warm-keep is the single-step launched-agent deploy: the sole step
      // IS the long-lived agent, so the child warm-keeps it across
      // messages. A multi-step deploy keeps instantiate-send-teardown per
      // step. The signal is carried explicitly down through the spawn env.
      const warmKeep = spec.definition.stepOrder.length === 1;
      const spawnOpts: SpawnOpts = {
        stepOrder,
        definitionHash,
        warmKeep,
        onInferenceEvent: (event) => {
          // The event arrives HMAC-verified over the child's event channel.
          // Re-narrow it to the hub's `InferenceEvent` union; a parse
          // failure means upstream corruption, so drop it loudly rather
          // than forwarding an unvalidated payload onto the hub timeline.
          const validated = parseInferenceEvent(event);
          if (validated instanceof type.errors) {
            logger.warn`dropping workflow inference event for ${spec.agentAddress}: ${validated.summary}`;
            return;
          }
          publishInferenceEvent(spec.agentAddress, validated, spec.sessionId);
        },
      };

      // Record the deployment-address mapping BEFORE `spawn`, because
      // `spawn` kicks off `replayProcessingToInbox`, whose workflow-run
      // substrate write routes through the boot-edge pack-pushing facade and
      // resolves this mapping to address the outbound pack frame. Recording
      // it after `spawn` (as the other registrations below are) loses the
      // race: the replay's write throws "no run address registered" (a
      // real defect masked as a swallowed best-effort warning in the
      // supervisor's replay catch). Constraint ownership: the registry owns
      // "address is resolvable"; the spawn path must satisfy that contract
      // before the replay writes. The finally unwinds it on any failure
      // between here and the end of the try.
      deps.registerDeployment({
        runId,
        agentAddress: spec.agentAddress,
      });
      deploymentRegistered = true;

      // Surface spawn-time errors structurally: a subprocess spawner that
      // crashes immediately rejects here, and the caller converts the
      // rejection into a structured failure frame. The supervisor is
      // registered against the deployment address only after spawn succeeds,
      // so a spawn-time rejection leaves the registry untouched.
      await wired.supervisor.spawn(spawnOpts);
      wiredForUnwind = wired;
      activeSupervisors.set(spec.agentAddress, wired);
      supervisorRegistered = true;

      // Bind the deployment's mail address to this supervisor's
      // `routeInbound` so the hub-link dispatches inbound mail into the
      // supervisor's mail-bus subscription. Registration happens after
      // `spawn` succeeds so a spawn-time rejection leaves the registry
      // untouched.
      deps.multistepMailRouter?.register(spec.agentAddress, (message) =>
        wired.routeInbound(message),
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
      // run's grants to `runs/<runId>/grants.json` in the deployment's
      // workflow-run repo. The `runId` selects the per-run destination; the
      // step-fan-out fields are inert in that mode but the shared write
      // machinery still takes them.
      deps.multistepGrantsRouter?.register(spec.agentAddress, async (args) => {
        try {
          await writeStepGrants({
            repoStore: deps.repoStore,
            anchorRunId: runId,
            stepOrder: spec.definition.stepOrder,
            deriveStepRepoId: stepStrategy.deriveStepRepoId,
            grants: args.stepGrants,
            runId: args.runId,
          });
        } catch (cause) {
          // The per-run grants file did not land. Poison the runId so the
          // grants barrier fails the run instead of starting it under the
          // deploy-time grant set, then re-throw so the hub-link logs the
          // durable-write failure loudly.
          poisonedRunIds.add(args.runId);
          throw cause;
        }
      });
      // Register the sources-rotation handler ONLY for a single-step warm
      // deployment: it has one long-lived agent whose sources can be
      // swapped in place. A multi-step deployment has no single warm agent,
      // so it registers no handler and `tryRoute` reports its address as
      // unrouted.
      if (warmKeep) {
        // A single-step deployment's source table has exactly one entry,
        // keyed by the head step. Derive that key once here (the layer that
        // owns the single-key invariant); `deliverSources` stays flat and
        // stepId-agnostic.
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
            // Swap `currentSources` synchronously BEFORE the durable persist.
            // `currentSources` is the process-local respawn hint the
            // supervisor reads synchronously through `dynamicSpawnEnv`, so a
            // recycle that interleaves the persist `await` must respawn the
            // child on the SAME sources being persisted, not the stale prior
            // table. The obvious inverse -- persist first, then swap -- is
            // rejected: it leaves the child on the OLD sources during the
            // persist window while the record has already moved to NEW, so a
            // recycle there respawns stale and a restart would "correct" it,
            // i.e. the running child contradicts durable intent. Swapping
            // first makes the only residual disagreement child-ahead-of-
            // durable on a failed persist, which the next recycle heals down
            // to the rolled-back durable truth -- the benign direction. The
            // wire boundary guarantees `args.sources[0]` is the default,
            // which the recycle env form pins as the active source.
            const prevSources = currentSources;
            currentSources = rotated;
            // The durable write still precedes the LIVE swap
            // (`deliverSources`), preserving persist-before-externally-visible
            // for state that outlives the process; only the process-local
            // respawn hint moves ahead. On a failed persist, roll the hint
            // back so `currentSources` and the record stay in agreement in the
            // common (no interleaved recycle) failure case -- the invariant
            // restart consistency depends on. Persistence lets the rotation
            // survive a full sidecar restart, not just a recycle: the boot
            // scan reseeds spec.sources from record.sources. Overwrites the
            // deploy-time record in place. Skipped when no data dir was wired
            // (a test router that never persists), matching the restore guard.
            if (stepStateDataDir !== undefined) {
              try {
                await persistWorkflowRunRecord(
                  stepStateDataDir,
                  runId,
                  buildWorkflowRunRecord(spec, rotated),
                );
              } catch (cause) {
                // Restoring unconditionally is safe because rotations for one
                // deployment are serialized by the sidecar's per-connection
                // inbound-frame queue: each hub frame, sources.update
                // included, runs its handler to completion on that queue
                // before the next frame's handler starts, so no second
                // rotation is in flight whose committed table this rollback
                // could clobber. This does NOT rely on the hub pacing its
                // sends -- the hub dispatches sources.update fire-and-forget;
                // the sidecar frame queue is the sole serializer. Parallelizing
                // inbound-frame dispatch would break this rollback.
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

      // Register the credential-delivery handler for EVERY deployment (not only
      // warm single-step ones): the material cell is per-child and read by
      // every step's tool capabilities. The handler hands the delivery to the
      // supervisor's `deliverCredentials`, which sends a `credentials-updated`
      // control frame to the child where the material cell is swapped. No
      // durable persist -- credential material never touches disk.
      deps.multistepCredentialsRouter?.register(
        spec.agentAddress,
        async (args) => {
          await wired.supervisor.deliverCredentials({
            delivery: args.delivery,
          });
        },
      );
      routersRegistered = true;

      succeeded = true;
      return { publicKey: deploymentPublicKey };
    } finally {
      if (!succeeded) {
        // Unwind in reverse registration order so each step undoes state
        // the success path confirmed; ordering matches the `undeploy` hook.
        if (routersRegistered) {
          deps.multistepMailRouter?.unregister(spec.agentAddress);
          deps.multistepSignalRouter?.unregister(spec.agentAddress);
          deps.multistepDrainRouter?.unregister(spec.agentAddress);
          deps.multistepGrantsRouter?.unregister(spec.agentAddress);
          // Unregister unconditionally: the sources handler was registered
          // only for a single-step deploy, but `unregister` is a no-op for
          // an address that never registered one, so a multi-step unwind
          // safely calls it too.
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
          // Reverse the single-step head's `recordHubKey` so a failed deploy
          // leaves no in-memory hub key behind. `forgetAgent` also drops the
          // agent keypair cache `loadOrGenerateKey` populated, which is safe:
          // the transport registration is already unwound above, nothing reads
          // that cache after unwind, and a redeploy reloads the keypair from
          // disk. The on-disk deploy-tree repo `initRepo` created is
          // deliberately NOT reversed. It is idempotent and the hub re-pushes
          // the deploy pack on every redeploy, so it is benign residue; and
          // decisively, the durable Ed25519 identity keypair lives inside that
          // same directory (`keys/` nests under the agent repo dir), so
          // removing the repo would destroy an identity a rerouted head must
          // keep across a failed redeploy.
          deps.keyStore.forgetAgent(spec.agentAddress);
        }
        if (deploymentRegistered) {
          // Reverse the pre-spawn `registerDeployment`: drop the address
          // mapping so a failed spawn leaves the boot-edge registry as it
          // found it. Registered first (before spawn), unwound last. A
          // subsequent stale workflow-run write for the dead deployment then
          // surfaces structurally (`registry.resolve` returns null) rather
          // than resolving to the address of a deployment that never came up.
          deps.unregisterDeployment({
            runId,
            agentAddress: spec.agentAddress,
          });
        }
      }
    }
  }

  /**
   * Provision one step of a multi-step deploy WITHOUT spawning. The hub
   * stages each step's deploy tree before firing the deployment-level
   * workflow frame; a full-closure deploy pack still needs an initialized
   * agent-state repo to apply into and the hub key recorded to verify the
   * pack commit signature. This does exactly those two things -- the same
   * harness-free `initRepo` + `recordHubKey` seam the single-step head uses
   * -- and constructs no supervisor or child. The deployment-level workflow
   * frame (fired once after every step is provisioned) spawns the child,
   * which reads each step's staged deploy tree from disk.
   *
   * Returns the sidecar's principal public key so the link's
   * `agent.deploy.ack` carries a key, matching the multi-step ack. A
   * per-step address is workflow-derived and records no `agent_instance`
   * key, so the hub discards this value.
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
    // Reject a re-deploy of an address already live OR mid-deploy in this
    // process BEFORE touching any durable state. The durable writes below (the
    // run record, the materialized closure, step grants) are destructive
    // overwrites of state owned by whatever deployment currently holds the
    // address; overwriting is only legal when this deploy owns the address.
    // `activeSupervisors` catches an address whose deploy has completed;
    // `reservingDeployAddresses` catches one whose deploy is still in flight.
    // The map is populated only after `spawn` succeeds, so the has-check alone
    // leaves a window in which two frames both pass and the loser's catch below
    // deletes the winner's live record; the reservation set closes it. A
    // re-deploy after `undeploy` passes: `undeploy` drops the
    // `activeSupervisors` entry, and a failed or completed deploy has already
    // cleared its reservation.
    if (
      activeSupervisors.has(frame.agentAddress) ||
      reservingDeployAddresses.has(frame.agentAddress)
    ) {
      throw new Error(
        `sidecar deploy router: ${frame.agentAddress} is already deployed; undeploy it before redeploying`,
      );
    }

    const runId = deriveDeploymentId(frame.agentAddress);

    // Resolve the sidecar data dir once: the run record, the materialized
    // closure, and the per-step scratch all root under it. Required for any
    // deployment that spawns a child.
    const dataDir = stepStateDataDir;
    if (typeof dataDir !== "string" || dataDir.length === 0) {
      throw new Error(
        "sidecar deploy router: SIDECAR_DATA_DIR must be present in the multi-step substrate env; the run record and workflow-process child root under it",
      );
    }

    // Claim the deployment slug BEFORE any durable write so a colliding runId
    // (two distinct addresses projecting to the same slug) is rejected before
    // the closure, the step grants, or the supervisor touch disk -- the
    // router's "no repo state touched before rejection" guarantee. The claim is
    // released on any failure below; a successful deploy keeps it (the undeploy
    // hook releases it at teardown). The spawn core owns unwinding the
    // supervisor and registrations it stands up; the slug is the caller's.
    claimSlug(runId, frame.agentAddress);
    // Hold the single-flight reservation across the async body below and clear
    // it in the finally. Everything above is synchronous and throws before any
    // durable write, so the reservation is only needed from the first await
    // here onward; the top-of-method guard already consults this set for a
    // concurrent frame, and claimSlug/runId derivation above cannot yield
    // control before this point.
    reservingDeployAddresses.add(frame.agentAddress);
    try {
      // Source-ref apply -- the only deploy lineage. Materialize EXACTLY the
      // hub's frozen dependency `closure` and evaluate the PINNED CODE to the
      // workflow definition; the deploy frame carries no inline definition to
      // trust. The closure is applied byte-for-byte (concrete versions +
      // integrity SRIs); the sidecar never re-resolves the pin at apply time.
      // The re-evaluated projection IS the runnable definition, and the child's
      // load-boundary re-verify recomputes the wire hash over the closure and
      // fails closed if it diverges from the hub-approved hash, so a closure
      // that no longer projects to the approved content cannot deploy.
      //
      // Check the frame's inline source assets out into the durable
      // per-deployment store the closure materializes from. Reclaim the store
      // first so a redeploy drops assets no longer referenced. This runs only on
      // the DEPLOY path -- restore re-reads the store the deploy persisted, with
      // no re-delivery -- so the checkout lives here, not in
      // `materializeDeploymentClosure` (which also runs on restore). A
      // registry-sourced pin delivers no assets and only clears the store.
      const assetStore = deploymentSourceAssetRoot(dataDir, runId);
      const gitStore = deploymentSourceGitRoot(dataDir, runId);
      await rm(assetStore, { recursive: true, force: true });
      await rm(gitStore, { recursive: true, force: true });
      if (projection.assets !== undefined && projection.assets.length > 0) {
        await materializeWorkflowAssets({
          assets: projection.assets,
          closure: projection.sourceRef.closure,
          assetRoot: assetStore,
          gitDirRoot: gitStore,
          maxAssetPayloadBytes: MAX_INLINE_ASSET_PAYLOAD_BYTES,
        });
      }
      // Safe to reclaim the instance dir inside the helper: this deploy is
      // single-flight-guarded (the reservation above) and the child is not yet
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

      // Structural invariants the wire arktype does not cover (non-empty
      // stepOrder, every stepOrder entry backed by a `steps` entry AND a
      // `sources` entry), checked against the closure-derived definition -- the
      // frame carries none to cover. Mirrors the restore path.
      validateWorkflowProjection({
        definition: effectiveDefinition,
        sources: projection.sources,
      });

      // Source-admission gate: reject a deploy where any step pins an inference
      // provider this sidecar cannot build. Every source in a step's failover
      // chain must be buildable -- a chain with an unbuildable tail would fail
      // only after the reactor failed over onto it -- so this iterates the whole
      // list. The throw propagates back through the deploy frame so the hub's
      // `deployWorkflow` rejects synchronously at deploy time.
      for (const stepId of effectiveDefinition.stepOrder) {
        const chain = projection.sources[stepId];
        if (chain !== undefined) {
          for (const source of chain) deps.assertSourceBuildable(source);
        }
      }

      // Single-agent deploy vs. derived multi-step deploy. A one-step
      // definition keeps the deploy's own mail address and its grants in the
      // agent-state repo keyed by the run id; a multi-step definition derives
      // `<runId>-<stepId>` per step for both the mail address and the
      // agent-state repo id, isolating each step's grants in its own repo.
      const stepStrategy = createStepStrategy({
        legacyAddress: frame.agentAddress,
        stepOrder: effectiveDefinition.stepOrder,
        multistepDeriveStepAddress,
      });

      // Per-body approved wire hashes, keyed by body id, from the frame's
      // referenced onTrigger bodies. A body without an approved hash contributes
      // no entry rather than a null one.
      const referencedDefinitionHashes: Record<string, string> = {};
      for (const referenced of projection.referencedDefinitions ?? []) {
        if (referenced.approvedWireHash !== undefined) {
          referencedDefinitionHashes[referenced.definition.id] =
            referenced.approvedWireHash;
        }
      }

      // The spec the shared spawn core consumes, and the durable record that
      // lets a boot-time restore rebuild the SAME spec (definition re-evaluated
      // from the pinned closure, grants from the step repos, and the record's
      // frame/in-memory-only inputs: sources, session id, single-step hub key).
      const spec: WorkflowDeploySpec = {
        agentAddress: frame.agentAddress,
        definition: effectiveDefinition,
        sources: projection.sources,
        approvedWireHash: projection.approvedWireHash,
        referencedDefinitionHashes,
        sessionId: frame.config.sessionId,
        hubPublicKey:
          effectiveDefinition.stepOrder.length === 1
            ? frame.hubPublicKey
            : undefined,
        // The sidecar-local dir of the just-materialized closure the spawn core
        // threads into the child's env so it re-evaluates the pinned code.
        closurePackageDir: applied.packageDir,
        // The source-ref pin the record persists so a restore can
        // re-materialize the closure.
        sourceRef: projection.sourceRef,
      };
      const record = buildWorkflowRunRecord(spec, spec.sources);

      // Persist the run record BEFORE the spawn so a crash mid-spawn leaves a
      // record the boot scan re-drives (an idempotent re-spawn; the child's
      // in-flight-run discovery resumes any run). A soft-failed deploy deletes
      // it below, so only a crash-interrupted deploy leaves one.
      await persistWorkflowRunRecord(dataDir, runId, record);

      // Materialize each extracted onTrigger section body's per-step inference
      // sources to `assets/workflow/<bodyRef>/sources.json`. The body
      // DEFINITION is not staged: the run child resolves each body in-memory
      // from the parent's re-verified closure and hard-fails rather than reading
      // a body definition off disk. The sources ride on disk (not through env)
      // because the body child is in-process and loses its env across a restart.
      for (const referenced of projection.referencedDefinitions ?? []) {
        await materializeWorkflowSources(
          dataDir,
          referenced.definition.id,
          referenced.sources,
        );
      }

      // Grants bridge: the spawned child does not see the frame; it reads
      // each step's grants out of `state/grants.json` in the step's
      // agent-state repo while the supervisor assembles the
      // credentialsSnapshot. Write the operator-approved
      // `frame.config.grants` to the same repo the supervisor reads via
      // `deriveStepRepoId`, before the spawn core, so the read sees them.
      await writeStepGrants({
        repoStore: deps.repoStore,
        anchorRunId: runId,
        stepOrder: effectiveDefinition.stepOrder,
        deriveStepRepoId: stepStrategy.deriveStepRepoId,
        grants: frame.config.grants,
      });

      // Hand off to the shared spawn core.
      return await spawnWorkflowRun(spec, projection.credentials);
    } catch (cause) {
      // Soft failure (this process survived, the deploy threw): drop the
      // record and release the slug so the failed deploy is neither restored
      // nor leaks its slug. The record delete must not mask the real deploy
      // error or skip releasing the slug: a rejecting delete is logged (the
      // orphaned record is a durable-state leak the next boot scan re-drives)
      // but `cause` is still what propagates and the slug is still released.
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
      // Release the single-flight reservation whether the deploy succeeded or
      // threw. On success the address is now in `activeSupervisors`, which the
      // guard also consults, so a later re-deploy is still rejected.
      reservingDeployAddresses.delete(frame.agentAddress);
    }
  }

  return {
    async deploy(frame): Promise<DeployRouterResult> {
      if (frame.provisionStep === true) {
        return await provisionStep(frame);
      }
      if (frame.workflow !== undefined) {
        return await deployMultiStep(frame, frame.workflow);
      }
      // Every deploy stages through the workflow-run substrate: a
      // provision-step frame primes the per-step repo, and a workflow
      // frame spawns the supervised child. A frame carrying neither is
      // an unsupported shape -- there is no in-process fall-through.
      throw new Error(
        `sidecar deploy router: unsupported deploy frame for ${frame.agentAddress}; a deploy must carry provisionStep or a workflow definition`,
      );
    },
    async undeploy(frame): Promise<void> {
      // Symmetric teardown for `deploy`: release the per-deployment
      // routing state both branches install so a stale `signal.deliver`
      // / `drain.deliver` / `mail.inbound` aimed at the dead deployment
      // address is rejected by the router rather than dispatched into
      // an orphan supervisor handler. The unregister calls are
      // idempotent -- they are no-ops when no handler is registered.
      //
      // Routers come down BEFORE the supervisor's `shutdown()` so any
      // hub-side frame racing the undeploy is dropped at the router
      // boundary rather than dispatched into a supervisor that is in
      // the middle of tearing its child down. The pattern is: drop
      // racing frames first, then unwind the underlying resource.
      const runId = deriveDeploymentId(frame.agentAddress);
      deps.multistepMailRouter?.unregister(frame.agentAddress);
      deps.multistepSignalRouter?.unregister(frame.agentAddress);
      deps.multistepDrainRouter?.unregister(frame.agentAddress);
      deps.multistepGrantsRouter?.unregister(frame.agentAddress);
      // Unregister unconditionally (a no-op for a multi-step address that
      // registered no sources handler), matching the sibling routers.
      deps.multistepSourcesRouter?.unregister(frame.agentAddress);
      deps.multistepCredentialsRouter?.unregister(frame.agentAddress);
      // Shut the per-deployment supervisor down so the workflow-process
      // child, its IPC pipes, and its event-channel fd are released.
      // The supervisor's `shutdown()` is idempotent (returns early when
      // the supervisor is already in `idle`/`stopped`) and handles the
      // kill + `exited` await internally. The map entry is removed
      // before the await so a subsequent re-deploy on the same address
      // cannot observe a stale handle even if `shutdown()` rejects.
      const wired = activeSupervisors.get(frame.agentAddress);
      if (wired !== undefined) {
        activeSupervisors.delete(frame.agentAddress);
        await wired.supervisor.shutdown();
        // Drop the deployment address's transport registration installed at
        // spawn (OUTBOUND half of mailbox ownership, §3a). Both single- and
        // multi-step register the deployment address for outbound signing, so
        // this tears down a real registration for either; `unregister` is a
        // no-op only if the spawn failed before registering, so it is safe to
        // call unconditionally for any spawned deployment.
        deps.transport.unregister(frame.agentAddress);
        // Reclaim the deployment's per-step local-disk scratch now that
        // its supervisor + workflow-process child are torn down. The
        // whole `workflow-step-state/<runId>/` subtree goes: the
        // warm single-step agent's stable workspace under `warm/` (the
        // dir bounded keying parks per agent) AND any cold `runs/<runId>/`
        // subtrees a multi-step deploy's per-run cleanup did not already
        // drop. Awaiting `shutdown()` above guarantees no child still
        // holds the scratch, so this is a safe `rm -rf`. The durable
        // conversation under `agent-conversation-state/` is a DIFFERENT
        // root and is deliberately NOT touched here -- a re-deploy on the
        // same address must restore the prior conversation from it.
        if (stepStateDataDir !== undefined) {
          await rm(pathJoin(stepStateDataDir, "workflow-step-state", runId), {
            recursive: true,
            force: true,
          });
        }
      }
      // Drop the run record so a boot-time restore does not re-spawn a
      // torn-down deployment, and reclaim a source-ref deployment's
      // materialized closure tree AND its durable source-asset store. All run
      // on every undeploy -- not only when a supervisor was active -- so state
      // left behind by a crash-interrupted deploy, or by a source-ref restore
      // that materialized the closure and then failed to spawn (registry down),
      // is reclaimed too. A registry-sourced deployment never creates the source
      // store, so its `force` remove is a no-op there.
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
        // No substrate config was wired (a test router that never spawns a
        // child): nothing was ever persisted under this data dir, so there
        // is nothing to restore.
        return;
      }

      const scanned = await scanWorkflowRunRecords(dataDir);
      // Restore serially, not in parallel: deterministic boot-log ordering,
      // one isolable warning per failed record, and no concurrent
      // child-spawn / transport-register storm. Restore runs before
      // `hubLink.connect()`, so there are no concurrent deploys to contend
      // with. Each record's failure is caught so one bad deployment cannot
      // strand the rest.
      for (const { runId, record } of scanned) {
        try {
          // Integrity: the stored address must re-derive to its own directory
          // name. A mismatch means a corrupt or misplaced record; skip it
          // rather than restore a deployment under the wrong slug. (A source-ref
          // record missing its source/closure/approvedWireHash is rejected
          // earlier, at the scan boundary, by the record schema's discriminated
          // union -- so no bespoke source-ref guard is needed here.)
          const derived = deriveDeploymentId(record.agentAddress);
          if (derived !== runId) {
            logger.warn`skipping workflow deployment restore: ${record.agentAddress} derives slug ${derived}, not its directory ${runId}`;
            continue;
          }

          // Reconstruct this deployment's runnable definition. Source-ref is
          // the only lineage: re-materialize the pinned closure and evaluate the
          // pinned code to the live definition, then project it to the inert
          // wire shape -- the SAME computation the deploy path applies
          // (`WorkflowProjectionDefinition(projectLiveToInert(...))`). The
          // closure IS the source of truth; no on-disk definition is read. The
          // helper reclaims the instance dir first, which is safe here because
          // the prior process (the only reader) is dead and restore is serial
          // before `hubLink.connect()`, so no concurrent reader holds it.
          // Registry-sourced entries fetch from the content-addressed closure
          // cache (a hit populated on the original deploy, surviving restart);
          // asset-sourced entries read from the durable source store the
          // original deploy checked out (`materializeDeploymentClosure` derives
          // the mounts from the pin, so no re-delivery is needed). Both are
          // SRI-verified. A cache/store miss soft-fails the record (kept for the
          // next boot), matching the `assertSourceBuildable` retry-on-later-boot
          // behavior below. The schema guarantees a source-ref record carries a
          // `sourceRef` pin, so no undefined-check is needed.
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

          // Structural invariants the wire arktype does not cover (non-empty
          // stepOrder, every stepOrder entry backed by a `steps` entry AND a
          // `sources` entry). The closure eval skips the deploy frame's coverage
          // narrow, so this is where its definition-vs-sources coverage is
          // checked.
          validateWorkflowProjection({ definition, sources: record.sources });

          // Re-run the source-admission gate: refuse to restore a deployment
          // whose pinned provider this sidecar can no longer build. Every
          // source in a step's failover chain must be buildable, so this
          // iterates the whole list. The record is KEPT (not deleted) so a
          // later boot with the provider restored retries it.
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
            // The hub-approved wire hash the original deploy persisted, so the
            // restore re-spawn carries the same `DEFINITION_HASH` rather than a
            // recompute. Always present -- the record schema requires it.
            approvedWireHash: record.approvedWireHash,
            // Re-thread the per-body approved hashes the original deploy
            // persisted, so a restored onTrigger body clears the same
            // re-verify barrier a fresh deploy does. These hashes are the
            // out-of-band pins the barrier checks each body against. Absent for
            // a deployment with no bodies -- an empty map then.
            referencedDefinitionHashes: record.referencedDefinitionHashes ?? {},
            sessionId: record.sessionId,
            hubPublicKey: record.hubPublicKey,
            // The sidecar-local dir of the just-materialized closure the spawn
            // core threads into the child's env so it re-evaluates the pinned
            // code.
            closurePackageDir,
            // Carry the source-ref pin so a post-restore source rotation --
            // which rebuilds the record from the spec -- re-persists it; without
            // this a rotation would silently drop it and wedge the NEXT restart.
            sourceRef: record.sourceRef,
          };

          // The slug is the caller's, matching `deployMultiStep`: claim before
          // the spawn, release on failure. Unlike deploy's soft-fail, restore
          // does NOT delete the record and does NOT re-materialize
          // `workflow.json` or the step grants -- all of that is already on
          // disk from the original deploy. A failed restore just warns and
          // leaves the record for the next boot; there is deliberately no GC
          // of a permanently-unrestorable record here (an operator reclaims it
          // by undeploying the address).
          //
          // Release only a slug THIS pass newly claimed: if the address is
          // already live (its slug still held by the running deployment), the
          // core's double-spawn guard throws, and freeing the slug then would
          // strand a live deployment's collision guard. `claimSlug` is a
          // no-op for an already-held (runId, address) pair, so the
          // pre-claim check distinguishes the two.
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
      // `activeSupervisors` is keyed by deployment run address and holds
      // exactly the deployments with a live supervisor (deploy and restore
      // add; undeploy and spawn-unwind remove), so its keys are the addresses
      // this sidecar can currently route mail to.
      return [...activeSupervisors.keys()];
    },
    reEmitParkedCorrelations(address: string): void {
      const wired = activeSupervisors.get(address);
      if (wired === undefined) {
        // Edge boundary: the hub reports this address routable but no live
        // supervisor owns it -- a deployment torn down, or not yet respawned.
        // Re-registering a torn-down deployment would be wrong, so skipping is
        // correct. Logged at debug (not warn) so a supervisor unexpectedly
        // missing for a live deployment is still traceable without crying wolf
        // on the ordinary torn-down case.
        logger.debug`re-emit on reconnect: no active supervisor for ${address}; skipping`;
        return;
      }
      // Fire-and-forget: the driver is best-effort and watchdog-bounded inside
      // the supervisor, so the reconnect fan-out never awaits it. The promise
      // is dropped deliberately; the `.catch` is defense-in-depth since the
      // driver already swallows its own query failures.
      void wired.supervisor.reEmitParkedCorrelations().catch((cause) => {
        const message = cause instanceof Error ? cause.message : String(cause);
        logger.warn`re-emit of parked correlations on hub reconnect failed for ${address}: ${message}`;
      });
    },
  };
}

/**
 * Logical mail-audit reference the supervisor stamps onto every
 * inbox/processing/consumed envelope for sidecar-hosted deployments.
 * The substrate does not dereference the value; it is a host-side
 * pointer the audit consumer joins on. The mail audit is keyed by the
 * deployment id plus the parsed messageId, which is unique per inbound
 * message and stable across the FIFO pipeline's
 * enqueue/dequeue/markConsumed transitions.
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
 * pre-wired. The router calls this once per multi-step `agent.deploy`
 * frame to stand up the workflow-process child that hosts the
 * deployment.
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
  // Per-run grants sink. The supervisor awaits this and pushes the
  // resulting snapshot to the child before the run's `trigger.fire`; a
  // throw propagates and the dispatch barrier fails the run rather than
  // firing the trigger against absent grants. The snapshot is the run's
  // own per-run grants file, which every legitimate birth path writes
  // before dispatch (see `assembleRunCredentialsSnapshot`).
  const onRunStart = (args: {
    runId: string;
    anchorRunId: string;
  }): Promise<CredentialsSnapshot> => {
    // Fail closed on a run whose `run.grants` write was recorded as failed:
    // its per-run grants file never landed. This is the known-failed-write
    // case, distinct from the general absent-file backstop in
    // `assembleRunCredentialsSnapshot` -- both fail the run closed, but this
    // one names the recorded write failure specifically so a poisoned run's
    // diagnostics point at the failed push rather than a generic absence.
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
    subprocessSpawner: opts.subprocessSpawner ?? defaultSubprocessSpawner,
    binaryPath: opts.binaryPath ?? SIDECAR_WORKFLOW_CHILD_BINARY,
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

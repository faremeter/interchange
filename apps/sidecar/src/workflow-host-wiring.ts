// Thin wiring module that constructs `createWorkflowSupervisor` with
// this sidecar's host-specific bindings: the existing mail-bus
// instance, the sidecar's Ed25519 signing keypair, the substrate
// RepoStore handle, and `Bun.spawn` as the subprocess spawner. Any
// logic that would benefit a future alternative-sidecar
// implementation lives inside `@intx/workflow-host`, not here.

import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, join as pathJoin } from "node:path";
import { fileURLToPath } from "node:url";

import { type } from "arktype";

import { derivePublicKeyBytes, signEd25519 } from "@intx/crypto";
import { getLogger } from "@intx/log";
import type { HubTransport } from "@intx/mail-memory";
import {
  parseAgentId,
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
  STEP_GRANTS_PATH,
  STEP_GRANTS_REF,
  wrapHubTransportAsMailBus,
  type CredentialsSnapshot,
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
  type WorkflowSupervisor,
} from "@intx/workflow-host";
import { hexEncode } from "@intx/types";
import {
  parseInferenceEvent,
  type CryptoProvider,
  type InferenceEvent,
  type InferenceSource,
  type KeyPair,
} from "@intx/types/runtime";
import {
  AgentDeployWorkflow,
  type AgentDeployFrame,
} from "@intx/types/sidecar";
import { STEP_ID_PATTERN } from "@intx/workflow";
import { deriveWorkflowRunRepoId } from "@intx/workflow-deploy";

import type {
  MultistepDrainRouter,
  MultistepMailRouter,
  MultistepSignalRouter,
} from "./workflow-run-pack-client";
import {
  deleteWorkflowDeploymentRecord,
  scanWorkflowDeploymentRecords,
  writeWorkflowDeploymentRecord,
  type WorkflowDeploymentRecord,
} from "./workflow-deployment-record";

const logger = getLogger(["interchange", "sidecar", "workflow-host-wiring"]);

/**
 * Project an agent address into the substrate-safe id of its
 * workflow-run repo. Both deploy branches key `{ kind: "workflow-run",
 * id }` by this slug, and the supervisor principal's `deploymentId`
 * must equal that id for the workflow-run kind handler's authz check to
 * pass. The derivation is owned by `@intx/workflow-deploy` so the hub's
 * read routes reconstruct the identical id; this thin delegator keeps
 * the sidecar's call sites readable while the rationale and the
 * substrate `SAFE_REPO_ID` contract live with the shared function.
 */
export function deriveDeploymentId(agentAddress: string): string {
  return deriveWorkflowRunRepoId(agentAddress);
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
 * `stepOrder.length === 1` is the agent-launch identity deploy: the sole
 * step IS the legacy launched agent, so its grants live in the legacy
 * agent-state repo keyed by `parseAgentId(legacyAddress)`. This is
 * exactly the repo the legacy agent identity keys, so the spawned child
 * reads the agent's grants from where the agent's identity already
 * lives, and the deployment frame's `ins_<hex>` address is preserved
 * (the deploy-ack listener finds the `agent_instance` row, the
 * workflow-run repo stays keyed by `deriveWorkflowRunRepoId(legacy)`).
 *
 * Any other step count is a derived multi-step deploy: each step gets a
 * derived `<deploymentId>-<stepId>` mail address (via the router's
 * `multistepDeriveStepAddress`) and a derived agent-state repo under the
 * default `<deploymentId>-<stepId>` convention.
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
    deriveStepRepoId: ({ deploymentId, stepId }) => ({
      kind: "agent-state",
      id: `${deploymentId}-${stepId}`,
    }),
  };
}

/**
 * Write every step's grants into its agent-state repo so the
 * supervisor's `assembleCredentialsSnapshot` (invoked inside `spawn()`)
 * reads them off the working tree at `STEP_GRANTS_PATH`. The on-disk
 * shape is `{ grants: WireGrantRule[] }` -- the envelope
 * `assembleCredentialsSnapshot` validates (`{ grants: unknown[] }`) and
 * the child's `evaluateGrants` adapter narrows to `GrantRule[]`.
 *
 * The same `deriveStepRepoId` the supervisor reads with keys the write,
 * so read and write address the same repo. The write is on the spawn
 * critical path: a failure rejects the deploy (the caller's `finally`
 * unwinds the partial state) rather than spawning a child that would
 * fail every authorize closed against an empty grant set.
 */
async function writeStepGrants(args: {
  repoStore: RepoStore;
  deploymentId: string;
  stepOrder: readonly string[];
  deriveStepRepoId: DeriveStepRepoId;
  grants: readonly unknown[] | undefined;
}): Promise<void> {
  // The deploy frame's validated HarnessConfig always carries a `grants`
  // array (possibly empty); an absent array means "no grants", which
  // serializes to the same fail-closed empty file the snapshot expects.
  // Coerce here so the on-disk envelope is always a valid `{ grants: [] }`
  // rather than `{}` (which the snapshot's validator rejects).
  const grants = args.grants ?? [];
  const serialized = JSON.stringify({ grants }, null, 2);
  for (const stepId of args.stepOrder) {
    const repoId = args.deriveStepRepoId({
      deploymentId: args.deploymentId,
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
  deploymentId: string;
  /**
   * Step count of the deployed `WorkflowDefinition` (`stepOrder.length`).
   * Threaded into the child's spawn-time env so its deploy-tree read
   * collapses onto the head for a single-step deployment.
   */
  stepCount: number;
  /** Deployment's mail address. */
  deploymentMailAddress: string;
  /** Per-step mail-address derivation. */
  deriveStepAddress: DeriveStepAddress;
  /**
   * Optional override of the per-step `agent-state` repo identity the
   * supervisor reads grants from while assembling the
   * credentialsSnapshot. Defaults to the `<deploymentId>-<stepId>`
   * convention; the single-step launched-agent deploy supplies a
   * derivation that returns the legacy agent-state repo so the spawned
   * child reads grants from the same repo the legacy agent identity
   * keys.
   */
  deriveStepRepoId?: DeriveStepRepoId;
  /** Substrate-config keys propagated to the child via spawn-time env. */
  substrateEnv: Record<string, string>;
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
};

export type SidecarWorkflowSupervisor = {
  supervisor: WorkflowSupervisor;
  /** Hand a delivered inbound message off to the supervisor's mail subscription. */
  routeInbound(message: Uint8Array): void;
  /** Snapshot accessor that proxies the supervisor's credentials view. */
  getCredentialsSnapshot(): CredentialsSnapshot | null;
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
 * Project a value into a canonical JSON string with deterministically
 * sorted object keys. Used at the router boundary to mint a stable
 * content hash of the wire-projected workflow definition; the
 * orchestrator's hand-off task computes the same hash from the same
 * canonical form, so a downstream verifier comparing the two values
 * sees byte equality.
 *
 * The shape mirrors the canonicalizer the runlocal repo-store uses
 * for equality checks (`packages/workflow/src/runlocal/repo-store.ts`).
 */
function canonicalJsonStringify(value: unknown): string {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map(canonicalJsonStringify).join(",")}]`;
  }
  const entries = Object.entries(value).sort(([a], [b]) =>
    a < b ? -1 : a > b ? 1 : 0,
  );
  return `{${entries
    .map(([k, v]) => `${JSON.stringify(k)}:${canonicalJsonStringify(v)}`)
    .join(",")}}`;
}

/**
 * Compute the deploy router's content hash for the wire-projected
 * workflow definition. SHA-256 of the canonical JSON of the
 * `WorkflowDefinition` projection, hex-encoded. The supervisor and the
 * workflow-process child read the value out of the spawn-time env
 * verbatim; it is the deployment's content-addressed handle.
 *
 * The router computes this locally so the multi-step branch does not
 * round-trip the hub for a hash the orchestrator's hand-off task will
 * also derive deterministically from the same canonical form.
 */
export async function computeWireDefinitionHash(
  definition: unknown,
): Promise<string> {
  const canonical = canonicalJsonStringify(definition);
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(canonical),
  );
  return hexEncode(new Uint8Array(digest));
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
 * the sidecar boot edge additionally calls `restoreWorkflowDeployments` once,
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
  restoreWorkflowDeployments(): Promise<void>;
  /**
   * The workflow-substrate deployment addresses (`ins_dep_...`) this router
   * currently hosts a live supervisor for -- the set of addresses this
   * sidecar can route mail to. The boot edge announces these to the hub on
   * (re)connect so the hub re-registers them for routing: they are hub-minted
   * and carry no per-address key, so unlike single-agent sessions they are
   * not re-established by the challenge flow, and without this announcement
   * the hub drops their route on a WS reconnect. Reflects `deploy`/`undeploy`
   * and boot-time restore live, so a caller re-reads it per connect.
   */
  activeAddresses(): string[];
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
   * Record a `(deploymentId -> agentAddress)` mapping the boot edge's
   * workflow-run pack push facade consults when it must address an
   * outbound pack frame. Fires once per inbound `agent.deploy` frame
   * before the deployment's supervisor spawns, so the first pack push
   * the child triggers sees the mapping. Tests that do not exercise
   * the pack push path may pass a no-op.
   */
  registerDeployment: (entry: {
    deploymentId: string;
    agentAddress: string;
  }) => void;
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
    deploymentId: string;
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
   * deployment's agent address plus the deploy's session id through to
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
   * Optional override for the multi-step branch's per-step mail-address
   * derivation. Defaults to `${deploymentId}-${stepId}@<deploymentDomain>`
   * derived from the frame's agent address. Tests inject a deterministic
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
  const multistepSubstrateEnv = deps.multistepSubstrateEnv ?? {};
  // Sidecar data dir the deployment's per-step scratch is rooted under
  // (`<dataDir>/workflow-step-state/<deploymentId>/...`). Resolved once
  // from the boot-edge substrate env so the undeploy hook can reclaim
  // the whole subtree. Absent only when the router is wired without
  // substrate config (a test that never spawns a child), in which case
  // no child ever rooted scratch and the undeploy reclaim is correctly
  // skipped.
  const stepStateDataDir = multistepSubstrateEnv.SIDECAR_DATA_DIR;
  const multistepSpawner =
    deps.multistepSubprocessSpawner ?? defaultSubprocessSpawner;
  const multistepDeriveStepAddress: DeriveStepAddress =
    deps.multistepDeriveStepAddress ??
    (({ deploymentId, stepId }) => `${deploymentId}-${stepId}`);

  // Per-deployment supervisor tracking. The multi-step branch
  // constructs one `SidecarWorkflowSupervisor` per `agent.deploy`
  // frame; the supervisor owns the workflow-process child, its IPC
  // pipes, and its event-channel fd. The undeploy hook consults this
  // map to call `supervisor.shutdown()` so the child's lifetime ends
  // with the deployment.
  const activeSupervisors = new Map<string, SidecarWorkflowSupervisor>();

  // Synchronous single-flight guard for the deploy path. The real supervisor
  // does not exist until inside `spawnWorkflowDeployment`, so `deployMultiStep`
  // cannot reserve its `activeSupervisors` slot up front; instead it records
  // the address here synchronously, before its first await, and clears it in a
  // finally once the deploy settles. `activeSupervisors` is populated only
  // after `spawn` succeeds, so the has-check alone leaves a window in which two
  // same-address frames both pass and the loser's unwind deletes the winner's
  // live deployment record. This set closes that window: a second frame that
  // arrives while the first is mid-deploy is rejected before it touches any
  // durable state. Only the live deploy path reserves; the boot restore path
  // is serial and relies on the `activeSupervisors` backstop instead.
  const reservingDeployAddresses = new Set<string>();

  // Slug-collision tracking. `deriveDeploymentId` substitutes
  // disallowed characters with `-`, which is deterministic but lossy:
  // two distinct agent addresses can collapse to the same slug, and
  // a collision would let the second deploy silently overwrite the
  // first deploy's workflow-run repo state (the slug IS the repoId).
  // This map records the first-claimer; a subsequent deploy that
  // produces the same slug from a different address is rejected at
  // the router before any supervisor or repo state is touched.
  const slugClaims = new Map<string, string>();

  function claimSlug(deploymentId: string, agentAddress: string): void {
    const existing = slugClaims.get(deploymentId);
    if (existing !== undefined && existing !== agentAddress) {
      throw new Error(
        `deriveDeploymentId collision: agent addresses ${JSON.stringify(existing)} and ${JSON.stringify(agentAddress)} both project to deploymentId ${JSON.stringify(deploymentId)}`,
      );
    }
    // A same-address re-claim is a defensive no-op: the `activeSupervisors`
    // guard rejects a live re-deploy before claimSlug is re-invoked, and a
    // failed or undeployed deploy releases the slug first, so in practice
    // `existing` is only ever undefined or a different address here.
    slugClaims.set(deploymentId, agentAddress);
  }

  function releaseSlug(deploymentId: string, agentAddress: string): void {
    const existing = slugClaims.get(deploymentId);
    if (existing === agentAddress) slugClaims.delete(deploymentId);
  }

  /**
   * Materialize the workflow definition on the sidecar's local substrate so
   * the workflow-process child's `loadWorkflowDefinition` can read
   * `workflow.json` out of the workflow-asset repo's working tree. The
   * destination mirrors the bare RepoStore's `getRepoDir` for
   * `{ kind: "workflow", id }`:
   * `${SIDECAR_DATA_DIR}/assets/workflow/<id>/workflow.json`. The child reads
   * via `fs.readFile`, so writing the bytes outside git suffices. This is
   * deploy-only durable state; the restore path finds it already on disk.
   */
  async function materializeWorkflowJson(
    sidecarDataDir: string | undefined,
    definition: NonNullable<AgentDeployFrame["workflow"]>["definition"],
  ): Promise<void> {
    if (typeof sidecarDataDir !== "string" || sidecarDataDir.length === 0) {
      throw new Error(
        "sidecar deploy router: SIDECAR_DATA_DIR must be present in the multi-step substrate env; the workflow-process child resolves the workflow-asset repo dir against this data dir",
      );
    }
    const workflowAssetPath = pathJoin(
      sidecarDataDir,
      "assets",
      "workflow",
      definition.id,
      "workflow.json",
    );
    const workflowAssetBytes = JSON.stringify(definition, null, 2);
    try {
      await mkdir(dirname(workflowAssetPath), { recursive: true });
      // Idempotent: only rewrite when the on-disk content differs. Treats a
      // missing file as different.
      let existing: string | null = null;
      try {
        existing = await readFile(workflowAssetPath, "utf8");
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
      if (existing !== workflowAssetBytes) {
        await writeFile(workflowAssetPath, workflowAssetBytes, "utf8");
      }
    } catch (cause) {
      const reason = cause instanceof Error ? cause.message : String(cause);
      throw new Error(
        `sidecar deploy router: failed to materialize workflow.json at ${workflowAssetPath}: ${reason}`,
        { cause },
      );
    }
  }

  /**
   * Read a workflow definition back off the sidecar's local substrate for a
   * boot-time restore. Mirrors `materializeWorkflowJson`'s path derivation
   * (`${dataDir}/assets/workflow/<definitionId>/workflow.json`). Returns the
   * parsed-but-unvalidated JSON: the on-disk file is untrusted at restore
   * (partial write, corruption, tamper), so the caller re-validates it through
   * the same wire + structural gates the deploy path applies. A missing file
   * or unparseable JSON throws; the restore loop's per-record catch converts
   * that into a warn-and-skip.
   */
  async function readWorkflowJson(
    sidecarDataDir: string,
    definitionId: string,
  ): Promise<unknown> {
    const workflowAssetPath = pathJoin(
      sidecarDataDir,
      "assets",
      "workflow",
      definitionId,
      "workflow.json",
    );
    const raw = await readFile(workflowAssetPath, "utf8");
    return JSON.parse(raw);
  }

  /**
   * The per-deployment inputs the shared spawn core needs to stand up a
   * workflow deployment, independent of the live deploy frame. The live
   * deploy path builds this from `frame`/`projection`; a boot-time restore
   * path builds the same shape from the persisted deployment record.
   */
  interface WorkflowDeploySpec {
    agentAddress: string;
    definition: NonNullable<AgentDeployFrame["workflow"]>["definition"];
    sources: NonNullable<AgentDeployFrame["workflow"]>["sources"];
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
  async function spawnWorkflowDeployment(
    spec: WorkflowDeploySpec,
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
    const deploymentId = deriveDeploymentId(spec.agentAddress);

    // Single-step launched-agent deploy vs. derived multi-step deploy. A
    // one-step deployment keeps the deployment's own (legacy) mail address
    // and its grants in the legacy agent-state repo keyed by the legacy
    // instance id. A multi-step deployment derives `<deploymentId>-<stepId>`
    // per step for both the mail address and the agent-state repo id.
    const stepStrategy = createStepStrategy({
      legacyAddress: spec.agentAddress,
      stepOrder: spec.definition.stepOrder,
      multistepDeriveStepAddress,
    });

    // Unwind every piece of spawn state if any step between here and
    // `registerDeployment` throws, so a failed spawn leaks no
    // freshly-spawned workflow-process child, `activeSupervisors` entry,
    // transport registration, or multistep router registration. The
    // ordering inside the finally is the reverse of the success-path
    // registration order. The caller owns the deployment slug: it must
    // claim the collision guard before any durable write and release it on
    // failure, so the slug is not touched here.
    let succeeded = false;
    let wiredForUnwind: SidecarWorkflowSupervisor | undefined;
    let supervisorRegistered = false;
    let routersRegistered = false;
    let agentTransportRegistered = false;
    let hubKeyRecorded = false;
    try {
      const definitionHash = await computeWireDefinitionHash(spec.definition);

      // Per-deployment substrate-config keys the workflow-substrate-factory
      // validator requires. The boot edge's `multistepSubstrateEnv` carries
      // the boot-edge constants; the four workflow-definition / workflow-run
      // identity keys are derived per-deploy here. `STEP_INFERENCE_SOURCES`
      // threads each step's ordered inference-source failover chain into the
      // child.
      const substrateEnv: Record<string, string> = {
        ...multistepSubstrateEnv,
        WORKFLOW_DEFINITION_REPO_ID: spec.definition.id,
        WORKFLOW_DEFINITION_REF: "refs/heads/main",
        WORKFLOW_RUN_REPO_ID: deploymentId,
        WORKFLOW_RUN_REF: "refs/heads/main",
        [STEP_INFERENCE_SOURCES_ENV_KEY]: JSON.stringify(spec.sources),
      };

      const wired = createSidecarWorkflowSupervisor({
        transport: deps.transport,
        repoStore: deps.repoStore,
        signingKeySeed: deps.signingKeySeed,
        workflowRunRepoId: {
          kind: "workflow-run",
          id: deploymentId,
        },
        workflowRunRef: "refs/heads/main",
        deploymentId,
        stepCount: spec.definition.stepOrder.length,
        deploymentMailAddress: spec.agentAddress,
        deriveStepAddress: stepStrategy.deriveStepAddress,
        deriveStepRepoId: stepStrategy.deriveStepRepoId,
        substrateEnv,
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

      // The public key the deploy ack surfaces to the hub. For a single-step
      // head it is the AGENT key, set inside the block below; a genuine
      // multi-step deployment has no head agent identity and falls back to the
      // supervisor principal key at the return. (The registered deployment
      // keypair above is used purely for outbound signing; a multi-step
      // deployment address is workflow-derived, incurs no reconnect challenge,
      // and so records no `agent_instance.publicKey` -- carrying it on the ack
      // would be data written nowhere and read nowhere.)
      let headAgentPublicKey: string | undefined;
      if (spec.definition.stepOrder.length === 1) {
        // A single-step head IS an agent identity: it signs its own outbound
        // mail AND its reconnect challenges with this agent key (via the key
        // store's signChallenge). The hub records the ack's key into
        // `agent_instance.publicKey` (for a rerouted instance head, which has
        // an instance row and is not workflow-derived) and verifies the
        // reconnect challenge against it, so the ack MUST carry the agent key,
        // not the supervisor principal key -- otherwise verification fails.
        headAgentPublicKey = hexEncode(keyPair.publicKey);

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
      deps.multistepMailRouter?.register(spec.agentAddress, (message) => {
        wired.routeInbound(message);
      });
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
      routersRegistered = true;

      // Resolve the ack public key BEFORE registering the deployment
      // address so an (unreachable, deterministic) derivation failure
      // unwinds the spawn without having touched the boot-edge
      // `DeploymentAddressRegistry`. A single-step head acks its agent key
      // (captured above); a multi-step deployment acks the supervisor
      // principal key its workflow-run events are signed with.
      const publicKey =
        headAgentPublicKey ??
        (await derivePrincipalPublicKeyHex(deps.signingKeySeed));

      // Register the deployment-address mapping last so a failure in any
      // earlier step leaves the boot-edge `DeploymentAddressRegistry`
      // untouched. Nothing fallible runs after it, so the finally unwind
      // has no registry entry to reverse.
      deps.registerDeployment({
        deploymentId,
        agentAddress: spec.agentAddress,
      });

      succeeded = true;
      return { publicKey };
    } finally {
      if (!succeeded) {
        // Unwind in reverse registration order so each step undoes state
        // the success path confirmed; ordering matches the `undeploy` hook.
        if (routersRegistered) {
          deps.multistepMailRouter?.unregister(spec.agentAddress);
          deps.multistepSignalRouter?.unregister(spec.agentAddress);
          deps.multistepDrainRouter?.unregister(spec.agentAddress);
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
    // Boundary validation: a malformed projection is rejected at the
    // router edge before the supervisor is constructed so the link
    // surfaces a structured failure rather than a hung `starting`
    // supervisor.
    validateWorkflowProjection(projection);

    // Source-admission gate: reject a deploy where any step pins an
    // inference provider this sidecar cannot build, BEFORE any state is
    // claimed or the child is spawned. The throw propagates back through
    // the deploy frame so the hub's `deployWorkflow` rejects synchronously
    // at deploy time, rather than the child failing the run when the
    // step's inference first resolves. Covers single- and multi-step: the
    // projection's `narrow` guarantees every stepOrder entry has a
    // `sources` entry. Every source in a step's failover chain must be
    // buildable -- a chain with an unbuildable tail would fail only after
    // the reactor failed over onto it -- so this iterates the whole list.
    for (const stepId of projection.definition.stepOrder) {
      const chain = projection.sources[stepId];
      if (chain !== undefined) {
        for (const source of chain) deps.assertSourceBuildable(source);
      }
    }

    // Reject a re-deploy of an address already live OR mid-deploy in this
    // process BEFORE touching any durable state. The durable writes below (the
    // restore record, workflow.json, step grants) are destructive overwrites of
    // state owned by whatever deployment currently holds the address;
    // overwriting is only legal when this deploy owns the address.
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

    const deploymentId = deriveDeploymentId(frame.agentAddress);

    // Single-step launched-agent deploy vs. derived multi-step deploy.
    //
    // A one-step projection is the agent-launch identity path: the sole
    // step keeps the deployment's own (legacy) mail address, and its
    // grants live in the legacy agent-state repo keyed by the legacy
    // instance id (`parseAgentId(frame.agentAddress)`). This preserves
    // the identity the legacy agent-deploy path established -- the
    // workflow-run repo stays keyed by `deriveWorkflowRunRepoId(legacy)`
    // and `agent_instance.address` remains the `ins_<hex>` legacy shape.
    //
    // A multi-step projection derives `<deploymentId>-<stepId>` per step
    // for both the mail address and the agent-state repo id, isolating
    // each step's grants in its own repo.
    const stepStrategy = createStepStrategy({
      legacyAddress: frame.agentAddress,
      stepOrder: projection.definition.stepOrder,
      multistepDeriveStepAddress,
    });

    // Claim the deployment slug BEFORE any durable write so a colliding
    // deploymentId (two distinct addresses projecting to the same slug) is
    // rejected before `workflow.json`, the step grants, or the supervisor
    // touch disk -- the router's "no repo state touched before rejection"
    // guarantee. The claim is released on any failure below; a successful
    // deploy keeps it (the undeploy hook releases it at teardown). The
    // spawn core owns unwinding the supervisor and registrations it stands
    // up; the slug is the caller's.
    // Resolve the sidecar data dir once: the deployment record, workflow.json,
    // and the per-step scratch all root under it. Required for any deployment
    // that spawns a child.
    const dataDir = stepStateDataDir;
    if (typeof dataDir !== "string" || dataDir.length === 0) {
      throw new Error(
        "sidecar deploy router: SIDECAR_DATA_DIR must be present in the multi-step substrate env; the deployment record and workflow-process child root under it",
      );
    }

    // The spec the shared spawn core consumes, and the durable record that
    // lets a boot-time restore rebuild the SAME spec (definition re-read from
    // workflow.json by id, grants from the step repos, and the record's
    // frame/in-memory-only inputs: sources, session id, single-step hub key).
    const spec: WorkflowDeploySpec = {
      agentAddress: frame.agentAddress,
      definition: projection.definition,
      sources: projection.sources,
      sessionId: frame.config.sessionId,
      hubPublicKey:
        projection.definition.stepOrder.length === 1
          ? frame.hubPublicKey
          : undefined,
    };
    const record: WorkflowDeploymentRecord = {
      version: 1,
      agentAddress: spec.agentAddress,
      definitionId: spec.definition.id,
      sources: spec.sources,
      ...(spec.sessionId !== undefined ? { sessionId: spec.sessionId } : {}),
      ...(spec.hubPublicKey !== undefined
        ? { hubPublicKey: spec.hubPublicKey }
        : {}),
    };

    claimSlug(deploymentId, frame.agentAddress);
    // Hold the single-flight reservation across the async body below and clear
    // it in the finally. Everything above is synchronous and throws before any
    // durable write, so the reservation is only needed from the first await
    // here onward; the top-of-method guard already consults this set for a
    // concurrent frame, and claimSlug/deploymentId derivation above cannot
    // yield control before this point.
    reservingDeployAddresses.add(frame.agentAddress);
    try {
      // Persist the deployment record BEFORE the spawn so a crash mid-spawn
      // leaves a record the boot scan re-drives (an idempotent re-spawn; the
      // child's in-flight-run discovery resumes any run). A soft-failed deploy
      // deletes it below, so only a crash-interrupted deploy leaves one.
      await writeWorkflowDeploymentRecord(dataDir, deploymentId, record);

      // Materialize the deploy-only durable state the spawned child and the
      // supervisor read from disk: the workflow definition (`workflow.json`)
      // and each step's grants. The restore path finds both already on disk
      // and skips this; both land before the shared spawn core runs.
      await materializeWorkflowJson(dataDir, projection.definition);

      // Grants bridge: the spawned child does not see the frame; it reads
      // each step's grants out of `state/grants.json` in the step's
      // agent-state repo while the supervisor assembles the
      // credentialsSnapshot. Write the operator-approved
      // `frame.config.grants` to the same repo the supervisor reads via
      // `deriveStepRepoId`, before the spawn core, so the read sees them.
      await writeStepGrants({
        repoStore: deps.repoStore,
        deploymentId,
        stepOrder: projection.definition.stepOrder,
        deriveStepRepoId: stepStrategy.deriveStepRepoId,
        grants: frame.config.grants,
      });

      // Hand off to the shared spawn core.
      return await spawnWorkflowDeployment(spec);
    } catch (cause) {
      // Soft failure (this process survived, the deploy threw): drop the
      // record and release the slug so the failed deploy is neither restored
      // nor leaks its slug.
      await deleteWorkflowDeploymentRecord(dataDir, deploymentId);
      releaseSlug(deploymentId, frame.agentAddress);
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
      const deploymentId = deriveDeploymentId(frame.agentAddress);
      deps.multistepMailRouter?.unregister(frame.agentAddress);
      deps.multistepSignalRouter?.unregister(frame.agentAddress);
      deps.multistepDrainRouter?.unregister(frame.agentAddress);
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
        // whole `workflow-step-state/<deploymentId>/` subtree goes: the
        // warm single-step agent's stable workspace under `warm/` (the
        // dir bounded keying parks per agent) AND any cold `runs/<runId>/`
        // subtrees a multi-step deploy's per-run cleanup did not already
        // drop. Awaiting `shutdown()` above guarantees no child still
        // holds the scratch, so this is a safe `rm -rf`. The durable
        // conversation under `agent-conversation-state/` is a DIFFERENT
        // root and is deliberately NOT touched here -- a re-deploy on the
        // same address must restore the prior conversation from it.
        if (stepStateDataDir !== undefined) {
          await rm(
            pathJoin(stepStateDataDir, "workflow-step-state", deploymentId),
            { recursive: true, force: true },
          );
        }
      }
      // Drop the deployment record so a boot-time restore does not re-spawn a
      // torn-down deployment. Runs on every undeploy -- not only when a
      // supervisor was active -- so a record left behind by a
      // crash-interrupted deploy is reclaimed too.
      if (stepStateDataDir !== undefined) {
        await deleteWorkflowDeploymentRecord(stepStateDataDir, deploymentId);
      }
      releaseSlug(deploymentId, frame.agentAddress);
      deps.unregisterDeployment({
        deploymentId,
        agentAddress: frame.agentAddress,
      });
    },
    async restoreWorkflowDeployments(): Promise<void> {
      const dataDir = stepStateDataDir;
      if (dataDir === undefined) {
        // No substrate config was wired (a test router that never spawns a
        // child): nothing was ever persisted under this data dir, so there
        // is nothing to restore.
        return;
      }

      const scanned = await scanWorkflowDeploymentRecords(dataDir);
      // Restore serially, not in parallel: deterministic boot-log ordering,
      // one isolable warning per failed record, and no concurrent
      // child-spawn / transport-register storm. Restore runs before
      // `hubLink.connect()`, so there are no concurrent deploys to contend
      // with. Each record's failure is caught so one bad deployment cannot
      // strand the rest.
      for (const { deploymentId, record } of scanned) {
        try {
          // Integrity: the stored address must re-derive to its own directory
          // name. A mismatch means a corrupt or misplaced record; skip it
          // rather than restore a deployment under the wrong slug.
          const derived = deriveDeploymentId(record.agentAddress);
          if (derived !== deploymentId) {
            logger.warn`skipping workflow deployment restore: ${record.agentAddress} derives slug ${derived}, not its directory ${deploymentId}`;
            continue;
          }

          // Re-read and RE-VALIDATE the definition off disk with the exact
          // gates the deploy path applies: the wire arktype
          // (`AgentDeployWorkflow`) to narrow the untrusted on-disk shape,
          // then `validateWorkflowProjection` for the structural invariants
          // the arktype does not cover (non-empty stepOrder, every stepOrder
          // entry backed by a `steps` entry). The on-disk `workflow.json` is
          // untrusted at restore, so it must clear the same bar a fresh
          // deploy frame clears -- no weaker.
          const definitionRaw = await readWorkflowJson(
            dataDir,
            record.definitionId,
          );
          const projection = AgentDeployWorkflow({
            definition: definitionRaw,
            sources: record.sources,
          });
          if (projection instanceof type.errors) {
            logger.warn`skipping workflow deployment restore for ${record.agentAddress}: workflow.json failed validation: ${projection.summary}`;
            continue;
          }
          validateWorkflowProjection(projection);

          // Re-run the source-admission gate: refuse to restore a deployment
          // whose pinned provider this sidecar can no longer build. Every
          // source in a step's failover chain must be buildable, so this
          // iterates the whole list. The record is KEPT (not deleted) so a
          // later boot with the provider restored retries it.
          for (const stepId of projection.definition.stepOrder) {
            const chain = projection.sources[stepId];
            if (chain !== undefined) {
              for (const source of chain) deps.assertSourceBuildable(source);
            }
          }

          const spec: WorkflowDeploySpec = {
            agentAddress: record.agentAddress,
            definition: projection.definition,
            sources: projection.sources,
            sessionId: record.sessionId,
            hubPublicKey: record.hubPublicKey,
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
          // no-op for an already-held (deploymentId, address) pair, so the
          // pre-claim check distinguishes the two.
          const slugNewlyClaimed =
            slugClaims.get(deploymentId) !== record.agentAddress;
          claimSlug(deploymentId, record.agentAddress);
          try {
            await spawnWorkflowDeployment(spec);
            logger.info`Restored workflow deployment for ${record.agentAddress}`;
          } catch (cause) {
            if (slugNewlyClaimed) {
              releaseSlug(deploymentId, record.agentAddress);
            }
            throw cause;
          }
        } catch (cause) {
          const reason = cause instanceof Error ? cause.message : String(cause);
          logger.warn`Failed to restore workflow deployment ${deploymentId}: ${reason}`;
        }
      }
    },
    activeAddresses(): string[] {
      // `activeSupervisors` is keyed by deployment agent address and holds
      // exactly the deployments with a live supervisor (deploy and restore
      // add; undeploy and spawn-unwind remove), so its keys are the addresses
      // this sidecar can currently route mail to.
      return [...activeSupervisors.keys()];
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
export function deriveSidecarMailAuditRef(deploymentId: string): (
  messageId: string,
  rawMessage: Uint8Array,
) => {
  store: string;
  path: string;
} {
  return (messageId, _rawMessage) => ({
    store: "sidecar-mail-audit",
    path: `${deploymentId}/${messageId}`,
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
    deploymentId: opts.deploymentId,
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
    workflowRunRepoId: opts.workflowRunRepoId,
    workflowRunRef: opts.workflowRunRef,
    deploymentId: opts.deploymentId,
    stepCount: opts.stepCount,
    deploymentMailAddress: opts.deploymentMailAddress,
    readPrincipal: supervisorPrincipal,
    deriveStepAddress: opts.deriveStepAddress,
    ...(opts.deriveStepRepoId !== undefined
      ? { deriveStepRepoId: opts.deriveStepRepoId }
      : {}),
    deriveMailAuditRef: deriveSidecarMailAuditRef(opts.deploymentId),
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
      mailBus.routeInbound(opts.deploymentMailAddress, message);
    },
    getCredentialsSnapshot: () => supervisor.getCredentialsSnapshot(),
  };
}

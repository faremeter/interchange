// Thin wiring module that constructs `createWorkflowSupervisor` with
// this sidecar's host-specific bindings: the existing mail-bus
// instance, the sidecar's Ed25519 signing keypair, the substrate
// RepoStore handle, `Bun.spawn` as the subprocess spawner, and a
// host-injected `trivialLaunch` callback that drives the legacy
// single-agent provisioning surface for trivial (1-step) deploys.
// Any logic that would benefit a future alternative-sidecar
// implementation lives inside `@intx/workflow-host`, not here.

import { createHash, createPublicKey, sign as nodeSign } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join as pathJoin } from "node:path";
import { fileURLToPath } from "node:url";

import { importPrivateKeyBytes } from "@intx/crypto-node";
import { getLogger } from "@intx/log";
import type { HubTransport } from "@intx/mail-memory";
import type {
  RepoId,
  RepoStore,
  WorkflowRunSupervisorPrincipal,
} from "@intx/hub-sessions";
import type {
  AgentKeyStore,
  DeployRouter,
  DeployRouterResult,
  SessionManager,
} from "@intx/hub-agent";
import {
  createWorkflowSupervisor,
  wrapHubTransportAsMailBus,
  type CredentialsSnapshot,
  type DeriveStepAddress,
  type EventPayload,
  type FrameReader,
  type HubTransportMailBusAdapter,
  type NdjsonReader,
  type NdjsonWriter,
  type RecordRunEvent,
  type SpawnOpts,
  type SubprocessHandle,
  type SubprocessSpawner,
  type SupervisorRunEvent,
  type TrivialLaunch,
  type WorkflowSupervisor,
} from "@intx/workflow-host";
import type { InferenceEvent } from "@intx/types/runtime";
import type { AgentDeployFrame } from "@intx/types/sidecar";
import { STEP_ID_PATTERN } from "@intx/workflow";

import type {
  MultistepDrainRouter,
  MultistepMailRouter,
  MultistepSignalRouter,
} from "./workflow-run-pack-client";

const logger = getLogger(["interchange", "sidecar", "workflow-host-wiring"]);

/**
 * Project an agent address into a substrate-safe deployment id for
 * the trivial branch. The workflow-run repo's `repoId.id` must match
 * `/^[a-zA-Z0-9_-]+$/` (see `SAFE_REPO_ID` in
 * `packages/hub-sessions/src/repo-store/types.ts`), and the supervisor
 * principal's `deploymentId` must equal `workflowRunRepoId.id` for
 * the workflow-run kind handler's authz check to pass. The strict
 * regex rejects `@` and `.`, which both appear in every agent
 * address. The substrate test
 * `packages/hub-sessions/src/agent-repo.test.ts` explicitly asserts
 * `agent@domain` is rejected as `repo_id_invalid`, so the regex is
 * the substrate's contract surface for repo-path safety; widening it
 * would mean updating that contract and the test suite that pins it.
 *
 * Substitute disallowed characters with `-`. The mapping is lossy
 * (two distinct addresses can collapse to the same slug) but
 * deterministic; trivial deployments share one workflow-run repo
 * per agent address by design, and a collision implies two
 * deployments are claiming the same trivial workflow surface.
 */
export function deriveTrivialDeploymentId(agentAddress: string): string {
  return agentAddress.replaceAll(/[^a-zA-Z0-9_-]/g, "-");
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
  /** Deployment's mail address. */
  deploymentMailAddress: string;
  /** Per-step mail-address derivation. */
  deriveStepAddress: DeriveStepAddress;
  /** Substrate-config keys propagated to the child via spawn-time env. */
  substrateEnv: Record<string, string>;
  /**
   * Host-injected callback the supervisor's trivial branch invokes.
   * For the production sidecar this closes over the
   * `SessionManager.provisionAgent` flow plus the hub-pairing-key
   * recording the legacy `agent.deploy` handler performed inline.
   */
  trivialLaunch: TrivialLaunch;
  /**
   * Override the subprocess spawner. Tests inject a deterministic
   * mock; production defaults to the `Bun.spawn`-backed
   * `defaultSubprocessSpawner`.
   */
  subprocessSpawner?: SubprocessSpawner;
  /** Override the `bin/workflow-child` path. */
  binaryPath?: string;
};

export type SidecarWorkflowSupervisor = {
  supervisor: WorkflowSupervisor;
  /** Hand a delivered inbound message off to the supervisor's mail subscription. */
  routeInbound(message: Uint8Array): void;
  /** Snapshot accessor that proxies the supervisor's credentials view. */
  getCredentialsSnapshot(): CredentialsSnapshot | null;
};

/**
 * Construct the sidecar's `DeployRouter`. The router holds the
 * `sessions` + `keyStore` closures the workflow-host supervisor's
 * `trivialLaunch` needs and constructs a fresh per-deployment
 * supervisor on every inbound `agent.deploy` frame. The trivial
 * branch then calls back into `sessions.provisionAgent` plus the
 * pre-existing hub-pairing-key recording so the bytes flowing
 * through the deploy-flow gate test path stay bit-identical to the
 * pre-supervisor path.
 *
 * Multi-step deploys (frames carrying a workflow definition with
 * `steps.length >= 2`) route through `supervisor.deploy`'s
 * multi-step branch -- the IPC channel, child spawn, and
 * `credentialsSnapshot` assembly. The routing seam exists here so
 * the frame-format extension that carries the workflow definition is
 * a pure data-shape change.
 */
/**
 * Stable step identifier the trivial workflow uses for its single
 * step. The on-disk `StepStarted` / `StepCompleted` envelopes carry
 * this value verbatim; it is opaque to the supervisor and the
 * substrate, but downstream audit-log consumers join run events on
 * it. The trivial workflow has exactly one step per run, so the id
 * is a constant rather than a per-deploy mint.
 */
const TRIVIAL_STEP_ID = "trivial";

/**
 * Env key the multi-step branch uses to carry per-step inference source
 * pins from `frame.workflow.sources` down to the workflow-process child.
 * The substrate factory's `buildEnv` reads this and resolves a source
 * per step at step invocation; the supervisor itself is opaque to the
 * value (it is plumbed through `bindings.substrateEnv` verbatim).
 *
 * Listed here so the router and the future substrate-factory consumer
 * spell the key the same way without a magic-string trip hazard.
 */
export const STEP_INFERENCE_SOURCES_ENV_KEY = "STEP_INFERENCE_SOURCES";

/**
 * Validate the wire-projected workflow definition at the deploy-router
 * boundary. The arktype `AgentDeployFrame` validator at the wire edge
 * already enforces the structural shape (`id`, `stepOrder`, `steps`,
 * `sources` table covering every `stepOrder` entry). This function
 * re-asserts the invariants the router relies on so a wire-edge change
 * does not let a malformed projection slip into `supervisor.spawn()`
 * silently:
 *
 *   - `definition.id` is a non-empty string.
 *   - `definition.stepOrder` is non-empty and every entry matches
 *     `STEP_ID_PATTERN` (so per-step mail-address derivation never
 *     needs escaping at the substrate boundary).
 *   - Every `stepOrder` entry has a corresponding `steps[id]` entry
 *     (the wire shape lets `steps[id]` be `unknown`, but its presence
 *     is required so the workflow-process child can resolve a step's
 *     primitive at run time).
 *   - Every `stepOrder` entry has a corresponding `sources[id]` entry
 *     (the arktype narrow already enforces this; the re-check here
 *     surfaces a structured error at the router rather than relying on
 *     the wire validator alone).
 *
 * A rejection here surfaces as a thrown `Error` the link's deploy frame
 * caller converts into a structured failure reply.
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
export function computeWireDefinitionHash(definition: unknown): string {
  const canonical = canonicalJsonStringify(definition);
  return createHash("sha256").update(canonical, "utf8").digest("hex");
}

/**
 * Derive the supervisor's principal public key from the sidecar's
 * Ed25519 signing seed. The supervisor signs every workflow-run event
 * with this key; the multi-step branch surfaces it to the link so the
 * hub records the verifying key for the deployment's signed events.
 */
function derivePrincipalPublicKeyHex(signingKeySeed: Uint8Array): string {
  const privateKey = importPrivateKeyBytes(signingKeySeed);
  const publicKey = createPublicKey(privateKey);
  // Node exports Ed25519 SPKI in DER; the raw 32-byte point is the
  // last 32 bytes of the structure (RFC 8410). The export keeps this
  // module independent of `exportPublicKeyBytes` from `@intx/crypto-node`,
  // which is not part of the package's public surface.
  const der = publicKey.export({ type: "spki", format: "der" });
  if (der.length < 32) {
    throw new Error(
      `sidecar deploy router: unexpected SPKI DER length ${String(der.length)} for Ed25519 public key`,
    );
  }
  return der.subarray(der.length - 32).toString("hex");
}

export function createSidecarDeployRouter(deps: {
  sessions: SessionManager;
  keyStore: AgentKeyStore;
  onAgentEvent: SessionManager["onAgentEvent"];
  transport: HubTransport;
  repoStore: RepoStore;
  signingKeySeed: Uint8Array;
  /**
   * Record a `(deploymentId -> agentAddress)` mapping the boot edge's
   * workflow-run pack push facade consults when it must address an
   * outbound pack frame. Fires once per inbound `agent.deploy` frame
   * before the supervisor's `deploy()` call so the first `recordRunEvent`
   * commit (which triggers the push hook) sees the mapping. Tests that
   * do not exercise the pack push path may pass a no-op.
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
   * frame. Defaults to an empty record so trivial-only deployments do
   * not require the boot edge to thread substrate config through the
   * router.
   */
  multistepSubstrateEnv?: Record<string, string>;
  /**
   * Subprocess spawner the multi-step branch hands to the supervisor.
   * Defaults to the production `Bun.spawn`-backed
   * `defaultSubprocessSpawner`; tests inject a deterministic mock.
   * The trivial branch never invokes the spawner.
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
   * deployment's agent address through to the callback so a downstream
   * fan-out can route events to per-agent listeners. Defaults to a
   * no-op; production wiring supplies the event publisher.
   */
  publishWorkflowInferenceEvent?: (
    agentAddress: string,
    event: EventPayload,
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
   * subscription. The trivial branch never touches this registry --
   * its mail path is the legacy session surface.
   *
   * Optional so tests that exercise the trivial branch (or the
   * multi-step branch without an end-to-end mail loop) can omit the
   * binding; an absent registry simply means multi-step inbound mail
   * cannot route through the hub-link until the wiring is plumbed.
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
   * Optional so tests that exercise the trivial branch (or the
   * multi-step branch without an end-to-end signal loop) can omit the
   * binding; an absent registry means hub-side signals cannot route
   * through the hub-link until the wiring is plumbed.
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
   * Optional so tests that exercise the trivial branch (or the
   * multi-step branch without an end-to-end drain loop) can omit the
   * binding; an absent registry means hub-side drain frames cannot
   * route through the hub-link until the wiring is plumbed.
   */
  multistepDrainRouter?: MultistepDrainRouter;
}): DeployRouter {
  const principalPublicKeyHex = derivePrincipalPublicKeyHex(
    deps.signingKeySeed,
  );
  const publishInferenceEvent =
    deps.publishWorkflowInferenceEvent ??
    ((_address: string, _event: EventPayload): void => {
      /* no-op default: tests and production-without-a-publisher
         deployments do not consume events. */
    });
  const multistepSubstrateEnv = deps.multistepSubstrateEnv ?? {};
  const multistepSpawner =
    deps.multistepSubprocessSpawner ?? defaultSubprocessSpawner;
  const multistepDeriveStepAddress: DeriveStepAddress =
    deps.multistepDeriveStepAddress ??
    (({ deploymentId, stepId }) => `${deploymentId}-${stepId}`);

  async function deployMultiStep(
    frame: AgentDeployFrame,
    projection: NonNullable<AgentDeployFrame["workflow"]>,
  ): Promise<DeployRouterResult> {
    // Boundary validation: a malformed projection is rejected at the
    // router edge before the supervisor is constructed so the link
    // surfaces a structured failure rather than a hung `starting`
    // supervisor.
    validateWorkflowProjection(projection);

    const deploymentId = deriveTrivialDeploymentId(frame.agentAddress);
    deps.registerDeployment({
      deploymentId,
      agentAddress: frame.agentAddress,
    });

    const definitionHash = computeWireDefinitionHash(projection.definition);

    // Per-deployment substrate-config keys the workflow-substrate-factory
    // validator requires (`SIDECAR_SUBSTRATE_CONFIG_KEYS` /
    // `SubstrateConfig` in `workflow-substrate-factory.ts`). The boot
    // edge's `multistepSubstrateEnv` only carries the boot-edge constants
    // (`SIDECAR_DATA_DIR`, signing keys, hub link anchors); the four
    // workflow-definition / workflow-run identity keys must be derived
    // per-deploy here so the child's substrate-config validator passes
    // at startup.
    //
    // `WORKFLOW_RUN_REPO_ID` mirrors `workflowRunRepoId.id` (the
    // substrate-safe slug of the deployment address) and
    // `WORKFLOW_RUN_REF` mirrors `workflowRunRef`, so the child resolves
    // the same workflow-run repo the supervisor is writing into.
    //
    // `WORKFLOW_DEFINITION_REPO_ID` is set to `projection.definition.id`
    // (the workflow asset's repo id; see the orchestrator's
    // `WorkflowRepoWriter.writeWorkflowRepo` call, which writes the
    // asset repo keyed by `workflow.id`). The child's
    // `loadWorkflowDefinition` in
    // `packages/workflow-host/src/child/run-child.ts` reads
    // `workflow.json` out of this repo's working tree. The current
    // Phase I multi-step test path does not yet stage the workflow
    // asset on the sidecar's substrate (the orchestrator writes the
    // asset to the hub repo store, not the sidecar's data dir), so the
    // child's `loadWorkflowDefinition` will not actually find a
    // `workflow.json` until the workflow-asset deploy lands on the
    // sidecar. The value is structurally correct -- a consistent,
    // deterministic id derived from the definition -- so the substrate-
    // config validator passes and the next gap (if any) surfaces
    // structurally rather than at the env-keys boundary.
    //
    // `WORKFLOW_DEFINITION_REF` is `"refs/heads/main"` to mirror the
    // hub's `DEFAULT_ASSET_REF` and the workflow-run ref the supervisor
    // uses.
    const substrateEnv: Record<string, string> = {
      ...multistepSubstrateEnv,
      WORKFLOW_DEFINITION_REPO_ID: projection.definition.id,
      WORKFLOW_DEFINITION_REF: "refs/heads/main",
      WORKFLOW_RUN_REPO_ID: deploymentId,
      WORKFLOW_RUN_REF: "refs/heads/main",
      [STEP_INFERENCE_SOURCES_ENV_KEY]: JSON.stringify(projection.sources),
    };

    // Materialize the workflow asset on the sidecar's local substrate
    // so the workflow-process child's `loadWorkflowDefinition` can read
    // `workflow.json` out of the workflow-asset repo's working tree
    // (`packages/workflow-host/src/child/run-child.ts`). The hub's
    // orchestrator writes the asset to the hub's repo store; the
    // sidecar's substrate is a separate data dir on disk, and nothing
    // replicates between the two today.
    //
    // The frame inlines the validated `WorkflowDefinition` already
    // (see `AgentDeployFrame.workflow.definition`), so the router has
    // the bytes in scope. The destination path mirrors the bare
    // RepoStore's `getRepoDir` for `{ kind: "workflow", id }`:
    // `${SIDECAR_DATA_DIR}/assets/workflow/<id>/workflow.json`. The
    // workflow-asset repo kind handler's `directoryPrefix` is
    // `assets/workflow` (see `packages/hub-sessions/src/workflow-kind.ts`).
    //
    // The child reads via `fs.readFile`, not via a git ref resolution,
    // so writing the bytes outside any git operation is sufficient.
    const sidecarDataDir = substrateEnv.SIDECAR_DATA_DIR;
    if (typeof sidecarDataDir !== "string" || sidecarDataDir.length === 0) {
      throw new Error(
        "sidecar deploy router: SIDECAR_DATA_DIR must be present in the multi-step substrate env for the multi-step branch; the workflow-process child resolves the workflow-asset repo dir against this data dir",
      );
    }
    const workflowAssetPath = pathJoin(
      sidecarDataDir,
      "assets",
      "workflow",
      projection.definition.id,
      "workflow.json",
    );
    const workflowAssetBytes = JSON.stringify(projection.definition, null, 2);
    try {
      await mkdir(dirname(workflowAssetPath), { recursive: true });
      // Idempotent: only rewrite when the on-disk content differs from
      // the projection. Treats a missing file as different.
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
      deploymentMailAddress: frame.agentAddress,
      deriveStepAddress: multistepDeriveStepAddress,
      substrateEnv,
      subprocessSpawner: multistepSpawner,
      ...(deps.multistepBinaryPath !== undefined
        ? { binaryPath: deps.multistepBinaryPath }
        : {}),
      // The multi-step branch never invokes trivialLaunch, but the
      // supervisor's constructor requires the binding. Wire a sentinel
      // that throws so a stray invocation surfaces loudly rather than
      // silently succeeding.
      trivialLaunch: () => {
        throw new Error(
          "sidecar deploy router: trivialLaunch invoked on the multi-step branch; this is a programming bug",
        );
      },
    });

    const stepOrder = [...projection.definition.stepOrder];
    const spawnOpts: SpawnOpts = {
      stepOrder,
      definitionHash,
      onInferenceEvent: (event) => {
        publishInferenceEvent(frame.agentAddress, event);
      },
    };

    // Surface spawn-time errors structurally: if the subprocess
    // spawner crashes immediately (binary missing, env malformed,
    // EXEC error) the supervisor's `wireChild` races the child's
    // `exited` against `readyPromise` and the rejection propagates
    // here. The router lets it surface; the link's deploy handler
    // converts the rejection into a structured failure frame.
    await wired.supervisor.spawn(spawnOpts);

    // Bind the deployment's mail address to this supervisor's
    // `routeInbound` so the sidecar's hub-link dispatches inbound
    // mail for the deployment address into the supervisor's mail-bus
    // subscription rather than the legacy session path. The legacy
    // path is the wrong receiver for multi-step deployments: the
    // deployment address is never registered on `transport` (no
    // `startSession` runs against it) and there is no `sessions`
    // entry to satisfy `commitInboundMail`. Registration happens
    // after `spawn` succeeds so a spawn-time rejection leaves the
    // registry untouched.
    deps.multistepMailRouter?.register(frame.agentAddress, (message) => {
      wired.routeInbound(message);
    });
    // Register the signal-delivery handler against the deployment
    // address so a hub-side `signal.deliver` frame dispatches through
    // the supervisor's `deliverSignal`, which forwards a control IPC
    // `signal.deliver` to the workflow-process child. The child writes
    // the resulting `SignalReceived` event through its own substrate;
    // the workflow-run pack-push pipeline then propagates the commit
    // to the hub with no concurrent writer at the workflow-run ref.
    deps.multistepSignalRouter?.register(frame.agentAddress, async (args) => {
      await wired.supervisor.deliverSignal({
        runId: args.runId,
        signalName: args.signalName,
        signalId: args.signalId,
        payload: args.payload,
      });
    });
    // Register the drain handler against the deployment address so a
    // hub-side `drain.deliver` frame dispatches through the
    // supervisor's `drain`, which forwards a `drain` control IPC frame
    // to the workflow-process child and arms one `drainTimeout`
    // accumulator per in-flight run. Cancel-mode in-flight steps abort
    // on the child side; wait-mode steps continue. Each accumulator
    // commits a signed `CancelRequested{origin: "supervisor-drain"}`
    // against the workflow-run repo when the deadline expires.
    deps.multistepDrainRouter?.register(frame.agentAddress, async (args) => {
      await wired.supervisor.drain({ deadlineMs: args.deadlineMs });
    });

    return { publicKey: principalPublicKeyHex };
  }

  return {
    async deploy(frame): Promise<DeployRouterResult> {
      if (frame.workflow !== undefined) {
        return await deployMultiStep(frame, frame.workflow);
      }
      let publicKey: string | undefined;
      const deploymentId = deriveTrivialDeploymentId(frame.agentAddress);
      deps.registerDeployment({
        deploymentId,
        agentAddress: frame.agentAddress,
      });
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
        deploymentMailAddress: frame.agentAddress,
        deriveStepAddress: ({ deploymentId: dep, stepId }) =>
          `${dep}-${stepId}`,
        substrateEnv: {},
        trivialLaunch: async (bindings) => {
          const result = await deps.sessions.provisionAgent(
            // The trivialLaunch contract treats `config` as opaque
            // bytes the host minted; for the sidecar wiring the
            // bytes are a `HarnessConfig` the frame carried
            // verbatim, and `SessionManager.provisionAgent`
            // expects exactly that.
            // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- frame.config is the validated HarnessConfig the link surfaced
            bindings.config as Parameters<SessionManager["provisionAgent"]>[0],
          );
          deps.keyStore.recordHubKey(
            bindings.agentAddress,
            bindings.hubPublicKey,
          );
          await deps.sessions.persistHubPublicKey(
            bindings.agentAddress,
            bindings.hubPublicKey,
          );
          publicKey = result.publicKey;
          // Subscribe to per-agent InferenceEvents and project the
          // reactor's run-bracket vocabulary onto the workflow-run
          // event chain. The seam is a no-op until the harness
          // dispatches the first `message.run.started`; the
          // disposer is not held here because the trivial branch
          // shares the agent's lifetime with the deployment and
          // SessionManager prunes the listener set on
          // destroySession through the closure's natural unbind.
          // The reactor brackets one workflow-run per inbound
          // mail; each `message.run.started` mints a fresh runId
          // and brackets the chain.
          const cell: TrivialRunCell = {
            runId: null,
            stepStarted: false,
          };
          deps.onAgentEvent(bindings.agentAddress, (event) => {
            driveTrivialRunChain(event, bindings.recordRunEvent, cell).catch(
              (err: unknown) => {
                // Capture rejections inside the listener so a substrate
                // failure (e.g. the hub rejecting the workflow-run pack
                // push) does not surface as an unhandled rejection on
                // the host process. The trivial branch's audit chain
                // is best-effort against the deploy path; persistent
                // substrate or transport misconfigurations log loudly
                // here without killing the agent's reactor.
                const msg = err instanceof Error ? err.message : String(err);
                logger.warn`trivial run-event recording failed for ${bindings.agentAddress}: ${msg}`;
              },
            );
          });
        },
      });
      await wired.supervisor.deploy({
        agentAddress: frame.agentAddress,
        agentId: frame.agentId,
        config: frame.config,
        hubPublicKey: frame.hubPublicKey,
      });
      if (publicKey === undefined) {
        throw new Error(
          "sidecar deploy router: trivialLaunch did not surface a public key",
        );
      }
      return { publicKey };
    },
    async undeploy(frame): Promise<void> {
      // Symmetric teardown for `deploy`: release the per-deployment
      // routing state both branches install so a stale `signal.deliver`
      // / `drain.deliver` / `mail.inbound` aimed at the dead deployment
      // address is rejected by the router rather than dispatched into
      // an orphan supervisor handler. The unregister calls are
      // idempotent and safe to invoke for both branches even though
      // the trivial branch never registers against the multi-step
      // routers; those calls are no-ops when no handler is registered.
      const deploymentId = deriveTrivialDeploymentId(frame.agentAddress);
      deps.multistepMailRouter?.unregister(frame.agentAddress);
      deps.multistepSignalRouter?.unregister(frame.agentAddress);
      deps.multistepDrainRouter?.unregister(frame.agentAddress);
      deps.unregisterDeployment({
        deploymentId,
        agentAddress: frame.agentAddress,
      });
    },
  };
}

/**
 * Per-deployment cell tracking the active workflow-run bracket. The
 * trivial branch holds one cell per agent: each `message.run.started`
 * mints a fresh `runId`, the first `inference.start` after that flips
 * `stepStarted` true, and the matching `message.run.ended` reads both
 * back out before clearing the cell. Two outstanding brackets at once
 * are not possible for the trivial path because the reactor
 * serializes inbound messages.
 */
export interface TrivialRunCell {
  runId: string | null;
  stepStarted: boolean;
}

/**
 * Placeholder definition hash baked into the trivial workflow's
 * `RunStarted` envelopes. The trivial workflow's content-addressed
 * definition lands with the trivial-deploy capability walk; until
 * then the on-disk envelope carries a stable sentinel so audit-log
 * consumers see a consistent value across deployments.
 */
const TRIVIAL_DEFINITION_HASH = "trivial:v1";

/**
 * Map one InferenceEvent into `recordRunEvent` calls when the
 * reactor's run-bracket vocabulary lines up with a workflow-run
 * lifecycle moment. The mapping is:
 *
 *   - `message.run.started` -> RunStarted (mints a new workflow runId)
 *   - first `inference.start` after RunStarted -> StepStarted (attempt 1)
 *   - `message.run.ended` status=completed -> StepCompleted + RunCompleted
 *   - `message.run.ended` status=failed -> StepCompleted
 *
 * The runId is the `messageRunId` the reactor minted. `StepStarted`
 * is suppressed if a second `inference.start` arrives within the
 * same bracket (the trivial workflow's single step does not retry
 * inside one run; the next attempt would be a separate workflow-run
 * instance).
 *
 * The supervisor's `SupervisorRunEvent` union covers `RunCompleted`
 * but not `RunFailed`; the trivial branch surfaces a failed run by
 * recording `StepCompleted` against the step that failed (so the
 * per-step audit trail closes) and letting the absence of a
 * subsequent `RunCompleted` mark the run as unsuccessful for
 * downstream consumers. Widening the supervisor's
 * `SupervisorRunEvent` union to carry `RunFailed` lands when the
 * substrate kind handler grows the matching signature-verification
 * path.
 */
export async function driveTrivialRunChain(
  event: InferenceEvent,
  recordRunEvent: RecordRunEvent,
  cell: TrivialRunCell,
): Promise<void> {
  if (event.type === "message.run.started") {
    cell.runId = event.data.messageRunId;
    cell.stepStarted = false;
    const runStarted: SupervisorRunEvent = {
      kind: "RunStarted",
      runId: event.data.messageRunId,
      at: new Date().toISOString(),
      definitionHash: TRIVIAL_DEFINITION_HASH,
      trigger: {
        type: "mail",
        payload: { messageId: event.data.messageId },
      },
      consumedMessageId: event.data.messageId,
    };
    await recordRunEvent(runStarted);
    return;
  }
  if (event.type === "inference.start") {
    if (cell.runId === null) return;
    if (cell.stepStarted) return;
    cell.stepStarted = true;
    await recordRunEvent({
      kind: "StepStarted",
      runId: cell.runId,
      at: new Date().toISOString(),
      stepId: TRIVIAL_STEP_ID,
      attempt: 1,
      input: { ref: "refs/heads/main" },
    });
    return;
  }
  if (event.type === "message.run.ended") {
    const runId = cell.runId;
    if (runId === null) return;
    cell.runId = null;
    cell.stepStarted = false;
    const at = new Date().toISOString();
    await recordRunEvent({
      kind: "StepCompleted",
      runId,
      at,
      stepId: TRIVIAL_STEP_ID,
      attempt: 1,
      output: { ref: "refs/heads/main" },
    });
    if (event.data.status === "completed") {
      await recordRunEvent({
        kind: "RunCompleted",
        runId,
        at,
      });
    }
  }
}

/**
 * Logical mail-audit reference the supervisor stamps onto every
 * inbox/processing/consumed envelope for sidecar-hosted deployments.
 * The substrate does not dereference the value; it is a host-side
 * pointer the audit consumer joins on. The trivial-branch single-agent
 * mail audit is keyed by the deployment id plus the parsed messageId,
 * which is unique per inbound message and stable across the FIFO
 * pipeline's enqueue/dequeue/markConsumed transitions.
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
 * pre-wired. The host calls this once per `agent.deploy` frame; the
 * supervisor's trivial branch routes the deploy through the
 * host-supplied `trivialLaunch` callback so the on-wire and
 * on-disk surfaces stay bit-identical to the pre-supervisor path.
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
    signAsPrincipal: (kind, payload) => {
      const key = importPrivateKeyBytes(opts.signingKeySeed);
      const sig = nodeSign(null, payload, key);
      return { sig: new Uint8Array(sig), principalKind: kind };
    },
    mailBus,
    subprocessSpawner: opts.subprocessSpawner ?? defaultSubprocessSpawner,
    binaryPath: opts.binaryPath ?? SIDECAR_WORKFLOW_CHILD_BINARY,
    substrateEnv: opts.substrateEnv,
    workflowRunRepoId: opts.workflowRunRepoId,
    workflowRunRef: opts.workflowRunRef,
    deploymentId: opts.deploymentId,
    deploymentMailAddress: opts.deploymentMailAddress,
    readPrincipal: supervisorPrincipal,
    deriveStepAddress: opts.deriveStepAddress,
    trivialLaunch: opts.trivialLaunch,
    deriveMailAuditRef: deriveSidecarMailAuditRef(opts.deploymentId),
  });
  return {
    supervisor,
    routeInbound(message) {
      mailBus.routeInbound(opts.deploymentMailAddress, message);
    },
    getCredentialsSnapshot: () => supervisor.getCredentialsSnapshot(),
  };
}

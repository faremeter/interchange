// Public type shapes for the per-deployment supervisor surface.
//
// The supervisor takes its bindings as constructor arguments: a
// `RepoStore` substrate handle, a per-principal signing callback,
// mail-bus bindings, a subprocess spawner. None of these shapes
// depend on a specific host implementation -- the sidecar, an
// alternative sidecar, an integration test harness, and a future CLI
// can each construct a supervisor by wiring its own concrete
// instances against the same interface.

import type {
  Principal,
  RepoStore as SubstrateRepoStore,
} from "@intx/hub-sessions";

import type { FrameReader, NdjsonReader, NdjsonWriter } from "../ipc/index";
import type {
  CommitRunEventResult,
  SupervisorRunEvent,
} from "./run-event-signing";

/**
 * Workflow-side principal kinds the supervisor signs on behalf of.
 * Mirrors the kinds the workflow-run kind handler binds to specific
 * CancelRequested origins.
 *
 * - `supervisor`: the supervisor's own identity. Used for every
 *   CancelRequested origin in the Q3 map except `hub-admin`, plus
 *   drain audit frames in later commits.
 */
export type WorkflowSupervisorPrincipalKind = "supervisor";

/**
 * Output of a `signAsPrincipal` invocation. Carries the raw 64-byte
 * Ed25519 signature plus the principal kind the supervisor asked the
 * host to sign as so a downstream verifier can map the signature to
 * the right public key without consulting external metadata.
 */
export type SignedPayload = {
  /** Raw signature bytes from Ed25519 (64 bytes per RFC 8032). */
  readonly sig: Uint8Array;
  /** Principal kind the host signed as. */
  readonly principalKind: WorkflowSupervisorPrincipalKind;
};

/**
 * Host-supplied per-principal signing callback. The supervisor never
 * holds the principal's private key; it asks the host to mint a
 * signature for the supplied canonical payload bytes under the
 * named principal's key.
 *
 * The host wires this against its own key inventory (the sidecar's
 * existing Ed25519 signing keypair, the integration harness's test
 * key). The supervisor surfaces nothing about how the key is held;
 * the callback is the entire surface.
 */
export type PrincipalSigner = (
  kind: WorkflowSupervisorPrincipalKind,
  payload: Uint8Array,
) => SignedPayload;

/**
 * Mail-bus interface the supervisor needs. The shape is the minimal
 * subset of an existing mail-bus API the supervisor's spawn / mail-
 * trigger / teardown lifecycle reaches into; it does not pin the
 * supervisor to `InMemoryTransport` or any other concrete bus.
 *
 * `subscribeMailForAddress` returns a disposer the supervisor calls
 * during teardown. The supplied handler is invoked with the raw RFC
 * 2822 message bytes of each inbound message at the address.
 */
export interface MailBusBindings {
  registerAddress(address: string): void;
  unregisterAddress(address: string): void;
  subscribeMailForAddress(
    address: string,
    handler: (rawMessage: Uint8Array) => void,
  ): () => void;
}

/**
 * Handle the subprocess spawner returns to the supervisor. The
 * fields mirror what `Bun.spawn` returns; tests substitute an
 * in-process implementation that fulfills the same shape.
 *
 * `stdin`/`stdout` carry the control channel (NDJSON over stdio,
 * Ed25519-signed by the supervisor). `eventSocket` carries the
 * event channel (HMAC-authenticated). `kill()` terminates the
 * process; `exited` resolves when the process exits with the
 * terminal exit code.
 */
export interface SubprocessHandle {
  readonly pid: number;
  /**
   * Writer for the supervisor-to-child control channel (stdin on the
   * child). The supervisor's control-channel sender feeds NDJSON
   * lines through this writer.
   */
  readonly controlWriter: NdjsonWriter;
  /**
   * Reader for the child-to-supervisor control channel (stdout on
   * the child). Carries the child's `ready` frame and the rare
   * upstream control messages the child sends back.
   */
  readonly controlReader: NdjsonReader;
  /**
   * Supervisor-side handle on the inherited event-channel
   * socketpair. The supervisor's event-channel receiver consumes
   * authenticated InferenceEvent frames from here.
   */
  readonly eventReader: FrameReader;
  kill(signal?: number | string): void;
  exited: Promise<number>;
}

/**
 * Subprocess spawner the supervisor invokes to spawn the workflow-
 * process. The host injects `Bun.spawn` in production; tests inject
 * a deterministic mock. The supervisor invokes this with the
 * resolved binary path, the spawn-time env (carrying only the IPC
 * trust anchors plus substrate-config keys -- never the supervisor's
 * private key), and the prepared event-channel socketpair handle.
 */
export type SubprocessSpawner = (args: {
  /** Absolute path to the package-owned `bin/workflow-child` script. */
  binaryPath: string;
  /** Fresh env object containing IPC trust anchors + substrate-config keys. */
  env: Record<string, string>;
}) => SubprocessHandle;

/**
 * Frame the supervisor receives at its deploy ingress. The shape is
 * a structural projection of the sidecar's `agent.deploy` frame --
 * the supervisor does not depend on `@intx/types` for the wire
 * type. `config` is opaque to the supervisor; the host owns its
 * interpretation and passes it through to the trivial-launch
 * callback (multi-step routing carries it into spawn-time env in
 * later commits).
 */
export interface SupervisorDeployFrame {
  agentAddress: string;
  agentId: string;
  config: unknown;
  hubPublicKey: string;
}

/**
 * Callback the supervisor hands to the host so the host's per-message
 * reactor / harness lifecycle can drive the canonical run-event chain
 * (`RunStarted` -> `StepStarted` -> `StepCompleted` -> `RunCompleted`)
 * for the trivial deploy. The supervisor's closure resolves the
 * workflow-run repo identity, mints the `signAsPrincipal` signature
 * for each event, and commits the on-disk blob; the host calls this
 * with the event payload at the appropriate reactor moment.
 *
 * The on-disk envelope is identical to the one the multi-step branch
 * commits via its workflow-process child, which makes a trivial
 * deployment's audit trail indistinguishable from a multi-step one
 * from a downstream consumer's perspective. The split between the
 * two branches is process topology (in-process commit vs IPC-forwarded
 * commit), not observability.
 */
export type RecordRunEvent = (
  event: SupervisorRunEvent,
) => Promise<CommitRunEventResult>;

/**
 * Arguments handed to `trivialLaunch`. Mirrors the deploy frame
 * unchanged today (the trivial branch is a true passthrough); kept
 * as its own type so future trivial-only context (e.g. a
 * supervisor-derived deployment id) can attach without widening
 * the deploy frame itself.
 *
 * `recordRunEvent` is the seam the host wires into its existing
 * per-message reactor moments (`message.run.started` /
 * `message.run.ended`) to drive the canonical workflow-run event
 * chain inline from the supervisor's address space. Hosts that have
 * not yet wired the reactor seam supply a `trivialLaunch` body that
 * does not invoke the callback; the supervisor commits no events in
 * that case but the capability is available without further wiring
 * surgery.
 */
export interface TrivialLaunchBindings {
  agentAddress: string;
  agentId: string;
  config: unknown;
  hubPublicKey: string;
  recordRunEvent: RecordRunEvent;
}

/**
 * Host-injected callback the supervisor invokes on the trivial
 * branch. The supervisor's deploy() routes here for every single-
 * step deployment; the callback owns the entire trivial deploy.
 *
 * Invariants preserved by the trivial branch:
 *
 *   - The supervisor does not open an IPC channel.
 *   - The supervisor does not spawn a workflow-process child.
 *   - `credentialsSnapshot` is multi-step-only; the trivial branch
 *     leaves `getCredentialsSnapshot()` returning `null`.
 *
 * The supervisor DOES emit the canonical workflow-run event chain
 * (`RunStarted` / `StepStarted` / `StepCompleted` / `RunCompleted`)
 * for the trivial deploy inline from the supervisor process via
 * `signAsPrincipal` against the workflow-run repo. The chain fires
 * per inbound mail trigger (one run per fire) and is driven by the
 * host calling `bindings.recordRunEvent(...)` from its reactor /
 * harness lifecycle moments. The trivial-branch observability is
 * therefore identical to the multi-step branch's; the two branches
 * differ in process topology, not event surface.
 *
 * The host wires `trivialLaunch` against the legacy single-agent
 * provisioning surface so the on-wire bytes and on-disk surfaces
 * stay bit-identical to the pre-supervisor path; the `recordRunEvent`
 * hook is additive (the host's reactor calls it from existing
 * lifecycle brackets without changing the deploy-tree contents).
 */
export type TrivialLaunch = (bindings: TrivialLaunchBindings) => Promise<void>;

/**
 * Constructor arguments for `createWorkflowSupervisor`. The shape
 * is greybeard's Q1 (a) call: one `RepoStore` handle plus a
 * `signAsPrincipal` callback that mints signatures on demand per
 * principal, rather than pre-minting per-principal `RepoStore` views.
 * Every write-site is explicit about which principal it claims to
 * be, and the supervisor never holds a private key in plaintext.
 */
export interface WorkflowSupervisorBindings {
  /** Substrate handle the supervisor reads grants from and commits events to. */
  repoStore: SubstrateRepoStore;
  /** Per-principal signing callback. See `PrincipalSigner`. */
  signAsPrincipal: PrincipalSigner;
  /** Mail-bus surface for address registration and inbound subscription. */
  mailBus: MailBusBindings;
  /** Subprocess spawner the supervisor invokes per spawn. */
  subprocessSpawner: SubprocessSpawner;
  /**
   * Absolute path to the package-owned `bin/workflow-child` script
   * the spawner invokes. Pre-resolved by the host so the supervisor
   * does not have to consult `require.resolve` / `import.meta.resolve`
   * itself (tests inject a sentinel path the spawner mock asserts on).
   */
  binaryPath: string;
  /**
   * Substrate-config keys the binary needs to construct a RepoStore.
   * The supervisor carries these straight from the host into the
   * child's spawn-time env without inspecting them; the binary's
   * construction logic owns the shape.
   */
  substrateEnv: Record<string, string>;
  /**
   * Workflow-run repo identity for the deployment. The supervisor
   * commits its own CancelRequested / drain events here.
   */
  workflowRunRepoId: import("@intx/hub-sessions").RepoId;
  /** Workflow-run repo ref the supervisor commits events to. */
  workflowRunRef: string;
  /** Deployment id baked into the supervisor's principal claims. */
  deploymentId: string;
  /**
   * Mail address the deployment registers on the bus. The
   * supervisor registers this on spawn and unregisters on teardown;
   * inbound mail at this address flows through the supervisor's
   * trigger.fire path.
   */
  deploymentMailAddress: string;
  /**
   * The supervisor's substrate principal for read-only operations
   * the supervisor performs in its own right (e.g. enumerating
   * step grants). Cancel-signing constructs its own principal at
   * commit time; this binding is for non-write operations.
   */
  readPrincipal: Principal;
  /**
   * Per-step mail-address derivation the supervisor uses while
   * assembling the credentialsSnapshot. See `credentials.ts`.
   */
  deriveStepAddress: import("./credentials").DeriveStepAddress;
  /**
   * Optional override for the step's agent-state repo identity. The
   * default convention is `<deploymentId>-<stepId>`.
   */
  deriveStepRepoId?: import("./credentials").AssembleCredentialsSnapshotOpts["deriveStepRepoId"];
  /**
   * Optional override for the per-spawn IPC keypair factory. Each
   * spawn mints a fresh control-channel Ed25519 keypair; this hook
   * lets the host supply a deterministic factory (a test harness
   * that needs to assert on the env's HOST_PUBKEY, or a wiring path
   * that wants the keypair lifecycle to flow through its own crypto
   * boundary). Production wires it against the same
   * `@intx/crypto-node` generator the supervisor would have used.
   */
  ipcKeyPairFactory?: () => Promise<{
    privateKey: Uint8Array;
    publicKey: Uint8Array;
  }>;
  /**
   * Host-injected callback the supervisor invokes on the trivial
   * branch of `deploy(frame)`. Required for hosts that route deploy
   * frames through `supervisor.deploy`; the multi-step branch does
   * not consult it. See `TrivialLaunch` for the invariants.
   */
  trivialLaunch: TrivialLaunch;
  /**
   * Operator-overridable per-deployment `drainTimeout` in
   * milliseconds. The supervisor's `drain()` path threads this value
   * into every drainTimeout accumulator it arms. Absent value defers
   * to the accumulator's `DEFAULT_DRAIN_TIMEOUT_MS` constant.
   */
  drainTimeoutMs?: number;
  /**
   * Optional override for the supervisor's drainTimeout accumulator
   * factory. Production wires this against
   * `createDrainTimeoutAccumulator` directly; tests inject a mock
   * factory so the supervisor's drain arming becomes observable
   * without rigging a fake timer host. The factory shape matches
   * `createDrainTimeoutAccumulator`'s public signature exactly.
   */
  drainTimeoutAccumulatorFactory?: import("./drain-timeout").DrainTimeoutAccumulatorFactory;
  /**
   * Clock the supervisor threads into the drainTimeout accumulator.
   * Production wires `() => Date.now()`; tests inject a deterministic
   * fake clock. Defaults to `Date.now` when omitted.
   */
  now?: () => number;
  /**
   * Scheduling primitive the supervisor threads into the drainTimeout
   * accumulator. Production wires `(cb, ms) => setTimeout(cb, ms)`;
   * tests inject a deterministic timer host. Defaults to
   * `setTimeout` when omitted.
   */
  setTimer?: (cb: () => void, ms: number) => unknown;
  /**
   * Disposer paired with `setTimer`. Production wires
   * `(h) => clearTimeout(h as ReturnType<typeof setTimeout>)`;
   * tests inject the matching disposer for their fake timer host.
   * Defaults to `clearTimeout` when omitted.
   */
  clearTimer?: (handle: unknown) => void;
}

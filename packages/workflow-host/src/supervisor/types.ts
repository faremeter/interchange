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
  DequeueToProcessingResult,
  EnqueueInboxArgs,
  EnqueueInboxResult,
  MarkConsumedArgs,
  MarkConsumedResult,
  Principal,
  RepoId,
  RepoStore as SubstrateRepoStore,
  ReplayProcessingToInboxResult,
} from "@intx/hub-sessions/substrate";
import type {
  InferenceSource,
  OutboundMessage,
  SendReceipt,
} from "@intx/types/runtime";
import type { RunCancelled, RunCompleted, RunFailed } from "@intx/workflow";
import type { WorkflowDefinition } from "@intx/workflow/definition";

import type { FrameReader, NdjsonReader, NdjsonWriter } from "../ipc/index";
import type {
  CommitRunEventResult,
  SupervisorRunEvent,
} from "./run-event-signing";

/**
 * Terminal workflow-run event the supervisor's drain accumulators
 * consume to settle ahead of the drainTimeout deadline. The shape is
 * the discriminated union of every terminal kind the workflow-run
 * state machine emits; downstream wiring can switch on `kind` without
 * having to import the workflow package directly.
 */
export type TerminalRunEvent = RunCompleted | RunFailed | RunCancelled;

/**
 * Per-runId terminal-event source the supervisor consumes to settle
 * armed drainTimeout accumulators and the inbox dispatch loop when the
 * run reaches a terminal phase. Each invocation returns an
 * `AsyncIterable` scoped to one `runId`; the consumer pulls until the
 * first terminal event arrives or the iterator is finalised (via
 * `return()` on the iterator, which the accumulator calls during
 * `stop`).
 *
 * The supervisor owns a per-cohort broadcaster that implements this
 * shape against the `terminal.event` upstream control frames the
 * workflow-process child emits when its runtime body settles a run.
 * The accumulator and the dispatch loop borrow the broadcaster's
 * subscribe surface; consumers therefore do not have to round-trip
 * the workflow-run substrate from the supervisor's address space.
 */
export type TerminalEventSource = (
  runId: string,
) => AsyncIterable<TerminalRunEvent>;

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
) => Promise<SignedPayload>;

/**
 * Mail-bus interface the supervisor needs. The shape is the minimal
 * subset of an existing mail-bus API the supervisor's spawn / mail-
 * trigger / teardown lifecycle reaches into; it does not pin the
 * supervisor to `InMemoryTransport` or any other concrete bus.
 *
 * `subscribeMailForAddress` returns a disposer the supervisor calls
 * during teardown. The supplied handler is invoked with the raw RFC
 * 2822 message bytes of each inbound message at the address.
 *
 * `sendOutbound` is the OUTBOUND half of mailbox ownership (§3a). The
 * supervisor is the sole mail owner: the workflow-process child never
 * holds the agent's signing key and never calls `transport.send`
 * itself. When a step agent produces a reply (or invokes a mail-send
 * tool), the child forwards the structured `OutboundMessage` plus the
 * sender (agent) address up over the control IPC; the supervisor calls
 * `sendOutbound` to perform the actual signed send through the host's
 * real transport. The host implementation signs the message with the
 * sender's `CryptoProvider` exactly as the in-process path does
 * (`@intx/mail-memory`'s `executeSend`), so the outbound mail carries
 * the AGENT's signature with full parity to the pre-supervisor path.
 *
 * The signing identity lives at the host transport (registered per
 * address via `register(address, crypto)`); the supervisor does not
 * hold the agent's key and the child does not either. A send for an
 * address with no registered crypto surfaces loudly (the host
 * implementation throws) rather than emitting unsigned mail.
 */
export interface MailBusBindings {
  registerAddress(address: string): void;
  unregisterAddress(address: string): void;
  subscribeMailForAddress(
    address: string,
    handler: (rawMessage: Uint8Array) => void,
  ): () => void;
  sendOutbound(
    senderAddress: string,
    message: OutboundMessage,
  ): Promise<SendReceipt>;
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
 *
 * `workflow` is the multi-step projection. Absent on every trivial-
 * launch frame; presence is the discriminator the deploy router uses
 * to branch into `supervisor.spawn()` instead of `trivialLaunch`. The
 * field carries the workflow definition (so the supervisor can
 * construct the per-step substrate env without round-tripping the
 * hub) and each step's ordered inference-source failover chain keyed by
 * `definition.stepOrder` step ids.
 */
export interface SupervisorDeployFrame {
  agentAddress: string;
  agentId: string;
  config: unknown;
  hubPublicKey: string;
  workflow?: {
    definition: WorkflowDefinition;
    sources: Record<string, InferenceSource[]>;
  };
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
 * Logical pointer to the raw mail bytes the inbox claim-check
 * envelope stamps. The substrate stores this as the `mailAuditRef`
 * on every inbox/processing/consumed envelope; the substrate itself
 * never dereferences it. The boot edge supplies a derivation
 * coherent with wherever the deployment's mail audit actually lives
 * (a sidecar's session-mail audit, an alternative host's audit
 * store, etc.).
 */
export type MailAuditRef = { store: string; path: string };

/**
 * Host-supplied derivation that maps a parsed `messageId` plus the
 * raw mail bytes into the logical audit reference the inbox claim-
 * check envelope carries. Pure -- the supervisor invokes it on the
 * mail-arrival hot path and expects no I/O. Absent binding falls
 * back to a deterministic in-process derivation that synthesizes
 * `{ store: "in-process", path: <messageId> }` so the supervisor's
 * library tests that do not stand up an audit store still work; the
 * production sidecar boot edge supplies a real derivation.
 */
export type DeriveMailAuditRef = (
  messageId: string,
  rawMessage: Uint8Array,
) => MailAuditRef;

/**
 * Inbox claim-check primitives the supervisor's mail-arrival path
 * and dispatch loop reach into. Production wires this against the
 * concrete `enqueueInbox` / `dequeueToProcessing` / `markConsumed`
 * / `replayProcessingToInbox` functions exported from
 * `@intx/hub-sessions`; tests inject a deterministic in-memory
 * stub so the supervisor's dispatch loop is observable without a
 * real git substrate.
 *
 * The shape mirrors the upstream functions exactly so a binding
 * miss surfaces as a structural type error rather than a runtime
 * surprise.
 */
export interface InboxPrimitives {
  enqueueInbox(
    store: SubstrateRepoStore,
    principal: Principal,
    repoId: RepoId,
    args: EnqueueInboxArgs,
  ): Promise<EnqueueInboxResult>;
  dequeueToProcessing(
    store: SubstrateRepoStore,
    principal: Principal,
    repoId: RepoId,
    address: string,
  ): Promise<DequeueToProcessingResult>;
  markConsumed(
    store: SubstrateRepoStore,
    principal: Principal,
    repoId: RepoId,
    args: MarkConsumedArgs,
  ): Promise<MarkConsumedResult>;
  replayProcessingToInbox(
    store: SubstrateRepoStore,
    principal: Principal,
    repoId: RepoId,
    address: string,
  ): Promise<ReplayProcessingToInboxResult>;
}

/**
 * Constructor arguments for `createWorkflowSupervisor`. One
 * `RepoStore` handle plus a `signAsPrincipal` callback that mints
 * signatures on demand per principal, rather than pre-minting
 * per-principal `RepoStore` views. Every write-site is explicit
 * about which principal it claims to be, and the supervisor never
 * holds a private key in plaintext.
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
   * Number of steps in the deployed `WorkflowDefinition`
   * (`stepOrder.length`). The supervisor threads this into the child's
   * spawn-time env (`STEP_COUNT`) so the child's deploy-tree read
   * (`resolveStepAddress` in the sidecar step tools) collapses onto the
   * head for a single-step deployment exactly as the host's producer
   * push does -- one source of truth for the head/step collapse across
   * the two processes. Fixed for the deployment's lifetime.
   */
  stepCount: number;
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
  deriveStepRepoId?: import("./credentials").DeriveStepRepoId;
  /**
   * Optional override for the per-spawn IPC keypair factory. Each
   * spawn mints a fresh control-channel Ed25519 keypair; this hook
   * lets the host supply a deterministic factory (a test harness
   * that needs to assert on the env's HOST_PUBKEY, or a wiring path
   * that wants the keypair lifecycle to flow through its own crypto
   * boundary). Production wires it against the same
   * `@intx/crypto` generator the supervisor would have used.
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
   * General scheduling primitive the supervisor threads into its timed
   * waits: the drainTimeout accumulator, the spawn ready-handshake
   * timeout, and that timeout's SIGTERM->SIGKILL kill escalation.
   * Production wires `(cb, ms) => setTimeout(cb, ms)`; tests inject a
   * deterministic timer host. Defaults to `setTimeout` when omitted.
   */
  setTimer?: (cb: () => void, ms: number) => unknown;
  /**
   * Disposer paired with `setTimer`. Production wires
   * `(h) => clearTimeout(h as ReturnType<typeof setTimeout>)`;
   * tests inject the matching disposer for their fake timer host.
   * Defaults to `clearTimeout` when omitted.
   */
  clearTimer?: (handle: unknown) => void;
  /**
   * Optional recycle-policy configuration. Absent or all-`undefined`
   * fields disable the periodic check; the supervisor's policy timer
   * is not armed. Operator overrides land here via the host's
   * per-deployment config.
   */
  recyclePolicy?: import("./recycle").RecyclePolicyBounds;
  /**
   * Optional RSS reader the policy consults per tick when
   * `recyclePolicy.maxRssBytes` is set. Returns the workflow-process
   * child's current resident-set size in bytes, or `undefined` if the
   * host does not have a current sample.
   */
  readRssBytes?: () => number | undefined;
  /**
   * Optional grants-age reader the policy consults per tick when
   * `recyclePolicy.maxGrantsAgeMs` is set. Returns the wall-clock age
   * (ms) of the most recent grants refresh visible to this
   * supervisor, or `undefined` if no refresh has been observed yet.
   */
  readGrantsAgeMs?: () => number | undefined;
  /**
   * Optional now-reader for the recycle policy. Defaults to
   * `Date.now()`; tests inject a deterministic clock.
   */
  recyclePolicyNow?: () => number;
  /**
   * Optional setTimer/clearTimer pair for the recycle policy and the
   * recycle path's SIGKILL escalation. Defaults to
   * `setTimeout`/`clearTimeout`; tests inject a controllable timer.
   */
  recyclePolicySetTimer?: (cb: () => void, ms: number) => unknown;
  recyclePolicyClearTimer?: (handle: unknown) => void;
  /**
   * Compute the logical audit reference for an inbox entry. The
   * substrate stamps this onto the envelope; the substrate itself
   * does not dereference it. The boot edge supplies a derivation
   * coherent with wherever the deployment's mail audit lives.
   *
   * Absent binding falls back to `{ store: "in-process",
   * path: <messageId> }`, a deterministic derivation that lets the
   * supervisor's library tests run without a host-side audit store.
   */
  deriveMailAuditRef?: DeriveMailAuditRef;
  /**
   * Inbox claim-check primitives the dispatch loop reaches into.
   * Production wires this against `@intx/hub-sessions`'s concrete
   * `enqueueInbox` / `dequeueToProcessing` / `markConsumed` /
   * `replayProcessingToInbox`; tests inject a deterministic
   * in-memory stub so the dispatch loop is observable without a
   * git substrate.
   */
  inboxPrimitives?: InboxPrimitives;
  /**
   * Workflow-run substrate principal the supervisor uses to author
   * inbox/processing/consumed writes. The substrate's workflow-run
   * kind handler accepts a `{ kind: "supervisor", deploymentId }`
   * principal for claim-check writes; the supervisor constructs this
   * value once at bindings construction and reuses it for every
   * claim-check operation. Defaults to `{ kind: "supervisor",
   * deploymentId }` derived from `bindings.deploymentId`; tests
   * override it when they need to assert on a structurally distinct
   * principal shape.
   */
  inboxWritePrincipal?: Principal;
  /**
   * Retention horizon for the consumed dedup index, in milliseconds.
   * Threaded into every `markConsumed` so the per-address watermark
   * advances to `consumedAt - consumedRetentionMs` and the consumed/
   * index is pruned to a bounded steady state. This is an
   * OPERATOR-policy value: the longest window in which the same
   * message could legitimately be re-submitted and still must be
   * caught as a duplicate. The boot edge resolves the operator's
   * config and supplies it; absent, `DEFAULT_CONSUMED_RETENTION_MS`
   * (24h) applies. The invariant the operator owns: the horizon must
   * be >= the maximum redelivery window of any at-least-once source if
   * one is ever added, or dedup breaks (a breach surfaces loudly as a
   * refused stale enqueue, not silent double-processing).
   */
  consumedRetentionMs?: number;
  /**
   * Bound on the child's spawn-time `ready` handshake, in milliseconds.
   * A spawned child that neither emits `ready` nor exits would block
   * `spawn` forever; on expiry the supervisor kills the child (SIGTERM,
   * then SIGKILL) and rejects the spawn. The boot edge resolves the
   * operator's config and supplies it; absent, `DEFAULT_READY_TIMEOUT_MS`
   * (30s) applies. Callers surface the rejection through their existing
   * spawn-failure path, so a wedged child fails the deploy (or, on the
   * sidecar, is skipped by boot-time restore) instead of hanging it.
   */
  readyTimeoutMs?: number;
  /**
   * Watchdog timeout (ms) for the supervisor's substrate-write
   * handler's wait on the dispatch loop's `markConsumed` when a
   * terminal-event blob lands in a proxied write. Defaults to
   * `DEFAULT_TERMINAL_WRITE_WATCHDOG_MS`. Tests inject a small value
   * so the watchdog path is observable without holding a test loop
   * for the production duration.
   */
  terminalWriteWatchdogMs?: number;
  /**
   * Optional per-message dispatch-timing observer. When supplied, the
   * dispatch loop invokes it twice per dispatched inbox entry: once with
   * marker `"dispatch-start"` the instant the entry is dequeued for
   * dispatch (claim-check read complete, `trigger.fire` about to be
   * forwarded) and once with marker `"reply-produced"` the instant the
   * child's terminal-event frame for that run lands back at the
   * supervisor (the run's reply is produced and committed). Both marks
   * carry the same monotonic `atMs` clock so an observer can compute the
   * per-message infra round-trip the unified path adds over a bare
   * in-process `agent.send`.
   *
   * This is a pure observability hook with no control-flow effect. It is
   * absent in production and wired only by the Phase 4.7 latency-gate
   * benchmark, which threads it through the sidecar host so the
   * supervisor (running in the sidecar subprocess, where both ends of
   * the IPC round-trip are visible in one process) emits a parseable
   * timing line. A throwing observer is swallowed and logged so a
   * benchmark hook bug cannot wedge dispatch.
   */
  onDispatchTiming?: (mark: DispatchTimingMark) => void;
  /**
   * D2 §10c forced-repack A/B toggle (measurement-only). When supplied
   * with a non-zero `everyMessages`, the dispatch loop forces a
   * `git gc`/repack of the workflow-run repo every `everyMessages`-th
   * dispatched message (after `markConsumed`, under the single-writer
   * discipline). This exists solely to discriminate pack-growth from
   * tree-fan-out as the dominant per-message substrate cost: if forcing a
   * repack flattens the per-leg slope the cost is loose-object/pack
   * growth (cheap pack/gc fix); if it does not, the cost is the
   * per-commit root-tree rewrite scaling with `runs/` + `consumed/`
   * fan-out (run-model change). Absent in production -- the dispatch path
   * forks no `git gc` when this is unset.
   */
  repackEveryMessages?: { everyMessages: number };
}

/**
 * The five per-message substrate legs the D2 attribution splits the
 * unified path's substrate tax across. Each is a single git
 * `writeTreePreservingPrefix` commit (or, for `runevent`, one of several
 * commits per message) against the growing workflow-run repo:
 *
 *   - `enqueue`      — `enqueueInbox` in `onMailMessage`, BEFORE dispatch
 *                      (paid OUTSIDE the dispatch-start..reply-produced
 *                      window; its growth is invisible to the 4.7 bracket).
 *   - `dequeue`      — `dequeueToProcessing`, the claim-check READ at the
 *                      head of `dispatchOne` (inside the window).
 *   - `runevent`     — a run-event bracket commit
 *                      (`runs/<runId>/events/<seq>.json`), arriving as a
 *                      child-proxied `substrate.write.request` (inside the
 *                      window). One message may produce several; each is
 *                      stamped and the D2 post-processing sums them and
 *                      counts them per message.
 *   - `markconsumed` — `markConsumed` at the tail of `dispatchOne`, AFTER
 *                      `reply-produced` (paid OUTSIDE the window).
 *   - `wal`          — the D1 conversation WAL append / checkpoint
 *                      (`agent-state/<key>/...`), arriving as a
 *                      child-proxied `substrate.write.request`. The control
 *                      leg: post-D1 it should be small and flat.
 */
export type DispatchSubstrateLeg =
  | "enqueue"
  | "dequeue"
  | "runevent"
  | "markconsumed"
  | "wal";

/**
 * Structural counters sampled at a `leg` mark's `"end"` phase so the D2
 * attribution can explain WHY a leg grows, not merely that it does
 * (design §10b). All are cheap filesystem reads against the workflow-run
 * repo's on-disk working tree, taken only when the observer is wired.
 *
 *   - `runsFanOut`     — entry count under `runs/` (one subdir per message;
 *                        never pruned). The candidate-(i) "collapse runs"
 *                        win is sized by this.
 *   - `consumedFanOut` — entry count under
 *                        `addresses/<addr>/consumed/` (one dedup entry per
 *                        message; never pruned). The candidate-(iv) "prune
 *                        consumed" win is sized by this.
 *   - `looseObjects`   — count of loose git objects under
 *                        `.git/objects/<xx>/` (the pack-growth proxy; the
 *                        §10c repack A/B targets this).
 *   - `gitBytes`       — total byte size of the repo's `.git` directory
 *                        (loose + pack), a coarse repo-size proxy.
 */
export type DispatchStructuralCounters = {
  runsFanOut: number;
  consumedFanOut: number;
  looseObjects: number;
  gitBytes: number;
};

/**
 * One observation emitted by `WorkflowSupervisorBindings.onDispatchTiming`.
 *
 * The `"roundtrip"` variant is the 4.7 latency-gate bracket: pair the
 * `"dispatch-start"` and `"reply-produced"` marks for the same `runId` to
 * recover the per-message round-trip. `atMs` is a high-resolution
 * monotonic timestamp (`performance.now()`).
 *
 * The `"leg"` variant is the D2 per-leg attribution surface: a paired
 * `start`/`end` mark around one of the five substrate commits
 * (`DispatchSubstrateLeg`), so each leg's per-message slope (ms added per
 * sustained message) and floor (intercept) can be fit independently. The
 * `end` mark of a `runevent`/`markconsumed`/`enqueue`/`dequeue` leg also
 * carries the structural counters sampled at commit time. Both variants
 * flow through the same off-by-default observer; production leaves the
 * observer unwired and samples no clock and no counter.
 */
export type DispatchTimingMark =
  | {
      kind: "roundtrip";
      runId: string;
      marker: "dispatch-start" | "reply-produced";
      atMs: number;
    }
  | {
      kind: "leg";
      runId: string;
      leg: DispatchSubstrateLeg;
      phase: "start" | "end";
      atMs: number;
      /** Sampled only on the `"end"` phase; absent on `"start"`. */
      counters?: DispatchStructuralCounters;
    };

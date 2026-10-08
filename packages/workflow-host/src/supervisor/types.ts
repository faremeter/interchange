// Public type shapes for the per-deployment supervisor surface.
//
// The supervisor takes its bindings as constructor arguments (RepoStore,
// per-principal signing callback, mail-bus bindings, subprocess spawner),
// so any host -- sidecar, test harness, or future CLI -- can construct it
// against the same interface.

import type {
  DequeueToProcessingResult,
  EnqueueInboxArgs,
  EnqueueInboxOutcome,
  MarkConsumedArgs,
  MarkConsumedResult,
  Principal,
  RepoId,
  RepoStore as SubstrateRepoStore,
  ReplayProcessingToInboxOpts,
  ReplayProcessingToInboxResult,
} from "@intx/hub-sessions/substrate";
import type { SignalKind } from "@intx/types";
import type {
  ApprovalSnapshot,
  OutboundMessage,
  SendReceipt,
} from "@intx/types/runtime";
import type { RunCancelled, RunCompleted, RunFailed } from "@intx/workflow";

import type { FrameReader, NdjsonReader, NdjsonWriter } from "../ipc/index";

/** Terminal run event union the drain accumulators settle on; consumers switch on kind without importing the workflow package. */
export type TerminalRunEvent = RunCompleted | RunFailed | RunCancelled;

/** Per-runId terminal-event source the drain accumulators and dispatch loop consume; each call returns an AsyncIterable scoped to one runId. The supervisor's per-cohort broadcaster implements it from the child's terminal.event frames. */
export type TerminalEventSource = (
  runId: string,
) => AsyncIterable<TerminalRunEvent>;

/** Principal kinds the supervisor signs as; mirrors the kinds the workflow-run kind handler binds to CancelRequested origins. */
export type WorkflowSupervisorPrincipalKind = "supervisor";

/** Output of a signAsPrincipal call: the raw Ed25519 signature plus the principal kind, so a verifier maps it to the right key. */
export type SignedPayload = {
  /** Raw signature bytes from Ed25519 (64 bytes per RFC 8032). */
  readonly sig: Uint8Array;
  /** Principal kind the host signed as. */
  readonly principalKind: WorkflowSupervisorPrincipalKind;
};

/** Host-supplied per-principal signing callback: the supervisor never holds the principal's private key, it asks the host to sign. */
export type PrincipalSigner = (
  kind: WorkflowSupervisorPrincipalKind,
  payload: Uint8Array,
) => Promise<SignedPayload>;

/** Minimal mail-bus surface for the supervisor's spawn/mail/teardown lifecycle; not pinned to any concrete bus.

subscribeMailForAddress returns a disposer; the handler resolves on durable acceptance (ack) and rejects otherwise (withhold). sendOutbound is the OUTBOUND half of mailbox ownership (§3a): the supervisor performs the signed send through the host transport so the mail carries the agent's signature; an address with no registered crypto throws loudly. */
export interface MailBusBindings {
  registerAddress(address: string): void;
  unregisterAddress(address: string): void;
  subscribeMailForAddress(
    address: string,
    handler: (rawMessage: Uint8Array) => Promise<void>,
  ): () => void;
  sendOutbound(
    senderAddress: string,
    message: OutboundMessage,
  ): Promise<SendReceipt>;
}

/** Handle the spawner returns; mirrors Bun.spawn's shape so tests can substitute an in-process implementation. stdin/stdout carry the signed control channel, eventReader the HMAC-authenticated event channel. */
export interface SubprocessHandle {
  readonly pid: number;
  /** Writer for the supervisor-to-child control channel (child stdin); the control sender feeds NDJSON lines through it. */
  readonly controlWriter: NdjsonWriter;
  /** Reader for the child-to-supervisor control channel (child stdout); carries ready and rare upstream frames. */
  readonly controlReader: NdjsonReader;
  /** Supervisor-side handle on the event-channel socketpair; the event receiver consumes authenticated frames from here. */
  readonly eventReader: FrameReader;
  kill(signal?: number | string): void;
  exited: Promise<number>;
}

/** Spawner the supervisor invokes per spawn; production injects Bun.spawn, tests a deterministic mock. The env carries only IPC trust anchors plus substrate-config keys, never the supervisor's private key. */
export type SubprocessSpawner = (args: {
  /** Absolute path to the host-owned `bin/workflow-child` script. */
  binaryPath: string;
  /** Fresh env object containing IPC trust anchors + substrate-config keys. */
  env: Record<string, string>;
}) => SubprocessHandle;

/** Logical pointer to the mail bytes stamped on every claim-check envelope; the substrate never dereferences it, the boot edge supplies a derivation coherent with where mail audit lives. */
export type MailAuditRef = { store: string; path: string };

/** Pure host-supplied derivation of the claim-check mail audit ref; absent, an in-process { store: "in-process", path: messageId } fallback keeps library tests audit-store-free. */
export type DeriveMailAuditRef = (
  messageId: string,
  rawMessage: Uint8Array,
) => MailAuditRef;

/** Inbox claim-check primitives; production wires the @intx/hub-sessions functions, tests a deterministic in-memory stub. The shape mirrors the upstream functions exactly so a binding miss is a type error, not a runtime surprise. */
export interface InboxPrimitives {
  enqueueInbox(
    store: SubstrateRepoStore,
    principal: Principal,
    repoId: RepoId,
    args: EnqueueInboxArgs,
  ): Promise<EnqueueInboxOutcome>;
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
    opts?: ReplayProcessingToInboxOpts,
  ): Promise<ReplayProcessingToInboxResult>;
}

/** A park the supervisor forwards to the host, stamped with its own anchorRunId and agentAddress; the host turns it into a signal.correlation.register frame. */
export interface SuspensionRegistration {
  runId: string;
  correlationId: string;
  kind: SignalKind;
  anchorRunId: string;
  agentAddress: string;
  /** Approver snapshot of the parked tool call; present only for an ask-rail suspension. */
  approvalSnapshot?: ApprovalSnapshot;
}

/** Constructor arguments for createWorkflowSupervisor: a RepoStore handle plus a signAsPrincipal callback; every write site claims its principal and the supervisor never holds a private key. */
export interface WorkflowSupervisorBindings {
  /** Substrate handle the supervisor reads grants from and commits events to. */
  repoStore: SubstrateRepoStore;
  /** Per-principal signing callback. See `PrincipalSigner`. */
  signAsPrincipal: PrincipalSigner;
  /** Mail-bus surface for address registration and inbound subscription. */
  mailBus: MailBusBindings;
  /** Optional suspension sink, invoked by the park.notify arm and reEmitParkedCorrelations. Best-effort: a throwing sink is logged and both callers keep going. */
  onSuspensionRegister?: (registration: SuspensionRegistration) => void;
  /** Self-termination sink, fired when the supervisor reaches a terminal phase on its own (crash-loop latch, channel crash while recycling, recycle failure) but not on host shutdown or a failed initial spawn; the sidecar reclaims the deployment address. MUST be idempotent and, unlike onSuspensionRegister, total: a missed reclaim strands the address until an operator undeploys. */
  onSelfTerminate?: (info: {
    phase: "stopped" | "crash-looping";
    reason: string;
  }) => void;
  /** Per-run grants source consulted before each trigger.fire. A request/response contract: the supervisor pushes the returned snapshot before the trigger, and a throwing sink fails the run (synthesized RunFailed) rather than firing against absent grants. When wired it is the SOLE grants push (spawn skips its snapshot). */
  onRunStart?: (args: {
    runId: string;
    anchorRunId: string;
  }) => Promise<import("./credentials").CredentialsSnapshot>;
  /** Decrypted credential material delivered on the pre-trigger barrier; rotations flow through deliverCredentials, not this static binding. */
  credentialDelivery?: import("@intx/types/sidecar").CredentialDelivery;
  /** Subprocess spawner the supervisor invokes per spawn. */
  subprocessSpawner: SubprocessSpawner;
  /** Absolute path to the workflow-child script the spawner invokes; pre-resolved by the host. */
  binaryPath: string;
  /** Substrate-config keys carried into the child's spawn-time env; the supervisor never inspects them. */
  substrateEnv: Record<string, string>;
  /** Per-spawn env entries recomputed for every spawn and recycle respawn (unlike the frozen substrateEnv), so a host-revised value reaches the respawned child. Keys layer over substrateEnv, under the IPC anchors. */
  dynamicSpawnEnv: () => Record<string, string>;
  /**
   * Workflow-run repo identity for the deployment. The supervisor
   * commits its own CancelRequested / drain events here.
   */
  workflowRunRepoId: import("@intx/hub-sessions").RepoId;
  /** Workflow-run repo ref the supervisor commits events to. */
  workflowRunRef: string;
  /** Anchor run id baked into the supervisor's principal claims. */
  anchorRunId: string;
  /** Step count (stepOrder.length), threaded as STEP_COUNT so the child's deploy-tree read collapses onto the head for single-step deployments exactly as the host's push does. Fixed for the deployment's lifetime. */
  stepCount: number;
  /** Mail address registered on spawn, unregistered on teardown; inbound mail flows through the trigger.fire path. */
  deploymentMailAddress: string;
  /** Supervisor principal for its own read-only operations (e.g. enumerating step grants). */
  readPrincipal: Principal;
  /**
   * Per-step mail-address derivation the supervisor uses while
   * assembling the credentialsSnapshot. See `credentials.ts`.
   */
  deriveStepAddress: import("./credentials").DeriveStepAddress;
  /** Optional override for the step's agent-state repo identity; default is <anchorRunId>-<stepId>. */
  deriveStepRepoId?: import("./credentials").DeriveStepRepoId;
  /** Optional per-spawn IPC keypair factory override; tests use a deterministic factory to assert on HOST_PUBKEY. */
  ipcKeyPairFactory?: () => Promise<{
    privateKey: Uint8Array;
    publicKey: Uint8Array;
  }>;
  /** Operator-overridable drain timeout (ms), threaded into every drain accumulator; absent defers to DEFAULT_DRAIN_TIMEOUT_MS. */
  drainTimeoutMs?: number;
  /** Optional drainTimeout accumulator factory override; tests inject a mock so drain arming is observable without a fake timer host. */
  drainTimeoutAccumulatorFactory?: import("./drain-timeout").DrainTimeoutAccumulatorFactory;
  /** Clock threaded into the drain accumulator; tests inject a deterministic fake, defaults to Date.now. */
  now?: () => number;
  /** Scheduling primitive for the drain accumulator, ready handshake, and kill escalation; tests inject a deterministic timer host, defaults to setTimeout. */
  setTimer?: (cb: () => void, ms: number) => unknown;
  /** Disposer paired with setTimer; defaults to clearTimeout. */
  clearTimer?: (handle: unknown) => void;
  /** Optional recycle-policy bounds; absent or all-undefined disables the periodic check. */
  recyclePolicy?: import("./recycle").RecyclePolicyBounds;
  /** RSS reader the policy consults per tick when maxRssBytes is set; undefined when no current sample exists. */
  readRssBytes?: () => number | undefined;
  /** Grants-age reader the policy consults when maxGrantsAgeMs is set; undefined before any refresh. */
  readGrantsAgeMs?: () => number | undefined;
  /** Now-reader for the recycle policy; tests inject a deterministic clock. */
  recyclePolicyNow?: () => number;
  /** Timer pair for the recycle policy and its SIGKILL escalation; tests inject a controllable timer. */
  recyclePolicySetTimer?: (cb: () => void, ms: number) => unknown;
  recyclePolicyClearTimer?: (handle: unknown) => void;
  /** Compute the claim-check mail audit ref; absent, an in-process fallback keeps library tests audit-store-free. */
  deriveMailAuditRef?: DeriveMailAuditRef;
  /** Inbox claim-check primitives for the dispatch loop; tests inject a deterministic in-memory stub. */
  inboxPrimitives?: InboxPrimitives;
  /** Principal authoring claim-check writes; defaults to { kind: "supervisor", anchorRunId }, overridable for structural assertions. */
  inboxWritePrincipal?: Principal;
  /** Consumed-dedup retention horizon (ms) threaded into every markConsumed, pruning the index to a bounded steady state. Operator-policy value: must be >= the max redelivery window of any at-least-once source, or dedup breaks (a breach surfaces as a refused stale enqueue). */
  consumedRetentionMs?: number;
  /** Bound on the ready handshake: a child that neither readies nor exits would block spawn forever, so on expiry the supervisor kills it (SIGTERM then SIGKILL) and rejects. */
  readyTimeoutMs?: number;
  /** Crash-loop guard: unexpected exits within the window before the deployment latches; operator-owned bound. */
  crashLoopMaxCount?: number;
  /** Sliding window over which crashLoopMaxCount exits latch the deployment. */
  crashLoopWindowMs?: number;
  /** Respawned-child uptime that resets the crash counter, so a crash burst followed by stability does not latch. */
  crashLoopStableResetMs?: number;
  /** Initial respawn backoff (ms); each respawn doubles it up to the cap, a stable run resets it. */
  respawnBackoffInitialMs?: number;
  /** Cap on exponential respawn backoff (ms). Config invariant: keep it below crashLoopWindowMs -- a cap at or above the window lets a slow flapper's crashes age out before the count latches. */
  respawnBackoffMaxMs?: number;
  /** Watchdog on the parked-correlations response wait: a wedged-but-alive child never aborts its cohort, so cap the wait; on expiry the next re-establishment re-drives. */
  parkedQueryWatchdogMs?: number;
  /** Optional per-message dispatch-timing observer: dispatch-start on dequeue, reply-produced when the run's terminal frame lands, both on a monotonic clock so the per-message round-trip is computable. Pure observability, absent in production; a throwing observer is swallowed and logged. */
  onDispatchTiming?: (mark: DispatchTimingMark) => void;
  /** D2 §10c forced-repack A/B toggle (measurement-only): force a repack every everyMessages-th message to discriminate pack growth from tree fan-out as the dominant per-message substrate cost. Absent in production. */
  repackEveryMessages?: { everyMessages: number };
}

/** The five per-message substrate legs the D2 attribution splits the substrate tax across: enqueue (before dispatch), dequeue (the claim-check read), runevent (run-event commits, inside the window), markconsumed (after reply-produced), and wal (the D1 conversation WAL). */
export type DispatchSubstrateLeg =
  | "enqueue"
  | "dequeue"
  | "runevent"
  | "markconsumed"
  | "wal";

/** Structural counters sampled at a leg's end so the D2 attribution can explain WHY a leg grows: runs/ and consumed/ fan-out, loose-object count (pack-growth proxy), and .git byte size. */
export type DispatchStructuralCounters = {
  runsFanOut: number;
  consumedFanOut: number;
  looseObjects: number;
  gitBytes: number;
};

/** One onDispatchTiming observation, keyed on messageId (the top-level run id is stable per deployment and cannot distinguish messages). "roundtrip" pairs dispatch-start/reply-produced; "leg" pairs start/end around one substrate commit, with structural counters on the end mark. */
export type DispatchTimingMark =
  | {
      kind: "roundtrip";
      messageId: string;
      marker: "dispatch-start" | "reply-produced";
      atMs: number;
    }
  | {
      kind: "leg";
      messageId: string;
      leg: DispatchSubstrateLeg;
      phase: "start" | "end";
      atMs: number;
      /** Sampled only on the `"end"` phase; absent on `"start"`. */
      counters?: DispatchStructuralCounters;
    };

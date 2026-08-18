// Hub-side websocket handler for sidecar connections.
//
// Accepts websocket upgrades, processes register frames, maintains a routing
// table of agentAddress → sidecar connection, and dispatches frames between
// sidecars and the hub's internal systems.

import { getLogger } from "@intx/log";
import { verifyEd25519 } from "@intx/crypto";
import { chunkPack, createPackReceiver } from "@intx/pack-transport";
import {
  base64Decode,
  deriveMessageId,
  deriveWorkflowRunId,
  hexDecode,
  hexEncode,
  isRunAddress,
} from "@intx/types";
import { deriveWorkflowRunRepoId } from "@intx/workflow-deploy";
import { type } from "arktype";
import {
  SidecarFrame,
  type AgentDeployAckFrame,
  type AgentDeployFrame,
  type PackAckFrame,
  type HubFrame,
  type PackPushFrame,
  type PackDoneFrame,
  type PackRejectFrame,
  type RepoId,
  type RunGrantsFrame,
  type SignalCorrelationRegisterFrame,
  type CredentialDelivery,
  type WorkflowSourceAssetMount,
  type WorkflowProjectionDefinition,
} from "@intx/types/sidecar";
import type {
  ConnectorThreadState,
  HarnessConfig,
  InferenceSource,
} from "@intx/types/runtime";
import type { SidecarCredentialIdentity } from "../sidecar-allocation/contracts";
import type { ToolPackageManifest } from "@intx/types/tool-packages";
import type { WorkflowDefinitionSource } from "@intx/types/workflow-sources";
import {
  createSidecarEmitter,
  type SidecarEventEmitter,
  type SidecarLookups,
  type SidecarMailPersistedRow,
} from "./sidecar-events";

const logger = getLogger(["hub", "ws", "sidecar"]);

export type SidecarConnection = {
  sidecarId: string;
  identity: SidecarAuthIdentity;
  agentAddresses: Set<string>;
  // Workflow-substrate deployment run addresses (`run_<hex>@domain`) this
  // connection hosts. Kept separate from `agentAddresses`: these are hub-minted
  // and
  // registered for routing WITHOUT the per-address challenge, so they must
  // not be dragged through the challenge/re-add dance the session addresses
  // take. `handleClose` cleans both sets out of `addressIndex`.
  workflowAddresses: Set<string>;
  send(frame: HubFrame): void;
};

/**
 * Whether this connection owns `address` for routing/lifecycle purposes --
 * as a challenged session address OR a hub-minted workflow-substrate address.
 * The two sets are kept physically distinct (they differ on the challenge /
 * re-add path), but ownership readers -- pack-transfer authorization,
 * in-flight cancellation, disconnect teardown -- must see the union, or a
 * reconnected workflow deployment (which lives only in `workflowAddresses`)
 * is silently treated as unowned even though its mail routes.
 */
function connOwnsAddress(conn: SidecarConnection, address: string): boolean {
  return (
    conn.agentAddresses.has(address) || conn.workflowAddresses.has(address)
  );
}

/**
 * Bind pack writes to the repository implied by the authenticated address.
 * An allocated credential is narrower still: it may only write its one
 * deployment's workflow-run repository and never a standalone agent-state
 * repository.
 */
function connCanPushRepo(
  conn: SidecarConnection,
  agentAddress: string,
  repoId: RepoId,
): boolean {
  if (
    conn.identity.kind === "allocated" &&
    agentAddress !== conn.identity.workflowRunAddress
  ) {
    return false;
  }
  if (repoId.kind === "agent-state") {
    return conn.identity.kind === "shared" && repoId.id === agentAddress;
  }
  return (
    repoId.kind === "workflow-run" &&
    repoId.id === deriveWorkflowRunRepoId(agentAddress)
  );
}

/** The deduped set of every address this connection owns (session + workflow). */
function ownedAddresses(conn: SidecarConnection): Set<string> {
  return new Set([...conn.agentAddresses, ...conn.workflowAddresses]);
}

type PendingRequest = {
  requestId: string;
  ws: WsHandle;
  resolve(): void;
  reject(error: string): void;
  timer: ReturnType<typeof setTimeout>;
};

export type SendPackOptions = {
  /**
   * Repo-relative mount path under the sidecar's per-agent workspace.
   * When set, the receiving sidecar materializes the pack as plain
   * files at `<workspaceRoot>/<mountPath>/` and does NOT apply it to
   * the agent's deploy git tree. Absent for agent-state deploy/state
   * packs, which continue to apply to the deploy tree.
   */
  mountPath?: string;
  /**
   * Override the `repoId` emitted on the wire. The agent-state flow
   * defaults to `{ kind: "agent-state", id: agentAddress }`; asset
   * packs must pass the SOURCE asset's id so audit can correlate the
   * pack back to its hub-side origin. Workflow-run restoration uses a
   * dedicated allocation-bound sender that supplies its derived repo id.
   */
  repoId?: RepoId;
};

/**
 * Everything a `sendProbe` caller supplies to populate the outbound
 * `workflow.probe.request` frame: where the definition's bytes come from, the
 * frozen dependency closure the hub already resolved, and the
 * `interchange.workflow` entry-module path whose evaluation produces the
 * `WorkflowDefinition`. The `requestId` is minted inside `sendProbe`, not
 * supplied here.
 */
export type SendProbeArgs = {
  source: WorkflowDefinitionSource;
  closure: ToolPackageManifest;
  entry: string;
  /** Hub assets a `kind:"asset"` closure entry reads from, delivered inline. */
  assets?: WorkflowSourceAssetMount[];
};

/**
 * The payload a `sendProbe` promise resolves with, lifted off the sidecar's
 * `workflow.probe.result` frame: the inert needs-surface projection of the
 * probed workflow, the inert grant set derived from it, and the projection's
 * content hash.
 */
export type WorkflowProbeResult = {
  projection: WorkflowProjectionDefinition;
  grants: string[];
  wireHash: string;
};

export type SidecarRouter = {
  handleOpen(ws: WsHandle): void;
  handleMessage(ws: WsHandle, data: string): void;
  handleClose(ws: WsHandle): void;

  routeMail(
    agentAddress: string,
    rawMessage: string,
    messageId?: string,
  ): boolean;
  /**
   * Deliver a run's authorization grants to the sidecar hosting the named
   * deployment-level mail address, ahead of the trigger mail that starts the
   * run. Routes through the same per-address channel as `routeMail`: over the
   * live connection when the deployment is connected, and into the disconnect
   * queue when the deployment dropped in the window before its first
   * reconnect (while its address is still on `agentAddresses`) -- so grants
   * are queued for a disconnected deployment exactly when the trigger mail is,
   * and ride the same reconnect flush. After a challenged reconnect the
   * address moves to `workflowAddresses`, which carries no queue (that
   * generation's in-flight state is reconstructed sidecar-locally); a
   * `run.grants` then has no queue to ride and this returns `false`. Returns
   * `false` whenever the address is unroutable; the caller keeps any stable-run
   * grant reservation so a later first-delivery attempt reuses it.
   */
  sendRunGrants(
    agentAddress: string,
    runId: string,
    stepGrants: RunGrantsFrame["stepGrants"],
  ): boolean;
  /**
   * Returns the current connector-thread state for the named agent, or
   * `null` if the agent has no active connector thread (or if the
   * sidecar has not yet reported any state — e.g. mid-reconnect, before
   * the harness has loaded its context store). The state is cached
   * from `connector.state.changed` frames; callers should treat `null`
   * as "no threading info available" and fall through to whatever
   * default the calling path uses.
   */
  getConnectorState(agentAddress: string): ConnectorThreadState | null;
  /**
   * Send an `agent.deploy` frame to the sidecar. When `workflow` is
   * supplied, the frame carries the multi-step deploy projection
   * (workflow definition plus per-step source pins); the sidecar's
   * deploy router routes it to the workflow deploy path. The sole
   * caller supplies `workflow` on every deploy; per-step provisioning
   * uses `sendProvisionStep`.
   *
   * The returned promise resolves with the supervisor's principal
   * public key (hex-encoded Ed25519) carried on `agent.deploy.ack`.
   * The legacy callers that ignore the return value continue to work
   * unchanged.
   */
  sendAgentDeploy(
    agentAddress: string,
    config: HarnessConfig,
    workflow?: AgentDeployFrame["workflow"],
  ): Promise<{ publicKey: string }>;
  /**
   * Ask a connected sidecar to probe a code-sourced workflow WITHOUT
   * deploying it, and resolve with the sidecar's inert answer (needs-surface
   * projection + grant set + content hash). Selects any connected sidecar via
   * `findSidecarForNewAgent` -- the probe runs in the sidecar's pre-deploy
   * state, so it needs no deployed agent and enters no address map -- and
   * correlates the round-trip purely by a minted `requestId`. Rejects if no
   * sidecar is connected, if the probe times out (`probeTimeoutMs`), if the
   * sidecar answers `workflow.probe.error`, or if the sidecar disconnects with
   * the probe in flight.
   *
   * Optional so existing `SidecarRouter` consumers -- and their test doubles
   * -- that never probe need not implement it, mirroring `DeployRouter`'s
   * optional `undeploy`. The concrete `createSidecarRouter` always provides
   * it.
   */
  sendProbe?(args: SendProbeArgs): Promise<WorkflowProbeResult>;
  sendAgentUndeploy(agentAddress: string, reason: string): Promise<void>;
  sendSourcesUpdate(
    agentAddress: string,
    sources: InferenceSource[],
    defaultSource: string,
  ): Promise<void>;
  sendCredentialsUpdate(
    agentAddress: string,
    delivery: CredentialDelivery,
  ): Promise<void>;
  sendPack(
    agentAddress: string,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
    options?: SendPackOptions,
  ): Promise<void>;
  /**
   * Bind a per-step workflow-substrate address to a sidecar for the staging
   * window of a multi-step deploy, so `sendPack` can route the step's deploy
   * and asset packs before the deployment-level frame spawns the child. The
   * address enters the keyless `workflowAddresses` routing set; call
   * `unbindStepRoute` once the step's packs land. Throws if no sidecar is
   * available.
   */
  bindStepRoute(stepAddress: string): void;
  /**
   * Remove a per-step route bound by `bindStepRoute`. Idempotent: an unbound
   * address is a no-op.
   */
  unbindStepRoute(stepAddress: string): void;
  /**
   * Provision one step of a multi-step deploy on the sidecar WITHOUT
   * spawning: the sidecar initializes the step's agent-state repo and
   * records the hub key so the follow-up deploy pack applies and verifies.
   * The step address must already be bound via `bindStepRoute`. Resolves
   * once the sidecar acks, so the caller can then deliver the deploy pack.
   */
  sendProvisionStep(agentAddress: string, config: HarnessConfig): Promise<void>;
  sendSyncRequest(agentAddress: string): void;
  /**
   * Deliver a workflow-run signal to the sidecar that hosts the named
   * deployment-level mail address. The sidecar's hub-link routes the
   * frame through its `signalInboundRouter` into the deployment's
   * supervisor, which sends a `signal.deliver` control IPC frame to
   * the workflow-process child. The child commits the resulting
   * `SignalReceived` event through its own substrate -- the single
   * writer of the workflow-run repo on the sidecar side -- so the
   * pack-push pipeline that propagates the commit to the hub never
   * sees a concurrent writer at the same ref.
   *
   * Throws when no sidecar is registered for `agentAddress`; the
   * caller is responsible for ensuring the deployment is live.
   */
  sendSignalDeliver(opts: {
    agentAddress: string;
    runId: string;
    signalName: string;
    signalId: string;
    payload: unknown;
  }): void;
  /**
   * Deliver a workflow-host drain control payload to the sidecar that
   * hosts the named deployment-level mail address. The sidecar's
   * hub-link routes the frame through its `drainInboundRouter` into
   * the deployment's supervisor, which sends a `drain` control IPC
   * frame to the workflow-process child and arms one `drainTimeout`
   * accumulator per in-flight run. Cancel-mode steps abort on the
   * child side; wait-mode steps continue. Accumulators commit a
   * signed `CancelRequested{origin: "supervisor-drain"}` against the
   * workflow-run repo when the deadline expires.
   *
   * Throws when no sidecar is registered for `agentAddress`; the
   * caller is responsible for ensuring the deployment is live.
   */
  sendDrain(opts: { agentAddress: string; deadlineMs: number }): void;

  subscribeAgent(
    agentAddress: string,
    callback: (event: unknown) => void,
  ): () => void;
  dispatchAgentEvent(agentAddress: string, event: unknown): void;

  getConnectedSidecars(): string[];
  getRoutableAddresses(): string[];

  /** Typed event emitter for the receiver-dispatch surface. See
   * `sidecar-events.ts` for the event map and emission semantics. */
  events: SidecarEventEmitter;
};

/**
 * A verified sidecar-connection identity resolved by an authenticator from
 * the credentials a sidecar presents on the WebSocket handshake. The
 * `sidecarId` is the connection's own trusted id; it is not the untrusted
 * `sidecarId` claimed on the register/reconnect frame, and it carries no
 * tenant scope. Modeled as a discriminated union so a future non-sidecar
 * principal (e.g. an operator user) can be added as an additional arm
 * without changing existing consumers.
 */
export type SidecarAuthIdentity = SidecarCredentialIdentity;

export type AllocatedSidecarTarget = {
  readonly allocationId: string;
  readonly generation: number;
};

export type SidecarAllocationRouter = {
  /** Advance the in-memory trust boundary before provisioning a generation. */
  fenceAllocation(allocationId: string, generation: number): void;
  /** Resolve once the exact authenticated allocation generation is connected. */
  waitForAllocatedSidecar(
    target: AllocatedSidecarTarget,
    timeoutMs: number,
  ): Promise<void>;
  /** Check exact allocated readiness without parking a reconciliation worker. */
  isAllocatedSidecarReady(target: AllocatedSidecarTarget): Promise<boolean>;
  /** Check whether the exact generation already hosts its workflow supervisor. */
  isAllocatedWorkflowActive(target: AllocatedSidecarTarget): Promise<boolean>;
  sendAgentDeployToAllocation(
    target: AllocatedSidecarTarget,
    agentAddress: string,
    config: HarnessConfig,
    workflow?: AgentDeployFrame["workflow"],
  ): Promise<{ publicKey: string }>;
  sendPackToAllocation(
    target: AllocatedSidecarTarget,
    agentAddress: string,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
    options?: SendPackOptions,
  ): Promise<void>;
  /**
   * Restore one Hub-authoritative workflow-run ref onto the exact allocation
   * generation before its deployment address is routed or supervisor spawned.
   */
  sendWorkflowRunPackToAllocation(
    target: AllocatedSidecarTarget,
    agentAddress: string,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
  ): Promise<void>;
  bindAllocatedStepRoute(
    target: AllocatedSidecarTarget,
    stepAddress: string,
  ): Promise<void>;
  unbindAllocatedStepRoute(
    target: AllocatedSidecarTarget,
    stepAddress: string,
  ): void;
  sendProvisionStepToAllocation(
    target: AllocatedSidecarTarget,
    agentAddress: string,
    config: HarnessConfig,
  ): Promise<void>;
  /**
   * Deliver one durable workflow trigger to the exact allocation generation.
   * Grants and mail are written to the same websocket in FIFO order. The
   * returned promise proves only that both frames were sent; the sidecar's
   * durable-inbox acknowledgement is surfaced separately through
   * `mail.inbound.acknowledged`.
   */
  sendWorkflowRunDispatchToAllocation(
    target: AllocatedSidecarTarget,
    agentAddress: string,
    runId: string,
    stepGrants: RunGrantsFrame["stepGrants"],
    rawMessage: string,
    messageId: string,
  ): Promise<void>;
  /** Deliver an idempotent signal to the exact exclusive generation. */
  sendSignalDeliverToAllocation(
    target: AllocatedSidecarTarget,
    opts: {
      agentAddress: string;
      runId: string;
      signalName: string;
      signalId: string;
      payload: unknown;
    },
  ): Promise<void>;
};

/**
 * Resolves the credentials a sidecar presents on the handshake to a
 * verified identity, or `null` when the credentials are not recognized.
 * The claimed `sidecarId` is an unauthenticated hint; the authenticator
 * derives the trusted identity from the `token` and the returned
 * `sidecarId` is what the router keys connection state off of.
 */
export type SidecarAuthenticator = (claim: {
  sidecarId: string;
  token: string;
}) => Promise<SidecarAuthIdentity | null>;

export type SidecarRouterConfig = {
  requestTimeoutMs?: number;
  /** Hex-encoded 32-byte Ed25519 public key for signing deploy commits.
   * Included in agent.deploy frames so sidecars can verify pack signatures. */
  hubPublicKey?: string;
  /** Resolves each register/reconnect handshake to a verified sidecar
   * identity. Required: without it a connection could route on an
   * unverified frame claim. Return null to reject the handshake. */
  authenticateSidecar: SidecarAuthenticator;
  /** Revalidate durable identity at registration and routing boundaries. */
  validateSidecarIdentity?: (
    identity: SidecarAuthIdentity,
    use: "registration" | "readiness" | "routing",
  ) => Promise<boolean>;
  challengeTimeoutMs?: number;
  /** Timeout for a `sendProbe` round-trip. A probe materializes a workflow's
   * dependency closure and evaluates it on the sidecar, so it can run longer
   * than a routine `sendRequest`; it gets its own timeout rather than sharing
   * the request timeout. */
  probeTimeoutMs?: number;
  disconnectQueueMaxSize?: number;
  disconnectQueueTTLMs?: number;
  pingTimeoutMs?: number;
  /** Interval between redelivery attempts of a connected-window `mail.inbound`
   * the sidecar has not yet acknowledged with `mail.inbound.ack`. */
  mailAckRetryIntervalMs?: number;
  /** Maximum redelivery attempts before the hub stops retrying an un-acked
   * connected-window `mail.inbound`. Bounds the retry so a sidecar that never
   * acks does not accumulate an unbounded timer per delivery. */
  mailAckMaxRetries?: number;
  /** Query handlers the wire layer issues during frame processing.
   * Each lookup is one-handler-returns-a-value; for multi-subscriber
   * notifications use `router.events.on(...)` instead.
   *
   * `lookupDeployRef` and the `deploy.ref.stale` event are paired by
   * convention: the wire layer only issues the staleness comparison
   * when the lookup is set, and only emits the event on a confirmed
   * mismatch. The host is responsible for subscribing a listener
   * whenever the lookup is provided; the router does not enforce
   * the pairing. */
  lookups?: SidecarLookups;
};

// Minimal handle so the router doesn't depend on a specific WebSocket impl.
export type WsHandle = {
  send(data: string): void;
  close(): void;
};

const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
const DEFAULT_CHALLENGE_TIMEOUT_MS = 30_000;
// A probe fetches a workflow's dependency closure from a registry and
// evaluates it on the sidecar, so it runs longer than a routine request; its
// default timeout is correspondingly wider than DEFAULT_REQUEST_TIMEOUT_MS.
export const DEFAULT_PROBE_TIMEOUT_MS = 60_000;
const DEFAULT_DISCONNECT_QUEUE_MAX_SIZE = 100;
const DEFAULT_DISCONNECT_QUEUE_TTL_MS = 5 * 60 * 1000;
const DEFAULT_PING_TIMEOUT_MS = 60_000;
const DEFAULT_MAIL_ACK_RETRY_INTERVAL_MS = 10_000;
const DEFAULT_MAIL_ACK_MAX_RETRIES = 5;

export function createSidecarRouter(
  config: SidecarRouterConfig,
): SidecarRouter & SidecarAllocationRouter {
  const {
    requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS,
    challengeTimeoutMs = DEFAULT_CHALLENGE_TIMEOUT_MS,
    probeTimeoutMs = DEFAULT_PROBE_TIMEOUT_MS,
    hubPublicKey: hubPublicKeyHex,
    authenticateSidecar,
    validateSidecarIdentity = async () => true,
    disconnectQueueMaxSize = DEFAULT_DISCONNECT_QUEUE_MAX_SIZE,
    disconnectQueueTTLMs = DEFAULT_DISCONNECT_QUEUE_TTL_MS,
    pingTimeoutMs = DEFAULT_PING_TIMEOUT_MS,
    mailAckRetryIntervalMs = DEFAULT_MAIL_ACK_RETRY_INTERVAL_MS,
    mailAckMaxRetries = DEFAULT_MAIL_ACK_MAX_RETRIES,
    lookups = {},
  } = config;

  // Receiver-dispatch surface. Wire-layer callsites emit events here;
  // host code subscribes via `router.events`.
  const events = createSidecarEmitter();

  // ws handle → registered connection
  const connections = new Map<WsHandle, SidecarConnection>();
  const allocatedConnections = new Map<
    string,
    {
      ws: WsHandle;
      identity: Extract<SidecarAuthIdentity, { kind: "allocated" }>;
    }
  >();
  const allocationFences = new Map<string, number>();
  type AllocationWaiter = {
    generation: number;
    resolve(): void;
    reject(error: Error): void;
    timer: ReturnType<typeof setTimeout>;
  };
  const allocationWaiters = new Map<string, Set<AllocationWaiter>>();
  // agentAddress → ws handle (routing table)
  const addressIndex = new Map<string, WsHandle>();
  // requestId → pending promise
  const pending = new Map<string, PendingRequest>();
  // agentAddress → pending deploy promise (matched by agent.deploy.ack/agent.error)
  type PendingDeploy = {
    agentAddress: string;
    ws: WsHandle;
    resolve(publicKey: string): void;
    reject(error: string): void;
    timer: ReturnType<typeof setTimeout>;
  };
  const pendingDeploys = new Map<string, PendingDeploy>();
  // ws handle → pending challenge (awaiting challenge.response)
  type PendingChallenge = {
    sidecarId: string;
    challenges: Map<string, { nonce: Uint8Array; publicKey: Uint8Array }>;
    deployRefs: Record<string, string>;
    timer: ReturnType<typeof setTimeout>;
  };
  const pendingChallenges = new Map<WsHandle, PendingChallenge>();
  // agentAddress → queued frames for disconnected agents awaiting reconnect
  type DisconnectedAgent = {
    queue: HubFrame[];
    timer: ReturnType<typeof setTimeout>;
  };
  const disconnectedAgents = new Map<string, DisconnectedAgent>();
  // agentAddress → messageId → connected-window mail awaiting a
  // `mail.inbound.ack`. A `mail.inbound` delivered over a LIVE connection with
  // a hub-minted messageId is tracked here and redelivered -- identical bytes,
  // same messageId -- on a timer until the sidecar acknowledges its durable
  // inbox write, so a frame silently dropped in the connected window (a socket
  // that half-died before the sidecar wrote the message) is recovered rather
  // than lost. The sidecar inbox is idempotent on messageId, so a redelivery
  // of a message the sidecar already wrote is deduped there: at-least-once
  // redelivery is effectively-once.
  type PendingMailEntry = {
    agentAddress: string;
    messageId: string;
    frame: HubFrame;
    attempts: number;
    timer: ReturnType<typeof setTimeout>;
    // When this mail triggers a workflow run, the run's already-materialized
    // grants ride alongside it. Redelivery replays this snapshot as a
    // `run.grants` frame AHEAD of the mail so the redelivered trigger lands on
    // a sidecar that has the run's grants, rather than failing its onRunStart
    // barrier closed. Re-materializing at redelivery time is unsafe (it carries
    // commit/authority semantics); replaying the same bytes is not.
    runGrants?: { runId: string; stepGrants: RunGrantsFrame["stepGrants"] };
    /** Present for a durable trigger pinned to an exclusive allocation. */
    allocatedTarget?: AllocatedSidecarTarget;
  };
  const pendingMail = new Map<string, Map<string, PendingMailEntry>>();
  // agentAddress → retention TTL timer for un-acked pending mail held across a
  // disconnect. On close the per-entry retry timers are cleared (the socket is
  // gone) but the entries are RETAINED so a verified reconnect can redeliver
  // them; this timer bounds that retention so a sidecar that never reconnects
  // does not leak entries. Cleared when the address reconnects (redelivery) or
  // its last pending entry is acked.
  const pendingMailRetention = new Map<string, ReturnType<typeof setTimeout>>();
  // agentAddress → set of subscriber callbacks for agent events
  const agentSubscribers = new Map<string, Set<(event: unknown) => void>>();
  // agentAddress → cached connector-thread state, populated by
  // connector.state.changed frames. Hub-side mail composition reads this
  // to set threading headers on user-originated mail. Absent entries mean
  // "no state reported yet" (e.g. mid-reconnect); callers must treat that
  // identically to a null entry (no active thread).
  const connectorStates = new Map<string, ConnectorThreadState | null>();
  // ws handle → liveness timer (reset on each ping from the sidecar)
  const livenessTimers = new Map<WsHandle, ReturnType<typeof setTimeout>>();

  // Per-ws serialization chain for QUEUE-class frames (see `frameBypassesQueue`
  // for the split and the invariant behind it). A frame that establishes or
  // reads routing waits for earlier queued frames on the same ws to complete,
  // so it observes their finished effects -- most importantly an async
  // register's routing write, which would otherwise land after a following
  // connector.state.changed / mail / pack frame and silently drop it. It holds
  // a single in-flight promise per ws (replaced each queued frame), cleared on
  // close.
  const messageChains = new Map<WsHandle, Promise<void>>();

  // transferId → pending pack transfer (resolved by repo.pack.ack, rejected by repo.pack.reject)
  type PendingPack = {
    transferId: string;
    ws: WsHandle;
    agentAddress: string;
    repoId: RepoId;
    resolve(): void;
    reject(error: string): void;
    timer: ReturnType<typeof setTimeout>;
  };
  const pendingPacks = new Map<string, PendingPack>();
  let packCounter = 0;

  // agentAddress → pending undeploy (resolved by agent.undeploy.ack)
  type PendingUndeploy = {
    agentAddress: string;
    ws: WsHandle;
    resolve(): void;
    reject(error: string): void;
    timer: ReturnType<typeof setTimeout>;
  };
  const pendingUndeploys = new Map<string, PendingUndeploy>();

  // requestId → pending workflow probe (resolved by workflow.probe.result,
  // rejected by workflow.probe.error). Result-carrying, unlike `pending`
  // (which resolves void): a probe returns the sidecar's inert projection +
  // grant set + wire hash. Keyed on requestId alone -- the probe runs in the
  // sidecar's pre-deploy state and enters no address map, so `handleClose`'s
  // ws-keyed sweep is its ONLY disconnect cleanup.
  type PendingProbe = {
    requestId: string;
    ws: WsHandle;
    resolve(result: WorkflowProbeResult): void;
    reject(error: string): void;
    timer: ReturnType<typeof setTimeout>;
  };
  const pendingProbes = new Map<string, PendingProbe>();

  // Receives agent-state packs pushed from sidecars. The wire frames
  // (`repo.pack.push` / `repo.pack.done`) are shared with the
  // workflow-run flow; dispatch on `repoId.kind` picks which receiver
  // observes the chunks. The two receivers maintain independent
  // in-flight pack state and independent cancel-by-agent semantics so a
  // pending workflow-run transfer cannot disturb a concurrent agent-
  // state transfer for the same agent and vice versa.
  const agentStatePackReceiver = createPackReceiver();
  const workflowRunPackReceiver = createPackReceiver();

  let requestCounter = 0;

  // Surface disconnect-queue mail that is being dropped rather than delivered.
  // Every dropped frame reaches the same channel routing failures already use
  // (`mail.outbound.undelivered`, logged by the orchestrator), plus a warn that
  // names the recipient and the drop count so a size-cap eviction or a TTL
  // expiry of a still-full queue is visible instead of silent. A queued frame
  // is always a `mail.inbound` carrying the sender's rawMessage; a frame of any
  // other shape has no rawMessage to relay and is surfaced by the warn alone.
  function surfaceDroppedFrames(
    agentAddress: string,
    frames: HubFrame[],
    reason: string,
  ): void {
    if (frames.length === 0) return;
    logger.warn`Dropping ${String(frames.length)} queued message(s) for ${agentAddress}: ${reason}`;
    for (const frame of frames) {
      if (frame.type !== "mail.inbound") continue;
      events.emit("mail.outbound.undelivered", {
        rawMessage: frame.rawMessage,
        recipients: [agentAddress],
      });
    }
  }

  function enqueueForDisconnected(
    agentAddress: string,
    frame: HubFrame,
  ): boolean {
    const entry = disconnectedAgents.get(agentAddress);
    if (entry === undefined) return false;

    if (entry.queue.length >= disconnectQueueMaxSize) {
      const evicted = entry.queue.shift();
      if (evicted !== undefined) {
        surfaceDroppedFrames(agentAddress, [evicted], "disconnect queue full");
      }
    }
    entry.queue.push(frame);
    return true;
  }

  function flushDisconnectedQueue(
    agentAddress: string,
    conn: SidecarConnection,
  ): void {
    const entry = disconnectedAgents.get(agentAddress);
    if (entry === undefined) return;

    clearTimeout(entry.timer);
    disconnectedAgents.delete(agentAddress);

    for (const frame of entry.queue) {
      conn.send(frame);
    }
    if (entry.queue.length > 0) {
      logger.info`Flushed ${String(entry.queue.length)} queued message(s) to ${agentAddress}`;
    }
  }

  // Track a connected-window `mail.inbound` for redelivery until the sidecar
  // acks its durable inbox write. Replaces any prior entry for the same
  // (agentAddress, messageId) -- clearing its timer first so no timer leaks --
  // which keeps a re-sent delivery from arming a second concurrent retry loop.
  function trackPendingMail(
    agentAddress: string,
    messageId: string,
    frame: HubFrame,
    runGrants?: { runId: string; stepGrants: RunGrantsFrame["stepGrants"] },
    allocatedTarget?: AllocatedSidecarTarget,
  ): void {
    let byId = pendingMail.get(agentAddress);
    if (byId === undefined) {
      byId = new Map();
      pendingMail.set(agentAddress, byId);
    }
    const existing = byId.get(messageId);
    if (existing !== undefined) clearTimeout(existing.timer);
    byId.set(messageId, {
      agentAddress,
      messageId,
      frame,
      attempts: 0,
      timer: setTimeout(
        () => retryPendingMail(agentAddress, messageId),
        mailAckRetryIntervalMs,
      ),
      ...(runGrants !== undefined ? { runGrants } : {}),
      ...(allocatedTarget !== undefined ? { allocatedTarget } : {}),
    });
  }

  // Replay a mail-triggered run's grants ahead of a redelivery of its trigger
  // mail. Same-connection FIFO lands the `run.grants` frame before the mail, so
  // the redelivered run resolves its onRunStart barrier instead of failing
  // closed on missing grants.
  function replayRunGrantsAhead(
    conn: SidecarConnection,
    entry: PendingMailEntry,
  ): void {
    if (entry.runGrants === undefined) return;
    conn.send({
      type: "run.grants",
      agentAddress: entry.agentAddress,
      runId: entry.runGrants.runId,
      stepGrants: entry.runGrants.stepGrants,
    });
  }

  function deletePendingMail(
    byId: Map<string, PendingMailEntry>,
    agentAddress: string,
    messageId: string,
  ): void {
    byId.delete(messageId);
    if (byId.size === 0) {
      pendingMail.delete(agentAddress);
      // The retention TTL guards a non-empty pending set; drop it once the set
      // is empty so it never outlives the entries it was bounding.
      const retention = pendingMailRetention.get(agentAddress);
      if (retention !== undefined) {
        clearTimeout(retention);
        pendingMailRetention.delete(agentAddress);
      }
    }
  }

  function retryPendingMail(agentAddress: string, messageId: string): void {
    const byId = pendingMail.get(agentAddress);
    if (byId === undefined) return;
    const entry = byId.get(messageId);
    if (entry === undefined) return;

    if (entry.attempts >= mailAckMaxRetries) {
      // The sidecar never acked within the retry budget. The ack is withheld
      // precisely because the sidecar's durable inbox write failed, so the
      // mail was NOT delivered: surface it as undelivered so the host can relay
      // it onto an external transport, then drop the pending entry so its timer
      // does not leak.
      if (entry.frame.type === "mail.inbound") {
        events.emit("mail.outbound.undelivered", {
          rawMessage: entry.frame.rawMessage,
          recipients: [agentAddress],
        });
      }
      deletePendingMail(byId, agentAddress, messageId);
      logger.warn`Gave up redelivering mail ${messageId} to ${agentAddress} after ${String(entry.attempts)} un-acked attempt(s)`;
      return;
    }

    // Redeliver over the address's CURRENT owner: a verified reconnect may have
    // moved the address to a new connection since the original delivery.
    const ws = addressIndex.get(agentAddress);
    const conn = ws !== undefined ? connections.get(ws) : undefined;
    const allocated =
      entry.allocatedTarget === undefined
        ? undefined
        : allocatedConnections.get(entry.allocatedTarget.allocationId);
    const targetStillOwnsAddress =
      entry.allocatedTarget === undefined ||
      (allocated !== undefined &&
        allocated.identity.generation === entry.allocatedTarget.generation &&
        allocated.ws === ws);
    if (conn === undefined || !targetStillOwnsAddress) {
      // No live connection to recover into. Connected-window redelivery only
      // applies while the address is routable; a disconnected address is not
      // retried here.
      deletePendingMail(byId, agentAddress, messageId);
      logger.warn`Dropping un-acked mail ${messageId} for ${agentAddress}: no live connection to redeliver over`;
      return;
    }

    entry.attempts += 1;
    replayRunGrantsAhead(conn, entry);
    conn.send(entry.frame);
    entry.timer = setTimeout(
      () => retryPendingMail(agentAddress, messageId),
      mailAckRetryIntervalMs,
    );
  }

  function resolvePendingMail(agentAddress: string, messageId: string): void {
    const byId = pendingMail.get(agentAddress);
    if (byId === undefined) return;
    const entry = byId.get(messageId);
    if (entry === undefined) return;
    clearTimeout(entry.timer);
    deletePendingMail(byId, agentAddress, messageId);
  }

  // Hold an address's un-acked pending mail across a disconnect. The per-entry
  // retry timers are cleared -- retrying over the dead socket is pointless --
  // but the entries are KEPT so a verified reconnect can redeliver them. A
  // retention TTL (the disconnect-queue horizon) bounds the hold so a sidecar
  // that never reconnects does not leak; on expiry the still-un-acked entries
  // are surfaced as `mail.outbound.undelivered` so the host can relay them,
  // since a withheld ack means the sidecar's durable write never landed.
  function retainPendingMailForAddress(agentAddress: string): void {
    const byId = pendingMail.get(agentAddress);
    if (byId === undefined) return;
    for (const entry of byId.values()) clearTimeout(entry.timer);
    const existing = pendingMailRetention.get(agentAddress);
    if (existing !== undefined) clearTimeout(existing);
    const timer = setTimeout(() => {
      pendingMailRetention.delete(agentAddress);
      const expired = pendingMail.get(agentAddress);
      pendingMail.delete(agentAddress);
      if (expired !== undefined && expired.size > 0) {
        for (const entry of expired.values()) {
          if (entry.frame.type !== "mail.inbound") continue;
          events.emit("mail.outbound.undelivered", {
            rawMessage: entry.frame.rawMessage,
            recipients: [agentAddress],
          });
        }
        logger.warn`Dropping ${String(expired.size)} un-acked message(s) for ${agentAddress}: pending-mail retention TTL expired`;
      }
    }, disconnectQueueTTLMs);
    pendingMailRetention.set(agentAddress, timer);
  }

  // Redeliver an address's retained un-acked pending mail on a verified
  // reconnect. Replays identical bytes (same messageId) over the new
  // connection, so the sidecar's inbox dedups a message it already wrote
  // (effectively-once) and processes one it had dropped (no loss). Re-arms the
  // connected-window retry over the new connection with a fresh per-generation
  // budget, so a redelivery that is itself dropped before its ack is retried.
  function redeliverPendingMail(
    agentAddress: string,
    conn: SidecarConnection,
  ): void {
    const retention = pendingMailRetention.get(agentAddress);
    if (retention !== undefined) {
      clearTimeout(retention);
      pendingMailRetention.delete(agentAddress);
    }
    const byId = pendingMail.get(agentAddress);
    if (byId === undefined) return;
    for (const entry of [...byId.values()]) {
      if (
        entry.allocatedTarget !== undefined &&
        (conn.identity.kind !== "allocated" ||
          conn.identity.allocationId !== entry.allocatedTarget.allocationId ||
          conn.identity.generation !== entry.allocatedTarget.generation)
      ) {
        // The Hub-owned dispatch row survives generation replacement and will
        // be requeued by the allocation-ready callback. Do not leak or replay
        // this generation-local retry entry onto a different worker.
        clearTimeout(entry.timer);
        deletePendingMail(byId, agentAddress, entry.messageId);
        continue;
      }
      replayRunGrantsAhead(conn, entry);
      conn.send(entry.frame);
      entry.attempts = 0;
      entry.timer = setTimeout(
        () => retryPendingMail(agentAddress, entry.messageId),
        mailAckRetryIntervalMs,
      );
    }
    if (byId.size > 0) {
      logger.info`Redelivered ${String(byId.size)} un-acked message(s) to ${agentAddress} on reconnect`;
    }
  }

  function resetLivenessTimer(ws: WsHandle): void {
    const existing = livenessTimers.get(ws);
    if (existing !== undefined) clearTimeout(existing);

    const timer = setTimeout(() => {
      livenessTimers.delete(ws);
      logger.warn`Sidecar ping timeout, closing connection`;
      ws.close();
    }, pingTimeoutMs);
    livenessTimers.set(ws, timer);
  }

  function handlePing(ws: WsHandle): void {
    resetLivenessTimer(ws);
    // Always respond with pong, even before register/reconnect completes.
    // The sidecar's ping timer starts on open, which may fire before the
    // async registration handshake finishes.
    ws.send(JSON.stringify({ type: "pong" }));
  }

  function handleOpen(ws: WsHandle): void {
    // Connection is not usable until a register frame arrives.
    // Start the liveness timer immediately — a sidecar that connects
    // but never sends a ping will be reaped.
    resetLivenessTimer(ws);
  }

  function handleMessage(ws: WsHandle, data: string): void {
    let raw: unknown;
    try {
      raw = JSON.parse(data) as unknown;
    } catch {
      logger.warn`Unparseable frame from sidecar connection`;
      return;
    }
    const validated = SidecarFrame(raw);
    if (validated instanceof type.errors) {
      logger.warn`Invalid sidecar frame: ${validated.summary}`;
      return;
    }
    const frame = validated;

    // Bypass frames (liveness + terminal responses to outbound requests)
    // dispatch immediately: they resolve the very promises a queued handler
    // may be blocked on, so queuing them would deadlock the round-trip.
    if (frameBypassesQueue(frame)) {
      // Guard so a bypass handler's failure -- a synchronous throw or an async
      // ack handler's rejection -- is logged rather than floating out of the
      // immediate dispatch. The async wrapper turns a synchronous throw into a
      // rejection too, matching the queue path's .then/.catch coverage.
      void (async () => dispatchFrame(ws, frame))().catch((err: unknown) => {
        logger.warn`Frame handler failed for ${frame.type}: ${err instanceof Error ? err.message : String(err)}`;
      });
      return;
    }
    // Everything else serializes per ws so a frame that establishes or reads
    // routing observes earlier queued frames' completed effects.
    const prev = messageChains.get(ws) ?? Promise.resolve();
    const next = prev
      .then(() => dispatchFrame(ws, frame))
      .catch((err: unknown) => {
        logger.warn`Frame handler failed for ${frame.type}: ${err instanceof Error ? err.message : String(err)}`;
      });
    messageChains.set(ws, next);
  }

  function assertNever(x: never): never {
    throw new Error(`Unclassified sidecar frame type: ${JSON.stringify(x)}`);
  }

  // Whether `frame` bypasses the per-ws serialization chain. Invariant: a frame
  // bypasses IFF it is liveness (ping) OR a terminal response to an
  // already-issued outbound request -- correlated purely by
  // requestId/transferId/agentAddress in the pending maps, touching no routing
  // state. Such a frame has no ordering obligation against new inbound frames
  // (a response cannot resolve "too early" for a request that already went
  // out), and it is exactly what in-flight queued handlers block on, so it MUST
  // run out of band or the challenge round-trip deadlocks (challenge.response
  // -> agent.reconnected reaction -> sendSourcesUpdate awaits a later
  // session.ack). Every other frame establishes or reads routing, or carries an
  // inbound payload whose order matters, so it queues. The exhaustive switch +
  // assertNever makes adding a SidecarFrame variant without classifying it a
  // compile error, not a latent deadlock or a silent bypass hole.
  function frameBypassesQueue(frame: SidecarFrame): boolean {
    switch (frame.type) {
      case "ping":
      case "session.ack":
      case "session.error":
      case "agent.deploy.ack":
      case "agent.error":
      case "agent.undeploy.ack":
      case "repo.pack.ack":
      case "repo.pack.reject":
      case "workflow.probe.result":
      case "workflow.probe.error":
        return true;
      case "register":
      case "reconnect":
      case "challenge.response":
      case "mail.outbound":
      case "agent.event":
      case "connector.state.changed":
      case "mail.inbound.ack":
      case "signal.correlation.register":
      case "repo.pack.push":
      case "repo.pack.done":
        return false;
      default:
        return assertNever(frame);
    }
  }

  // Runs one frame's handler. Returns the handler's promise for async handlers
  // so the per-ws chain can await bounded completion; sync handlers return
  // void. Never awaits a promise that resolves on a LATER same-ws frame -- the
  // only such await (the challenge round-trip's session.ack) is reached via a
  // bypass frame, which does not queue.
  function dispatchFrame(
    ws: WsHandle,
    frame: SidecarFrame,
  ): void | Promise<void> {
    switch (frame.type) {
      case "register": {
        const agentAddresses = frame.agentAddresses;
        return authenticateHandshake(ws, frame, (identity) =>
          handleRegister(ws, identity, agentAddresses),
        );
      }
      case "reconnect": {
        const agentAddresses = frame.agentAddresses;
        const deployRefs = frame.deployRefs ?? {};
        return authenticateHandshake(ws, frame, (identity) =>
          handleReconnect(ws, identity, agentAddresses, deployRefs),
        );
      }
      case "challenge.response":
        return handleChallengeResponse(ws, frame.responses);
      case "agent.deploy.ack":
        return handleDeployAck(ws, frame);
      case "agent.error":
        rejectDeployPendingFromFrame(ws, frame.agentAddress, frame.error);
        rejectUndeployPending(ws, frame.agentAddress, frame.error);
        return;
      case "agent.undeploy.ack":
        resolveUndeployPending(ws, frame.agentAddress);
        return;
      case "ping":
        handlePing(ws);
        return;
      case "mail.outbound":
        if (frame.delivered !== true) {
          return handleMailOutbound(frame.rawMessage, frame.recipients);
        }
        if (lookups.persistMail && frame.senderAddress) {
          return handleMailPersist(
            lookups.persistMail,
            frame.rawMessage,
            frame.senderAddress,
            frame.recipients,
          );
        }
        if (!frame.senderAddress) {
          logger.warn`Dropping delivered mail.outbound frame with no senderAddress`;
        } else {
          logger.warn`Dropping delivered mail.outbound frame: no persistMail lookup configured`;
        }
        return;
      case "agent.event":
        events.emit("agent.event", {
          agentAddress: frame.agentAddress,
          sessionId: frame.sessionId,
          event: frame.event,
        });
        dispatchToSubscribers(frame.agentAddress, frame.event);
        return;
      case "connector.state.changed":
        // Gate the cache write on the sending sidecar actually owning
        // the named agent. A misbehaving sidecar that knows another
        // agent's address could otherwise poison the cached state.
        if (addressIndex.get(frame.agentAddress) !== ws) {
          logger.warn`Dropping connector.state.changed for ${frame.agentAddress}: not registered to this sidecar`;
          return;
        }
        connectorStates.set(frame.agentAddress, frame.connectorState);
        events.emit("connector.state.changed", {
          agentAddress: frame.agentAddress,
          connectorState: frame.connectorState,
        });
        return;
      case "mail.inbound.ack": {
        // Terminal receipt for a connected-window `mail.inbound`: the sidecar
        // has durably written the message to its inbox. Gate on ownership --
        // like connector.state.changed and signal.correlation.register -- so a
        // sidecar cannot clear another sidecar's pending mail. The messageId is
        // a hub-minted id only the owning sidecar ever received on the frame,
        // so the ownership check is defense-in-depth, not the sole guard.
        const conn = connections.get(ws);
        if (conn === undefined) return;
        if (!connOwnsAddress(conn, frame.agentAddress)) {
          logger.warn`Dropping mail.inbound.ack for ${frame.agentAddress}: not registered to this sidecar`;
          return;
        }
        resolvePendingMail(frame.agentAddress, frame.messageId);
        events.emit("mail.inbound.acknowledged", {
          agentAddress: frame.agentAddress,
          messageId: frame.messageId,
          ...(conn.identity.kind === "allocated"
            ? {
                allocated: {
                  allocationId: conn.identity.allocationId,
                  anchorRunId: conn.identity.anchorRunId,
                  generation: conn.identity.generation,
                },
              }
            : {}),
        });
        return;
      }
      case "signal.correlation.register":
        return handleSignalCorrelationRegister(ws, frame);
      case "session.ack":
        resolvePending(frame.requestId);
        return;
      case "session.error":
        rejectPending(frame.requestId, frame.error);
        return;
      case "repo.pack.ack":
        resolvePackPending(ws, frame);
        return;
      case "repo.pack.reject":
        rejectPackPending(ws, frame);
        return;
      case "repo.pack.push":
        handlePackPush(ws, frame);
        return;
      case "repo.pack.done":
        return handlePackDone(ws, frame);
      case "workflow.probe.result":
        resolveProbe(frame.requestId, {
          projection: frame.projection,
          grants: frame.grants,
          wireHash: frame.wireHash,
        });
        return;
      case "workflow.probe.error":
        rejectProbe(frame.requestId, frame.error);
        return;
      default:
        return assertNever(frame);
    }
  }

  // Authenticate a register/reconnect handshake exactly once, then run the
  // frame's handler with the verified identity. The claimed `sidecarId` on
  // the frame is an unauthenticated hint: it is logged if it disagrees with
  // the verified id but never trusted -- routing keys off the verified id.
  // Fails closed by closing the connection when the authenticator rejects
  // (returns null) or throws (e.g. a database failure), so a handshake never
  // proceeds on unverified credentials.
  async function authenticateHandshake(
    ws: WsHandle,
    frame: { type: string; sidecarId: string; token: string },
    run: (identity: SidecarAuthIdentity) => Promise<void>,
  ): Promise<void> {
    let identity: SidecarAuthIdentity | null;
    try {
      identity = await authenticateSidecar({
        sidecarId: frame.sidecarId,
        token: frame.token,
      });
    } catch (err) {
      logger.error`Rejected ${frame.type} from claimed sidecar ${frame.sidecarId}: authenticator failed: ${err instanceof Error ? err.message : String(err)}`;
      ws.close();
      return;
    }
    if (identity === null) {
      logger.warn`Rejected ${frame.type} from claimed sidecar ${frame.sidecarId}: invalid token`;
      ws.close();
      return;
    }
    if (identity.sidecarId !== frame.sidecarId) {
      logger.warn`Sidecar ${frame.type} claimed id ${frame.sidecarId} but token verifies as ${identity.sidecarId}; keying off the verified id`;
    }
    if (!(await validateSidecarIdentity(identity, "registration"))) {
      logger.warn`Rejected ${frame.type} from sidecar ${identity.sidecarId}: credential identity is no longer current`;
      ws.close();
      return;
    }
    await run(identity);
  }

  async function notifyAllocationWaiters(allocationId: string): Promise<void> {
    const waiters = allocationWaiters.get(allocationId);
    const current = allocatedConnections.get(allocationId);
    if (waiters === undefined || current === undefined) return;
    if (!(await validateSidecarIdentity(current.identity, "readiness"))) return;

    for (const waiter of [...waiters]) {
      if (waiter.generation !== current.identity.generation) continue;
      clearTimeout(waiter.timer);
      waiters.delete(waiter);
      waiter.resolve();
    }
    if (waiters.size === 0) allocationWaiters.delete(allocationId);
  }

  async function handleAllocatedRegister(
    ws: WsHandle,
    identity: Extract<SidecarAuthIdentity, { kind: "allocated" }>,
    agentAddresses: string[],
  ): Promise<void> {
    if (allocationFences.get(identity.allocationId) !== identity.generation) {
      logger.warn`Rejected allocated sidecar ${identity.sidecarId}: allocation ${identity.allocationId} generation ${String(identity.generation)} is not fenced as current`;
      ws.close();
      return;
    }
    if (
      agentAddresses.length > 0 &&
      !(await validateSidecarIdentity(identity, "routing"))
    ) {
      logger.warn`Rejected allocated sidecar ${identity.sidecarId}: allocation ${identity.allocationId} is not ready to reclaim routes`;
      ws.close();
      return;
    }

    const existingOnSocket = connections.get(ws);
    const newlyRoutedAddresses = new Set<string>();
    for (const address of agentAddresses) {
      const alreadyOwned =
        existingOnSocket?.identity.kind === "allocated" &&
        existingOnSocket.identity.allocationId === identity.allocationId &&
        addressIndex.get(address) === ws &&
        connOwnsAddress(existingOnSocket, address);
      if (!alreadyOwned && address !== identity.workflowRunAddress) {
        logger.warn`Rejected allocated sidecar ${identity.sidecarId}: allocation ${identity.allocationId} claimed unrelated address ${address}`;
        ws.close();
        return;
      }
      if (!alreadyOwned) newlyRoutedAddresses.add(address);
    }

    if (allocationFences.get(identity.allocationId) !== identity.generation) {
      ws.close();
      return;
    }
    const current = allocatedConnections.get(identity.allocationId);
    if (current !== undefined && current.ws !== ws) {
      // A current-generation takeover is a reconnect, not a capacity loss.
      // Remove the old socket from the allocation index before closing it so
      // handleClose does not emit a false allocated-disconnect event.
      allocatedConnections.delete(identity.allocationId);
      handleClose(current.ws);
      current.ws.close();
    }

    const conn: SidecarConnection = existingOnSocket ?? {
      sidecarId: identity.sidecarId,
      identity,
      agentAddresses: new Set(),
      workflowAddresses: new Set(),
      send(frame: HubFrame) {
        ws.send(JSON.stringify(frame));
      },
    };
    if (
      conn.identity.kind !== "allocated" ||
      conn.identity.allocationId !== identity.allocationId ||
      conn.identity.generation !== identity.generation
    ) {
      logger.warn`Rejected allocated sidecar ${identity.sidecarId}: socket identity changed during registration`;
      ws.close();
      return;
    }

    connections.set(ws, conn);
    for (const address of agentAddresses) {
      conn.workflowAddresses.add(address);
      addressIndex.set(address, ws);
    }
    allocatedConnections.set(identity.allocationId, { ws, identity });
    for (const address of newlyRoutedAddresses) {
      redeliverPendingMail(address, conn);
    }
    logger.info`Allocated sidecar ${identity.sidecarId} registered for allocation ${identity.allocationId} generation ${String(identity.generation)}`;
    await notifyAllocationWaiters(identity.allocationId);
    events.emit("sidecar.allocated.connected", {
      allocationId: identity.allocationId,
      generation: identity.generation,
    });
  }

  async function handleRegister(
    ws: WsHandle,
    identity: SidecarAuthIdentity,
    agentAddresses: string[],
  ): Promise<void> {
    if (identity.kind === "allocated") {
      await handleAllocatedRegister(ws, identity, agentAddresses);
      return;
    }
    const sidecarId = identity.sidecarId;

    // Key-existence gate. A register frame is token-authenticated but carries
    // no per-address ownership proof, so it may route an address ONLY if that
    // address has no stored key yet -- a genuine keyless first-deploy (the
    // token-bounded first-deploy trust model). An address that already has a
    // key must prove ownership through the challenged reconnect path; routing
    // it here on token auth alone is the register-frame sibling of the
    // reconnect hijack. The keyless-only set is computed up front, BEFORE the
    // ghost-cleanup and every routing mutation below, so a rejected address
    // touches nothing: no eviction of a live owner, hence no downgrade from
    // hijack to denial-of-service on the victim.
    const lookupKey = lookups.lookupPublicKey;
    const routableAddresses: string[] = [];
    for (const addr of agentAddresses) {
      if (lookupKey === undefined) {
        // Fail closed: without the ownership lookup a keyed address cannot be
        // told apart from a first-deploy, so route nothing and surface the
        // misconfiguration. Empty first-connect registers never reach here.
        logger.error`Cannot gate register routing for ${addr}: lookupPublicKey is not configured; refusing to route (challenged reconnect required)`;
        continue;
      }
      let existingKey: string | null;
      try {
        existingKey = await lookupKey(addr);
      } catch (err) {
        // Fail closed on a lookup error (e.g. a transient DB failure): route
        // nothing for this address and surface the failure, rather than let
        // the rejection float out of this void-dispatched handler and take
        // down the hub.
        logger.error`Key lookup failed for ${addr} during register: ${err instanceof Error ? err.message : String(err)}; failing closed (challenged reconnect required)`;
        continue;
      }
      if (existingKey !== null) {
        logger.warn`Refusing to route ${addr} via register: address already has a stored key; ownership must be proven via challenged reconnect`;
        continue;
      }
      routableAddresses.push(addr);
    }

    // Additive re-register: inherit every address this ws already owns and ADD
    // the frame's keyless first-deploys. Register never drops an owned route --
    // an address proved via challenged reconnect stays routed, and removal
    // happens via undeploy/disconnect, not register-omission.
    //
    // (Under the retired full-set model a register frame carried the sidecar's
    // complete live set, so an omitted address meant "removed". Now the frame
    // carries only keyless first-deploys the sidecar is adding -- keyed
    // addresses arrive via reconnect -- so omission is meaningless, and a
    // replace-on-register would wrongly drop an earlier first-deploy when a
    // later one is registered, as well as any reconnect-verified keyed route.)
    const existing = connections.get(ws);
    const inheritedAgent = new Set(existing?.agentAddresses);
    const inheritedWorkflow = new Set(existing?.workflowAddresses);

    // Clean up ghost entries from OTHER connections for the newly-claimed
    // addresses only. An inherited address is already owned by this ws
    // (prevWs === ws), so it needs no eviction and its in-flight deploy must
    // not be cancelled.
    for (const addr of routableAddresses) {
      const prevWs = addressIndex.get(addr);
      if (prevWs !== undefined && prevWs !== ws) {
        const prevConn = connections.get(prevWs);
        if (prevConn !== undefined) {
          prevConn.agentAddresses.delete(addr);
        }
        // The new owner is about to take over; the prior owner's
        // cached state must not survive into the new owner's window
        // before its bootstrap frame arrives.
        connectorStates.delete(addr);
      }

      // Cancel any in-flight deploy for this address since the
      // reconnecting sidecar is taking ownership.
      const staleDeployReq = pendingDeploys.get(addr);
      if (staleDeployReq !== undefined) {
        clearTimeout(staleDeployReq.timer);
        pendingDeploys.delete(addr);
        staleDeployReq.reject(
          `Sidecar ${sidecarId} reconnected and claimed address "${addr}"`,
        );
      }
    }

    // New keyless first-deploys join the inherited session set; the workflow
    // set is inherited unchanged (register never adds to it -- a
    // workflow-derived address routes only through the challenged reconnect).
    for (const addr of routableAddresses) {
      inheritedAgent.add(addr);
    }
    const addrSet = inheritedAgent;
    const workflowSet = inheritedWorkflow;

    const conn: SidecarConnection = {
      sidecarId,
      identity,
      agentAddresses: addrSet,
      workflowAddresses: workflowSet,
      send(frame: HubFrame) {
        ws.send(JSON.stringify(frame));
      },
    };

    connections.set(ws, conn);
    // Only the newly-claimed addresses need a routing write + queue discard;
    // inherited addresses already point at this ws in addressIndex.
    for (const addr of routableAddresses) {
      addressIndex.set(addr, ws);
      // Discard any disconnect queue — register has no identity
      // verification, so flushing to an unverified connection is unsafe.
      // Use reconnect with challenge/response to preserve queued messages.
      const staleQueue = disconnectedAgents.get(addr);
      if (staleQueue !== undefined) {
        clearTimeout(staleQueue.timer);
        if (staleQueue.queue.length > 0) {
          logger.warn`Discarding ${String(staleQueue.queue.length)} queued message(s) for ${addr} on unverified register`;
        }
        disconnectedAgents.delete(addr);
      }
    }
    // Only keyless first-deploy addresses reach `addressIndex` here; an
    // address that already has a stored key was filtered out by the gate
    // above and must re-enter routing through the challenged reconnect path.
    // Because the gate runs before the ghost-cleanup, a rejected (keyed)
    // address never evicts its prior owner -- register cannot reclaim or
    // disrupt a victim's route on token auth alone.

    logger.info`Sidecar ${sidecarId} registered; routed ${String(addrSet.size)} of ${String(agentAddresses.length)} address(es) (keyless first-deploy only)`;
  }

  async function handleReconnect(
    ws: WsHandle,
    identity: SidecarAuthIdentity,
    agentAddresses: string[],
    deployRefs: Record<string, string> = {},
  ): Promise<void> {
    if (identity.kind === "allocated") {
      await handleAllocatedRegister(ws, identity, agentAddresses);
      return;
    }
    const sidecarId = identity.sidecarId;

    const lookupKey = lookups.lookupPublicKey;
    if (lookupKey === undefined) {
      logger.error`Received reconnect frame but no lookupPublicKey is configured`;
      ws.close();
      return;
    }

    // Cancel any existing pending challenge for this ws before doing
    // async work, so concurrent reconnect frames don't race.
    const existingChallenge = pendingChallenges.get(ws);
    if (existingChallenge !== undefined) {
      clearTimeout(existingChallenge.timer);
      pendingChallenges.delete(ws);
    }

    // Capture the addresses this live connection already owns. The
    // internal register below clears them, but ones the reconnect is not
    // re-challenging are still valid — an agent provisioned during the
    // sidecar's restore window is routed into addressIndex by its deploy
    // and is not part of the disk-restored set this reconnect carries.
    // Without preserving them, the reconnect silently drops a
    // freshly-deployed agent from routing.
    const previouslyOwned = new Set(connections.get(ws)?.agentAddresses);

    // Register the sidecar connection immediately (with no addresses) so it
    // can receive frames while the ownership challenge is pending. Every
    // reconnect address -- session and workflow-derived alike -- enters
    // routing only through the verified path below, never unchallenged here.
    // Empty address list, so the key-existence gate has nothing to await.
    // The identity was already verified at the dispatch boundary, so the
    // internal register does not re-authenticate.
    await handleRegister(ws, identity, []);

    const conn = connections.get(ws);
    if (conn === undefined) return;

    // Re-add the still-owned addresses the register cleared but this
    // reconnect is not re-challenging. The challenged addresses below
    // re-enter addressIndex through the verified path instead.
    const claimedAddresses = new Set(agentAddresses);
    for (const addr of previouslyOwned) {
      if (claimedAddresses.has(addr)) continue;
      conn.agentAddresses.add(addr);
      addressIndex.set(addr, ws);
    }

    // Look up stored public keys for all claimed addresses. Fail closed on a
    // lookup error (e.g. a transient DB failure): treat the address as
    // unverifiable so it fails its challenge and stays unrouted, rather than
    // letting the rejection float out of this void-dispatched handler as an
    // unhandled rejection that could take down the hub.
    const keyLookups = await Promise.all(
      agentAddresses.map(async (addr) => {
        try {
          return { address: addr, publicKeyHex: await lookupKey(addr) };
        } catch (err) {
          logger.error`Key lookup failed for ${addr} during reconnect: ${err instanceof Error ? err.message : String(err)}; failing closed`;
          return { address: addr, publicKeyHex: null };
        }
      }),
    );

    // If the connection was closed or superseded while we were awaiting
    // key lookups, bail out.
    if (!connections.has(ws)) return;

    const challenges = new Map<
      string,
      { nonce: Uint8Array; publicKey: Uint8Array }
    >();
    const challengeEntries: { address: string; nonce: string }[] = [];

    for (const { address, publicKeyHex } of keyLookups) {
      if (publicKeyHex === null) {
        conn.send({
          type: "challenge.failed",
          address,
          reason: "Unknown run address",
        });
        continue;
      }

      let publicKey: Uint8Array;
      try {
        publicKey = hexDecode(publicKeyHex);
      } catch {
        conn.send({
          type: "challenge.failed",
          address,
          reason: "Stored public key is corrupt",
        });
        logger.error`Corrupt stored public key for ${address}`;
        continue;
      }

      const nonce = crypto.getRandomValues(new Uint8Array(32));
      challenges.set(address, { nonce, publicKey });
      challengeEntries.push({ address, nonce: hexEncode(nonce) });
    }

    if (challenges.size === 0) return;

    // If another reconnect completed while we were building the
    // challenge, it will have written its own entry. Don't overwrite.
    if (pendingChallenges.has(ws)) return;

    const timer = setTimeout(() => {
      pendingChallenges.delete(ws);
      logger.warn`Challenge timed out for sidecar ${sidecarId}`;
    }, challengeTimeoutMs);

    pendingChallenges.set(ws, {
      sidecarId,
      challenges,
      deployRefs,
      timer,
    });

    conn.send({ type: "challenge", challenges: challengeEntries });
  }

  async function handleChallengeResponse(
    ws: WsHandle,
    responses: { address: string; signature: string }[],
  ): Promise<void> {
    const challenge = pendingChallenges.get(ws);
    if (challenge === undefined) {
      logger.warn`Received challenge.response with no pending challenge`;
      return;
    }

    clearTimeout(challenge.timer);
    pendingChallenges.delete(ws);

    const conn = connections.get(ws);
    if (conn === undefined) return;

    const verified: string[] = [];
    const responded = new Set<string>();

    for (const { address, signature } of responses) {
      responded.add(address);

      const entry = challenge.challenges.get(address);
      if (entry === undefined) {
        conn.send({
          type: "challenge.failed",
          address,
          reason: "Address was not challenged",
        });
        continue;
      }

      let valid = false;
      try {
        const nonceBytes = entry.nonce;
        const addressBytes = new TextEncoder().encode(address);
        const payload = new Uint8Array(nonceBytes.length + addressBytes.length);
        payload.set(nonceBytes);
        payload.set(addressBytes, nonceBytes.length);

        const sigBytes = hexDecode(signature);
        valid = await verifyEd25519(payload, sigBytes, entry.publicKey);
      } catch (err) {
        logger.warn`Challenge failed for ${address}: ${err instanceof Error ? err.message : String(err)}`;
      }

      if (valid) {
        verified.push(address);
      } else {
        conn.send({
          type: "challenge.failed",
          address,
          reason: "Signature verification failed",
        });
      }
    }

    // Notify about challenged addresses that were omitted from the response.
    for (const address of challenge.challenges.keys()) {
      if (!responded.has(address)) {
        conn.send({
          type: "challenge.failed",
          address,
          reason: "No response provided for challenged address",
        });
        logger.warn`Challenge failed for ${address}: no response provided`;
      }
    }

    // Add verified addresses to the routing table immediately so that
    // `agent.reconnected` subscribers can use sendRequest-based methods
    // (e.g. sendSourcesUpdate). Addresses that fail governance are
    // rolled back from the routing table afterward.
    for (const addr of verified) {
      // If a different ws still owns this address (live takeover via
      // verified reconnect), evict its cached connector state before
      // routing flips. The new owner's harness will bootstrap via
      // restore-fires-callback.
      const prevWs = addressIndex.get(addr);
      if (prevWs !== undefined && prevWs !== ws) {
        connectorStates.delete(addr);
        // Evict the reclaimed address from the superseded connection's owned
        // set. handleClose's cancelByAgent sweep iterates a connection's owned
        // union WITHOUT an ownership guard, so if the stale connection still
        // listed this address it would cancel THIS connection's in-flight pack
        // transfer for it when it finally closes. Delete from both sets: a
        // workflow-derived address lives on the workflow set, a launched agent
        // on the session set, and delete is a no-op for the absent one.
        const prevConn = connections.get(prevWs);
        if (prevConn !== undefined) {
          prevConn.workflowAddresses.delete(addr);
          prevConn.agentAddresses.delete(addr);
        }
      }
      // A run address goes on the workflow set (no disconnect queue -- its
      // in-flight state is reconstructed sidecar-locally on the next reconnect),
      // so handleClose reclaims it correctly. The routing pointer is the same
      // either way; only now it is written behind a passed challenge.
      //
      // The `else` (session set, queued for reconnect) is the retired
      // launched-agent path: launched agents no longer exist (the folded-launch
      // route was removed), so no current producer reaches it. It is left in
      // place for the reconnect-subsystem teardown to remove as its own
      // reviewable change, not folded into this collapse.
      if (isRunAddress(addr)) {
        conn.workflowAddresses.add(addr);
      } else {
        conn.agentAddresses.add(addr);
      }
      addressIndex.set(addr, ws);
    }

    const ready: string[] = [];
    const failed: string[] = [];

    for (const addr of verified) {
      // A workflow run needs routing + queue flush only, which the passed
      // challenge has now made safe: its in-flight state is reconstructed
      // sidecar-locally, so it does not enrol in the `agent.reconnected` session
      // reaction (event-collector restore). Skip the reaction for it.
      //
      // The reaction below was the retired launched-agent path (restore a
      // launched agent's collector). Launched agents no longer exist, so no
      // current producer reaches it; the `agent.reconnected` listener is gone,
      // so `listenerCount` is 0 and the emit is inert. Left for the
      // reconnect-subsystem teardown to remove.
      if (isRunAddress(addr)) {
        ready.push(addr);
        continue;
      }
      if (events.listenerCount("agent.reconnected") === 0) {
        ready.push(addr);
        continue;
      }
      try {
        await events.emitAndAwait("agent.reconnected", { agentAddress: addr });
        ready.push(addr);
      } catch (err) {
        logger.error`Failed to handle reconnection for ${addr}: ${err instanceof Error ? err.message : String(err)}`;
        failed.push(addr);
      }
    }

    // If a second reconnect arrived during the callback loop, our conn
    // is orphaned — handleRegister already rebuilt the connection and
    // cleared addressIndex. Bail out; the new reconnect flow will
    // re-verify these addresses from scratch.
    if (connections.get(ws) !== conn) {
      logger.warn`Challenge response processing aborted: connection superseded by new reconnect`;
      return;
    }

    // Roll back failed addresses from the routing table. Only the session set
    // is touched: a workflow-derived address can never be in `failed` -- it
    // early-`continue`s to `ready` above, before the reaction that populates
    // `failed` -- so it is never on the workflow set at this point.
    for (const addr of failed) {
      conn.agentAddresses.delete(addr);
      addressIndex.delete(addr);
    }

    // Flush queued messages and redeliver retained un-acked mail, only for
    // ready addresses. The disconnect queue carries mail that arrived WHILE
    // disconnected; pending-mail redelivery carries mail that was delivered
    // over the prior live connection but never acked (the connected-window
    // drop). Both replay over the verified new connection.
    for (const addr of ready) {
      flushDisconnectedQueue(addr, conn);
      redeliverPendingMail(addr, conn);
    }

    // Re-deploy agents whose deploy ref is stale or absent. Fire-and-forget
    // so reconnect completion is not blocked on pack transfer. The
    // wire layer owns the staleness comparison; the event fires only
    // when staleness is confirmed.
    const checkDeployRef = lookups.lookupDeployRef;
    if (checkDeployRef !== undefined) {
      for (const addr of ready) {
        // Workflow deployments are pinned-forever: a deployment keeps its
        // deploy-time definition until an explicit undeploy/redeploy, so the
        // deploy-ref freshness catch-up is deliberately NOT run for a run
        // address. A definition edited on the hub while the sidecar was
        // disconnected does not reconcile on reconnect; it affects only newly
        // created deployments. The deployment's in-flight run state is
        // reconstructed sidecar-locally at restore, not re-fetched here. Do NOT
        // add a reconcile path for these addresses -- see the "Workflow
        // Definition Versioning: Pinned-Forever" note under "Reconnect
        // Sequencing" in docs/IMPLEMENTATION.md. (Every routable address is a
        // run address now; the fall-through was the retired launched-agent
        // catch-up path.)
        if (isRunAddress(addr)) continue;
        void (async () => {
          try {
            const hubRef = await checkDeployRef(addr);
            if (hubRef === null) return;
            const sidecarRef = challenge.deployRefs[addr];
            if (sidecarRef === hubRef) return;

            logger.info`Re-deploying ${addr}: sidecar ref ${sidecarRef ?? "(none)"} != hub ref ${hubRef.slice(0, 8)}`;
            await events.emitAndAwait("deploy.ref.stale", {
              agentAddress: addr,
            });
          } catch (err) {
            logger.error`Failed to re-deploy ${addr} after reconnect: ${err instanceof Error ? err.message : String(err)}`;
          }
        })();
      }
    }

    // Failed addresses: reset their queue TTL so messages survive until
    // the next reconnect attempt, and notify the sidecar.
    for (const addr of failed) {
      const entry = disconnectedAgents.get(addr);
      if (entry !== undefined) {
        clearTimeout(entry.timer);
        entry.timer = setTimeout(() => {
          const expired = disconnectedAgents.get(addr);
          disconnectedAgents.delete(addr);
          if (expired !== undefined) {
            surfaceDroppedFrames(
              addr,
              expired.queue,
              "disconnect queue TTL expired",
            );
          }
        }, disconnectQueueTTLMs);
      }
      conn.send({
        type: "challenge.failed",
        address: addr,
        reason: "Reconnection rejected by governance",
      });
    }

    logger.info`Sidecar ${challenge.sidecarId} reconnected with ${String(ready.length)} verified agent(s)${failed.length > 0 ? `, ${String(failed.length)} rejected` : ""}`;
  }

  async function handleMailOutbound(
    rawMessage: string,
    recipients: string[],
  ): Promise<void> {
    // A mail addressed to more than one workflow deployment would birth a
    // run per recipient from a single inbound mail. The stable runId
    // removed the Message-ID collision that originally forced this guard --
    // each recipient now derives its own per-deployment runId (its mail
    // address), so it is no longer a runId-collision guard. It stays a
    // deliberate one-workflow-recipient-per-mail restriction because the
    // fan-out is not verified end-to-end: per-recipient grants
    // materialization, consumed-tracking, and reply-addressing all assume a
    // single workflow recipient today. Lifting it means proving those three
    // hold per recipient, not just relaxing this check -- so fail loudly
    // rather than materialize a partial set. The guard only applies when a
    // materializer is wired -- absent one, no run is born from the mail, so
    // there is nothing to restrict.
    if (lookups.materializeMailTriggeredRunGrants !== undefined) {
      // A workflow recipient is one this hub owns: its address parses as a run
      // address. An external/federated address does not, and is not ours to
      // materialize a run for.
      const workflowRecipients = recipients.filter(isRunAddress);
      if (workflowRecipients.length > 1) {
        throw new Error(
          `mail addressed to multiple workflow-derived recipients (${workflowRecipients.join(", ")}); materializing a run for more than one workflow deployment from a single mail is unsupported`,
        );
      }
    }

    // Route to locally connected sidecars first, then try disconnect queues.
    const unrouted: string[] = [];
    for (const recipient of recipients) {
      // Each recipient is isolated: a materialization failure or a
      // fail-closed rejection for one must not drop the mail for its
      // co-recipients. The catch fails THIS recipient closed (its run never
      // starts under-authorized) and continues to the rest.
      try {
        const outcome = await deliverMailToRecipient(recipient, rawMessage);
        if (outcome === "unrouted") unrouted.push(recipient);
      } catch (err) {
        logger.error`Failed to deliver mail to ${recipient}: ${err instanceof Error ? err.message : String(err)}`;
      }
    }

    // Anything not routed locally is emitted as a notification. The
    // host decides whether to relay onto an external transport, log,
    // or drop. The wire layer takes no stance.
    if (unrouted.length > 0) {
      events.emit("mail.outbound.undelivered", {
        rawMessage,
        recipients: unrouted,
      });
    }
  }

  // Deliver an inbound mail to one recipient, materializing a
  // mail-triggered run's grants first when the recipient is a workflow
  // deployment. Returns:
  //   - `routed`: the mail reached a live connection or disconnect queue.
  //   - `unrouted`: the mail was locally undeliverable and should be
  //     relayed externally by the host.
  //   - `failed-closed`: the run's grants could not be materialized safely,
  //     so the mail is deliberately DROPPED for this recipient (not relayed)
  //     to keep its run from starting under-authorized.
  //
  // A workflow deployment is the only recipient whose inbound mail can first
  // fire its stable run. Its grants are reserved, and the `run.grants` frame is
  // sent BEFORE the mail. Same-address FIFO guarantees it lands ahead of the
  // mail that dispatches the run, so the run's `onRunStart` barrier resolves its
  // grants rather than failing closed. Reservation happens before routing so
  // concurrent first deliveries cannot send different snapshots; a routing
  // failure leaves a grants-only, still-unfired run.
  async function deliverMailToRecipient(
    recipient: string,
    rawMessage: string,
  ): Promise<"routed" | "unrouted" | "failed-closed"> {
    if (
      lookups.materializeMailTriggeredRunGrants !== undefined &&
      isRunAddress(recipient)
    ) {
      const runId = deriveWorkflowRunId(recipient);
      const result = await lookups.materializeMailTriggeredRunGrants({
        agentAddress: recipient,
        runId,
      });
      if (result.outcome === "rejected") {
        // The run's grants could not be materialized with sufficient
        // authority or it is already terminal. Fail the mail closed for this
        // recipient: routing or external relay would bypass that decision.
        logger.error`Refusing mail-triggered run ${runId} for ${recipient}: grant materialization rejected (${result.code}): ${result.message}`;
        return "failed-closed";
      }
      if (result.outcome === "materialized") {
        // Send the run's grants ahead of the mail. A `false` here means the
        // deployment is unroutable. Do not route the mail that would dispatch
        // it; the grants-only reservation remains the canonical snapshot for a
        // later first-delivery attempt.
        if (!sendRunGrants(recipient, runId, result.stepGrants)) {
          logger.error`Deployment ${recipient} is not routable for run ${runId}; retaining the unfired run's grant reservation for retry`;
          return "unrouted";
        }
        // Route through the messageId handshake `routeMail` -- NOT a
        // fire-and-forget send. This branch COMMITS a run, so a mail dropped in
        // the connected window (a socket that half-dies before the sidecar's
        // durable-write ack) would otherwise leave the run row "running"
        // forever with no body and no error. `routeMail` tracks the delivery
        // and redelivers identical bytes on reconnect, bringing the mail-relay
        // run-trigger to parity with the HTTP-trigger path. The messageId is the
        // mail's own id (derived over the same bytes the sidecar derives), so a
        // redelivery replays identically and the downstream RunStarted /
        // stable-runId dedup makes it effectively-once.
        const messageId = await deriveMessageId(base64Decode(rawMessage));
        const outcome: "routed" | "unrouted" = routeMail(
          recipient,
          rawMessage,
          messageId,
          { runId, stepGrants: result.stepGrants },
        )
          ? "routed"
          : "unrouted";
        return outcome;
      }
      // `skip`: the address named no deployed workflow deployment. Forward
      // the mail without grants -- the run, if any, is not ours to
      // authorize. No run is committed here, so no ack handshake is needed.
    }

    return routeMail(recipient, rawMessage) ? "routed" : "unrouted";
  }

  async function handleMailPersist(
    persist: NonNullable<SidecarLookups["persistMail"]>,
    rawMessage: string,
    senderAddress: string,
    recipients: string[],
  ): Promise<void> {
    let results: SidecarMailPersistedRow[];
    let raw: Uint8Array;
    try {
      raw = Uint8Array.from(atob(rawMessage), (c) => c.charCodeAt(0));
      results = await persist({
        senderAddress,
        recipients,
        raw,
      });
    } catch (err) {
      logger.error`Failed to persist mail from ${senderAddress}: ${err instanceof Error ? err.message : String(err)}`;
      return;
    }

    for (const result of results) {
      events.emit("mail.persisted", {
        id: result.id,
        raw,
        createdAt: result.createdAt,
        direction: result.direction,
        runId: result.runId,
        address: result.address,
      });
    }
  }

  async function handleSignalCorrelationRegister(
    ws: WsHandle,
    frame: SignalCorrelationRegisterFrame,
  ): Promise<void> {
    // Gate the co-write on the sending sidecar actually owning the named
    // deployment address, mirroring the connector.state.changed and pack
    // handlers. A workflow deployment routes on the keyless workflow set, so
    // ownership is the union check, not addressIndex identity alone. Without
    // it a misbehaving sidecar that knows another deployment's address could
    // register a spurious correlation against it.
    const conn = connections.get(ws);
    if (conn === undefined) return;
    if (!connOwnsAddress(conn, frame.agentAddress)) {
      logger.warn`Dropping signal.correlation.register for ${frame.agentAddress}: not registered to this sidecar`;
      return;
    }

    const register = lookups.registerSignalCorrelation;
    if (register === undefined) {
      logger.warn`Dropping signal.correlation.register for ${frame.agentAddress}: no registerSignalCorrelation lookup configured`;
      return;
    }

    try {
      await register({
        correlationId: frame.correlationId,
        runId: frame.runId,
        anchorRunId: frame.anchorRunId,
        agentAddress: frame.agentAddress,
        kind: frame.kind,
        approvalSnapshot: frame.snapshot,
      });
      // The co-write resolves only when a row exists -- freshly inserted or
      // already present (both stores are idempotent on the correlationId). Ack
      // so the sidecar's link stops retrying a register whose frame may have
      // been lost on an open socket. A thrown co-write (undeployed deployment,
      // id mismatch) means no row, so no ack: the sidecar keeps retrying and
      // the reconnect re-emit remains the ultimate backstop.
      conn.send({
        type: "signal.correlation.register.ack",
        agentAddress: frame.agentAddress,
        correlationId: frame.correlationId,
      });
    } catch (err) {
      logger.error`Failed to register signal correlation ${frame.correlationId} for ${frame.agentAddress}: ${err instanceof Error ? err.message : String(err)}`;
    }
  }

  function handleClose(ws: WsHandle): void {
    const conn = connections.get(ws);
    if (conn === undefined) return;
    let allocated: { allocationId: string; generation: number } | undefined;

    for (const addr of conn.agentAddresses) {
      // Only remove routing and pending state if this connection still
      // owns the address. A reconnected sidecar may have already claimed it.
      if (addressIndex.get(addr) === ws) {
        addressIndex.delete(addr);
        // Drop cached connector state for the same reason: a takeover
        // sidecar's state lives in connectorStates under the same key,
        // and only this owner's close should evict it. The next
        // reconnect re-bootstraps via the router's
        // restore-fires-callback path.
        connectorStates.delete(addr);
        // Retain this address's un-acked pending mail across the disconnect:
        // its in-flight retry timers target a dead socket (cleared), but the
        // entries are held so a verified reconnect redelivers them, closing the
        // connected-window drop rather than losing the mail. Bounded by a
        // retention TTL.
        retainPendingMailForAddress(addr);
        // Create a queue entry so messages can accumulate while the
        // sidecar is disconnected. Skip if the agent is being undeployed --
        // there is no point queuing messages for an agent being torn down.
        if (!pendingUndeploys.has(addr)) {
          const timer = setTimeout(() => {
            const expired = disconnectedAgents.get(addr);
            disconnectedAgents.delete(addr);
            if (expired !== undefined) {
              surfaceDroppedFrames(
                addr,
                expired.queue,
                "disconnect queue TTL expired",
              );
            }
          }, disconnectQueueTTLMs);
          disconnectedAgents.set(addr, { queue: [], timer });
        }
      }
    }
    // Remove this connection's workflow-substrate routes. No disconnect queue
    // is created: these addresses re-register (with the complete live set)
    // when the sidecar reconnects, and their in-flight run state is
    // reconstructed sidecar-locally, not from a hub-side queue. The ownership
    // guard mirrors the session loop above so a takeover by a newer ws is not
    // clobbered by the prior owner's close.
    for (const addr of conn.workflowAddresses) {
      if (addressIndex.get(addr) === ws) {
        addressIndex.delete(addr);
        // Retain un-acked workflow trigger mail across the disconnect for the
        // same reason as the session loop above -- a challenged reconnect
        // redelivers it. This is un-acked TRIGGER mail, distinct from the
        // deployment's in-flight run state (reconstructed sidecar-locally); the
        // "no disconnect queue" note above is about that run state, not this.
        retainPendingMailForAddress(addr);
      }
    }
    if (conn.identity.kind === "allocated") {
      const current = allocatedConnections.get(conn.identity.allocationId);
      if (current?.ws === ws) {
        allocatedConnections.delete(conn.identity.allocationId);
        allocated = {
          allocationId: conn.identity.allocationId,
          generation: conn.identity.generation,
        };
      }
    }
    connections.delete(ws);

    // Cancel the liveness timer for this connection.
    const livenessTimer = livenessTimers.get(ws);
    if (livenessTimer !== undefined) {
      clearTimeout(livenessTimer);
      livenessTimers.delete(ws);
    }

    // Drop the per-ws serialization chain; no more frames will queue on it.
    messageChains.delete(ws);

    // Cancel any pending challenge for this connection.
    const challengeReq = pendingChallenges.get(ws);
    if (challengeReq !== undefined) {
      clearTimeout(challengeReq.timer);
      pendingChallenges.delete(ws);
    }

    // Reject any in-flight requests that were sent to this sidecar.
    for (const [requestId, req] of pending) {
      if (req.ws !== ws) continue;
      clearTimeout(req.timer);
      pending.delete(requestId);
      req.reject(`Sidecar ${conn.sidecarId} disconnected`);
    }

    // Reject every deploy issued on this socket, including allocated
    // workflow deployments stored in `workflowAddresses` rather than
    // `agentAddresses`.
    for (const [agentAddress, req] of pendingDeploys) {
      if (req.ws !== ws) continue;
      clearTimeout(req.timer);
      pendingDeploys.delete(agentAddress);
      req.reject(`Sidecar ${conn.sidecarId} disconnected`);
    }

    // Reject any in-flight pack transfers for this sidecar.
    for (const [transferId, pack] of pendingPacks) {
      if (pack.ws !== ws) continue;
      clearTimeout(pack.timer);
      pendingPacks.delete(transferId);
      pack.reject(`Sidecar ${conn.sidecarId} disconnected`);
    }

    // Reject any in-flight undeploys for this sidecar.
    for (const [addr, req] of pendingUndeploys) {
      if (req.ws !== ws) continue;
      clearTimeout(req.timer);
      pendingUndeploys.delete(addr);
      req.reject(`Sidecar ${conn.sidecarId} disconnected`);
    }

    // Reject any in-flight probes sent to this sidecar. A probe never enters
    // the address maps, so this ws-keyed sweep is its ONLY disconnect cleanup:
    // without it a probe whose sidecar drops mid-flight would hang until its
    // own timeout instead of failing fast on the disconnect.
    for (const [requestId, probe] of pendingProbes) {
      if (probe.ws !== ws) continue;
      clearTimeout(probe.timer);
      pendingProbes.delete(requestId);
      probe.reject(`Sidecar ${conn.sidecarId} disconnected`);
    }

    // Cancel any in-flight inbound pack transfers from this sidecar
    // across both receivers. The two receivers track their own in-
    // flight transferIds, so a pending workflow-run transfer for an
    // agent that just disconnected won't outlive the connection just
    // because the agent-state receiver has nothing to cancel. Iterate the
    // owned union so a reconnected workflow deployment's transfer is
    // cancelled too; the deduped set avoids a double-cancel for an address
    // that is in both sets. A reclaimed address is not present here -- the
    // verified reconnect path that took it over evicts it from this
    // (superseded) connection's owned set -- so a stale close does not cancel
    // the new owner's work.
    const owned = ownedAddresses(conn);
    for (const addr of owned) {
      agentStatePackReceiver.cancelByAgent(addr);
      workflowRunPackReceiver.cancelByAgent(addr);
    }

    events.emit("sidecar.disconnect", {
      ownedAddresses: [...owned],
      ...(allocated !== undefined ? { allocated } : {}),
    });

    logger.info`Sidecar ${conn.sidecarId} disconnected`;
  }

  function nextRequestId(): string {
    return `req-${++requestCounter}`;
  }

  function sendRequest(
    agentAddress: string,
    buildFrame: (requestId: string) => HubFrame,
  ): Promise<void> {
    const ws = addressIndex.get(agentAddress);
    if (ws === undefined) {
      return Promise.reject(
        new Error(`No sidecar connected for agent "${agentAddress}"`),
      );
    }
    const conn = connections.get(ws);
    if (conn === undefined) {
      return Promise.reject(
        new Error(`No sidecar connected for agent "${agentAddress}"`),
      );
    }

    const requestId = nextRequestId();
    const frame = buildFrame(requestId);

    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        pending.delete(requestId);
        reject(
          new Error(
            `Request ${requestId} timed out after ${requestTimeoutMs}ms`,
          ),
        );
      }, requestTimeoutMs);

      pending.set(requestId, {
        requestId,
        ws,
        resolve,
        reject(error: string) {
          reject(new Error(error));
        },
        timer,
      });

      conn.send(frame);
    });
  }

  function resolvePending(requestId: string): void {
    const req = pending.get(requestId);
    if (req === undefined) return;
    clearTimeout(req.timer);
    pending.delete(requestId);
    req.resolve();
  }

  function rejectPending(requestId: string, error: string): void {
    const req = pending.get(requestId);
    if (req === undefined) return;
    clearTimeout(req.timer);
    pending.delete(requestId);
    req.reject(error);
  }

  function packResponseMatches(
    entry: PendingPack,
    ws: WsHandle,
    frame: PackAckFrame | PackRejectFrame,
  ): boolean {
    return (
      entry.ws === ws &&
      entry.agentAddress === frame.agentAddress &&
      entry.repoId.kind === frame.repoId.kind &&
      entry.repoId.id === frame.repoId.id
    );
  }

  function resolvePackPending(ws: WsHandle, frame: PackAckFrame): void {
    const entry = pendingPacks.get(frame.transferId);
    if (entry === undefined) return;
    if (!packResponseMatches(entry, ws, frame)) {
      logger.warn`Ignoring repo.pack.ack for transfer ${frame.transferId} from a connection that does not own the pending transfer`;
      return;
    }
    clearTimeout(entry.timer);
    pendingPacks.delete(frame.transferId);
    entry.resolve();
  }

  function rejectPackPending(ws: WsHandle, frame: PackRejectFrame): void {
    const entry = pendingPacks.get(frame.transferId);
    if (entry === undefined) return;
    if (!packResponseMatches(entry, ws, frame)) {
      logger.warn`Ignoring repo.pack.reject for transfer ${frame.transferId} from a connection that does not own the pending transfer`;
      return;
    }
    clearTimeout(entry.timer);
    pendingPacks.delete(frame.transferId);
    // Surface the receiver's specific cause when it carried one, so the awaiting
    // push sees "corrupt: <detail>" rather than only the coarse reason.
    entry.reject(
      frame.detail !== undefined
        ? `${frame.reason}: ${frame.detail}`
        : frame.reason,
    );
  }

  function resolveUndeployPending(ws: WsHandle, agentAddress: string): void {
    const req = pendingUndeploys.get(agentAddress);
    if (req === undefined) {
      logger.warn`Received agent.undeploy.ack for "${agentAddress}" with no pending undeploy`;
      return;
    }
    if (req.ws !== ws) return;
    clearTimeout(req.timer);
    pendingUndeploys.delete(agentAddress);
    req.resolve();
  }

  function rejectUndeployPending(
    ws: WsHandle,
    agentAddress: string,
    error: string,
  ): void {
    const req = pendingUndeploys.get(agentAddress);
    if (req === undefined) return;
    if (req.ws !== ws) return;
    clearTimeout(req.timer);
    pendingUndeploys.delete(agentAddress);
    req.reject(error);
  }

  function resolveProbe(requestId: string, result: WorkflowProbeResult): void {
    const req = pendingProbes.get(requestId);
    if (req === undefined) return;
    clearTimeout(req.timer);
    pendingProbes.delete(requestId);
    req.resolve(result);
  }

  function rejectProbe(requestId: string, error: string): void {
    const req = pendingProbes.get(requestId);
    if (req === undefined) return;
    clearTimeout(req.timer);
    pendingProbes.delete(requestId);
    req.reject(error);
  }

  // Routing rule: pick the receiver dedicated to the repoId.kind the
  // frame carries. The receivers' in-flight state is independent, so a
  // workflow-run transferId can never collide with or evict an
  // agent-state transferId for the same agentAddress.
  function pickPackReceiver(
    repoId: RepoId,
  ): { receiver: ReturnType<typeof createPackReceiver> } | null {
    switch (repoId.kind) {
      case "agent-state":
        return { receiver: agentStatePackReceiver };
      case "workflow-run":
        return { receiver: workflowRunPackReceiver };
      // The remaining kinds in `RepoKind` (`skill`, `package-registry`,
      // `workflow`) have no sidecar->hub pack flow today. A frame
      // arriving with those kinds is malformed at this layer.
      default:
        return null;
    }
  }

  function pickReceivePackLookup(
    repoId: RepoId,
  ): SidecarLookups["receiveWorkflowRunPack"] | undefined {
    switch (repoId.kind) {
      case "agent-state":
        // The agent-state lookup ignores the `source` argument the workflow-run
        // lookup takes; the two are otherwise the same contract.
        return lookups.receiveAgentStatePack;
      case "workflow-run":
        return lookups.receiveWorkflowRunPack;
      default:
        return undefined;
    }
  }

  function handlePackPush(ws: WsHandle, frame: PackPushFrame): void {
    const conn = connections.get(ws);
    if (conn === undefined) return;
    if (!connOwnsAddress(conn, frame.agentAddress)) {
      logger.warn`Received repo.pack.push for unrouted agent ${frame.agentAddress}`;
      return;
    }
    if (!connCanPushRepo(conn, frame.agentAddress, frame.repoId)) {
      logger.warn`Rejected repo.pack.push outside sidecar ${conn.sidecarId}'s authenticated repository scope`;
      conn.send({
        type: "repo.pack.reject",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
        reason: "path_violation",
      });
      return;
    }

    const picked = pickPackReceiver(frame.repoId);
    if (picked === null) {
      logger.warn`Received repo.pack.push with unsupported repoId.kind ${frame.repoId.kind}`;
      conn.send({
        type: "repo.pack.reject",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
        reason: "corrupt",
      });
      return;
    }

    const reason = picked.receiver.handlePush(frame);
    if (reason !== null) {
      conn.send({
        type: "repo.pack.reject",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
        reason,
      });
    }
  }

  async function handlePackDone(
    ws: WsHandle,
    frame: PackDoneFrame,
  ): Promise<void> {
    const conn = connections.get(ws);
    if (conn === undefined) return;
    if (!connOwnsAddress(conn, frame.agentAddress)) {
      logger.warn`Received repo.pack.done for unrouted agent ${frame.agentAddress}`;
      return;
    }
    if (!connCanPushRepo(conn, frame.agentAddress, frame.repoId)) {
      logger.warn`Rejected repo.pack.done outside sidecar ${conn.sidecarId}'s authenticated repository scope`;
      conn.send({
        type: "repo.pack.reject",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
        reason: "path_violation",
      });
      return;
    }

    const picked = pickPackReceiver(frame.repoId);
    if (picked === null) {
      logger.warn`Received repo.pack.done with unsupported repoId.kind ${frame.repoId.kind}`;
      conn.send({
        type: "repo.pack.reject",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
        reason: "corrupt",
      });
      return;
    }

    const result = picked.receiver.handleDone(frame);
    if (result === null) {
      conn.send({
        type: "repo.pack.reject",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
        reason: "corrupt",
      });
      return;
    }

    const receivePackLookup = pickReceivePackLookup(frame.repoId);
    if (receivePackLookup === undefined) {
      conn.send({
        type: "repo.pack.ack",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
      });
      return;
    }

    const verdict = await receivePackLookup(
      frame.repoId,
      result.pack,
      result.ref,
      result.commitSha,
      conn.identity.kind === "allocated"
        ? {
            kind: "allocated",
            agentAddress: frame.agentAddress,
            allocationId: conn.identity.allocationId,
            anchorRunId: conn.identity.anchorRunId,
            generation: conn.identity.generation,
          }
        : { kind: "shared", agentAddress: frame.agentAddress },
    );

    // Connection may have closed during async verification.
    const currentConn = connections.get(ws);
    if (currentConn === undefined) return;

    if (verdict.accepted) {
      currentConn.send({
        type: "repo.pack.ack",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
      });
    } else {
      currentConn.send({
        type: "repo.pack.reject",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
        reason: verdict.reason,
      });
    }
  }

  /**
   * Bind a per-step workflow-substrate address to a sidecar for the staging
   * window of a multi-step deploy, so `sendPack` can route the step's deploy
   * and asset packs before the deployment-level frame spawns the child.
   *
   * The address is hub-minted and workflow-derived (no per-address key), so
   * it enters the keyless `workflowAddresses` set -- never the challenged
   * `agentAddresses` set -- and is torn down by `unbindStepRoute` once the
   * step's packs land. `handleClose` reclaims it if the sidecar drops
   * mid-stage. Per-step addresses are not runtime-routed (mail, signals, and
   * drains use the deployment address), so the binding is transient: it is
   * never persisted into the reconnect set and never resurrected on
   * reconnect.
   */
  function fenceAllocation(allocationId: string, generation: number): void {
    const existing = allocationFences.get(allocationId);
    if (existing !== undefined && generation < existing) {
      throw new Error(
        `Cannot move allocation ${allocationId} fence backward from ${String(existing)} to ${String(generation)}`,
      );
    }
    allocationFences.set(allocationId, generation);

    const current = allocatedConnections.get(allocationId);
    if (current !== undefined && current.identity.generation !== generation) {
      handleClose(current.ws);
      current.ws.close();
    }

    const waiters = allocationWaiters.get(allocationId);
    if (waiters === undefined) return;
    for (const waiter of [...waiters]) {
      if (waiter.generation === generation) continue;
      clearTimeout(waiter.timer);
      waiters.delete(waiter);
      waiter.reject(
        new Error(
          `Allocation ${allocationId} advanced to generation ${String(generation)}`,
        ),
      );
    }
    if (waiters.size === 0) allocationWaiters.delete(allocationId);
  }

  async function getAllocatedConnection(
    target: AllocatedSidecarTarget,
    use: "readiness" | "routing",
  ): Promise<{ ws: WsHandle; conn: SidecarConnection }> {
    if (allocationFences.get(target.allocationId) !== target.generation) {
      throw new Error(
        `Allocation ${target.allocationId} generation ${String(target.generation)} is not current`,
      );
    }
    const current = allocatedConnections.get(target.allocationId);
    if (
      current === undefined ||
      current.identity.generation !== target.generation
    ) {
      throw new Error(
        `Allocated sidecar is not connected for allocation ${target.allocationId} generation ${String(target.generation)}`,
      );
    }
    if (!(await validateSidecarIdentity(current.identity, use))) {
      if (allocatedConnections.get(target.allocationId) === current) {
        handleClose(current.ws);
        current.ws.close();
      }
      throw new Error(
        `Allocated sidecar identity is no longer current for allocation ${target.allocationId}`,
      );
    }
    if (allocatedConnections.get(target.allocationId) !== current) {
      throw new Error(
        `Allocated sidecar connection changed for allocation ${target.allocationId}`,
      );
    }
    const conn = connections.get(current.ws);
    if (
      conn === undefined ||
      conn.identity.kind !== "allocated" ||
      conn.identity.allocationId !== target.allocationId ||
      conn.identity.generation !== target.generation
    ) {
      throw new Error(
        `Allocated sidecar is not connected for allocation ${target.allocationId}`,
      );
    }
    return { ws: current.ws, conn };
  }

  async function isAllocatedSidecarReady(
    target: AllocatedSidecarTarget,
  ): Promise<boolean> {
    try {
      await getAllocatedConnection(target, "readiness");
      return true;
    } catch {
      return false;
    }
  }

  async function isAllocatedWorkflowActive(
    target: AllocatedSidecarTarget,
  ): Promise<boolean> {
    try {
      const { conn } = await getAllocatedConnection(target, "readiness");
      if (conn.identity.kind !== "allocated") return false;
      return conn.workflowAddresses.has(conn.identity.workflowRunAddress);
    } catch {
      return false;
    }
  }

  async function waitForAllocatedSidecar(
    target: AllocatedSidecarTarget,
    timeoutMs: number,
  ): Promise<void> {
    if (await isAllocatedSidecarReady(target)) return;
    if (allocationFences.get(target.allocationId) !== target.generation) {
      throw new Error(
        `Allocation ${target.allocationId} generation ${String(target.generation)} is not current`,
      );
    }
    if (timeoutMs <= 0) {
      throw new Error(
        `Timed out waiting for allocated sidecar ${target.allocationId}`,
      );
    }

    await new Promise<void>((resolve, reject) => {
      const waiter: AllocationWaiter = {
        generation: target.generation,
        resolve,
        reject,
        timer: setTimeout(() => {
          const current = allocationWaiters.get(target.allocationId);
          current?.delete(waiter);
          if (current?.size === 0) {
            allocationWaiters.delete(target.allocationId);
          }
          reject(
            new Error(
              `Timed out waiting for allocated sidecar ${target.allocationId} generation ${String(target.generation)}`,
            ),
          );
        }, timeoutMs),
      };
      let waiters = allocationWaiters.get(target.allocationId);
      if (waiters === undefined) {
        waiters = new Set();
        allocationWaiters.set(target.allocationId, waiters);
      }
      waiters.add(waiter);
      void notifyAllocationWaiters(target.allocationId);
    });
  }

  function bindStepRoute(stepAddress: string): void {
    const ws =
      addressIndex.get(stepAddress) ?? findSidecarForNewAgent(stepAddress);
    if (ws === undefined) {
      throw new Error(
        `No sidecar available to stage workflow step "${stepAddress}"`,
      );
    }
    const conn = connections.get(ws);
    if (conn === undefined) {
      throw new Error(
        `No sidecar connected to stage workflow step "${stepAddress}"`,
      );
    }
    if (conn.identity.kind !== "shared") {
      throw new Error(
        `Allocated sidecar ${conn.sidecarId} cannot receive ordinary workflow step ${stepAddress}`,
      );
    }
    conn.workflowAddresses.add(stepAddress);
    addressIndex.set(stepAddress, ws);
  }

  async function bindAllocatedStepRoute(
    target: AllocatedSidecarTarget,
    stepAddress: string,
  ): Promise<void> {
    const { ws, conn } = await getAllocatedConnection(target, "routing");
    const existing = addressIndex.get(stepAddress);
    if (existing !== undefined && existing !== ws) {
      throw new Error(
        `Workflow step ${stepAddress} is already routed to another sidecar`,
      );
    }
    conn.workflowAddresses.add(stepAddress);
    addressIndex.set(stepAddress, ws);
  }

  /**
   * Remove a per-step route bound by `bindStepRoute` once the step's packs
   * have landed. Idempotent: an address that was never bound (or already
   * unbound, e.g. by a mid-stage `handleClose`) is a no-op.
   */
  function unbindStepRoute(stepAddress: string): void {
    const ws = addressIndex.get(stepAddress);
    if (ws === undefined) return;
    const conn = connections.get(ws);
    if (conn !== undefined) {
      conn.workflowAddresses.delete(stepAddress);
    }
    addressIndex.delete(stepAddress);
  }

  function unbindAllocatedStepRoute(
    target: AllocatedSidecarTarget,
    stepAddress: string,
  ): void {
    const current = allocatedConnections.get(target.allocationId);
    if (
      current === undefined ||
      current.identity.generation !== target.generation
    ) {
      return;
    }
    if (addressIndex.get(stepAddress) !== current.ws) return;
    connections.get(current.ws)?.workflowAddresses.delete(stepAddress);
    addressIndex.delete(stepAddress);
  }

  // Pack transfers may take longer than session requests due to data volume.
  const PACK_TIMEOUT_MS = requestTimeoutMs * 4;

  function sendPack(
    agentAddress: string,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
    options?: SendPackOptions,
  ): Promise<void> {
    const ws = addressIndex.get(agentAddress);
    if (ws === undefined) {
      return Promise.reject(
        new Error(`No sidecar connected for agent "${agentAddress}"`),
      );
    }
    const conn = connections.get(ws);
    if (conn === undefined) {
      return Promise.reject(
        new Error(`No sidecar connected for agent "${agentAddress}"`),
      );
    }

    if (conn.identity.kind !== "shared") {
      return Promise.reject(
        new Error(
          `Allocated sidecar ${conn.sidecarId} requires allocation-bound pack routing for "${agentAddress}"`,
        ),
      );
    }
    return sendPackOnConnection(
      ws,
      conn,
      agentAddress,
      pack,
      ref,
      commitSha,
      options,
    );
  }

  function sendPackOnConnection(
    ws: WsHandle,
    conn: SidecarConnection,
    agentAddress: string,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
    options?: SendPackOptions,
  ): Promise<void> {
    const transferId = `pack-${++packCounter}`;
    // For the agent-state flow the destination agent and the source repo
    // are the same entity, so `repoId.id === agentAddress`. Asset packs
    // override this with the SOURCE asset's id so audit can correlate
    // the pack back to its hub-side origin.
    const repoId: RepoId = options?.repoId ?? {
      kind: "agent-state",
      id: agentAddress,
    };
    const mountPath = options?.mountPath;

    // Register pending entry before sending frames so that a synchronous
    // repo.pack.ack (e.g. in tests or loopback transports) resolves correctly.
    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        pendingPacks.delete(transferId);
        reject(
          new Error(
            `Pack transfer ${transferId} timed out after ${PACK_TIMEOUT_MS}ms`,
          ),
        );
      }, PACK_TIMEOUT_MS);

      pendingPacks.set(transferId, {
        transferId,
        ws,
        agentAddress,
        repoId,
        resolve,
        reject(error: string) {
          reject(new Error(`Pack rejected: ${error}`));
        },
        timer,
      });

      // Send chunks
      for (const chunk of chunkPack(pack)) {
        conn.send({
          type: "repo.pack.push",
          agentAddress,
          repoId,
          transferId,
          seq: chunk.seq,
          data: chunk.data,
        });
      }

      // Send done
      conn.send({
        type: "repo.pack.done",
        agentAddress,
        repoId,
        transferId,
        ref,
        commitSha,
        ...(mountPath !== undefined ? { mountPath } : {}),
      });
    });
  }

  async function sendPackToAllocation(
    target: AllocatedSidecarTarget,
    agentAddress: string,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
    options?: SendPackOptions,
  ): Promise<void> {
    const { ws, conn } = await getAllocatedConnection(target, "routing");
    if (addressIndex.get(agentAddress) !== ws) {
      throw new Error(
        `Address ${agentAddress} is not routed on allocation ${target.allocationId}`,
      );
    }
    return sendPackOnConnection(
      ws,
      conn,
      agentAddress,
      pack,
      ref,
      commitSha,
      options,
    );
  }

  async function sendWorkflowRunPackToAllocation(
    target: AllocatedSidecarTarget,
    agentAddress: string,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
  ): Promise<void> {
    const { ws, conn } = await getAllocatedConnection(target, "routing");
    if (conn.identity.kind !== "allocated") {
      throw new Error(
        `Allocation ${target.allocationId} resolved to a shared sidecar`,
      );
    }
    if (agentAddress !== conn.identity.workflowRunAddress) {
      throw new Error(
        `Allocation ${target.allocationId} cannot restore unrelated address ${agentAddress}`,
      );
    }
    if (conn.workflowAddresses.has(agentAddress)) {
      throw new Error(
        `Allocation ${target.allocationId} already hosts active workflow ${agentAddress}; refusing to overwrite its run history`,
      );
    }
    return sendPackOnConnection(ws, conn, agentAddress, pack, ref, commitSha, {
      repoId: {
        kind: "workflow-run",
        id: deriveWorkflowRunRepoId(agentAddress),
      },
    });
  }

  function routeMail(
    agentAddress: string,
    rawMessage: string,
    messageId?: string,
    runGrants?: { runId: string; stepGrants: RunGrantsFrame["stepGrants"] },
  ): boolean {
    // Carry the hub-minted messageId on the frame so the sidecar's durable-
    // receipt ack (`mail.inbound.ack`) keys on the same id the hub tracks, and
    // a redelivery replays identical bytes for the downstream RunStarted dedup.
    // Optional: the workflow-trigger and session-conversation callers supply
    // it (they participate in the ack/retry handshake); a caller without a
    // hub-minted id omits it and the delivery is not tracked for redelivery.
    const frame: HubFrame = {
      type: "mail.inbound",
      agentAddress,
      rawMessage,
      ...(messageId !== undefined ? { messageId } : {}),
    };
    const ws = addressIndex.get(agentAddress);
    if (ws !== undefined) {
      const conn = connections.get(ws);
      if (conn !== undefined) {
        conn.send(frame);
        // Track the delivery for redelivery until the sidecar acks its durable
        // inbox write. Only mail carrying a hub-minted messageId participates
        // in the ack handshake; relayed agent-to-agent mail omits it and is
        // delivered fire-and-forget as before. A mail that triggered a workflow
        // run carries the run's grants so redelivery can replay them ahead of
        // the mail.
        if (messageId !== undefined) {
          trackPendingMail(agentAddress, messageId, frame, runGrants);
        }
        return true;
      }
    }

    // If the agent recently disconnected, queue for delivery on reconnect.
    return enqueueForDisconnected(agentAddress, frame);
  }

  function sendRunGrants(
    agentAddress: string,
    runId: string,
    stepGrants: RunGrantsFrame["stepGrants"],
  ): boolean {
    const frame: HubFrame = {
      type: "run.grants",
      agentAddress,
      runId,
      stepGrants,
    };

    const ws = addressIndex.get(agentAddress);
    if (ws !== undefined) {
      const conn = connections.get(ws);
      if (conn !== undefined) {
        conn.send(frame);
        return true;
      }
    }

    // Mirror routeMail: if the address has a live disconnect queue, ride it
    // so a run.grants issued in the window between deploy and the first
    // reconnect survives the same way the dispatching trigger mail does.
    // A queue exists only while the deployment address is still on
    // agentAddresses (pre-first-reconnect); after a challenged reconnect it
    // moves to workflowAddresses, which handleClose leaves unqueued because
    // that generation's in-flight run state is reconstructed sidecar-locally.
    // Returning without enqueueing there is correct; enqueueing is what keeps
    // grants and mail from diverging in the pre-reconnect window.
    return enqueueForDisconnected(agentAddress, frame);
  }

  async function sendWorkflowRunDispatchToAllocation(
    target: AllocatedSidecarTarget,
    agentAddress: string,
    runId: string,
    stepGrants: RunGrantsFrame["stepGrants"],
    rawMessage: string,
    messageId: string,
  ): Promise<void> {
    const { ws, conn } = await getAllocatedConnection(target, "routing");
    if (addressIndex.get(agentAddress) !== ws) {
      throw new Error(
        `Address ${agentAddress} is not routed on allocation ${target.allocationId}`,
      );
    }
    const runGrants = { runId, stepGrants };
    conn.send({
      type: "run.grants",
      agentAddress,
      runId,
      stepGrants,
    });
    const frame: HubFrame = {
      type: "mail.inbound",
      agentAddress,
      rawMessage,
      messageId,
    };
    conn.send(frame);
    trackPendingMail(agentAddress, messageId, frame, runGrants, target);
  }

  async function handleDeployAck(
    ws: WsHandle,
    frame: AgentDeployAckFrame,
  ): Promise<void> {
    const req = pendingDeploys.get(frame.agentAddress);
    if (req === undefined) {
      logger.warn`Received agent.deploy.ack for "${frame.agentAddress}" with no pending deploy`;
      return;
    }
    if (req.ws !== ws) return;

    if (events.listenerCount("agent.deploy.ack") > 0) {
      try {
        const identity = connections.get(ws)?.identity;
        await events.emitAndAwait("agent.deploy.ack", {
          agentAddress: frame.agentAddress,
          publicKey: frame.publicKey,
          ...(identity?.kind === "allocated"
            ? {
                allocated: {
                  allocationId: identity.allocationId,
                  anchorRunId: identity.anchorRunId,
                  generation: identity.generation,
                },
              }
            : {}),
        });
      } catch (err) {
        rejectDeployPending(
          req,
          `Failed to store public key: ${err instanceof Error ? err.message : String(err)}`,
        );
        return;
      }
    }
    resolveDeployPending(req, frame.publicKey);
  }

  function resolveDeployPending(req: PendingDeploy, publicKey: string): void {
    if (pendingDeploys.get(req.agentAddress) !== req) return;
    clearTimeout(req.timer);
    pendingDeploys.delete(req.agentAddress);
    req.resolve(publicKey);
  }

  function rejectDeployPending(req: PendingDeploy, error: string): void {
    if (pendingDeploys.get(req.agentAddress) !== req) return;
    clearTimeout(req.timer);
    pendingDeploys.delete(req.agentAddress);
    req.reject(error);
  }

  function rejectDeployPendingFromFrame(
    ws: WsHandle,
    agentAddress: string,
    error: string,
  ): void {
    const req = pendingDeploys.get(agentAddress);
    if (req === undefined || req.ws !== ws) return;
    rejectDeployPending(req, error);
  }

  async function sendAgentDeploy(
    agentAddress: string,
    harnessConfig: HarnessConfig,
    workflow?: AgentDeployFrame["workflow"],
  ): Promise<{ publicKey: string }> {
    if (hubPublicKeyHex === undefined) {
      throw new Error("Hub signing key is required for agent deployment");
    }
    const ws =
      addressIndex.get(agentAddress) ?? findSidecarForNewAgent(agentAddress);
    if (ws === undefined) {
      throw new Error(`No sidecar available for agent "${agentAddress}"`);
    }
    const conn = connections.get(ws);
    if (conn === undefined) {
      throw new Error(`No sidecar connected for agent "${agentAddress}"`);
    }
    if (conn.identity.kind !== "shared") {
      throw new Error(
        `Allocated sidecar ${conn.sidecarId} requires allocation-bound deploy routing`,
      );
    }
    return sendAgentDeployOnConnection(
      ws,
      conn,
      agentAddress,
      harnessConfig,
      workflow,
    );
  }

  function sendAgentDeployOnConnection(
    ws: WsHandle,
    conn: SidecarConnection,
    agentAddress: string,
    harnessConfig: HarnessConfig,
    workflow?: AgentDeployFrame["workflow"],
  ): Promise<{ publicKey: string }> {
    if (hubPublicKeyHex === undefined) {
      throw new Error("Hub signing key is required for agent deployment");
    }

    if (pendingDeploys.has(agentAddress)) {
      throw new Error(`Deploy already in progress for agent "${agentAddress}"`);
    }

    const addressSet =
      conn.identity.kind === "allocated"
        ? conn.workflowAddresses
        : conn.agentAddresses;
    addressSet.add(agentAddress);
    addressIndex.set(agentAddress, ws);

    return new Promise<{ publicKey: string }>((resolve, reject) => {
      const timer = setTimeout(() => {
        pendingDeploys.delete(agentAddress);
        if (addressIndex.get(agentAddress) === ws) {
          addressSet.delete(agentAddress);
          addressIndex.delete(agentAddress);
        }
        reject(
          new Error(
            `Deploy of "${agentAddress}" timed out after ${requestTimeoutMs}ms`,
          ),
        );
      }, requestTimeoutMs);

      pendingDeploys.set(agentAddress, {
        agentAddress,
        ws,
        resolve(publicKey) {
          resolve({ publicKey });
        },
        reject(error: string) {
          if (addressIndex.get(agentAddress) === ws) {
            addressSet.delete(agentAddress);
            addressIndex.delete(agentAddress);
          }
          reject(new Error(error));
        },
        timer,
      });

      conn.send({
        type: "agent.deploy",
        agentAddress,
        agentId: harnessConfig.agentId,
        config: harnessConfig,
        hubPublicKey: hubPublicKeyHex,
        ...(workflow !== undefined ? { workflow } : {}),
      });
    });
  }

  async function sendAgentDeployToAllocation(
    target: AllocatedSidecarTarget,
    agentAddress: string,
    harnessConfig: HarnessConfig,
    workflow?: AgentDeployFrame["workflow"],
  ): Promise<{ publicKey: string }> {
    const { ws, conn } = await getAllocatedConnection(target, "routing");
    if (conn.identity.kind !== "allocated") {
      throw new Error(
        `Allocation ${target.allocationId} resolved to a shared sidecar`,
      );
    }
    if (agentAddress !== conn.identity.workflowRunAddress) {
      throw new Error(
        `Allocation ${target.allocationId} cannot deploy unrelated address ${agentAddress}`,
      );
    }
    const existing = addressIndex.get(agentAddress);
    if (existing !== undefined && existing !== ws) {
      throw new Error(
        `Deployment ${agentAddress} is already routed to another sidecar`,
      );
    }
    return sendAgentDeployOnConnection(
      ws,
      conn,
      agentAddress,
      harnessConfig,
      workflow,
    );
  }

  /**
   * Provision one step of a multi-step deploy on the sidecar WITHOUT
   * spawning: the sidecar initializes the step's agent-state repo and
   * records the hub key, so the follow-up deploy pack applies into a repo
   * and verifies against the recorded key -- but no supervisor or child is
   * constructed. The deployment-level workflow frame, sent once after every
   * step is provisioned, spawns the child.
   *
   * The step address must already be bound via `bindStepRoute`, which
   * resolves and records the sidecar; this reuses that route rather than
   * touching `agentAddresses`. Waits for the sidecar's `agent.deploy.ack`
   * so the caller can safely deliver the deploy pack afterward. On failure
   * the caller owns tearing the route down via `unbindStepRoute`.
   */
  function sendProvisionStep(
    agentAddress: string,
    harnessConfig: HarnessConfig,
  ): Promise<void> {
    const ws = addressIndex.get(agentAddress);
    if (ws === undefined) {
      throw new Error(
        `Step route for "${agentAddress}" is not bound; call bindStepRoute before provisioning`,
      );
    }
    const conn = connections.get(ws);
    if (conn === undefined) {
      throw new Error(`No sidecar connected for agent "${agentAddress}"`);
    }
    if (conn.identity.kind !== "shared") {
      throw new Error(
        `Allocated sidecar ${conn.sidecarId} requires allocation-bound step provisioning`,
      );
    }
    return sendProvisionStepOnConnection(ws, conn, agentAddress, harnessConfig);
  }

  function sendProvisionStepOnConnection(
    ws: WsHandle,
    conn: SidecarConnection,
    agentAddress: string,
    harnessConfig: HarnessConfig,
  ): Promise<void> {
    if (hubPublicKeyHex === undefined) {
      throw new Error("Hub signing key is required for step provisioning");
    }
    if (pendingDeploys.has(agentAddress)) {
      throw new Error(`Deploy already in progress for agent "${agentAddress}"`);
    }

    const hubKey = hubPublicKeyHex;
    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        pendingDeploys.delete(agentAddress);
        reject(
          new Error(
            `Step provision of "${agentAddress}" timed out after ${requestTimeoutMs}ms`,
          ),
        );
      }, requestTimeoutMs);
      // The sidecar's `agent.deploy.ack` resolves this through
      // `resolveDeployPending` -> `req.resolve()`. The per-step address is
      // workflow-derived and records no hub-side key, so the ack's public
      // key is not needed and this resolves void.
      pendingDeploys.set(agentAddress, {
        agentAddress,
        ws,
        resolve(_publicKey) {
          resolve();
        },
        reject(error: string) {
          reject(new Error(error));
        },
        timer,
      });

      conn.send({
        type: "agent.deploy",
        agentAddress,
        agentId: harnessConfig.agentId,
        config: harnessConfig,
        hubPublicKey: hubKey,
        provisionStep: true,
      });
    });
  }

  async function sendProvisionStepToAllocation(
    target: AllocatedSidecarTarget,
    agentAddress: string,
    harnessConfig: HarnessConfig,
  ): Promise<void> {
    const { ws, conn } = await getAllocatedConnection(target, "routing");
    if (addressIndex.get(agentAddress) !== ws) {
      throw new Error(
        `Step route ${agentAddress} is not bound to allocation ${target.allocationId}`,
      );
    }
    return sendProvisionStepOnConnection(ws, conn, agentAddress, harnessConfig);
  }

  function findSidecarForNewAgent(_agentAddress: string): WsHandle | undefined {
    for (const [ws, conn] of connections) {
      if (conn.identity.kind === "shared") return ws;
    }
    return undefined;
  }

  function sendProbe(args: SendProbeArgs): Promise<WorkflowProbeResult> {
    // Select any connected sidecar, exactly as the pre-deploy window does.
    // A probe runs in the sidecar's pre-deploy state, so it targets a
    // connection, not an address -- `findSidecarForNewAgent` ignores its
    // argument and returns the first connection. Throw immediately on an empty
    // registry rather than register a probe that could only time out.
    const ws = findSidecarForNewAgent("");
    if (ws === undefined) {
      return Promise.reject(
        new Error("No sidecar available to probe workflow"),
      );
    }
    const conn = connections.get(ws);
    if (conn === undefined) {
      return Promise.reject(
        new Error("No sidecar connected to probe workflow"),
      );
    }

    const requestId = nextRequestId();

    return new Promise<WorkflowProbeResult>((resolve, reject) => {
      const timer = setTimeout(() => {
        pendingProbes.delete(requestId);
        reject(
          new Error(`Probe ${requestId} timed out after ${probeTimeoutMs}ms`),
        );
      }, probeTimeoutMs);

      pendingProbes.set(requestId, {
        requestId,
        ws,
        resolve,
        reject(error: string) {
          reject(new Error(error));
        },
        timer,
      });

      conn.send({
        type: "workflow.probe.request",
        requestId,
        source: args.source,
        closure: args.closure,
        entry: args.entry,
        ...(args.assets !== undefined ? { assets: args.assets } : {}),
      });
    });
  }

  function sendAgentUndeploy(
    agentAddress: string,
    reason: string,
  ): Promise<void> {
    const ws = addressIndex.get(agentAddress);
    if (ws === undefined) {
      return Promise.reject(
        new Error(`No sidecar connected for agent "${agentAddress}"`),
      );
    }
    const conn = connections.get(ws);
    if (conn === undefined) {
      return Promise.reject(
        new Error(`No sidecar connected for agent "${agentAddress}"`),
      );
    }

    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        pendingUndeploys.delete(agentAddress);
        removeAgentAddress(ws, agentAddress);
        reject(
          new Error(
            `Undeploy of "${agentAddress}" timed out after ${requestTimeoutMs}ms`,
          ),
        );
      }, requestTimeoutMs);

      pendingUndeploys.set(agentAddress, {
        agentAddress,
        ws,
        resolve() {
          removeAgentAddress(ws, agentAddress);
          resolve();
        },
        reject(error: string) {
          removeAgentAddress(ws, agentAddress);
          reject(new Error(error));
        },
        timer,
      });

      conn.send({
        type: "agent.undeploy",
        agentAddress,
        reason,
      });
    });
  }

  function removeAgentAddress(ws: WsHandle, agentAddress: string): void {
    addressIndex.delete(agentAddress);
    const conn = connections.get(ws);
    if (conn !== undefined) {
      conn.agentAddresses.delete(agentAddress);
    }
  }

  function dispatchToSubscribers(agentAddress: string, event: unknown): void {
    const subs = agentSubscribers.get(agentAddress);
    if (subs === undefined) return;
    for (const cb of [...subs]) {
      try {
        cb(event);
      } catch (err) {
        logger.warn`Agent subscriber threw: ${err instanceof Error ? err.message : String(err)}`;
      }
    }
  }

  function subscribeAgent(
    agentAddress: string,
    callback: (event: unknown) => void,
  ): () => void {
    let subs = agentSubscribers.get(agentAddress);
    if (subs === undefined) {
      subs = new Set();
      agentSubscribers.set(agentAddress, subs);
    }
    subs.add(callback);
    return () => {
      const current = agentSubscribers.get(agentAddress);
      if (current === undefined) return;
      current.delete(callback);
      if (current.size === 0) {
        agentSubscribers.delete(agentAddress);
      }
    };
  }

  function getConnectedSidecars(): string[] {
    return Array.from(connections.values()).map((c) => c.sidecarId);
  }

  function getRoutableAddresses(): string[] {
    return Array.from(addressIndex.keys());
  }

  function getConnectorState(
    agentAddress: string,
  ): ConnectorThreadState | null {
    return connectorStates.get(agentAddress) ?? null;
  }

  async function sendSourcesUpdate(
    agentAddress: string,
    sources: InferenceSource[],
    defaultSource: string,
  ): Promise<void> {
    await sendRequest(agentAddress, (requestId) => ({
      type: "sources.update",
      requestId,
      agentAddress,
      sources,
      defaultSource,
    }));
  }

  async function sendCredentialsUpdate(
    agentAddress: string,
    delivery: CredentialDelivery,
  ): Promise<void> {
    await sendRequest(agentAddress, (requestId) => ({
      type: "credentials.update",
      requestId,
      agentAddress,
      delivery,
    }));
  }

  function sendSyncRequest(agentAddress: string): void {
    const ws = addressIndex.get(agentAddress);
    if (ws === undefined) {
      throw new Error(`No sidecar connected for agent "${agentAddress}"`);
    }
    const conn = connections.get(ws);
    if (conn === undefined) {
      throw new Error(`No sidecar connected for agent "${agentAddress}"`);
    }

    const transferId = `sync-${++packCounter}`;
    conn.send({
      type: "sync.request",
      agentAddress,
      transferId,
    });
  }

  function sendSignalDeliver(opts: {
    agentAddress: string;
    runId: string;
    signalName: string;
    signalId: string;
    payload: unknown;
  }): void {
    const ws = addressIndex.get(opts.agentAddress);
    if (ws === undefined) {
      throw new Error(
        `No sidecar connected for deployment "${opts.agentAddress}"`,
      );
    }
    const conn = connections.get(ws);
    if (conn === undefined) {
      throw new Error(
        `No sidecar connected for deployment "${opts.agentAddress}"`,
      );
    }
    conn.send({
      type: "signal.deliver",
      agentAddress: opts.agentAddress,
      runId: opts.runId,
      signalName: opts.signalName,
      signalId: opts.signalId,
      payload: opts.payload,
    });
  }

  async function sendSignalDeliverToAllocation(
    target: AllocatedSidecarTarget,
    opts: {
      agentAddress: string;
      runId: string;
      signalName: string;
      signalId: string;
      payload: unknown;
    },
  ): Promise<void> {
    const { ws, conn } = await getAllocatedConnection(target, "routing");
    if (addressIndex.get(opts.agentAddress) !== ws) {
      throw new Error(
        `Address ${opts.agentAddress} is not routed on allocation ${target.allocationId}`,
      );
    }
    conn.send({ type: "signal.deliver", ...opts });
  }

  function sendDrain(opts: { agentAddress: string; deadlineMs: number }): void {
    const ws = addressIndex.get(opts.agentAddress);
    if (ws === undefined) {
      throw new Error(
        `No sidecar connected for deployment "${opts.agentAddress}"`,
      );
    }
    const conn = connections.get(ws);
    if (conn === undefined) {
      throw new Error(
        `No sidecar connected for deployment "${opts.agentAddress}"`,
      );
    }
    conn.send({
      type: "drain.deliver",
      agentAddress: opts.agentAddress,
      deadlineMs: opts.deadlineMs,
    });
  }

  return {
    handleOpen,
    handleMessage,
    handleClose,
    routeMail,
    sendRunGrants,
    sendAgentDeploy,
    sendProbe,
    sendAgentUndeploy,
    sendSourcesUpdate,
    sendCredentialsUpdate,
    sendPack,
    sendPackToAllocation,
    sendWorkflowRunPackToAllocation,
    fenceAllocation,
    waitForAllocatedSidecar,
    isAllocatedSidecarReady,
    isAllocatedWorkflowActive,
    sendAgentDeployToAllocation,
    bindStepRoute,
    bindAllocatedStepRoute,
    unbindStepRoute,
    unbindAllocatedStepRoute,
    sendProvisionStep,
    sendProvisionStepToAllocation,
    sendWorkflowRunDispatchToAllocation,
    sendSyncRequest,
    sendSignalDeliver,
    sendSignalDeliverToAllocation,
    sendDrain,
    subscribeAgent,
    dispatchAgentEvent: dispatchToSubscribers,
    getConnectedSidecars,
    getRoutableAddresses,
    getConnectorState,
    events,
  };
}

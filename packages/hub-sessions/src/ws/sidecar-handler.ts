// Hub-side websocket handler for sidecar connections.
//
// Accepts websocket upgrades, processes register frames, maintains a routing
// table of agentAddress → sidecar connection, and dispatches frames between
// sidecars and the hub's internal systems.

import { getLogger } from "@intx/log";
import { chunkPack, createPackReceiver } from "@intx/pack-transport";
import {
  base64Decode,
  deriveMessageId,
  deriveWorkflowRunId,
  isRunAddress,
} from "@intx/types";
import type { GrantWalkSnapshot } from "@intx/types";
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
import {
  PendingTracker,
  type PendingEntry,
  type WsHandle,
} from "./pending-tracker";

const logger = getLogger(["hub", "ws", "sidecar"]);

/**
 * A deploy-frame send failure, tagged with whether the `agent.deploy` frame
 * reached the wire. `frameSent: false` means the send was refused before
 * `conn.send` (a guard failed, or the send threw synchronously) -- the deploy
 * provably never started, so a caller may safely roll back anything it staged.
 * `frameSent: true` means the frame was sent and the failure came afterward (ack
 * timeout, sidecar disconnect), so the sidecar may hold a live agent.
 */
export interface DeployFrameFailure extends Error {
  readonly frameSent: boolean;
}

function deployFrameFailure(
  message: string,
  frameSent: boolean,
): DeployFrameFailure {
  return Object.assign(new Error(message), { frameSent });
}

export function isDeployFrameFailure(err: unknown): err is DeployFrameFailure {
  return (
    err instanceof Error &&
    "frameSent" in err &&
    typeof err.frameSent === "boolean"
  );
}

export type SidecarConnection = {
  sidecarId: string;
  identity: SidecarAuthIdentity;
  agentAddresses: Set<string>;
  // Workflow-substrate deployment run addresses (`run_<hex>@domain`) this
  // connection hosts. Kept separate from the legacy `agentAddresses` set so
  // workflow route teardown and recovery remain explicit. `handleClose` cleans
  // both sets out of `addressIndex`.
  workflowAddresses: Set<string>;
  send(frame: HubFrame): void;
};

/**
 * Whether this connection owns `address` for routing/lifecycle purposes.
 * The legacy and workflow sets remain physically distinct, but ownership
 * readers -- pack-transfer authorization,
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
  if (conn.identity.kind !== "allocated") return false;
  if (agentAddress !== conn.identity.workflowRunAddress) {
    return false;
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
 * probed workflow, the inert grant set derived from it, the un-flattened grant
 * walk snapshot the set is derived from, and the projection's content hash.
 */
export type WorkflowProbeResult = {
  projection: WorkflowProjectionDefinition;
  grants: string[];
  grantWalkSnapshot: GrantWalkSnapshot;
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
   * and ride the same reconnect flush. After an authenticated reconnect the
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
  sendAgentUndeploy(agentAddress: string, reason: string): Promise<void>;
  sendSourcesUpdate(
    agentAddress: string,
    sources: InferenceSource[],
    defaultSource: string,
  ): Promise<void>;
  sendCredentialsUpdate(
    agentAddress: string,
    delivery: CredentialDelivery,
    revoke?: string[],
  ): Promise<void>;
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
  /**
   * Remove an exact generation's fence after its durable owner becomes
   * terminal. Durable identity validation rejects later stale reconnects.
   */
  retireAllocation(target: AllocatedSidecarTarget): void;
  /** Resolve once the exact authenticated allocation generation is connected. */
  waitForAllocatedSidecar(
    target: AllocatedSidecarTarget,
    timeoutMs: number,
  ): Promise<void>;
  /** Check exact allocated readiness without parking a reconciliation worker. */
  isAllocatedSidecarReady(target: AllocatedSidecarTarget): Promise<boolean>;
  /** Check whether the exact generation already hosts its workflow supervisor. */
  isAllocatedWorkflowActive(target: AllocatedSidecarTarget): Promise<boolean>;
  /** Probe a workflow on the exact provisioned allocation generation. */
  sendProbeToAllocation(
    target: AllocatedSidecarTarget,
    args: SendProbeArgs,
  ): Promise<WorkflowProbeResult>;
  /** Close an exact provisioned connection before changing its durable owner. */
  disconnectAllocation(target: AllocatedSidecarTarget): void;
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
  /** Deliver an idempotent signal to the exact provisioned generation. */
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
  validateSidecarIdentity: (
    identity: SidecarAuthIdentity,
    use: "registration" | "readiness" | "routing",
  ) => Promise<boolean>;
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
  /** Query handlers the wire layer issues during frame processing. */
  lookups?: SidecarLookups;
};

// Re-exported so existing consumers keep importing the handle type from the
// router module; the definition now lives in `pending-tracker.ts`, which also
// operates on it.
export type { WsHandle };

const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
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
    probeTimeoutMs = DEFAULT_PROBE_TIMEOUT_MS,
    hubPublicKey: hubPublicKeyHex,
    authenticateSidecar,
    validateSidecarIdentity,
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
      identity: SidecarAuthIdentity;
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
  // requestId → pending promise (resolved by session.ack, rejected by
  // session.error). `PendingTracker` owns the register/timeout/settle/sweep
  // lifecycle shared by all five pending round-trips below; each entry's
  // resolve/reject closures carry the per-round-trip cleanup.
  const pendingRequests = new PendingTracker<string>();
  // agentAddress → pending deploy promise (matched by agent.deploy.ack/agent.error)
  const pendingDeploys = new PendingTracker<string, string>();
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
    /** Present for a durable trigger pinned to a provisioned allocation. */
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

  // transferId → pending pack transfer (resolved by repo.pack.ack, rejected
  // by repo.pack.reject). The entry carries the send-site agentAddress and
  // repoId so an ack/reject is honored only when it comes from the
  // connection that owns the transfer for the same repo.
  type PackTransferMeta = { agentAddress: string; repoId: RepoId };
  const pendingPacks = new PendingTracker<string, void, PackTransferMeta>();
  let packCounter = 0;

  // agentAddress → pending undeploy (resolved by agent.undeploy.ack)
  const pendingUndeploys = new PendingTracker<string>();

  // requestId → pending workflow probe (resolved by workflow.probe.result,
  // rejected by workflow.probe.error). Result-carrying, unlike the other
  // trackers (which resolve void): a probe returns the sidecar's inert
  // projection + grant set + wire hash. Keyed on requestId alone -- the
  // probe runs in the sidecar's pre-deploy state and enters no address map,
  // so `handleClose`'s ws-keyed sweep is its ONLY disconnect cleanup.
  const pendingProbes = new PendingTracker<string, WorkflowProbeResult>();

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
  // run out of band. Every other frame establishes or reads routing, or carries
  // an inbound payload whose order matters, so it queues. The exhaustive switch
  // + assertNever makes adding a SidecarFrame variant without classifying it a
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
  // void. Never awaits a promise that resolves on a later same-ws frame.
  function dispatchFrame(
    ws: WsHandle,
    frame: SidecarFrame,
  ): void | Promise<void> {
    const registeredIdentity = connections.get(ws)?.identity;
    if (
      registeredIdentity?.kind === "probe" &&
      frame.type !== "register" &&
      frame.type !== "reconnect" &&
      frame.type !== "ping" &&
      frame.type !== "workflow.probe.result" &&
      frame.type !== "workflow.probe.error"
    ) {
      logger.warn`Rejected ${frame.type} from probe sidecar ${registeredIdentity.sidecarId}`;
      handleClose(ws);
      ws.close();
      return;
    }
    switch (frame.type) {
      case "register": {
        const agentAddresses = frame.agentAddresses;
        return authenticateHandshake(ws, frame, (identity) =>
          handleRegister(ws, identity, agentAddresses),
        );
      }
      case "reconnect": {
        const agentAddresses = frame.agentAddresses;
        return authenticateHandshake(ws, frame, (identity) =>
          handleReconnect(ws, identity, agentAddresses),
        );
      }
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
      case "mail.outbound": {
        const conn = connections.get(ws);
        if (conn === undefined) return;
        if (!connOwnsAddress(conn, frame.senderAddress)) {
          logger.warn`Dropping mail.outbound from ${frame.senderAddress}: not registered to this sidecar`;
          return;
        }
        if (frame.delivered !== true) {
          return handleMailOutbound(frame.rawMessage, frame.recipients);
        }
        if (lookups.persistMail) {
          return handleMailPersist(
            lookups.persistMail,
            frame.rawMessage,
            frame.senderAddress,
            frame.recipients,
          );
        }
        logger.warn`Dropping delivered mail.outbound frame: no persistMail lookup configured`;
        return;
      }
      case "agent.event": {
        const conn = connections.get(ws);
        if (conn === undefined) return;
        if (!connOwnsAddress(conn, frame.agentAddress)) {
          logger.warn`Dropping agent.event for ${frame.agentAddress}: not registered to this sidecar`;
          return;
        }
        events.emit("agent.event", {
          agentAddress: frame.agentAddress,
          sessionId: frame.sessionId,
          event: frame.event,
        });
        dispatchToSubscribers(frame.agentAddress, frame.event);
        return;
      }
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
        pendingRequests.resolve(frame.requestId);
        return;
      case "session.error":
        pendingRequests.reject(frame.requestId, frame.error);
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
        resolveProbe(ws, frame.requestId, {
          projection: frame.projection,
          grants: frame.grants,
          grantWalkSnapshot: frame.grantWalkSnapshot,
          wireHash: frame.wireHash,
        });
        return;
      case "workflow.probe.error":
        rejectProbe(ws, frame.requestId, frame.error);
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
    identity: SidecarAuthIdentity,
    agentAddresses: string[],
  ): Promise<void> {
    if (allocationFences.get(identity.allocationId) !== identity.generation) {
      logger.warn`Rejected allocated sidecar ${identity.sidecarId}: allocation ${identity.allocationId} generation ${String(identity.generation)} is not fenced as current`;
      ws.close();
      return;
    }
    if (identity.kind === "probe" && agentAddresses.length > 0) {
      logger.warn`Rejected probe sidecar ${identity.sidecarId}: probe ${identity.allocationId} claimed workflow addresses`;
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
      if (
        identity.kind !== "allocated" ||
        (!alreadyOwned && address !== identity.workflowRunAddress)
      ) {
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
      conn.identity.kind !== identity.kind ||
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
    // Reconcile a reconnecting deployment's credentials, closing the offline
    // window: a credential revoked, deleted, or rotated while the sidecar was
    // disconnected is applied to the child now. Fire-and-forget so
    // registration is not blocked; the lookup no-ops for a run that persisted
    // no credential refs.
    const resyncCredentials = lookups.resyncCredentials;
    if (resyncCredentials !== undefined) {
      for (const address of newlyRoutedAddresses) {
        if (!isRunAddress(address)) continue;
        resyncCredentials(address);
      }
    }
    logger.info`Provisioned sidecar ${identity.sidecarId} registered for allocation ${identity.allocationId} generation ${String(identity.generation)}`;
    await notifyAllocationWaiters(identity.allocationId);
    if (identity.kind === "allocated") {
      events.emit("sidecar.allocated.connected", {
        allocationId: identity.allocationId,
        generation: identity.generation,
      });
    }
  }

  async function handleRegister(
    ws: WsHandle,
    identity: SidecarAuthIdentity,
    agentAddresses: string[],
  ): Promise<void> {
    await handleAllocatedRegister(ws, identity, agentAddresses);
  }

  async function handleReconnect(
    ws: WsHandle,
    identity: SidecarAuthIdentity,
    agentAddresses: string[],
  ): Promise<void> {
    await handleAllocatedRegister(ws, identity, agentAddresses);
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
      // This does NOT let mail mutate a run's authorization. First delivery
      // reserves and commits the run's grants (the mail IS the trigger);
      // every later delivery only RE-READS the current committed grants
      // (`loadCommittedRunGrants`) and re-asserts them ahead of the dispatch.
      // The committed rows already carry any standing-approval change (an
      // approve/reject-with-`always` resolution mutates them through its own
      // path), so this re-send is idempotent -- it re-establishes the run's
      // current floor on the sidecar, self-healing a `grants.json` a sidecar
      // may have lost, and never overwrites it with anything staler.
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
        connectorStates.delete(addr);
        // Retain un-acked workflow trigger mail across the disconnect for the
        // same reason as the session loop above -- an authenticated reconnect
        // redelivers it. This is un-acked TRIGGER mail, distinct from the
        // deployment's in-flight run state (reconstructed sidecar-locally); the
        // "no disconnect queue" note above is about that run state, not this.
        retainPendingMailForAddress(addr);
      }
    }
    const current = allocatedConnections.get(conn.identity.allocationId);
    if (current?.ws === ws) {
      allocatedConnections.delete(conn.identity.allocationId);
      if (conn.identity.kind === "allocated") {
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

    // Reject any in-flight requests that were sent to this sidecar. Each
    // entry's reject closure runs its own per-site cleanup (the deploy and
    // undeploy closures roll routing back), exactly as a frame-error
    // rejection would.
    pendingRequests.rejectAllForWs(
      ws,
      `Sidecar ${conn.sidecarId} disconnected`,
    );
    // Reject every deploy issued on this socket, including allocated
    // workflow deployments stored in `workflowAddresses` rather than
    // `agentAddresses`.
    pendingDeploys.rejectAllForWs(ws, `Sidecar ${conn.sidecarId} disconnected`);
    // Reject any in-flight pack transfers for this sidecar.
    pendingPacks.rejectAllForWs(ws, `Sidecar ${conn.sidecarId} disconnected`);
    // Reject any in-flight undeploys for this sidecar.
    pendingUndeploys.rejectAllForWs(
      ws,
      `Sidecar ${conn.sidecarId} disconnected`,
    );
    // Reject any in-flight probes sent to this sidecar. A probe never enters
    // the address maps, so this ws-keyed sweep is its ONLY disconnect cleanup:
    // without it a probe whose sidecar drops mid-flight would hang until its
    // own timeout instead of failing fast on the disconnect.
    pendingProbes.rejectAllForWs(ws, `Sidecar ${conn.sidecarId} disconnected`);

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
      pendingRequests.register(
        requestId,
        ws,
        {
          timeoutMs: requestTimeoutMs,
          timeoutMessage: `Request ${requestId} timed out after ${requestTimeoutMs}ms`,
          resolve,
          reject(error: string) {
            reject(new Error(error));
          },
        },
        undefined,
      );

      conn.send(frame);
    });
  }

  function packResponseMatches(
    entry: PendingEntry<string, void, PackTransferMeta>,
    ws: WsHandle,
    frame: PackAckFrame | PackRejectFrame,
  ): boolean {
    return (
      entry.ws === ws &&
      entry.meta.agentAddress === frame.agentAddress &&
      entry.meta.repoId.kind === frame.repoId.kind &&
      entry.meta.repoId.id === frame.repoId.id
    );
  }

  function resolvePackPending(ws: WsHandle, frame: PackAckFrame): void {
    const entry = pendingPacks.get(frame.transferId);
    if (entry === undefined) return;
    if (!packResponseMatches(entry, ws, frame)) {
      logger.warn`Ignoring repo.pack.ack for transfer ${frame.transferId} from a connection that does not own the pending transfer`;
      return;
    }
    pendingPacks.resolve(frame.transferId);
  }

  function rejectPackPending(ws: WsHandle, frame: PackRejectFrame): void {
    const entry = pendingPacks.get(frame.transferId);
    if (entry === undefined) return;
    if (!packResponseMatches(entry, ws, frame)) {
      logger.warn`Ignoring repo.pack.reject for transfer ${frame.transferId} from a connection that does not own the pending transfer`;
      return;
    }
    // Surface the receiver's specific cause when it carried one, so the awaiting
    // push sees "corrupt: <detail>" rather than only the coarse reason. The
    // "Pack rejected:" prefix is applied here rather than in the entry's
    // reject closure because a TIMEOUT rejection must not carry it.
    pendingPacks.reject(
      frame.transferId,
      `Pack rejected: ${
        frame.detail !== undefined
          ? `${frame.reason}: ${frame.detail}`
          : frame.reason
      }`,
    );
  }

  function resolveUndeployPending(ws: WsHandle, agentAddress: string): void {
    const req = pendingUndeploys.get(agentAddress);
    if (req === undefined) {
      logger.warn`Received agent.undeploy.ack for "${agentAddress}" with no pending undeploy`;
      return;
    }
    if (req.ws !== ws) return;
    pendingUndeploys.resolve(agentAddress);
  }

  function rejectUndeployPending(
    ws: WsHandle,
    agentAddress: string,
    error: string,
  ): void {
    const req = pendingUndeploys.get(agentAddress);
    if (req === undefined) return;
    if (req.ws !== ws) return;
    pendingUndeploys.reject(agentAddress, error);
  }

  function resolveProbe(
    ws: WsHandle,
    requestId: string,
    result: WorkflowProbeResult,
  ): void {
    const req = pendingProbes.get(requestId);
    if (req === undefined || req.ws !== ws) return;
    pendingProbes.resolve(requestId, result);
  }

  function rejectProbe(ws: WsHandle, requestId: string, error: string): void {
    const req = pendingProbes.get(requestId);
    if (req === undefined || req.ws !== ws) return;
    pendingProbes.reject(requestId, error);
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
    if (conn.identity.kind !== "allocated") return;

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
    if (conn.identity.kind !== "allocated") return;
    const identity = conn.identity;

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
      {
        kind: "allocated",
        agentAddress: frame.agentAddress,
        allocationId: identity.allocationId,
        anchorRunId: identity.anchorRunId,
        generation: identity.generation,
      },
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
   * The address is Hub-minted and workflow-derived, so it enters the
   * `workflowAddresses` set rather than the legacy `agentAddresses` set and is
   * torn down by `unbindStepRoute` once the
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

  function retireAllocation(target: AllocatedSidecarTarget): void {
    if (allocationFences.get(target.allocationId) !== target.generation) return;

    disconnectAllocation(target);
    allocationFences.delete(target.allocationId);

    const waiters = allocationWaiters.get(target.allocationId);
    if (waiters === undefined) return;
    allocationWaiters.delete(target.allocationId);
    for (const waiter of waiters) {
      clearTimeout(waiter.timer);
      waiter.reject(
        new Error(
          `Allocation ${target.allocationId} generation ${String(target.generation)} retired`,
        ),
      );
    }
  }

  async function getProvisionedConnection(
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
      conn.identity.allocationId !== target.allocationId ||
      conn.identity.generation !== target.generation
    ) {
      throw new Error(
        `Allocated sidecar is not connected for allocation ${target.allocationId}`,
      );
    }
    return { ws: current.ws, conn };
  }

  async function getAllocatedConnection(
    target: AllocatedSidecarTarget,
    use: "readiness" | "routing",
  ): Promise<{
    ws: WsHandle;
    conn: SidecarConnection & {
      identity: Extract<SidecarAuthIdentity, { kind: "allocated" }>;
    };
  }> {
    const current = await getProvisionedConnection(target, use);
    if (current.conn.identity.kind !== "allocated") {
      throw new Error(
        `Allocation ${target.allocationId} is connected as probe capacity`,
      );
    }
    return {
      ws: current.ws,
      conn: { ...current.conn, identity: current.conn.identity },
    };
  }

  async function isAllocatedSidecarReady(
    target: AllocatedSidecarTarget,
  ): Promise<boolean> {
    try {
      await getProvisionedConnection(target, "readiness");
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
      pendingPacks.register(
        transferId,
        ws,
        {
          timeoutMs: PACK_TIMEOUT_MS,
          timeoutMessage: `Pack transfer ${transferId} timed out after ${PACK_TIMEOUT_MS}ms`,
          resolve,
          reject(error: string) {
            reject(new Error(error));
          },
        },
        { agentAddress, repoId },
      );

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
    // agentAddresses (pre-first-reconnect); after an authenticated reconnect it
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
        pendingDeploys.reject(
          frame.agentAddress,
          `Failed to store public key: ${err instanceof Error ? err.message : String(err)}`,
        );
        return;
      }
    }
    pendingDeploys.resolve(frame.agentAddress, frame.publicKey);
  }

  function rejectDeployPendingFromFrame(
    ws: WsHandle,
    agentAddress: string,
    error: string,
  ): void {
    const req = pendingDeploys.get(agentAddress);
    if (req === undefined || req.ws !== ws) return;
    // Settle by key, not by the `req` object: a key lookup observes the
    // CURRENT entry, so a stale handle cannot settle a replaced round-trip.
    pendingDeploys.reject(agentAddress, error);
  }

  function sendAgentDeployOnConnection(
    ws: WsHandle,
    conn: SidecarConnection,
    agentAddress: string,
    harnessConfig: HarnessConfig,
    workflow?: AgentDeployFrame["workflow"],
  ): Promise<{ publicKey: string }> {
    if (hubPublicKeyHex === undefined) {
      throw deployFrameFailure(
        "Hub signing key is required for agent deployment",
        false,
      );
    }

    if (pendingDeploys.has(agentAddress)) {
      throw deployFrameFailure(
        `Deploy already in progress for agent "${agentAddress}"`,
        false,
      );
    }

    const addressSet =
      conn.identity.kind === "allocated"
        ? conn.workflowAddresses
        : conn.agentAddresses;
    addressSet.add(agentAddress);
    addressIndex.set(agentAddress, ws);

    return new Promise<{ publicKey: string }>((resolve, reject) => {
      // Timeout and frame-error rejections share this closure, so the routing
      // rollback and the `frameSent: true` tag live in one place.
      pendingDeploys.register(
        agentAddress,
        ws,
        {
          timeoutMs: requestTimeoutMs,
          timeoutMessage: `Deploy of "${agentAddress}" timed out after ${requestTimeoutMs}ms`,
          resolve(publicKey) {
            resolve({ publicKey });
          },
          reject(error: string) {
            if (addressIndex.get(agentAddress) === ws) {
              addressSet.delete(agentAddress);
              addressIndex.delete(agentAddress);
            }
            reject(deployFrameFailure(error, true));
          },
        },
        undefined,
      );

      try {
        conn.send({
          type: "agent.deploy",
          agentAddress,
          agentId: harnessConfig.agentId,
          config: harnessConfig,
          hubPublicKey: hubPublicKeyHex,
          ...(workflow !== undefined ? { workflow } : {}),
        });
      } catch (err) {
        // A synchronous send failure means the frame never reached the wire.
        // Drop the pending entry (and its armed timer) and reject as not-sent
        // so a caller may safely roll back what it staged. The drop bypasses
        // the entry's reject closure: this failure must report
        // `frameSent: false`, and the timer must not fire later and
        // double-reject.
        pendingDeploys.delete(agentAddress);
        if (addressIndex.get(agentAddress) === ws) {
          addressSet.delete(agentAddress);
          addressIndex.delete(agentAddress);
        }
        reject(
          deployFrameFailure(
            `Deploy of "${agentAddress}" failed to send: ${err instanceof Error ? err.message : String(err)}`,
            false,
          ),
        );
      }
    });
  }

  async function sendAgentDeployToAllocation(
    target: AllocatedSidecarTarget,
    agentAddress: string,
    harnessConfig: HarnessConfig,
    workflow?: AgentDeployFrame["workflow"],
  ): Promise<{ publicKey: string }> {
    const { ws, conn } = await getAllocatedConnection(target, "routing");
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
      // The sidecar's `agent.deploy.ack` resolves this through
      // `pendingDeploys.resolve`. The per-step address is workflow-derived
      // and records no hub-side key, so the ack's public key is not needed
      // and this resolves void.
      pendingDeploys.register(
        agentAddress,
        ws,
        {
          timeoutMs: requestTimeoutMs,
          timeoutMessage: `Step provision of "${agentAddress}" timed out after ${requestTimeoutMs}ms`,
          resolve(_publicKey) {
            resolve();
          },
          reject(error: string) {
            reject(new Error(error));
          },
        },
        undefined,
      );

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

  function sendProbeOnConnection(
    ws: WsHandle,
    conn: SidecarConnection,
    args: SendProbeArgs,
  ): Promise<WorkflowProbeResult> {
    const requestId = nextRequestId();

    return new Promise<WorkflowProbeResult>((resolve, reject) => {
      pendingProbes.register(
        requestId,
        ws,
        {
          timeoutMs: probeTimeoutMs,
          timeoutMessage: `Probe ${requestId} timed out after ${probeTimeoutMs}ms`,
          resolve,
          reject(error: string) {
            reject(new Error(error));
          },
        },
        undefined,
      );

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

  async function sendProbeToAllocation(
    target: AllocatedSidecarTarget,
    args: SendProbeArgs,
  ): Promise<WorkflowProbeResult> {
    const { ws, conn } = await getProvisionedConnection(target, "routing");
    return sendProbeOnConnection(ws, conn, args);
  }

  function disconnectAllocation(target: AllocatedSidecarTarget): void {
    const current = allocatedConnections.get(target.allocationId);
    if (
      current === undefined ||
      current.identity.generation !== target.generation
    ) {
      return;
    }
    handleClose(current.ws);
    current.ws.close();
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
      // Timeout, ack, and error rejection share one closure so the routing
      // teardown runs exactly once no matter how the round-trip settles.
      pendingUndeploys.register(
        agentAddress,
        ws,
        {
          timeoutMs: requestTimeoutMs,
          timeoutMessage: `Undeploy of "${agentAddress}" timed out after ${requestTimeoutMs}ms`,
          resolve() {
            removeAgentAddress(ws, agentAddress);
            resolve();
          },
          reject(error: string) {
            removeAgentAddress(ws, agentAddress);
            reject(new Error(error));
          },
        },
        undefined,
      );

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
    revoke?: string[],
  ): Promise<void> {
    await sendRequest(agentAddress, (requestId) => ({
      type: "credentials.update",
      requestId,
      agentAddress,
      delivery,
      ...(revoke !== undefined ? { revoke } : {}),
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
    sendProbeToAllocation,
    disconnectAllocation,
    sendAgentUndeploy,
    sendSourcesUpdate,
    sendCredentialsUpdate,
    sendPackToAllocation,
    sendWorkflowRunPackToAllocation,
    fenceAllocation,
    retireAllocation,
    waitForAllocatedSidecar,
    isAllocatedSidecarReady,
    isAllocatedWorkflowActive,
    sendAgentDeployToAllocation,
    bindAllocatedStepRoute,
    unbindAllocatedStepRoute,
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

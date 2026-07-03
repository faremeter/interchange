// Hub-side websocket handler for sidecar connections.
//
// Accepts websocket upgrades, processes register frames, maintains a routing
// table of agentAddress → sidecar connection, and dispatches frames between
// sidecars and the hub's internal systems.

import { getLogger } from "@intx/log";
import { verifyEd25519 } from "@intx/crypto";
import { chunkPack, createPackReceiver } from "@intx/pack-transport";
import { hexDecode, hexEncode } from "@intx/types";
import { type } from "arktype";
import {
  SidecarFrame,
  type AgentDeployFrame,
  type HubFrame,
  type PackPushFrame,
  type PackDoneFrame,
  type RepoId,
} from "@intx/types/sidecar";
import type {
  AbortReason,
  ConnectorThreadState,
  HarnessConfig,
  InferenceSource,
} from "@intx/types/runtime";
import type { GrantRule } from "@intx/types/authz";
import {
  createSidecarEmitter,
  type SidecarEventEmitter,
  type SidecarLookups,
  type SidecarMailPersistedRow,
} from "./sidecar-events";

const logger = getLogger(["hub", "ws", "sidecar"]);

export type SidecarConnection = {
  sidecarId: string;
  agentAddresses: Set<string>;
  // Workflow-substrate deployment addresses (ins_dep_...) this connection
  // hosts. Kept separate from `agentAddresses`: these are hub-minted and
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
   * pack back to its hub-side origin.
   */
  repoId?: RepoId;
};

export type SidecarRouter = {
  handleOpen(ws: WsHandle): void;
  handleMessage(ws: WsHandle, data: string): void;
  handleClose(ws: WsHandle): void;

  routeMail(agentAddress: string, rawMessage: string): boolean;
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
   * deploy router uses the field's presence to discriminate the
   * multi-step branch from the trivial branch. Absent on every legacy
   * agent-deploy.
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
  sendAgentUndeploy(agentAddress: string, reason: string): Promise<void>;
  sendSessionStart(agentAddress: string): Promise<void>;
  sendSessionAbort(agentAddress: string, reason: AbortReason): Promise<void>;
  sendGrantsUpdate(agentAddress: string, grants: GrantRule[]): Promise<void>;
  sendSourcesUpdate(
    agentAddress: string,
    sources: InferenceSource[],
    defaultSource: string,
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

export type SidecarRouterConfig = {
  requestTimeoutMs?: number;
  /** Hex-encoded 32-byte Ed25519 public key for signing deploy commits.
   * Included in agent.deploy frames so sidecars can verify pack signatures. */
  hubPublicKey?: string;
  validateToken?: (sidecarId: string, token: string) => boolean;
  challengeTimeoutMs?: number;
  disconnectQueueMaxSize?: number;
  disconnectQueueTTLMs?: number;
  pingTimeoutMs?: number;
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
const DEFAULT_DISCONNECT_QUEUE_MAX_SIZE = 100;
const DEFAULT_DISCONNECT_QUEUE_TTL_MS = 5 * 60 * 1000;
const DEFAULT_PING_TIMEOUT_MS = 60_000;

export function createSidecarRouter(
  config: SidecarRouterConfig = {},
): SidecarRouter {
  const {
    requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS,
    challengeTimeoutMs = DEFAULT_CHALLENGE_TIMEOUT_MS,
    hubPublicKey: hubPublicKeyHex,
    validateToken,
    disconnectQueueMaxSize = DEFAULT_DISCONNECT_QUEUE_MAX_SIZE,
    disconnectQueueTTLMs = DEFAULT_DISCONNECT_QUEUE_TTL_MS,
    pingTimeoutMs = DEFAULT_PING_TIMEOUT_MS,
    lookups = {},
  } = config;

  // Receiver-dispatch surface. Wire-layer callsites emit events here;
  // host code subscribes via `router.events`.
  const events = createSidecarEmitter();

  // ws handle → registered connection
  const connections = new Map<WsHandle, SidecarConnection>();
  // agentAddress → ws handle (routing table)
  const addressIndex = new Map<string, WsHandle>();
  // requestId → pending promise
  const pending = new Map<string, PendingRequest>();
  // agentAddress → pending deploy promise (matched by agent.deploy.ack/agent.error)
  type PendingDeploy = {
    agentAddress: string;
    ws: WsHandle;
    resolve(): void;
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

  // transferId → pending pack transfer (resolved by repo.pack.ack, rejected by repo.pack.reject)
  type PendingPack = {
    transferId: string;
    ws: WsHandle;
    resolve(): void;
    reject(error: string): void;
    timer: ReturnType<typeof setTimeout>;
  };
  const pendingPacks = new Map<string, PendingPack>();
  let packCounter = 0;

  // agentAddress → pending session start (resolved by session.start.ack)
  type PendingSessionStart = {
    agentAddress: string;
    ws: WsHandle;
    resolve(): void;
    reject(error: string): void;
    timer: ReturnType<typeof setTimeout>;
  };
  const pendingSessionStarts = new Map<string, PendingSessionStart>();

  // agentAddress → pending undeploy (resolved by agent.undeploy.ack)
  type PendingUndeploy = {
    agentAddress: string;
    ws: WsHandle;
    resolve(): void;
    reject(error: string): void;
    timer: ReturnType<typeof setTimeout>;
  };
  const pendingUndeploys = new Map<string, PendingUndeploy>();

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

  function enqueueForDisconnected(
    agentAddress: string,
    frame: HubFrame,
  ): boolean {
    const entry = disconnectedAgents.get(agentAddress);
    if (entry === undefined) return false;

    if (entry.queue.length >= disconnectQueueMaxSize) {
      entry.queue.shift();
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

    switch (frame.type) {
      case "register":
        handleRegister(
          ws,
          frame.sidecarId,
          frame.token,
          frame.agentAddresses,
          frame.workflowAddresses ?? [],
        );
        break;
      case "reconnect":
        void handleReconnect(
          ws,
          frame.sidecarId,
          frame.token,
          frame.agentAddresses,
          frame.workflowAddresses ?? [],
          frame.deployRefs ?? {},
        );
        break;
      case "challenge.response":
        void handleChallengeResponse(ws, frame.responses);
        break;
      case "agent.deploy.ack":
        void handleDeployAck(frame.agentAddress, frame.publicKey);
        break;
      case "agent.error":
        rejectDeployPending(frame.agentAddress, frame.error);
        rejectSessionStartPending(frame.agentAddress, frame.error);
        rejectUndeployPending(frame.agentAddress, frame.error);
        break;
      case "session.start.ack":
        resolveSessionStartPending(frame.agentAddress);
        break;
      case "agent.undeploy.ack":
        resolveUndeployPending(frame.agentAddress);
        break;
      case "ping":
        handlePing(ws);
        break;
      case "mail.outbound":
        if (frame.delivered !== true) {
          handleMailOutbound(frame.rawMessage, frame.recipients);
        } else if (lookups.persistMail && frame.senderAddress) {
          void handleMailPersist(
            lookups.persistMail,
            frame.rawMessage,
            frame.senderAddress,
            frame.recipients,
          );
        } else if (frame.delivered === true) {
          if (!frame.senderAddress) {
            logger.warn`Dropping delivered mail.outbound frame with no senderAddress`;
          } else {
            logger.warn`Dropping delivered mail.outbound frame: no persistMail lookup configured`;
          }
        }
        break;
      case "agent.event":
        events.emit("agent.event", {
          agentAddress: frame.agentAddress,
          sessionId: frame.sessionId,
          event: frame.event,
        });
        dispatchToSubscribers(frame.agentAddress, frame.event);
        break;
      case "connector.state.changed":
        // Gate the cache write on the sending sidecar actually owning
        // the named agent. A misbehaving sidecar that knows another
        // agent's address could otherwise poison the cached state.
        if (addressIndex.get(frame.agentAddress) !== ws) {
          logger.warn`Dropping connector.state.changed for ${frame.agentAddress}: not registered to this sidecar`;
          break;
        }
        connectorStates.set(frame.agentAddress, frame.connectorState);
        events.emit("connector.state.changed", {
          agentAddress: frame.agentAddress,
          connectorState: frame.connectorState,
        });
        break;
      case "session.ack":
        resolvePending(frame.requestId);
        break;
      case "session.error":
        rejectPending(frame.requestId, frame.error);
        break;
      case "repo.pack.ack":
        resolvePackPending(frame.transferId);
        break;
      case "repo.pack.reject":
        rejectPackPending(frame.transferId, frame.reason);
        break;
      case "repo.pack.push":
        handlePackPush(ws, frame);
        break;
      case "repo.pack.done":
        void handlePackDone(ws, frame);
        break;
      case "deploy.apply.error":
        // Gate the failure emit on the sending sidecar actually
        // owning the named agent. A misbehaving sidecar that knows
        // another agent's address could otherwise drive the hub to
        // record an apply failure against a deploy the other agent's
        // sidecar still considers live, contaminating audit trails
        // and any failure-driven rollback logic the hub runs.
        if (addressIndex.get(frame.agentAddress) !== ws) {
          logger.warn`Dropping deploy.apply.error for ${frame.agentAddress}: not registered to this sidecar`;
          break;
        }
        events.emit("deploy.apply.error", {
          agentAddress: frame.agentAddress,
          attemptId: frame.attemptId,
          previousDeployId: frame.previousDeployId,
          category: frame.category,
          message: frame.message,
          ...(frame.package !== undefined ? { package: frame.package } : {}),
          occurredAt: frame.occurredAt,
        });
        break;
      default:
        logger.warn`Unknown frame type from sidecar: ${(frame as { type: string }).type}`;
    }
  }

  function handleRegister(
    ws: WsHandle,
    sidecarId: string,
    token: string,
    agentAddresses: string[],
    workflowAddresses: string[] = [],
  ): void {
    if (validateToken !== undefined && !validateToken(sidecarId, token)) {
      logger.warn`Rejected registration from sidecar ${sidecarId}: invalid token`;
      ws.close();
      return;
    }

    // If this same ws is re-registering, drop its addressIndex entries so
    // the new register/reconnect rebuilds the owned set (handleReconnect
    // and the loop below re-add the addresses that remain owned). Do not
    // drop connectorStates here: this branch is reached only on a same-ws
    // re-register, where the harness is live and its connector state is
    // current. A genuinely restarting harness opens a new ws (so existing
    // is undefined) and its stale state is cleared by the cross-ws ghost
    // loop below and by handleClose.
    const existing = connections.get(ws);
    if (existing !== undefined) {
      for (const addr of existing.agentAddresses) {
        addressIndex.delete(addr);
      }
      for (const addr of existing.workflowAddresses) {
        addressIndex.delete(addr);
      }
    }

    const addrSet = new Set(agentAddresses);
    // The frame carries the COMPLETE current live workflow-address set, so
    // this replaces (not merges) the connection's workflow routing.
    const workflowSet = new Set(workflowAddresses);

    // Clean up ghost entries from other connections that previously
    // owned addresses this sidecar is now claiming.
    for (const addr of addrSet) {
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

    // Same ghost cleanup for reclaimed workflow-substrate addresses: evict the
    // address from the prior owner's set. Without this, when the superseded
    // connection later closes (the abrupt-restart overlap window), its
    // `handleClose` teardown would iterate the stale address and cancel THIS
    // connection's live in-flight pack transfer / abandon its collector for
    // the deployment it just took over. `cancelByAgent` is keyed by address,
    // not by connection, so the stale close would hit the new owner. The
    // session loop above evicts the reclaimed address the same way.
    for (const addr of workflowSet) {
      const prevWs = addressIndex.get(addr);
      if (prevWs !== undefined && prevWs !== ws) {
        const prevConn = connections.get(prevWs);
        if (prevConn !== undefined) {
          prevConn.workflowAddresses.delete(addr);
        }
      }
    }

    const conn: SidecarConnection = {
      sidecarId,
      agentAddresses: addrSet,
      workflowAddresses: workflowSet,
      send(frame: HubFrame) {
        ws.send(JSON.stringify(frame));
      },
    };

    connections.set(ws, conn);
    for (const addr of addrSet) {
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
    // Re-register workflow-substrate addresses for routing directly. They are
    // hub-minted with no per-address key (no `agent_instance` row), so there
    // is no challenge to run -- the same way they first entered `addressIndex`
    // at deploy time via `sendAgentDeploy`. A later-connecting ws claiming the
    // same address overwrites the pointer here; the prior owner's `handleClose`
    // then no-ops on it via its ownership guard.
    for (const addr of workflowSet) {
      addressIndex.set(addr, ws);
    }

    logger.info`Sidecar ${sidecarId} registered with ${String(agentAddresses.length)} agents and ${String(workflowSet.size)} workflow deployments`;
  }

  async function handleReconnect(
    ws: WsHandle,
    sidecarId: string,
    token: string,
    agentAddresses: string[],
    workflowAddresses: string[] = [],
    deployRefs: Record<string, string> = {},
  ): Promise<void> {
    if (validateToken !== undefined && !validateToken(sidecarId, token)) {
      logger.warn`Rejected reconnect from sidecar ${sidecarId}: invalid token`;
      ws.close();
      return;
    }

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

    // Register the sidecar connection immediately (with no session addresses,
    // but WITH the workflow-substrate addresses -- those need no challenge)
    // so it can receive frames while the session challenge is pending.
    handleRegister(ws, sidecarId, token, [], workflowAddresses);

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

    // Look up stored public keys for all claimed addresses.
    const keyLookups = await Promise.all(
      agentAddresses.map(async (addr) => ({
        address: addr,
        publicKeyHex: await lookupKey(addr),
      })),
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
          reason: "Unknown agent address",
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
    // (e.g. sendGrantsUpdate). Addresses that fail governance are
    // rolled back from the routing table afterward.
    for (const addr of verified) {
      // If a different ws still owns this address (live takeover via
      // verified reconnect), evict its cached connector state before
      // routing flips. The new owner's harness will bootstrap via
      // restore-fires-callback.
      const prevWs = addressIndex.get(addr);
      if (prevWs !== undefined && prevWs !== ws) {
        connectorStates.delete(addr);
      }
      conn.agentAddresses.add(addr);
      addressIndex.set(addr, ws);
    }

    const ready: string[] = [];
    const failed: string[] = [];

    for (const addr of verified) {
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

    // Roll back failed addresses from the routing table.
    for (const addr of failed) {
      conn.agentAddresses.delete(addr);
      addressIndex.delete(addr);
    }

    // Flush queued messages only for ready addresses.
    for (const addr of ready) {
      flushDisconnectedQueue(addr, conn);
    }

    // Re-deploy agents whose deploy ref is stale or absent. Fire-and-forget
    // so reconnect completion is not blocked on pack transfer. The
    // wire layer owns the staleness comparison; the event fires only
    // when staleness is confirmed.
    const checkDeployRef = lookups.lookupDeployRef;
    if (checkDeployRef !== undefined) {
      for (const addr of ready) {
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
          disconnectedAgents.delete(addr);
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

  function handleMailOutbound(rawMessage: string, recipients: string[]): void {
    // Route to locally connected sidecars first, then try disconnect queues.
    const unrouted: string[] = [];
    for (const recipient of recipients) {
      const targetWs = addressIndex.get(recipient);
      if (targetWs !== undefined) {
        const conn = connections.get(targetWs);
        if (conn !== undefined) {
          conn.send({
            type: "mail.inbound",
            agentAddress: recipient,
            rawMessage,
          });
          continue;
        }
      }

      const frame: HubFrame = {
        type: "mail.inbound",
        agentAddress: recipient,
        rawMessage,
      };
      if (enqueueForDisconnected(recipient, frame)) continue;

      unrouted.push(recipient);
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
        instanceId: result.instanceId,
        address: result.address,
      });
    }
  }

  function handleClose(ws: WsHandle): void {
    const conn = connections.get(ws);
    if (conn === undefined) return;

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
        const deployReq = pendingDeploys.get(addr);
        if (deployReq !== undefined && deployReq.ws === ws) {
          clearTimeout(deployReq.timer);
          pendingDeploys.delete(addr);
          deployReq.reject(`Sidecar ${conn.sidecarId} disconnected`);
        }

        // Create a queue entry so messages can accumulate while the
        // sidecar is disconnected. Skip if the agent is being undeployed
        // or has a pending session start — there is no point queuing
        // messages for an agent being torn down or one that never started.
        if (!pendingUndeploys.has(addr) && !pendingSessionStarts.has(addr)) {
          const timer = setTimeout(() => {
            disconnectedAgents.delete(addr);
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
      }
    }
    connections.delete(ws);

    // Cancel the liveness timer for this connection.
    const livenessTimer = livenessTimers.get(ws);
    if (livenessTimer !== undefined) {
      clearTimeout(livenessTimer);
      livenessTimers.delete(ws);
    }

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

    // Reject any in-flight pack transfers for this sidecar.
    for (const [transferId, pack] of pendingPacks) {
      if (pack.ws !== ws) continue;
      clearTimeout(pack.timer);
      pendingPacks.delete(transferId);
      pack.reject(`Sidecar ${conn.sidecarId} disconnected`);
    }

    // Reject any in-flight session starts for this sidecar.
    for (const [addr, req] of pendingSessionStarts) {
      if (req.ws !== ws) continue;
      clearTimeout(req.timer);
      pendingSessionStarts.delete(addr);
      req.reject(`Sidecar ${conn.sidecarId} disconnected`);
    }

    // Reject any in-flight undeploys for this sidecar.
    for (const [addr, req] of pendingUndeploys) {
      if (req.ws !== ws) continue;
      clearTimeout(req.timer);
      pendingUndeploys.delete(addr);
      req.reject(`Sidecar ${conn.sidecarId} disconnected`);
    }

    // Cancel any in-flight inbound pack transfers from this sidecar
    // across both receivers. The two receivers track their own in-
    // flight transferIds, so a pending workflow-run transfer for an
    // agent that just disconnected won't outlive the connection just
    // because the agent-state receiver has nothing to cancel. Iterate the
    // owned union so a reconnected workflow deployment's transfer is
    // cancelled too; the deduped set avoids a double-cancel for an address
    // that is in both sets. A reclaimed address is not present here -- the
    // ghost cleanup in handleRegister evicts it from this (superseded)
    // connection -- so a stale close does not cancel the new owner's work.
    const owned = ownedAddresses(conn);
    for (const addr of owned) {
      agentStatePackReceiver.cancelByAgent(addr);
      workflowRunPackReceiver.cancelByAgent(addr);
    }

    events.emit("sidecar.disconnect", {
      ownedAddresses: [...owned],
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

  function resolvePackPending(transferId: string): void {
    const entry = pendingPacks.get(transferId);
    if (entry === undefined) return;
    clearTimeout(entry.timer);
    pendingPacks.delete(transferId);
    entry.resolve();
  }

  function rejectPackPending(transferId: string, reason: string): void {
    const entry = pendingPacks.get(transferId);
    if (entry === undefined) return;
    clearTimeout(entry.timer);
    pendingPacks.delete(transferId);
    entry.reject(reason);
  }

  function resolveSessionStartPending(agentAddress: string): void {
    const req = pendingSessionStarts.get(agentAddress);
    if (req === undefined) {
      logger.warn`Received session.start.ack for "${agentAddress}" with no pending start`;
      return;
    }
    clearTimeout(req.timer);
    pendingSessionStarts.delete(agentAddress);
    req.resolve();
  }

  function rejectSessionStartPending(
    agentAddress: string,
    error: string,
  ): void {
    const req = pendingSessionStarts.get(agentAddress);
    if (req === undefined) return;
    clearTimeout(req.timer);
    pendingSessionStarts.delete(agentAddress);
    req.reject(error);
  }

  function resolveUndeployPending(agentAddress: string): void {
    const req = pendingUndeploys.get(agentAddress);
    if (req === undefined) {
      logger.warn`Received agent.undeploy.ack for "${agentAddress}" with no pending undeploy`;
      return;
    }
    clearTimeout(req.timer);
    pendingUndeploys.delete(agentAddress);
    req.resolve();
  }

  function rejectUndeployPending(agentAddress: string, error: string): void {
    const req = pendingUndeploys.get(agentAddress);
    if (req === undefined) return;
    clearTimeout(req.timer);
    pendingUndeploys.delete(agentAddress);
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
  ): SidecarLookups["receiveAgentStatePack"] | undefined {
    switch (repoId.kind) {
      case "agent-state":
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

  function routeMail(agentAddress: string, rawMessage: string): boolean {
    const ws = addressIndex.get(agentAddress);
    if (ws !== undefined) {
      const conn = connections.get(ws);
      if (conn !== undefined) {
        conn.send({
          type: "mail.inbound",
          agentAddress,
          rawMessage,
        });
        return true;
      }
    }

    // If the agent recently disconnected, queue for delivery on reconnect.
    const frame: HubFrame = { type: "mail.inbound", agentAddress, rawMessage };
    return enqueueForDisconnected(agentAddress, frame);
  }

  async function handleDeployAck(
    agentAddress: string,
    publicKey: string,
  ): Promise<void> {
    if (!pendingDeploys.has(agentAddress)) {
      logger.warn`Received agent.deploy.ack for "${agentAddress}" with no pending deploy`;
      return;
    }

    if (events.listenerCount("agent.deploy.ack") > 0) {
      try {
        await events.emitAndAwait("agent.deploy.ack", {
          agentAddress,
          publicKey,
        });
      } catch (err) {
        rejectDeployPending(
          agentAddress,
          `Failed to store public key: ${err instanceof Error ? err.message : String(err)}`,
        );
        return;
      }
    }
    resolveDeployPending(agentAddress);
  }

  function resolveDeployPending(agentAddress: string): void {
    const req = pendingDeploys.get(agentAddress);
    if (req === undefined) return;
    clearTimeout(req.timer);
    pendingDeploys.delete(agentAddress);
    req.resolve();
  }

  function rejectDeployPending(agentAddress: string, error: string): void {
    const req = pendingDeploys.get(agentAddress);
    if (req === undefined) return;
    clearTimeout(req.timer);
    pendingDeploys.delete(agentAddress);
    req.reject(error);
  }

  async function sendAgentDeploy(
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

    const ws =
      addressIndex.get(agentAddress) ?? findSidecarForNewAgent(agentAddress);

    if (ws === undefined) {
      throw new Error(`No sidecar available for agent "${agentAddress}"`);
    }

    const conn = connections.get(ws);
    if (conn === undefined) {
      throw new Error(`No sidecar connected for agent "${agentAddress}"`);
    }

    conn.agentAddresses.add(agentAddress);
    addressIndex.set(agentAddress, ws);

    return new Promise<{ publicKey: string }>((resolve, reject) => {
      // The deploy-ack handler does not currently thread the
      // sidecar-reported public key back through the pending-deploy
      // resolver; it only fires `agent.deploy.ack` listeners and then
      // resolves the pending deploy. Capture the key via a one-shot
      // listener so the return value carries it without a wire-shape
      // change to `agent.deploy.ack`.
      let capturedPublicKey: string | undefined;
      const detachListener = events.on("agent.deploy.ack", (payload) => {
        if (payload.agentAddress === agentAddress) {
          capturedPublicKey = payload.publicKey;
        }
      });

      const timer = setTimeout(() => {
        detachListener();
        pendingDeploys.delete(agentAddress);
        if (addressIndex.get(agentAddress) === ws) {
          conn.agentAddresses.delete(agentAddress);
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
        resolve() {
          detachListener();
          if (capturedPublicKey === undefined) {
            reject(
              new Error(
                `Deploy of "${agentAddress}" resolved without an agent.deploy.ack publicKey payload`,
              ),
            );
            return;
          }
          resolve({ publicKey: capturedPublicKey });
        },
        reject(error: string) {
          detachListener();
          if (addressIndex.get(agentAddress) === ws) {
            conn.agentAddresses.delete(agentAddress);
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

  function findSidecarForNewAgent(_agentAddress: string): WsHandle | undefined {
    const first = connections.entries().next();
    if (first.done) return undefined;
    return first.value[0];
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

  function sendSessionStart(agentAddress: string): Promise<void> {
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
        pendingSessionStarts.delete(agentAddress);
        removeAgentAddress(ws, agentAddress);
        reject(
          new Error(
            `Session start for "${agentAddress}" timed out after ${requestTimeoutMs}ms`,
          ),
        );
      }, requestTimeoutMs);

      pendingSessionStarts.set(agentAddress, {
        agentAddress,
        ws,
        resolve() {
          resolve();
        },
        reject(error: string) {
          removeAgentAddress(ws, agentAddress);
          reject(new Error(error));
        },
        timer,
      });

      conn.send({
        type: "session.start",
        agentAddress,
      });
    });
  }

  async function sendSessionAbort(
    agentAddress: string,
    reason: AbortReason,
  ): Promise<void> {
    await sendRequest(agentAddress, (requestId) => ({
      type: "session.abort",
      requestId,
      agentAddress,
      reason,
    }));
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

  async function sendGrantsUpdate(
    agentAddress: string,
    grants: GrantRule[],
  ): Promise<void> {
    await sendRequest(agentAddress, (requestId) => ({
      type: "grants.update",
      requestId,
      agentAddress,
      grants,
    }));
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
    sendAgentDeploy,
    sendAgentUndeploy,
    sendSessionStart,
    sendSessionAbort,
    sendGrantsUpdate,
    sendSourcesUpdate,
    sendPack,
    bindStepRoute,
    unbindStepRoute,
    sendSyncRequest,
    sendSignalDeliver,
    sendDrain,
    subscribeAgent,
    dispatchAgentEvent: dispatchToSubscribers,
    getConnectedSidecars,
    getRoutableAddresses,
    getConnectorState,
    events,
  };
}

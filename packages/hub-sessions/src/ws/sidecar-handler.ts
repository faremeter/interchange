// Hub-side websocket handler for sidecar connections.
//
// Accepts websocket upgrades, processes register frames, maintains a routing
// table of agentAddress → sidecar connection, and dispatches frames between
// sidecars and the hub's internal systems.

import { getLogger } from "@intx/log";
import { verifyEd25519 } from "@intx/crypto";
import { chunkPack, createPackReceiver } from "@intx/pack-transport";
import { hexDecode, hexEncode } from "@intx/types";
import { isWorkflowDerivedAddress } from "@intx/workflow-deploy";
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
  ConnectorThreadState,
  HarnessConfig,
  InferenceSource,
} from "@intx/types/runtime";
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
  sendAgentUndeploy(agentAddress: string, reason: string): Promise<void>;
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
        return true;
      case "register":
      case "reconnect":
      case "challenge.response":
      case "mail.outbound":
      case "agent.event":
      case "connector.state.changed":
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
      case "register":
        return handleRegister(
          ws,
          frame.sidecarId,
          frame.token,
          frame.agentAddresses,
        );
      case "reconnect":
        return handleReconnect(
          ws,
          frame.sidecarId,
          frame.token,
          frame.agentAddresses,
          frame.deployRefs ?? {},
        );
      case "challenge.response":
        return handleChallengeResponse(ws, frame.responses);
      case "agent.deploy.ack":
        return handleDeployAck(frame.agentAddress, frame.publicKey);
      case "agent.error":
        rejectDeployPending(frame.agentAddress, frame.error);
        rejectUndeployPending(frame.agentAddress, frame.error);
        return;
      case "agent.undeploy.ack":
        resolveUndeployPending(frame.agentAddress);
        return;
      case "ping":
        handlePing(ws);
        return;
      case "mail.outbound":
        if (frame.delivered !== true) {
          handleMailOutbound(frame.rawMessage, frame.recipients);
          return;
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
      case "session.ack":
        resolvePending(frame.requestId);
        return;
      case "session.error":
        rejectPending(frame.requestId, frame.error);
        return;
      case "repo.pack.ack":
        resolvePackPending(frame.transferId);
        return;
      case "repo.pack.reject":
        rejectPackPending(frame.transferId, frame.reason);
        return;
      case "repo.pack.push":
        handlePackPush(ws, frame);
        return;
      case "repo.pack.done":
        return handlePackDone(ws, frame);
      default:
        return assertNever(frame);
    }
  }

  async function handleRegister(
    ws: WsHandle,
    sidecarId: string,
    token: string,
    agentAddresses: string[],
  ): Promise<void> {
    if (validateToken !== undefined && !validateToken(sidecarId, token)) {
      logger.warn`Rejected registration from sidecar ${sidecarId}: invalid token`;
      ws.close();
      return;
    }

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
    sidecarId: string,
    token: string,
    agentAddresses: string[],
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

    // Register the sidecar connection immediately (with no addresses) so it
    // can receive frames while the ownership challenge is pending. Every
    // reconnect address -- session and workflow-derived alike -- enters
    // routing only through the verified path below, never unchallenged here.
    // Empty address list, so the key-existence gate has nothing to await.
    await handleRegister(ws, sidecarId, token, []);

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
      // Track the address on the set that matches its lifecycle so
      // handleClose reclaims it correctly: a workflow-derived deployment
      // address goes on the workflow set (no disconnect queue -- its
      // in-flight state is reconstructed sidecar-locally on the next
      // reconnect), a launched agent on the session set (queued for
      // reconnect). The routing pointer is the same either way; only now
      // it is written behind a passed challenge.
      if (isWorkflowDerivedAddress(addr)) {
        conn.workflowAddresses.add(addr);
      } else {
        conn.agentAddresses.add(addr);
      }
      addressIndex.set(addr, ws);
    }

    const ready: string[] = [];
    const failed: string[] = [];

    for (const addr of verified) {
      // The `agent.reconnected` reaction is session lifecycle -- instance
      // status flip, event-collector restore -- owned by the agent_instance
      // concept. A workflow-derived deployment address has no agent_instance
      // row, so the reaction's `requireInstance` would throw and roll the
      // just-verified address back out of routing. It needs routing + queue
      // flush only, which the passed challenge has now made safe; skip the
      // session reaction for it.
      if (isWorkflowDerivedAddress(addr)) {
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
        // Deploy-pack freshness (createDeployPack / parseAgentId) is an
        // agent-repo path scoped to launched agents; it is not validated for
        // workflow-derived deployment addresses, and the current register
        // path never ran it for them. Skip it here to match that.
        if (isWorkflowDerivedAddress(addr)) continue;
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
    if (hubPublicKeyHex === undefined) {
      throw new Error("Hub signing key is required for step provisioning");
    }
    if (pendingDeploys.has(agentAddress)) {
      throw new Error(`Deploy already in progress for agent "${agentAddress}"`);
    }
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
        resolve() {
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
    sendSourcesUpdate,
    sendPack,
    bindStepRoute,
    unbindStepRoute,
    sendProvisionStep,
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

// Hub-side websocket handler for sidecar connections.
//
// Accepts websocket upgrades, processes register frames, maintains a routing
// table of agentAddress → sidecar connection, and dispatches frames between
// sidecars and the hub's internal systems.

import { randomBytes } from "node:crypto";
import { getLogger } from "@intx/log";
import { verifyEd25519 } from "@intx/crypto-node";
import { chunkPack, createPackReceiver } from "@intx/pack-transport";
import { hexDecode, hexEncode } from "@intx/types";
import { type } from "arktype";
import {
  SidecarFrame,
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
  send(frame: HubFrame): void;
};

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
  sendAgentDeploy(agentAddress: string, config: HarnessConfig): Promise<void>;
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
  sendSyncRequest(agentAddress: string): void;

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

  // Receives state packs pushed from sidecars.
  const statePackReceiver = createPackReceiver();

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
        handleRegister(ws, frame.sidecarId, frame.token, frame.agentAddresses);
        break;
      case "reconnect":
        void handleReconnect(
          ws,
          frame.sidecarId,
          frame.token,
          frame.agentAddresses,
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
        handleStatePackPush(ws, frame);
        break;
      case "repo.pack.done":
        void handleStatePackDone(ws, frame);
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
  ): void {
    if (validateToken !== undefined && !validateToken(sidecarId, token)) {
      logger.warn`Rejected registration from sidecar ${sidecarId}: invalid token`;
      ws.close();
      return;
    }

    // If this sidecar was already connected, clean up old state.
    const existing = connections.get(ws);
    if (existing !== undefined) {
      for (const addr of existing.agentAddresses) {
        addressIndex.delete(addr);
        // The harness on this ws is restarting; the cached connector
        // state from its previous incarnation is now stale. The new
        // harness will bootstrap via restore-fires-callback.
        connectorStates.delete(addr);
      }
    }

    const addrSet = new Set(agentAddresses);

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

    const conn: SidecarConnection = {
      sidecarId,
      agentAddresses: addrSet,
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

    logger.info`Sidecar ${sidecarId} registered with ${String(agentAddresses.length)} agents`;
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

    // Register the sidecar connection immediately (with no addresses)
    // so it can receive frames while the challenge is pending.
    handleRegister(ws, sidecarId, token, []);

    const conn = connections.get(ws);
    if (conn === undefined) return;

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

      const nonce = randomBytes(32);
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
        valid = verifyEd25519(payload, sigBytes, entry.publicKey);
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

    // Cancel any in-flight inbound state transfers from this sidecar.
    for (const addr of conn.agentAddresses) {
      statePackReceiver.cancelByAgent(addr);
    }

    events.emit("sidecar.disconnect", {
      agentAddresses: [...conn.agentAddresses],
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

  function handleStatePackPush(ws: WsHandle, frame: PackPushFrame): void {
    const conn = connections.get(ws);
    if (conn === undefined) return;
    if (!conn.agentAddresses.has(frame.agentAddress)) {
      logger.warn`Received repo.pack.push for unrouted agent ${frame.agentAddress}`;
      return;
    }

    const reason = statePackReceiver.handlePush(frame);
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

  async function handleStatePackDone(
    ws: WsHandle,
    frame: PackDoneFrame,
  ): Promise<void> {
    const conn = connections.get(ws);
    if (conn === undefined) return;
    if (!conn.agentAddresses.has(frame.agentAddress)) {
      logger.warn`Received repo.pack.done for unrouted agent ${frame.agentAddress}`;
      return;
    }

    const result = statePackReceiver.handleDone(frame);
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

    const receiveStatePack = lookups.receiveStatePack;
    if (receiveStatePack === undefined) {
      conn.send({
        type: "repo.pack.ack",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
      });
      return;
    }

    const verdict = await receiveStatePack(
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
  ): Promise<void> {
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

    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
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
          resolve();
        },
        reject(error: string) {
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
    sendSyncRequest,
    subscribeAgent,
    dispatchAgentEvent: dispatchToSubscribers,
    getConnectedSidecars,
    getRoutableAddresses,
    getConnectorState,
    events,
  };
}

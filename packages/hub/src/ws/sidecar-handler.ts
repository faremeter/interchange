// Hub-side websocket handler for sidecar connections.
//
// Accepts websocket upgrades, processes register frames, maintains a routing
// table of agentAddress → sidecar connection, and dispatches frames between
// sidecars and the hub's internal systems.

import { randomBytes } from "node:crypto";
import { getLogger } from "@interchange/log";
import { verifyEd25519 } from "@interchange/crypto-node";
import { chunkPack, createPackReceiver } from "@interchange/pack-transport";
import { type } from "arktype";
import {
  SidecarFrame,
  type PackRejectReason,
  type HubFrame,
  type PackPushFrame,
  type PackDoneFrame,
} from "@interchange/types/sidecar";
import type {
  AbortReason,
  HarnessConfig,
  ProviderConfig,
} from "@interchange/types/runtime";
import type { GrantRule } from "@interchange/types/authz";

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

export type SidecarRouter = {
  handleOpen(ws: WsHandle): void;
  handleMessage(ws: WsHandle, data: string): void;
  handleClose(ws: WsHandle): void;

  routeMail(agentAddress: string, rawMessage: string): boolean;
  sendAgentDeploy(agentAddress: string, config: HarnessConfig): Promise<void>;
  sendAgentUndeploy(agentAddress: string, reason: string): Promise<void>;
  sendSessionStart(agentAddress: string): Promise<void>;
  sendSessionAbort(agentAddress: string, reason: AbortReason): Promise<void>;
  sendGrantsUpdate(agentAddress: string, grants: GrantRule[]): Promise<void>;
  sendProvidersUpdate(
    agentAddress: string,
    providers: ProviderConfig[],
  ): Promise<void>;
  sendPack(
    agentAddress: string,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
  ): Promise<void>;
  sendSyncRequest(agentAddress: string): void;

  subscribeAgent(
    agentAddress: string,
    callback: (event: unknown) => void,
  ): () => void;
  dispatchAgentEvent(agentAddress: string, event: unknown): void;

  getConnectedSidecars(): string[];
  getRoutableAddresses(): string[];
};

export type SidecarRouterConfig = {
  requestTimeoutMs?: number;
  /** Hex-encoded 32-byte Ed25519 public key for signing deploy commits.
   * Included in agent.deploy frames so sidecars can verify pack signatures. */
  hubPublicKey?: string;
  onAgentEvent?: (
    agentAddress: string,
    sessionId: string,
    event: unknown,
  ) => void;
  onSidecarDisconnect?: (agentAddresses: string[]) => void;
  onMailOutbound?: (rawMessage: string, recipients: string[]) => void;
  validateToken?: (sidecarId: string, token: string) => boolean;
  lookupPublicKey?: (agentAddress: string) => Promise<string | null>;
  onAgentDeployAck?: (agentAddress: string, publicKey: string) => Promise<void>;
  onAgentReconnected?: (agentAddress: string) => Promise<void>;
  lookupDeployRef?: (agentAddress: string) => Promise<string | null>;
  onDeployRefStale?: (agentAddress: string) => Promise<void>;
  challengeTimeoutMs?: number;
  disconnectQueueMaxSize?: number;
  disconnectQueueTTLMs?: number;
  pingTimeoutMs?: number;
  onMailPersist?: (args: {
    senderAddress: string;
    direction: "outbound";
    raw: Uint8Array;
  }) => Promise<{
    id: string;
    instanceId: string | null;
    address: string;
    createdAt: Date;
  }>;
  onMailPersisted?: (row: {
    id: string;
    raw: Uint8Array;
    createdAt: Date;
    direction: "inbound" | "outbound";
    instanceId: string | null;
    address: string;
  }) => void;
  onStatePackReceived?: (
    agentAddress: string,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
  ) => Promise<
    { accepted: true } | { accepted: false; reason: PackRejectReason }
  >;
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
    onAgentEvent,
    onSidecarDisconnect,
    onMailOutbound,
    onMailPersist,
    onMailPersisted,
    validateToken,
    lookupPublicKey,
    onAgentDeployAck,
    onAgentReconnected,
    lookupDeployRef,
    onDeployRefStale,
    disconnectQueueMaxSize = DEFAULT_DISCONNECT_QUEUE_MAX_SIZE,
    disconnectQueueTTLMs = DEFAULT_DISCONNECT_QUEUE_TTL_MS,
    pingTimeoutMs = DEFAULT_PING_TIMEOUT_MS,
    onStatePackReceived,
  } = config;

  if ((lookupDeployRef === undefined) !== (onDeployRefStale === undefined)) {
    throw new Error(
      "lookupDeployRef and onDeployRefStale must both be provided or both omitted",
    );
  }

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
  // ws handle → liveness timer (reset on each ping from the sidecar)
  const livenessTimers = new Map<WsHandle, ReturnType<typeof setTimeout>>();

  // transferId → pending pack transfer (resolved by pack.ack, rejected by pack.reject)
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
        } else if (onMailPersist && frame.senderAddress) {
          void handleMailPersist(
            onMailPersist,
            frame.rawMessage,
            frame.senderAddress,
          );
        } else if (frame.delivered === true) {
          if (!frame.senderAddress) {
            logger.warn`Dropping delivered mail.outbound frame with no senderAddress`;
          } else {
            logger.warn`Dropping delivered mail.outbound frame: no onMailPersist handler configured`;
          }
        }
        break;
      case "agent.event":
        onAgentEvent?.(frame.agentAddress, frame.sessionId, frame.event);
        dispatchToSubscribers(frame.agentAddress, frame.event);
        break;
      case "session.ack":
        resolvePending(frame.requestId);
        break;
      case "session.error":
        rejectPending(frame.requestId, frame.error);
        break;
      case "pack.ack":
        resolvePackPending(frame.transferId);
        break;
      case "pack.reject":
        rejectPackPending(frame.transferId, frame.reason);
        break;
      case "pack.push":
        handleStatePackPush(ws, frame);
        break;
      case "pack.done":
        void handleStatePackDone(ws, frame);
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

    if (lookupPublicKey === undefined) {
      logger.error`Received reconnect frame but no lookupPublicKey callback is configured`;
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
    const lookups = await Promise.all(
      agentAddresses.map(async (addr) => ({
        address: addr,
        publicKeyHex: await lookupPublicKey(addr),
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

    for (const { address, publicKeyHex } of lookups) {
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
    // the onAgentReconnected callback can use sendRequest-based methods
    // (e.g. sendGrantsUpdate). Addresses that fail governance are
    // rolled back from the routing table afterward.
    for (const addr of verified) {
      conn.agentAddresses.add(addr);
      addressIndex.set(addr, ws);
    }

    const ready: string[] = [];
    const failed: string[] = [];

    for (const addr of verified) {
      if (onAgentReconnected !== undefined) {
        try {
          await onAgentReconnected(addr);
          ready.push(addr);
        } catch (err) {
          logger.error`Failed to handle reconnection for ${addr}: ${err instanceof Error ? err.message : String(err)}`;
          failed.push(addr);
        }
      } else {
        ready.push(addr);
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
    // so reconnect completion is not blocked on pack transfer.
    if (lookupDeployRef !== undefined && onDeployRefStale !== undefined) {
      for (const addr of ready) {
        void (async () => {
          try {
            const hubRef = await lookupDeployRef(addr);
            if (hubRef === null) return;
            const sidecarRef = challenge.deployRefs[addr];
            if (sidecarRef === hubRef) return;

            logger.info`Re-deploying ${addr}: sidecar ref ${sidecarRef ?? "(none)"} != hub ref ${hubRef.slice(0, 8)}`;
            await onDeployRefStale(addr);
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

    // Anything not routed locally goes to the external handler.
    if (unrouted.length > 0) {
      if (onMailOutbound !== undefined) {
        onMailOutbound(rawMessage, unrouted);
      } else {
        logger.warn`No mail outbound handler; dropping mail for ${unrouted.join(", ")}`;
      }
    }
  }

  async function handleMailPersist(
    persist: NonNullable<SidecarRouterConfig["onMailPersist"]>,
    rawMessage: string,
    senderAddress: string,
  ): Promise<void> {
    let result;
    let raw: Uint8Array;
    try {
      raw = Uint8Array.from(atob(rawMessage), (c) => c.charCodeAt(0));
      result = await persist({
        senderAddress,
        direction: "outbound",
        raw,
      });
    } catch (err) {
      logger.error`Failed to persist outbound mail from ${senderAddress}: ${err instanceof Error ? err.message : String(err)}`;
      return;
    }

    onMailPersisted?.({
      id: result.id,
      raw,
      createdAt: result.createdAt,
      direction: "outbound",
      instanceId: result.instanceId,
      address: result.address,
    });
  }

  function handleClose(ws: WsHandle): void {
    const conn = connections.get(ws);
    if (conn === undefined) return;

    for (const addr of conn.agentAddresses) {
      // Only remove routing and pending state if this connection still
      // owns the address. A reconnected sidecar may have already claimed it.
      if (addressIndex.get(addr) === ws) {
        addressIndex.delete(addr);
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

    if (onSidecarDisconnect !== undefined) {
      onSidecarDisconnect([...conn.agentAddresses]);
    }

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
      logger.warn`Received pack.push for unrouted agent ${frame.agentAddress}`;
      return;
    }

    const reason = statePackReceiver.handlePush(frame);
    if (reason !== null) {
      conn.send({
        type: "pack.reject",
        agentAddress: frame.agentAddress,
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
      logger.warn`Received pack.done for unrouted agent ${frame.agentAddress}`;
      return;
    }

    const result = statePackReceiver.handleDone(frame);
    if (result === null) {
      conn.send({
        type: "pack.reject",
        agentAddress: frame.agentAddress,
        transferId: frame.transferId,
        reason: "corrupt",
      });
      return;
    }

    if (onStatePackReceived === undefined) {
      conn.send({
        type: "pack.ack",
        agentAddress: frame.agentAddress,
        transferId: frame.transferId,
      });
      return;
    }

    const verdict = await onStatePackReceived(
      frame.agentAddress,
      result.pack,
      result.ref,
      result.commitSha,
    );

    // Connection may have closed during async verification.
    const currentConn = connections.get(ws);
    if (currentConn === undefined) return;

    if (verdict.accepted) {
      currentConn.send({
        type: "pack.ack",
        agentAddress: frame.agentAddress,
        transferId: frame.transferId,
      });
    } else {
      currentConn.send({
        type: "pack.reject",
        agentAddress: frame.agentAddress,
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

    // Register pending entry before sending frames so that a synchronous
    // pack.ack (e.g. in tests or loopback transports) resolves correctly.
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
          type: "pack.push",
          agentAddress,
          transferId,
          seq: chunk.seq,
          data: chunk.data,
        });
      }

      // Send done
      conn.send({
        type: "pack.done",
        agentAddress,
        transferId,
        ref,
        commitSha,
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

    if (onAgentDeployAck !== undefined) {
      try {
        await onAgentDeployAck(agentAddress, publicKey);
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

  async function sendProvidersUpdate(
    agentAddress: string,
    providers: ProviderConfig[],
  ): Promise<void> {
    await sendRequest(agentAddress, (requestId) => ({
      type: "providers.update",
      requestId,
      agentAddress,
      providers,
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
    sendProvidersUpdate,
    sendPack,
    sendSyncRequest,
    subscribeAgent,
    dispatchAgentEvent: dispatchToSubscribers,
    getConnectedSidecars,
    getRoutableAddresses,
  };
}

function hexEncode(bytes: Uint8Array): string {
  return Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

function hexDecode(hex: string): Uint8Array {
  if (hex.length % 2 !== 0) {
    throw new Error(`Hex string must have even length, got ${hex.length}`);
  }
  if (!/^[0-9a-fA-F]*$/.test(hex)) {
    throw new Error("Hex string contains invalid characters");
  }
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(hex.substring(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}

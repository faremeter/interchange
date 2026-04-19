// Hub-side websocket handler for sidecar connections.
//
// Accepts websocket upgrades, processes register frames, maintains a routing
// table of agentAddress → sidecar connection, and dispatches frames between
// sidecars and the hub's internal systems.

import { randomBytes } from "node:crypto";
import { getLogger } from "@interchange/log";
import { verifyEd25519 } from "@interchange/crypto-node";
import type {
  SidecarFrame,
  HubFrame,
  WireAttachment,
} from "@interchange/types/sidecar";
import type { AbortReason, HarnessConfig } from "@interchange/types/runtime";

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
  sendAgentUndeploy(agentAddress: string, reason: string): void;
  sendSessionAbort(agentAddress: string, reason: AbortReason): Promise<void>;
  sendMessage(
    agentAddress: string,
    sessionId: string,
    content: string,
    attachments?: WireAttachment[],
  ): Promise<void>;

  subscribeSession(
    sessionId: string,
    callback: (event: unknown) => void,
  ): () => void;

  getConnectedSidecars(): string[];
  getRoutableAddresses(): string[];
};

export type SidecarRouterConfig = {
  requestTimeoutMs?: number;
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
  challengeTimeoutMs?: number;
  disconnectQueueMaxSize?: number;
  disconnectQueueTTLMs?: number;
};

// Minimal handle so the router doesn't depend on a specific WebSocket impl.
export type WsHandle = {
  send(data: string): void;
  close(): void;
};

const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
const DEFAULT_CHALLENGE_TIMEOUT_MS = 30_000;
const DEFAULT_DISCONNECT_QUEUE_MAX_SIZE = 100;
const DEFAULT_DISCONNECT_QUEUE_TTL_MS = 5 * 60 * 1000; // 5 minutes

export function createSidecarRouter(
  config: SidecarRouterConfig = {},
): SidecarRouter {
  const {
    requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS,
    challengeTimeoutMs = DEFAULT_CHALLENGE_TIMEOUT_MS,
    onAgentEvent,
    onSidecarDisconnect,
    onMailOutbound,
    validateToken,
    lookupPublicKey,
    onAgentDeployAck,
    disconnectQueueMaxSize = DEFAULT_DISCONNECT_QUEUE_MAX_SIZE,
    disconnectQueueTTLMs = DEFAULT_DISCONNECT_QUEUE_TTL_MS,
  } = config;

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
    timer: ReturnType<typeof setTimeout>;
  };
  const pendingChallenges = new Map<WsHandle, PendingChallenge>();
  // agentAddress → queued frames for disconnected agents awaiting reconnect
  type DisconnectedAgent = {
    queue: HubFrame[];
    timer: ReturnType<typeof setTimeout>;
  };
  const disconnectedAgents = new Map<string, DisconnectedAgent>();
  // sessionId → set of subscriber callbacks for agent events
  const sessionSubscribers = new Map<string, Set<(event: unknown) => void>>();

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

  function handleOpen(_ws: WsHandle): void {
    // Connection is not usable until a register frame arrives.
  }

  function handleMessage(ws: WsHandle, data: string): void {
    let frame: SidecarFrame;
    try {
      frame = JSON.parse(data) as SidecarFrame;
    } catch {
      logger.warn`Unparseable frame from sidecar connection`;
      return;
    }

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
        );
        break;
      case "challenge.response":
        handleChallengeResponse(ws, frame.responses);
        break;
      case "agent.deploy.ack":
        void handleDeployAck(frame.agentAddress, frame.publicKey);
        break;
      case "agent.error":
        rejectDeployPending(frame.agentAddress, frame.error);
        break;
      case "mail.outbound":
        handleMailOutbound(frame.rawMessage, frame.recipients);
        break;
      case "agent.event":
        onAgentEvent?.(frame.agentAddress, frame.sessionId, frame.event);
        dispatchToSubscribers(frame.sessionId, frame.event);
        break;
      case "session.ack":
        resolvePending(frame.requestId);
        break;
      case "session.error":
        rejectPending(frame.requestId, frame.error);
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
      timer,
    });

    conn.send({ type: "challenge", challenges: challengeEntries });
  }

  function handleChallengeResponse(
    ws: WsHandle,
    responses: { address: string; signature: string }[],
  ): void {
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

    // Add verified addresses to the routing table and flush queued messages.
    for (const addr of verified) {
      conn.agentAddresses.add(addr);
      addressIndex.set(addr, ws);
      flushDisconnectedQueue(addr, conn);
    }

    logger.info`Sidecar ${challenge.sidecarId} reconnected with ${String(verified.length)} verified agent(s)`;
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
        // sidecar is disconnected. The entry expires after the TTL.
        const timer = setTimeout(() => {
          disconnectedAgents.delete(addr);
        }, disconnectQueueTTLMs);
        disconnectedAgents.set(addr, { queue: [], timer });
      }
    }
    connections.delete(ws);

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
      });
    });
  }

  function findSidecarForNewAgent(_agentAddress: string): WsHandle | undefined {
    const first = connections.entries().next();
    if (first.done) return undefined;
    return first.value[0];
  }

  function sendAgentUndeploy(agentAddress: string, reason: string): void {
    const ws = addressIndex.get(agentAddress);
    if (ws === undefined) {
      throw new Error(`No sidecar connected for agent "${agentAddress}"`);
    }
    const conn = connections.get(ws);
    if (conn === undefined) {
      throw new Error(`No sidecar connected for agent "${agentAddress}"`);
    }

    conn.send({
      type: "agent.undeploy",
      agentAddress,
      reason,
    });

    removeAgentAddress(ws, agentAddress);
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

  async function sendMessage(
    agentAddress: string,
    sessionId: string,
    content: string,
    attachments?: WireAttachment[],
  ): Promise<void> {
    // If the agent is disconnected, queue for delivery on reconnect.
    if (!addressIndex.has(agentAddress)) {
      const frame: HubFrame = {
        type: "message.send",
        requestId: nextRequestId(),
        agentAddress,
        sessionId,
        content,
        ...(attachments !== undefined ? { attachments } : {}),
      };
      if (enqueueForDisconnected(agentAddress, frame)) return;
    }

    await sendRequest(agentAddress, (requestId) => ({
      type: "message.send",
      requestId,
      agentAddress,
      sessionId,
      content,
      ...(attachments !== undefined ? { attachments } : {}),
    }));
  }

  function removeAgentAddress(ws: WsHandle, agentAddress: string): void {
    addressIndex.delete(agentAddress);
    const conn = connections.get(ws);
    if (conn !== undefined) {
      conn.agentAddresses.delete(agentAddress);
    }
  }

  function dispatchToSubscribers(sessionId: string, event: unknown): void {
    const subs = sessionSubscribers.get(sessionId);
    if (subs === undefined) return;
    for (const cb of [...subs]) {
      try {
        cb(event);
      } catch (err) {
        logger.warn`Session subscriber threw: ${err instanceof Error ? err.message : String(err)}`;
      }
    }
  }

  function subscribeSession(
    sessionId: string,
    callback: (event: unknown) => void,
  ): () => void {
    let subs = sessionSubscribers.get(sessionId);
    if (subs === undefined) {
      subs = new Set();
      sessionSubscribers.set(sessionId, subs);
    }
    subs.add(callback);
    return () => {
      const current = sessionSubscribers.get(sessionId);
      if (current === undefined) return;
      current.delete(callback);
      if (current.size === 0) {
        sessionSubscribers.delete(sessionId);
      }
    };
  }

  function getConnectedSidecars(): string[] {
    return Array.from(connections.values()).map((c) => c.sidecarId);
  }

  function getRoutableAddresses(): string[] {
    return Array.from(addressIndex.keys());
  }

  return {
    handleOpen,
    handleMessage,
    handleClose,
    routeMail,
    sendAgentDeploy,
    sendAgentUndeploy,
    sendSessionAbort,
    sendMessage,
    subscribeSession,
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

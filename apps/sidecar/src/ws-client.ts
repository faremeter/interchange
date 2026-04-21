// Websocket client connecting the sidecar to the hub.
//
// Sends a register or reconnect frame on connect, forwards outbound mail
// and inference events, and handles inbound agent lifecycle commands from
// the hub. Owns per-agent key material for challenge/response signing.

import { sign as nodeSign } from "node:crypto";
import { getLogger } from "@interchange/log";
import { importPrivateKeyBytes } from "@interchange/crypto-node";
import type { InMemoryTransport } from "@interchange/message-memory";
import type {
  HubFrame,
  SidecarFrame,
  AgentDeployFrame,
  AgentUndeployFrame,
  ChallengeFrame,
  ChallengeFailedFrame,
  SessionAbortFrame,
  MessageSendFrame,
  GrantsUpdateFrame,
  PackPushFrame,
  PackDoneFrame,
  PackAckFrame,
  PackRejectFrame,
  SyncRequestFrame,
} from "@interchange/types/sidecar";
import type { InboundMessage, KeyPair } from "@interchange/types/runtime";

import type { SessionManager, SessionEventSink } from "./session-manager";
import { createPackReceiver, chunkPack } from "@interchange/pack-transport";
import { hexEncode } from "./key-store";

const logger = getLogger(["interchange", "sidecar", "ws"]);

const DEFAULT_PING_INTERVAL_MS = 30_000;

export type WsClientConfig = {
  hubUrl: string;
  sidecarId: string;
  token: string;
  transport: InMemoryTransport;
  sessions: SessionManager;
  pingIntervalMs?: number;
};

export type WsClient = {
  connect(): void;
  close(): void;
  sendEvent: SessionEventSink;
};

export function createWsClient(config: WsClientConfig): WsClient {
  const {
    hubUrl,
    sidecarId,
    token,
    transport,
    sessions,
    pingIntervalMs = DEFAULT_PING_INTERVAL_MS,
  } = config;

  let ws: WebSocket | null = null;
  let closed = false;
  let pingTimer: ReturnType<typeof setInterval> | null = null;
  let lastPongAt = 0;

  // Per-agent key pairs for challenge signing.
  // Populated by restoreSessions on connect, augmented on agent.deploy.
  const agentKeys = new Map<string, KeyPair>();

  const packReceiver = createPackReceiver();

  // Outbound frames queued while disconnected.
  const MAX_QUEUE = 1024;
  const queue: SidecarFrame[] = [];

  function send(frame: SidecarFrame): void {
    if (ws !== null && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(frame));
      return;
    }
    if (queue.length >= MAX_QUEUE) {
      logger.warn`Outbound queue full, dropping oldest frame`;
      queue.shift();
    }
    queue.push(frame);
  }

  function flush(): void {
    while (
      queue.length > 0 &&
      ws !== null &&
      ws.readyState === WebSocket.OPEN
    ) {
      ws.send(JSON.stringify(queue.shift()));
    }
  }

  // Wire the transport's remote send handler to push mail.outbound frames.
  transport.setRemoteSendHandler(async (rawMessage, recipients) => {
    const encoded = uint8ArrayToBase64(rawMessage);
    send({
      type: "mail.outbound",
      rawMessage: encoded,
      recipients,
    });
  });

  async function handleAgentDeploy(frame: AgentDeployFrame): Promise<void> {
    try {
      const result = await sessions.createSession(frame.config);
      agentKeys.set(frame.agentAddress, result.keyPair);
      send({
        type: "agent.deploy.ack",
        agentAddress: frame.agentAddress,
        publicKey: result.publicKey,
      });
      logger.info`Deployed agent ${frame.agentAddress} (session ${result.sessionId})`;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      send({
        type: "agent.error",
        agentAddress: frame.agentAddress,
        error: message,
      });
    }
  }

  function handleAgentUndeploy(frame: AgentUndeployFrame): void {
    try {
      sessions.destroySession(frame.agentAddress);
      agentKeys.delete(frame.agentAddress);
      logger.info`Undeployed agent ${frame.agentAddress}: ${frame.reason}`;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.warn`Failed to undeploy ${frame.agentAddress}: ${msg}`;
    }
  }

  function handleChallenge(frame: ChallengeFrame): void {
    const responses: { address: string; signature: string }[] = [];

    for (const { address, nonce } of frame.challenges) {
      const keys = agentKeys.get(address);
      if (keys === undefined) {
        logger.warn`No key pair for challenged address ${address}`;
        continue;
      }

      const nonceBytes = hexDecode(nonce);
      const addressBytes = new TextEncoder().encode(address);
      const payload = new Uint8Array(nonceBytes.length + addressBytes.length);
      payload.set(nonceBytes);
      payload.set(addressBytes, nonceBytes.length);

      const privateKey = importPrivateKeyBytes(keys.privateKey);
      const sig = nodeSign(null, payload, privateKey);

      responses.push({
        address,
        signature: hexEncode(new Uint8Array(sig)),
      });
    }

    send({ type: "challenge.response", responses });
  }

  function handleChallengeFailed(frame: ChallengeFailedFrame): void {
    agentKeys.delete(frame.address);
    logger.warn`Challenge failed for ${frame.address}: ${frame.reason}`;
  }

  function handleSessionAbort(frame: SessionAbortFrame): void {
    try {
      sessions.abortSession(frame.agentAddress, frame.reason);
      send({ type: "session.ack", requestId: frame.requestId });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      send({
        type: "session.error",
        requestId: frame.requestId,
        error: message,
      });
    }
  }

  async function handleGrantsUpdate(frame: GrantsUpdateFrame): Promise<void> {
    try {
      await sessions.updateGrants(frame.agentAddress, frame.grants);
      send({ type: "session.ack", requestId: frame.requestId });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      send({
        type: "session.error",
        requestId: frame.requestId,
        error: message,
      });
    }
  }

  function handleMessageSend(frame: MessageSendFrame): void {
    try {
      if (frame.attachments !== undefined && frame.attachments.length > 0) {
        logger.warn`Dropping ${String(frame.attachments.length)} attachment(s) from message.send for ${frame.agentAddress}: URL-to-bytes fetch not implemented`;
      }
      const inbound = buildInboundMessage(frame);
      sessions.deliverMessage(frame.agentAddress, inbound);
      send({ type: "session.ack", requestId: frame.requestId });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      send({
        type: "session.error",
        requestId: frame.requestId,
        error: message,
      });
    }
  }

  function handlePackPush(frame: PackPushFrame): void {
    const reason = packReceiver.handlePush(frame);
    if (reason !== null) {
      send({
        type: "pack.reject",
        agentAddress: frame.agentAddress,
        transferId: frame.transferId,
        reason,
      });
    }
  }

  async function handlePackDone(frame: PackDoneFrame): Promise<void> {
    const result = packReceiver.handleDone(frame);
    if (result === null) {
      send({
        type: "pack.reject",
        agentAddress: frame.agentAddress,
        transferId: frame.transferId,
        reason: "corrupt",
      });
      return;
    }

    try {
      await sessions.applyDeployPack(
        frame.agentAddress,
        result.pack,
        result.ref,
        result.commitSha,
        frame.transferId,
      );
      send({
        type: "pack.ack",
        agentAddress: frame.agentAddress,
        transferId: frame.transferId,
      });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      const reason = msg.startsWith("sha_mismatch")
        ? "sha_mismatch"
        : "corrupt";
      logger.warn`Pack apply failed for ${frame.agentAddress}: ${msg}`;
      send({
        type: "pack.reject",
        agentAddress: frame.agentAddress,
        transferId: frame.transferId,
        reason,
      });
    }
  }

  // Tracks outbound state pack transfers (sidecar → hub).
  const pendingStatePacks = new Map<
    string,
    { resolve(): void; reject(error: string): void }
  >();

  async function handleSyncRequest(frame: SyncRequestFrame): Promise<void> {
    const { agentAddress, transferId } = frame;
    try {
      const { pack, commitSha, ref } =
        await sessions.createStatePack(agentAddress);

      // Register pending entry before sending frames so that a synchronous
      // pack.ack (e.g. in tests or loopback transports) resolves correctly.
      const ackPromise = new Promise<void>((resolve, reject) => {
        pendingStatePacks.set(transferId, {
          resolve,
          reject(error: string) {
            reject(new Error(error));
          },
        });
      });

      for (const chunk of chunkPack(pack)) {
        send({
          type: "pack.push",
          agentAddress,
          transferId,
          seq: chunk.seq,
          data: chunk.data,
        });
      }

      send({
        type: "pack.done",
        agentAddress,
        transferId,
        ref,
        commitSha,
      });

      await ackPromise;

      logger.info`State push complete for ${agentAddress} (${commitSha.slice(0, 8)})`;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.warn`State push failed for ${agentAddress}: ${msg}`;
    }
  }

  function handlePackAck(frame: PackAckFrame): void {
    const entry = pendingStatePacks.get(frame.transferId);
    if (entry === undefined) {
      logger.warn`Received pack.ack for unknown transferId ${frame.transferId}`;
      return;
    }
    pendingStatePacks.delete(frame.transferId);
    entry.resolve();
  }

  function handlePackReject(frame: PackRejectFrame): void {
    const entry = pendingStatePacks.get(frame.transferId);
    if (entry === undefined) {
      logger.warn`Received pack.reject for unknown transferId ${frame.transferId}`;
      return;
    }
    pendingStatePacks.delete(frame.transferId);
    entry.reject(`Pack rejected by hub: ${frame.reason}`);
  }

  async function handleMessage(data: string): Promise<void> {
    let frame: HubFrame;
    try {
      frame = JSON.parse(data) as HubFrame;
    } catch {
      logger.warn`Received unparseable frame from hub`;
      return;
    }

    switch (frame.type) {
      case "mail.inbound": {
        const rawBytes = base64ToUint8Array(frame.rawMessage);
        deliverLocalMail(frame.agentAddress, rawBytes);
        break;
      }
      case "agent.deploy":
        await handleAgentDeploy(frame);
        break;
      case "agent.undeploy":
        handleAgentUndeploy(frame);
        break;
      case "challenge":
        handleChallenge(frame);
        break;
      case "pong":
        lastPongAt = Date.now();
        break;
      case "challenge.failed":
        handleChallengeFailed(frame);
        break;
      case "session.abort":
        handleSessionAbort(frame);
        break;
      case "message.send":
        handleMessageSend(frame);
        break;
      case "grants.update":
        await handleGrantsUpdate(frame);
        break;
      case "pack.push":
        handlePackPush(frame);
        break;
      case "pack.done":
        await handlePackDone(frame);
        break;
      case "sync.request":
        void handleSyncRequest(frame);
        break;
      case "pack.ack":
        handlePackAck(frame);
        break;
      case "pack.reject":
        handlePackReject(frame);
        break;
      default:
        logger.warn`Unknown frame type from hub: ${(frame as { type: string }).type}`;
    }
  }

  function deliverLocalMail(agentAddress: string, message: Uint8Array): void {
    try {
      transport.deliver(agentAddress, message);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.warn`Failed to deliver inbound mail to ${agentAddress}: ${msg}`;
    }
  }

  function connect(): void {
    if (closed) return;

    ws = new WebSocket(hubUrl);

    ws.addEventListener("open", () => {
      logger.info`Connected to hub at ${hubUrl}`;

      // Start keepalive pings. Record the current time as the initial
      // pong timestamp so the first interval check doesn't immediately
      // declare the connection dead.
      lastPongAt = Date.now();
      pingTimer = setInterval(() => {
        if (Date.now() - lastPongAt >= pingIntervalMs * 2) {
          logger.warn`Hub pong timeout, closing connection`;
          // Clear the timer immediately so it doesn't keep firing while
          // the close event is pending.
          if (pingTimer !== null) {
            clearInterval(pingTimer);
            pingTimer = null;
          }
          // Close the socket directly (not the exported close()) so the
          // close event listener triggers reconnection.
          ws?.close();
          return;
        }
        send({ type: "ping" });
      }, pingIntervalMs);

      // Clear any in-flight pack transfers from the previous connection.
      packReceiver.reset();
      for (const [id, entry] of pendingStatePacks) {
        pendingStatePacks.delete(id);
        entry.reject("Connection lost");
      }

      // Restore sessions from disk before announcing to the hub.
      // Harnesses must be running before the hub starts routing messages.
      void (async () => {
        try {
          const { restored, failed } = await sessions.restoreSessions();

          if (restored.length > 0) {
            for (const entry of restored) {
              agentKeys.set(entry.address, entry.keyPair);
            }
            send({
              type: "reconnect",
              sidecarId,
              token,
              agentAddresses: restored.map((e) => e.address),
            });
            if (failed.length > 0) {
              logger.warn`Reconnected ${String(restored.length)} agent(s), ${String(failed.length)} failed to restore`;
            } else {
              logger.info`Sent reconnect with ${String(restored.length)} agent(s)`;
            }
          } else {
            send({
              type: "register",
              sidecarId,
              token,
              agentAddresses: sessions.getAddresses(),
            });
          }

          flush();
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          logger.error`Session restore failed, closing connection: ${msg}`;
          ws?.close();
        }
      })();
    });

    ws.addEventListener("message", (event) => {
      if (typeof event.data === "string") {
        void handleMessage(event.data);
      }
    });

    ws.addEventListener("close", () => {
      logger.info`Disconnected from hub`;
      ws = null;
      if (pingTimer !== null) {
        clearInterval(pingTimer);
        pingTimer = null;
      }
      if (!closed) {
        setTimeout(() => connect(), 3000);
      }
    });

    ws.addEventListener("error", (event) => {
      logger.warn`WebSocket error: ${String(event)}`;
    });
  }

  function close(): void {
    closed = true;
    if (pingTimer !== null) {
      clearInterval(pingTimer);
      pingTimer = null;
    }
    if (ws !== null) {
      ws.close();
      ws = null;
    }
  }

  const sendEvent: SessionEventSink = (agentAddress, sessionId, event) => {
    send({
      type: "agent.event",
      agentAddress,
      sessionId,
      event,
    });
  };

  return { connect, close, sendEvent };
}

function uint8ArrayToBase64(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary);
}

function base64ToUint8Array(base64: string): Uint8Array {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}

function hexDecode(hex: string): Uint8Array {
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(hex.substring(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}

// Synthesize an InboundMessage from a hub message.send frame.
function buildInboundMessage(frame: MessageSendFrame): InboundMessage {
  return {
    ref: { uid: 0, mailbox: "SYNTHETIC" },
    headers: {
      from: "user@hub",
      to: [frame.agentAddress],
      date: new Date().toISOString(),
      messageId: `<${crypto.randomUUID()}@hub>`,
      subject: "User message",
      interchangeSessionId: frame.sessionId,
    },
    flags: [],
    content: frame.content,
    signatureStatus: "missing",
  };
}

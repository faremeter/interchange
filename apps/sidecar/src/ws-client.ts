// Websocket client connecting the sidecar to the hub.
//
// Sends a register or reconnect frame on connect, forwards outbound mail
// and inference events, and handles inbound agent lifecycle commands from
// the hub. Owns per-agent key material for challenge/response signing.

import { sign as nodeSign } from "node:crypto";
import { getLogger } from "@interchange/log";
import {
  importPrivateKeyBytes,
  verifySshSignature,
} from "@interchange/crypto-node";
import type { InMemoryTransport } from "@interchange/mail-memory";
import { type } from "arktype";
import {
  HubFrame,
  type SidecarFrame,
  type AgentDeployFrame,
  type AgentUndeployFrame,
  type ChallengeFrame,
  type ChallengeFailedFrame,
  type SessionAbortFrame,
  type SessionStartFrame,
  type GrantsUpdateFrame,
  type ProvidersUpdateFrame,
  type PackPushFrame,
  type PackDoneFrame,
  type PackAckFrame,
  type PackRejectFrame,
  type SyncRequestFrame,
} from "@interchange/types/sidecar";
import type { KeyPair } from "@interchange/types/runtime";

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

  // Hub's signing public key per agent, for deploy commit verification.
  const hubKeys = new Map<string, Uint8Array>();

  const packReceiver = createPackReceiver();

  // Serialize frame processing so async handlers (deploy, undeploy, abort)
  // cannot race against each other.
  let messageQueue: Promise<void> = Promise.resolve();

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

  // Wire the transport's remote send handler to push mail.outbound frames
  // for routing. These carry only the raw message and recipients — the hub
  // routes them to the destination sidecar.
  transport.setRemoteSendHandler(async (rawMessage, recipients) => {
    const encoded = uint8ArrayToBase64(rawMessage);
    send({
      type: "mail.outbound",
      rawMessage: encoded,
      recipients,
    });
  });

  // Forward every send to the hub for audit and event emission. Local-only
  // sends are marked delivered: true so the hub does not re-route them.
  // Remote sends are marked delivered: true as well — routing was already
  // handled by the RemoteSendHandler above.
  transport.addMessageSentHandler(async (ctx) => {
    const encoded = uint8ArrayToBase64(ctx.rawMessage);
    const sessionId = sessions.getSessionId(ctx.senderAddress);
    send({
      type: "mail.outbound",
      rawMessage: encoded,
      recipients: ctx.recipients,
      senderAddress: ctx.senderAddress,
      ...(sessionId !== undefined ? { sessionId } : {}),
      messageId: ctx.messageId,
      to: ctx.to,
      ...(ctx.cc.length > 0 ? { cc: ctx.cc } : {}),
      delivered: true,
    });
  });

  async function handleAgentDeploy(frame: AgentDeployFrame): Promise<void> {
    try {
      const result = await sessions.provisionAgent(frame.config);
      agentKeys.set(frame.agentAddress, result.keyPair);
      hubKeys.set(frame.agentAddress, hexDecode(frame.hubPublicKey));
      await sessions.persistHubPublicKey(
        frame.agentAddress,
        frame.hubPublicKey,
      );
      send({
        type: "agent.deploy.ack",
        agentAddress: frame.agentAddress,
        publicKey: result.publicKey,
      });
      logger.info`Provisioned agent ${frame.agentAddress}`;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      send({
        type: "agent.error",
        agentAddress: frame.agentAddress,
        error: message,
      });
    }
  }

  async function handleSessionStart(frame: SessionStartFrame): Promise<void> {
    try {
      await sessions.startSession(frame.agentAddress);
      send({
        type: "session.start.ack",
        agentAddress: frame.agentAddress,
      });
      logger.info`Started session for ${frame.agentAddress}`;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      send({
        type: "agent.error",
        agentAddress: frame.agentAddress,
        error: message,
      });
    }
  }

  async function handleAgentUndeploy(frame: AgentUndeployFrame): Promise<void> {
    let statePushed = false;

    // Stop the harness first.
    try {
      await sessions.destroySession(frame.agentAddress);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.warn`Failed to stop session for ${frame.agentAddress}: ${msg}`;
    }

    // Best-effort state push to the hub before deleting the directory.
    // statePushed reflects whether we sent the pack frames, not whether
    // the hub acknowledged them. We intentionally skip waiting for
    // pack.ack here to avoid blocking the undeploy on a round-trip that
    // may never complete if the hub is shutting down.
    try {
      const { pack, commitSha, ref } = await sessions.createStatePack(
        frame.agentAddress,
      );

      for (const chunk of chunkPack(pack)) {
        send({
          type: "pack.push",
          agentAddress: frame.agentAddress,
          transferId: `undeploy-${frame.agentAddress}`,
          seq: chunk.seq,
          data: chunk.data,
        });
      }
      send({
        type: "pack.done",
        agentAddress: frame.agentAddress,
        transferId: `undeploy-${frame.agentAddress}`,
        ref,
        commitSha,
      });

      statePushed = true;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.warn`State push failed for ${frame.agentAddress}: ${msg}`;
    }

    // Delete the agent directory.
    try {
      await sessions.deleteAgentDir(frame.agentAddress);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.warn`Failed to delete agent directory for ${frame.agentAddress}: ${msg}`;
    }

    agentKeys.delete(frame.agentAddress);
    hubKeys.delete(frame.agentAddress);

    send({
      type: "agent.undeploy.ack",
      agentAddress: frame.agentAddress,
      statePushed,
    });
    logger.info`Undeployed agent ${frame.agentAddress}: ${frame.reason}`;
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

  async function handleChallengeFailed(
    frame: ChallengeFailedFrame,
  ): Promise<void> {
    agentKeys.delete(frame.address);
    hubKeys.delete(frame.address);

    // The hub rejected this agent during reconnect — tear it down so
    // the address is freed for future deploys.
    try {
      await sessions.destroySession(frame.address);
    } catch {
      // Ignore — the agent may not have a running session.
    }

    logger.warn`Challenge failed for ${frame.address}, agent torn down: ${frame.reason}`;
  }

  async function handleSessionAbort(frame: SessionAbortFrame): Promise<void> {
    try {
      await sessions.abortSession(frame.agentAddress, frame.reason);
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

  async function handleProvidersUpdate(
    frame: ProvidersUpdateFrame,
  ): Promise<void> {
    try {
      await sessions.updateProviders(frame.agentAddress, frame.providers);
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
      const hubKey = hubKeys.get(frame.agentAddress);
      if (hubKey === undefined) {
        throw new Error("signature_invalid: no hub public key for agent");
      }
      const verifyCommit = (payload: string, signature: string) =>
        verifySshSignature(payload, signature, hubKey);

      await sessions.applyDeployPack(
        frame.agentAddress,
        result.pack,
        result.ref,
        result.commitSha,
        frame.transferId,
        verifyCommit,
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
        : msg.startsWith("signature_invalid") ||
            msg.startsWith("signature_unsigned")
          ? "signature_invalid"
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
    let raw: unknown;
    try {
      raw = JSON.parse(data) as unknown;
    } catch {
      logger.warn`Received unparseable frame from hub`;
      return;
    }
    const validated = HubFrame(raw);
    if (validated instanceof type.errors) {
      logger.warn`Invalid hub frame: ${validated.summary}`;
      return;
    }
    const frame = validated;

    switch (frame.type) {
      case "mail.inbound": {
        const rawBytes = base64ToUint8Array(frame.rawMessage);
        deliverLocalMail(frame.agentAddress, rawBytes);
        void sessions.commitInboundMail(frame.agentAddress, rawBytes);
        break;
      }
      case "agent.deploy":
        await handleAgentDeploy(frame);
        break;
      case "session.start":
        await handleSessionStart(frame);
        break;
      case "agent.undeploy":
        await handleAgentUndeploy(frame);
        break;
      case "challenge":
        handleChallenge(frame);
        break;
      case "pong":
        lastPongAt = Date.now();
        break;
      case "challenge.failed":
        await handleChallengeFailed(frame);
        break;
      case "session.abort":
        await handleSessionAbort(frame);
        break;
      case "grants.update":
        await handleGrantsUpdate(frame);
        break;
      case "providers.update":
        await handleProvidersUpdate(frame);
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

      lastPongAt = Date.now();
      pingTimer = setInterval(() => {
        if (Date.now() - lastPongAt >= pingIntervalMs * 2) {
          logger.warn`Hub pong timeout, closing connection`;
          if (pingTimer !== null) {
            clearInterval(pingTimer);
            pingTimer = null;
          }
          ws?.close();
          return;
        }
        send({ type: "ping" });
      }, pingIntervalMs);

      packReceiver.reset();
      for (const [id, entry] of pendingStatePacks) {
        pendingStatePacks.delete(id);
        entry.reject("Connection lost");
      }

      void (async () => {
        try {
          const { restored, failed } = await sessions.restoreSessions();

          if (restored.length > 0) {
            const deployRefs: Record<string, string> = {};
            for (const entry of restored) {
              agentKeys.set(entry.address, entry.keyPair);
              if (entry.hubPublicKey !== undefined) {
                hubKeys.set(entry.address, hexDecode(entry.hubPublicKey));
              }
              const ref = await sessions.getDeployRef(entry.address);
              if (ref !== null) {
                deployRefs[entry.address] = ref;
              }
            }
            send({
              type: "reconnect",
              sidecarId,
              token,
              agentAddresses: restored.map((e) => e.address),
              deployRefs,
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
        messageQueue = messageQueue.then(() => handleMessage(event.data));
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
  if (hex.length % 2 !== 0) {
    throw new Error(`hexDecode: odd-length input (${hex.length} chars)`);
  }
  if (!/^[0-9a-fA-F]*$/.test(hex)) {
    throw new Error("hexDecode: input contains non-hex characters");
  }
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(hex.substring(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}

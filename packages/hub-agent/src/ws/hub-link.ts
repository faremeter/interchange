// HubLink: the sidecar-side WebSocket protocol.
//
// Connects to the hub, sends the register or reconnect frame, forwards
// outbound mail and inference events, and handles inbound agent
// lifecycle commands. Per-agent key material lives on AgentKeyStore;
// the link calls into the store for challenge signing, deploy-commit
// verification, and hub-key bookkeeping. The wire layer itself never
// touches raw key bytes.

import { getLogger } from "@intx/log";
import type { HubTransport } from "@intx/mail-memory";
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
  type SourcesUpdateFrame,
  type PackPushFrame,
  type PackDoneFrame,
  type PackAckFrame,
  type PackRejectFrame,
  type RepoId,
  type SyncRequestFrame,
} from "@intx/types/sidecar";
import { createPackReceiver, chunkPack } from "@intx/pack-transport";
import { base64Decode, base64Encode, hexDecode, hexEncode } from "@intx/types";

import type { AgentKeyStore } from "../agent-key-store";
import type {
  ConnectorStateSink,
  SessionEventSink,
  SessionManager,
} from "../session-manager";

const logger = getLogger(["interchange", "hub-agent", "ws"]);

const DEFAULT_PING_INTERVAL_MS = 30_000;
const DEFAULT_RECONNECT_DELAY_MS = 3_000;

/**
 * Schedules a deferred callback and returns a cancel function. Injection
 * point for tests: a fake scheduler records the callback so the test
 * can observe whether cancellation actually happened, without relying
 * on wall-clock waits.
 */
export type ReconnectScheduler = (
  callback: () => void,
  delayMs: number,
) => () => void;

const defaultScheduleReconnect: ReconnectScheduler = (callback, delayMs) => {
  const handle = setTimeout(callback, delayMs);
  return () => {
    clearTimeout(handle);
  };
};

export type HubLinkConfig = {
  hubURL: string;
  sidecarId: string;
  token: string;
  transport: HubTransport;
  sessions: SessionManager;
  /**
   * Key custody and per-frame crypto. HubLink calls into the store for
   * challenge signing, deploy-commit verification, hub-key recording,
   * and per-agent forgetting; it does not maintain its own copy of
   * those tables.
   */
  keyStore: AgentKeyStore;
  pingIntervalMs?: number;
  reconnectDelayMs?: number;
  scheduleReconnect?: ReconnectScheduler;
};

export type HubLink = {
  /**
   * Open the connection. Must not be called after `close()`; calling it
   * on a closed client throws.
   */
  connect(): void;
  close(): void;
  sendEvent: SessionEventSink;
  sendConnectorState: ConnectorStateSink;
};

export function createHubLink(config: HubLinkConfig): HubLink {
  const {
    hubURL,
    sidecarId,
    token,
    transport,
    sessions,
    keyStore,
    pingIntervalMs = DEFAULT_PING_INTERVAL_MS,
    reconnectDelayMs = DEFAULT_RECONNECT_DELAY_MS,
    scheduleReconnect = defaultScheduleReconnect,
  } = config;

  let ws: WebSocket | null = null;
  let closed = false;
  let pingTimer: ReturnType<typeof setInterval> | null = null;
  let cancelReconnect: (() => void) | null = null;
  let lastPongAt = 0;

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
    const encoded = base64Encode(rawMessage);
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
    const encoded = base64Encode(ctx.rawMessage);
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
      // SessionManager.provisionAgent already populated AgentKeyStore's
      // in-memory keypair cache via keyStore.loadOrGenerateKey. We
      // record the hub-side pairing key here so subsequent deploy
      // pack frames have a verifier ready.
      keyStore.recordHubKey(frame.agentAddress, frame.hubPublicKey);
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
    // repo.pack.ack here to avoid blocking the undeploy on a round-trip
    // that may never complete if the hub is shutting down.
    try {
      const { pack, commitSha, ref } = await sessions.createStatePack(
        frame.agentAddress,
      );
      const repoId: RepoId = {
        kind: "agent-state",
        id: frame.agentAddress,
      };

      for (const chunk of chunkPack(pack)) {
        send({
          type: "repo.pack.push",
          agentAddress: frame.agentAddress,
          repoId,
          transferId: `undeploy-${frame.agentAddress}`,
          seq: chunk.seq,
          data: chunk.data,
        });
      }
      send({
        type: "repo.pack.done",
        agentAddress: frame.agentAddress,
        repoId,
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

    keyStore.forgetAgent(frame.agentAddress);

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
      const nonceBytes = hexDecode(nonce);
      const addressBytes = new TextEncoder().encode(address);
      const payload = new Uint8Array(nonceBytes.length + addressBytes.length);
      payload.set(nonceBytes);
      payload.set(addressBytes, nonceBytes.length);

      const sig = keyStore.signChallenge(address, payload);
      if (sig === null) {
        logger.warn`No key pair for challenged address ${address}`;
        continue;
      }

      responses.push({
        address,
        signature: hexEncode(sig),
      });
    }

    send({ type: "challenge.response", responses });
  }

  async function handleChallengeFailed(
    frame: ChallengeFailedFrame,
  ): Promise<void> {
    // The hub rejected this agent during reconnect — tear it down so
    // the address is freed for future deploys. The agent may not have
    // an active session (provisioned but never started, or already
    // destroyed by a concurrent path), so any error from destroySession
    // is logged but does not abort the wire layer. Destroy first so any
    // disposer or mail-commit code still has the agent's crypto handle;
    // forget the key material only once the session lifecycle methods
    // have returned.
    try {
      await sessions.destroySession(frame.address);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.warn`destroySession during challenge.failed for ${frame.address}: ${msg}`;
    }
    keyStore.forgetAgent(frame.address);

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

  async function handleSourcesUpdate(frame: SourcesUpdateFrame): Promise<void> {
    try {
      await sessions.updateSources(
        frame.agentAddress,
        frame.sources,
        frame.defaultSource,
      );
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
        type: "repo.pack.reject",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
        reason,
      });
    }
  }

  async function handlePackDone(frame: PackDoneFrame): Promise<void> {
    const result = packReceiver.handleDone(frame);
    if (result === null) {
      send({
        type: "repo.pack.reject",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
        reason: "corrupt",
      });
      return;
    }

    try {
      if (frame.mountPath !== undefined) {
        // Asset pack: route to the workspace materializer. Use
        // frame.agentAddress for destination routing — frame.repoId.id
        // names the source asset at the hub, which is a different
        // entity than the destination agent.
        await sessions.applyAssetPack(
          frame.agentAddress,
          frame.mountPath,
          result.pack,
          result.ref,
          result.commitSha,
        );
      } else {
        const verifyCommit = (payload: string, signature: string) =>
          keyStore.verifyDeployCommit(frame.agentAddress, payload, signature);

        await sessions.applyDeployPack(
          frame.agentAddress,
          result.pack,
          result.ref,
          result.commitSha,
          frame.transferId,
          verifyCommit,
        );
      }
      send({
        type: "repo.pack.ack",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
      });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      // asset_materialization_failed errors mirror deploy materialization
      // errors into the same `corrupt` bucket — finer-grained
      // classification is out of scope for v1 asset packs.
      const reason = msg.startsWith("sha_mismatch")
        ? "sha_mismatch"
        : msg.startsWith("signature_invalid") ||
            msg.startsWith("signature_unsigned")
          ? "signature_invalid"
          : "corrupt";
      logger.warn`Pack apply failed for ${frame.agentAddress}: ${msg}`;
      send({
        type: "repo.pack.reject",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
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
      const repoId: RepoId = { kind: "agent-state", id: agentAddress };

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
          type: "repo.pack.push",
          agentAddress,
          repoId,
          transferId,
          seq: chunk.seq,
          data: chunk.data,
        });
      }

      send({
        type: "repo.pack.done",
        agentAddress,
        repoId,
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
      logger.warn`Received repo.pack.ack for unknown transferId ${frame.transferId}`;
      return;
    }
    pendingStatePacks.delete(frame.transferId);
    entry.resolve();
  }

  function handlePackReject(frame: PackRejectFrame): void {
    const entry = pendingStatePacks.get(frame.transferId);
    if (entry === undefined) {
      logger.warn`Received repo.pack.reject for unknown transferId ${frame.transferId}`;
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
        const rawBytes = base64Decode(frame.rawMessage);
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
      case "sources.update":
        await handleSourcesUpdate(frame);
        break;
      case "repo.pack.push":
        handlePackPush(frame);
        break;
      case "repo.pack.done":
        await handlePackDone(frame);
        break;
      case "sync.request":
        void handleSyncRequest(frame);
        break;
      case "repo.pack.ack":
        handlePackAck(frame);
        break;
      case "repo.pack.reject":
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
    // Reconnect cancellation in close() is the load-bearing protection
    // against post-close reconnect attempts. A caller invoking connect()
    // after close() is a misuse, not a recoverable state — fail loudly.
    if (closed) {
      throw new Error("HubLink.connect called after close");
    }

    ws = new WebSocket(hubURL);

    ws.addEventListener("open", () => {
      logger.info`Connected to hub at ${hubURL}`;

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
              // SessionManager.restoreSessions populated AgentKeyStore's
              // in-memory keypair cache via scanKeys. Replay the
              // hub-side pairing record here so verifyDeployCommit can
              // accept incoming packs without re-running an agent.deploy.
              if (entry.hubPublicKey !== undefined) {
                keyStore.recordHubKey(entry.address, entry.hubPublicKey);
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
        cancelReconnect = scheduleReconnect(() => {
          cancelReconnect = null;
          // Defense in depth for fake or misbehaving schedulers whose
          // cancel function is a no-op: re-check `closed` before
          // re-entering connect() so a fired-but-not-yet-executed
          // callback after close() does not propagate the
          // "called after close" throw out of the scheduler.
          if (closed) return;
          connect();
        }, reconnectDelayMs);
      }
    });

    ws.addEventListener("error", (event) => {
      logger.warn`WebSocket error: ${String(event)}`;
    });
  }

  function close(): void {
    closed = true;
    if (cancelReconnect !== null) {
      cancelReconnect();
      cancelReconnect = null;
    }
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

  const sendConnectorState: ConnectorStateSink = (
    agentAddress,
    connectorState,
  ) => {
    send({
      type: "connector.state.changed",
      agentAddress,
      connectorState,
    });
  };

  return { connect, close, sendEvent, sendConnectorState };
}

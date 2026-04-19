// Websocket client connecting the sidecar to the hub.
//
// Sends a register frame on connect, forwards outbound mail and inference
// events, and handles inbound session lifecycle commands from the hub.

import { getLogger } from "@interchange/log";
import type { InMemoryTransport } from "@interchange/message-memory";
import type {
  HubFrame,
  SidecarFrame,
  SessionCreateFrame,
  SessionDestroyFrame,
  SessionAbortFrame,
  MessageSendFrame,
} from "@interchange/types/sidecar";
import type { InboundMessage } from "@interchange/types/runtime";

import type { SessionManager, SessionEventSink } from "./session-manager";

const logger = getLogger(["interchange", "sidecar", "ws"]);

export type WsClientConfig = {
  hubUrl: string;
  sidecarId: string;
  token: string;
  transport: InMemoryTransport;
  sessions: SessionManager;
};

export type WsClient = {
  connect(): void;
  close(): void;
  sendEvent: SessionEventSink;
};

export function createWsClient(config: WsClientConfig): WsClient {
  const { hubUrl, sidecarId, token, transport, sessions } = config;

  let ws: WebSocket | null = null;
  let closed = false;

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

  async function handleSessionCreate(frame: SessionCreateFrame): Promise<void> {
    try {
      const sessionId = await sessions.createSession(frame.config);
      send({ type: "session.ack", requestId: frame.requestId });
      logger.info`Acked session.create for ${frame.config.agentAddress} (session ${sessionId})`;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      send({
        type: "session.error",
        requestId: frame.requestId,
        error: message,
      });
    }
  }

  function handleSessionDestroy(frame: SessionDestroyFrame): void {
    try {
      sessions.destroySession(frame.agentAddress);
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
      case "session.create":
        await handleSessionCreate(frame);
        break;
      case "session.destroy":
        handleSessionDestroy(frame);
        break;
      case "session.abort":
        handleSessionAbort(frame);
        break;
      case "message.send":
        handleMessageSend(frame);
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
      send({
        type: "register",
        sidecarId,
        token,
        agentAddresses: sessions.getAddresses(),
      });
      flush();
    });

    ws.addEventListener("message", (event) => {
      if (typeof event.data === "string") {
        handleMessage(event.data);
      }
    });

    ws.addEventListener("close", () => {
      logger.info`Disconnected from hub`;
      ws = null;
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

// Synthesize an InboundMessage from a hub message.send frame. These messages
// did not arrive through IMAP, so the ref and signature fields are synthetic.
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

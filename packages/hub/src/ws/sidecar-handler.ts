// Hub-side websocket handler for sidecar connections.
//
// Accepts websocket upgrades, processes register frames, maintains a routing
// table of agentAddress → sidecar connection, and dispatches frames between
// sidecars and the hub's internal systems.

import { getLogger } from "@interchange/log";
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
  sendSessionCreate(agentAddress: string, config: HarnessConfig): Promise<void>;
  sendSessionDestroy(agentAddress: string): Promise<void>;
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
  onMailOutbound?: (rawMessage: string, recipients: string[]) => void;
  validateToken?: (sidecarId: string, token: string) => boolean;
};

// Minimal handle so the router doesn't depend on a specific WebSocket impl.
export type WsHandle = {
  send(data: string): void;
  close(): void;
};

const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;

export function createSidecarRouter(
  config: SidecarRouterConfig = {},
): SidecarRouter {
  const {
    requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS,
    onAgentEvent,
    onMailOutbound,
    validateToken,
  } = config;

  // ws handle → registered connection
  const connections = new Map<WsHandle, SidecarConnection>();
  // agentAddress → ws handle (routing table)
  const addressIndex = new Map<string, WsHandle>();
  // requestId → pending promise
  const pending = new Map<string, PendingRequest>();
  // agentAddress → number of in-flight session.create requests
  const pendingCreates = new Map<string, number>();
  // sessionId → set of subscriber callbacks for agent events
  const sessionSubscribers = new Map<string, Set<(event: unknown) => void>>();

  let requestCounter = 0;

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
    }

    logger.info`Sidecar ${sidecarId} registered with ${String(agentAddresses.length)} agents`;
  }

  function handleMailOutbound(rawMessage: string, recipients: string[]): void {
    // Route to locally connected sidecars first.
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
        pendingCreates.delete(addr);
      }
    }
    connections.delete(ws);

    // Reject any in-flight requests that were sent to this sidecar.
    for (const [requestId, req] of pending) {
      if (req.ws !== ws) continue;
      clearTimeout(req.timer);
      pending.delete(requestId);
      req.reject(`Sidecar ${conn.sidecarId} disconnected`);
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
    if (ws === undefined) return false;
    const conn = connections.get(ws);
    if (conn === undefined) return false;

    conn.send({
      type: "mail.inbound",
      agentAddress,
      rawMessage,
    });
    return true;
  }

  async function sendSessionCreate(
    agentAddress: string,
    harnessConfig: HarnessConfig,
  ): Promise<void> {
    // The hub knows the address from the config, so update the routing
    // table proactively. If the sidecar nacks, we clean up.
    const ws =
      addressIndex.get(agentAddress) ?? findSidecarForNewSession(agentAddress);

    if (ws === undefined) {
      throw new Error(`No sidecar available for agent "${agentAddress}"`);
    }

    // Update the address index so the new agent is routable immediately
    // after ack.
    const conn = connections.get(ws);
    if (conn !== undefined) {
      conn.agentAddresses.add(agentAddress);
      addressIndex.set(agentAddress, ws);
    }

    pendingCreates.set(
      agentAddress,
      (pendingCreates.get(agentAddress) ?? 0) + 1,
    );

    try {
      await sendRequest(agentAddress, (requestId) => ({
        type: "session.create",
        requestId,
        config: harnessConfig,
      }));
    } catch (err) {
      // Only roll back routing if no other create is still in flight
      // for this address and this connection still owns it.
      const remaining = (pendingCreates.get(agentAddress) ?? 1) - 1;
      if (remaining <= 0) {
        pendingCreates.delete(agentAddress);
        if (conn !== undefined && addressIndex.get(agentAddress) === ws) {
          conn.agentAddresses.delete(agentAddress);
          addressIndex.delete(agentAddress);
        }
      } else {
        pendingCreates.set(agentAddress, remaining);
      }
      throw err;
    }

    // Successful create — decrement the counter.
    const remaining = (pendingCreates.get(agentAddress) ?? 1) - 1;
    if (remaining <= 0) {
      pendingCreates.delete(agentAddress);
    } else {
      pendingCreates.set(agentAddress, remaining);
    }
  }

  // When creating a session for an agent address not yet in the routing
  // table, we need to pick a sidecar. This fallback picks the first
  // connected sidecar.
  function findSidecarForNewSession(
    _agentAddress: string,
  ): WsHandle | undefined {
    const first = connections.entries().next();
    if (first.done) return undefined;
    return first.value[0];
  }

  async function sendSessionDestroy(agentAddress: string): Promise<void> {
    const ws = addressIndex.get(agentAddress);
    await sendRequest(agentAddress, (requestId) => ({
      type: "session.destroy",
      requestId,
      agentAddress,
    }));
    if (ws !== undefined && addressIndex.get(agentAddress) === ws) {
      removeAgentAddress(ws, agentAddress);
    }
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
    sendSessionCreate,
    sendSessionDestroy,
    sendSessionAbort,
    sendMessage,
    subscribeSession,
    getConnectedSidecars,
    getRoutableAddresses,
  };
}

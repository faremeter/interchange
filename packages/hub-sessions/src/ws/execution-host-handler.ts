import { type } from "arktype";

import { sha256 } from "@intx/crypto";
import type { ExecutionHostSession, ExecutionHostSessionStore } from "@intx/db";
import { getLogger } from "@intx/log";
import {
  ExecutionHostFrame,
  type ExecutionHostAssignmentFrame,
  type ExecutionHostHubFrame,
  type ExecutionHostReleaseFrame,
} from "@intx/types";

import type { WsHandle } from "./sidecar-handler";

const logger = getLogger(["hub", "ws", "execution-host"]);

type HostConnection = {
  readonly ws: WsHandle;
  closed: boolean;
  queue: Promise<void>;
  session?: ExecutionHostSession;
  leaseTimer?: ReturnType<typeof setTimeout>;
};

type PendingHostOperation = {
  readonly connection: HostConnection;
  readonly promise: Promise<undefined>;
  readonly resolve: (value: undefined) => void;
  readonly reject: (cause: unknown) => void;
  readonly timer: ReturnType<typeof setTimeout>;
};

export type ExecutionHostControlRouter = {
  handleOpen(ws: WsHandle): void;
  handleMessage(ws: WsHandle, data: string): void;
  handleClose(ws: WsHandle): void;
  getConnectedSession(hostId: string): ExecutionHostSession | null;
  listConnectedSessions(): readonly ExecutionHostSession[];
  sendAssignment(
    session: ExecutionHostSession,
    frame: Omit<ExecutionHostAssignmentFrame, "type">,
    timeoutMs?: number,
  ): Promise<void>;
  sendRelease(
    session: ExecutionHostSession,
    frame: Omit<ExecutionHostReleaseFrame, "type">,
    timeoutMs?: number,
  ): Promise<void>;
};

export type CreateExecutionHostControlRouterOpts = {
  readonly store: ExecutionHostSessionStore;
  readonly hubInstanceId: string;
  readonly leaseDurationMs?: number;
  readonly now?: () => Date;
  readonly createSessionId?: () => string;
};

const DEFAULT_LEASE_DURATION_MS = 45_000;
const DEFAULT_OPERATION_TIMEOUT_MS = 30_000;

export function createExecutionHostControlRouter({
  store,
  hubInstanceId,
  leaseDurationMs = DEFAULT_LEASE_DURATION_MS,
  now = () => new Date(),
  createSessionId = () => `hsn_${crypto.randomUUID()}`,
}: CreateExecutionHostControlRouterOpts): ExecutionHostControlRouter {
  if (hubInstanceId.trim() === "") {
    throw new Error("Execution host control router requires a Hub instance id");
  }
  if (leaseDurationMs <= 0) {
    throw new Error("Execution host lease duration must be positive");
  }

  const connections = new Map<WsHandle, HostConnection>();
  const connectionsByHost = new Map<string, HostConnection>();
  const pendingOperations = new Map<string, PendingHostOperation>();

  function handleOpen(ws: WsHandle): void {
    connections.set(ws, { ws, closed: false, queue: Promise.resolve() });
  }

  function handleMessage(ws: WsHandle, data: string): void {
    const connection = connections.get(ws);
    if (connection === undefined || connection.closed) return;
    connection.queue = connection.queue
      .then(() => processMessage(connection, data))
      .catch((cause: unknown) => {
        logger.warn`Execution host message failed: ${cause instanceof Error ? cause.message : String(cause)}`;
        closeConnection(connection);
      });
  }

  async function processMessage(
    connection: HostConnection,
    data: string,
  ): Promise<void> {
    let raw: unknown;
    try {
      raw = JSON.parse(data);
    } catch (cause) {
      throw new Error("Execution host sent invalid JSON", { cause });
    }
    const frame = ExecutionHostFrame(raw);
    if (frame instanceof type.errors) {
      throw new Error(`Execution host sent an invalid frame: ${frame.summary}`);
    }

    if (connection.session === undefined) {
      if (frame.type !== "host.register") {
        throw new Error("Execution host must register before sending frames");
      }
      await register(connection, frame);
      return;
    }

    if (frame.type === "ping") {
      await refresh(connection);
      return;
    }
    if (frame.type === "host.register") {
      throw new Error("Execution host cannot register twice on one connection");
    }
    settleOperation(connection, frame);
  }

  async function register(
    connection: HostConnection,
    frame: Extract<typeof ExecutionHostFrame.infer, { type: "host.register" }>,
  ): Promise<void> {
    const timestamp = now();
    const session = await store.begin({
      tokenHashSha256: await sha256(frame.token),
      sessionId: createSessionId(),
      hubInstanceId,
      capabilities: frame.capabilities,
      leaseExpiresAt: new Date(timestamp.getTime() + leaseDurationMs),
      now: timestamp,
    });
    if (session === null) {
      throw new Error("Execution host credential is invalid or inactive");
    }
    if (connection.closed) {
      await store.expire({
        hostId: session.hostId,
        sessionId: session.sessionId,
        generation: session.generation,
        hubInstanceId,
        now: timestamp,
      });
      return;
    }
    // The token decides identity; the claimed id is advisory, so a mismatch warns instead of rejecting.
    if (frame.hostId !== session.hostId) {
      logger.warn`Execution host claimed id ${frame.hostId} but token resolves to ${session.hostId}`;
    }

    const previous = connectionsByHost.get(session.hostId);
    connection.session = session;
    connectionsByHost.set(session.hostId, connection);
    scheduleLeaseExpiry(connection);
    // One live connection per host: the new registration supersedes the previous one.
    if (previous !== undefined && previous !== connection) {
      closeConnection(previous);
    }
    send(connection, {
      type: "host.registered",
      hostId: session.hostId,
      principalId: session.principalId,
      sessionId: session.sessionId,
      sessionGeneration: session.generation,
      leaseExpiresAt: session.leaseExpiresAt.toISOString(),
    });
  }

  async function refresh(connection: HostConnection): Promise<void> {
    const session = connection.session;
    if (session === undefined) return;
    const timestamp = now();
    const leaseExpiresAt = new Date(timestamp.getTime() + leaseDurationMs);
    const refreshed = await store.refresh({
      hostId: session.hostId,
      sessionId: session.sessionId,
      generation: session.generation,
      hubInstanceId,
      leaseExpiresAt,
      now: timestamp,
    });
    if (!refreshed || connectionsByHost.get(session.hostId) !== connection) {
      throw new Error("Execution host session is stale");
    }
    connection.session = { ...session, leaseExpiresAt };
    scheduleLeaseExpiry(connection);
    send(connection, { type: "pong" });
  }

  function scheduleLeaseExpiry(connection: HostConnection): void {
    if (connection.leaseTimer !== undefined) {
      clearTimeout(connection.leaseTimer);
    }
    connection.leaseTimer = setTimeout(() => {
      if (!connection.closed) closeConnection(connection);
    }, leaseDurationMs);
    connection.leaseTimer.unref?.();
  }

  function send(
    connection: HostConnection,
    frame: ExecutionHostHubFrame,
  ): void {
    if (connection.closed) return;
    connection.ws.send(JSON.stringify(frame));
  }

  function operationKey(
    kind: "assignment" | "release",
    frame: { allocationId: string; generation: number; sidecarId: string },
  ): string {
    return `${kind}:${frame.allocationId}:${String(frame.generation)}:${frame.sidecarId}`;
  }

  function settleOperation(
    connection: HostConnection,
    frame: Extract<
      typeof ExecutionHostFrame.infer,
      { type: "host.assignment.ack" | "host.release.ack" }
    >,
  ): void {
    const key = operationKey(
      frame.type === "host.assignment.ack" ? "assignment" : "release",
      frame,
    );
    const pending = pendingOperations.get(key);
    if (pending === undefined || pending.connection !== connection) return;
    clearTimeout(pending.timer);
    pendingOperations.delete(key);
    pending.resolve(undefined);
  }

  function requestOperation(
    session: ExecutionHostSession,
    frame: ExecutionHostAssignmentFrame | ExecutionHostReleaseFrame,
    timeoutMs: number,
  ): Promise<void> {
    const connection = connectionsByHost.get(session.hostId);
    if (
      connection === undefined ||
      connection.closed ||
      connection.session?.sessionId !== session.sessionId ||
      connection.session.generation !== session.generation
    ) {
      return Promise.reject(
        new Error("Execution host session is not connected"),
      );
    }
    const kind = frame.type === "host.assignment" ? "assignment" : "release";
    const key = operationKey(kind, frame);
    const existing = pendingOperations.get(key);
    if (existing !== undefined) return existing.promise;
    const deferred = Promise.withResolvers<undefined>();
    const timer = setTimeout(() => {
      pendingOperations.delete(key);
      deferred.reject(
        new Error(`Execution host ${kind} acknowledgement timed out`),
      );
    }, timeoutMs);
    timer.unref?.();
    const pending: PendingHostOperation = {
      connection,
      promise: deferred.promise,
      resolve: deferred.resolve,
      reject: deferred.reject,
      timer,
    };
    pendingOperations.set(key, pending);
    try {
      send(connection, frame);
    } catch (cause) {
      clearTimeout(timer);
      pendingOperations.delete(key);
      deferred.reject(cause);
    }
    return pending.promise;
  }

  function closeConnection(connection: HostConnection): void {
    if (connection.closed) return;
    connection.closed = true;
    connections.delete(connection.ws);
    if (connection.leaseTimer !== undefined) {
      clearTimeout(connection.leaseTimer);
    }
    const session = connection.session;
    if (
      session !== undefined &&
      connectionsByHost.get(session.hostId) === connection
    ) {
      connectionsByHost.delete(session.hostId);
    }
    try {
      connection.ws.close();
    } catch (cause) {
      logger.warn`Failed to close execution host socket: ${cause instanceof Error ? cause.message : String(cause)}`;
    }
    if (session !== undefined) {
      for (const [key, pending] of pendingOperations) {
        if (pending.connection !== connection) continue;
        clearTimeout(pending.timer);
        pendingOperations.delete(key);
        pending.reject(new Error("Execution host connection closed"));
      }
      void store
        .expire({
          hostId: session.hostId,
          sessionId: session.sessionId,
          generation: session.generation,
          hubInstanceId,
          now: now(),
        })
        .catch((cause: unknown) => {
          logger.warn`Failed to expire execution host ${session.hostId} session ${session.sessionId}: ${cause instanceof Error ? cause.message : String(cause)}`;
        });
    }
  }

  function handleClose(ws: WsHandle): void {
    const connection = connections.get(ws);
    if (connection !== undefined) closeConnection(connection);
  }

  function getConnectedSession(hostId: string): ExecutionHostSession | null {
    const connection = connectionsByHost.get(hostId);
    return connection?.session ?? null;
  }

  function listConnectedSessions(): readonly ExecutionHostSession[] {
    return [...connectionsByHost.values()].flatMap((connection) =>
      connection.session === undefined ? [] : [connection.session],
    );
  }

  function sendAssignment(
    session: ExecutionHostSession,
    frame: Omit<ExecutionHostAssignmentFrame, "type">,
    timeoutMs = DEFAULT_OPERATION_TIMEOUT_MS,
  ): Promise<void> {
    return requestOperation(
      session,
      { ...frame, type: "host.assignment" },
      timeoutMs,
    );
  }

  function sendRelease(
    session: ExecutionHostSession,
    frame: Omit<ExecutionHostReleaseFrame, "type">,
    timeoutMs = DEFAULT_OPERATION_TIMEOUT_MS,
  ): Promise<void> {
    return requestOperation(
      session,
      { ...frame, type: "host.release" },
      timeoutMs,
    );
  }

  return {
    getConnectedSession,
    handleClose,
    handleMessage,
    handleOpen,
    listConnectedSessions,
    sendAssignment,
    sendRelease,
  };
}

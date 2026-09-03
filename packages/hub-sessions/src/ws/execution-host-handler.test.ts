import { describe, expect, test } from "bun:test";

import type { ExecutionHostSession, ExecutionHostSessionStore } from "@intx/db";

import {
  createExecutionHostControlRouter,
  type ExecutionHostControlRouter,
} from "./execution-host-handler";
import type { WsHandle } from "./sidecar-handler";

function createWs(): WsHandle & { sent: string[]; closed: boolean } {
  return {
    sent: [],
    closed: false,
    send(data) {
      this.sent.push(data);
    },
    close() {
      this.closed = true;
    },
  };
}

function createStore() {
  let current: ExecutionHostSession | null = null;
  const expired: { sessionId: string; generation: number }[] = [];
  const store: ExecutionHostSessionStore = {
    async begin(args) {
      current = {
        hostId: "host-1",
        principalId: "principal-host-1",
        ownerPrincipalId: "principal-owner-1",
        tenantId: "tenant-1",
        sessionId: args.sessionId,
        generation: (current?.generation ?? 0) + 1,
        hubInstanceId: args.hubInstanceId,
        capabilities: args.capabilities,
        leaseExpiresAt: args.leaseExpiresAt,
      };
      return current;
    },
    async expire(args) {
      expired.push({
        sessionId: args.sessionId,
        generation: args.generation,
      });
      if (
        current?.sessionId !== args.sessionId ||
        current.generation !== args.generation
      ) {
        return false;
      }
      current = { ...current, leaseExpiresAt: args.now ?? new Date() };
      return true;
    },
    async findCurrent() {
      return current;
    },
    async refresh(args) {
      if (
        current?.sessionId !== args.sessionId ||
        current.generation !== args.generation
      ) {
        return false;
      }
      current = { ...current, leaseExpiresAt: args.leaseExpiresAt };
      return true;
    },
  };
  return { expired, store };
}

async function waitFor(predicate: () => boolean): Promise<void> {
  const deadline = performance.now() + 1_000;
  while (performance.now() < deadline) {
    if (predicate()) return;
    await Bun.sleep(5);
  }
  throw new Error("Timed out waiting for execution host router");
}

function register(
  router: ExecutionHostControlRouter,
  ws: WsHandle,
  hostId = "host-1",
): void {
  router.handleOpen(ws);
  router.handleMessage(
    ws,
    JSON.stringify({
      type: "host.register",
      hostId,
      token: "secret",
      capabilities: [{ capability: "runtime:browser", state: "available" }],
    }),
  );
}

describe("createExecutionHostControlRouter", () => {
  test.each(["socket", "database"])(
    "drains pending operations when disconnect encounters a %s failure",
    async (failure) => {
      const { expired, store } = createStore();
      const router = createExecutionHostControlRouter({
        store,
        hubInstanceId: "hub-1",
        createSessionId: () => "session-1",
      });
      const ws = createWs();
      register(router, ws);
      await waitFor(() => ws.sent.length === 1);
      const session = router.getConnectedSession("host-1");
      if (session === null) throw new Error("expected registered host session");

      if (failure === "socket") {
        ws.close = () => {
          throw new Error("Socket close failed");
        };
      } else {
        const expire = store.expire;
        store.expire = async (args) => {
          await expire(args);
          throw new Error("Database unavailable");
        };
      }
      const released = router
        .sendRelease(session, {
          allocationId: "allocation-1",
          generation: 2,
          sidecarId: "sidecar-1",
        })
        .catch((cause: unknown) => cause);

      expect(() => router.handleClose(ws)).not.toThrow();
      expect(await released).toMatchObject({
        message: "Execution host connection closed",
      });
      expect(router.getConnectedSession("host-1")).toBeNull();
      expect(expired).toEqual([{ sessionId: "session-1", generation: 1 }]);
    },
  );

  test("derives host identity from the token-backed store", async () => {
    const { store } = createStore();
    const router = createExecutionHostControlRouter({
      store,
      hubInstanceId: "hub-1",
      createSessionId: () => "session-1",
    });
    const ws = createWs();

    register(router, ws, "spoofed-host");
    await waitFor(() => ws.sent.length === 1);

    expect(JSON.parse(ws.sent[0] ?? "null")).toMatchObject({
      type: "host.registered",
      hostId: "host-1",
      principalId: "principal-host-1",
      sessionId: "session-1",
      sessionGeneration: 1,
    });
  });

  test("a replacement connection fences the old socket", async () => {
    const { expired, store } = createStore();
    let sessionCounter = 0;
    const router = createExecutionHostControlRouter({
      store,
      hubInstanceId: "hub-1",
      createSessionId: () => {
        sessionCounter += 1;
        return `session-${String(sessionCounter)}`;
      },
    });
    const first = createWs();
    const second = createWs();
    register(router, first);
    await waitFor(() => first.sent.length === 1);

    register(router, second);
    await waitFor(() => second.sent.length === 1);

    expect(first.closed).toBe(true);
    expect(JSON.parse(second.sent[0] ?? "null")).toMatchObject({
      sessionId: "session-2",
      sessionGeneration: 2,
    });
    expect(router.getConnectedSession("host-1")).toMatchObject({
      sessionId: "session-2",
      generation: 2,
    });
    expect(expired).toContainEqual({ sessionId: "session-1", generation: 1 });
  });

  test("rejects frames sent before registration", async () => {
    const { store } = createStore();
    const router = createExecutionHostControlRouter({
      store,
      hubInstanceId: "hub-1",
    });
    const ws = createWs();
    router.handleOpen(ws);

    router.handleMessage(ws, JSON.stringify({ type: "ping" }));
    await waitFor(() => ws.closed);

    expect(router.getConnectedSession("host-1")).toBeNull();
  });

  test("settles assignment and release requests only from the current connection", async () => {
    const { store } = createStore();
    const router = createExecutionHostControlRouter({
      store,
      hubInstanceId: "hub-1",
      createSessionId: () => "session-1",
    });
    const ws = createWs();
    register(router, ws);
    await waitFor(() => ws.sent.length === 1);
    const session = router.getConnectedSession("host-1");
    if (session === null) throw new Error("expected connected host");

    const assigned = router.sendAssignment(
      session,
      {
        allocationId: "allocation-1",
        generation: 1,
        tenantId: "tenant-1",
        anchorRunId: "run-1",
        sidecarId: "sidecar-1",
        sidecarToken: "sidecar-token",
        hubWebSocketUrl: "wss://hub.example/api/sidecars/ws",
      },
      100,
    );
    await waitFor(() => ws.sent.length === 2);
    expect(JSON.parse(ws.sent[1] ?? "null")).toMatchObject({
      type: "host.assignment",
      allocationId: "allocation-1",
      sidecarId: "sidecar-1",
    });
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "host.assignment.ack",
        allocationId: "allocation-1",
        generation: 1,
        sidecarId: "sidecar-1",
      }),
    );
    await assigned;

    const released = router.sendRelease(
      session,
      { allocationId: "allocation-1", generation: 2, sidecarId: "sidecar-1" },
      100,
    );
    await waitFor(() => ws.sent.length === 3);
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "host.release.ack",
        allocationId: "allocation-1",
        generation: 2,
        sidecarId: "sidecar-1",
      }),
    );
    await released;
  });

  test("does not accept an acknowledgement from a replaced socket", async () => {
    const { store } = createStore();
    let sessionCounter = 0;
    const router = createExecutionHostControlRouter({
      store,
      hubInstanceId: "hub-1",
      createSessionId: () => {
        sessionCounter += 1;
        return `session-${String(sessionCounter)}`;
      },
    });
    const oldSocket = createWs();
    const currentSocket = createWs();
    register(router, oldSocket);
    await waitFor(() => oldSocket.sent.length === 1);
    register(router, currentSocket);
    await waitFor(() => currentSocket.sent.length === 1);
    const session = router.getConnectedSession("host-1");
    if (session === null) throw new Error("expected current host session");

    const assigned = router.sendAssignment(
      session,
      {
        allocationId: "allocation-1",
        generation: 1,
        tenantId: "tenant-1",
        anchorRunId: "run-1",
        sidecarId: "sidecar-1",
        sidecarToken: "sidecar-token",
        hubWebSocketUrl: "wss://hub.example/api/sidecars/ws",
      },
      10,
    );
    router.handleMessage(
      oldSocket,
      JSON.stringify({
        type: "host.assignment.ack",
        allocationId: "allocation-1",
        generation: 1,
        sidecarId: "sidecar-1",
      }),
    );

    await expect(assigned).rejects.toThrow(/acknowledgement timed out/);
  });
});

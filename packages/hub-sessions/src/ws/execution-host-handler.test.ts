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
  for (let attempt = 0; attempt < 20; attempt += 1) {
    if (predicate()) return;
    await Bun.sleep(0);
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
});

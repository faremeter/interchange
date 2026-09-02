import { describe, expect, test } from "bun:test";

import {
  connectAllocated,
  createAllocatedRouter,
  createMockWs,
  parsedFrames,
  TEST_IDENTITY,
  TEST_TARGET,
  tick,
} from "./sidecar-handler.test-helpers";
import { createSidecarRouter } from "./sidecar-handler";

describe("SidecarRouter allocation connection lifecycle", () => {
  test("responds to ping with pong", async () => {
    const router = createAllocatedRouter();
    const ws = await connectAllocated(router);

    router.handleMessage(ws, JSON.stringify({ type: "ping" }));

    expect(parsedFrames(ws)).toContainEqual({ type: "pong" });
  });

  test("ping resets the connection liveness deadline", async () => {
    const router = createAllocatedRouter({ pingTimeoutMs: 30 });
    const ws = await connectAllocated(router);
    await new Promise((resolve) => setTimeout(resolve, 20));

    router.handleMessage(ws, JSON.stringify({ type: "ping" }));
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(ws.closed).toBe(false);

    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(ws.closed).toBe(true);
  });

  test("closes a connection that misses its ping deadline", async () => {
    const router = createAllocatedRouter({ pingTimeoutMs: 20 });
    const ws = await connectAllocated(router);

    await new Promise((resolve) => setTimeout(resolve, 40));

    expect(ws.closed).toBe(true);
  });

  test("rejects invalid and throwing authenticators", async () => {
    const invalid = createSidecarRouter({
      authenticateSidecar: async () => null,
      validateSidecarIdentity: async () => true,
    });
    const invalidWs = createMockWs();
    invalid.handleOpen(invalidWs);
    invalid.handleMessage(
      invalidWs,
      JSON.stringify({
        type: "register",
        sidecarId: "claimed",
        token: "invalid",
        agentAddresses: [],
      }),
    );
    await tick();

    const throwing = createSidecarRouter({
      authenticateSidecar: async () => {
        throw new Error("auth unavailable");
      },
      validateSidecarIdentity: async () => true,
    });
    const throwingWs = createMockWs();
    throwing.handleOpen(throwingWs);
    throwing.handleMessage(
      throwingWs,
      JSON.stringify({
        type: "register",
        sidecarId: "claimed",
        token: "unknown",
        agentAddresses: [],
      }),
    );
    await tick();

    expect(invalidWs.closed).toBe(true);
    expect(throwingWs.closed).toBe(true);
  });

  test("keys the connection by authenticated rather than claimed identity", async () => {
    const router = createAllocatedRouter();
    const ws = createMockWs();
    router.handleOpen(ws);
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "register",
        sidecarId: "spoofed-sidecar",
        token: "token",
        agentAddresses: [],
      }),
    );
    await tick();

    expect(router.getConnectedSidecars()).toEqual([TEST_IDENTITY.sidecarId]);
  });

  test("emits allocation connect and disconnect for the current socket", async () => {
    const connected: unknown[] = [];
    const disconnected: unknown[] = [];
    const router = createAllocatedRouter();
    router.events.on("sidecar.allocated.connected", (event) => {
      connected.push(event);
    });
    router.events.on("sidecar.disconnect", (event) => {
      disconnected.push(event);
    });
    const ws = await connectAllocated(router);

    router.handleClose(ws);

    expect(connected).toEqual([TEST_TARGET]);
    expect(disconnected).toEqual([
      { ownedAddresses: [], allocated: TEST_TARGET },
    ]);
  });

  test("retires terminal allocation fences and rejects their waiters", async () => {
    const router = createAllocatedRouter();
    router.fenceAllocation(TEST_TARGET.allocationId, 2);
    const waiting = router.waitForAllocatedSidecar(
      { allocationId: TEST_TARGET.allocationId, generation: 2 },
      500,
    );
    await tick();

    router.retireAllocation({
      allocationId: TEST_TARGET.allocationId,
      generation: 2,
    });

    await expect(waiting).rejects.toThrow(/retired/);
    expect(() =>
      router.fenceAllocation(TEST_TARGET.allocationId, 1),
    ).not.toThrow();
  });

  test("tracks connector state only for an address owned by the allocation", async () => {
    const changed: unknown[] = [];
    const router = createAllocatedRouter();
    router.events.on("connector.state.changed", (event) => {
      changed.push(event);
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    const connectorState = {
      threadRoot: "root-1",
      lastMessageId: "last-1",
      replyTo: "user@example.test",
      cc: [],
    };

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "connector.state.changed",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        connectorState,
      }),
    );
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "connector.state.changed",
        agentAddress: "unowned@example.test",
        connectorState,
      }),
    );
    await tick();

    expect(router.getConnectorState(TEST_IDENTITY.workflowRunAddress)).toEqual(
      connectorState,
    );
    expect(router.getConnectorState("unowned@example.test")).toBeNull();
    expect(changed).toEqual([
      {
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        connectorState,
      },
    ]);

    router.handleClose(ws);
    expect(
      router.getConnectorState(TEST_IDENTITY.workflowRunAddress),
    ).toBeNull();
  });

  test("serializes an address-dependent frame behind asynchronous registration", async () => {
    const router = createAllocatedRouter();
    const ws = createMockWs();
    const connectorState = {
      threadRoot: "root-serialized",
      lastMessageId: "last-serialized",
      replyTo: "user@example.test",
      cc: [],
    };
    router.handleOpen(ws);
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "register",
        sidecarId: TEST_IDENTITY.sidecarId,
        token: "token",
        agentAddresses: [TEST_IDENTITY.workflowRunAddress],
      }),
    );
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "connector.state.changed",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        connectorState,
      }),
    );
    await tick();
    await tick();

    expect(router.getConnectorState(TEST_IDENTITY.workflowRunAddress)).toEqual(
      connectorState,
    );
  });

  test("an empty re-registration does not drop an owned workflow route", async () => {
    const router = createAllocatedRouter();
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "register",
        sidecarId: TEST_IDENTITY.sidecarId,
        token: "token",
        agentAddresses: [],
      }),
    );
    await tick();

    expect(router.getRoutableAddresses()).toEqual([
      TEST_IDENTITY.workflowRunAddress,
    ]);
  });

  test("forwards owned agent events and drops unowned claims", async () => {
    const emitted: unknown[] = [];
    const subscribed: unknown[] = [];
    const router = createAllocatedRouter();
    router.events.on("agent.event", (event) => {
      emitted.push(event);
    });
    const unsubscribe = router.subscribeAgent(
      TEST_IDENTITY.workflowRunAddress,
      (event) => subscribed.push(event),
    );
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    const event = { type: "reactor.start", seq: 0, data: {} };
    const frame = {
      type: "agent.event",
      agentAddress: TEST_IDENTITY.workflowRunAddress,
      sessionId: "session-1",
      event,
    };

    router.handleMessage(ws, JSON.stringify(frame));
    await tick();
    router.handleMessage(
      ws,
      JSON.stringify({
        ...frame,
        agentAddress: "other@tenant.example",
      }),
    );
    await tick();
    unsubscribe();
    router.handleMessage(ws, JSON.stringify(frame));
    await tick();

    expect(emitted).toHaveLength(2);
    expect(subscribed).toEqual([event]);
  });
});

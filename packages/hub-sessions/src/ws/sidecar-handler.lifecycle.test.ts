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
import {
  createSidecarRouter,
  SidecarIdentityValidationError,
} from "./sidecar-handler";

/**
 * The router's timers, driven by the test. The intervals were already
 * injectable; arming was not, so a test had to sleep past an interval to
 * observe what it triggered.
 */
function createManualTimers(): {
  scheduleTimeout: (handler: () => void, ms: number) => () => void;
  armed: () => { ms: number; cancelled: boolean }[];
  armedCount: () => number;
  fireAll: () => void;
} {
  const entries: { ms: number; fire: () => void; cancelled: boolean }[] = [];
  return {
    scheduleTimeout(handler, ms) {
      const entry = { ms, fire: handler, cancelled: false };
      entries.push(entry);
      return () => {
        entry.cancelled = true;
      };
    },
    armed: () => entries.map((e) => ({ ms: e.ms, cancelled: e.cancelled })),
    armedCount: () => entries.filter((e) => !e.cancelled).length,
    fireAll() {
      for (const entry of entries) {
        if (entry.cancelled) continue;
        entry.cancelled = true;
        entry.fire();
      }
    },
  };
}

describe("SidecarRouter allocation connection lifecycle", () => {
  test("responds to ping with pong", async () => {
    const router = createAllocatedRouter();
    const ws = await connectAllocated(router);

    router.handleMessage(ws, JSON.stringify({ type: "ping" }));

    expect(parsedFrames(ws)).toContainEqual({ type: "pong" });
  });

  test("ping resets the connection liveness deadline", async () => {
    // The subject is that the ping REPLACES the armed deadline, which is a
    // claim about which timer exists, not about elapsed time. Sleeping part
    // way into the window and asserting the socket is still open only holds
    // while the pause lands inside the window: a loaded worker overshoots
    // it, the deadline fires first, and the test fails on a ping that was
    // merely late.
    const timers = createManualTimers();
    const router = createAllocatedRouter({
      pingTimeoutMs: 30,
      scheduleTimeout: timers.scheduleTimeout,
    });
    const ws = await connectAllocated(router);
    expect(timers.armed()).toHaveLength(1);

    router.handleMessage(ws, JSON.stringify({ type: "ping" }));
    // The connect-time deadline is cancelled and a fresh one armed in its
    // place, so re-read rather than reusing the earlier snapshot. Firing the
    // cancelled one must not close the socket.
    const afterPing = timers.armed();
    expect(afterPing).toHaveLength(2);
    expect(afterPing[0]?.cancelled).toBe(true);
    expect(afterPing[1]?.cancelled).toBe(false);
    expect(ws.closed).toBe(false);

    // Only the replacement is live, so firing what remains armed is firing
    // the deadline the ping installed -- and that is what closes the socket.
    expect(timers.armedCount()).toBe(1);
    timers.fireAll();
    expect(ws.closed).toBe(true);
  });

  test("closes a connection that misses its ping deadline", async () => {
    const timers = createManualTimers();
    const router = createAllocatedRouter({
      pingTimeoutMs: 20,
      scheduleTimeout: timers.scheduleTimeout,
    });
    const ws = await connectAllocated(router);

    // Firing the armed deadline is the missed ping; waiting out twice the
    // interval was a slower way of arriving at the same fact.
    timers.fireAll();
    expect(ws.closed).toBe(true);
  });

  test("rejects invalid and throwing authenticators", async () => {
    const invalid = createSidecarRouter({
      withExecutableWorkflowRun: async (_target, send) => send(),
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
      withExecutableWorkflowRun: async (_target, send) => send(),
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

  test("reports a clean timeout when a later validation supersedes a transient failure", async () => {
    let readinessCalls = 0;
    const router = createAllocatedRouter({
      validateSidecarIdentity: async (_identity, use) => {
        if (use !== "readiness") return true;
        readinessCalls += 1;
        if (readinessCalls === 1) throw new Error("transient lookup failure");
        return false;
      },
    });
    const ws = await connectAllocated(router);
    const waiting = router.waitForAllocatedSidecar(TEST_TARGET, 30);
    const error = await waiting.catch((cause: unknown) => cause);

    expect(readinessCalls).toBeGreaterThanOrEqual(2);
    if (!(error instanceof Error)) throw new Error("expected an Error");
    expect(error).not.toBeInstanceOf(SidecarIdentityValidationError);
    expect(error.message).toMatch(/Timed out waiting/);

    router.handleClose(ws);
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

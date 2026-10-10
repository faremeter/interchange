import { describe, expect, test } from "bun:test";

import {
  AgentUndeployFrame,
  WorkflowControlFrame,
  type HostedIncarnation,
} from "@intx/types/sidecar";
import { waitUntil } from "@intx/types/testing";
import type { ToolPackageManifest } from "@intx/types/tool-packages";

import {
  createMockWs,
  deployReply,
  helloFrame,
  parsedFrames,
  sidecarAuth,
  TEST_CONFIG,
  tick,
  undeployAck,
} from "./sidecar-handler.test-helpers";
import {
  createSidecarRouter,
  type AllocatedSidecarTarget,
  type SidecarAuthIdentity,
  type SidecarRouterConfig,
} from "./sidecar-handler";

type AllocatedBinding = Extract<SidecarAuthIdentity, { kind: "allocated" }>;
type TestRouter = ReturnType<typeof createSidecarRouter>;
type TestWs = ReturnType<typeof createMockWs>;

const SIDECAR = "sc-shared";
const PUBLIC_KEY = "ab".repeat(32);

function allocation(allocationId: string, generation = 1): AllocatedBinding {
  return {
    kind: "allocated",
    sidecarId: SIDECAR,
    allocationId,
    tenantId: "tenant-1",
    anchorRunId: `run_${allocationId}`,
    workflowRunAddress: `run_${allocationId}@tenant.example`,
    generation,
  };
}

const probe: SidecarAuthIdentity = {
  kind: "probe",
  sidecarId: SIDECAR,
  allocationId: "probe-1",
  tenantId: "tenant-1",
  generation: 1,
};

const first = allocation("alloc-a");
const second = allocation("alloc-b");

function target(binding: SidecarAuthIdentity): AllocatedSidecarTarget {
  return { allocationId: binding.allocationId, generation: binding.generation };
}

// `hosted` is what the database reports the shared sidecar hosts. Tests
// replace it to model a placement that changed while the sidecar stayed
// connected.
function createSharedRouter(
  bindings: readonly SidecarAuthIdentity[],
  config: Partial<SidecarRouterConfig> = {},
) {
  const hosted = { bindings };
  const router = createSidecarRouter({
    withExecutableWorkflowRun: async (_target, send) => send(),
    ...sidecarAuth((sidecarId) =>
      sidecarId === SIDECAR ? hosted.bindings : [],
    ),
    validateSidecarIdentity: async () => true,
    hubPublicKey: "a".repeat(64),
    requestTimeoutMs: 500,
    ...config,
  });
  for (const binding of bindings) {
    router.fenceAllocation(binding.allocationId, binding.generation);
  }
  return { router, hosted };
}

// What a hello reports: an address alone is a live incarnation at generation 1.
type Reported = string | HostedIncarnation;

function sendHandshake(
  router: TestRouter,
  ws: TestWs,
  reported: readonly Reported[] = [],
): void {
  router.handleMessage(
    ws,
    helloFrame(
      SIDECAR,
      reported.map((incarnation) =>
        typeof incarnation === "string"
          ? { address: incarnation, generation: 1, state: "live" }
          : incarnation,
      ),
    ),
  );
}

// Sends the handshake without waiting for its registration to finish.
function openSocket(
  router: TestRouter,
  reported: readonly Reported[] = [],
): TestWs {
  const ws = createMockWs();
  router.handleOpen(ws);
  sendHandshake(router, ws, reported);
  return ws;
}

// Holds each bindings read made while holding until the test settles it.
// Settling a read with `false` fails it as a database error would.
function heldReads() {
  const reads: PromiseWithResolvers<boolean>[] = [];
  let holding = false;
  return {
    reads,
    hold(on: boolean) {
      holding = on;
    },
    async read(bindings: readonly SidecarAuthIdentity[]) {
      if (holding) {
        const read = Promise.withResolvers<boolean>();
        reads.push(read);
        if (!(await read.promise)) throw new Error("database unavailable");
      }
      return bindings;
    },
  };
}

// Holds each readiness validation made while holding until the test settles
// it, in the order they were made.
function heldReadiness() {
  const validations: PromiseWithResolvers<boolean>[] = [];
  let holding = false;
  return {
    hold(on: boolean) {
      holding = on;
    },
    settle(index: number, outcome: boolean | Error) {
      const validation = validations[index];
      if (validation === undefined) {
        throw new Error(
          `readiness validation ${String(index)} was never made; ${String(validations.length)} were`,
        );
      }
      if (outcome instanceof Error) validation.reject(outcome);
      else validation.resolve(outcome);
    },
    get count() {
      return validations.length;
    },
    async validate(_identity: SidecarAuthIdentity, use: string) {
      if (use !== "readiness" || !holding) return true;
      const validation = Promise.withResolvers<boolean>();
      validations.push(validation);
      return validation.promise;
    },
  };
}

async function reconnect(
  router: TestRouter,
  reported: readonly Reported[] = [],
): Promise<TestWs> {
  const ws = openSocket(router, reported);
  await tick();
  return ws;
}

function framesOfType(ws: TestWs, type: string): Record<string, unknown>[] {
  return parsedFrames(ws).flatMap((frame) => {
    if (typeof frame !== "object" || frame === null) return [];
    if (!("type" in frame) || frame.type !== type) return [];
    return [Object.fromEntries(Object.entries(frame))];
  });
}

describe("SidecarRouter allocation cleanup", () => {
  test("sync transfers a committed release before its caller publishes the fence", async () => {
    const { router, hosted } = createSharedRouter([first, second]);
    const ws = await reconnect(router, [
      first.workflowRunAddress,
      second.workflowRunAddress,
    ]);
    const cleanup = { ...first, kind: "cleanup", generation: 2 } as const;
    hosted.bindings = [cleanup, second];
    await router.syncSidecar(SIDECAR);
    expect(router.getCleanupConnection(target(cleanup))).toBe(ws);
    expect(framesOfType(ws, "agent.undeploy")).toEqual([]);
    router.handleClose(ws);
  });

  test.each(["transfer", "retire"] as const)(
    "a stale binding read cannot undo a cleanup %s",
    async (change) => {
      const cleanup = { ...first, kind: "cleanup", generation: 2 } as const;
      let bindings: readonly SidecarAuthIdentity[] =
        change === "retire" ? [cleanup, second] : [first, second];
      let blocking = false;
      const reading = Promise.withResolvers<undefined>();
      const finish = Promise.withResolvers<undefined>();
      const { router } = createSharedRouter(bindings, {
        resolveSidecarBindings: async () => {
          const snapshot = bindings;
          if (blocking) {
            reading.resolve(undefined);
            await finish.promise;
          }
          return snapshot;
        },
      });
      const ws = await reconnect(router, [
        first.workflowRunAddress,
        second.workflowRunAddress,
      ]);
      blocking = true;
      const syncing = router.syncSidecar(SIDECAR);
      try {
        await reading.promise;
        if (change === "transfer") {
          bindings = [cleanup, second];
          router.fenceAllocation(first.allocationId, 2, {
            cleanup: { sidecarId: SIDECAR },
          });
        } else {
          bindings = [second];
          router.retireAllocation(target(cleanup));
        }
        finish.resolve(undefined);
        await syncing;
        expect(router.getCleanupConnection(target(cleanup))).toBe(
          change === "transfer" ? ws : undefined,
        );
        expect(framesOfType(ws, "agent.undeploy")).toEqual([]);
      } finally {
        finish.resolve(undefined);
        router.handleClose(ws);
      }
    },
  );

  test("a late stop acknowledgement preserves the committed cleanup binding", async () => {
    let released = false;
    const { router, hosted } = createSharedRouter([first], {
      validateSidecarIdentity: async (binding) =>
        !released || binding.kind === "cleanup",
    });
    const ws = await reconnect(router, [first.workflowRunAddress]);
    const pending = router
      .sendWorkflowControl(
        target(first),
        {
          runId: first.anchorRunId,
          agentAddress: first.workflowRunAddress,
          action: "stop",
          reason: "Policy expired",
        },
        500,
      )
      .catch((cause: unknown) => cause);
    await ws.awaitSent((sent) =>
      sent.some((frame) => frame.includes('"workflow.control"')),
    );
    const frame = WorkflowControlFrame.assert(
      framesOfType(ws, "workflow.control")[0],
    );
    const cleanup = { ...first, kind: "cleanup", generation: 2 } as const;
    hosted.bindings = [cleanup];
    released = true;
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "workflow.control.ack",
        requestId: frame.requestId,
        refTips: {},
      }),
    );
    expect(await pending).toBeInstanceOf(Error);
    expect(router.getCleanupConnection(target(cleanup))).toBe(ws);
    expect(framesOfType(ws, "agent.undeploy")).toEqual([]);
    expect(ws.closed).toBe(false);
    router.handleClose(ws);
  });
  const cleanup = { ...first, kind: "cleanup", generation: 2 } as const;

  async function connectForCleanup(
    router: TestRouter,
    reported: readonly Reported[] = [],
  ) {
    const ws = openSocket(router, reported);
    await ws.awaitSent((sent) => sent.some((raw) => raw.includes('"welcome"')));
    return ws;
  }

  async function requestCleanup(
    router: TestRouter,
    ws: TestWs,
    signal = new AbortController().signal,
    expectedConnection: object = ws,
  ) {
    const sentBefore = ws.sent.length;
    const pending = router
      .undeployAllocation(target(cleanup), 1234, signal, expectedConnection)
      .catch((error: unknown) => error);
    await ws.awaitSent((sent) =>
      sent.slice(sentBefore).some((raw) => raw.includes('"agent.undeploy"')),
    );
    const frame = AgentUndeployFrame.assert(
      framesOfType(ws, "agent.undeploy").at(-1),
    );
    return { pending, frame };
  }

  test.each([false, true])(
    "hands a live release to one tracked cleanup request with another allocation present = %s",
    async (shared) => {
      const others = shared ? [second] : [];
      const { router, hosted } = createSharedRouter([first, ...others]);
      const ws = openSocket(router, [
        first.workflowRunAddress,
        ...others.map((held) => held.workflowRunAddress),
      ]);
      await ws.awaitSent((sent) =>
        sent.some((raw) => raw.includes('"welcome"')),
      );
      hosted.bindings = [cleanup, ...others];
      router.fenceAllocation(cleanup.allocationId, cleanup.generation, {
        cleanup: { sidecarId: SIDECAR },
      });
      expect(ws.closed).toBe(false);
      await router.syncSidecar(SIDECAR);
      expect(framesOfType(ws, "agent.undeploy")).toEqual([]);
      expect(router.getRoutableAddresses()).toEqual(
        others.map((held) => held.workflowRunAddress),
      );

      // A repeated hello still reporting the old generation must not send an
      // untracked undeploy ahead of the reconciler's acknowledged request.
      sendHandshake(router, ws, [
        first.workflowRunAddress,
        ...others.map((held) => held.workflowRunAddress),
      ]);
      await router.syncSidecar(SIDECAR);
      expect(framesOfType(ws, "agent.undeploy")).toEqual([]);
      const stopped = () =>
        router.handleMessage(
          ws,
          JSON.stringify({
            type: "deployment.stopped",
            agentAddress: first.workflowRunAddress,
            generation: first.generation,
            error: "child stopped while releasing",
          }),
        );
      stopped();
      await router.syncSidecar(SIDECAR);
      expect(framesOfType(ws, "agent.undeploy")).toEqual([]);
      const { pending, frame } = await requestCleanup(router, ws);
      stopped();
      await router.syncSidecar(SIDECAR);
      expect(framesOfType(ws, "agent.undeploy")).toHaveLength(1);
      router.handleMessage(
        ws,
        JSON.stringify({ ...frame, type: "agent.undeploy.ack" }),
      );
      expect(await pending).toBeUndefined();
      router.handleClose(ws);
    },
  );

  test("a cleanup connection guard refuses to send on a replacement socket", async () => {
    const { router } = createSharedRouter([cleanup]);
    const original = await connectForCleanup(router);
    expect(router.getCleanupConnection(target(cleanup))).toBe(original);
    router.handleClose(original);
    const replacement = await connectForCleanup(router);
    expect(router.getCleanupConnection(target(cleanup))).toBe(replacement);
    await expect(
      router.undeployAllocation(
        target(cleanup),
        1234,
        new AbortController().signal,
        original,
      ),
    ).rejects.toThrow("Cleanup connection changed");
    expect(framesOfType(replacement, "agent.undeploy")).toEqual([]);
    const { pending, frame } = await requestCleanup(
      router,
      replacement,
      undefined,
      replacement,
    );
    router.handleMessage(
      replacement,
      JSON.stringify({ ...frame, type: "agent.undeploy.ack" }),
    );
    expect(await pending).toBeUndefined();
    router.handleClose(replacement);
  });

  test("retiring a release fence before binding sync closes its cleanup connection", async () => {
    const { router } = createSharedRouter([first]);
    const ws = await connectForCleanup(router, [first.workflowRunAddress]);
    router.fenceAllocation(cleanup.allocationId, cleanup.generation, {
      cleanup: { sidecarId: SIDECAR },
    });
    expect(ws.closed).toBe(false);
    expect(router.getRoutableAddresses()).toEqual([]);
    router.retireAllocation(target(cleanup));
    expect(ws.closed).toBe(true);
    expect(framesOfType(ws, "agent.undeploy")).toEqual([]);
  });

  test.each(["allocated", "cleanup"] as const)(
    "rejects a second %s undeploy without disturbing the first caller",
    async (kind) => {
      const binding = kind === "cleanup" ? cleanup : first;
      const { router } = createSharedRouter([binding]);
      const ws = await connectForCleanup(
        router,
        kind === "allocated" ? [first.workflowRunAddress] : [],
      );
      const request = () =>
        kind === "cleanup"
          ? router.undeployAllocation(
              target(cleanup),
              1234,
              new AbortController().signal,
              ws,
            )
          : router.sendAgentUndeploy(first.workflowRunAddress, "test");
      const pending = request().catch((error: unknown) => error);
      await ws.awaitSent((sent) =>
        sent.some((raw) => raw.includes('"agent.undeploy"')),
      );
      const frame = AgentUndeployFrame.assert(
        framesOfType(ws, "agent.undeploy").at(-1),
      );
      await expect(request()).rejects.toThrow("already pending");
      expect(framesOfType(ws, "agent.undeploy")).toHaveLength(1);
      router.handleMessage(
        ws,
        JSON.stringify({ ...frame, type: "agent.undeploy.ack" }),
      );
      expect(await pending).toBeUndefined();
      router.handleClose(ws);
    },
  );

  test("welcomes a sidecar whose only remaining allocation is cleanup, without routing it", async () => {
    const { router } = createSharedRouter([cleanup]);
    const ws = await connectForCleanup(router, [first.workflowRunAddress]);
    expect(framesOfType(ws, "welcome")).toEqual([
      { type: "welcome", routed: [] },
    ]);
    expect(router.getRoutableAddresses()).toEqual([]);
    expect(ws.closed).toBe(false);
    expect(await router.isAllocatedSidecarReady(target(cleanup))).toBe(false);
    await expect(
      router.sendAgentDeployToAllocation(
        target(cleanup),
        first.workflowRunAddress,
        configFor(first),
      ),
    ).rejects.toThrow("does not permit");

    const { pending, frame } = await requestCleanup(router, ws);
    expect(frame.generation).toBe(2);
    router.handleMessage(
      ws,
      JSON.stringify({ ...frame, type: "agent.undeploy.ack" }),
    );
    expect(await pending).toBeUndefined();
    router.retireAllocation(target(cleanup));
    expect(ws.closed).toBe(true);
  });

  test.each(["error", "disconnect", "abort", "timeout"] as const)(
    "a cleanup %s fails the attempt and permits a fresh request",
    async (outcome) => {
      const timers = new Set<() => void>();
      const { router } = createSharedRouter([cleanup], {
        scheduleTimeout(handler, ms) {
          if (ms === 1234) timers.add(handler);
          return () => {
            timers.delete(handler);
          };
        },
      });
      let ws = await connectForCleanup(router);
      const controller = new AbortController();
      const { pending, frame } = await requestCleanup(
        router,
        ws,
        controller.signal,
      );
      if (outcome === "error") {
        router.handleMessage(
          ws,
          JSON.stringify({
            ...frame,
            type: "agent.undeploy.error",
            error: "disk busy",
          }),
        );
      } else if (outcome === "disconnect") {
        router.handleClose(ws);
      } else if (outcome === "abort") {
        controller.abort(new Error("lease ended"));
      } else {
        expect(timers.size).toBe(1);
        for (const fire of [...timers]) {
          timers.delete(fire);
          fire();
        }
      }
      expect(await pending).toBeInstanceOf(Error);
      expect(timers.size).toBe(0);

      if (outcome === "disconnect") ws = await connectForCleanup(router);
      const retry = await requestCleanup(router, ws);
      expect(retry.frame.requestId).not.toBe(frame.requestId);
      router.handleMessage(
        ws,
        JSON.stringify({ ...retry.frame, type: "agent.undeploy.ack" }),
      );
      expect(await retry.pending).toBeUndefined();
      router.handleClose(ws);
    },
  );

  test("a lost acknowledgement can be confirmed after both Hub and sidecar restart with no record left", async () => {
    const before = createSharedRouter([cleanup]).router;
    const oldWs = await connectForCleanup(before);
    const original = await requestCleanup(before, oldWs);
    // Cleanup completed, but the socket dropped before the reply reached the Hub.
    before.handleClose(oldWs);
    expect(await original.pending).toBeInstanceOf(Error);

    const after = createSharedRouter([cleanup]).router;
    const newWs = await connectForCleanup(after);
    const retry = await requestCleanup(after, newWs);
    expect(retry.frame.generation).toBe(original.frame.generation);
    after.handleMessage(
      newWs,
      JSON.stringify({ ...retry.frame, type: "agent.undeploy.ack" }),
    );
    expect(await retry.pending).toBeUndefined();
    after.handleClose(newWs);
  });

  test.each(["generation", "request", "socket"] as const)(
    "an acknowledgement with the wrong %s cannot confirm cleanup",
    async (mismatch) => {
      const { router } = createSharedRouter([cleanup]);
      const ws = await connectForCleanup(router);
      const { pending, frame } = await requestCleanup(router, ws);
      router.handleMessage(
        mismatch === "socket" ? createMockWs() : ws,
        JSON.stringify({
          ...frame,
          type: "agent.undeploy.ack",
          ...(mismatch === "generation" ? { generation: 1 } : {}),
          ...(mismatch === "request" ? { requestId: "old-request" } : {}),
        }),
      );
      // Replies bypass the connection queue. The actual failure must still win.
      router.handleMessage(
        ws,
        JSON.stringify({
          ...frame,
          type: "agent.undeploy.error",
          error: "not removed",
        }),
      );
      expect(await pending).toMatchObject({ message: "not removed" });
      router.handleClose(ws);
    },
  );

  test("advancing the allocation fence rejects a cleanup still awaiting its reply", async () => {
    const { router } = createSharedRouter([cleanup]);
    const ws = await connectForCleanup(router);
    const { pending, frame } = await requestCleanup(router, ws);
    router.fenceAllocation(cleanup.allocationId, 3);
    router.handleMessage(
      ws,
      JSON.stringify({ ...frame, type: "agent.undeploy.ack" }),
    );
    expect(await pending).toBeInstanceOf(Error);
    expect(ws.closed).toBe(true);
  });
});

function recordDisconnects(router: TestRouter) {
  const events: unknown[] = [];
  router.events.on("sidecar.disconnect", (event) => {
    events.push(event);
  });
  return events;
}

function configFor(binding: AllocatedBinding) {
  return { ...TEST_CONFIG, agentAddress: binding.workflowRunAddress };
}

describe("SidecarRouter shared sidecars", () => {
  test("routes every deployment the sidecar hosts", async () => {
    const { router } = createSharedRouter([first, second]);

    const ws = await reconnect(router, [
      first.workflowRunAddress,
      second.workflowRunAddress,
    ]);

    expect(ws.closed).toBe(false);
    expect(router.getRoutableAddresses()).toEqual([
      first.workflowRunAddress,
      second.workflowRunAddress,
    ]);
    expect(await router.isAllocatedSidecarReady(target(first))).toBe(true);
    expect(await router.isAllocatedSidecarReady(target(second))).toBe(true);
  });

  test("refuses work for a co-hosted deployment named under another allocation", async () => {
    const { router } = createSharedRouter([first, second]);
    const ws = await reconnect(router, [
      first.workflowRunAddress,
      second.workflowRunAddress,
    ]);
    const address = second.workflowRunAddress;
    const sentBefore = ws.sent.length;

    const attempts = [
      () =>
        router.sendSignalDeliverToAllocation(target(first), {
          agentAddress: address,
          runId: second.anchorRunId,
          signalName: "go",
          signalId: "signal-1",
          payload: null,
        }),
      () =>
        router.sendWorkflowRunDispatchToAllocation(
          target(first),
          address,
          second.anchorRunId,
          [],
          "raw",
          "user@tenant.example",
          "message-1",
        ),
      () =>
        router.sendPackToAllocation(
          target(first),
          address,
          new Uint8Array(),
          "refs/heads/main",
          "0".repeat(40),
        ),
      () =>
        router.sendProvisionStepToAllocation(
          target(first),
          address,
          TEST_CONFIG,
        ),
    ];

    for (const attempt of attempts) {
      await expect(attempt()).rejects.toThrow(
        new RegExp(`${address} is not (routed on|bound to) allocation alloc-a`),
      );
    }
    expect(ws.sent.length).toBe(sentBefore);
  });

  test("a generation advance detaches only that allocation", async () => {
    const { router } = createSharedRouter([first, second]);
    const disconnects = recordDisconnects(router);
    const ws = await reconnect(router, [
      first.workflowRunAddress,
      second.workflowRunAddress,
    ]);

    router.fenceAllocation(first.allocationId, 2);

    expect(ws.closed).toBe(false);
    expect(router.getRoutableAddresses()).toEqual([second.workflowRunAddress]);
    expect(await router.isAllocatedSidecarReady(target(second))).toBe(true);
    expect(framesOfType(ws, "agent.undeploy")).toEqual([
      {
        type: "agent.undeploy",
        requestId: expect.any(String),
        agentAddress: first.workflowRunAddress,
        generation: 1,
        reason: "Generation 2 superseded it",
      },
    ]);
    expect(disconnects).toEqual([
      {
        ownedAddresses: [first.workflowRunAddress],
        allocated: [target(first)],
      },
    ]);
  });

  test("retiring the last allocation closes the socket", async () => {
    const { router } = createSharedRouter([first]);
    const ws = await reconnect(router, [first.workflowRunAddress]);

    router.retireAllocation(target(first));

    expect(ws.closed).toBe(true);
    expect(framesOfType(ws, "agent.undeploy")).toEqual([
      {
        type: "agent.undeploy",
        requestId: expect.any(String),
        agentAddress: first.workflowRunAddress,
        generation: 1,
        reason: "It was released",
      },
    ]);
    expect(router.getConnectedSidecars()).toEqual([]);
  });

  test("closing the socket reports every allocation it hosted", async () => {
    const { router } = createSharedRouter([first, second]);
    const disconnects = recordDisconnects(router);
    const ws = await reconnect(router, [
      first.workflowRunAddress,
      second.workflowRunAddress,
    ]);

    router.handleClose(ws);

    expect(disconnects).toEqual([
      {
        ownedAddresses: [first.workflowRunAddress, second.workflowRunAddress],
        allocated: [target(first), target(second)],
      },
    ]);
  });

  test("a socket that closes while its registration announces its bindings announces none as connected", async () => {
    const readiness = heldReadiness();
    const { router } = createSharedRouter([first, second], {
      validateSidecarIdentity: readiness.validate,
    });
    const log: string[] = [];
    router.events.on("sidecar.disconnect", ({ allocated }) => {
      for (const lost of allocated) log.push(`disconnect ${lost.allocationId}`);
    });
    router.events.on("sidecar.allocated.connected", ({ allocationId }) => {
      log.push(`connected ${allocationId}`);
    });
    // A wait on the first allocation makes the announce validate its
    // readiness, which is held until the socket has closed.
    const waiting = router.waitForAllocatedSidecar(target(first), 60_000);
    readiness.hold(true);

    const ws = openSocket(router, [
      first.workflowRunAddress,
      second.workflowRunAddress,
    ]);
    await waitUntil(() => readiness.count === 1);
    router.handleClose(ws);
    readiness.settle(0, true);
    // The rest of the registration runs in microtasks, so one macrotask turn
    // sees it finish.
    await tick();

    expect(log).toEqual(["disconnect alloc-a", "disconnect alloc-b"]);
    router.retireAllocation(target(first));
    await expect(waiting).rejects.toThrow();
  });

  test("sync attaches an allocation placed on the connected sidecar", async () => {
    const { router, hosted } = createSharedRouter([first]);
    const connected: unknown[] = [];
    router.events.on("sidecar.allocated.connected", (event) => {
      connected.push(event);
    });
    const ws = await reconnect(router, [first.workflowRunAddress]);
    router.fenceAllocation(second.allocationId, second.generation);
    const ready = router.waitForAllocatedSidecar(target(second), 60_000);

    hosted.bindings = [first, second];
    await router.syncSidecar(SIDECAR);
    await ready;
    await router.syncSidecar(SIDECAR);

    expect(ws.closed).toBe(false);
    expect(connected).toEqual([target(first), target(second)]);
    expect(await router.isAllocatedSidecarReady(target(first))).toBe(true);
  });

  test("sync detaches an allocation that left the sidecar", async () => {
    const { router, hosted } = createSharedRouter([first, second]);
    const ws = await reconnect(router, [
      first.workflowRunAddress,
      second.workflowRunAddress,
    ]);

    hosted.bindings = [second];
    await router.syncSidecar(SIDECAR);

    expect(ws.closed).toBe(false);
    expect(router.getRoutableAddresses()).toEqual([second.workflowRunAddress]);
    expect(framesOfType(ws, "agent.undeploy")).toEqual([
      {
        type: "agent.undeploy",
        requestId: expect.any(String),
        agentAddress: first.workflowRunAddress,
        generation: 1,
        reason: "Its binding is no longer current",
      },
    ]);
  });

  test("a sync that changes nothing leaves a readiness check in flight ready", async () => {
    const readiness = heldReadiness();
    const { router } = createSharedRouter([first, second], {
      validateSidecarIdentity: readiness.validate,
    });
    await reconnect(router, [
      first.workflowRunAddress,
      second.workflowRunAddress,
    ]);

    readiness.hold(true);
    const ready = router.isAllocatedSidecarReady(target(first));
    await tick();
    await router.syncSidecar(SIDECAR);
    readiness.settle(0, true);

    expect(await ready).toBe(true);
  });

  test("a sync that changes nothing lets a parked wait finish", async () => {
    const readiness = heldReadiness();
    const { router } = createSharedRouter([first, second], {
      validateSidecarIdentity: readiness.validate,
    });
    await reconnect(router, [
      first.workflowRunAddress,
      second.workflowRunAddress,
    ]);

    readiness.hold(true);
    const ready = router.waitForAllocatedSidecar(target(first), 60_000);
    await tick();
    // The wait's own check fails to validate, so it parks and validates again.
    readiness.settle(0, new Error("database unavailable"));
    await tick();
    expect(readiness.count).toBe(2);
    await router.syncSidecar(SIDECAR);
    readiness.settle(1, true);

    await ready;
  });

  test("sync turns a probe into the allocation that adopted it", async () => {
    const adopted = allocation(probe.allocationId, probe.generation);
    const { router, hosted } = createSharedRouter([probe]);
    const ws = await reconnect(router);

    hosted.bindings = [adopted];
    await router.syncSidecar(SIDECAR);
    const deployed = router.sendAgentDeployToAllocation(
      target(adopted),
      adopted.workflowRunAddress,
      configFor(adopted),
    );
    await ws.awaitSent((sent) =>
      sent.some((raw) => raw.includes('"agent.deploy"')),
    );
    router.handleMessage(
      ws,
      deployReply(ws, { publicKey: PUBLIC_KEY }, adopted.workflowRunAddress),
    );

    expect(await deployed).toEqual({ publicKey: PUBLIC_KEY });
    expect(ws.closed).toBe(false);
  });

  test("a probe binding its allocation adopted is not held as the allocation", async () => {
    const adopted = allocation(probe.allocationId, probe.generation);
    let probeCurrent = true;
    const { router, hosted } = createSharedRouter([probe], {
      validateSidecarIdentity: async (identity) =>
        identity.kind !== "probe" || probeCurrent,
    });
    const ws = await reconnect(router);
    probeCurrent = false;
    hosted.bindings = [adopted];

    // Validating the held probe would detach it and close the socket, so the
    // caller syncs instead.
    expect(router.holdsAllocatedBinding(target(adopted))).toBe(false);
    await router.syncSidecar(SIDECAR);

    expect(router.holdsAllocatedBinding(target(adopted))).toBe(true);
    expect(await router.isAllocatedSidecarReady(target(adopted))).toBe(true);
    expect(ws.closed).toBe(false);
  });

  test("reconnect undeploys a deployment whose first deploy never completed", async () => {
    const { router } = createSharedRouter([first, second], {
      validateSidecarIdentity: async (identity, use) =>
        (use !== "reclaim" && use !== "retention") ||
        identity.allocationId !== second.allocationId,
    });

    const ws = await reconnect(router, [
      first.workflowRunAddress,
      second.workflowRunAddress,
    ]);

    expect(router.getRoutableAddresses()).toEqual([first.workflowRunAddress]);
    expect(framesOfType(ws, "agent.undeploy")).toEqual([
      {
        type: "agent.undeploy",
        requestId: expect.any(String),
        agentAddress: second.workflowRunAddress,
        generation: 1,
        reason: "The Hub does not keep this incarnation on this sidecar",
      },
    ]);
    // The binding is current, so it attaches, but its deployment is not
    // routed.
    expect(await router.isAllocatedSidecarReady(target(second))).toBe(true);
    expect(await router.isAllocatedWorkflowActive(target(second))).toBe(false);
  });

  test("validates a full inventory concurrently and rechecks its fences before routing", async () => {
    const bindings = Array.from({ length: 128 }, (_, index) =>
      allocation(`parallel-${String(index)}`),
    );
    const release = Promise.withResolvers<undefined>();
    const concurrent = Promise.withResolvers<undefined>();
    let inFlight = 0;
    let maximumInFlight = 0;
    const { router } = createSharedRouter(bindings, {
      validateSidecarIdentity: async (_identity, use) => {
        if (use !== "reclaim") return true;
        inFlight += 1;
        maximumInFlight = Math.max(maximumInFlight, inFlight);
        if (inFlight === 2) concurrent.resolve(undefined);
        await release.promise;
        inFlight -= 1;
        return true;
      },
    });
    const addresses = bindings.map((binding) => binding.workflowRunAddress);
    const ws = openSocket(router, addresses);
    try {
      await concurrent.promise;
      expect(router.getRoutableAddresses()).toEqual([]);
      expect(framesOfType(ws, "welcome")).toEqual([]);
      router.fenceAllocation("parallel-0", 2);
      release.resolve(undefined);
      await ws.awaitSent((sent) =>
        sent.some((raw) => raw.includes('"welcome"')),
      );

      expect(maximumInFlight).toBeGreaterThan(1);
      expect(maximumInFlight).toBeLessThanOrEqual(8);
      expect(router.getRoutableAddresses()).toEqual(addresses.slice(1));
      expect(framesOfType(ws, "agent.undeploy")).toMatchObject([
        { agentAddress: addresses[0], generation: 1 },
      ]);
    } finally {
      release.resolve(undefined);
      router.handleClose(ws);
    }
  });

  test("a failed concurrent validation publishes no partial routes", async () => {
    const firstCheck = Promise.withResolvers<boolean>();
    const secondCheck = Promise.withResolvers<boolean>();
    const secondStarted = Promise.withResolvers<undefined>();
    const closed = Promise.withResolvers<undefined>();
    const { router } = createSharedRouter([first, second], {
      validateSidecarIdentity: async (identity, use) => {
        if (use !== "reclaim") return true;
        if (identity.allocationId === first.allocationId)
          return firstCheck.promise;
        secondStarted.resolve(undefined);
        return secondCheck.promise;
      },
    });
    const ws = createMockWs();
    const close = ws.close.bind(ws);
    ws.close = () => {
      close();
      closed.resolve(undefined);
    };
    router.handleOpen(ws);
    sendHandshake(router, ws, [
      first.workflowRunAddress,
      second.workflowRunAddress,
    ]);
    await secondStarted.promise;
    firstCheck.reject(new Error("database unavailable"));
    await closed.promise;
    secondCheck.resolve(true);
    await tick();

    expect(router.getRoutableAddresses()).toEqual([]);
    expect(framesOfType(ws, "welcome")).toEqual([]);
  });

  test("keeps a retained copy unrouted until its allocation leaves", async () => {
    const { router } = createSharedRouter([first, second], {
      validateSidecarIdentity: async (identity, use) =>
        use !== "reclaim" || identity.allocationId !== first.allocationId,
    });

    const ws = await reconnect(router, [
      first.workflowRunAddress,
      second.workflowRunAddress,
    ]);

    // Its local state is kept for the deployment's retention.
    expect(framesOfType(ws, "agent.undeploy")).toEqual([]);
    expect(router.getRoutableAddresses()).toEqual([second.workflowRunAddress]);
    expect(framesOfType(ws, "welcome")).toEqual([
      {
        type: "welcome",
        routed: [{ address: second.workflowRunAddress, generation: 1 }],
      },
    ]);

    router.fenceAllocation(first.allocationId, 2);

    expect(framesOfType(ws, "agent.undeploy")).toEqual([
      {
        type: "agent.undeploy",
        requestId: expect.any(String),
        agentAddress: first.workflowRunAddress,
        generation: 1,
        reason: "Generation 2 superseded it",
      },
    ]);
  });

  test("undeploys a retained copy whose allocation leaves while the sidecar registers", async () => {
    let release = (): void => undefined;
    const { router } = createSharedRouter([first, second], {
      validateSidecarIdentity: async (identity, use) => {
        if (identity.allocationId !== first.allocationId) return true;
        if (use === "reclaim") return false;
        if (use === "retention") release();
        return true;
      },
    });
    release = () => {
      router.fenceAllocation(first.allocationId, 2);
    };

    const ws = await reconnect(router, [
      first.workflowRunAddress,
      second.workflowRunAddress,
    ]);

    expect(framesOfType(ws, "agent.undeploy")).toEqual([
      {
        type: "agent.undeploy",
        requestId: expect.any(String),
        agentAddress: first.workflowRunAddress,
        generation: 1,
        reason: "The deployment is not current on this sidecar",
      },
    ]);
    expect(router.getRoutableAddresses()).toEqual([second.workflowRunAddress]);
  });

  test("undeploys a retained copy when the sidecar hosts nothing current", async () => {
    const { router } = createSharedRouter([first], {
      validateSidecarIdentity: async (_identity, use) => use !== "reclaim",
    });
    router.fenceAllocation(first.allocationId, 2);

    const ws = await reconnect(router, [first.workflowRunAddress]);

    expect(framesOfType(ws, "agent.undeploy")).toEqual([
      {
        type: "agent.undeploy",
        requestId: expect.any(String),
        agentAddress: first.workflowRunAddress,
        generation: 1,
        reason: "The deployment is not current on this sidecar",
      },
    ]);
    expect(ws.closed).toBe(true);
  });

  test("a sidecar reconnecting on a new socket takes every allocation along", async () => {
    const { router } = createSharedRouter([first, second]);
    const disconnects = recordDisconnects(router);
    const addresses = [first.workflowRunAddress, second.workflowRunAddress];
    const previous = await reconnect(router, addresses);

    const next = await reconnect(router, addresses);

    expect(previous.closed).toBe(true);
    expect(next.closed).toBe(false);
    expect(disconnects).toEqual([{ ownedAddresses: addresses, allocated: [] }]);
    expect(router.getRoutableAddresses()).toEqual(addresses);
    expect(await router.isAllocatedSidecarReady(target(first))).toBe(true);
    expect(await router.isAllocatedSidecarReady(target(second))).toBe(true);
  });

  test("detaching an allocation fails only its own in-flight deploy", async () => {
    const { router } = createSharedRouter([first, second]);
    const ws = await reconnect(router);
    const firstDeploy = router
      .sendAgentDeployToAllocation(
        target(first),
        first.workflowRunAddress,
        configFor(first),
      )
      .catch((error: unknown) => error);
    const secondDeploy = router.sendAgentDeployToAllocation(
      target(second),
      second.workflowRunAddress,
      configFor(second),
    );
    await ws.awaitSent(
      (sent) =>
        sent.filter((raw) => raw.includes('"agent.deploy"')).length === 2,
    );

    router.fenceAllocation(first.allocationId, 2);
    router.handleMessage(
      ws,
      deployReply(ws, { publicKey: PUBLIC_KEY }, second.workflowRunAddress),
    );

    expect(await firstDeploy).toMatchObject({ frameSent: true });
    expect(await secondDeploy).toEqual({ publicKey: PUBLIC_KEY });
    expect(framesOfType(ws, "agent.undeploy")).toEqual([
      {
        type: "agent.undeploy",
        requestId: expect.any(String),
        agentAddress: first.workflowRunAddress,
        generation: 1,
        reason: "Generation 2 superseded it",
      },
    ]);
  });

  test("releasing a probe leaves the sidecar's deployments connected", async () => {
    const closure: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [],
      entries: [],
    };
    const { router } = createSharedRouter([first, probe]);
    const ws = await reconnect(router, [first.workflowRunAddress]);
    const probing = router
      .sendProbeToAllocation(target(probe), {
        source: { kind: "registry", registry: "npmjs" },
        closure,
        entry: "./workflow.js",
      })
      .catch((error: unknown) => error);
    await ws.awaitSent((sent) =>
      sent.some((raw) => raw.includes('"workflow.probe.request"')),
    );

    router.detachAllocation(target(probe));

    expect(await probing).toBeInstanceOf(Error);
    expect(ws.closed).toBe(false);
    expect(router.getRoutableAddresses()).toEqual([first.workflowRunAddress]);
    expect(framesOfType(ws, "agent.undeploy")).toEqual([]);
  });

  test("an allocation leaving keeps the probe the sidecar still hosts", async () => {
    const closure: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [],
      entries: [],
    };
    const { router } = createSharedRouter([first, probe]);
    const ws = await reconnect(router, [first.workflowRunAddress]);
    let probeSettled = false;
    void router
      .sendProbeToAllocation(target(probe), {
        source: { kind: "registry", registry: "npmjs" },
        closure,
        entry: "./workflow.js",
      })
      .finally(() => {
        probeSettled = true;
      })
      .catch(() => undefined);
    await ws.awaitSent((sent) =>
      sent.some((raw) => raw.includes('"workflow.probe.request"')),
    );

    router.fenceAllocation(first.allocationId, 2);
    router.handleMessage(ws, undeployAck(ws, first.workflowRunAddress));
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "agent.event",
        agentAddress: first.workflowRunAddress,
        generation: 1,
        sessionId: "session-late",
        event: { type: "reactor.start", seq: 0, data: {} },
      }),
    );
    await tick();

    expect(ws.closed).toBe(false);
    expect(probeSettled).toBe(false);
  });

  test("a deploy that timed out is still undeployed when its allocation leaves", async () => {
    const { router } = createSharedRouter([first, second], {
      requestTimeoutMs: 1,
    });
    const ws = await reconnect(router);
    const deployed = router
      .sendAgentDeployToAllocation(
        target(first),
        first.workflowRunAddress,
        configFor(first),
      )
      .catch((error: unknown) => error);
    expect(await deployed).toBeInstanceOf(Error);

    router.fenceAllocation(first.allocationId, 2);

    expect(ws.closed).toBe(false);
    expect(framesOfType(ws, "agent.undeploy")).toEqual([
      {
        type: "agent.undeploy",
        requestId: expect.any(String),
        agentAddress: first.workflowRunAddress,
        generation: 1,
        reason: "Generation 2 superseded it",
      },
    ]);
  });
});

describe("SidecarRouter work placed while a shared sidecar connects", () => {
  test.each(["queued", "reading"])(
    "a cancelled %s sync cannot attach a late probe binding",
    async (phase) => {
      const entered = Promise.withResolvers<undefined>();
      const release = Promise.withResolvers<undefined>();
      let holding = false;
      let reads = 0;
      const { router, hosted } = createSharedRouter([first], {
        async resolveSidecarBindings() {
          reads += 1;
          const snapshot = hosted.bindings;
          if (holding) {
            entered.resolve(undefined);
            await release.promise;
          }
          return [...snapshot];
        },
      });
      const ws = await reconnect(router, [first.workflowRunAddress]);
      const controller = new AbortController();
      const cancelled = new Error("Probe connection expired");
      holding = true;
      const preceding =
        phase === "queued" ? router.syncSidecar(SIDECAR) : undefined;
      if (preceding !== undefined) await entered.promise;
      hosted.bindings = [first, probe];
      router.fenceAllocation(probe.allocationId, probe.generation);
      const syncing = router
        .syncSidecar(SIDECAR, controller.signal)
        .catch((error: unknown) => error);
      try {
        await entered.promise;
        controller.abort(cancelled);
        release.resolve(undefined);
        await preceding;
        expect(await syncing).toBe(cancelled);
        expect(reads).toBe(2);
        expect(await router.isAllocatedSidecarReady(target(probe))).toBe(false);
        expect(await router.isAllocatedSidecarReady(target(first))).toBe(true);
        expect(router.getRoutableAddresses()).toEqual([
          first.workflowRunAddress,
        ]);
        expect(ws.closed).toBe(false);
      } finally {
        release.resolve(undefined);
        await Promise.allSettled([syncing, preceding]);
        router.handleClose(ws);
      }
    },
  );

  test("cancels a readiness waiter while its notification validation is pending", async () => {
    const entered = Promise.withResolvers<undefined>();
    const release = Promise.withResolvers<boolean>();
    const { router, hosted } = createSharedRouter([first], {
      async validateSidecarIdentity(identity, use) {
        if (
          identity.allocationId === probe.allocationId &&
          use === "readiness"
        ) {
          entered.resolve(undefined);
          return release.promise;
        }
        return true;
      },
    });
    const ws = await reconnect(router, [first.workflowRunAddress]);
    router.fenceAllocation(probe.allocationId, probe.generation);
    const controller = new AbortController();
    const cancelled = new Error("Probe connection expired");
    const waiting = router
      .waitForAllocatedSidecar(
        target(probe),
        60_000,
        undefined,
        controller.signal,
      )
      .catch((error: unknown) => error);
    hosted.bindings = [first, probe];
    const syncing = router.syncSidecar(SIDECAR);
    try {
      await entered.promise;
      controller.abort(cancelled);
      expect(await waiting).toBe(cancelled);
      release.resolve(true);
      await syncing;
      expect(await router.isAllocatedSidecarReady(target(probe))).toBe(true);
      expect(ws.closed).toBe(false);
    } finally {
      release.resolve(true);
      controller.abort(cancelled);
      await Promise.allSettled([waiting, syncing]);
      router.handleClose(ws);
    }
  });

  test("attaches a placement committed while the handshake authenticates", async () => {
    const authenticated = Promise.withResolvers<boolean>();
    const { router, hosted } = createSharedRouter([first], {
      authenticateSidecar: async ({ sidecarId }) => {
        await authenticated.promise;
        return { sidecarId };
      },
    });
    const ws = openSocket(router);
    await tick();

    hosted.bindings = [first, probe];
    router.fenceAllocation(probe.allocationId, probe.generation);
    await router.syncSidecar(SIDECAR);
    const ready = router.waitForAllocatedSidecar(target(probe), 60_000);
    authenticated.resolve(true);

    await ready;
    expect(ws.closed).toBe(false);
  });

  test("a sync issued while the sidecar registers runs after the registration", async () => {
    const validated = Promise.withResolvers<boolean>();
    let holdValidation = true;
    const { router, hosted } = createSharedRouter([first], {
      validateSidecarIdentity: async (_identity, use) => {
        if (use === "registration" && holdValidation) await validated.promise;
        return true;
      },
    });
    const ws = openSocket(router);
    await tick();

    hosted.bindings = [first, second];
    router.fenceAllocation(second.allocationId, second.generation);
    let synced = false;
    const syncing = router.syncSidecar(SIDECAR).then(() => {
      synced = true;
    });
    await tick();
    expect(synced).toBe(false);
    holdValidation = false;
    validated.resolve(true);

    await syncing;
    expect(ws.closed).toBe(false);
    expect(await router.isAllocatedSidecarReady(target(first))).toBe(true);
    expect(await router.isAllocatedSidecarReady(target(second))).toBe(true);
  });

  test("an allocation synced onto the previous socket moves with the reconnect", async () => {
    const authenticated = Promise.withResolvers<boolean>();
    let holdAuthentication = false;
    const { router, hosted } = createSharedRouter([first], {
      authenticateSidecar: async ({ sidecarId }) => {
        if (holdAuthentication) await authenticated.promise;
        return { sidecarId };
      },
    });
    const disconnects = recordDisconnects(router);
    const previous = await reconnect(router, [first.workflowRunAddress]);
    holdAuthentication = true;
    const next = openSocket(router, [first.workflowRunAddress]);
    await tick();

    hosted.bindings = [first, second];
    router.fenceAllocation(second.allocationId, second.generation);
    await router.syncSidecar(SIDECAR);
    authenticated.resolve(true);
    await tick();

    expect(previous.closed).toBe(true);
    expect(next.closed).toBe(false);
    expect(disconnects).toEqual([
      { ownedAddresses: [first.workflowRunAddress], allocated: [] },
    ]);
    expect(router.getRoutableAddresses()).toEqual([first.workflowRunAddress]);
    expect(await router.isAllocatedSidecarReady(target(second))).toBe(true);
    expect(framesOfType(previous, "agent.undeploy")).toEqual([]);
  });

  test("a sync queued behind a rejected registration reaches the connected socket", async () => {
    const held = heldReads();
    const { router, hosted } = createSharedRouter([first], {
      resolveSidecarBindings: () => held.read(hosted.bindings),
    });
    const connected = await reconnect(router, [first.workflowRunAddress]);
    held.hold(true);
    const rejected = openSocket(router, [first.workflowRunAddress]);
    await tick();

    hosted.bindings = [first, second];
    router.fenceAllocation(second.allocationId, second.generation);
    const syncing = router.syncSidecar(SIDECAR);
    held.hold(false);
    held.reads[0]?.resolve(false);
    await syncing;

    await router.waitForAllocatedSidecar(target(second), 60_000);
    expect(rejected.closed).toBe(true);
    expect(connected.closed).toBe(false);
    expect(router.getRoutableAddresses()).toEqual([first.workflowRunAddress]);
  });

  test("a sync queued behind a closed registration reaches the connected socket", async () => {
    const held = heldReads();
    const { router, hosted } = createSharedRouter([first], {
      resolveSidecarBindings: () => held.read(hosted.bindings),
    });
    const connected = await reconnect(router, [first.workflowRunAddress]);
    held.hold(true);
    const closing = openSocket(router, [first.workflowRunAddress]);
    await tick();

    hosted.bindings = [first, probe];
    router.fenceAllocation(probe.allocationId, probe.generation);
    const syncing = router.syncSidecar(SIDECAR);
    router.handleClose(closing);
    held.hold(false);
    held.reads[0]?.resolve(true);
    await syncing;

    await router.waitForAllocatedSidecar(target(probe), 60_000);
    expect(connected.closed).toBe(false);
    expect(router.getConnectedSidecars()).toEqual([SIDECAR]);
  });

  test("a sync queued behind a superseded registration lands on the newer one", async () => {
    const held = heldReads();
    const { router, hosted } = createSharedRouter([first], {
      resolveSidecarBindings: () => held.read(hosted.bindings),
    });
    const serving = await reconnect(router, [first.workflowRunAddress]);
    held.hold(true);
    const earlier = openSocket(router, [first.workflowRunAddress]);
    await tick();

    hosted.bindings = [first, probe];
    router.fenceAllocation(probe.allocationId, probe.generation);
    const syncing = router.syncSidecar(SIDECAR);
    const later = openSocket(router, [first.workflowRunAddress]);
    await tick();
    expect(held.reads).toHaveLength(2);
    held.hold(false);
    held.reads[0]?.resolve(true);
    await tick();
    held.reads[1]?.resolve(true);
    await syncing;

    await router.waitForAllocatedSidecar(target(probe), 60_000);
    expect(serving.closed).toBe(true);
    expect(earlier.closed).toBe(true);
    expect(later.closed).toBe(false);
  });

  for (const stage of ["authenticating", "registering"] as const) {
    test(`a socket that closes while ${stage} is never registered`, async () => {
      const held = Promise.withResolvers<boolean>();
      const { router } = createSharedRouter([first], {
        authenticateSidecar: async ({ sidecarId }) => {
          if (stage === "authenticating") await held.promise;
          return { sidecarId };
        },
        validateSidecarIdentity: async (_identity, use) => {
          if (stage === "registering" && use === "registration")
            await held.promise;
          return true;
        },
      });
      const ws = openSocket(router, [first.workflowRunAddress]);
      await tick();

      router.handleClose(ws);
      held.resolve(true);
      await tick();

      expect(router.getConnectedSidecars()).toEqual([]);
      expect(router.getRoutableAddresses()).toEqual([]);
      expect(await router.isAllocatedSidecarReady(target(first))).toBe(false);
    });
  }

  test("the latest handshake takes the sidecar over", async () => {
    const held = heldReads();
    const { router, hosted } = createSharedRouter([first], {
      resolveSidecarBindings: () => held.read(hosted.bindings),
    });
    const disconnects = recordDisconnects(router);
    const serving = await reconnect(router, [first.workflowRunAddress]);
    held.hold(true);
    const earlier = openSocket(router, [first.workflowRunAddress]);
    const later = openSocket(router, [first.workflowRunAddress]);
    await tick();
    expect(held.reads).toHaveLength(2);

    held.reads[0]?.resolve(true);
    await tick();
    expect(earlier.closed).toBe(true);
    expect(serving.closed).toBe(false);

    held.reads[1]?.resolve(true);
    await tick();
    expect(serving.closed).toBe(true);
    expect(later.closed).toBe(false);
    expect(router.getRoutableAddresses()).toEqual([first.workflowRunAddress]);
    expect(disconnects).toEqual([
      { ownedAddresses: [first.workflowRunAddress], allocated: [] },
    ]);
  });

  test("a repeated handshake that loses its claim leaves its socket to the newer one", async () => {
    const held = heldReads();
    const { router, hosted } = createSharedRouter([first], {
      resolveSidecarBindings: () => held.read(hosted.bindings),
    });
    const disconnects = recordDisconnects(router);
    const serving = await reconnect(router, [first.workflowRunAddress]);
    held.hold(true);
    sendHandshake(router, serving, [first.workflowRunAddress]);
    await tick();
    const next = openSocket(router, [first.workflowRunAddress]);
    await tick();
    expect(held.reads).toHaveLength(2);

    held.reads[0]?.resolve(true);
    await tick();
    expect(serving.closed).toBe(false);
    held.reads[1]?.resolve(true);
    await tick();

    expect(serving.closed).toBe(true);
    expect(next.closed).toBe(false);
    expect(await router.isAllocatedSidecarReady(target(first))).toBe(true);
    expect(disconnects).toEqual([
      { ownedAddresses: [first.workflowRunAddress], allocated: [] },
    ]);
  });

  test("a repeated handshake that loses its claim to a failing one keeps serving", async () => {
    const held = heldReads();
    const { router, hosted } = createSharedRouter([first], {
      resolveSidecarBindings: () => held.read(hosted.bindings),
    });
    const serving = await reconnect(router, [first.workflowRunAddress]);
    held.hold(true);
    sendHandshake(router, serving, [first.workflowRunAddress]);
    await tick();
    const next = openSocket(router, [first.workflowRunAddress]);
    await tick();
    expect(held.reads).toHaveLength(2);

    held.reads[0]?.resolve(true);
    await tick();
    held.reads[1]?.resolve(false);
    await tick();

    expect(next.closed).toBe(true);
    expect(serving.closed).toBe(false);
    expect(router.getConnectedSidecars()).toEqual([SIDECAR]);
    expect(await router.isAllocatedSidecarReady(target(first))).toBe(true);
  });

  test("a registration whose read went stale reads again", async () => {
    const validated = Promise.withResolvers<boolean>();
    let holdValidation = false;
    const { router, hosted } = createSharedRouter([first], {
      validateSidecarIdentity: async (_identity, use) => {
        if (use === "registration" && holdValidation) await validated.promise;
        return true;
      },
    });
    const previous = await reconnect(router, [first.workflowRunAddress]);
    holdValidation = true;
    const next = openSocket(router, [first.workflowRunAddress]);
    await tick();

    hosted.bindings = [first, probe];
    router.fenceAllocation(probe.allocationId, probe.generation);
    const syncing = router.syncSidecar(SIDECAR);
    hosted.bindings = [probe];
    router.retireAllocation(target(first));
    holdValidation = false;
    validated.resolve(true);
    await syncing;

    expect(previous.closed).toBe(true);
    expect(next.closed).toBe(false);
    expect(await router.isAllocatedSidecarReady(target(probe))).toBe(true);
    expect(framesOfType(next, "agent.undeploy")).toEqual([
      {
        type: "agent.undeploy",
        requestId: expect.any(String),
        agentAddress: first.workflowRunAddress,
        generation: 1,
        reason: "The Hub does not keep this incarnation on this sidecar",
      },
    ]);
  });
});

describe("SidecarRouter deploy replies on a shared sidecar", () => {
  function deployFrames(ws: TestWs): number {
    return ws.sent.filter((raw) => raw.includes('"agent.deploy"')).length;
  }

  for (const late of ["ack", "error"] as const) {
    test(`a late deploy ${late} for a superseded generation leaves the next deploy pending`, async () => {
      const { router, hosted } = createSharedRouter([first, second]);
      const ws = await reconnect(router);
      const superseded = router
        .sendAgentDeployToAllocation(
          target(first),
          first.workflowRunAddress,
          configFor(first),
        )
        .catch((error: unknown) => error);
      await ws.awaitSent(() => deployFrames(ws) === 1);
      // The sidecar's answer to generation 1's deploy, arriving only after
      // generation 2's deploy is in flight.
      const lateReply = deployReply(
        ws,
        late === "ack"
          ? { publicKey: "cd".repeat(32) }
          : { error: "deploy failed" },
        first.workflowRunAddress,
      );

      const next = allocation(first.allocationId, 2);
      router.fenceAllocation(next.allocationId, next.generation);
      expect(await superseded).toMatchObject({ frameSent: true });
      hosted.bindings = [next, second];
      await router.syncSidecar(SIDECAR);
      router.handleMessage(ws, undeployAck(ws, first.workflowRunAddress));
      let settled = false;
      const deploying = router
        .sendAgentDeployToAllocation(
          target(next),
          next.workflowRunAddress,
          configFor(next),
        )
        .finally(() => {
          settled = true;
        });
      await ws.awaitSent(() => deployFrames(ws) === 2);

      router.handleMessage(ws, lateReply);
      await tick();
      expect(settled).toBe(false);

      router.handleMessage(
        ws,
        deployReply(ws, { publicKey: PUBLIC_KEY }, first.workflowRunAddress),
      );
      expect(await deploying).toEqual({ publicKey: PUBLIC_KEY });
    });
  }

  test("a deploy frame that failed to send awaits no reply", async () => {
    const { router } = createSharedRouter([first]);
    const ws = await reconnect(router);
    const send = ws.send.bind(ws);
    ws.send = () => {
      throw new Error("socket busy");
    };
    const unsent = await router
      .sendAgentDeployToAllocation(
        target(first),
        first.workflowRunAddress,
        configFor(first),
      )
      .catch((error: unknown) => error);
    expect(unsent).toMatchObject({ frameSent: false });
    ws.send = send;

    const deploying = router.sendAgentDeployToAllocation(
      target(first),
      first.workflowRunAddress,
      configFor(first),
    );
    await ws.awaitSent(() => deployFrames(ws) === 1);
    router.handleMessage(
      ws,
      deployReply(ws, { publicKey: PUBLIC_KEY }, first.workflowRunAddress),
    );

    expect(await deploying).toEqual({ publicKey: PUBLIC_KEY });
  });
});

describe("SidecarRouter pushes on a shared sidecar", () => {
  test("rejects a push that arrives after its allocation left the sidecar, once", async () => {
    const { router } = createSharedRouter([first, second]);
    const ws = await reconnect(router, [
      first.workflowRunAddress,
      second.workflowRunAddress,
    ]);
    const repoId = { kind: "workflow-run", id: first.anchorRunId } as const;

    router.fenceAllocation(first.allocationId, 2);
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "repo.pack.push",
        agentAddress: first.workflowRunAddress,
        generation: 1,
        repoId,
        transferId: "late-push",
        seq: 0,
        data: "AAAA",
      }),
    );
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "repo.pack.done",
        agentAddress: first.workflowRunAddress,
        generation: 1,
        repoId,
        transferId: "late-push",
        ref: "refs/heads/main",
        commitSha: "a".repeat(40),
      }),
    );
    await tick();

    const rejected = {
      type: "repo.pack.reject",
      agentAddress: first.workflowRunAddress,
      repoId,
      transferId: "late-push",
      reason: "path_violation",
    };
    expect(framesOfType(ws, "repo.pack.reject")).toEqual([rejected]);
    expect(ws.closed).toBe(false);
  });
});

describe("SidecarRouter replacing a generation on the same sidecar", () => {
  test("deploys an address again only once the sidecar answers its undeploy, crediting nothing the removed copy sent", async () => {
    const { router } = createSharedRouter([first]);
    // The sidecar reports generation 1 still deploying from an earlier
    // connection, so the Hub undeploys it, and a deploy of the address on the
    // same connection waits for that undeploy's answer.
    const ws = await reconnect(router, [
      { address: first.workflowRunAddress, generation: 1, state: "deploying" },
    ]);
    const credited: unknown[] = [];
    router.events.on("agent.event", (event) => {
      credited.push(event);
    });
    const event = JSON.stringify({
      type: "agent.event",
      agentAddress: first.workflowRunAddress,
      generation: 1,
      sessionId: "session-1",
      event: { type: "reactor.start", seq: 0, data: {} },
    });

    const deploying = router.sendAgentDeployToAllocation(
      target(first),
      first.workflowRunAddress,
      configFor(first),
    );
    await tick();
    expect(framesOfType(ws, "agent.deploy")).toEqual([]);

    // The removed copy is still sending when its undeploy is answered.
    router.handleMessage(ws, event);
    router.handleMessage(ws, undeployAck(ws, first.workflowRunAddress));
    await ws.awaitSent((sent) =>
      sent.some((raw) => raw.includes('"agent.deploy"')),
    );
    router.handleMessage(
      ws,
      deployReply(ws, { publicKey: PUBLIC_KEY }, first.workflowRunAddress),
    );
    expect(await deploying).toEqual({ publicKey: PUBLIC_KEY });
    expect(credited).toEqual([]);

    router.handleMessage(ws, event);
    await tick();
    expect(credited).toHaveLength(1);
  });

  test("a deploy waiting on an unanswered undeploy gives up after the request timeout", async () => {
    const { router } = createSharedRouter([first], { requestTimeoutMs: 20 });
    await reconnect(router, [
      { address: first.workflowRunAddress, generation: 1, state: "deploying" },
    ]);

    const unanswered = await router
      .sendAgentDeployToAllocation(
        target(first),
        first.workflowRunAddress,
        configFor(first),
      )
      .catch((error: unknown) => error);

    expect(unanswered).toMatchObject({
      frameSent: false,
      message: `${first.workflowRunAddress} is still being undeployed on sidecar ${SIDECAR} after 20ms`,
    });
  });

  for (const outcome of ["stores", "fails"] as const) {
    test(`an ack whose listener ${outcome} after its deploy was replaced leaves the replacement's deploy to its own reply`, async () => {
      const { router, hosted } = createSharedRouter([first, second]);
      const ws = await reconnect(router, [second.workflowRunAddress]);
      const storing = Promise.withResolvers<undefined>();
      let acks = 0;
      router.events.on("agent.deploy.ack", async () => {
        acks += 1;
        if (acks > 1) return;
        await storing.promise;
        if (outcome === "fails") throw new Error("database unavailable");
      });

      const replaced = router
        .sendAgentDeployToAllocation(
          target(first),
          first.workflowRunAddress,
          configFor(first),
        )
        .catch((error: unknown) => error);
      await ws.awaitSent((sent) =>
        sent.some((raw) => raw.includes('"agent.deploy"')),
      );
      router.handleMessage(
        ws,
        deployReply(ws, { publicKey: PUBLIC_KEY }, first.workflowRunAddress),
      );
      await tick();
      expect(acks).toBe(1);

      const next = allocation(first.allocationId, 2);
      router.fenceAllocation(next.allocationId, next.generation);
      hosted.bindings = [next, second];
      await router.syncSidecar(SIDECAR);
      expect(await replaced).toBeInstanceOf(Error);
      router.handleMessage(ws, undeployAck(ws, first.workflowRunAddress));
      let settled = false;
      const replacing = router
        .sendAgentDeployToAllocation(
          target(next),
          next.workflowRunAddress,
          configFor(next),
        )
        .finally(() => {
          settled = true;
        });
      await ws.awaitSent(
        () =>
          framesOfType(ws, "agent.deploy").filter(
            (frame) => frame["generation"] === 2,
          ).length === 1,
      );

      storing.resolve(undefined);
      await tick();
      expect(settled).toBe(false);

      const replacementKey = "cd".repeat(32);
      router.handleMessage(
        ws,
        deployReply(ws, { publicKey: replacementKey }, next.workflowRunAddress),
      );
      expect(await replacing).toEqual({ publicKey: replacementKey });
    });
  }

  test("attaches the replacement at once and credits it nothing the replaced generation sends", async () => {
    const { router, hosted } = createSharedRouter([first, second]);
    const ws = await reconnect(router, [
      first.workflowRunAddress,
      second.workflowRunAddress,
    ]);
    const credited: unknown[] = [];
    router.events.on("agent.event", (event) => {
      credited.push(event);
    });
    router.events.on("mail.inbound.acknowledged", (event) => {
      credited.push(event);
    });
    const next = allocation(first.allocationId, 2);
    router.fenceAllocation(next.allocationId, next.generation);
    hosted.bindings = [next, second];
    await router.syncSidecar(SIDECAR);
    expect(await router.isAllocatedSidecarReady(target(next))).toBe(true);

    const deploying = router.sendAgentDeployToAllocation(
      target(next),
      next.workflowRunAddress,
      configFor(next),
    );
    router.handleMessage(ws, undeployAck(ws, first.workflowRunAddress));
    await ws.awaitSent((sent) =>
      sent.some((raw) => raw.includes('"agent.deploy"')),
    );
    router.handleMessage(
      ws,
      deployReply(ws, { publicKey: PUBLIC_KEY }, next.workflowRunAddress),
    );
    await deploying;

    // Generation 1 is still draining on the sidecar while generation 2 owns
    // the address.
    const event = (generation: number) =>
      JSON.stringify({
        type: "agent.event",
        agentAddress: first.workflowRunAddress,
        generation,
        sessionId: "session-1",
        event: { type: "reactor.start", seq: 0, data: {} },
      });
    router.handleMessage(ws, event(1));
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "mail.inbound.ack",
        agentAddress: first.workflowRunAddress,
        generation: 1,
        messageId: "mail-to-replaced",
      }),
    );
    await tick();
    expect(credited).toEqual([]);

    router.handleMessage(ws, event(2));
    await tick();
    expect(credited).toHaveLength(1);
    expect(ws.closed).toBe(false);
  });

  for (const state of ["live", "stopped", "tearing-down"] as const) {
    test(`undeploys an earlier generation the sidecar still reports ${state} after reconnecting`, async () => {
      const { router, hosted } = createSharedRouter([first, second]);
      const previous = await reconnect(router, [
        first.workflowRunAddress,
        second.workflowRunAddress,
      ]);
      const next = allocation(first.allocationId, 2);
      router.fenceAllocation(next.allocationId, next.generation);
      // The undeploy the fence sent is lost with this connection.
      router.handleClose(previous);

      hosted.bindings = [next, second];
      const ws = await reconnect(router, [
        { address: first.workflowRunAddress, generation: 1, state },
        second.workflowRunAddress,
      ]);

      expect(framesOfType(ws, "agent.undeploy")).toEqual([
        {
          type: "agent.undeploy",
          requestId: expect.any(String),
          agentAddress: first.workflowRunAddress,
          generation: 1,
          reason: "The Hub does not keep this incarnation on this sidecar",
        },
      ]);
      expect(router.getRoutableAddresses()).toEqual([
        second.workflowRunAddress,
      ]);
      expect(await router.isAllocatedSidecarReady(target(next))).toBe(true);
      expect(framesOfType(ws, "welcome")).toEqual([
        {
          type: "welcome",
          routed: [{ address: second.workflowRunAddress, generation: 1 }],
        },
      ]);
      expect(ws.closed).toBe(false);
    });
  }

  for (const state of ["deploying", "tearing-down"] as const) {
    test(`undeploys the current generation instead of routing it when the sidecar reports it ${state}`, async () => {
      const { router } = createSharedRouter([first, second]);

      const ws = await reconnect(router, [
        { address: first.workflowRunAddress, generation: 1, state },
        second.workflowRunAddress,
      ]);

      expect(framesOfType(ws, "agent.undeploy")).toEqual([
        {
          type: "agent.undeploy",
          requestId: expect.any(String),
          agentAddress: first.workflowRunAddress,
          generation: 1,
          reason: "The Hub does not keep this incarnation on this sidecar",
        },
      ]);
      expect(router.getRoutableAddresses()).toEqual([
        second.workflowRunAddress,
      ]);
      expect(framesOfType(ws, "welcome")).toEqual([
        {
          type: "welcome",
          routed: [{ address: second.workflowRunAddress, generation: 1 }],
        },
      ]);
      expect(ws.closed).toBe(false);
    });
  }

  test("keeps a stopped incarnation of a current binding unrouted without undeploying it", async () => {
    const { router } = createSharedRouter([first, second]);

    const ws = await reconnect(router, [
      { address: first.workflowRunAddress, generation: 1, state: "stopped" },
      second.workflowRunAddress,
    ]);

    // Its local state is kept for inspection until the Hub releases the
    // deployment.
    expect(framesOfType(ws, "agent.undeploy")).toEqual([]);
    expect(router.getRoutableAddresses()).toEqual([second.workflowRunAddress]);
    expect(framesOfType(ws, "welcome")).toEqual([
      {
        type: "welcome",
        routed: [{ address: second.workflowRunAddress, generation: 1 }],
      },
    ]);
    expect(ws.closed).toBe(false);
  });

  test.each(
    (["live", "stopped", "tearing-down"] as const).flatMap((state) =>
      [SIDECAR, "sc-replacement"].map((cleanupSidecarId) => ({
        state,
        cleanupSidecarId,
      })),
    ),
  )(
    "hello rechecks cleanup ownership for a $state copy when cleanup belongs to $cleanupSidecarId",
    async ({ state, cleanupSidecarId }) => {
      const checkingNeighbour = Promise.withResolvers<undefined>();
      const finishCheck = Promise.withResolvers<boolean>();
      const { router, hosted } = createSharedRouter([first, second], {
        validateSidecarIdentity: async (identity, use) => {
          if (
            identity.allocationId === second.allocationId &&
            use === "reclaim"
          ) {
            checkingNeighbour.resolve(undefined);
            return finishCheck.promise;
          }
          return true;
        },
      });
      const sameSidecar = cleanupSidecarId === SIDECAR;
      const cleanup = {
        ...first,
        kind: "cleanup",
        sidecarId: cleanupSidecarId,
        generation: sameSidecar ? 2 : 3,
      } as const;
      const ws = openSocket(router, [
        { address: first.workflowRunAddress, generation: 1, state },
        second.workflowRunAddress,
      ]);
      try {
        await checkingNeighbour.promise;
        hosted.bindings = sameSidecar ? [cleanup, second] : [second];
        router.fenceAllocation(first.allocationId, cleanup.generation, {
          cleanup: { sidecarId: cleanupSidecarId },
        });
        finishCheck.resolve(true);
        await ws.awaitSent((sent) =>
          sent.some((raw) => raw.includes('"welcome"')),
        );
        const removals = framesOfType(ws, "agent.undeploy");
        expect(removals).toHaveLength(sameSidecar ? 0 : 1);
        if (!sameSidecar)
          expect(removals[0]).toMatchObject({
            agentAddress: first.workflowRunAddress,
            generation: 1,
          });
        expect(router.getRoutableAddresses()).toEqual([
          second.workflowRunAddress,
        ]);

        // A sync attaches only this sidecar's cleanup binding. Cleanup of a
        // replacement elsewhere must not protect the old copy from removal.
        await router.syncSidecar(SIDECAR);
        expect(router.getCleanupConnection(target(cleanup)) !== undefined).toBe(
          sameSidecar,
        );
        expect(framesOfType(ws, "agent.undeploy")).toHaveLength(
          sameSidecar ? 0 : 1,
        );
        expect(router.getRoutableAddresses()).toEqual([
          second.workflowRunAddress,
        ]);
      } finally {
        finishCheck.resolve(true);
        router.handleClose(ws);
      }
    },
  );

  test.each([false, true])(
    "undeploys a stopped copy released during hello (reported error: %s)",
    async (reportedError) => {
      const checkingNeighbour = Promise.withResolvers<undefined>();
      const finishCheck = Promise.withResolvers<boolean>();
      const { router } = createSharedRouter([first, second], {
        validateSidecarIdentity: async (identity, use) => {
          if (
            identity.allocationId === second.allocationId &&
            use === "reclaim"
          ) {
            checkingNeighbour.resolve(undefined);
            return finishCheck.promise;
          }
          return true;
        },
      });
      const ws = openSocket(router, [
        {
          address: first.workflowRunAddress,
          generation: 1,
          state: "stopped",
          ...(reportedError ? { error: "The child ended itself" } : {}),
        },
        second.workflowRunAddress,
      ]);

      try {
        await checkingNeighbour.promise;
        router.fenceAllocation(first.allocationId, 2);
        finishCheck.resolve(true);
        await ws.awaitSent((sent) =>
          sent.some((raw) => raw.includes('"welcome"')),
        );

        expect(framesOfType(ws, "agent.undeploy")).toEqual([
          {
            type: "agent.undeploy",
            requestId: expect.any(String),
            agentAddress: first.workflowRunAddress,
            generation: 1,
            reason: "The deployment is not current on this sidecar",
          },
        ]);
        expect(router.getRoutableAddresses()).toEqual([
          second.workflowRunAddress,
        ]);
        expect(router.reportedDeploymentFailure(target(first))).toBeUndefined();
        expect(ws.closed).toBe(false);
      } finally {
        finishCheck.resolve(true);
        router.handleClose(ws);
      }
    },
  );

  test("notes why a current binding's incarnation stopped on its own until its fence moves past it", async () => {
    const { router } = createSharedRouter([first, second]);

    await reconnect(router, [
      {
        address: first.workflowRunAddress,
        generation: 1,
        state: "stopped",
        error: "The child ended itself",
      },
      { address: second.workflowRunAddress, generation: 1, state: "stopped" },
    ]);

    expect(router.reportedDeploymentFailure(target(first))).toBe(
      "The child ended itself",
    );
    // A stop the Hub asked for carries no error.
    expect(router.reportedDeploymentFailure(target(second))).toBeUndefined();
    expect(
      router.reportedDeploymentFailure({ ...target(first), generation: 2 }),
    ).toBeUndefined();

    router.fenceAllocation(first.allocationId, 2);
    expect(router.reportedDeploymentFailure(target(first))).toBeUndefined();
  });

  test("keeps a stop report, and when it was first heard, across reconnects", async () => {
    let clock = new Date("2026-10-01T12:00:00.000Z");
    const firstHeard = clock;
    const { router } = createSharedRouter([first], { now: () => clock });
    const stopped: HostedIncarnation = {
      address: first.workflowRunAddress,
      generation: 1,
      state: "stopped",
      error: "The child ended itself",
    };

    const ws = await reconnect(router, [stopped]);
    router.handleClose(ws);
    // The sidecar's reason outlives its connection.
    expect(router.reportedDeploymentFailure(target(first))).toBe(
      "The child ended itself",
    );

    clock = new Date(firstHeard.getTime() + 30_000);
    await reconnect(router, [{ ...stopped, error: "A later reason" }]);
    const history = await router.stoppedDeploymentHistory(target(first));
    expect(history?.reportedAt).toEqual(firstHeard);
    expect(router.reportedDeploymentFailure(target(first))).toBe(
      "The child ended itself",
    );
  });

  test("undeploys a live copy of a generation the sidecar reported stopped", async () => {
    const { router } = createSharedRouter([first, second]);
    const ws = await reconnect(router, [
      {
        address: first.workflowRunAddress,
        generation: 1,
        state: "stopped",
        error: "The child ended itself",
      },
      second.workflowRunAddress,
    ]);
    router.handleClose(ws);

    // A sidecar restart that lost the stopped mark respawned it.
    const restarted = await reconnect(router, [
      first.workflowRunAddress,
      second.workflowRunAddress,
    ]);

    expect(framesOfType(restarted, "agent.undeploy")).toEqual([
      {
        type: "agent.undeploy",
        requestId: expect.any(String),
        agentAddress: first.workflowRunAddress,
        generation: 1,
        reason: "The Hub does not keep this incarnation on this sidecar",
      },
    ]);
    expect(router.getRoutableAddresses()).toEqual([second.workflowRunAddress]);
    expect(await router.isAllocatedWorkflowActive(target(first))).toBe(false);
    expect(router.reportedDeploymentFailure(target(first))).toBe(
      "The child ended itself",
    );
  });

  test("keeps a connection open for a current binding whose reported incarnation it undeploys", async () => {
    const { router } = createSharedRouter([second], {
      validateSidecarIdentity: async (_identity, use) =>
        use !== "reclaim" && use !== "retention",
    });

    const ws = await reconnect(router, [second.workflowRunAddress]);

    expect(ws.closed).toBe(false);
    expect(framesOfType(ws, "agent.undeploy")).toHaveLength(1);
    expect(await router.isAllocatedSidecarReady(target(second))).toBe(true);
    expect(await router.isAllocatedWorkflowActive(target(second))).toBe(false);
  });
});

describe("SidecarRouter mail across a takeover on a shared sidecar", () => {
  test("a takeover during a reconnect's redelivery leaves the retention of mail the new hello does not route", async () => {
    let holdNext = false;
    let release: (() => void) | undefined;
    const undelivered: string[][] = [];
    const { router } = createSharedRouter([first, second], {
      withExecutableWorkflowRun: async (_target, send) => {
        if (holdNext) {
          holdNext = false;
          await new Promise<void>((resolve) => {
            release = resolve;
          });
        }
        return send();
      },
      mailHoldTTLMs: 20,
    });
    router.events.on("mail.outbound.undelivered", ({ recipients }) => {
      undelivered.push(recipients);
    });
    const original = await reconnect(router, [
      first.workflowRunAddress,
      second.workflowRunAddress,
    ]);
    await router.routeMail(first.workflowRunAddress, "bWFpbA==", "u@x", "m-a");
    await router.routeMail(second.workflowRunAddress, "bWFpbA==", "u@x", "m-b");
    router.handleClose(original);

    // The reconnect's redelivery parks in the first deployment's admission.
    holdNext = true;
    const parked = openSocket(router, [
      first.workflowRunAddress,
      second.workflowRunAddress,
    ]);
    await waitUntil(() => release !== undefined);

    // A newer handshake takes the sidecar over before the parked loop reaches
    // the second deployment, whose copy is gone.
    await reconnect(router, [first.workflowRunAddress]);
    expect(parked.closed).toBe(true);
    release?.();

    // The second deployment's mail is still given up when its retention ends.
    await waitUntil(() => undelivered.length > 0);
    expect(undelivered).toEqual([[second.workflowRunAddress]]);
  });
});

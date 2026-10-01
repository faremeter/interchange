import { describe, expect, test } from "bun:test";

import { waitUntil } from "@intx/types/testing";
import type { ToolPackageManifest } from "@intx/types/tool-packages";

import {
  createMockWs,
  deployReply,
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

function sendHandshake(
  router: TestRouter,
  ws: TestWs,
  agentAddresses: string[] = [],
): void {
  router.handleMessage(
    ws,
    JSON.stringify({
      type: "reconnect",
      sidecarId: SIDECAR,
      token: "token",
      agentAddresses,
    }),
  );
}

// Sends the handshake without waiting for its registration to finish.
function openSocket(router: TestRouter, agentAddresses: string[] = []): TestWs {
  const ws = createMockWs();
  router.handleOpen(ws);
  sendHandshake(router, ws, agentAddresses);
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
  agentAddresses: string[] = [],
): Promise<TestWs> {
  const ws = openSocket(router, agentAddresses);
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
        reason: "The deployment is not current on this sidecar",
      },
    ]);
    expect(await router.isAllocatedSidecarReady(target(second))).toBe(true);
    expect(await router.isAllocatedWorkflowActive(target(second))).toBe(false);
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

    router.fenceAllocation(first.allocationId, 2);

    expect(framesOfType(ws, "agent.undeploy")).toEqual([
      {
        type: "agent.undeploy",
        requestId: expect.any(String),
        agentAddress: first.workflowRunAddress,
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
        reason: "The deployment is not current on this sidecar",
      },
    ]);
    expect(router.getRoutableAddresses()).toEqual([second.workflowRunAddress]);
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
        const bindings = hosted.bindings;
        await authenticated.promise;
        return { sidecarId, bindings };
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
        const bindings = hosted.bindings;
        if (holdAuthentication) await authenticated.promise;
        return { sidecarId, bindings };
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
      const { router, hosted } = createSharedRouter([first], {
        authenticateSidecar: async ({ sidecarId }) => {
          const bindings = hosted.bindings;
          if (stage === "authenticating") await held.promise;
          return { sidecarId, bindings };
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
        reason: "The deployment is not current on this sidecar",
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

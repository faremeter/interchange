import { describe, expect, test } from "bun:test";

import type { HostedIncarnation } from "@intx/types/sidecar";
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
        use !== "reclaim" || identity.allocationId !== second.allocationId,
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

  test("validates a repeated reported address once", async () => {
    let reclaims = 0;
    const { router } = createSharedRouter([first], {
      validateSidecarIdentity: async (_identity, use) => {
        if (use !== "reclaim") return true;
        reclaims += 1;
        return false;
      },
    });

    await reconnect(router, [
      first.workflowRunAddress,
      first.workflowRunAddress,
    ]);

    expect(reclaims).toBe(1);
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
      validateSidecarIdentity: async (_identity, use) => use !== "reclaim",
    });

    const ws = await reconnect(router, [second.workflowRunAddress]);

    expect(ws.closed).toBe(false);
    expect(framesOfType(ws, "agent.undeploy")).toHaveLength(1);
    expect(await router.isAllocatedSidecarReady(target(second))).toBe(true);
    expect(await router.isAllocatedWorkflowActive(target(second))).toBe(false);
  });
});

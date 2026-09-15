import { describe, expect, test } from "bun:test";

import type { SidecarAllocation } from "@intx/db";

import {
  createWorkflowDispatchService,
  type WorkflowDispatchServiceDeps,
} from "./workflow-dispatch-service";

const NOW = new Date("2026-08-03T14:00:00.000Z");

type DispatchStore = WorkflowDispatchServiceDeps["dispatchStore"];
type AllocationStore = WorkflowDispatchServiceDeps["allocationStore"];
type ClaimedDispatch = NonNullable<
  Awaited<ReturnType<DispatchStore["claimNextPending"]>>
>;

function dispatch(overrides: Partial<ClaimedDispatch> = {}): ClaimedDispatch {
  return {
    id: "dispatch-1",
    anchorRunId: "deployment-1",
    messageId: "message-1",
    kind: "mail",
    senderAddress: "principal-1@tenant-1.example",
    rawMessage: new TextEncoder().encode("raw mail"),
    stepGrants: [],
    status: "pending",
    acknowledgedGeneration: null,
    attemptCount: 0,
    nextAttemptAt: NOW,
    deliveryLeaseId: null,
    deliveryLeaseExpiresAt: null,
    failureCode: null,
    failureMessage: null,
    createdAt: NOW,
    updatedAt: NOW,
    acknowledgedAt: null,
    settledAt: null,
    ...overrides,
  };
}

function allocation(
  overrides: Partial<SidecarAllocation> = {},
): SidecarAllocation {
  return {
    id: "allocation-1",
    anchorRunId: "deployment-1",
    tenantId: "tenant-1",
    provisionerId: "test",
    provisionerApiVersion: 1,
    provisionerBindingFingerprint: "test:v1",
    sidecarId: "sidecar-1",
    status: "allocated",
    generation: 2,
    ensureAcceptedGeneration: 2,
    ensureAttempts: 1,
    destroyAttempts: 0,
    createdAt: NOW,
    updatedAt: NOW,
    ...overrides,
  };
}

function fakeDispatchStore(
  overrides: Partial<DispatchStore> = {},
): DispatchStore {
  const unused = (name: string) => async () => {
    throw new Error(`unexpected dispatch store call: ${name}`);
  };
  return {
    acknowledge: unused("acknowledge"),
    claimNextPending: async () => null,
    enqueue: unused("enqueue"),
    enqueueSignal: unused("enqueueSignal"),
    requeueUnsettled: unused("requeueUnsettled"),
    scheduleRetry: unused("scheduleRetry"),
    settle: unused("settle"),
    ...overrides,
  };
}

function fakeAllocationStore(
  overrides: Partial<AllocationStore> = {},
): AllocationStore {
  return {
    findByAnchorRunId: async () => null,
    ...overrides,
  };
}

describe("createWorkflowDispatchService", () => {
  test("routes trigger bytes with distinct deployment address and run id", async () => {
    const claimed = dispatch();
    let didClaim = false;
    const deliveries: unknown[][] = [];
    const service = createWorkflowDispatchService({
      dispatchStore: fakeDispatchStore({
        claimNextPending: async () => {
          if (didClaim) return null;
          didClaim = true;
          return claimed;
        },
      }),
      allocationStore: fakeAllocationStore({
        findByAnchorRunId: async () => allocation(),
      }),
      router: {
        sendSignalDeliverToAllocation: async () => {
          throw new Error("must not route a signal");
        },
        sendWorkflowRunDispatchToAllocation: async (...args) => {
          deliveries.push(args);
        },
      },
      resolveAnchorAddress: async () => "run_abc@acme.localhost",
      createLeaseId: () => "lease-1",
      now: () => NOW,
    });

    expect(await service.reconcileUntilIdle()).toBe(1);
    expect(deliveries).toEqual([
      [
        { allocationId: "allocation-1", generation: 2 },
        "run_abc@acme.localhost",
        "run_abc",
        [],
        "cmF3IG1haWw=",
        "principal-1@tenant-1.example",
        "message-1",
        expect.any(AbortSignal),
      ],
    ]);
  });

  for (const reason of ["connecting", "initializing"] as const) {
    test(`retries without routing while an allocation is ${reason}`, async () => {
      const retries: unknown[] = [];
      let didClaim = false;
      const service = createWorkflowDispatchService({
        dispatchStore: fakeDispatchStore({
          claimNextPending: async () => {
            if (didClaim) return null;
            didClaim = true;
            return dispatch({ attemptCount: 2 });
          },
          scheduleRetry: async (args) => {
            retries.push(args);
            return dispatch();
          },
        }),
        allocationStore: fakeAllocationStore({
          findByAnchorRunId: async () =>
            allocation(
              reason === "connecting"
                ? { connectDeadline: new Date(NOW.getTime() + 60_000) }
                : { initializationLeaseId: "old-initializer" },
            ),
        }),
        router: {
          sendSignalDeliverToAllocation: async () => {
            throw new Error("must not route");
          },
          sendWorkflowRunDispatchToAllocation: async () => {
            throw new Error("must not route");
          },
        },
        resolveAnchorAddress: async () => "workflow@tenant.example",
        createLeaseId: () => "lease-1",
        retryDelayMs: () => 1_000,
        now: () => NOW,
      });

      expect(await service.reconcileUntilIdle()).toBe(1);
      expect(retries).toEqual([
        expect.objectContaining({
          dispatchId: "dispatch-1",
          expectedLeaseId: "lease-1",
          code: "allocation_not_ready",
          nextAttemptAt: new Date(NOW.getTime() + 1_000),
        }),
      ]);
    });
  }

  test("delegates inbox acknowledgement to the atomically fenced store", async () => {
    const acknowledgements: unknown[] = [];
    const store = fakeDispatchStore({
      acknowledge: async (args) => {
        acknowledgements.push(args);
        return dispatch({
          status: "acknowledged",
          acknowledgedGeneration: args.generation,
        });
      },
    });
    const service = createWorkflowDispatchService({
      dispatchStore: store,
      allocationStore: fakeAllocationStore(),
      router: {
        sendSignalDeliverToAllocation: async () => undefined,
        sendWorkflowRunDispatchToAllocation: async () => undefined,
      },
      resolveAnchorAddress: async () => "workflow@tenant.example",
      now: () => NOW,
    });

    await service.acknowledge({
      allocationId: "allocation-1",
      anchorRunId: "deployment-1",
      generation: 1,
      messageId: "message-1",
    });
    await service.acknowledge({
      allocationId: "allocation-1",
      anchorRunId: "deployment-1",
      generation: 2,
      messageId: "message-1",
    });

    expect(acknowledgements).toEqual([
      {
        allocationId: "allocation-1",
        anchorRunId: "deployment-1",
        generation: 1,
        messageId: "message-1",
        now: NOW,
      },
      {
        allocationId: "allocation-1",
        anchorRunId: "deployment-1",
        generation: 2,
        messageId: "message-1",
        now: NOW,
      },
    ]);
  });

  test("requeues both pending and acknowledged payloads when a worker is ready", async () => {
    const anchors: string[] = [];
    const service = createWorkflowDispatchService({
      dispatchStore: fakeDispatchStore({
        requeueUnsettled: async (anchorRunId) => {
          anchors.push(anchorRunId);
          return 2;
        },
      }),
      allocationStore: fakeAllocationStore(),
      router: {
        sendSignalDeliverToAllocation: async () => undefined,
        sendWorkflowRunDispatchToAllocation: async () => undefined,
      },
      resolveAnchorAddress: async () => "workflow@tenant.example",
    });

    expect(
      await service.requeueForReadyAllocation(allocation().anchorRunId),
    ).toBe(2);
    expect(anchors).toEqual(["deployment-1"]);
  });

  test("replays a durable signal to the exact accepted generation", async () => {
    const signal = {
      type: "signal.deliver" as const,
      agentAddress: "workflow@tenant.example",
      runId: "workflow@tenant.example",
      signalName: "continue",
      signalId: "signal-1",
      payload: { approved: true },
    };
    let didClaim = false;
    const deliveries: unknown[][] = [];
    const service = createWorkflowDispatchService({
      dispatchStore: fakeDispatchStore({
        claimNextPending: async () => {
          if (didClaim) return null;
          didClaim = true;
          return dispatch({
            kind: "signal",
            messageId: signal.signalId,
            rawMessage: new TextEncoder().encode(JSON.stringify(signal)),
          });
        },
      }),
      allocationStore: fakeAllocationStore({
        findByAnchorRunId: async () => allocation(),
      }),
      router: {
        sendSignalDeliverToAllocation: async (...args) => {
          deliveries.push(args);
        },
        sendWorkflowRunDispatchToAllocation: async () => {
          throw new Error("must not route signal as mail");
        },
      },
      resolveAnchorAddress: async () => signal.agentAddress,
      createLeaseId: () => "lease-1",
      now: () => NOW,
    });

    expect(await service.reconcileUntilIdle()).toBe(1);
    expect(deliveries).toEqual([
      [
        { allocationId: "allocation-1", generation: 2 },
        {
          agentAddress: signal.agentAddress,
          runId: signal.runId,
          signalName: signal.signalName,
          signalId: signal.signalId,
          payload: signal.payload,
        },
        expect.any(AbortSignal),
      ],
    ]);
  });

  test("an expired delivery remains excluded across drains until its database read settles", async () => {
    const stuck = dispatch({
      id: "dispatch-stuck",
      anchorRunId: "deployment-stuck",
      messageId: "message-stuck",
    });
    const healthy = dispatch({
      id: "dispatch-healthy",
      anchorRunId: "deployment-healthy",
      messageId: "message-healthy",
    });
    const leased = new Set<string>();
    const pending = [stuck];
    const claims: string[] = [];
    const exclusions: (readonly string[])[] = [];
    const stuckClaimed = Promise.withResolvers<boolean>();
    const releaseStuck = Promise.withResolvers<string>();
    const delivered: string[] = [];
    let addressReads = 0;
    const service = createWorkflowDispatchService({
      dispatchStore: fakeDispatchStore({
        claimNextPending: async ({ excludedDispatchIds = [] }) => {
          exclusions.push(excludedDispatchIds);
          const next = pending.find(
            (candidate) =>
              !leased.has(candidate.id) &&
              !excludedDispatchIds.includes(candidate.id),
          );
          if (next === undefined) return null;
          leased.add(next.id);
          claims.push(next.id);
          return next;
        },
      }),
      allocationStore: fakeAllocationStore({
        findByAnchorRunId: async (anchorRunId) =>
          allocation({
            id: `allocation-for-${anchorRunId}`,
            anchorRunId,
          }),
      }),
      router: {
        sendSignalDeliverToAllocation: async () => {
          throw new Error("must not route mail as signal");
        },
        sendWorkflowRunDispatchToAllocation: async (
          _target,
          _agentAddress,
          _runId,
          _stepGrants,
          _rawMessage,
          _senderAddress,
          messageId,
        ) => {
          delivered.push(messageId);
        },
      },
      resolveAnchorAddress: async (anchorRunId) => {
        if (anchorRunId === stuck.anchorRunId && ++addressReads === 1) {
          stuckClaimed.resolve(true);
          return releaseStuck.promise;
        }
        return "run_abc@acme.localhost";
      },
      leaseDurationMs: 30,
      createLeaseId: () => "lease-1",
      now: () => NOW,
    });

    service.wake();
    await stuckClaimed.promise;
    try {
      // Database lease expiry must not admit a second copy of the same I/O.
      leased.delete(stuck.id);
      expect(await service.reconcileNext()).toBe(false);
      expect(exclusions.at(-1)).toContain(stuck.id);
      await new Promise((resolve) => setTimeout(resolve, 60));
      expect(await service.reconcileNext()).toBe(false);
      expect(exclusions.at(-1)).toContain(stuck.id);

      pending.push(healthy);
      expect(await service.reconcileNext()).toBe(true);
      expect(delivered).toEqual(["message-healthy"]);
      expect(claims).toEqual([stuck.id, healthy.id]);
      expect(addressReads).toBe(1);
    } finally {
      releaseStuck.resolve("run_abc@acme.localhost");
    }
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(delivered).toEqual(["message-healthy"]);
    expect(await service.reconcileNext()).toBe(true);
    expect(delivered).toEqual(["message-healthy", "message-stuck"]);
    expect(addressReads).toBe(2);
  });

  test("a late claim cannot duplicate a delivery already executing", async () => {
    const lateClaim = Promise.withResolvers<ClaimedDispatch>();
    const entered = Promise.withResolvers<boolean>();
    const release = Promise.withResolvers<string>();
    let claims = 0;
    let addressReads = 0;
    let sends = 0;
    const exclusions: (readonly string[])[] = [];
    const service = createWorkflowDispatchService({
      dispatchStore: fakeDispatchStore({
        claimNextPending: (args) => {
          exclusions.push(args.excludedDispatchIds ?? []);
          return ++claims === 1
            ? Promise.resolve(dispatch())
            : lateClaim.promise;
        },
      }),
      allocationStore: fakeAllocationStore({
        findByAnchorRunId: async () => allocation(),
      }),
      router: {
        sendSignalDeliverToAllocation: async () => undefined,
        sendWorkflowRunDispatchToAllocation: async () => {
          sends += 1;
        },
      },
      resolveAnchorAddress: () => {
        addressReads += 1;
        entered.resolve(true);
        return release.promise;
      },
    });
    const first = service.reconcileNext();
    const second = service.reconcileNext();
    await entered.promise;
    try {
      lateClaim.resolve(dispatch());
      await second;
      expect(exclusions).toEqual([[], []]);
      expect(addressReads).toBe(1);
      expect(sends).toBe(0);
    } finally {
      lateClaim.resolve(dispatch());
      release.resolve("run_abc@acme.localhost");
      await Promise.all([first, second]);
    }
    expect(sends).toBe(1);
  });
});

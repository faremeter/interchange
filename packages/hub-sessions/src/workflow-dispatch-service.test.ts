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
    placement: { sharing: "exclusive", reuse: "never" },
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
  test("routes immutable trigger bytes to the accepted allocation generation", async () => {
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
      resolveAnchorAddress: async () => "workflow@tenant.example",
      createLeaseId: () => "lease-1",
      now: () => NOW,
    });

    expect(await service.reconcileUntilIdle()).toBe(1);
    expect(deliveries).toEqual([
      [
        { allocationId: "allocation-1", generation: 2 },
        "workflow@tenant.example",
        "workflow@tenant.example",
        [],
        "cmF3IG1haWw=",
        "message-1",
      ],
    ]);
  });

  test("retries without routing while an allocation is not ready", async () => {
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
          allocation({ connectDeadline: new Date(NOW.getTime() + 60_000) }),
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
      ],
    ]);
  });
});

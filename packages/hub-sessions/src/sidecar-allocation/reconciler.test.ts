import { describe, expect, test } from "bun:test";
import { sha256 } from "@intx/crypto";
import type { SidecarAllocation, SidecarAllocationStore } from "@intx/db";
import { hexEncode } from "@intx/types";

import { SessionLaunchError } from "../session-service";
import type { SidecarProvisioner } from "./contracts";
import {
  createSidecarAllocationReconciler,
  type SidecarAllocationReconcilerDeps,
} from "./reconciler";

const NOW = new Date("2026-08-03T12:00:00.000Z");

function allocation(
  overrides: Partial<SidecarAllocation> = {},
): SidecarAllocation {
  return {
    id: "alloc-1",
    anchorRunId: "run-anchor",
    tenantId: "tenant-1",
    provisionerId: "test",
    provisionerApiVersion: 1,
    provisionerBindingFingerprint: "test:v1",
    status: "pending",
    generation: 0,
    nextAttemptAt: NOW,
    ensureAttempts: 0,
    destroyAttempts: 0,
    createdAt: NOW,
    updatedAt: NOW,
    ...overrides,
  };
}

type AllocationStore = SidecarAllocationReconcilerDeps["allocationStore"];

function fakeStore(overrides: Partial<AllocationStore> = {}): AllocationStore {
  const notUsed = (name: string) => async () => {
    throw new Error(`unexpected store call: ${name}`);
  };
  return {
    beginReplacement: notUsed("beginReplacement"),
    beginUnrecoverableRelease: notUsed("beginUnrecoverableRelease"),
    bindInitialSidecar: notUsed("bindInitialSidecar"),
    bindReplacementSidecar: notUsed("bindReplacementSidecar"),
    claimNextReconcilable: async () => null,
    extendReconciliationLease: async () => true,
    failWithoutInfrastructure: notUsed("failWithoutInfrastructure"),
    listActive: async () => [],
    markAllocated: notUsed("markAllocated"),
    markConnectionLost: notUsed("markConnectionLost"),
    markConnectionReady: notUsed("markConnectionReady"),
    markReleased: notUsed("markReleased"),
    parkReconciliation: async () => true,
    scheduleReconnectIfUnscheduled: notUsed("scheduleReconnectIfUnscheduled"),
    scheduleRetry: notUsed("scheduleRetry"),
    wakeReconciliation: async () => true,
    ...overrides,
  };
}

function testProvisioner(
  overrides: Partial<SidecarProvisioner> = {},
): SidecarProvisioner {
  return {
    id: "test",
    apiVersion: 1,
    bindingFingerprint: "test:v1",
    capabilities: [],
    async ensure() {
      return { kind: "accepted" };
    },
    async destroy() {
      return { kind: "destroyed" };
    },
    ...overrides,
  };
}

function deps(args: {
  store: AllocationStore;
  provisioner?: SidecarProvisioner;
  fences?: [string, number][];
  ready?: boolean;
  waitError?: Error;
  onReady?: (row: SidecarAllocation) => Promise<void>;
}): SidecarAllocationReconcilerDeps {
  const provisioner = args.provisioner ?? testProvisioner();
  return {
    allocationStore: args.store,
    plugins: {
      getDefaultProvisioner: () => provisioner,
      getProvisioner: (id) => (id === provisioner.id ? provisioner : null),
      selectProvisioner: () => ({ ok: true, provisioner }),
    },
    router: {
      fenceAllocation(id, generation) {
        args.fences?.push([id, generation]);
      },
      isAllocatedSidecarReady: async () => args.ready ?? true,
      waitForAllocatedSidecar: async () => {
        if (args.waitError !== undefined) throw args.waitError;
      },
    },
    hubWebSocketUrl: "wss://hub.example/ws/sidecar",
    ...(args.onReady !== undefined ? { onReady: args.onReady } : {}),
    now: () => NOW,
    createSidecarId: () => "sc-new",
    createToken: () => "token-new",
    createLeaseId: () => "lease-1",
  };
}

describe("createSidecarAllocationReconciler", () => {
  test("persists identity and fence before ensuring infrastructure", async () => {
    const pending = allocation();
    const provisioning = allocation({
      status: "provisioning",
      generation: 1,
      sidecarId: "sc-new",
      connectDeadline: new Date(NOW.getTime() + 120_000),
      reconciliationLeaseId: "lease-1",
    });
    const allocated = allocation({
      ...provisioning,
      status: "allocated",
      ensureAcceptedGeneration: 1,
    });
    const calls: string[] = [];
    let claimed = false;
    let storedHash: Uint8Array | undefined;
    let ensureToken: string | undefined;
    const store = fakeStore({
      claimNextReconcilable: async () => {
        if (claimed) return null;
        claimed = true;
        return pending;
      },
      bindInitialSidecar: async (args) => {
        calls.push("bind");
        storedHash = args.tokenHashSha256;
        return provisioning;
      },
      markAllocated: async () => allocated,
      markConnectionReady: async () => allocated,
    });
    const provisioner = testProvisioner({
      async ensure(request) {
        calls.push("ensure");
        ensureToken = request.token;
        return { kind: "accepted", externalRef: "vm-1" };
      },
    });
    const fences: [string, number][] = [];
    const reconciler = createSidecarAllocationReconciler(
      deps({ store, provisioner, fences }),
    );

    expect(await reconciler.reconcileNext()).toBe(true);

    expect(calls).toEqual(["bind", "ensure"]);
    expect(fences).toEqual([
      ["alloc-1", 0],
      ["alloc-1", 1],
    ]);
    expect(ensureToken).toBe("token-new");
    expect(hexEncode(storedHash ?? new Uint8Array())).toBe(
      hexEncode(await sha256("token-new")),
    );
  });

  test("parks an accepted provision without waiting for its websocket", async () => {
    const pending = allocation();
    const provisioning = allocation({
      status: "provisioning",
      generation: 1,
      sidecarId: "sc-new",
      connectDeadline: new Date(NOW.getTime() + 120_000),
      reconciliationLeaseId: "lease-1",
    });
    const allocated = allocation({
      ...provisioning,
      status: "allocated",
      ensureAcceptedGeneration: 1,
    });
    let claimed = false;
    let parked = false;
    let waited = false;
    const store = fakeStore({
      claimNextReconcilable: async () => {
        if (claimed) return null;
        claimed = true;
        return pending;
      },
      bindInitialSidecar: async () => provisioning,
      markAllocated: async () => allocated,
      parkReconciliation: async () => {
        parked = true;
        return true;
      },
    });
    const base = deps({ store, ready: false });
    const reconciler = createSidecarAllocationReconciler({
      ...base,
      router: {
        ...base.router,
        waitForAllocatedSidecar: async () => {
          waited = true;
        },
      },
    });

    await reconciler.reconcileNext();

    expect(parked).toBe(true);
    expect(waited).toBe(false);
  });

  test("initializes a worker that connects before ensure is accepted", async () => {
    const pending = allocation();
    const provisioning = allocation({
      status: "provisioning",
      generation: 1,
      sidecarId: "sc-new",
      connectDeadline: new Date(NOW.getTime() + 120_000),
      reconciliationLeaseId: "lease-1",
    });
    const allocated = allocation({
      ...provisioning,
      status: "allocated",
      ensureAcceptedGeneration: 1,
    });
    const calls: string[] = [];
    let connected = false;
    const store = fakeStore({
      claimNextReconcilable: async () => pending,
      bindInitialSidecar: async () => provisioning,
      markAllocated: async () => allocated,
      markConnectionReady: async () => {
        calls.push("ready");
        return allocated;
      },
    });
    const provisioner = testProvisioner({
      async ensure() {
        connected = true;
        return { kind: "accepted" };
      },
    });
    const base = deps({
      store,
      provisioner,
      onReady: async () => {
        calls.push("initialize");
      },
    });
    const reconciler = createSidecarAllocationReconciler({
      ...base,
      router: {
        ...base.router,
        isAllocatedSidecarReady: async () => connected,
      },
    });

    await reconciler.reconcileNext();

    expect(calls).toEqual(["initialize", "ready"]);
  });

  test("fails terminally when ensure is rejected as non-retryable", async () => {
    const pending = allocation();
    const provisioning = allocation({
      status: "provisioning",
      generation: 1,
      sidecarId: "sc-new",
      connectDeadline: new Date(NOW.getTime() + 120_000),
      reconciliationLeaseId: "lease-1",
    });
    let claimed = false;
    let failed:
      | Parameters<AllocationStore["failWithoutInfrastructure"]>[0]
      | undefined;
    const store = fakeStore({
      claimNextReconcilable: async () => {
        if (claimed) return null;
        claimed = true;
        return pending;
      },
      bindInitialSidecar: async () => provisioning,
      failWithoutInfrastructure: async (args) => {
        failed = args;
        return allocation({ status: "failed", generation: 1 });
      },
    });
    const provisioner = testProvisioner({
      async ensure() {
        return {
          kind: "rejected",
          code: "quota_disabled",
          message: "Provisioning is disabled for this account",
          retryable: false,
        };
      },
    });
    const reconciler = createSidecarAllocationReconciler(
      deps({ store, provisioner }),
    );

    expect(await reconciler.reconcileUntilIdle()).toBe(1);
    expect(failed).toEqual({
      allocationId: "alloc-1",
      expectedStatus: "provisioning",
      expectedGeneration: 1,
      code: "quota_disabled",
      message: "Provisioning is disabled for this account",
      expectedLeaseId: "lease-1",
      now: NOW,
    });
  });

  test("backs off before replacing a retryable ensure rejection", async () => {
    const pending = allocation();
    const provisioning = allocation({
      status: "provisioning",
      generation: 1,
      sidecarId: "sc-new",
      connectDeadline: new Date(NOW.getTime() + 120_000),
      reconciliationLeaseId: "lease-1",
    });
    let replacement:
      | Parameters<AllocationStore["beginReplacement"]>[0]
      | undefined;
    const store = fakeStore({
      claimNextReconcilable: async () => pending,
      bindInitialSidecar: async () => provisioning,
      beginReplacement: async (args) => {
        replacement = args;
        return allocation({ status: "replacing", generation: 2 });
      },
    });
    const provisioner = testProvisioner({
      async ensure() {
        return {
          kind: "rejected",
          code: "capacity_unavailable",
          message: "Capacity is temporarily unavailable",
          retryable: true,
        };
      },
    });
    const reconciler = createSidecarAllocationReconciler({
      ...deps({ store, provisioner }),
      retryDelayMs: () => 5_000,
    });

    await reconciler.reconcileNext();

    expect(replacement).toEqual({
      allocationId: "alloc-1",
      expectedStatus: "provisioning",
      expectedGeneration: 1,
      expectedLeaseId: "lease-1",
      failureCode: "capacity_unavailable",
      failureMessage: "Capacity is temporarily unavailable",
      nextAttemptAt: new Date(NOW.getTime() + 5_000),
      now: NOW,
    });
  });

  test("backs off before replacing an unknown ensure outcome", async () => {
    const pending = allocation();
    const provisioning = allocation({
      status: "provisioning",
      generation: 1,
      sidecarId: "sc-new",
      connectDeadline: new Date(NOW.getTime() + 120_000),
      reconciliationLeaseId: "lease-1",
    });
    let replacement:
      | Parameters<AllocationStore["beginReplacement"]>[0]
      | undefined;
    const store = fakeStore({
      claimNextReconcilable: async () => pending,
      bindInitialSidecar: async () => provisioning,
      beginReplacement: async (args) => {
        replacement = args;
        return allocation({ status: "replacing", generation: 2 });
      },
    });
    const provisioner = testProvisioner({
      async ensure() {
        throw new Error("provider request timed out");
      },
    });
    const reconciler = createSidecarAllocationReconciler({
      ...deps({ store, provisioner }),
      retryDelayMs: () => 5_000,
    });

    await reconciler.reconcileNext();

    expect(replacement).toMatchObject({
      failureCode: "ensure_failed",
      failureMessage: "provider request timed out",
      nextAttemptAt: new Date(NOW.getTime() + 5_000),
    });
  });

  test("backs off after an unexpected failure before a connection deadline exists", async () => {
    const pending = allocation({ reconciliationLeaseId: "lease-1" });
    let parked:
      | {
          allocationId: string;
          leaseId: string;
          policy: Parameters<SidecarAllocationStore["parkReconciliation"]>[2];
        }
      | undefined;
    const retryAttempts: number[] = [];
    const store = fakeStore({
      claimNextReconcilable: async () => pending,
      bindInitialSidecar: async () => {
        throw new Error("database temporarily unavailable");
      },
      parkReconciliation: async (allocationId, leaseId, policy) => {
        parked = { allocationId, leaseId, policy };
        return true;
      },
    });
    const reconciler = createSidecarAllocationReconciler({
      ...deps({ store }),
      retryDelayMs: (attempt) => {
        retryAttempts.push(attempt);
        return 30_000;
      },
    });

    await reconciler.reconcileNext();

    expect(retryAttempts).toEqual([5]);
    expect(parked).toEqual({
      allocationId: "alloc-1",
      leaseId: "lease-1",
      policy: {
        kind: "retry-after-error",
        notBefore: new Date(NOW.getTime() + 30_000),
      },
    });
  });

  test("advances an uncertain provisioning generation after restart", async () => {
    const provisioning = allocation({
      status: "provisioning",
      generation: 1,
      sidecarId: "sc-old",
    });
    const replacing = allocation({
      ...provisioning,
      status: "replacing",
      generation: 2,
    });
    const fences: [string, number][] = [];
    let replacement:
      | { expectedLeaseId: string; nextAttemptAt: Date }
      | undefined;
    const store = fakeStore({
      claimNextReconcilable: async () => provisioning,
      beginReplacement: async (args) => {
        replacement = {
          expectedLeaseId: args.expectedLeaseId,
          nextAttemptAt: args.nextAttemptAt,
        };
        return replacing;
      },
    });
    const reconciler = createSidecarAllocationReconciler(
      deps({ store, fences }),
    );

    await reconciler.reconcileNext();

    expect(fences).toEqual([
      ["alloc-1", 1],
      ["alloc-1", 2],
    ]);
    expect(replacement).toEqual({
      expectedLeaseId: "lease-1",
      nextAttemptAt: new Date(NOW.getTime() + 1_000),
    });
  });

  test("destroys the old worker before ensuring its replacement", async () => {
    const replacing = allocation({
      status: "replacing",
      generation: 2,
      sidecarId: "sc-old",
      externalRef: "vm-old",
    });
    const provisioning = allocation({
      ...replacing,
      status: "provisioning",
      sidecarId: "sc-new",
      connectDeadline: new Date(NOW.getTime() + 120_000),
    });
    const allocated = allocation({
      ...provisioning,
      status: "allocated",
      ensureAcceptedGeneration: 2,
    });
    const calls: string[] = [];
    const provisioner = testProvisioner({
      async destroy() {
        calls.push("destroy");
        return { kind: "destroyed" };
      },
      async ensure() {
        calls.push("ensure");
        return { kind: "accepted" };
      },
    });
    const store = fakeStore({
      claimNextReconcilable: async () => replacing,
      bindReplacementSidecar: async () => {
        calls.push("bind");
        return provisioning;
      },
      markAllocated: async () => allocated,
      markConnectionReady: async () => allocated,
    });
    const reconciler = createSidecarAllocationReconciler(
      deps({ store, provisioner }),
    );

    await reconciler.reconcileNext();

    expect(calls).toEqual(["destroy", "bind", "ensure"]);
  });

  test("replaces an allocated worker that misses its connection deadline", async () => {
    const allocated = allocation({
      status: "allocated",
      generation: 1,
      sidecarId: "sc-old",
      ensureAcceptedGeneration: 1,
      connectDeadline: NOW,
    });
    const replacing = allocation({
      status: "replacing",
      generation: 2,
      sidecarId: "sc-old",
    });
    let replacement:
      | {
          failureCode: string;
          expectedLeaseId: string;
          nextAttemptAt: Date;
        }
      | undefined;
    const store = fakeStore({
      claimNextReconcilable: async () => allocated,
      beginReplacement: async (args) => {
        replacement = {
          failureCode: args.failureCode,
          expectedLeaseId: args.expectedLeaseId,
          nextAttemptAt: args.nextAttemptAt,
        };
        return replacing;
      },
    });
    const reconciler = createSidecarAllocationReconciler({
      ...deps({
        store,
        ready: false,
        waitError: new Error("connect timeout"),
      }),
      enableAutomaticReplacementRecovery: true,
    });

    await reconciler.reconcileNext();

    expect(replacement).toEqual({
      failureCode: "sidecar_connect_failed",
      expectedLeaseId: "lease-1",
      nextAttemptAt: new Date(NOW.getTime() + 1_000),
    });
  });

  test("retries initialization without replacing a connected generation", async () => {
    const allocated = allocation({
      status: "allocated",
      generation: 1,
      sidecarId: "sc-current",
      ensureAcceptedGeneration: 1,
      connectDeadline: NOW,
      ensureAttempts: 1,
      reconciliationLeaseId: "lease-1",
    });
    let claimed = false;
    let scheduled: Parameters<AllocationStore["scheduleRetry"]>[0] | undefined;
    const store = fakeStore({
      claimNextReconcilable: async () => {
        if (claimed) return null;
        claimed = true;
        return allocated;
      },
      scheduleRetry: async (args) => {
        scheduled = args;
        return allocated;
      },
    });
    const reconciler = createSidecarAllocationReconciler(
      deps({
        store,
        ready: true,
        onReady: async () => {
          throw new Error("catalog temporarily unavailable");
        },
      }),
    );

    await reconciler.reconcileNext();

    expect(scheduled).toEqual({
      allocationId: "alloc-1",
      expectedStatus: "allocated",
      expectedGeneration: 1,
      nextAttemptAt: new Date(NOW.getTime() + 30_000),
      expectedLeaseId: "lease-1",
      failure: {
        code: "sidecar_initialization_failed",
        message: "catalog temporarily unavailable",
      },
      now: NOW,
    });
  });

  test("releases a generation whose initialization leaked a supervisor", async () => {
    const allocated = allocation({
      status: "allocated",
      generation: 1,
      sidecarId: "sc-current",
      ensureAcceptedGeneration: 1,
      connectDeadline: NOW,
      reconciliationLeaseId: "lease-1",
    });
    const releasing = allocation({
      status: "releasing",
      generation: 2,
      sidecarId: "sc-current",
    });
    let claimed = false;
    let released:
      | Parameters<AllocationStore["beginUnrecoverableRelease"]>[0]
      | undefined;
    const fences: [string, number][] = [];
    const store = fakeStore({
      claimNextReconcilable: async () => {
        if (claimed) return null;
        claimed = true;
        return allocated;
      },
      beginUnrecoverableRelease: async (args) => {
        released = args;
        return releasing;
      },
    });
    const reconciler = createSidecarAllocationReconciler(
      deps({
        store,
        fences,
        ready: true,
        onReady: async () => {
          throw new SessionLaunchError(
            "provision",
            new Error("deploy pack failed"),
            true,
          );
        },
      }),
    );

    await reconciler.reconcileNext();

    expect(released).toMatchObject({
      allocationId: "alloc-1",
      expectedGeneration: 1,
      expectedLeaseId: "lease-1",
      failureCode: "sidecar_initialization_uncertain",
      failureMessage: "Automatic recovery is disabled: deploy pack failed",
    });
    expect(fences).toEqual([
      ["alloc-1", 1],
      ["alloc-1", 2],
    ]);
  });

  test("does not fence a newer generation after losing the release race", async () => {
    const allocated = allocation({
      status: "allocated",
      generation: 1,
      sidecarId: "sc-current",
      ensureAcceptedGeneration: 1,
      connectDeadline: NOW,
      reconciliationLeaseId: "lease-1",
    });
    let claimed = false;
    const fences: [string, number][] = [];
    const store = fakeStore({
      claimNextReconcilable: async () => {
        if (claimed) return null;
        claimed = true;
        return allocated;
      },
      beginUnrecoverableRelease: async () => null,
    });
    const reconciler = createSidecarAllocationReconciler(
      deps({
        store,
        fences,
        ready: true,
        onReady: async () => {
          throw new SessionLaunchError(
            "provision",
            new Error("deploy pack failed"),
            true,
          );
        },
      }),
    );

    await reconciler.reconcileNext();

    expect(fences).toEqual([["alloc-1", 1]]);
  });

  test("marks a connected generation ready after initialization succeeds", async () => {
    const allocated = allocation({
      status: "allocated",
      generation: 1,
      sidecarId: "sc-current",
      ensureAcceptedGeneration: 1,
      connectDeadline: NOW,
      reconciliationLeaseId: "lease-1",
    });
    let claimed = false;
    const calls: string[] = [];
    const store = fakeStore({
      claimNextReconcilable: async () => {
        if (claimed) return null;
        claimed = true;
        return allocated;
      },
      markConnectionReady: async () => {
        calls.push("ready");
        return allocated;
      },
    });
    const reconciler = createSidecarAllocationReconciler(
      deps({
        store,
        ready: true,
        onReady: async () => {
          calls.push("initialize");
        },
      }),
    );

    await reconciler.reconcileNext();

    expect(calls).toEqual(["initialize", "ready"]);
  });

  test("does not release capacity when the final ready write fails", async () => {
    const allocated = allocation({
      status: "allocated",
      generation: 1,
      sidecarId: "sc-current",
      ensureAcceptedGeneration: 1,
      connectDeadline: NOW,
      reconciliationLeaseId: "lease-1",
    });
    let claimed = false;
    let releaseStarted = false;
    let parked = false;
    const store = fakeStore({
      claimNextReconcilable: async () => {
        if (claimed) return null;
        claimed = true;
        return allocated;
      },
      beginUnrecoverableRelease: async () => {
        releaseStarted = true;
        return allocation({ status: "releasing", generation: 2 });
      },
      markConnectionReady: async () => {
        throw new Error("database temporarily unavailable");
      },
      parkReconciliation: async () => {
        parked = true;
        return true;
      },
    });
    const reconciler = createSidecarAllocationReconciler(
      deps({ store, ready: true, onReady: () => Promise.resolve() }),
    );

    await reconciler.reconcileNext();

    expect(releaseStarted).toBe(false);
    expect(parked).toBe(true);
  });

  test("fails a lost allocated worker instead of recovering by default", async () => {
    const allocated = allocation({
      status: "allocated",
      generation: 1,
      sidecarId: "sc-old",
      ensureAcceptedGeneration: 1,
      connectDeadline: NOW,
    });
    const releasing = allocation({
      status: "releasing",
      generation: 2,
      sidecarId: "sc-old",
    });
    let failure:
      | { failureCode: string; failureMessage: string; expectedLeaseId: string }
      | undefined;
    const fences: [string, number][] = [];
    const store = fakeStore({
      claimNextReconcilable: async () => allocated,
      beginUnrecoverableRelease: async (args) => {
        failure = {
          failureCode: args.failureCode,
          failureMessage: args.failureMessage,
          expectedLeaseId: args.expectedLeaseId,
        };
        return releasing;
      },
    });
    const reconciler = createSidecarAllocationReconciler(
      deps({
        store,
        fences,
        ready: false,
        waitError: new Error("connect timeout"),
      }),
    );

    await reconciler.reconcileNext();

    expect(failure).toEqual({
      failureCode: "sidecar_connect_failed",
      failureMessage: "Automatic recovery is disabled: connect timeout",
      expectedLeaseId: "lease-1",
    });
    expect(fences).toEqual([
      ["alloc-1", 1],
      ["alloc-1", 2],
    ]);
  });

  test("rebuilds fences without erasing durable retry schedules", async () => {
    const { nextAttemptAt: _nextAttemptAt, ...unscheduled } = allocation({
      id: "alloc-unscheduled",
      generation: 2,
      status: "replacing",
    });
    const active = [
      allocation({
        id: "alloc-a",
        generation: 1,
        status: "replacing",
        nextAttemptAt: new Date(NOW.getTime() + 30_000),
      }),
      unscheduled,
      allocation({ id: "alloc-b", status: "allocated", generation: 4 }),
    ];
    const wakes: [string, number][] = [];
    const reconnects: [string, number, Date][] = [];
    const fences: [string, number][] = [];
    const store = fakeStore({
      listActive: async () => active,
      wakeReconciliation: async (id, generation) => {
        wakes.push([id, generation]);
        return true;
      },
      markConnectionLost: async (args) => {
        reconnects.push([
          args.allocationId,
          args.generation,
          args.connectDeadline,
        ]);
        return active[2] ?? null;
      },
    });
    const reconciler = createSidecarAllocationReconciler(
      deps({ store, fences }),
    );

    await reconciler.initialize();

    expect(fences).toEqual([
      ["alloc-a", 1],
      ["alloc-unscheduled", 2],
      ["alloc-b", 4],
    ]);
    expect(wakes).toEqual([["alloc-unscheduled", 2]]);
    expect(reconnects).toEqual([
      ["alloc-b", 4, new Date(NOW.getTime() + 120_000)],
    ]);
  });

  test("durably schedules reconnect grace and wakes the exact generation on reconnect", async () => {
    const calls: string[] = [];
    const store = fakeStore({
      markConnectionLost: async (args) => {
        calls.push(
          `lost:${args.allocationId}:${String(args.generation)}:${args.connectDeadline.toISOString()}`,
        );
        return allocation({ status: "allocated", generation: args.generation });
      },
      wakeReconciliation: async (id, generation) => {
        calls.push(`connected:${id}:${String(generation)}`);
        return true;
      },
    });
    const reconciler = createSidecarAllocationReconciler(deps({ store }));

    await reconciler.handleDisconnect({
      allocationId: "alloc-1",
      generation: 3,
    });
    await reconciler.handleConnected({
      allocationId: "alloc-1",
      generation: 3,
    });

    expect(calls).toEqual([
      "lost:alloc-1:3:2026-08-03T12:02:00.000Z",
      "connected:alloc-1:3",
    ]);
  });

  test("repairs an unscheduled allocation after its disconnect write fails", async () => {
    const { nextAttemptAt: _nextAttemptAt, ...unscheduled } = allocation({
      status: "allocated",
      generation: 3,
      ensureAcceptedGeneration: 3,
    });
    const repairs: [string, number, Date][] = [];
    const store = fakeStore({
      listActive: async () => [unscheduled],
      markConnectionLost: async () => {
        throw new Error("database unavailable");
      },
      scheduleReconnectIfUnscheduled: async (args) => {
        repairs.push([
          args.allocationId,
          args.generation,
          args.connectDeadline,
        ]);
        return unscheduled;
      },
    });
    const reconciler = createSidecarAllocationReconciler(
      deps({ store, ready: false }),
    );

    await expect(
      reconciler.handleDisconnect({
        allocationId: "alloc-1",
        generation: 3,
      }),
    ).rejects.toThrow("database unavailable");
    await reconciler.repairUnscheduledConnections();

    expect(repairs).toEqual([
      ["alloc-1", 3, new Date(NOW.getTime() + 120_000)],
    ]);
  });

  test("does not repair an allocation with a ready connection", async () => {
    const { nextAttemptAt: _nextAttemptAt, ...unscheduled } = allocation({
      status: "allocated",
      generation: 1,
      ensureAcceptedGeneration: 1,
    });
    const repairs: string[] = [];
    const store = fakeStore({
      listActive: async () => [unscheduled],
      scheduleReconnectIfUnscheduled: async (args) => {
        repairs.push(args.allocationId);
        return unscheduled;
      },
    });
    const reconciler = createSidecarAllocationReconciler(
      deps({ store, ready: true }),
    );

    await reconciler.repairUnscheduledConnections();

    expect(repairs).toEqual([]);
  });
});

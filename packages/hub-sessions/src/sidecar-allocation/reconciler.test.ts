import { describe, expect, test } from "bun:test";
import { sha256 } from "@intx/crypto";
import type { SidecarAllocation, SidecarAllocationStore } from "@intx/db";
import { hexEncode } from "@intx/types";

import { SessionLaunchError } from "../session-service";
import { tick } from "../ws/sidecar-handler.test-helpers";
import type { EnsureSidecarResult, SidecarProvisioner } from "./contracts";
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
    isReconciliationLeaseCurrent: async () => true,
    markAllocated: notUsed("markAllocated"),
    markConnectionLost: notUsed("markConnectionLost"),
    markConnectionReady: notUsed("markConnectionReady"),
    markDestroyFailed: notUsed("markDestroyFailed"),
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
  retired?: [string, number][];
  ready?: boolean;
  waitError?: Error;
  onReady?: (row: SidecarAllocation) => Promise<void>;
}): SidecarAllocationReconcilerDeps {
  const provisioner = args.provisioner ?? testProvisioner();
  return {
    allocationStore: args.store,
    plugins: {
      getProvisioner: (id) => (id === provisioner.id ? provisioner : null),
      selectProvisioner: async () => ({ ok: true, provisioner }),
    },
    router: {
      fenceAllocation(id, generation) {
        args.fences?.push([id, generation]);
      },
      retireAllocation({ allocationId, generation }) {
        args.retired?.push([allocationId, generation]);
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
    const retired: [string, number][] = [];
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
      deps({ store, provisioner, retired }),
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
    expect(retired).toEqual([["alloc-1", 1]]);
  });

  test("retires a fence after releasing destroyed capacity", async () => {
    const releasing = allocation({
      status: "releasing",
      generation: 2,
      sidecarId: "sc-old",
      reconciliationLeaseId: "lease-1",
    });
    const released = allocation({
      ...releasing,
      status: "released",
    });
    let claimed = false;
    const retired: [string, number][] = [];
    const store = fakeStore({
      claimNextReconcilable: async () => {
        if (claimed) return null;
        claimed = true;
        return releasing;
      },
      markReleased: async () => released,
    });
    const reconciler = createSidecarAllocationReconciler(
      deps({ store, retired }),
    );

    await reconciler.reconcileNext();

    expect(retired).toEqual([["alloc-1", 2]]);
  });

  for (const status of ["releasing", "replacing"] as const) {
    test(`stops ${status} after a permanent destroy rejection`, async () => {
      const current = allocation({
        status,
        generation: 2,
        sidecarId: "sc-old",
        externalRef: "vm-old",
        reconciliationLeaseId: "lease-1",
      });
      const calls: string[] = [];
      const retired: [string, number][] = [];
      let failed:
        | Parameters<AllocationStore["markDestroyFailed"]>[0]
        | undefined;
      const store = fakeStore({
        claimNextReconcilable: async () => current,
        markDestroyFailed: async (args) => {
          calls.push("fail");
          failed = args;
          return allocation({ ...current, status: "destroy_failed" });
        },
        scheduleRetry: async () => {
          calls.push("retry");
          return null;
        },
        bindReplacementSidecar: async () => {
          calls.push("replace");
          return null;
        },
        markReleased: async () => {
          calls.push("release");
          return null;
        },
        parkReconciliation: async () => {
          calls.push("park");
          return true;
        },
      });
      const provisioner = testProvisioner({
        async destroy() {
          calls.push("destroy");
          return {
            kind: "rejected",
            code: "credentials_revoked",
            message: "Credentials no longer permit deleting this worker",
            retryable: false,
          };
        },
      });
      const reconciler = createSidecarAllocationReconciler(
        deps({ store, provisioner, retired }),
      );

      await reconciler.reconcileNext();

      expect(calls).toEqual(["destroy", "fail"]);
      expect(failed).toEqual({
        allocationId: "alloc-1",
        expectedGeneration: 2,
        expectedLeaseId: "lease-1",
        code: "credentials_revoked",
        message: "Credentials no longer permit deleting this worker",
        now: NOW,
      });
      expect(retired).toEqual([["alloc-1", 2]]);
    });

    for (const failure of ["retryable rejection", "thrown error"] as const) {
      test(`retries ${status} after a destroy ${failure}`, async () => {
        const current = allocation({
          status,
          generation: 2,
          sidecarId: "sc-old",
          destroyAttempts: 3,
          reconciliationLeaseId: "lease-1",
        });
        let scheduled:
          | Parameters<AllocationStore["scheduleRetry"]>[0]
          | undefined;
        const retired: [string, number][] = [];
        const store = fakeStore({
          claimNextReconcilable: async () => current,
          scheduleRetry: async (args) => {
            scheduled = args;
            return current;
          },
        });
        const provisioner = testProvisioner({
          async destroy() {
            if (failure === "thrown error") throw new Error("provider timeout");
            return {
              kind: "rejected",
              code: "provider_unavailable",
              message: "Provider temporarily unavailable",
              retryable: true,
            };
          },
        });
        const reconciler = createSidecarAllocationReconciler(
          deps({ store, provisioner, retired }),
        );

        await reconciler.reconcileNext();

        expect(scheduled).toEqual({
          allocationId: "alloc-1",
          expectedStatus: status,
          expectedGeneration: 2,
          expectedLeaseId: "lease-1",
          nextAttemptAt: new Date(NOW.getTime() + 8_000),
          attempt: "destroy",
          now: NOW,
        });
        expect(retired).toEqual([]);
      });
    }
  }

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

describe("provisioner operation deadlines", () => {
  test("fences a timed out ensure and ignores its late acceptance", async () => {
    const provisioning = allocation({
      status: "provisioning",
      generation: 1,
      sidecarId: "sc-new",
    });
    const completion = Promise.withResolvers<EnsureSidecarResult>();
    let signal: AbortSignal | undefined;
    let accepted = false;
    let replaced = false;
    const fences: [string, number][] = [];
    const store = fakeStore({
      claimNextReconcilable: async () => allocation(),
      bindInitialSidecar: async () => provisioning,
      markAllocated: async () => {
        accepted = true;
        return null;
      },
      beginReplacement: async (args) => {
        replaced = true;
        expect(args.expectedGeneration).toBe(1);
        expect(args.expectedLeaseId).toBe("lease-1");
        return allocation({ status: "replacing", generation: 2 });
      },
    });
    const reconciler = createSidecarAllocationReconciler({
      ...deps({
        store,
        fences,
        provisioner: testProvisioner({
          ensure(request) {
            signal = request.signal;
            return completion.promise;
          },
        }),
      }),
      operationTimeoutMs: 10,
    });

    await reconciler.reconcileNext();
    expect(signal?.aborted).toBe(true);
    expect(replaced).toBe(true);
    expect(fences).toContainEqual(["alloc-1", 2]);
    completion.resolve({ kind: "accepted", externalRef: "late-worker" });
    await completion.promise;
    expect(accepted).toBe(false);
  });

  test("keeps replacement pending when destruction times out", async () => {
    let retried = false;
    let ensured = false;
    let signal: AbortSignal | undefined;
    const reconciler = createSidecarAllocationReconciler({
      ...deps({
        store: fakeStore({
          claimNextReconcilable: async () =>
            allocation({
              status: "replacing",
              generation: 2,
              sidecarId: "sc-old",
            }),
          scheduleRetry: async (args) => {
            expect(args.expectedStatus).toBe("replacing");
            expect(args.attempt).toBe("destroy");
            retried = true;
            return null;
          },
        }),
        provisioner: testProvisioner({
          destroy(request) {
            signal = request.signal;
            return new Promise(() => {
              // This provider never acknowledges cancellation or destruction.
            });
          },
          async ensure() {
            ensured = true;
            return { kind: "accepted" };
          },
        }),
      }),
      operationTimeoutMs: 10,
    });

    await reconciler.reconcileNext();
    expect(signal?.aborted).toBe(true);
    expect(retried).toBe(true);
    expect(ensured).toBe(false);
  });
});

describe("reconciliation ownership", () => {
  test("renews the lease while binding a replacement between provider operations", async () => {
    const binding = Promise.withResolvers<SidecarAllocation>();
    const renewedWhileBinding = Promise.withResolvers<boolean>();
    const provisioning = allocation({
      status: "provisioning",
      generation: 2,
      sidecarId: "sc-new",
    });
    const calls: string[] = [];
    let isBinding = false;
    const reconciler = createSidecarAllocationReconciler({
      ...deps({
        store: fakeStore({
          claimNextReconcilable: async () =>
            allocation({
              status: "replacing",
              generation: 2,
              sidecarId: "sc-old",
            }),
          bindReplacementSidecar: () => {
            isBinding = true;
            return binding.promise;
          },
          extendReconciliationLease: async () => {
            if (isBinding) renewedWhileBinding.resolve(true);
            return true;
          },
          markAllocated: async () => ({
            ...provisioning,
            status: "allocated",
            ensureAcceptedGeneration: 2,
          }),
          markConnectionReady: async () => {
            calls.push("ready");
            return null;
          },
        }),
        onReady: async () => {
          calls.push("initialize");
        },
      }),
      leaseDurationMs: 600,
    });
    const work = reconciler.reconcileNext();
    const deadline = setTimeout(
      () => renewedWhileBinding.resolve(false),
      1_000,
    );
    try {
      expect(await renewedWhileBinding.promise).toBe(true);
    } finally {
      clearTimeout(deadline);
      isBinding = false;
      binding.resolve(provisioning);
      await work;
    }
    expect(calls).toEqual(["initialize", "ready"]);
  });

  test.each(["accepted", "rejected", "error"] as const)(
    "ignores a completed claim's late renewal %s after the allocation is reclaimed",
    async (outcome) => {
      const renewalEntered = Promise.withResolvers<boolean>();
      const renewal = Promise.withResolvers<boolean>();
      const firstInitialization = Promise.withResolvers<boolean>();
      const nextEntered = Promise.withResolvers<boolean>();
      const nextInitialization = Promise.withResolvers<boolean>();
      const readyLeases: (string | undefined)[] = [];
      const signals: AbortSignal[] = [];
      let leases = 0;
      const reconciler = createSidecarAllocationReconciler({
        ...deps({
          store: fakeStore({
            claimNextReconcilable: async () =>
              allocation({ status: "allocated", generation: 1 }),
            extendReconciliationLease: (_id, leaseId) => {
              if (leaseId !== "lease-1") return Promise.resolve(true);
              renewalEntered.resolve(true);
              return renewal.promise;
            },
            markConnectionReady: async (args) => {
              readyLeases.push(args.expectedLeaseId);
              return null;
            },
          }),
        }),
        createLeaseId: () => `lease-${String(++leases)}`,
        leaseDurationMs: 300,
        onReady: async (_row, { signal, leaseId }) => {
          signals.push(signal);
          if (leaseId === "lease-1") {
            await firstInitialization.promise;
          } else {
            nextEntered.resolve(true);
            await nextInitialization.promise;
          }
          signal.throwIfAborted();
        },
      });
      const first = reconciler.reconcileNext();
      await renewalEntered.promise;
      firstInitialization.resolve(true);
      await first;
      expect(readyLeases).toEqual(["lease-1"]);

      const next = reconciler.reconcileNext();
      await nextEntered.promise;
      try {
        if (outcome === "error")
          renewal.reject(new Error("old connection lost"));
        else renewal.resolve(outcome === "accepted");
        await tick();
        expect(signals.map((signal) => signal.aborted)).toEqual([false, false]);
      } finally {
        nextInitialization.resolve(true);
        await next;
      }
      expect(readyLeases).toEqual(["lease-1", "lease-2"]);
    },
  );

  test.each(["ensure", "destroy", "initialize"] as const)(
    "does not %s when the claimed lease is no longer current",
    async (operation) => {
      const claimed =
        operation === "ensure"
          ? allocation({ reconciliationLeaseId: "lease-1" })
          : allocation({
              status: operation === "destroy" ? "releasing" : "allocated",
              generation: 1,
              sidecarId: "sc-current",
              ensureAcceptedGeneration: 1,
              reconciliationLeaseId: "lease-1",
            });
      const leaseChecks: Parameters<
        AllocationStore["isReconciliationLeaseCurrent"]
      >[] = [];
      const calls: string[] = [];
      const recordWrite = async () => {
        calls.push("write");
        return null;
      };
      const reconciler = createSidecarAllocationReconciler({
        ...deps({
          store: fakeStore({
            claimNextReconcilable: async () => claimed,
            bindInitialSidecar: async () =>
              allocation({
                status: "provisioning",
                generation: 1,
                sidecarId: "sc-new",
                reconciliationLeaseId: "lease-1",
              }),
            isReconciliationLeaseCurrent: async (...args) => {
              leaseChecks.push(args);
              return false;
            },
            markAllocated: recordWrite,
            markReleased: recordWrite,
            markConnectionReady: recordWrite,
            scheduleRetry: recordWrite,
            beginReplacement: recordWrite,
            beginUnrecoverableRelease: recordWrite,
            parkReconciliation: async () => {
              calls.push("park");
              return true;
            },
          }),
          provisioner: testProvisioner({
            async ensure() {
              calls.push("ensure");
              return { kind: "accepted" };
            },
            async destroy() {
              calls.push("destroy");
              return { kind: "destroyed" };
            },
          }),
        }),
        onReady: async () => {
          calls.push("initialize");
        },
      });

      await reconciler.reconcileNext();

      expect(calls).toEqual([]);
      expect(leaseChecks).toEqual([["alloc-1", 1, "lease-1"]]);
    },
  );

  test("times out a hung claim without initializing its late result", async () => {
    const claim = Promise.withResolvers<SidecarAllocation | null>();
    let initialized = false;
    const reconciler = createSidecarAllocationReconciler({
      ...deps({
        store: fakeStore({ claimNextReconcilable: () => claim.promise }),
      }),
      operationTimeoutMs: 10,
      onReady: async () => {
        initialized = true;
      },
    });
    try {
      await expect(reconciler.reconcileNext()).rejects.toThrow(
        "Sidecar allocation claim timed out",
      );
    } finally {
      claim.resolve(allocation({ status: "allocated", generation: 1 }));
      await claim.promise;
    }
    expect(initialized).toBe(false);
  });

  test("retries a hung lease validation without starting initialization", async () => {
    const validation = Promise.withResolvers<boolean>();
    const calls: string[] = [];
    const reconciler = createSidecarAllocationReconciler({
      ...deps({
        store: fakeStore({
          claimNextReconcilable: async () =>
            allocation({ status: "allocated", generation: 1 }),
          isReconciliationLeaseCurrent: () => validation.promise,
          scheduleRetry: async () => {
            calls.push("retry");
            return null;
          },
        }),
      }),
      operationTimeoutMs: 10,
      onReady: async () => {
        calls.push("initialize");
      },
    });
    try {
      await reconciler.reconcileNext();
    } finally {
      validation.resolve(true);
      await validation.promise;
    }
    expect(calls).toEqual(["retry"]);
  });

  test("cancels initialization when its lease cannot be renewed", async () => {
    let signal: AbortSignal | undefined;
    let ready = false;
    let retried = false;
    const reconciler = createSidecarAllocationReconciler({
      ...deps({
        store: fakeStore({
          claimNextReconcilable: async () =>
            allocation({ status: "allocated", generation: 1 }),
          extendReconciliationLease: async () => false,
          markConnectionReady: async () => {
            ready = true;
            return null;
          },
          scheduleRetry: async () => {
            retried = true;
            return null;
          },
        }),
      }),
      leaseDurationMs: 30,
      onReady: (_allocation, context) => {
        signal = context.signal;
        return new Promise(() => {
          // The interrupted initialization never completes on its own.
        });
      },
    });
    await reconciler.reconcileNext();
    expect(signal?.aborted).toBe(true);
    expect(ready).toBe(false);
    expect(retried).toBe(false);
  });

  test("parks the allocation when lease renewal throws transiently", async () => {
    let parked = 0;
    let parkedLeaseId: string | undefined;
    const reconciler = createSidecarAllocationReconciler({
      ...deps({
        store: fakeStore({
          claimNextReconcilable: async () =>
            allocation({ status: "allocated", generation: 1 }),
          extendReconciliationLease: async () => {
            throw new Error("db blip");
          },
          parkReconciliation: async (_allocationId, leaseId) => {
            parked += 1;
            parkedLeaseId = leaseId;
            return true;
          },
        }),
      }),
      // The throwing renewal is what must park the allocation, so the
      // renewal has to be reached first. The heartbeat schedules it one
      // third of the lease after it starts, and a renewal that arrives after
      // the lease has elapsed aborts as a plain lease loss instead, which
      // parks nothing. The slack is the remaining two thirds -- twenty
      // milliseconds at a lease of thirty, which a loaded machine spends on
      // scheduling delay alone. Three hundred keeps the ratio and makes it
      // two hundred.
      leaseDurationMs: 300,
      onReady: (_allocation, _context) => {
        return new Promise(() => {
          // The interrupted initialization never completes on its own.
        });
      },
    });
    expect(await reconciler.reconcileNext()).toBe(true);
    expect(parked).toBe(1);
    expect(parkedLeaseId).toBe("lease-1");
  });

  test("initialization can finish after the provider operation deadline", async () => {
    const calls: string[] = [];
    const secondRenewal = Promise.withResolvers<boolean>();
    let renewals = 0;
    const reconciler = createSidecarAllocationReconciler({
      ...deps({
        store: fakeStore({
          claimNextReconcilable: async () =>
            allocation({ status: "allocated", generation: 1 }),
          extendReconciliationLease: async () => {
            renewals += 1;
            if (renewals === 2) secondRenewal.resolve(true);
            return true;
          },
          markConnectionReady: async () => {
            calls.push("ready");
            return null;
          },
          beginUnrecoverableRelease: async () => {
            calls.push("release");
            return allocation({ status: "releasing", generation: 2 });
          },
        }),
      }),
      operationTimeoutMs: 10,
      // The lease is scaffolding here, and it sets a real deadline against a
      // real timer: the heartbeat schedules its first renewal one third of
      // the lease after it starts, and that renewal must land before the
      // lease itself elapses or the reconciliation aborts as lease-lost. The
      // slack is the remaining two thirds -- twenty milliseconds at a lease
      // of thirty, which a loaded machine spends on scheduling delay alone,
      // and then nothing reaches `calls`. Three hundred keeps the same ratio
      // and makes that slack two hundred milliseconds, while the deadline
      // this test is about stays at ten.
      leaseDurationMs: 300,
      onReady: async (_allocation, { signal }) => {
        // The subject is that initialization may outlast the operation
        // deadline while lease renewals keep the claim alive. The renewals
        // run through the store double, so it reports the second one and
        // this awaits that report rather than polling for it.
        await secondRenewal.promise;
        signal.throwIfAborted();
        calls.push("initialized");
      },
    });
    await reconciler.reconcileNext();
    // Two renewals were reported before initialization returned, and both
    // fell after the ten-millisecond operation deadline -- so "initialized"
    // here is initialization surviving that deadline, not merely running.
    expect(calls).toEqual(["initialized", "ready"]);
  });

  test("cancels initialization when renewal hangs and ignores its late success", async () => {
    const renewalEntered = Promise.withResolvers<boolean>();
    const renewal = Promise.withResolvers<boolean>();
    const preparation = Promise.withResolvers<boolean>();
    const resumed = Promise.withResolvers<boolean>();
    const calls: string[] = [];
    let signal: AbortSignal | undefined;
    const reconciler = createSidecarAllocationReconciler({
      ...deps({
        store: fakeStore({
          claimNextReconcilable: async () =>
            allocation({ status: "allocated", generation: 1 }),
          extendReconciliationLease: () => {
            renewalEntered.resolve(true);
            return renewal.promise;
          },
          markConnectionReady: async () => {
            calls.push("ready");
            return null;
          },
          scheduleRetry: async () => {
            calls.push("retry");
            return null;
          },
          beginUnrecoverableRelease: async () => {
            calls.push("release");
            return null;
          },
        }),
      }),
      leaseDurationMs: 30,
      onReady: async (_allocation, context) => {
        signal = context.signal;
        await preparation.promise;
        try {
          context.signal.throwIfAborted();
          calls.push("deploy");
        } finally {
          // Reported from a `finally`, so the report is ordered after the
          // hook has decided whether to record the deploy.
          resumed.resolve(true);
        }
      },
    });
    const work = reconciler.reconcileNext();
    try {
      await renewalEntered.promise;
      // The renewal never reports, so the lease expires and the controller
      // aborts. `runSidecarOperation` races the initialization against that
      // abort, so `reconcileNext` returns on it without waiting for the hung
      // renewal or the pending preparation -- and awaiting it is what
      // establishes that the abort was carried all the way out.
      // `signal.aborted` says only that the abort was raised.
      await work;
      expect(signal?.aborted).toBe(true);
      expect(calls).toEqual([]);
    } finally {
      renewal.resolve(true);
      preparation.resolve(true);
    }
    // Both late arrivals are queued by the two lines above, the renewal's
    // continuation first. The hook's report is queued behind its own
    // resumption, so this await sits after every effect either could have
    // had, and the empty `calls` is the late success being ignored.
    await resumed.promise;
    expect(signal?.aborted).toBe(true);
    expect(calls).toEqual([]);
  });

  test("excludes an allocation until its local initialization has finished", async () => {
    const entered = Promise.withResolvers<boolean>();
    const finish = Promise.withResolvers<boolean>();
    const exclusions: (readonly string[])[] = [];
    let claimed = false;
    const reconciler = createSidecarAllocationReconciler({
      ...deps({
        store: fakeStore({
          claimNextReconcilable: async (args) => {
            exclusions.push(args.excludedAllocationIds ?? []);
            if (claimed) return null;
            claimed = true;
            return allocation({ status: "allocated", generation: 1 });
          },
          markConnectionReady: async () => null,
        }),
      }),
      onReady: async () => {
        entered.resolve(true);
        await finish.promise;
      },
    });
    const first = reconciler.reconcileNext();
    try {
      await entered.promise;
      expect(await reconciler.reconcileNext()).toBe(false);
    } finally {
      finish.resolve(true);
      await first;
    }
    expect(await reconciler.reconcileNext()).toBe(false);
    expect(exclusions).toEqual([[], ["alloc-1"], []]);
  });

  for (const connectedFirst of [false, true]) {
    test(`disconnect cancels initialization before its database write finishes${connectedFirst ? " after a connect" : ""}`, async () => {
      const entered = Promise.withResolvers<boolean>();
      const write = Promise.withResolvers<SidecarAllocation | null>();
      const late = Promise.withResolvers<boolean>();
      const calls: string[] = [];
      let signal: AbortSignal | undefined;
      const reconciler = createSidecarAllocationReconciler({
        ...deps({
          store: fakeStore({
            claimNextReconcilable: async () =>
              allocation({ status: "allocated", generation: 1 }),
            markConnectionLost: () => write.promise,
            markConnectionReady: async () => {
              calls.push("ready");
              return null;
            },
            scheduleRetry: async () => {
              calls.push("retry");
              return null;
            },
            beginUnrecoverableRelease: async () => {
              calls.push("release");
              return null;
            },
          }),
        }),
        onReady: async (_allocation, context) => {
          signal = context.signal;
          entered.resolve(true);
          await late.promise;
          context.signal.throwIfAborted();
        },
      });
      const work = reconciler.reconcileNext();
      await entered.promise;
      const target = { allocationId: "alloc-1", generation: 1 };
      if (connectedFirst) await reconciler.handleConnected(target);
      const disconnect = reconciler.handleDisconnect(target);
      try {
        expect(signal?.aborted).toBe(true);
        await work;
        expect(calls).toEqual([]);
      } finally {
        write.resolve(null);
        late.resolve(true);
        await disconnect;
      }
    });
  }

  test("a disconnect during ensure clears an earlier slow connect notification", async () => {
    const entered = Promise.withResolvers<boolean>();
    const ensure = Promise.withResolvers<EnsureSidecarResult>();
    const wake = Promise.withResolvers<boolean>();
    const provisioning = allocation({
      status: "provisioning",
      generation: 1,
      sidecarId: "sc-new",
      connectDeadline: new Date(NOW.getTime() + 120_000),
    });
    const calls: string[] = [];
    const reconciler = createSidecarAllocationReconciler(
      deps({
        ready: false,
        store: fakeStore({
          claimNextReconcilable: async () => allocation(),
          bindInitialSidecar: async () => provisioning,
          markAllocated: async () =>
            allocation({
              ...provisioning,
              status: "allocated",
              ensureAcceptedGeneration: 1,
            }),
          wakeReconciliation: () => wake.promise,
          markConnectionLost: async () => null,
          parkReconciliation: async () => {
            calls.push("park");
            return true;
          },
          scheduleRetry: async () => {
            calls.push("retry");
            return null;
          },
        }),
        provisioner: testProvisioner({
          ensure() {
            entered.resolve(true);
            return ensure.promise;
          },
        }),
      }),
    );
    const work = reconciler.reconcileNext();
    await entered.promise;
    const target = { allocationId: "alloc-1", generation: 1 };
    const connected = reconciler.handleConnected(target);
    const disconnected = reconciler.handleDisconnect(target);
    try {
      wake.resolve(true);
      await Promise.all([connected, disconnected]);
    } finally {
      ensure.resolve({ kind: "accepted" });
      await work;
    }
    expect(calls).toEqual(["park"]);
  });

  test.each(["success", "failure"] as const)(
    "consumes the initial connect before initialization reports %s",
    async (outcome) => {
      const entered = Promise.withResolvers<boolean>();
      const ensure = Promise.withResolvers<EnsureSidecarResult>();
      const wake = Promise.withResolvers<boolean>();
      const readinessChecked = Promise.withResolvers<boolean>();
      const provisioning = allocation({
        status: "provisioning",
        generation: 1,
        sidecarId: "sc-new",
      });
      const allocated = allocation({
        ...provisioning,
        status: "allocated",
        ensureAcceptedGeneration: 1,
      });
      const calls: string[] = [];
      let scheduled:
        | Parameters<AllocationStore["scheduleRetry"]>[0]
        | undefined;
      let initializations = 0;
      const dependencies = deps({
        store: fakeStore({
          claimNextReconcilable: async () => allocation(),
          bindInitialSidecar: async () => provisioning,
          markAllocated: async () => allocated,
          wakeReconciliation: () => wake.promise,
          scheduleRetry: async (args) => {
            scheduled = args;
            calls.push("retry");
            return allocated;
          },
          markConnectionReady: async () => {
            calls.push("ready");
            return allocated;
          },
        }),
        provisioner: testProvisioner({
          ensure() {
            entered.resolve(true);
            return ensure.promise;
          },
        }),
      });
      const reconciler = createSidecarAllocationReconciler({
        ...dependencies,
        router: {
          ...dependencies.router,
          isAllocatedSidecarReady: async () => {
            readinessChecked.resolve(true);
            return true;
          },
        },
        onReady: async () => {
          initializations += 1;
          if (outcome === "failure") {
            throw new Error("catalog temporarily unavailable");
          }
        },
      });
      const work = reconciler.reconcileNext();
      await entered.promise;
      const connected = reconciler.handleConnected({
        allocationId: "alloc-1",
        generation: 1,
      });
      try {
        ensure.resolve({ kind: "accepted" });
        await readinessChecked.promise;
      } finally {
        wake.resolve(true);
        await Promise.all([connected, work]);
      }

      expect(initializations).toBe(1);
      if (outcome === "success") {
        expect(calls).toEqual(["ready"]);
        expect(scheduled).toBeUndefined();
      } else {
        expect(calls).toEqual(["retry"]);
        expect(scheduled).toMatchObject({
          expectedLeaseId: "lease-1",
          nextAttemptAt: new Date(NOW.getTime() + 30_000),
          failure: {
            code: "sidecar_initialization_failed",
            message: "catalog temporarily unavailable",
          },
        });
      }
    },
  );

  test.each(["success", "failure"] as const)(
    "preserves a later connect and the initialization %s",
    async (outcome) => {
      const entered = Promise.withResolvers<boolean>();
      const initialized = Promise.withResolvers<boolean>();
      const provisioning = allocation({
        status: "provisioning",
        generation: 1,
        sidecarId: "sc-new",
      });
      const allocated = allocation({
        ...provisioning,
        status: "allocated",
        ensureAcceptedGeneration: 1,
      });
      const calls: string[] = [];
      let claims = 0;
      let initializations = 0;
      const reconciler = createSidecarAllocationReconciler({
        ...deps({
          store: fakeStore({
            claimNextReconcilable: async () =>
              claims++ === 0 ? allocation() : allocated,
            bindInitialSidecar: async () => provisioning,
            markAllocated: async () => allocated,
            scheduleRetry: async (args) => {
              expect(args.expectedGeneration).toBe(1);
              if (outcome === "failure") {
                expect(args.nextAttemptAt).toEqual(
                  new Date(NOW.getTime() + 30_000),
                );
                expect(args.failure).toEqual({
                  code: "sidecar_initialization_failed",
                  message: "catalog temporarily unavailable",
                });
              } else {
                expect(args.nextAttemptAt).toEqual(NOW);
              }
              calls.push("retry");
              return allocated;
            },
            markConnectionReady: async () => {
              calls.push("ready");
              return allocated;
            },
          }),
        }),
        onReady: async () => {
          if (initializations++ === 0) {
            entered.resolve(true);
            await initialized.promise;
            if (outcome === "failure") {
              throw new Error("catalog temporarily unavailable");
            }
          }
        },
      });
      const first = reconciler.reconcileNext();
      await entered.promise;
      try {
        await reconciler.handleConnected({
          allocationId: "alloc-1",
          generation: 1,
        });
      } finally {
        initialized.resolve(true);
        await first;
      }
      expect(calls).toEqual(["retry"]);
      await reconciler.reconcileNext();
      expect(calls).toEqual(["retry", "ready"]);
    },
  );

  test("applies reconnect after an earlier disconnect write completes", async () => {
    const disconnectWrite = Promise.withResolvers<SidecarAllocation | null>();
    const calls: string[] = [];
    const reconciler = createSidecarAllocationReconciler(
      deps({
        store: fakeStore({
          markConnectionLost: () => {
            calls.push("disconnect");
            return disconnectWrite.promise;
          },
          wakeReconciliation: async () => {
            calls.push("connect");
            return true;
          },
        }),
      }),
    );
    const target = { allocationId: "alloc-1", generation: 1 };
    const disconnected = reconciler.handleDisconnect(target);
    const connected = reconciler.handleConnected(target);
    await Promise.resolve();
    await Promise.resolve();
    expect(calls).toEqual(["disconnect"]);
    disconnectWrite.resolve(null);
    await Promise.all([disconnected, connected]);
    expect(calls).toEqual(["disconnect", "connect"]);
  });
});

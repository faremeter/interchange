import { describe, expect, test } from "bun:test";
import { sha256 } from "@intx/crypto";
import type { SidecarAllocation, SidecarAllocationStore } from "@intx/db";
import { hexEncode } from "@intx/types";

import { SessionLaunchError } from "../session-service";
import {
  isDeployFrameFailure,
  SidecarIdentityValidationError,
} from "../ws/sidecar-handler";
import {
  connectAllocated,
  createAllocatedRouter,
  TEST_CONFIG,
  TEST_IDENTITY,
  TEST_TARGET,
  tick,
} from "../ws/sidecar-handler.test-helpers";
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
  readyError?: Error;
  waitError?: Error;
  adapterManifest?: SidecarAllocationReconcilerDeps["adapterManifest"];
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
      isAllocatedSidecarReady: async () => {
        if (args.readyError !== undefined) throw args.readyError;
        return args.ready ?? true;
      },
      waitForAllocatedSidecar: async () => {
        if (args.waitError !== undefined) throw args.waitError;
      },
    },
    hubWebSocketUrl: "wss://hub.example/ws/sidecar",
    ...(args.adapterManifest !== undefined
      ? { adapterManifest: args.adapterManifest }
      : {}),
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

  test("forwards the declared adapter manifest on the ensure request", async () => {
    const pending = allocation();
    const provisioning = allocation({
      status: "provisioning",
      generation: 1,
      sidecarId: "sc-new",
      connectDeadline: new Date(NOW.getTime() + 120_000),
      reconciliationLeaseId: "lease-1",
    });
    const adapterManifest = [
      {
        provider: "openai-responses",
        specifier: "@corbits/openai-responses",
        export: "createAdapter",
      },
    ];
    let claimed = false;
    const store = fakeStore({
      claimNextReconcilable: async () => {
        if (claimed) return null;
        claimed = true;
        return pending;
      },
      bindInitialSidecar: async () => provisioning,
      parkReconciliation: async () => true,
    });
    let ensuredManifest: unknown;
    const provisioner = testProvisioner({
      async ensure(request) {
        ensuredManifest = request.adapterManifest;
        return { kind: "accepted" };
      },
    });
    const reconciler = createSidecarAllocationReconciler(
      deps({ store, provisioner, adapterManifest }),
    );

    expect(await reconciler.reconcileNext()).toBe(true);
    expect(ensuredManifest).toEqual(adapterManifest);
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

  test("retries when identity validation fails inside the connection wait", async () => {
    const allocated = allocation({
      status: "allocated",
      generation: 1,
      sidecarId: "sc-current",
      ensureAcceptedGeneration: 1,
      connectDeadline: NOW,
      reconciliationLeaseId: "lease-1",
    });
    let released = false;
    let parked:
      | {
          allocationId: string;
          leaseId: string;
          policy: Parameters<SidecarAllocationStore["parkReconciliation"]>[2];
        }
      | undefined;
    const store = fakeStore({
      claimNextReconcilable: async () => allocated,
      beginReplacement: async () => {
        released = true;
        return null;
      },
      beginUnrecoverableRelease: async () => {
        released = true;
        return null;
      },
      parkReconciliation: async (allocationId, leaseId, policy) => {
        parked = { allocationId, leaseId, policy };
        return true;
      },
    });
    const reconciler = createSidecarAllocationReconciler({
      ...deps({
        store,
        ready: false,
        waitError: new SidecarIdentityValidationError(
          "alloc-1",
          1,
          new Error("statement timeout"),
        ),
      }),
      retryDelayMs: () => 30_000,
    });

    await reconciler.reconcileNext();

    expect(released).toBe(false);
    expect(parked).toEqual({
      allocationId: "alloc-1",
      leaseId: "lease-1",
      policy: {
        kind: "retry-after-error",
        notBefore: new Date(NOW.getTime() + 30_000),
      },
    });
  });

  test("retries when the initial readiness check cannot validate identity", async () => {
    const allocated = allocation({
      status: "allocated",
      generation: 1,
      sidecarId: "sc-current",
      ensureAcceptedGeneration: 1,
      connectDeadline: NOW,
      reconciliationLeaseId: "lease-1",
    });
    let released = false;
    let parked:
      | {
          allocationId: string;
          leaseId: string;
          policy: Parameters<SidecarAllocationStore["parkReconciliation"]>[2];
        }
      | undefined;
    const store = fakeStore({
      claimNextReconcilable: async () => allocated,
      beginReplacement: async () => {
        released = true;
        return null;
      },
      beginUnrecoverableRelease: async () => {
        released = true;
        return null;
      },
      parkReconciliation: async (allocationId, leaseId, policy) => {
        parked = { allocationId, leaseId, policy };
        return true;
      },
    });
    const reconciler = createSidecarAllocationReconciler({
      ...deps({
        store,
        readyError: new SidecarIdentityValidationError(
          "alloc-1",
          1,
          new Error("statement timeout"),
        ),
      }),
      retryDelayMs: () => 30_000,
    });

    await reconciler.reconcileNext();

    expect(released).toBe(false);
    expect(parked).toEqual({
      allocationId: "alloc-1",
      leaseId: "lease-1",
      policy: {
        kind: "retry-after-error",
        notBefore: new Date(NOW.getTime() + 30_000),
      },
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
      scheduleRetry: async () => null,
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

  test("leaves an allocation with inconclusive readiness for the next repair sweep", async () => {
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
      deps({
        store,
        readyError: new SidecarIdentityValidationError(
          "alloc-1",
          1,
          new Error("statement timeout"),
        ),
      }),
    );

    await reconciler.repairUnscheduledConnections();

    expect(repairs).toEqual([]);
  });
});

describe("durable initialization outcomes", () => {
  test("a late claim cannot overtake cancelled cleanup or park behind it", async () => {
    const current = allocation({
      status: "allocated",
      generation: 1,
      ensureAcceptedGeneration: 1,
      initializationLeaseId: "old-initializer",
    });
    const transitioned = allocation({ status: "releasing", generation: 2 });
    const entered = Promise.withResolvers<boolean>();
    const response = Promise.withResolvers<SidecarAllocation | null>();
    const lateClaim = Promise.withResolvers<SidecarAllocation | null>();
    const exclusions: (readonly string[])[] = [];
    const fences: [string, number][] = [];
    let claims = 0;
    let lateFinished = false;
    const writes: string[] = [];
    const reconciler = createSidecarAllocationReconciler({
      ...deps({
        fences,
        store: fakeStore({
          claimNextReconcilable: (args) => {
            exclusions.push(args.excludedAllocationIds ?? []);
            return ++claims === 1
              ? Promise.resolve(current)
              : lateClaim.promise;
          },
          beginUnrecoverableRelease: () => {
            entered.resolve(true);
            return response.promise;
          },
          extendReconciliationLease: async () => false,
          parkReconciliation: async () => {
            writes.push("park");
            await response.promise;
            return true;
          },
          markReleased: async () => {
            writes.push("release");
            return null;
          },
        }),
      }),
      leaseDurationMs: 60,
    });
    const first = reconciler.reconcileNext();
    const late = reconciler.reconcileNext().then(() => {
      lateFinished = true;
    });
    await entered.promise;
    try {
      await first;
      // Its exclusion snapshot predates cleanup; its row reflects the commit
      // whose response the first claim is still waiting for.
      lateClaim.resolve(transitioned);
      await tick();
      expect(lateFinished).toBe(true);
      expect(exclusions).toEqual([[], []]);
      expect(writes).toEqual([]);
      expect(fences).toEqual([[current.id, 1]]);
    } finally {
      lateClaim.resolve(null);
      response.resolve(transitioned);
      await Promise.all([first, late]);
    }
    await tick();
    expect(fences).toEqual([
      [current.id, 1],
      [current.id, 2],
    ]);
  });

  for (const recovery of [false, true]) {
    for (const scenario of [
      { interruption: "lease loss", outcome: "committed" },
      { interruption: "renewal failure", outcome: "committed" },
      { interruption: "lease expiry", outcome: "committed" },
      { interruption: "operation deadline", outcome: "committed" },
      { interruption: "operation deadline", outcome: "unchanged" },
      { interruption: "operation deadline", outcome: "rejected" },
    ] as const) {
      test(`releases a cleanup slot on ${scenario.interruption} before its ${scenario.outcome} response (recovery=${String(recovery)})`, async () => {
        const current = allocation({
          status: "allocated",
          generation: 1,
          ensureAcceptedGeneration: 1,
          initializationLeaseId: "old-initializer",
        });
        const other = allocation({
          id: "alloc-2",
          status: "allocated",
          generation: 1,
          ensureAcceptedGeneration: 1,
        });
        const transitioned = allocation({
          status: recovery ? "replacing" : "releasing",
          generation: 2,
        });
        const entered = Promise.withResolvers<boolean>();
        const response = Promise.withResolvers<SidecarAllocation | null>();
        const renewal = Promise.withResolvers<boolean>();
        const exclusions: (readonly string[])[] = [];
        const fences: [string, number][] = [];
        const ready: string[] = [];
        let cleanupSettled = false;
        let cleanupCalls = 0;
        let parked = false;
        let finished = false;
        const cleanup: AllocationStore["beginUnrecoverableRelease"] = async (
          args,
        ) => {
          expect(args).toMatchObject({
            expectedGeneration: 1,
            expectedLeaseId: "lease-1",
            onlyIfInitializationIncomplete: true,
            expectedInitializationLeaseId: "old-initializer",
          });
          cleanupCalls += 1;
          entered.resolve(true);
          try {
            return await response.promise;
          } finally {
            cleanupSettled = true;
          }
        };
        const reconciler = createSidecarAllocationReconciler({
          ...deps({
            fences,
            store: fakeStore({
              claimNextReconcilable: async (args) => {
                const excluded = args.excludedAllocationIds ?? [];
                exclusions.push(excluded);
                if (!cleanupSettled && !excluded.includes(current.id))
                  return current;
                return ready.includes(other.id) ? null : other;
              },
              beginUnrecoverableRelease: cleanup,
              beginReplacement: cleanup,
              extendReconciliationLease: async (id) => {
                if (id !== current.id) return true;
                switch (scenario.interruption) {
                  case "lease loss":
                    return false;
                  case "renewal failure":
                    throw new Error("Renewal connection failed");
                  case "lease expiry":
                    return renewal.promise;
                  case "operation deadline":
                    return true;
                }
              },
              parkReconciliation: async () => {
                parked = true;
                // A fallback write would wait behind the same stalled cleanup.
                await response.promise;
                return true;
              },
              markConnectionReady: async (args) => {
                ready.push(args.allocationId);
                return other;
              },
            }),
          }),
          enableAutomaticReplacementRecovery: recovery,
          leaseDurationMs:
            scenario.interruption === "operation deadline" ? 1_000 : 60,
          operationTimeoutMs:
            scenario.interruption === "operation deadline" ? 30 : 1_000,
        });
        const work = reconciler.reconcileNext().then(() => {
          finished = true;
        });
        await entered.promise;
        try {
          await new Promise((resolve) => setTimeout(resolve, 150));
          expect(finished).toBe(true);
          expect(parked).toBe(false);
          expect(await reconciler.reconcileNext()).toBe(true);
          expect(exclusions[1]).toContain(current.id);
          expect(ready).toEqual([other.id]);
          expect(await reconciler.reconcileNext()).toBe(false);
          expect(cleanupCalls).toBe(1);
          expect(fences).not.toContainEqual([current.id, 2]);
        } finally {
          if (scenario.outcome === "rejected")
            response.reject(new Error("Cleanup response lost"));
          else
            response.resolve(
              scenario.outcome === "committed" ? transitioned : null,
            );
          renewal.resolve(false);
          await work;
        }
        await tick();
        const expectedFences: [string, number][] = [
          [current.id, 1],
          [other.id, 1],
        ];
        if (scenario.outcome === "committed")
          expectedFences.push([current.id, 2]);
        expect(fences).toEqual(expectedFences);
        expect(await reconciler.reconcileNext()).toBe(false);
        expect(exclusions.at(-1)).not.toContain(current.id);
        expect(ready).toEqual([other.id]);
        expect(parked).toBe(false);
      });
    }
  }

  test("recovers deferred mail before waiting for a disconnected worker", async () => {
    const calls: string[] = [];
    const base = deps({
      store: fakeStore({
        claimNextReconcilable: async () =>
          allocation({
            status: "allocated",
            generation: 1,
            ensureAcceptedGeneration: 1,
          }),
        beginUnrecoverableRelease: async () => {
          calls.push("release");
          return allocation({ status: "releasing", generation: 2 });
        },
      }),
      ready: false,
    });
    const reconciler = createSidecarAllocationReconciler({
      ...base,
      router: {
        ...base.router,
        waitForAllocatedSidecar: async () => {
          calls.push("wait");
          throw new Error("worker never reconnected");
        },
      },
      onInitializationRecovery: async (_allocation, { signal, leaseId }) => {
        expect(signal.aborted).toBe(false);
        expect(leaseId).toBe("lease-1");
        calls.push("recover");
      },
    });
    await reconciler.reconcileNext();
    expect(calls).toEqual(["recover", "wait", "release"]);
  });

  for (const recovery of [false, true]) {
    test(`a cancelled deploy with a late acknowledgement is fenced before retry (recovery=${String(recovery)})`, async () => {
      let current = allocation({
        id: TEST_TARGET.allocationId,
        status: "allocated",
        generation: TEST_TARGET.generation,
        anchorRunId: TEST_IDENTITY.anchorRunId,
        tenantId: TEST_IDENTITY.tenantId,
        ensureAcceptedGeneration: TEST_TARGET.generation,
        sidecarId: TEST_IDENTITY.sidecarId,
      });
      const router = createAllocatedRouter({ requestTimeoutMs: 80 });
      const ws = await connectAllocated(router);
      const timedOut = Promise.withResolvers<boolean>();
      let claims = 0;
      let initialized = 0;
      let cleanedUp = false;
      const transition = async () => {
        cleanedUp = true;
        current = {
          ...current,
          status: recovery ? "replacing" : "releasing",
          generation: current.generation + 1,
        };
        return current;
      };
      const store = fakeStore({
        claimNextReconcilable: async () => (claims++ < 2 ? current : null),
        extendReconciliationLease: async () => {
          throw new Error("transient renewal failure");
        },
        beginReplacement: transition,
        beginUnrecoverableRelease: transition,
      });
      const reconciler = createSidecarAllocationReconciler({
        ...deps({ store }),
        router,
        leaseDurationMs: 60,
        retryDelayMs: () => 1,
        enableAutomaticReplacementRecovery: recovery,
        onReady: async (_row, { signal, leaseId }) => {
          initialized += 1;
          try {
            await router.sendAgentDeployToAllocation(
              TEST_TARGET,
              TEST_IDENTITY.workflowRunAddress,
              TEST_CONFIG,
              undefined,
              signal,
              async () => {
                current = { ...current, initializationLeaseId: leaseId };
              },
            );
          } catch (cause) {
            if (!isDeployFrameFailure(cause)) throw cause;
            expect(cause.frameSent).toBe(true);
            timedOut.resolve(true);
            throw new SessionLaunchError("start", cause, true);
          }
        },
      });
      await reconciler.reconcileNext();
      expect(current.initializationLeaseId).toBe("lease-1");
      await timedOut.promise;
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "agent.deploy.ack",
          agentAddress: TEST_IDENTITY.workflowRunAddress,
          publicKey: "c".repeat(64),
        }),
      );
      await tick();
      expect(await router.isAllocatedWorkflowActive(TEST_TARGET)).toBe(false);
      await reconciler.reconcileNext();
      expect(initialized).toBe(1);
      expect(cleanedUp).toBe(true);
      expect(ws.closed).toBe(true);
      expect(
        ws.sent.some((frame) => frame.includes('"type":"repo.pack.push"')),
      ).toBe(false);
    });
  }

  for (const recovery of [false, true]) {
    test(`rechecks readiness after the observed initialization is rolled back (recovery=${String(recovery)})`, async () => {
      const current = allocation({
        status: "allocated",
        generation: 1,
        ensureAcceptedGeneration: 1,
      });
      const calls: string[] = [];
      let claims = 0;
      const cleanup: AllocationStore["beginUnrecoverableRelease"] = async (
        args,
      ) => {
        expect(args).toMatchObject({
          onlyIfInitializationIncomplete: true,
          expectedInitializationLeaseId: "old-owner",
        });
        calls.push("rollback observed");
        return null;
      };
      const reconciler = createSidecarAllocationReconciler({
        ...deps({
          store: fakeStore({
            claimNextReconcilable: async () =>
              claims++ === 0
                ? { ...current, initializationLeaseId: "old-owner" }
                : current,
            beginUnrecoverableRelease: cleanup,
            beginReplacement: cleanup,
            scheduleRetry: async () => {
              calls.push("retry");
              return current;
            },
            markConnectionReady: async () => {
              calls.push("ready");
              return current;
            },
          }),
          onReady: async () => {
            calls.push("initialize");
          },
        }),
        enableAutomaticReplacementRecovery: recovery,
      });
      await reconciler.reconcileNext();
      expect(calls).toEqual(["rollback observed", "retry"]);
      await reconciler.reconcileNext();
      expect(calls).toEqual([
        "rollback observed",
        "retry",
        "initialize",
        "ready",
      ]);
    });
  }

  test("an interrupted initialization is released without waiting for a socket", async () => {
    const current = allocation({
      status: "allocated",
      generation: 1,
      ensureAcceptedGeneration: 1,
      initializationLeaseId: "old-owner",
    });
    let released = false;
    const store = fakeStore({
      claimNextReconcilable: async () => current,
      beginUnrecoverableRelease: async () => {
        released = true;
        return { ...current, status: "releasing", generation: 2 };
      },
    });
    const reconciler = createSidecarAllocationReconciler(
      deps({
        store,
        ready: false,
        waitError: new Error("must not wait"),
        onReady: async () => {
          throw new Error("must not initialize");
        },
      }),
    );
    await reconciler.reconcileNext();
    expect(released).toBe(true);
  });

  test("a committed initialization re-enters the callback after its response is lost", async () => {
    const current = allocation({
      status: "allocated",
      generation: 1,
      ensureAcceptedGeneration: 1,
    });
    let initialized = 0;
    let ready = false;
    let retried = false;
    const store = fakeStore({
      claimNextReconcilable: async () => current,
      beginUnrecoverableRelease: async (args) => {
        expect(args.onlyIfInitializationIncomplete).toBe(true);
        return null; // The DB observed committed completion under the lock.
      },
      scheduleRetry: async (args) => {
        expect(args.nextAttemptAt).toEqual(NOW);
        retried = true;
        return current;
      },
      markConnectionReady: async () => {
        ready = true;
        return current;
      },
    });
    const reconciler = createSidecarAllocationReconciler(
      deps({
        store,
        onReady: async () => {
          if (++initialized === 1)
            throw new SessionLaunchError(
              "start",
              new Error("commit response lost"),
              true,
            );
        },
      }),
    );
    await reconciler.reconcileNext();
    expect(retried).toBe(true);
    expect(ready).toBe(false);
    await reconciler.reconcileNext();
    expect(initialized).toBe(2);
    expect(ready).toBe(true);
  });
});

describe("provisioner operation deadlines", () => {
  test.each(["operation timeout", "lease expiry"] as const)(
    "retries a nested readiness lookup after %s without releasing the connected worker",
    async (interruption) => {
      const current = allocation({
        status: "allocated",
        generation: TEST_TARGET.generation,
        ensureAcceptedGeneration: TEST_TARGET.generation,
        connectDeadline: new Date(0),
      });
      const lookup = Promise.withResolvers<boolean>();
      const renewal = Promise.withResolvers<boolean>();
      let blockLookup = false;
      let nestedLookups = 0;
      const router = createAllocatedRouter({
        validateSidecarIdentity: async (_identity, use) => {
          if (blockLookup && use === "readiness") {
            nestedLookups += 1;
            return lookup.promise;
          }
          return true;
        },
      });
      let socket: Awaited<ReturnType<typeof connectAllocated>> | undefined;
      let parked = false;
      let released = false;
      let initialized = false;
      let ready = false;
      const reconciler = createSidecarAllocationReconciler({
        ...deps({
          store: fakeStore({
            claimNextReconcilable: async (args) =>
              args.excludedAllocationIds?.includes(current.id) ? null : current,
            extendReconciliationLease: () => renewal.promise,
            isReconciliationLeaseCurrent: async () => {
              if (socket === undefined) {
                socket = await connectAllocated(router);
                blockLookup = true;
              }
              return true;
            },
            parkReconciliation: async () => {
              parked = true;
              return true;
            },
            beginUnrecoverableRelease: async () => {
              released = true;
              return { ...current, status: "releasing", generation: 2 };
            },
            markConnectionReady: async () => {
              ready = true;
              return current;
            },
          }),
        }),
        router,
        operationTimeoutMs: interruption === "operation timeout" ? 50 : 1_000,
        leaseDurationMs: interruption === "lease expiry" ? 50 : 1_000,
        onReady: async () => {
          initialized = true;
        },
      });
      router.events.on("sidecar.allocated.connected", (target) =>
        reconciler.handleConnected(target),
      );
      try {
        await reconciler.reconcileNext();
        expect(released).toBe(false);
        expect(socket?.closed).toBe(false);
        expect(parked).toBe(interruption === "operation timeout");
        expect(initialized).toBe(false);
        expect(await reconciler.reconcileNext()).toBe(false);
        expect(nestedLookups).toBe(1);

        blockLookup = false;
        lookup.resolve(true);
        renewal.resolve(true);
        await tick();
        expect(initialized).toBe(false);
        expect(ready).toBe(false);
        expect(await reconciler.reconcileNext()).toBe(true);
        expect(initialized).toBe(true);
        expect(ready).toBe(true);
      } finally {
        lookup.resolve(true);
        renewal.resolve(true);
        if (socket !== undefined) router.handleClose(socket);
      }
    },
  );

  test("still releases a missing worker after its connection deadline expires", async () => {
    const current = allocation({
      status: "allocated",
      generation: TEST_TARGET.generation,
      ensureAcceptedGeneration: TEST_TARGET.generation,
      connectDeadline: new Date(0),
    });
    let released = false;
    let initialized = false;
    const reconciler = createSidecarAllocationReconciler({
      ...deps({
        store: fakeStore({
          claimNextReconcilable: async () => current,
          beginUnrecoverableRelease: async () => {
            released = true;
            return { ...current, status: "releasing", generation: 2 };
          },
        }),
      }),
      router: createAllocatedRouter(),
      operationTimeoutMs: 30,
      onReady: async () => {
        initialized = true;
      },
    });
    await reconciler.reconcileNext();
    expect(released).toBe(true);
    expect(initialized).toBe(false);
  });

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
    "retains a completed claim's pending renewal until it settles with %s",
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
            claimNextReconcilable: async ({ excludedAllocationIds = [] }) =>
              excludedAllocationIds.includes("alloc-1")
                ? null
                : allocation({ status: "allocated", generation: 1 }),
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

      try {
        expect(await reconciler.reconcileNext()).toBe(false);
        if (outcome === "error")
          renewal.reject(new Error("old connection lost"));
        else renewal.resolve(outcome === "accepted");
        await tick();
        expect(signals.map((signal) => signal.aborted)).toEqual([false]);
        const next = reconciler.reconcileNext();
        await nextEntered.promise;
        nextInitialization.resolve(true);
        await next;
        expect(signals.map((signal) => signal.aborted)).toEqual([false, false]);
      } finally {
        renewal.resolve(true);
        nextInitialization.resolve(true);
      }
      expect(readyLeases).toEqual(["lease-1", "lease-3"]);
    },
  );

  test("keeps a committed ready write ordered before reconnect after renewal observes lease release", async () => {
    const writeEntered = Promise.withResolvers<boolean>();
    const readyResponse = Promise.withResolvers<SidecarAllocation | null>();
    const current = allocation({ status: "allocated", generation: 1 });
    const other = allocation({ ...current, id: "alloc-2" });
    const exclusions: (readonly string[])[] = [];
    const calls: string[] = [];
    let readyCommitted = false;
    let otherReady = false;
    const reconciler = createSidecarAllocationReconciler({
      ...deps({
        store: fakeStore({
          claimNextReconcilable: async (args) => {
            const excluded = args.excludedAllocationIds ?? [];
            exclusions.push(excluded);
            if (!excluded.includes(current.id)) return current;
            return otherReady ? null : other;
          },
          extendReconciliationLease: async (id) =>
            id !== current.id || !readyCommitted,
          markConnectionReady: async (args) => {
            if (args.allocationId === current.id && !readyCommitted) {
              readyCommitted = true;
              writeEntered.resolve(true);
              return readyResponse.promise;
            }
            if (args.allocationId === other.id) otherReady = true;
            calls.push(`ready:${args.allocationId}`);
            return current;
          },
          wakeReconciliation: async () => {
            calls.push("wake");
            return true;
          },
        }),
      }),
      leaseDurationMs: 300,
    });
    const work = reconciler.reconcileNext();
    await writeEntered.promise;
    const connected = reconciler.handleConnected({
      allocationId: current.id,
      generation: current.generation,
    });
    try {
      await work;
      expect(calls).toEqual([]);
      expect(await reconciler.reconcileNext()).toBe(true);
      expect(exclusions[1]).toContain(current.id);
      expect(calls).toEqual(["ready:alloc-2"]);
    } finally {
      readyResponse.resolve(current);
      await Promise.all([connected, work]);
    }
    expect(calls).toEqual(["ready:alloc-2", "wake"]);
    expect(await reconciler.reconcileNext()).toBe(true);
    expect(calls).toEqual(["ready:alloc-2", "wake", "ready:alloc-1"]);
  });

  for (const phase of ["allocated", "ensure accepted", "recovery"] as const) {
    test.each(["disconnect", "operation deadline", "lease expiry"] as const)(
      `releases a slot during ${phase} queries on %s and ignores the late result`,
      async (interruption) => {
        const entered = Promise.withResolvers<boolean>();
        const readiness = Promise.withResolvers<boolean>();
        const renewal = Promise.withResolvers<boolean>();
        const current = allocation({
          status: "allocated",
          generation: 1,
          ensureAcceptedGeneration: 1,
          sidecarId: "sc-current",
        });
        const other = allocation({ ...current, id: "alloc-2" });
        const calls: string[] = [];
        const exclusions: (readonly string[])[] = [];
        let claims = 0;
        let readinessChecks = 0;
        let settled = false;
        const dependencies = deps({
          store: fakeStore({
            claimNextReconcilable: async (args) => {
              exclusions.push(args.excludedAllocationIds ?? []);
              if (++claims === 2) return other;
              if (claims > 2) return current;
              return phase === "ensure accepted" ? allocation() : current;
            },
            bindInitialSidecar: async () => ({
              ...current,
              status: "provisioning",
            }),
            markAllocated: async () => current,
            extendReconciliationLease: (id) =>
              id === current.id && interruption === "lease expiry"
                ? renewal.promise
                : Promise.resolve(true),
            markConnectionLost: async () => current,
            parkReconciliation: async (id) => {
              calls.push(`park:${id}`);
              return true;
            },
            markConnectionReady: async (args) => {
              calls.push(`ready:${args.allocationId}`);
              return null;
            },
          }),
        });
        const query = (allocationId: string) => {
          if (allocationId !== current.id) return Promise.resolve(true);
          readinessChecks += 1;
          entered.resolve(true);
          return readiness.promise;
        };
        const reconciler = createSidecarAllocationReconciler({
          ...dependencies,
          leaseDurationMs: interruption === "lease expiry" ? 40 : 1_000,
          operationTimeoutMs:
            interruption === "operation deadline" ? 30 : 1_000,
          router: {
            ...dependencies.router,
            isAllocatedSidecarReady: (target) =>
              phase === "recovery"
                ? Promise.resolve(true)
                : query(target.allocationId),
          },
          ...(phase === "recovery"
            ? {
                onInitializationRecovery: async (row: SidecarAllocation) => {
                  await query(row.id);
                },
              }
            : {}),
          onReady: async (row) => {
            calls.push(`initialize:${row.id}`);
          },
        });
        const work = reconciler.reconcileNext().then(() => {
          settled = true;
        });
        await entered.promise;
        if (interruption === "disconnect") {
          await reconciler.handleDisconnect({
            allocationId: current.id,
            generation: current.generation,
          });
        }
        try {
          await new Promise((resolve) => setTimeout(resolve, 100));
          expect(settled).toBe(true);
          expect(calls).toEqual(
            interruption === "operation deadline" ? ["park:alloc-1"] : [],
          );
          expect(await reconciler.reconcileNext()).toBe(true);
          expect(exclusions[1]).toContain(current.id);
          expect(readinessChecks).toBe(1);
          expect(calls.slice(-2)).toEqual([
            "initialize:alloc-2",
            "ready:alloc-2",
          ]);
        } finally {
          renewal.resolve(true);
          readiness.resolve(true);
          await work;
        }
        await tick();
        expect(calls).not.toContain("initialize:alloc-1");
        expect(calls).not.toContain("ready:alloc-1");
        expect(await reconciler.reconcileNext()).toBe(true);
        expect(exclusions[2]).not.toContain(current.id);
        expect(readinessChecks).toBe(2);
        expect(calls.slice(-2)).toEqual([
          "initialize:alloc-1",
          "ready:alloc-1",
        ]);
      },
    );
  }

  test.each(["readiness", "recovery"] as const)(
    "a late claim cannot duplicate a cancelled %s query and a rejected query permits retry",
    async (phase) => {
      const current = allocation({ status: "allocated", generation: 1 });
      const lateClaim = Promise.withResolvers<SidecarAllocation>();
      const entered = Promise.withResolvers<boolean>();
      const readiness = Promise.withResolvers<boolean>();
      const exclusions: (readonly string[])[] = [];
      let claims = 0;
      let readinessChecks = 0;
      let initialized = 0;
      let parks = 0;
      const dependencies = deps({
        store: fakeStore({
          claimNextReconcilable: (args) => {
            exclusions.push(args.excludedAllocationIds ?? []);
            return ++claims === 2
              ? lateClaim.promise
              : Promise.resolve(current);
          },
          markConnectionLost: async () => current,
          markConnectionReady: async () => null,
          parkReconciliation: async () => {
            parks += 1;
            return true;
          },
        }),
      });
      const query = () => {
        if (++readinessChecks > 1) return Promise.resolve(true);
        entered.resolve(true);
        return readiness.promise;
      };
      const reconciler = createSidecarAllocationReconciler({
        ...dependencies,
        router: {
          ...dependencies.router,
          isAllocatedSidecarReady: () =>
            phase === "recovery" ? Promise.resolve(true) : query(),
        },
        ...(phase === "recovery"
          ? {
              onInitializationRecovery: async () => {
                await query();
              },
            }
          : {}),
        onReady: async () => {
          initialized += 1;
        },
      });
      // Both claims take their exclusion snapshot before either response arrives.
      const first = reconciler.reconcileNext();
      const second = reconciler.reconcileNext();
      await entered.promise;
      try {
        await reconciler.handleDisconnect({
          allocationId: current.id,
          generation: current.generation,
        });
        await first;
        lateClaim.resolve(current);
        await second;
        expect(exclusions).toEqual([[], []]);
        expect(readinessChecks).toBe(1);
        expect(initialized).toBe(0);
        expect(parks).toBe(0);
      } finally {
        lateClaim.resolve(current);
        readiness.reject(new Error("old database connection closed"));
        await Promise.all([first, second]);
      }
      await tick();
      expect(await reconciler.reconcileNext()).toBe(true);
      expect(readinessChecks).toBe(2);
      expect(initialized).toBe(1);
    },
  );

  for (const phase of ["initialization", "completion"] as const) {
    test.each(["lease expiry", "disconnect", "operation deadline"] as const)(
      `releases a slot waiting for ${phase} on %s without overtaking connection writes`,
      async (interruption) => {
        const entered = Promise.withResolvers<boolean>();
        const proceed = Promise.withResolvers<boolean>();
        const wakeEntered = Promise.withResolvers<boolean>();
        const wake = Promise.withResolvers<boolean>();
        const current = allocation({ status: "allocated", generation: 1 });
        const other = allocation({
          id: "alloc-2",
          status: "allocated",
          generation: 1,
        });
        const ready: string[] = [];
        const initialized: string[] = [];
        const exclusions: (readonly string[])[] = [];
        let paused = false;
        let lease = 0;
        let finished = false;
        const pauseOnce = async () => {
          if (paused) return;
          paused = true;
          entered.resolve(true);
          await proceed.promise;
        };
        const dependencies = deps({
          store: fakeStore({
            claimNextReconcilable: async (args) => {
              const excluded = args.excludedAllocationIds ?? [];
              exclusions.push(excluded);
              return (
                [current, other].find(
                  (candidate) =>
                    !excluded.includes(candidate.id) &&
                    !ready.includes(candidate.id),
                ) ?? null
              );
            },
            wakeReconciliation: () => {
              wakeEntered.resolve(true);
              return wake.promise;
            },
            // Renewal of this row waits behind its blocked connection write.
            extendReconciliationLease: (id) =>
              id === current.id ? wake.promise : Promise.resolve(true),
            markConnectionLost: async () => current,
            markConnectionReady: async (args) => {
              ready.push(args.allocationId);
              return current;
            },
          }),
        });
        const reconciler = createSidecarAllocationReconciler({
          ...dependencies,
          createLeaseId: () => `lease-${String(++lease)}`,
          leaseDurationMs: interruption === "lease expiry" ? 40 : 1_000,
          operationTimeoutMs:
            interruption === "operation deadline" ? 30 : 1_000,
          router: {
            ...dependencies.router,
            isAllocatedSidecarReady: async (target) => {
              if (
                target.allocationId === current.id &&
                phase === "initialization"
              ) {
                await pauseOnce();
              }
              return true;
            },
          },
          onReady: async (row) => {
            initialized.push(row.id);
            if (row.id === current.id && phase === "completion") {
              await pauseOnce();
            }
          },
        });
        const work = reconciler.reconcileNext().then(() => {
          finished = true;
        });
        await entered.promise;
        const target = { allocationId: current.id, generation: 1 };
        const connected = reconciler.handleConnected(target);
        await wakeEntered.promise;
        proceed.resolve(true);
        await tick();
        const disconnected =
          interruption === "disconnect"
            ? reconciler.handleDisconnect(target)
            : Promise.resolve();
        try {
          await new Promise((resolve) => setTimeout(resolve, 100));
          expect(finished).toBe(true);
          expect(ready).toEqual([]);

          expect(await reconciler.reconcileNext()).toBe(true);
          expect(exclusions[1]).toContain(current.id);
          expect(ready).toEqual([other.id]);
          expect(initialized).toEqual(
            phase === "initialization" ? [other.id] : [current.id, other.id],
          );
        } finally {
          wake.resolve(true);
          await Promise.all([connected, disconnected, work]);
        }
        await tick();

        // The abandoned queued callback must not initialize or publish readiness
        // when the old connection write finally returns. A fresh claim may retry.
        expect(ready).toEqual([other.id]);
        expect(await reconciler.reconcileNext()).toBe(true);
        expect(ready).toEqual([other.id, current.id]);
      },
    );
  }

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

  for (const phase of ["readiness", "recovery", "lease validation"] as const) {
    for (const settlement of ["resolve", "reject"] as const) {
      test(`bounds abandoned ${phase} queries across allocations until they ${settlement}`, async () => {
        const firstRead = Promise.withResolvers<boolean>();
        const secondRead = Promise.withResolvers<boolean>();
        const firstEntered = Promise.withResolvers<boolean>();
        const secondEntered = Promise.withResolvers<boolean>();
        const thirdClaim = Promise.withResolvers<SidecarAllocation | null>();
        const thirdClaimEntered = Promise.withResolvers<boolean>();
        const rows = ["alloc-1", "alloc-2", "alloc-3"].map((id) =>
          allocation({ id, status: "allocated", generation: 1 }),
        );
        let claims = 0;
        const initialized: string[] = [];
        const dependencies = deps({
          store: fakeStore({
            claimNextReconcilable: async () => {
              const row = rows[claims++] ?? null;
              if (claims === 3) {
                thirdClaimEntered.resolve(true);
                return thirdClaim.promise;
              }
              return row;
            },
            markConnectionReady: async () => null,
            scheduleRetry: async () => null,
          }),
        });
        const query = (allocationId: string) => {
          if (allocationId === "alloc-1") {
            firstEntered.resolve(true);
            return firstRead.promise;
          }
          if (allocationId === "alloc-2") {
            secondEntered.resolve(true);
            return secondRead.promise;
          }
          return Promise.resolve(true);
        };
        const reconciler = createSidecarAllocationReconciler({
          ...dependencies,
          maxConcurrentClaims: 2,
          operationTimeoutMs: 30,
          leaseDurationMs: 1_000,
          allocationStore: {
            ...dependencies.allocationStore,
            isReconciliationLeaseCurrent: (allocationId) =>
              phase === "lease validation"
                ? query(allocationId)
                : Promise.resolve(true),
          },
          router: {
            ...dependencies.router,
            isAllocatedSidecarReady: (target) =>
              phase === "readiness"
                ? query(target.allocationId)
                : Promise.resolve(true),
          },
          ...(phase === "recovery"
            ? {
                onInitializationRecovery: async (row: SidecarAllocation) => {
                  await query(row.id);
                },
              }
            : {}),
          onReady: async (row) => {
            initialized.push(row.id);
          },
        });
        const first = reconciler.reconcileNext();
        const work = [first];
        try {
          await firstEntered.promise;
          // One active allocation with a pending query occupies one slot.
          const second = reconciler.reconcileNext();
          work.push(second);
          expect(
            await Promise.race([
              secondEntered.promise,
              second.then(() => false),
            ]),
          ).toBe(true);
          await Promise.all(work);

          for (let attempt = 0; attempt < 10; attempt += 1) {
            expect(await reconciler.reconcileNext()).toBe(false);
          }
          expect(claims).toBe(2);
          expect(initialized).toEqual([]);

          if (settlement === "resolve") firstRead.resolve(true);
          else firstRead.reject(new Error("Old database connection closed"));
          await tick();
          const third = reconciler.reconcileNext();
          work.push(third);
          await thirdClaimEntered.promise;
          // The remaining abandoned read and this pending claim share the limit.
          expect(await reconciler.reconcileNext()).toBe(false);
          expect(claims).toBe(3);
          thirdClaim.resolve(rows[2] ?? null);
          expect(await third).toBe(true);
          expect(claims).toBe(3);
          expect(initialized).toEqual(["alloc-3"]);
        } finally {
          firstRead.resolve(true);
          secondRead.resolve(true);
          thirdClaim.resolve(null);
          await Promise.allSettled(work);
          await tick();
        }
      });
    }
  }

  for (const firstSettled of ["readiness", "renewal"] as const) {
    test(`retains overlapping queries after ${firstSettled} settles`, async () => {
      const readiness = Promise.withResolvers<boolean>();
      const renewal = Promise.withResolvers<boolean>();
      const renewalEntered = Promise.withResolvers<boolean>();
      let claims = 0;
      const initialized: string[] = [];
      const dependencies = deps({
        store: fakeStore({
          claimNextReconcilable: async () =>
            allocation({
              id: `alloc-${String(++claims)}`,
              status: "allocated",
              generation: 1,
            }),
          extendReconciliationLease: () => {
            renewalEntered.resolve(true);
            return renewal.promise;
          },
          markConnectionReady: async () => null,
        }),
      });
      const reconciler = createSidecarAllocationReconciler({
        ...dependencies,
        maxConcurrentClaims: 1,
        leaseDurationMs: 90,
        router: {
          ...dependencies.router,
          isAllocatedSidecarReady: ({ allocationId }) =>
            allocationId === "alloc-1"
              ? readiness.promise
              : Promise.resolve(true),
        },
        onReady: async (row) => {
          initialized.push(row.id);
        },
      });
      const first = reconciler.reconcileNext();
      try {
        await renewalEntered.promise;
        await first;
        expect(await reconciler.reconcileNext()).toBe(false);
        if (firstSettled === "readiness") readiness.resolve(true);
        else renewal.resolve(true);
        await tick();
        expect(await reconciler.reconcileNext()).toBe(false);
        expect(claims).toBe(1);

        readiness.resolve(true);
        renewal.resolve(true);
        await tick();
        expect(await reconciler.reconcileNext()).toBe(true);
        expect(claims).toBe(2);
        expect(initialized).toEqual(["alloc-2"]);
      } finally {
        readiness.resolve(true);
        renewal.resolve(true);
        await first;
      }
    });
  }

  test("retains capacity until a timed-out connection write settles", async () => {
    const write = Promise.withResolvers<SidecarAllocation | null>();
    let claims = 0;
    let writes = 0;
    const reconciler = createSidecarAllocationReconciler({
      ...deps({
        store: fakeStore({
          claimNextReconcilable: async () =>
            allocation({
              id: `alloc-${String(++claims)}`,
              status: "allocated",
              generation: 1,
            }),
          markConnectionReady: () =>
            ++writes === 1 ? write.promise : Promise.resolve(null),
        }),
      }),
      maxConcurrentClaims: 1,
      operationTimeoutMs: 30,
    });
    try {
      expect(await reconciler.reconcileNext()).toBe(true);
      expect(writes).toBe(1);
      expect(await reconciler.reconcileNext()).toBe(false);
      expect(claims).toBe(1);
      write.resolve(null);
      await tick();
      expect(await reconciler.reconcileNext()).toBe(true);
      expect(claims).toBe(2);
      expect(writes).toBe(2);
    } finally {
      write.resolve(null);
      await tick();
    }
  });

  test("keeps capacity reserved while a claim becomes active reconciliation", async () => {
    const claim = Promise.withResolvers<SidecarAllocation | null>();
    const claimEntered = Promise.withResolvers<boolean>();
    const finish = Promise.withResolvers<boolean>();
    let claims = 0;
    let initialized = false;
    const reconciler = createSidecarAllocationReconciler({
      ...deps({
        store: fakeStore({
          claimNextReconcilable: () => {
            claims += 1;
            claimEntered.resolve(true);
            return claims === 1 ? claim.promise : Promise.resolve(null);
          },
          markConnectionReady: async () => null,
        }),
      }),
      maxConcurrentClaims: 1,
      onReady: async () => {
        initialized = true;
        await finish.promise;
      },
    });
    const work = reconciler.reconcileNext();
    await claimEntered.promise;
    const competing = claim.promise.then(async () => {
      for (let attempt = 0; attempt < 50 && !initialized; attempt += 1) {
        expect(await reconciler.reconcileNext()).toBe(false);
      }
      expect(initialized).toBe(true);
    });
    try {
      claim.resolve(allocation({ status: "allocated", generation: 1 }));
      await competing;
      expect(claims).toBe(1);
      expect(await reconciler.reconcileNext()).toBe(false);
      expect(claims).toBe(1);
    } finally {
      claim.resolve(null);
      finish.resolve(true);
      await Promise.allSettled([work, competing]);
    }
    expect(await reconciler.reconcileNext()).toBe(false);
    expect(claims).toBe(2);
  });

  test("releases capacity after empty, rejected, and synchronously thrown claims", async () => {
    let claims = 0;
    const reconciler = createSidecarAllocationReconciler({
      ...deps({
        store: fakeStore({
          claimNextReconcilable: () => {
            claims += 1;
            if (claims === 2)
              return Promise.reject(new Error("Rejected claim"));
            if (claims === 3) throw new Error("Thrown claim");
            return Promise.resolve(null);
          },
        }),
      }),
      maxConcurrentClaims: 1,
    });
    expect(await reconciler.reconcileNext()).toBe(false);
    await expect(reconciler.reconcileNext()).rejects.toThrow("Rejected claim");
    await expect(reconciler.reconcileNext()).rejects.toThrow("Thrown claim");
    expect(await reconciler.reconcileNext()).toBe(false);
    expect(claims).toBe(4);
  });

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

  for (const settlement of ["resolve", "reject"] as const) {
    test(`bounds outstanding claims across timeouts and resumes after they ${settlement}`, async () => {
      const claims = Array.from({ length: 2 }, () =>
        Promise.withResolvers<SidecarAllocation | null>(),
      );
      let started = 0;
      let initialized = false;
      const reconciler = createSidecarAllocationReconciler({
        ...deps({
          store: fakeStore({
            claimNextReconcilable: () => {
              const claim = claims[started++];
              return claim?.promise ?? Promise.resolve(null);
            },
          }),
        }),
        operationTimeoutMs: 10,
        maxConcurrentClaims: 2,
        onReady: async () => {
          initialized = true;
        },
      });
      try {
        const results = await Promise.allSettled([
          reconciler.reconcileNext(),
          reconciler.reconcileNext(),
          reconciler.reconcileNext(),
        ]);
        expect(results.map((result) => result.status)).toEqual([
          "rejected",
          "rejected",
          "fulfilled",
        ]);
        expect(results[2]).toEqual({ status: "fulfilled", value: false });
        expect(await reconciler.reconcileNext()).toBe(false);
        expect(await reconciler.reconcileNext()).toBe(false);
        expect(started).toBe(2);
        const first = claims[0];
        if (first === undefined) throw new Error("Missing first claim");
        if (settlement === "resolve")
          first.resolve(allocation({ status: "allocated", generation: 1 }));
        else first.reject(new Error("delayed database failure"));
        await tick();
        expect(await reconciler.reconcileNext()).toBe(false);
        expect(started).toBe(3);
        expect(initialized).toBe(false);
      } finally {
        for (const claim of claims) claim.resolve(null);
        await Promise.allSettled(claims.map((claim) => claim.promise));
      }
    });
  }

  test("retries a lease query failure when the worker reconnects during validation", async () => {
    const current = allocation({
      status: "allocated",
      generation: TEST_TARGET.generation,
      ensureAcceptedGeneration: TEST_TARGET.generation,
      connectDeadline: new Date(0),
    });
    const router = createAllocatedRouter();
    const calls: string[] = [];
    let validations = 0;
    let socket: Awaited<ReturnType<typeof connectAllocated>> | undefined;
    const reconciler = createSidecarAllocationReconciler({
      ...deps({
        store: fakeStore({
          claimNextReconcilable: async () => current,
          isReconciliationLeaseCurrent: async () => {
            validations += 1;
            if (validations === 2) {
              socket = await connectAllocated(router);
              throw new Error("Database connection terminated unexpectedly");
            }
            return true;
          },
          parkReconciliation: async () => {
            calls.push("park");
            return true;
          },
          beginUnrecoverableRelease: async () => {
            calls.push("release");
            return { ...current, status: "releasing", generation: 2 };
          },
          markConnectionReady: async () => {
            calls.push("ready");
            return current;
          },
        }),
      }),
      router,
      onInitializationRecovery: async () => {
        calls.push("recover");
      },
      onReady: async () => {
        calls.push("initialize");
      },
    });
    router.events.on("sidecar.allocated.connected", (target) =>
      reconciler.handleConnected(target),
    );
    try {
      await reconciler.reconcileNext();
      expect(calls).toEqual(["recover"]);
      expect(socket?.closed).toBe(false);
      expect(await router.isAllocatedSidecarReady(TEST_TARGET)).toBe(true);

      await reconciler.reconcileNext();
      expect(calls).toEqual(["recover", "recover", "initialize", "ready"]);
      expect(socket?.closed).toBe(false);
    } finally {
      if (socket !== undefined) router.handleClose(socket);
    }
  });

  test("retries an identity validation failure for a connected worker past its deadline", async () => {
    const current = allocation({
      status: "allocated",
      generation: TEST_TARGET.generation,
      ensureAcceptedGeneration: TEST_TARGET.generation,
      connectDeadline: new Date(0),
    });
    let failValidation = false;
    const router = createAllocatedRouter({
      validateSidecarIdentity: async () => {
        if (failValidation) throw new Error("statement timeout");
        return true;
      },
    });
    const calls: string[] = [];
    let parkKind: string | undefined;
    const reconciler = createSidecarAllocationReconciler({
      ...deps({
        store: fakeStore({
          claimNextReconcilable: async () => current,
          parkReconciliation: async (_allocationId, _leaseId, policy) => {
            calls.push("park");
            parkKind = policy.kind;
            return true;
          },
          beginReplacement: async () => {
            calls.push("release");
            return null;
          },
          beginUnrecoverableRelease: async () => {
            calls.push("release");
            return null;
          },
          markConnectionReady: async () => {
            calls.push("ready");
            return current;
          },
        }),
      }),
      router,
      onInitializationRecovery: async () => {
        calls.push("recover");
      },
      onReady: async () => {
        calls.push("initialize");
      },
    });
    const socket = await connectAllocated(router);
    failValidation = true;
    try {
      await reconciler.reconcileNext();
      expect(calls).toEqual(["recover", "park"]);
      expect(parkKind).toBe("retry-after-error");
      expect(socket.closed).toBe(false);

      failValidation = false;
      await reconciler.reconcileNext();
      expect(calls).toEqual([
        "recover",
        "park",
        "recover",
        "initialize",
        "ready",
      ]);
      expect(socket.closed).toBe(false);
      expect(await router.isAllocatedSidecarReady(TEST_TARGET)).toBe(true);
    } finally {
      router.handleClose(socket);
    }
  });

  for (const settlement of ["resolve", "reject"] as const) {
    test(`retains a notification lookup after the connection deadline until it ${settlement}s`, async () => {
      const validation = Promise.withResolvers<boolean>();
      const entered = Promise.withResolvers<boolean>();
      let checking = true;
      const router = createAllocatedRouter({
        validateSidecarIdentity: async (_identity, use) => {
          if (checking && use === "readiness") {
            entered.resolve(true);
            return validation.promise;
          }
          return true;
        },
      });
      const current = allocation({
        status: "allocated",
        generation: TEST_TARGET.generation,
        ensureAcceptedGeneration: TEST_TARGET.generation,
        connectDeadline: new Date(NOW.getTime() + 50),
      });
      const calls: string[] = [];
      let claims = 0;
      const reconciler = createSidecarAllocationReconciler({
        ...deps({
          store: fakeStore({
            claimNextReconcilable: async () => {
              claims += 1;
              return current;
            },
            parkReconciliation: async (_id, _leaseId, policy) => {
              expect(policy.kind).toBe("retry-after-error");
              calls.push("park");
              return true;
            },
            beginUnrecoverableRelease: async () => {
              calls.push("release");
              return { ...current, status: "releasing", generation: 2 };
            },
            markConnectionReady: async () => {
              calls.push("ready");
              return current;
            },
          }),
        }),
        router,
        maxConcurrentClaims: 1,
        leaseDurationMs: 1_000,
        operationTimeoutMs: 500,
        onReady: async () => {
          calls.push("initialize");
        },
      });
      router.events.on("sidecar.allocated.connected", (target) =>
        reconciler.handleConnected(target),
      );

      const reconciling = reconciler.reconcileNext();
      await tick();
      const socket = await connectAllocated(router, [
        TEST_IDENTITY.workflowRunAddress,
      ]);
      try {
        await entered.promise;
        await reconciling;
        expect(calls).toEqual(["park"]);
        expect(socket.closed).toBe(false);
        expect(await reconciler.reconcileNext()).toBe(false);
        expect(claims).toBe(1);

        checking = false;
        if (settlement === "resolve") validation.resolve(true);
        else validation.reject(new Error("statement timeout"));
        await tick();
        expect(calls).toEqual(["park"]);

        expect(await reconciler.reconcileNext()).toBe(true);
        expect(claims).toBe(2);
        expect(calls).toEqual(["park", "initialize", "ready"]);
        expect(socket.closed).toBe(false);
      } finally {
        checking = false;
        validation.resolve(true);
        await reconciling;
        await tick();
        router.handleClose(socket);
      }
    });
  }

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

  test("a renewal failure finishes without parking while its read retains capacity", async () => {
    const readiness = Promise.withResolvers<boolean>();
    const park = Promise.withResolvers<boolean>();
    const parkEntered = Promise.withResolvers<string>();
    let claims = 0;
    let renewals = 0;
    let reads = 0;
    let initialized = 0;
    const dependencies = deps({
      store: fakeStore({
        claimNextReconcilable: async () => {
          claims += 1;
          return allocation({ status: "allocated", generation: 1 });
        },
        extendReconciliationLease: async () => {
          renewals += 1;
          throw new Error("Database connection failed");
        },
        parkReconciliation: () => {
          parkEntered.resolve("park");
          return park.promise;
        },
        markConnectionReady: async () => null,
      }),
    });
    const reconciler = createSidecarAllocationReconciler({
      ...dependencies,
      maxConcurrentClaims: 1,
      leaseDurationMs: 90,
      router: {
        ...dependencies.router,
        isAllocatedSidecarReady: () =>
          ++reads === 1 ? readiness.promise : Promise.resolve(true),
      },
      onReady: async () => {
        initialized += 1;
      },
    });
    const work = reconciler.reconcileNext();
    try {
      expect(
        await Promise.race([work.then(() => "finished"), parkEntered.promise]),
      ).toBe("finished");
      expect(renewals).toBe(1);
      expect(initialized).toBe(0);
      expect(await reconciler.reconcileNext()).toBe(false);
      expect(claims).toBe(1);

      readiness.resolve(true);
      await tick();
      expect(initialized).toBe(0);
      expect(await reconciler.reconcileNext()).toBe(true);
      expect(claims).toBe(2);
      expect(initialized).toBe(1);
    } finally {
      readiness.resolve(true);
      park.resolve(false);
      await work;
    }
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

  test("a late claim leaves active initialization alone without parking", async () => {
    const current = allocation({ status: "allocated", generation: 1 });
    const entered = Promise.withResolvers<boolean>();
    const finish = Promise.withResolvers<boolean>();
    const lateClaim = Promise.withResolvers<SidecarAllocation>();
    const exclusions: (readonly string[])[] = [];
    let claims = 0;
    let initialized = 0;
    let parks = 0;
    const reconciler = createSidecarAllocationReconciler({
      ...deps({
        store: fakeStore({
          claimNextReconcilable: ({ excludedAllocationIds = [] }) => {
            exclusions.push(excludedAllocationIds);
            if (++claims === 1) return Promise.resolve(current);
            return lateClaim.promise;
          },
          parkReconciliation: async () => {
            parks += 1;
            return true;
          },
          markConnectionReady: async () => null,
        }),
      }),
      onReady: async () => {
        initialized += 1;
        entered.resolve(true);
        await finish.promise;
      },
    });
    const first = reconciler.reconcileNext();
    const late = reconciler.reconcileNext();
    try {
      await entered.promise;
      lateClaim.resolve(current);
      expect(await late).toBe(true);
      expect(exclusions).toEqual([[], []]);
      expect(initialized).toBe(1);
      expect(parks).toBe(0);
    } finally {
      lateClaim.resolve(current);
      finish.resolve(true);
      await Promise.all([first, late]);
    }
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

  test("excludes an allocation until its cancelled initialization settles", async () => {
    const entered = Promise.withResolvers<boolean>();
    const init = Promise.withResolvers<boolean>();
    const exclusions: (readonly string[])[] = [];
    const calls: string[] = [];
    let allowClaim = true;
    const reconciler = createSidecarAllocationReconciler({
      ...deps({
        store: fakeStore({
          claimNextReconcilable: async (args) => {
            exclusions.push(args.excludedAllocationIds ?? []);
            if (!allowClaim) return null;
            allowClaim = false;
            return allocation({ status: "allocated", generation: 1 });
          },
          markConnectionLost: async () => null,
          markConnectionReady: async () => {
            calls.push("ready");
            return allocation({ status: "allocated", generation: 1 });
          },
        }),
      }),
      maxConcurrentClaims: 1,
      onReady: async (_allocation, context) => {
        entered.resolve(true);
        await init.promise;
        context.signal.throwIfAborted();
        calls.push("initialize");
      },
    });
    const work = reconciler.reconcileNext();
    await entered.promise;
    const disconnect = reconciler.handleDisconnect({
      allocationId: "alloc-1",
      generation: 1,
    });
    try {
      await disconnect;
      await work;
      expect(calls).toEqual([]);
      expect(await reconciler.reconcileNext()).toBe(false);
      expect(exclusions).toEqual([[]]);

      init.resolve(true);
      await tick();
      allowClaim = true;
      expect(await reconciler.reconcileNext()).toBe(true);
      expect(exclusions).toEqual([[], []]);
      expect(calls).toEqual(["initialize", "ready"]);
    } finally {
      init.resolve(true);
      await work;
    }
  });

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
      // A late connect schedules an immediate follow-up however the
      // initialization went: the follow-up redeploys a replaced worker and
      // is a no-op when the worker is unchanged.
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

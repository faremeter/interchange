import { describe, expect, test } from "bun:test";
import { sha256 } from "@intx/crypto";
import {
  SidecarReuseRejectedError,
  type SidecarAllocation,
  type SidecarAllocationStore,
} from "@intx/db";
import { hexEncode } from "@intx/types";
import { configureSync, getConfig } from "@intx/log";

import { SessionLaunchError } from "../session-service";
import {
  isDeployFrameFailure,
  SidecarIdentityValidationError,
} from "../ws/sidecar-handler";
import {
  connectAllocated,
  createAllocatedRouter,
  deployReply,
  TEST_CONFIG,
  TEST_IDENTITY,
  TEST_TARGET,
  tick,
} from "../ws/sidecar-handler.test-helpers";
import type { EnsureSidecarResult, SidecarProvisioner } from "./contracts";
import {
  SidecarDeploymentMissingError,
  SidecarDeploymentHistoryPendingError,
  SidecarDeploymentStoppedError,
  SidecarFirstDeployError,
} from "./operation";
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
    maxDisconnectedMs: 900_000,
    status: "pending",
    generation: 0,
    nextAttemptAt: NOW,
    ensureAttempts: 0,
    destroyAttempts: 0,
    deploymentCleanupConfirmed: false,
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
    failStoppedDeployment: notUsed("failStoppedDeployment"),
    beginRelease: notUsed("beginRelease"),
    beginUnrecoverableRelease: notUsed("beginUnrecoverableRelease"),
    bindInitialSidecar: notUsed("bindInitialSidecar"),
    bindReplacementSidecar: notUsed("bindReplacementSidecar"),
    claimNextReconcilable: async () => null,
    extendReconciliationLease: async () => true,
    failWithoutInfrastructure: notUsed("failWithoutInfrastructure"),
    listActive: async () => [],
    isReconciliationLeaseCurrent: async () => true,
    hasRunnableAnchor: async () => true,
    markAllocated: notUsed("markAllocated"),
    markConnectionLost: notUsed("markConnectionLost"),
    markConnectionReady: notUsed("markConnectionReady"),
    markDestroyFailed: notUsed("markDestroyFailed"),
    markReleased: notUsed("markReleased"),
    confirmDeploymentCleanup: async () => true,
    parkReconciliation: async () => true,
    scheduleReconnectAfterHubStart: notUsed("scheduleReconnectAfterHubStart"),
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
      return { kind: "destroyed", cleanup: "confirmed" };
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
  /** The binding attaches only once its sidecar is synced. */
  readyOnlyAfterSync?: boolean;
  synced?: string[];
  /** Sidecar syncs and readiness checks, in the order they ran. */
  checks?: string[];
  readyError?: Error;
  waitError?: Error;
  /** Why the sidecar reported the deployment stopped on its own, if it did. */
  reportedStop?: string;
  onReady?: (row: SidecarAllocation) => Promise<void>;
}): SidecarAllocationReconcilerDeps {
  const provisioner = args.provisioner ?? testProvisioner();
  let syncedOnce = false;
  const cleanupConnection = {};
  return {
    allocationStore: args.store,
    plugins: {
      getProvisioner: (id) => (id === provisioner.id ? provisioner : null),
      selectProvisioner: async () => ({ ok: true, provisioner }),
    },
    router: {
      getCleanupConnection: () =>
        args.ready === false ? undefined : cleanupConnection,
      undeployAllocation: async () => undefined,
      fenceAllocation(id, generation) {
        args.fences?.push([id, generation]);
      },
      retireAllocation({ allocationId, generation }) {
        args.retired?.push([allocationId, generation]);
      },
      isAllocatedSidecarReady: async () => {
        args.checks?.push("ready");
        if (args.readyError !== undefined) throw args.readyError;
        if (args.readyOnlyAfterSync === true) return syncedOnce;
        return args.ready ?? true;
      },
      waitForAllocatedSidecar: async () => {
        if (args.waitError !== undefined) throw args.waitError;
      },
      holdsAllocatedBinding: () =>
        args.readyOnlyAfterSync === true ? syncedOnce : true,
      reportedDeploymentFailure: () => args.reportedStop,
      syncSidecar: async (sidecarId) => {
        syncedOnce = true;
        args.synced?.push(sidecarId);
        args.checks?.push(`sync:${sidecarId}`);
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
  function releasingState() {
    const state = {
      row: allocation({
        status: "releasing",
        generation: 2,
        ensureAcceptedGeneration: 1,
        sidecarId: "sc-shared",
      }),
    };
    const store = fakeStore({
      claimNextReconcilable: async () =>
        state.row.status === "releasing" ? state.row : null,
      listActive: async () => [state.row],
      confirmDeploymentCleanup: async () => {
        const { connectDeadline: _deadline, ...row } = state.row;
        state.row = { ...row, deploymentCleanupConfirmed: true };
        return true;
      },
      scheduleRetry: async ({ nextAttemptAt, attempt }) => {
        state.row = {
          ...state.row,
          nextAttemptAt,
          destroyAttempts:
            state.row.destroyAttempts + (attempt === "destroy" ? 1 : 0),
        };
        return state.row;
      },
      markDestroyFailed: async ({ code, message }) => {
        const { nextAttemptAt: _next, ...row } = state.row;
        state.row = {
          ...row,
          status: "destroy_failed",
          failureCode: code,
          failureMessage: message,
          destroyAttempts: row.destroyAttempts + 1,
        };
        return state.row;
      },
      markReleased: async () => {
        state.row = { ...state.row, status: "released" };
        return state.row;
      },
    });
    return { state, store };
  }

  test.each([
    "missing provider",
    "provider binding mismatch",
    "retryable rejection",
    "missing acknowledgement",
    "permanent rejection",
    "confirmed fallback",
  ])(
    "logs cleanup failure without recording a transient workflow error for %s",
    async (scenario) => {
      const saved = getConfig();
      if (saved === null) throw new Error("logging is not configured");
      const logs: { level: string; message: string }[] = [];
      configureSync({
        reset: true,
        sinks: {
          capture: (record) => {
            if (record.category.join(".") === "hub.sidecar-allocation")
              logs.push({
                level: record.level,
                message: record.message.join(""),
              });
          },
        },
        loggers: [
          {
            category: ["hub", "sidecar-allocation"],
            lowestLevel: "warning",
            sinks: ["capture"],
          },
          {
            category: ["logtape", "meta"],
            lowestLevel: "warning",
            sinks: ["capture"],
          },
        ],
      });
      try {
        const { state, store } = releasingState();
        state.row = {
          ...state.row,
          failureCode: "workflow_stop_failed",
          failureMessage: "original workflow cause",
        };
        const retry = store.scheduleRetry;
        store.scheduleRetry = async (args) => {
          expect(args.failure).toBeUndefined();
          return retry(args);
        };
        const options = deps({
          store,
          provisioner: testProvisioner({
            destroy: async () =>
              scenario === "retryable rejection" ||
              scenario === "permanent rejection"
                ? {
                    kind: "rejected",
                    code: "provider_refused",
                    message: "provider cannot remove its hold",
                    retryable: scenario === "retryable rejection",
                  }
                : {
                    kind: "destroyed",
                    cleanup:
                      scenario === "confirmed fallback"
                        ? "confirmed"
                        : "required",
                  },
          }),
        });
        if (scenario === "missing provider")
          options.plugins.getProvisioner = () => null;
        else if (scenario === "provider binding mismatch")
          options.plugins.getProvisioner = () =>
            testProvisioner({ bindingFingerprint: "other-backend" });
        else if (
          scenario !== "retryable rejection" &&
          scenario !== "permanent rejection"
        )
          options.router.undeployAllocation = async () => {
            throw new Error("cleanup acknowledgement timed out");
          };
        await createSidecarAllocationReconciler(options).reconcileNext();
        const terminal = scenario === "permanent rejection";
        expect(state.row.status).toBe(
          terminal
            ? "destroy_failed"
            : scenario === "confirmed fallback"
              ? "released"
              : "releasing",
        );
        if (!terminal) {
          expect(state.row.failureCode).toBe("workflow_stop_failed");
          expect(state.row.failureMessage).toBe("original workflow cause");
        }
        const message =
          scenario === "missing provider"
            ? "Provisioner test is missing from the plugin registry"
            : scenario === "provider binding mismatch"
              ? "has binding other-backend; this allocation requires binding test:v1"
              : scenario === "retryable rejection" ||
                  scenario === "permanent rejection"
                ? "provider cannot remove its hold"
                : "cleanup acknowledgement timed out";
        expect(
          logs.some(
            (log) =>
              log.level ===
                (terminal ||
                scenario === "missing provider" ||
                scenario === "provider binding mismatch"
                  ? "error"
                  : "warning") &&
              log.message.includes(message) &&
              log.message.includes("alloc-1") &&
              log.message.includes("sc-shared") &&
              log.message.includes("generation 2"),
          ),
        ).toBe(true);
        if (terminal)
          expect(
            logs.some(
              (log) =>
                log.level === "error" &&
                log.message.includes("original workflow cause"),
            ),
          ).toBe(true);
      } finally {
        configureSync({ reset: true, ...saved });
      }
    },
  );

  test.each(["missing", "binding mismatch"])(
    "waits for a %s provisioner without spending attempts and resumes when the original binding returns",
    async (unavailable) => {
      const { state, store } = releasingState();
      state.row = { ...state.row, destroyAttempts: 9 };
      let clock = NOW;
      let restored = false;
      let destroys = 0;
      let undeploys = 0;
      const provisioner = testProvisioner({
        destroy: async () => {
          destroys++;
          return { kind: "destroyed", cleanup: "required" };
        },
      });
      const options = deps({ store, provisioner });
      options.plugins.getProvisioner = () =>
        restored
          ? provisioner
          : unavailable === "missing"
            ? null
            : testProvisioner({
                bindingFingerprint: "other-backend",
                destroy: async () => {
                  throw new Error("Must not call a different backend");
                },
              });
      options.router.undeployAllocation = async () => {
        undeploys++;
      };
      const claim = store.claimNextReconcilable;
      store.claimNextReconcilable = (args) =>
        state.row.nextAttemptAt !== undefined &&
        state.row.nextAttemptAt <= clock
          ? claim(args)
          : Promise.resolve(null);
      const retry = store.scheduleRetry;
      store.scheduleRetry = async (args) => {
        expect(args.attempt).toBeUndefined();
        expect(args.minimumDelayMs).toBe(30_000);
        return retry(args);
      };
      const reconciler = createSidecarAllocationReconciler({
        ...options,
        now: () => clock,
      });
      for (let check = 0; check < 12; check++) {
        expect(await reconciler.reconcileNext()).toBe(true);
        expect(state.row).toMatchObject({
          status: "releasing",
          destroyAttempts: 9,
          deploymentCleanupConfirmed: true,
          nextAttemptAt: new Date(clock.getTime() + 30_000),
        });
        expect(await reconciler.reconcileNext()).toBe(false);
        clock = new Date(clock.getTime() + 30_000);
      }
      expect(destroys).toBe(0);
      expect(undeploys).toBe(1);
      restored = true;
      options.router.getCleanupConnection = () => undefined;
      const restarted = createSidecarAllocationReconciler({
        ...options,
        now: () => clock,
      });
      await restarted.initialize();
      expect(await restarted.reconcileNext()).toBe(true);
      expect(state.row.status).toBe("released");
      expect(destroys).toBe(1);
      expect(undeploys).toBe(1);
    },
  );

  test("missing provider and sidecar connections do not consume cleanup attempts", async () => {
    const { state, store } = releasingState();
    const options = deps({ store, ready: false });
    options.plugins.getProvisioner = () => null;
    await createSidecarAllocationReconciler(options).reconcileNext();
    expect(state.row).toMatchObject({
      status: "releasing",
      destroyAttempts: 0,
      deploymentCleanupConfirmed: false,
      nextAttemptAt: new Date(NOW.getTime() + 30_000),
    });
  });

  test.each(["missing", "mismatched", "throwing", "permanent refusal"])(
    "stops the connected deployment before a %s provider can prevent release",
    async (failure) => {
      const { state, store } = releasingState();
      let running = true;
      let cleanups = 0;
      const provider = testProvisioner({
        bindingFingerprint:
          failure === "mismatched" ? "other-binding" : "test:v1",
        destroy: async () => {
          expect(running).toBe(false);
          if (failure === "throwing") throw new Error("provider down");
          return {
            kind: "rejected",
            code: "permission_denied",
            message: "provider denied release",
            retryable: false,
          };
        },
      });
      const options = deps({ store, provisioner: provider });
      options.plugins.getProvisioner = () => {
        expect(running).toBe(false);
        return failure === "missing" ? null : provider;
      };
      options.router.undeployAllocation = async () => {
        cleanups += 1;
        running = false;
      };
      await createSidecarAllocationReconciler(options).reconcileNext();
      expect(running).toBe(false);
      expect(cleanups).toBe(1);
      expect(state.row.status).toBe(
        failure === "permanent refusal" ? "destroy_failed" : "releasing",
      );
    },
  );

  test("keeps confirmed cleanup when the release database write fails after the acknowledgement", async () => {
    let row = allocation({
      status: "releasing",
      generation: 2,
      ensureAcceptedGeneration: 1,
      sidecarId: "sc-shared",
    });
    let writes = 0;
    let confirmations = 0;
    let released = false;
    const store = fakeStore({
      claimNextReconcilable: async () => row,
      confirmDeploymentCleanup: async () => {
        row = { ...row, deploymentCleanupConfirmed: true };
        return true;
      },
      markReleased: async () => {
        writes += 1;
        if (writes === 1) throw new Error("database unavailable");
        released = true;
        return { ...row, status: "released" };
      },
    });
    const options = deps({
      store,
      provisioner: testProvisioner({
        destroy: async () => ({ kind: "destroyed", cleanup: "required" }),
      }),
    });
    options.router.undeployAllocation = async () => {
      confirmations += 1;
    };
    await createSidecarAllocationReconciler(options).reconcileNext();
    expect(released).toBe(false);
    expect(row.deploymentCleanupConfirmed).toBe(true);
    options.router.getCleanupConnection = () => undefined;
    await createSidecarAllocationReconciler(options).reconcileNext();
    expect(confirmations).toBe(1);
    expect(released).toBe(true);
  });

  test("still destroys the worker when connected cleanup fails", async () => {
    const { state, store } = releasingState();
    const calls: string[] = [];
    const options = deps({
      store,
      provisioner: testProvisioner({
        destroy: async () => {
          calls.push("destroy");
          return { kind: "destroyed", cleanup: "confirmed" };
        },
      }),
    });
    options.router.undeployAllocation = async () => {
      calls.push("cleanup");
      throw new Error("cleanup timed out");
    };
    await createSidecarAllocationReconciler(options).reconcileNext();
    expect(calls).toEqual(["cleanup", "destroy"]);
    expect(state.row.status).toBe("released");
  });

  test.each(["required", "throws"] as const)(
    "cleans a sidecar that appears during provider destruction when it %s",
    async (outcome) => {
      const { state, store } = releasingState();
      const reconnected = {};
      let connection: object | undefined;
      const calls: string[] = [];
      const options = deps({
        store,
        provisioner: testProvisioner({
          destroy: async () => {
            calls.push("destroy");
            connection = reconnected;
            if (outcome === "throws")
              throw new Error("provider failed after reconnect");
            return { kind: "destroyed", cleanup: "required" };
          },
        }),
      });
      options.router.getCleanupConnection = () => connection;
      options.router.undeployAllocation = async (
        _target,
        _timeout,
        _signal,
        expectedConnection,
      ) => {
        expect(expectedConnection).toBe(reconnected);
        calls.push("cleanup");
      };
      await createSidecarAllocationReconciler(options).reconcileNext();
      expect(calls).toEqual(["destroy", "cleanup"]);
      expect(state.row.status).toBe(
        outcome === "required" ? "released" : "releasing",
      );
    },
  );

  test("retries on a replacement connection during destroy without repeating the old connection", async () => {
    const { state, store } = releasingState();
    const original = {};
    const replacement = {};
    let connection = original;
    const attempted: object[] = [];
    const options = deps({
      store,
      provisioner: testProvisioner({
        destroy: async () => {
          connection = replacement;
          return { kind: "destroyed", cleanup: "required" };
        },
      }),
    });
    options.router.getCleanupConnection = () => connection;
    options.router.undeployAllocation = async (
      _target,
      _timeout,
      _signal,
      expectedConnection,
    ) => {
      attempted.push(expectedConnection);
      if (expectedConnection === original) throw new Error("connection lost");
    };
    await createSidecarAllocationReconciler(options).reconcileNext();
    expect(attempted).toEqual([original, replacement]);
    expect(state.row.status).toBe("released");
  });

  test("keeps shared capacity releasing until the sidecar confirms cleanup", async () => {
    const row = allocation({
      status: "releasing",
      generation: 2,
      ensureAcceptedGeneration: 1,
      sidecarId: "sc-shared",
    });
    const started = Promise.withResolvers<undefined>();
    const cleaned = Promise.withResolvers<undefined>();
    const calls: string[] = [];
    const store = fakeStore({
      claimNextReconcilable: async () => row,
      markReleased: async (args) => {
        expect(args).toMatchObject({
          allocationId: row.id,
          generation: 2,
          expectedLeaseId: "lease-1",
        });
        calls.push("released");
        return { ...row, status: "released" };
      },
    });
    const options = deps({
      store,
      provisioner: testProvisioner({
        destroy: async () => {
          calls.push("provider released hold");
          return { kind: "destroyed", cleanup: "required" };
        },
      }),
    });
    options.router.undeployAllocation = async (target, _timeout, signal) => {
      expect(target).toEqual({ allocationId: row.id, generation: 2 });
      expect(signal?.aborted).toBe(false);
      calls.push("requested cleanup");
      started.resolve(undefined);
      await cleaned.promise;
    };
    const running = createSidecarAllocationReconciler(options).reconcileNext();
    await started.promise;
    expect(calls).toEqual(["requested cleanup"]);
    cleaned.resolve(undefined);
    await running;
    expect(calls).toEqual([
      "requested cleanup",
      "provider released hold",
      "released",
    ]);
  });

  test.each(["reply lost", "cleanup failed"])(
    "retries a releasing allocation after %s and a Hub restart",
    async (failure) => {
      let row = allocation({
        status: "releasing",
        generation: 2,
        ensureAcceptedGeneration: 1,
        sidecarId: "sc-shared",
      });
      let released = false;
      let retry: Parameters<AllocationStore["scheduleRetry"]>[0] | undefined;
      const store = fakeStore({
        claimNextReconcilable: async () => row,
        listActive: async () => [row],
        scheduleRetry: async (args) => {
          retry = args;
          row = { ...row, nextAttemptAt: args.nextAttemptAt };
          return row;
        },
        markReleased: async () => {
          released = true;
          return { ...row, status: "released" };
        },
      });
      let destroys = 0;
      const provisioner = testProvisioner({
        destroy: async () => {
          destroys += 1;
          return { kind: "destroyed", cleanup: "required" };
        },
      });
      const first = deps({ store, provisioner });
      first.router.undeployAllocation = async () => {
        throw new Error(failure);
      };
      await createSidecarAllocationReconciler(first).reconcileNext();
      expect(released).toBe(false);
      expect(retry).toMatchObject({
        expectedStatus: "releasing",
        expectedGeneration: 2,
        nextAttemptAt: new Date(NOW.getTime() + 1_000),
      });

      expect(retry?.failure).toBeUndefined();
      let confirmations = 0;
      const restarted = deps({ store, provisioner });
      restarted.router.undeployAllocation = async () => {
        confirmations += 1;
      };
      const reconciler = createSidecarAllocationReconciler(restarted);
      await reconciler.initialize();
      await reconciler.reconcileNext();
      expect(destroys).toBe(2);
      expect(confirmations).toBe(1);
      expect(released).toBe(true);
    },
  );

  test("reconfirms cleanup when the release database write fails after the acknowledgement", async () => {
    const row = allocation({
      status: "releasing",
      generation: 2,
      ensureAcceptedGeneration: 1,
      sidecarId: "sc-shared",
    });
    let writes = 0;
    let confirmations = 0;
    let released = false;
    const store = fakeStore({
      claimNextReconcilable: async () => row,
      markReleased: async () => {
        writes += 1;
        if (writes === 1) throw new Error("database unavailable");
        released = true;
        return { ...row, status: "released" };
      },
    });
    const options = deps({
      store,
      provisioner: testProvisioner({
        destroy: async () => ({ kind: "destroyed", cleanup: "required" }),
      }),
    });
    options.router.undeployAllocation = async () => {
      confirmations += 1;
    };
    await createSidecarAllocationReconciler(options).reconcileNext();
    expect(released).toBe(false);
    await createSidecarAllocationReconciler(options).reconcileNext();
    expect(confirmations).toBe(2);
    expect(released).toBe(true);
  });

  test.each([
    { cleanup: "confirmed", accepted: true },
    { cleanup: "required", accepted: false },
  ] as const)(
    "does not wait for a nonexistent worker when cleanup is $cleanup and ensure acceptance is $accepted",
    async ({ cleanup, accepted }) => {
      const row = allocation({
        status: "releasing",
        generation: 2,
        sidecarId: "sc-old",
        ...(accepted ? { ensureAcceptedGeneration: 1 } : {}),
      });
      let released = false;
      const store = fakeStore({
        claimNextReconcilable: async () => row,
        markReleased: async () => {
          released = true;
          return { ...row, status: "released" };
        },
      });
      const options = deps({
        store,
        provisioner: testProvisioner({
          destroy: async () => ({ kind: "destroyed", cleanup }),
        }),
      });
      options.router.getCleanupConnection = () => undefined;
      options.router.undeployAllocation = async () => {
        throw new Error("must not need a cleanup connection");
      };
      await createSidecarAllocationReconciler(options).reconcileNext();
      expect(released).toBe(true);
    },
  );

  test("one allocation waiting for cleanup does not block another release", async () => {
    const first = allocation({
      status: "releasing",
      generation: 2,
      ensureAcceptedGeneration: 1,
      sidecarId: "sc-shared",
    });
    const second = allocation({
      ...first,
      id: "alloc-2",
      sidecarId: "sc-other",
    });
    const rows = [first, second];
    const released: string[] = [];
    const started = Promise.withResolvers<undefined>();
    const cleaned = Promise.withResolvers<undefined>();
    const store = fakeStore({
      claimNextReconcilable: async () => rows.shift() ?? null,
      markReleased: async ({ allocationId }) => {
        released.push(allocationId);
        return {
          ...(allocationId === first.id ? first : second),
          status: "released",
        };
      },
    });
    const options = deps({
      store,
      provisioner: testProvisioner({
        destroy: async ({ allocationId }) => ({
          kind: "destroyed",
          cleanup: allocationId === first.id ? "required" : "confirmed",
        }),
      }),
    });
    const connection = {};
    options.router.getCleanupConnection = (target) =>
      target.allocationId === first.id ? connection : undefined;
    options.router.undeployAllocation = async () => {
      started.resolve(undefined);
      await cleaned.promise;
    };
    const reconciler = createSidecarAllocationReconciler({
      ...options,
      maxConcurrentClaims: 2,
    });
    const waiting = reconciler.reconcileNext();
    await started.promise;
    await reconciler.reconcileNext();
    expect(released).toEqual([second.id]);
    cleaned.resolve(undefined);
    await waiting;
    expect(released).toEqual([second.id, first.id]);
  });

  test("releases an expired or terminal anchor before provisioning another worker", async () => {
    let claimed = false;
    let releaseRequested = false;
    const store = fakeStore({
      claimNextReconcilable: async () => {
        if (claimed) return null;
        claimed = true;
        return allocation();
      },
      hasRunnableAnchor: async () => false,
      beginRelease: async () => {
        releaseRequested = true;
        return allocation({ status: "releasing", generation: 1 });
      },
    });
    const reconciler = createSidecarAllocationReconciler(
      deps({
        store,
        provisioner: testProvisioner({
          ensure: async () => {
            throw new Error("must not provision an expired workflow");
          },
        }),
      }),
    );
    expect(await reconciler.reconcileNext()).toBe(true);
    expect(releaseRequested).toBe(true);
  });

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

  test("records the existing sidecar a provisioner placed the generation on", async () => {
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
      sidecarId: "sc-shared",
      ensureAcceptedGeneration: 1,
    });
    let claimed = false;
    const placements: (string | undefined)[] = [];
    const synced: string[] = [];
    const store = fakeStore({
      claimNextReconcilable: async () => {
        if (claimed) return null;
        claimed = true;
        return pending;
      },
      bindInitialSidecar: async () => provisioning,
      markAllocated: async (args) => {
        placements.push(args.sidecarId);
        return allocated;
      },
      markConnectionReady: async () => allocated,
    });
    const provisioner = testProvisioner({
      async ensure() {
        return { kind: "accepted", sidecarId: "sc-shared" };
      },
    });
    const reconciler = createSidecarAllocationReconciler(
      deps({ store, provisioner, synced, readyOnlyAfterSync: true }),
    );

    await reconciler.reconcileNext();

    expect(placements).toEqual(["sc-shared"]);
    expect(synced).toContain("sc-shared");
  });

  test("replaces a generation placed on a sidecar the Hub cannot reuse", async () => {
    const pending = allocation();
    const provisioning = allocation({
      status: "provisioning",
      generation: 1,
      sidecarId: "sc-new",
      connectDeadline: new Date(NOW.getTime() + 120_000),
      reconciliationLeaseId: "lease-1",
    });
    let claimed = false;
    const replacements: string[] = [];
    const store = fakeStore({
      claimNextReconcilable: async () => {
        if (claimed) return null;
        claimed = true;
        return pending;
      },
      bindInitialSidecar: async () => provisioning,
      markAllocated: async () => {
        throw new SidecarReuseRejectedError(
          "sc-foreign",
          "it hosts no current probe or allocation of provisioner test",
        );
      },
      beginReplacement: async (args) => {
        replacements.push(args.failureCode);
        return allocation({
          ...provisioning,
          status: "replacing",
          generation: 2,
        });
      },
    });
    const provisioner = testProvisioner({
      async ensure() {
        return { kind: "accepted", sidecarId: "sc-foreign" };
      },
    });
    const fences: [string, number][] = [];
    const reconciler = createSidecarAllocationReconciler(
      deps({ store, provisioner, fences }),
    );

    await reconciler.reconcileNext();

    expect(replacements).toEqual(["sidecar_reuse_rejected"]);
    expect(fences.at(-1)).toEqual(["alloc-1", 2]);
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
      const options = deps({ store, provisioner, retired });
      const cleanupBindings: boolean[] = [];
      options.router.fenceAllocation = (_id, _generation, flags) => {
        cleanupBindings.push(flags?.cleanup?.sidecarId === current.sidecarId);
      };
      const reconciler = createSidecarAllocationReconciler(options);

      await reconciler.reconcileNext();

      expect(cleanupBindings).toEqual([status === "releasing", true]);
      expect(calls).toEqual(["destroy", "fail"]);
      expect(failed).toEqual({
        allocationId: "alloc-1",
        expectedGeneration: 2,
        expectedLeaseId: "lease-1",
        code: "credentials_revoked",
        message: "Credentials no longer permit deleting this worker",
        now: NOW,
      });
      expect(retired).toEqual([]);
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
        return { kind: "destroyed", cleanup: "confirmed" };
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

  test("retries a first deploy that failed before its frame was sent, from its first failure", async () => {
    for (const firstDeployFailedAt of [
      undefined,
      new Date(NOW.getTime() - 30_000),
    ]) {
      const allocated = allocation({
        status: "allocated",
        generation: 1,
        sidecarId: "sc-current",
        ensureAcceptedGeneration: 1,
        reconciliationLeaseId: "lease-1",
        ...(firstDeployFailedAt !== undefined ? { firstDeployFailedAt } : {}),
      });
      let claimed = false;
      let scheduled:
        | Parameters<AllocationStore["scheduleRetry"]>[0]
        | undefined;
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
            throw new SidecarFirstDeployError(
              new Error("catalog temporarily unavailable"),
            );
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
        firstDeployFailedAt: firstDeployFailedAt ?? NOW,
        now: NOW,
      });
    }
  });

  test("fails a first deploy that keeps failing for a minute", async () => {
    const allocated = allocation({
      status: "allocated",
      generation: 1,
      sidecarId: "sc-current",
      ensureAcceptedGeneration: 1,
      reconciliationLeaseId: "lease-1",
      firstDeployFailedAt: new Date(NOW.getTime() - 60_000),
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
          throw new SidecarFirstDeployError(
            new Error("catalog temporarily unavailable"),
          );
        },
      }),
    );

    await reconciler.reconcileNext();

    expect(released).toMatchObject({
      allocationId: "alloc-1",
      expectedGeneration: 1,
      expectedLeaseId: "lease-1",
      failureCode: "sidecar_initialization_failed",
      failureMessage:
        "The first deploy kept failing for 60 seconds: catalog temporarily unavailable",
    });
    expect(fences).toEqual([
      ["alloc-1", 1],
      ["alloc-1", 2],
    ]);
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
      failureMessage: "deploy pack failed",
    });
    expect(fences).toEqual([
      ["alloc-1", 1],
      ["alloc-1", 2],
    ]);
  });

  test("releases a generation whose sidecar no longer holds its deployment", async () => {
    const allocated = allocation({
      status: "allocated",
      generation: 1,
      sidecarId: "sc-current",
      ensureAcceptedGeneration: 1,
      connectDeadline: NOW,
      reconciliationLeaseId: "lease-1",
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
        return allocation({ status: "releasing", generation: 2 });
      },
    });
    const reconciler = createSidecarAllocationReconciler(
      deps({
        store,
        fences,
        ready: true,
        onReady: async () => {
          throw new SidecarDeploymentMissingError("alloc-1", 1);
        },
      }),
    );

    await reconciler.reconcileNext();

    // A completed initialization is no reason to keep it: the sidecar's own
    // report says the deployment is gone.
    expect(released).toEqual({
      allocationId: "alloc-1",
      expectedGeneration: 1,
      expectedLeaseId: "lease-1",
      failureCode: "sidecar_deployment_missing",
      failureMessage:
        "The sidecar of allocation alloc-1 generation 1 no longer holds its deployment",
      now: NOW,
    });
    expect(fences).toEqual([
      ["alloc-1", 1],
      ["alloc-1", 2],
    ]);
  });

  test("fails a deployment its sidecar reports stopped and keeps the capacity", async () => {
    const allocated = allocation({
      status: "allocated",
      generation: 1,
      sidecarId: "sc-current",
      ensureAcceptedGeneration: 1,
      connectDeadline: NOW,
      reconciliationLeaseId: "lease-1",
    });
    let claimed = false;
    const calls: unknown[] = [];
    const fences: [string, number][] = [];
    const store = fakeStore({
      claimNextReconcilable: async () => {
        if (claimed) return null;
        claimed = true;
        return allocated;
      },
      failStoppedDeployment: async (args) => {
        calls.push(["fail", args]);
        return true;
      },
    });
    const reconciler = createSidecarAllocationReconciler(
      deps({
        store,
        fences,
        ready: true,
        onReady: async () => {
          throw new SidecarDeploymentStoppedError("The child ended itself");
        },
      }),
    );

    await reconciler.reconcileNext();

    // One store call fails it and settles it, keeping the reason: marking the
    // connection ready separately would clear it.
    expect(calls).toEqual([
      [
        "fail",
        {
          allocationId: "alloc-1",
          expectedGeneration: 1,
          expectedLeaseId: "lease-1",
          failureCode: "sidecar_deployment_stopped",
          failureMessage: "The child ended itself",
          now: NOW,
        },
      ],
    ]);
    // The generation is not fenced: its stopped copy stays for inspection.
    expect(fences).toEqual([["alloc-1", 1]]);
  });

  test("looks again while the Hub waits for a stopped deployment's history", async () => {
    const allocated = allocation({
      status: "allocated",
      generation: 1,
      sidecarId: "sc-current",
      ensureAcceptedGeneration: 1,
      connectDeadline: NOW,
      reconciliationLeaseId: "lease-1",
    });
    let claimed = false;
    const calls: unknown[] = [];
    const retryAt = new Date(NOW.getTime() + 1_000);
    const store = fakeStore({
      claimNextReconcilable: async () => {
        if (claimed) return null;
        claimed = true;
        return allocated;
      },
      scheduleRetry: async (args) => {
        calls.push(["retry", args]);
        return allocated;
      },
    });
    const reconciler = createSidecarAllocationReconciler(
      deps({
        store,
        ready: true,
        onReady: async () => {
          throw new SidecarDeploymentHistoryPendingError(
            "refs/heads/main is at b on the Hub and c on the worker",
            retryAt,
          );
        },
      }),
    );

    await reconciler.reconcileNext();

    expect(calls).toEqual([
      [
        "retry",
        {
          allocationId: "alloc-1",
          expectedStatus: "allocated",
          expectedGeneration: 1,
          expectedLeaseId: "lease-1",
          nextAttemptAt: retryAt,
          now: NOW,
        },
      ],
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

  test("syncs the sidecar's bindings only for a binding not yet attached", async () => {
    const allocated = allocation({
      status: "allocated",
      generation: 1,
      sidecarId: "sc-connected",
      ensureAcceptedGeneration: 1,
      connectDeadline: NOW,
      reconciliationLeaseId: "lease-1",
    });
    for (const { readyOnlyAfterSync, expected } of [
      // The sync queues behind the sidecar's other frames, so an attached
      // binding skips it.
      { readyOnlyAfterSync: false, expected: ["ready"] },
      {
        readyOnlyAfterSync: true,
        expected: ["sync:sc-connected", "ready"],
      },
    ]) {
      let claimed = false;
      const checks: string[] = [];
      const store = fakeStore({
        claimNextReconcilable: async () => {
          if (claimed) return null;
          claimed = true;
          return allocated;
        },
        markConnectionReady: async () => allocated,
      });
      const reconciler = createSidecarAllocationReconciler(
        deps({ store, ready: true, readyOnlyAfterSync, checks }),
      );

      await reconciler.reconcileNext();

      expect(checks).toEqual(expected);
    }
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

  test("fails a lost worker that reported its deployment stopped for the reason it gave", async () => {
    const allocated = allocation({
      status: "allocated",
      generation: 1,
      sidecarId: "sc-old",
      ensureAcceptedGeneration: 1,
      connectDeadline: NOW,
    });
    let failure:
      | Parameters<AllocationStore["beginUnrecoverableRelease"]>[0]
      | undefined;
    const store = fakeStore({
      claimNextReconcilable: async () => allocated,
      beginUnrecoverableRelease: async (args) => {
        failure = args;
        return allocation({
          status: "releasing",
          generation: 2,
          sidecarId: "sc-old",
        });
      },
    });
    const reconciler = createSidecarAllocationReconciler(
      deps({
        store,
        ready: false,
        waitError: new Error("connect timeout"),
        reportedStop: "Its workflow child ended itself (running): crash loop",
      }),
    );

    await reconciler.reconcileNext();

    expect(failure).toMatchObject({
      failureCode: "sidecar_deployment_stopped",
      failureMessage:
        "Its workflow child ended itself (running): crash loop; its sidecar disconnected, and history the Hub never received may be lost",
    });
  });

  test("fails a lost allocated worker", async () => {
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
      failureMessage: "connect timeout",
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
      allocation({
        id: "alloc-b",
        status: "allocated",
        generation: 4,
        connectDeadline: new Date(NOW.getTime() - 60_000),
      }),
    ];
    const wakes: [string, number][] = [];
    const reconnects: unknown[] = [];
    const fences: [string, number][] = [];
    const store = fakeStore({
      listActive: async () => active,
      wakeReconciliation: async (id, generation) => {
        wakes.push([id, generation]);
        return true;
      },
      scheduleReconnectAfterHubStart: async (args) => {
        reconnects.push(args);
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
    // Both windows count from Hub start; the store picks the one that applies.
    expect(reconnects).toEqual([
      {
        allocationId: "alloc-b",
        generation: 4,
        now: NOW,
        firstConnectDeadline: new Date(NOW.getTime() + 120_000),
      },
    ]);
  });

  test("durably starts the disconnect limit and wakes the exact generation on reconnect", async () => {
    const calls: string[] = [];
    const store = fakeStore({
      markConnectionLost: async (args) => {
        calls.push(
          `lost:${args.allocationId}:${String(args.generation)}:${String(args.now?.toISOString())}`,
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
      `lost:alloc-1:3:${NOW.toISOString()}`,
      "connected:alloc-1:3",
    ]);
  });

  test("repairs an unscheduled allocation after its disconnect write fails", async () => {
    const { nextAttemptAt: _nextAttemptAt, ...unscheduled } = allocation({
      status: "allocated",
      generation: 3,
      ensureAcceptedGeneration: 3,
    });
    const repairs: [string, number, Date | undefined][] = [];
    const store = fakeStore({
      listActive: async () => [unscheduled],
      markConnectionLost: async () => {
        throw new Error("database unavailable");
      },
      scheduleReconnectIfUnscheduled: async (args) => {
        repairs.push([args.allocationId, args.generation, args.now]);
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

    expect(repairs).toEqual([["alloc-1", 3, NOW]]);
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

  test("wakes a ready allocation whose reported stop the Hub has not recorded", async () => {
    for (const { failureCode, runnable, woken } of [
      { failureCode: undefined, runnable: true, woken: true },
      {
        failureCode: "sidecar_deployment_stopped",
        runnable: true,
        woken: false,
      },
      // A cancel that raced the report left nothing for a wake to fail.
      { failureCode: undefined, runnable: false, woken: false },
    ]) {
      const { nextAttemptAt: _nextAttemptAt, ...unscheduled } = allocation({
        status: "allocated",
        generation: 1,
        ensureAcceptedGeneration: 1,
        ...(failureCode !== undefined ? { failureCode } : {}),
      });
      const wakes: [string, number][] = [];
      const store = fakeStore({
        listActive: async () => [unscheduled],
        hasRunnableAnchor: async () => runnable,
        wakeReconciliation: async (id, generation) => {
          wakes.push([id, generation]);
          return true;
        },
      });
      const reconciler = createSidecarAllocationReconciler(
        deps({ store, ready: true, reportedStop: "The child ended itself" }),
      );

      await reconciler.repairUnscheduledConnections();

      expect(wakes).toEqual(woken ? [["alloc-1", 1]] : []);
    }
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

  for (const scenario of [
    { interruption: "lease loss", outcome: "committed" },
    { interruption: "renewal failure", outcome: "committed" },
    { interruption: "lease expiry", outcome: "committed" },
    { interruption: "operation deadline", outcome: "committed" },
    { interruption: "operation deadline", outcome: "unchanged" },
    { interruption: "operation deadline", outcome: "rejected" },
  ] as const) {
    test(`releases a cleanup slot on ${scenario.interruption} before its ${scenario.outcome} response`, async () => {
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
        status: "releasing",
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

  test(`a cancelled deploy with a late acknowledgement is fenced before retry`, async () => {
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
        status: "releasing",
        generation: current.generation + 1,
      };
      return current;
    };
    const store = fakeStore({
      claimNextReconcilable: async () => (claims++ < 2 ? current : null),
      extendReconciliationLease: async () => {
        throw new Error("transient renewal failure");
      },
      beginUnrecoverableRelease: transition,
    });
    const reconciler = createSidecarAllocationReconciler({
      ...deps({ store }),
      router,
      leaseDurationMs: 60,
      retryDelayMs: () => 1,
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
    router.handleMessage(ws, deployReply(ws, { publicKey: "c".repeat(64) }));
    await tick();
    expect(await router.isAllocatedWorkflowActive(TEST_TARGET)).toBe(false);
    await reconciler.reconcileNext();
    expect(initialized).toBe(1);
    expect(cleanedUp).toBe(true);
    expect(ws.closed).toBe(false);
    expect(router.getRoutableAddresses()).toEqual([]);
    expect(
      ws.sent.some((frame) => frame.includes('"type":"repo.pack.push"')),
    ).toBe(false);
    router.handleClose(ws);
  });

  test(`rechecks readiness after the observed initialization is rolled back`, async () => {
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
              return { kind: "destroyed", cleanup: "confirmed" };
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
      // initialization went: the follow-up fails a deployment a restarted
      // worker no longer holds and is a no-op when the worker is unchanged.
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

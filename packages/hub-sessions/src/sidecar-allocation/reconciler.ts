import { type } from "arktype";

import { sha256 } from "@intx/crypto";
import type { SidecarAllocation, SidecarAllocationStore } from "@intx/db";
import { getLogger } from "@intx/log";
import { hexEncode } from "@intx/types";

import type {
  AllocatedSidecarTarget,
  SidecarAllocationRouter,
} from "../ws/sidecar-handler";
import { SessionLaunchError } from "../session-service";
import {
  DestroySidecarResult,
  EnsureSidecarResult,
  type SidecarProvisioner,
} from "./contracts";
import type { SidecarPluginRegistry } from "./plugin-registry";
import {
  DEFAULT_SIDECAR_OPERATION_TIMEOUT_MS,
  runSidecarOperation,
  type SidecarReconciliationContext,
} from "./operation";

const logger = getLogger(["hub", "sidecar-allocation"]);

type AllocationStore = Pick<
  SidecarAllocationStore,
  | "beginReplacement"
  | "beginUnrecoverableRelease"
  | "bindInitialSidecar"
  | "bindReplacementSidecar"
  | "claimNextReconcilable"
  | "extendReconciliationLease"
  | "failWithoutInfrastructure"
  | "listActive"
  | "isReconciliationLeaseCurrent"
  | "markAllocated"
  | "markConnectionLost"
  | "markConnectionReady"
  | "markDestroyFailed"
  | "markReleased"
  | "parkReconciliation"
  | "scheduleReconnectIfUnscheduled"
  | "scheduleRetry"
  | "wakeReconciliation"
>;

export type SidecarAllocationReconcilerDeps = {
  readonly allocationStore: AllocationStore;
  readonly plugins: SidecarPluginRegistry;
  readonly router: Pick<
    SidecarAllocationRouter,
    | "fenceAllocation"
    | "isAllocatedSidecarReady"
    | "retireAllocation"
    | "waitForAllocatedSidecar"
  >;
  readonly hubWebSocketUrl: string;
  /** Idempotently restores and deploys one connected allocation generation. */
  readonly onReady?: (
    allocation: SidecarAllocation,
    reconciliation: SidecarReconciliationContext,
  ) => Promise<void>;
  /**
   * Replace an allocated worker after its reconnect grace expires. Disabled by
   * default because Hub recovery does not restore arbitrary sidecar or
   * isolation-container filesystem state, so automatic continuation could run
   * without state the previous worker produced.
   */
  readonly enableAutomaticReplacementRecovery?: boolean;
  readonly leaseDurationMs?: number;
  readonly connectTimeoutMs?: number;
  readonly operationTimeoutMs?: number;
  readonly retryDelayMs?: (attempt: number) => number;
  readonly now?: () => Date;
  readonly createSidecarId?: () => string;
  readonly createToken?: () => string;
  readonly createLeaseId?: () => string;
};

export type SidecarAllocationReconciler = {
  /** Rebuild all trust fences before accepting allocated connections. */
  initialize(): Promise<void>;
  /** Starts a durable reconnect grace period for the exact lost generation. */
  handleDisconnect(target: AllocatedSidecarTarget): Promise<void>;
  /** Wakes recovery as soon as the exact generation reconnects. */
  handleConnected(target: AllocatedSidecarTarget): Promise<void>;
  /** Repairs allocated generations left unscheduled after a lost event write. */
  repairUnscheduledConnections(): Promise<void>;
  /** Reconcile at most one due allocation. Returns false when none are due. */
  reconcileNext(): Promise<boolean>;
  /** Drain the currently due queue, bounded to catch accidental hot loops. */
  reconcileUntilIdle(maxIterations?: number): Promise<number>;
};

class ReconciliationLeaseLostError extends Error {
  constructor(allocationId: string, cause?: unknown) {
    super(`Reconciliation lease lost for allocation ${allocationId}`, {
      cause,
    });
    this.name = "ReconciliationLeaseLostError";
  }
}

const DEFAULT_LEASE_DURATION_MS = 60_000;
const DEFAULT_CONNECT_TIMEOUT_MS = 120_000;
const MAX_RETRY_BACKOFF_ATTEMPT = 5;

function randomHex(bytes: number): string {
  return hexEncode(crypto.getRandomValues(new Uint8Array(bytes)));
}

function defaultRetryDelay(attempt: number): number {
  return Math.min(
    1_000 * 2 ** Math.min(attempt, MAX_RETRY_BACKOFF_ATTEMPT),
    30_000,
  );
}

function parseEnsureResult(value: unknown): EnsureSidecarResult {
  const result = EnsureSidecarResult(value);
  if (result instanceof type.errors) {
    throw new Error(
      `sidecar provisioner returned an invalid ensure result: ${result.summary}`,
    );
  }
  return result;
}

function parseDestroyResult(value: unknown): DestroySidecarResult {
  const result = DestroySidecarResult(value);
  if (result instanceof type.errors) {
    throw new Error(
      `sidecar provisioner returned an invalid destroy result: ${result.summary}`,
    );
  }
  return result;
}

export function createSidecarAllocationReconciler({
  allocationStore,
  plugins,
  router,
  hubWebSocketUrl,
  onReady,
  enableAutomaticReplacementRecovery = false,
  leaseDurationMs = DEFAULT_LEASE_DURATION_MS,
  connectTimeoutMs = DEFAULT_CONNECT_TIMEOUT_MS,
  operationTimeoutMs = DEFAULT_SIDECAR_OPERATION_TIMEOUT_MS,
  retryDelayMs = defaultRetryDelay,
  now = () => new Date(),
  createSidecarId = () => `sc_${randomHex(16)}`,
  createToken = () => `intx_sc_${randomHex(32)}`,
  createLeaseId = () => `lease_${randomHex(16)}`,
}: SidecarAllocationReconcilerDeps): SidecarAllocationReconciler {
  if (leaseDurationMs <= 0) throw new Error("leaseDurationMs must be positive");
  if (connectTimeoutMs <= 0) {
    throw new Error("connectTimeoutMs must be positive");
  }
  if (!Number.isSafeInteger(operationTimeoutMs) || operationTimeoutMs <= 0) {
    throw new Error("operationTimeoutMs must be a positive integer");
  }

  const activeAllocations = new Map<
    string,
    {
      allocation: SidecarAllocation;
      controller: AbortController;
      leaseDeadline: number;
      pendingConnect: AllocatedSidecarTarget | null;
    }
  >();

  function trackAllocation(allocation: SidecarAllocation): void {
    const active = activeAllocations.get(allocation.id);
    if (active === undefined) return;
    if (active.allocation.generation !== allocation.generation)
      active.pendingConnect = null;
    active.allocation = allocation;
  }

  async function finishReconciliation(
    allocationId: string,
    apply: (pendingConnect: boolean) => Promise<unknown>,
  ): Promise<void> {
    const active = activeAllocations.get(allocationId);
    if (active === undefined)
      throw new ReconciliationLeaseLostError(allocationId);
    const target = { allocationId, generation: active.allocation.generation };
    await queueConnectionEvent(target, async () => {
      active.controller.signal.throwIfAborted();
      await apply(
        active.pendingConnect?.generation === target.generation &&
          active.allocation.status === "allocated",
      );
    });
  }

  function provisionerFor(
    allocation: SidecarAllocation,
  ): SidecarProvisioner | null {
    const provisioner = plugins.getProvisioner(allocation.provisionerId);
    if (
      provisioner === null ||
      provisioner.apiVersion !== allocation.provisionerApiVersion ||
      provisioner.bindingFingerprint !==
        allocation.provisionerBindingFingerprint
    ) {
      return null;
    }
    return provisioner;
  }

  function retryAt(attempt: number): Date {
    return new Date(now().getTime() + retryDelayMs(attempt));
  }

  async function withLeaseHeartbeat<T>(
    allocation: SidecarAllocation,
    leaseId: string,
    operationName: string,
    operation: (context: SidecarReconciliationContext) => Promise<T>,
    timeoutMs?: number,
  ): Promise<T> {
    trackAllocation(allocation);
    const active = activeAllocations.get(allocation.id);
    if (active === undefined)
      throw new ReconciliationLeaseLostError(allocation.id);
    const controller = active.controller;
    let finished = false;
    let expiryTimer: ReturnType<typeof setTimeout> | undefined;
    const checkLeaseExpiry = (): void => {
      if (finished || controller.signal.aborted) return;
      const remaining = active.leaseDeadline - performance.now();
      if (remaining <= 0) {
        controller.abort(new ReconciliationLeaseLostError(allocation.id));
      } else {
        expiryTimer = setTimeout(checkLeaseExpiry, Math.ceil(remaining));
      }
    };
    let renewing = false;
    const renew = async (): Promise<void> => {
      if (renewing || controller.signal.aborted) return;
      renewing = true;
      const startedAt = performance.now();
      try {
        const renewed = await allocationStore.extendReconciliationLease(
          allocation.id,
          leaseId,
          leaseDurationMs,
        );
        if (finished || controller.signal.aborted) return;
        if (!renewed || performance.now() >= active.leaseDeadline) {
          controller.abort(new ReconciliationLeaseLostError(allocation.id));
        } else {
          // The database grants the lease during the request. Counting from its
          // start avoids extending ownership by the response's transit time.
          active.leaseDeadline = startedAt + leaseDurationMs;
        }
      } catch (error) {
        if (!finished) {
          controller.abort(
            new ReconciliationLeaseLostError(allocation.id, error),
          );
        }
      } finally {
        renewing = false;
      }
    };
    const interval = setInterval(
      () => {
        void renew();
      },
      Math.max(1, Math.floor(leaseDurationMs / 3)),
    );
    checkLeaseExpiry();
    try {
      return await runSidecarOperation(
        operationName,
        timeoutMs,
        async (signal) => {
          if (
            !(await runSidecarOperation(
              "Reconciliation lease validation",
              operationTimeoutMs,
              () =>
                allocationStore.isReconciliationLeaseCurrent(
                  allocation.id,
                  allocation.generation,
                  leaseId,
                ),
              signal,
            ))
          ) {
            throw new ReconciliationLeaseLostError(allocation.id);
          }
          if (performance.now() >= active.leaseDeadline) {
            controller.abort(new ReconciliationLeaseLostError(allocation.id));
          }
          signal.throwIfAborted();
          return operation({ signal, leaseId });
        },
        controller.signal,
      );
    } catch (error) {
      controller.signal.throwIfAborted();
      throw error;
    } finally {
      finished = true;
      clearInterval(interval);
      clearTimeout(expiryTimer);
    }
  }

  async function replaceAfterFailure(
    allocation: SidecarAllocation,
    leaseId: string,
    code: string,
    message: string,
  ): Promise<void> {
    if (
      allocation.status === "allocated" &&
      !enableAutomaticReplacementRecovery
    ) {
      const releasing = await allocationStore.beginUnrecoverableRelease({
        allocationId: allocation.id,
        expectedGeneration: allocation.generation,
        expectedLeaseId: leaseId,
        failureCode: code,
        failureMessage: `Automatic recovery is disabled: ${message}`,
        now: now(),
      });
      if (releasing !== null) {
        router.fenceAllocation(releasing.id, releasing.generation);
      }
      return;
    }
    const replaced = await allocationStore.beginReplacement({
      allocationId: allocation.id,
      expectedStatus:
        allocation.status === "allocated" ? "allocated" : "provisioning",
      expectedGeneration: allocation.generation,
      expectedLeaseId: leaseId,
      nextAttemptAt: retryAt(
        allocation.ensureAttempts + allocation.destroyAttempts,
      ),
      failureCode: code,
      failureMessage: message,
      now: now(),
    });
    if (replaced !== null) {
      router.fenceAllocation(replaced.id, replaced.generation);
    }
  }

  async function waitUntilReady(
    allocation: SidecarAllocation,
    leaseId: string,
    connectionAlreadyReady = false,
  ): Promise<void> {
    const target = {
      allocationId: allocation.id,
      generation: allocation.generation,
    };
    const deadline = allocation.connectDeadline;
    const remaining =
      deadline === undefined ? 0 : deadline.getTime() - now().getTime();
    try {
      if (
        !connectionAlreadyReady &&
        !(await router.isAllocatedSidecarReady(target))
      ) {
        await withLeaseHeartbeat(
          allocation,
          leaseId,
          "Sidecar connection",
          () => router.waitForAllocatedSidecar(target, Math.max(0, remaining)),
          operationTimeoutMs,
        );
      }
    } catch (error) {
      if (error instanceof ReconciliationLeaseLostError) throw error;
      await replaceAfterFailure(
        allocation,
        leaseId,
        "sidecar_connect_failed",
        error instanceof Error ? error.message : String(error),
      );
      return;
    }

    await queueConnectionEvent(target, async () => {
      const active = activeAllocations.get(allocation.id);
      if (active?.allocation.generation !== allocation.generation)
        throw new ReconciliationLeaseLostError(allocation.id);
      active.controller.signal.throwIfAborted();
      active.pendingConnect = null;
    });

    if (onReady !== undefined) {
      try {
        await withLeaseHeartbeat(
          allocation,
          leaseId,
          "Workflow initialization",
          (context) => onReady(allocation, context),
        );
      } catch (error) {
        if (error instanceof ReconciliationLeaseLostError) throw error;
        if (error instanceof SessionLaunchError && error.leakedAgent) {
          await replaceAfterFailure(
            allocation,
            leaseId,
            "sidecar_initialization_uncertain",
            error.message,
          );
          return;
        }
        await finishReconciliation(allocation.id, () =>
          allocationStore.scheduleRetry({
            allocationId: allocation.id,
            expectedStatus: "allocated",
            expectedGeneration: allocation.generation,
            // Initialization has no separate durable attempt counter. Use the
            // capped delay so a persistent launch error cannot create a hot loop.
            nextAttemptAt: retryAt(MAX_RETRY_BACKOFF_ATTEMPT),
            expectedLeaseId: leaseId,
            failure: {
              code: "sidecar_initialization_failed",
              message: error instanceof Error ? error.message : String(error),
            },
            now: now(),
          }),
        );
        return;
      }
    }

    await finishReconciliation(allocation.id, (pendingConnect) =>
      pendingConnect
        ? allocationStore.scheduleRetry({
            allocationId: allocation.id,
            expectedStatus: "allocated",
            expectedGeneration: allocation.generation,
            expectedLeaseId: leaseId,
            nextAttemptAt: now(),
            now: now(),
          })
        : allocationStore.markConnectionReady({
            allocationId: allocation.id,
            generation: allocation.generation,
            expectedLeaseId: leaseId,
            now: now(),
          }),
    );
  }

  async function acceptEnsure(
    allocation: SidecarAllocation,
    leaseId: string,
    provisioner: SidecarProvisioner,
    token: string,
  ): Promise<void> {
    if (allocation.sidecarId === undefined) {
      throw new Error(`Allocation ${allocation.id} has no sidecar identity`);
    }
    const sidecarId = allocation.sidecarId;
    let result: EnsureSidecarResult;
    try {
      result = parseEnsureResult(
        await withLeaseHeartbeat(
          allocation,
          leaseId,
          "Sidecar ensure",
          ({ signal }) =>
            provisioner.ensure({
              signal,
              allocationId: allocation.id,
              generation: allocation.generation,
              tenantId: allocation.tenantId,
              anchorRunId: allocation.anchorRunId,
              sidecarId,
              token,
              hubWebSocketUrl,
            }),
          operationTimeoutMs,
        ),
      );
    } catch (error) {
      if (error instanceof ReconciliationLeaseLostError) throw error;
      await replaceAfterFailure(
        allocation,
        leaseId,
        "ensure_failed",
        error instanceof Error ? error.message : String(error),
      );
      return;
    }
    if (result.kind === "rejected") {
      if (!result.retryable) {
        const failed = await allocationStore.failWithoutInfrastructure({
          allocationId: allocation.id,
          expectedStatus: "provisioning",
          expectedGeneration: allocation.generation,
          code: result.code,
          message: result.message,
          expectedLeaseId: leaseId,
          now: now(),
        });
        if (failed !== null) {
          router.retireAllocation({
            allocationId: failed.id,
            generation: failed.generation,
          });
        }
        return;
      }
      await replaceAfterFailure(
        allocation,
        leaseId,
        result.code,
        result.message,
      );
      return;
    }

    const allocated = await allocationStore.markAllocated({
      allocationId: allocation.id,
      generation: allocation.generation,
      ...(result.externalRef !== undefined
        ? { externalRef: result.externalRef }
        : {}),
      expectedLeaseId: leaseId,
      now: now(),
    });
    if (allocated !== null) {
      trackAllocation(allocated);
      const target = {
        allocationId: allocated.id,
        generation: allocated.generation,
      };
      if (await router.isAllocatedSidecarReady(target)) {
        await waitUntilReady(allocated, leaseId, true);
        return;
      }
      // Provisioning acceptance and websocket readiness are separate durable
      // transitions. Do not hold the single reconciliation loop for the full
      // connection timeout: park this lease at its persisted deadline and let
      // sidecar.allocated.connected wake it immediately when the worker arrives.
      await finishReconciliation(allocated.id, (pendingConnect) =>
        pendingConnect
          ? allocationStore.scheduleRetry({
              allocationId: allocated.id,
              expectedStatus: "allocated",
              expectedGeneration: allocated.generation,
              expectedLeaseId: leaseId,
              nextAttemptAt: now(),
              now: now(),
            })
          : allocationStore.parkReconciliation(allocated.id, leaseId, {
              kind: "await-connection",
              fallbackNextAttemptAt: retryAt(MAX_RETRY_BACKOFF_ATTEMPT),
            }),
      );
    }
  }

  async function bindAndEnsure(
    allocation: SidecarAllocation,
    leaseId: string,
    provisioner: SidecarProvisioner,
    replacement: boolean,
  ): Promise<void> {
    const token = createToken();
    const sidecarId = createSidecarId();
    const connectDeadline = new Date(now().getTime() + connectTimeoutMs);
    const bound = replacement
      ? await allocationStore.bindReplacementSidecar({
          allocationId: allocation.id,
          generation: allocation.generation,
          sidecarId,
          tokenHashSha256: await sha256(token),
          connectDeadline,
          expectedLeaseId: leaseId,
          now: now(),
        })
      : await allocationStore.bindInitialSidecar({
          allocationId: allocation.id,
          expectedGeneration: allocation.generation,
          sidecarId,
          tokenHashSha256: await sha256(token),
          connectDeadline,
          expectedLeaseId: leaseId,
          now: now(),
        });
    if (bound === null) return;

    trackAllocation(bound);
    router.fenceAllocation(bound.id, bound.generation);
    await acceptEnsure(bound, leaseId, provisioner, token);
  }

  async function retryDestroy(
    allocation: SidecarAllocation,
    leaseId: string,
  ): Promise<void> {
    if (
      allocation.status !== "replacing" &&
      allocation.status !== "releasing"
    ) {
      throw new Error(
        `Cannot retry destroy while allocation ${allocation.id} is ${allocation.status}`,
      );
    }
    const status = allocation.status;
    await finishReconciliation(allocation.id, () =>
      allocationStore.scheduleRetry({
        allocationId: allocation.id,
        expectedStatus: status,
        expectedGeneration: allocation.generation,
        nextAttemptAt: retryAt(allocation.destroyAttempts),
        expectedLeaseId: leaseId,
        attempt: "destroy",
        now: now(),
      }),
    );
  }

  async function destroyCurrent(
    allocation: SidecarAllocation,
    leaseId: string,
    provisioner: SidecarProvisioner,
  ): Promise<boolean> {
    if (allocation.sidecarId === undefined) return true;
    const sidecarId = allocation.sidecarId;
    let result: DestroySidecarResult;
    try {
      result = parseDestroyResult(
        await withLeaseHeartbeat(
          allocation,
          leaseId,
          "Sidecar destroy",
          ({ signal }) =>
            provisioner.destroy({
              signal,
              allocationId: allocation.id,
              generation: allocation.generation,
              sidecarId,
              ...(allocation.externalRef !== undefined
                ? { externalRef: allocation.externalRef }
                : {}),
            }),
          operationTimeoutMs,
        ),
      );
    } catch (error) {
      if (error instanceof ReconciliationLeaseLostError) throw error;
      logger.warn`Destroy failed for allocation ${allocation.id}: ${error instanceof Error ? error.message : String(error)}`;
      await retryDestroy(allocation, leaseId);
      return false;
    }
    if (result.kind === "destroyed") return true;
    if (!result.retryable) {
      const failed = await allocationStore.markDestroyFailed({
        allocationId: allocation.id,
        expectedGeneration: allocation.generation,
        expectedLeaseId: leaseId,
        code: result.code,
        message: result.message,
        now: now(),
      });
      if (failed !== null) {
        router.retireAllocation({
          allocationId: failed.id,
          generation: failed.generation,
        });
      }
      return false;
    }
    await retryDestroy(allocation, leaseId);
    return false;
  }

  async function reconcile(
    allocation: SidecarAllocation,
    leaseId: string,
  ): Promise<void> {
    router.fenceAllocation(allocation.id, allocation.generation);
    if (
      allocation.status === "released" ||
      allocation.status === "failed" ||
      allocation.status === "destroy_failed"
    ) {
      return;
    }
    const provisioner = provisionerFor(allocation);
    if (provisioner === null) {
      if (allocation.status === "pending") {
        const failed = await allocationStore.failWithoutInfrastructure({
          allocationId: allocation.id,
          expectedStatus: "pending",
          expectedGeneration: allocation.generation,
          code: "provisioner_unavailable",
          message: `Provisioner ${allocation.provisionerId} is unavailable or its binding changed`,
          expectedLeaseId: leaseId,
          now: now(),
        });
        if (failed !== null) {
          router.retireAllocation({
            allocationId: failed.id,
            generation: failed.generation,
          });
        }
      } else {
        const status = allocation.status;
        await finishReconciliation(allocation.id, () =>
          allocationStore.scheduleRetry({
            allocationId: allocation.id,
            expectedStatus: status,
            expectedGeneration: allocation.generation,
            nextAttemptAt: retryAt(
              allocation.ensureAttempts + allocation.destroyAttempts,
            ),
            expectedLeaseId: leaseId,
            now: now(),
          }),
        );
      }
      return;
    }

    switch (allocation.status) {
      case "pending":
        await bindAndEnsure(allocation, leaseId, provisioner, false);
        return;
      case "provisioning":
        // The raw bearer token is deliberately not durable. Re-entering this
        // state means the process died before ensure acceptance was recorded.
        await replaceAfterFailure(
          allocation,
          leaseId,
          "ensure_outcome_unknown",
          "Hub restarted before sidecar provisioning acceptance was recorded",
        );
        return;
      case "allocated":
        await waitUntilReady(allocation, leaseId);
        return;
      case "replacing":
        if (!(await destroyCurrent(allocation, leaseId, provisioner))) return;
        await bindAndEnsure(allocation, leaseId, provisioner, true);
        return;
      case "releasing": {
        if (!(await destroyCurrent(allocation, leaseId, provisioner))) return;
        const released = await allocationStore.markReleased({
          allocationId: allocation.id,
          generation: allocation.generation,
          expectedLeaseId: leaseId,
          now: now(),
        });
        if (released !== null) {
          router.retireAllocation({
            allocationId: released.id,
            generation: released.generation,
          });
        }
        return;
      }
      default: {
        const exhaustive: never = allocation.status;
        throw new Error(
          `Allocation ${allocation.id} has unhandled status ${String(exhaustive)}`,
        );
      }
    }
  }

  async function initialize(): Promise<void> {
    for (const allocation of await allocationStore.listActive()) {
      router.fenceAllocation(allocation.id, allocation.generation);
      if (allocation.status === "allocated") {
        await allocationStore.markConnectionLost({
          allocationId: allocation.id,
          generation: allocation.generation,
          connectDeadline:
            allocation.connectDeadline ??
            new Date(now().getTime() + connectTimeoutMs),
          now: now(),
        });
      } else if (allocation.nextAttemptAt === undefined) {
        // Scheduled retries are durable state. Only repair an unscheduled
        // active row; moving an existing deadline earlier would erase provider
        // backoff whenever the Hub restarts.
        await allocationStore.wakeReconciliation(
          allocation.id,
          allocation.generation,
        );
      }
    }
  }

  const connectionEvents = new Map<string, Promise<void>>();

  function queueConnectionEvent(
    target: AllocatedSidecarTarget,
    apply: () => Promise<void>,
  ): Promise<void> {
    const previous =
      connectionEvents.get(target.allocationId) ?? Promise.resolve();
    const pending = previous.catch(() => undefined).then(apply);
    connectionEvents.set(target.allocationId, pending);
    const settled = () => {
      if (connectionEvents.get(target.allocationId) === pending)
        connectionEvents.delete(target.allocationId);
    };
    void pending.then(settled, settled);
    return pending;
  }

  function noteDisconnect(
    target: AllocatedSidecarTarget,
    leaseInvalidated = false,
  ): void {
    const active = activeAllocations.get(target.allocationId);
    if (active?.allocation.generation !== target.generation) return;
    active.pendingConnect = null;
    if (leaseInvalidated || active.allocation.status === "allocated") {
      active.controller.abort(
        new ReconciliationLeaseLostError(target.allocationId),
      );
    }
  }

  function noteConnect(target: AllocatedSidecarTarget): void {
    const active = activeAllocations.get(target.allocationId);
    if (active?.allocation.generation === target.generation)
      active.pendingConnect = target;
  }

  function handleDisconnect(target: AllocatedSidecarTarget): Promise<void> {
    noteDisconnect(target);
    return queueConnectionEvent(target, async () => {
      const disconnected = await allocationStore.markConnectionLost({
        allocationId: target.allocationId,
        generation: target.generation,
        connectDeadline: new Date(now().getTime() + connectTimeoutMs),
        now: now(),
      });
      noteDisconnect(target, disconnected !== null);
    });
  }

  function handleConnected(target: AllocatedSidecarTarget): Promise<void> {
    noteConnect(target);
    return queueConnectionEvent(target, async () => {
      await allocationStore.wakeReconciliation(
        target.allocationId,
        target.generation,
      );
      noteConnect(target);
    });
  }

  async function repairUnscheduledConnections(): Promise<void> {
    for (const allocation of await allocationStore.listActive()) {
      if (
        allocation.status !== "allocated" ||
        allocation.nextAttemptAt !== undefined ||
        allocation.reconciliationLeaseId !== undefined ||
        allocation.reconciliationLeaseExpiresAt !== undefined
      ) {
        continue;
      }
      const target = {
        allocationId: allocation.id,
        generation: allocation.generation,
      };
      if (await router.isAllocatedSidecarReady(target)) continue;
      try {
        await allocationStore.scheduleReconnectIfUnscheduled({
          ...target,
          connectDeadline:
            allocation.connectDeadline ??
            new Date(now().getTime() + connectTimeoutMs),
          now: now(),
        });
      } catch (error) {
        logger.warn`Failed to repair allocation ${allocation.id} reconnect schedule: ${error instanceof Error ? error.message : String(error)}`;
      }
    }
  }

  async function reconcileNext(): Promise<boolean> {
    const leaseId = createLeaseId();
    const claimStartedAt = performance.now();
    const allocation = await runSidecarOperation(
      "Sidecar allocation claim",
      operationTimeoutMs,
      () =>
        allocationStore.claimNextReconcilable({
          leaseId,
          leaseDurationMs,
          excludedAllocationIds: [...activeAllocations.keys()],
        }),
    );
    if (allocation === null) return false;
    // A claim started before another local claim returned can outlive its lease.
    if (activeAllocations.has(allocation.id)) {
      await allocationStore.parkReconciliation(allocation.id, leaseId, {
        kind: "retry-after-error",
        notBefore: retryAt(MAX_RETRY_BACKOFF_ATTEMPT),
      });
      return true;
    }
    activeAllocations.set(allocation.id, {
      allocation,
      controller: new AbortController(),
      leaseDeadline: claimStartedAt + leaseDurationMs,
      pendingConnect: null,
    });
    try {
      await reconcile(allocation, leaseId);
    } catch (error) {
      if (error instanceof ReconciliationLeaseLostError) {
        const cause = error.cause;
        if (cause === undefined) {
          logger.info`Allocation ${allocation.id} reconciliation stopped: lease ${leaseId} is no longer current`;
        } else {
          logger.warn`Allocation ${allocation.id} reconciliation stopped because renewal of lease ${leaseId} failed: ${cause instanceof Error ? cause.message : String(cause)}`;
          try {
            await allocationStore.parkReconciliation(allocation.id, leaseId, {
              kind: "retry-after-error",
              notBefore: retryAt(MAX_RETRY_BACKOFF_ATTEMPT),
            });
          } catch (parkError) {
            logger.warn`Failed to park allocation ${allocation.id} after renewal failure: ${parkError instanceof Error ? parkError.message : String(parkError)}`;
          }
        }
        return true;
      }
      logger.error`Allocation ${allocation.id} reconciliation failed: ${error instanceof Error ? error.message : String(error)}`;
      await finishReconciliation(allocation.id, () =>
        allocationStore.parkReconciliation(allocation.id, leaseId, {
          kind: "retry-after-error",
          notBefore: retryAt(MAX_RETRY_BACKOFF_ATTEMPT),
        }),
      );
    } finally {
      activeAllocations.delete(allocation.id);
    }
    return true;
  }

  async function reconcileUntilIdle(maxIterations = 100): Promise<number> {
    let reconciled = 0;
    while (reconciled < maxIterations && (await reconcileNext())) {
      reconciled += 1;
    }
    return reconciled;
  }

  return {
    initialize,
    handleDisconnect,
    handleConnected,
    repairUnscheduledConnections,
    reconcileNext,
    reconcileUntilIdle,
  };
}

import { type } from "arktype";

import { sha256 } from "@intx/crypto";
import type { SidecarAllocation, SidecarAllocationStore } from "@intx/db";
import { getLogger } from "@intx/log";
import { hexEncode } from "@intx/types";

import type {
  AllocatedSidecarTarget,
  SidecarAllocationRouter,
} from "../ws/sidecar-handler";
import { SidecarIdentityValidationError } from "../ws/sidecar-handler";
import { SessionLaunchError } from "../session-service";
import { DEFAULT_SIDECAR_ALLOCATION_CONCURRENCY } from "../reconciliation-scheduler";
import {
  DestroySidecarResult,
  EnsureSidecarResult,
  type SidecarProvisioner,
} from "./contracts";
import type { SidecarPluginRegistry } from "./plugin-registry";
import {
  DEFAULT_SIDECAR_OPERATION_TIMEOUT_MS,
  runSidecarOperation,
  SidecarOperationTimeoutError,
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
  /**
   * Resolve the previous attempt's deferred mail under the newly claimed
   * lease, before connection waits or cleanup.
   */
  readonly onInitializationRecovery?: (
    allocation: SidecarAllocation,
    reconciliation: SidecarReconciliationContext,
  ) => Promise<void>;
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
  /**
   * Bounds admitted claims, active reconciliation, and retained allocation
   * queries or writes.
   */
  readonly maxConcurrentClaims?: number;
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
  onInitializationRecovery,
  onReady,
  enableAutomaticReplacementRecovery = false,
  leaseDurationMs = DEFAULT_LEASE_DURATION_MS,
  connectTimeoutMs = DEFAULT_CONNECT_TIMEOUT_MS,
  operationTimeoutMs = DEFAULT_SIDECAR_OPERATION_TIMEOUT_MS,
  maxConcurrentClaims = DEFAULT_SIDECAR_ALLOCATION_CONCURRENCY,
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
  if (!Number.isSafeInteger(maxConcurrentClaims) || maxConcurrentClaims <= 0) {
    throw new Error("maxConcurrentClaims must be a positive integer");
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
    await queueReconciliationStep(target, async () => {
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

  async function withReconciliationLease<T>(
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
    try {
      return await runSidecarOperation(
        operationName,
        timeoutMs,
        async (signal) => {
          if (
            !(await runSidecarOperation(
              "Reconciliation lease validation",
              operationTimeoutMs,
              async () => {
                try {
                  return await trackAllocationQuery(allocation.id, () =>
                    allocationStore.isReconciliationLeaseCurrent(
                      allocation.id,
                      allocation.generation,
                      leaseId,
                    ),
                  );
                } catch (cause) {
                  throw new ReconciliationLeaseLostError(allocation.id, cause);
                }
              },
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
    }
  }

  // A timed-out lookup still occupies its database connection. Exclude its
  // allocation until it settles so retries cannot accumulate duplicate reads.
  const pendingAllocationQueries = new Map<string, { count: number }>();

  async function trackAllocationQuery<T>(
    allocationId: string,
    query: () => Promise<T>,
  ): Promise<T> {
    const pending = pendingAllocationQueries.get(allocationId) ?? { count: 0 };
    pending.count += 1;
    pendingAllocationQueries.set(allocationId, pending);
    try {
      return await query();
    } finally {
      pending.count -= 1;
      if (pending.count === 0) pendingAllocationQueries.delete(allocationId);
    }
  }

  async function isSidecarReady(
    allocation: SidecarAllocation,
  ): Promise<boolean> {
    const active = activeAllocations.get(allocation.id);
    if (active === undefined)
      throw new ReconciliationLeaseLostError(allocation.id);
    return runSidecarOperation(
      "Sidecar readiness",
      operationTimeoutMs,
      () =>
        trackAllocationQuery(allocation.id, () =>
          router.isAllocatedSidecarReady({
            allocationId: allocation.id,
            generation: allocation.generation,
          }),
        ),
      active.controller.signal,
    );
  }

  async function replaceAfterFailure(
    allocation: SidecarAllocation,
    leaseId: string,
    code: string,
    message: string,
    {
      onlyIfInitializationIncomplete = false,
    }: { onlyIfInitializationIncomplete?: boolean } = {},
  ): Promise<void> {
    const initializationCheck = onlyIfInitializationIncomplete
      ? {
          onlyIfInitializationIncomplete: true,
          ...(allocation.initializationLeaseId !== undefined
            ? {
                expectedInitializationLeaseId: allocation.initializationLeaseId,
              }
            : {}),
        }
      : {};
    let shouldRetryInitialization = false;
    await queueReconciliationStep(
      { allocationId: allocation.id, generation: allocation.generation },
      async () => {
        const updated =
          allocation.status === "allocated" &&
          !enableAutomaticReplacementRecovery
            ? await allocationStore.beginUnrecoverableRelease({
                ...initializationCheck,
                allocationId: allocation.id,
                expectedGeneration: allocation.generation,
                expectedLeaseId: leaseId,
                failureCode: code,
                failureMessage: `Automatic recovery is disabled: ${message}`,
                now: now(),
              })
            : await allocationStore.beginReplacement({
                ...initializationCheck,
                allocationId: allocation.id,
                expectedStatus:
                  allocation.status === "allocated"
                    ? "allocated"
                    : "provisioning",
                expectedGeneration: allocation.generation,
                expectedLeaseId: leaseId,
                nextAttemptAt: retryAt(
                  allocation.ensureAttempts + allocation.destroyAttempts,
                ),
                failureCode: code,
                failureMessage: message,
                now: now(),
              });
        if (updated !== null) {
          // A late commit still advances the fence before this queued work
          // settles and its allocation becomes eligible for another claim.
          router.fenceAllocation(updated.id, updated.generation);
        } else {
          shouldRetryInitialization = onlyIfInitializationIncomplete;
        }
      },
    );
    if (shouldRetryInitialization)
      await retryInitialization(allocation, leaseId);
  }

  async function retryInitialization(
    allocation: SidecarAllocation,
    leaseId: string,
  ): Promise<void> {
    // A publication or unsent rollback may have committed since the claim.
    // Retry the ordinary callback, including initialization and dispatch requeue.
    // If ownership was lost instead, this lease-guarded update changes nothing.
    await finishReconciliation(allocation.id, () =>
      allocationStore.scheduleRetry({
        allocationId: allocation.id,
        expectedStatus: "allocated",
        expectedGeneration: allocation.generation,
        expectedLeaseId: leaseId,
        nextAttemptAt: now(),
        now: now(),
      }),
    );
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
    // A stalled identity query is not evidence that the worker missed its
    // connection deadline. Let it retry without releasing the generation.
    const connectionReady =
      connectionAlreadyReady || (await isSidecarReady(allocation));
    try {
      if (!connectionReady) {
        await withReconciliationLease(
          allocation,
          leaseId,
          "Sidecar connection",
          () =>
            trackAllocationQuery(allocation.id, () =>
              router.waitForAllocatedSidecar(
                target,
                Math.max(0, remaining),
                (validation) => {
                  // The waiter can expire before a notification lookup settles.
                  // Retain its exclusion and capacity independently of the wait.
                  void trackAllocationQuery(
                    allocation.id,
                    () => validation,
                  ).catch(() => undefined);
                },
              ),
            ),
          operationTimeoutMs,
        );
      }
    } catch (error) {
      // Let the router report connection expiry. Our outer deadline can expire
      // during lease or identity validation without establishing worker loss.
      // A failed identity lookup is likewise inconclusive: the worker may be
      // healthy behind it, so retry instead of releasing the generation.
      if (
        error instanceof ReconciliationLeaseLostError ||
        error instanceof SidecarOperationTimeoutError ||
        error instanceof SidecarIdentityValidationError
      )
        throw error;
      await replaceAfterFailure(
        allocation,
        leaseId,
        "sidecar_connect_failed",
        error instanceof Error ? error.message : String(error),
      );
      return;
    }

    await queueReconciliationStep(target, async () => {
      const active = activeAllocations.get(allocation.id);
      if (active?.allocation.generation !== allocation.generation)
        throw new ReconciliationLeaseLostError(allocation.id);
      active.controller.signal.throwIfAborted();
      active.pendingConnect = null;
    });

    if (onReady !== undefined) {
      try {
        await withReconciliationLease(
          allocation,
          leaseId,
          "Workflow initialization",
          (context) =>
            trackAllocationQuery(allocation.id, () =>
              onReady(allocation, context),
            ),
        );
      } catch (error) {
        if (error instanceof ReconciliationLeaseLostError) throw error;
        if (error instanceof SessionLaunchError && error.leakedAgent) {
          await replaceAfterFailure(
            allocation,
            leaseId,
            "sidecar_initialization_uncertain",
            error.message,
            { onlyIfInitializationIncomplete: true },
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

    // A connect that lands during initialization schedules an immediate
    // follow-up even on success: the new socket may be a restarted worker
    // with an empty inventory (takeover suppresses the disconnect event), in
    // which case the follow-up redeploys and restores it. When the worker is
    // unchanged the follow-up is a no-op: deployReadyAllocation returns early
    // once the workflow is active and its key is recorded.
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
        await withReconciliationLease(
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
      if (await isSidecarReady(allocated)) {
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
        await withReconciliationLease(
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
    if (
      allocation.status === "allocated" &&
      onInitializationRecovery !== undefined
    ) {
      await withReconciliationLease(
        allocation,
        leaseId,
        "Sender deployment recovery",
        (context) =>
          trackAllocationQuery(allocation.id, () =>
            onInitializationRecovery(allocation, context),
          ),
        operationTimeoutMs,
      );
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
        if (allocation.initializationLeaseId !== undefined) {
          await replaceAfterFailure(
            allocation,
            leaseId,
            "sidecar_initialization_uncertain",
            "A previous initialization attempt did not record completion",
            { onlyIfInitializationIncomplete: true },
          );
          return;
        }
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

  async function queueReconciliationStep(
    target: AllocatedSidecarTarget,
    apply: () => Promise<void>,
  ): Promise<void> {
    const active = activeAllocations.get(target.allocationId);
    if (active?.allocation.generation !== target.generation)
      throw new ReconciliationLeaseLostError(target.allocationId);
    try {
      await runSidecarOperation(
        "Allocation connection events",
        operationTimeoutMs,
        (signal) =>
          queueConnectionEvent(target, async () => {
            signal.throwIfAborted();
            await apply();
          }),
        active.controller.signal,
      );
    } catch (error) {
      if (error instanceof SidecarOperationTimeoutError) {
        // Stop this lease without queuing another write behind the same stalled
        // event. Keep the actual operation queued until it settles, preserving
        // ordering with reconnects and excluding this allocation from new claims.
        const cancelled = new ReconciliationLeaseLostError(target.allocationId);
        active.controller.abort(cancelled);
        throw cancelled;
      }
      throw error;
    }
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
      try {
        if (await router.isAllocatedSidecarReady(target)) continue;
      } catch (error) {
        // Unknown readiness is not absence. Leave the allocation for the next
        // repair sweep instead of scheduling a reconnect the worker may hold.
        if (error instanceof SidecarIdentityValidationError) continue;
        throw error;
      }
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

  let admittedClaims = 0;

  async function reconcileNext(): Promise<boolean> {
    const retainedAllocations = new Set([
      ...connectionEvents.keys(),
      ...pendingAllocationQueries.keys(),
    ]);
    // An active claim already owns capacity for its allocation's pending work.
    for (const allocationId of activeAllocations.keys())
      retainedAllocations.delete(allocationId);
    if (admittedClaims + retainedAllocations.size >= maxConcurrentClaims)
      return false;

    admittedClaims += 1;
    let claim: ReturnType<AllocationStore["claimNextReconcilable"]> | undefined;
    try {
      const leaseId = createLeaseId();
      const claimStartedAt = performance.now();
      const allocation = await runSidecarOperation(
        "Sidecar allocation claim",
        operationTimeoutMs,
        () => {
          claim = allocationStore.claimNextReconcilable({
            leaseId,
            leaseDurationMs,
            excludedAllocationIds: [
              ...new Set([
                ...activeAllocations.keys(),
                ...connectionEvents.keys(),
                ...pendingAllocationQueries.keys(),
              ]),
            ],
          });
          return claim;
        },
      );
      if (allocation === null) return false;
      return await reconcileClaim(allocation, leaseId, claimStartedAt);
    } finally {
      const release = () => {
        admittedClaims -= 1;
      };
      // Keep the reservation through the claim-to-reconciliation handoff. A
      // timed-out claim still owns capacity until its database query settles.
      if (claim === undefined) release();
      else void claim.then(release, release);
    }
  }

  async function reconcileClaim(
    allocation: SidecarAllocation,
    leaseId: string,
    claimStartedAt: number,
  ): Promise<boolean> {
    // Local work may have started since this claim's exclusion snapshot. Let
    // its lease expire without adding another database write behind that work.
    if (
      connectionEvents.has(allocation.id) ||
      activeAllocations.has(allocation.id) ||
      pendingAllocationQueries.has(allocation.id)
    )
      return true;
    const active = {
      allocation,
      controller: new AbortController(),
      leaseDeadline: claimStartedAt + leaseDurationMs,
      pendingConnect: null,
    };
    activeAllocations.set(allocation.id, active);
    let finished = false;
    let renewing = false;
    const renew = async (): Promise<void> => {
      if (finished || renewing || active.controller.signal.aborted) return;
      renewing = true;
      const startedAt = performance.now();
      try {
        const renewed = await trackAllocationQuery(allocation.id, () =>
          allocationStore.extendReconciliationLease(
            allocation.id,
            leaseId,
            leaseDurationMs,
          ),
        );
        if (finished || active.controller.signal.aborted) return;
        if (!renewed || performance.now() >= active.leaseDeadline) {
          active.controller.abort(
            new ReconciliationLeaseLostError(allocation.id),
          );
        } else {
          // The database grants the lease during the request. Counting from its
          // start avoids extending ownership by the response's transit time.
          active.leaseDeadline = startedAt + leaseDurationMs;
        }
      } catch (error) {
        if (!finished && !active.controller.signal.aborted) {
          active.controller.abort(
            new ReconciliationLeaseLostError(allocation.id, error),
          );
        }
      } finally {
        renewing = false;
      }
    };
    // One heartbeat covers the whole claim, including database transitions
    // between provider calls. Short stages must not keep postponing renewal.
    const heartbeat = setInterval(
      () => {
        void renew();
      },
      Math.max(1, Math.floor(leaseDurationMs / 3)),
    );
    let expiryTimer: ReturnType<typeof setTimeout> | undefined;
    const checkLeaseExpiry = (): void => {
      if (active.controller.signal.aborted) return;
      const remaining = active.leaseDeadline - performance.now();
      if (remaining <= 0) {
        active.controller.abort(
          new ReconciliationLeaseLostError(allocation.id),
        );
      } else {
        expiryTimer = setTimeout(checkLeaseExpiry, Math.ceil(remaining));
      }
    };
    checkLeaseExpiry();
    try {
      active.controller.signal.throwIfAborted();
      await reconcile(allocation, leaseId);
    } catch (error) {
      if (error instanceof ReconciliationLeaseLostError) {
        const cause = error.cause;
        if (cause === undefined) {
          logger.info`Allocation ${allocation.id} reconciliation stopped: lease ${leaseId} is no longer current`;
        } else {
          logger.warn`Allocation ${allocation.id} reconciliation stopped because lease ${leaseId} could not be confirmed: ${cause instanceof Error ? cause.message : String(cause)}`;
        }
        // The durable schedule survives the claim. Stop renewing and let the
        // lease expire; handling a lease failure must not require another write.
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
      finished = true;
      clearInterval(heartbeat);
      clearTimeout(expiryTimer);
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

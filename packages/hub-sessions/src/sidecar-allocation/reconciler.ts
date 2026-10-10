import { type } from "arktype";

import { sha256 } from "@intx/crypto";
import { DeploymentRejectedError } from "@intx/types/sidecar";
import {
  SIDECAR_DEPLOYMENT_STOPPED_FAILURE_CODE,
  SIDECAR_CLEANUP_UNCONFIRMED_FAILURE_CODE,
  SIDECAR_CLEANUP_RETRY_EXHAUSTED_FAILURE_CODE,
  SIDECAR_CLEANUP_DISCONNECT_TIMEOUT_FAILURE_CODE,
  SidecarReuseRejectedError,
  SidecarInventoryUnavailableError,
  type SidecarAllocation,
  type SidecarAllocationStore,
} from "@intx/db";
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
  SidecarDeploymentHistoryPendingError,
  SidecarDeploymentMissingError,
  SidecarDeploymentStoppedError,
  SidecarFirstDeployError,
  SidecarOperationTimeoutError,
  type SidecarReconciliationContext,
} from "./operation";

const logger = getLogger(["hub", "sidecar-allocation"]);

type AllocationStore = Pick<
  SidecarAllocationStore,
  | "beginReplacement"
  | "beginRelease"
  | "beginUnrecoverableRelease"
  | "failStoppedDeployment"
  | "bindInitialSidecar"
  | "bindReplacementSidecar"
  | "claimNextReconcilable"
  | "extendReconciliationLease"
  | "failWithoutInfrastructure"
  | "listActive"
  | "isReconciliationLeaseCurrent"
  | "hasRunnableAnchor"
  | "markAllocated"
  | "markConnectionLost"
  | "markConnectionReady"
  | "markDestroyFailed"
  | "markReleased"
  | "confirmDeploymentCleanup"
  | "recordCleanupConnection"
  | "resumeCleanupAfterDisconnect"
  | "parkReconciliation"
  | "scheduleReconnectAfterHubStart"
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
    | "getCleanupConnection"
    | "holdsAllocatedBinding"
    | "isAllocatedSidecarReady"
    | "reportedDeploymentFailure"
    | "retireAllocation"
    | "syncSidecar"
    | "undeployAllocation"
    | "waitForAllocatedSidecar"
    | "waitForSidecarInventory"
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
  /** Idempotently runs a connected allocation generation's first deploy. */
  readonly onReady?: (
    allocation: SidecarAllocation,
    reconciliation: SidecarReconciliationContext,
  ) => Promise<void>;
  readonly leaseDurationMs?: number;
  readonly connectTimeoutMs?: number;
  readonly operationTimeoutMs?: number;
  /** Failed attempts allowed for a release before polling stops. */
  readonly maxCleanupAttempts?: number;
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
  /**
   * Gives the exact lost generation as long as its allocation allows to
   * reconnect before it is failed.
   */
  handleDisconnect(target: AllocatedSidecarTarget): Promise<void>;
  /** Wakes recovery as soon as the exact generation reconnects. */
  handleConnected(target: AllocatedSidecarTarget): Promise<void>;
  /** Repairs missed readiness and cleanup wakes, and discards obsolete waits. */
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
// How long a first deploy that keeps failing before its deploy frame is sent
// is retried, from its first failure.
const FIRST_DEPLOY_RETRY_LIMIT_MS = 60_000;
const PROVISIONER_AVAILABILITY_RETRY_MS = 30_000;

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
  leaseDurationMs = DEFAULT_LEASE_DURATION_MS,
  connectTimeoutMs = DEFAULT_CONNECT_TIMEOUT_MS,
  operationTimeoutMs = DEFAULT_SIDECAR_OPERATION_TIMEOUT_MS,
  maxCleanupAttempts = 10,
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
  if (!Number.isSafeInteger(maxCleanupAttempts) || maxCleanupAttempts <= 0) {
    throw new Error("maxCleanupAttempts must be a positive integer");
  }

  function fence(allocation: SidecarAllocation): void {
    discardChangedPlacement(allocation);
    router.fenceAllocation(allocation.id, allocation.generation, {
      ...(allocation.sidecarId !== undefined &&
      (allocation.status === "releasing" ||
        allocation.status === "destroy_failed")
        ? { cleanup: { sidecarId: allocation.sidecarId } }
        : {}),
    });
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

  type PendingPlacement = {
    readonly generation: number;
    readonly result: Extract<EnsureSidecarResult, { kind: "accepted" }>;
    readonly deadline: Date;
    waiter: { controller: AbortController; ready: boolean } | undefined;
  };
  const pendingPlacements = new Map<string, PendingPlacement>();
  function discardPlacement(allocationId: string): void {
    const pending = pendingPlacements.get(allocationId);
    pendingPlacements.delete(allocationId);
    pending?.waiter?.controller.abort();
  }

  function discardChangedPlacement(allocation: SidecarAllocation): void {
    const pending = pendingPlacements.get(allocation.id);
    if (
      pending !== undefined &&
      (allocation.status !== "provisioning" ||
        pending.generation !== allocation.generation)
    )
      discardPlacement(allocation.id);
  }

  function wakePendingPlacement(
    allocationId: string,
    pending: PendingPlacement,
  ): Promise<void> {
    const waiter = pending.waiter;
    return queueConnectionEvent(
      { allocationId, generation: pending.generation },
      async () => {
        if (
          pendingPlacements.get(allocationId) !== pending ||
          pending.waiter !== waiter
        )
          return;
        if (
          !(await allocationStore.wakeReconciliation(
            allocationId,
            pending.generation,
          )) &&
          pendingPlacements.get(allocationId) === pending
        )
          discardPlacement(allocationId);
      },
    );
  }

  function watchPlacementInventory(
    allocationId: string,
    pending: PendingPlacement,
    sidecarId: string,
  ): void {
    pending.waiter?.controller.abort();
    const waiter = { controller: new AbortController(), ready: false };
    pending.waiter = waiter;
    // This subscription owns no claim or lease. The durable schedule enforces
    // its deadline; cancellation and the repair sweep discard obsolete waits.
    void router
      .waitForSidecarInventory(sidecarId, waiter.controller.signal)
      .then(async () => {
        if (
          pendingPlacements.get(allocationId) !== pending ||
          pending.waiter !== waiter
        )
          return;
        // Preserve timely inventory across a delayed claim, without letting a
        // late hello extend the original connection deadline.
        waiter.ready = now() <= pending.deadline;
        await wakePendingPlacement(allocationId, pending);
      })
      .catch((cause: unknown) => {
        if (!waiter.controller.signal.aborted)
          logger.warn`Could not wake inventory placement ${allocationId}: ${cause instanceof Error ? cause.message : String(cause)}`;
      });
  }

  function trackAllocation(allocation: SidecarAllocation): void {
    discardChangedPlacement(allocation);
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
  ): { provisioner: SidecarProvisioner } | { error: string } {
    const provisioner = plugins.getProvisioner(allocation.provisionerId);
    if (provisioner === null)
      return {
        error: `Provisioner ${allocation.provisionerId} is missing from the plugin registry`,
      };
    if (provisioner.apiVersion !== allocation.provisionerApiVersion)
      return {
        error: `Provisioner ${allocation.provisionerId} has API version ${String(provisioner.apiVersion)}; this allocation requires version ${String(allocation.provisionerApiVersion)}`,
      };
    if (
      provisioner.bindingFingerprint !==
      allocation.provisionerBindingFingerprint
    )
      return {
        error: `Provisioner ${allocation.provisionerId} has binding ${provisioner.bindingFingerprint}; this allocation requires binding ${allocation.provisionerBindingFingerprint}`,
      };
    return { provisioner };
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
    const sidecarId = allocation.sidecarId;
    return runSidecarOperation(
      "Sidecar readiness",
      operationTimeoutMs,
      () =>
        trackAllocationQuery(allocation.id, async () => {
          const target = {
            allocationId: allocation.id,
            generation: allocation.generation,
          };
          // The allocation may have been placed on a sidecar that stayed
          // connected, which registered before this binding existed. The sync
          // waits behind the sidecar's other frames, such as another
          // deployment's pack ingest, so it runs only for a binding not yet
          // attached rather than hold a claim slot for one that is.
          if (sidecarId !== undefined && !router.holdsAllocatedBinding(target))
            await router.syncSidecar(sidecarId);
          return router.isAllocatedSidecarReady(target);
        }),
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
        // An allocated worker is never replaced: it may still be running the
        // deployment, and the Hub cannot restore its local state. Provisioning
        // ran nothing yet, so a new generation replaces it.
        const updated =
          allocation.status === "allocated"
            ? await allocationStore.beginUnrecoverableRelease({
                ...initializationCheck,
                allocationId: allocation.id,
                expectedStatus: "allocated",
                expectedGeneration: allocation.generation,
                expectedLeaseId: leaseId,
                failureCode: code,
                failureMessage: message,
                now: now(),
              })
            : await allocationStore.beginReplacement({
                allocationId: allocation.id,
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
          fence(updated);
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
      // A sidecar that reported the deployment stopped and then stayed away
      // fails it for the reason it gave, unless a Hub restart has since
      // dropped that report.
      const stopped = router.reportedDeploymentFailure(target);
      await replaceAfterFailure(
        allocation,
        leaseId,
        stopped === undefined
          ? "sidecar_connect_failed"
          : SIDECAR_DEPLOYMENT_STOPPED_FAILURE_CODE,
        stopped === undefined
          ? error instanceof Error
            ? error.message
            : String(error)
          : `${stopped}; its sidecar disconnected, and history the Hub never received may be lost`,
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
        if (error instanceof DeploymentRejectedError) {
          await replaceAfterFailure(
            allocation,
            leaseId,
            "sidecar_deployment_rejected",
            error.message,
            { onlyIfInitializationIncomplete: true },
          );
          return;
        }
        if (error instanceof SidecarDeploymentMissingError) {
          await replaceAfterFailure(
            allocation,
            leaseId,
            "sidecar_deployment_missing",
            error.message,
          );
          return;
        }
        if (error instanceof SidecarDeploymentHistoryPendingError) {
          // A report that landed meanwhile may carry the tips the Hub holds.
          await finishReconciliation(allocation.id, (pendingConnect) =>
            allocationStore.scheduleRetry({
              allocationId: allocation.id,
              expectedStatus: "allocated",
              expectedGeneration: allocation.generation,
              expectedLeaseId: leaseId,
              nextAttemptAt: pendingConnect ? now() : error.retryAt,
              now: now(),
            }),
          );
          return;
        }
        if (error instanceof SidecarDeploymentStoppedError) {
          // The capacity stays: the stopped copy's files are kept for
          // inspection until the retention for failed runs releases them.
          await finishReconciliation(allocation.id, () =>
            allocationStore.failStoppedDeployment({
              allocationId: allocation.id,
              expectedGeneration: allocation.generation,
              expectedLeaseId: leaseId,
              failureCode: SIDECAR_DEPLOYMENT_STOPPED_FAILURE_CODE,
              failureMessage: error.message,
              now: now(),
            }),
          );
          return;
        }
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
        const message = error instanceof Error ? error.message : String(error);
        // Only a first deploy is given up on: a failure around a deployment
        // that already runs, such as an unreachable database, says nothing
        // about the deployment.
        const firstDeployFailedAt =
          error instanceof SidecarFirstDeployError
            ? (allocation.firstDeployFailedAt ?? now())
            : undefined;
        if (
          firstDeployFailedAt !== undefined &&
          now().getTime() - firstDeployFailedAt.getTime() >=
            FIRST_DEPLOY_RETRY_LIMIT_MS
        ) {
          await replaceAfterFailure(
            allocation,
            leaseId,
            "sidecar_initialization_failed",
            `The first deploy kept failing for ${String(FIRST_DEPLOY_RETRY_LIMIT_MS / 1000)} seconds: ${message}`,
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
            failure: { code: "sidecar_initialization_failed", message },
            ...(firstDeployFailedAt !== undefined
              ? { firstDeployFailedAt }
              : {}),
            now: now(),
          }),
        );
        return;
      }
    }

    // A connect that lands during initialization schedules an immediate
    // follow-up even on success: the new socket may be a restarted worker
    // with an empty inventory (takeover suppresses the disconnect event), in
    // which case the follow-up fails the deployment it no longer holds. When
    // the worker is unchanged the follow-up is a no-op: deployReadyAllocation
    // returns early once the workflow is active and its key is recorded.
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

    await acceptPlacement(allocation, leaseId, result);
  }

  async function failInventoryPlacement(
    allocation: SidecarAllocation,
    leaseId: string,
  ): Promise<void> {
    await queueReconciliationStep(
      { allocationId: allocation.id, generation: allocation.generation },
      async () => {
        const releasing = await allocationStore.beginUnrecoverableRelease({
          allocationId: allocation.id,
          expectedStatus: "provisioning",
          expectedGeneration: allocation.generation,
          expectedLeaseId: leaseId,
          failureCode: "sidecar_inventory_unavailable",
          failureMessage:
            "Sidecar inventory did not become available before the connection deadline",
          now: now(),
        });
        if (releasing !== null) {
          trackAllocation(releasing);
          fence(releasing);
        }
      },
    );
  }

  async function acceptPlacement(
    allocation: SidecarAllocation,
    leaseId: string,
    result: Extract<EnsureSidecarResult, { kind: "accepted" }>,
  ): Promise<void> {
    const pending = pendingPlacements.get(allocation.id);
    if (
      pending !== undefined &&
      pending.deadline <= now() &&
      pending.waiter?.ready !== true
    ) {
      await failInventoryPlacement(allocation, leaseId);
      return;
    }

    let allocated: SidecarAllocation | null;
    try {
      allocated = await allocationStore.markAllocated({
        allocationId: allocation.id,
        generation: allocation.generation,
        ...(result.sidecarId !== undefined
          ? { sidecarId: result.sidecarId }
          : {}),
        ...(result.externalRef !== undefined
          ? { externalRef: result.externalRef }
          : {}),
        expectedLeaseId: leaseId,
        now: now(),
      });
    } catch (error) {
      if (error instanceof SidecarInventoryUnavailableError) {
        if (allocation.connectDeadline === undefined)
          throw new Error(
            `Provisioning allocation ${allocation.id} has no connection deadline`,
          );
        const placement = pending ?? {
          generation: allocation.generation,
          result,
          deadline: allocation.connectDeadline,
          waiter: undefined,
        };
        pendingPlacements.set(allocation.id, placement);
        if (placement.deadline <= now()) {
          await failInventoryPlacement(allocation, leaseId);
          return;
        }
        watchPlacementInventory(allocation.id, placement, error.sidecarId);
        await finishReconciliation(allocation.id, () =>
          allocationStore.scheduleRetry({
            allocationId: allocation.id,
            expectedStatus: "provisioning",
            expectedGeneration: allocation.generation,
            expectedLeaseId: leaseId,
            // A hello before this write must not be overwritten by the deadline.
            // A hello during it queues its wake behind this same event lane.
            nextAttemptAt:
              placement.waiter?.ready === true ? now() : placement.deadline,
            // Postgres may consider the deadline due before the Hub does.
            // Keep those early claims at least a second apart on its clock.
            ...(placement.waiter?.ready === true
              ? {}
              : { minimumDelayMs: 1_000 }),
            now: now(),
          }),
        );
        return;
      }
      if (!(error instanceof SidecarReuseRejectedError)) throw error;
      await replaceAfterFailure(
        allocation,
        leaseId,
        "sidecar_reuse_rejected",
        error.message,
      );
      return;
    }
    if (allocated !== null) {
      trackAllocation(allocated);
      if (await isSidecarReady(allocated)) {
        await waitUntilReady(allocated, leaseId, true);
        return;
      }
      // Provisioning acceptance and websocket readiness are separate durable
      // transitions. Do not hold a reconciliation slot for the full
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
    if (
      !(await allocationStore.hasRunnableAnchor(allocation.anchorRunId, now()))
    ) {
      const releasing = await allocationStore.beginRelease({
        allocationId: allocation.id,
        expectedStatus: replacement ? "replacing" : "pending",
        expectedGeneration: allocation.generation,
        expectedLeaseId: leaseId,
        now: now(),
      });
      if (releasing !== null) fence(releasing);
      return;
    }
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
    fence(bound);
    await acceptEnsure(bound, leaseId, provisioner, token);
  }

  async function retryDestroy(
    allocation: SidecarAllocation,
    leaseId: string,
    cleanup: CleanupProgress,
    failure: { code: string; message: string },
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
    logger.warn`Cleanup attempt ${String(allocation.destroyAttempts + 1)} failed for allocation ${allocation.id} on sidecar ${allocation.sidecarId ?? "unknown"} generation ${String(allocation.generation)}: ${failure.code}: ${failure.message}`;
    if (
      status === "releasing" &&
      allocation.destroyAttempts + 1 >= maxCleanupAttempts
    ) {
      await finishReconciliation(allocation.id, async () => {
        const failed = await allocationStore.markDestroyFailed({
          allocationId: allocation.id,
          expectedGeneration: allocation.generation,
          expectedLeaseId: leaseId,
          code: SIDECAR_CLEANUP_RETRY_EXHAUSTED_FAILURE_CODE,
          message: `Cleanup remains unconfirmed after ${String(allocation.destroyAttempts + 1)} attempts: ${failure.message}`,
          now: now(),
        });
        if (failed !== null)
          logger.error`Cleanup retry budget exhausted for allocation ${allocation.id} on sidecar ${allocation.sidecarId ?? "unknown"} generation ${String(allocation.generation)}; operator recovery is required. Cleanup error: ${failure.message}. Prior allocation reason: ${allocation.failureCode ?? "none"} ${allocation.failureMessage ?? ""}`;
      });
      return;
    }
    await finishReconciliation(allocation.id, () =>
      allocationStore.scheduleRetry({
        allocationId: allocation.id,
        expectedStatus: status,
        expectedGeneration: allocation.generation,
        nextAttemptAt:
          !cleanup.confirmed &&
          activeAllocations.get(allocation.id)?.pendingConnect?.generation ===
            allocation.generation
            ? now()
            : retryAt(allocation.destroyAttempts),
        expectedLeaseId: leaseId,
        attempt: "destroy",
        now: now(),
      }),
    );
  }

  type CleanupProgress = {
    confirmed: boolean;
    failed: boolean;
    attemptedConnections: Set<object>;
    message: string;
  };

  async function confirmCleanup(
    allocation: SidecarAllocation,
    leaseId: string,
    cleanup: CleanupProgress,
  ): Promise<void> {
    if (cleanup.confirmed) return;
    const target = {
      allocationId: allocation.id,
      generation: allocation.generation,
    };
    await queueReconciliationStep(target, async () => {
      if (
        !(await allocationStore.confirmDeploymentCleanup({
          ...target,
          expectedLeaseId: leaseId,
          now: now(),
        }))
      )
        throw new ReconciliationLeaseLostError(allocation.id);
      cleanup.confirmed = true;
    });
  }

  async function cleanUpConnectedSidecar(
    allocation: SidecarAllocation,
    leaseId: string,
    cleanup: CleanupProgress,
  ): Promise<void> {
    const target = {
      allocationId: allocation.id,
      generation: allocation.generation,
    };
    if (cleanup.confirmed) return;
    if (allocation.sidecarId === undefined) return;
    const sidecarId = allocation.sidecarId;
    const connectionBeforeSync = router.getCleanupConnection(target);
    let connection: ReturnType<typeof router.getCleanupConnection>;
    try {
      const acknowledged = await withReconciliationLease(
        allocation,
        leaseId,
        "Sidecar deployment cleanup",
        ({ signal }) =>
          trackAllocationQuery(allocation.id, async () => {
            await router.syncSidecar(sidecarId, signal);
            signal.throwIfAborted();
            // Select the connection after its wake finishes, so a wake already
            // handled by this attempt cannot bypass the retry backoff.
            await queueReconciliationStep(target, async () => {
              signal.throwIfAborted();
              const current = router.getCleanupConnection(target);
              if (current === undefined) return;
              const active = activeAllocations.get(allocation.id);
              if (active !== undefined) active.pendingConnect = null;
              if (cleanup.attemptedConnections.has(current)) return;
              connection = current;
              cleanup.attemptedConnections.add(current);
            });
            if (connection === undefined) return false;
            if (
              allocation.status === "releasing" &&
              allocation.connectDeadline !== undefined
            ) {
              const connected = await allocationStore.recordCleanupConnection({
                allocationId: allocation.id,
                generation: allocation.generation,
                expectedLeaseId: leaseId,
                connected: true,
                now: now(),
              });
              if (connected === null)
                throw new ReconciliationLeaseLostError(allocation.id);
              signal.throwIfAborted();
            }
            await router.undeployAllocation(
              target,
              operationTimeoutMs,
              signal,
              connection,
            );
            return true;
          }),
        operationTimeoutMs,
      );
      if (!acknowledged) return;
    } catch (error) {
      if (error instanceof ReconciliationLeaseLostError) throw error;
      cleanup.failed = true;
      cleanup.message = error instanceof Error ? error.message : String(error);
      logger.warn`Sidecar cleanup request failed for allocation ${allocation.id} on sidecar ${sidecarId} generation ${String(allocation.generation)}: ${cleanup.message}`;
      if (connection === undefined && connectionBeforeSync !== undefined) {
        // A failed setup handled this connection's wake too. Preserve a
        // replacement that arrived during the failure for the next attempt.
        await queueReconciliationStep(target, async () => {
          const active = activeAllocations.get(allocation.id);
          if (
            router.getCleanupConnection(target) === connectionBeforeSync &&
            active?.allocation.generation === allocation.generation
          ) {
            active.pendingConnect = null;
          }
        });
      }
      return;
    }
    // Persistence failures use the reconciliation retry; they are not failed
    // undeploys and must not consume the cleanup attempt budget.
    await confirmCleanup(allocation, leaseId, cleanup);
  }

  async function waitForCleanupConnection(
    allocation: SidecarAllocation,
    leaseId: string,
  ): Promise<void> {
    await finishReconciliation(allocation.id, async () => {
      const target = {
        allocationId: allocation.id,
        generation: allocation.generation,
      };
      let deadline: Date | undefined;
      // This check and the park share the connection-event lane: a reconnect
      // observed during the write schedules an immediate claim after it.
      if (router.getCleanupConnection(target) === undefined) {
        const waiting = await allocationStore.recordCleanupConnection({
          ...target,
          expectedLeaseId: leaseId,
          connected: false,
          now: now(),
        });
        if (waiting === null) return;
        // Registration can finish while the deadline write waits on the database.
        // Let the next claim use that connection instead of failing a live peer.
        if (router.getCleanupConnection(target) === undefined) {
          deadline = waiting.connectDeadline;
          if (deadline === undefined)
            throw new Error("Disconnected cleanup has no connection deadline");
        }
      }
      if (deadline !== undefined && deadline <= now()) {
        const failed = await allocationStore.markDestroyFailed({
          allocationId: allocation.id,
          expectedGeneration: allocation.generation,
          expectedLeaseId: leaseId,
          code: SIDECAR_CLEANUP_DISCONNECT_TIMEOUT_FAILURE_CODE,
          message: `Sidecar did not reconnect before the cleanup deadline ${deadline.toISOString()}`,
          countAttempt: false,
          now: now(),
        });
        if (failed !== null)
          logger.error`Cleanup connection deadline ${deadline.toISOString()} expired for allocation ${allocation.id} on sidecar ${allocation.sidecarId ?? "unknown"} generation ${String(allocation.generation)}; capacity remains reserved. Prior allocation reason: ${allocation.failureCode ?? "none"} ${allocation.failureMessage ?? ""}`;
        return;
      }
      await allocationStore.scheduleRetry({
        allocationId: allocation.id,
        expectedStatus: "releasing",
        expectedGeneration: allocation.generation,
        expectedLeaseId: leaseId,
        nextAttemptAt: deadline ?? now(),
        ...(deadline === undefined ? {} : { minimumDelayMs: 1_000 }),
        now: now(),
      });
    });
  }

  async function destroyCurrent(
    allocation: SidecarAllocation,
    leaseId: string,
    provisioner: SidecarProvisioner,
    cleanup: CleanupProgress,
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
      await cleanUpConnectedSidecar(allocation, leaseId, cleanup);
      await retryDestroy(allocation, leaseId, cleanup, {
        code: SIDECAR_CLEANUP_UNCONFIRMED_FAILURE_CODE,
        message: error instanceof Error ? error.message : String(error),
      });
      return false;
    }
    if (result.kind === "destroyed" && result.cleanup === "confirmed")
      await confirmCleanup(allocation, leaseId, cleanup);
    if (
      result.kind === "destroyed" &&
      (result.cleanup === "confirmed" || cleanup.confirmed)
    )
      return true;

    // A sidecar may have connected during destruction, including a failed
    // provider call. Try that connection without repeating an earlier request.
    await cleanUpConnectedSidecar(allocation, leaseId, cleanup);
    if (result.kind === "destroyed") {
      if (cleanup.confirmed) return true;
      if (allocation.status === "releasing" && !cleanup.failed) {
        await waitForCleanupConnection(allocation, leaseId);
        return false;
      }
      await retryDestroy(allocation, leaseId, cleanup, {
        code: SIDECAR_CLEANUP_UNCONFIRMED_FAILURE_CODE,
        message: cleanup.message,
      });
      return false;
    }
    if (!result.retryable) {
      logger.error`Provisioner permanently rejected cleanup for allocation ${allocation.id} on sidecar ${sidecarId} generation ${String(allocation.generation)}: ${result.code} ${result.message}. Prior allocation reason: ${allocation.failureCode ?? "none"} ${allocation.failureMessage ?? ""}`;
      const failed = await allocationStore.markDestroyFailed({
        allocationId: allocation.id,
        expectedGeneration: allocation.generation,
        expectedLeaseId: leaseId,
        // A provider rejection must not impersonate a Hub timeout and gain
        // reconnect recovery, or be reported as retry-budget exhaustion.
        code:
          result.code === SIDECAR_CLEANUP_RETRY_EXHAUSTED_FAILURE_CODE ||
          result.code === SIDECAR_CLEANUP_DISCONNECT_TIMEOUT_FAILURE_CODE
            ? "sidecar_provisioner_cleanup_rejected"
            : result.code,
        message: result.message,
        now: now(),
      });
      if (failed !== null) fence(failed);
      return false;
    }
    await retryDestroy(allocation, leaseId, cleanup, {
      code: SIDECAR_CLEANUP_UNCONFIRMED_FAILURE_CODE,
      message: result.message,
    });
    return false;
  }

  async function completeRelease(
    allocation: SidecarAllocation,
    leaseId: string,
  ): Promise<void> {
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
  }

  async function reconcileCleanup(
    allocation: SidecarAllocation,
    leaseId: string,
  ): Promise<void> {
    if (
      allocation.status === "releasing" &&
      allocation.sidecarId === undefined
    ) {
      await completeRelease(allocation, leaseId);
      return;
    }
    const cleanup: CleanupProgress = {
      // An unaccepted ensure could not have received a deployment frame.
      confirmed:
        allocation.deploymentCleanupConfirmed ||
        allocation.ensureAcceptedGeneration === undefined,
      failed: false,
      attemptedConnections: new Set(),
      message: "No sidecar connection is available to confirm cleanup",
    };
    // Stopping a connected deployment must not depend on provider availability.
    await cleanUpConnectedSidecar(allocation, leaseId, cleanup);
    const selected = provisionerFor(allocation);
    if ("error" in selected) {
      logger.error`Cleanup blocked for allocation ${allocation.id} on sidecar ${allocation.sidecarId ?? "unknown"} generation ${String(allocation.generation)}: ${selected.error}. Restore the matching provisioner to finish releasing capacity`;
      if (cleanup.failed) {
        // An actual sidecar failure still spends an attempt even though its
        // provider is unavailable. Missing configuration alone does not.
        await retryDestroy(allocation, leaseId, cleanup, {
          code: SIDECAR_CLEANUP_UNCONFIRMED_FAILURE_CODE,
          message: cleanup.message,
        });
      } else {
        await finishReconciliation(allocation.id, () =>
          allocationStore.scheduleRetry({
            allocationId: allocation.id,
            expectedStatus:
              allocation.status === "replacing" ? "replacing" : "releasing",
            expectedGeneration: allocation.generation,
            expectedLeaseId: leaseId,
            nextAttemptAt: new Date(
              now().getTime() + PROVISIONER_AVAILABILITY_RETRY_MS,
            ),
            minimumDelayMs: PROVISIONER_AVAILABILITY_RETRY_MS,
            now: now(),
          }),
        );
      }
      return;
    }
    const { provisioner } = selected;
    if (!(await destroyCurrent(allocation, leaseId, provisioner, cleanup)))
      return;
    if (allocation.status === "replacing") {
      await bindAndEnsure(allocation, leaseId, provisioner, true);
      return;
    }
    await completeRelease(allocation, leaseId);
  }

  async function reconcile(
    allocation: SidecarAllocation,
    leaseId: string,
  ): Promise<void> {
    fence(allocation);
    if (
      allocation.status === "released" ||
      allocation.status === "failed" ||
      allocation.status === "destroy_failed"
    ) {
      return;
    }
    if (
      allocation.status === "releasing" ||
      allocation.status === "replacing"
    ) {
      await reconcileCleanup(allocation, leaseId);
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
    const pendingPlacement = pendingPlacements.get(allocation.id);
    if (
      allocation.status === "provisioning" &&
      pendingPlacement !== undefined
    ) {
      await acceptPlacement(allocation, leaseId, pendingPlacement.result);
      return;
    }
    const selected = provisionerFor(allocation);
    if ("error" in selected) {
      if (allocation.status === "pending") {
        const failed = await allocationStore.failWithoutInfrastructure({
          allocationId: allocation.id,
          expectedStatus: "pending",
          expectedGeneration: allocation.generation,
          code: "provisioner_unavailable",
          message: selected.error,
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
    const { provisioner } = selected;

    switch (allocation.status) {
      case "pending":
        await bindAndEnsure(allocation, leaseId, provisioner, false);
        return;
      case "provisioning": {
        // The raw bearer token and unrecorded acceptance are not durable. A
        // restart has no parked result to resume, so it fences uncertain work.
        await replaceAfterFailure(
          allocation,
          leaseId,
          "ensure_outcome_unknown",
          "Hub restarted before sidecar provisioning acceptance was recorded",
        );
        return;
      }
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
      default: {
        const exhaustive: never = allocation.status;
        throw new Error(
          `Allocation ${allocation.id} has unhandled status ${String(exhaustive)}`,
        );
      }
    }
  }

  async function initialize(): Promise<void> {
    const startedAt = now();
    for (const allocation of await allocationStore.listActive()) {
      fence(allocation);
      if (allocation.status === "destroy_failed") continue;
      if (allocation.status === "allocated") {
        await allocationStore.scheduleReconnectAfterHubStart({
          allocationId: allocation.id,
          generation: allocation.generation,
          now: startedAt,
          firstConnectDeadline: new Date(
            startedAt.getTime() + connectTimeoutMs,
          ),
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
        const cancelled = new ReconciliationLeaseLostError(
          target.allocationId,
          error,
        );
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
      const lostAt = now();
      const disconnected = await allocationStore.markConnectionLost({
        allocationId: target.allocationId,
        generation: target.generation,
        now: lostAt,
        firstConnectDeadline: new Date(lostAt.getTime() + connectTimeoutMs),
      });
      noteDisconnect(target, disconnected !== null);
    });
  }

  function handleConnected(target: AllocatedSidecarTarget): Promise<void> {
    noteConnect(target);
    return queueConnectionEvent(target, async () => {
      if (
        router.getCleanupConnection(target) !== undefined &&
        (await allocationStore.resumeCleanupAfterDisconnect(
          target.allocationId,
          target.generation,
        ))
      ) {
        noteConnect(target);
        return;
      }
      await allocationStore.wakeReconciliation(
        target.allocationId,
        target.generation,
        {
          connected:
            router.getCleanupConnection(target) !== undefined ||
            router.holdsAllocatedBinding(target),
        },
      );
      noteConnect(target);
    });
  }

  async function repairUnscheduledConnections(): Promise<void> {
    const parked = [...pendingPlacements];
    const allocations = await allocationStore.listActive();
    const current = new Map(
      allocations.map((allocation) => [allocation.id, allocation]),
    );
    for (const [allocationId, pending] of parked) {
      if (pendingPlacements.get(allocationId) !== pending) continue;
      const allocation = current.get(allocationId);
      if (allocation === undefined) discardPlacement(allocationId);
      else {
        discardChangedPlacement(allocation);
        if (
          pendingPlacements.get(allocationId) === pending &&
          pending.waiter?.ready === true
        ) {
          try {
            await wakePendingPlacement(allocationId, pending);
          } catch (error) {
            logger.warn`Failed to repair inventory placement ${allocationId}: ${error instanceof Error ? error.message : String(error)}`;
          }
        }
      }
    }
    for (const allocation of allocations) {
      const parkedCleanup =
        allocation.status === "releasing" &&
        allocation.connectDeadline !== undefined &&
        allocation.nextAttemptAt?.getTime() ===
          allocation.connectDeadline.getTime()
          ? {
              deadline: allocation.connectDeadline,
              destroyAttempts: allocation.destroyAttempts,
            }
          : undefined;
      if (
        parkedCleanup !== undefined ||
        (allocation.status === "destroy_failed" &&
          allocation.failureCode ===
            SIDECAR_CLEANUP_DISCONNECT_TIMEOUT_FAILURE_CODE)
      ) {
        const target = {
          allocationId: allocation.id,
          generation: allocation.generation,
        };
        try {
          await queueConnectionEvent(target, async () => {
            if (router.getCleanupConnection(target) !== undefined)
              await allocationStore.resumeCleanupAfterDisconnect(
                target.allocationId,
                target.generation,
                parkedCleanup,
              );
          });
        } catch (error) {
          logger.warn`Failed to resume disconnected cleanup for allocation ${allocation.id}: ${error instanceof Error ? error.message : String(error)}`;
        }
        continue;
      }
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
      let ready: boolean;
      try {
        ready = await router.isAllocatedSidecarReady(target);
      } catch (error) {
        // Unknown readiness is not absence. Leave the allocation for the next
        // repair sweep instead of scheduling a reconnect the worker may hold.
        if (error instanceof SidecarIdentityValidationError) continue;
        throw error;
      }
      try {
        if (!ready) {
          const repairedAt = now();
          await allocationStore.scheduleReconnectIfUnscheduled({
            ...target,
            now: repairedAt,
            firstConnectDeadline: new Date(
              repairedAt.getTime() + connectTimeoutMs,
            ),
          });
        } else if (
          router.reportedDeploymentFailure(target) !== undefined &&
          allocation.failureCode !== SIDECAR_DEPLOYMENT_STOPPED_FAILURE_CODE &&
          (await allocationStore.hasRunnableAnchor(
            allocation.anchorRunId,
            now(),
          ))
        ) {
          // A stop the sidecar reported is acted on by one wake. If that wake
          // was lost, the allocation rests on its ready connection with the
          // stop not yet recorded. Once the anchor can no longer run, a wake
          // has nothing left to fail, so none is made.
          await allocationStore.wakeReconciliation(
            allocation.id,
            allocation.generation,
          );
        }
      } catch (error) {
        logger.warn`Failed to repair allocation ${allocation.id} schedule: ${error instanceof Error ? error.message : String(error)}`;
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

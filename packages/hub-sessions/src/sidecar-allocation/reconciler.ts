import { type } from "arktype";

import { sha256 } from "@intx/crypto";
import type { SidecarAllocation, SidecarAllocationStore } from "@intx/db";
import { getLogger } from "@intx/log";
import { hexEncode } from "@intx/types";

import type { SidecarAllocationRouter } from "../ws/sidecar-handler";
import {
  DestroySidecarResult,
  EnsureSidecarResult,
  type SidecarProvisioner,
} from "./contracts";
import type { SidecarPluginRegistry } from "./plugin-registry";

const logger = getLogger(["hub", "sidecar-allocation"]);

type AllocationStore = Pick<
  SidecarAllocationStore,
  | "beginReplacement"
  | "bindInitialSidecar"
  | "bindReplacementSidecar"
  | "claimNextReconcilable"
  | "extendReconciliationLease"
  | "failWithoutInfrastructure"
  | "listActive"
  | "markAllocated"
  | "markConnectionReady"
  | "markReleased"
  | "parkReconciliation"
  | "scheduleRetry"
  | "wakeReconciliation"
>;

export type SidecarAllocationReconcilerDeps = {
  readonly allocationStore: AllocationStore;
  readonly plugins: SidecarPluginRegistry;
  readonly router: Pick<
    SidecarAllocationRouter,
    "fenceAllocation" | "isAllocatedSidecarReady" | "waitForAllocatedSidecar"
  >;
  readonly hubWebSocketUrl: string;
  /** Idempotently restores and deploys one connected allocation generation. */
  readonly onReady?: (allocation: SidecarAllocation) => Promise<void>;
  readonly leaseDurationMs?: number;
  readonly connectTimeoutMs?: number;
  readonly retryDelayMs?: (attempt: number) => number;
  readonly now?: () => Date;
  readonly createSidecarId?: () => string;
  readonly createToken?: () => string;
  readonly createLeaseId?: () => string;
};

export type SidecarAllocationReconciler = {
  /** Rebuild all trust fences before accepting allocated connections. */
  initialize(): Promise<void>;
  /** Reconcile at most one due allocation. Returns false when none are due. */
  reconcileNext(): Promise<boolean>;
  /** Drain the currently due queue, bounded to catch accidental hot loops. */
  reconcileUntilIdle(maxIterations?: number): Promise<number>;
};

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
  leaseDurationMs = DEFAULT_LEASE_DURATION_MS,
  connectTimeoutMs = DEFAULT_CONNECT_TIMEOUT_MS,
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
    operation: () => Promise<T>,
  ): Promise<T> {
    const interval = setInterval(
      () => {
        void allocationStore
          .extendReconciliationLease(allocation.id, leaseId, leaseDurationMs)
          .catch((error: unknown) => {
            logger.warn`Failed to extend allocation ${allocation.id} lease: ${error instanceof Error ? error.message : String(error)}`;
          });
      },
      Math.max(1, Math.floor(leaseDurationMs / 3)),
    );
    try {
      return await operation();
    } finally {
      clearInterval(interval);
    }
  }

  async function replaceAfterFailure(
    allocation: SidecarAllocation,
    leaseId: string,
    code: string,
    message: string,
  ): Promise<void> {
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
  ): Promise<void> {
    const target = {
      allocationId: allocation.id,
      generation: allocation.generation,
    };
    const deadline = allocation.connectDeadline;
    const remaining =
      deadline === undefined ? 0 : deadline.getTime() - now().getTime();
    try {
      if (!(await router.isAllocatedSidecarReady(target))) {
        await withLeaseHeartbeat(allocation, leaseId, () =>
          router.waitForAllocatedSidecar(target, Math.max(0, remaining)),
        );
      }
    } catch (error) {
      await replaceAfterFailure(
        allocation,
        leaseId,
        "sidecar_connect_failed",
        error instanceof Error ? error.message : String(error),
      );
      return;
    }

    if (onReady !== undefined) {
      try {
        await withLeaseHeartbeat(allocation, leaseId, () =>
          onReady(allocation),
        );
      } catch (error) {
        await allocationStore.scheduleRetry({
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
        });
        return;
      }
    }

    await allocationStore.markConnectionReady({
      allocationId: allocation.id,
      generation: allocation.generation,
      expectedLeaseId: leaseId,
      now: now(),
    });
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
        await withLeaseHeartbeat(allocation, leaseId, () =>
          provisioner.ensure({
            allocationId: allocation.id,
            generation: allocation.generation,
            tenantId: allocation.tenantId,
            anchorRunId: allocation.anchorRunId,
            sidecarId,
            token,
            hubWebSocketUrl,
          }),
        ),
      );
    } catch (error) {
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
        await allocationStore.failWithoutInfrastructure({
          allocationId: allocation.id,
          expectedStatus: "provisioning",
          expectedGeneration: allocation.generation,
          code: result.code,
          message: result.message,
          expectedLeaseId: leaseId,
          now: now(),
        });
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
    if (allocated !== null) await waitUntilReady(allocated, leaseId);
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
    await allocationStore.scheduleRetry({
      allocationId: allocation.id,
      expectedStatus: allocation.status,
      expectedGeneration: allocation.generation,
      nextAttemptAt: retryAt(allocation.destroyAttempts),
      expectedLeaseId: leaseId,
      attempt: "destroy",
      now: now(),
    });
  }

  async function destroyCurrent(
    allocation: SidecarAllocation,
    leaseId: string,
    provisioner: SidecarProvisioner,
  ): Promise<boolean> {
    if (allocation.sidecarId === undefined) return true;
    const sidecarId = allocation.sidecarId;
    try {
      const result = parseDestroyResult(
        await withLeaseHeartbeat(allocation, leaseId, () =>
          provisioner.destroy({
            allocationId: allocation.id,
            generation: allocation.generation,
            sidecarId,
            ...(allocation.externalRef !== undefined
              ? { externalRef: allocation.externalRef }
              : {}),
          }),
        ),
      );
      if (result.kind === "destroyed") return true;
    } catch (error) {
      logger.warn`Destroy failed for allocation ${allocation.id}: ${error instanceof Error ? error.message : String(error)}`;
    }
    await retryDestroy(allocation, leaseId);
    return false;
  }

  async function reconcile(
    allocation: SidecarAllocation,
    leaseId: string,
  ): Promise<void> {
    router.fenceAllocation(allocation.id, allocation.generation);
    if (allocation.status === "released" || allocation.status === "failed") {
      return;
    }
    const provisioner = provisionerFor(allocation);
    if (provisioner === null) {
      if (allocation.status === "pending") {
        await allocationStore.failWithoutInfrastructure({
          allocationId: allocation.id,
          expectedStatus: "pending",
          expectedGeneration: allocation.generation,
          code: "provisioner_unavailable",
          message: `Provisioner ${allocation.provisionerId} is unavailable or its binding changed`,
          expectedLeaseId: leaseId,
          now: now(),
        });
      } else {
        await allocationStore.scheduleRetry({
          allocationId: allocation.id,
          expectedStatus: allocation.status,
          expectedGeneration: allocation.generation,
          nextAttemptAt: retryAt(
            allocation.ensureAttempts + allocation.destroyAttempts,
          ),
          expectedLeaseId: leaseId,
          now: now(),
        });
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
      case "releasing":
        if (!(await destroyCurrent(allocation, leaseId, provisioner))) return;
        await allocationStore.markReleased({
          allocationId: allocation.id,
          generation: allocation.generation,
          expectedLeaseId: leaseId,
          now: now(),
        });
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
    for (const allocation of await allocationStore.listActive()) {
      router.fenceAllocation(allocation.id, allocation.generation);
      await allocationStore.wakeReconciliation(
        allocation.id,
        allocation.generation,
      );
    }
  }

  async function reconcileNext(): Promise<boolean> {
    const leaseId = createLeaseId();
    const allocation = await allocationStore.claimNextReconcilable({
      leaseId,
      leaseDurationMs,
    });
    if (allocation === null) return false;
    try {
      await reconcile(allocation, leaseId);
    } catch (error) {
      logger.error`Allocation ${allocation.id} reconciliation failed: ${error instanceof Error ? error.message : String(error)}`;
      await allocationStore.parkReconciliation(allocation.id, leaseId, {
        kind: "retry-after-error",
        notBefore: retryAt(MAX_RETRY_BACKOFF_ATTEMPT),
      });
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

  return { initialize, reconcileNext, reconcileUntilIdle };
}

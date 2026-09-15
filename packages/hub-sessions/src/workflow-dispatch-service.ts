import type {
  DBExecutor,
  EnqueueWorkflowRunDispatchArgs,
  EnqueueWorkflowRunDispatchResult,
  EnqueueWorkflowSignalDispatchArgs,
  SidecarAllocation,
  SidecarAllocationStore,
  WorkflowRunDispatchStore,
} from "@intx/db";
import { getLogger } from "@intx/log";
import { base64Encode, deriveWorkflowRunId, hexEncode } from "@intx/types";
import { SignalDeliverFrame } from "@intx/types/sidecar";

import type {
  AllocatedSidecarTarget,
  SidecarAllocationRouter,
} from "./ws/sidecar-handler";
import {
  runSidecarOperation,
  SidecarOperationTimeoutError,
} from "./sidecar-allocation/operation";

const logger = getLogger(["hub", "workflow-dispatch"]);

type DispatchStore = Pick<
  WorkflowRunDispatchStore,
  | "acknowledge"
  | "claimNextPending"
  | "enqueue"
  | "enqueueSignal"
  | "requeueUnsettled"
  | "scheduleRetry"
  | "settle"
>;

type AllocationStore = Pick<SidecarAllocationStore, "findByAnchorRunId">;

type DispatchRouter = Pick<
  SidecarAllocationRouter,
  "sendSignalDeliverToAllocation" | "sendWorkflowRunDispatchToAllocation"
>;

type ClaimedDispatch = NonNullable<
  Awaited<ReturnType<WorkflowRunDispatchStore["claimNextPending"]>>
>;

export type WorkflowDispatchAcknowledgement = {
  readonly allocationId: string;
  readonly anchorRunId: string;
  readonly generation: number;
  readonly messageId: string;
};

export type WorkflowDispatchService = {
  enqueue(
    args: EnqueueWorkflowRunDispatchArgs,
    tx?: DBExecutor,
  ): Promise<EnqueueWorkflowRunDispatchResult>;
  enqueueSignal(
    args: EnqueueWorkflowSignalDispatchArgs,
    tx?: DBExecutor,
  ): Promise<EnqueueWorkflowRunDispatchResult>;
  acknowledge(args: WorkflowDispatchAcknowledgement): Promise<void>;
  settle(anchorRunId: string, messageId: string): Promise<void>;
  requeueForReadyAllocation(anchorRunId: string): Promise<number>;
  reconcileNext(): Promise<boolean>;
  reconcileUntilIdle(maxIterations?: number): Promise<number>;
  wake(): void;
};

export type WorkflowDispatchServiceDeps = {
  readonly dispatchStore: DispatchStore;
  readonly allocationStore: AllocationStore;
  readonly router: DispatchRouter;
  /** Resolve the deployment anchor's durable routing address. */
  readonly resolveAnchorAddress: (
    anchorRunId: string,
  ) => Promise<string | null>;
  readonly leaseDurationMs?: number;
  readonly retryDelayMs?: (attempt: number) => number;
  readonly now?: () => Date;
  readonly createLeaseId?: () => string;
};

const DEFAULT_LEASE_DURATION_MS = 30_000;

function defaultRetryDelay(attempt: number): number {
  return Math.min(500 * 2 ** Math.min(attempt, 6), 30_000);
}

function randomLeaseId(): string {
  return `dispatch_lease_${hexEncode(crypto.getRandomValues(new Uint8Array(16)))}`;
}

function targetForReadyAllocation(
  allocation: SidecarAllocation,
): AllocatedSidecarTarget | null {
  if (
    allocation.status !== "allocated" ||
    allocation.ensureAcceptedGeneration !== allocation.generation ||
    allocation.connectDeadline !== undefined
  ) {
    return null;
  }
  return {
    allocationId: allocation.id,
    generation: allocation.generation,
  };
}

/**
 * Drives Hub-owned workflow triggers onto provisioned sidecars. The database
 * row is the delivery authority: websocket acceptance never deletes the raw
 * payload, and a generation replacement requeues every row that has not been
 * settled by the workflow-run Git claim-check.
 */
export function createWorkflowDispatchService({
  dispatchStore,
  allocationStore,
  router,
  resolveAnchorAddress,
  leaseDurationMs = DEFAULT_LEASE_DURATION_MS,
  retryDelayMs = defaultRetryDelay,
  now = () => new Date(),
  createLeaseId = randomLeaseId,
}: WorkflowDispatchServiceDeps): WorkflowDispatchService {
  if (!Number.isSafeInteger(leaseDurationMs) || leaseDurationMs <= 0) {
    throw new Error("leaseDurationMs must be a positive integer");
  }

  let drainPromise: Promise<void> | null = null;
  const activeDispatches = new Set<string>();

  function retryAt(attempt: number): Date {
    return new Date(now().getTime() + retryDelayMs(attempt));
  }

  async function retry(
    dispatch: ClaimedDispatch,
    leaseId: string,
    code: string,
    message: string,
  ): Promise<void> {
    await dispatchStore.scheduleRetry({
      dispatchId: dispatch.id,
      nextAttemptAt: retryAt(dispatch.attemptCount),
      code,
      message,
      expectedLeaseId: leaseId,
      now: now(),
    });
  }

  async function reconcileNext(): Promise<boolean> {
    const leaseId = createLeaseId();
    const claimStartedAt = performance.now();
    const dispatch = await dispatchStore.claimNextPending({
      leaseId,
      leaseDurationMs,
      excludedDispatchIds: [...activeDispatches],
    });
    if (dispatch === null) return false;

    // A concurrent claim can outlive both its exclusion snapshot and the old
    // lease. Do not start another delivery while its original I/O is pending.
    if (activeDispatches.has(dispatch.id)) return true;
    const remaining = leaseDurationMs - (performance.now() - claimStartedAt);
    if (remaining <= 0) return true;

    activeDispatches.add(dispatch.id);
    try {
      await runSidecarOperation(
        "Workflow dispatch",
        Math.ceil(remaining),
        async (signal) => {
          try {
            await deliver(dispatch, leaseId, signal);
          } finally {
            // The deadline frees the drain, but unfinished I/O keeps this
            // dispatch excluded until it actually settles.
            activeDispatches.delete(dispatch.id);
          }
        },
      );
    } catch (error) {
      if (!(error instanceof SidecarOperationTimeoutError)) throw error;
      // The durable lease expires independently. Do not start another database
      // write here: a stuck retry write must not occupy the freed drain either.
      logger.warn`Dispatch ${dispatch.id} stopped at its delivery deadline`;
    }
    return true;
  }

  async function deliver(
    dispatch: ClaimedDispatch,
    leaseId: string,
    signal: AbortSignal,
  ): Promise<void> {
    const allocation = await allocationStore.findByAnchorRunId(
      dispatch.anchorRunId,
    );
    signal.throwIfAborted();
    if (allocation === null) {
      await retry(
        dispatch,
        leaseId,
        "allocation_missing",
        `No sidecar allocation exists for workflow anchor ${dispatch.anchorRunId}`,
      );
      return;
    }
    const target = targetForReadyAllocation(allocation);
    if (target === null) {
      await retry(
        dispatch,
        leaseId,
        "allocation_not_ready",
        `Sidecar allocation ${allocation.id} is not ready for delivery`,
      );
      return;
    }
    const agentAddress = await resolveAnchorAddress(dispatch.anchorRunId);
    signal.throwIfAborted();
    if (agentAddress === null) {
      await retry(
        dispatch,
        leaseId,
        "anchor_address_missing",
        `Workflow anchor ${dispatch.anchorRunId} has no routing address`,
      );
      return;
    }

    try {
      if (dispatch.kind === "signal") {
        const frame = SignalDeliverFrame.assert(
          JSON.parse(new TextDecoder().decode(dispatch.rawMessage)),
        );
        await router.sendSignalDeliverToAllocation(
          target,
          {
            agentAddress: frame.agentAddress,
            runId: frame.runId,
            signalName: frame.signalName,
            signalId: frame.signalId,
            payload: frame.payload,
          },
          signal,
        );
      } else {
        // A deliverable mail dispatch always carries the sender persisted at
        // enqueue (the workflow_run_dispatch mail-sender check enforces it).
        // A null here means the row bypassed that invariant, so fail loudly
        // rather than deliver with no authenticated sender or fall back to the
        // MIME From.
        if (dispatch.senderAddress === null) {
          throw new Error(
            `mail dispatch ${dispatch.id} has no persisted authenticated sender`,
          );
        }
        await router.sendWorkflowRunDispatchToAllocation(
          target,
          agentAddress,
          deriveWorkflowRunId(agentAddress),
          dispatch.stepGrants,
          base64Encode(dispatch.rawMessage),
          dispatch.senderAddress,
          dispatch.messageId,
          signal,
        );
      }
      // Keep the delivery lease until the sidecar acknowledges its durable
      // inbox write. If that ack never arrives, lease expiry makes the same
      // immutable payload claimable again.
    } catch (error) {
      signal.throwIfAborted();
      await retry(
        dispatch,
        leaseId,
        "dispatch_unroutable",
        error instanceof Error ? error.message : String(error),
      );
    }
  }

  async function reconcileUntilIdle(maxIterations = 100): Promise<number> {
    let reconciled = 0;
    while (reconciled < maxIterations && (await reconcileNext())) {
      reconciled += 1;
    }
    return reconciled;
  }

  function wake(): void {
    if (drainPromise !== null) return;
    drainPromise = Promise.resolve()
      .then(async () => {
        await reconcileUntilIdle();
      })
      .catch((error: unknown) => {
        logger.error`Workflow dispatch reconciliation failed: ${error instanceof Error ? error.message : String(error)}`;
      })
      .finally(() => {
        drainPromise = null;
      });
  }

  return {
    async enqueue(args, tx) {
      const result = await dispatchStore.enqueue(args, tx);
      wake();
      return result;
    },

    async enqueueSignal(args, tx) {
      const result = await dispatchStore.enqueueSignal(args, tx);
      wake();
      return result;
    },

    async acknowledge(args) {
      await dispatchStore.acknowledge({
        ...args,
        now: now(),
      });
    },

    async settle(anchorRunId, messageId) {
      await dispatchStore.settle(anchorRunId, messageId, now());
    },

    async requeueForReadyAllocation(anchorRunId) {
      const count = await dispatchStore.requeueUnsettled(anchorRunId);
      wake();
      return count;
    },

    reconcileNext,
    reconcileUntilIdle,
    wake,
  };
}

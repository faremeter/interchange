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
import { base64Encode, hexEncode } from "@intx/types";
import { SignalDeliverFrame } from "@intx/types/sidecar";

import type {
  AllocatedSidecarTarget,
  SidecarAllocationRouter,
} from "./ws/sidecar-handler";

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
 * Drives Hub-owned workflow triggers onto exclusive sidecars. The database
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
  if (leaseDurationMs <= 0) {
    throw new Error("leaseDurationMs must be positive");
  }

  let drainPromise: Promise<void> | null = null;

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
    const dispatch = await dispatchStore.claimNextPending({
      leaseId,
      leaseDurationMs,
    });
    if (dispatch === null) return false;

    const allocation = await allocationStore.findByAnchorRunId(
      dispatch.anchorRunId,
    );
    if (allocation === null) {
      await retry(
        dispatch,
        leaseId,
        "allocation_missing",
        `No sidecar allocation exists for workflow anchor ${dispatch.anchorRunId}`,
      );
      return true;
    }
    const target = targetForReadyAllocation(allocation);
    if (target === null) {
      await retry(
        dispatch,
        leaseId,
        "allocation_not_ready",
        `Sidecar allocation ${allocation.id} is not ready for delivery`,
      );
      return true;
    }
    const agentAddress = await resolveAnchorAddress(dispatch.anchorRunId);
    if (agentAddress === null) {
      await retry(
        dispatch,
        leaseId,
        "anchor_address_missing",
        `Workflow anchor ${dispatch.anchorRunId} has no routing address`,
      );
      return true;
    }

    try {
      if (dispatch.kind === "signal") {
        const signal = SignalDeliverFrame.assert(
          JSON.parse(new TextDecoder().decode(dispatch.rawMessage)),
        );
        await router.sendSignalDeliverToAllocation(target, {
          agentAddress: signal.agentAddress,
          runId: signal.runId,
          signalName: signal.signalName,
          signalId: signal.signalId,
          payload: signal.payload,
        });
      } else {
        await router.sendWorkflowRunDispatchToAllocation(
          target,
          agentAddress,
          // Every trigger of a deployment uses its stable mail address as the
          // supervisor run id.
          agentAddress,
          dispatch.stepGrants,
          base64Encode(dispatch.rawMessage),
          dispatch.messageId,
        );
      }
      // Keep the delivery lease until the sidecar acknowledges its durable
      // inbox write. If that ack never arrives, lease expiry makes the same
      // immutable payload claimable again.
    } catch (error) {
      await retry(
        dispatch,
        leaseId,
        "dispatch_unroutable",
        error instanceof Error ? error.message : String(error),
      );
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

import {
  and,
  asc,
  desc,
  eq,
  gt,
  inArray,
  isNotNull,
  isNull,
  lte,
  notInArray,
  or,
  sql,
  type SQL,
} from "drizzle-orm";
import type { AnyPgColumn } from "drizzle-orm/pg-core";

import {
  createSidecarAllocationStore,
  createWorkflowPendingProjectionStore,
  createWorkflowRunStore,
  createWorkflowRunDispatchStore,
  parseWorkflowRunRow,
  workflowCapacityReleaseAt,
  SIDECAR_CLEANUP_RETRY_EXHAUSTED_FAILURE_CODE,
  SIDECAR_CLEANUP_DISCONNECT_TIMEOUT_FAILURE_CODE,
  type DB,
  type DBExecutor,
  type BeginSidecarReleaseArgs,
  type SidecarAllocation,
} from "@intx/db";
import {
  liveWorkflowRunStatuses,
  isLiveWorkflowRunStatus,
  principal,
  sidecarAllocation,
  workflowPendingProjection,
  workflowRun,
} from "@intx/db/schema";
import { getLogger } from "@intx/log";
import { isSidecarAllocationDispatchable } from "@intx/types";

import type { WorkflowHistoryReceiveTracker } from "./workflow-history-receives";
import type { WorkflowRunReader } from "./workflow-run-reader";
import {
  WorkflowControlRejectedError,
  WorkflowControlTimeoutError,
  type AllocatedSidecarTarget,
  type SidecarAllocationRouter,
} from "./ws/sidecar-handler";
import {
  classifyTerminalEvent,
  workflowRunRepoIdForAddress,
  WORKFLOW_RUN_REF,
} from "./workflow-run-kind";
import { projectTerminalRun } from "./workflow-run-terminal-projection";
import {
  createAnchorRunSweep,
  type AnchorRunSweepQueries,
} from "./anchor-run-sweep";

const logger = getLogger(["hub", "workflow-lifecycle"]);
const SWEEP_INTERVAL_MS = 1_000;
// A receive clears its own pending projection within one pack transfer. The
// sweep leaves younger rows to it so healthy deployments cost no Git reads.
const PENDING_PROJECTION_GRACE_MS = 30_000;
// A moving tip is reread a few times; after that the next pass retries, so a
// deployment that pushes constantly cannot hold a caller here.
const MAX_RECOVERY_ATTEMPTS = 3;
const RECOVERY_BACKOFF_MIN_MS = 5_000;
const RECOVERY_BACKOFF_MAX_MS = 5 * 60_000;
const CANCEL_CONTROL_TIMEOUT_MS = 5_000;
const STOP_CONTROL_TIMEOUT_MS = 10_000;
const MAX_CONCURRENT_RETAINS_PER_SIDECAR = 8;

type Run = ReturnType<typeof parseWorkflowRunRow>;
type Allocation = typeof sidecarAllocation.$inferSelect;
type ReleaseResult =
  | "pending"
  | "released"
  | "live"
  | "history_pending"
  | "not_found"
  | "cleanup_exhausted"
  | "cleanup_disconnected"
  | "cleanup_failed";
// `request` is an explicit caller waiting on the answer, so it reads Git even
// while the sweep is backing off; `stop` and `sweep` respect the backoff, and
// only `sweep` leaves young rows to the receive that wrote them.
type RecoveryTrigger = "request" | "stop" | "sweep";

export type WorkflowLifecycleServiceDeps = {
  db: DB["db"];
  retentionRouter: Pick<
    SidecarAllocationRouter,
    | "retainAllocation"
    | "getAllocationRetention"
    | "getRetentionCandidates"
    | "holdsAllocatedBinding"
    | "fenceAllocation"
  >;
  runReader: WorkflowRunReader;
  historyReceives: WorkflowHistoryReceiveTracker;
  now?: () => Date;
  cancelGraceMs?: number;
  unreachableStopGraceMs?: number;
  sendControl?: SidecarAllocationRouter["sendWorkflowControl"];
};

export function createWorkflowLifecycleService({
  db,
  retentionRouter,
  runReader,
  historyReceives,
  now = () => new Date(),
  cancelGraceMs = 30_000,
  unreachableStopGraceMs = 60_000,
  sendControl,
}: WorkflowLifecycleServiceDeps) {
  if (cancelGraceMs < 0 || !Number.isFinite(cancelGraceMs))
    throw new Error("Cancellation grace must be finite and non-negative");
  if (unreachableStopGraceMs < 0 || !Number.isFinite(unreachableStopGraceMs))
    throw new Error("Unreachable stop grace must be finite and non-negative");
  // Connections are rebuilt after a restart, so an unreachable worker gets the
  // full grace from this service's start before its capacity is reclaimed.
  const startedAt = now().getTime();
  const allocations = createSidecarAllocationStore(db);
  const dispatches = createWorkflowRunDispatchStore(db);
  const runs = createWorkflowRunStore(db);
  const pendingProjections = createWorkflowPendingProjectionStore(db);
  const recoveryBackoff = new Map<
    string,
    { failures: number; nextAt: number; pendingIds: readonly string[] }
  >();
  const retentionBackoff = new Map<
    string,
    {
      generation: number;
      failures: number;
      nextAt: number;
    }
  >();
  const retentionsInFlight = new Set<string>();
  const retentionsPerSidecar = new Map<string, number>();

  function hasRetentionRoom(sidecarId: string): boolean {
    return (
      (retentionsPerSidecar.get(sidecarId) ?? 0) <
      MAX_CONCURRENT_RETAINS_PER_SIDECAR
    );
  }

  async function withRun<T>(
    tenantId: string,
    runId: string,
    action: (
      tx: DBExecutor,
      run: Run,
      allocation: Allocation | undefined,
      release: (
        args: BeginSidecarReleaseArgs,
      ) => Promise<SidecarAllocation | null>,
    ) => Promise<T>,
  ) {
    const releases: SidecarAllocation[] = [];
    const result = await db.transaction(async (tx) => {
      // Match pack ingestion's lock order: allocation row first.
      const [allocation] = await tx
        .select()
        .from(sidecarAllocation)
        .where(
          and(
            eq(sidecarAllocation.anchorRunId, runId),
            eq(sidecarAllocation.tenantId, tenantId),
          ),
        )
        .for("update");
      const [row] = await tx
        .select()
        .from(workflowRun)
        .where(
          and(
            eq(workflowRun.id, runId),
            eq(workflowRun.tenantId, tenantId),
            eq(workflowRun.anchorRunId, runId),
          ),
        )
        .for("update");
      if (row === undefined) return null;
      return action(tx, parseWorkflowRunRow(row), allocation, async (args) => {
        const released = await allocations.beginRelease(args, tx);
        if (released !== null) releases.push(released);
        return released;
      });
    });
    for (const released of releases)
      retentionRouter.fenceAllocation(released.id, released.generation, {
        ...(released.sidecarId === undefined
          ? {}
          : { cleanup: { sidecarId: released.sidecarId } }),
      });
    return result;
  }

  // Project accepted history for the deployment and claim the pending rows
  // that history covers. Returns false when Git moved after the read, so the
  // read may have missed a receive and nothing was claimed.
  async function reconcileFromGit(
    tenantId: string,
    runId: string,
    anchor: { address: string | null; definitionId: string; createdAt: Date },
    claimable: readonly { id: string; historyRequired: boolean }[],
  ): Promise<boolean> {
    const settled = await db
      .select({ id: workflowRun.id })
      .from(workflowRun)
      .where(
        and(
          eq(workflowRun.anchorRunId, runId),
          notInArray(workflowRun.status, [...liveWorkflowRunStatuses]),
        ),
      );
    const settledIds = new Set(settled.map((row) => row.id));
    const repoId =
      anchor.address === null
        ? null
        : workflowRunRepoIdForAddress(anchor.address);
    // Settled rows are final, so only live runs and runs without a row can
    // still be missing an accepted outcome.
    const history: Awaited<
      ReturnType<WorkflowRunReader["readLatestRunEvents"]>
    > =
      repoId === null
        ? { tip: null, events: new Map() }
        : await runReader.readLatestRunEvents(
            repoId,
            WORKFLOW_RUN_REF,
            (id) => !settledIds.has(id),
          );
    // A receive writes the ref last and nothing removes refs, so a repository
    // without one never advanced, while a missing repository means accepted
    // history is unavailable, not empty.
    if (
      history.tip === null &&
      claimable.some((row) => row.historyRequired) &&
      (repoId === null || !(await runReader.hasRepository(repoId)))
    )
      throw new Error("Accepted workflow history is missing from the Hub");
    // Only the allocation row is locked: pack ingestion holds it while
    // advancing Git, so an unchanged tip under it proves the read covered every
    // claimed receive. Locking the anchor row as well would deadlock against a
    // receive minting a child row, whose foreign key needs it.
    return db.transaction(async (tx) => {
      await tx
        .select({ id: sidecarAllocation.id })
        .from(sidecarAllocation)
        .where(eq(sidecarAllocation.anchorRunId, runId))
        .for("update");
      if (
        repoId !== null &&
        (await runReader.resolveRefTip(repoId, WORKFLOW_RUN_REF)) !==
          history.tip
      )
        return false;
      for (const [id, event] of history.events) {
        if (event === null) continue;
        const terminal = classifyTerminalEvent(event.type);
        if (!terminal.terminal) continue;
        const outcome = await projectTerminalRun(tx, runs, {
          anchor: {
            id: runId,
            tenantId,
            definitionId: anchor.definitionId,
            createdAt: anchor.createdAt,
          },
          runId: id,
          status: terminal.status,
          terminalEvent: event.body,
          now: now(),
        });
        if (outcome === "foreign")
          logger.error`Ignoring terminal event for run ${id}: it does not belong to deployment ${runId}`;
      }
      await pendingProjections.claim(
        runId,
        claimable.map((row) => row.id),
        tx,
      );
      return true;
    });
  }

  // Reconcile a deployment whose run rows may disagree with accepted Git
  // history. Failures back off per deployment; callers act on whatever the
  // database records, and a forced stop waits while rows remain.
  async function recoverHistory(
    tenantId: string,
    runId: string,
    trigger: RecoveryTrigger,
  ): Promise<void> {
    // Keyed by tenant too, so a request naming another tenant's run cannot
    // reset that run's backoff.
    const backoffKey = JSON.stringify([tenantId, runId]);
    const backoff = recoveryBackoff.get(backoffKey);
    if (
      trigger !== "request" &&
      backoff !== undefined &&
      now().getTime() < backoff.nextAt
    )
      return;
    const grace = trigger === "sweep" ? PENDING_PROJECTION_GRACE_MS : 0;
    let attempted: readonly string[] = [];
    try {
      for (let attempt = 0; attempt < MAX_RECOVERY_ATTEMPTS; attempt += 1) {
        const listed = await pendingProjections.list(runId);
        // Only receives whose Git transaction has ended can be claimed; one
        // still running may advance Git after the read.
        const claimable = listed.filter(
          (row) => !historyReceives.isInFlight(row.id),
        );
        const cutoff = now().getTime() - grace;
        if (!claimable.some((row) => row.createdAt.getTime() <= cutoff)) {
          recoveryBackoff.delete(backoffKey);
          return;
        }
        attempted = claimable.map((row) => row.id);
        const anchor = await db.query.workflowRun.findFirst({
          where: and(
            eq(workflowRun.id, runId),
            eq(workflowRun.tenantId, tenantId),
            eq(workflowRun.anchorRunId, runId),
          ),
          columns: { address: true, definitionId: true, createdAt: true },
        });
        if (anchor === undefined) {
          recoveryBackoff.delete(backoffKey);
          return;
        }
        const claimed = await reconcileFromGit(
          tenantId,
          runId,
          anchor,
          claimable,
        );
        if (claimed) {
          if (backoff !== undefined)
            logger.info`Reconciled accepted history for ${runId}`;
          recoveryBackoff.delete(backoffKey);
          return;
        }
      }
    } catch (error) {
      // Failures are consecutive only while history that already failed is
      // still pending; once a receive settles it, the next failure starts over.
      const failures =
        backoff !== undefined &&
        (attempted.length === 0 ||
          attempted.some((id) => backoff.pendingIds.includes(id)))
          ? backoff.failures + 1
          : 1;
      const delay = Math.min(
        RECOVERY_BACKOFF_MAX_MS,
        RECOVERY_BACKOFF_MIN_MS * 2 ** (failures - 1),
      );
      recoveryBackoff.set(backoffKey, {
        failures,
        nextAt: now().getTime() + delay,
        pendingIds:
          attempted.length === 0 ? (backoff?.pendingIds ?? []) : attempted,
      });
      const message = error instanceof Error ? error.message : String(error);
      if (failures === 1)
        logger.warn`Cannot reconcile accepted history for ${runId}; its outcomes are recorded once it can: ${message}`;
      else if (
        delay === RECOVERY_BACKOFF_MAX_MS &&
        RECOVERY_BACKOFF_MIN_MS * 2 ** (failures - 2) < RECOVERY_BACKOFF_MAX_MS
      )
        logger.error`Accepted history for ${runId} is still unreconciled after ${failures} attempts; run statuses may be stale and a forced stop stays unrecorded: ${message}`;
    }
  }

  async function getStatus(tenantId: string, runId: string) {
    const row = await db.query.workflowRun.findFirst({
      where: and(
        eq(workflowRun.id, runId),
        eq(workflowRun.tenantId, tenantId),
        eq(workflowRun.anchorRunId, runId),
      ),
    });
    if (row === undefined) return null;
    const run = parseWorkflowRunRow(row);
    const allocation = await allocations.findByAnchorRunId(runId);
    return {
      runId,
      status: run.status,
      policy: run.lifecyclePolicy ?? {},
      expiresAt: run.expiresAt?.toISOString() ?? null,
      cancellationRequestedAt:
        run.cancellationRequestedAt?.toISOString() ?? null,
      cancellationDeadline: run.cancellationDeadline?.toISOString() ?? null,
      cancellationReason: run.cancellationReason ?? null,
      capacityReleaseAt: run.capacityReleaseAt?.toISOString() ?? null,
      allocation:
        allocation === null
          ? null
          : {
              id: allocation.id,
              status: allocation.status,
              failureCode: allocation.failureCode ?? null,
              failureMessage: allocation.failureMessage ?? null,
            },
    };
  }

  async function releaseCapacity(
    tenantId: string,
    runId: string,
  ): Promise<ReleaseResult> {
    await recoverHistory(tenantId, runId, "request");
    const result = await withRun(
      tenantId,
      runId,
      async (tx, run, allocation): Promise<ReleaseResult> => {
        if (allocation?.status === "destroy_failed") {
          if (
            allocation.failureCode ===
            SIDECAR_CLEANUP_DISCONNECT_TIMEOUT_FAILURE_CODE
          )
            return "cleanup_disconnected";
          return allocation.failureCode ===
            SIDECAR_CLEANUP_RETRY_EXHAUSTED_FAILURE_CODE
            ? "cleanup_exhausted"
            : "cleanup_failed";
        }
        // A pending projection may hold the anchor's or a child's accepted
        // terminal outcome, even when the anchor row is already terminal.
        if (await pendingProjections.hasAny(runId, tx))
          return "history_pending";
        if (isLiveWorkflowRunStatus(run.status)) return "live";
        if (
          allocation === undefined ||
          allocation.status === "released" ||
          allocation.status === "failed"
        )
          return "released";
        const requestedAt = now();
        if (
          run.capacityReleaseAt === null ||
          run.capacityReleaseAt > requestedAt
        ) {
          await tx
            .update(workflowRun)
            .set({ capacityReleaseAt: requestedAt })
            .where(eq(workflowRun.id, runId));
        }
        return "pending";
      },
    );
    return result ?? "not_found";
  }

  async function beginCancellation(
    tx: DBExecutor,
    run: Run,
    reason: string,
  ): Promise<Run> {
    if (run.cancellationRequestedAt !== null) return run;
    const requestedAt = now();
    const deadline = new Date(requestedAt.getTime() + cancelGraceMs);
    await tx
      .update(workflowRun)
      .set({
        cancellationRequestedAt: requestedAt,
        cancellationDeadline: deadline,
        cancellationReason: reason,
      })
      .where(eq(workflowRun.id, run.id));
    return {
      ...run,
      cancellationRequestedAt: requestedAt,
      cancellationDeadline: deadline,
      cancellationReason: reason,
    };
  }

  function getRequestedCancellation(run: Run): {
    deadline: Date;
    reason: string;
  } {
    // beginCancellation writes the request, deadline, and reason together.
    if (run.cancellationDeadline === null || run.cancellationReason === null)
      throw new Error(
        `Workflow run ${run.id} has an incomplete cancellation request`,
      );
    return {
      deadline: run.cancellationDeadline,
      reason: run.cancellationReason,
    };
  }

  async function requestCancellation(
    tenantId: string,
    runId: string,
    reason: string,
  ): Promise<"pending" | "terminal" | "not_found"> {
    await recoverHistory(tenantId, runId, "request");
    const result = await withRun(tenantId, runId, async (tx, run) => {
      if (!isLiveWorkflowRunStatus(run.status)) return "terminal" as const;
      await beginCancellation(tx, run, reason);
      return "pending" as const;
    });
    return result ?? "not_found";
  }

  async function markStopped(tx: DBExecutor, run: Run): Promise<void> {
    // Forced termination may leave only a partial event log. The Hub records
    // the outcome after the worker confirms its stop or the provisioner confirms
    // destruction; it never reports cancellation while an unfenced worker lives.
    // A pending projection means accepted history may hold an outcome a live
    // row does not show yet, so the stop is recorded only after recovery clears
    // it. The allocation lock keeps a new receive from advancing Git meanwhile.
    if (await pendingProjections.hasAny(run.id, tx)) return;
    const endedAt = now();
    const stopped = await tx
      .update(workflowRun)
      .set({ status: "cancelled", endedAt })
      .where(
        and(
          eq(workflowRun.anchorRunId, run.id),
          inArray(workflowRun.status, [...liveWorkflowRunStatuses]),
        ),
      )
      .returning({ principalId: workflowRun.principalId });
    const principalIds = stopped.flatMap((row) =>
      row.principalId === null ? [] : [row.principalId],
    );
    if (principalIds.length > 0)
      await tx
        .update(principal)
        .set({ status: "deactivated", updatedAt: endedAt })
        .where(inArray(principal.id, principalIds));
    await dispatches.abandonUnsettled(
      run.id,
      "workflow_cancelled",
      getRequestedCancellation(run).reason,
      endedAt,
      tx,
    );
  }

  type ControlRequest = {
    target: AllocatedSidecarTarget;
    tenantId: string;
    runId: string;
    agentAddress: string;
    action: "cancel" | "stop";
    reason: string;
    timeoutMs: number;
    cancellationDeadline: Date;
  };

  async function forceRelease(command: ControlRequest): Promise<void> {
    await withRun(
      command.tenantId,
      command.runId,
      async (_tx, run, allocation, release) => {
        if (
          !isLiveWorkflowRunStatus(run.status) ||
          allocation === undefined ||
          allocation.generation !== command.target.generation ||
          !isSidecarAllocationDispatchable(allocation.status)
        )
          return;
        await release({
          allocationId: allocation.id,
          expectedGeneration: allocation.generation,
          expectedStatus: allocation.status,
          failureCode: "workflow_stop_failed",
          failureMessage:
            "Workflow stop could not be confirmed; reclaiming capacity",
          now: now(),
        });
      },
    );
  }

  async function recordConfirmedStop(command: ControlRequest): Promise<void> {
    // An outcome that cannot be recorded yet is retried by the next sweep,
    // which repeats the stop.
    await recoverHistory(command.tenantId, command.runId, "stop");
    await withRun(
      command.tenantId,
      command.runId,
      async (tx, run, allocation) => {
        if (
          allocation?.generation === command.target.generation &&
          isLiveWorkflowRunStatus(run.status)
        )
          await markStopped(tx, run);
      },
    );
  }

  // Pack ingestion holds a live run's allocation lock for a whole receive, so
  // a live run with nothing due skips the locked pass. Retention confirmation
  // also needs no write; terminal runs lock only to derive or begin release.
  async function needsLockedPass(
    tenantId: string,
    runId: string,
  ): Promise<boolean> {
    const run = await db.query.workflowRun.findFirst({
      where: and(
        eq(workflowRun.id, runId),
        eq(workflowRun.tenantId, tenantId),
        eq(workflowRun.anchorRunId, runId),
      ),
      columns: {
        status: true,
        expiresAt: true,
        cancellationRequestedAt: true,
        infrastructureFailedAt: true,
        capacityReleaseAt: true,
        endedAt: true,
        lifecyclePolicy: true,
      },
    });
    if (run === undefined) return false;
    if (run.infrastructureFailedAt !== null) return true;
    if (!isLiveWorkflowRunStatus(run.status)) {
      return run.capacityReleaseAt === null
        ? workflowCapacityReleaseAt(run) !== null
        : run.capacityReleaseAt <= now();
    }
    return (
      run.cancellationRequestedAt !== null ||
      (run.expiresAt !== null && run.expiresAt <= now())
    );
  }

  async function retainTerminalAllocation(tenantId: string, runId: string) {
    const [allocation] = await db
      .select({
        id: sidecarAllocation.id,
        sidecarId: sidecarAllocation.sidecarId,
        generation: sidecarAllocation.generation,
        status: workflowRun.status,
        endedAt: workflowRun.endedAt,
        capacityReleaseAt: workflowRun.capacityReleaseAt,
        lifecyclePolicy: workflowRun.lifecyclePolicy,
      })
      .from(sidecarAllocation)
      .innerJoin(workflowRun, eq(workflowRun.id, sidecarAllocation.anchorRunId))
      .where(
        and(
          eq(workflowRun.id, runId),
          eq(workflowRun.tenantId, tenantId),
          notInArray(workflowRun.status, [...liveWorkflowRunStatuses]),
          isNotNull(workflowRun.publicKey),
          eq(sidecarAllocation.status, "allocated"),
          eq(
            sidecarAllocation.ensureAcceptedGeneration,
            sidecarAllocation.generation,
          ),
          isNull(sidecarAllocation.initializationLeaseId),
        ),
      );
    if (allocation === undefined || allocation.sidecarId === null)
      return undefined;
    const target = {
      allocationId: allocation.id,
      generation: allocation.generation,
    };
    if (!retentionRouter.holdsAllocatedBinding(target)) return undefined;
    const retention = retentionRouter.getAllocationRetention(target);
    const releaseAt = workflowCapacityReleaseAt(allocation);
    // Overflow and policy expiry retain the existing accepted-history release
    // policy. A kept decision alone does not prove the Hub received final history.
    if (retention === "refused" || (releaseAt !== null && releaseAt <= now()))
      return retention === undefined ? undefined : { ...target, retention };
    const previous = retentionBackoff.get(target.allocationId);
    const backoff =
      previous?.generation === target.generation ? previous : undefined;
    if (backoff !== undefined && now().getTime() < backoff.nextAt)
      return undefined;
    if (
      !retentionsInFlight.has(target.allocationId) &&
      hasRetentionRoom(allocation.sidecarId)
    ) {
      retentionsInFlight.add(target.allocationId);
      retentionsPerSidecar.set(
        allocation.sidecarId,
        (retentionsPerSidecar.get(allocation.sidecarId) ?? 0) + 1,
      );
      void requestRetention(
        target,
        allocation.sidecarId,
        runId,
        backoff?.failures ?? 0,
      );
    }
    return retention === undefined ? undefined : { ...target, retention };
  }

  async function requestRetention(
    target: AllocatedSidecarTarget,
    sidecarId: string,
    runId: string,
    previousFailures: number,
  ): Promise<void> {
    try {
      await retentionRouter.retainAllocation(target, STOP_CONTROL_TIMEOUT_MS);
      retentionBackoff.delete(target.allocationId);
    } catch (cause) {
      const failures = previousFailures + 1;
      retentionBackoff.set(target.allocationId, {
        generation: target.generation,
        failures,
        nextAt:
          now().getTime() +
          Math.min(
            RECOVERY_BACKOFF_MAX_MS,
            RECOVERY_BACKOFF_MIN_MS * 2 ** Math.min(failures - 1, 6),
          ),
      });
      logger.warn`Retention remains unconfirmed for ${runId}: ${cause instanceof Error ? cause.message : String(cause)}`;
    } finally {
      retentionsInFlight.delete(target.allocationId);
      const count = retentionsPerSidecar.get(sidecarId);
      if (count === undefined || count < 1) {
        logger.error`Missing in-flight retention count for sidecar ${sidecarId} after retaining ${runId}`;
      } else if (count === 1) {
        retentionsPerSidecar.delete(sidecarId);
      } else {
        retentionsPerSidecar.set(sidecarId, count - 1);
      }
    }
  }

  async function reconcileRun(tenantId: string, runId: string): Promise<void> {
    // Request retention before recovering history, but keep network waits out
    // of the lifecycle workers so a silent sidecar cannot delay other runs'
    // cancellation or expiry. The next sweep observes the router's confirmed
    // retention, including refusals requiring release.
    let retained: Awaited<ReturnType<typeof retainTerminalAllocation>>;
    try {
      retained = await retainTerminalAllocation(tenantId, runId);
    } catch (cause) {
      logger.warn`Retention remains unconfirmed for ${runId}: ${cause instanceof Error ? cause.message : String(cause)}`;
    }
    await recoverHistory(tenantId, runId, "sweep");
    if (
      retained?.retention !== "refused" &&
      !(await needsLockedPass(tenantId, runId))
    )
      return;
    const command = await withRun(
      tenantId,
      runId,
      async (
        tx,
        original,
        allocation,
        release,
      ): Promise<ControlRequest | null> => {
        // Capacity was lost for good while accepted history was unreconciled;
        // once recovery has recorded that history, the runs still live fail.
        if (original.infrastructureFailedAt !== null) {
          await allocations.applyDeferredInfrastructureFailure(runId, tx);
          return null;
        }
        let run = original;
        if (isLiveWorkflowRunStatus(run.status)) {
          if (run.expiresAt !== null && run.expiresAt <= now()) {
            run = await beginCancellation(
              tx,
              run,
              "Maximum deployment lifetime exceeded",
            );
          }
          if (run.cancellationRequestedAt === null) return null;
          if (
            allocation === undefined ||
            allocation.status === "released" ||
            allocation.status === "failed"
          ) {
            await markStopped(tx, run);
            return null;
          }
          if (
            allocation.status === "releasing" ||
            allocation.status === "destroy_failed"
          )
            return null;
          if (
            allocation.status !== "allocated" ||
            allocation.sidecarId === null ||
            run.address === null
          ) {
            await release({
              allocationId: allocation.id,
              expectedStatus: allocation.status,
              expectedGeneration: allocation.generation,
              now: now(),
            });
            return null;
          }
          const cancellation = getRequestedCancellation(run);
          const remaining = cancellation.deadline.getTime() - now().getTime();
          return {
            target: {
              allocationId: allocation.id,
              generation: allocation.generation,
            },
            tenantId,
            runId,
            agentAddress: run.address,
            action: remaining > 0 ? "cancel" : "stop",
            reason: cancellation.reason,
            timeoutMs:
              remaining > 0
                ? Math.max(1, Math.min(CANCEL_CONTROL_TIMEOUT_MS, remaining))
                : STOP_CONTROL_TIMEOUT_MS,
            cancellationDeadline: cancellation.deadline,
          };
        }
        if (
          allocation === undefined ||
          !isSidecarAllocationDispatchable(allocation.status)
        )
          return null;
        if (run.status === "deployed" || run.status === "running") return null;
        const refusal =
          retained?.retention === "refused" &&
          retained.allocationId === allocation.id &&
          retained.generation === allocation.generation
            ? {
                failureCode: "sidecar_retention_limit_exceeded",
                failureMessage:
                  "The sidecar could not retain this copy because its kept-record limit was reached",
              }
            : undefined;
        if (
          refusal !== undefined &&
          !(await pendingProjections.hasAny(runId, tx))
        ) {
          await release({
            allocationId: allocation.id,
            expectedStatus: allocation.status,
            expectedGeneration: allocation.generation,
            ...refusal,
            now: now(),
          });
          return null;
        }
        const releaseAt = workflowCapacityReleaseAt(run);
        if (releaseAt === null) return null;
        if (run.capacityReleaseAt === null)
          await tx
            .update(workflowRun)
            .set({ capacityReleaseAt: releaseAt })
            .where(eq(workflowRun.id, runId));
        if (releaseAt > now()) return null;
        await release({
          allocationId: allocation.id,
          expectedStatus: allocation.status,
          expectedGeneration: allocation.generation,
          ...refusal,
          now: now(),
        });
        return null;
      },
    );
    if (command === null) return;
    if (sendControl === undefined) {
      logger.warn`Workflow control is unavailable for ${runId}`;
      if (command.action === "stop") await forceRelease(command);
      return;
    }
    try {
      await sendControl(
        command.target,
        {
          runId,
          agentAddress: command.agentAddress,
          action: command.action,
          reason: command.reason,
        },
        command.timeoutMs,
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      // A live worker that stayed silent or refused has failed to stop. Any
      // other failure is retried briefly, since this Hub may not hold the
      // worker's connection yet, the worker may still be deploying, its
      // acknowledgement may not have been processed in time, or its history
      // may not have reached the Hub yet. Once the grace passes, no failure
      // can postpone the stop further.
      const failedDefinitively =
        error instanceof WorkflowControlTimeoutError ||
        error instanceof WorkflowControlRejectedError;
      const retriedTooLong =
        now().getTime() >=
        Math.max(command.cancellationDeadline.getTime(), startedAt) +
          unreachableStopGraceMs;
      if (command.action === "stop" && (failedDefinitively || retriedTooLong)) {
        logger.warn`Workflow stop failed for ${runId}: ${message}`;
        await forceRelease(command);
      } else {
        logger.info`Workflow ${command.action} for ${runId} deferred: ${message}`;
      }
      return;
    }
    if (command.action === "stop") await recordConfirmedStop(command);
  }

  // A run whose work stays due, such as a cancellation waiting on its worker
  // or history under backoff, remains eligible. Pause at the end of each pass
  // so the scheduler's immediate refill cannot repeatedly scan it in a loop.
  const sweep = createAnchorRunSweep(
    createLifecycleSweepQueries(db, now, () => {
      const candidates = retentionRouter.getRetentionCandidates();
      const current = new Map(
        candidates.map((target) => [target.allocationId, target.generation]),
      );
      for (const [id, backoff] of retentionBackoff)
        if (current.get(id) !== backoff.generation) retentionBackoff.delete(id);
      return candidates.filter((target) => {
        // A confirmed refusal needs release, not a retention request. Other
        // lifecycle branches still select expiry, cancellation and history work
        // even when this retention branch has no request capacity.
        if (retentionRouter.getAllocationRetention(target) === "refused")
          return true;
        if (
          retentionsInFlight.has(target.allocationId) ||
          !hasRetentionRoom(target.sidecarId)
        )
          return false;
        const backoff = retentionBackoff.get(target.allocationId);
        return backoff === undefined || backoff.nextAt <= now().getTime();
      });
    }),
    {
      intervalMs: SWEEP_INTERVAL_MS,
      now,
    },
  );

  async function reconcileNext(): Promise<boolean> {
    const run = await sweep.select();
    if (run === null) return false;
    try {
      await reconcileRun(run.tenantId, run.id);
    } catch (error) {
      logger.error`Lifecycle reconciliation failed for ${run.id}: ${error instanceof Error ? error.message : String(error)}`;
    } finally {
      sweep.release(run.id);
    }
    return true;
  }

  return { getStatus, releaseCapacity, requestCancellation, reconcileNext };
}

export type WorkflowLifecycleService = ReturnType<
  typeof createWorkflowLifecycleService
>;

/**
 * Selects the deployments `reconcileRun` can act on: a live run that is
 * cancelling or past its expiry, terminal copies needing retention or
 * release, a deferred infrastructure failure, and
 * accepted history left unprojected past its grace. A new action there needs a
 * branch here. Deployments are never deleted, so each branch starts from an
 * index over rows still active in its own sense and reaches an anchor only by
 * key: a pass costs time in proportion to current capacity, not to every
 * deployment ever created. Status lists are SQL literals rather than bound
 * parameters: a plan made without parameter values could not otherwise use the
 * partial indexes.
 *
 * Each branch keeps its next candidate for the rest of a pass and is read again
 * only once the sweep takes that candidate, so a branch with nothing due is
 * scanned once per pass rather than once per selection. Work that becomes due
 * behind a branch's kept candidate waits for the next pass.
 */
export function createLifecycleSweepQueries(
  db: DB["db"],
  now: () => Date,
  getRetentionCandidates: SidecarAllocationRouter["getRetentionCandidates"],
): AnchorRunSweepQueries<{ id: string; tenantId: string }> {
  // Selection runs once per candidate of every pass, so it is built once.
  function prepareBranches(afterCursor: boolean) {
    const inPass = (column: AnyPgColumn) =>
      and(
        ...(afterCursor ? [gt(column, sql.placeholder("afterRunId"))] : []),
        lte(column, sql.placeholder("passEnd")),
        sql`${column} <> all(${sql.placeholder("activeRunIds")})`,
      );
    // An anchor is reached only by key from a candidate row. Postgres badly
    // underestimates `id = anchor_run_id`, and a join would let it hash every
    // deployment instead; a limited lateral lookup cannot be flattened into one.
    const anchorOf = (anchorRunId: AnyPgColumn, condition?: SQL) =>
      db
        .select({ present: sql<number>`1`.as("present") })
        .from(workflowRun)
        .where(
          and(
            eq(workflowRun.id, anchorRunId),
            eq(workflowRun.anchorRunId, workflowRun.id),
            condition,
          ),
        )
        .limit(1)
        .as("anchor");
    // Retention work uses a connection snapshot; generation checks discard a
    // candidate that changed while the pass ran. Release and history work do
    // not depend on a connection or an outstanding retention decision.
    const retentionDue = db
      .select({ id: sidecarAllocation.anchorRunId })
      .from(sidecarAllocation)
      .crossJoinLateral(
        anchorOf(
          sidecarAllocation.anchorRunId,
          and(
            notInArray(workflowRun.status, [...liveWorkflowRunStatuses]),
            isNotNull(workflowRun.publicKey),
            // Undated policies go through releaseDue, which derives the deadline.
            or(
              gt(
                workflowRun.capacityReleaseAt,
                sql.param(
                  sql.placeholder("now"),
                  workflowRun.capacityReleaseAt,
                ),
              ),
              and(
                isNull(workflowRun.capacityReleaseAt),
                sql`${workflowRun.lifecyclePolicy} -> 'capacityRetention' ->> ${workflowRun.status} is null`,
              ),
            ),
          ),
        ),
      )
      .where(
        and(
          eq(sidecarAllocation.status, "allocated"),
          sql`${sidecarAllocation.id} = any(${sql.placeholder("retentionIds")}::text[])`,
          sql`${sidecarAllocation.generation} = (${sql.placeholder("retentionGenerations")}::jsonb ->> ${sidecarAllocation.id})::integer`,
          eq(
            sidecarAllocation.ensureAcceptedGeneration,
            sidecarAllocation.generation,
          ),
          isNull(sidecarAllocation.initializationLeaseId),
          inPass(sidecarAllocation.anchorRunId),
        ),
      )
      .orderBy(asc(sidecarAllocation.anchorRunId))
      .limit(1);
    const releaseDue = db
      .select({ id: sidecarAllocation.anchorRunId })
      .from(sidecarAllocation)
      .crossJoinLateral(
        anchorOf(
          sidecarAllocation.anchorRunId,
          and(
            notInArray(workflowRun.status, [...liveWorkflowRunStatuses]),
            or(
              lte(
                workflowRun.capacityReleaseAt,
                sql.param(
                  sql.placeholder("now"),
                  workflowRun.capacityReleaseAt,
                ),
              ),
              and(
                isNull(workflowRun.capacityReleaseAt),
                sql`${workflowRun.lifecyclePolicy} -> 'capacityRetention' ->> ${workflowRun.status} is not null`,
              ),
            ),
          ),
        ),
      )
      .where(
        and(
          sql`${sidecarAllocation.status} in ('pending', 'provisioning', 'allocated', 'replacing')`,
          inPass(sidecarAllocation.anchorRunId),
        ),
      )
      .orderBy(asc(sidecarAllocation.anchorRunId))
      .limit(1);
    const liveDue = db
      .select({ id: workflowRun.id })
      .from(workflowRun)
      .where(
        and(
          sql`${workflowRun.id} = ${workflowRun.anchorRunId} and ${workflowRun.status} in ('deployed', 'running')`,
          or(
            isNotNull(workflowRun.cancellationRequestedAt),
            lte(
              workflowRun.expiresAt,
              sql.param(sql.placeholder("now"), workflowRun.expiresAt),
            ),
          ),
          inPass(workflowRun.id),
        ),
      )
      .orderBy(asc(workflowRun.id))
      .limit(1);
    const infrastructureFailed = db
      .select({ id: workflowRun.id })
      .from(workflowRun)
      .where(
        and(
          isNotNull(workflowRun.infrastructureFailedAt),
          eq(workflowRun.id, workflowRun.anchorRunId),
          inPass(workflowRun.id),
        ),
      )
      .orderBy(asc(workflowRun.id))
      .limit(1);
    // Any deployment, live or not, whose accepted history may still be
    // unprojected.
    const unprojectedHistory = db
      .select({ id: workflowPendingProjection.anchorRunId })
      .from(workflowPendingProjection)
      .crossJoinLateral(anchorOf(workflowPendingProjection.anchorRunId))
      .where(
        and(
          lte(
            workflowPendingProjection.createdAt,
            sql.param(
              sql.placeholder("pendingCutoff"),
              workflowPendingProjection.createdAt,
            ),
          ),
          inPass(workflowPendingProjection.anchorRunId),
        ),
      )
      .orderBy(asc(workflowPendingProjection.anchorRunId))
      .limit(1);
    const position = afterCursor ? "next" : "first";
    return {
      retention: retentionDue.prepare(`lifecycle_sweep_retention_${position}`),
      release: releaseDue.prepare(`lifecycle_sweep_release_${position}`),
      live: liveDue.prepare(`lifecycle_sweep_live_${position}`),
      infrastructure: infrastructureFailed.prepare(
        `lifecycle_sweep_infrastructure_${position}`,
      ),
      history: unprojectedHistory.prepare(
        `lifecycle_sweep_history_${position}`,
      ),
    };
  }
  const first = prepareBranches(false);
  const next = prepareBranches(true);
  const branches = (
    ["retention", "release", "live", "infrastructure", "history"] as const
  ).map((name) => ({ name, first: first[name], next: next[name] }));
  // Postgres picks the lowest candidate, so the choice follows the same
  // collation as the cursor comparisons.
  const lowest = db
    .select({ id: workflowRun.id, tenantId: workflowRun.tenantId })
    .from(workflowRun)
    .where(
      eq(
        workflowRun.id,
        sql`least(${sql.join(
          branches.map(
            (_, index) => sql`${sql.placeholder(`candidate${index}`)}::text`,
          ),
          sql`, `,
        )})`,
      ),
    )
    .prepare("lifecycle_sweep_lowest");
  // Undefined until the branch is read this pass, null once it has nothing left.
  let candidates: (string | null | undefined)[] = [];
  let retentionIds: string[] = [];
  let retentionGenerations = "{}";
  return {
    async findPassEnd() {
      const [last] = await db
        .select({ id: workflowRun.id })
        .from(workflowRun)
        .where(eq(workflowRun.id, workflowRun.anchorRunId))
        .orderBy(desc(workflowRun.id))
        .limit(1);
      return last?.id;
    },
    async findNext({ afterRunId, passEnd, activeRunIds }) {
      if (afterRunId === undefined) {
        candidates = branches.map(() => undefined);
        const retention = getRetentionCandidates();
        retentionIds = retention.map((target) => target.allocationId);
        retentionGenerations = JSON.stringify(
          Object.fromEntries(
            retention.map((target) => [target.allocationId, target.generation]),
          ),
        );
      }
      const at = now();
      const values = {
        passEnd,
        activeRunIds,
        retentionIds,
        retentionGenerations,
        now: at,
        pendingCutoff: new Date(at.getTime() - PENDING_PROJECTION_GRACE_MS),
      };
      for (const [index, branch] of branches.entries()) {
        if (branch.name === "retention" && retentionIds.length === 0) {
          candidates[index] = null;
          continue;
        }
        const candidate = candidates[index];
        if (
          candidate === null ||
          (candidate !== undefined && candidate !== afterRunId)
        )
          continue;
        const [row] =
          afterRunId === undefined
            ? await branch.first.execute(values)
            : await branch.next.execute({ ...values, afterRunId });
        candidates[index] = row?.id ?? null;
      }
      if (candidates.every((candidate) => candidate === null)) return undefined;
      const [selected] = await lowest.execute(
        Object.fromEntries(
          candidates.map((candidate, index) => [
            `candidate${index}`,
            candidate,
          ]),
        ),
      );
      return selected;
    },
  };
}

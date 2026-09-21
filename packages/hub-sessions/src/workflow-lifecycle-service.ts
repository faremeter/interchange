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
  type DB,
  type DBExecutor,
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
import {
  isSidecarAllocationDispatchable,
  lifecycleDeadline,
} from "@intx/types";

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

type Run = ReturnType<typeof parseWorkflowRunRow>;
type Allocation = typeof sidecarAllocation.$inferSelect;
type ReleaseResult =
  | "pending"
  | "released"
  | "live"
  | "history_pending"
  | "not_found"
  | "cleanup_failed";
// `request` is an explicit caller waiting on the answer, so it reads Git even
// while the sweep is backing off; `stop` and `sweep` respect the backoff, and
// only `sweep` leaves young rows to the receive that wrote them.
type RecoveryTrigger = "request" | "stop" | "sweep";

export type WorkflowLifecycleServiceDeps = {
  db: DB["db"];
  runReader: WorkflowRunReader;
  historyReceives: WorkflowHistoryReceiveTracker;
  now?: () => Date;
  cancelGraceMs?: number;
  unreachableStopGraceMs?: number;
  sendControl?: SidecarAllocationRouter["sendWorkflowControl"];
};

export function createWorkflowLifecycleService({
  db,
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

  async function withRun<T>(
    tenantId: string,
    runId: string,
    action: (
      tx: DBExecutor,
      run: Run,
      allocation: Allocation | undefined,
    ) => Promise<T>,
  ) {
    return db.transaction(async (tx) => {
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
      return action(tx, parseWorkflowRunRow(row), allocation);
    });
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
        // A live row with a pending projection may be terminal in accepted
        // history, so "live" would be a guess.
        if (isLiveWorkflowRunStatus(run.status))
          return (await pendingProjections.hasAny(runId, tx))
            ? "history_pending"
            : "live";
        if (
          allocation === undefined ||
          allocation.status === "released" ||
          allocation.status === "failed"
        )
          return "released";
        if (allocation.status === "destroy_failed") return "cleanup_failed";
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
      async (tx, run, allocation) => {
        if (
          !isLiveWorkflowRunStatus(run.status) ||
          allocation === undefined ||
          allocation.generation !== command.target.generation ||
          !isSidecarAllocationDispatchable(allocation.status)
        )
          return;
        await allocations.beginRelease(
          {
            allocationId: allocation.id,
            expectedGeneration: allocation.generation,
            expectedStatus: allocation.status,
            failureCode: "workflow_stop_failed",
            failureMessage:
              "Workflow stop could not be confirmed; reclaiming capacity",
            now: now(),
          },
          tx,
        );
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
  // a live run with nothing due skips the locked pass. Terminal runs take it:
  // ingestion refuses their packs without receiving under the lock.
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
      },
    });
    if (run === undefined) return false;
    if (!isLiveWorkflowRunStatus(run.status)) return true;
    return (
      run.cancellationRequestedAt !== null ||
      (run.expiresAt !== null && run.expiresAt <= now())
    );
  }

  async function reconcileRun(tenantId: string, runId: string): Promise<void> {
    await recoverHistory(tenantId, runId, "sweep");
    if (!(await needsLockedPass(tenantId, runId))) return;
    const command = await withRun(
      tenantId,
      runId,
      async (tx, original, allocation): Promise<ControlRequest | null> => {
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
            await allocations.beginRelease(
              {
                allocationId: allocation.id,
                expectedStatus: allocation.status,
                expectedGeneration: allocation.generation,
                now: now(),
              },
              tx,
            );
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
        const retention = run.lifecyclePolicy?.capacityRetention?.[run.status];
        const releaseAt =
          run.capacityReleaseAt ??
          (retention === undefined || run.endedAt === null
            ? null
            : lifecycleDeadline(run.endedAt, retention));
        if (releaseAt === null) return null;
        if (run.capacityReleaseAt === null)
          await tx
            .update(workflowRun)
            .set({ capacityReleaseAt: releaseAt })
            .where(eq(workflowRun.id, runId));
        if (releaseAt > now()) return null;
        await allocations.beginRelease(
          {
            allocationId: allocation.id,
            expectedStatus: allocation.status,
            expectedGeneration: allocation.generation,
            now: now(),
          },
          tx,
        );
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
      // worker's connection yet or its acknowledgement may not have been
      // processed in time. Once the grace passes, no failure can postpone the
      // stop further.
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
  const sweep = createAnchorRunSweep(createLifecycleSweepQueries(db, now), {
    intervalMs: SWEEP_INTERVAL_MS,
    now,
  });

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
 * cancelling or past its expiry, retained capacity whose release time has
 * passed or is not yet recorded, and accepted history left unprojected past
 * its grace. A new action there needs a branch here. Deployments are never
 * deleted, so each branch starts from an index over rows still active in its
 * own sense and reaches an anchor only by key: a pass costs time in proportion
 * to current capacity, not to every deployment ever created. Status lists are
 * SQL literals rather than bound parameters: a plan made without parameter
 * values could not otherwise use the partial indexes.
 *
 * Each branch keeps its next candidate for the rest of a pass and is read again
 * only once the sweep takes that candidate, so a branch with nothing due is
 * scanned once per pass rather than once per selection. Work that becomes due
 * behind a branch's kept candidate waits for the next pass.
 */
export function createLifecycleSweepQueries(
  db: DB["db"],
  now: () => Date,
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
    // Retained capacity is due once its release time passes. The first visit
    // after the run ends derives that time from the saved policy; a run
    // without a policy has none until an explicit release sets it.
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
                isNotNull(workflowRun.lifecyclePolicy),
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
      release: releaseDue.prepare(`lifecycle_sweep_release_${position}`),
      live: liveDue.prepare(`lifecycle_sweep_live_${position}`),
      history: unprojectedHistory.prepare(
        `lifecycle_sweep_history_${position}`,
      ),
    };
  }
  const first = prepareBranches(false);
  const next = prepareBranches(true);
  const branches = (["release", "live", "history"] as const).map((name) => ({
    first: first[name],
    next: next[name],
  }));
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
      if (afterRunId === undefined) candidates = branches.map(() => undefined);
      const at = now();
      const values = {
        passEnd,
        activeRunIds,
        now: at,
        pendingCutoff: new Date(at.getTime() - PENDING_PROJECTION_GRACE_MS),
      };
      for (const [index, branch] of branches.entries()) {
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

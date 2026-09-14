import {
  and,
  eq,
  exists,
  inArray,
  isNotNull,
  lte,
  notInArray,
  or,
} from "drizzle-orm";

import {
  createSidecarAllocationStore,
  createWorkflowPendingProjectionStore,
  createWorkflowRunStore,
  parseWorkflowRunRow,
  type DB,
  type DBExecutor,
} from "@intx/db";
import {
  isLiveWorkflowRunStatus,
  liveWorkflowRunStatuses,
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
import type { AllocatedSidecarTarget } from "./ws/sidecar-handler";
import {
  classifyTerminalEvent,
  workflowRunRepoIdForAddress,
  WORKFLOW_RUN_REF,
} from "./workflow-run-kind";
import { projectTerminalRun } from "./workflow-run-terminal-projection";

const logger = getLogger(["hub", "workflow-lifecycle"]);
// A receive clears its own pending projection within one pack transfer. The
// sweep leaves younger rows to it so healthy deployments cost no Git reads.
const PENDING_PROJECTION_GRACE_MS = 30_000;
// A moving tip is reread a few times; after that the next pass retries, so a
// deployment that pushes constantly cannot hold a caller here.
const MAX_RECOVERY_ATTEMPTS = 3;
const RECOVERY_BACKOFF_MIN_MS = 5_000;
const RECOVERY_BACKOFF_MAX_MS = 5 * 60_000;

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
// while the sweep is backing off; only `sweep` leaves young rows to the
// receive that wrote them.
type RecoveryTrigger = "request" | "sweep";

export type WorkflowLifecycleServiceDeps = {
  db: DB["db"];
  runReader: WorkflowRunReader;
  historyReceives: WorkflowHistoryReceiveTracker;
  now?: () => Date;
};

export function createWorkflowLifecycleService({
  db,
  runReader,
  historyReceives,
  now = () => new Date(),
}: WorkflowLifecycleServiceDeps) {
  const allocations = createSidecarAllocationStore(db);
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
        .where(eq(sidecarAllocation.anchorRunId, runId))
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
  // database records.
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

  async function reconcileRun(tenantId: string, runId: string): Promise<void> {
    await recoverHistory(tenantId, runId, "sweep");
    await withRun(
      tenantId,
      runId,
      async (tx, run, allocation): Promise<AllocatedSidecarTarget | null> => {
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
        if (run.capacityReleaseAt === null) {
          await tx
            .update(workflowRun)
            .set({ capacityReleaseAt: releaseAt })
            .where(eq(workflowRun.id, runId));
        }
        if (releaseAt > now()) return null;
        const releasing = await allocations.beginRelease(
          {
            allocationId: allocation.id,
            expectedStatus: allocation.status,
            expectedGeneration: allocation.generation,
            now: now(),
          },
          tx,
        );
        return releasing === null
          ? null
          : { allocationId: releasing.id, generation: releasing.generation };
      },
    );
  }

  async function reconcile(): Promise<void> {
    const candidates = await db
      .select({ id: workflowRun.id, tenantId: workflowRun.tenantId })
      .from(workflowRun)
      .leftJoin(
        sidecarAllocation,
        eq(sidecarAllocation.anchorRunId, workflowRun.id),
      )
      .where(
        and(
          eq(workflowRun.id, workflowRun.anchorRunId),
          or(
            and(
              or(
                isNotNull(workflowRun.lifecyclePolicy),
                isNotNull(workflowRun.capacityReleaseAt),
              ),
              inArray(sidecarAllocation.status, [
                "pending",
                "provisioning",
                "allocated",
                "replacing",
              ]),
            ),
            // Any deployment, live or not, whose accepted history may still
            // be unprojected.
            exists(
              db
                .select({ id: workflowPendingProjection.id })
                .from(workflowPendingProjection)
                .where(
                  and(
                    eq(workflowPendingProjection.anchorRunId, workflowRun.id),
                    lte(
                      workflowPendingProjection.createdAt,
                      new Date(now().getTime() - PENDING_PROJECTION_GRACE_MS),
                    ),
                  ),
                ),
            ),
          ),
        ),
      );
    for (const run of candidates) {
      try {
        await reconcileRun(run.tenantId, run.id);
      } catch (error) {
        logger.error`Lifecycle reconciliation failed for ${run.id}: ${error instanceof Error ? error.message : String(error)}`;
      }
    }
  }

  return { getStatus, releaseCapacity, reconcile };
}

export type WorkflowLifecycleService = ReturnType<
  typeof createWorkflowLifecycleService
>;

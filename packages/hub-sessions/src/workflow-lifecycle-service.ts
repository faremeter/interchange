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
  deleteRunTokensForPrincipals,
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
import {
  isSidecarAllocationDispatchable,
  type WorkflowStandbyState,
} from "@intx/types";

import type { WorkflowHistoryReceiveTracker } from "./workflow-history-receives";
import type { SidecarEventMap } from "./ws/sidecar-events";
import type { WorkflowRunReader } from "./workflow-run-reader";
import {
  WorkflowControlNotConnectedError,
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
// A standby request is answered once every run waits, however long that takes,
// so this bounds a wake and the Hub's handling of an answer.
const STANDBY_CONTROL_TIMEOUT_MS = 60_000;
// A standby report the Hub could not record is retried for this long, holding
// back its allocation's later reports and answers, before the sidecar's next
// hello is left to correct it.
const REPORT_RETRY_MIN_MS = 5_000;
const REPORT_RETRY_MAX_MS = 60_000;
const REPORT_RETRY_GIVE_UP_MS = 5 * 60_000;
const MAX_CONCURRENT_RETAINS_PER_SIDECAR = 8;
const STOP_FAILED_RELEASE = {
  failureCode: "workflow_stop_failed",
  failureMessage: "Workflow stop could not be confirmed; reclaiming capacity",
};
const REPORTED_RUNNING_REASON =
  "The sidecar reported the workflow child running";

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
type ReleaseStoppedCleanupResult =
  | "released"
  | "not_found"
  | "not_stopped"
  | "generation_mismatch";
// `request` is an explicit caller waiting on the answer, so it reads Git even
// while the sweep is backing off; `stop` and `sweep` respect the backoff, and
// only `sweep` leaves young rows to the receive that wrote them.
type RecoveryTrigger = "request" | "stop" | "sweep";
type StandbyAction = "standby" | "wake";

export type WorkflowLifecycleServiceDeps = {
  db: DB["db"];
  retentionRouter: Pick<
    SidecarAllocationRouter,
    | "retainAllocation"
    | "getAllocationRetention"
    | "getRetentionCandidates"
    | "holdsAllocatedBinding"
    | "fenceAllocation"
    | "retireAllocation"
  >;
  runReader: WorkflowRunReader;
  historyReceives: WorkflowHistoryReceiveTracker;
  now?: () => Date;
  cancelGraceMs?: number;
  unreachableStopGraceMs?: number;
  /**
   * How long a sidecar must be gone from this Hub before an operator cancel
   * releases its capacity without the worker's answer.
   */
  operatorCancelReconnectGraceMs?: number;
  sendControl?: SidecarAllocationRouter["sendWorkflowControl"];
  standbyRouter?: Pick<
    SidecarAllocationRouter,
    "putAllocationInStandby" | "wakeAllocation" | "holdsAllocatedBinding"
  >;
  /** Called after each standby or wake command ends, answered or not. */
  onStandbyCommandSettled?: (runId: string) => void;
};

export function createWorkflowLifecycleService({
  db,
  retentionRouter,
  runReader,
  historyReceives,
  now = () => new Date(),
  cancelGraceMs = 30_000,
  unreachableStopGraceMs = 60_000,
  operatorCancelReconnectGraceMs = 15_000,
  sendControl,
  standbyRouter,
  onStandbyCommandSettled,
}: WorkflowLifecycleServiceDeps) {
  if (cancelGraceMs < 0 || !Number.isFinite(cancelGraceMs))
    throw new Error("Cancellation grace must be finite and non-negative");
  if (unreachableStopGraceMs < 0 || !Number.isFinite(unreachableStopGraceMs))
    throw new Error("Unreachable stop grace must be finite and non-negative");
  if (
    operatorCancelReconnectGraceMs < 0 ||
    !Number.isFinite(operatorCancelReconnectGraceMs)
  )
    throw new Error(
      "Operator cancel reconnect grace must be finite and non-negative",
    );
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
  // The standby or wake request each run has outstanding, so a sweep does not
  // repeat one the sidecar is still answering.
  const standbyCommands = new Map<
    string,
    {
      readonly action: StandbyAction;
      readonly target: AllocatedSidecarTarget;
    }
  >();
  // The last turn taken for each allocation generation's reports and answers.
  const observationTails = new Map<string, Promise<void>>();
  // The last standby drive queued for each run.
  const standbyDrives = new Map<string, Promise<void>>();
  // Standby reports whose write failed, by allocation generation. Each keeps
  // its turn until the sweep records it or gives up.
  const failedReports = new Map<
    string,
    {
      report: SidecarEventMap["deployment.standby"];
      firstFailedAt: number;
      failures: number;
      nextAt: number;
      done: () => void;
    }
  >();

  // Reports and answers from a generation's sidecar are recorded in the order
  // the Hub received them, so an earlier one never overwrites a later one. A
  // turn is taken when its report or answer arrives.
  function takeObservationTurn(target: AllocatedSidecarTarget): {
    ready: Promise<void>;
    done: () => void;
  } {
    const key = `${target.allocationId}:${String(target.generation)}`;
    const previous = observationTails.get(key) ?? Promise.resolve();
    const turn = Promise.withResolvers<undefined>();
    const tail = previous.then(() => turn.promise);
    observationTails.set(key, tail);
    return {
      ready: previous,
      done: () => {
        turn.resolve(undefined);
        void tail.then(() => {
          if (observationTails.get(key) === tail) observationTails.delete(key);
        });
      },
    };
  }
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
    // Only the allocation row is locked: pack ingestion moves Git only
    // while it holds this row, so an unchanged tip under it proves the read
    // covered every claimed receive. Locking the anchor row as well would
    // deadlock against a receive minting a child row, whose foreign key
    // needs it.
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
      standby: isLiveWorkflowRunStatus(run.status)
        ? {
            state: run.standbyState ?? ("awake" as const),
            changedAt: run.standbyStateAt?.toISOString() ?? null,
            wakeAt:
              run.standbyState === "standby"
                ? (run.standbyWakeAt?.toISOString() ?? null)
                : null,
            reason: run.standbyReason,
          }
        : null,
      allocation:
        allocation === null
          ? null
          : {
              id: allocation.id,
              generation: allocation.generation,
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
        // Report the cleanup outcome before pending history can mask it.
        // Disconnect timeouts resume on reconnect; other failures need an operator.
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

  async function releaseStoppedCleanup(
    tenantId: string,
    runId: string,
    generation: number,
    releasedBy: string,
  ): Promise<ReleaseStoppedCleanupResult> {
    const result = await allocations.releaseStoppedCleanup({
      tenantId,
      anchorRunId: runId,
      generation,
      releasedBy,
      now: now(),
    });
    if (result.kind === "released")
      retentionRouter.retireAllocation({
        allocationId: result.allocation.id,
        generation: result.allocation.generation,
      });
    return result.kind === "already_released" ? "released" : result.kind;
  }

  async function beginCancellation(
    tx: DBExecutor,
    run: Run,
    reason: string,
    origin: "operator" | "expiry",
  ): Promise<Run> {
    const at = now();
    if (run.cancellationRequestedAt !== null) {
      // The first request keeps its deadline and reason. A later operator
      // request still records that an operator asked.
      if (origin !== "operator" || run.operatorCancellationRequestedAt !== null)
        return run;
      await tx
        .update(workflowRun)
        .set({ operatorCancellationRequestedAt: at })
        .where(eq(workflowRun.id, run.id));
      return { ...run, operatorCancellationRequestedAt: at };
    }
    const deadline = new Date(at.getTime() + cancelGraceMs);
    const operatorCancellationRequestedAt = origin === "operator" ? at : null;
    // Cancelling wakes a child in standby and abandons a standby request in
    // progress, so a standby request no longer stands, nor does the reason
    // an earlier one was refused.
    const wake =
      run.standbyState === null
        ? run.standbyReason === null
          ? {}
          : { standbyReason: null }
        : {
            standbyState: null,
            standbyStateAt: at,
            standbyWakeAt: null,
            standbyReason: null,
          };
    await tx
      .update(workflowRun)
      .set({
        cancellationRequestedAt: at,
        cancellationDeadline: deadline,
        cancellationReason: reason,
        operatorCancellationRequestedAt,
        ...wake,
      })
      .where(eq(workflowRun.id, run.id));
    return {
      ...run,
      cancellationRequestedAt: at,
      cancellationDeadline: deadline,
      cancellationReason: reason,
      operatorCancellationRequestedAt,
      ...wake,
    };
  }

  function getRequestedCancellation(run: Run): {
    deadline: Date;
    reason: string;
  } {
    // beginCancellation writes the first request, its deadline, and its
    // reason together.
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
    // Not `withRun`: a pack receive holds the allocation row while it
    // writes the ref, the sweep holds it too, and a request must not wait
    // on either. The anchor lock
    // alone orders this against every `withRun` and admission, which
    // lock the anchor FOR UPDATE, and against the allocation store's
    // capacity failures, which lock it before they check for a request.
    // NO KEY UPDATE, not FOR UPDATE, so a foreign-key check from a child
    // run insert does not block it.
    // History is not recovered here either, because that also locks the
    // allocation row; the sweep recovers it before it acts on the
    // request, and never records `cancelled` over accepted history.
    return db.transaction(async (tx) => {
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
        .for("no key update");
      if (row === undefined) return "not_found" as const;
      const run = parseWorkflowRunRow(row);
      if (!isLiveWorkflowRunStatus(run.status)) return "terminal" as const;
      await beginCancellation(tx, run, reason, "operator");
      return "pending" as const;
    });
  }

  // The Hub records the wanted state, and the sweep asks the sidecar for it
  // until the sidecar answers.
  async function requestStandbyState(
    tenantId: string,
    runId: string,
    wanted: "standby" | "awake",
  ): Promise<
    | "pending"
    | "settled"
    | "terminal"
    | "cancelling"
    | "unallocated"
    | "not_found"
  > {
    const [settled, pending] =
      wanted === "standby"
        ? (["standby", "entering"] as const)
        : ([null, "waking"] as const);
    // Not `withRun`, as for a cancellation: a pack receive and the sweep hold
    // the allocation row, and a request must not wait on either. The anchor
    // lock orders it against every `withRun`. History is not recovered here
    // either. The sweep recovers it before its own drive; a drive started
    // elsewhere that finds history missing is put off until the sweep's.
    const result = await db.transaction(async (tx) => {
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
        .for("no key update");
      if (row === undefined) return "not_found" as const;
      const run = parseWorkflowRunRow(row);
      if (!isLiveWorkflowRunStatus(run.status)) return "terminal" as const;
      if (run.cancellationRequestedAt !== null) return "cancelling" as const;
      if (run.standbyState === settled) return "settled" as const;
      const [allocation] = await tx
        .select({ status: sidecarAllocation.status })
        .from(sidecarAllocation)
        .where(
          and(
            eq(sidecarAllocation.anchorRunId, runId),
            eq(sidecarAllocation.tenantId, tenantId),
          ),
        );
      // No sidecar could ever carry the request out.
      if (
        allocation === undefined ||
        !isSidecarAllocationDispatchable(allocation.status)
      )
        return "unallocated" as const;
      if (run.standbyState !== pending) {
        // A JS date, since an answer is matched against this value read back,
        // which drops microseconds.
        await tx
          .update(workflowRun)
          .set({
            standbyState: pending,
            standbyStateAt: now(),
            standbyWakeAt: null,
            standbyReason: null,
          })
          .where(eq(workflowRun.id, runId));
      }
      return "pending" as const;
    });
    // A standby request in progress answers only once every run waits, so a
    // wake that abandons it goes out now.
    if (
      result === "pending" &&
      wanted === "awake" &&
      standbyCommands.get(runId)?.action === "standby"
    ) {
      void driveStandbyState(runId).catch((cause: unknown) => {
        logger.warn`Workflow wake for ${runId} waits for the sweep: ${cause instanceof Error ? cause.message : String(cause)}`;
      });
    }
    return result;
  }

  type StandbyCommand = {
    target: AllocatedSidecarTarget;
    runId: string;
    action: StandbyAction;
    // The request being carried out; an answer applies only while it stands.
    stateAt: Date;
  };

  // The anchor run an allocation generation serves.
  function anchorOf(target: AllocatedSidecarTarget) {
    return db
      .select({ id: sidecarAllocation.anchorRunId })
      .from(sidecarAllocation)
      .where(
        and(
          eq(sidecarAllocation.id, target.allocationId),
          eq(sidecarAllocation.generation, target.generation),
        ),
      );
  }

  // Records what a generation's sidecar did with that allocation row locked
  // first, as pack ingestion does, so a replacement cannot commit in between;
  // skipped once the generation moved on.
  async function withReportingGeneration(
    target: AllocatedSidecarTarget,
    write: (tx: DBExecutor, runId: string) => Promise<unknown>,
  ): Promise<void> {
    await db.transaction(async (tx) => {
      const [allocation] = await tx
        .select({ runId: sidecarAllocation.anchorRunId })
        .from(sidecarAllocation)
        .where(
          and(
            eq(sidecarAllocation.id, target.allocationId),
            eq(sidecarAllocation.generation, target.generation),
          ),
        )
        .for("update");
      if (allocation !== undefined) await write(tx, allocation.runId);
    });
  }

  // A run's drives decide one at a time, each from a read taken after the one
  // before it sent its command, so a command chosen from an older read never
  // reaches the sidecar after one chosen from a newer read. Answers are
  // awaited outside the queue.
  function driveStandbyState(runId: string): Promise<void> {
    const previous = standbyDrives.get(runId) ?? Promise.resolve();
    const drive = previous.then(() => sendPendingStandbyCommand(runId));
    const tail = drive.catch(() => undefined);
    standbyDrives.set(runId, tail);
    void tail.then(() => {
      if (standbyDrives.get(runId) === tail) standbyDrives.delete(runId);
    });
    return drive;
  }

  // Sends the command that carries out a pending standby or wake request once
  // the Hub can deliver it: the copy is deployed and its sidecar connected. A
  // command waits for its answer or its connection; the sweep sends again
  // after either.
  async function sendPendingStandbyCommand(runId: string) {
    const [row] = await db
      .select({
        run: workflowRun,
        allocationId: sidecarAllocation.id,
        allocationStatus: sidecarAllocation.status,
        generation: sidecarAllocation.generation,
        sidecarId: sidecarAllocation.sidecarId,
      })
      .from(workflowRun)
      .innerJoin(
        sidecarAllocation,
        eq(sidecarAllocation.anchorRunId, workflowRun.id),
      )
      .where(
        and(eq(workflowRun.id, runId), eq(workflowRun.anchorRunId, runId)),
      );
    if (row === undefined) return;
    const run = parseWorkflowRunRow(row.run);
    if (
      (run.standbyState !== "entering" && run.standbyState !== "waking") ||
      run.standbyStateAt === null ||
      !isLiveWorkflowRunStatus(run.status) ||
      run.cancellationRequestedAt !== null ||
      row.allocationStatus !== "allocated" ||
      row.sidecarId === null ||
      run.publicKey === null
    )
      return;
    const action: StandbyAction =
      run.standbyState === "entering" ? "standby" : "wake";
    if (standbyRouter === undefined) {
      logger.warn`Workflow standby is unavailable for ${runId}`;
      return;
    }
    const target = {
      allocationId: row.allocationId,
      generation: row.generation,
    };
    if (!standbyRouter.holdsAllocatedBinding(target)) return;
    const inFlight = standbyCommands.get(runId);
    if (
      inFlight?.action === action &&
      inFlight.target.generation === row.generation
    )
      return;
    const entry = { action, target };
    standbyCommands.set(runId, entry);
    void sendStandbyCommand(standbyRouter, {
      target,
      runId,
      action,
      stateAt: run.standbyStateAt,
    }).then(() => {
      if (standbyCommands.get(runId) === entry) standbyCommands.delete(runId);
      onStandbyCommandSettled?.(runId);
    });
  }

  async function sendStandbyCommand(
    router: NonNullable<WorkflowLifecycleServiceDeps["standbyRouter"]>,
    command: StandbyCommand,
  ): Promise<void> {
    let turn: ReturnType<typeof takeObservationTurn> | undefined;
    const onAnswered = (): void => {
      turn ??= takeObservationTurn(command.target);
    };
    try {
      let change: Parameters<typeof settleStandbyCommand>[1];
      try {
        if (command.action === "standby") {
          const { wakeAt } = await router.putAllocationInStandby(
            command.target,
            STANDBY_CONTROL_TIMEOUT_MS,
            onAnswered,
          );
          change = {
            standbyState: "standby",
            standbyWakeAt: wakeAt === undefined ? null : new Date(wakeAt),
          };
        } else {
          await router.wakeAllocation(
            command.target,
            STANDBY_CONTROL_TIMEOUT_MS,
            onAnswered,
          );
          change = { standbyState: null };
        }
      } catch (error) {
        await turn?.ready;
        const message = error instanceof Error ? error.message : String(error);
        // Only the sidecar's refusal ends a request; the sweep repeats one that
        // got no answer.
        if (!(error instanceof WorkflowControlRejectedError)) {
          logger.info`Workflow ${command.action} for ${command.runId} deferred: ${message}`;
          return;
        }
        logger.warn`Workflow ${command.action} for ${command.runId} was refused: ${message}`;
        await settleStandbyCommand(command, {
          standbyState: null,
          standbyReason: `The sidecar refused to ${command.action === "standby" ? "put the deployment in standby" : "wake the deployment"}: ${message}`,
        }).catch((cause: unknown) => {
          logger.error`Could not record the refused ${command.action} for ${command.runId}: ${cause instanceof Error ? cause.message : String(cause)}`;
        });
        return;
      }
      await turn?.ready;
      // The request still stands, so the sweep asks again and records the
      // answer the sidecar gives then.
      await settleStandbyCommand(command, change).catch((cause: unknown) => {
        logger.error`Could not record the answered ${command.action} for ${command.runId}; the sweep asks again: ${cause instanceof Error ? cause.message : String(cause)}`;
      });
    } finally {
      turn?.done();
    }
  }

  // Records an answer while the request it carried out still stands.
  async function settleStandbyCommand(
    command: StandbyCommand,
    change: {
      standbyState: WorkflowStandbyState | null;
      standbyWakeAt?: Date | null;
      standbyReason?: string;
    },
  ): Promise<void> {
    await withReportingGeneration(command.target, (tx, runId) =>
      tx
        .update(workflowRun)
        .set({ standbyWakeAt: null, ...change, standbyStateAt: now() })
        .where(
          and(
            eq(workflowRun.id, runId),
            eq(
              workflowRun.standbyState,
              command.action === "standby" ? "entering" : "waking",
            ),
            eq(workflowRun.standbyStateAt, command.stateAt),
          ),
        ),
    );
  }

  // The sidecar's word on whether the child is in standby: each hello reports
  // it, and a child in standby that an input woke says why. It takes its turn
  // on arrival, so it is recorded after every answer received before it. The
  // sidecar sends it once, so a write that fails keeps the turn for the sweep
  // to retry.
  async function noteDeploymentInStandby(
    report: SidecarEventMap["deployment.standby"],
  ): Promise<void> {
    const turn = takeObservationTurn(report);
    try {
      await recordDeploymentInStandby(report, turn.ready);
    } catch (cause) {
      // Held first in line, so its retry never waits on another report.
      await turn.ready;
      const failedAt = now().getTime();
      failedReports.set(`${report.allocationId}:${String(report.generation)}`, {
        report,
        firstFailedAt: failedAt,
        failures: 1,
        nextAt: failedAt + REPORT_RETRY_MIN_MS,
        done: turn.done,
      });
      logger.warn`Could not record the standby report of allocation ${report.allocationId}; the sweep retries it: ${cause instanceof Error ? cause.message : String(cause)}`;
      return;
    }
    turn.done();
  }

  // Retries one failed standby report that is due. Returns whether it tried.
  async function retryFailedReport(): Promise<boolean> {
    const at = now().getTime();
    const due = [...failedReports].find(([, failed]) => failed.nextAt <= at);
    if (due === undefined) return false;
    const [key, failed] = due;
    failedReports.delete(key);
    try {
      await recordDeploymentInStandby(failed.report, Promise.resolve());
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      const failures = failed.failures + 1;
      if (at - failed.firstFailedAt >= REPORT_RETRY_GIVE_UP_MS) {
        logger.error`Gave up recording the standby report of allocation ${failed.report.allocationId} after ${String(failures)} attempts; the Hub's standby record may be wrong until its sidecar's next hello: ${message}`;
        failed.done();
        return true;
      }
      failedReports.set(key, {
        ...failed,
        failures,
        nextAt:
          at +
          Math.min(
            REPORT_RETRY_MAX_MS,
            REPORT_RETRY_MIN_MS * 2 ** (failures - 1),
          ),
      });
      return true;
    }
    failed.done();
    return true;
  }

  async function recordDeploymentInStandby(
    report: SidecarEventMap["deployment.standby"],
    earlier: Promise<void>,
  ): Promise<void> {
    const [anchor] = await anchorOf(report);
    if (anchor === undefined) return;
    await earlier;
    if (report.inStandby) {
      // A child in standby that no standby request stands for is woken; a
      // pending standby request is recorded from its command's answer.
      let unrequested = false;
      await withReportingGeneration(report, async (tx, runId) => {
        const woken = await tx
          .update(workflowRun)
          .set({
            standbyState: "waking",
            standbyStateAt: now(),
            standbyWakeAt: null,
            standbyReason:
              "The sidecar reported the workflow child in standby, and no standby was asked for",
          })
          .where(
            and(
              eq(workflowRun.id, runId),
              isNull(workflowRun.standbyState),
              inArray(workflowRun.status, [...liveWorkflowRunStatuses]),
              isNull(workflowRun.cancellationRequestedAt),
            ),
          )
          .returning({ id: workflowRun.id });
        unrequested = woken.length > 0;
      });
      if (unrequested)
        logger.warn`Waking the workflow child of ${anchor.id}: its sidecar reports it in standby, and no standby is asked for`;
      await driveStandbyState(anchor.id);
      return;
    }
    // A running child ends a recorded standby, and an input that woke it
    // also carried out a pending wake. A pending standby request stands: the
    // sidecar carries it out next.
    await withReportingGeneration(report, async (tx, runId) => {
      await tx
        .update(workflowRun)
        .set({
          standbyState: null,
          standbyStateAt: now(),
          standbyWakeAt: null,
          standbyReason: report.reason ?? REPORTED_RUNNING_REASON,
        })
        .where(
          and(
            eq(workflowRun.id, runId),
            report.reason === undefined
              ? eq(workflowRun.standbyState, "standby")
              : inArray(workflowRun.standbyState, ["standby", "waking"]),
          ),
        );
      // After a reconnect the `hello` can end the standby request before the
      // wake report held across it arrives; that report still says what woke
      // the child.
      if (report.reason !== undefined) {
        await tx
          .update(workflowRun)
          .set({ standbyReason: report.reason })
          .where(
            and(
              eq(workflowRun.id, runId),
              isNull(workflowRun.standbyState),
              eq(workflowRun.standbyReason, REPORTED_RUNNING_REASON),
            ),
          );
      }
    });
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
    if (principalIds.length > 0) {
      await tx
        .update(principal)
        .set({ status: "deactivated", updatedAt: endedAt })
        .where(inArray(principal.id, principalIds));
      await deleteRunTokensForPrincipals(tx, principalIds);
    }
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
    operatorRequested: boolean;
  };

  // beginRelease clears the message when a code comes without one, so both
  // are required.
  async function forceRelease(
    command: ControlRequest,
    failure: { failureCode: string; failureMessage: string },
  ): Promise<void> {
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
          ...failure,
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

  // Pack ingestion holds a live run's allocation lock while it moves the
  // ref, so a live run with nothing due skips the locked pass instead of
  // waiting behind a receive. Retention confirmation also needs no write;
  // terminal runs lock only to derive or begin release.
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
    ) {
      // A run that needs the locked pass is being cancelled, has lost its
      // capacity, or has ended, so no sidecar could carry out a standby
      // request for it.
      await driveStandbyState(runId);
      return;
    }
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
              "expiry",
            );
          }
          if (run.cancellationRequestedAt === null) return null;
          if (
            allocation === undefined ||
            allocation.status === "released" ||
            allocation.status === "failed" ||
            ((allocation.status === "releasing" ||
              allocation.status === "destroy_failed") &&
              allocation.deploymentCleanupConfirmed)
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
            operatorRequested: run.operatorCancellationRequestedAt !== null,
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
      if (command.action === "stop")
        await forceRelease(command, STOP_FAILED_RELEASE);
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
      // An operator asked for this cancellation, so a sidecar that has been
      // gone from this Hub for the reconnect grace loses its capacity without
      // a wait for the cancellation deadline and the stop grace.
      const releaseForOperator =
        command.operatorRequested &&
        error instanceof WorkflowControlNotConnectedError &&
        now().getTime() - error.notConnectedSince.getTime() >=
          operatorCancelReconnectGraceMs;
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
      if (releaseForOperator) {
        logger.warn`Workflow ${command.action} for ${runId} found no sidecar: ${message}`;
        await forceRelease(command, {
          failureCode: "workflow_cancel_unreachable",
          failureMessage:
            "The sidecar was not connected when an operator cancelled the workflow; reclaiming capacity",
        });
      } else if (
        command.action === "stop" &&
        (failedDefinitively || retriedTooLong)
      ) {
        logger.warn`Workflow stop failed for ${runId}: ${message}`;
        await forceRelease(command, STOP_FAILED_RELEASE);
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
    createLifecycleSweepQueries(
      db,
      now,
      () => {
        const candidates = retentionRouter.getRetentionCandidates();
        const current = new Map(
          candidates.map((target) => [target.allocationId, target.generation]),
        );
        for (const [id, backoff] of retentionBackoff)
          if (current.get(id) !== backoff.generation)
            retentionBackoff.delete(id);
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
      },
      standbyCommandsInFlight,
    ),
    {
      intervalMs: SWEEP_INTERVAL_MS,
      now,
    },
  );

  // A command whose binding is gone, or that a newer request replaced, does
  // not hold its run back.
  function standbyCommandsInFlight() {
    const enterStandby: string[] = [];
    const wake: string[] = [];
    for (const [runId, { action, target }] of standbyCommands) {
      if (standbyRouter?.holdsAllocatedBinding(target) !== true) continue;
      (action === "standby" ? enterStandby : wake).push(runId);
    }
    return { enterStandby, wake };
  }

  async function reconcileNext(): Promise<boolean> {
    if (await retryFailedReport()) return true;
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

  return {
    getStatus,
    releaseCapacity,
    releaseStoppedCleanup,
    requestCancellation,
    requestStandby: (tenantId: string, runId: string) =>
      requestStandbyState(tenantId, runId, "standby"),
    requestWake: (tenantId: string, runId: string) =>
      requestStandbyState(tenantId, runId, "awake"),
    noteDeploymentInStandby,
    reconcileNext,
  };
}

export type WorkflowLifecycleService = ReturnType<
  typeof createWorkflowLifecycleService
>;

/**
 * Selects the deployments `reconcileRun` can act on: a live run that is
 * cancelling, past its expiry, or waiting on a standby or wake request its
 * sidecar is not already answering, terminal
 * copies needing retention or release, a deferred infrastructure failure, and
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
  getStandbyCommandsInFlight: () => {
    enterStandby: string[];
    wake: string[];
  },
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
            sql`(${workflowRun.standbyState} = 'entering' and ${workflowRun.id} <> all(${sql.placeholder("standbyInFlight")}::text[]))`,
            sql`(${workflowRun.standbyState} = 'waking' and ${workflowRun.id} <> all(${sql.placeholder("wakesInFlight")}::text[]))`,
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
      const inFlight = getStandbyCommandsInFlight();
      const values = {
        passEnd,
        activeRunIds,
        retentionIds,
        retentionGenerations,
        standbyInFlight: inFlight.enterStandby,
        wakesInFlight: inFlight.wake,
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

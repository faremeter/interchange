import {
  and,
  asc,
  desc,
  eq,
  exists,
  gt,
  inArray,
  lte,
  notExists,
  notInArray,
  sql,
  type SQL,
} from "drizzle-orm";

import {
  createWorkflowPendingProjectionStore,
  createWorkflowRunDispatchStore,
  type DB,
  type DBExecutor,
  type WorkflowRunDispatchStore,
} from "@intx/db";
import {
  isLiveWorkflowRunStatus,
  liveWorkflowRunStatuses,
  sidecarAllocation,
  workflowPendingProjection,
  workflowRun,
  workflowRunDispatch,
} from "@intx/db/schema";
import { getLogger } from "@intx/log";
import { isSidecarAllocationDispatchable } from "@intx/types";

import { createAnchorRunSweep } from "./anchor-run-sweep";
import type { CommittedReads, RepoStore } from "./repo-store";
import type {
  AcceptedWorkflowDispatch,
  WorkflowDispatchEvidence,
} from "./workflow-dispatch-evidence";
import {
  listAcceptedWorkflowDispatches,
  processingHoldsWorkflowDispatch,
  readConsumedWorkflowDispatches,
  rowsNamedByEvidence,
  settleFromEvidence,
  targetRunOf,
  type DispatchOutcome,
  type UnsettledDispatch,
} from "./workflow-dispatch-settlement";
import {
  readCommittedWorkflowRunLifecycle,
  workflowRunRepoIdForAddress,
  WORKFLOW_RUN_CLAIM_CHECK_REF,
  WORKFLOW_RUN_REF,
} from "./workflow-run-kind";

const logger = getLogger(["hub", "workflow-dispatch-projection"]);
export const DEFAULT_WORKFLOW_PROJECTION_CONCURRENCY = 8;
const DEFAULT_VISIT_BATCH = 1000;

export type WorkflowDispatchProjection = ReturnType<
  typeof createWorkflowDispatchProjection
>;

const terminalRunFailure = (runId: string): DispatchOutcome => ({
  status: "failed",
  code: "workflow_run_terminal",
  message: `Workflow run ${runId} is terminal and cannot accept this dispatch`,
});

// Accepted history cannot be rewritten, so each update is conditional on
// a row still unresolved, and a row another writer resolved first stays as
// it is.
async function writeOutcomes(
  dispatches: WorkflowRunDispatchStore,
  anchorRunId: string,
  outcomes: ReadonlyMap<string, DispatchOutcome>,
  now: Date,
  tx?: DBExecutor,
): Promise<void> {
  if (outcomes.size === 0) return;
  await dispatches.resolveMany(
    anchorRunId,
    [...outcomes].map(([deliveryKey, outcome]) =>
      outcome.status === "settled"
        ? { deliveryKey }
        : {
            deliveryKey,
            failure: { code: outcome.code, message: outcome.message },
          },
    ),
    now,
    tx,
  );
}

/**
 * Settle a deployment's unsettled dispatches from evidence a receive or an
 * enqueue found, in `tx`.
 */
export async function applyWorkflowDispatchEvidence(
  dispatches: WorkflowRunDispatchStore,
  tx: DBExecutor,
  anchor: { readonly id: string; readonly address: string },
  evidence: WorkflowDispatchEvidence,
  now: Date,
): Promise<void> {
  if (evidence.consumed.length === 0 && evidence.accepted.length === 0) return;
  const unsettled = await dispatches.listUnsettledKeysNamed(
    anchor.id,
    rowsNamedByEvidence(anchor, evidence),
    tx,
  );
  if (unsettled.length === 0) return;
  const { outcomes } = settleFromEvidence(anchor, unsettled, evidence);
  await writeOutcomes(dispatches, anchor.id, outcomes, now, tx);
}

/**
 * Settle or fail the unsettled signals to runs in `endedRuns`, from each
 * run's closed log in `reads`. Another receive can hold a signal's
 * evidence and not have settled it yet, so the whole log decides, not only
 * the push that ended the run.
 */
export async function closeSignalsToEndedRuns(
  dispatches: WorkflowRunDispatchStore,
  db: DBExecutor,
  anchor: { readonly id: string; readonly address: string },
  endedRuns: ReadonlySet<string>,
  reads: CommittedReads,
  now: Date,
): Promise<void> {
  const signalsByRun = new Map<string, UnsettledDispatch[]>();
  for (const dispatch of await dispatches.listUnsettledKeys(anchor.id, db)) {
    if (dispatch.kind !== "signal") continue;
    const runId = targetRunOf(dispatch, anchor.id);
    if (!endedRuns.has(runId)) continue;
    const signals = signalsByRun.get(runId);
    if (signals === undefined) signalsByRun.set(runId, [dispatch]);
    else signals.push(dispatch);
  }
  const outcomes = new Map<string, DispatchOutcome>();
  for (const [runId, signals] of signalsByRun) {
    const accepted = await listAcceptedWorkflowDispatches(
      reads,
      runId,
      new Set(signals.map((signal) => signal.deliveryKey)),
    );
    const fromLog = settleFromEvidence(anchor, signals, {
      consumed: [],
      accepted,
    });
    for (const [key, outcome] of fromLog.outcomes) outcomes.set(key, outcome);
    for (const key of fromLog.remaining.keys())
      outcomes.set(key, terminalRunFailure(runId));
  }
  await writeOutcomes(dispatches, anchor.id, outcomes, now, db);
}

/**
 * Settle mail queued in a transaction that has committed, from a consumed
 * record already in Git. The same sender can relay mail with this
 * Message-ID between sidecars, which takes no row, and a sidecar that
 * predates the delivery key filed records under the bare Message-ID;
 * either way the sidecar dedupes the delivery without recording anything
 * new. The enqueue held the deployment's allocation lock, which every
 * receive holds to move Git, so a receive that moved Git before it is
 * visible here, and one after it lists the row.
 *
 * Returns `in-processing` for mail the deployment holds in processing
 * with no record yet: a run may already have accepted it in a log this
 * check does not read, so the caller asks the dispatch projection to
 * revisit the deployment. It reads the ref durably, so it runs outside
 * database locks.
 */
export async function settleQueuedMailFromGit(
  repoStore: Pick<RepoStore, "openCommittedReads">,
  dispatches: WorkflowRunDispatchStore,
  db: DBExecutor,
  anchor: { readonly id: string; readonly address: string },
  queued: { readonly deliveryKey: string; readonly messageId: string },
  now: Date,
): Promise<"checked" | "in-processing"> {
  const consumedReads = await repoStore.openCommittedReads(
    { kind: "hub" },
    workflowRunRepoIdForAddress(anchor.address),
    WORKFLOW_RUN_CLAIM_CHECK_REF,
    { durable: true },
  );
  if (consumedReads === null) return "checked";
  const identities = new Set([queued.deliveryKey, queued.messageId]);
  const consumed = await readConsumedWorkflowDispatches(
    consumedReads,
    anchor.address,
    identities,
  );
  if (consumed.length > 0) {
    await applyWorkflowDispatchEvidence(
      dispatches,
      db,
      anchor,
      { consumed, accepted: [] },
      now,
    );
    return "checked";
  }
  return (await processingHoldsWorkflowDispatch(
    consumedReads,
    anchor.address,
    identities,
  ))
    ? "in-processing"
    : "checked";
}

/** Projects accepted Git evidence without depending on a live worker or allocation. */
export function createWorkflowDispatchProjection({
  db,
  repoStore,
  maxConcurrentProjections = DEFAULT_WORKFLOW_PROJECTION_CONCURRENCY,
  maxVisitBatch = DEFAULT_VISIT_BATCH,
}: {
  db: DB["db"];
  repoStore: Pick<RepoStore, "openCommittedReads">;
  /** Bounds background scans and their unfinished work; ingestion owns its own queue. */
  maxConcurrentProjections?: number;
  /**
   * Bounds the deployments to visit that one eligibility query names, well
   * under the database's limit on bound parameters.
   */
  maxVisitBatch?: number;
}) {
  if (
    !Number.isSafeInteger(maxConcurrentProjections) ||
    maxConcurrentProjections <= 0
  ) {
    throw new Error("Projection concurrency must be a positive integer");
  }
  if (!Number.isSafeInteger(maxVisitBatch) || maxVisitBatch <= 0) {
    throw new Error("Projection visit batch must be a positive integer");
  }
  const dispatches = createWorkflowRunDispatchStore(db);
  const pendingProjections = createWorkflowPendingProjectionStore(db);
  let admitted = 0;

  /** Settle a deployment's dispatches from the tips of its accepted history. */
  async function project(anchorRunId: string): Promise<void> {
    const [anchor] = await db
      .select({ address: workflowRun.address })
      .from(workflowRun)
      .where(
        and(
          eq(workflowRun.id, anchorRunId),
          eq(workflowRun.anchorRunId, anchorRunId),
        ),
      );
    if (anchor === undefined || anchor.address === null) return;
    // Capture the candidate deliveries before pinning Git. A later enqueue is
    // outside this projection, even if the run becomes terminal during the read.
    const unsettled = await dispatches.listUnsettled(anchorRunId);
    if (unsettled.length === 0) return;
    const repoId = workflowRunRepoIdForAddress(anchor.address);
    // Pin run history first, then consumed outcomes so an older consumed view
    // cannot hide a rejection already present when terminal state was observed.
    // Outcomes go to SQL, so both refs are read durably.
    let reads: CommittedReads | null = null;
    let runHistoryFailure: { cause: unknown } | null = null;
    try {
      reads = await repoStore.openCommittedReads(
        { kind: "hub" },
        repoId,
        WORKFLOW_RUN_REF,
        { durable: true },
      );
    } catch (cause) {
      runHistoryFailure = { cause };
    }
    const consumedReads = await repoStore.openCommittedReads(
      { kind: "hub" },
      repoId,
      WORKFLOW_RUN_CLAIM_CHECK_REF,
      { durable: true },
    );
    const deployment = { id: anchorRunId, address: anchor.address };
    const mailIds = new Set<string>();
    for (const dispatch of unsettled) {
      if (dispatch.kind !== "mail") continue;
      mailIds.add(dispatch.deliveryKey);
      mailIds.add(dispatch.messageId);
    }
    // Consumed records decide mail before any run log is read, and their
    // outcomes are written first, so a log that cannot be read does not
    // hold back an outcome the records already give.
    const byRecord = settleFromEvidence(deployment, unsettled, {
      consumed:
        consumedReads === null
          ? []
          : await readConsumedWorkflowDispatches(
              consumedReads,
              anchor.address,
              mailIds,
            ),
      accepted: [],
    });
    await writeOutcomes(dispatches, anchorRunId, byRecord.outcomes, new Date());
    if (runHistoryFailure !== null) throw runHistoryFailure.cause;
    const open = [...byRecord.remaining.values()];
    const targetIdsByRun = new Map<string, Set<string>>();
    const signalKeysByRun = new Map<string, string[]>();
    for (const dispatch of open) {
      const runId = targetRunOf(dispatch, anchorRunId);
      const targets = targetIdsByRun.get(runId) ?? new Set<string>();
      targets.add(dispatch.deliveryKey);
      if (dispatch.kind === "mail") targets.add(dispatch.messageId);
      targetIdsByRun.set(runId, targets);
      if (dispatch.kind === "signal") {
        const keys = signalKeysByRun.get(runId);
        if (keys === undefined)
          signalKeysByRun.set(runId, [dispatch.deliveryKey]);
        else keys.push(dispatch.deliveryKey);
      }
    }
    const accepted: AcceptedWorkflowDispatch[] = [];
    if (reads !== null) {
      for (const [runId, targets] of targetIdsByRun) {
        accepted.push(
          ...(await listAcceptedWorkflowDispatches(reads, runId, targets)),
        );
      }
    }
    const { outcomes, remaining } = settleFromEvidence(deployment, open, {
      consumed: [],
      accepted,
    });
    for (const runId of targetIdsByRun.keys()) {
      if (
        (await readCommittedWorkflowRunLifecycle(reads, runId)) !== "terminal"
      )
        continue;
      // A mail rejection is recorded on the events ref, which can trail
      // the run log, so mail waits for final history below.
      for (const key of signalKeysByRun.get(runId) ?? []) {
        if (!remaining.delete(key)) continue;
        outcomes.set(key, terminalRunFailure(runId));
      }
    }
    const anchorTerminal =
      remaining.size > 0 &&
      reads !== null &&
      (await readCommittedWorkflowRunLifecycle(reads, anchorRunId)) ===
        "terminal";
    const unscannedAbandoned = [...remaining.values()].filter(
      (dispatch) =>
        dispatch.status === "abandoned" && dispatch.nextAttemptAt !== null,
    );
    const historyFinal =
      (anchorTerminal || unscannedAbandoned.length > 0) &&
      (await isHistoryFinal(anchorRunId, repoId, [
        { ref: WORKFLOW_RUN_REF, reads },
        { ref: WORKFLOW_RUN_CLAIM_CHECK_REF, reads: consumedReads },
      ]));
    if (anchorTerminal && historyFinal) {
      for (const key of remaining.keys()) {
        outcomes.set(key, {
          status: "failed",
          code: "workflow_run_terminal",
          message: `Workflow deployment ${anchor.address} is terminal and cannot accept this dispatch`,
        });
      }
    }
    // Each conditional row update can commit independently: a failed pass
    // leaves the remainder discoverable, and a large backlog never holds
    // allocation or run locks during cleanup.
    const now = new Date();
    await writeOutcomes(dispatches, anchorRunId, outcomes, now);
    // Final history has been read once, so later scans cannot resolve what
    // remains abandoned.
    if (historyFinal)
      await dispatches.concludeAbandonedScans(
        anchorRunId,
        unscannedAbandoned.map((dispatch) => dispatch.deliveryKey),
        now,
      );
  }

  // Pack receipt opens a pending row before Git can move and requires a live
  // anchor row and allocated worker under the allocation lock. After either
  // gate closes and no receive is pending, pinned refs that are still current
  // are the deployment's final history.
  async function isHistoryFinal(
    anchorRunId: string,
    repoId: { kind: "workflow-run"; id: string },
    pinned: readonly { ref: string; reads: CommittedReads | null }[],
  ): Promise<boolean> {
    const [current] = await db
      .select({
        status: workflowRun.status,
        allocationStatus: sidecarAllocation.status,
      })
      .from(workflowRun)
      .leftJoin(
        sidecarAllocation,
        eq(sidecarAllocation.anchorRunId, workflowRun.id),
      )
      .where(eq(workflowRun.id, anchorRunId))
      .limit(1);
    const allocationClosed =
      current?.allocationStatus != null &&
      !isSidecarAllocationDispatchable(current.allocationStatus);
    if (
      current === undefined ||
      (isLiveWorkflowRunStatus(current.status) && !allocationClosed) ||
      (await pendingProjections.hasAny(anchorRunId))
    )
      return false;
    for (const { ref, reads } of pinned) {
      const latest = await repoStore.openCommittedReads(
        { kind: "hub" },
        repoId,
        ref,
      );
      // Only a receive creates the repository or writes a ref, so with none
      // pending an absent ref stays absent.
      if (reads === null || latest === null) {
        if (reads !== latest) return false;
        continue;
      }
      if ((await latest.treeOid("")) !== (await reads.treeOid("")))
        return false;
    }
    return true;
  }

  // SQL literals, so a plan made without parameter values can still use the
  // partial index on unresolved dispatches.
  const unresolvedDispatch = () =>
    sql`(${workflowRunDispatch.status} in ('pending', 'acknowledged') or (${workflowRunDispatch.status} = 'abandoned' and ${workflowRunDispatch.nextAttemptAt} is not null))`;
  const dispatchesWhere = (condition: SQL) =>
    exists(
      db
        .select({ id: workflowRunDispatch.id })
        .from(workflowRunDispatch)
        .where(
          and(eq(workflowRunDispatch.anchorRunId, workflowRun.id), condition),
        ),
    );

  // Live deployments the pass visits until one visit succeeds, however long
  // a pending row or a failed visit holds them back. Each maps to the number
  // of its latest request, so a visit that began before a later request does
  // not answer it. A query names at most `maxVisitBatch` of them, and each
  // pass names the next ones, so deployments a pending row holds back cannot
  // starve the rest.
  const toVisit = new Map<string, number>();
  let visitRequests = 0;
  let seeded = false;
  let visitOffset = 0;
  let visitBatch: string[] = [];
  const nextVisitBatch = (): string[] => {
    const ids = [...toVisit.keys()];
    if (ids.length <= maxVisitBatch) return ids;
    const start = visitOffset % ids.length;
    visitOffset = start + maxVisitBatch;
    return [...ids, ...ids].slice(start, start + maxVisitBatch);
  };

  // The background pass decides what only final history can: the
  // dispatches of a deployment that ended, and abandoned dispatches owed a
  // scan. A live deployment's dispatches settle as its receives bring their
  // evidence, and a receive cut short after its ref moved keeps a pending
  // row, which lifecycle recovery reconciles, so the pass waits while one is
  // open.
  function eligibleRuns(activeRunIds: string[], visitIds: string[]) {
    return and(
      eq(workflowRun.id, workflowRun.anchorRunId),
      ...(activeRunIds.length === 0
        ? []
        : [notInArray(workflowRun.id, activeRunIds)]),
      notExists(
        db
          .select({ id: workflowPendingProjection.id })
          .from(workflowPendingProjection)
          .where(eq(workflowPendingProjection.anchorRunId, workflowRun.id)),
      ),
      // One EXISTS over the unresolved rows, which the partial index
      // serves, so the planner can start from them instead of every
      // deployment.
      dispatchesWhere(
        sql`${unresolvedDispatch()} and (${sql.join(
          [
            sql`(${workflowRunDispatch.status} = 'abandoned' and ${workflowRunDispatch.nextAttemptAt} is not null)`,
            notInArray(workflowRun.status, [...liveWorkflowRunStatuses]),
            ...(visitIds.length === 0
              ? []
              : [inArray(workflowRun.id, visitIds)]),
          ],
          sql` or `,
        )})`,
      ),
    );
  }

  const sweep = createAnchorRunSweep({
    async findPassEnd(activeRunIds) {
      // Revisit requests do not survive a restart, and older rows can have
      // unresolved dispatches with no pending row, so the first pass visits
      // every live deployment that has some.
      if (!seeded) {
        const live = await db
          .select({ id: workflowRun.id })
          .from(workflowRun)
          .where(
            and(
              eq(workflowRun.id, workflowRun.anchorRunId),
              inArray(workflowRun.status, [...liveWorkflowRunStatuses]),
              dispatchesWhere(unresolvedDispatch()),
            ),
          );
        for (const run of live) revisit(run.id);
        seeded = true;
      }
      // A deployment whose dispatches resolved some other way needs no
      // visit, and would otherwise hold its place in the batch.
      const batch = nextVisitBatch();
      if (batch.length > 0) {
        const requested = new Map(batch.map((id) => [id, toVisit.get(id)]));
        const open = new Set(
          (
            await db
              .select({ id: workflowRun.id })
              .from(workflowRun)
              .where(
                and(
                  inArray(workflowRun.id, batch),
                  dispatchesWhere(unresolvedDispatch()),
                ),
              )
          ).map((run) => run.id),
        );
        for (const id of batch) {
          if (!open.has(id) && toVisit.get(id) === requested.get(id))
            toVisit.delete(id);
        }
      }
      visitBatch = batch.filter((id) => toVisit.has(id));
      const [last] = await db
        .select({ id: workflowRun.id })
        .from(workflowRun)
        .where(eligibleRuns(activeRunIds, visitBatch))
        .orderBy(desc(workflowRun.id))
        .limit(1);
      return last?.id;
    },
    async findNext({ afterRunId, passEnd, activeRunIds }) {
      const [candidate] = await db
        .select({ id: workflowRun.id })
        .from(workflowRun)
        .where(
          and(
            eligibleRuns(activeRunIds, visitBatch),
            lte(workflowRun.id, passEnd),
            ...(afterRunId === undefined
              ? []
              : [gt(workflowRun.id, afterRunId)]),
          ),
        )
        .orderBy(asc(workflowRun.id))
        .limit(1);
      return candidate;
    },
  });

  async function reconcileNext(): Promise<void> {
    if (admitted >= maxConcurrentProjections) return;
    admitted += 1;
    let run: { id: string } | null = null;
    try {
      run = await sweep.select();
      if (run !== null) {
        const requests = toVisit.get(run.id);
        await project(run.id);
        if (toVisit.get(run.id) === requests) toVisit.delete(run.id);
      }
    } catch (cause) {
      logger.error`Dispatch projection failed for ${run?.id ?? "candidate selection"}: ${cause instanceof Error ? cause.message : String(cause)}`;
    } finally {
      if (run !== null) sweep.release(run.id);
      admitted -= 1;
    }
  }

  /**
   * Have the pass visit a live deployment until one visit succeeds, for a
   * dispatch whose evidence could not be checked when it was queued, or a
   * recovery whose projection failed.
   */
  function revisit(anchorRunId: string): void {
    visitRequests += 1;
    toVisit.set(anchorRunId, visitRequests);
  }

  /**
   * Settle a deployment's dispatches for lifecycle recovery. A failure must
   * not hold back the run outcomes recovery decides, so it is logged and the
   * pass revisits the deployment until a visit succeeds.
   */
  async function recover(anchorRunId: string): Promise<void> {
    try {
      await project(anchorRunId);
    } catch (cause) {
      logger.error`Dispatch recovery failed for ${anchorRunId}; the projection pass retries it: ${cause instanceof Error ? cause.message : String(cause)}`;
      revisit(anchorRunId);
    }
  }

  return { project, reconcileNext, revisit, recover };
}

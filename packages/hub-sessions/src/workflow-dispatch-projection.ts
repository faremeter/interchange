import {
  and,
  asc,
  desc,
  eq,
  exists,
  gt,
  lte,
  notInArray,
  sql,
} from "drizzle-orm";

import {
  createWorkflowPendingProjectionStore,
  createWorkflowRunDispatchStore,
  type DB,
} from "@intx/db";
import {
  isLiveWorkflowRunStatus,
  sidecarAllocation,
  workflowRun,
  workflowRunDispatch,
} from "@intx/db/schema";
import { getLogger } from "@intx/log";
import { isSidecarAllocationDispatchable } from "@intx/types";
import { SignalDeliverFrame } from "@intx/types/sidecar";

import { createAnchorRunSweep } from "./anchor-run-sweep";
import type { CommittedReads, RepoStore } from "./repo-store";
import {
  listAcceptedWorkflowDispatches,
  listConsumedWorkflowDispatches,
} from "./workflow-dispatch-settlement";
import {
  readCommittedWorkflowRunLifecycle,
  workflowRunRepoIdForAddress,
  WORKFLOW_RUN_REF,
} from "./workflow-run-kind";

const logger = getLogger(["hub", "workflow-dispatch-projection"]);
export const DEFAULT_WORKFLOW_PROJECTION_CONCURRENCY = 8;

/** Projects accepted Git evidence without depending on a live worker or allocation. */
export function createWorkflowDispatchProjection({
  db,
  repoStore,
  maxConcurrentProjections = DEFAULT_WORKFLOW_PROJECTION_CONCURRENCY,
}: {
  db: DB["db"];
  repoStore: Pick<RepoStore, "openCommittedReads">;
  /** Bounds background scans and their unfinished work; ingestion owns its own queue. */
  maxConcurrentProjections?: number;
}) {
  if (
    !Number.isSafeInteger(maxConcurrentProjections) ||
    maxConcurrentProjections <= 0
  ) {
    throw new Error("Projection concurrency must be a positive integer");
  }
  const dispatches = createWorkflowRunDispatchStore(db);
  const pendingProjections = createWorkflowPendingProjectionStore(db);
  let admitted = 0;

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
    const reads = await repoStore.openCommittedReads(
      { kind: "hub" },
      repoId,
      WORKFLOW_RUN_REF,
    );
    // Pin run history first, then consumed outcomes so an older consumed view
    // cannot hide a rejection already present when terminal state was observed.
    const consumedReads = await repoStore.openCommittedReads(
      { kind: "hub" },
      repoId,
      "refs/heads/events",
    );
    const remaining = new Map(
      unsettled.map((dispatch) => [dispatch.messageId, dispatch]),
    );
    const outcomes = new Map<
      string,
      | { status: "settled" }
      | { status: "failed"; code: string; message: string }
    >();
    const consumedDispatches =
      consumedReads === null
        ? []
        : await listConsumedWorkflowDispatches(consumedReads);
    for (const consumed of consumedDispatches) {
      if (consumed.address !== anchor.address) continue;
      if (remaining.get(consumed.messageId)?.kind !== "mail") continue;
      outcomes.set(
        consumed.messageId,
        consumed.rejection === undefined
          ? { status: "settled" }
          : {
              status: "failed",
              code: consumed.rejection.code,
              message: consumed.rejection.message,
            },
      );
      remaining.delete(consumed.messageId);
    }
    const messageIdsByRun = new Map<string, Set<string>>();
    for (const dispatch of remaining.values()) {
      const runId =
        dispatch.kind === "mail"
          ? anchorRunId
          : SignalDeliverFrame.assert(
              JSON.parse(new TextDecoder().decode(dispatch.rawMessage)),
            ).runId;
      const ids = messageIdsByRun.get(runId) ?? new Set<string>();
      ids.add(dispatch.messageId);
      messageIdsByRun.set(runId, ids);
    }
    for (const [runId, ids] of messageIdsByRun) {
      const acceptedDispatches =
        reads === null
          ? []
          : await listAcceptedWorkflowDispatches(reads, runId, ids);
      for (const accepted of acceptedDispatches) {
        const dispatch = remaining.get(accepted.messageId);
        if (dispatch === undefined) continue;
        // Mail resuming an input gate is also recorded as SignalReceived.
        // The Hub's message-id uniqueness spans both dispatch kinds within
        // this anchor; RunStarted alone still cannot prove a signal delivery.
        if (accepted.kind === "mail" && dispatch.kind !== "mail") continue;
        outcomes.set(accepted.messageId, { status: "settled" });
        remaining.delete(accepted.messageId);
      }
      if (
        (await readCommittedWorkflowRunLifecycle(reads, runId)) === "terminal"
      ) {
        for (const messageId of ids) {
          // A mail rejection is recorded on the events ref, which can trail the
          // run log, so mail waits for final history below.
          if (remaining.get(messageId)?.kind === "mail") continue;
          if (!remaining.delete(messageId)) continue;
          outcomes.set(messageId, {
            status: "failed",
            code: "workflow_run_terminal",
            message: `Workflow run ${runId} is terminal and cannot accept this dispatch`,
          });
        }
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
        { ref: "refs/heads/events", reads: consumedReads },
      ]));
    if (anchorTerminal && historyFinal) {
      for (const messageId of remaining.keys()) {
        outcomes.set(messageId, {
          status: "failed",
          code: "workflow_run_terminal",
          message: `Workflow deployment ${anchor.address} is terminal and cannot accept this dispatch`,
        });
      }
    }
    // Accepted history cannot be rewritten. Each conditional row update can
    // commit independently: a failed pass leaves the remainder discoverable,
    // and a large backlog never holds allocation or run locks during cleanup.
    const now = new Date();
    for (const [messageId, outcome] of outcomes) {
      if (outcome.status === "settled") {
        await dispatches.settle(anchorRunId, messageId, now);
      } else {
        await dispatches.fail({
          anchorRunId,
          messageId,
          code: outcome.code,
          message: outcome.message,
          now,
        });
      }
    }
    // Final history has been read once, so later scans cannot resolve what
    // remains abandoned.
    if (historyFinal)
      await dispatches.concludeAbandonedScans(
        anchorRunId,
        unscannedAbandoned.map((dispatch) => dispatch.messageId),
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

  function eligibleRuns(activeRunIds: string[]) {
    return and(
      eq(workflowRun.id, workflowRun.anchorRunId),
      ...(activeRunIds.length === 0
        ? []
        : [notInArray(workflowRun.id, activeRunIds)]),
      exists(
        db
          .select({ id: workflowRunDispatch.id })
          .from(workflowRunDispatch)
          .where(
            and(
              eq(workflowRunDispatch.anchorRunId, workflowRun.id),
              // A SQL literal, so a plan made without parameter values can
              // still use the partial index on unresolved dispatches.
              sql`(${workflowRunDispatch.status} in ('pending', 'acknowledged') or (${workflowRunDispatch.status} = 'abandoned' and ${workflowRunDispatch.nextAttemptAt} is not null))`,
            ),
          ),
      ),
    );
  }

  const sweep = createAnchorRunSweep({
    async findPassEnd(activeRunIds) {
      const [last] = await db
        .select({ id: workflowRun.id })
        .from(workflowRun)
        .where(eligibleRuns(activeRunIds))
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
            eligibleRuns(activeRunIds),
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
      if (run !== null) await project(run.id);
    } catch (cause) {
      logger.error`Dispatch projection failed for ${run?.id ?? "candidate selection"}: ${cause instanceof Error ? cause.message : String(cause)}`;
    } finally {
      if (run !== null) sweep.release(run.id);
      admitted -= 1;
    }
  }

  return { project, reconcileNext };
}

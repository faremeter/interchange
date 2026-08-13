import { and, eq, inArray, isNull } from "drizzle-orm";

import type { DB, DBExecutor } from "./client";
import { liveWorkflowRunStatuses, workflowRun } from "./schema/workflow-run";
import { parseWorkflowRunRow } from "./parse-row";

type DBHandle = DB["db"];

type WorkflowRunInsert = typeof workflowRun.$inferInsert;
type ParsedWorkflowRun = ReturnType<typeof parseWorkflowRunRow>;

/**
 * Store for the `workflow_run` table, the per-run authorization subject that
 * approvals and signal correlations anchor to. Each method accepts an optional
 * transaction handle so the run row can be written in the same transaction that
 * co-writes the run's principal or its approval.
 */
export function createWorkflowRunStore(db: DBHandle) {
  return {
    async create(
      row: WorkflowRunInsert,
      tx?: DBExecutor,
    ): Promise<ParsedWorkflowRun> {
      const [inserted] = await (tx ?? db)
        .insert(workflowRun)
        .values(row)
        .returning();
      if (inserted === undefined) {
        throw new Error(
          `workflowRunStore.create: insert returned no row for ${row.id}`,
        );
      }
      return parseWorkflowRunRow(inserted);
    },

    /**
     * Idempotent variant of `create`. On an `id` primary-key conflict the
     * insert is a no-op and this returns `null` rather than throwing, so a
     * redelivered createIfAbsent (workflow-log replay, supervisor restart
     * re-emitting) does not fail the run's co-write. Returns the parsed row
     * only when this call performed the insert.
     */
    async createIfAbsent(
      row: WorkflowRunInsert,
      tx?: DBExecutor,
    ): Promise<ParsedWorkflowRun | null> {
      const [inserted] = await (tx ?? db)
        .insert(workflowRun)
        .values(row)
        .onConflictDoNothing({ target: workflowRun.id })
        .returning();
      return inserted === undefined ? null : parseWorkflowRunRow(inserted);
    },

    /**
     * Anchor an externally-triggered run onto its deploy-time anchor row. The
     * anchor is created at deploy (born "deployed" with a null principal); the
     * first trigger reconciles it here, attaching `principalId` and flipping
     * "deployed" -> "running" in one UPDATE. The insert is a conflict-tolerant
     * safety net: the anchor provably exists by the time a trigger runs, so it
     * normally no-ops. The three-part guard makes the reconcile single-shot and
     * safe: `principalId IS NULL` never overwrites a principal a concurrent
     * winner already set, and `status = 'deployed'` never resurrects a run a
     * concurrent teardown already settled terminal (the null-principal guard
     * alone would). `principalId` must be non-null: only the externally-
     * triggered path anchors through here, and it always mints one.
     */
    async anchorWithPrincipal(
      row: WorkflowRunInsert & { principalId: string },
      tx?: DBExecutor,
    ): Promise<void> {
      const executor = tx ?? db;
      const [inserted] = await executor
        .insert(workflowRun)
        .values(row)
        .onConflictDoNothing({ target: workflowRun.id })
        .returning();
      if (inserted !== undefined) return;
      await executor
        .update(workflowRun)
        .set({ principalId: row.principalId, status: "running" })
        .where(
          and(
            eq(workflowRun.id, row.id),
            isNull(workflowRun.principalId),
            eq(workflowRun.status, "deployed"),
          ),
        );
    },

    /**
     * Atomically settle a live run into a terminal state. The live-status guard
     * (`status IN ('deployed','running')`) makes the flip single-shot: the first
     * caller stamps the terminal status and `endedAt` and gets the row back; any
     * later caller matches no row and receives null, so the run is not
     * re-terminated and its `endedAt` is not overwritten. Accepting "deployed"
     * settles a deployment torn down before its first trigger. This is a safety
     * property, not a recovery path -- it makes a second call (a manual replay
     * against an already-settled run) a harmless no-op; it does not by itself
     * re-drive a flip that failed. Returns the parsed row only on the winning
     * flip.
     */
    async markTerminal(
      runId: string,
      status: "completed" | "failed" | "cancelled",
      endedAt: Date,
      tx?: DBExecutor,
    ): Promise<ParsedWorkflowRun | null> {
      const [updated] = await (tx ?? db)
        .update(workflowRun)
        .set({ status, endedAt })
        .where(
          and(
            eq(workflowRun.id, runId),
            inArray(workflowRun.status, [...liveWorkflowRunStatuses]),
          ),
        )
        .returning();
      return updated === undefined ? null : parseWorkflowRunRow(updated);
    },
  };
}

export type WorkflowRunStore = ReturnType<typeof createWorkflowRunStore>;

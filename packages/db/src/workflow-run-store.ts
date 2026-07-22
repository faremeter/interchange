import { and, eq, isNull } from "drizzle-orm";

import type { DB, DBExecutor } from "./client";
import { workflowRun } from "./schema/workflow-run";
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
     * Anchor an externally-triggered run whose row may already exist. The run
     * row has two co-writers keyed on the same id: the trigger route (which
     * mints a principal) and the lazy anchor in signal-correlation
     * registration (which inserts with a null principal when the run parks
     * before the trigger commits). This insert is conflict-tolerant, and on a
     * prior null-principal insert it reconciles by attaching `principalId`
     * without disturbing the row's status. The `principalId IS NULL` guard
     * keeps the attach single-shot, so it never overwrites a principal a
     * concurrent winner already set. `principalId` must be non-null: only the
     * externally-triggered path anchors through here, and it always mints one.
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
        .set({ principalId: row.principalId })
        .where(
          and(eq(workflowRun.id, row.id), isNull(workflowRun.principalId)),
        );
    },
  };
}

export type WorkflowRunStore = ReturnType<typeof createWorkflowRunStore>;

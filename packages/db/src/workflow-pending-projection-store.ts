import { and, asc, eq, inArray } from "drizzle-orm";

import type { DB, DBExecutor } from "./client";
import { workflowPendingProjection } from "./schema/workflow-pending-projection";

type DBHandle = DB["db"];

/**
 * Store for `workflow_pending_projection`, the durable record that a
 * workflow-run pack receive may have advanced Git without its run-status
 * projection landing. See the schema for the invariant the rows carry.
 */
export function createWorkflowPendingProjectionStore(db: DBHandle) {
  return {
    async open(id: string, anchorRunId: string): Promise<void> {
      await db.insert(workflowPendingProjection).values({ id, anchorRunId });
    },

    async close(id: string, tx?: DBExecutor): Promise<void> {
      await (tx ?? db)
        .delete(workflowPendingProjection)
        .where(eq(workflowPendingProjection.id, id));
    },

    async list(
      anchorRunId: string,
      tx?: DBExecutor,
    ): Promise<{ id: string; createdAt: Date; historyRequired: boolean }[]> {
      return (tx ?? db)
        .select({
          id: workflowPendingProjection.id,
          createdAt: workflowPendingProjection.createdAt,
          historyRequired: workflowPendingProjection.historyRequired,
        })
        .from(workflowPendingProjection)
        .where(eq(workflowPendingProjection.anchorRunId, anchorRunId))
        .orderBy(asc(workflowPendingProjection.createdAt));
    },

    async hasAny(anchorRunId: string, tx?: DBExecutor): Promise<boolean> {
      const [row] = await (tx ?? db)
        .select({ id: workflowPendingProjection.id })
        .from(workflowPendingProjection)
        .where(eq(workflowPendingProjection.anchorRunId, anchorRunId))
        .limit(1);
      return row !== undefined;
    },

    async claim(
      anchorRunId: string,
      ids: readonly string[],
      tx: DBExecutor,
    ): Promise<void> {
      if (ids.length === 0) return;
      await tx
        .delete(workflowPendingProjection)
        .where(
          and(
            eq(workflowPendingProjection.anchorRunId, anchorRunId),
            inArray(workflowPendingProjection.id, [...ids]),
          ),
        );
    },
  };
}

export type WorkflowPendingProjectionStore = ReturnType<
  typeof createWorkflowPendingProjectionStore
>;

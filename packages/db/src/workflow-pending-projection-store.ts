import { eq } from "drizzle-orm";

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
  };
}

export type WorkflowPendingProjectionStore = ReturnType<
  typeof createWorkflowPendingProjectionStore
>;

import { boolean, index, pgTable, text, timestamp } from "drizzle-orm/pg-core";

import { workflowRun } from "./workflow-run";

/**
 * One workflow-run pack receive whose run-status projection is not yet
 * confirmed. The row is written before the receive can advance Git, because Git
 * acceptance and the `workflow_run` projection are not atomic: a projection that
 * fails after the ref advances leaves no other durable trace. The receive
 * deletes its row once every run it carried reached a final decision, or when
 * it provably left Git unchanged; lifecycle recovery deletes rows it has
 * reconciled from Git. While any row exists for a deployment, its
 * `workflow_run` rows may disagree with accepted history.
 */
export const workflowPendingProjection = pgTable(
  "workflow_pending_projection",
  {
    id: text("id").primaryKey(),
    anchorRunId: text("anchor_run_id")
      .notNull()
      .references(() => workflowRun.id, { onDelete: "cascade" }),
    createdAt: timestamp("created_at").notNull().defaultNow(),
    // A receive that reached Git leaves the deployment's ref in place, so a
    // missing ref under its row means history is unavailable, not empty. Only
    // rows that cannot know whether Git was ever written clear on a missing ref.
    historyRequired: boolean("history_required").notNull().default(true),
  },
  (t) => [
    index("workflow_pending_projection_anchor_idx").on(
      t.anchorRunId,
      t.createdAt,
    ),
  ],
);

import {
  index,
  pgTable,
  text,
  timestamp,
  uniqueIndex,
} from "drizzle-orm/pg-core";

import { workflowRun } from "./workflow-run";

// workflow_run_execution is the per-TURN record of a run. A long-lived run
// (`workflow_run`, the run aggregate) resumes in place across many messages;
// each inbound message drives exactly one execution -- one turn. The
// single-shot running->terminal guard is per turn and belongs here, not on the
// run aggregate: a redelivered terminal re-settles the SAME execution rather
// than re-terminating the whole run, and a new message opens a NEW execution
// against the same run.
//
// `id` is the executionId, derived deterministically from the driving message
// (see `deriveExecutionId`), so a redelivery of the same bytes resolves to the
// same execution rather than a fresh one. The table is the hub-side projection
// of the run's turn-boundary events (`TurnStarted`/`TurnCompleted`/`TurnFailed`)
// that the read surface exposes so per-turn outcomes are visible.
export const workflowRunExecution = pgTable(
  "workflow_run_execution",
  {
    id: text("id").primaryKey(),
    // The run this execution is a turn of. `cascade` matches `workflow_run`'s
    // own deletes so a removed run takes its executions with it.
    runId: text("run_id")
      .notNull()
      .references(() => workflowRun.id, { onDelete: "cascade" }),
    // The inbound message that drove this turn; the executionId is derived from
    // it, but the raw messageId is retained for correlation and read-surface
    // display.
    messageId: text("message_id").notNull(),
    // A turn is born "running" and settles to "completed" or "failed" when its
    // terminal turn event lands. Unlike the run aggregate, a turn is never
    // "cancelled" on its own -- cancellation is a run-level disposition.
    status: text("status", {
      enum: ["running", "completed", "failed"],
    })
      .notNull()
      .default("running"),
    startedAt: timestamp("started_at").notNull().defaultNow(),
    // Nullable: a turn has no end time until it reaches a terminal state.
    endedAt: timestamp("ended_at"),
    // Populated only on a `failed` turn (e.g. a malformed inbound mail that
    // could not resume the run); null otherwise.
    failureReason: text("failure_reason"),
  },
  (t) => [
    // The read surface lists a run's executions in turn order; the leftmost
    // `runId` column serves that lookup. Unique on `(runId, id)` as the
    // race backstop the design mandates against a concurrent double-insert of
    // the same turn.
    uniqueIndex("workflow_run_execution_run_id_id_idx").on(t.runId, t.id),
    index("workflow_run_execution_status_idx").on(t.status),
  ],
);

import { sql } from "drizzle-orm";
import {
  check,
  index,
  integer,
  jsonb,
  pgTable,
  text,
  timestamp,
  uniqueIndex,
} from "drizzle-orm/pg-core";

import { bytea } from "./column-types";
import { workflowRun } from "./workflow-run";

export const workflowRunDispatchStatuses = [
  "pending",
  "acknowledged",
  "settled",
  "failed",
] as const;

export type WorkflowRunDispatchStatus =
  (typeof workflowRunDispatchStatuses)[number];

export const workflowRunDispatchKinds = ["mail", "signal"] as const;
export type WorkflowRunDispatchKind = (typeof workflowRunDispatchKinds)[number];

/**
 * Hub-owned delivery state for one inbound workflow message. The raw payload
 * remains durable until the workflow-run Git claim-check reaches `consumed`;
 * a sidecar-local enqueue acknowledgement alone does not settle the row.
 */
export const workflowRunDispatch = pgTable(
  "workflow_run_dispatch",
  {
    id: text("id").primaryKey(),
    anchorRunId: text("anchor_run_id")
      .notNull()
      .references(() => workflowRun.id, { onDelete: "cascade" }),
    messageId: text("message_id").notNull(),
    kind: text("kind")
      .$type<WorkflowRunDispatchKind>()
      .notNull()
      .default("mail"),
    rawMessage: bytea("raw_message").notNull(),
    stepGrants: jsonb("step_grants").notNull(),
    status: text("status")
      .$type<WorkflowRunDispatchStatus>()
      .notNull()
      .default("pending"),
    acknowledgedGeneration: integer("acknowledged_generation"),
    attemptCount: integer("attempt_count").notNull().default(0),
    nextAttemptAt: timestamp("next_attempt_at").defaultNow(),
    deliveryLeaseId: text("delivery_lease_id"),
    deliveryLeaseExpiresAt: timestamp("delivery_lease_expires_at"),
    failureCode: text("failure_code"),
    failureMessage: text("failure_message"),
    createdAt: timestamp("created_at").notNull().defaultNow(),
    updatedAt: timestamp("updated_at").notNull().defaultNow(),
    acknowledgedAt: timestamp("acknowledged_at"),
    settledAt: timestamp("settled_at"),
  },
  (t) => [
    uniqueIndex("workflow_run_dispatch_anchor_message_idx").on(
      t.anchorRunId,
      t.messageId,
    ),
    index("workflow_run_dispatch_delivery_idx")
      .on(t.nextAttemptAt, t.createdAt)
      .where(sql`${t.status} = 'pending'`),
    index("workflow_run_dispatch_anchor_status_idx").on(
      t.anchorRunId,
      t.status,
    ),
    check(
      "workflow_run_dispatch_status_check",
      sql`${t.status} in ('pending', 'acknowledged', 'settled', 'failed')`,
    ),
    check(
      "workflow_run_dispatch_kind_check",
      sql`${t.kind} in ('mail', 'signal')`,
    ),
    check(
      "workflow_run_dispatch_attempt_count_check",
      sql`${t.attemptCount} >= 0`,
    ),
    check(
      "workflow_run_dispatch_acknowledged_generation_check",
      sql`${t.acknowledgedGeneration} is null or ${t.acknowledgedGeneration} >= 0`,
    ),
    check(
      "workflow_run_dispatch_acknowledged_state_check",
      sql`${t.status} <> 'acknowledged' or ${t.acknowledgedGeneration} is not null`,
    ),
    check(
      "workflow_run_dispatch_pending_schedule_check",
      sql`${t.status} <> 'pending' or ${t.nextAttemptAt} is not null`,
    ),
  ],
);

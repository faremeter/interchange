import { pgTable, text, timestamp } from "drizzle-orm/pg-core";

import { principal } from "./principals";
import { tenant } from "./tenants";
import { workflowDeployment } from "./workflow-deployments";

// workflow_run is the per-run authorization subject: one row per run of a
// workflow deployment. It gives "this approval belongs to a real run of this
// deployment" a database referent, so approvals and signal correlations anchor
// to a run row rather than a bare run id string.
//
// The id is opaque and heterogeneous: externally-triggered runs use the
// trigger mail's messageId, while internal (workflow-spawned) runs use a
// freshly minted run id. Both are just run identifiers, so no format is
// enforced on the column.
export const workflowRun = pgTable("workflow_run", {
  id: text("id").primaryKey(),
  deploymentId: text("deployment_id")
    .notNull()
    .references(() => workflowDeployment.id, { onDelete: "cascade" }),
  tenantId: text("tenant_id")
    .notNull()
    .references(() => tenant.id, { onDelete: "cascade" }),
  // Nullable by design: an internal, workflow-spawned run has no principal of
  // its own -- it inherits the deployment's grants at read time -- so null is
  // the honest signal for "this run has no own principal." `onDelete: "set
  // null"` is deliberate (unlike agent_instance's principal, which uses the
  // implicit default): deleting a run's principal nulls the column rather than
  // wedging the run row.
  principalId: text("principal_id").references(() => principal.id, {
    onDelete: "set null",
  }),
  // Only "running" is ever written today; a run is born running and stays so.
  // The terminal transitions are a separate concern and no code writes them.
  status: text("status", {
    enum: ["running", "completed", "failed", "cancelled"],
  })
    .notNull()
    .default("running"),
  createdAt: timestamp("created_at").notNull().defaultNow(),
  // Nullable: a run has no end time until it reaches a terminal state.
  endedAt: timestamp("ended_at"),
});

import { pgTable, text, timestamp } from "drizzle-orm/pg-core";

import { tenant } from "./tenants";
import { workflowDeployment } from "./workflow-deployments";

export const signalCorrelation = pgTable("signal_correlation", {
  correlationId: text("correlation_id").primaryKey(),
  tenantId: text("tenant_id")
    .notNull()
    .references(() => tenant.id, { onDelete: "cascade" }),
  // Co-written with an `approval` that references the same deployment and
  // already cascades on its delete. A correlation whose deployment is gone can
  // never resolve, and its co-written approval has already cascaded away, so
  // cascading the correlation keeps the pair consistent rather than leaving a
  // correlation pointing at a vanished deployment.
  deploymentId: text("deployment_id")
    .notNull()
    .references(() => workflowDeployment.id, { onDelete: "cascade" }),
  agentAddress: text("agent_address").notNull(),
  runId: text("run_id").notNull(),
  signalName: text("signal_name").notNull(),
  kind: text("kind", { enum: ["approval"] }).notNull(),
  signalId: text("signal_id"),
  resolvedAt: timestamp("resolved_at"),
  createdAt: timestamp("created_at").notNull().defaultNow(),
});

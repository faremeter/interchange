import { pgTable, text, timestamp } from "drizzle-orm/pg-core";

import { tenant } from "./tenants";
import { workflowDeployment } from "./workflow-deployments";
import { workflowRun } from "./workflow-run";

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
  // References the run this correlation belongs to. Cascades on the run's
  // delete: a correlation whose run is gone can never resolve, and the
  // deployment-level cascade above already removes it when the whole deployment
  // is torn down. The `deployment_id` FK stays as the lock and cascade anchor
  // the co-write orders against.
  runId: text("run_id")
    .notNull()
    .references(() => workflowRun.id, { onDelete: "cascade" }),
  signalName: text("signal_name").notNull(),
  kind: text("kind", { enum: ["approval"] }).notNull(),
  signalId: text("signal_id"),
  resolvedAt: timestamp("resolved_at"),
  createdAt: timestamp("created_at").notNull().defaultNow(),
});

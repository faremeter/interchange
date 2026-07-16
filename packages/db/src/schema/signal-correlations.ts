import { pgTable, text, timestamp } from "drizzle-orm/pg-core";

import { tenant } from "./tenants";

export const signalCorrelation = pgTable("signal_correlation", {
  correlationId: text("correlation_id").primaryKey(),
  tenantId: text("tenant_id")
    .notNull()
    .references(() => tenant.id, { onDelete: "cascade" }),
  deploymentId: text("deployment_id").notNull(),
  agentAddress: text("agent_address").notNull(),
  runId: text("run_id").notNull(),
  signalName: text("signal_name").notNull(),
  kind: text("kind", { enum: ["approval"] }).notNull(),
  signalId: text("signal_id"),
  resolvedAt: timestamp("resolved_at"),
  createdAt: timestamp("created_at").notNull().defaultNow(),
});

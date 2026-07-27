import { pgTable, text, timestamp } from "drizzle-orm/pg-core";

import { principal } from "./principals";
import { tenant } from "./tenants";
import { workflowDefinition } from "./workflow-definitions";

export const agentSession = pgTable("agent_session", {
  id: text("id").primaryKey(),
  tenantId: text("tenant_id")
    .notNull()
    .references(() => tenant.id, { onDelete: "cascade" }),
  // Retains the agent_id column name, but its values are workflow_definition
  // ids: a launched session is keyed to the folded definition it runs, so it
  // survives the agent table's retirement. ON DELETE restrict -- session
  // history is an audit record, so a definition with live sessions cannot be
  // dropped out from under them.
  agentId: text("agent_id")
    .notNull()
    .references(() => workflowDefinition.id, { onDelete: "restrict" }),
  principalId: text("principal_id")
    .notNull()
    .references(() => principal.id),
  status: text("status", {
    enum: ["active", "ending", "ended"],
  })
    .notNull()
    .default("active"),
  createdAt: timestamp("created_at").notNull().defaultNow(),
  updatedAt: timestamp("updated_at").notNull().defaultNow(),
  endedAt: timestamp("ended_at"),
});

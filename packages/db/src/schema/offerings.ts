import { jsonb, pgTable, text, timestamp } from "drizzle-orm/pg-core";

import { tenant } from "./tenants";
import { workflowDefinition } from "./workflow-definitions";

export const offering = pgTable("offering", {
  id: text("id").primaryKey(),
  // The workflow definition this offering prices. The column keeps its
  // `agent_id` name for contract stability -- the offering API exposes
  // `agentId`/`agentName` -- but it now holds a workflow_definition id (a folded
  // agent's definition). `restrict`, not cascade: an offering is a commercial
  // pricing record, so deleting its definition must consciously deal with the
  // offering rather than silently vaporize it.
  agentId: text("agent_id")
    .notNull()
    .references(() => workflowDefinition.id, { onDelete: "restrict" }),
  tenantId: text("tenant_id")
    .notNull()
    .references(() => tenant.id, { onDelete: "cascade" }),
  name: text("name").notNull(),
  description: text("description"),
  pricing: jsonb("pricing"),
  schema: jsonb("schema"),
  createdAt: timestamp("created_at").notNull().defaultNow(),
  updatedAt: timestamp("updated_at").notNull().defaultNow(),
});

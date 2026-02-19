import { jsonb, pgTable, text, timestamp } from "drizzle-orm/pg-core";

import { agent } from "./agents";
import { tenant } from "./tenants";

export const offering = pgTable("offering", {
  id: text("id").primaryKey(),
  agentId: text("agent_id")
    .notNull()
    .references(() => agent.id, { onDelete: "cascade" }),
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

import { pgTable, text, timestamp } from "drizzle-orm/pg-core";

import { agent } from "./agents";
import { principal } from "./principals";
import { tenant } from "./tenants";

export const agentSession = pgTable("agent_session", {
  id: text("id").primaryKey(),
  tenantId: text("tenant_id")
    .notNull()
    .references(() => tenant.id, { onDelete: "cascade" }),
  agentId: text("agent_id")
    .notNull()
    .references(() => agent.id, { onDelete: "cascade" }),
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

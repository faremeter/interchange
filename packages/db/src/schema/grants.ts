import { jsonb, pgTable, text, timestamp } from "drizzle-orm/pg-core";

import { principal } from "./principals";
import { role } from "./roles";
import { tenant } from "./tenants";

export const grant = pgTable("grant", {
  id: text("id").primaryKey(),
  tenantId: text("tenant_id")
    .notNull()
    .references(() => tenant.id, { onDelete: "cascade" }),
  roleId: text("role_id").references(() => role.id, { onDelete: "cascade" }),
  principalId: text("principal_id").references(() => principal.id, {
    onDelete: "cascade",
  }),
  resource: text("resource").notNull(),
  action: text("action").notNull(),
  effect: text("effect", { enum: ["allow", "deny", "ask"] }).notNull(),
  conditions: jsonb("conditions"),
  source: text("source", {
    enum: ["system", "role", "creator", "invoker"],
  }).notNull(),
  expiresAt: timestamp("expires_at"),
  createdAt: timestamp("created_at").notNull().defaultNow(),
  updatedAt: timestamp("updated_at").notNull().defaultNow(),
});

import { jsonb, pgTable, text, timestamp } from "drizzle-orm/pg-core";

import { agent } from "./agents";
import { agentInstance } from "./instances";
import { principal } from "./principals";
import { tenant } from "./tenants";

export const approval = pgTable("approval", {
  id: text("id").primaryKey(),
  tenantId: text("tenant_id")
    .notNull()
    .references(() => tenant.id, { onDelete: "cascade" }),
  instanceId: text("instance_id")
    .notNull()
    .references(() => agentInstance.id, { onDelete: "cascade" }),
  agentId: text("agent_id")
    .notNull()
    .references(() => agent.id, { onDelete: "cascade" }),
  originPrincipalId: text("origin_principal_id")
    .notNull()
    .references(() => principal.id, { onDelete: "cascade" }),
  correlationId: text("correlation_id").notNull().unique(),
  // The approver-facing tool snapshot. Nullable because the reactor's
  // suspend-time event does not carry the snapshot: the tool definition and
  // arguments are not reachable at the reactor's suspend point without
  // inference-layer plumbing, so the row is created without them and they are
  // enriched later once that plumbing exists.
  toolDefinition: jsonb("tool_definition"),
  toolArguments: jsonb("tool_arguments"),
  scope: text("scope", { enum: ["once", "always"] }),
  status: text("status", {
    enum: ["pending", "approved", "rejected", "timeout", "expired"],
  })
    .notNull()
    .default("pending"),
  originKind: text("origin_kind", {
    enum: ["system", "role", "creator", "invoker"],
  }).notNull(),
  timeoutAt: timestamp("timeout_at").notNull(),
  resolvedAt: timestamp("resolved_at"),
  createdAt: timestamp("created_at").notNull().defaultNow(),
  updatedAt: timestamp("updated_at").notNull().defaultNow(),
});

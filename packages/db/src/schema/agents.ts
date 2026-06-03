import { jsonb, pgTable, text, timestamp } from "drizzle-orm/pg-core";

import { principal } from "./principals";
import { tenant } from "./tenants";

export const agent = pgTable("agent", {
  id: text("id").primaryKey(),
  tenantId: text("tenant_id")
    .notNull()
    .references(() => tenant.id, { onDelete: "cascade" }),
  // Tracks the definition author's principal for resolving source:"creator"
  // grant requirements at launch. See AUTH.md § Grant Requirements on Definitions.
  creatorPrincipalId: text("creator_principal_id")
    .notNull()
    .references(() => principal.id),
  name: text("name").notNull(),
  description: text("description"),
  systemPrompt: text("system_prompt"),
  contextConfig: jsonb("context_config"),
  initialState: jsonb("initial_state"),
  modelConfig: jsonb("model_config"),
  capabilities: jsonb("capabilities"),
  credentialRequirements: jsonb("credential_requirements"),
  // Grant requirements manifest — resolved at launch into materialized grants
  // on the instance principal. See AUTH.md § Grant Requirements on Definitions.
  grantRequirements: jsonb("grant_requirements"),
  currentVersion: text("current_version").notNull().default("1"),
  status: text("status", {
    enum: ["deployed", "stopped"],
  })
    .notNull()
    .default("deployed"),
  createdAt: timestamp("created_at").notNull().defaultNow(),
  updatedAt: timestamp("updated_at").notNull().defaultNow(),
});

export const agentVersion = pgTable("agent_version", {
  id: text("id").primaryKey(),
  agentId: text("agent_id")
    .notNull()
    .references(() => agent.id, { onDelete: "cascade" }),
  version: text("version").notNull(),
  status: text("status", {
    enum: ["active", "inactive", "failed"],
  })
    .notNull()
    .default("active"),
  createdAt: timestamp("created_at").notNull().defaultNow(),
});

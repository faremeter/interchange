import { jsonb, pgTable, text, timestamp } from "drizzle-orm/pg-core";

import { principal } from "./principals";
import { sidecar } from "./sidecar";
import { tenant } from "./tenants";

export const agent = pgTable("agent", {
  id: text("id").primaryKey(),
  tenantId: text("tenant_id")
    .notNull()
    .references(() => tenant.id, { onDelete: "cascade" }),
  principalId: text("principal_id")
    .notNull()
    .references(() => principal.id),
  name: text("name").notNull(),
  description: text("description"),
  systemPrompt: text("system_prompt"),
  skills: jsonb("skills"),
  contextConfig: jsonb("context_config"),
  initialState: jsonb("initial_state"),
  modelConfig: jsonb("model_config"),
  capabilities: jsonb("capabilities"),
  credentialRequirements: jsonb("credential_requirements"),
  currentVersion: text("current_version").notNull().default("1"),
  status: text("status", {
    enum: ["deployed", "stopped", "updating", "error", "running"],
  })
    .notNull()
    .default("deployed"),
  sidecarId: text("sidecar_id").references(() => sidecar.id, {
    onDelete: "set null",
  }),
  publicKey: text("public_key"),
  kernelId: text("kernel_id"),
  sessionId: text("session_id"),
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

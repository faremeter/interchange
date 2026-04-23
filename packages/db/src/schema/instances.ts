import { pgTable, text, timestamp } from "drizzle-orm/pg-core";

import { agent, agentVersion } from "./agents";
import { agentSession } from "./sessions";
import { principal } from "./principals";
import { sidecar } from "./sidecar";
import { tenant } from "./tenants";

export const agentInstance = pgTable("agent_instance", {
  id: text("id").primaryKey(),
  agentId: text("agent_id")
    .notNull()
    .references(() => agent.id, { onDelete: "cascade" }),
  tenantId: text("tenant_id")
    .notNull()
    .references(() => tenant.id, { onDelete: "cascade" }),
  principalId: text("principal_id")
    .notNull()
    .references(() => principal.id),
  address: text("address").notNull().unique(),
  versionId: text("version_id").references(() => agentVersion.id),
  sessionId: text("session_id").references(() => agentSession.id),
  status: text("status", {
    enum: ["deployed", "running", "updating", "error", "stopped"],
  })
    .notNull()
    .default("deployed"),
  sidecarId: text("sidecar_id").references(() => sidecar.id, {
    onDelete: "set null",
  }),
  publicKey: text("public_key"),
  kernelId: text("kernel_id"),
  createdAt: timestamp("created_at").notNull().defaultNow(),
  updatedAt: timestamp("updated_at").notNull().defaultNow(),
  endedAt: timestamp("ended_at"),
});

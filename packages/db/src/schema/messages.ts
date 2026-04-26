import { integer, jsonb, pgTable, text, timestamp } from "drizzle-orm/pg-core";

import { agentInstance } from "./instances";
import { agentSession } from "./sessions";
import { tenant } from "./tenants";

export const sessionMessage = pgTable("session_message", {
  id: text("id").primaryKey(),
  sessionId: text("session_id")
    .notNull()
    .references(() => agentSession.id, { onDelete: "cascade" }),
  instanceId: text("instance_id").references(() => agentInstance.id, {
    onDelete: "cascade",
  }),
  tenantId: text("tenant_id")
    .notNull()
    .references(() => tenant.id, { onDelete: "cascade" }),
  role: text("role", { enum: ["user", "assistant"] }).notNull(),
  from: text("from").notNull(),
  status: text("status", { enum: ["pending", "delivered", "failed"] })
    .notNull()
    .default("pending"),
  createdAt: timestamp("created_at").notNull().defaultNow(),
});

export const messagePart = pgTable("message_part", {
  id: text("id").primaryKey(),
  messageId: text("message_id")
    .notNull()
    .references(() => sessionMessage.id, { onDelete: "cascade" }),
  sessionId: text("session_id")
    .notNull()
    .references(() => agentSession.id, { onDelete: "cascade" }),
  type: text("type", {
    enum: [
      "text",
      "reasoning",
      "tool",
      "file",
      "error",
      "step-start",
      "step-finish",
      "snapshot",
      "patch",
    ],
  }).notNull(),
  content: text("content"),
  metadata: jsonb("metadata"),
  ordinal: integer("ordinal").notNull(),
});

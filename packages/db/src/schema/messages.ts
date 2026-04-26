import {
  customType,
  index,
  integer,
  jsonb,
  pgTable,
  text,
  timestamp,
} from "drizzle-orm/pg-core";

import { agentInstance } from "./instances";
import { agentSession } from "./sessions";
import { tenant } from "./tenants";

const bytea = customType<{ data: Uint8Array; driverData: Buffer }>({
  dataType() {
    return "bytea";
  },
  toDriver(value) {
    return Buffer.from(value);
  },
  fromDriver(value) {
    return new Uint8Array(value);
  },
});

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

export const inferenceTurn = pgTable(
  "inference_turn",
  {
    id: text("id").primaryKey(),
    sessionId: text("session_id")
      .notNull()
      .references(() => agentSession.id, { onDelete: "cascade" }),
    instanceId: text("instance_id")
      .notNull()
      .references(() => agentInstance.id, { onDelete: "cascade" }),
    tenantId: text("tenant_id")
      .notNull()
      .references(() => tenant.id, { onDelete: "cascade" }),
    model: text("model").notNull(),
    status: text("status", { enum: ["running", "completed", "failed"] })
      .notNull()
      .default("running"),
    startedAt: timestamp("started_at").notNull(),
    endedAt: timestamp("ended_at"),
  },
  (t) => [
    index("inference_turn_instance_id_started_at_idx").on(
      t.instanceId,
      t.startedAt,
    ),
  ],
);

export const turnPart = pgTable("turn_part", {
  id: text("id").primaryKey(),
  turnId: text("turn_id")
    .notNull()
    .references(() => inferenceTurn.id, { onDelete: "cascade" }),
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

export const sessionMail = pgTable(
  "session_mail",
  {
    id: text("id").primaryKey(),
    sessionId: text("session_id")
      .notNull()
      .references(() => agentSession.id, { onDelete: "cascade" }),
    instanceId: text("instance_id").references(() => agentInstance.id, {
      onDelete: "set null",
    }),
    tenantId: text("tenant_id")
      .notNull()
      .references(() => tenant.id, { onDelete: "cascade" }),
    direction: text("direction", { enum: ["inbound", "outbound"] }).notNull(),
    status: text("status", { enum: ["pending", "delivered"] })
      .notNull()
      .default("pending"),
    raw: bytea("raw").notNull(),
    createdAt: timestamp("created_at").notNull().defaultNow(),
  },
  (t) => [
    index("session_mail_instance_id_created_at_idx").on(
      t.instanceId,
      t.createdAt,
    ),
  ],
);

import {
  index,
  integer,
  jsonb,
  pgTable,
  text,
  timestamp,
} from "drizzle-orm/pg-core";

import { bytea } from "./column-types";
import { agentInstance } from "./instances";
import { agentSession } from "./sessions";
import { tenant } from "./tenants";

export const inferenceTurn = pgTable(
  "inference_turn",
  {
    id: text("id").primaryKey(),
    sessionId: text("session_id")
      .notNull()
      .references(() => agentSession.id, { onDelete: "cascade" }),
    // The endpoint that produced this turn: a legacy agent_instance id or a
    // folded workflow_run id, drawn from one shared id space. This is a
    // polymorphic reference across two tables, so it carries no foreign key
    // (mirroring workflow_definition.originAgentId); the collector-creation
    // layer owns the invariant that the id names a live endpoint. It stays
    // NOT NULL -- a turn always names its producer.
    instanceId: text("instance_id").notNull(),
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
      "refusal",
      "safety_rating",
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
    // A folded run's mail carries a null instanceId, so its history and prior-
    // mail queries key on the session instead. This index supports that scan
    // the way the instanceId index supports a legacy instance's.
    index("session_mail_session_id_created_at_idx").on(
      t.sessionId,
      t.createdAt,
    ),
  ],
);

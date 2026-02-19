import { jsonb, pgTable, text, timestamp } from "drizzle-orm/pg-core";

import { agent } from "./agents";
import { tenant } from "./tenants";

export const wallet = pgTable("wallet", {
  id: text("id").primaryKey(),
  tenantId: text("tenant_id")
    .notNull()
    .references(() => tenant.id, { onDelete: "cascade" }),
  name: text("name").notNull(),
  backendType: text("backend_type", {
    enum: ["crypto", "fiat", "credits"],
  }).notNull(),
  currency: text("currency").notNull(),
  balance: text("balance").notNull().default("0"),
  config: jsonb("config"),
  createdAt: timestamp("created_at").notNull().defaultNow(),
  updatedAt: timestamp("updated_at").notNull().defaultNow(),
});

export const transaction = pgTable("transaction", {
  id: text("id").primaryKey(),
  walletId: text("wallet_id")
    .notNull()
    .references(() => wallet.id, { onDelete: "cascade" }),
  agentId: text("agent_id").references(() => agent.id, {
    onDelete: "set null",
  }),
  direction: text("direction", {
    enum: ["inbound", "outbound"],
  }).notNull(),
  amount: text("amount").notNull(),
  currency: text("currency").notNull(),
  recipientId: text("recipient_id"),
  senderId: text("sender_id"),
  requestId: text("request_id"),
  status: text("status", {
    enum: ["pending", "completed", "failed"],
  })
    .notNull()
    .default("pending"),
  createdAt: timestamp("created_at").notNull().defaultNow(),
});

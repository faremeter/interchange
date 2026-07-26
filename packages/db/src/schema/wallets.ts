import { jsonb, pgTable, text, timestamp } from "drizzle-orm/pg-core";

import { tenant } from "./tenants";
import { workflowRun } from "./workflow-run";

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
  // The run this ledger entry is attributed to. Nullable with set-null on
  // delete: ledger history must survive deletion of the run it references.
  runId: text("run_id").references(() => workflowRun.id, {
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

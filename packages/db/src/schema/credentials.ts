import { jsonb, pgTable, text, timestamp } from "drizzle-orm/pg-core";

import { tenant } from "./tenants";

export const credential = pgTable("credential", {
  id: text("id").primaryKey(),
  tenantId: text("tenant_id")
    .notNull()
    .references(() => tenant.id, { onDelete: "cascade" }),
  name: text("name").notNull(),
  type: text("type", {
    enum: ["api_key", "oauth_token", "certificate", "other"],
  }).notNull(),
  description: text("description"),
  secret: text("secret").notNull(),
  metadata: jsonb("metadata"),
  createdAt: timestamp("created_at").notNull().defaultNow(),
  updatedAt: timestamp("updated_at").notNull().defaultNow(),
});

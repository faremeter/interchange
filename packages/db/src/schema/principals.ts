import { pgTable, text, timestamp, unique } from "drizzle-orm/pg-core";

import { user } from "./auth";
import { tenant } from "./tenants";

export const principal = pgTable(
  "principal",
  {
    id: text("id").primaryKey(),
    tenantId: text("tenant_id")
      .notNull()
      .references(() => tenant.id, { onDelete: "cascade" }),
    kind: text("kind", { enum: ["user", "agent"] }).notNull(),
    refId: text("ref_id").notNull(),
    status: text("status", {
      enum: ["active", "suspended", "invited", "deactivated"],
    }).notNull(),
    createdAt: timestamp("created_at").notNull().defaultNow(),
    updatedAt: timestamp("updated_at").notNull().defaultNow(),
  },
  (t) => [unique().on(t.tenantId, t.kind, t.refId)],
);

// The refId on a user principal points to the auth user table.
// We don't add a FK constraint because refId also points to agent.id
// when kind='agent', and Postgres can't do conditional FKs.
// The application layer enforces referential integrity.
export { user };

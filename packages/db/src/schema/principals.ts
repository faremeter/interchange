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
    kind: text("kind", { enum: ["user", "agent", "workflow"] }).notNull(),
    refId: text("ref_id").notNull(),
    status: text("status", {
      enum: ["active", "suspended", "invited", "deactivated"],
    }).notNull(),
    createdAt: timestamp("created_at").notNull().defaultNow(),
    updatedAt: timestamp("updated_at").notNull().defaultNow(),
  },
  (t) => [unique().on(t.tenantId, t.kind, t.refId)],
);

// refId is polymorphic on kind: the auth user for kind='user', and either a
// workflow_run or a workflow_definition for kind='workflow'. A `kind='agent'`
// principal is a legacy survivor of the fold -- its refId named an
// `agent_instance` row, a table now dropped, so it dangles and is inert.
// Postgres can't express a conditional FK across those targets, so there is no
// FK constraint here and the application layer enforces referential integrity.
export { user };

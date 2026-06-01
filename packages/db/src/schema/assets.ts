import { pgTable, text, timestamp, unique } from "drizzle-orm/pg-core";

import { principal } from "./principals";
import { tenant } from "./tenants";

export const asset = pgTable(
  "asset",
  {
    id: text("id").primaryKey(),
    tenantId: text("tenant_id")
      .notNull()
      .references(() => tenant.id, { onDelete: "cascade" }),
    kind: text("kind").notNull(),
    name: text("name").notNull(),
    displayName: text("display_name"),
    creatorPrincipalId: text("creator_principal_id").references(
      () => principal.id,
      { onDelete: "set null" },
    ),
    createdAt: timestamp("created_at").notNull().defaultNow(),
    updatedAt: timestamp("updated_at").notNull().defaultNow(),
  },
  (t) => [unique("asset_tenant_kind_name").on(t.tenantId, t.kind, t.name)],
);

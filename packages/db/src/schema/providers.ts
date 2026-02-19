import { jsonb, pgTable, text, timestamp, unique } from "drizzle-orm/pg-core";

import { tenant } from "./tenants";

export const provider = pgTable(
  "provider",
  {
    id: text("id").primaryKey(),
    tenantId: text("tenant_id")
      .notNull()
      .references(() => tenant.id, { onDelete: "cascade" }),
    name: text("name").notNull(),
    plugin: text("plugin").notNull(),
    authorizationUrl: text("authorization_url"),
    tokenUrl: text("token_url"),
    userInfoUrl: text("user_info_url"),
    scopes: text("scopes").array(),
    metadata: jsonb("metadata"),
    createdAt: timestamp("created_at").notNull().defaultNow(),
    updatedAt: timestamp("updated_at").notNull().defaultNow(),
  },
  (t) => [unique("provider_tenant_name").on(t.tenantId, t.name)],
);

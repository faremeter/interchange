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
    // The API origin a credential from this provider authenticates to (e.g.
    // https://api.github.com). Optional: OAuth-login-only providers have no
    // API-call origin. A provider that backs an origin-pinned credential must
    // have one, enforced loudly at credential-shape time, not by the schema.
    apiBaseUrl: text("api_base_url"),
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

import { jsonb, pgTable, text, timestamp, unique } from "drizzle-orm/pg-core";

import { provider } from "./providers";
import { tenant } from "./tenants";

export const oauthClient = pgTable(
  "oauth_client",
  {
    id: text("id").primaryKey(),
    tenantId: text("tenant_id")
      .notNull()
      .references(() => tenant.id, { onDelete: "cascade" }),
    providerId: text("provider_id")
      .notNull()
      .references(() => provider.id, { onDelete: "cascade" }),
    name: text("name").notNull(),
    clientId: text("client_id").notNull(),
    clientSecret: text("client_secret").notNull(),
    redirectUris: text("redirect_uris").array(),
    defaultScopes: text("default_scopes").array(),
    metadata: jsonb("metadata"),
    createdAt: timestamp("created_at").notNull().defaultNow(),
    updatedAt: timestamp("updated_at").notNull().defaultNow(),
  },
  (t) => [unique("oauth_client_tenant_provider").on(t.tenantId, t.providerId)],
);

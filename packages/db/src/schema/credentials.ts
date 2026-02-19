import { jsonb, pgTable, text, timestamp, unique } from "drizzle-orm/pg-core";

import { oauthClient } from "./oauth-clients";
import { principal } from "./principals";
import { provider } from "./providers";
import { tenant } from "./tenants";

export const credential = pgTable(
  "credential",
  {
    id: text("id").primaryKey(),
    tenantId: text("tenant_id")
      .notNull()
      .references(() => tenant.id, { onDelete: "cascade" }),
    principalId: text("principal_id").references(() => principal.id, {
      onDelete: "set null",
    }),
    providerId: text("provider_id")
      .notNull()
      .references(() => provider.id, { onDelete: "cascade" }),
    oauthClientId: text("oauth_client_id").references(() => oauthClient.id, {
      onDelete: "set null",
    }),
    name: text("name").notNull(),
    type: text("type", {
      enum: ["api_key", "oauth_token", "certificate", "other"],
    }).notNull(),
    description: text("description"),
    secret: text("secret").notNull(),
    refreshSecret: text("refresh_secret"),
    scopes: text("scopes").array(),
    expiresAt: timestamp("expires_at"),
    status: text("status", {
      enum: ["active", "expired", "revoked", "error"],
    })
      .notNull()
      .default("active"),
    metadata: jsonb("metadata"),
    createdAt: timestamp("created_at").notNull().defaultNow(),
    updatedAt: timestamp("updated_at").notNull().defaultNow(),
  },
  (t) => [unique("credential_tenant_name").on(t.tenantId, t.name)],
);

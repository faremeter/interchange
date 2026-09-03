import { pgTable, text, timestamp, unique } from "drizzle-orm/pg-core";

import { bytea } from "./column-types";
import { principal } from "./principals";
import { tenant } from "./tenants";

export const executionHost = pgTable(
  "execution_host",
  {
    id: text("id").primaryKey(),
    tenantId: text("tenant_id")
      .notNull()
      .references(() => tenant.id, { onDelete: "cascade" }),
    principalId: text("principal_id")
      .notNull()
      .references(() => principal.id, { onDelete: "cascade" }),
    ownerPrincipalId: text("owner_principal_id")
      .notNull()
      .references(() => principal.id, { onDelete: "restrict" }),
    displayName: text("display_name").notNull(),
    tokenHashSha256: bytea("token_hash_sha256").notNull().unique(),
    createdAt: timestamp("created_at").notNull().defaultNow(),
    updatedAt: timestamp("updated_at").notNull().defaultNow(),
  },
  (t) => [unique().on(t.principalId)],
);

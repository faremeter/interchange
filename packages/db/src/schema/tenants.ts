import { sql } from "drizzle-orm";
import {
  check,
  foreignKey,
  jsonb,
  pgTable,
  text,
  timestamp,
  unique,
  uniqueIndex,
} from "drizzle-orm/pg-core";

export const tenant = pgTable(
  "tenant",
  {
    id: text("id").primaryKey(),
    name: text("name").notNull(),
    slug: text("slug").notNull().unique(),
    // Unique on `lower(domain)`, not the raw column: an inbound mail `From` is
    // lowercased when parsed, so a sender address must map to exactly one
    // tenant. A case-sensitive unique would let case-variant domains coexist and
    // let a case-variant registration shadow another tenant's senders. The
    // creation boundary lowercases the domain; this index enforces it for every
    // write path.
    domain: text("domain").notNull(),
    parentId: text("parent_id"),
    config: jsonb("config"),
    createdAt: timestamp("created_at").notNull().defaultNow(),
    updatedAt: timestamp("updated_at").notNull().defaultNow(),
  },
  (t) => [
    foreignKey({ columns: [t.parentId], foreignColumns: [t.id] }),
    uniqueIndex("tenant_domain_lower_idx").on(sql`lower(${t.domain})`),
    // Constrains `slug`, and only `slug`, to a single DNS label: RFC 1035
    // section 2.3.1 as relaxed by RFC 1123 section 2.1, the same grammar the
    // `TenantSlug` validator applies at the create route, enforced here for
    // every write path including the ones that bypass that route. `domain`
    // carries no grammar constraint and nothing here relates the two columns.
    check(
      "tenant_slug_dns_label_check",
      sql`${t.slug} ~ '^[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?$' and length(${t.slug}) <= 63`,
    ),
  ],
);

export const federationTrust = pgTable(
  "federation_trust",
  {
    id: text("id").primaryKey(),
    tenantId: text("tenant_id")
      .notNull()
      .references(() => tenant.id, { onDelete: "cascade" }),
    targetTenantId: text("target_tenant_id")
      .notNull()
      .references(() => tenant.id, { onDelete: "cascade" }),
    direction: text("direction", {
      enum: ["inbound", "outbound", "bilateral"],
    }).notNull(),
    createdAt: timestamp("created_at").notNull().defaultNow(),
  },
  (t) => [unique().on(t.tenantId, t.targetTenantId)],
);

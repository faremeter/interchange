import {
  foreignKey,
  jsonb,
  pgTable,
  text,
  timestamp,
  unique,
} from "drizzle-orm/pg-core";

export const tenant = pgTable(
  "tenant",
  {
    id: text("id").primaryKey(),
    name: text("name").notNull(),
    slug: text("slug").notNull().unique(),
    domain: text("domain").notNull().unique(),
    parentId: text("parent_id"),
    config: jsonb("config"),
    createdAt: timestamp("created_at").notNull().defaultNow(),
    updatedAt: timestamp("updated_at").notNull().defaultNow(),
  },
  (t) => [foreignKey({ columns: [t.parentId], foreignColumns: [t.id] })],
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

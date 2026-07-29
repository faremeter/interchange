import {
  boolean,
  pgTable,
  primaryKey,
  text,
  timestamp,
} from "drizzle-orm/pg-core";

import { principal } from "./principals";
import { tenant } from "./tenants";
import { workflowDefinition } from "./workflow-definitions";

export const role = pgTable("role", {
  id: text("id").primaryKey(),
  tenantId: text("tenant_id")
    .notNull()
    .references(() => tenant.id, { onDelete: "cascade" }),
  name: text("name").notNull(),
  description: text("description"),
  isSystem: boolean("is_system").notNull().default(false),
  createdAt: timestamp("created_at").notNull().defaultNow(),
  updatedAt: timestamp("updated_at").notNull().defaultNow(),
});

export const agentRole = pgTable(
  "agent_role",
  {
    // Retains the agent_id column name, but its values are workflow_definition
    // ids: every agent was folded to exactly one definition, and role
    // assignments follow the definition so they survived the agent table's
    // retirement.
    agentId: text("agent_id")
      .notNull()
      .references(() => workflowDefinition.id, { onDelete: "cascade" }),
    roleId: text("role_id")
      .notNull()
      .references(() => role.id, { onDelete: "cascade" }),
    createdAt: timestamp("created_at").notNull().defaultNow(),
  },
  (t) => [primaryKey({ columns: [t.agentId, t.roleId] })],
);

export const principalRole = pgTable(
  "principal_role",
  {
    principalId: text("principal_id")
      .notNull()
      .references(() => principal.id, { onDelete: "cascade" }),
    roleId: text("role_id")
      .notNull()
      .references(() => role.id, { onDelete: "cascade" }),
    createdAt: timestamp("created_at").notNull().defaultNow(),
  },
  (t) => [primaryKey({ columns: [t.principalId, t.roleId] })],
);

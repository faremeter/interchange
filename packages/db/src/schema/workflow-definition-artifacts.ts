import { sql } from "drizzle-orm";
import {
  check,
  index,
  jsonb,
  pgTable,
  text,
  timestamp,
} from "drizzle-orm/pg-core";

import { asset } from "./assets";
import { tenant } from "./tenants";

// A revision freezes dependency resolution independently of the probe cache
// version. Invalidating a validated probe must not silently upgrade its
// dependencies.
export const workflowDefinitionRevision = pgTable(
  "workflow_definition_revision",
  {
    id: text("id").primaryKey(),
    tenantId: text("tenant_id")
      .notNull()
      .references(() => tenant.id, { onDelete: "restrict" }),
    assetId: text("asset_id")
      .notNull()
      .references(() => asset.id, { onDelete: "restrict" }),
    assetCommitSha: text("asset_commit_sha").notNull(),
    source: jsonb("source").notNull(),
    entry: text("entry").notNull(),
    closure: jsonb("closure").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
);

export const workflowDefinitionArtifact = pgTable(
  "workflow_definition_artifact",
  {
    id: text("id").primaryKey(),
    revisionId: text("revision_id")
      .notNull()
      .references(() => workflowDefinitionRevision.id, {
        onDelete: "restrict",
      }),
    result: jsonb("result"),
    leaseOwner: text("lease_owner"),
    leaseExpiresAt: timestamp("lease_expires_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (t) => [
    index("workflow_definition_artifact_revision_idx").on(t.revisionId),
    check(
      "workflow_definition_artifact_state_check",
      sql`(${t.result} is null and ${t.leaseOwner} is not null and ${t.leaseExpiresAt} is not null) or (${t.result} is not null and ${t.leaseOwner} is null and ${t.leaseExpiresAt} is null)`,
    ),
  ],
);

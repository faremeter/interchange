import { sql } from "drizzle-orm";
import {
  index,
  jsonb,
  pgTable,
  text,
  timestamp,
  unique,
  uniqueIndex,
} from "drizzle-orm/pg-core";

import { asset } from "./assets";
import { principal } from "./principals";
import { tenant } from "./tenants";

// workflow_definition is the first-class definition entity for the workflow
// model: one row per deployable definition, with a version table alongside it.
//
// Unlike `agent`, the definition body (system prompt, context/model config,
// tool packages) is not stored on this row. For a workflow-origin definition
// the body lives in the `workflow`-kind asset the row points at; for an
// agent-origin definition (backfilled from `agent`) the body is not a workflow
// asset at all, which is why `asset_id` is nullable. The unique index over
// `asset_id` still bounds it to at most one definition per workflow asset,
// because Postgres treats NULLs as distinct.
export const workflowDefinition = pgTable(
  "workflow_definition",
  {
    id: text("id").primaryKey(),
    tenantId: text("tenant_id")
      .notNull()
      .references(() => tenant.id, { onDelete: "cascade" }),
    // Nullable to match the weaker of the two backfill sources: `asset`'s
    // creator principal is `onDelete: "set null"`, so a workflow asset whose
    // creator principal was removed carries no creator onto the definition.
    creatorPrincipalId: text("creator_principal_id").references(
      () => principal.id,
    ),
    // The `workflow`-kind asset holding this definition's body, or null for an
    // agent-origin definition that has no workflow asset. `restrict`: the
    // definition is the first-class entity, so deleting the asset must not
    // cascade into deleting the definition.
    assetId: text("asset_id").references(() => asset.id, {
      onDelete: "restrict",
    }),
    // Transient backfill scaffolding: the `agent.id` an agent-origin definition
    // was materialized from, so the grant re-key and the definition-level FK
    // re-anchors can join agent-keyed rows to their new definition. Null for
    // workflow-origin definitions (their `asset_id` is the back-reference).
    // A plain text column, not an FK, to avoid coupling to the `agent` table's
    // lifecycle during the migration; dropped when the agent table is dropped.
    originAgentId: text("origin_agent_id"),
    name: text("name").notNull(),
    description: text("description"),
    // Grant requirements manifest, resolved at launch into materialized grants.
    // Validated as GrantRequirement[] at parse time.
    grantRequirements: jsonb("grant_requirements"),
    // Model requirements manifest for an agent-origin (folded) definition: the
    // canonical model names + provider preferences its launch resolves against
    // the live tenant catalog into credential-bearing inference sources, fresh
    // each launch. Null for a workflow-origin definition, which carries no such
    // manifest -- a native workflow deploy supplies its sources directly.
    // Validated as ModelRequirements at parse time. Mirrors the field the
    // retired `agent` row carried, so a folded launch resolves models without
    // the agent row. A null (or empty) manifest is legitimate -- it resolves to
    // an unlaunchable empty source chain, exactly as the agent field did.
    modelRequirements: jsonb("model_requirements"),
    currentVersion: text("current_version").notNull().default("1"),
    status: text("status", {
      enum: ["deployed", "stopped"],
    })
      .notNull()
      .default("deployed"),
    createdAt: timestamp("created_at").notNull().defaultNow(),
    updatedAt: timestamp("updated_at").notNull().defaultNow(),
  },
  (t) => [
    index("workflow_definition_tenant_idx").on(t.tenantId, t.createdAt),
    uniqueIndex("workflow_definition_asset_idx").on(t.assetId),
    // Partial UNIQUE over `origin_agent_id` (where not null): at most one
    // definition per source agent, so the run-once backfill's double-fold is a
    // structural fail-loud rather than a procedural query-guard, and the
    // re-key joins from agent-keyed rows still resolve to one row. Partial
    // because workflow-origin definitions leave `origin_agent_id` null and must
    // not collide. Dropped with the column when the agent table is retired.
    uniqueIndex("workflow_definition_origin_agent_idx")
      .on(t.originAgentId)
      .where(sql`${t.originAgentId} is not null`),
  ],
);

export const workflowDefinitionVersion = pgTable(
  "workflow_definition_version",
  {
    id: text("id").primaryKey(),
    definitionId: text("definition_id")
      .notNull()
      .references(() => workflowDefinition.id, { onDelete: "cascade" }),
    version: text("version").notNull(),
    status: text("status", {
      enum: ["active", "inactive", "failed"],
    })
      .notNull()
      .default("active"),
    createdAt: timestamp("created_at").notNull().defaultNow(),
  },
  (t) => [
    // Unique per definition so rollback-by-version resolves to one row. The
    // mirror source `agent_version` omits this; the fresh table constrains it.
    unique("workflow_definition_version_definition_version").on(
      t.definitionId,
      t.version,
    ),
    index("workflow_definition_version_definition_idx").on(t.definitionId),
  ],
);

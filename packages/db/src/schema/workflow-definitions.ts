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
// The definition body (system prompt, context/model config, tool packages) is
// not stored on this row. For a workflow-kind definition the body lives in the
// `workflow`-kind asset the row points at; an instance-kind definition (a
// folded agent) has no workflow asset -- its launch synthesizes the body from
// this row's own columns -- which is why `asset_id` is nullable. The unique
// index over `asset_id` still bounds it to at most one definition per workflow
// asset, because Postgres treats NULLs as distinct.
export const workflowDefinition = pgTable(
  "workflow_definition",
  {
    id: text("id").primaryKey(),
    tenantId: text("tenant_id")
      .notNull()
      .references(() => tenant.id, { onDelete: "cascade" }),
    // Nullable to match the weaker of the two definition sources: `asset`'s
    // creator principal is `onDelete: "set null"`, so a workflow asset whose
    // creator principal was removed carries no creator onto the definition.
    creatorPrincipalId: text("creator_principal_id").references(
      () => principal.id,
    ),
    // The `workflow`-kind asset holding this definition's body, or null for an
    // instance-kind definition that has no workflow asset. `restrict`: the
    // definition is the first-class entity, so deleting the asset must not
    // cascade into deleting the definition.
    assetId: text("asset_id").references(() => asset.id, {
      onDelete: "restrict",
    }),
    name: text("name").notNull(),
    description: text("description"),
    // Grant requirements manifest, resolved at launch into materialized grants.
    // Validated as GrantRequirement[] at parse time.
    grantRequirements: jsonb("grant_requirements"),
    // Model requirements manifest for an instance-kind (folded) definition: the
    // canonical model names + provider preferences its launch resolves against
    // the live tenant catalog into credential-bearing inference sources, fresh
    // each launch. Null for a workflow-kind definition, which carries no such
    // manifest -- a native workflow deploy supplies its sources directly.
    // Validated as ModelRequirements at parse time. A null (or empty) manifest
    // is legitimate -- it resolves to an unlaunchable empty source chain.
    modelRequirements: jsonb("model_requirements"),
    // Whether this definition launches as a single born-running instance
    // (`instance`) or deploys as a multi-step workflow (`workflow`). The launch
    // and read surfaces gate on this to classify a definition: an instance-kind
    // definition is a folded agent, a workflow-kind definition is a native
    // workflow.
    kind: text("kind", { enum: ["instance", "workflow"] })
      .notNull()
      .default("workflow"),
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

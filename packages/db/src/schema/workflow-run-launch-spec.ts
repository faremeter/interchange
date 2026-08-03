import { integer, jsonb, pgTable, text, timestamp } from "drizzle-orm/pg-core";

import { principal } from "./principals";
import { workflowRun } from "./workflow-run";

/**
 * Immutable inputs required to recreate an exclusively placed workflow on a
 * replacement sidecar. Secrets are deliberately excluded: source offering
 * ids are resolved against the current catalog and principal authority when a
 * generation is launched.
 */
export const workflowRunLaunchSpec = pgTable("workflow_run_launch_spec", {
  anchorRunId: text("anchor_run_id")
    .primaryKey()
    .references(() => workflowRun.id, { onDelete: "cascade" }),
  schemaVersion: integer("schema_version").notNull().default(1),
  sessionId: text("session_id").notNull(),
  deploymentDomain: text("deployment_domain").notNull(),
  sourceAuthorityPrincipalId: text("source_authority_principal_id")
    .notNull()
    .references(() => principal.id, { onDelete: "restrict" }),
  definitionSnapshot: jsonb("definition_snapshot").notNull(),
  definitionHash: text("definition_hash").notNull(),
  sourceOfferingIds: jsonb("source_offering_ids").notNull(),
  defaultSourceOfferingId: text("default_source_offering_id").notNull(),
  deployContent: jsonb("deploy_content").notNull(),
  toolPackagePins: jsonb("tool_package_pins"),
  createdAt: timestamp("created_at").notNull().defaultNow(),
});

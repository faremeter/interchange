import { index, pgTable, text, timestamp } from "drizzle-orm/pg-core";

import { asset } from "./assets";
import { tenant } from "./tenants";

// workflow_deployment is a projection table written at deploy time by
// the general workflow deploy path. The RepoStore substrate has no
// cross-repo / by-kind listing API, so this table is the queryable
// index that backs "list the workflow deployments for a tenant".
//
// One row per deployment. `definitionAssetId` points at the
// `workflow`-kind asset whose `workflow.json` was hydrated into the
// deployed definition; `status` tracks the deployment's lifecycle. The
// row is the only authoritative record of the deployment outside the
// on-disk workflow / workflow-run repos.
export type WorkflowDeploymentStatus = "deployed" | "error";

export const workflowDeployment = pgTable(
  "workflow_deployment",
  {
    id: text("id").primaryKey(),
    tenantId: text("tenant_id")
      .notNull()
      .references(() => tenant.id, { onDelete: "cascade" }),
    definitionAssetId: text("definition_asset_id")
      .notNull()
      .references(() => asset.id, { onDelete: "cascade" }),
    status: text("status")
      .$type<WorkflowDeploymentStatus>()
      .notNull()
      .default("deployed"),
    createdAt: timestamp("created_at").notNull().defaultNow(),
  },
  (t) => [index("workflow_deployment_tenant_idx").on(t.tenantId, t.createdAt)],
);

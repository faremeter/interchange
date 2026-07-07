import {
  index,
  pgTable,
  text,
  timestamp,
  uniqueIndex,
} from "drizzle-orm/pg-core";

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
    // The deployment's routable address (`ins_<id>@<domain>`), stored rather
    // than re-derived at read time so the reconnect ownership challenge can
    // look up the deployment's public key by address, symmetrically with the
    // `agent_instance` path. Unique so a double-insert fails loud.
    //
    // Added NOT NULL with no default and no backfill. Like the repo's other
    // such adds (e.g. `credential.provider_id`), the migration relies on the
    // table being empty when it runs: this table and the `address` column
    // land one migration apart, both unreleased, so no populated row predates
    // the column and none needs a backfilled address.
    address: text("address").notNull(),
    // The Ed25519 public key the sidecar minted for this deployment address,
    // persisted at deploy-ack. Nullable by design: the row is written at
    // deploy-start and the key arrives at ack, so a not-yet-acked deployment
    // reads `null` and its reconnect challenge fails closed -- the address
    // stays unrouted rather than routing without ownership proof.
    publicKey: text("public_key"),
    status: text("status")
      .$type<WorkflowDeploymentStatus>()
      .notNull()
      .default("deployed"),
    createdAt: timestamp("created_at").notNull().defaultNow(),
  },
  (t) => [
    index("workflow_deployment_tenant_idx").on(t.tenantId, t.createdAt),
    uniqueIndex("workflow_deployment_address_idx").on(t.address),
  ],
);

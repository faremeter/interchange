import { index, jsonb, pgTable, text, timestamp } from "drizzle-orm/pg-core";

import { principal } from "./principals";
import { sidecar } from "./sidecar";
import { tenant } from "./tenants";
import { workflowDefinition } from "./workflow-definitions";
import { workflowDeployment } from "./workflow-deployments";

// workflow_run is the per-run authorization subject: one row per run of a
// workflow deployment. It gives "this approval belongs to a real run of this
// deployment" a database referent, so approvals and signal correlations anchor
// to a run row rather than a bare run id string.
//
// The id is opaque and heterogeneous: externally-triggered runs use the
// trigger mail's messageId, while internal (workflow-spawned) runs use a
// freshly minted run id. Both are just run identifiers, so no format is
// enforced on the column.
export const workflowRun = pgTable(
  "workflow_run",
  {
    id: text("id").primaryKey(),
    // The definition a run belongs to. Nullable and unwritten for now -- no
    // code sets it yet. It is the anchor a folded agent-origin run (which has no
    // deployment) uses, while a native-workflow run anchors on `deploymentId`.
    // `cascade` matches `deploymentId`'s onDelete so this column can take over
    // as the run's anchor, if the deployment table is dissolved, without
    // changing delete behavior.
    definitionId: text("definition_id").references(
      () => workflowDefinition.id,
      {
        onDelete: "cascade",
      },
    ),
    // Nullable as of the fold: a folded agent-origin run carries a
    // `definitionId` and no deployment, while native-workflow runs still set
    // this. The only runtime read of the column (`resolveWorkflowPrincipalNames`)
    // degrades gracefully when it is null.
    deploymentId: text("deployment_id").references(
      () => workflowDeployment.id,
      { onDelete: "cascade" },
    ),
    tenantId: text("tenant_id")
      .notNull()
      .references(() => tenant.id, { onDelete: "cascade" }),
    // Nullable by design: an internal, workflow-spawned run has no principal of
    // its own -- it inherits the deployment's grants at read time -- so null is
    // the honest signal for "this run has no own principal." `onDelete: "set
    // null"` is deliberate (unlike agent_instance's principal, which uses the
    // implicit default): deleting a run's principal nulls the column rather than
    // wedging the run row.
    principalId: text("principal_id").references(() => principal.id, {
      onDelete: "set null",
    }),
    // A run is born "running". It settles into a terminal status when its
    // terminal event lands on the workflow-run ref: the hub's pack-receive path
    // flips this column under a `status = 'running'` guard so a redelivered
    // terminal event does not re-terminate the run.
    status: text("status", {
      enum: ["running", "completed", "failed", "cancelled"],
    })
      .notNull()
      .default("running"),
    // Runtime bindings a folded run carries once the deploy path materializes
    // it, mirroring `agent_instance`; no code writes them yet, so they are null
    // shadow columns for now. `address` is nullable and NOT unique (unlike the
    // instance's NOT NULL UNIQUE): a run is not the addressable routing
    // endpoint, and runs of one deployment share its address, so a unique index
    // would collide.
    address: text("address"),
    publicKey: text("public_key"),
    sidecarId: text("sidecar_id").references(() => sidecar.id, {
      onDelete: "set null",
    }),
    kernelId: text("kernel_id"),
    modelPreferences: jsonb("model_preferences"),
    createdAt: timestamp("created_at").notNull().defaultNow(),
    // Nullable: a run has no end time until it reaches a terminal state.
    endedAt: timestamp("ended_at"),
  },
  (t) => [
    // Supports lookups of a definition's runs, the read path folded runs use
    // once `definitionId` is the anchor.
    index("workflow_run_definition_idx").on(t.definitionId),
  ],
);

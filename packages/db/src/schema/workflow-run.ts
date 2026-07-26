import { sql } from "drizzle-orm";
import {
  type AnyPgColumn,
  index,
  jsonb,
  pgTable,
  text,
  timestamp,
  uniqueIndex,
} from "drizzle-orm/pg-core";

import { principal } from "./principals";
import { sidecar } from "./sidecar";
import { tenant } from "./tenants";
import { workflowDefinition } from "./workflow-definitions";

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
    // The definition a run belongs to. A folded agent-origin run (which has no
    // deployment) sets it at launch; a native-workflow run takes its
    // deployment's definition -- backfilled for existing runs, and set at birth
    // for new ones -- and still anchors on `deploymentId`. Nullable: a run whose
    // deployment's asset was never folded has no definition yet, so it anchors
    // on `deploymentId` alone until that gap closes. `cascade` matches
    // `deploymentId`'s onDelete so this column can take over as the run's
    // anchor, if the deployment table is dissolved, without changing delete
    // behavior.
    definitionId: text("definition_id").references(
      () => workflowDefinition.id,
      {
        onDelete: "cascade",
      },
    ),
    // Nullable as of the fold: a folded agent-origin run carries a
    // `definitionId` and no deployment, while a deployment's anchor run sets
    // this to its own id and a child run sets it to its anchor's id. It
    // references the anchor `workflow_run` -- the run whose id equals the
    // deployment id -- so an anchor row is self-referential. The only runtime
    // read of the column (`resolveWorkflowPrincipalNames`) degrades gracefully
    // when it is null.
    deploymentId: text("deployment_id").references(
      (): AnyPgColumn => workflowRun.id,
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
    // Runtime bindings a folded run carries, mirroring `agent_instance`: a
    // folded run IS the launched instance, so it owns its routing endpoint.
    // `address` is that endpoint -- nullable because a run that anchors on a
    // deployment leaves it null and routes via the deployment, but unique among
    // the folded runs that set it (the partial unique index below). The folded
    // launch writes `address` and `modelPreferences`; `publicKey` is persisted
    // when the sidecar acks the deploy; no code populates `sidecarId`/`kernelId`.
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
    // A folded run owns a unique routing address, exactly like the agent
    // instance it stands in for; a run that anchors on a deployment instead
    // leaves `address` null and is not indexed. Partial so the many null-address
    // rows never collide and are kept out of the index.
    uniqueIndex("workflow_run_address_idx")
      .on(t.address)
      .where(sql`${t.address} is not null`),
  ],
);

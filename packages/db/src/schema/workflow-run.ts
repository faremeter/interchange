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
// The id is opaque and heterogeneous: a deployment's addressable top-level
// run uses its stable mail address, while internal body-child runs use freshly
// minted synthetic ids. Both are run identifiers, so no format is enforced on
// the column.
export const workflowRun = pgTable(
  "workflow_run",
  {
    id: text("id").primaryKey(),
    // The definition a run belongs to, carried by every run: a folded
    // agent-origin run sets it at launch, and a deployment's anchor run and its
    // child runs carry the deployment's definition. It is the run's anchor to a
    // first-class definition. `cascade` matches `anchorRunId`'s onDelete, so
    // deleting the definition removes its runs.
    definitionId: text("definition_id")
      .notNull()
      .references(() => workflowDefinition.id, { onDelete: "cascade" }),
    // Nullable as of the fold: a folded agent-origin run carries a
    // `definitionId` and no deployment, while a deployment's anchor run sets
    // this to its own id and a child run sets it to its anchor's id. It
    // references the anchor `workflow_run` -- the run whose id equals the
    // deployment id -- so an anchor row is self-referential. The only runtime
    // read of the column (`resolveWorkflowPrincipalNames`) degrades gracefully
    // when it is null.
    anchorRunId: text("anchor_run_id").references(
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
    // A deployment's anchor run is born "deployed": the deploy->first-trigger
    // window is live but not yet running. The first trigger reconciles the
    // anchor and flips this column "deployed" -> "running" (see
    // `anchorWithPrincipal`). A child run has no such window -- it exists only
    // once execution spawns it -- so it is born "running". A run settles into a
    // terminal status when its terminal event lands on the workflow-run ref:
    // the hub's pack-receive path flips this column under a live-status guard so
    // a redelivered terminal event does not re-terminate the run.
    status: text("status", {
      enum: ["deployed", "running", "completed", "failed", "cancelled"],
    })
      .notNull()
      .default("running"),
    // Runtime bindings a folded run carries, mirroring `agent_instance`: a
    // folded run IS the launched instance, so it owns its routing endpoint.
    // `address` is that endpoint -- nullable because an address-less child run
    // routes via its deployment (a deployment's anchor run instead carries a
    // workflow-derived address), but unique among the runs that set it (the
    // partial unique index below). The folded launch writes `address` and
    // `modelPreferences`; `publicKey` is persisted when the sidecar acks the
    // deploy; no code populates `sidecarId`/`kernelId`.
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

// The statuses a run holds while live -- not yet settled into a terminal state.
// A deployment anchor is born "deployed" and flips to "running" on its first
// trigger; a child run is born "running". Every status gate that must treat a
// live run as live tests membership here, so "which statuses count as live" has
// a single definition rather than a `status !== "running"` scattered across the
// workflow-run domain. The `satisfies` bound makes a drift from the column enum
// a compile error.
export const liveWorkflowRunStatuses = [
  "deployed",
  "running",
] as const satisfies readonly (typeof workflowRun.$inferSelect)["status"][];

export function isLiveWorkflowRunStatus(status: string): boolean {
  return liveWorkflowRunStatuses.some((live) => live === status);
}

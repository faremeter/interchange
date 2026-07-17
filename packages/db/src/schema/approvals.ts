import { index, jsonb, pgTable, text, timestamp } from "drizzle-orm/pg-core";

import { tenant } from "./tenants";
import { workflowDeployment } from "./workflow-deployments";

// Every approval originates from a workflow deployment, so the origin is
// modeled as the deployment rather than a launched single agent. A workflow
// deployment has no `agent_instance` or `agent` row, and the principal its run
// executes under is a substrate principal, not a `principal`-table row -- so
// none of those tables can hold a valid referent for an approval's origin.
export const approval = pgTable(
  "approval",
  {
    id: text("id").primaryKey(),
    tenantId: text("tenant_id")
      .notNull()
      .references(() => tenant.id, { onDelete: "cascade" }),
    deploymentId: text("deployment_id")
      .notNull()
      .references(() => workflowDeployment.id, { onDelete: "cascade" }),
    runId: text("run_id").notNull(),
    agentAddress: text("agent_address").notNull(),
    correlationId: text("correlation_id").notNull().unique(),
    // The approver-facing tool snapshot. Nullable because the reactor's
    // suspend-time event does not carry the snapshot: the tool definition and
    // arguments are not reachable at the reactor's suspend point without
    // inference-layer plumbing, so the row is created without them and they are
    // enriched later once that plumbing exists.
    toolDefinition: jsonb("tool_definition"),
    toolArguments: jsonb("tool_arguments"),
    scope: text("scope", { enum: ["once", "always"] }),
    status: text("status", {
      enum: ["pending", "approved", "rejected", "timeout", "expired"],
    })
      .notNull()
      .default("pending"),
    // Nullable so an approval that holds indefinitely can be recorded with no
    // deadline. An agent-step suspend parks on its correlation's signal with no
    // timeout (`parkOnSignal` is called without a `timeout`), so no `timeoutAt`
    // reaches the sidecar frame that co-writes this row -- the deadline lives in
    // the reactor's gate, which the hold-indefinitely suspend path never sets.
    // A null `timeoutAt` is the hold-indefinitely case; a resolver that adds
    // per-workflow expiry populates it when a deadline is configured. Parallels
    // the tool-snapshot columns above, which are likewise deferred.
    timeoutAt: timestamp("timeout_at"),
    resolvedAt: timestamp("resolved_at"),
    createdAt: timestamp("created_at").notNull().defaultNow(),
    updatedAt: timestamp("updated_at").notNull().defaultNow(),
  },
  (t) => [
    index("approval_tenant_status_idx").on(t.tenantId, t.status, t.createdAt),
    index("approval_deployment_idx").on(t.deploymentId),
  ],
);

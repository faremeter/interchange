import { sql } from "drizzle-orm";
import {
  check,
  index,
  integer,
  jsonb,
  pgTable,
  text,
  timestamp,
} from "drizzle-orm/pg-core";

import type { WorkflowProbeResultFrame } from "@intx/types/sidecar";
import type { WorkflowDefinitionSource } from "@intx/types/workflow-sources";

import { asset } from "./assets";
import { sidecar } from "./sidecar";
import { tenant } from "./tenants";

type WorkflowProbeResult = Omit<WorkflowProbeResultFrame, "type" | "requestId">;

export type WorkflowProbeStatus =
  | "pending"
  | "provisioning"
  | "probing"
  | "releasing"
  | "succeeded"
  | "failed";

/**
 * A workflow probe and the temporary provisioned capacity executing it.
 *
 * Deployment intent deliberately does not live here. The source and entry are
 * the probe input; the result is the probe output. The provisioner binding is
 * retained only so uncertain or abandoned capacity can be destroyed safely.
 */
export const workflowProbe = pgTable(
  "workflow_probe",
  {
    id: text("id").primaryKey(),
    tenantId: text("tenant_id")
      .notNull()
      .references(() => tenant.id, { onDelete: "restrict" }),
    definitionAssetId: text("definition_asset_id")
      .notNull()
      .references(() => asset.id, { onDelete: "restrict" }),
    source: jsonb("source").$type<WorkflowDefinitionSource>().notNull(),
    entry: text("entry").notNull(),
    pin: text("pin"),
    status: text("status")
      .$type<WorkflowProbeStatus>()
      .notNull()
      .default("pending"),
    provisionerId: text("provisioner_id").notNull(),
    provisionerApiVersion: integer("provisioner_api_version")
      .$type<1>()
      .notNull(),
    provisionerBindingFingerprint: text(
      "provisioner_binding_fingerprint",
    ).notNull(),
    sidecarId: text("sidecar_id").references(() => sidecar.id, {
      onDelete: "restrict",
    }),
    generation: integer("generation").notNull().default(0),
    externalRef: text("external_ref"),
    result: jsonb("result").$type<WorkflowProbeResult>(),
    failureCode: text("failure_code"),
    failureMessage: text("failure_message"),
    createdAt: timestamp("created_at").notNull().defaultNow(),
    updatedAt: timestamp("updated_at").notNull().defaultNow(),
  },
  (t) => [
    index("workflow_probe_active_idx")
      .on(t.createdAt)
      .where(
        sql`${t.status} in ('pending', 'provisioning', 'probing', 'releasing')`,
      ),
    index("workflow_probe_sidecar_idx").on(t.sidecarId),
    check(
      "workflow_probe_status_check",
      sql`${t.status} in ('pending', 'provisioning', 'probing', 'releasing', 'succeeded', 'failed')`,
    ),
    check(
      "workflow_probe_succeeded_result_check",
      sql`${t.status} <> 'succeeded' or ${t.result} is not null`,
    ),
    check("workflow_probe_generation_check", sql`${t.generation} >= 0`),
  ],
);

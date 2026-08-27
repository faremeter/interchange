import { sql } from "drizzle-orm";
import {
  check,
  index,
  integer,
  pgTable,
  text,
  timestamp,
  uniqueIndex,
} from "drizzle-orm/pg-core";

import type { SidecarAllocationStatus } from "@intx/types";

import { sidecar } from "./sidecar";
import { tenant } from "./tenants";
import { workflowRun } from "./workflow-run";

export const sidecarAllocation = pgTable(
  "sidecar_allocation",
  {
    id: text("id").primaryKey(),
    anchorRunId: text("anchor_run_id")
      .notNull()
      .references(() => workflowRun.id, { onDelete: "cascade" }),
    tenantId: text("tenant_id")
      .notNull()
      .references(() => tenant.id, { onDelete: "restrict" }),
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
    status: text("status")
      .$type<SidecarAllocationStatus>()
      .notNull()
      .default("pending"),
    generation: integer("generation").notNull().default(0),
    ensureAcceptedGeneration: integer("ensure_accepted_generation"),
    externalRef: text("external_ref"),
    nextAttemptAt: timestamp("next_attempt_at"),
    reconciliationLeaseId: text("reconciliation_lease_id"),
    reconciliationLeaseExpiresAt: timestamp("reconciliation_lease_expires_at"),
    ensureAttempts: integer("ensure_attempts").notNull().default(0),
    destroyAttempts: integer("destroy_attempts").notNull().default(0),
    connectDeadline: timestamp("connect_deadline"),
    failureCode: text("failure_code"),
    failureMessage: text("failure_message"),
    createdAt: timestamp("created_at").notNull().defaultNow(),
    updatedAt: timestamp("updated_at").notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex("sidecar_allocation_anchor_run_idx").on(t.anchorRunId),
    uniqueIndex("sidecar_allocation_active_sidecar_idx")
      .on(t.sidecarId)
      .where(
        sql`${t.status} in ('provisioning', 'allocated', 'replacing', 'releasing')`,
      ),
    index("sidecar_allocation_sidecar_idx").on(t.sidecarId),
    index("sidecar_allocation_reconciliation_idx")
      .on(t.nextAttemptAt, t.createdAt)
      .where(
        sql`${t.status} in ('pending', 'provisioning', 'allocated', 'replacing', 'releasing') and ${t.nextAttemptAt} is not null`,
      ),
    check(
      "sidecar_allocation_status_check",
      sql`${t.status} in ('pending', 'provisioning', 'allocated', 'replacing', 'releasing', 'released', 'failed')`,
    ),
    check("sidecar_allocation_generation_check", sql`${t.generation} >= 0`),
    check(
      "sidecar_allocation_accepted_generation_check",
      sql`${t.ensureAcceptedGeneration} is null or ${t.ensureAcceptedGeneration} <= ${t.generation}`,
    ),
  ],
);

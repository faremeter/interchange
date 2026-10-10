import { sql } from "drizzle-orm";
import {
  bigint,
  boolean,
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
      .$type<2>()
      .notNull(),
    provisionerBindingFingerprint: text(
      "provisioner_binding_fingerprint",
    ).notNull(),
    sidecarId: text("sidecar_id").references(() => sidecar.id, {
      onDelete: "restrict",
    }),
    // The digest of the adapter manifest that the Hub sent, or will send, in
    // the ensure for this generation. For a row with a sidecar, it is the
    // manifest that the sidecar booted with. The Hub uses it to place new
    // work only on a sidecar that loaded the manifest the work needs.
    adapterManifestDigest: text("adapter_manifest_digest").notNull(),
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
    initializationLeaseId: text("initialization_lease_id"),
    ensureAttempts: integer("ensure_attempts").notNull().default(0),
    destroyAttempts: integer("destroy_attempts").notNull().default(0),
    deploymentCleanupConfirmed: boolean("deployment_cleanup_confirmed")
      .notNull()
      .default(false),
    providerHoldReleased: boolean("provider_hold_released")
      .notNull()
      .default(false),
    connectDeadline: timestamp("connect_deadline"),
    // How long the sidecar may stay disconnected before the Hub fails the
    // deployment, from the deployment's lifecycle policy.
    maxDisconnectedMs: bigint("max_disconnected_ms", {
      mode: "number",
    }).notNull(),
    // When the deployment's first deploy first failed before its deploy frame
    // was sent; the Hub stops retrying it a while after.
    firstDeployFailedAt: timestamp("first_deploy_failed_at"),
    failureCode: text("failure_code"),
    failureMessage: text("failure_message"),
    createdAt: timestamp("created_at").notNull().defaultNow(),
    updatedAt: timestamp("updated_at").notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex("sidecar_allocation_anchor_run_idx").on(t.anchorRunId),
    index("sidecar_allocation_sidecar_idx").on(t.sidecarId),
    index("sidecar_allocation_reconciliation_idx")
      .on(t.nextAttemptAt, t.createdAt)
      .where(
        sql`${t.status} in ('pending', 'provisioning', 'allocated', 'replacing', 'releasing') and ${t.nextAttemptAt} is not null`,
      ),
    index("sidecar_allocation_dispatchable_anchor_idx")
      .on(t.anchorRunId)
      .where(
        sql`${t.status} in ('pending', 'provisioning', 'allocated', 'replacing')`,
      ),
    check(
      "sidecar_allocation_status_check",
      sql`${t.status} in ('pending', 'provisioning', 'allocated', 'replacing', 'releasing', 'destroy_failed', 'released', 'failed')`,
    ),
    check("sidecar_allocation_generation_check", sql`${t.generation} >= 0`),
    check(
      "sidecar_allocation_max_disconnected_check",
      sql`${t.maxDisconnectedMs} > 0`,
    ),
    check(
      "sidecar_allocation_accepted_generation_check",
      sql`${t.ensureAcceptedGeneration} is null or ${t.ensureAcceptedGeneration} <= ${t.generation}`,
    ),
    check(
      "sidecar_allocation_initialization_check",
      sql`${t.initializationLeaseId} is null or (${t.status} = 'allocated' and ${t.ensureAcceptedGeneration} is not null and ${t.ensureAcceptedGeneration} = ${t.generation})`,
    ),
  ],
);

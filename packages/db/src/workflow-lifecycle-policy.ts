import { and, eq, inArray } from "drizzle-orm";

import {
  WorkflowLifecyclePolicy,
  clampWorkflowLifecyclePolicy,
  resolveWorkflowLifecyclePolicy,
  type ResolvedWorkflowLifecyclePolicy,
} from "@intx/types";

import type { DB, DBExecutor } from "./client";
import {
  parseTenantConfig,
  parseWorkflowDefinitionLifecyclePolicy,
} from "./parse-row";
import { getAncestorChain } from "./tenant-hierarchy";
import { tenant } from "./schema/tenants";
import { workflowDefinition } from "./schema/workflow-definitions";
import { isLiveWorkflowRunStatus, workflowRun } from "./schema/workflow-run";
import { sidecarAllocation } from "./schema/sidecar-allocation";

export type WorkflowRunExecutionTarget = {
  readonly allocationId: string;
  readonly generation: number;
  readonly sidecarId: string;
  readonly tenantId: string;
  readonly anchorRunId: string;
  readonly workflowRunAddress: string;
};

export class WorkflowRunNotExecutableError extends Error {
  readonly reason: "stopping" | "terminal";
  constructor(runId: string, reason: "stopping" | "terminal") {
    super(`Workflow run ${runId} is ${reason} and no longer accepts work`);
    this.name = "WorkflowRunNotExecutableError";
    this.reason = reason;
  }
}

/**
 * A live run is stopping once cancellation is requested or its deadline
 * passes, before its status leaves the live set. Admission uses the original
 * deadline, including before the expiry sweep runs.
 */
export function workflowRunExecutability(
  run: {
    status: string;
    expiresAt: Date | null;
    cancellationRequestedAt: Date | null;
  },
  now = new Date(),
): "executable" | "stopping" | "terminal" {
  if (!isLiveWorkflowRunStatus(run.status)) return "terminal";
  return run.cancellationRequestedAt == null &&
    (run.expiresAt == null || run.expiresAt > now)
    ? "executable"
    : "stopping";
}

export function canExecuteWorkflowRun(
  run: Parameters<typeof workflowRunExecutability>[0],
  now = new Date(),
): boolean {
  return workflowRunExecutability(run, now) === "executable";
}

/**
 * Serialize a synchronous send with cancellation and allocation retirement
 * through the anchor run's row lock. Pack ingestion holds only the allocation
 * row, so a long receive does not delay delivery. This relies on every path
 * that retires the allocation of a run that can still execute also writing the
 * anchor row, as replacement and unrecoverable release do.
 */
export async function withExecutableWorkflowRun(
  db: DB["db"],
  target: WorkflowRunExecutionTarget,
  send: () => boolean,
  signal?: AbortSignal,
): Promise<boolean> {
  signal?.throwIfAborted();
  return db.transaction(async (tx) => {
    const [run] = await tx
      .select({
        status: workflowRun.status,
        expiresAt: workflowRun.expiresAt,
        cancellationRequestedAt: workflowRun.cancellationRequestedAt,
      })
      .from(workflowRun)
      .where(
        and(
          eq(workflowRun.id, target.anchorRunId),
          eq(workflowRun.anchorRunId, target.anchorRunId),
          eq(workflowRun.tenantId, target.tenantId),
          eq(workflowRun.address, target.workflowRunAddress),
        ),
      )
      .for("update");
    signal?.throwIfAborted();
    // Read under the anchor lock, so a retirement that committed while this
    // send waited is visible.
    const [allocation] = await tx
      .select({ id: sidecarAllocation.id })
      .from(sidecarAllocation)
      .where(
        and(
          eq(sidecarAllocation.id, target.allocationId),
          eq(sidecarAllocation.generation, target.generation),
          eq(sidecarAllocation.ensureAcceptedGeneration, target.generation),
          eq(sidecarAllocation.sidecarId, target.sidecarId),
          eq(sidecarAllocation.tenantId, target.tenantId),
          eq(sidecarAllocation.anchorRunId, target.anchorRunId),
          eq(sidecarAllocation.status, "allocated"),
        ),
      );
    signal?.throwIfAborted();
    // Evaluate the clock after the lock, never before a potentially slow read.
    const state =
      run === undefined ? "terminal" : workflowRunExecutability(run);
    // A run that cannot execute is reported as such even once its allocation
    // is retired, so callers drop its work instead of retrying it.
    if (state !== "executable")
      throw new WorkflowRunNotExecutableError(target.anchorRunId, state);
    if (allocation === undefined)
      throw new Error(
        `Allocation ${target.allocationId} is not available for delivery`,
      );
    return send();
  });
}

export async function loadTenantLifecyclePolicies(
  db: DBExecutor,
  tenantId: string,
) {
  const chain = await getAncestorChain(db, tenantId);
  const rows = await db
    .select({ id: tenant.id, config: tenant.config })
    .from(tenant)
    .where(inArray(tenant.id, chain));
  const configs = new Map(rows.map((row) => [row.id, row.config]));
  return chain.reverse().map((id) => {
    const raw = configs.get(id);
    return raw == null ? {} : (parseTenantConfig(id, raw).lifecycle ?? {});
  });
}

/**
 * Validate a policy set below `inheritFrom` against the limits it inherits.
 * Ancestors are capped first, so a value a later ancestor edit made stale
 * cannot reject an otherwise valid policy.
 */
export async function validateLifecyclePolicyEdit(
  db: DBExecutor,
  inheritFrom: string | null,
  policy: WorkflowLifecyclePolicy,
) {
  const inherited =
    inheritFrom === null
      ? []
      : await loadTenantLifecyclePolicies(db, inheritFrom);
  return resolveWorkflowLifecyclePolicy([
    clampWorkflowLifecyclePolicy(inherited),
    policy,
  ]);
}

/**
 * `defaults` fills fields no tenant or installed workflow sets. Unlike a
 * tenant value it is not a ceiling, so either level may set a longer duration.
 */
export async function resolveDeploymentLifecyclePolicy(
  db: DBExecutor,
  tenantId: string,
  definitionId: string,
  defaults: ResolvedWorkflowLifecyclePolicy,
): Promise<ResolvedWorkflowLifecyclePolicy> {
  const policies = await loadTenantLifecyclePolicies(db, tenantId);
  const [definition] = await db
    .select({
      tenantId: workflowDefinition.tenantId,
      policy: workflowDefinition.lifecyclePolicy,
    })
    .from(workflowDefinition)
    .where(eq(workflowDefinition.id, definitionId));
  if (definition === undefined || definition.tenantId !== tenantId) {
    throw new Error("Workflow definition does not belong to deployment tenant");
  }
  policies.push(parseWorkflowDefinitionLifecyclePolicy(definition.policy));
  const policy = clampWorkflowLifecyclePolicy(policies);
  return {
    maxLifetime: policy.maxLifetime ?? defaults.maxLifetime,
    capacityRetention: {
      ...defaults.capacityRetention,
      ...policy.capacityRetention,
    },
  };
}

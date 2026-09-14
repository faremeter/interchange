import { eq, inArray } from "drizzle-orm";

import {
  TenantConfig,
  WorkflowLifecyclePolicy,
  resolveWorkflowLifecyclePolicy,
} from "@intx/types";

import type { DBExecutor } from "./client";
import { getAncestorChain } from "./tenant-hierarchy";
import { tenant } from "./schema/tenants";
import { workflowDefinition } from "./schema/workflow-definitions";

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
    return raw == null ? {} : (TenantConfig.assert(raw).lifecycle ?? {});
  });
}

export async function resolveDeploymentLifecyclePolicy(
  db: DBExecutor,
  tenantId: string,
  definitionId: string,
) {
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
  policies.push(
    definition.policy == null
      ? {}
      : WorkflowLifecyclePolicy.assert(definition.policy),
  );
  return resolveWorkflowLifecyclePolicy(policies);
}

import { eq, inArray } from "drizzle-orm";

import {
  WorkflowLifecyclePolicy,
  clampWorkflowLifecyclePolicy,
  resolveWorkflowLifecyclePolicy,
  type ResolvedWorkflowLifecyclePolicy,
} from "@intx/types";

import type { DBExecutor } from "./client";
import {
  parseTenantConfig,
  parseWorkflowDefinitionLifecyclePolicy,
} from "./parse-row";
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

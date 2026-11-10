import {
  createWorkflowRunLaunchSpecStore,
  resolveTenantSidecarCapabilityPolicies,
  type DB,
} from "@intx/db";
import type {
  SidecarCapabilityPolicy,
  TenantSidecarCapabilityPolicy,
} from "@intx/types";

import type { EffectiveSidecarCapabilityPolicy } from "./capability-policy";

/** What a deployment's provisioner must satisfy: tenant policy plus the workflow's own requirements. */
export function deploymentCapabilityPolicy(
  tenantPolicies: readonly TenantSidecarCapabilityPolicy[],
  projection: { readonly sidecarPlacement?: SidecarCapabilityPolicy },
): EffectiveSidecarCapabilityPolicy {
  return {
    tenantPolicies,
    workflowRules: projection.sidecarPlacement?.capabilities ?? [],
  };
}

/**
 * Resolves that policy for a deployment that already exists: the tenant
 * policy as it is now, with the requirements of the workflow frozen at deploy.
 */
export function createDeploymentCapabilityPolicyResolver(db: DB["db"]) {
  const launchSpecs = createWorkflowRunLaunchSpecStore(db);
  return async (deployment: {
    readonly tenantId: string;
    readonly anchorRunId: string;
  }): Promise<EffectiveSidecarCapabilityPolicy> => {
    const [tenantPolicies, spec] = await Promise.all([
      resolveTenantSidecarCapabilityPolicies(db, deployment.tenantId),
      launchSpecs.get(deployment.anchorRunId),
    ]);
    if (spec === null) {
      throw new Error(
        `Deployment ${deployment.anchorRunId} has no launch specification`,
      );
    }
    return deploymentCapabilityPolicy(
      tenantPolicies,
      spec.frozenApprovalBundle.projection,
    );
  };
}

import { authorizeAction } from "@intx/authz";
import type { ConditionRegistry, GrantStore } from "@intx/types/authz";

export type CanUseExecutionHostArgs = {
  grantStore: GrantStore;
  conditionRegistry: ConditionRegistry;
  tenantId: string;
  placementPrincipalId: string;
  hostId: string;
  ownerPrincipalId: string;
};

export async function canUseExecutionHost({
  grantStore,
  conditionRegistry,
  tenantId,
  placementPrincipalId,
  hostId,
  ownerPrincipalId,
}: CanUseExecutionHostArgs): Promise<boolean> {
  if (ownerPrincipalId === placementPrincipalId) return true;

  const grants = await grantStore.collectGrants(placementPrincipalId, tenantId);
  return (
    await authorizeAction(grants, `host:${hostId}`, "use", {
      registry: conditionRegistry,
      principalId: placementPrincipalId,
      tenantId,
    })
  ).ok;
}

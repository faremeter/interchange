import type { SidecarPlacementRequirement, TenantConfig } from "@intx/types";

const EXCLUSIVE_PLACEMENT: SidecarPlacementRequirement = Object.freeze({
  sharing: "exclusive",
  reuse: "never",
});

export type ResolveEffectiveSidecarPlacementOpts = {
  /** Tenant configs ordered from the workflow tenant through its ancestors. */
  readonly tenantConfigs: readonly TenantConfig[];
};

/**
 * Resolves the placement fixed onto a new workflow run. An exclusive
 * requirement at any tenant ancestor forces exclusive placement; no tenant
 * configuration can weaken it.
 */
export function resolveEffectiveSidecarPlacement({
  tenantConfigs,
}: ResolveEffectiveSidecarPlacementOpts): SidecarPlacementRequirement | null {
  const tenantPlacements = tenantConfigs.flatMap((config) =>
    config.sidecarPlacement?.sharing === "exclusive"
      ? [config.sidecarPlacement]
      : [],
  );
  if (tenantPlacements.length === 0) {
    return null;
  }
  const tenantRequiresFreshCapacity = tenantPlacements.some(
    (placement) => placement.reuse !== "same-deployment",
  );
  return tenantRequiresFreshCapacity
    ? EXCLUSIVE_PLACEMENT
    : { sharing: "exclusive", reuse: "same-deployment" };
}

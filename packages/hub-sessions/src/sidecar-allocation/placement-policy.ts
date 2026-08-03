import type { SidecarPlacementRequirement, TenantConfig } from "@intx/types";

const EXCLUSIVE_PLACEMENT: SidecarPlacementRequirement = Object.freeze({
  sharing: "exclusive",
  reuse: "never",
});

export type ResolveEffectiveSidecarPlacementOpts = {
  readonly workflowPlacement?: SidecarPlacementRequirement;
  /** Tenant configs ordered from the workflow tenant through its ancestors. */
  readonly tenantConfigs: readonly TenantConfig[];
};

/**
 * Resolves the placement fixed onto a new workflow run. An exclusive
 * requirement at either the workflow or any tenant ancestor can only
 * strengthen placement; no tenant configuration can weaken it.
 */
export function resolveEffectiveSidecarPlacement({
  workflowPlacement,
  tenantConfigs,
}: ResolveEffectiveSidecarPlacementOpts): SidecarPlacementRequirement | null {
  const tenantPlacements = tenantConfigs.flatMap((config) =>
    config.sidecarPlacement?.sharing === "exclusive"
      ? [config.sidecarPlacement]
      : [],
  );
  const tenantRequiresFreshCapacity = tenantPlacements.some(
    (placement) => placement.reuse !== "same-deployment",
  );
  if (workflowPlacement?.sharing === "exclusive") {
    return tenantRequiresFreshCapacity
      ? EXCLUSIVE_PLACEMENT
      : {
          sharing: "exclusive",
          reuse: workflowPlacement.reuse ?? "never",
        };
  }
  if (tenantPlacements.length > 0) {
    return tenantRequiresFreshCapacity
      ? EXCLUSIVE_PLACEMENT
      : { sharing: "exclusive", reuse: "same-deployment" };
  }
  return null;
}

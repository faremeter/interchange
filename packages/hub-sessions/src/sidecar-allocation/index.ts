export {
  resolveEffectiveSidecarPlacement,
  type ResolveEffectiveSidecarPlacementOpts,
} from "./placement-policy";
export {
  createSidecarPluginRegistry,
  type CreateSidecarPluginRegistryOpts,
  type SidecarPluginRegistry,
} from "./plugin-registry";
export type {
  DestroySidecarRequest,
  DestroySidecarResult,
  EnsureSidecarRequest,
  EnsureSidecarResult,
  SidecarCredentialIdentity,
  SidecarCredentialResolver,
  SidecarOperationFailure,
  SidecarProvisioner,
} from "./contracts";
export {
  createSidecarAllocationReconciler,
  type SidecarAllocationReconciler,
  type SidecarAllocationReconcilerDeps,
} from "./reconciler";

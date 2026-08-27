export {
  createSidecarPluginRegistry,
  type CreateSidecarPluginRegistryOpts,
  type SidecarPluginRegistry,
  type SidecarProvisionerSelection,
} from "./plugin-registry";
export {
  matchSidecarCapabilityPolicy,
  type EffectiveSidecarCapabilityPolicy,
  type SidecarCapabilityMatch,
  type SidecarCapabilityMismatch,
} from "./capability-policy";
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

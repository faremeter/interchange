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
export {
  DestroySidecarResult,
  EnsureSidecarResult,
  SidecarOperationFailure,
  type DestroySidecarRequest,
  type EnsureSidecarRequest,
  type SidecarCredentialIdentity,
  type SidecarCredentialResolver,
  type SidecarProvisioner,
} from "./contracts";
export {
  createSidecarAllocationReconciler,
  type SidecarAllocationReconciler,
  type SidecarAllocationReconcilerDeps,
} from "./reconciler";

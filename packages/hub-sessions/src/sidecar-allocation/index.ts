export {
  chooseFirstSidecarProvisioner,
  createSidecarPluginRegistry,
  type CreateSidecarPluginRegistryOpts,
  type SidecarPluginRegistry,
  type SidecarProvisionerChooser,
  type SidecarProvisionerSelectionContext,
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
  type ClaimExistingSidecarOpts,
  type DestroySidecarRequest,
  type ExistingHostCandidate,
  type ExistingHostChooser,
  type ExistingSidecarCapacity,
  type EnsureSidecarRequest,
  type SidecarCredentialIdentity,
  type SidecarCredentialResolver,
  type SidecarProvisioner,
  type SidecarProvisionerContext,
} from "./contracts";
export {
  createSidecarAllocationReconciler,
  type SidecarAllocationReconciler,
  type SidecarAllocationReconcilerDeps,
} from "./reconciler";
export {
  createExistingSidecarCapacity,
  type CreateExistingSidecarCapacityOpts,
  type ExistingSidecarHostAccessRequest,
} from "./existing-sidecar-capacity";

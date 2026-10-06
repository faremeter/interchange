// @intx/workflow-host/child -- the sidecar child factory.
//
// Builds the workflow-process substrate the sidecar's workflow child runs.
// The process that boots the child supplies tool materialization and the
// child-grant cap. The package barrel does not load this entry.

export {
  SIDECAR_SUBSTRATE_CONFIG_KEYS,
  createSidecarSubstrateFactory,
} from "./substrate-factory";
export type {
  StepToolCacheConfig,
  StepToolMaterialization,
} from "./step-tools";

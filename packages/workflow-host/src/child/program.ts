// @intx/workflow-host/child -- the child process entry and substrate factory.
//
// Boots the workflow child from its process environment and builds its substrate.
// The host supplies tool materialization and the child-grant cap.
// The package barrel does not load this entry.

export {
  SIDECAR_SUBSTRATE_CONFIG_KEYS,
  createSidecarSubstrateFactory,
} from "./substrate-factory";
export type {
  StepToolCacheConfig,
  StepToolMaterialization,
} from "./step-tools";

export { runWorkflowChildFromProcessEnv } from "./from-process-env";

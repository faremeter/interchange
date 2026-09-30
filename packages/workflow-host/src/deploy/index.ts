// @intx/workflow-host/deploy -- the sidecar deploy program.
//
// Stages a workflow deployment onto a supervised workflow-process child.
// The process that boots this program supplies closure apply, asset
// delivery, registry and platform resolution, repo-id derivation, and the
// child spawner and binary. The workflow child does not load this entry.
export {
  createSidecarDeployRouter,
  createSidecarWorkflowSupervisor,
  resolveDeploymentAssetMounts,
  type SidecarDeployRouter,
} from "./workflow-host-wiring";
export {
  createDeploymentAddressRegistry,
  createMultistepCredentialsRouter,
  createMultistepDrainRouter,
  createMultistepGrantsRouter,
  createMultistepMailRouter,
  createMultistepSignalRouter,
  createMultistepSourcesRouter,
  createWorkflowRunPackClient,
  createWorkflowRunPackPushingRepoStore,
} from "./workflow-run-pack-client";
export { createWorkflowRunPackRestorer } from "./workflow-run-pack-restore";
export {
  removeFileAtomicDurable,
  writeFileAtomicDurable,
} from "./atomic-write";
export { WORKFLOW_RUN_RECORD_FILENAME } from "./workflow-run-record";

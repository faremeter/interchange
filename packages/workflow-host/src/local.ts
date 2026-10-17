// @intx/workflow-host/local -- run a definition outside a sidecar with the
// real step invoker.
//
// `runLocal` in `@intx/workflow` drives a definition in process; its agent
// steps run through the `invokeStep` a caller supplies. This entry exports the
// step invoker the deployed host uses, the builders that give a step its
// tools and credentials, and the authorize closure and grant evaluator the
// deployed host authorizes with, plus a snapshot builder that derives a
// deployed run's grants from the definition. A local run then executes steps
// with the deployed step invoker and authorizes them with the same grants and
// evaluator as a deployed source-ref run; `createLocalCredentialsSnapshot`
// names where a local run differs.
//
// Nothing this entry imports is a Node or Bun module, so it bundles for a
// browser. Some code it reaches uses the `Buffer` global at run time (MIME
// header encoding in `@intx/mime`); a browser caller installs one, as
// `@intx/storage-isogit/browser` does. The package root also exports the
// supervisor, the run child and the substrate adapters, which assume a Node
// filesystem and process.
export {
  createWorkflowStepInvoker,
  type StepEnvBase,
  type WorkflowStepInvokerOpts,
} from "./adapters/step-invoker";
export {
  attachStepCredentialWiring,
  attachStepTools,
  createToolBearingAgentFactory,
  deriveToolMarkFloorGrants,
  type StepToolMaterialization,
} from "./child/step-tools";
export {
  createCredentialsBackedAuthorize,
  createGrantEvaluator,
  type CredentialsSnapshotRef,
  type GrantEvaluator,
} from "./child/credentials-backed-authorize";
export {
  createLocalCredentialsSnapshot,
  snapshotStepGrants,
  type LocalCredentialsSnapshotOpts,
} from "./local-credentials";
export type {
  CredentialsSnapshot,
  CredentialsSnapshotStep,
} from "./supervisor/credentials";

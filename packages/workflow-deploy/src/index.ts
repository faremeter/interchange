// @intx/workflow-deploy -- deploy-time validation and orchestration of
// workflows.
//
// Surfaces today:
//   - capability walk: structural lift of `getRequiredEnvKeys` that
//     emits the grant-shape declarations the operator-approval gate
//     consumes.
//   - approval gate: consumes the walk's output plus an operator-
//     supplied `ApprovalSet` and yields a per-step pending delta.
//   - deploy derivation + source pinning: pure address derivation
//     (`deriveRunAddress`, `deriveStepAddress`, `resolveStepAddress`, ...)
//     and per-step inference-source resolution against the operator-
//     approved grant set (`pickStepInferenceSource`,
//     `buildInertProjectionStepSources`).

export {
  walkCapabilities,
  type CapabilityWalkResult,
  type GrantDeclarations,
  type PluginToolDefinitions,
} from "./capability-walk";
export {
  createApprovalSetGate,
  createApprovalSourceGate,
  type ApprovalDecision,
  type ApprovalSet,
  type ApprovalSource,
  type CapabilityApprovalGate,
} from "./capability-approval";
export { extractFoldedBody, type FoldedBody } from "./fold-synthesis";
export {
  enumerateInertOnTriggerBodies,
  type EnumeratedInertOnTriggerBody,
  type InertBodyStepPreference,
} from "./inert-ontrigger-bodies";
export {
  isSourceApproved,
  pickStepInferenceSource,
  buildInertProjectionStepSources,
  buildSingleStepAgentDefinition,
  deriveRunAddress,
  deriveRunAgentId,
  deriveStepAddress,
  resolveStepAddress,
  deriveStepAgentId,
  deriveWorkflowRunRepoId,
  WorkflowDefinitionInvalidError,
  type DeployContent,
} from "./orchestrator";

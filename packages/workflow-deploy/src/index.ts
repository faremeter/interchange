// @intx/workflow-deploy -- deploy-time validation and orchestration of
// workflows: the capability walk and its grant shapes, the operator-approval
// gate, the child-grant cap, pure deploy address derivation, and per-step
// inference-source pinning against the operator-approved grant set.

export {
  walkCapabilities,
  type CapabilityWalkResult,
  type GrantDeclarations,
  type PluginToolDefinitions,
} from "./capability-walk";
export {
  collectDeclaredCredentialConsumers,
  collectDeclaredResources,
  filterGrantsToDeclaredResources,
} from "./child-grant-filter";
export {
  approvalItemsFromSet,
  approvalSetFromItems,
  createApprovalSet,
  createApprovalSetGate,
  createApprovalSourceGate,
  isApprovedGrantRequirement,
  type ApprovalDecision,
  type ApprovalSet,
  type ApprovalSource,
  type CapabilityApprovalGate,
} from "./capability-approval";
export { extractFoldedBody, type FoldedBody } from "./fold-synthesis";
export {
  enumerateInertBodies,
  inertFlatNamespaceStepIds,
  inertLoopBody,
  inertNestedBodies,
  type EnumeratedInertBody,
  type InertBodyStepPreference,
} from "./inert-ontrigger-bodies";
export {
  pickStepInferenceSource,
  pinInertStepSources,
  collectAgentBearingStepIds,
  buildInertProjectionStepSources,
  buildInertBodyStepSources,
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

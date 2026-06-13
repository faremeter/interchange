// @intx/workflow-deploy -- deploy-time validation and orchestration of
// workflows.
//
// Surfaces today:
//   - capability walk: structural lift of `getRequiredEnvKeys` that
//     emits the grant-shape declarations the operator-approval gate
//     consumes.
//   - approval gate: consumes the walk's output plus an operator-
//     supplied `ApprovalSet` and yields a per-step pending delta.
//   - orchestrator: validates the workflow, runs the walk + approval
//     gate, writes the workflow repo, and branches on the trivial-vs-
//     multi-step dichotomy for per-agent launches.

export {
  walkCapabilities,
  type CapabilityWalkResult,
  type GrantDeclarations,
} from "./capability-walk";
export {
  createApprovalSetGate,
  createApprovalSourceGate,
  type ApprovalDecision,
  type ApprovalSet,
  type ApprovalSource,
  type CapabilityApprovalGate,
} from "./capability-approval";
export {
  createWorkflowDeployOrchestrator,
  deriveDeploymentAddress,
  deriveDeploymentAgentId,
  deriveStepAddress,
  deriveStepAgentId,
  deriveStepInstanceId,
  wrapHarnessAsTrivialAgent,
  CapabilityApprovalDeniedError,
  MultiStepDeployHandoffMissingError,
  MultiStepDeploymentArgsMissingError,
  WorkflowDefinitionInvalidError,
  type DeployContent,
  type DeployWorkflowArgs,
  type DeployWorkflowResult,
  type LaunchSessionFn,
  type MultiStepDeployResult,
  type SendMultiStepDeployFn,
  type WorkflowDeployOrchestrator,
  type WorkflowDeployOrchestratorDeps,
  type WorkflowRepoWriter,
} from "./orchestrator";

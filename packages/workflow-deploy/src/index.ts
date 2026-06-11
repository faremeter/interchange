// @intx/workflow-deploy -- deploy-time validation of workflows.
//
// Two surfaces today:
//   - capability walk: structural lift of `getRequiredEnvKeys` that
//     emits the grant-shape declarations the operator-approval gate
//     consumes.
//   - approval gate: skeleton for the orchestrator to call into; the
//     concrete implementation lands when the orchestrator is wired.

export {
  walkCapabilities,
  type CapabilityWalkResult,
  type GrantDeclarations,
} from "./capability-walk";
export {
  createNotYetImplementedApprovalGate,
  type ApprovalDecision,
  type ApprovalSource,
  type CapabilityApprovalGate,
} from "./capability-approval";

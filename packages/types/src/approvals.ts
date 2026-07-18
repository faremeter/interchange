import { type } from "arktype";

export const ApprovalResponse = type({
  id: "string",
  tenantId: "string",
  deploymentId: type("string").describe(
    "The workflow deployment the approval originates from. Every approval is raised during a workflow run; there is no launched single agent or agent-definition row behind it.",
  ),
  runId: "string",
  agentAddress: "string",
  correlationId: type("string").describe(
    "Ties the approval to the suspension it resolves. The parked run awaits the control signal keyed by this id.",
  ),
  toolDefinition: type("Record<string, unknown>").describe(
    "The approver-facing tool snapshot (name, description, input schema) captured at suspend time.",
  ),
  toolArguments: "Record<string, unknown>",
  scope: "'once' | 'always' | null",
  status: "'pending' | 'approved' | 'rejected' | 'timeout' | 'expired'",
  timeoutAt: type("string | null").describe(
    "Deadline after which the approval expires. Null records a hold-indefinitely approval with no deadline.",
  ),
  resolvedAt: "string | null",
  createdAt: "string",
  updatedAt: "string",
});

export const ApproveAction = type({
  scope: "'once' | 'always'",
});

export const RejectAction = type({
  "message?": "string",
});

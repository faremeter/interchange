import { type } from "arktype";

export const ApprovalResponse = type({
  id: "string",
  tenantId: "string",
  principalId: "string",
  agentId: "string",
  sessionId: type("string").describe(
    "Internal FK to the session channel. The approval was created during an instance's execution; the instance ID can be resolved via the session relationship.",
  ),
  resource: "string",
  action: "string",
  "context?": "Record<string, unknown> | null",
  status: "'pending' | 'approved' | 'rejected'",
  createdAt: "string",
  "resolvedAt?": "string | null",
});

export const ApproveAction = type({
  scope: "'once' | 'always'",
});

export const RejectAction = type({
  "message?": "string",
});

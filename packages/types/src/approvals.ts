import { type } from "arktype";

export const ApprovalResponse = type({
  id: "string",
  tenantId: "string",
  principalId: "string",
  agentId: "string",
  sessionId: "string",
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

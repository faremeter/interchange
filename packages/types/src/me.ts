import { type } from "arktype";

export const UserProfile = type({
  id: "string",
  name: "string",
  email: "string",
  emailVerified: "boolean",
  "image?": "string | null",
  createdAt: "string",
  updatedAt: "string",
});

export const PrincipalSummary = type({
  principalId: "string",
  tenantId: "string",
  tenantName: "string",
  tenantSlug: "string",
  kind: "'user' | 'agent'",
  status: "'active' | 'suspended' | 'invited' | 'deactivated'",
  roles: type({
    id: "string",
    name: "string",
  }).array(),
});

export const WorkflowRunSummary = type({
  id: "string",
  tenantId: "string",
  tenantName: "string",
  definitionId: "string",
  definitionName: "string",
  address: "string",
  status: "'deployed' | 'running' | 'updating' | 'error' | 'stopped'",
  createdAt: "string",
});

export const SessionSummary = type({
  id: "string",
  tenantId: "string",
  tenantName: "string",
  definitionId: "string",
  definitionName: "string",
  status: "'idle' | 'ending' | 'ended'",
  createdAt: "string",
  "lastActivityAt?": "string | null",
});

export const ApprovalSummary = type({
  id: "string",
  tenantId: "string",
  tenantName: "string",
  definitionId: "string",
  definitionName: "string",
  sessionId: type("string").describe(
    "Internal FK to the session channel. The run ID can be resolved via the session relationship.",
  ),
  resource: "string",
  action: "string",
  createdAt: "string",
});

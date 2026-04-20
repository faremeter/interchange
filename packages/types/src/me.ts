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

export const AgentSummary = type({
  id: "string",
  tenantId: "string",
  tenantName: "string",
  name: "string",
  "description?": "string | null",
  status: "'deployed' | 'stopped' | 'updating' | 'error'",
});

export const SessionSummary = type({
  id: "string",
  tenantId: "string",
  tenantName: "string",
  agentId: "string",
  agentName: "string",
  status: "'idle' | 'ending' | 'ended'",
  createdAt: "string",
  "lastActivityAt?": "string | null",
});

export const ApprovalSummary = type({
  id: "string",
  tenantId: "string",
  tenantName: "string",
  agentId: "string",
  agentName: "string",
  sessionId: "string",
  resource: "string",
  action: "string",
  createdAt: "string",
});

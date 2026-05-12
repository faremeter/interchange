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

export const InstanceSummary = type({
  id: "string",
  tenantId: "string",
  tenantName: "string",
  agentId: "string",
  agentName: "string",
  address: "string",
  status: "'deployed' | 'running' | 'updating' | 'error' | 'stopped'",
  createdAt: "string",
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
  sessionId: type("string").describe(
    "Internal FK to the session channel. The instance ID can be resolved via the session relationship.",
  ),
  resource: "string",
  action: "string",
  createdAt: "string",
});

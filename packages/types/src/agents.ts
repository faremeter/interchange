import { type } from "arktype";
import { grantEffects } from "./grants";

export const delegationSources = ["tenant", "creator", "invoker"] as const;
export type DelegationSource = (typeof delegationSources)[number];

// Preserve the old name for downstream consumers during migration
export const credentialRequirementSources = delegationSources;
export type CredentialRequirementSource = DelegationSource;

export const agentDefinitionStatuses = ["deployed", "stopped"] as const;
export type AgentDefinitionStatus = (typeof agentDefinitionStatuses)[number];

export const agentInstanceStatuses = [
  "deployed",
  "running",
  "updating",
  "error",
  "stopped",
] as const;
export type AgentInstanceStatus = (typeof agentInstanceStatuses)[number];

const DelegationSourceType = type.enumerated(...delegationSources);
const AgentDefinitionStatusType = type.enumerated(...agentDefinitionStatuses);
const AgentInstanceStatusType = type.enumerated(...agentInstanceStatuses);
const Effect = type.enumerated(...grantEffects);

export const CredentialRequirement = type({
  providerName: "string",
  "scopes?": "string[]",
  source: DelegationSourceType,
  "name?": "string",
});

export const GrantRequirement = type({
  resource: "string",
  action: "string",
  "effect?": Effect,
  source: DelegationSourceType,
  "conditions?": "Record<string, unknown> | null",
});

export const CreateAgent = type({
  name: "string",
  "description?": "string",
  "systemPrompt?": "string",
  "skills?": "Record<string, unknown>",
  "contextConfig?": "Record<string, unknown>",
  "initialState?": "Record<string, unknown>",
  "modelConfig?": "Record<string, unknown>",
  "capabilities?": "Record<string, unknown>",
  "credentialRequirements?": CredentialRequirement.array(),
  "grantRequirements?": GrantRequirement.array(),
});

export const UpdateAgent = type({
  "name?": "string",
  "description?": "string",
  "systemPrompt?": "string",
  "skills?": "Record<string, unknown>",
  "contextConfig?": "Record<string, unknown>",
  "initialState?": "Record<string, unknown>",
  "modelConfig?": "Record<string, unknown>",
  "capabilities?": "Record<string, unknown>",
  "credentialRequirements?": CredentialRequirement.array(),
  "grantRequirements?": GrantRequirement.array(),
});

export const AgentResponse = type({
  id: "string",
  tenantId: "string",
  // TODO: remove null once all definitions have been backfilled
  "creatorPrincipalId?": "string | null",
  name: "string",
  "description?": "string | null",
  "systemPrompt?": "string | null",
  "skills?": "Record<string, unknown>",
  "contextConfig?": "Record<string, unknown>",
  "initialState?": "Record<string, unknown>",
  "modelConfig?": "Record<string, unknown>",
  currentVersion: "string",
  status: AgentDefinitionStatusType,
  "capabilities?": "Record<string, unknown>",
  "credentialRequirements?": CredentialRequirement.array(),
  "grantRequirements?": GrantRequirement.array(),
  createdAt: "string",
  updatedAt: "string",
});

export const CreateAgentInstance = type({
  agentId: "string",
  "invokerGrants?": type({
    resource: "string",
    action: "string",
    "effect?": Effect,
    "conditions?": "Record<string, unknown> | null",
  }).array(),
});

export const AgentInstanceResponse = type({
  id: "string",
  agentId: "string",
  agentName: "string",
  tenantId: "string",
  address: "string",
  status: AgentInstanceStatusType,
  "publicKey?": "string | null",
  "kernelId?": "string | null",
  "sidecarId?": "string | null",
  createdAt: "string",
  updatedAt: "string",
  "endedAt?": "string | null",
});

export const AgentVersion = type({
  version: "string",
  status: "'active' | 'inactive' | 'failed'",
  createdAt: "string",
});

export const AgentHealth = type({
  liveness: "'ok' | 'unhealthy'",
  readiness: "'ok' | 'not_ready' | 'unhealthy'",
  "lastCheckedAt?": "string | null",
});

export const RollbackRequest = type({
  version: "string",
});

export const Offering = type({
  id: "string",
  agentId: "string",
  name: "string",
  "description?": "string | null",
  "pricing?": {
    "base?": {
      amount: "string",
      currency: "string",
    },
    "methods?": "string[]",
    "negotiable?": "boolean",
    "bounds?": {
      "min?": "string",
      "max?": "string",
    },
  },
});

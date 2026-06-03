import { type } from "arktype";
import { grantEffects } from "./grants";

export const credentialRequirementSources = [
  "tenant",
  "creator",
  "invoker",
] as const;
export type CredentialRequirementSource =
  (typeof credentialRequirementSources)[number];

export const grantRequirementSources = ["creator", "invoker"] as const;
export type GrantRequirementSource = (typeof grantRequirementSources)[number];

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

const CredentialSourceType = type.enumerated(...credentialRequirementSources);
const GrantSourceType = type.enumerated(...grantRequirementSources);
const AgentDefinitionStatusType = type.enumerated(...agentDefinitionStatuses);
const AgentInstanceStatusType = type.enumerated(...agentInstanceStatuses);
const Effect = type.enumerated(...grantEffects);

export const CredentialRequirement = type({
  providerName: "string",
  "scopes?": "string[]",
  source: CredentialSourceType,
  "name?": "string",
});

export const GrantRequirement = type({
  resource: "string",
  action: "string",
  "effect?": Effect,
  source: GrantSourceType,
  "conditions?": "Record<string, unknown> | null",
});

export const CreateAgent = type({
  name: "string",
  "description?": "string",
  "systemPrompt?": "string",
  "contextConfig?": "Record<string, unknown>",
  "initialState?": "Record<string, unknown>",
  "modelConfig?": "Record<string, unknown>",
  "capabilities?": "Record<string, unknown>",
  "credentialRequirements?": CredentialRequirement.array(),
  "grantRequirements?": GrantRequirement.array().describe(
    "A grant requirements manifest, not live grants. Each entry declares a resource, action, and source (creator or invoker). The control plane resolves these requirements at each agent launch against the current authority of the creator and invoker.",
  ),
  "roleIds?": "string[]",
});

export const UpdateAgent = type({
  "name?": "string",
  "description?": "string",
  "systemPrompt?": "string",
  "contextConfig?": "Record<string, unknown>",
  "initialState?": "Record<string, unknown>",
  "modelConfig?": "Record<string, unknown>",
  "capabilities?": "Record<string, unknown>",
  "credentialRequirements?": CredentialRequirement.array(),
  "grantRequirements?": GrantRequirement.array(),
  "roleIds?": "string[]",
});

export const AgentResponse = type({
  id: "string",
  tenantId: "string",
  creatorPrincipalId: type("string").describe(
    "Identifies the definition author's principal (definitions have no principalId of their own). Used for resolving creator-sourced grant and credential requirements.",
  ),
  name: "string",
  "description?": "string | null",
  "systemPrompt?": "string | null",
  "contextConfig?": "Record<string, unknown>",
  "initialState?": "Record<string, unknown>",
  "modelConfig?": "Record<string, unknown>",
  currentVersion: "string",
  status: AgentDefinitionStatusType,
  "capabilities?": "Record<string, unknown>",
  "credentialRequirements?": CredentialRequirement.array(),
  "grantRequirements?": GrantRequirement.array(),
  "roles?": type({ id: "string", name: "string" }).array(),
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
  })
    .array()
    .describe(
      "Capabilities the invoker is willing to delegate to the agent, resolved against the invoker's own authority at launch. These are materialized as grants on the agent principal in addition to any grants from the definition's own requirements.",
    ),
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

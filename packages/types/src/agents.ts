import { type } from "arktype";
import { grantEffects } from "./grants";
import { ToolPackagePin, ToolPackagePinArray } from "./tool-packages";

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
  source: CredentialSourceType.describe(
    "Whose credential satisfies this requirement at launch: `tenant` (a credential owned by the tenant), `creator` (the definition author's), or `invoker` (whoever launched the agent).",
  ),
  "name?": "string",
});

export const GrantRequirement = type({
  resource: "string",
  action: "string",
  "effect?": Effect.describe(
    "Effect to assign the materialized grant: `allow`, `deny`, or `ask`. Defaults to `allow` when omitted.",
  ),
  source: GrantSourceType.describe(
    "Whose authority the grant is resolved against at launch: `creator` (the definition author) or `invoker` (whoever launched the agent). The requirement is only satisfied if that party actually holds the requested capability.",
  ),
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
  "toolPackages?": ToolPackagePinArray.describe(
    "Tool packages pinned by this agent definition. Each entry must use a valid npm package name (lowercase, optionally `@scope/`-prefixed) and a parseable semver range; the array must contain no duplicate names. The hub resolves the full dependency closure at deploy-assembly time and ships the manifest to the sidecar; the sidecar materializes each pinned package and registers its tools with the harness.",
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
  "toolPackages?": ToolPackagePinArray,
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
  status: AgentDefinitionStatusType.describe(
    "Lifecycle state of the agent definition: `deployed` (a launchable version is active) or `stopped` (deactivated, no new instances launch).",
  ),
  "capabilities?": "Record<string, unknown>",
  "credentialRequirements?": CredentialRequirement.array(),
  "grantRequirements?": GrantRequirement.array(),
  toolPackages: ToolPackagePin.array().describe(
    "Tool packages this definition pins. Always present; an empty array means the definition pins no packages (the agent runs with whatever non-tool-package factories the sidecar harness ships).",
  ),
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
  status: AgentInstanceStatusType.describe(
    "Lifecycle state of this running instance: `deployed` (provisioned on a sidecar, not yet started), `running` (started and serving), `updating` (rolling to a new definition version), `error` (launch or runtime failure), or `stopped` (undeployed).",
  ),
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

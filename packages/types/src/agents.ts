import { type } from "arktype";
import { ModelRequirements } from "./catalog";
import { CredentialRequirement } from "./credentials";
import { GrantRequirement } from "./grants";
import { ToolPackagePin, ToolPackagePinArray } from "./tool-packages";

const modelRequirementsDescription =
  "Model needs declared by canonical name, with optional per-model capability filters and provider preferences. Resolved against the tenant catalog at launch to build the ordered inference sources; it does not introduce providers the tenant catalog lacks.";

export const agentDefinitionStatuses = ["deployed", "stopped"] as const;
export type AgentDefinitionStatus = (typeof agentDefinitionStatuses)[number];

const AgentDefinitionStatusType = type.enumerated(...agentDefinitionStatuses);

export const CreateAgent = type({
  name: "string",
  "description?": "string",
  "systemPrompt?": "string",
  "contextConfig?": "Record<string, unknown>",
  "initialState?": "Record<string, unknown>",
  "modelConfig?": "Record<string, unknown>",
  "capabilities?": "Record<string, unknown>",
  "credentialRequirements?": CredentialRequirement.array(),
  "modelRequirements?": ModelRequirements.describe(
    modelRequirementsDescription,
  ),
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
  "modelRequirements?": ModelRequirements.describe(
    modelRequirementsDescription,
  ),
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
  "modelRequirements?": ModelRequirements.describe(
    modelRequirementsDescription,
  ),
  "grantRequirements?": GrantRequirement.array(),
  toolPackages: ToolPackagePin.array().describe(
    "Tool packages this definition pins. Always present; an empty array means the definition pins no packages (the agent runs with whatever non-tool-package factories the sidecar harness ships).",
  ),
  "roles?": type({ id: "string", name: "string" }).array(),
  createdAt: "string",
  updatedAt: "string",
});

export const AgentVersion = type({
  version: "string",
  status: "'active' | 'inactive' | 'failed'",
  createdAt: "string",
});

export const RollbackRequest = type({
  version: "string",
});

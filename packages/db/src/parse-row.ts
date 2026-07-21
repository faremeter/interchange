import { type } from "arktype";

import {
  Capability,
  CredentialRequirement,
  GrantRequirement,
  grantEffects,
  grantOrigins,
  ModelProviderPlugin,
  ModelRequirements,
  signalKinds,
} from "@intx/types";
import { RepoAction } from "@intx/types/sidecar";
import { ToolPackagePinArray } from "@intx/types/tool-packages";

import type {
  agent,
  agentVersion,
  approval,
  credential,
  gitToken,
  grant,
  modelOffering,
  modelProvider,
  oauthClient,
  offering,
  provider,
  signalCorrelation,
  tenant,
  transaction,
  turnPart,
  wallet,
} from "./schema";

const JSONObject = type("Record<string, unknown>");

const GrantEffectValidator = type.enumerated(...grantEffects);
const GrantOriginValidator = type.enumerated(...grantOrigins);

const approvalScopes = ["once", "always"] as const;
const ApprovalScopeValidator = type.enumerated(...approvalScopes);

const approvalStatuses = [
  "pending",
  "approved",
  "rejected",
  "timeout",
  "expired",
] as const;
const ApprovalStatusValidator = type.enumerated(...approvalStatuses);

const SignalKindValidator = type.enumerated(...signalKinds);

const agentVersionStatuses = ["active", "inactive", "failed"] as const;
const AgentVersionStatusValidator = type.enumerated(...agentVersionStatuses);

const credentialTypes = [
  "api_key",
  "oauth_token",
  "certificate",
  "other",
] as const;
const CredentialTypeValidator = type.enumerated(...credentialTypes);

const credentialStatuses = ["active", "expired", "revoked", "error"] as const;
const CredentialStatusValidator = type.enumerated(...credentialStatuses);

const walletBackendTypes = ["crypto", "fiat", "credits"] as const;
const WalletBackendTypeValidator = type.enumerated(...walletBackendTypes);

const transactionDirections = ["inbound", "outbound"] as const;
const TransactionDirectionValidator = type.enumerated(...transactionDirections);

const transactionStatuses = ["pending", "completed", "failed"] as const;
const TransactionStatusValidator = type.enumerated(...transactionStatuses);

const gitTokenKinds = ["pat", "svc"] as const;
export const GitTokenKindValidator = type.enumerated(...gitTokenKinds);

const turnPartTypes = [
  "text",
  "reasoning",
  "tool",
  "file",
  "error",
  "refusal",
  "step-start",
  "step-finish",
  "snapshot",
  "patch",
] as const;
const TurnPartTypeValidator = type.enumerated(...turnPartTypes);

export function parseAgentRow(row: typeof agent.$inferSelect) {
  return {
    ...row,
    contextConfig:
      row.contextConfig !== null ? JSONObject.assert(row.contextConfig) : null,
    initialState:
      row.initialState !== null ? JSONObject.assert(row.initialState) : null,
    modelConfig:
      row.modelConfig !== null ? JSONObject.assert(row.modelConfig) : null,
    capabilities:
      row.capabilities !== null ? JSONObject.assert(row.capabilities) : null,
    credentialRequirements:
      row.credentialRequirements !== null
        ? CredentialRequirement.array().assert(row.credentialRequirements)
        : null,
    modelRequirements:
      row.modelRequirements !== null
        ? ModelRequirements.assert(row.modelRequirements)
        : null,
    grantRequirements:
      row.grantRequirements !== null
        ? GrantRequirement.array().assert(row.grantRequirements)
        : null,
    toolPackages: ToolPackagePinArray.assert(row.toolPackages),
  };
}

export function parseAgentVersionRow(row: typeof agentVersion.$inferSelect) {
  return {
    ...row,
    status: AgentVersionStatusValidator.assert(row.status),
  };
}

export function parseGrantRow(row: typeof grant.$inferSelect) {
  return {
    ...row,
    effect: GrantEffectValidator.assert(row.effect),
    origin: GrantOriginValidator.assert(row.origin),
    conditions:
      row.conditions !== null ? JSONObject.assert(row.conditions) : null,
  };
}

export function parseApprovalRow(row: typeof approval.$inferSelect) {
  return {
    ...row,
    scope: row.scope !== null ? ApprovalScopeValidator.assert(row.scope) : null,
    status: ApprovalStatusValidator.assert(row.status),
    toolDefinition: JSONObject.assert(row.toolDefinition),
    toolArguments: JSONObject.assert(row.toolArguments),
  };
}

export function parseSignalCorrelationRow(
  row: typeof signalCorrelation.$inferSelect,
) {
  return {
    ...row,
    kind: SignalKindValidator.assert(row.kind),
  };
}

export function parseOfferingRow(row: typeof offering.$inferSelect) {
  return {
    ...row,
    pricing: row.pricing !== null ? JSONObject.assert(row.pricing) : null,
    schema: row.schema !== null ? JSONObject.assert(row.schema) : null,
  };
}

export function parseCredentialRow(row: typeof credential.$inferSelect) {
  return {
    ...row,
    type: CredentialTypeValidator.assert(row.type),
    status: CredentialStatusValidator.assert(row.status),
    metadata: row.metadata !== null ? JSONObject.assert(row.metadata) : null,
  };
}

export function parseProviderRow(row: typeof provider.$inferSelect) {
  return {
    ...row,
    metadata: row.metadata !== null ? JSONObject.assert(row.metadata) : null,
  };
}

export function parseModelProviderRow(row: typeof modelProvider.$inferSelect) {
  return {
    ...row,
    plugin: ModelProviderPlugin.assert(row.plugin),
  };
}

export function parseModelOfferingRow(row: typeof modelOffering.$inferSelect) {
  return {
    ...row,
    capabilities: Capability.array().assert(row.capabilities),
    quirks: row.quirks !== null ? JSONObject.assert(row.quirks) : null,
  };
}

export function parseTenantRow(row: typeof tenant.$inferSelect) {
  return {
    ...row,
    config: row.config !== null ? JSONObject.assert(row.config) : null,
  };
}

export function parseWalletRow(row: typeof wallet.$inferSelect) {
  return {
    ...row,
    backendType: WalletBackendTypeValidator.assert(row.backendType),
    config: row.config !== null ? JSONObject.assert(row.config) : null,
  };
}

export function parseTransactionRow(row: typeof transaction.$inferSelect) {
  return {
    ...row,
    direction: TransactionDirectionValidator.assert(row.direction),
    status: TransactionStatusValidator.assert(row.status),
  };
}

export function parseOAuthClientRow(row: typeof oauthClient.$inferSelect) {
  return {
    ...row,
    metadata: row.metadata !== null ? JSONObject.assert(row.metadata) : null,
  };
}

export function parseGitTokenRow(row: typeof gitToken.$inferSelect) {
  return {
    ...row,
    kind: GitTokenKindValidator.assert(row.kind),
    actions: RepoAction.array().assert(row.actions),
  };
}

export function parseTurnPartType(
  partType: string,
): (typeof turnPart.$inferInsert)["type"] {
  return TurnPartTypeValidator.assert(partType);
}

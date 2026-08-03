import { type } from "arktype";

import {
  Capability,
  CredentialBinding,
  GrantRequirement,
  grantEffects,
  grantOrigins,
  InvokerModelPreferences,
  ModelProviderPlugin,
  ModelRequirements,
  principalKinds,
  principalStatuses,
  signalKinds,
  TenantConfig,
  workflowDefinitionStatuses,
  workflowDefinitionVersionStatuses,
} from "@intx/types";
import { WireGrantRule } from "@intx/types/grant-wire";
import { RepoAction } from "@intx/types/sidecar";
import { ToolPackagePinArray } from "@intx/types/tool-packages";

import type {
  approval,
  credential,
  gitToken,
  grant,
  modelOffering,
  modelProvider,
  oauthClient,
  offering,
  principal,
  provider,
  signalCorrelation,
  tenant,
  transaction,
  turnPart,
  wallet,
  workflowDefinition,
  workflowDefinitionVersion,
  workflowRun,
  workflowRunDispatch,
  workflowRunLaunchSpec,
} from "./schema";

const JSONObject = type("Record<string, unknown>");
const StringArray = type("string[]");
const WireGrantRuleArray = WireGrantRule.array();

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

const workflowRunStatuses = [
  "running",
  "completed",
  "failed",
  "cancelled",
] as const;
const WorkflowRunStatusValidator = type.enumerated(...workflowRunStatuses);
const WorkflowRunDispatchStatusValidator = type.enumerated(
  "pending",
  "acknowledged",
  "settled",
  "failed",
);

const WorkflowDefinitionStatusValidator = type.enumerated(
  ...workflowDefinitionStatuses,
);
const WorkflowDefinitionVersionStatusValidator = type.enumerated(
  ...workflowDefinitionVersionStatuses,
);

const PrincipalKindValidator = type.enumerated(...principalKinds);
const PrincipalStatusValidator = type.enumerated(...principalStatuses);

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
  "safety_rating",
  "step-start",
  "step-finish",
  "snapshot",
  "patch",
] as const;
const TurnPartTypeValidator = type.enumerated(...turnPartTypes);

export function parseWorkflowDefinitionRow(
  row: typeof workflowDefinition.$inferSelect,
) {
  return {
    ...row,
    status: WorkflowDefinitionStatusValidator.assert(row.status),
    grantRequirements:
      row.grantRequirements !== null
        ? GrantRequirement.array().assert(row.grantRequirements)
        : null,
    modelRequirements:
      row.modelRequirements !== null
        ? ModelRequirements.assert(row.modelRequirements)
        : null,
    // Nullable jsonb: `null` on a real drizzle row, `undefined` only on a
    // partial row-shaped test stub that predates the column. Both mean "no
    // bindings" -- treat them alike rather than asserting `undefined` as an
    // array.
    credentialBindings:
      row.credentialBindings === null || row.credentialBindings === undefined
        ? null
        : CredentialBinding.array().assert(row.credentialBindings),
  };
}

export function parseWorkflowDefinitionVersionRow(
  row: typeof workflowDefinitionVersion.$inferSelect,
) {
  return {
    ...row,
    status: WorkflowDefinitionVersionStatusValidator.assert(row.status),
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

export function parsePrincipalRow(row: typeof principal.$inferSelect) {
  return {
    ...row,
    kind: PrincipalKindValidator.assert(row.kind),
    status: PrincipalStatusValidator.assert(row.status),
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

export function parseWorkflowRunRow(row: typeof workflowRun.$inferSelect) {
  return {
    ...row,
    status: WorkflowRunStatusValidator.assert(row.status),
    modelPreferences:
      row.modelPreferences !== null
        ? InvokerModelPreferences.assert(row.modelPreferences)
        : null,
  };
}

export function parseWorkflowRunLaunchSpecRow(
  row: typeof workflowRunLaunchSpec.$inferSelect,
) {
  const sourceOfferingIds = StringArray.assert(row.sourceOfferingIds);
  assertLaunchSpecSources(sourceOfferingIds, row.defaultSourceOfferingId);
  return {
    ...row,
    definitionSnapshot: JSONObject.assert(row.definitionSnapshot),
    sourceOfferingIds,
    deployContent: JSONObject.assert(row.deployContent),
    toolPackagePins:
      row.toolPackagePins !== null
        ? ToolPackagePinArray.assert(row.toolPackagePins)
        : null,
  };
}

export function parseWorkflowRunDispatchRow(
  row: typeof workflowRunDispatch.$inferSelect,
) {
  return {
    ...row,
    status: WorkflowRunDispatchStatusValidator.assert(row.status),
    stepGrants: WireGrantRuleArray.assert(row.stepGrants),
  };
}

function assertLaunchSpecSources(
  sourceOfferingIds: readonly string[],
  defaultSourceOfferingId: string,
): void {
  if (sourceOfferingIds.length === 0) {
    throw new Error(
      "workflow launch spec requires at least one source offering",
    );
  }
  if (new Set(sourceOfferingIds).size !== sourceOfferingIds.length) {
    throw new Error("workflow launch spec source offering ids must be unique");
  }
  if (!sourceOfferingIds.includes(defaultSourceOfferingId)) {
    throw new Error(
      `workflow launch spec default source ${defaultSourceOfferingId} is not in its source offering ids`,
    );
  }
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
    config: row.config !== null ? TenantConfig.assert(row.config) : null,
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

import { type } from "arktype";
import { InvokerModelPreferences } from "./catalog";
import { grantEffects } from "./grants";
import { ApprovalResponse } from "./approvals";

const Effect = type.enumerated(...grantEffects);

export const workflowRunStatuses = [
  "deployed",
  "running",
  "updating",
  "error",
  "stopped",
] as const;
export type WorkflowRunStatus = (typeof workflowRunStatuses)[number];

const WorkflowRunStatusType = type.enumerated(...workflowRunStatuses);

export const CreateWorkflowRun = type({
  definitionId: "string",
  "modelPreferences?": InvokerModelPreferences.describe(
    "The invoker's per-model provider preferences for this launch. Applied over the tenant-visible providers after the definition's preferences; it can only reorder or restrict, never introduce a provider the tenant catalog lacks. Persisted on the run so re-resolution reuses it.",
  ),
  "invokerGrants?": type({
    resource: "string",
    action: "string",
    "effect?": Effect,
    "conditions?": "Record<string, unknown> | null",
  })
    .array()
    .describe(
      "Capabilities the invoker is willing to delegate to the run, resolved against the invoker's own authority at launch. These are materialized as grants on the run principal in addition to any grants from the definition's own requirements.",
    ),
});

export const WorkflowRunResponse = type({
  id: "string",
  definitionId: "string",
  definitionName: "string",
  tenantId: "string",
  address: "string",
  status: WorkflowRunStatusType.describe(
    "Lifecycle state of this run: `deployed` (provisioned on a sidecar, not yet started), `running` (started and serving), `updating` (rolling to a new definition version), `error` (launch or runtime failure), or `stopped` (undeployed).",
  ),
  "publicKey?": "string | null",
  "kernelId?": "string | null",
  "sidecarId?": "string | null",
  createdAt: "string",
  updatedAt: "string",
  "endedAt?": "string | null",
});

export const WorkflowRunHealth = type({
  liveness: "'ok' | 'unhealthy'",
  readiness: "'ok' | 'not_ready' | 'unhealthy'",
  "lastCheckedAt?": "string | null",
});

export const RunAuthorizationGrant = type({
  resource: "string",
  action: "string",
  effect: Effect,
});

export const RunAuthorizationResponse = type({
  runId: "string",
  grants: RunAuthorizationGrant.array().describe(
    "The run's effective authorization floor: each capability the run's principal holds with its resolved effect. A standing 'always' approval mutates the tool's committed grant in place at resolve time (approve-always sets 'allow', reject-always sets 'deny'), so a standing-resolved tool reads that effect directly. Read straight from the run's committed grants; complete for the source-ref deploy lineage (the shipping pipeline), whose committed grants carry every tool's effect. A pinned-tool deploy's ask floor is injected sidecar-side and is not reflected here.",
  ),
});

export const RunApprovalsResponse = type({
  runId: "string",
  approvals: ApprovalResponse.array().describe(
    "The run's approval decisions, newest first, across every status. A tool an operator turned into a standing allow appears here with scope 'always' and status 'approved'.",
  ),
});

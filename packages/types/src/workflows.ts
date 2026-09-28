// Status vocabulary for the first-class workflow definition model, kept in its
// own workflow-scoped module.
export const workflowDefinitionStatuses = ["deployed", "stopped"] as const;
export type WorkflowDefinitionStatus =
  (typeof workflowDefinitionStatuses)[number];

export const workflowDefinitionVersionStatuses = [
  "active",
  "inactive",
  "failed",
] as const;
export type WorkflowDefinitionVersionStatus =
  (typeof workflowDefinitionVersionStatuses)[number];

import { type } from "arktype";

import { ConnectorThreadState, ConversationTurn, TokenUsage } from "./runtime";

const WorkflowDefinitionStatusType = type.enumerated(
  ...workflowDefinitionStatuses,
);
const WorkflowDefinitionVersionStatusType = type.enumerated(
  ...workflowDefinitionVersionStatuses,
);

// One entry in a definition's version history.
export const WorkflowDefinitionVersion = type({
  version: "string",
  status: WorkflowDefinitionVersionStatusType,
  createdAt: "string",
});

// The first-class workflow definition, as returned by the definition routes.
export const WorkflowDefinitionResponse = type({
  id: "string",
  tenantId: "string",
  name: "string",
  "description?": "string | null",
  currentVersion: "string",
  status: WorkflowDefinitionStatusType.describe(
    "Lifecycle state of the definition: `deployed` (a launchable version is active) or `stopped` (deactivated).",
  ),
  createdAt: "string",
  updatedAt: "string",
});

// Rollback a definition to a prior version.
export const WorkflowRollbackRequest = type({
  version: "string",
});

export const WorkflowDeploymentStatus = type.enumerated(
  "deployed",
  "pending",
  "recovering",
  "releasing",
  "released",
  "failed",
  "destroy_failed",
);
export type WorkflowDeploymentStatus = typeof WorkflowDeploymentStatus.infer;

export const WorkflowDeploymentResponse = type({
  id: "string",
  tenantId: "string",
  definitionAssetId: "string",
  status: WorkflowDeploymentStatus.describe(
    "Deployment lifecycle status. `failed` is a terminal failure with no infrastructure. `destroy_failed` is a permanent cleanup failure where infrastructure may remain and require operator cleanup.",
  ),
  createdAt: "string",
});
export type WorkflowDeploymentResponse =
  typeof WorkflowDeploymentResponse.infer;

// An agent step's state as exported from a workflow run, and as a later
// deployment imports it. It is plain client-owned data: nothing links the
// exporting and importing runs, and a client may edit it in between. Pending
// operations are deliberately absent -- their correlation ids only mean
// something inside the run that registered them.
export const StepStateSnapshot = type({
  version: type("1").describe("Snapshot format version."),
  turns: ConversationTurn.array().describe(
    "The step agent's conversation history, oldest first.",
  ),
  tokenUsage: TokenUsage,
  connectorState: ConnectorThreadState.or("null").describe(
    "Mail thread the agent replies on, or null when no thread is active. An import must carry null: the imported step starts without a thread, and a single-step agent starts one from the sender of its first mail.",
  ),
});
export type StepStateSnapshot = typeof StepStateSnapshot.infer;

// Deploy-time step state: a snapshot per agent step, keyed by the id of the
// step it seeds. Imported state is committed to the run's history as JSON,
// which has no encoding for a non-finite number: `JSON.parse` reads an
// out-of-range literal such as `1e999` as `Infinity`, and serializing it
// writes `null`. Such a number is rejected here rather than silently
// changing in the seed. Serializing the seed also recurses once per level
// of nesting, so client JSON nested deep enough (a tool call's arguments)
// would overflow the call stack; no real conversation nests near the cap.
const MAX_STEP_STATE_DEPTH = 64;

export const StepStateImport = type({ "[string]": StepStateSnapshot }).narrow(
  (stepState, ctx) => {
    const problem = findUnstorableStepState(stepState);
    // `actual` replaces the default description of the rejected value,
    // which would print the value and overflow on the same nesting.
    return problem === null || ctx.reject(problem);
  },
);
export type StepStateImport = typeof StepStateImport.infer;

// Walks with an explicit stack, so it cannot overflow on the nesting it
// exists to reject.
export function findUnstorableStepState(
  value: unknown,
): { expected: string; actual: string } | null {
  const pending = [{ value, depth: 0 }];
  for (let next = pending.pop(); next !== undefined; next = pending.pop()) {
    if (typeof next.value === "number" && !Number.isFinite(next.value)) {
      return {
        expected: "state whose numbers are all finite",
        actual: String(next.value),
      };
    }
    if (typeof next.value === "object" && next.value !== null) {
      if (next.depth >= MAX_STEP_STATE_DEPTH) {
        return {
          expected: `state nested at most ${String(MAX_STEP_STATE_DEPTH)} levels deep`,
          actual: "nested deeper",
        };
      }
      for (const child of Object.values(next.value)) {
        pending.push({ value: child, depth: next.depth + 1 });
      }
    }
  }
  return null;
}

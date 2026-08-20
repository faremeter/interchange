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

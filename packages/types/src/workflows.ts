// Status vocabularies for the first-class workflow definition model. Kept in a
// workflow-scoped module, deliberately separate from the coincident
// `agentDefinitionStatuses` / agent version statuses in `./agents`: the agent
// types are removed when the agent surface is retired, so a workflow table or
// validator must not depend on them.
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

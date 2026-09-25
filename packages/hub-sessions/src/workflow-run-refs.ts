// The ref holding a run's events and its steps' state. The other restorable
// ref carries only the claim-check inbox.
export const WORKFLOW_RUN_STATE_REF = "refs/heads/main";

export const WORKFLOW_RUN_RESTORE_REFS = [
  WORKFLOW_RUN_STATE_REF,
  "refs/heads/events",
] as const;

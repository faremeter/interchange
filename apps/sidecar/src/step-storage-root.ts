import path from "node:path";

import type { RepoId } from "@intx/hub-sessions/substrate";

/**
 * Root of a deployment's multi-step (cold) step storage on this host:
 * `<dataDir>/workflow-step-state/<repoId>/runs/`, holding one
 * `<runId>/steps/<stepId>/attempt-<n>/` local store and workspace per step
 * attempt the host ran.
 */
export function coldStepStorageRoot(args: {
  dataDir: string;
  workflowRunRepoId: RepoId;
}): string {
  return path.join(
    args.dataDir,
    "workflow-step-state",
    args.workflowRunRepoId.id,
    "runs",
  );
}

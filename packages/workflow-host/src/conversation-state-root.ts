import path from "node:path";

/**
 * The directory holding a deployment's local durable-conversation stores, one
 * per agent key, under the sidecar data dir. The workflow child writes them and
 * the deploy router clears them, so both derive the path here.
 */
export function conversationStateRoot(
  dataDir: string,
  workflowRunRepoId: string,
): string {
  return path.join(dataDir, "agent-conversation-state", workflowRunRepoId);
}

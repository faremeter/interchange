import path from "node:path";

/**
 * The directory that held a deployment's local durable-conversation stores,
 * one per agent key, under the sidecar data dir. No current code writes it;
 * the deploy router removes the copies an earlier version wrote at undeploy.
 */
export function conversationStateRoot(
  dataDir: string,
  workflowRunRepoId: string,
): string {
  return path.join(dataDir, "agent-conversation-state", workflowRunRepoId);
}

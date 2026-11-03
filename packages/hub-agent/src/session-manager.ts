// The directory teardown the deploy path and the hub-link call.
//
// The in-process session runtime -- harness construction, agent
// provisioning, disk restore, and per-agent mail audit -- has been
// retired: every agent now runs as a supervised workflow-process child
// on the workflow-run substrate.

import type { AgentRepoStore } from "./agent-repo-store";

export type SessionManagerConfig = {
  repoStore: AgentRepoStore;
};

export type SessionManager = {
  deleteAgentDir(agentAddress: string): Promise<void>;
  /**
   * Session id for an address' outbound mail forwarding. Always undefined
   * now that no in-process sessions exist; the hub-link tolerates a
   * missing id and forwards the mail without one.
   */
  getSessionId(agentAddress: string): string | undefined;
};

export function createSessionManager(
  config: SessionManagerConfig,
): SessionManager {
  const { repoStore } = config;

  return {
    deleteAgentDir: (agentAddress: string) => repoStore.remove(agentAddress),
    getSessionId: () => undefined,
  };
}

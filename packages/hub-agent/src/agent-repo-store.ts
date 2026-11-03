// Per-agent on-disk directory layout.
//
// Owns the removal of an agent's directory. Key custody lives in
// AgentKeyStore alongside this store; both share the directory-layout
// helpers in agent-paths.

import fsp from "node:fs/promises";
import { getLogger } from "@intx/log";
import { syncRemovedPathDurable } from "@intx/storage-isogit/node";

import { agentDir } from "./agent-paths";

const logger = getLogger(["interchange", "hub-agent", "repo-store"]);

export type AgentRepoStore = {
  /**
   * Resolve the on-disk directory for an agent. Exposed for integration
   * tests that need to assert against disk state without depending on
   * the (intentionally opaque) directory naming scheme.
   */
  getAgentDir(address: string): string;
  remove(address: string): Promise<void>;
};

export function createAgentRepoStore(config: {
  dataDir: string;
}): AgentRepoStore {
  const { dataDir } = config;

  function getAgentDir(address: string): string {
    return agentDir(dataDir, address);
  }

  async function remove(address: string): Promise<void> {
    await fsp.rm(getAgentDir(address), { recursive: true, force: true });
    await syncRemovedPathDurable(getAgentDir(address));
    logger.info`Deleted agent directory for ${address}`;
  }

  return {
    getAgentDir,
    remove,
  };
}

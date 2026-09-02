// Per-agent on-disk repository layout.
//
// Owns the isogit repo wrapper and the deploy-pack apply / state-pack
// produce flow. Key custody lives in AgentKeyStore alongside this
// store; both share the directory-layout helpers in agent-paths.

import fsp from "node:fs/promises";
import { getLogger } from "@intx/log";
import {
  initAgentRepo,
  applyPack,
  createDeployPack,
  currentBranch,
  type CommitVerifier,
} from "@intx/storage-isogit/node";

import { agentDir } from "./agent-paths";

const logger = getLogger(["interchange", "hub-agent", "repo-store"]);

export type ApplyDeployPackArgs = {
  address: string;
  pack: Uint8Array;
  ref: string;
  commitSha: string;
  transferId: string;
  verifyCommit?: CommitVerifier;
};

export type AgentRepoStore = {
  /**
   * Resolve the on-disk directory for an agent. Exposed for integration
   * tests that need to assert against disk state without depending on
   * the (intentionally opaque) directory naming scheme.
   */
  getAgentDir(address: string): string;
  initRepo(address: string): Promise<void>;
  applyDeployPack(args: ApplyDeployPackArgs): Promise<void>;
  createStatePack(
    address: string,
  ): Promise<{ pack: Uint8Array; commitSha: string; ref: string }>;
  remove(address: string): Promise<void>;
};

export function createAgentRepoStore(config: {
  dataDir: string;
}): AgentRepoStore {
  const { dataDir } = config;

  function getAgentDir(address: string): string {
    return agentDir(dataDir, address);
  }

  async function initRepo(address: string): Promise<void> {
    await initAgentRepo(getAgentDir(address));
  }

  async function applyDeployPackImpl(args: ApplyDeployPackArgs): Promise<void> {
    const { address, pack, ref, commitSha, transferId, verifyCommit } = args;
    await applyPack(
      getAgentDir(address),
      pack,
      ref,
      commitSha,
      transferId,
      verifyCommit,
    );
    logger.info`Applied deploy pack for ${address} at ${commitSha.slice(0, 8)}`;
  }

  async function createStatePack(
    address: string,
  ): Promise<{ pack: Uint8Array; commitSha: string; ref: string }> {
    const dir = getAgentDir(address);
    const branch = await currentBranch(dir);
    const ref = `refs/heads/${branch}`;
    const { pack, commitSha } = await createDeployPack(dir, ref);
    return { pack, commitSha, ref };
  }

  async function remove(address: string): Promise<void> {
    await fsp.rm(getAgentDir(address), { recursive: true });
    logger.info`Deleted agent directory for ${address}`;
  }

  return {
    getAgentDir,
    initRepo,
    applyDeployPack: applyDeployPackImpl,
    createStatePack,
    remove,
  };
}

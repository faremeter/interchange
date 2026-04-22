import type { ContextStore, AuditStore } from "@interchange/types/runtime";
import { initAgentRepo } from "./init";
import { IsogitStore } from "./store";
import type { CommitSigner } from "./signer";

export type { ContextStore, AuditStore, CommitSigner };
export type { CommitVerifier, TreeValidator } from "./pack-receive";
export { IsogitStore };
export {
  switchBranch,
  createAndSwitchBranch,
  currentBranch,
  listBranches,
  logHistory,
} from "./history";
export { initRepo, initAgentRepo } from "./init";
export { applyPack, receivePackObjects } from "./pack-receive";
export { createDeployPack } from "./pack-send";
export { collectReachableObjects } from "./object-walk";

/**
 * Initialize an agent repository at `dir` and return a store backed by that
 * repository. The returned object implements both ContextStore (inference
 * state) and AuditStore (tool authorization records).
 */
export async function createIsogitStore(
  dir: string,
  signer?: CommitSigner,
): Promise<ContextStore & AuditStore> {
  await initAgentRepo(dir);
  return new IsogitStore(dir, signer);
}

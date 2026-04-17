import type { ContextStore, AuditStore } from "@interchange/types/runtime";
import { initAgentRepo } from "./init";
import { IsogitStore } from "./store";

export type { ContextStore, AuditStore };
export { IsogitStore };
export {
  switchBranch,
  createAndSwitchBranch,
  currentBranch,
  listBranches,
  logHistory,
} from "./history";
export { initAgentRepo } from "./init";

/**
 * Initialize an agent repository at `dir` and return a ContextStore backed
 * by that repository. This is the primary entry point for downstream
 * consumers.
 */
export async function createIsogitStore(dir: string): Promise<ContextStore> {
  await initAgentRepo(dir);
  return new IsogitStore(dir);
}

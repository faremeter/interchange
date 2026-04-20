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
 * Initialize an agent repository at `dir` and return a store backed by that
 * repository. The returned object implements both ContextStore (inference
 * state) and AuditStore (tool authorization records).
 */
export async function createIsogitStore(
  dir: string,
): Promise<ContextStore & AuditStore> {
  await initAgentRepo(dir);
  return new IsogitStore(dir);
}

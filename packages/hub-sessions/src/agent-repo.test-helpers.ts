import type { AgentRepoStore } from "./agent-repo";
import type { NewlyTerminalRun, RepoId } from "./repo-store";

/**
 * Prepare and commit a workflow-run pack in one call, for a test that
 * only needs the pack applied. Returns the runs the pack drove terminal,
 * and none when the ref already held the pack tip at either step.
 *
 * Duplicated in tests/hub-agent/lib/deploy-flow-env.ts: the composite
 * tsconfig boundary between packages/hub-sessions and tests/ stops
 * either tree from importing the other's test helpers. Change both.
 */
export async function applyWorkflowRunPack(
  store: Pick<AgentRepoStore, "prepareWorkflowRunPack">,
  repoId: RepoId,
  pack: Uint8Array,
  ref: string,
  commitSha: string,
): Promise<NewlyTerminalRun[]> {
  const prepared = await store.prepareWorkflowRunPack(
    repoId,
    pack,
    ref,
    commitSha,
  );
  if (prepared.kind === "already_applied") return [];
  const result = await prepared.commit();
  return result.kind === "applied" ? result.newlyTerminalRuns : [];
}

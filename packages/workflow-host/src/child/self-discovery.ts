// Self-discovery: enumerate in-flight runs from the workflow-run repo
// at startup and surface their event logs so the child can resume
// each one in place.
//
// The supervisor does not pass an explicit list of runs; the
// workflow-run repo's `runs/<runId>/` subtree is the authoritative
// ledger. A run is in-flight if its log lacks a terminal event
// (`RunCompleted`, `RunFailed`, `RunCancelled`). Resume seeds the
// runtime body via the seed-events path on `runtimeRun`.
//
// Working-tree-read pattern: the substrate's `getRepoDir(repoId)` is a
// pure path computation; sibling production adapters
// (`adapters/repo-store.ts`, `adapters/blob-substrate.ts`,
// `adapters/spawn-child.ts`) read working-tree contents directly via
// `node:fs/promises`. Self-discovery follows the same path so a
// startup scan does not need to consult the git object database.

import type {
  RepoId,
  RepoStore as SubstrateRepoStore,
} from "@intx/hub-sessions";
import {
  isTerminalRunPhase,
  resumeFromLog,
  type RunState,
  type WorkflowEvent,
} from "@intx/workflow";

import type { RepoStore as RuntimeRepoStore } from "@intx/workflow";

const RUNS_PREFIX = "runs";

/**
 * Per-run discovery entry. The runtime body re-applies `seedEvents`
 * via its `resumeFromEvents` path and resumes the run from
 * `resumedState`. The caller hands both into `runtimeRun`.
 */
export interface DiscoveredRun {
  runId: string;
  seedEvents: readonly WorkflowEvent[];
  resumedState: RunState;
}

export interface DiscoverRunsOpts {
  /** Workflow-run substrate repo store. */
  substrate: SubstrateRepoStore;
  /** Workflow-run repo identity. */
  repoId: RepoId;
  /** Runtime-env `RepoStore` adapter the runs are read through. */
  runtimeRepoStore: RuntimeRepoStore;
}

/**
 * Enumerate `runs/<runId>/` subdirectories and return one
 * `DiscoveredRun` entry per run whose log does not already end with a
 * terminal event. Runs that already terminated are skipped because
 * resume against a terminal log would still settle without progress
 * but would generate spurious "resume seed" reads for no benefit.
 */
export async function discoverInFlightRuns(
  opts: DiscoverRunsOpts,
): Promise<readonly DiscoveredRun[]> {
  const fs = await import("node:fs/promises");
  const path = await import("node:path");
  const dir = opts.substrate.getRepoDir(opts.repoId);
  const runsDir = path.join(dir, RUNS_PREFIX);
  let runDirs: string[];
  try {
    runDirs = await fs.readdir(runsDir);
  } catch (cause) {
    if (isErrnoNotFound(cause)) return [];
    throw cause;
  }
  const out: DiscoveredRun[] = [];
  for (const runId of runDirs) {
    const events = await opts.runtimeRepoStore.read(runId);
    if (events.length === 0) continue;
    const resumed = resumeFromLog(runId, events);
    if (isTerminalRunPhase(resumed.phase)) continue;
    out.push({ runId, seedEvents: events, resumedState: resumed });
  }
  return out;
}

function isErrnoNotFound(cause: unknown): boolean {
  if (cause === null || typeof cause !== "object") return false;
  const code = (cause as { code?: unknown }).code;
  return code === "ENOENT";
}

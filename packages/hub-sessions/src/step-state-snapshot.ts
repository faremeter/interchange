import { type } from "arktype";

import { StepStateSnapshot } from "@intx/types";

import type { CommittedReads, RepoId, RepoStore } from "./repo-store/types";
import {
  createCommittedStepStateReader,
  reconstructStepState,
} from "./step-state";
import {
  WORKFLOW_RUN_STEP_SEED_FILE,
  workflowRunLegacyAgentStatePrefix,
  workflowRunStepSeedPath,
  workflowRunStepStatePrefix,
} from "./workflow-run-kind";

const HUB_PRINCIPAL = { kind: "hub" } as const;
const WORKFLOW_RUN_REF = "refs/heads/main";

/**
 * Export an agent step's state as last committed to the Hub's copy of a
 * deployment's workflow-run history. `runId` is the deployment's top-level
 * run: a single-step deployment that predates the steps subtree, and whose
 * agent has not restored since, still keeps its conversation at the legacy
 * `agent-state/<stepId>/`, which is read in its place. A step that has no
 * state of its own yet exports the seed its deployment imported for it.
 * Pending operations are left out, since their correlation ids only mean
 * something inside the run that registered them. Returns `null` when the
 * step has neither committed state nor a seed.
 */
export async function readStepStateSnapshot(args: {
  repoStore: Pick<RepoStore, "openCommittedReads">;
  repoId: RepoId;
  runId: string;
  stepId: string;
}): Promise<StepStateSnapshot | null> {
  const reads = await args.repoStore.openCommittedReads(
    HUB_PRINCIPAL,
    args.repoId,
    WORKFLOW_RUN_REF,
  );
  if (reads === null) return null;
  const label = `${args.repoId.id} run ${args.runId} step ${args.stepId}`;
  const state =
    (await reconstructStepState(
      createCommittedStepStateReader(
        reads,
        workflowRunStepStatePrefix(args.runId, args.stepId),
      ),
      label,
    )) ??
    (await reconstructStepState(
      createCommittedStepStateReader(
        reads,
        workflowRunLegacyAgentStatePrefix(args.stepId),
      ),
      label,
    ));
  if (state === null) {
    return readCommittedSeed(reads, args.runId, args.stepId);
  }
  const snapshot = StepStateSnapshot({
    version: 1,
    turns: state.turns,
    tokenUsage: state.tokenUsage,
    connectorState: state.connectorState,
  });
  if (snapshot instanceof type.errors) {
    throw new Error(
      `step state for ${label} is not a valid snapshot: ${snapshot.summary}`,
    );
  }
  return snapshot;
}

/**
 * Parse a step's seed (`WORKFLOW_RUN_STEP_SEED_FILE`), read from
 * `seedPath`. A seed that is not a valid snapshot throws: starting the step
 * without the state its deployment imported would lose that state silently.
 */
export function parseStepStateSeed(
  raw: string,
  seedPath: string,
): StepStateSnapshot {
  let body: unknown;
  try {
    body = JSON.parse(raw);
  } catch (cause) {
    throw new Error(`step seed ${seedPath} is not valid JSON`, { cause });
  }
  const seed = StepStateSnapshot(body);
  if (seed instanceof type.errors) {
    throw new Error(
      `step seed ${seedPath} is not a valid snapshot: ${seed.summary}`,
    );
  }
  return seed;
}

async function readCommittedSeed(
  reads: CommittedReads,
  runId: string,
  stepId: string,
): Promise<StepStateSnapshot | null> {
  const seedPath = workflowRunStepSeedPath(runId, stepId);
  const stepDir = seedPath.slice(0, seedPath.lastIndexOf("/"));
  const entry = (await reads.listDir(stepDir)).find(
    (candidate) =>
      candidate.name === WORKFLOW_RUN_STEP_SEED_FILE &&
      candidate.type === "blob",
  );
  if (entry === undefined) return null;
  return parseStepStateSeed(
    new TextDecoder().decode(await reads.readBlobByOid(entry.oid)),
    seedPath,
  );
}

// Deploy-time import of agent step state. A deployment request can carry a
// `StepStateSnapshot` per agent step, typically exported from an earlier run
// of any deployment. The Hub turns each into the step's seed
// (`WORKFLOW_RUN_STEP_SEED_FILE`) in the new deployment's workflow-run
// history before the deployment first starts, so the history the deploy
// replays onto its sidecar already carries it and the step starts from it.
//
// Imported turns can name messages by their mailbox UID (the mail tools
// address a message as `{ uid, mailbox }`), and those UIDs belong to the
// exporting deployment's mailbox. A fresh mailbox numbers from 1, so such a
// UID would sooner or later name an unrelated message of the new deployment,
// and a reply meant for the old message would silently go to the new one.
// The import therefore starts the new deployment's mailbox at the largest
// `mailboxUidNext` the snapshots carry: every imported UID then names no
// message at all, and a tool call that uses one fails instead.

import { type } from "arktype";

import {
  assertWellFormedToolSequence,
  transformMessages,
} from "@intx/inference";
import type { StepStateImport, StepStateSnapshot } from "@intx/types";
import type { ConversationTurn, InferenceSource } from "@intx/types/runtime";
import type { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import { deriveWorkflowRunRepoId } from "@intx/workflow-deploy";

import { MAILBOX_INDEX_VERSION, type MailboxIndex } from "./mailbox-index";
import type { RepoStore } from "./repo-store/types";
import {
  WORKFLOW_RUN_MAILBOX_INBOX_DIR,
  WORKFLOW_RUN_MAILBOX_INDEX_FILE,
  WORKFLOW_RUN_MAILBOX_PREFIX,
  WORKFLOW_RUN_STEP_SEED_FILE,
  workflowRunStepSeedPath,
} from "./workflow-run-kind";

const HUB_PRINCIPAL = { kind: "hub" } as const;
const WORKFLOW_RUN_REF = "refs/heads/main";

const AgentStep = type({ kind: "'step'" });

export type StepStateSeedsResult =
  | { readonly ok: true; readonly files: Readonly<Record<string, string>> }
  | { readonly ok: false; readonly reason: string };

/**
 * Build the files that start a new deployment from imported step state,
 * keyed by repo path: a seed per step and, when a snapshot bounds its
 * mailbox UIDs, the deployment's initial mailbox index. Every step id in
 * `stepState` must name one of the workflow's top-level agent steps: those
 * run in the deployment's top-level run, `runId`, where the seeds are filed.
 *
 * A snapshot continues only on the model that produced it: every assistant
 * turn must record the model the step is pinned to, since the Hub does not
 * convert a conversation between models. A tool call left without a result
 * is closed with a synthetic error result, since no provider accepts an
 * unanswered call; the same pass turns safety-rating blocks into text and
 * drops assistant turns with no content. The closed turns must then pair
 * every tool result with an earlier call and repeat no call or result, the
 * check the step's reactor applies to every prompt: a seed that fails it
 * would fail every attempt of the step.
 *
 * A snapshot must import `connectorState: null`. The thread's recipients
 * are whatever the client wrote, so an imported thread would let the
 * importer choose where the step's replies go; a thread the step starts
 * itself replies only to senders that mailed its deployment.
 *
 * A result that is not `ok` carries the reason the import was refused.
 *
 * The mailbox index gets `uidValidity`, which must be fresh: the new mailbox
 * holds none of the exporting mailbox's messages.
 */
export function buildStepStateSeeds(args: {
  projection: WorkflowProjectionDefinition;
  sources: Readonly<Record<string, readonly InferenceSource[]>>;
  runId: string;
  stepState: StepStateImport;
  uidValidity: number;
}): StepStateSeedsResult {
  const imported = Object.entries(args.stepState);
  const unknownStepIds = imported
    .map(([stepId]) => stepId)
    .filter(
      (stepId) =>
        !args.projection.stepOrder.includes(stepId) ||
        AgentStep(args.projection.steps[stepId]) instanceof type.errors,
    );
  if (unknownStepIds.length > 0) {
    return {
      ok: false,
      reason: `Step state names ${unknownStepIds
        .map((stepId) => JSON.stringify(stepId))
        .join(", ")}, which the workflow has no top-level agent step for`,
    };
  }

  const files: Record<string, string> = {};
  for (const [stepId, snapshot] of imported) {
    if (snapshot.connectorState !== null) {
      return {
        ok: false,
        reason: `Step state for ${JSON.stringify(stepId)} carries a reply thread; import it with connectorState null, since a step replies only to senders that mailed its own deployment`,
      };
    }
    const targetModel = args.sources[stepId]?.[0]?.model;
    if (targetModel === undefined) {
      throw new Error(`agent step ${stepId} was pinned no inference source`);
    }
    const otherModel = findOtherModel(snapshot.turns, targetModel);
    if (otherModel !== null) {
      return {
        ok: false,
        reason: `Step state for ${JSON.stringify(stepId)} ${otherModel}, but the step runs on ${JSON.stringify(targetModel)}; the Hub does not convert a conversation between models, so import it into a step that runs on the same model`,
      };
    }
    const seed: StepStateSnapshot = {
      ...snapshot,
      turns: transformMessages(snapshot.turns, { targetModel }),
    };
    try {
      assertWellFormedToolSequence(seed.turns);
    } catch (cause) {
      return {
        ok: false,
        reason: `Step state for ${JSON.stringify(stepId)} is not a conversation its step can continue: ${cause instanceof Error ? cause.message : String(cause)}`,
      };
    }
    files[workflowRunStepSeedPath(args.runId, stepId)] = JSON.stringify(seed);
  }
  const uidNexts = imported.flatMap(([, snapshot]) =>
    snapshot.mailboxUidNext === undefined ? [] : [snapshot.mailboxUidNext],
  );
  if (uidNexts.length > 0) {
    const index: MailboxIndex = {
      version: MAILBOX_INDEX_VERSION,
      uidValidity: args.uidValidity,
      uidNext: Math.max(...uidNexts),
      highestModSeq: 0,
      messages: [],
      expunged: [],
    };
    files[
      `${WORKFLOW_RUN_MAILBOX_PREFIX}/${WORKFLOW_RUN_MAILBOX_INBOX_DIR}/${WORKFLOW_RUN_MAILBOX_INDEX_FILE}`
    ] = JSON.stringify(index);
  }
  return { ok: true, files };
}

/**
 * Describe the first assistant turn not produced by `model`, or `null` when
 * every assistant turn records `model`.
 */
function findOtherModel(
  turns: readonly ConversationTurn[],
  model: string,
): string | null {
  for (const turn of turns) {
    if (turn.role !== "assistant" || turn.model === model) continue;
    return turn.model === undefined
      ? "has an assistant turn that records no model"
      : `was produced by ${JSON.stringify(turn.model)}`;
  }
  return null;
}

/**
 * Commit seeds from `buildStepStateSeeds` to the Hub's copy of the
 * deployment's workflow-run history. Writes nothing when there are none.
 */
export async function writeStepStateSeeds(args: {
  repoStore: Pick<RepoStore, "writeTree">;
  deploymentAddress: string;
  files: Readonly<Record<string, string>>;
}): Promise<void> {
  const paths = Object.keys(args.files);
  if (paths.length === 0) return;
  const steps = paths.filter((p) =>
    p.endsWith(`/${WORKFLOW_RUN_STEP_SEED_FILE}`),
  ).length;
  await args.repoStore.writeTree(
    HUB_PRINCIPAL,
    {
      kind: "workflow-run",
      id: deriveWorkflowRunRepoId(args.deploymentAddress),
    },
    WORKFLOW_RUN_REF,
    {
      files: { ...args.files },
      message: `Import state for ${String(steps)} step(s)`,
    },
  );
}

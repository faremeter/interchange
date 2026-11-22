// Deploy-time import of agent step state. A deployment request can carry a
// `StepStateSnapshot` per agent step, typically exported from an earlier run
// of any deployment. The Hub turns each into the step's seed
// (`WORKFLOW_RUN_STEP_SEED_FILE`) in the new deployment's workflow-run
// history before the deployment first starts, so the history the deploy
// restores onto its sidecar already carries it and the step starts from it.
//
// A snapshot is keyed by the key of the step's place (`stepPlaceKey`). The
// key is a top-level agent step's id, or `<section>.<step>` for an agent step
// in the inline body of a top-level onTrigger section. Each event of such a
// section runs its body again at the same place, so the body step continues
// the imported conversation. A loop body step and a map item also have keys,
// but the import and the export refuse them.
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
import type {
  WorkflowProjectionDefinition,
  WorkflowProjectionWithSources,
} from "@intx/types/sidecar";
import {
  isTopLevelStepPlaceKey,
  MAX_STEP_PLACE_SEGMENT_BYTES,
  measureStepPlaceKey,
} from "@intx/types/step-place";
import type { StepWalkDescent } from "@intx/workflow/definition";
import { inlineBodyRef } from "@intx/workflow/projection";
import { inertNestedBodies } from "@intx/workflow-deploy";

import { MAILBOX_INDEX_VERSION, type MailboxIndex } from "./mailbox-index";
import type { LocalRepoStore } from "./repo-store";
import type { RepoStore } from "./repo-store/types";
import {
  WORKFLOW_RUN_MAILBOX_INBOX_DIR,
  WORKFLOW_RUN_MAILBOX_INDEX_FILE,
  WORKFLOW_RUN_MAILBOX_PREFIX,
  WORKFLOW_RUN_REF,
  WORKFLOW_RUN_STEP_SEED_FILE,
  workflowRunRepoIdForAddress,
  workflowRunStepSeedPath,
} from "./workflow-run-kind";

const HUB_PRINCIPAL = { kind: "hub" } as const;

const AgentStep = type({ kind: "'step'" });
const StepKind = type({ kind: "string" });

// Only the inline body of an onTrigger section: the key of a body step names
// its section, so no other body is reached.
const ON_TRIGGER_BODY_DESCENT: StepWalkDescent = Object.freeze({
  loopBodies: false,
  inlineOnTriggerBodies: true,
  inlineChildWorkflowBodies: false,
});

const BODY_STEP_KEY_FORM = `a key names a top-level agent step, or an agent step in the body of a top-level onTrigger section as "<section>.<step>"`;

// A refusal is stored on the deployment's probe and returned to the caller,
// and the text it quotes comes from the request, so it names at most this
// many step ids and cuts quoted text at this many characters.
const MAX_REPORTED_STEP_IDS = 10;
const MAX_REPORTED_TEXT_LENGTH = 200;

function reportedText(text: string): string {
  return text.length > MAX_REPORTED_TEXT_LENGTH
    ? `${text.slice(0, MAX_REPORTED_TEXT_LENGTH)}…`
    : text;
}

/** An agent step in the inline body of a top-level onTrigger section. */
export type OnTriggerBodyStepKey =
  | {
      readonly ok: true;
      readonly sectionId: string;
      readonly stepId: string;
      /** The body's ref, the id its pinned sources are filed under. */
      readonly bodyRef: string;
    }
  | { readonly ok: false; readonly reason: string };

/**
 * Resolve a step state key that holds a `.` against the deployment's frozen
 * projection. The key must be `<section>.<step>`, where `<section>` is a
 * top-level onTrigger step with an inline body and `<step>` is an agent step
 * of that body. Import and export apply this one rule.
 *
 * A key with more than one `.`, a map item's key, a section that is not an
 * onTrigger step, and a body step that is not an agent step are refused. A
 * `{ ref }` body is refused too: its steps are in a separately deployed
 * definition that the projection does not hold, so the Hub cannot check them.
 */
export function resolveOnTriggerBodyStepKey(
  projection: WorkflowProjectionDefinition,
  key: string,
): OnTriggerBodyStepKey {
  const refuse = (problem: string): OnTriggerBodyStepKey => ({
    ok: false,
    reason: `Step state key ${JSON.stringify(reportedText(key))} ${problem}`,
  });
  const path = key.split(".");
  const [sectionId, stepId] = path;
  if (path.length !== 2 || sectionId === undefined || stepId === undefined) {
    return refuse(
      `names a step more than one level below the top-level run; ${BODY_STEP_KEY_FORM}`,
    );
  }
  if (key.includes("[")) {
    return refuse(`names a map item; ${BODY_STEP_KEY_FORM}`);
  }
  const { segmentBytes } = measureStepPlaceKey({
    stepPath: [sectionId, stepId],
  });
  if (segmentBytes > MAX_STEP_PLACE_SEGMENT_BYTES) {
    return refuse(
      `encodes to ${String(segmentBytes)} bytes; a step's state directory name is limited to ${String(MAX_STEP_PLACE_SEGMENT_BYTES)} bytes`,
    );
  }
  const section = projection.stepOrder.includes(sectionId)
    ? projection.steps[sectionId]
    : undefined;
  const sectionKind = StepKind(section);
  if (sectionKind instanceof type.errors) {
    return refuse("names no top-level step of the workflow");
  }
  if (sectionKind.kind !== "onTrigger") {
    return refuse(
      `names a top-level step of kind ${JSON.stringify(sectionKind.kind)}, not an onTrigger section; ${BODY_STEP_KEY_FORM}`,
    );
  }
  const [body] = inertNestedBodies(section, ON_TRIGGER_BODY_DESCENT);
  if (body === undefined) {
    return refuse(
      "names an onTrigger section whose body is a { ref } to a separately deployed workflow, whose steps the Hub cannot check",
    );
  }
  if (
    !body.stepOrder.includes(stepId) ||
    AgentStep(body.steps[stepId]) instanceof type.errors
  ) {
    return refuse("names no agent step in the body of its onTrigger section");
  }
  return {
    ok: true,
    sectionId,
    stepId,
    bodyRef: inlineBodyRef(projection.id, sectionId),
  };
}

/** Whether `stepId` names one of the workflow's top-level agent steps. */
function namesTopLevelAgentStep(
  projection: WorkflowProjectionDefinition,
  stepId: string,
): boolean {
  return (
    projection.stepOrder.includes(stepId) &&
    !(AgentStep(projection.steps[stepId]) instanceof type.errors)
  );
}

/**
 * Check one step state key against the deployment's frozen projection with
 * the rule import and export share. A key with no `.` must name a top-level
 * agent step, so a top-level map item's key such as `m[2]` is refused. A key
 * with a `.` must name an agent step in the inline body of a top-level
 * onTrigger section (`resolveOnTriggerBodyStepKey`).
 */
export function checkStepStateKey(
  projection: WorkflowProjectionDefinition,
  key: string,
): { readonly ok: true } | { readonly ok: false; readonly reason: string } {
  if (!isTopLevelStepPlaceKey(key)) {
    return resolveOnTriggerBodyStepKey(projection, key);
  }
  if (namesTopLevelAgentStep(projection, key)) return { ok: true };
  return {
    ok: false,
    reason: `Step state key ${JSON.stringify(reportedText(key))} names no top-level agent step of the workflow; ${BODY_STEP_KEY_FORM}`,
  };
}

export type StepStateSeedsResult =
  | { readonly ok: true; readonly files: Readonly<Record<string, string>> }
  | { readonly ok: false; readonly reason: string };

/**
 * Build the files that start a new deployment from imported step state,
 * keyed by repo path: a seed per step and, when a snapshot bounds its
 * mailbox UIDs, the deployment's initial mailbox index. Every key in
 * `stepState` must name one of the workflow's top-level agent steps, or an
 * agent step in the inline body of a top-level onTrigger section as
 * `<section>.<step>` (`resolveOnTriggerBodyStepKey`). Every step keeps its
 * state under the deployment's top-level run, `runId`, so the seeds are
 * filed there at their keys.
 *
 * `sources` are the top-level steps' pinned sources, and
 * `referencedDefinitions` are the inline bodies with the sources pinned to
 * their steps (`buildReferencedWorkflowSourcePins`).
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
 * A result that is not `ok` carries the reason the import was refused.
 *
 * The mailbox index gets `uidValidity`, which must be fresh: the new mailbox
 * holds none of the exporting mailbox's messages.
 */
export function buildStepStateSeeds(args: {
  projection: WorkflowProjectionDefinition;
  sources: Readonly<Record<string, readonly InferenceSource[]>>;
  referencedDefinitions: readonly WorkflowProjectionWithSources[];
  runId: string;
  stepState: StepStateImport;
  uidValidity: number;
}): StepStateSeedsResult {
  const imported = Object.entries(args.stepState);
  const unknownStepIds = imported
    .map(([stepId]) => stepId)
    .filter(
      (stepId) =>
        isTopLevelStepPlaceKey(stepId) &&
        !namesTopLevelAgentStep(args.projection, stepId),
    );
  if (unknownStepIds.length > 0) {
    const named = unknownStepIds
      .slice(0, MAX_REPORTED_STEP_IDS)
      .map((stepId) => JSON.stringify(stepId));
    const unnamed = unknownStepIds.length - named.length;
    if (unnamed > 0) named.push(`and ${String(unnamed)} more`);
    return {
      ok: false,
      reason: `Step state names ${named.join(", ")}, which the workflow has no top-level agent step for`,
    };
  }

  const pinnedSources = new Map<string, readonly InferenceSource[]>();
  for (const [key] of imported) {
    if (isTopLevelStepPlaceKey(key)) {
      const stepSources = args.sources[key];
      if (stepSources !== undefined) pinnedSources.set(key, stepSources);
      continue;
    }
    const place = resolveOnTriggerBodyStepKey(args.projection, key);
    if (!place.ok) return place;
    const stepSources = args.referencedDefinitions.find(
      (body) => body.definition.id === place.bodyRef,
    )?.sources[place.stepId];
    if (stepSources !== undefined) pinnedSources.set(key, stepSources);
  }

  const files: Record<string, string> = {};
  for (const [stepId, snapshot] of imported) {
    const targetModel = pinnedSources.get(stepId)?.[0]?.model;
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
        reason: `Step state for ${JSON.stringify(stepId)} is not a conversation its step can continue: ${reportedText(cause instanceof Error ? cause.message : String(cause))}`,
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
      : `was produced by ${JSON.stringify(reportedText(turn.model))}`;
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
    workflowRunRepoIdForAddress(args.deploymentAddress),
    WORKFLOW_RUN_REF,
    {
      files: { ...args.files },
      message: `Import state for ${String(steps)} step(s)`,
    },
  );
}

/**
 * Delete the workflow-run repo `writeStepStateSeeds` wrote to. Only for a
 * deployment that never came to exist, since the repo holds its whole history.
 */
export async function removeStepStateSeeds(args: {
  repoStore: Pick<LocalRepoStore, "removeRepo">;
  deploymentAddress: string;
}): Promise<void> {
  await args.repoStore.removeRepo(
    workflowRunRepoIdForAddress(args.deploymentAddress),
  );
}

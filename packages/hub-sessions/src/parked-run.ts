// Whether a lost worker's committed log is a park the runtime would
// re-attach. The judgment is the workflow runtime's `classifyParkedRun`;
// this module reads the anchor's committed log and frozen projection for it.

import {
  classifyParkedRun,
  type ParkedRunClassification,
  type WorkflowEvent,
} from "@intx/workflow";

import type {
  WorkflowRunEvent,
  WorkflowRunReader,
} from "./workflow-run-reader";
import {
  WORKFLOW_RUN_REF,
  workflowRunRepoIdForAddress,
} from "./workflow-run-kind";

export type { ParkedRunClassification, ParkedRunVerdict } from "@intx/workflow";

export type ParkedRunClassifier = (allocation: {
  readonly anchorRunId: string;
}) => Promise<ParkedRunClassification>;

export function createParkedRunClassifier(deps: {
  readonly runReader: WorkflowRunReader;
  readonly addressForAnchor: (anchorRunId: string) => Promise<string | null>;
  readonly projectionForAnchor: (anchorRunId: string) => Promise<unknown>;
}): ParkedRunClassifier {
  return async (allocation) => {
    try {
      return await classifyAnchor(deps, allocation.anchorRunId);
    } catch (cause) {
      return {
        verdict: "unknown",
        reason: cause instanceof Error ? cause.message : String(cause),
      };
    }
  };
}

async function classifyAnchor(
  deps: {
    readonly runReader: WorkflowRunReader;
    readonly addressForAnchor: (anchorRunId: string) => Promise<string | null>;
    readonly projectionForAnchor: (anchorRunId: string) => Promise<unknown>;
  },
  anchorRunId: string,
): Promise<ParkedRunClassification> {
  const address = await deps.addressForAnchor(anchorRunId);
  if (address === null) {
    return { verdict: "unknown", reason: "anchor run has no address" };
  }
  const repoId = workflowRunRepoIdForAddress(address);
  if (!(await deps.runReader.hasRepository(repoId))) {
    return { verdict: "unknown", reason: "workflow run repository is missing" };
  }
  const projection = await deps.projectionForAnchor(anchorRunId);
  const steps = projectionSteps(projection);
  if (steps === undefined) {
    return { verdict: "unknown", reason: "anchor has no frozen projection" };
  }
  return classifyParkedRun({
    anchorRunId,
    steps,
    readRun: async (runId) => {
      const events = await deps.runReader.readRunEvents(
        repoId,
        WORKFLOW_RUN_REF,
        runId,
      );
      return events.map(toWorkflowEvent);
    },
    mode: "replace",
  });
}

function toWorkflowEvent(event: WorkflowRunEvent): WorkflowEvent {
  const rest: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(event.body)) {
    if (key === "type" || key === "seq") continue;
    rest[key] = value;
  }
  const built: Record<string, unknown> = {
    ...rest,
    kind: event.type,
    seq: event.seq,
  };
  // The reader has already required a string `type`. `resumeFromLog`
  // narrows on `kind`; there is no second validator at this boundary.
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- kind is the on-disk type; the state machine narrows the union
  return built as unknown as WorkflowEvent;
}

function projectionSteps(
  projection: unknown,
): Readonly<Record<string, unknown>> | undefined {
  if (!isRecord(projection)) return undefined;
  const steps = projection["steps"];
  if (!isRecord(steps)) return undefined;
  return steps;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

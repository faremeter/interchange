// Probes that a workflow-run commit reads only the events it adds, plus
// the parent's last event, and that skipping the carried events leaves
// every event-log verdict unchanged.
import { describe, test, expect } from "bun:test";
import {
  workflowRunKindHandler,
  WORKFLOW_RUN_GITIGNORE_PATH,
  WORKFLOW_RUN_RUNS_PREFIX,
} from "./workflow-run-kind";
import { validatePushArgs } from "./repo-store/validate-push.test-helpers";
import type { Principal } from "./repo-store";

const REF = "refs/heads/events";
const HUB: Principal = { kind: "hub" };
const EVENTS = `${WORKFLOW_RUN_RUNS_PREFIX}/run-x/events`;

function eventPath(seq: number): string {
  return `${EVENTS}/${String(seq)}.json`;
}

function eventBody(seq: number, type: string): string {
  return JSON.stringify({ seq, type });
}

// A run holding `count` events, seq 1 through `count`.
function runOf(count: number): Record<string, string> {
  const files: Record<string, string> = { [WORKFLOW_RUN_GITIGNORE_PATH]: "" };
  for (let seq = 1; seq <= count; seq++) {
    files[eventPath(seq)] = eventBody(
      seq,
      seq === 1 ? "RunStarted" : "StepCompleted",
    );
  }
  return files;
}

async function validate(
  files: Record<string, string>,
  priorFiles: Record<string, string>,
) {
  const args = validatePushArgs({
    repoId: { kind: "workflow-run", id: "append-probe" },
    ref: REF,
    principal: HUB,
    files,
    priorFiles,
  });
  const read: string[] = [];
  const result = await workflowRunKindHandler.validatePush({
    ...args,
    readBlob: async (blobPath) => {
      read.push(blobPath);
      return args.readBlob(blobPath);
    },
  });
  return { result, read };
}

describe("workflow-run validation of an event append", () => {
  test("a commit reads the events it adds and the parent's last event", async () => {
    const prior = runOf(200);
    const files = {
      ...prior,
      [eventPath(201)]: eventBody(201, "StepCompleted"),
      [eventPath(202)]: eventBody(202, "StepCompleted"),
    };
    const { result, read } = await validate(files, prior);
    expect(result.ok).toBe(true);
    expect(read.sort()).toEqual(
      [eventPath(200), eventPath(201), eventPath(202)].sort(),
    );
  });

  test("an event after the parent's terminal event is rejected", async () => {
    const prior = {
      ...runOf(3),
      [eventPath(4)]: eventBody(4, "RunCompleted"),
    };
    const files = {
      ...prior,
      [eventPath(5)]: eventBody(5, "StepCompleted"),
    };
    const { result } = await validate(files, prior);
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.reason).toContain(
      "has event at seq 5 after terminal RunCompleted at seq 4",
    );
  });

  test("a terminal event added before the carried events is rejected", async () => {
    const prior: Record<string, string> = { [WORKFLOW_RUN_GITIGNORE_PATH]: "" };
    for (let seq = 2; seq <= 4; seq++) {
      prior[eventPath(seq)] = eventBody(seq, "StepCompleted");
    }
    const files = { ...prior, [eventPath(1)]: eventBody(1, "RunFailed") };
    const { result } = await validate(files, prior);
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.reason).toContain(
      "has event at seq 2 after terminal RunFailed at seq 1",
    );
  });

  test("a newly added terminal event still reports the run as terminal", async () => {
    const prior = runOf(50);
    const terminal = eventBody(51, "RunCompleted");
    const files = { ...prior, [eventPath(51)]: terminal };
    const { result, read } = await validate(files, prior);
    expect(result.ok).toBe(true);
    if (!result.ok) throw new Error("unreachable");
    expect(result.newlyTerminalRuns).toEqual([
      { runId: "run-x", status: "completed", terminalEventJson: terminal },
    ]);
    expect(read).not.toContain(eventPath(1));
  });
});

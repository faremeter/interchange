import { describe, expect, test } from "bun:test";

import type { WorkflowEvent } from "../state-machine/index";
import { classifyParkedRun, type ParkedRunMode } from "./parked-run";

const AT = "2026-10-01T00:00:00.000Z";
const ANCHOR = "run-anchor";
const CHILD = "run-child";

const steps = {
  gate: { kind: "awaitSignal", id: "gate" },
  think: { kind: "step", id: "think" },
  nap: { kind: "sleep", id: "nap" },
  spawn: {
    kind: "childWorkflow",
    id: "spawn",
    definition: {
      inline: {
        steps: {
          work: { kind: "step", id: "work" },
          wait: { kind: "awaitSignal", id: "wait" },
        },
      },
    },
  },
  spawnByRef: {
    kind: "childWorkflow",
    id: "spawnByRef",
    definition: { ref: "child-def" },
  },
};

function runStarted(runId: string, seq: number): WorkflowEvent {
  return {
    kind: "RunStarted",
    seq,
    at: AT,
    runId,
    definitionHash: "hash",
    trigger: { type: "manual", payload: null },
  };
}

function stepStarted(stepId: string, seq: number): WorkflowEvent {
  return {
    kind: "StepStarted",
    seq,
    at: AT,
    stepId,
    attempt: 1,
    input: { ref: "inline:null" },
  };
}

function signalAwaited(stepId: string, seq: number): WorkflowEvent {
  return { kind: "SignalAwaited", seq, at: AT, stepId, signalName: stepId };
}

function classify(
  logs: Readonly<Record<string, readonly WorkflowEvent[]>>,
  mode: ParkedRunMode,
) {
  return classifyParkedRun({
    anchorRunId: ANCHOR,
    steps,
    readRun: async (runId) => logs[runId] ?? [],
    mode,
  });
}

describe("classifyParkedRun in park mode", () => {
  test("a run waiting on a signal is parked", async () => {
    const result = await classify(
      {
        [ANCHOR]: [
          runStarted(ANCHOR, 1),
          stepStarted("gate", 2),
          signalAwaited("gate", 3),
        ],
      },
      "park",
    );
    expect(result.verdict).toBe("parked");
    expect(result.wakeAt).toBeUndefined();
  });

  test("a deployment whose run has not started is parked", async () => {
    expect((await classify({}, "park")).verdict).toBe("parked");
    expect((await classify({}, "replace")).verdict).toBe("not-parked");
  });

  test("an agent step mid-invocation is not parked, though a lost worker beside a park is", async () => {
    const logs = {
      [ANCHOR]: [
        runStarted(ANCHOR, 1),
        stepStarted("gate", 2),
        signalAwaited("gate", 3),
        stepStarted("think", 4),
      ],
    };
    const parked = await classify(logs, "park");
    expect(parked.verdict).toBe("not-parked");
    expect(parked.reason).toBe("step think is mid-invocation");
    expect((await classify(logs, "replace")).verdict).toBe("parked");
  });

  test("a sleep alone is parked until its timer", async () => {
    const fireAt = "2026-10-02T00:00:00.000Z";
    const logs = {
      [ANCHOR]: [
        runStarted(ANCHOR, 1),
        stepStarted("nap", 2),
        {
          kind: "TimerSet",
          seq: 3,
          at: AT,
          timerId: "t1",
          fireAt,
          stepId: "nap",
        } satisfies WorkflowEvent,
      ],
    };
    const parked = await classify(logs, "park");
    expect(parked.verdict).toBe("parked");
    expect(parked.wakeAt).toBe(fireAt);
    expect((await classify(logs, "replace")).verdict).toBe("not-parked");
  });

  test("a child workflow mid-invocation is not parked", async () => {
    const result = await classify(
      {
        [ANCHOR]: [
          runStarted(ANCHOR, 1),
          stepStarted("spawn", 2),
          {
            kind: "ChildSpawned",
            seq: 3,
            at: AT,
            stepId: "spawn",
            childRunId: CHILD,
            childDefinitionRef: "spawn",
          },
        ],
        [CHILD]: [runStarted(CHILD, 1), stepStarted("work", 2)],
      },
      "park",
    );
    expect(result.verdict).toBe("not-parked");
    expect(result.reason).toBe("step work is mid-invocation");
  });

  test("a child workflow waiting on a signal is parked", async () => {
    const logs = {
      [ANCHOR]: [
        runStarted(ANCHOR, 1),
        stepStarted("spawn", 2),
        {
          kind: "ChildSpawned",
          seq: 3,
          at: AT,
          stepId: "spawn",
          childRunId: CHILD,
          childDefinitionRef: "spawn",
        } satisfies WorkflowEvent,
        signalAwaited("spawn", 4),
      ],
      [CHILD]: [
        runStarted(CHILD, 1),
        stepStarted("wait", 2),
        signalAwaited("wait", 3),
      ],
    };
    expect((await classify(logs, "park")).verdict).toBe("parked");
    expect((await classify(logs, "replace")).verdict).toBe("parked");
  });

  test("a live child whose definition is a ref is unknown", async () => {
    const result = await classify(
      {
        [ANCHOR]: [
          runStarted(ANCHOR, 1),
          stepStarted("spawnByRef", 2),
          {
            kind: "ChildSpawned",
            seq: 3,
            at: AT,
            stepId: "spawnByRef",
            childRunId: CHILD,
            childDefinitionRef: "child-def",
          },
        ],
        [CHILD]: [runStarted(CHILD, 1)],
      },
      "park",
    );
    expect(result.verdict).toBe("unknown");
  });

  test("a finished run is not parked", async () => {
    const result = await classify(
      {
        [ANCHOR]: [
          runStarted(ANCHOR, 1),
          { kind: "RunCompleted", seq: 2, at: AT },
        ],
      },
      "park",
    );
    expect(result.verdict).toBe("not-parked");
  });
});

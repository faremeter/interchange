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
          rest: { kind: "sleep", id: "rest" },
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

function stepStarted(
  stepId: string,
  seq: number,
): Extract<WorkflowEvent, { kind: "StepStarted" }> {
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

function signalReceived(
  signalName: string,
  signalId: string,
  seq: number,
): WorkflowEvent {
  return {
    kind: "SignalReceived",
    seq,
    at: AT,
    signalName,
    signalId,
    payload: null,
  };
}

function stepCompleted(stepId: string, seq: number): WorkflowEvent {
  return {
    kind: "StepCompleted",
    seq,
    at: AT,
    stepId,
    attempt: 1,
    output: { ref: "inline:null" },
  };
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

  test("a run that took its trigger and started nothing has input to act on", async () => {
    const result = await classify(
      { [ANCHOR]: [runStarted(ANCHOR, 1)] },
      "park",
    );
    expect(result).toEqual({
      verdict: "not-parked",
      reason: `run ${ANCHOR} has input it has not acted on`,
      pendingInput: true,
    });
  });

  test("a signal that settled a gate with nothing started after it is input to act on", async () => {
    const result = await classify(
      {
        [ANCHOR]: [
          runStarted(ANCHOR, 1),
          stepStarted("gate", 2),
          signalAwaited("gate", 3),
          signalReceived("gate", "sig-1", 4),
          stepCompleted("gate", 5),
        ],
      },
      "park",
    );
    expect(result.pendingInput).toBe(true);
  });

  test("a timer that settled a sleep with nothing started after it is input to act on", async () => {
    const result = await classify(
      {
        [ANCHOR]: [
          runStarted(ANCHOR, 1),
          stepStarted("nap", 2),
          {
            kind: "TimerSet",
            seq: 3,
            at: AT,
            timerId: "t1",
            fireAt: AT,
            stepId: "nap",
          },
          { kind: "TimerFired", seq: 4, at: AT, timerId: "t1" },
          stepCompleted("nap", 5),
        ],
      },
      "park",
    );
    expect(result.pendingInput).toBe(true);
  });

  test("a turn the run handled before waiting again is not input to act on", async () => {
    const result = await classify(
      {
        [ANCHOR]: [
          runStarted(ANCHOR, 1),
          stepStarted("think", 2),
          signalAwaited("think", 3),
          signalReceived("think", "turn-2", 4),
          signalAwaited("think", 5),
        ],
      },
      "park",
    );
    expect(result.verdict).toBe("parked");
    expect(result.pendingInput).toBeUndefined();
  });

  test("a signal queued after a trigger the run has not acted on leaves that input pending", async () => {
    const result = await classify(
      {
        [ANCHOR]: [
          runStarted(ANCHOR, 1),
          signalReceived("later", "sig-early", 2),
        ],
      },
      "park",
    );
    expect(result.pendingInput).toBe(true);
  });

  test("a signal queued after a gate the run has not acted on leaves that input pending", async () => {
    const result = await classify(
      {
        [ANCHOR]: [
          runStarted(ANCHOR, 1),
          stepStarted("gate", 2),
          signalAwaited("gate", 3),
          signalReceived("gate", "sig-1", 4),
          stepCompleted("gate", 5),
          signalReceived("later", "sig-early", 6),
        ],
      },
      "park",
    );
    expect(result.pendingInput).toBe(true);
  });

  test("a step on another branch finishing does not act on a gate's input", async () => {
    const result = await classify(
      {
        [ANCHOR]: [
          runStarted(ANCHOR, 1),
          stepStarted("gate", 2),
          stepStarted("think", 3),
          signalAwaited("gate", 4),
          signalReceived("gate", "sig-1", 5),
          stepCompleted("gate", 6),
          stepCompleted("think", 7),
        ],
      },
      "park",
    );
    expect(result.pendingInput).toBe(true);
  });

  test("a step that starts before a settled gate lands does not act on the gate's input", async () => {
    const result = await classify(
      {
        [ANCHOR]: [
          runStarted(ANCHOR, 1),
          stepStarted("gate", 2),
          signalAwaited("gate", 3),
          signalReceived("gate", "sig-1", 4),
          stepStarted("think", 5),
          stepCompleted("gate", 6),
          stepCompleted("think", 7),
        ],
      },
      "park",
    );
    expect(result.pendingInput).toBe(true);
  });

  test("a branch skipped on another branch does not act on a gate's input", async () => {
    const result = await classify(
      {
        [ANCHOR]: [
          runStarted(ANCHOR, 1),
          stepStarted("gate", 2),
          signalAwaited("gate", 3),
          signalReceived("gate", "sig-1", 4),
          stepCompleted("gate", 5),
          { ...stepStarted("think", 6), skip: true },
          stepCompleted("think", 7),
        ],
      },
      "park",
    );
    expect(result.pendingInput).toBe(true);
  });

  test("a retried step finishing does not act on a gate's input", async () => {
    const result = await classify(
      {
        [ANCHOR]: [
          runStarted(ANCHOR, 1),
          stepStarted("think", 2),
          stepStarted("gate", 3),
          signalAwaited("gate", 4),
          {
            kind: "StepFailed",
            seq: 5,
            at: AT,
            stepId: "think",
            attempt: 1,
            error: { message: "flaky" },
            retriesExhausted: false,
          },
          {
            kind: "TimerSet",
            seq: 6,
            at: AT,
            timerId: "retry-1",
            fireAt: AT,
            stepId: "think",
          },
          {
            kind: "AttemptScheduled",
            seq: 7,
            at: AT,
            stepId: "think",
            nextAttempt: 2,
            timerId: "retry-1",
            fireAt: AT,
          },
          signalReceived("gate", "sig-1", 8),
          stepCompleted("gate", 9),
          { kind: "TimerFired", seq: 10, at: AT, timerId: "retry-1" },
          {
            kind: "StepCompleted",
            seq: 11,
            at: AT,
            stepId: "think",
            attempt: 2,
            output: { ref: "inline:null" },
          },
        ],
      },
      "park",
    );
    expect(result.pendingInput).toBe(true);
  });

  test("a container running on another branch does not hide a gate's input", async () => {
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
          stepStarted("gate", 4),
          signalAwaited("gate", 5),
          signalReceived("gate", "sig-1", 6),
          stepCompleted("gate", 7),
        ],
        [CHILD]: [
          runStarted(CHILD, 1),
          stepStarted("rest", 2),
          {
            kind: "TimerSet",
            seq: 3,
            at: AT,
            timerId: "t1",
            fireAt: "2026-10-02T00:00:00.000Z",
            stepId: "rest",
          },
        ],
      },
      "park",
    );
    expect(result.pendingInput).toBe(true);
  });

  test("a repeat of a signal the run already received is not new input", async () => {
    const result = await classify(
      {
        [ANCHOR]: [
          runStarted(ANCHOR, 1),
          stepStarted("gate", 2),
          signalAwaited("gate", 3),
          signalReceived("gate", "sig-1", 4),
          stepCompleted("gate", 5),
          stepStarted("think", 6),
          stepCompleted("think", 7),
          signalReceived("gate", "sig-1", 8),
        ],
      },
      "park",
    );
    expect(result.verdict).toBe("parked");
  });

  test("a signal queued before its await, which the await took, is input to act on", async () => {
    const result = await classify(
      {
        [ANCHOR]: [
          runStarted(ANCHOR, 1),
          signalReceived("gate", "sig-early", 2),
          stepStarted("gate", 3),
          signalAwaited("gate", 4),
          stepCompleted("gate", 5),
        ],
      },
      "park",
    );
    expect(result.pendingInput).toBe(true);
  });

  test("a signal the run only queued is not input to act on", async () => {
    const result = await classify(
      {
        [ANCHOR]: [
          runStarted(ANCHOR, 1),
          stepStarted("gate", 2),
          signalAwaited("gate", 3),
          signalReceived("later", "sig-early", 4),
        ],
      },
      "park",
    );
    expect(result.verdict).toBe("parked");
  });

  test("a nested run's own start is its parent step's progress, not input", async () => {
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
        [CHILD]: [runStarted(CHILD, 1)],
      },
      "park",
    );
    expect(result.pendingInput).toBeUndefined();
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

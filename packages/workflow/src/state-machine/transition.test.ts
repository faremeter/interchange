import { describe, test, expect } from "bun:test";

import {
  applyEvent,
  emptyState,
  resumeFromLog,
  TransitionError,
  type RunState,
  type WorkflowEvent,
} from "./index";

const RUN = "run-1";
const T = "2026-06-09T00:00:00.000Z";

function fresh(): RunState {
  return emptyState(RUN);
}

function expectThrowsAt(events: WorkflowEvent[], match: RegExp): void {
  let state = fresh();
  let thrown: unknown;
  try {
    for (const e of events) {
      state = applyEvent(state, e);
    }
  } catch (cause) {
    thrown = cause;
  }
  expect(thrown).toBeInstanceOf(TransitionError);
  if (thrown instanceof TransitionError) {
    expect(thrown.message).toMatch(match);
  }
}

function startRun(opts?: { consumedMessageId?: string }): WorkflowEvent {
  const event: WorkflowEvent = {
    kind: "RunStarted",
    seq: 1,
    at: T,
    runId: RUN,
    definitionHash: "h",
    trigger: { type: "manual", payload: null },
    ...(opts?.consumedMessageId !== undefined
      ? { consumedMessageId: opts.consumedMessageId }
      : {}),
  };
  return event;
}

describe("applyEvent: sequence monotonicity", () => {
  test("rejects seq < 1", () => {
    expectThrowsAt(
      [
        {
          kind: "RunStarted",
          seq: 0,
          at: T,
          runId: RUN,
          definitionHash: "h",
          trigger: { type: "manual", payload: null },
        },
      ],
      /sequence must be >= 1/,
    );
  });

  test("rejects non-monotonic seq", () => {
    expectThrowsAt(
      [
        startRun(),
        {
          kind: "StepStarted",
          seq: 1,
          at: T,
          stepId: "a",
          attempt: 1,
          input: { ref: "i" },
        },
      ],
      /non-monotonic/,
    );
  });

  test("accepts strictly increasing seq", () => {
    let state = fresh();
    state = applyEvent(state, startRun());
    state = applyEvent(state, {
      kind: "StepStarted",
      seq: 2,
      at: T,
      stepId: "a",
      attempt: 1,
      input: { ref: "i" },
    });
    expect(state.lastSeq).toBe(2);
  });
});

describe("applyEvent: RunStarted", () => {
  test("transitions pending to running", () => {
    const state = applyEvent(fresh(), startRun());
    expect(state.phase).toBe("running");
    expect(state.definitionHash).toBe("h");
  });

  test("rejects re-issue with previously consumed message-id", () => {
    let state = fresh();
    state = applyEvent(state, startRun({ consumedMessageId: "msg-1" }));
    expect(state.consumedMessageIds.has("msg-1")).toBe(true);
    // Drive run to completion so we can verify the dedup invariant against a
    // hypothetical re-issue against a fresh state with the consumed id preloaded.
    const preloaded: RunState = {
      ...emptyState(RUN),
      consumedMessageIds: new Set(["msg-1"]),
    };
    expect(() =>
      applyEvent(preloaded, startRun({ consumedMessageId: "msg-1" })),
    ).toThrow(/consumed message-id msg-1/);
  });

  test("consumedMessageId dedup is independent across distinct message-ids", () => {
    // Two distinct message-ids consumed against two separate runs are
    // both retained on each run's consumedMessageIds set. The invariant
    // covers the per-run scope; the per-address FIFO serialization
    // invariant is enforced by the queue substrate, not the state
    // machine (see events.ts on RunStarted.consumedMessageId).
    const runA = applyEvent(emptyState("run-A"), {
      kind: "RunStarted",
      seq: 1,
      at: T,
      runId: "run-A",
      definitionHash: "h",
      trigger: { type: "mail", payload: null },
      consumedMessageId: "msg-A",
    });
    const runB = applyEvent(emptyState("run-B"), {
      kind: "RunStarted",
      seq: 1,
      at: T,
      runId: "run-B",
      definitionHash: "h",
      trigger: { type: "mail", payload: null },
      consumedMessageId: "msg-B",
    });
    expect(runA.consumedMessageIds.has("msg-A")).toBe(true);
    expect(runA.consumedMessageIds.has("msg-B")).toBe(false);
    expect(runB.consumedMessageIds.has("msg-B")).toBe(true);
    expect(runB.consumedMessageIds.has("msg-A")).toBe(false);
  });

  test("rejects RunStarted in non-pending phase", () => {
    expectThrowsAt([startRun(), startRun()], /non-monotonic/);
    // Strict phase check: bump seq so the seq guard does not fire.
    expectThrowsAt(
      [
        startRun(),
        {
          ...startRun(),
          seq: 2,
        },
      ],
      /RunStarted in phase running/,
    );
  });
});

describe("applyEvent: StepStarted", () => {
  test("places step in-flight", () => {
    let state = fresh();
    state = applyEvent(state, startRun());
    state = applyEvent(state, {
      kind: "StepStarted",
      seq: 2,
      at: T,
      stepId: "a",
      attempt: 1,
      input: { ref: "i" },
    });
    expect(state.steps.get("a")?.phase).toBe("in-flight");
  });

  test("rejects re-issued StepStarted against an existing step", () => {
    expectThrowsAt(
      [
        startRun(),
        {
          kind: "StepStarted",
          seq: 2,
          at: T,
          stepId: "a",
          attempt: 1,
          input: { ref: "i" },
        },
        {
          kind: "StepStarted",
          seq: 3,
          at: T,
          stepId: "a",
          attempt: 2,
          input: { ref: "i" },
        },
      ],
      /StepStarted for step a which already has state/,
    );
  });

  test("rejected before RunStarted", () => {
    expectThrowsAt(
      [
        {
          kind: "StepStarted",
          seq: 1,
          at: T,
          stepId: "a",
          attempt: 1,
          input: { ref: "i" },
        },
      ],
      /expected phase running, was pending/,
    );
  });
});

describe("applyEvent: StepCompleted / StepFailed", () => {
  test("StepCompleted moves step to completed", () => {
    let state = fresh();
    state = applyEvent(state, startRun());
    state = applyEvent(state, {
      kind: "StepStarted",
      seq: 2,
      at: T,
      stepId: "a",
      attempt: 1,
      input: { ref: "i" },
    });
    state = applyEvent(state, {
      kind: "StepCompleted",
      seq: 3,
      at: T,
      stepId: "a",
      attempt: 1,
      output: { ref: "o" },
    });
    expect(state.steps.get("a")?.phase).toBe("completed");
    expect(state.steps.get("a")?.outputRef).toBe("o");
  });

  test("StepCompleted on an already-terminal step throws", () => {
    expectThrowsAt(
      [
        startRun(),
        {
          kind: "StepStarted",
          seq: 2,
          at: T,
          stepId: "a",
          attempt: 1,
          input: { ref: "i" },
        },
        {
          kind: "StepCompleted",
          seq: 3,
          at: T,
          stepId: "a",
          attempt: 1,
          output: { ref: "o" },
        },
        {
          kind: "StepCompleted",
          seq: 4,
          at: T,
          stepId: "a",
          attempt: 1,
          output: { ref: "o2" },
        },
      ],
      /StepCompleted for step already in terminal phase completed/,
    );
  });

  test("StepFailed records the error", () => {
    let state = fresh();
    state = applyEvent(state, startRun());
    state = applyEvent(state, {
      kind: "StepStarted",
      seq: 2,
      at: T,
      stepId: "a",
      attempt: 1,
      input: { ref: "i" },
    });
    state = applyEvent(state, {
      kind: "StepFailed",
      seq: 3,
      at: T,
      stepId: "a",
      attempt: 1,
      error: { message: "boom" },
      retriesExhausted: true,
    });
    expect(state.steps.get("a")?.phase).toBe("failed");
    expect(state.steps.get("a")?.lastError?.message).toBe("boom");
  });
});

describe("applyEvent: AttemptScheduled and TimerSet pairing", () => {
  test("AttemptScheduled without prior TimerSet throws", () => {
    expectThrowsAt(
      [
        startRun(),
        {
          kind: "StepStarted",
          seq: 2,
          at: T,
          stepId: "a",
          attempt: 1,
          input: { ref: "i" },
        },
        {
          kind: "AttemptScheduled",
          seq: 3,
          at: T,
          stepId: "a",
          nextAttempt: 2,
          timerId: "t-1",
          fireAt: T,
        },
      ],
      /references unknown timer t-1/,
    );
  });

  test("AttemptScheduled with mismatched stepId throws", () => {
    expectThrowsAt(
      [
        startRun(),
        {
          kind: "StepStarted",
          seq: 2,
          at: T,
          stepId: "a",
          attempt: 1,
          input: { ref: "i" },
        },
        {
          kind: "TimerSet",
          seq: 3,
          at: T,
          timerId: "t-1",
          fireAt: T,
          stepId: "b",
        },
        {
          kind: "AttemptScheduled",
          seq: 4,
          at: T,
          stepId: "a",
          nextAttempt: 2,
          timerId: "t-1",
          fireAt: T,
        },
      ],
      /timer t-1 is bound to step b, not a/,
    );
  });

  test("AttemptScheduled with non-increasing attempt throws", () => {
    expectThrowsAt(
      [
        startRun(),
        {
          kind: "StepStarted",
          seq: 2,
          at: T,
          stepId: "a",
          attempt: 2,
          input: { ref: "i" },
        },
        {
          kind: "TimerSet",
          seq: 3,
          at: T,
          timerId: "t-1",
          fireAt: T,
          stepId: "a",
        },
        {
          kind: "AttemptScheduled",
          seq: 4,
          at: T,
          stepId: "a",
          nextAttempt: 2,
          timerId: "t-1",
          fireAt: T,
        },
      ],
      /non-increasing attempt/,
    );
  });

  test("AttemptScheduled places step awaiting-timer", () => {
    let state = fresh();
    state = applyEvent(state, startRun());
    state = applyEvent(state, {
      kind: "StepStarted",
      seq: 2,
      at: T,
      stepId: "a",
      attempt: 1,
      input: { ref: "i" },
    });
    state = applyEvent(state, {
      kind: "TimerSet",
      seq: 3,
      at: T,
      timerId: "t-1",
      fireAt: T,
      stepId: "a",
    });
    state = applyEvent(state, {
      kind: "AttemptScheduled",
      seq: 4,
      at: T,
      stepId: "a",
      nextAttempt: 2,
      timerId: "t-1",
      fireAt: T,
    });
    expect(state.steps.get("a")?.phase).toBe("awaiting-timer");
    expect(state.steps.get("a")?.currentAttempt).toBe(2);
  });
});

describe("applyEvent: signal delivery", () => {
  test("FIFO single-consumer dispatches to first awaiter", () => {
    let state = fresh();
    state = applyEvent(state, startRun());
    state = applyEvent(state, {
      kind: "StepStarted",
      seq: 2,
      at: T,
      stepId: "a",
      attempt: 1,
      input: { ref: "i" },
    });
    state = applyEvent(state, {
      kind: "StepStarted",
      seq: 3,
      at: T,
      stepId: "b",
      attempt: 1,
      input: { ref: "i" },
    });
    state = applyEvent(state, {
      kind: "SignalAwaited",
      seq: 4,
      at: T,
      stepId: "a",
      signalName: "approve",
    });
    state = applyEvent(state, {
      kind: "SignalAwaited",
      seq: 5,
      at: T,
      stepId: "b",
      signalName: "approve",
    });
    state = applyEvent(state, {
      kind: "SignalReceived",
      seq: 6,
      at: T,
      signalName: "approve",
      signalId: "s-1",
      payload: 1,
    });
    expect(state.steps.get("a")?.phase).toBe("in-flight");
    expect(state.steps.get("b")?.phase).toBe("awaiting-signal");
  });

  test("pre-await queues signal under its name", () => {
    let state = fresh();
    state = applyEvent(state, startRun());
    state = applyEvent(state, {
      kind: "StepStarted",
      seq: 2,
      at: T,
      stepId: "a",
      attempt: 1,
      input: { ref: "i" },
    });
    state = applyEvent(state, {
      kind: "SignalReceived",
      seq: 3,
      at: T,
      signalName: "approve",
      signalId: "s-1",
      payload: "y",
    });
    expect(state.unconsumedSignals.get("approve")?.length).toBe(1);
    state = applyEvent(state, {
      kind: "SignalAwaited",
      seq: 4,
      at: T,
      stepId: "a",
      signalName: "approve",
    });
    expect(state.steps.get("a")?.phase).toBe("in-flight");
    expect(state.unconsumedSignals.has("approve")).toBe(false);
  });

  test("duplicate signalId is idempotent no-op", () => {
    let state = fresh();
    state = applyEvent(state, startRun());
    state = applyEvent(state, {
      kind: "StepStarted",
      seq: 2,
      at: T,
      stepId: "a",
      attempt: 1,
      input: { ref: "i" },
    });
    state = applyEvent(state, {
      kind: "SignalAwaited",
      seq: 3,
      at: T,
      stepId: "a",
      signalName: "approve",
    });
    state = applyEvent(state, {
      kind: "SignalReceived",
      seq: 4,
      at: T,
      signalName: "approve",
      signalId: "s-1",
      payload: "y",
    });
    expect(state.steps.get("a")?.phase).toBe("in-flight");
    state = applyEvent(state, {
      kind: "SignalReceived",
      seq: 5,
      at: T,
      signalName: "approve",
      signalId: "s-1",
      payload: "y",
    });
    // No new queueing, no phase change downstream.
    expect(state.unconsumedSignals.has("approve")).toBe(false);
  });
});

describe("applyEvent: cancellation", () => {
  test("CancelRequested transitions running to cancelling", () => {
    let state = fresh();
    state = applyEvent(state, startRun());
    state = applyEvent(state, {
      kind: "CancelRequested",
      seq: 2,
      at: T,
      reason: "op",
      origin: "self",
    });
    expect(state.phase).toBe("cancelling");
    expect(state.cancelReason).toBe("op");
  });

  test("CancelRequested rejects unknown origin", () => {
    expectThrowsAt(
      [
        startRun(),
        {
          kind: "CancelRequested",
          seq: 2,
          at: T,
          reason: "op",
          // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- exercising shape validation
          origin: "bogus" as "self",
        },
      ],
      /unknown origin bogus/,
    );
  });

  test("RunFailed rejected once cancelling (cancellation wins)", () => {
    expectThrowsAt(
      [
        startRun(),
        {
          kind: "CancelRequested",
          seq: 2,
          at: T,
          reason: "op",
          origin: "self",
        },
        {
          kind: "RunFailed",
          seq: 3,
          at: T,
          error: { message: "x" },
        },
      ],
      /RunFailed in phase cancelling/,
    );
  });

  test("CancelPropagated leaves already-terminal steps intact", () => {
    let state = fresh();
    state = applyEvent(state, startRun());
    state = applyEvent(state, {
      kind: "StepStarted",
      seq: 2,
      at: T,
      stepId: "a",
      attempt: 1,
      input: { ref: "i" },
    });
    state = applyEvent(state, {
      kind: "StepCompleted",
      seq: 3,
      at: T,
      stepId: "a",
      attempt: 1,
      output: { ref: "o" },
    });
    state = applyEvent(state, {
      kind: "CancelRequested",
      seq: 4,
      at: T,
      reason: "op",
      origin: "self",
    });
    state = applyEvent(state, {
      kind: "CancelPropagated",
      seq: 5,
      at: T,
      stepId: "a",
    });
    expect(state.steps.get("a")?.phase).toBe("completed");
  });

  test("CancelPropagated cancels an in-flight step", () => {
    let state = fresh();
    state = applyEvent(state, startRun());
    state = applyEvent(state, {
      kind: "StepStarted",
      seq: 2,
      at: T,
      stepId: "a",
      attempt: 1,
      input: { ref: "i" },
    });
    state = applyEvent(state, {
      kind: "CancelRequested",
      seq: 3,
      at: T,
      reason: "op",
      origin: "supervisor-drain",
    });
    state = applyEvent(state, {
      kind: "CancelPropagated",
      seq: 4,
      at: T,
      stepId: "a",
    });
    expect(state.steps.get("a")?.phase).toBe("cancelled");
  });
});

describe("applyEvent: children", () => {
  test("ChildSpawned tracks the child", () => {
    let state = fresh();
    state = applyEvent(state, startRun());
    state = applyEvent(state, {
      kind: "StepStarted",
      seq: 2,
      at: T,
      stepId: "a",
      attempt: 1,
      input: { ref: "i" },
    });
    state = applyEvent(state, {
      kind: "ChildSpawned",
      seq: 3,
      at: T,
      stepId: "a",
      childRunId: "child-1",
      childDefinitionRef: "h2",
    });
    expect(state.children.get("child-1")?.spawnedBy).toBe("a");
  });

  test("ChildCancelRequested marks the child cancel-requested", () => {
    let state = fresh();
    state = applyEvent(state, startRun());
    state = applyEvent(state, {
      kind: "StepStarted",
      seq: 2,
      at: T,
      stepId: "a",
      attempt: 1,
      input: { ref: "i" },
    });
    state = applyEvent(state, {
      kind: "ChildSpawned",
      seq: 3,
      at: T,
      stepId: "a",
      childRunId: "child-1",
      childDefinitionRef: "h2",
    });
    state = applyEvent(state, {
      kind: "ChildCancelRequested",
      seq: 4,
      at: T,
      childRunId: "child-1",
    });
    expect(state.children.get("child-1")?.cancelRequested).toBe(true);
  });

  test("ChildCompleted records terminal status idempotently", () => {
    let state = fresh();
    state = applyEvent(state, startRun());
    state = applyEvent(state, {
      kind: "StepStarted",
      seq: 2,
      at: T,
      stepId: "a",
      attempt: 1,
      input: { ref: "i" },
    });
    state = applyEvent(state, {
      kind: "ChildSpawned",
      seq: 3,
      at: T,
      stepId: "a",
      childRunId: "child-1",
      childDefinitionRef: "h2",
    });
    state = applyEvent(state, {
      kind: "ChildCompleted",
      seq: 4,
      at: T,
      childRunId: "child-1",
      terminalStatus: "completed",
    });
    state = applyEvent(state, {
      kind: "ChildCompleted",
      seq: 5,
      at: T,
      childRunId: "child-1",
      terminalStatus: "failed",
    });
    expect(state.children.get("child-1")?.terminalStatus).toBe("completed");
  });

  test("rejects a duplicate ChildSpawned for the same child id", () => {
    expectThrowsAt(
      [
        startRun(),
        {
          kind: "StepStarted",
          seq: 2,
          at: T,
          stepId: "a",
          attempt: 1,
          input: { ref: "i" },
        },
        {
          kind: "ChildSpawned",
          seq: 3,
          at: T,
          stepId: "a",
          childRunId: "child-1",
          childDefinitionRef: "h2",
        },
        {
          kind: "ChildSpawned",
          seq: 4,
          at: T,
          stepId: "a",
          childRunId: "child-1",
          childDefinitionRef: "h2",
        },
      ],
      /already has state/,
    );
  });

  test("a re-emitted ChildSpawned cannot clobber a child's terminal state", () => {
    // Without the guard the second spawn would reset terminalStatus to
    // undefined, resurrecting a finished child into the cancel cascade.
    // This is the spike's R4 regression, encoded against the reducer.
    let state = fresh();
    state = applyEvent(state, startRun());
    state = applyEvent(state, {
      kind: "StepStarted",
      seq: 2,
      at: T,
      stepId: "a",
      attempt: 1,
      input: { ref: "i" },
    });
    state = applyEvent(state, {
      kind: "ChildSpawned",
      seq: 3,
      at: T,
      stepId: "a",
      childRunId: "child-1",
      childDefinitionRef: "h2",
    });
    state = applyEvent(state, {
      kind: "ChildCompleted",
      seq: 4,
      at: T,
      childRunId: "child-1",
      terminalStatus: "completed",
    });
    const settled = state;
    let thrown: unknown;
    try {
      applyEvent(settled, {
        kind: "ChildSpawned",
        seq: 5,
        at: T,
        stepId: "a",
        childRunId: "child-1",
        childDefinitionRef: "h2",
      });
    } catch (cause) {
      thrown = cause;
    }
    expect(thrown).toBeInstanceOf(TransitionError);
    if (thrown instanceof TransitionError) {
      expect(thrown.code).toBe("child-already-spawned");
    }
    expect(settled.children.get("child-1")?.terminalStatus).toBe("completed");
  });
});

describe("applyEvent: terminal phase locks", () => {
  test("no events accepted after RunCompleted", () => {
    expectThrowsAt(
      [
        startRun(),
        { kind: "RunCompleted", seq: 2, at: T },
        {
          kind: "StepStarted",
          seq: 3,
          at: T,
          stepId: "a",
          attempt: 1,
          input: { ref: "i" },
        },
      ],
      /event after terminal phase completed/,
    );
  });

  test("RunCancelled requires cancelling phase", () => {
    expectThrowsAt(
      [startRun(), { kind: "RunCancelled", seq: 2, at: T }],
      /RunCancelled in phase running/,
    );
  });
});

describe("resumeFromLog", () => {
  test("replays a full log to the same terminal state", () => {
    const log: WorkflowEvent[] = [
      startRun(),
      {
        kind: "StepStarted",
        seq: 2,
        at: T,
        stepId: "a",
        attempt: 1,
        input: { ref: "i" },
      },
      {
        kind: "StepCompleted",
        seq: 3,
        at: T,
        stepId: "a",
        attempt: 1,
        output: { ref: "o" },
      },
      { kind: "RunCompleted", seq: 4, at: T },
    ];
    const state = resumeFromLog(RUN, log);
    expect(state.phase).toBe("completed");
    expect(state.steps.get("a")?.outputRef).toBe("o");
  });

  test("partial cancellation leaves state in cancelling phase", () => {
    const log: WorkflowEvent[] = [
      startRun(),
      {
        kind: "StepStarted",
        seq: 2,
        at: T,
        stepId: "a",
        attempt: 1,
        input: { ref: "i" },
      },
      {
        kind: "CancelRequested",
        seq: 3,
        at: T,
        reason: "op",
        origin: "self",
      },
    ];
    const state = resumeFromLog(RUN, log);
    expect(state.phase).toBe("cancelling");
    expect(state.steps.get("a")?.phase).toBe("in-flight");
  });

  test("signal/timer race converges on resume (timer wins)", () => {
    const log: WorkflowEvent[] = [
      startRun(),
      {
        kind: "StepStarted",
        seq: 2,
        at: T,
        stepId: "a",
        attempt: 1,
        input: { ref: "i" },
      },
      {
        kind: "TimerSet",
        seq: 3,
        at: T,
        timerId: "t-1",
        fireAt: T,
        stepId: "a",
      },
      {
        kind: "SignalAwaited",
        seq: 4,
        at: T,
        stepId: "a",
        signalName: "approve",
        timeoutAt: T,
      },
      // Timer wins the race; the subsequent SignalReceived is a no-op
      // because the step is already in-flight.
      { kind: "TimerFired", seq: 5, at: T, timerId: "t-1" },
      {
        kind: "SignalReceived",
        seq: 6,
        at: T,
        signalName: "approve",
        signalId: "s-1",
        payload: null,
      },
    ];
    const state = resumeFromLog(RUN, log);
    expect(state.steps.get("a")?.phase).toBe("in-flight");
  });
});

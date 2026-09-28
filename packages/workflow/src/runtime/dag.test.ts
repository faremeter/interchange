// nextSchedulable's resume carve-out for an onTrigger section, and its hold
// on an onFailure handler.
//
// A resumed onTrigger container is left non-terminal in the run state:
// awaiting-signal while parked between events, or in-flight while a body
// run is mid-flight. nextSchedulable must RE-OFFER it so runOnTrigger can
// re-derive its position from the log; without the carve-out the section
// reads as a generic non-terminal step, gets skipped, and the run stalls.
//
// An onFailure handler names its unit in `after`, but only a unit that
// routed to it may start it. A unit that completed, or failed without
// routing, leaves the handler for its branch's skip.

import { describe, test, expect } from "bun:test";

import { defineAgent, type AgentDefinition, type BaseEnv } from "@intx/agent";

import {
  defineWorkflow,
  onTrigger,
  step,
  type WorkflowDefinition,
} from "../definition/index";
import {
  resumeFromLog,
  type RunState,
  type WorkflowEvent,
} from "../state-machine/index";
import { nextSchedulable } from "./dag";

function makeAgent(id: string): AgentDefinition<BaseEnv> {
  return defineAgent({
    id,
    systemPrompt: "you are " + id,
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "fake", model: "fake" }] },
  });
}

const body: WorkflowDefinition = defineWorkflow({
  id: "body",
  trigger: { type: "manual" },
  steps: { s: step({ agent: makeAgent("s") }) },
});

const wf: WorkflowDefinition = defineWorkflow({
  id: "wf",
  steps: {
    section: onTrigger({ on: { type: "mail", to: "x@y.example" }, body }),
  },
});

const at = new Date().toISOString();

function runStarted(runId: string): WorkflowEvent {
  return {
    kind: "RunStarted",
    seq: 1,
    at,
    runId,
    definitionHash: "x",
    trigger: { type: "mail", payload: undefined },
  };
}

function sectionStarted(): WorkflowEvent {
  return {
    kind: "StepStarted",
    seq: 2,
    at,
    stepId: "section",
    attempt: 1,
    input: { ref: "inline:null" },
  };
}

describe("nextSchedulable onTrigger resume carve-out", () => {
  test("re-offers a resumed section parked awaiting-signal", () => {
    const state = resumeFromLog("r", [
      runStarted("r"),
      sectionStarted(),
      {
        kind: "SignalAwaited",
        seq: 3,
        at,
        stepId: "section",
        signalName: "sig",
        parkKind: "input",
      },
    ]);
    expect(state.steps.get("section")?.phase).toBe("awaiting-signal");
    const offered = nextSchedulable(wf, state, new Set());
    expect(offered.map((p) => p.id)).toContain("section");
  });

  test("re-offers a resumed section in-flight mid body run", () => {
    const state = resumeFromLog("r", [runStarted("r"), sectionStarted()]);
    expect(state.steps.get("section")?.phase).toBe("in-flight");
    const offered = nextSchedulable(wf, state, new Set());
    expect(offered.map((p) => p.id)).toContain("section");
  });
});

const routing: WorkflowDefinition = defineWorkflow({
  id: "routing",
  trigger: { type: "manual" },
  steps: {
    unit: step({ agent: makeAgent("unit"), onFailure: "rescue" }),
    rescue: step({ agent: makeAgent("rescue"), after: ["unit"] }),
    normal: step({ agent: makeAgent("normal"), after: ["unit"] }),
  },
});

function unitSettledBy(settle: WorkflowEvent): RunState {
  return resumeFromLog("r", [
    runStarted("r"),
    {
      kind: "StepStarted",
      seq: 2,
      at,
      stepId: "unit",
      attempt: 1,
      input: { ref: "inline:null" },
    },
    settle,
  ]);
}

function unitFailed(routedTo?: string): WorkflowEvent {
  return {
    kind: "StepFailed",
    seq: 3,
    at,
    stepId: "unit",
    attempt: 1,
    error: { message: "boom" },
    retriesExhausted: true,
    ...(routedTo !== undefined ? { routedTo } : {}),
  };
}

function offeredIds(state: RunState): string[] {
  return nextSchedulable(routing, state, new Set()).map((p) => p.id);
}

describe("nextSchedulable onFailure handler", () => {
  test("offers the handler once its unit routes to it", () => {
    expect(offeredIds(unitSettledBy(unitFailed("rescue")))).toContain("rescue");
  });

  test("holds the handler of a unit whose failure did not route", () => {
    expect(offeredIds(unitSettledBy(unitFailed()))).toEqual(["normal"]);
  });

  test("holds the handler of a completed unit whose branch is not skipped yet", () => {
    const completed: WorkflowEvent = {
      kind: "StepCompleted",
      seq: 3,
      at,
      stepId: "unit",
      attempt: 1,
      output: { ref: "inline:null" },
    };
    expect(offeredIds(unitSettledBy(completed))).toEqual(["normal"]);
  });
});

// Pins which steps `collectAgentBearingStepIds` reports as able to invoke
// inference.
//
// Every step of a frozen projection carries a pinned inference source, because
// the wire shape requires one per step. Only some of those steps can issue a
// request through it. A consumer that hands out something a step is entitled to
// -- a decrypted credential, notably -- must tell the two apart from the
// definition rather than from the pinned map, which cannot distinguish them.
//
// The loop case is the one a reader doubts: the container is excluded while the
// agent inside its body is included, because a loop body's steps land in the
// same flat namespace and are classified on their own visit.

import { describe, test, expect } from "bun:test";

import { type } from "arktype";
import { defineAgent } from "@intx/agent";
import { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import {
  action,
  awaitSignal,
  defineWorkflow,
  gate,
  loop,
  map,
  onTrigger,
  projectLiveToInert,
  step,
} from "@intx/workflow";

import { collectAgentBearingStepIds } from "./orchestrator";

function agent(id: string) {
  return defineAgent({
    id,
    systemPrompt: "agent-bearing test agent",
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "anthropic", model: "worker-model" }] },
  });
}

function projectionOf(def: Parameters<typeof projectLiveToInert>[0]) {
  const projection = WorkflowProjectionDefinition(
    JSON.parse(JSON.stringify(projectLiveToInert(def))),
  );
  if (projection instanceof type.errors) {
    throw new Error(`projection failed validation: ${projection.summary}`);
  }
  return projection;
}

const CONTEXT = "collectAgentBearingStepIds test: ";

describe("collectAgentBearingStepIds", () => {
  test("reports the agent step and none of the deterministic primitives", () => {
    const def = defineWorkflow({
      id: "wf-mixed",
      trigger: { type: "manual" },
      steps: {
        gather: action({ handler: "gather" }),
        think: step({ agent: agent("thinker"), after: ["gather"] }),
        enough: gate({
          when: { from: "steps.think.output.ok" },
          then: "hold",
          else: "persist",
          after: ["think"],
        }),
        hold: awaitSignal({
          name: "decision",
          timeout: 1000,
          onTimeout: "persist",
          after: ["enough"],
        }),
        persist: action({ handler: "persist", after: ["hold"] }),
      },
    });

    const agentSteps = collectAgentBearingStepIds({
      definition: projectionOf(def),
      context: CONTEXT,
    });

    expect([...agentSteps].sort()).toEqual(["think"]);
  });

  test("excludes the loop container but reports the agent inside its body", () => {
    const body = defineWorkflow({
      id: "loop-body",
      trigger: { type: "manual" },
      steps: { turn: step({ agent: agent("turn-agent") }) },
    });
    const def = defineWorkflow({
      id: "wf-loop",
      trigger: { type: "manual" },
      steps: {
        rework: loop({
          body,
          while: "w",
          carry: "c",
          input: { literal: 0 },
          maxIterations: 3,
          onExhausted: "done",
        }),
        done: action({ handler: "done", after: ["rework"] }),
      },
    });

    const agentSteps = collectAgentBearingStepIds({
      definition: projectionOf(def),
      context: CONTEXT,
    });

    // The container itself never issues a request; the body step does.
    expect(agentSteps.has("rework")).toBe(false);
    expect(agentSteps.has("done")).toBe(false);
    expect(agentSteps.has("turn")).toBe(true);
  });

  test("reports a map over an agent, keyed by the map step's own id", () => {
    // The fan-out instances resolve their source through the map step's id, so
    // excluding it would starve every instance of the credential.
    const def = defineWorkflow({
      id: "wf-map",
      trigger: { type: "manual" },
      steps: {
        seed: action({ handler: "seed" }),
        fan: map({
          over: { from: "steps.seed.output.items" },
          step: step({ agent: agent("fan-agent") }),
          after: ["seed"],
        }),
      },
    });

    const agentSteps = collectAgentBearingStepIds({
      definition: projectionOf(def),
      context: CONTEXT,
    });

    expect(agentSteps.has("fan")).toBe(true);
    expect(agentSteps.has("seed")).toBe(false);
  });

  test("excludes an onTrigger container and does not reach into its body", () => {
    // An onTrigger body is lifted to its own definition and classified there,
    // so reaching into it here would double-count it against the wrong map.
    const def = defineWorkflow({
      id: "wf-ontrigger",
      trigger: { type: "manual" },
      steps: {
        section: onTrigger({
          on: { type: "mail", to: "run_x@workflow.interchange" },
          body: defineWorkflow({
            id: "authored-body",
            trigger: { type: "manual" },
            steps: { work: step({ agent: agent("body-agent") }) },
          }),
        }),
      },
    });

    const agentSteps = collectAgentBearingStepIds({
      definition: projectionOf(def),
      context: CONTEXT,
    });

    expect(agentSteps.has("section")).toBe(false);
    expect(agentSteps.has("work")).toBe(false);
  });

  test("reports an agent nested two loop levels deep", () => {
    const inner = defineWorkflow({
      id: "inner-body",
      trigger: { type: "manual" },
      steps: { deep: step({ agent: agent("deep-agent") }) },
    });
    const outer = defineWorkflow({
      id: "outer-body",
      trigger: { type: "manual" },
      steps: {
        innerLoop: loop({
          body: inner,
          while: "w",
          carry: "c",
          input: { literal: 0 },
          maxIterations: 2,
          onExhausted: "settle",
        }),
        settle: action({ handler: "settle", after: ["innerLoop"] }),
      },
    });
    const def = defineWorkflow({
      id: "wf-nested-loops",
      trigger: { type: "manual" },
      steps: {
        outerLoop: loop({
          body: outer,
          while: "w",
          carry: "c",
          input: { literal: 0 },
          maxIterations: 2,
          onExhausted: "done",
        }),
        done: action({ handler: "done", after: ["outerLoop"] }),
      },
    });

    const agentSteps = collectAgentBearingStepIds({
      definition: projectionOf(def),
      context: CONTEXT,
    });

    expect([...agentSteps].sort()).toEqual(["deep"]);
  });

  test("reports an agent that declares no model sources", () => {
    const def = defineWorkflow({
      id: "wf-agent-nopref",
      trigger: { type: "manual" },
      steps: {
        think: step({
          agent: defineAgent({
            id: "thinker",
            systemPrompt: "think",
            tools: [],
            capabilities: [],
            inference: { sources: [] },
          }),
        }),
      },
    });

    // It resolves a source at runtime, so it is entitled to one's credential
    // even though it advertised no preference.
    expect(
      collectAgentBearingStepIds({
        definition: projectionOf(def),
        context: CONTEXT,
      }).has("think"),
    ).toBe(true);
  });
});

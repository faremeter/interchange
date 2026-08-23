// Pins the loop-body recursion in `buildInertProjectionStepSources`.
//
// A loop body runs in-process as a child run sharing the parent's env, so its
// agent steps resolve their pinned inference source from the same flat top-level
// sources map. The source pin therefore recurses into loop bodies; without that
// recursion a deployed loop-with-agent-body crashes at the first iteration with
// "no InferenceSource pinned". A body step id that collides with another step
// must resolve to the same source, else the deploy fails closed.

import { describe, test, expect } from "bun:test";

import { type } from "arktype";
import { defineAgent } from "@intx/agent";
import type { HarnessConfig } from "@intx/types/runtime";
import { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import { defineWorkflow, loop, projectLiveToInert, step } from "@intx/workflow";

import {
  buildInertProjectionStepSources,
  WorkflowDefinitionInvalidError,
} from "./orchestrator";

function agent(id: string, model: string) {
  return defineAgent({
    id,
    systemPrompt: "loop source test agent",
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "anthropic", model }] },
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

const CONFIG: HarnessConfig = {
  sessionId: "ses-loop-src",
  agentId: "ag_loop_src",
  tenantId: "tenant-1",
  principalId: "prin-1",
  agentAddress: "run_loopsrc@workflow.interchange",
  systemPrompt: "shared",
  tools: [],
  grants: [],
  sources: [
    {
      id: "src-worker",
      provider: "anthropic",
      baseURL: "https://api.example/anthropic",
      apiKey: "secret-w",
      model: "worker-model",
    },
    {
      id: "src-other",
      provider: "anthropic",
      baseURL: "https://api.example/anthropic",
      apiKey: "secret-o",
      model: "other-model",
    },
  ],
  defaultSource: "src-worker",
};

describe("buildInertProjectionStepSources (loop bodies)", () => {
  test("pins a loop body's agent step, keyed by its plain step id", () => {
    const body = defineWorkflow({
      id: "loop-body",
      trigger: { type: "manual" },
      steps: { turn: step({ agent: agent("turn-agent", "worker-model") }) },
    });
    const def = defineWorkflow({
      id: "wf-loop-src",
      trigger: { type: "manual" },
      steps: {
        rework: loop({
          body,
          while: "w",
          carry: "c",
          input: { literal: 0 },
          maxIterations: 3,
          onExhausted: "esc",
        }),
        esc: step({
          agent: agent("esc-agent", "worker-model"),
          after: ["rework"],
        }),
      },
    });

    const sources = buildInertProjectionStepSources({
      projection: projectionOf(def),
      config: CONFIG,
      operatorApprovals: new Set(["inference.source:anthropic:worker-model"]),
    });

    // The loop-body step is pinned (the recursion) -- without it, "turn" is
    // absent and the deployed loop crashes at iteration one.
    expect(sources["turn"]?.[0]?.id).toBe("src-worker");
    // The top-level steps are pinned too: the loop container falls back to the
    // approved default, the dependent agent step to its preference.
    expect(sources["rework"]?.[0]?.id).toBe("src-worker");
    expect(sources["esc"]?.[0]?.id).toBe("src-worker");
  });

  test("fails closed when two loop bodies share a step id but resolve to different sources", () => {
    const bodyA = defineWorkflow({
      id: "body-a",
      trigger: { type: "manual" },
      steps: { turn: step({ agent: agent("a-agent", "worker-model") }) },
    });
    const bodyB = defineWorkflow({
      id: "body-b",
      trigger: { type: "manual" },
      steps: { turn: step({ agent: agent("b-agent", "other-model") }) },
    });
    const def = defineWorkflow({
      id: "wf-collision",
      trigger: { type: "manual" },
      steps: {
        loopA: loop({
          body: bodyA,
          while: "w",
          carry: "c",
          maxIterations: 2,
          onExhausted: "esc",
        }),
        loopB: loop({
          body: bodyB,
          while: "w",
          carry: "c",
          maxIterations: 2,
          onExhausted: "esc",
          after: ["loopA"],
        }),
        esc: step({
          agent: agent("esc-agent", "worker-model"),
          after: ["loopA", "loopB"],
        }),
      },
    });

    expect(() =>
      buildInertProjectionStepSources({
        projection: projectionOf(def),
        config: CONFIG,
        operatorApprovals: new Set([
          "inference.source:anthropic:worker-model",
          "inference.source:anthropic:other-model",
        ]),
      }),
    ).toThrow(WorkflowDefinitionInvalidError);
  });
});

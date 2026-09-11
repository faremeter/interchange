// Pins the leaf policy `buildInertBodyStepSources` applies to a lifted body.
//
// The policy keys on whether a step is agent-bearing, never on whether it
// declared a model preference. The two are easy to confuse: an agent that
// declares no `modelSources` also arrives without a preference, and on a
// config whose default is approved both branches return the same source. They
// diverge only when the default is unapproved -- the agent step must still
// fail the operator-approval gate, while a step that cannot invoke inference
// takes the placeholder.

import { describe, test, expect } from "bun:test";

import { type } from "arktype";
import { defineAgent } from "@intx/agent";
import type { HarnessConfig } from "@intx/types/runtime";
import { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import {
  action,
  defineWorkflow,
  projectLiveToInert,
  step,
} from "@intx/workflow";

import {
  buildInertBodyStepSources,
  WorkflowDefinitionInvalidError,
} from "./orchestrator";

function projectionOf(def: Parameters<typeof projectLiveToInert>[0]) {
  const projection = WorkflowProjectionDefinition(
    JSON.parse(JSON.stringify(projectLiveToInert(def))),
  );
  if (projection instanceof type.errors) {
    throw new Error(`projection failed validation: ${projection.summary}`);
  }
  return projection;
}

const DEFAULT_SOURCE = {
  id: "src-default",
  provider: "openai",
  baseURL: "https://api.example/openai",
  credentialId: "secret-o",
  model: "default-model",
};

const CONFIG: HarnessConfig = {
  sessionId: "ses-inert-body",
  agentId: "ag_inert_body",
  tenantId: "tenant-1",
  principalId: "prin-1",
  agentAddress: "run_inertbody@workflow.interchange",
  systemPrompt: "shared",
  tools: [],
  grants: [],
  sources: [DEFAULT_SOURCE],
  defaultSource: DEFAULT_SOURCE.id,
};

// What a probe of a body carrying no agent source produces: no
// `inference.source:` entry, because no agent advertised one.
const NO_SOURCE_APPROVALS = new Set<string>(["effect:fs:write"]);

describe("buildInertBodyStepSources", () => {
  test("pins a step that cannot invoke inference to the unapproved default", () => {
    const def = defineWorkflow({
      id: "wf-body-action",
      trigger: { type: "manual" },
      steps: {
        gather: action({
          handler: "gather",
          effect: { requires: ["fs:write"] },
        }),
      },
    });

    const sources = buildInertBodyStepSources({
      definition: projectionOf(def),
      workflowId: "wf-body-action__body",
      config: CONFIG,
      operatorApprovals: NO_SOURCE_APPROVALS,
    });

    expect(sources["gather"]).toEqual([DEFAULT_SOURCE]);
  });

  test("still gates an agent step that declares no model sources", () => {
    const def = defineWorkflow({
      id: "wf-body-agent-nopref",
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

    expect(() =>
      buildInertBodyStepSources({
        definition: projectionOf(def),
        workflowId: "wf-body-agent-nopref__body",
        config: CONFIG,
        operatorApprovals: NO_SOURCE_APPROVALS,
      }),
    ).toThrow(WorkflowDefinitionInvalidError);
  });

  test("resolves the placeholder per leaf, not once per walk", () => {
    // Every step here resolves a source of its own, so nothing needs the
    // placeholder and the dangling defaultSource must go unnoticed. Hoisting
    // the lookup out of the leaf callback reads as a harmless cleanup and
    // would make this body fail to pin.
    const def = defineWorkflow({
      id: "wf-body-all-agent",
      trigger: { type: "manual" },
      steps: {
        think: step({
          agent: defineAgent({
            id: "thinker",
            systemPrompt: "think",
            tools: [],
            capabilities: [],
            inference: {
              sources: [
                {
                  provider: DEFAULT_SOURCE.provider,
                  model: DEFAULT_SOURCE.model,
                },
              ],
            },
          }),
        }),
      },
    });

    const sources = buildInertBodyStepSources({
      definition: projectionOf(def),
      workflowId: "wf-body-all-agent__body",
      config: { ...CONFIG, defaultSource: "src-missing" },
      operatorApprovals: new Set<string>([
        `inference.source:${DEFAULT_SOURCE.provider}:${DEFAULT_SOURCE.model}`,
      ]),
    });

    expect(sources["think"]).toEqual([DEFAULT_SOURCE]);
  });

  test("fails closed when defaultSource names no entry in the source list", () => {
    const def = defineWorkflow({
      id: "wf-body-dangling",
      trigger: { type: "manual" },
      steps: { gather: action({ handler: "gather" }) },
    });

    expect(() =>
      buildInertBodyStepSources({
        definition: projectionOf(def),
        workflowId: "wf-body-dangling__body",
        config: { ...CONFIG, defaultSource: "src-missing" },
        operatorApprovals: NO_SOURCE_APPROVALS,
      }),
    ).toThrow(
      /step gather needs an inert placeholder source, but defaultSource "src-missing" names no entry/,
    );
  });
});

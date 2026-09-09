// Pins the non-agent top-level contract for `buildInertProjectionStepSources`.
//
// A step that cannot invoke inference is pinned an inert placeholder -- the
// deploy's default source -- and is not approval-gated. Its pin exists only
// because the wire shape requires a source for every step.
//
// Gating that placeholder on an `inference.source:` approval demanded a grant
// the capability walk emits only from agent definitions. A workflow whose
// default source no agent happened to declare could not deploy, and one with no
// agent step at all could never deploy. Agent steps keep the resolver and its
// operator-approval gate.

import { describe, test, expect } from "bun:test";

import { type } from "arktype";
import { defineAgent } from "@intx/agent";
import type { HarnessConfig } from "@intx/types/runtime";
import { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import {
  action,
  awaitSignal,
  defineWorkflow,
  gate,
  projectLiveToInert,
  step,
} from "@intx/workflow";

import {
  buildInertProjectionStepSources,
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
  sessionId: "ses-nonagent-top",
  agentId: "ag_nonagent_top",
  tenantId: "tenant-1",
  principalId: "prin-1",
  agentAddress: "run_nonagenttop@workflow.interchange",
  systemPrompt: "shared",
  tools: [],
  grants: [],
  sources: [DEFAULT_SOURCE],
  defaultSource: DEFAULT_SOURCE.id,
};

// The approved set a probe of an all-action definition produces: effect
// grants only, no `inference.source:` entry, because no agent advertised one.
const NO_SOURCE_APPROVALS = new Set<string>(["effect:fs:write"]);

describe("buildInertProjectionStepSources (non-agent top-level steps)", () => {
  test("pins every step of an all-action definition to the default placeholder", () => {
    const def = defineWorkflow({
      id: "wf-all-action",
      trigger: { type: "manual" },
      steps: {
        gather: action({
          handler: "gather",
          effect: { requires: ["fs:write"] },
        }),
        enough: gate({
          when: { from: "steps.gather.output.ok" },
          then: "hold",
          else: "persist",
          after: ["gather"],
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

    const sources = buildInertProjectionStepSources({
      projection: projectionOf(def),
      config: CONFIG,
      operatorApprovals: NO_SOURCE_APPROVALS,
    });

    for (const stepId of ["gather", "enough", "hold", "persist"]) {
      expect(sources[stepId]).toEqual([DEFAULT_SOURCE]);
    }
  });

  test("still refuses an agent step whose only source is not approved", () => {
    const def = defineWorkflow({
      id: "wf-agent-unapproved",
      trigger: { type: "manual" },
      steps: {
        gather: action({ handler: "gather" }),
        think: step({
          agent: defineAgent({
            id: "thinker",
            systemPrompt: "think",
            tools: [],
            capabilities: [],
            inference: {
              sources: [{ provider: "openai", model: "default-model" }],
            },
          }),
          after: ["gather"],
        }),
      },
    });

    expect(() =>
      buildInertProjectionStepSources({
        projection: projectionOf(def),
        config: CONFIG,
        operatorApprovals: NO_SOURCE_APPROVALS,
      }),
    ).toThrow(WorkflowDefinitionInvalidError);
  });

  test("fails closed when the deploy config has no default source to pin", () => {
    const def = defineWorkflow({
      id: "wf-no-default",
      trigger: { type: "manual" },
      steps: { gather: action({ handler: "gather" }) },
    });

    expect(() =>
      buildInertProjectionStepSources({
        projection: projectionOf(def),
        config: { ...CONFIG, defaultSource: "src-missing" },
        operatorApprovals: NO_SOURCE_APPROVALS,
      }),
    ).toThrow(WorkflowDefinitionInvalidError);
  });

  test("pins an approved agent beside an action whose placeholder is unapproved", () => {
    // The mixed case, and the one that shows the reach of this rule. Before it,
    // a workflow like this deployed only when the tenant's default source
    // happened to carry a (provider, model) some agent in the workflow also
    // declared -- a coincidence, not a decision. The action step's pin is now
    // ungated, while the agent step's is not.
    const AGENT_SOURCE = {
      id: "src-agent",
      provider: "anthropic",
      baseURL: "https://api.example/anthropic",
      credentialId: "secret-a",
      model: "worker-model",
    };
    const def = defineWorkflow({
      id: "wf-mixed-toplevel",
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
                  provider: AGENT_SOURCE.provider,
                  model: AGENT_SOURCE.model,
                },
              ],
            },
          }),
        }),
        gather: action({ handler: "gather", after: ["think"] }),
      },
    });

    const sources = buildInertProjectionStepSources({
      projection: projectionOf(def),
      config: {
        ...CONFIG,
        sources: [AGENT_SOURCE, DEFAULT_SOURCE],
        defaultSource: DEFAULT_SOURCE.id,
      },
      // Only the agent's pair is approved. The default's is not.
      operatorApprovals: new Set<string>([
        `inference.source:${AGENT_SOURCE.provider}:${AGENT_SOURCE.model}`,
      ]),
    });

    expect(sources["think"]).toEqual([AGENT_SOURCE]);
    expect(sources["gather"]).toEqual([DEFAULT_SOURCE]);
  });
});

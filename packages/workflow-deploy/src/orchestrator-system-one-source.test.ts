// Pins that System One is an ordinary inference source, not a built-in special
// case. The deploy keys the grant and the pin on (provider, model). Provider
// "corbits-system-one" is admitted only when that pair is approved and a
// matching row exists; an empty modelSources list still falls through the
// normal default, and colliding loop-body ids still require the same source.

import { describe, test, expect } from "bun:test";

import { type } from "arktype";
import { defineAgent } from "@intx/agent";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import { defineWorkflow, loop, projectLiveToInert, step } from "@intx/workflow";

import {
  buildInertProjectionStepSources,
  pickStepInferenceSource,
  WorkflowDefinitionInvalidError,
} from "./orchestrator";
import { createApprovalSet } from "./capability-approval";

const PROVIDER = "corbits-system-one";
const MODEL = "jev-latest";
const GRANT = `inference.source:${PROVIDER}:${MODEL}`;
const PREFERRED = { provider: PROVIDER, model: MODEL };

const SYSTEM_ONE: InferenceSource = {
  id: "src-system-one",
  provider: PROVIDER,
  baseURL: "https://system-one.example",
  credentialId: "secret-s1",
  model: MODEL,
};

function makeConfig(args: {
  sources: HarnessConfig["sources"];
  defaultSource: string;
}): HarnessConfig {
  return {
    sessionId: "ses-system-one",
    agentId: "ag_system_one",
    tenantId: "tenant-1",
    principalId: "prin-1",
    agentAddress: "run_system_one@workflow.interchange",
    systemPrompt: "shared-prompt",
    tools: [],
    grants: [],
    sources: args.sources,
    defaultSource: args.defaultSource,
  };
}

function agent(id: string, provider: string, model: string) {
  return defineAgent({
    id,
    systemPrompt: "system one source test agent",
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider, model }] },
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

describe("pickStepInferenceSource (corbits-system-one)", () => {
  test("pins the first declared source when its grant is approved", () => {
    const config = makeConfig({
      sources: [
        SYSTEM_ONE,
        {
          id: "src-other",
          provider: "openai",
          baseURL: "https://api.example/openai",
          credentialId: "secret-o",
          model: "other-model",
        },
      ],
      defaultSource: "src-other",
    });

    const picked = pickStepInferenceSource({
      preferred: PREFERRED,
      stepId: "only",
      workflowId: "wf_system_one",
      config,
      operatorApprovals: createApprovalSet([GRANT]),
    });

    expect(picked).toEqual(SYSTEM_ONE);
  });

  test("throws when the grant is missing and the default is not approved", () => {
    const config = makeConfig({
      sources: [SYSTEM_ONE],
      defaultSource: SYSTEM_ONE.id,
    });

    expect(() =>
      pickStepInferenceSource({
        preferred: PREFERRED,
        stepId: "only",
        workflowId: "wf_system_one",
        config,
        operatorApprovals: createApprovalSet([]),
      }),
    ).toThrow(WorkflowDefinitionInvalidError);
  });

  test("throws when no row matches and the default is not an approved source", () => {
    const config = makeConfig({
      sources: [
        {
          id: "src-default",
          provider: "openai",
          baseURL: "https://api.example/openai",
          credentialId: "secret-o",
          model: "default-model",
        },
      ],
      defaultSource: "src-default",
    });

    expect(() =>
      pickStepInferenceSource({
        preferred: PREFERRED,
        stepId: "only",
        workflowId: "wf_system_one",
        config,
        operatorApprovals: createApprovalSet([GRANT]),
      }),
    ).toThrow(WorkflowDefinitionInvalidError);
  });

  test("pins a matching row even when its id is not the source name", () => {
    const renamed: InferenceSource = { ...SYSTEM_ONE, id: "src-renamed" };
    const config = makeConfig({
      sources: [renamed],
      defaultSource: "src-missing",
    });

    const picked = pickStepInferenceSource({
      preferred: PREFERRED,
      stepId: "only",
      workflowId: "wf_system_one",
      config,
      operatorApprovals: createApprovalSet([GRANT]),
    });

    expect(picked.id).toBe("src-renamed");
    expect(picked.provider).toBe(PROVIDER);
    expect(picked.model).toBe(MODEL);
  });

  test("does not pin a row that shares the model under a different provider", () => {
    const sameModel: InferenceSource = {
      id: "src-other-provider",
      provider: "openai",
      baseURL: "https://api.example/openai",
      credentialId: "secret-o",
      model: MODEL,
    };
    const config = makeConfig({
      sources: [sameModel],
      defaultSource: sameModel.id,
    });

    expect(() =>
      pickStepInferenceSource({
        preferred: PREFERRED,
        stepId: "only",
        workflowId: "wf_system_one",
        config,
        operatorApprovals: createApprovalSet([GRANT]),
      }),
    ).toThrow(WorkflowDefinitionInvalidError);
  });
});

describe("buildInertProjectionStepSources (corbits-system-one)", () => {
  const config = makeConfig({
    sources: [
      SYSTEM_ONE,
      {
        id: "src-other",
        provider: "anthropic",
        baseURL: "https://api.example/anthropic",
        credentialId: "secret-a",
        model: "other-model",
      },
    ],
    defaultSource: "src-other",
  });

  test("does not special-case System One when modelSources is empty", () => {
    const raw = WorkflowProjectionDefinition({
      id: "wf-empty-sources",
      triggers: [],
      stepOrder: ["blank"],
      steps: {
        blank: { kind: "step", agent: { modelSources: [] } },
      },
    });
    if (raw instanceof type.errors) {
      throw new Error(`projection failed validation: ${raw.summary}`);
    }

    // The System One grant is approved, but the step declared no source. The
    // pin must not reach for that row; it falls back to the default, which is
    // not approved.
    expect(() =>
      buildInertProjectionStepSources({
        projection: raw,
        config,
        operatorApprovals: createApprovalSet([GRANT]),
      }),
    ).toThrow(WorkflowDefinitionInvalidError);
  });

  test("pins colliding loop-body ids only when they resolve to the same source", () => {
    const bodyA = defineWorkflow({
      id: "body-a",
      trigger: { type: "manual" },
      steps: { turn: step({ agent: agent("a-agent", PROVIDER, MODEL) }) },
    });
    const bodySame = defineWorkflow({
      id: "body-same",
      trigger: { type: "manual" },
      steps: { turn: step({ agent: agent("same-agent", PROVIDER, MODEL) }) },
    });
    const bodyOther = defineWorkflow({
      id: "body-other",
      trigger: { type: "manual" },
      steps: {
        turn: step({ agent: agent("other-agent", "anthropic", "other-model") }),
      },
    });

    const same = defineWorkflow({
      id: "wf-same",
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
          body: bodySame,
          while: "w",
          carry: "c",
          maxIterations: 2,
          onExhausted: "esc",
          after: ["loopA"],
        }),
        esc: step({
          agent: agent("esc-agent", PROVIDER, MODEL),
          after: ["loopA", "loopB"],
        }),
      },
    });

    const sources = buildInertProjectionStepSources({
      projection: projectionOf(same),
      config,
      operatorApprovals: createApprovalSet([GRANT]),
    });
    expect(sources["turn"]?.[0]?.id).toBe(SYSTEM_ONE.id);

    const different = defineWorkflow({
      id: "wf-different",
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
          body: bodyOther,
          while: "w",
          carry: "c",
          maxIterations: 2,
          onExhausted: "esc",
          after: ["loopA"],
        }),
        esc: step({
          agent: agent("esc-agent", PROVIDER, MODEL),
          after: ["loopA", "loopB"],
        }),
      },
    });

    expect(() =>
      buildInertProjectionStepSources({
        projection: projectionOf(different),
        config,
        operatorApprovals: createApprovalSet([
          GRANT,
          "inference.source:anthropic:other-model",
        ]),
      }),
    ).toThrow(WorkflowDefinitionInvalidError);
  });
});

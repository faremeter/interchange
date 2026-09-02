// Pins the agent-step contract for `pickStepInferenceSource`.
//
// The deploy must not pin a step to a `HarnessConfig.defaultSource` whose
// `(provider, model)` was never approved by the operator. The capability walk
// emits `inference.source:<provider>:<model>` grants for every source the
// operator approved; the source-pinning pass must cross-check whichever source
// it chooses against that set before letting the deploy proceed. Falling back
// to an unapproved default would let a deploy slip past the approval gate by
// pinning a different `(provider, model)` than the agent's preferred source.

import { describe, test, expect } from "bun:test";

import type { HarnessConfig } from "@intx/types/runtime";

import type { InferenceSource } from "@intx/types/runtime";

import {
  isSourceApproved,
  pickStepInferenceSource,
  WorkflowDefinitionInvalidError,
} from "./orchestrator";

// The agent's first declared source, the `(provider, model)` preference the
// deploy reads off the step agent and feeds the picker.
const PREFERRED = { provider: "anthropic", model: "preferred-model" };

function makeConfig(args: {
  sources: HarnessConfig["sources"];
  defaultSource: string;
}): HarnessConfig {
  return {
    sessionId: "ses-fallback",
    agentId: "ag_fallback",
    tenantId: "tenant-1",
    principalId: "prin-1",
    agentAddress: "run_fallback@workflow.interchange",
    systemPrompt: "shared-prompt",
    tools: [],
    grants: [],
    sources: args.sources,
    defaultSource: args.defaultSource,
  };
}

describe("pickStepInferenceSource (agent step)", () => {
  test("throws when the agent's preferred source is missing and the defaultSource's (provider, model) is not in the approved grants", () => {
    // HarnessConfig has only a default-pinned source for openai:default-model,
    // not the agent's preferred (anthropic, preferred-model).
    const config = makeConfig({
      sources: [
        {
          id: "src-default",
          provider: "openai",
          baseURL: "https://api.example/openai",
          credentialId: "secret",
          model: "default-model",
        },
      ],
      defaultSource: "src-default",
    });

    // Approvals cover the agent's preferred source but NOT the defaultSource's
    // (openai, default-model). This is the capability-walk-bypass shape: the
    // operator approved one (provider, model), the picker would otherwise
    // silently pin a different one.
    const approvals = new Set<string>([
      "inference.source:anthropic:preferred-model",
    ]);

    expect(() =>
      pickStepInferenceSource({
        preferred: PREFERRED,
        stepId: "only",
        workflowId: "wf_fallback",
        config,
        operatorApprovals: approvals,
      }),
    ).toThrow(WorkflowDefinitionInvalidError);
  });

  test("uses the defaultSource when its (provider, model) is in the approved grants", () => {
    const config = makeConfig({
      sources: [
        {
          id: "src-default",
          provider: "openai",
          baseURL: "https://api.example/openai",
          credentialId: "secret",
          model: "default-model",
        },
      ],
      defaultSource: "src-default",
    });

    // The operator approved BOTH the agent's preferred shape and the
    // defaultSource's (provider, model). The agent's preferred source does not
    // resolve against HarnessConfig.sources, so the picker legitimately falls
    // back to the default -- which is approved.
    const approvals = new Set<string>([
      "inference.source:anthropic:preferred-model",
      "inference.source:openai:default-model",
    ]);

    const picked = pickStepInferenceSource({
      preferred: PREFERRED,
      stepId: "only",
      workflowId: "wf_fallback",
      config,
      operatorApprovals: approvals,
    });

    expect(picked).toEqual({
      id: "src-default",
      provider: "openai",
      baseURL: "https://api.example/openai",
      credentialId: "secret",
      model: "default-model",
    });
  });

  test("uses the agent's preferred source when it matches an approved HarnessConfig source", () => {
    const config = makeConfig({
      sources: [
        {
          id: "src-preferred",
          provider: "anthropic",
          baseURL: "https://api.example/anthropic",
          credentialId: "secret-a",
          model: "preferred-model",
        },
        {
          id: "src-other",
          provider: "openai",
          baseURL: "https://api.example/openai",
          credentialId: "secret-b",
          model: "other-model",
        },
      ],
      defaultSource: "src-other",
    });

    // The agent's preferred (provider, model) is approved AND resolves against
    // the deploy's HarnessConfig.sources -- the picker pins it directly without
    // consulting the default.
    const approvals = new Set<string>([
      "inference.source:anthropic:preferred-model",
    ]);

    const picked = pickStepInferenceSource({
      preferred: PREFERRED,
      stepId: "only",
      workflowId: "wf_fallback",
      config,
      operatorApprovals: approvals,
    });

    expect(picked).toEqual({
      id: "src-preferred",
      provider: "anthropic",
      baseURL: "https://api.example/anthropic",
      credentialId: "secret-a",
      model: "preferred-model",
    });
  });
});

describe("isSourceApproved", () => {
  const source: InferenceSource = {
    id: "src-a",
    provider: "anthropic",
    baseURL: "https://api.example/anthropic",
    credentialId: "secret",
    model: "claude",
  };

  test("true when the source's provider:model is in the approved grant set", () => {
    const approvals = new Set<string>(["inference.source:anthropic:claude"]);
    expect(isSourceApproved(source, approvals)).toBe(true);
  });

  test("false when the source's provider:model is not approved", () => {
    const approvals = new Set<string>(["inference.source:openai:gpt"]);
    expect(isSourceApproved(source, approvals)).toBe(false);
  });

  test("keys on the exact provider:model pair, not the source id", () => {
    // Approving the source id (not the provider:model grant shape) must not
    // admit the source -- the grant is keyed by (provider, model).
    const approvals = new Set<string>(["inference.source:src-a"]);
    expect(isSourceApproved(source, approvals)).toBe(false);
  });
});

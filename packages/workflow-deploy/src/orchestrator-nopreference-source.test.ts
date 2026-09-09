// Pins the fallback contract for `pickStepInferenceSource` when a step
// declares no preference.
//
// The picker sees only `preferred: null`, never what kind of step asked. The
// caller that reaches it that way is an agent step whose `modelSources` is
// empty: it resolves a source at runtime, so it can issue a request, but it
// advertised no `(provider, model)` for the capability walk to surface and the
// operator to approve. A step that cannot invoke inference does not arrive
// here at all -- it takes the inert placeholder.
//
// Concrete shape: falling back to the `HarnessConfig.defaultSource` is allowed
// only when that source's `(provider, model)` is in the operator-approved
// grants. Otherwise the pin must fail loudly -- a silent fallback would hand an
// inference-capable step a source the operator never approved, which is the
// capability-walk bypass this test pins against.

import { describe, test, expect } from "bun:test";

import type { HarnessConfig } from "@intx/types/runtime";

import {
  pickStepInferenceSource,
  WorkflowDefinitionInvalidError,
} from "./orchestrator";

function makeConfig(args: {
  sources: HarnessConfig["sources"];
  defaultSource: string;
}): HarnessConfig {
  return {
    sessionId: "ses-nonagent",
    agentId: "ag_nonagent",
    tenantId: "tenant-1",
    principalId: "prin-1",
    agentAddress: "run_nonagent@workflow.interchange",
    systemPrompt: "shared-prompt",
    tools: [],
    grants: [],
    sources: args.sources,
    defaultSource: args.defaultSource,
  };
}

describe("pickStepInferenceSource (step with no declared preference)", () => {
  test("rejects falling back to a defaultSource whose (provider, model) is not approved", () => {
    // HarnessConfig carries TWO sources: one an agent declared, and a
    // distinct default. A step that declared no preference falls back to the
    // default, whose (provider, model) is NOT in the approved set, so the
    // picker must refuse to pin it.
    const config = makeConfig({
      sources: [
        {
          id: "src-anthropic",
          provider: "anthropic",
          baseURL: "https://api.example/anthropic",
          credentialId: "secret-a",
          model: "worker-model",
        },
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

    // Approve one agent's (provider, model) but NOT the default's
    // (openai, default-model). A step that declared no source advertises
    // nothing for the walk to surface, so the source pin is the only place the
    // unapproved fallback is caught.
    const approvals = new Set<string>([
      "inference.source:anthropic:worker-model",
    ]);

    expect(() =>
      pickStepInferenceSource({
        preferred: null,
        stepId: "cooldown",
        workflowId: "wf_nonagent",
        config,
        operatorApprovals: approvals,
      }),
    ).toThrow(WorkflowDefinitionInvalidError);
  });

  test("fails loudly when the only available source is unapproved", () => {
    // A lone source, unapproved. Absent the source-pin cross-check the approval
    // gate has nothing to fail on, since a step that declares no source emits
    // no `inference.source:` grant.
    const config = makeConfig({
      sources: [
        {
          id: "src-lambda",
          provider: "lambda",
          baseURL: "https://api.example/lambda",
          credentialId: "secret-l",
          model: "default-lambda",
        },
      ],
      defaultSource: "src-lambda",
    });

    const approvals = new Set<string>();

    expect(() =>
      pickStepInferenceSource({
        preferred: null,
        stepId: "nap",
        workflowId: "wf_sleep_only",
        config,
        operatorApprovals: approvals,
      }),
    ).toThrow(WorkflowDefinitionInvalidError);
  });

  test("allows the fallback when the default's (provider, model) is approved", () => {
    const config = makeConfig({
      sources: [
        {
          id: "src-anthropic",
          provider: "anthropic",
          baseURL: "https://api.example/anthropic",
          credentialId: "secret-a",
          model: "worker-model",
        },
      ],
      defaultSource: "src-anthropic",
    });

    // The operator approved the (provider, model) of the source the step falls
    // back to, so the pin proceeds.
    const approvals = new Set<string>([
      "inference.source:anthropic:worker-model",
    ]);

    const picked = pickStepInferenceSource({
      preferred: null,
      stepId: "cooldown",
      workflowId: "wf_nonagent",
      config,
      operatorApprovals: approvals,
    });

    expect(picked).toEqual({
      id: "src-anthropic",
      provider: "anthropic",
      baseURL: "https://api.example/anthropic",
      credentialId: "secret-a",
      model: "worker-model",
    });
  });
});

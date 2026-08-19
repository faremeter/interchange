// Pins the non-agent-step contract for `pickStepInferenceSource`.
//
// Non-agent primitives (sleep, gate, awaitSignal, ...) carry no agent
// preference, so the picker is called with `preferred: null` and would fall
// through to the `HarnessConfig.defaultSource`. The capability walk emits NO
// `inference.source:<provider>:<model>` grant for these steps -- it only emits
// source grants from agent definitions. The picker must not paper over that
// absence by pinning a source the operator never approved.
//
// Concrete shape: if a non-agent step is pinned to the defaultSource, that
// source's `(provider, model)` must be in the operator-approved grants.
// Otherwise the pin must fail loudly -- silent fallback is the capability-walk
// bypass this test pins against.

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

describe("pickStepInferenceSource (non-agent step)", () => {
  test("rejects pinning a non-agent step to a defaultSource whose (provider, model) is not approved", () => {
    // HarnessConfig carries TWO sources: an agent source and a distinct
    // default. A non-agent step has no preference and falls back to the
    // default. The default's (provider, model) is NOT in the approved set, so
    // the picker must refuse to pin it.
    const config = makeConfig({
      sources: [
        {
          id: "src-anthropic",
          provider: "anthropic",
          baseURL: "https://api.example/anthropic",
          apiKey: "secret-a",
          model: "worker-model",
        },
        {
          id: "src-default",
          provider: "openai",
          baseURL: "https://api.example/openai",
          apiKey: "secret-o",
          model: "default-model",
        },
      ],
      defaultSource: "src-default",
    });

    // Approve the agent's (provider, model) but NOT the default's
    // (openai, default-model). The walk surfaces nothing for a non-agent step,
    // so the source pin is the only place the unapproved fallback is caught.
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

  test("fails loudly when the only available source for a non-agent step is unapproved", () => {
    // A lone source, unapproved. Absent the source-pin cross-check the approval
    // gate has nothing to fail on, since a non-agent step emits no
    // `inference.source:` grant.
    const config = makeConfig({
      sources: [
        {
          id: "src-lambda",
          provider: "lambda",
          baseURL: "https://api.example/lambda",
          apiKey: "secret-l",
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

  test("allows pinning a non-agent step when the default's (provider, model) is approved", () => {
    const config = makeConfig({
      sources: [
        {
          id: "src-anthropic",
          provider: "anthropic",
          baseURL: "https://api.example/anthropic",
          apiKey: "secret-a",
          model: "worker-model",
        },
      ],
      defaultSource: "src-anthropic",
    });

    // The operator approved the (provider, model) of the source the non-agent
    // step falls back to, so the pin proceeds.
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
      apiKey: "secret-a",
      model: "worker-model",
    });
  });
});

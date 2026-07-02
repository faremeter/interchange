// Pins the agent-step contract for `pickStepInferenceSource`:
//
// The orchestrator must not pin a step to a `HarnessConfig.defaultSource`
// whose `(provider, model)` was never approved by the operator. The
// capability walk emits `inference.source:<provider>:<model>` grants for
// every source the operator approved; the orchestrator's source-pinning
// pass must cross-check whichever source it chooses against that set
// before letting the deploy proceed. Falling back to an unapproved
// default would let a deploy slip past the approval gate by pinning a
// different `(provider, model)` than the agent's preferred source.

import { describe, test, expect } from "bun:test";

import {
  createDefaultDirectorRegistry,
  defaultDirectorFactory,
  defineAgent,
  type AgentDefinition,
  type AnnotatedToolFactory,
  type BaseEnv,
} from "@intx/agent";
import type { HarnessConfig } from "@intx/types/runtime";
import {
  defineWorkflow,
  step,
  type WorkflowDefinition,
} from "@intx/workflow/definition";

import {
  CapabilityApprovalDeniedError,
  createWorkflowDeployOrchestrator,
  WorkflowDefinitionInvalidError,
  type DeployContent,
  type DeploySingleStepFn,
  type LaunchSessionFn,
  type SendMultiStepDeployFn,
  type WorkflowRepoWriter,
} from "./orchestrator";

function makeMailFactory(): AnnotatedToolFactory<BaseEnv> {
  const factory = (_env: BaseEnv) => ({
    definitions: [],
    run: () =>
      Promise.resolve({ callId: "", content: "", isError: false as const }),
  });
  return Object.assign(factory, {
    id: "@intx/tools-mail/sidecar-bundle",
    requires: [] as readonly string[],
  });
}

function makeAgent(
  id: string,
  provider: string,
  model: string,
): AgentDefinition<BaseEnv> {
  return defineAgent({
    id,
    systemPrompt: `you are ${id}`,
    tools: [makeMailFactory()],
    capabilities: [],
    inference: {
      sources: [{ provider, model }],
    },
  });
}

function makeWorkflow(agent: AgentDefinition<BaseEnv>): WorkflowDefinition {
  return defineWorkflow({
    id: "wf_fallback",
    trigger: { type: "manual" },
    steps: {
      only: step({ agent, after: [] }),
    },
  });
}

const DEPLOY_CONTENT_BASE: DeployContent = {
  systemPrompt: "shared-prompt",
};

function makeConfig(args: {
  sources: HarnessConfig["sources"];
  defaultSource: string;
}): HarnessConfig {
  return {
    sessionId: "ses-fallback",
    agentId: "ag_fallback",
    tenantId: "tenant-1",
    principalId: "prin-1",
    agentAddress: "ins_fallback@workflow.interchange",
    systemPrompt: "shared-prompt",
    tools: [],
    grants: [],
    sources: args.sources,
    defaultSource: args.defaultSource,
  };
}

function createRecordingDeps() {
  const launches: { agentId: string }[] = [];
  const launch: LaunchSessionFn = async (params) => {
    launches.push({ agentId: params.agentId });
  };
  const sources: Record<string, unknown>[] = [];
  const multiStep: SendMultiStepDeployFn = async (params) => {
    sources.push(params.sources);
    return { publicKey: "00".repeat(32) };
  };
  // A one-step workflow deploys once at the head via this hand-off; it
  // records the pinned sources exactly as the multi-step hand-off does so
  // the source-pin assertions read the same `sources` array regardless of
  // which branch the deploy took.
  const singleStep: DeploySingleStepFn = async (params) => {
    sources.push(params.sources);
    return { publicKey: "00".repeat(32) };
  };
  const repoWrites: { workflowRepoId: string }[] = [];
  const workflowRepo: WorkflowRepoWriter = {
    async writeWorkflowRepo(params) {
      repoWrites.push({ workflowRepoId: params.workflowRepoId });
    },
  };
  return {
    launches,
    launch,
    sources,
    multiStep,
    singleStep,
    workflowRepo,
    repoWrites,
  };
}

describe("pickStepInferenceSource (agent step)", () => {
  test("throws when the agent's preferred source is missing and the defaultSource's (provider, model) is not in the approved grants", async () => {
    const agent = makeAgent("ag1", "anthropic", "preferred-model");
    const workflow = makeWorkflow(agent);
    const directorRegistry = createDefaultDirectorRegistry();
    const deps = createRecordingDeps();
    const orchestrator = createWorkflowDeployOrchestrator({
      directorRegistry,
      workflowRepo: deps.workflowRepo,
      launchSession: deps.launch,
      sendMultiStepDeploy: deps.multiStep,
      deploySingleStepAtHead: deps.singleStep,
    });

    // HarnessConfig has only a default-pinned source for openai:default-model,
    // not the agent's preferred (anthropic, preferred-model).
    const config = makeConfig({
      sources: [
        {
          id: "src-default",
          provider: "openai",
          baseURL: "https://api.example/openai",
          apiKey: "secret",
          model: "default-model",
        },
      ],
      defaultSource: "src-default",
    });

    // Approvals cover the agent's preferred source (which the walk
    // emits as a grant) plus the tool and director grants -- but NOT
    // the defaultSource's (openai, default-model). This is the
    // capability-walk-bypass shape: the operator approved one
    // (provider, model), the orchestrator silently pins a different one.
    const approvals = new Set<string>([
      "tool:@intx/tools-mail/sidecar-bundle",
      `director:${defaultDirectorFactory.id}`,
      "inference.source:anthropic:preferred-model",
    ]);

    await expect(
      orchestrator.deployWorkflow({
        workflow,
        deploymentId: "dep_fallback",
        deploymentDomain: "workflow.interchange",
        config,
        deployContent: DEPLOY_CONTENT_BASE,
        hubPublicKey: "00".repeat(32),
        operatorApprovals: approvals,
      }),
    ).rejects.toBeInstanceOf(WorkflowDefinitionInvalidError);

    // The hand-off must not fire when source pinning is rejected.
    expect(deps.sources).toHaveLength(0);
  });

  test("uses the defaultSource when its (provider, model) is in the approved grants", async () => {
    const agent = makeAgent("ag1", "anthropic", "preferred-model");
    const workflow = makeWorkflow(agent);
    const directorRegistry = createDefaultDirectorRegistry();
    const deps = createRecordingDeps();
    const orchestrator = createWorkflowDeployOrchestrator({
      directorRegistry,
      workflowRepo: deps.workflowRepo,
      launchSession: deps.launch,
      sendMultiStepDeploy: deps.multiStep,
      deploySingleStepAtHead: deps.singleStep,
    });

    const config = makeConfig({
      sources: [
        {
          id: "src-default",
          provider: "openai",
          baseURL: "https://api.example/openai",
          apiKey: "secret",
          model: "default-model",
        },
      ],
      defaultSource: "src-default",
    });

    // The operator approved BOTH the agent's preferred shape (the walk
    // emits it) and the defaultSource's (provider, model). The agent's
    // preferred source does not resolve against HarnessConfig.sources,
    // so the orchestrator legitimately falls back to the default --
    // which is approved, so the deploy proceeds.
    const approvals = new Set<string>([
      "tool:@intx/tools-mail/sidecar-bundle",
      `director:${defaultDirectorFactory.id}`,
      "inference.source:anthropic:preferred-model",
      "inference.source:openai:default-model",
    ]);

    const result = await orchestrator.deployWorkflow({
      workflow,
      deploymentId: "dep_fallback",
      deploymentDomain: "workflow.interchange",
      config,
      deployContent: DEPLOY_CONTENT_BASE,
      hubPublicKey: "00".repeat(32),
      operatorApprovals: approvals,
    });

    expect(result.kind).toBe("multi-step");
    expect(deps.sources).toHaveLength(1);
    const sources = deps.sources[0];
    if (sources === undefined) throw new Error("missing sources");
    expect(sources.only).toEqual({
      id: "src-default",
      provider: "openai",
      baseURL: "https://api.example/openai",
      apiKey: "secret",
      model: "default-model",
    });
  });

  test("uses the agent's preferred source when it matches an approved HarnessConfig source", async () => {
    const agent = makeAgent("ag1", "anthropic", "preferred-model");
    const workflow = makeWorkflow(agent);
    const directorRegistry = createDefaultDirectorRegistry();
    const deps = createRecordingDeps();
    const orchestrator = createWorkflowDeployOrchestrator({
      directorRegistry,
      workflowRepo: deps.workflowRepo,
      launchSession: deps.launch,
      sendMultiStepDeploy: deps.multiStep,
      deploySingleStepAtHead: deps.singleStep,
    });

    const config = makeConfig({
      sources: [
        {
          id: "src-preferred",
          provider: "anthropic",
          baseURL: "https://api.example/anthropic",
          apiKey: "secret-a",
          model: "preferred-model",
        },
        {
          id: "src-other",
          provider: "openai",
          baseURL: "https://api.example/openai",
          apiKey: "secret-b",
          model: "other-model",
        },
      ],
      defaultSource: "src-other",
    });

    // The agent's preferred (provider, model) is approved AND resolves
    // against the deploy's HarnessConfig.sources -- the orchestrator
    // pins it directly without consulting the default.
    const approvals = new Set<string>([
      "tool:@intx/tools-mail/sidecar-bundle",
      `director:${defaultDirectorFactory.id}`,
      "inference.source:anthropic:preferred-model",
    ]);

    const result = await orchestrator.deployWorkflow({
      workflow,
      deploymentId: "dep_fallback",
      deploymentDomain: "workflow.interchange",
      config,
      deployContent: DEPLOY_CONTENT_BASE,
      hubPublicKey: "00".repeat(32),
      operatorApprovals: approvals,
    });

    expect(result.kind).toBe("multi-step");
    expect(deps.sources).toHaveLength(1);
    const sources = deps.sources[0];
    if (sources === undefined) throw new Error("missing sources");
    expect(sources.only).toEqual({
      id: "src-preferred",
      provider: "anthropic",
      baseURL: "https://api.example/anthropic",
      apiKey: "secret-a",
      model: "preferred-model",
    });
  });

  test("regression: unapproved fallback is also rejected at the approval gate when the walk surfaces it", async () => {
    // Confirms the approval gate continues to fail loudly when the
    // capability walk itself surfaces an unapproved grant, independent
    // of the source-pinning cross-check the orchestrator adds.
    const agent = makeAgent("ag1", "anthropic", "preferred-model");
    const workflow = makeWorkflow(agent);
    const directorRegistry = createDefaultDirectorRegistry();
    const deps = createRecordingDeps();
    const orchestrator = createWorkflowDeployOrchestrator({
      directorRegistry,
      workflowRepo: deps.workflowRepo,
      launchSession: deps.launch,
      sendMultiStepDeploy: deps.multiStep,
      deploySingleStepAtHead: deps.singleStep,
    });
    const config = makeConfig({
      sources: [
        {
          id: "src-default",
          provider: "openai",
          baseURL: "https://api.example/openai",
          apiKey: "secret",
          model: "default-model",
        },
      ],
      defaultSource: "src-default",
    });
    // No approvals at all -- the gate fails before the source pin runs.
    const approvals = new Set<string>();

    await expect(
      orchestrator.deployWorkflow({
        workflow,
        deploymentId: "dep_fallback",
        deploymentDomain: "workflow.interchange",
        config,
        deployContent: DEPLOY_CONTENT_BASE,
        hubPublicKey: "00".repeat(32),
        operatorApprovals: approvals,
      }),
    ).rejects.toBeInstanceOf(CapabilityApprovalDeniedError);
  });
});

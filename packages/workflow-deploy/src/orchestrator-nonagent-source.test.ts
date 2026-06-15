// Pins the non-agent-step contract for `pickStepInferenceSource`:
//
// Non-agent primitives (sleep, gate, awaitSignal, ...) carry no agent
// preference, so `pickStepInferenceSource` would fall through to the
// `HarnessConfig.defaultSource`. The capability walk emits NO
// `inference.source:<provider>:<model>` grant for these steps -- the
// walk only emits source grants from agent definitions. The
// orchestrator must not paper over that absence by pinning a source
// the operator never approved.
//
// Concrete shape: if the orchestrator pins a non-agent step to the
// defaultSource, the `(provider, model)` of that source must be in the
// operator-approved grants. Otherwise the deploy must fail loudly --
// silent fallback is the capability-walk bypass this test pins against.

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
  sleep,
  step,
  type WorkflowDefinition,
} from "@intx/workflow/definition";

import {
  createWorkflowDeployOrchestrator,
  WorkflowDefinitionInvalidError,
  type DeployContent,
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

function makeMixedWorkflow(
  agent: AgentDefinition<BaseEnv>,
): WorkflowDefinition {
  return defineWorkflow({
    id: "wf_nonagent",
    trigger: { type: "manual" },
    steps: {
      worker: step({ agent, after: [] }),
      cooldown: sleep({ duration: 1000, after: ["worker"] }),
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
    sessionId: "ses-nonagent",
    agentId: "ag_nonagent",
    tenantId: "tenant-1",
    principalId: "prin-1",
    agentAddress: "ins_nonagent@workflow.interchange",
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
  const repoWrites: { workflowRepoId: string }[] = [];
  const workflowRepo: WorkflowRepoWriter = {
    async writeWorkflowRepo(params) {
      repoWrites.push({ workflowRepoId: params.workflowRepoId });
    },
  };
  return { launches, launch, sources, multiStep, workflowRepo, repoWrites };
}

describe("pickStepInferenceSource (non-agent step)", () => {
  test("rejects pinning a sleep step to a defaultSource whose (provider, model) is not approved", async () => {
    const agent = makeAgent("worker", "anthropic", "worker-model");
    const workflow = makeMixedWorkflow(agent);
    const directorRegistry = createDefaultDirectorRegistry();
    const deps = createRecordingDeps();
    const orchestrator = createWorkflowDeployOrchestrator({
      directorRegistry,
      workflowRepo: deps.workflowRepo,
      launchSession: deps.launch,
      sendMultiStepDeploy: deps.multiStep,
    });

    // HarnessConfig carries TWO sources: the agent's preferred and a
    // distinct default. The agent step pins to the preferred one
    // (which the walk emits and the operator approved); the sleep
    // step has no preference and falls back to the default. The
    // default's (provider, model) is NOT in the approved set, so the
    // orchestrator must refuse to pin the sleep step.
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

    // Approve the agent's preferred (provider, model) -- so the walk
    // passes the gate -- the tool, and the director. But NOT the
    // default's (openai, default-model). The walk surfaces nothing
    // for the sleep step, so the gate accepts the deploy; the source
    // pin is the only place the unapproved fallback can be caught.
    const approvals = new Set<string>([
      "tool:@intx/tools-mail/sidecar-bundle",
      `director:${defaultDirectorFactory.id}`,
      "inference.source:anthropic:worker-model",
    ]);

    await expect(
      orchestrator.deployWorkflow({
        workflow,
        deploymentId: "dep_nonagent",
        deploymentDomain: "workflow.interchange",
        config,
        deployContent: DEPLOY_CONTENT_BASE,
        hubPublicKey: "00".repeat(32),
        operatorApprovals: approvals,
      }),
    ).rejects.toBeInstanceOf(WorkflowDefinitionInvalidError);

    // The hand-off must not fire when the non-agent source pin is rejected.
    expect(deps.sources).toHaveLength(0);
  });

  test("fails loudly when the only available source for a non-agent step is unapproved", async () => {
    // Workflow with only a sleep step (no agent step). The orchestrator
    // would otherwise silently pin the sleep step to the defaultSource;
    // here the default's (provider, model) is unapproved, so the deploy
    // must reject. This is the load-bearing assertion: a non-agent step
    // walks emit NO `inference.source:` grant, so absent the source-pin
    // cross-check the approval gate has nothing to fail on.
    const workflow = defineWorkflow({
      id: "wf_sleep_only",
      trigger: { type: "manual" },
      steps: {
        nap: sleep({ duration: 1000, after: [] }),
      },
    });
    const directorRegistry = createDefaultDirectorRegistry();
    const deps = createRecordingDeps();
    const orchestrator = createWorkflowDeployOrchestrator({
      directorRegistry,
      workflowRepo: deps.workflowRepo,
      launchSession: deps.launch,
      sendMultiStepDeploy: deps.multiStep,
    });

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

    // No approvals at all -- the walk emits no grants for the
    // single sleep step (no agent, no triggers besides `manual`),
    // so the gate accepts the deploy. The orchestrator's source-pin
    // cross-check is the only place the unapproved default can be
    // caught.
    const approvals = new Set<string>();

    await expect(
      orchestrator.deployWorkflow({
        workflow,
        deploymentId: "dep_nonagent",
        deploymentDomain: "workflow.interchange",
        config,
        deployContent: DEPLOY_CONTENT_BASE,
        hubPublicKey: "00".repeat(32),
        operatorApprovals: approvals,
      }),
    ).rejects.toBeInstanceOf(WorkflowDefinitionInvalidError);

    expect(deps.sources).toHaveLength(0);
  });

  test("allows pinning a non-agent step when the default's (provider, model) is approved", async () => {
    const agent = makeAgent("worker", "anthropic", "worker-model");
    const workflow = makeMixedWorkflow(agent);
    const directorRegistry = createDefaultDirectorRegistry();
    const deps = createRecordingDeps();
    const orchestrator = createWorkflowDeployOrchestrator({
      directorRegistry,
      workflowRepo: deps.workflowRepo,
      launchSession: deps.launch,
      sendMultiStepDeploy: deps.multiStep,
    });

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
    // The operator approved the (provider, model) of the source the
    // non-agent step would be pinned to, so the deploy proceeds.
    const approvals = new Set<string>([
      "tool:@intx/tools-mail/sidecar-bundle",
      `director:${defaultDirectorFactory.id}`,
      "inference.source:anthropic:worker-model",
    ]);

    const result = await orchestrator.deployWorkflow({
      workflow,
      deploymentId: "dep_nonagent",
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
    // Both steps land on the approved source.
    expect(sources.worker).toBeDefined();
    expect(sources.cooldown).toEqual({
      id: "src-anthropic",
      provider: "anthropic",
      baseURL: "https://api.example/anthropic",
      apiKey: "secret-a",
      model: "worker-model",
    });
  });
});

import { describe, test, expect } from "bun:test";

import { deriveWorkflowRunId } from "@intx/types";

import {
  createDefaultDirectorRegistry,
  createDirectorRegistry,
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
  deriveRunAddress,
  deriveRunAgentId,
  deriveStepAddress,
  deriveStepAgentId,
  deriveStepRunId,
  deriveWorkflowRunRepoId,
  MultiStepDeployHandoffMissingError,
  MultiStepDeploymentArgsMissingError,
  SingleStepDeployHandoffMissingError,
  WorkflowDefinitionInvalidError,
  wrapHarnessAsSingleStepWorkflow,
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
    // The walk keys `tool:` grants on each declared definition name, so
    // the fixture must declare a real tool or it contributes no tool
    // grant and the approval assertions pass vacuously.
    definitions: [{ name: "mail_send" }],
  });
}

function makeAgent(
  id: string,
  systemPrompt = `you are ${id}`,
): AgentDefinition<BaseEnv> {
  return defineAgent({
    id,
    systemPrompt,
    tools: [makeMailFactory()],
    capabilities: [],
    inference: {
      sources: [{ provider: "anthropic", model: "mock-model" }],
    },
  });
}

function makeSingleStepWorkflow(
  agent: AgentDefinition<BaseEnv>,
): WorkflowDefinition {
  return defineWorkflow({
    id: "wf_trivial",
    agent,
    trigger: { type: "mail", to: "run_legacy-agent@integration.interchange" },
  });
}

function makeMultiStepWorkflow(): WorkflowDefinition {
  return defineWorkflow({
    id: "wf_multi",
    trigger: { type: "manual" },
    steps: {
      plan: step({ agent: makeAgent("plan", "you plan"), after: [] }),
      execute: step({
        agent: makeAgent("execute", "you execute"),
        after: ["plan"],
      }),
    },
  });
}

const HARNESS_CONFIG_BASE: HarnessConfig = {
  sessionId: "ses-1",
  agentId: "legacy-agent",
  tenantId: "tenant-1",
  principalId: "prin-1",
  agentAddress: "run_legacy-agent@integration.interchange",
  systemPrompt: "legacy-prompt",
  tools: [],
  grants: [],
  sources: [
    {
      id: "src-anthropic-1",
      provider: "anthropic",
      baseURL: "https://api.example/anthropic",
      apiKey: "secret-key",
      model: "mock-model",
    },
  ],
  defaultSource: "src-anthropic-1",
};

const DEPLOY_CONTENT_BASE: DeployContent = {
  systemPrompt: "legacy-prompt",
};

function approvedGrantsForWorkflow(
  workflow: WorkflowDefinition,
  agents: readonly AgentDefinition<BaseEnv>[],
): Set<string> {
  const approvals = new Set<string>();
  for (const agent of agents) {
    for (const factory of agent.toolFactories) {
      for (const definition of factory.definitions) {
        approvals.add(`tool:${definition.name}`);
      }
    }
    for (const capability of agent.capabilities) {
      approvals.add(`capability:${capability}`);
    }
    for (const source of agent.inference.sources) {
      approvals.add(`inference.source:${source.provider}:${source.model}`);
    }
  }
  approvals.add(`director:${defaultDirectorFactory.id}`);
  for (const trigger of workflow.triggers) {
    if (trigger.type === "mail") {
      approvals.add(`mail.address:${trigger.to}`);
      const at = trigger.to.lastIndexOf("@");
      if (at >= 0 && at < trigger.to.length - 1) {
        approvals.add(`mail.send:${trigger.to.slice(at + 1)}`);
      }
    }
  }
  return approvals;
}

type RecordedWorkflowRepoWrite = {
  workflowRepoId: string;
  files: Map<string, string>;
};

function createRecordingWorkflowRepoWriter(): WorkflowRepoWriter & {
  writes: RecordedWorkflowRepoWrite[];
} {
  const writes: RecordedWorkflowRepoWrite[] = [];
  return {
    writes,
    async writeWorkflowRepo(args) {
      writes.push({
        workflowRepoId: args.workflowRepoId,
        files: new Map(args.files),
      });
    },
  };
}

type RecordedLaunch = {
  agentAddress: string;
  agentId: string;
  runId: string;
  config: HarnessConfig;
  deployContent: DeployContent;
  toolPackagePins?: readonly unknown[];
};

function createRecordingLaunch(): {
  fn: LaunchSessionFn;
  launches: RecordedLaunch[];
} {
  const launches: RecordedLaunch[] = [];
  const fn: LaunchSessionFn = async (params) => {
    launches.push({
      agentAddress: params.agentAddress,
      agentId: params.agentId,
      runId: params.runId,
      config: params.config,
      deployContent: params.deployContent,
      ...(params.toolPackagePins !== undefined
        ? { toolPackagePins: params.toolPackagePins }
        : {}),
    });
  };
  return { fn, launches };
}

type RecordedMultiStepDeploy = Parameters<SendMultiStepDeployFn>[0];

function createRecordingMultiStepDeploy(publicKey = "ff".repeat(32)): {
  fn: SendMultiStepDeployFn;
  calls: RecordedMultiStepDeploy[];
} {
  const calls: RecordedMultiStepDeploy[] = [];
  const fn: SendMultiStepDeployFn = async (params) => {
    calls.push(params);
    return { publicKey };
  };
  return { fn, calls };
}

type RecordedSingleStepDeploy = Parameters<DeploySingleStepFn>[0];

function createRecordingSingleStepDeploy(publicKey = "ff".repeat(32)): {
  fn: DeploySingleStepFn;
  calls: RecordedSingleStepDeploy[];
} {
  const calls: RecordedSingleStepDeploy[] = [];
  const fn: DeploySingleStepFn = async (params) => {
    calls.push(params);
    return { publicKey };
  };
  return { fn, calls };
}

describe("createWorkflowDeployOrchestrator", () => {
  describe("deploy provisioning", () => {
    test("passes through toolPackagePins to every step launch", async () => {
      const workflow = makeMultiStepWorkflow();
      const planAgent = workflow.steps.plan;
      const executeAgent = workflow.steps.execute;
      if (planAgent?.kind !== "step" || executeAgent?.kind !== "step") {
        throw new Error("expected both steps to be step primitives");
      }
      const directorRegistry = createDefaultDirectorRegistry();
      const workflowRepo = createRecordingWorkflowRepoWriter();
      const launch = createRecordingLaunch();
      const multiStep = createRecordingMultiStepDeploy();
      const orchestrator = createWorkflowDeployOrchestrator({
        directorRegistry,
        workflowRepo,
        launchSession: launch.fn,
        sendMultiStepDeploy: multiStep.fn,
      });
      const approvals = approvedGrantsForWorkflow(workflow, [
        planAgent.agent,
        executeAgent.agent,
      ]);
      const pins = [{ name: "@vendor/pkg", version: "1.0.0" }] as const;

      await orchestrator.deployWorkflow({
        workflow,
        runId: "dep_pins",
        deploymentDomain: "workflow.interchange",
        config: HARNESS_CONFIG_BASE,
        deployContent: DEPLOY_CONTENT_BASE,
        hubPublicKey: "00".repeat(32),
        toolPackagePins: pins,
        operatorApprovals: approvals,
      });

      expect(launch.launches).toHaveLength(2);
      for (const launched of launch.launches) {
        expect(launched.toolPackagePins).toEqual(pins);
      }
    });

    test("writes the workflow repo before launching any step", async () => {
      const workflow = makeMultiStepWorkflow();
      const planAgent = workflow.steps.plan;
      const executeAgent = workflow.steps.execute;
      if (planAgent?.kind !== "step" || executeAgent?.kind !== "step") {
        throw new Error("expected both steps to be step primitives");
      }
      const directorRegistry = createDefaultDirectorRegistry();
      const order: string[] = [];
      const launch: LaunchSessionFn = async () => {
        order.push("launch");
      };
      const recordingRepo: WorkflowRepoWriter = {
        async writeWorkflowRepo(args) {
          order.push("repo");
          expect(args.files.has("workflow.json")).toBe(true);
          expect(args.files.has("capability-declarations.json")).toBe(true);
          expect(args.files.has(".gitignore")).toBe(true);
        },
      };
      const multiStep = createRecordingMultiStepDeploy();
      const orchestrator = createWorkflowDeployOrchestrator({
        directorRegistry,
        workflowRepo: recordingRepo,
        launchSession: launch,
        sendMultiStepDeploy: multiStep.fn,
      });
      const approvals = approvedGrantsForWorkflow(workflow, [
        planAgent.agent,
        executeAgent.agent,
      ]);

      await orchestrator.deployWorkflow({
        workflow,
        runId: "dep_order",
        deploymentDomain: "workflow.interchange",
        config: HARNESS_CONFIG_BASE,
        deployContent: DEPLOY_CONTENT_BASE,
        hubPublicKey: "00".repeat(32),
        operatorApprovals: approvals,
      });

      // The workflow repo lands before any per-step agent-state write.
      expect(order).toEqual(["repo", "launch", "launch"]);
    });
  });

  describe("multi-step branch", () => {
    test("derives per-step addresses and launches in stepOrder", async () => {
      const workflow = makeMultiStepWorkflow();
      const planAgent = workflow.steps.plan;
      const executeAgent = workflow.steps.execute;
      if (planAgent?.kind !== "step" || executeAgent?.kind !== "step") {
        throw new Error("expected both steps to be step primitives");
      }
      const directorRegistry = createDefaultDirectorRegistry();
      const workflowRepo = createRecordingWorkflowRepoWriter();
      const launch = createRecordingLaunch();
      const multiStep = createRecordingMultiStepDeploy();
      const orchestrator = createWorkflowDeployOrchestrator({
        directorRegistry,
        workflowRepo,
        launchSession: launch.fn,
        sendMultiStepDeploy: multiStep.fn,
      });
      const approvals = approvedGrantsForWorkflow(workflow, [
        planAgent.agent,
        executeAgent.agent,
      ]);

      const result = await orchestrator.deployWorkflow({
        workflow,
        runId: "dep_abc123",
        deploymentDomain: "workflow.interchange",
        config: HARNESS_CONFIG_BASE,
        deployContent: DEPLOY_CONTENT_BASE,
        hubPublicKey: "00".repeat(32),
        operatorApprovals: approvals,
      });

      expect(launch.launches).toHaveLength(2);
      const [planLaunch, executeLaunch] = launch.launches;
      if (planLaunch === undefined || executeLaunch === undefined) {
        throw new Error("missing launches");
      }
      expect(planLaunch.agentAddress).toBe(
        "dep_abc123-plan@workflow.interchange",
      );
      expect(planLaunch.agentId).toBe("dep_abc123-plan");
      expect(planLaunch.runId).toBe("dep_abc123-plan");
      expect(planLaunch.config.agentAddress).toBe(planLaunch.agentAddress);
      expect(planLaunch.config.agentId).toBe(planLaunch.agentId);
      expect(planLaunch.config.systemPrompt).toBe("you plan");
      expect(planLaunch.deployContent.systemPrompt).toBe("you plan");

      expect(executeLaunch.agentAddress).toBe(
        "dep_abc123-execute@workflow.interchange",
      );
      expect(executeLaunch.agentId).toBe("dep_abc123-execute");
      expect(executeLaunch.runId).toBe("dep_abc123-execute");
      expect(executeLaunch.config.systemPrompt).toBe("you execute");
      expect(executeLaunch.deployContent.systemPrompt).toBe("you execute");

      expect(multiStep.calls).toHaveLength(1);
      const handoff = multiStep.calls[0];
      if (handoff === undefined) throw new Error("missing handoff");
      expect(handoff.agentAddress).toBe("dep_abc123@workflow.interchange");
      expect(handoff.agentId).toBe("dep_abc123");
      expect(handoff.hubPublicKey).toBe("00".repeat(32));
      expect(handoff.definition).toBe(workflow);
      expect(Object.keys(handoff.sources).sort()).toEqual(["execute", "plan"]);
      const baseSource = HARNESS_CONFIG_BASE.sources[0];
      if (baseSource === undefined) throw new Error("missing base source");
      // Each step pins a single-element failover chain: the multi-step branch
      // wraps its one picked source in a list.
      expect(handoff.sources.plan).toEqual([baseSource]);
      expect(handoff.sources.execute).toEqual([baseSource]);
      expect(result).toEqual({
        publicKey: "ff".repeat(32),
      });
    });

    test("calls sendMultiStepDeploy exactly once after the per-step launches", async () => {
      const workflow = makeMultiStepWorkflow();
      const planAgent = workflow.steps.plan;
      const executeAgent = workflow.steps.execute;
      if (planAgent?.kind !== "step" || executeAgent?.kind !== "step") {
        throw new Error("expected both steps to be step primitives");
      }
      const directorRegistry = createDefaultDirectorRegistry();
      const workflowRepo = createRecordingWorkflowRepoWriter();
      const order: string[] = [];
      const launch: LaunchSessionFn = async (params) => {
        order.push(`launch:${params.agentId}`);
      };
      const multiStep: SendMultiStepDeployFn = async () => {
        order.push("sendMultiStepDeploy");
        return { publicKey: "ab".repeat(32) };
      };
      const orchestrator = createWorkflowDeployOrchestrator({
        directorRegistry,
        workflowRepo,
        launchSession: launch,
        sendMultiStepDeploy: multiStep,
      });
      const approvals = approvedGrantsForWorkflow(workflow, [
        planAgent.agent,
        executeAgent.agent,
      ]);

      await orchestrator.deployWorkflow({
        workflow,
        runId: "dep_xy",
        deploymentDomain: "workflow.interchange",
        config: HARNESS_CONFIG_BASE,
        deployContent: DEPLOY_CONTENT_BASE,
        hubPublicKey: "00".repeat(32),
        operatorApprovals: approvals,
      });

      // The hand-off fires exactly once and only after the per-step
      // provisioning loop has finished.
      expect(order).toEqual([
        "launch:dep_xy-plan",
        "launch:dep_xy-execute",
        "sendMultiStepDeploy",
      ]);
    });

    test("throws when hubPublicKey is missing on the multi-step branch", async () => {
      const workflow = makeMultiStepWorkflow();
      const planAgent = workflow.steps.plan;
      const executeAgent = workflow.steps.execute;
      if (planAgent?.kind !== "step" || executeAgent?.kind !== "step") {
        throw new Error("expected step primitives");
      }
      const directorRegistry = createDefaultDirectorRegistry();
      const workflowRepo = createRecordingWorkflowRepoWriter();
      const launch = createRecordingLaunch();
      const multiStep = createRecordingMultiStepDeploy();
      const orchestrator = createWorkflowDeployOrchestrator({
        directorRegistry,
        workflowRepo,
        launchSession: launch.fn,
        sendMultiStepDeploy: multiStep.fn,
      });
      const approvals = approvedGrantsForWorkflow(workflow, [
        planAgent.agent,
        executeAgent.agent,
      ]);

      await expect(
        orchestrator.deployWorkflow({
          workflow,
          runId: "dep_abc123",
          deploymentDomain: "workflow.interchange",
          config: HARNESS_CONFIG_BASE,
          deployContent: DEPLOY_CONTENT_BASE,
          operatorApprovals: approvals,
        }),
      ).rejects.toBeInstanceOf(MultiStepDeploymentArgsMissingError);
    });

    test("throws when sendMultiStepDeploy dep is unwired", async () => {
      const workflow = makeMultiStepWorkflow();
      const planAgent = workflow.steps.plan;
      const executeAgent = workflow.steps.execute;
      if (planAgent?.kind !== "step" || executeAgent?.kind !== "step") {
        throw new Error("expected step primitives");
      }
      const directorRegistry = createDefaultDirectorRegistry();
      const workflowRepo = createRecordingWorkflowRepoWriter();
      const launch = createRecordingLaunch();
      const orchestrator = createWorkflowDeployOrchestrator({
        directorRegistry,
        workflowRepo,
        launchSession: launch.fn,
      });
      const approvals = approvedGrantsForWorkflow(workflow, [
        planAgent.agent,
        executeAgent.agent,
      ]);

      await expect(
        orchestrator.deployWorkflow({
          workflow,
          runId: "dep_abc123",
          deploymentDomain: "workflow.interchange",
          config: HARNESS_CONFIG_BASE,
          deployContent: DEPLOY_CONTENT_BASE,
          hubPublicKey: "00".repeat(32),
          operatorApprovals: approvals,
        }),
      ).rejects.toBeInstanceOf(MultiStepDeployHandoffMissingError);
    });

    test("throws when deploySingleStepAtHead dep is unwired", async () => {
      const agent = makeAgent("only");
      const workflow = makeSingleStepWorkflow(agent);
      const directorRegistry = createDefaultDirectorRegistry();
      const workflowRepo = createRecordingWorkflowRepoWriter();
      const launch = createRecordingLaunch();
      const orchestrator = createWorkflowDeployOrchestrator({
        directorRegistry,
        workflowRepo,
        launchSession: launch.fn,
      });
      const approvals = approvedGrantsForWorkflow(workflow, [agent]);

      await expect(
        orchestrator.deployWorkflow({
          workflow,
          runId: "dep_xyz",
          deploymentDomain: "workflow.interchange",
          config: HARNESS_CONFIG_BASE,
          deployContent: DEPLOY_CONTENT_BASE,
          hubPublicKey: "00".repeat(32),
          operatorApprovals: approvals,
        }),
      ).rejects.toBeInstanceOf(SingleStepDeployHandoffMissingError);
    });

    test("throws when anchorRunId is missing", async () => {
      const workflow = makeMultiStepWorkflow();
      const planAgent = workflow.steps.plan;
      const executeAgent = workflow.steps.execute;
      if (planAgent?.kind !== "step" || executeAgent?.kind !== "step") {
        throw new Error("expected step primitives");
      }
      const directorRegistry = createDefaultDirectorRegistry();
      const workflowRepo = createRecordingWorkflowRepoWriter();
      const launch = createRecordingLaunch();
      const orchestrator = createWorkflowDeployOrchestrator({
        directorRegistry,
        workflowRepo,
        launchSession: launch.fn,
      });
      const approvals = approvedGrantsForWorkflow(workflow, [
        planAgent.agent,
        executeAgent.agent,
      ]);

      await expect(
        orchestrator.deployWorkflow({
          workflow,
          deploymentDomain: "workflow.interchange",
          config: HARNESS_CONFIG_BASE,
          deployContent: DEPLOY_CONTENT_BASE,
          operatorApprovals: approvals,
        }),
      ).rejects.toBeInstanceOf(MultiStepDeploymentArgsMissingError);
    });

    test("throws when deploymentDomain is missing", async () => {
      const workflow = makeMultiStepWorkflow();
      const planAgent = workflow.steps.plan;
      const executeAgent = workflow.steps.execute;
      if (planAgent?.kind !== "step" || executeAgent?.kind !== "step") {
        throw new Error("expected step primitives");
      }
      const directorRegistry = createDefaultDirectorRegistry();
      const workflowRepo = createRecordingWorkflowRepoWriter();
      const launch = createRecordingLaunch();
      const orchestrator = createWorkflowDeployOrchestrator({
        directorRegistry,
        workflowRepo,
        launchSession: launch.fn,
      });
      const approvals = approvedGrantsForWorkflow(workflow, [
        planAgent.agent,
        executeAgent.agent,
      ]);

      await expect(
        orchestrator.deployWorkflow({
          workflow,
          runId: "dep_abc123",
          config: HARNESS_CONFIG_BASE,
          deployContent: DEPLOY_CONTENT_BASE,
          operatorApprovals: approvals,
        }),
      ).rejects.toBeInstanceOf(MultiStepDeploymentArgsMissingError);
    });

    test("single-step workflow deploys once at the head", async () => {
      const agent = makeAgent("only");
      const workflow = makeSingleStepWorkflow(agent);
      const directorRegistry = createDefaultDirectorRegistry();
      const workflowRepo = createRecordingWorkflowRepoWriter();
      const launch = createRecordingLaunch();
      const multiStep = createRecordingMultiStepDeploy();
      const singleStep = createRecordingSingleStepDeploy();
      const orchestrator = createWorkflowDeployOrchestrator({
        directorRegistry,
        workflowRepo,
        launchSession: launch.fn,
        sendMultiStepDeploy: multiStep.fn,
        deploySingleStepAtHead: singleStep.fn,
      });
      const approvals = approvedGrantsForWorkflow(workflow, [agent]);

      const result = await orchestrator.deployWorkflow({
        workflow,
        runId: "dep_xyz",
        deploymentDomain: "workflow.interchange",
        config: HARNESS_CONFIG_BASE,
        deployContent: DEPLOY_CONTENT_BASE,
        hubPublicKey: "00".repeat(32),
        operatorApprovals: approvals,
      });

      // A one-step workflow deploys ONCE at the head: the lone step
      // collapses onto the deployment (head) address, so there is no
      // per-step `launchSession` and no separate multi-step frame.
      expect(launch.launches).toHaveLength(0);
      expect(multiStep.calls).toHaveLength(0);
      expect(singleStep.calls).toHaveLength(1);
      const call = singleStep.calls[0];
      if (call === undefined) throw new Error("missing single-step deploy");
      expect(call.agentAddress).toBe("dep_xyz@workflow.interchange");
      expect(call.agentId).toBe("dep_xyz");
      expect(call.runId).toBe("dep_xyz");
      const expectedStepId = workflow.stepOrder[0];
      if (expectedStepId === undefined) {
        throw new Error("missing step id");
      }
      expect(call.sources[expectedStepId]).toBeDefined();
      expect(result.publicKey).toMatch(/^[0-9a-f]{64}$/);
    });

    test("single-step workflow pins the full ordered source chain", async () => {
      const agent = makeAgent("only");
      const workflow = makeSingleStepWorkflow(agent);
      const directorRegistry = createDefaultDirectorRegistry();
      const workflowRepo = createRecordingWorkflowRepoWriter();
      const launch = createRecordingLaunch();
      const multiStep = createRecordingMultiStepDeploy();
      const singleStep = createRecordingSingleStepDeploy();
      const orchestrator = createWorkflowDeployOrchestrator({
        directorRegistry,
        workflowRepo,
        launchSession: launch.fn,
        sendMultiStepDeploy: multiStep.fn,
        deploySingleStepAtHead: singleStep.fn,
      });
      // A two-element failover chain. Both elements share the agent's declared
      // (provider, model), so the single `inference.source:anthropic:mock-model`
      // approval covers both; the head is element 0 and equals defaultSource.
      const chain = [
        {
          id: "src-head",
          provider: "anthropic",
          baseURL: "https://api.example/head",
          apiKey: "secret-head",
          model: "mock-model",
        },
        {
          id: "src-tail",
          provider: "anthropic",
          baseURL: "https://api.example/tail",
          apiKey: "secret-tail",
          model: "mock-model",
        },
      ];
      const config: HarnessConfig = {
        ...HARNESS_CONFIG_BASE,
        sources: chain,
        defaultSource: "src-head",
      };
      const approvals = approvedGrantsForWorkflow(workflow, [agent]);

      await orchestrator.deployWorkflow({
        workflow,
        runId: "dep_chain",
        deploymentDomain: "workflow.interchange",
        config,
        deployContent: DEPLOY_CONTENT_BASE,
        hubPublicKey: "00".repeat(32),
        operatorApprovals: approvals,
      });

      expect(singleStep.calls).toHaveLength(1);
      const call = singleStep.calls[0];
      if (call === undefined) throw new Error("missing single-step deploy");
      const stepId = workflow.stepOrder[0];
      if (stepId === undefined) throw new Error("missing step id");
      // The whole ordered chain is pinned, not collapsed to a single source, so
      // the reactor fails over forward across it -- whole-workflow failover
      // matching the instance deploy path.
      expect(call.sources[stepId]).toEqual(chain);
    });

    test("single-step deploy rejects a chain source the operator never approved", async () => {
      const agent = makeAgent("only");
      const workflow = makeSingleStepWorkflow(agent);
      const directorRegistry = createDefaultDirectorRegistry();
      const workflowRepo = createRecordingWorkflowRepoWriter();
      const launch = createRecordingLaunch();
      const singleStep = createRecordingSingleStepDeploy();
      const orchestrator = createWorkflowDeployOrchestrator({
        directorRegistry,
        workflowRepo,
        launchSession: launch.fn,
        deploySingleStepAtHead: singleStep.fn,
      });
      // The head is approved and is the default; the tail is a (provider, model)
      // the agent never declared, so the walk never approved it. The gate must
      // reject the whole deploy rather than silently drop the unapproved tail.
      const config: HarnessConfig = {
        ...HARNESS_CONFIG_BASE,
        sources: [
          {
            id: "src-head",
            provider: "anthropic",
            baseURL: "https://api.example/head",
            apiKey: "secret-head",
            model: "mock-model",
          },
          {
            id: "src-rogue",
            provider: "openai",
            baseURL: "https://api.example/rogue",
            apiKey: "secret-rogue",
            model: "gpt-unapproved",
          },
        ],
        defaultSource: "src-head",
      };
      const approvals = approvedGrantsForWorkflow(workflow, [agent]);

      let caught: unknown;
      try {
        await orchestrator.deployWorkflow({
          workflow,
          runId: "dep_rogue",
          deploymentDomain: "workflow.interchange",
          config,
          deployContent: DEPLOY_CONTENT_BASE,
          hubPublicKey: "00".repeat(32),
          operatorApprovals: approvals,
        });
      } catch (err) {
        caught = err;
      }
      expect(caught).toBeInstanceOf(WorkflowDefinitionInvalidError);
      if (!(caught instanceof WorkflowDefinitionInvalidError)) {
        throw new Error("unreachable");
      }
      expect(caught.workflowId).toBe(workflow.id);
      expect(caught.message).toContain("openai");
      expect(caught.message).toContain("gpt-unapproved");
      // Rejected before any provisioning.
      expect(singleStep.calls).toHaveLength(0);
      expect(launch.launches).toHaveLength(0);
    });

    test("single-step deploy rejects a chain whose head is not the default source", async () => {
      const agent = makeAgent("only");
      const workflow = makeSingleStepWorkflow(agent);
      const directorRegistry = createDefaultDirectorRegistry();
      const workflowRepo = createRecordingWorkflowRepoWriter();
      const launch = createRecordingLaunch();
      const singleStep = createRecordingSingleStepDeploy();
      const orchestrator = createWorkflowDeployOrchestrator({
        directorRegistry,
        workflowRepo,
        launchSession: launch.fn,
        deploySingleStepAtHead: singleStep.fn,
      });
      // Both sources are approved (same provider+model), but defaultSource
      // points at element 1, not the head. The reactor activates element 0 and
      // fails over forward, so an inverted chain is rejected rather than
      // silently reordered.
      const config: HarnessConfig = {
        ...HARNESS_CONFIG_BASE,
        sources: [
          {
            id: "src-head",
            provider: "anthropic",
            baseURL: "https://api.example/head",
            apiKey: "secret-head",
            model: "mock-model",
          },
          {
            id: "src-second",
            provider: "anthropic",
            baseURL: "https://api.example/second",
            apiKey: "secret-second",
            model: "mock-model",
          },
        ],
        defaultSource: "src-second",
      };
      const approvals = approvedGrantsForWorkflow(workflow, [agent]);

      let caught: unknown;
      try {
        await orchestrator.deployWorkflow({
          workflow,
          runId: "dep_inverted",
          deploymentDomain: "workflow.interchange",
          config,
          deployContent: DEPLOY_CONTENT_BASE,
          hubPublicKey: "00".repeat(32),
          operatorApprovals: approvals,
        });
      } catch (err) {
        caught = err;
      }
      expect(caught).toBeInstanceOf(WorkflowDefinitionInvalidError);
      if (!(caught instanceof WorkflowDefinitionInvalidError)) {
        throw new Error("unreachable");
      }
      expect(caught.workflowId).toBe(workflow.id);
      expect(caught.message).toContain("src-second");
      expect(singleStep.calls).toHaveLength(0);
      expect(launch.launches).toHaveLength(0);
    });

    test("single-step deploy threads the step agent's tool package pins to the child", async () => {
      const pins = [{ name: "@intx/tools-posix", version: "1.2.3" }];
      // A folded step agent carries its tool pins on the definition (the
      // self-contained home under the workflow model), rather than relying on
      // pins supplied only at deploy time.
      const agent: AgentDefinition<BaseEnv> = {
        id: "ag_pinned",
        systemPrompt: "pinned agent",
        toolFactories: [],
        capabilities: [],
        inference: {
          sources: [{ provider: "anthropic", model: "mock-model" }],
        },
        toolPackagePins: pins,
      };
      const workflow = makeSingleStepWorkflow(agent);
      const directorRegistry = createDefaultDirectorRegistry();
      const workflowRepo = createRecordingWorkflowRepoWriter();
      const launch = createRecordingLaunch();
      const singleStep = createRecordingSingleStepDeploy();
      const orchestrator = createWorkflowDeployOrchestrator({
        directorRegistry,
        workflowRepo,
        launchSession: launch.fn,
        deploySingleStepAtHead: singleStep.fn,
      });
      const approvals = approvedGrantsForWorkflow(workflow, [agent]);

      await orchestrator.deployWorkflow({
        workflow,
        runId: "dep_pinned",
        deploymentDomain: "workflow.interchange",
        config: HARNESS_CONFIG_BASE,
        deployContent: DEPLOY_CONTENT_BASE,
        hubPublicKey: "00".repeat(32),
        operatorApprovals: approvals,
      });

      expect(singleStep.calls).toHaveLength(1);
      const call = singleStep.calls[0];
      if (call === undefined) throw new Error("missing single-step deploy");
      expect(call.toolPackagePins).toEqual(pins);
    });

    test("source-pin failure carries workflow.id and names the offending provider+model", async () => {
      const workflow = makeMultiStepWorkflow();
      const planAgent = workflow.steps.plan;
      const executeAgent = workflow.steps.execute;
      if (planAgent?.kind !== "step" || executeAgent?.kind !== "step") {
        throw new Error("expected both steps to be step primitives");
      }
      // HarnessConfig lists `anthropic:mock-model`; agents prefer the
      // same. Strip the source so neither the preferred nor the
      // defaultSource resolves; the pin must reject with the
      // workflow id and an error message that names the offending
      // `(provider, model)`.
      const configMissingSource: HarnessConfig = {
        ...HARNESS_CONFIG_BASE,
        sources: [],
        defaultSource: "src-missing",
      };
      const directorRegistry = createDefaultDirectorRegistry();
      const workflowRepo = createRecordingWorkflowRepoWriter();
      const launch = createRecordingLaunch();
      const multiStep = createRecordingMultiStepDeploy();
      const orchestrator = createWorkflowDeployOrchestrator({
        directorRegistry,
        workflowRepo,
        launchSession: launch.fn,
        sendMultiStepDeploy: multiStep.fn,
      });
      const approvals = approvedGrantsForWorkflow(workflow, [
        planAgent.agent,
        executeAgent.agent,
      ]);

      let caught: unknown;
      try {
        await orchestrator.deployWorkflow({
          workflow,
          runId: "dep_pinfail",
          deploymentDomain: "workflow.interchange",
          config: configMissingSource,
          deployContent: DEPLOY_CONTENT_BASE,
          hubPublicKey: "00".repeat(32),
          operatorApprovals: approvals,
        });
      } catch (err) {
        caught = err;
      }
      expect(caught).toBeInstanceOf(WorkflowDefinitionInvalidError);
      if (!(caught instanceof WorkflowDefinitionInvalidError)) {
        throw new Error("unreachable");
      }
      expect(caught.workflowId).toBe(workflow.id);
      const preferred = planAgent.agent.inference.sources[0];
      if (preferred === undefined) {
        throw new Error("missing preferred source");
      }
      expect(caught.message).toContain(preferred.provider);
      expect(caught.message).toContain(preferred.model);
      // Pin happens before any launch; the failed deploy must not have
      // provisioned an agent-state repo at the sidecar.
      expect(launch.launches).toHaveLength(0);
      expect(multiStep.calls).toHaveLength(0);
    });
  });

  describe("approval failures", () => {
    test("unapproved grant throws CapabilityApprovalDeniedError naming the step", async () => {
      const agent = makeAgent("legacy-agent");
      const workflow = makeSingleStepWorkflow(agent);
      const directorRegistry = createDefaultDirectorRegistry();
      const workflowRepo = createRecordingWorkflowRepoWriter();
      const launch = createRecordingLaunch();
      const orchestrator = createWorkflowDeployOrchestrator({
        directorRegistry,
        workflowRepo,
        launchSession: launch.fn,
      });

      const incompleteApprovals = approvedGrantsForWorkflow(workflow, [agent]);
      incompleteApprovals.delete("tool:mail_send");

      let captured: CapabilityApprovalDeniedError | undefined;
      try {
        await orchestrator.deployWorkflow({
          workflow,
          runId: "dep_legacy",
          deploymentDomain: "workflow.interchange",
          config: HARNESS_CONFIG_BASE,
          deployContent: DEPLOY_CONTENT_BASE,
          operatorApprovals: incompleteApprovals,
        });
      } catch (err) {
        if (!(err instanceof CapabilityApprovalDeniedError)) throw err;
        captured = err;
      }
      expect(captured).toBeInstanceOf(CapabilityApprovalDeniedError);
      expect(captured?.pending.size).toBe(1);
      const stepId = workflow.stepOrder[0];
      if (stepId === undefined) throw new Error("missing step id");
      expect(captured?.pending.get(stepId)).toEqual(["tool:mail_send"]);
      expect(launch.launches).toHaveLength(0);
      expect(workflowRepo.writes).toHaveLength(0);
    });

    test("zero approved sources throws with the offending step and missing source", async () => {
      const agent = makeAgent("legacy-agent");
      const workflow = makeSingleStepWorkflow(agent);
      const directorRegistry = createDefaultDirectorRegistry();
      const workflowRepo = createRecordingWorkflowRepoWriter();
      const launch = createRecordingLaunch();
      const orchestrator = createWorkflowDeployOrchestrator({
        directorRegistry,
        workflowRepo,
        launchSession: launch.fn,
      });

      let captured: CapabilityApprovalDeniedError | undefined;
      try {
        await orchestrator.deployWorkflow({
          workflow,
          runId: "dep_legacy",
          deploymentDomain: "workflow.interchange",
          config: HARNESS_CONFIG_BASE,
          deployContent: DEPLOY_CONTENT_BASE,
          operatorApprovals: new Set(),
        });
      } catch (err) {
        if (!(err instanceof CapabilityApprovalDeniedError)) throw err;
        captured = err;
      }
      expect(captured).toBeInstanceOf(CapabilityApprovalDeniedError);
      const stepId = workflow.stepOrder[0];
      if (stepId === undefined) throw new Error("missing step id");
      const missing = captured?.pending.get(stepId);
      expect(missing).toBeDefined();
      expect(missing?.length).toBeGreaterThan(0);
      expect(missing).toContain("inference.source:anthropic:mock-model");
      expect(launch.launches).toHaveLength(0);
    });

    test("unresolvable director surfaces with the expected error shape", async () => {
      const emptyRegistry = createDirectorRegistry({
        factories: [defaultDirectorFactory],
        defaultId: defaultDirectorFactory.id,
      });
      const agentWithMissingDirector: AgentDefinition<BaseEnv> = {
        id: "ag_unresolved",
        systemPrompt: "agent with missing director",
        director: { id: "@vendor/missing/director", config: {} },
        toolFactories: [makeMailFactory()],
        capabilities: [],
        inference: {
          sources: [{ provider: "anthropic", model: "mock-model" }],
        },
      };
      const workflow = defineWorkflow({
        id: "wf_unresolved",
        agent: agentWithMissingDirector,
        trigger: { type: "manual" },
      });
      const workflowRepo = createRecordingWorkflowRepoWriter();
      const launch = createRecordingLaunch();
      const orchestrator = createWorkflowDeployOrchestrator({
        directorRegistry: emptyRegistry,
        workflowRepo,
        launchSession: launch.fn,
      });
      const broad = new Set<string>([
        "tool:mail_send",
        "inference.source:anthropic:mock-model",
      ]);

      let captured: CapabilityApprovalDeniedError | undefined;
      try {
        await orchestrator.deployWorkflow({
          workflow,
          runId: "dep_legacy",
          deploymentDomain: "workflow.interchange",
          config: HARNESS_CONFIG_BASE,
          deployContent: DEPLOY_CONTENT_BASE,
          operatorApprovals: broad,
        });
      } catch (err) {
        if (!(err instanceof CapabilityApprovalDeniedError)) throw err;
        captured = err;
      }
      expect(captured).toBeInstanceOf(CapabilityApprovalDeniedError);
      expect(captured?.unresolvedDirectors).toEqual([
        "@vendor/missing/director",
      ]);
      expect(captured?.message).toContain(
        "unresolvable director: @vendor/missing/director",
      );
    });

    test("definition with an empty stepOrder fails validation", async () => {
      const directorRegistry = createDefaultDirectorRegistry();
      const workflowRepo = createRecordingWorkflowRepoWriter();
      const launch = createRecordingLaunch();
      const orchestrator = createWorkflowDeployOrchestrator({
        directorRegistry,
        workflowRepo,
        launchSession: launch.fn,
      });
      const bogus: WorkflowDefinition = {
        id: "wf_bogus",
        triggers: [{ type: "manual" }],
        steps: {},
        stepOrder: [],
      };
      await expect(
        orchestrator.deployWorkflow({
          workflow: bogus,
          runId: "dep_x",
          deploymentDomain: "workflow.interchange",
          config: HARNESS_CONFIG_BASE,
          deployContent: DEPLOY_CONTENT_BASE,
          operatorApprovals: new Set(),
        }),
      ).rejects.toBeInstanceOf(WorkflowDefinitionInvalidError);
    });
  });
});

describe("wrapHarnessAsSingleStepWorkflow", () => {
  test("derives id from config.agentId", () => {
    const agent = wrapHarnessAsSingleStepWorkflow({
      config: HARNESS_CONFIG_BASE,
      deployContent: DEPLOY_CONTENT_BASE,
    });
    expect(agent.id).toBe(HARNESS_CONFIG_BASE.agentId);
  });

  test("uses deployContent.systemPrompt for the agent's systemPrompt", () => {
    const customContent: DeployContent = {
      systemPrompt: "you are the trivial agent",
    };
    const agent = wrapHarnessAsSingleStepWorkflow({
      config: HARNESS_CONFIG_BASE,
      deployContent: customContent,
    });
    expect(agent.systemPrompt).toBe("you are the trivial agent");
  });

  test("projects inference.sources from config.sources by provider+model", () => {
    const config: HarnessConfig = {
      ...HARNESS_CONFIG_BASE,
      sources: [
        {
          id: "src-anthropic",
          provider: "anthropic",
          baseURL: "https://api.example/anthropic",
          apiKey: "secret-a",
          model: "claude-3",
        },
        {
          id: "src-openai",
          provider: "openai",
          baseURL: "https://api.example/openai",
          apiKey: "secret-b",
          model: "gpt-4",
        },
      ],
    };
    const agent = wrapHarnessAsSingleStepWorkflow({
      config,
      deployContent: DEPLOY_CONTENT_BASE,
    });
    expect(agent.inference.sources).toEqual([
      { provider: "anthropic", model: "claude-3" },
      { provider: "openai", model: "gpt-4" },
    ]);
  });

  test("empty toolFactories and capabilities (deploy tree is the source of truth)", () => {
    const agent = wrapHarnessAsSingleStepWorkflow({
      config: HARNESS_CONFIG_BASE,
      deployContent: DEPLOY_CONTENT_BASE,
    });
    expect(agent.toolFactories).toEqual([]);
    expect(agent.capabilities).toEqual([]);
  });

  test("no director ref (caller carries no director state in the trivial shape)", () => {
    const agent = wrapHarnessAsSingleStepWorkflow({
      config: HARNESS_CONFIG_BASE,
      deployContent: DEPLOY_CONTENT_BASE,
    });
    expect(agent.director).toBeUndefined();
  });
});

describe("per-step address derivation", () => {
  test("deriveStepAddress concatenates the run id, step, and deployment domain", () => {
    expect(
      deriveStepAddress({
        runId: "dep_abc",
        stepId: "step1",
        domain: "workflow.interchange",
      }),
    ).toBe("dep_abc-step1@workflow.interchange");
  });

  test("deriveStepAgentId concatenates the run id and step", () => {
    expect(deriveStepAgentId({ runId: "dep_abc", stepId: "x" })).toBe(
      "dep_abc-x",
    );
  });

  test("deriveStepRunId concatenates the run id and step", () => {
    expect(deriveStepRunId({ runId: "dep_abc", stepId: "x" })).toBe(
      "dep_abc-x",
    );
  });

  test("derivation is deterministic across calls", () => {
    const a = deriveStepAddress({
      runId: "dep_a",
      stepId: "s",
      domain: "d",
    });
    const b = deriveStepAddress({
      runId: "dep_a",
      stepId: "s",
      domain: "d",
    });
    expect(a).toBe(b);
  });

  test("deriveRunAddress drops the per-step suffix", () => {
    expect(
      deriveRunAddress({
        runId: "dep_abc",
        domain: "workflow.interchange",
      }),
    ).toBe("dep_abc@workflow.interchange");
  });

  test("deriveRunAgentId drops the per-step suffix", () => {
    expect(deriveRunAgentId({ runId: "dep_abc" })).toBe("dep_abc");
  });

  test("deriveWorkflowRunRepoId sanitizes the deployment address into a SAFE_REPO_ID slug", () => {
    const address = deriveRunAddress({
      runId: "dep_abc",
      domain: "acme.localhost",
    });
    expect(address).toBe("dep_abc@acme.localhost");
    const repoId = deriveWorkflowRunRepoId(address);
    expect(repoId).toBe("dep_abc-acme-localhost");
    expect(repoId).toMatch(/^[a-zA-Z0-9_-]+$/);
  });

  test("deriveWorkflowRunRepoId substitutes every @ and . that SAFE_REPO_ID rejects", () => {
    expect(deriveWorkflowRunRepoId("dep_x@a.b.c")).toBe("dep_x-a-b-c");
    // Already-safe slugs are passed through unchanged so a repo id that
    // never crossed an address boundary is stable.
    expect(deriveWorkflowRunRepoId("dep_safe-slug_1")).toBe("dep_safe-slug_1");
  });
});

describe("deriveRunAddress / deriveWorkflowRunId round-trip", () => {
  const domain = "workflow.interchange";

  test("deriveWorkflowRunId recovers the runId from deriveRunAddress", () => {
    expect(
      deriveWorkflowRunId(deriveRunAddress({ runId: "run_abc", domain })),
    ).toBe("run_abc");
  });

  test("round-trips a per-step address to its step-suffixed run id", () => {
    const stepAddress = deriveStepAddress({
      runId: "run_abc",
      stepId: "plan",
      domain,
    });
    expect(deriveWorkflowRunId(stepAddress)).toBe("run_abc-plan");
  });

  test("throws on an address whose local part is not a run id", () => {
    expect(() => deriveWorkflowRunId("ins_abc@workflow.interchange")).toThrow(
      "Invalid run address",
    );
  });
});

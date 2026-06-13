import { describe, test, expect } from "bun:test";

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
  deriveDeploymentAddress,
  deriveDeploymentAgentId,
  deriveStepAddress,
  deriveStepAgentId,
  deriveStepInstanceId,
  MultiStepDeployHandoffMissingError,
  MultiStepDeploymentArgsMissingError,
  WorkflowDefinitionInvalidError,
  wrapHarnessAsTrivialAgent,
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

function makeTrivialWorkflow(
  agent: AgentDefinition<BaseEnv>,
): WorkflowDefinition {
  return defineWorkflow({
    id: "wf_trivial",
    agent,
    trigger: { type: "mail", to: "ins_legacy-agent@integration.interchange" },
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
  agentAddress: "ins_legacy-agent@integration.interchange",
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
      approvals.add(`tool:${factory.id}`);
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
  instanceId: string;
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
      instanceId: params.instanceId,
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

describe("createWorkflowDeployOrchestrator", () => {
  describe("trivial branch", () => {
    test("preserves the existing agent address and launches once", async () => {
      const agent = makeAgent("legacy-agent");
      const workflow = makeTrivialWorkflow(agent);
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

      const approvals = approvedGrantsForWorkflow(workflow, [agent]);

      const result = await orchestrator.deployWorkflow({
        workflow,
        trivialBindings: {
          agentAddress: "ins_legacy-agent@integration.interchange",
          agentId: "legacy-agent",
          instanceId: "instance-legacy",
        },
        config: HARNESS_CONFIG_BASE,
        deployContent: DEPLOY_CONTENT_BASE,
        operatorApprovals: approvals,
      });

      expect(launch.launches).toHaveLength(1);
      const launched = launch.launches[0];
      if (launched === undefined) throw new Error("missing launch");
      expect(launched.agentAddress).toBe(
        "ins_legacy-agent@integration.interchange",
      );
      expect(launched.agentId).toBe("legacy-agent");
      expect(launched.instanceId).toBe("instance-legacy");
      expect(launched.config).toEqual(HARNESS_CONFIG_BASE);
      expect(launched.deployContent).toEqual(DEPLOY_CONTENT_BASE);
      // Regression: the trivial branch does NOT invoke the multi-step
      // deploy hand-off.
      expect(multiStep.calls).toHaveLength(0);
      expect(result).toEqual({ kind: "trivial" });
    });

    test("passes through toolPackagePins to the launch", async () => {
      const agent = makeAgent("legacy-agent");
      const workflow = makeTrivialWorkflow(agent);
      const directorRegistry = createDefaultDirectorRegistry();
      const workflowRepo = createRecordingWorkflowRepoWriter();
      const launch = createRecordingLaunch();
      const orchestrator = createWorkflowDeployOrchestrator({
        directorRegistry,
        workflowRepo,
        launchSession: launch.fn,
      });
      const approvals = approvedGrantsForWorkflow(workflow, [agent]);
      const pins = [{ name: "@vendor/pkg", version: "1.0.0" }] as const;

      await orchestrator.deployWorkflow({
        workflow,
        trivialBindings: {
          agentAddress: "ins_legacy-agent@integration.interchange",
          agentId: "legacy-agent",
          instanceId: "instance-legacy",
        },
        config: HARNESS_CONFIG_BASE,
        deployContent: DEPLOY_CONTENT_BASE,
        toolPackagePins: pins,
        operatorApprovals: approvals,
      });

      expect(launch.launches[0]?.toolPackagePins).toEqual(pins);
    });

    test("writes the workflow repo before launching", async () => {
      const agent = makeAgent("legacy-agent");
      const workflow = makeTrivialWorkflow(agent);
      const directorRegistry = createDefaultDirectorRegistry();
      const workflowRepo = createRecordingWorkflowRepoWriter();
      const order: string[] = [];
      const launch: LaunchSessionFn = async () => {
        order.push("launch");
      };
      const recordingRepo: WorkflowRepoWriter = {
        async writeWorkflowRepo(args) {
          order.push("repo");
          workflowRepo.writes.push({
            workflowRepoId: args.workflowRepoId,
            files: new Map(args.files),
          });
        },
      };
      const orchestrator = createWorkflowDeployOrchestrator({
        directorRegistry,
        workflowRepo: recordingRepo,
        launchSession: launch,
      });
      const approvals = approvedGrantsForWorkflow(workflow, [agent]);

      await orchestrator.deployWorkflow({
        workflow,
        trivialBindings: {
          agentAddress: "ins_legacy-agent@integration.interchange",
          agentId: "legacy-agent",
          instanceId: "instance-legacy",
        },
        config: HARNESS_CONFIG_BASE,
        deployContent: DEPLOY_CONTENT_BASE,
        operatorApprovals: approvals,
      });

      expect(order).toEqual(["repo", "launch"]);
      expect(workflowRepo.writes).toHaveLength(1);
      const write = workflowRepo.writes[0];
      if (write === undefined) throw new Error("missing write");
      expect(write.workflowRepoId).toBe("wf_trivial");
      expect(write.files.has("workflow.json")).toBe(true);
      expect(write.files.has("capability-declarations.json")).toBe(true);
      expect(write.files.has(".gitignore")).toBe(true);
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
        deploymentId: "dep_abc123",
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
        "ins_dep_abc123-plan@workflow.interchange",
      );
      expect(planLaunch.agentId).toBe("ins_dep_abc123-plan");
      expect(planLaunch.instanceId).toBe("ins_dep_abc123-plan");
      expect(planLaunch.config.agentAddress).toBe(planLaunch.agentAddress);
      expect(planLaunch.config.agentId).toBe(planLaunch.agentId);
      expect(planLaunch.config.systemPrompt).toBe("you plan");
      expect(planLaunch.deployContent.systemPrompt).toBe("you plan");

      expect(executeLaunch.agentAddress).toBe(
        "ins_dep_abc123-execute@workflow.interchange",
      );
      expect(executeLaunch.agentId).toBe("ins_dep_abc123-execute");
      expect(executeLaunch.instanceId).toBe("ins_dep_abc123-execute");
      expect(executeLaunch.config.systemPrompt).toBe("you execute");
      expect(executeLaunch.deployContent.systemPrompt).toBe("you execute");

      expect(multiStep.calls).toHaveLength(1);
      const handoff = multiStep.calls[0];
      if (handoff === undefined) throw new Error("missing handoff");
      expect(handoff.agentAddress).toBe("ins_dep_abc123@workflow.interchange");
      expect(handoff.agentId).toBe("ins_dep_abc123");
      expect(handoff.hubPublicKey).toBe("00".repeat(32));
      expect(handoff.definition).toBe(workflow);
      expect(Object.keys(handoff.sources).sort()).toEqual(["execute", "plan"]);
      const baseSource = HARNESS_CONFIG_BASE.sources[0];
      if (baseSource === undefined) throw new Error("missing base source");
      expect(handoff.sources.plan).toEqual(baseSource);
      expect(handoff.sources.execute).toEqual(baseSource);
      expect(result).toEqual({
        kind: "multi-step",
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
        deploymentId: "dep_xy",
        deploymentDomain: "workflow.interchange",
        config: HARNESS_CONFIG_BASE,
        deployContent: DEPLOY_CONTENT_BASE,
        hubPublicKey: "00".repeat(32),
        operatorApprovals: approvals,
      });

      // The hand-off fires exactly once and only after the per-step
      // provisioning loop has finished.
      expect(order).toEqual([
        "launch:ins_dep_xy-plan",
        "launch:ins_dep_xy-execute",
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
          deploymentId: "dep_abc123",
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
          deploymentId: "dep_abc123",
          deploymentDomain: "workflow.interchange",
          config: HARNESS_CONFIG_BASE,
          deployContent: DEPLOY_CONTENT_BASE,
          hubPublicKey: "00".repeat(32),
          operatorApprovals: approvals,
        }),
      ).rejects.toBeInstanceOf(MultiStepDeployHandoffMissingError);
    });

    test("throws when deploymentId is missing", async () => {
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
          deploymentId: "dep_abc123",
          config: HARNESS_CONFIG_BASE,
          deployContent: DEPLOY_CONTENT_BASE,
          operatorApprovals: approvals,
        }),
      ).rejects.toBeInstanceOf(MultiStepDeploymentArgsMissingError);
    });

    test("single-step workflow without trivialBindings takes the derived path", async () => {
      const agent = makeAgent("only");
      const workflow = makeTrivialWorkflow(agent);
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
      const approvals = approvedGrantsForWorkflow(workflow, [agent]);

      await orchestrator.deployWorkflow({
        workflow,
        deploymentId: "dep_xyz",
        deploymentDomain: "workflow.interchange",
        config: HARNESS_CONFIG_BASE,
        deployContent: DEPLOY_CONTENT_BASE,
        hubPublicKey: "00".repeat(32),
        operatorApprovals: approvals,
      });

      expect(launch.launches).toHaveLength(1);
      const launched = launch.launches[0];
      if (launched === undefined) throw new Error("missing launch");
      const expectedStepId = workflow.stepOrder[0];
      if (expectedStepId === undefined) {
        throw new Error("missing step id");
      }
      expect(launched.agentAddress).toBe(
        `ins_dep_xyz-${expectedStepId}@workflow.interchange`,
      );
      expect(multiStep.calls).toHaveLength(1);
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
          deploymentId: "dep_pinfail",
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
      const workflow = makeTrivialWorkflow(agent);
      const directorRegistry = createDefaultDirectorRegistry();
      const workflowRepo = createRecordingWorkflowRepoWriter();
      const launch = createRecordingLaunch();
      const orchestrator = createWorkflowDeployOrchestrator({
        directorRegistry,
        workflowRepo,
        launchSession: launch.fn,
      });

      const incompleteApprovals = approvedGrantsForWorkflow(workflow, [agent]);
      incompleteApprovals.delete("tool:@intx/tools-mail/sidecar-bundle");

      let captured: CapabilityApprovalDeniedError | undefined;
      try {
        await orchestrator.deployWorkflow({
          workflow,
          trivialBindings: {
            agentAddress: "ins_legacy-agent@integration.interchange",
            agentId: "legacy-agent",
            instanceId: "instance-legacy",
          },
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
      expect(captured?.pending.get(stepId)).toEqual([
        "tool:@intx/tools-mail/sidecar-bundle",
      ]);
      expect(launch.launches).toHaveLength(0);
      expect(workflowRepo.writes).toHaveLength(0);
    });

    test("zero approved sources throws with the offending step and missing source", async () => {
      const agent = makeAgent("legacy-agent");
      const workflow = makeTrivialWorkflow(agent);
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
          trivialBindings: {
            agentAddress: "ins_legacy-agent@integration.interchange",
            agentId: "legacy-agent",
            instanceId: "instance-legacy",
          },
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
        "tool:@intx/tools-mail/sidecar-bundle",
        "inference.source:anthropic:mock-model",
      ]);

      let captured: CapabilityApprovalDeniedError | undefined;
      try {
        await orchestrator.deployWorkflow({
          workflow,
          trivialBindings: {
            agentAddress: "ins_legacy-agent@integration.interchange",
            agentId: "legacy-agent",
            instanceId: "instance-legacy",
          },
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
          deploymentId: "dep_x",
          deploymentDomain: "workflow.interchange",
          config: HARNESS_CONFIG_BASE,
          deployContent: DEPLOY_CONTENT_BASE,
          operatorApprovals: new Set(),
        }),
      ).rejects.toBeInstanceOf(WorkflowDefinitionInvalidError);
    });
  });
});

describe("wrapHarnessAsTrivialAgent", () => {
  test("derives id from config.agentId", () => {
    const agent = wrapHarnessAsTrivialAgent({
      config: HARNESS_CONFIG_BASE,
      deployContent: DEPLOY_CONTENT_BASE,
    });
    expect(agent.id).toBe(HARNESS_CONFIG_BASE.agentId);
  });

  test("uses deployContent.systemPrompt for the agent's systemPrompt", () => {
    const customContent: DeployContent = {
      systemPrompt: "you are the trivial agent",
    };
    const agent = wrapHarnessAsTrivialAgent({
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
    const agent = wrapHarnessAsTrivialAgent({
      config,
      deployContent: DEPLOY_CONTENT_BASE,
    });
    expect(agent.inference.sources).toEqual([
      { provider: "anthropic", model: "claude-3" },
      { provider: "openai", model: "gpt-4" },
    ]);
  });

  test("empty toolFactories and capabilities (deploy tree is the source of truth)", () => {
    const agent = wrapHarnessAsTrivialAgent({
      config: HARNESS_CONFIG_BASE,
      deployContent: DEPLOY_CONTENT_BASE,
    });
    expect(agent.toolFactories).toEqual([]);
    expect(agent.capabilities).toEqual([]);
  });

  test("no director ref (caller carries no director state in the trivial shape)", () => {
    const agent = wrapHarnessAsTrivialAgent({
      config: HARNESS_CONFIG_BASE,
      deployContent: DEPLOY_CONTENT_BASE,
    });
    expect(agent.director).toBeUndefined();
  });
});

describe("per-step address derivation", () => {
  test("deriveStepAddress concatenates with the ins_ prefix and the deployment domain", () => {
    expect(
      deriveStepAddress({
        deploymentId: "dep_abc",
        stepId: "step1",
        deploymentDomain: "workflow.interchange",
      }),
    ).toBe("ins_dep_abc-step1@workflow.interchange");
  });

  test("deriveStepAgentId concatenates with the ins_ prefix", () => {
    expect(deriveStepAgentId({ deploymentId: "dep_abc", stepId: "x" })).toBe(
      "ins_dep_abc-x",
    );
  });

  test("deriveStepInstanceId concatenates with the ins_ prefix", () => {
    expect(deriveStepInstanceId({ deploymentId: "dep_abc", stepId: "x" })).toBe(
      "ins_dep_abc-x",
    );
  });

  test("derivation is deterministic across calls", () => {
    const a = deriveStepAddress({
      deploymentId: "dep_a",
      stepId: "s",
      deploymentDomain: "d",
    });
    const b = deriveStepAddress({
      deploymentId: "dep_a",
      stepId: "s",
      deploymentDomain: "d",
    });
    expect(a).toBe(b);
  });

  test("deriveDeploymentAddress drops the per-step suffix", () => {
    expect(
      deriveDeploymentAddress({
        deploymentId: "dep_abc",
        deploymentDomain: "workflow.interchange",
      }),
    ).toBe("ins_dep_abc@workflow.interchange");
  });

  test("deriveDeploymentAgentId drops the per-step suffix", () => {
    expect(deriveDeploymentAgentId({ deploymentId: "dep_abc" })).toBe(
      "ins_dep_abc",
    );
  });
});

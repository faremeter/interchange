// Parent -> child workflow integration test.
//
// Deploys a parent workflow with a `childWorkflow` primitive against the
// real hub + sidecar subprocess + mock inference fixture, fires the
// parent's mail trigger, and asserts the canonical parent/child event
// chain materializes in the deployment's workflow-run repo. Child events
// land under `runs/<childRunId>/events/` in the same workflow-run repo as
// the parent's `runs/<parentRunId>/events/` -- the sub-namespace shape
// the in-process `runChild` recursion produces.
//
// The child workflow is deployed first as its own workflow asset; the
// parent's `childWorkflow{definitionRef}` references the child by its
// `WorkflowDefinition.id`. The spawn-child adapter
// (`createWorkflowSpawnChild`) resolves the definitionRef against the
// workflow asset substrate at run time; the in-process `runChild`
// (`createSidecarRunChild`) builds a per-childRunId `WorkflowRuntimeEnv`
// and drives the child's `runtimeRun` to terminal status, settling the
// parent's spawn step with the child's terminal status.
//
// This file does not modify the pre-landed `deploy-flow-env` fixture.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { defineAgent, createDefaultDirectorRegistry } from "@intx/agent";
import type { HarnessConfig } from "@intx/types/runtime";
import {
  childWorkflow,
  defineWorkflow,
  step,
  type WorkflowDefinition,
} from "@intx/workflow";
import {
  createWorkflowDeployOrchestrator,
  deriveDeploymentAddress,
  type ApprovalSet,
  type LaunchSessionFn,
  type SendMultiStepDeployFn,
  type WorkflowRepoWriter,
} from "@intx/workflow-deploy";
import { deriveDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";
import type { RepoId, WorkflowRunHubPrincipal } from "@intx/hub-sessions";
import { DEFAULT_ASSET_REF } from "@intx/hub-sessions";

import {
  SESSION_ID,
  SIDECAR_ID,
  fireMailTrigger,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { toLaunchDeployContent } from "./launch-session-bridge";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const PARENT_DEPLOYMENT_ID = "child-workflow-parent-1";
const CHILD_DEPLOYMENT_ID = "child-workflow-child-1";
const CHILD_WORKFLOW_ID = `wf_${CHILD_DEPLOYMENT_ID}`;
const PARENT_WORKFLOW_ID = `wf_${PARENT_DEPLOYMENT_ID}`;
const WORKFLOW_RUN_REF = "refs/heads/main";

// Grandchild-depth deployment ids. Each rung is deployed as its own
// workflow asset; the parent's `childWorkflow{definitionRef}` references
// the child by id, and the child's `childWorkflow{definitionRef}` in
// turn references the grandchild. The sub-namespace scoping under
// `runs/<runId>/` should isolate every rung in the parent's
// workflow-run repo without leakage.
const NESTED_PARENT_DEPLOYMENT_ID = "child-workflow-nested-parent-1";
const NESTED_CHILD_DEPLOYMENT_ID = "child-workflow-nested-child-1";
const NESTED_GRANDCHILD_DEPLOYMENT_ID = "child-workflow-nested-grandchild-1";
const NESTED_PARENT_WORKFLOW_ID = `wf_${NESTED_PARENT_DEPLOYMENT_ID}`;
const NESTED_CHILD_WORKFLOW_ID = `wf_${NESTED_CHILD_DEPLOYMENT_ID}`;
const NESTED_GRANDCHILD_WORKFLOW_ID = `wf_${NESTED_GRANDCHILD_DEPLOYMENT_ID}`;

// Siblings-fanout deployment ids. The parent deploys 5
// `childWorkflow` primitives in `stepOrder`, each pointing at one of
// 5 distinct definitionRefs (deployed as 5 separate workflow assets).
const SIBLINGS_PARENT_DEPLOYMENT_ID = "child-workflow-siblings-parent-1";
const SIBLINGS_CHILD_COUNT = 5;
const SIBLINGS_CHILD_DEPLOYMENT_IDS: readonly string[] = Array.from(
  { length: SIBLINGS_CHILD_COUNT },
  (_unused, i) => `child-workflow-sibling-${(i + 1).toString()}`,
);
const SIBLINGS_PARENT_WORKFLOW_ID = `wf_${SIBLINGS_PARENT_DEPLOYMENT_ID}`;
const SIBLINGS_CHILD_WORKFLOW_IDS: readonly string[] =
  SIBLINGS_CHILD_DEPLOYMENT_IDS.map((id) => `wf_${id}`);

let env: DeployFlowEnv;

beforeAll(async () => {
  env = await startDeployFlowEnv();
});

afterAll(async () => {
  await env.teardown();
});

describe("parent -> child workflow round-trip", () => {
  test("sidecar registers with hub", () => {
    expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
  });

  test("parent run spawns child, child run lands under runs/<childRunId>/", async () => {
    const childAgent = defineAgent({
      id: "agent-child-step",
      systemPrompt: "You are the child workflow's step agent.",
      tools: [],
      capabilities: [],
      inference: {
        sources: [{ provider: "anthropic", model: "mock-model" }],
      },
    });
    const parentStep1Agent = defineAgent({
      id: "agent-parent-step1",
      systemPrompt: "You are the parent workflow's first step agent.",
      tools: [],
      capabilities: [],
      inference: {
        sources: [{ provider: "anthropic", model: "mock-model" }],
      },
    });
    const parentStep2Agent = defineAgent({
      id: "agent-parent-step2",
      systemPrompt: "You are the parent workflow's second step agent.",
      tools: [],
      capabilities: [],
      inference: {
        sources: [{ provider: "anthropic", model: "mock-model" }],
      },
    });

    const parentMailAddress = deriveDeploymentAddress({
      deploymentId: PARENT_DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
    });
    const childMailAddress = deriveDeploymentAddress({
      deploymentId: CHILD_DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
    });

    const childWorkflowDefinition: WorkflowDefinition = defineWorkflow({
      id: CHILD_WORKFLOW_ID,
      trigger: { type: "mail", to: childMailAddress },
      steps: {
        childStep: step({ agent: childAgent }),
      },
    });

    const parentWorkflowDefinition: WorkflowDefinition = defineWorkflow({
      id: PARENT_WORKFLOW_ID,
      trigger: { type: "mail", to: parentMailAddress },
      steps: {
        step1: step({ agent: parentStep1Agent }),
        spawn: childWorkflow({
          definitionRef: CHILD_WORKFLOW_ID,
          after: ["step1"],
        }),
        step2: step({ agent: parentStep2Agent, after: ["spawn"] }),
      },
    });

    const operatorApprovals: ApprovalSet = new Set<string>([
      "inference.source:anthropic:mock-model",
      "director:@intx/agent/default",
      `mail.address:${parentMailAddress}`,
      `mail.address:${childMailAddress}`,
      `mail.send:${DEPLOYMENT_DOMAIN}`,
    ]);

    const baseConfig = (
      address: string,
      agentId: string,
      systemPrompt: string,
    ): HarnessConfig => ({
      sessionId: SESSION_ID,
      agentId,
      tenantId: "tenant-1",
      principalId: "prin_integration-1",
      agentAddress: address,
      systemPrompt,
      tools: [],
      grants: [],
      sources: [
        {
          id: "anthropic:mock-model",
          provider: "anthropic",
          baseURL: `http://localhost:${env.inference.server.port}`,
          apiKey: "sk-mock",
          model: "mock-model",
        },
      ],
      defaultSource: "anthropic:mock-model",
    });

    const launchSession: LaunchSessionFn = async (orchestratorParams) => {
      await env.hub.sessionService.stageWorkflowStep({
        agentAddress: orchestratorParams.agentAddress,
        agentId: orchestratorParams.agentId,
        instanceId: orchestratorParams.instanceId,
        config: orchestratorParams.config,
        deployContent: toLaunchDeployContent(orchestratorParams.deployContent),
        ...(orchestratorParams.toolPackagePins !== undefined
          ? { toolPackagePins: orchestratorParams.toolPackagePins }
          : {}),
      });
    };

    const sendMultiStepDeploy: SendMultiStepDeployFn = async (params) =>
      env.hub.router.sendAgentDeploy(params.agentAddress, params.config, {
        definition: {
          id: params.definition.id,
          triggers: [...params.definition.triggers],
          stepOrder: [...params.definition.stepOrder],
          steps: params.definition.steps as Record<string, unknown>,
          ...(params.definition.state !== undefined
            ? { state: params.definition.state }
            : {}),
        },
        sources: params.sources,
      });

    const workflowRepo: WorkflowRepoWriter = {
      async writeWorkflowRepo(args) {
        const repoId: RepoId = { kind: "workflow", id: args.workflowRepoId };
        const principal: WorkflowRunHubPrincipal = { kind: "hub" };
        const files: Record<string, string> = {};
        for (const [k, v] of args.files) {
          files[k] = v;
        }
        await env.hub.agentRepoStore.repoStore.writeTree(
          principal,
          repoId,
          DEFAULT_ASSET_REF,
          {
            files,
            message: `child-workflow test: write workflow repo ${args.workflowRepoId}`,
          },
        );
      },
    };

    const orchestrator = createWorkflowDeployOrchestrator({
      directorRegistry: createDefaultDirectorRegistry(),
      workflowRepo,
      launchSession,
      sendMultiStepDeploy,
      // A single-step (child-spawning) workflow deploys once at the head
      // through the deploy core's single-step hand-off.
      deploySingleStepAtHead: (params) =>
        env.hub.sessionService.deploySingleStepAtHead(params),
    });

    const childResult = await orchestrator.deployWorkflow({
      workflow: childWorkflowDefinition,
      config: baseConfig(
        childMailAddress,
        `ins_${CHILD_DEPLOYMENT_ID}`,
        "Fallback prompt (overridden per step).",
      ),
      deployContent: {
        systemPrompt: "Fallback prompt (overridden per step).",
      },
      operatorApprovals,
      deploymentId: CHILD_DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
      hubPublicKey: "00".repeat(32),
    });
    expect(childResult.publicKey).toBeTruthy();

    const parentResult = await orchestrator.deployWorkflow({
      workflow: parentWorkflowDefinition,
      config: baseConfig(
        parentMailAddress,
        `ins_${PARENT_DEPLOYMENT_ID}`,
        "Fallback prompt (overridden per step).",
      ),
      deployContent: {
        systemPrompt: "Fallback prompt (overridden per step).",
      },
      operatorApprovals,
      deploymentId: PARENT_DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
      hubPublicKey: "00".repeat(32),
    });
    expect(parentResult.publicKey).toBeTruthy();

    const parentWorkflowRunRepoId: RepoId = {
      kind: "workflow-run",
      id: deriveDeploymentId(parentMailAddress),
    };
    env.registerDeployment({
      deploymentId: PARENT_DEPLOYMENT_ID,
      workflowDefinition: parentWorkflowDefinition,
      workflowRunRepoId: parentWorkflowRunRepoId,
      workflowRunRef: WORKFLOW_RUN_REF,
      mailAddress: parentMailAddress,
    });

    expect(env.hub.router.getRoutableAddresses()).toContain(parentMailAddress);

    await fireMailTrigger(env, parentMailAddress, {
      messageId: "<child-workflow-roundtrip-1@integration.interchange>",
    });

    const parentRunId = await waitForFirstRunId(env, parentWorkflowRunRepoId, {
      diagnostics: env.sidecarDiagnostics,
      timeoutMs: 20_000,
    });

    await waitFor(
      async () => {
        const events = await readWorkflowRunEvents(
          env,
          PARENT_DEPLOYMENT_ID,
          parentRunId,
        );
        return events.some((e) => e.type === "ChildSpawned");
      },
      { diagnostics: env.sidecarDiagnostics, timeoutMs: 20_000 },
    );

    const parentEvents = await readWorkflowRunEvents(
      env,
      PARENT_DEPLOYMENT_ID,
      parentRunId,
    );
    const spawnedEvent = parentEvents.find((e) => e.type === "ChildSpawned");
    if (spawnedEvent === undefined) throw new Error("unreachable");
    const childRunId = spawnedEvent.body["childRunId"];
    if (typeof childRunId !== "string") {
      throw new Error(
        `ChildSpawned event is missing a string childRunId field; got ${typeof childRunId}`,
      );
    }
    expect(spawnedEvent.body["childDefinitionRef"]).toBe(CHILD_WORKFLOW_ID);

    await waitFor(
      async () => {
        const events = await readWorkflowRunEvents(
          env,
          PARENT_DEPLOYMENT_ID,
          parentRunId,
        );
        return events.some((e) => e.type === "RunCompleted");
      },
      { diagnostics: env.sidecarDiagnostics, timeoutMs: 30_000 },
    );

    const finalParentEvents = await readWorkflowRunEvents(
      env,
      PARENT_DEPLOYMENT_ID,
      parentRunId,
    );
    const parentTypes = finalParentEvents.map((e) => e.type);
    const runStartedIdx = parentTypes.indexOf("RunStarted");
    const step1StartedIdx = parentTypes.findIndex(
      (t, i) =>
        t === "StepStarted" && finalParentEvents[i]?.body["stepId"] === "step1",
    );
    const step1CompletedIdx = parentTypes.findIndex(
      (t, i) =>
        t === "StepCompleted" &&
        finalParentEvents[i]?.body["stepId"] === "step1",
    );
    const spawnStartedIdx = parentTypes.findIndex(
      (t, i) =>
        t === "StepStarted" && finalParentEvents[i]?.body["stepId"] === "spawn",
    );
    const childSpawnedIdx = parentTypes.indexOf("ChildSpawned");
    const childCompletedIdx = parentTypes.indexOf("ChildCompleted");
    const spawnCompletedIdx = parentTypes.findIndex(
      (t, i) =>
        t === "StepCompleted" &&
        finalParentEvents[i]?.body["stepId"] === "spawn",
    );
    const step2StartedIdx = parentTypes.findIndex(
      (t, i) =>
        t === "StepStarted" && finalParentEvents[i]?.body["stepId"] === "step2",
    );
    const step2CompletedIdx = parentTypes.findIndex(
      (t, i) =>
        t === "StepCompleted" &&
        finalParentEvents[i]?.body["stepId"] === "step2",
    );
    const runCompletedIdx = parentTypes.indexOf("RunCompleted");

    expect(runStartedIdx).toBeGreaterThanOrEqual(0);
    expect(step1StartedIdx).toBeGreaterThan(runStartedIdx);
    expect(step1CompletedIdx).toBeGreaterThan(step1StartedIdx);
    expect(spawnStartedIdx).toBeGreaterThan(step1CompletedIdx);
    expect(childSpawnedIdx).toBeGreaterThan(spawnStartedIdx);
    expect(childCompletedIdx).toBeGreaterThan(childSpawnedIdx);
    expect(spawnCompletedIdx).toBeGreaterThan(childCompletedIdx);
    expect(step2StartedIdx).toBeGreaterThan(spawnCompletedIdx);
    expect(step2CompletedIdx).toBeGreaterThan(step2StartedIdx);
    expect(runCompletedIdx).toBeGreaterThan(step2CompletedIdx);

    const childCompletedBody = finalParentEvents[childCompletedIdx]?.body;
    if (childCompletedBody === undefined) throw new Error("unreachable");
    expect(childCompletedBody["childRunId"]).toBe(childRunId);
    expect(childCompletedBody["terminalStatus"]).toBe("completed");

    // Parent's namespace must contain only the parent's StepStarted /
    // StepCompleted entries. A regression that leaked the child's step
    // events into the parent's run log would expand the parentTypes
    // array silently; the ordering checks above would still pass.
    // Assert positively that no per-step event in the parent log
    // carries a stepId outside the parent's stepOrder.
    const parentStepIds = new Set(["step1", "spawn", "step2"]);
    for (const event of finalParentEvents) {
      if (event.type !== "StepStarted" && event.type !== "StepCompleted") {
        continue;
      }
      const stepId = event.body["stepId"];
      expect(parentStepIds.has(String(stepId))).toBe(true);
    }

    const spawnCompletedBody = finalParentEvents[spawnCompletedIdx]?.body;
    if (spawnCompletedBody === undefined) throw new Error("unreachable");

    // Child run lives under runs/<childRunId>/events/ in the same
    // workflow-run repo. Read it back through the fixture helper, which
    // resolves the events path against the parent's deployment handle.
    const childEvents = await readWorkflowRunEvents(
      env,
      PARENT_DEPLOYMENT_ID,
      childRunId,
    );
    expect(childEvents.length).toBeGreaterThan(0);
    const childTypes = childEvents.map((e) => e.type);
    const childRunStartedIdx = childTypes.indexOf("RunStarted");
    const childStepStartedIdx = childTypes.indexOf("StepStarted");
    const childStepCompletedIdx = childTypes.indexOf("StepCompleted");
    const childRunCompletedIdx = childTypes.indexOf("RunCompleted");

    expect(childRunStartedIdx).toBeGreaterThanOrEqual(0);
    expect(childStepStartedIdx).toBeGreaterThan(childRunStartedIdx);
    expect(childStepCompletedIdx).toBeGreaterThan(childStepStartedIdx);
    expect(childRunCompletedIdx).toBeGreaterThan(childStepCompletedIdx);

    const childRunStartedBody = childEvents[childRunStartedIdx]?.body;
    if (childRunStartedBody === undefined) throw new Error("unreachable");
    expect(childRunStartedBody["runId"]).toBe(childRunId);
    // The runtime body's RunStarted event does not (yet) carry
    // parentRunId / parentStepId attribution. Parent->child attribution
    // is observable through the parent's ChildSpawned/ChildCompleted
    // events (asserted above against childRunId). When the runtime
    // gains a "child RunStarted carries parent attribution" surface,
    // re-assert against childRunStartedBody["parentRunId"] /
    // childRunStartedBody["parentStepId"] here.
    void parentRunId;
  });

  // Grandchild-depth recursion. The sidecar's `createSidecarRunChild`
  // wires the child env's `spawnChild` via `createWorkflowSpawnChild`
  // against the same recursive `runChild`, so an in-process grandchild
  // spawn resolves the grandchild's `workflow.json` from the workflow-
  // asset substrate and drives a per-grandchildRunId `runtimeRun`
  // exactly the way the child's own spawn does. Sub-namespace scoping
  // continues to hold at every depth because each rung's runtime env
  // keys substrate operations on its own `runId`.
  test("parent -> child -> grandchild recursion at depth 2", async () => {
    // Recursion-depth coverage. The case above tests parent -> 1
    // child; the runtime's in-process `runChildWorkflow` is designed
    // for arbitrary depth, but the sub-namespace scoping
    // (parent/child/grandchild runs all coexist under
    // `runs/<runId>/` in the same workflow-run repo) has only been
    // exercised at depth 1.
    const grandchildAgent = defineAgent({
      id: "agent-grandchild-step",
      systemPrompt: "You are the grandchild workflow's step agent.",
      tools: [],
      capabilities: [],
      inference: {
        sources: [{ provider: "anthropic", model: "mock-model" }],
      },
    });
    const childStepAgent = defineAgent({
      id: "agent-nested-child-step",
      systemPrompt: "You are the nested child workflow's step agent.",
      tools: [],
      capabilities: [],
      inference: {
        sources: [{ provider: "anthropic", model: "mock-model" }],
      },
    });
    const parentStepAgent = defineAgent({
      id: "agent-nested-parent-step",
      systemPrompt: "You are the nested parent workflow's step agent.",
      tools: [],
      capabilities: [],
      inference: {
        sources: [{ provider: "anthropic", model: "mock-model" }],
      },
    });

    const parentMailAddress = deriveDeploymentAddress({
      deploymentId: NESTED_PARENT_DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
    });
    const childMailAddress = deriveDeploymentAddress({
      deploymentId: NESTED_CHILD_DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
    });
    const grandchildMailAddress = deriveDeploymentAddress({
      deploymentId: NESTED_GRANDCHILD_DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
    });

    const grandchildWorkflowDefinition: WorkflowDefinition = defineWorkflow({
      id: NESTED_GRANDCHILD_WORKFLOW_ID,
      trigger: { type: "mail", to: grandchildMailAddress },
      steps: {
        grandchildStep: step({ agent: grandchildAgent }),
      },
    });
    const childWorkflowDefinition: WorkflowDefinition = defineWorkflow({
      id: NESTED_CHILD_WORKFLOW_ID,
      trigger: { type: "mail", to: childMailAddress },
      steps: {
        childStep: step({ agent: childStepAgent }),
        spawnGrandchild: childWorkflow({
          definitionRef: NESTED_GRANDCHILD_WORKFLOW_ID,
          after: ["childStep"],
        }),
      },
    });
    const parentWorkflowDefinition: WorkflowDefinition = defineWorkflow({
      id: NESTED_PARENT_WORKFLOW_ID,
      trigger: { type: "mail", to: parentMailAddress },
      steps: {
        parentStep: step({ agent: parentStepAgent }),
        spawnChild: childWorkflow({
          definitionRef: NESTED_CHILD_WORKFLOW_ID,
          after: ["parentStep"],
        }),
      },
    });

    const operatorApprovals: ApprovalSet = new Set<string>([
      "inference.source:anthropic:mock-model",
      "director:@intx/agent/default",
      `mail.address:${parentMailAddress}`,
      `mail.address:${childMailAddress}`,
      `mail.address:${grandchildMailAddress}`,
      `mail.send:${DEPLOYMENT_DOMAIN}`,
    ]);

    const baseConfig = (
      address: string,
      agentId: string,
      systemPrompt: string,
    ): HarnessConfig => ({
      sessionId: SESSION_ID,
      agentId,
      tenantId: "tenant-1",
      principalId: "prin_integration-1",
      agentAddress: address,
      systemPrompt,
      tools: [],
      grants: [],
      sources: [
        {
          id: "anthropic:mock-model",
          provider: "anthropic",
          baseURL: `http://localhost:${env.inference.server.port}`,
          apiKey: "sk-mock",
          model: "mock-model",
        },
      ],
      defaultSource: "anthropic:mock-model",
    });

    const launchSession: LaunchSessionFn = async (orchestratorParams) => {
      await env.hub.sessionService.stageWorkflowStep({
        agentAddress: orchestratorParams.agentAddress,
        agentId: orchestratorParams.agentId,
        instanceId: orchestratorParams.instanceId,
        config: orchestratorParams.config,
        deployContent: toLaunchDeployContent(orchestratorParams.deployContent),
        ...(orchestratorParams.toolPackagePins !== undefined
          ? { toolPackagePins: orchestratorParams.toolPackagePins }
          : {}),
      });
    };

    const sendMultiStepDeploy: SendMultiStepDeployFn = async (params) =>
      env.hub.router.sendAgentDeploy(params.agentAddress, params.config, {
        definition: {
          id: params.definition.id,
          triggers: [...params.definition.triggers],
          stepOrder: [...params.definition.stepOrder],
          steps: params.definition.steps as Record<string, unknown>,
          ...(params.definition.state !== undefined
            ? { state: params.definition.state }
            : {}),
        },
        sources: params.sources,
      });

    const workflowRepo: WorkflowRepoWriter = {
      async writeWorkflowRepo(args) {
        const repoId: RepoId = { kind: "workflow", id: args.workflowRepoId };
        const principal: WorkflowRunHubPrincipal = { kind: "hub" };
        const files: Record<string, string> = {};
        for (const [k, v] of args.files) {
          files[k] = v;
        }
        await env.hub.agentRepoStore.repoStore.writeTree(
          principal,
          repoId,
          DEFAULT_ASSET_REF,
          {
            files,
            message: `nested-workflow test: write workflow repo ${args.workflowRepoId}`,
          },
        );
      },
    };

    const orchestrator = createWorkflowDeployOrchestrator({
      directorRegistry: createDefaultDirectorRegistry(),
      workflowRepo,
      launchSession,
      sendMultiStepDeploy,
      // A single-step (child-spawning) workflow deploys once at the head
      // through the deploy core's single-step hand-off.
      deploySingleStepAtHead: (params) =>
        env.hub.sessionService.deploySingleStepAtHead(params),
    });

    // Deploy grandchild and child as workflow assets so the parent's
    // spawn-child resolver can find them. Each gets its own deployment
    // so the workflow-asset substrate carries a distinct
    // `workflow.json` per definitionRef.
    const grandchildResult = await orchestrator.deployWorkflow({
      workflow: grandchildWorkflowDefinition,
      config: baseConfig(
        grandchildMailAddress,
        `ins_${NESTED_GRANDCHILD_DEPLOYMENT_ID}`,
        "Fallback prompt (overridden per step).",
      ),
      deployContent: {
        systemPrompt: "Fallback prompt (overridden per step).",
      },
      operatorApprovals,
      deploymentId: NESTED_GRANDCHILD_DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
      hubPublicKey: "00".repeat(32),
    });
    expect(grandchildResult.publicKey).toBeTruthy();

    const childResult = await orchestrator.deployWorkflow({
      workflow: childWorkflowDefinition,
      config: baseConfig(
        childMailAddress,
        `ins_${NESTED_CHILD_DEPLOYMENT_ID}`,
        "Fallback prompt (overridden per step).",
      ),
      deployContent: {
        systemPrompt: "Fallback prompt (overridden per step).",
      },
      operatorApprovals,
      deploymentId: NESTED_CHILD_DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
      hubPublicKey: "00".repeat(32),
    });
    expect(childResult.publicKey).toBeTruthy();

    const parentResult = await orchestrator.deployWorkflow({
      workflow: parentWorkflowDefinition,
      config: baseConfig(
        parentMailAddress,
        `ins_${NESTED_PARENT_DEPLOYMENT_ID}`,
        "Fallback prompt (overridden per step).",
      ),
      deployContent: {
        systemPrompt: "Fallback prompt (overridden per step).",
      },
      operatorApprovals,
      deploymentId: NESTED_PARENT_DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
      hubPublicKey: "00".repeat(32),
    });
    expect(parentResult.publicKey).toBeTruthy();

    const parentWorkflowRunRepoId: RepoId = {
      kind: "workflow-run",
      id: deriveDeploymentId(parentMailAddress),
    };
    env.registerDeployment({
      deploymentId: NESTED_PARENT_DEPLOYMENT_ID,
      workflowDefinition: parentWorkflowDefinition,
      workflowRunRepoId: parentWorkflowRunRepoId,
      workflowRunRef: WORKFLOW_RUN_REF,
      mailAddress: parentMailAddress,
    });

    expect(env.hub.router.getRoutableAddresses()).toContain(parentMailAddress);

    await fireMailTrigger(env, parentMailAddress, {
      messageId: "<nested-1@integration.interchange>",
    });

    const parentRunId = await waitForFirstRunId(env, parentWorkflowRunRepoId, {
      diagnostics: env.sidecarDiagnostics,
      timeoutMs: 30_000,
    });

    // Wait for the parent run to terminate. By then the child and
    // grandchild runs must have terminated too (the parent's
    // ChildCompleted only commits after the child returns terminal,
    // and the child's ChildCompleted likewise gates on the
    // grandchild).
    await waitFor(
      async () => {
        const events = await readWorkflowRunEvents(
          env,
          NESTED_PARENT_DEPLOYMENT_ID,
          parentRunId,
        );
        return events.some((e) => e.type === "RunCompleted");
      },
      { diagnostics: env.sidecarDiagnostics, timeoutMs: 60_000 },
    );

    const parentEvents = await readWorkflowRunEvents(
      env,
      NESTED_PARENT_DEPLOYMENT_ID,
      parentRunId,
    );
    const parentSpawned = parentEvents.find((e) => e.type === "ChildSpawned");
    if (parentSpawned === undefined) {
      throw new Error("nested test: parent run has no ChildSpawned event");
    }
    const childRunId = parentSpawned.body["childRunId"];
    if (typeof childRunId !== "string") {
      throw new Error(
        `nested test: ChildSpawned is missing string childRunId; got ${typeof childRunId}`,
      );
    }
    expect(parentSpawned.body["childDefinitionRef"]).toBe(
      NESTED_CHILD_WORKFLOW_ID,
    );

    const parentChildCompleted = parentEvents.find(
      (e) => e.type === "ChildCompleted",
    );
    if (parentChildCompleted === undefined) {
      throw new Error("nested test: parent has no ChildCompleted event");
    }
    expect(parentChildCompleted.body["childRunId"]).toBe(childRunId);
    expect(parentChildCompleted.body["terminalStatus"]).toBe("completed");

    // Child run lives under `runs/<childRunId>/events/` in the same
    // workflow-run repo (sub-namespace scoping). The child must have
    // its own ChildSpawned referencing the grandchild's runId and a
    // matching ChildCompleted.
    const childEvents = await readWorkflowRunEvents(
      env,
      NESTED_PARENT_DEPLOYMENT_ID,
      childRunId,
    );
    expect(childEvents.length).toBeGreaterThan(0);
    const childSpawned = childEvents.find((e) => e.type === "ChildSpawned");
    if (childSpawned === undefined) {
      throw new Error(
        `nested test: child run ${childRunId} has no ChildSpawned event`,
      );
    }
    const grandchildRunId = childSpawned.body["childRunId"];
    if (typeof grandchildRunId !== "string") {
      throw new Error(
        `nested test: child's ChildSpawned is missing string childRunId; got ${typeof grandchildRunId}`,
      );
    }
    expect(childSpawned.body["childDefinitionRef"]).toBe(
      NESTED_GRANDCHILD_WORKFLOW_ID,
    );

    const childChildCompleted = childEvents.find(
      (e) => e.type === "ChildCompleted",
    );
    if (childChildCompleted === undefined) {
      throw new Error("nested test: child has no ChildCompleted event");
    }
    expect(childChildCompleted.body["childRunId"]).toBe(grandchildRunId);
    expect(childChildCompleted.body["terminalStatus"]).toBe("completed");

    // Grandchild run lives under `runs/<grandchildRunId>/events/` in
    // the same parent workflow-run repo. The sub-namespace scoping
    // collapses cross-rung run logs into one repo without overwrite.
    const grandchildEvents = await readWorkflowRunEvents(
      env,
      NESTED_PARENT_DEPLOYMENT_ID,
      grandchildRunId,
    );
    expect(grandchildEvents.length).toBeGreaterThan(0);
    const grandchildTypes = grandchildEvents.map((e) => e.type);
    const grandchildRunStartedIdx = grandchildTypes.indexOf("RunStarted");
    const grandchildRunCompletedIdx = grandchildTypes.indexOf("RunCompleted");
    expect(grandchildRunStartedIdx).toBeGreaterThanOrEqual(0);
    expect(grandchildRunCompletedIdx).toBeGreaterThan(grandchildRunStartedIdx);

    const grandchildRunStartedBody =
      grandchildEvents[grandchildRunStartedIdx]?.body;
    if (grandchildRunStartedBody === undefined) throw new Error("unreachable");
    expect(grandchildRunStartedBody["runId"]).toBe(grandchildRunId);

    // Cross-rung distinctness: every runId is unique. The
    // sub-namespace scoping would silently collide if the runtime
    // re-used a runId across rungs.
    expect(new Set([parentRunId, childRunId, grandchildRunId]).size).toBe(3);
  });

  test(`parent -> ${String(SIBLINGS_CHILD_COUNT)} siblings via stepOrder`, async () => {
    // Sibling-fanout coverage. The runtime resolves
    // dependency-graph order via stepOrder; the parent must record
    // one ChildSpawned + one ChildCompleted per sibling and
    // reach RunCompleted with every sibling's run materialised under
    // a distinct `runs/<childRunId>/` sub-namespace.
    const parentStepAgent = defineAgent({
      id: "agent-siblings-parent-step",
      systemPrompt: "You are the siblings parent step agent.",
      tools: [],
      capabilities: [],
      inference: {
        sources: [{ provider: "anthropic", model: "mock-model" }],
      },
    });
    const siblingAgents = SIBLINGS_CHILD_DEPLOYMENT_IDS.map((id) =>
      defineAgent({
        id: `agent-${id}-step`,
        systemPrompt: `You are the ${id} workflow's step agent.`,
        tools: [],
        capabilities: [],
        inference: {
          sources: [{ provider: "anthropic", model: "mock-model" }],
        },
      }),
    );

    const parentMailAddress = deriveDeploymentAddress({
      deploymentId: SIBLINGS_PARENT_DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
    });
    const siblingMailAddresses = SIBLINGS_CHILD_DEPLOYMENT_IDS.map(
      (deploymentId) =>
        deriveDeploymentAddress({
          deploymentId,
          deploymentDomain: DEPLOYMENT_DOMAIN,
        }),
    );

    const siblingWorkflowDefinitions: WorkflowDefinition[] =
      SIBLINGS_CHILD_DEPLOYMENT_IDS.map((id, i) => {
        const address = siblingMailAddresses[i];
        const wfId = SIBLINGS_CHILD_WORKFLOW_IDS[i];
        const agent = siblingAgents[i];
        if (
          address === undefined ||
          wfId === undefined ||
          agent === undefined
        ) {
          throw new Error("unreachable");
        }
        return defineWorkflow({
          id: wfId,
          trigger: { type: "mail", to: address },
          steps: {
            [`${id}Step`]: step({ agent }),
          },
        });
      });

    // Parent steps: one parentStep, then 5 childWorkflow primitives
    // each after parentStep. Express via spread; type the steps record
    // explicitly so `defineWorkflow`'s narrow accepts it.
    const parentSteps: Record<
      string,
      ReturnType<typeof step> | ReturnType<typeof childWorkflow>
    > = {
      parentStep: step({ agent: parentStepAgent }),
    };
    for (let i = 0; i < SIBLINGS_CHILD_COUNT; i += 1) {
      const wfId = SIBLINGS_CHILD_WORKFLOW_IDS[i];
      if (wfId === undefined) throw new Error("unreachable");
      parentSteps[`spawn${(i + 1).toString()}`] = childWorkflow({
        definitionRef: wfId,
        after: ["parentStep"],
      });
    }

    const parentWorkflowDefinition: WorkflowDefinition = defineWorkflow({
      id: SIBLINGS_PARENT_WORKFLOW_ID,
      trigger: { type: "mail", to: parentMailAddress },
      steps: parentSteps,
    });

    const operatorApprovals: ApprovalSet = new Set<string>([
      "inference.source:anthropic:mock-model",
      "director:@intx/agent/default",
      `mail.address:${parentMailAddress}`,
      `mail.send:${DEPLOYMENT_DOMAIN}`,
      ...siblingMailAddresses.map((a) => `mail.address:${a}`),
    ]);

    const baseConfig = (
      address: string,
      agentId: string,
      systemPrompt: string,
    ): HarnessConfig => ({
      sessionId: SESSION_ID,
      agentId,
      tenantId: "tenant-1",
      principalId: "prin_integration-1",
      agentAddress: address,
      systemPrompt,
      tools: [],
      grants: [],
      sources: [
        {
          id: "anthropic:mock-model",
          provider: "anthropic",
          baseURL: `http://localhost:${env.inference.server.port}`,
          apiKey: "sk-mock",
          model: "mock-model",
        },
      ],
      defaultSource: "anthropic:mock-model",
    });

    const launchSession: LaunchSessionFn = async (orchestratorParams) => {
      await env.hub.sessionService.stageWorkflowStep({
        agentAddress: orchestratorParams.agentAddress,
        agentId: orchestratorParams.agentId,
        instanceId: orchestratorParams.instanceId,
        config: orchestratorParams.config,
        deployContent: toLaunchDeployContent(orchestratorParams.deployContent),
        ...(orchestratorParams.toolPackagePins !== undefined
          ? { toolPackagePins: orchestratorParams.toolPackagePins }
          : {}),
      });
    };

    const sendMultiStepDeploy: SendMultiStepDeployFn = async (params) =>
      env.hub.router.sendAgentDeploy(params.agentAddress, params.config, {
        definition: {
          id: params.definition.id,
          triggers: [...params.definition.triggers],
          stepOrder: [...params.definition.stepOrder],
          steps: params.definition.steps as Record<string, unknown>,
          ...(params.definition.state !== undefined
            ? { state: params.definition.state }
            : {}),
        },
        sources: params.sources,
      });

    const workflowRepo: WorkflowRepoWriter = {
      async writeWorkflowRepo(args) {
        const repoId: RepoId = { kind: "workflow", id: args.workflowRepoId };
        const principal: WorkflowRunHubPrincipal = { kind: "hub" };
        const files: Record<string, string> = {};
        for (const [k, v] of args.files) {
          files[k] = v;
        }
        await env.hub.agentRepoStore.repoStore.writeTree(
          principal,
          repoId,
          DEFAULT_ASSET_REF,
          {
            files,
            message: `siblings test: write workflow repo ${args.workflowRepoId}`,
          },
        );
      },
    };

    const orchestrator = createWorkflowDeployOrchestrator({
      directorRegistry: createDefaultDirectorRegistry(),
      workflowRepo,
      launchSession,
      sendMultiStepDeploy,
      // A single-step (child-spawning) workflow deploys once at the head
      // through the deploy core's single-step hand-off.
      deploySingleStepAtHead: (params) =>
        env.hub.sessionService.deploySingleStepAtHead(params),
    });

    for (let i = 0; i < SIBLINGS_CHILD_COUNT; i += 1) {
      const def = siblingWorkflowDefinitions[i];
      const address = siblingMailAddresses[i];
      const depId = SIBLINGS_CHILD_DEPLOYMENT_IDS[i];
      if (def === undefined || address === undefined || depId === undefined) {
        throw new Error("unreachable");
      }
      const r = await orchestrator.deployWorkflow({
        workflow: def,
        config: baseConfig(
          address,
          `ins_${depId}`,
          "Fallback prompt (overridden per step).",
        ),
        deployContent: {
          systemPrompt: "Fallback prompt (overridden per step).",
        },
        operatorApprovals,
        deploymentId: depId,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        hubPublicKey: "00".repeat(32),
      });
      expect(r.publicKey).toBeTruthy();
    }

    const parentResult = await orchestrator.deployWorkflow({
      workflow: parentWorkflowDefinition,
      config: baseConfig(
        parentMailAddress,
        `ins_${SIBLINGS_PARENT_DEPLOYMENT_ID}`,
        "Fallback prompt (overridden per step).",
      ),
      deployContent: {
        systemPrompt: "Fallback prompt (overridden per step).",
      },
      operatorApprovals,
      deploymentId: SIBLINGS_PARENT_DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
      hubPublicKey: "00".repeat(32),
    });
    expect(parentResult.publicKey).toBeTruthy();

    const parentWorkflowRunRepoId: RepoId = {
      kind: "workflow-run",
      id: deriveDeploymentId(parentMailAddress),
    };
    env.registerDeployment({
      deploymentId: SIBLINGS_PARENT_DEPLOYMENT_ID,
      workflowDefinition: parentWorkflowDefinition,
      workflowRunRepoId: parentWorkflowRunRepoId,
      workflowRunRef: WORKFLOW_RUN_REF,
      mailAddress: parentMailAddress,
    });

    expect(env.hub.router.getRoutableAddresses()).toContain(parentMailAddress);

    await fireMailTrigger(env, parentMailAddress, {
      messageId: "<siblings-1@integration.interchange>",
    });

    const parentRunId = await waitForFirstRunId(env, parentWorkflowRunRepoId, {
      diagnostics: env.sidecarDiagnostics,
      timeoutMs: 30_000,
    });

    // Wait for the parent run to reach RunCompleted. Every sibling
    // must terminate before the parent's terminal lands because the
    // runtime's spawn step does not complete until the child's
    // terminal status is committed.
    await waitFor(
      async () => {
        const events = await readWorkflowRunEvents(
          env,
          SIBLINGS_PARENT_DEPLOYMENT_ID,
          parentRunId,
        );
        return events.some((e) => e.type === "RunCompleted");
      },
      { diagnostics: env.sidecarDiagnostics, timeoutMs: 90_000 },
    );

    const parentEvents = await readWorkflowRunEvents(
      env,
      SIBLINGS_PARENT_DEPLOYMENT_ID,
      parentRunId,
    );
    const spawnedEvents = parentEvents.filter((e) => e.type === "ChildSpawned");
    const completedEvents = parentEvents.filter(
      (e) => e.type === "ChildCompleted",
    );
    expect(spawnedEvents.length).toBe(SIBLINGS_CHILD_COUNT);
    expect(completedEvents.length).toBe(SIBLINGS_CHILD_COUNT);

    // Each ChildSpawned must reference a distinct definitionRef and a
    // distinct childRunId; every ChildCompleted must reference one of
    // those runIds with terminalStatus completed.
    const spawnedRefs = new Set<string>();
    const spawnedRunIds = new Set<string>();
    for (const ev of spawnedEvents) {
      const ref = ev.body["childDefinitionRef"];
      const runId = ev.body["childRunId"];
      if (typeof ref !== "string") {
        throw new Error(
          `siblings test: ChildSpawned missing string childDefinitionRef`,
        );
      }
      if (typeof runId !== "string") {
        throw new Error(
          `siblings test: ChildSpawned missing string childRunId`,
        );
      }
      spawnedRefs.add(ref);
      spawnedRunIds.add(runId);
    }
    expect(spawnedRefs.size).toBe(SIBLINGS_CHILD_COUNT);
    expect(spawnedRunIds.size).toBe(SIBLINGS_CHILD_COUNT);
    expect([...spawnedRefs].sort()).toEqual(
      [...SIBLINGS_CHILD_WORKFLOW_IDS].sort(),
    );

    const completedRunIds = new Set<string>();
    for (const ev of completedEvents) {
      const runId = ev.body["childRunId"];
      const status = ev.body["terminalStatus"];
      if (typeof runId !== "string") {
        throw new Error(
          `siblings test: ChildCompleted missing string childRunId`,
        );
      }
      expect(status).toBe("completed");
      completedRunIds.add(runId);
    }
    expect(completedRunIds).toEqual(spawnedRunIds);

    // Each sibling run materialised under a distinct
    // `runs/<childRunId>/` sub-namespace and reached its own
    // RunCompleted.
    for (const childRunId of spawnedRunIds) {
      const childEvents = await readWorkflowRunEvents(
        env,
        SIBLINGS_PARENT_DEPLOYMENT_ID,
        childRunId,
      );
      expect(childEvents.length).toBeGreaterThan(0);
      const types = childEvents.map((e) => e.type);
      expect(types.indexOf("RunStarted")).toBeGreaterThanOrEqual(0);
      expect(types.indexOf("RunCompleted")).toBeGreaterThan(
        types.indexOf("RunStarted"),
      );
    }
  });
});

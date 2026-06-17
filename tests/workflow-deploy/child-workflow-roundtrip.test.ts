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
import fs from "node:fs";
import git from "isomorphic-git";

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
import { deriveTrivialDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";
import type { RepoId, WorkflowRunHubPrincipal } from "@intx/hub-sessions";
import { DEFAULT_ASSET_REF } from "@intx/hub-sessions";

import {
  SESSION_ID,
  SIDECAR_ID,
  fireMailTrigger,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const PARENT_DEPLOYMENT_ID = "child-workflow-parent-1";
const CHILD_DEPLOYMENT_ID = "child-workflow-child-1";
const CHILD_WORKFLOW_ID = `wf_${CHILD_DEPLOYMENT_ID}`;
const PARENT_WORKFLOW_ID = `wf_${PARENT_DEPLOYMENT_ID}`;
const WORKFLOW_RUN_REF = "refs/heads/main";

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
      await env.hub.sessionService.launchSession({
        agentAddress: orchestratorParams.agentAddress,
        agentId: orchestratorParams.agentId,
        instanceId: orchestratorParams.instanceId,
        config: orchestratorParams.config,
        deployContent: orchestratorParams.deployContent as Parameters<
          typeof env.hub.sessionService.launchSession
        >[0]["deployContent"],
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
          // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the wire validator carries the WorkflowDefinition steps record as Record<string, unknown>; the orchestrator emits the typed primitive union that satisfies the schema
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
    expect(childResult.kind).toBe("multi-step");

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
    expect(parentResult.kind).toBe("multi-step");

    const parentWorkflowRunRepoId: RepoId = {
      kind: "workflow-run",
      id: deriveTrivialDeploymentId(parentMailAddress),
    };
    env.registerDeployment({
      deploymentId: PARENT_DEPLOYMENT_ID,
      workflowDefinition: parentWorkflowDefinition,
      workflowRunRepoId: parentWorkflowRunRepoId,
      workflowRunRef: WORKFLOW_RUN_REF,
      mailAddress: parentMailAddress,
      supervisor: null,
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
});

/**
 * Poll the deployment's workflow-run repo for the first `runs/<runId>/`
 * entry the supervisor lands. The supervisor mints the runId from the
 * inbound mail bytes, so the test does not know it up front.
 */
async function waitForFirstRunId(
  env: DeployFlowEnv,
  workflowRunRepoId: RepoId,
  opts: { timeoutMs?: number; diagnostics?: () => string } = {},
): Promise<string> {
  const { timeoutMs = 10_000, diagnostics } = opts;
  const start = Date.now();
  for (;;) {
    let repoDir: string;
    try {
      repoDir = env.hub.agentRepoStore.repoStore.getRepoDir(workflowRunRepoId);
    } catch {
      repoDir = "";
    }
    if (repoDir.length > 0) {
      try {
        const oid = await git.resolveRef({
          fs,
          dir: repoDir,
          ref: "refs/heads/main",
        });
        const tree = await git.readTree({
          fs,
          dir: repoDir,
          oid,
          filepath: "runs",
        });
        const first = tree.tree.find((entry) => entry.type === "tree");
        if (first !== undefined) return first.path;
      } catch {
        /* ref/tree not present yet */
      }
    }
    if (Date.now() - start > timeoutMs) {
      const diag = diagnostics?.();
      const ctx = diag ? `\n${diag}` : "";
      throw new Error(
        `waitForFirstRunId timed out after ${String(timeoutMs)}ms for ${workflowRunRepoId.id}${ctx}`,
      );
    }
    await new Promise((r) => setTimeout(r, 50));
  }
}

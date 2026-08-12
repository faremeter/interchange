// CP-tools verify artifact for the agent->workflow fold.
//
// Proves the FOLDED representation works end-to-end through the real deploy
// path: a single-step workflow whose step agent carries its own tool-package
// pins on the definition (`toolPackagePins`), with nothing passed via the
// deploy call's `toolPackagePins`. The pins reach the child only because they
// ride on the agent (the A3b-2 threading); the sidecar loads them and its tool
// floor authorizes the tool at runtime, exactly as an agent instance does.
//
// It also folds in the failover-through-the-fold check: the deploy's source
// chain is a dead head (HTTP 500) followed by the healthy mock, both
// operator-approved. The child's reactor fails over forward off the dead head
// to the healthy tail -- which drives the tool call -- so a single run proves
// (1) the agent's pins materialized the real tool in-child with nothing at the
// deploy call, and (2) whole-workflow failover is preserved for a single-step
// folded agent.

import fs from "node:fs";
import path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { createDefaultDirectorRegistry } from "@intx/agent";
import type { HarnessConfig } from "@intx/types/runtime";
import type { ToolPackagePin } from "@intx/types/tool-packages";
import {
  createWorkflowDeployOrchestrator,
  deriveRunAddress,
  type ApprovalSet,
  type DeploySingleStepFn,
  type LaunchSessionFn,
  type SendMultiStepDeployFn,
  type WorkflowRepoWriter,
} from "@intx/workflow-deploy";
import { synthesizeFoldedWorkflow } from "@intx/workflow-deploy/testing";
import { deriveDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";
import type { RepoId, WorkflowRunHubPrincipal } from "@intx/hub-sessions";
import { DEFAULT_ASSET_REF } from "@intx/hub-sessions";

import {
  SESSION_ID,
  fireMailTrigger,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitForFirstRunId,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { toLaunchDeployContent } from "./launch-session-bridge";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "folded-tools-failover-1";
const WORKFLOW_RUN_REF = "refs/heads/main";

// The namespaced tool name the model calls and the walk grants on.
const TOOL_NAME = "@intx/tools-mail/sidecar-bundle:mail_send";
const SENTINEL_FILENAME = "folded-tool-ran.txt";
const SENTINEL_CONTENT = "executed-in-child";

const TOOL_PINS: readonly ToolPackagePin[] = [
  { name: "@intx/tools-mail", version: "0.1.2" },
];

let env: DeployFlowEnv;
// The dead head source: always HTTP 500 (a retryable category) so the child's
// reactor fails over off it. Owned here, not by the deploy-flow fixture.
let headRequests = 0;
let deadHead: ReturnType<typeof Bun.serve>;

beforeAll(async () => {
  env = await startDeployFlowEnv({
    inferenceToolCall: {
      toolName: TOOL_NAME,
      input: { to: SENTINEL_CONTENT, body: SENTINEL_FILENAME },
    },
  });
  deadHead = Bun.serve({
    port: 0,
    fetch() {
      headRequests += 1;
      return new Response("upstream boom", { status: 500 });
    },
  });
});

afterAll(async () => {
  await env.teardown();
  await deadHead.stop(true);
});

describe("folded tools + failover real-agent round-trip", () => {
  test("an on-asset folded agent materializes its tool in-child and fails over to the healthy source", async () => {
    const deploymentMailAddress = deriveRunAddress({
      runId: DEPLOYMENT_ID,
      domain: DEPLOYMENT_DOMAIN,
    });

    // The folded artifact, produced by the real synthesizer (A4-c). Its tool
    // pins ride on the step agent (pins-only): the sidecar materializes
    // `toolPackagePins` and its tool floor authorizes the tool at runtime,
    // with no live tool factories and nothing passed at the deploy call.
    const workflow = synthesizeFoldedWorkflow({
      workflowId: `wf_${DEPLOYMENT_ID}`,
      mailAddress: deploymentMailAddress,
      systemPrompt: "You are the folded single-step tool agent.",
      description: null,
      inferencePreferences: [{ provider: "anthropic", model: "mock-model" }],
      toolPackagePins: TOOL_PINS,
    });

    // The singular shorthand names the lone step; the sentinel path below keys
    // on it rather than a hard-coded id.
    const stepId = workflow.stepOrder[0];
    if (stepId === undefined) {
      throw new Error("synthesized folded workflow has no step");
    }

    // A two-element failover chain: element 0 is the dead head (default), the
    // tail is the healthy mock. Both share the agent's declared (provider,
    // model), so the one inference-source approval covers both.
    const config: HarnessConfig = {
      sessionId: SESSION_ID,
      agentId: `ins_${DEPLOYMENT_ID}`,
      tenantId: "tenant-1",
      principalId: "prin_folded-1",
      agentAddress: deploymentMailAddress,
      systemPrompt: "Fallback prompt (overridden per step by the orchestrator)",
      tools: [],
      grants: [],
      sources: [
        {
          id: "anthropic:dead-head",
          provider: "anthropic",
          baseURL: `http://localhost:${deadHead.port}`,
          apiKey: "sk-dead",
          model: "mock-model",
        },
        {
          id: "anthropic:mock-model",
          provider: "anthropic",
          baseURL: `http://localhost:${env.inference.server.port}`,
          apiKey: "sk-mock",
          model: "mock-model",
        },
      ],
      defaultSource: "anthropic:dead-head",
    };

    // Pins-only: the tool is authorized by the sidecar floor at runtime, so
    // the deploy's approval gate needs no `tool:<name>` grant -- it gates the
    // inference source, director, and mail grants only.
    const operatorApprovals: ApprovalSet = new Set<string>([
      "inference.source:anthropic:mock-model",
      "director:@intx/agent/default",
      `mail.address:${deploymentMailAddress}`,
      `mail.send:${DEPLOYMENT_DOMAIN}`,
    ]);

    const launchSession: LaunchSessionFn = async (orchestratorParams) => {
      await env.hub.sessionService.stageWorkflowStep({
        agentAddress: orchestratorParams.agentAddress,
        agentId: orchestratorParams.agentId,
        runId: orchestratorParams.instanceId,
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

    const deploySingleStepAtHead: DeploySingleStepFn = (params) =>
      env.hub.sessionService.deploySingleStepAtHead(params);

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
            message: `folded-tools-failover test: write workflow repo ${args.workflowRepoId}`,
          },
        );
      },
    };

    const orchestrator = createWorkflowDeployOrchestrator({
      directorRegistry: createDefaultDirectorRegistry(),
      workflowRepo,
      launchSession,
      sendMultiStepDeploy,
      deploySingleStepAtHead,
    });

    let result: Awaited<ReturnType<typeof orchestrator.deployWorkflow>>;
    try {
      // NOTE: no `toolPackagePins` passed here -- the pins reach the child
      // only because they ride on the folded agent (A3b-2 threading).
      result = await orchestrator.deployWorkflow({
        workflow,
        config,
        deployContent: { systemPrompt: config.systemPrompt },
        operatorApprovals,
        deploymentId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        hubPublicKey: "00".repeat(32),
      });
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      const diag = env.sidecarDiagnostics();
      throw new Error(
        `deployWorkflow failed: ${message}\n${diag.length > 0 ? diag : "<no sidecar diagnostics>"}`,
        { cause },
      );
    }
    expect(result.publicKey).toBeTruthy();

    const workflowRunRepoId: RepoId = {
      kind: "workflow-run",
      id: deriveDeploymentId(deploymentMailAddress),
    };
    env.registerDeployment({
      deploymentId: DEPLOYMENT_ID,
      workflowDefinition: workflow,
      workflowRunRepoId,
      workflowRunRef: WORKFLOW_RUN_REF,
      mailAddress: deploymentMailAddress,
    });

    await fireMailTrigger(env, deploymentMailAddress, {
      messageId: "<folded-tools-failover-1@integration.interchange>",
    });

    const runId = await waitForFirstRunId(env, workflowRunRepoId, {
      diagnostics: env.sidecarDiagnostics,
      timeoutMs: 20_000,
    });

    const terminal = await waitForWorkflowRunComplete(
      env,
      DEPLOYMENT_ID,
      runId,
      {
        timeoutMs: 20_000,
        diagnostics: env.sidecarDiagnostics,
      },
    );
    if (terminal.type !== "RunCompleted") {
      const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
      const failed = events.find(
        (e) => e.type === "StepFailed" || e.type === "RunFailed",
      );
      throw new Error(
        `expected RunCompleted, got ${terminal.type}: ${JSON.stringify(failed?.body)}\n${env.sidecarDiagnostics()}`,
      );
    }
    expect(terminal.type).toBe("RunCompleted");

    // Failover happened: the dead head was tried (and 500'd) before the reactor
    // advanced to the healthy tail that served the tool call.
    expect(headRequests).toBeGreaterThanOrEqual(1);
    expect(env.inference.requests.length).toBeGreaterThanOrEqual(2);

    // The materialized tool reached inference -- proof the loader ran in-child
    // from the AGENT's pins (none were passed at deploy time).
    const firstServed = env.inference.requests[0];
    if (firstServed === undefined) {
      throw new Error("no inference request captured");
    }
    const toolNames = (firstServed.tools ?? []).map((t) => t.name);
    expect(toolNames).toContain(TOOL_NAME);

    // THE PROOF the tool ran in-child: its `run` wrote a sentinel file into the
    // step agent's stable per-agent workspace.
    const stepWorkspace = path.join(
      env.sidecar.dataDir,
      "workflow-step-state",
      workflowRunRepoId.id,
      "warm",
      encodeURIComponent(stepId),
      "workspace",
    );
    const sentinelPath = path.join(stepWorkspace, SENTINEL_FILENAME);
    if (!fs.existsSync(sentinelPath)) {
      throw new Error(
        `tool sentinel file ${sentinelPath} was not written; the folded agent's pins did not materialize the tool in the child\n${env.sidecarDiagnostics()}`,
      );
    }
    expect(fs.readFileSync(sentinelPath, "utf-8")).toBe(SENTINEL_CONTENT);
  });
});

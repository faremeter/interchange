// Per-run grants barrier proof.
//
// The supervisor pushes the deployment's credentialsSnapshot to the child
// on a per-run basis -- right before each run's `trigger.fire`, gated by
// the dispatch loop's `onRunStart` barrier -- rather than once per spawn.
// The push is the child's authorize prerequisite: the child's authorize
// closure throws on a null snapshot, so a run whose grants never landed
// cannot authorize any resource. A granted tool that runs to completion
// therefore proves the per-run push landed on the child ahead of the
// trigger.
//
// This test deploys a one-step workflow whose step agent carries a granted
// tool (the synthetic `@intx/tools-mail` bundle seeded by the deploy-flow
// fixture), fires an inbound mail to trigger a run, and asserts the run
// reaches `RunCompleted` with the tool having executed in the child. The
// grants ride the real deploy path (the operator-approved rule the hub
// ships in-band, written into the step's agent-state repo by the deploy
// router's grants bridge); the supervisor's `onRunStart` sink reads them
// back with `assembleCredentialsSnapshot` and pushes them per run. Nothing
// here injects a snapshot at the supervisor boundary -- the grants resolve
// from the deployed repo exactly as production resolves them.

import fs from "node:fs";
import path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { defineAgent, createDefaultDirectorRegistry } from "@intx/agent";
import type { HarnessConfig } from "@intx/types/runtime";
import type { ToolPackagePin } from "@intx/types/tool-packages";
import { defineWorkflow, step, type WorkflowDefinition } from "@intx/workflow";
import {
  createWorkflowDeployOrchestrator,
  deriveDeploymentAddress,
  type ApprovalSet,
  type DeploySingleStepFn,
  type LaunchSessionFn,
  type SendMultiStepDeployFn,
  type WorkflowRepoWriter,
} from "@intx/workflow-deploy";
import { deriveDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";
import { sanitizeAddress } from "@intx/hub-agent";
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
const DEPLOYMENT_ID = "single-step-per-run-grants-1";
const STEP_ID = "step1";

const TOOL_NAME = "@intx/tools-mail/sidecar-bundle:mail_send";
const SENTINEL_FILENAME = "per-run-grants-ran.txt";
const SENTINEL_CONTENT = "authorized-per-run";

const TOOL_PINS: readonly ToolPackagePin[] = [
  { name: "@intx/tools-mail", version: "0.1.2" },
];

let env: DeployFlowEnv;

beforeAll(async () => {
  env = await startDeployFlowEnv({
    inferenceToolCall: {
      toolName: TOOL_NAME,
      input: { to: SENTINEL_CONTENT, body: SENTINEL_FILENAME },
    },
  });
});

afterAll(async () => {
  await env.teardown();
});

describe("single-step per-run grants barrier", () => {
  test("a granted tool authorizes against the per-run pushed grants and the run completes", async () => {
    const agent = defineAgent({
      id: "agent-step1",
      systemPrompt: "You are the single-step per-run grants agent.",
      tools: [],
      capabilities: [],
      inference: {
        sources: [{ provider: "anthropic", model: "mock-model" }],
      },
    });

    const deploymentMailAddress = deriveDeploymentAddress({
      deploymentId: DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
    });

    const workflow: WorkflowDefinition = defineWorkflow({
      id: `wf_${DEPLOYMENT_ID}`,
      trigger: { type: "mail", to: deploymentMailAddress },
      steps: {
        [STEP_ID]: step({ agent }),
      },
    });

    const config: HarnessConfig = {
      sessionId: SESSION_ID,
      agentId: `ins_${DEPLOYMENT_ID}`,
      tenantId: "tenant-1",
      principalId: "prin_integration-1",
      agentAddress: deploymentMailAddress,
      systemPrompt: "Fallback prompt (overridden per step by the orchestrator)",
      tools: [],
      // The operator-approved grant the hub ships in-band. The deploy
      // router's grants bridge writes it into the step's agent-state repo;
      // the supervisor's `onRunStart` sink reads it back per run and pushes
      // it to the child ahead of the trigger. Without the per-run push
      // landing, the child's authorize would throw on a null snapshot and
      // the run would fail before the tool ran.
      grants: [
        {
          id: "grant-per-run-tool-invoke",
          resource: `tool:${TOOL_NAME}`,
          action: "invoke",
          effect: "allow",
          origin: "creator",
          conditions: null,
          expiresAt: null,
          roleId: null,
          principalId: null,
        },
      ],
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
    };

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
            message: `single-step-per-run-grants test: write workflow repo ${args.workflowRepoId}`,
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
      result = await orchestrator.deployWorkflow({
        workflow,
        config,
        deployContent: { systemPrompt: config.systemPrompt },
        operatorApprovals,
        deploymentId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        hubPublicKey: "00".repeat(32),
        toolPackagePins: TOOL_PINS,
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
      workflowRunRef: "refs/heads/main",
      mailAddress: deploymentMailAddress,
    });

    await fireMailTrigger(env, deploymentMailAddress, {
      messageId: "<single-step-per-run-grants-1@integration.interchange>",
    });

    const runId = await waitForFirstRunId(env, workflowRunRepoId, {
      diagnostics: env.sidecarDiagnostics,
      timeoutMs: 20_000,
    });

    const terminal = await waitForWorkflowRunComplete(
      env,
      DEPLOYMENT_ID,
      runId,
      { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
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

    // The granted tool authorized in the child and executed: its `run`
    // wrote a sentinel into the warm step's stable workspace. If the
    // per-run grants push had not landed, the child's before-tool authz
    // gate would have blocked the call (or its authorize closure would have
    // thrown on a null snapshot) and no sentinel would exist.
    const stepWorkspace = path.join(
      env.sidecar.dataDir,
      "workflow-step-state",
      workflowRunRepoId.id,
      "warm",
      encodeURIComponent(STEP_ID),
      "workspace",
    );
    const sentinelPath = path.join(stepWorkspace, SENTINEL_FILENAME);
    if (!fs.existsSync(sentinelPath)) {
      throw new Error(
        `tool sentinel file ${sentinelPath} was not written; the granted tool did not authorize+run against the per-run grants\n${env.sidecarDiagnostics()}`,
      );
    }
    expect(fs.readFileSync(sentinelPath, "utf-8")).toBe(SENTINEL_CONTENT);

    // The model looped back after the tool_result, so the tool did not
    // silently no-op: the grant genuinely allowed the invocation.
    expect(env.inference.requests.length).toBeGreaterThanOrEqual(2);

    // Sanity: the deploy tree landed at the head (single-step collapse), the
    // on-disk source the child materialized the tool from.
    const headDeployDir = path.join(
      env.sidecar.dataDir,
      sanitizeAddress(deploymentMailAddress),
      "deploy",
    );
    expect(
      fs.existsSync(path.join(headDeployDir, "tool-packages-manifest.json")),
    ).toBe(true);
  });
});

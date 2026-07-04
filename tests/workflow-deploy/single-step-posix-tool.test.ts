// Phase 2 proof: a step's agent runs a REAL tool materialized in the
// spawned workflow-process child.
//
// Deploys a one-step workflow that pins a tool package (the synthetic
// `@intx/tools-mail` tarball seeded by the deploy-flow fixture). The
// hub resolves the pin into a tool-package manifest and ships it to the
// sidecar's per-step deploy tree; the child materializes the pinned
// closure IN-PROCESS (the loader runs in the child), attaches the tool
// factory to the step's agent, and runs the agent.
//
// The mock inference server is configured to emit a `tool_use` turn
// calling the pinned tool on the first request, then a text reply once
// the tool_result lands. The tool's `run` writes a sentinel file into
// the agent's `env.workdir` -- which, for a step agent, is the per-step
// workspace under the sidecar data dir. The test asserts that sentinel
// file exists, proving the tool actually EXECUTED in the child's
// filesystem view. It also asserts the mock saw the follow-up request
// (the tool_result round-trip) and the run reached a terminal phase.
//
// This is the test that proves real tools run in-child for Phase 2.

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
const DEPLOYMENT_ID = "single-step-posix-tool-1";
const WORKFLOW_RUN_REF = "refs/heads/main";
const STEP_ID = "step1";

// The tool the model is told to call. The loader namespaces the
// synthetic bundle's `mail_send` definition under the bundle id.
const TOOL_NAME = "@intx/tools-mail/sidecar-bundle:mail_send";
// The tool's `run` writes a file named after its `body` arg with its
// `to` arg as content; the test drives those values and asserts the
// file lands in the child's per-step workspace.
const SENTINEL_FILENAME = "posix-tool-ran.txt";
const SENTINEL_CONTENT = "executed-in-child";

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

describe("single-step posix-tool in-child execution", () => {
  test("the spawned child materializes and runs a real tool for the step", async () => {
    const agent = defineAgent({
      id: "agent-step1",
      systemPrompt: "You are the single-step tool agent.",
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
      // The child's step authorize now evaluates the agent's grants (the
      // supervisor's credentials snapshot, written from `config.grants`
      // by the deploy router's grants bridge). The tool the model calls
      // must therefore carry an allow grant for its `tool:<name>/invoke`
      // resource, or the inference layer's before-tool authz gate blocks
      // the call and the tool never runs.
      grants: [
        {
          id: "grant-posix-tool-invoke",
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
            message: `single-step-posix-tool test: write workflow repo ${args.workflowRepoId}`,
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
      workflowRunRef: WORKFLOW_RUN_REF,
      mailAddress: deploymentMailAddress,
    });

    await fireMailTrigger(env, deploymentMailAddress, {
      messageId: "<single-step-posix-tool-1@integration.interchange>",
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

    // The model was driven to call the tool on its first turn, then the
    // tool_result fed a second inference turn. Two (or more) requests
    // means the tool executed and the agent looped back -- the tool did
    // not silently no-op.
    expect(env.inference.requests.length).toBeGreaterThanOrEqual(2);

    // The first request must have exposed the materialized tool to the
    // model -- proof the loader ran in the child and the factory's
    // definition reached inference.
    const firstReq = env.inference.requests[0];
    if (firstReq === undefined)
      throw new Error("no inference request captured");
    const toolNames = (firstReq.tools ?? []).map((t) => t.name);
    expect(toolNames).toContain(TOOL_NAME);

    // THE PROOF that the tool ran IN THE CHILD: the tool's `run` wrote a
    // sentinel file into `env.workdir`, which for the warm single-step
    // agent is the STABLE per-agent workspace rooted at
    // `workflow-step-state/<repoId>/warm/<stepId>/workspace` (keyed by the
    // step identity, not the per-message runId, so the workspace is reused
    // across messages and bounded to one dir per agent). The file's
    // presence (with the content the tool was given) means the
    // materialized tool factory's `run` executed in the child's filesystem
    // view.
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
        `tool sentinel file ${sentinelPath} was not written; the materialized tool did not run in the child\n${env.sidecarDiagnostics()}`,
      );
    }
    expect(fs.readFileSync(sentinelPath, "utf-8")).toBe(SENTINEL_CONTENT);

    // The step's deploy tree (carrying the resolved tool-package
    // manifest) landed at the HEAD's legacy agent dir on the child -- the
    // on-disk source the child read for materialization. A single-step
    // workflow collapses its lone step onto the head, so the deploy tree
    // is staged at the deployment (head) address, not a per-step address.
    const headAddress = deriveDeploymentAddress({
      deploymentId: DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
    });
    const headDeployDir = path.join(
      env.sidecar.dataDir,
      sanitizeAddress(headAddress),
      "deploy",
    );
    expect(
      fs.existsSync(path.join(headDeployDir, "tool-packages-manifest.json")),
    ).toBe(true);
  });
});

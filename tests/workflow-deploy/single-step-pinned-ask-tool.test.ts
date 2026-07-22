// End-to-end proof that a PINNED, ask-marked tool authorizes on its own
// static mark, sidecar-side, and suspends for approval.
//
// A pinned tool ships as a tool package that loads in the spawned
// workflow-process child, so the hub's deploy-time capability walk (which
// reads only inline `agent.toolFactories`) never produces a `tool:<name>`
// grant for it. Before the sidecar tool-mark floor, such a tool would
// authorize against nothing and fail closed. This test deploys a one-step
// workflow that pins the synthetic `@intx/tools-mail` tarball whose static
// tool definition carries `approval: "ask"`, supplies NO hand-injected
// grant for the tool, and drives the model to call it.
//
// The proof: the run SUSPENDS (a `SignalAwaited` event lands on the
// workflow-run log) rather than completing or failing. That outcome is the
// discriminator across the three possibilities:
//   - silent allow -> the tool would run and the run would complete;
//   - deny -> the call would be blocked and the step would fail;
//   - ask -> the call suspends awaiting approval.
// Only a derived `ask` floor produces a suspend here, since no other grant
// authorizes the tool at all. The tool must NOT have run (no sentinel), and
// no terminal event may land while the step is parked.
//
// This runs against the REAL substrate path -- the hub resolves the pin,
// ships the manifest to the child, the child materializes the pinned
// closure in-process, derives the floor from the loaded factory's static
// mark, and the before-tool authz gate resolves it -- not a stub that
// bypasses the sidecar derivation.

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
import type { RepoId, WorkflowRunHubPrincipal } from "@intx/hub-sessions";
import { DEFAULT_ASSET_REF } from "@intx/hub-sessions";

import {
  SESSION_ID,
  fireMailTrigger,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { toLaunchDeployContent } from "./launch-session-bridge";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "single-step-pinned-ask-tool-1";
const WORKFLOW_RUN_REF = "refs/heads/main";
const STEP_ID = "step1";

const TOOL_NAME = "@intx/tools-mail/sidecar-bundle:mail_send";
const SENTINEL_FILENAME = "ask-tool-ran.txt";
const SENTINEL_CONTENT = "should-not-run-until-approved";

const TOOL_PINS: readonly ToolPackagePin[] = [
  { name: "@intx/tools-mail", version: "0.1.2" },
];

let env: DeployFlowEnv;

beforeAll(async () => {
  env = await startDeployFlowEnv({
    // The pinned tool's static definition carries `approval: "ask"`, so the
    // sidecar derives an `ask` floor for it -- the whole point of this test.
    approvalMarkedMailTool: true,
    inferenceToolCall: {
      toolName: TOOL_NAME,
      input: { to: SENTINEL_CONTENT, body: SENTINEL_FILENAME },
    },
  });
});

afterAll(async () => {
  await env.teardown();
});

describe("single-step pinned ask-marked tool", () => {
  test("suspends for approval on the sidecar-derived floor with no injected grant", async () => {
    const agent = defineAgent({
      id: "agent-step1",
      systemPrompt: "You are the single-step ask-tool agent.",
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
      // No grant for the pinned tool. The sidecar derives the tool's `ask`
      // floor from its static mark, so the tool authorizes -- and suspends
      // -- on its own. This is the load-bearing difference from the
      // hand-injected-ask approval tests.
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
            message: `single-step-pinned-ask-tool test: write workflow repo ${args.workflowRepoId}`,
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
      messageId: "<single-step-pinned-ask-tool-1@integration.interchange>",
    });

    const runId = await waitForFirstRunId(env, workflowRunRepoId, {
      diagnostics: env.sidecarDiagnostics,
      timeoutMs: 20_000,
    });

    // The run parks on the tool's approval gate: a `SignalAwaited` event
    // lands on the workflow-run log. It reaches the hub through the
    // pack-push pipeline, so wait for it rather than racing the push. Only
    // a derived `ask` floor produces this outcome -- no grant otherwise
    // authorizes the pinned tool.
    await waitFor(
      async () => {
        const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
        return events.some((e) => e.type === "SignalAwaited");
      },
      { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
    );

    const parkedEvents = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
    const parkedTypes = parkedEvents.map((e) => e.type);
    // Suspended, not silently allowed and not denied: no terminal event has
    // landed while the step is parked awaiting approval.
    expect(parkedTypes).not.toContain("RunCompleted");
    expect(parkedTypes).not.toContain("RunFailed");
    expect(parkedTypes).not.toContain("RunCancelled");

    // The tool has NOT run: the `ask` floor suspended the call before
    // execution. The sentinel would only appear if the tool executed.
    const stepWorkspace = path.join(
      env.sidecar.dataDir,
      "workflow-step-state",
      workflowRunRepoId.id,
      "warm",
      encodeURIComponent(STEP_ID),
      "workspace",
    );
    expect(fs.existsSync(path.join(stepWorkspace, SENTINEL_FILENAME))).toBe(
      false,
    );
  });
});

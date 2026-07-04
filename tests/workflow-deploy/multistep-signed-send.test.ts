// Multi-step signed outbound send regression test.
//
// A genuine multi-step (2+ step) workflow deployment must register a
// signing identity for its deployment mail address on the host transport,
// exactly as a single-step deployment does. Every step of a multi-step
// deployment signs its outbound mail as the ONE deployment-wide address
// (`ins_<deploymentId>@<domain>`), so if that address is not registered a
// step's `env.transport.send` rejects with "not registered", the step
// fails, and the run fails.
//
// This deploys a two-step workflow whose sending step calls the
// transport-backed `mail_send` tool (the mock inference drives the tool
// call on the first request that exposes it). The tool routes through the
// real outbound chain -- supervisor-backed transport -> outbound bridge ->
// `outbound.message` IPC -> supervisor `sendOutbound` -> host transport
// SIGNED send -- so the send reaches the host transport as the deployment
// address. The sidecar forwards the delivered `mail.outbound` frame to the
// hub for persistence, where the fixture captures its signing sender. A
// captured frame whose sender is the deployment address is a load-bearing
// proof that the address held a registered signing identity; a registration
// gap would reject the send inside the step and forward no frame.
//
// The sender is a step of a multi-step deployment (not a single-step head),
// so this covers the deployment-scoped registration the single-step path
// already had and the multi-step path lacked.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { defineAgent, createDefaultDirectorRegistry } from "@intx/agent";
import type { HarnessConfig } from "@intx/types/runtime";
import type { ToolPackagePin } from "@intx/types/tool-packages";
import { WireGrantRule } from "@intx/types/grant-wire";
import { defineWorkflow, step, type WorkflowDefinition } from "@intx/workflow";
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
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { toLaunchDeployContent } from "./launch-session-bridge";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "multistep-signed-send-1";
const WORKFLOW_RUN_REF = "refs/heads/main";
// The first step in `stepOrder`; the mock drives its inference to call the
// mail tool, so it is the step that performs the signed send.
const SENDER_STEP_ID = "send";

const TOOL_NAME = "@intx/tools-mail/sidecar-bundle:mail_send";
const GRANTED_RESOURCE = `tool:${TOOL_NAME}`;
const SENTINEL_FILENAME = "multistep-signed-send-receipt.txt";

const TOOL_PINS: readonly ToolPackagePin[] = [
  { name: "@intx/tools-mail", version: "0.1.2" },
];

const GRANTED_RULE: WireGrantRule = {
  id: "grant-tool-invoke",
  resource: GRANTED_RESOURCE,
  action: "invoke",
  effect: "allow",
  origin: "creator",
  conditions: null,
  expiresAt: null,
  roleId: null,
  principalId: null,
};

let env: DeployFlowEnv;
let deploymentMailAddress: string;

beforeAll(async () => {
  deploymentMailAddress = deriveDeploymentAddress({
    deploymentId: DEPLOYMENT_ID,
    deploymentDomain: DEPLOYMENT_DOMAIN,
  });
  // The transport-backed `mail_send` bundle sends through the real outbound
  // chain and sentinels on receipt; `inferenceToolCall` drives the model to
  // call it. The send targets the deployment's own address -- a local,
  // registered recipient once the deployment identity is on the transport --
  // so the signed send delivers without a remote leg.
  env = await startDeployFlowEnv({
    transportBackedMailTool: true,
    inferenceToolCall: {
      toolName: TOOL_NAME,
      input: { to: deploymentMailAddress, body: SENTINEL_FILENAME },
    },
  });
});

afterAll(async () => {
  await env.teardown();
});

describe("multi-step signed outbound send", () => {
  test("sidecar registers with hub", () => {
    expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
  });

  test("a step of a multi-step deployment signs and sends outbound mail", async () => {
    const sendAgent = defineAgent({
      id: "agent-multistep-send",
      systemPrompt: "You are the sending step agent.",
      tools: [],
      capabilities: [],
      inference: {
        sources: [{ provider: "anthropic", model: "mock-model" }],
      },
    });
    const tailAgent = defineAgent({
      id: "agent-multistep-tail",
      systemPrompt: "You are the trailing step agent.",
      tools: [],
      capabilities: [],
      inference: {
        sources: [{ provider: "anthropic", model: "mock-model" }],
      },
    });

    const workflow: WorkflowDefinition = defineWorkflow({
      id: `wf_${DEPLOYMENT_ID}`,
      trigger: { type: "mail", to: deploymentMailAddress },
      steps: {
        [SENDER_STEP_ID]: step({ agent: sendAgent }),
        tail: step({ agent: tailAgent, after: [SENDER_STEP_ID] }),
      },
    });

    const config: HarnessConfig = {
      sessionId: SESSION_ID,
      agentId: `ins_${DEPLOYMENT_ID}`,
      tenantId: "tenant-1",
      principalId: "prin_integration-1",
      agentAddress: deploymentMailAddress,
      systemPrompt: "Fallback prompt (overridden per step by orchestrator)",
      tools: [],
      grants: [GRANTED_RULE],
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
            message: `multistep-signed-send test: write workflow repo ${args.workflowRepoId}`,
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

    expect(env.hub.router.getRoutableAddresses()).toContain(
      deploymentMailAddress,
    );

    await fireMailTrigger(env, deploymentMailAddress, {
      messageId: "<multistep-signed-send-1@integration.interchange>",
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
    expect(terminal.type).toBe("RunCompleted");

    // The proof of the fix: the sidecar signed and delivered a `mail.outbound`
    // frame whose SIGNING SENDER is the deployment mail address. A multi-step
    // step signs its outbound sends as that one deployment address, so the
    // frame reaches the hub only when the address holds a registered signing
    // identity on the host transport. Without the registration the send throws
    // "not registered" inside the step and no frame is ever forwarded, leaving
    // `outboundMail` empty.
    await waitFor(
      () =>
        env.hub.outboundMail.some(
          (m) => m.senderAddress === deploymentMailAddress,
        ),
      { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
    );
    const signedSend = env.hub.outboundMail.find(
      (m) => m.senderAddress === deploymentMailAddress,
    );
    expect(signedSend).toBeDefined();
    expect(signedSend?.recipients).toContain(deploymentMailAddress);
  });
});

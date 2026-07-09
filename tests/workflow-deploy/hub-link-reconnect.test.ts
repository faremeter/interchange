// Smoke test for the hub-link disconnect/reconnect harness surface.
//
// Exercises the three helpers `deploy-flow-env` grew for the
// reconnect-survival acceptance work -- `settleThenDrop`,
// `waitForReconnect`, and (via `settleThenDrop`) `dropHubLink` -- plus the
// `lookupPublicKey`/`liveHandles` wiring that makes a dropped sidecar link
// reconnect instead of looping on a closed socket.
//
// Shape: deploy a single-step workflow, drive one mail trigger to
// `RunCompleted`, settle the pack-push pipeline and drop the hub link,
// wait for the deployment address to become routable again (the reconnect
// ownership challenge passing), then fire a second mail trigger and assert
// it also reaches `RunCompleted`. A deployed workflow survives the
// reconnect: the second run only exists because the sidecar re-established
// the link and re-entered routing.
//
// Harness justification: SPAWN-REAL. A real hub server, a real sidecar
// subprocess, a real workflow-process child, and a test inference
// provider. The drop is a genuine server-side WebSocket close; the
// reconnect is the sidecar's real `hub-link` reconnect path passing the
// hub's ownership challenge.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { defineAgent, createDefaultDirectorRegistry } from "@intx/agent";
import type { HarnessConfig } from "@intx/types/runtime";
import { defineWorkflow, step, type WorkflowDefinition } from "@intx/workflow";
import {
  createWorkflowDeployOrchestrator,
  deriveDeploymentAddress,
  isWorkflowDerivedAddress,
  type ApprovalSet,
  type DeploySingleStepFn,
  type LaunchSessionFn,
  type SendMultiStepDeployFn,
  type WorkflowRepoWriter,
} from "@intx/workflow-deploy";
import { deriveDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";
import {
  DEFAULT_ASSET_REF,
  type RepoId,
  type WorkflowRunHubPrincipal,
} from "@intx/hub-sessions";

import {
  SESSION_ID,
  SIDECAR_ID,
  dropHubLink,
  fireMailTrigger,
  listRunIds,
  settleThenDrop,
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  waitForReconnect,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { toLaunchDeployContent } from "./launch-session-bridge";

const DEPLOYMENT_DOMAIN = "integration.interchange";
// A launched-agent instance id: `ins_` + a hex-shaped local part, so the
// deployment address is the legacy `ins_<hex>` identity rather than a
// workflow-derived `ins_dep_<...>` address.
const DEPLOYMENT_ID = "d15c0nnec7ed0d0d15c0nnec7ed0d0d0";
const WORKFLOW_RUN_REF = "refs/heads/main";
const STEP_ID = "step1";

let env: DeployFlowEnv;
let deploymentMailAddress: string;

beforeAll(async () => {
  deploymentMailAddress = deriveDeploymentAddress({
    deploymentId: DEPLOYMENT_ID,
    deploymentDomain: DEPLOYMENT_DOMAIN,
  });
  env = await startDeployFlowEnv();
});

afterAll(async () => {
  await env.teardown();
});

describe("hub-link drop -> reconnect survival (harness smoke)", () => {
  test("sidecar registers with hub", () => {
    expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
  });

  test("deploy, run, settleThenDrop, reconnect, run again", async () => {
    expect(isWorkflowDerivedAddress(deploymentMailAddress)).toBe(false);

    // ---- deploy a single-step workflow ----
    const agent = defineAgent({
      id: "agent-reconnect-smoke",
      systemPrompt: "You are the reconnect smoke-test agent.",
      tools: [],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
    });
    const workflow: WorkflowDefinition = defineWorkflow({
      id: `wf_${DEPLOYMENT_ID}`,
      trigger: { type: "mail", to: deploymentMailAddress },
      steps: { [STEP_ID]: step({ agent }) },
    });
    const config: HarnessConfig = {
      sessionId: SESSION_ID,
      agentId: `ins_${DEPLOYMENT_ID}`,
      tenantId: "tenant-1",
      principalId: "prin_reconnect-smoke-1",
      agentAddress: deploymentMailAddress,
      systemPrompt: "Fallback prompt (overridden per step by the orchestrator)",
      tools: [],
      grants: [],
      sources: [
        {
          id: "anthropic:mock-model",
          provider: "anthropic",
          baseURL: `http://localhost:${String(env.inference.server.port)}`,
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

    const launchSession: LaunchSessionFn = async (p) => {
      await env.hub.sessionService.stageWorkflowStep({
        agentAddress: p.agentAddress,
        agentId: p.agentId,
        instanceId: p.instanceId,
        config: p.config,
        deployContent: toLaunchDeployContent(p.deployContent),
        ...(p.toolPackagePins !== undefined
          ? { toolPackagePins: p.toolPackagePins }
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
        for (const [k, v] of args.files) files[k] = v;
        await env.hub.agentRepoStore.repoStore.writeTree(
          principal,
          repoId,
          DEFAULT_ASSET_REF,
          { files, message: "reconnect smoke: write workflow repo" },
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

    const result = await orchestrator.deployWorkflow({
      workflow,
      config,
      deployContent: { systemPrompt: config.systemPrompt },
      operatorApprovals,
      deploymentId: DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
      hubPublicKey: "00".repeat(32),
    });
    expect(result.publicKey).toBeTruthy();

    await waitFor(() => env.hub.deployAcks.has(deploymentMailAddress), {
      timeoutMs: 20_000,
      diagnostics: env.sidecarDiagnostics,
    });

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

    // ---- first run to completion ----
    const first = await fireMailTrigger(env, deploymentMailAddress, {
      messageId: "<reconnect-smoke-1@integration.interchange>",
      content: "first",
    });
    const firstRunId = await waitForFirstRunId(env, workflowRunRepoId, {
      timeoutMs: 20_000,
      diagnostics: env.sidecarDiagnostics,
    });
    const firstTerminal = await waitForWorkflowRunComplete(
      env,
      DEPLOYMENT_ID,
      firstRunId,
      { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
    );
    expect(firstTerminal.type).toBe("RunCompleted");

    // ---- settle the pack pipeline, then drop the hub link ----
    expect(env.hub.router.getRoutableAddresses()).toContain(
      deploymentMailAddress,
    );
    await settleThenDrop(env, deploymentMailAddress);

    // The address leaves routing as the server-side close lands.
    await waitFor(
      () =>
        !env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
      { timeoutMs: 5_000, diagnostics: env.sidecarDiagnostics },
    );

    // ---- wait for reconnect + re-route ----
    const reconnectMs = await waitForReconnect(env, deploymentMailAddress, {
      timeoutMs: 20_000,
    });
    // The reconnect is the sidecar's 3s reconnect delay plus a handshake; a
    // generous lower bound guards against a false "already routable" pass
    // that never actually dropped, and the upper bound catches a hung link.
    expect(reconnectMs).toBeGreaterThan(1_000);
    expect(reconnectMs).toBeLessThan(20_000);
    expect(env.hub.router.getRoutableAddresses()).toContain(
      deploymentMailAddress,
    );

    // ---- second run to completion after reconnect ----
    const second = await fireMailTrigger(env, deploymentMailAddress, {
      messageId: "<reconnect-smoke-2@integration.interchange>",
      content: "second",
    });
    expect(second.messageId).not.toBe(first.messageId);

    const secondRunId = await (async () => {
      const start = Date.now();
      for (;;) {
        const ids = await listRunIds(env, workflowRunRepoId);
        const other = ids.find((id) => id !== firstRunId);
        if (other !== undefined) return other;
        if (Date.now() - start > 30_000) {
          throw new Error(
            `no second run after reconnect; saw runIds ${JSON.stringify(ids)}\n${env.sidecarDiagnostics()}`,
          );
        }
        await new Promise((r) => setTimeout(r, 50));
      }
    })();

    const secondTerminal = await waitForWorkflowRunComplete(
      env,
      DEPLOYMENT_ID,
      secondRunId,
      { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
    );
    expect(secondTerminal.type).toBe("RunCompleted");

    // `dropHubLink` is exported for the interrupted-pack regression path;
    // reference it here so the smoke test pins the full helper surface.
    expect(typeof dropHubLink).toBe("function");
  }, 120_000);
});

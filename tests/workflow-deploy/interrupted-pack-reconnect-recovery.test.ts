// An interrupted workflow-run pack recovers on reconnect with no fresh trigger.
//
// The reconnect-LIVENESS proof for the workflow-run-events push. A single-step
// run commits every event in one batch, so the supervisor ships exactly one
// refs/heads/main pack. The harness arms an arm-once interrupt on the hub: the
// FIRST run-events pack is applied durably on the hub, then every live link is
// dropped BEFORE the ack, so the sidecar's push rejects and latches "Connection
// lost". The sidecar then reconnects and re-challenges its deployment address.
//
// The contract this asserts: after the reconnect the run reaches RunCompleted
// on its own -- WITHOUT any fresh mail trigger to re-drive it. The advance-on-ack
// pack-tip cursor keeps the un-acked commits shippable (the data-integrity half);
// this test covers the liveness half -- the sidecar must re-drive the cancelled
// push once its address is routable again, and the re-ship must wait for the
// challenge to re-route the address rather than racing ahead of it.
//
// The settled-drop control is the regression guard: a drop AFTER the pack stream
// goes quiet (no push mid-flight) must still reconnect and run a fresh trigger to
// completion, so the liveness fix does not break ordinary reconnect survival.
//
// Harness justification: SPAWN-REAL. A real hub server, a real sidecar
// subprocess, a real workflow-process child, and a test inference provider. The
// drop is a genuine server-side WebSocket close mid-transfer; the recovery is
// the sidecar's real hub-link reconnect path passing the hub's ownership
// challenge and re-driving the latched push.

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
  fireMailTrigger,
  listRunIds,
  readWorkflowRunEvents,
  settleThenDrop,
  startDeployFlowEnv,
  waitFor,
  waitForReconnect,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { toLaunchDeployContent } from "./launch-session-bridge";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const WORKFLOW_RUN_REF = "refs/heads/main";
const STEP_ID = "step1";

let env: DeployFlowEnv;

beforeAll(async () => {
  env = await startDeployFlowEnv();
});

afterAll(async () => {
  await env.teardown();
});

/**
 * Deploy a one-step workflow through the multi-step branch (which spawns the
 * workflow-process child) and register its handle on the env. Returns the
 * deployment's mail address and workflow-run repo id.
 */
async function deploySingleStepWorkflow(
  deploymentId: string,
): Promise<{ deploymentMailAddress: string; workflowRunRepoId: RepoId }> {
  const deploymentMailAddress = deriveDeploymentAddress({
    deploymentId,
    deploymentDomain: DEPLOYMENT_DOMAIN,
  });

  const agent = defineAgent({
    id: `agent-${deploymentId}`,
    systemPrompt: "You are the interrupted-pack recovery test agent.",
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
  });
  const workflow: WorkflowDefinition = defineWorkflow({
    id: `wf_${deploymentId}`,
    trigger: { type: "mail", to: deploymentMailAddress },
    steps: { [STEP_ID]: step({ agent }) },
  });
  const config: HarnessConfig = {
    sessionId: SESSION_ID,
    agentId: `ins_${deploymentId}`,
    tenantId: "tenant-1",
    principalId: `prin_${deploymentId}`,
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
        { files, message: "interrupted-pack recovery: write workflow repo" },
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
    deploymentId,
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
    deploymentId,
    workflowDefinition: workflow,
    workflowRunRepoId,
    workflowRunRef: WORKFLOW_RUN_REF,
    mailAddress: deploymentMailAddress,
  });

  expect(env.hub.router.getRoutableAddresses()).toContain(
    deploymentMailAddress,
  );

  return { deploymentMailAddress, workflowRunRepoId };
}

/**
 * Poll the deployment's run event log until at least one run reaches a
 * RunCompleted terminal, or throw on timeout. Returns nothing; the assertion
 * is the absence of a throw.
 */
async function waitForAnyRunCompleted(
  deploymentId: string,
  workflowRunRepoId: RepoId,
  timeoutMs: number,
): Promise<void> {
  const start = Date.now();
  for (;;) {
    const ids = await listRunIds(env, workflowRunRepoId);
    for (const id of ids) {
      const events = await readWorkflowRunEvents(env, deploymentId, id);
      if (events.some((e) => e.type === "RunCompleted")) return;
    }
    if (Date.now() - start > timeoutMs) {
      throw new Error(
        `no run reached RunCompleted for ${deploymentId} within ${String(timeoutMs)}ms; runIds=${JSON.stringify(ids)}\n${env.sidecarDiagnostics()}`,
      );
    }
    await new Promise((r) => setTimeout(r, 100));
  }
}

describe("interrupted workflow-run pack recovers on reconnect", () => {
  test("sidecar registers with hub", () => {
    expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
  });

  test("armed mid-pack drop: run completes after reconnect with no fresh trigger", async () => {
    const deploymentId = "dep1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a";
    const { deploymentMailAddress, workflowRunRepoId } =
      await deploySingleStepWorkflow(deploymentId);
    expect(isWorkflowDerivedAddress(deploymentMailAddress)).toBe(false);

    // Arm the interrupt so the FIRST run-events pack of this run is applied
    // on the hub, then every live link is dropped before the ack. The
    // sidecar's push rejects and latches "Connection lost".
    env.hub.interrupt.armed = true;

    await fireMailTrigger(env, deploymentMailAddress, {
      messageId: "<interrupted-pack-1@integration.interchange>",
      content: "trigger",
    });

    // Wait for the interrupt to fire (armed flips back to false) and the
    // address to leave routing as the dropped link closes.
    await waitFor(() => env.hub.interrupt.armed === false, {
      timeoutMs: 30_000,
      diagnostics: env.sidecarDiagnostics,
    });
    expect(env.hub.interrupt.interruptedRef).toBe(WORKFLOW_RUN_REF);
    await waitFor(
      () =>
        !env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
      { timeoutMs: 10_000, diagnostics: env.sidecarDiagnostics },
    );

    // The sidecar reconnects and re-challenges its deployment address.
    const reconnectMs = await waitForReconnect(env, deploymentMailAddress, {
      timeoutMs: 30_000,
    });
    expect(reconnectMs).toBeGreaterThan(0);

    // The liveness contract: the run reaches RunCompleted on its own, with
    // NO fresh mail trigger to re-drive it. Capture the run-id count so the
    // assertion below can also confirm no second run was minted.
    await waitForAnyRunCompleted(deploymentId, workflowRunRepoId, 60_000);

    // Exactly one run exists: the recovery re-shipped the SAME run's events,
    // it did not mint a fresh run. A second run would mean the recovery
    // depended on a new trigger rather than re-driving the cancelled push.
    const finalRunIds = await listRunIds(env, workflowRunRepoId);
    expect(finalRunIds).toHaveLength(1);
  }, 180_000);

  test("settled drop control: a fresh trigger runs to completion after reconnect", async () => {
    const deploymentId = "dep2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b";
    const { deploymentMailAddress, workflowRunRepoId } =
      await deploySingleStepWorkflow(deploymentId);

    // Drive a first run to completion so the pack stream has something to
    // go quiet after -- settleThenDrop waits for a no-new-pack quiet window.
    await fireMailTrigger(env, deploymentMailAddress, {
      messageId: "<settled-control-1@integration.interchange>",
      content: "first",
    });
    await waitForAnyRunCompleted(deploymentId, workflowRunRepoId, 60_000);

    // Drop the link only after the pack stream has drained (no push
    // mid-flight), then wait for the sidecar to reconnect and re-route.
    await settleThenDrop(env, deploymentMailAddress, {
      quietMs: 750,
      timeoutMs: 30_000,
    });
    await waitFor(
      () =>
        !env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
      { timeoutMs: 10_000, diagnostics: env.sidecarDiagnostics },
    );
    await waitForReconnect(env, deploymentMailAddress, { timeoutMs: 30_000 });

    // A fresh trigger on the recovered link runs to completion. Retry the
    // trigger with a fresh message id per attempt: a trigger that lands while
    // a residual reconnect is in flight can be dropped before the supervisor
    // enqueues it.
    const runId = await (async () => {
      const start = Date.now();
      let attempt = 0;
      for (;;) {
        attempt += 1;
        await fireMailTrigger(env, deploymentMailAddress, {
          messageId: `<settled-control-recovered-${String(attempt)}@integration.interchange>`,
          content: "recovered",
        });
        const deadline = Date.now() + 10_000;
        while (Date.now() < deadline) {
          const ids = await listRunIds(env, workflowRunRepoId);
          // The first run already completed; wait for a SECOND run to appear.
          const second = ids[1];
          if (second !== undefined) return second;
          await new Promise((r) => setTimeout(r, 100));
        }
        if (Date.now() - start > 60_000) {
          throw new Error(
            `no fresh run produced on the recovered link after ${String(attempt)} triggers\n${env.sidecarDiagnostics()}`,
          );
        }
      }
    })();
    const terminal = await waitForWorkflowRunComplete(
      env,
      deploymentId,
      runId,
      { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
    );
    expect(terminal.type).toBe("RunCompleted");
  }, 180_000);
});

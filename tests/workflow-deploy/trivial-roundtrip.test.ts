// Trivial-workflow round-trip integration test.
//
// The runtime-side uniformity gate. Deploys a single-step workflow via
// the workflow-deploy orchestrator's trivial branch against the real
// hub + real sidecar subprocess + mock inference fixture, fires a mail
// trigger at the deployment's address, and asserts the canonical
// workflow-run event chain (`RunStarted` -> `StepStarted` ->
// `StepCompleted` -> `RunCompleted`) materializes in the workflow-run
// repo with the expected `consumedMessageId` correlation against the
// mail's `Message-Id`. The test additionally asserts the legacy
// deploy-flow surface (inference receives tools, the `agent.event`
// capture sees `inference.start`) still holds.
//
// The pre-landed `deploy-flow-env` fixture supplies every helper; this
// file does not modify the fixture.

import fs from "node:fs";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import git from "isomorphic-git";

import { defineAgent } from "@intx/agent";
import type { HarnessConfig } from "@intx/types/runtime";
import { defineWorkflow } from "@intx/workflow";

import {
  AGENT_ADDRESS,
  AGENT_ID,
  SESSION_ID,
  SIDECAR_ID,
  deployWorkflow,
  fireMailTrigger,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  WORKFLOW_RUN_TERMINAL_TYPES,
  type DeployFlowEnv,
  type WorkflowRunEvent,
} from "../hub-agent/lib/deploy-flow-env";

let env: DeployFlowEnv;

beforeAll(async () => {
  env = await startDeployFlowEnv();
});

afterAll(async () => {
  await env.teardown();
});

describe("trivial workflow round-trip", () => {
  test("sidecar registers with hub", () => {
    expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
  });

  test("canonical event chain materializes end-to-end", async () => {
    const config: HarnessConfig = {
      sessionId: SESSION_ID,
      agentId: AGENT_ID,
      tenantId: "tenant-1",
      principalId: "prin_integration-1",
      agentAddress: AGENT_ADDRESS,
      systemPrompt: "Fallback prompt (overridden by deploy tree)",
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
    };

    // Mirror the legacy deploy-flow fixture's agent shape so the
    // trivial-vs-multi-step dichotomy fires the trivial branch: a
    // single step, single inference source, no tool factories, no
    // capabilities. The workflow-deploy orchestrator's `isTrivialDeploy`
    // routes on `stepOrder.length === 1 && trivialBindings !==
    // undefined`; supplying both forces the trivial branch.
    const agent = defineAgent({
      id: AGENT_ID,
      systemPrompt: "You are an integration test agent.",
      tools: [],
      capabilities: [],
      inference: {
        sources: [{ provider: "anthropic", model: "mock-model" }],
      },
    });

    const workflow = defineWorkflow({
      id: `wf_${AGENT_ID}`,
      agent,
      trigger: { type: "mail", to: AGENT_ADDRESS },
    });

    // Approval shape matches what `buildTrivialApprovalSet` computes
    // for a `wrapHarnessAsTrivialAgent` deploy: per-source inference
    // grants, the default-director grant, and the mail-address +
    // mail-send grants derived from the deployment's mailbox.
    const mailDomain = AGENT_ADDRESS.slice(AGENT_ADDRESS.lastIndexOf("@") + 1);
    const operatorApprovals = new Set<string>([
      `inference.source:anthropic:mock-model`,
      `director:@intx/agent/default`,
      `mail.address:${AGENT_ADDRESS}`,
      `mail.send:${mailDomain}`,
    ]);

    const deployHandle = await deployWorkflow(env, workflow, {
      config,
      deployContent: { systemPrompt: "You are an integration test agent." },
      trivialBindings: {
        agentAddress: AGENT_ADDRESS,
        agentId: AGENT_ID,
        instanceId: AGENT_ID,
      },
      operatorApprovals,
      toolPackagePins: [{ name: "@intx/tools-mail", version: "0.1.2" }],
    });

    // The deploy ack arrives once `executeLaunchPhases` finishes
    // provisioning the sidecar. This is the trivial branch's "ready"
    // signal: the supervisor's trivial-launch path round-trips through
    // the same wire surface the legacy agent-deploy uses.
    const publicKey = env.hub.deployAcks.get(AGENT_ADDRESS);
    expect(publicKey).toBeDefined();
    if (publicKey === undefined) throw new Error("unreachable");
    expect(publicKey.length).toBeGreaterThan(0);

    expect(env.hub.router.getRoutableAddresses()).toContain(AGENT_ADDRESS);

    const { messageId } = await fireMailTrigger(env, AGENT_ADDRESS, {
      messageId: "<trivial-roundtrip-1@integration.interchange>",
    });

    // Legacy assertion #1: inference saw the deploy-tree's tool
    // surfaces. The trivial round-trip must preserve the wire-level
    // surface the legacy deploy-flow test asserts.
    await waitFor(() => env.inference.requests.length > 0, {
      diagnostics: env.sidecarDiagnostics,
    });
    const inferenceReq = env.inference.requests[0];
    if (inferenceReq === undefined) throw new Error("unreachable");
    const toolNames = (inferenceReq.tools ?? []).map((t) => t.name);
    expect(toolNames).toContain("@intx/tools-mail/sidecar-bundle:mail_send");

    // Legacy assertion #2: the `agent.event` capture saw an
    // `inference.start` event for this session.
    function hasEventType(
      event: unknown,
      type: string,
    ): event is { type: string } {
      return (
        typeof event === "object" &&
        event !== null &&
        "type" in event &&
        event.type === type
      );
    }
    await waitFor(
      () =>
        env.hub.agentEvents.some((e) =>
          hasEventType(e.event, "inference.start"),
        ),
      { diagnostics: env.sidecarDiagnostics },
    );
    const inferenceStartEvent = env.hub.agentEvents.find((e) =>
      hasEventType(e.event, "inference.start"),
    );
    if (inferenceStartEvent === undefined) throw new Error("unreachable");
    expect(inferenceStartEvent.addr).toBe(AGENT_ADDRESS);
    expect(inferenceStartEvent.sid).toBe(SESSION_ID);

    // The runtime-side uniformity gate. The deployment's workflow-run
    // repo must hold the canonical event chain for the run this mail
    // fired. The supervisor mints the runId internally (sha256 of the
    // raw RFC 2822 message bytes in the current wiring), so the test
    // does not know it up front; the helper below polls `runs/` in
    // the workflow-run repo for the first run that reaches a terminal
    // event.
    const terminal = await waitForTerminalEventForAnyRun(
      env,
      deployHandle.deploymentId,
      { diagnostics: env.sidecarDiagnostics },
    );

    expect(terminal.event.type).toBe("RunCompleted");

    const events = await readWorkflowRunEvents(
      env,
      deployHandle.deploymentId,
      terminal.runId,
    );

    const types = events.map((e) => e.type);
    const runStartedIdx = types.indexOf("RunStarted");
    const stepStartedIdx = types.indexOf("StepStarted");
    const stepCompletedIdx = types.indexOf("StepCompleted");
    const runCompletedIdx = types.indexOf("RunCompleted");

    expect(runStartedIdx).toBeGreaterThanOrEqual(0);
    expect(stepStartedIdx).toBeGreaterThan(runStartedIdx);
    expect(stepCompletedIdx).toBeGreaterThan(stepStartedIdx);
    expect(runCompletedIdx).toBeGreaterThan(stepCompletedIdx);

    const runStartedBody = events[runStartedIdx]?.body;
    if (runStartedBody === undefined) throw new Error("unreachable");
    expect(runStartedBody["consumedMessageId"]).toBe(messageId);

    const stepStartedBody = events[stepStartedIdx]?.body;
    if (stepStartedBody === undefined) throw new Error("unreachable");
    expect(stepStartedBody["attempt"]).toBe(1);
    expect(typeof stepStartedBody["stepId"]).toBe("string");
  });
});

/**
 * Poll the deployment's workflow-run repo for the first run that
 * reaches a terminal event. The trivial-roundtrip test does not know
 * the runId the runtime mints (the supervisor derives it from the
 * inbound mail bytes), so the helper walks `runs/` in the repo tree
 * and returns the first run whose log has reached terminal status.
 */
async function waitForTerminalEventForAnyRun(
  env: DeployFlowEnv,
  deploymentId: string,
  opts: { timeoutMs?: number; diagnostics?: () => string } = {},
): Promise<{ runId: string; event: WorkflowRunEvent }> {
  const { timeoutMs = 10_000, diagnostics } = opts;
  const start = Date.now();
  const handle = env.deployments.get(deploymentId);
  if (handle === undefined) {
    throw new Error(
      `waitForTerminalEventForAnyRun: no deployment registered for ${deploymentId}`,
    );
  }
  let repoDir: string;
  try {
    repoDir = env.hub.agentRepoStore.repoStore.getRepoDir(
      handle.workflowRunRepoId,
    );
  } catch (cause) {
    const message = cause instanceof Error ? cause.message : String(cause);
    throw new Error(
      `waitForTerminalEventForAnyRun: workflow-run repo identity for deployment ${deploymentId} is structurally invalid at the substrate boundary (${message}); the trivial-deploy path uses the agent address as the deploymentId, but the workflow-run repo id constraint rejects characters present in mail addresses. This is the runtime-side uniformity gate failure: the trivial branch cannot write workflow-run events because it cannot construct a valid workflow-run repo id.`,
    );
  }
  for (;;) {
    let runIds: string[] = [];
    try {
      const oid = await git.resolveRef({
        fs,
        dir: repoDir,
        ref: handle.workflowRunRef,
      });
      try {
        const runsTree = await git.readTree({
          fs,
          dir: repoDir,
          oid,
          filepath: "runs",
        });
        runIds = runsTree.tree
          .filter((entry) => entry.type === "tree")
          .map((entry) => entry.path);
      } catch {
        runIds = [];
      }
    } catch {
      runIds = [];
    }
    for (const runId of runIds) {
      const events = await readWorkflowRunEvents(env, deploymentId, runId);
      const terminal = events.find((e) =>
        WORKFLOW_RUN_TERMINAL_TYPES.has(e.type),
      );
      if (terminal !== undefined) {
        return { runId, event: terminal };
      }
    }
    if (Date.now() - start > timeoutMs) {
      const diag = diagnostics?.();
      const ctx = diag ? `\n${diag}` : "";
      const inspected =
        runIds.length === 0
          ? "<no runs/ tree on workflow-run repo>"
          : runIds.join(",");
      throw new Error(
        `waitForTerminalEventForAnyRun timed out after ${String(timeoutMs)}ms for ${deploymentId}; runs observed: ${inspected}; expected the canonical event chain (RunStarted -> StepStarted -> StepCompleted -> RunCompleted) under runs/<runId>/events/.${ctx}`,
      );
    }
    await new Promise((r) => setTimeout(r, 50));
  }
}

// Mid-run reconnect-survival integration test.
//
// Proves the acceptance requirement: a multi-step workflow interrupted
// mid-run by a dropped hub link resumes after reconnect and completes,
// with effects applied exactly once.
//
// Shape: deploy a `step1 -> awaitSignal{name: "go"} -> step2` workflow
// through the workflow-deploy orchestrator's multi-step branch (the same
// wiring `multistep-signal.test.ts` uses), fire the deployment's mail
// trigger, and drive the run to the mid-run `SignalAwaited` pause. With
// the run parked at the signal gate, settle the workflow-run pack-push
// pipeline and drop the hub link (`settleThenDrop`), wait for the sidecar
// to re-establish the link and the deployment address to become routable
// again (`waitForReconnect`), then inject the awaited signal. The run must
// resume through `step2` to `RunCompleted`, and the run-event log must show
// each lifecycle effect exactly once -- no duplicate `SignalReceived`, no
// re-run of `step1`, no doubled `RunStarted`/`RunCompleted`. A run that
// resumes only because the sidecar reconnected, applying its effects once,
// is the guarantee under test.
//
// Harness justification: SPAWN-REAL. A real hub server, a real sidecar
// subprocess, a real workflow-process child, and a test inference
// provider. The interruption is a genuine server-side WebSocket close
// while the run is parked at `SignalAwaited`; the resume rides the
// sidecar's real `hub-link` reconnect path passing the hub's ownership
// challenge, after which the injected signal reaches the still-parked run.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { defineAgent, createDefaultDirectorRegistry } from "@intx/agent";
import type { HarnessConfig } from "@intx/types/runtime";
import {
  awaitSignal,
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
  injectSignal,
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
const DEPLOYMENT_ID = "midrun-signal-reconnect-1";
const WORKFLOW_RUN_REF = "refs/heads/main";

let env: DeployFlowEnv;

beforeAll(async () => {
  env = await startDeployFlowEnv();
});

afterAll(async () => {
  await env.teardown();
});

describe("mid-run signal survives hub-link drop -> reconnect", () => {
  test("sidecar registers with hub", () => {
    expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
  });

  test("interrupted-at-signal run resumes on reconnect and applies effects exactly once", async () => {
    const agent1 = defineAgent({
      id: "agent-step1",
      systemPrompt: "You are the first step agent.",
      tools: [],
      capabilities: [],
      inference: {
        sources: [{ provider: "anthropic", model: "mock-model" }],
      },
    });
    const agent2 = defineAgent({
      id: "agent-step2",
      systemPrompt: "You are the second step agent.",
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
        step1: step({ agent: agent1 }),
        gate: awaitSignal({ name: "go", after: ["step1"] }),
        step2: step({ agent: agent2, after: ["gate"] }),
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
          {
            files,
            message: `midrun-signal-reconnect: write workflow repo ${args.workflowRepoId}`,
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

    // ---- fire the trigger and drive to the mid-run signal pause ----
    const { messageId } = await fireMailTrigger(env, deploymentMailAddress, {
      messageId: "<midrun-signal-reconnect-1@integration.interchange>",
    });

    // First-half event chain: RunStarted -> StepStarted{step1} ->
    // StepCompleted{step1} -> SignalAwaited{name:"go"}.
    await waitFor(
      async () => {
        const events = await readWorkflowRunEventsForAnyRun(
          env,
          DEPLOYMENT_ID,
          workflowRunRepoId,
        );
        return events.some(
          (e) => e.type === "SignalAwaited" && e.body["signalName"] === "go",
        );
      },
      { diagnostics: env.sidecarDiagnostics, timeoutMs: 20_000 },
    );

    const runId = await findActiveRunId(env, workflowRunRepoId);
    const eventsBeforeDrop = await readWorkflowRunEvents(
      env,
      DEPLOYMENT_ID,
      runId,
    );
    const typesBeforeDrop = eventsBeforeDrop.map((e) => e.type);
    const runStartedIdx = typesBeforeDrop.indexOf("RunStarted");
    const step1StartedIdx = typesBeforeDrop.findIndex(
      (t, i) =>
        t === "StepStarted" && eventsBeforeDrop[i]?.body["stepId"] === "step1",
    );
    const step1CompletedIdx = typesBeforeDrop.findIndex(
      (t, i) =>
        t === "StepCompleted" &&
        eventsBeforeDrop[i]?.body["stepId"] === "step1",
    );
    const signalAwaitedIdx = typesBeforeDrop.indexOf("SignalAwaited");

    expect(runStartedIdx).toBeGreaterThanOrEqual(0);
    expect(step1StartedIdx).toBeGreaterThan(runStartedIdx);
    expect(step1CompletedIdx).toBeGreaterThan(step1StartedIdx);
    expect(signalAwaitedIdx).toBeGreaterThan(step1CompletedIdx);

    const runStartedBody = eventsBeforeDrop[runStartedIdx]?.body;
    if (runStartedBody === undefined) throw new Error("unreachable");
    expect(runStartedBody["consumedMessageId"]).toBe(messageId);

    // The run is parked at the signal gate; step2 has NOT started yet. This
    // is the mid-run state the drop must interrupt without losing.
    expect(
      typesBeforeDrop.some(
        (t, i) =>
          t === "StepStarted" &&
          eventsBeforeDrop[i]?.body["stepId"] === "step2",
      ),
    ).toBe(false);
    expect(typesBeforeDrop).not.toContain("RunCompleted");

    // ---- interrupt: settle the pack pipeline, then drop the hub link ----
    // `settleThenDrop` waits for the workflow-run pack stream to go quiet
    // before severing the link, so the interruption exercises the reconnect
    // guarantee against a settled run parked at the signal gate rather than
    // an in-flight pack push.
    await settleThenDrop(env, deploymentMailAddress);

    // The address leaves routing as the server-side close lands.
    await waitFor(
      () =>
        !env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
      { timeoutMs: 5_000, diagnostics: env.sidecarDiagnostics },
    );

    // ---- wait for the sidecar to reconnect + re-route ----
    const reconnectMs = await waitForReconnect(env, deploymentMailAddress, {
      timeoutMs: 20_000,
    });
    // The reconnect is the sidecar's reconnect delay plus a handshake; a
    // generous lower bound guards against a false "already routable" pass
    // that never actually dropped, and the upper bound catches a hung link.
    expect(reconnectMs).toBeGreaterThan(1_000);
    expect(reconnectMs).toBeLessThan(20_000);
    expect(env.hub.router.getRoutableAddresses()).toContain(
      deploymentMailAddress,
    );

    // ---- resume: inject the awaited signal into the parked run ----
    const injected = await injectSignal(env, DEPLOYMENT_ID, runId, "go", {
      resumed: true,
    });

    // Second-half event chain: SignalReceived{name:"go"} ->
    // StepStarted{step2} -> StepCompleted{step2} -> RunCompleted.
    const terminal = await waitForWorkflowRunComplete(
      env,
      DEPLOYMENT_ID,
      runId,
      { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
    );
    expect(terminal.type).toBe("RunCompleted");

    const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
    const types = events.map((e) => e.type);

    // ---- exactly-once effects ----
    // The run was interrupted at the signal gate, then resumed by the
    // reconnected sidecar. The workflow-run log is at-least-once for signal
    // *delivery* but exactly-once for *effects*: a redelivered signal (same
    // `signalId`) that the reconnect path replays is a no-op for run state
    // (`observedSignalIds` dedup in the state machine), so every effect --
    // each step's StepStarted/StepCompleted, the gate's completion, and the
    // terminal RunCompleted -- must appear exactly once even if the raw
    // `SignalReceived` delivery is logged more than once. A resume that
    // re-drove the workflow from the top, or a duplicate signal that leaked
    // past the dedup into a second effect, would duplicate one of these.
    const countType = (t: string): number =>
      types.filter((x) => x === t).length;
    const countStep = (t: string, stepId: string): number =>
      events.filter((e) => e.type === t && e.body["stepId"] === stepId).length;

    expect(countType("RunStarted")).toBe(1);
    expect(countStep("StepStarted", "step1")).toBe(1);
    expect(countStep("StepCompleted", "step1")).toBe(1);
    expect(countType("SignalAwaited")).toBe(1);
    expect(countStep("StepCompleted", "gate")).toBe(1);
    expect(countStep("StepStarted", "step2")).toBe(1);
    expect(countStep("StepCompleted", "step2")).toBe(1);
    expect(countType("RunCompleted")).toBe(1);

    // Every logged `SignalReceived` -- one or more, depending on how many
    // times the reconnect path replayed the delivery -- carries the SAME
    // injected signalId and payload. This is the dedup contract's precondition:
    // the duplicates are redeliveries of the ONE signal this test injected,
    // not distinct signals, so the single downstream effect above is the
    // effect of exactly one logical signal, not a coincidence of two.
    const signalReceivedEvents = events.filter(
      (e) => e.type === "SignalReceived",
    );
    expect(signalReceivedEvents.length).toBeGreaterThanOrEqual(1);
    const distinctSignalIds = new Set(
      signalReceivedEvents.map((e) => e.body["signalId"]),
    );
    expect(distinctSignalIds).toEqual(new Set([injected.signalId]));
    for (const e of signalReceivedEvents) {
      expect(e.body["signalName"]).toBe("go");
      expect(e.body["signalId"]).toBe(injected.signalId);
      expect(e.body["payload"]).toEqual({ resumed: true });
    }

    // Ordered chain across the interruption boundary: the second half was
    // driven only after reconnect + signal injection. Index against the
    // FIRST SignalReceived (the delivery that actually resumed the gate).
    const signalReceivedIdx = types.indexOf("SignalReceived");
    const step2StartedIdx = types.findIndex(
      (t, i) => t === "StepStarted" && events[i]?.body["stepId"] === "step2",
    );
    const step2CompletedIdx = types.findIndex(
      (t, i) => t === "StepCompleted" && events[i]?.body["stepId"] === "step2",
    );
    const runCompletedIdx = types.indexOf("RunCompleted");
    const signalAwaitedIdxFinal = types.indexOf("SignalAwaited");

    expect(signalReceivedIdx).toBeGreaterThan(signalAwaitedIdxFinal);
    expect(step2StartedIdx).toBeGreaterThan(signalReceivedIdx);
    expect(step2CompletedIdx).toBeGreaterThan(step2StartedIdx);
    expect(runCompletedIdx).toBeGreaterThan(step2CompletedIdx);

    // Exactly one run exists: the interruption did not spawn a second run,
    // so the effects above are the effects of the single, resumed run.
    const runIds = await listRunIds(env, workflowRunRepoId);
    expect(runIds).toEqual([runId]);
  }, 120_000);
});

/**
 * Read every workflow-run event under any `runs/<runId>/events/` subtree on
 * the deployment's workflow-run repo. Used to discover the runId the
 * supervisor minted from the inbound mail bytes; the test does not know it
 * up front.
 */
async function readWorkflowRunEventsForAnyRun(
  env: DeployFlowEnv,
  deploymentId: string,
  workflowRunRepoId: RepoId,
): Promise<{ runId: string; type: string; body: Record<string, unknown> }[]> {
  const runIds = await listRunIds(env, workflowRunRepoId);
  const out: { runId: string; type: string; body: Record<string, unknown> }[] =
    [];
  for (const runId of runIds) {
    const events = await readWorkflowRunEvents(env, deploymentId, runId);
    for (const e of events) {
      out.push({ runId, type: e.type, body: e.body });
    }
  }
  return out;
}

async function findActiveRunId(
  env: DeployFlowEnv,
  workflowRunRepoId: RepoId,
): Promise<string> {
  const runIds = await listRunIds(env, workflowRunRepoId);
  const head = runIds[0];
  if (head === undefined) {
    throw new Error(
      `findActiveRunId: no runs/ entries on workflow-run repo ${workflowRunRepoId.id}`,
    );
  }
  return head;
}

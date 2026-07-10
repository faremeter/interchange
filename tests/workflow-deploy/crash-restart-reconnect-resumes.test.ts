// Crash-at-awaitSignal survival integration test.
//
// Proves the acceptance requirement: a run parked at an `awaitSignal`
// gate survives a sidecar PROCESS crash. After crash + restart, the fresh
// process's boot-time restore re-spawns the deployment and re-seeds the
// parked run; delivering the awaited signal resumes it through to
// RunCompleted with effects applied exactly once.
//
// Shape: deploy a `step1 -> awaitSignal{name:"go"} -> step2` workflow
// through the workflow-deploy orchestrator's multi-step branch (the same
// wiring `multistep-signal.test.ts` uses), fire the mail trigger, and
// drive the run to the mid-run `SignalAwaited` pause. Quiesce the
// workflow-run pack pipeline, then KILL the sidecar subprocess (process
// death, not a hub-link drop). Start a fresh sidecar against the crashed
// process's SIDECAR_DATA_DIR: its `restoreWorkflowDeployments()` re-spawns
// the deployment, whose workflow-process child re-seeds the parked run via
// `resumeFromEvents`. The runtime re-arms the awaiting-signal gate against
// the host-rehydrated signal channel (its `readState` reads the run's live
// reduced state). After reconnect, inject the signal; the run resumes
// through `step2` to `RunCompleted`.
//
// Effects are asserted exactly-once at the effect layer -- RunStarted,
// StepCompleted{step1}, StepCompleted{step2}, and RunCompleted each == 1,
// one runId -- NOT on the raw `SignalReceived` count: the reconnect/resume
// path may replay the delivery, and the state machine dedupes redeliveries
// by `signalId`, so a delivery-count assertion would be flaky by design.
//
// Harness justification: SPAWN-REAL. Real hub, real sidecar subprocess,
// real workflow-process child, mock inference. The crash is a genuine kill
// of the sidecar subprocess; the restart is a fresh sidecar against the
// dead process's SIDECAR_DATA_DIR, so survival rides the production
// boot-time restore path and the real reconnect ownership challenge.

import fs from "node:fs";

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
  startDeployFlowEnv,
  startSidecarSubprocess,
  waitFor,
  waitForReconnect,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
  type SidecarHandle,
} from "../hub-agent/lib/deploy-flow-env";
import { toLaunchDeployContent } from "./launch-session-bridge";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "crash-restart-resume-1";
const WORKFLOW_RUN_REF = "refs/heads/main";

let env: DeployFlowEnv;
let restartedSidecar: SidecarHandle | undefined;
const restartTempDirs: string[] = [];

beforeAll(async () => {
  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (restartedSidecar !== undefined) {
    restartedSidecar.proc.kill();
    await restartedSidecar.proc.exited;
  }
  await env.teardown();
  for (const dir of restartTempDirs.splice(0)) {
    await fs.promises.rm(dir, { recursive: true, force: true });
  }
});

/**
 * Settle the workflow-run pack-push pipeline (no drop). The crash below is
 * the drop (a process death), so only the quiescence guarantee is needed.
 */
async function settleWorkflowRunPacks(
  target: DeployFlowEnv,
  opts: { quietMs?: number; timeoutMs?: number } = {},
): Promise<void> {
  const quietMs = opts.quietMs ?? 500;
  const timeoutMs = opts.timeoutMs ?? 10_000;
  const start = Date.now();
  let lastCount = target.hub.workflowRunPackReceipts.count;
  let lastChange = Date.now();
  for (;;) {
    const current = target.hub.workflowRunPackReceipts.count;
    if (current !== lastCount) {
      lastCount = current;
      lastChange = Date.now();
    }
    if (Date.now() - lastChange >= quietMs) return;
    if (Date.now() - start > timeoutMs) {
      throw new Error(
        `settleWorkflowRunPacks: pack stream did not go quiet for ${String(quietMs)}ms within ${String(timeoutMs)}ms` +
          `\n${target.sidecarDiagnostics()}`,
      );
    }
    await new Promise((r) => setTimeout(r, 50));
  }
}

async function readAllRunEvents(
  target: DeployFlowEnv,
  workflowRunRepoId: RepoId,
): Promise<{ runId: string; type: string; body: Record<string, unknown> }[]> {
  const runIds = await listRunIds(target, workflowRunRepoId);
  const out: { runId: string; type: string; body: Record<string, unknown> }[] =
    [];
  for (const runId of runIds) {
    const events = await readWorkflowRunEvents(target, DEPLOYMENT_ID, runId);
    for (const e of events) out.push({ runId, type: e.type, body: e.body });
  }
  return out;
}

describe("sidecar crash + restart -> restore + reconnect resumes a run parked at awaitSignal", () => {
  test("sidecar registers with hub", () => {
    expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
  });

  test("mid-run crash at awaitSignal, restore re-spawns, reconnect resumes to RunCompleted exactly once", async () => {
    const agent1 = defineAgent({
      id: "agent-step1",
      systemPrompt: "You are the first step agent.",
      tools: [],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
    });
    const agent2 = defineAgent({
      id: "agent-step2",
      systemPrompt: "You are the second step agent.",
      tools: [],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
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
      principalId: "prin_crash-restart-1",
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
        for (const [k, v] of args.files) files[k] = v;
        await env.hub.agentRepoStore.repoStore.writeTree(
          principal,
          repoId,
          DEFAULT_ASSET_REF,
          { files, message: "crash-restart resume: write workflow repo" },
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

    // ---- fire the trigger, drive to the mid-run SignalAwaited pause ----
    const { messageId } = await fireMailTrigger(env, deploymentMailAddress, {
      messageId: "<crash-restart-resume-1@integration.interchange>",
    });

    await waitFor(
      async () => {
        const events = await readAllRunEvents(env, workflowRunRepoId);
        return events.some(
          (e) => e.type === "SignalAwaited" && e.body["signalName"] === "go",
        );
      },
      { diagnostics: env.sidecarDiagnostics, timeoutMs: 20_000 },
    );

    const runIdsAtPause = await listRunIds(env, workflowRunRepoId);
    const runId = runIdsAtPause[0];
    if (runId === undefined) {
      throw new Error(
        "no runs/ entry on the workflow-run repo at the SignalAwaited pause",
      );
    }

    const parked = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
    const parkedTypes = parked.map((e) => e.type);
    expect(parkedTypes).toContain("RunStarted");
    expect(
      parked.some(
        (e) => e.type === "StepCompleted" && e.body["stepId"] === "step1",
      ),
    ).toBe(true);
    expect(parkedTypes).toContain("SignalAwaited");
    expect(
      parked.some(
        (e) => e.type === "StepStarted" && e.body["stepId"] === "step2",
      ),
    ).toBe(false);
    expect(parkedTypes).not.toContain("RunCompleted");

    const runStartedBody = parked.find((e) => e.type === "RunStarted")?.body;
    if (runStartedBody === undefined) throw new Error("unreachable");
    expect(runStartedBody["consumedMessageId"]).toBe(messageId);

    // ---- quiesce, then CRASH the sidecar subprocess (process death) ----
    await settleWorkflowRunPacks(env);

    const crashedDataDir = env.sidecar.dataDir;
    env.sidecar.proc.kill();
    await env.sidecar.proc.exited;

    await waitFor(
      () =>
        !env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
      { timeoutMs: 10_000, diagnostics: env.sidecarDiagnostics },
    );

    // ---- RESTART: a fresh sidecar against the SAME data dir ----
    const hubPort = env.hub.server.port;
    if (hubPort === undefined) {
      throw new Error("hub.server.port is undefined after crash");
    }
    restartedSidecar = await startSidecarSubprocess({
      hubPort,
      registerTempDir: (dir) => {
        restartTempDirs.push(dir);
      },
      extraEnv: { SIDECAR_DATA_DIR: crashedDataDir },
    });
    const restoredDiagnostics = (): string =>
      `${env.sidecarDiagnostics()}\nrestored sidecar stderr:\n${restartedSidecar?.stderr.slice(-60).join("") ?? "<none>"}`;

    // ---- wait for the restored deployment to re-establish the hub link ----
    const reconnectMs = await waitForReconnect(env, deploymentMailAddress, {
      timeoutMs: 30_000,
    });
    expect(reconnectMs).toBeGreaterThan(0);
    expect(env.hub.router.getRoutableAddresses()).toContain(
      deploymentMailAddress,
    );

    // ---- deliver the signal, assert resume to RunCompleted ----
    // The restored child re-armed the parked awaiting-signal gate against
    // the run's live reduced state; the injected signal now resolves the
    // re-armed awaiter and drives the run to completion.
    const injected = await injectSignal(env, DEPLOYMENT_ID, runId, "go", {
      resumed: true,
    });

    const terminal = await waitForWorkflowRunComplete(
      env,
      DEPLOYMENT_ID,
      runId,
      { timeoutMs: 30_000, diagnostics: restoredDiagnostics },
    );
    expect(terminal.type).toBe("RunCompleted");

    // ---- exactly-once EFFECTS across the full (replay-inclusive) log ----
    const finalEvents = await readAllRunEvents(env, workflowRunRepoId);

    const distinctRunIds = new Set(finalEvents.map((e) => e.runId));
    expect(distinctRunIds).toEqual(new Set([runId]));

    const countType = (t: string): number =>
      finalEvents.filter((e) => e.type === t).length;
    const countStepCompleted = (stepId: string): number =>
      finalEvents.filter(
        (e) => e.type === "StepCompleted" && e.body["stepId"] === stepId,
      ).length;

    expect(countType("RunStarted")).toBe(1);
    expect(countStepCompleted("step1")).toBe(1);
    expect(countStepCompleted("step2")).toBe(1);
    expect(countType("RunCompleted")).toBe(1);

    const signalReceived = finalEvents.find(
      (e) => e.type === "SignalReceived" && e.body["signalName"] === "go",
    );
    if (signalReceived === undefined) {
      throw new Error("no SignalReceived{go} effect after reconnect + resume");
    }
    expect(signalReceived.body["signalId"]).toBe(injected.signalId);
    expect(signalReceived.body["payload"]).toEqual({ resumed: true });

    // Effect-layer exactly-once: each step's agent is invoked exactly once
    // across the crash + restart + resume. The mock inference server
    // records every request; each step agent carries a distinct system
    // prompt, so a recovery that re-ran a durably-completed step would show
    // a second invocation here even though the event log dedups the
    // duplicate. Match on the whole request JSON (the system prompt rides
    // in its own field the harness type does not surface).
    const invokedWithPrompt = (needle: string): number =>
      env.inference.requests.filter((r) => JSON.stringify(r).includes(needle))
        .length;
    expect(invokedWithPrompt("first step agent")).toBe(1);
    expect(invokedWithPrompt("second step agent")).toBe(1);
  }, 180_000);
});

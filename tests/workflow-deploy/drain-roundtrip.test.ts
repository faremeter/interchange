// Drain round-trip integration test.
//
// Deploys a `step1 -> awaitSignal{name: "never-arrives",
// drainBehavior: "cancel"}` workflow through the workflow-deploy
// orchestrator's multi-step branch, fires the deployment's mail
// trigger, observes the runtime pause at `SignalAwaited`, then
// initiates drain through the production hub -> sidecar -> supervisor
// -> workflow-process child pipeline.
//
// The H1 in-process drain tests
// (`packages/workflow-host/src/...` and
// `packages/workflow/src/runtime/drain.test.ts`) pin the canonical
// observable sequence: an `awaitSignal` step parked in cancel mode
// aborts the local step controller the moment the drain signal flips
// on the child side, the primitive's runner commits `StepFailed`, and
// the run reaches terminal `RunFailed`. The drainTimeout accumulator
// on the supervisor side stays armed but never escalates because the
// step's own abort tears the run down before the deadline lapses --
// the accumulator's role is to escalate when cancel-mode work
// *outlasts* the wire deadline, not when it cooperates immediately.
//
// This test pins the wire-level uniformity gate for that sequence:
// the hub router's `sendDrain` ships a `drain.deliver` frame to the
// sidecar, the sidecar's hub-link routes the frame through the
// multi-step drain registry into the supervisor's `drain`, which
// forwards a `drain` control IPC payload to the workflow-process
// child. The child's `DrainController` flips its signal; the runtime
// body's observation points pick it up on the next tick and abort
// the cancel-mode `awaitSignal` step; the local step's abort surfaces
// as `StepFailed`; the run terminates as `RunFailed`. The cascade is
// asserted end-to-end so a regression in any of the seven hops surfaces
// at this test.
//
// The orchestrator's multi-step branch is composed in-test (matching
// the multi-step signal round-trip) because the pre-landed
// `deploy-flow-env` fixture wires only the trivial `launchSession`
// callback against `env.hub.sessionService.launchSession`; the
// multi-step `sendMultiStepDeploy` hand-off is supplied here against
// `env.hub.router.sendAgentDeploy` so the sidecar's deploy router
// takes the workflow-process spawn path.

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
import { deriveTrivialDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";
import type { RepoId, WorkflowRunHubPrincipal } from "@intx/hub-sessions";
import { DEFAULT_ASSET_REF } from "@intx/hub-sessions";

import {
  SESSION_ID,
  SIDECAR_ID,
  fireMailTrigger,
  initiateDrain,
  listRunIds,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "drain-roundtrip-1";
const WORKFLOW_RUN_REF = "refs/heads/main";

// Wire `deadlineMs` carried on the drain.deliver frame. The child
// echoes this in its drain log; the supervisor-side accumulator runs
// against the per-deployment `drainTimeoutMs` policy on its bindings
// (default 5_000 ms), independent of this value. The accumulator does
// not escalate in this test's flow because the cancel-mode awaitSignal
// step aborts the moment the drain signal flips.
const DRAIN_DEADLINE_MS = 1_000;

let env: DeployFlowEnv;

beforeAll(async () => {
  env = await startDeployFlowEnv();
});

afterAll(async () => {
  await env.teardown();
});

describe("drain round-trip", () => {
  test("sidecar registers with hub", () => {
    expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
  });

  test("drain on a cancel-mode awaitSignal aborts the step and surfaces RunFailed", async () => {
    const agent1 = defineAgent({
      id: "agent-step1",
      systemPrompt: "You are the first step agent.",
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

    // The gate is `drainBehavior: "cancel"` so a mid-flight drain
    // flips the runtime body's drain signal, the awaitSignal step's
    // local controller aborts, and the primitive runner commits
    // StepFailed. The supervisor's per-run drainTimeout accumulator
    // arms in parallel but does not escalate -- the step aborts
    // first, the run reaches RunFailed, and the accumulator stops
    // cleanly on the supervisor's shutdown path.
    const workflow: WorkflowDefinition = defineWorkflow({
      id: `wf_${DEPLOYMENT_ID}`,
      trigger: { type: "mail", to: deploymentMailAddress },
      steps: {
        step1: step({ agent: agent1 }),
        gate: awaitSignal({
          name: "never-arrives",
          after: ["step1"],
          drainBehavior: "cancel",
        }),
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
      const deployContent = orchestratorParams.deployContent;
      await env.hub.sessionService.launchSession({
        agentAddress: orchestratorParams.agentAddress,
        agentId: orchestratorParams.agentId,
        instanceId: orchestratorParams.instanceId,
        config: orchestratorParams.config,
        deployContent: deployContent as Parameters<
          typeof env.hub.sessionService.launchSession
        >[0]["deployContent"],
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
          // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the wire validator carries WorkflowDefinition steps as Record<string, unknown>; the orchestrator emits the typed primitive union shape that satisfies the wire schema
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
            message: `drain-roundtrip test: write workflow repo ${args.workflowRepoId}`,
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
    expect(result.kind).toBe("multi-step");

    const workflowRunRepoId: RepoId = {
      kind: "workflow-run",
      id: deriveTrivialDeploymentId(deploymentMailAddress),
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
      messageId: "<drain-roundtrip-1@integration.interchange>",
    });

    // Wait until the runtime parks at the cancel-mode awaitSignal.
    // The accumulator only arms against the runId once the
    // supervisor's `forwardMailAndTrack` has tracked it, which the
    // SignalAwaited event implies (step1 already completed and the
    // gate is now blocked on a signal that never arrives).
    await waitFor(
      async () => {
        const events = await readWorkflowRunEventsForAnyRun(
          env,
          DEPLOYMENT_ID,
          workflowRunRepoId,
        );
        return events.some(
          (e) =>
            e.type === "SignalAwaited" &&
            e.body["signalName"] === "never-arrives",
        );
      },
      { diagnostics: env.sidecarDiagnostics, timeoutMs: 20_000 },
    );

    const runId = await findActiveRunId(env, workflowRunRepoId);

    // Ship the drain payload through the production wire pipeline:
    // hub `sendDrain` -> sidecar hub-link -> supervisor `drain` ->
    // workflow-process child `DrainController` plus host-side
    // drainTimeout accumulator.
    initiateDrain(env, DEPLOYMENT_ID, { deadlineMs: DRAIN_DEADLINE_MS });

    // The drain flips the child's `DrainController` signal on its
    // next tick. The runtime body's observation point on the awaiting
    // step's local controller aborts the cancel-mode awaitSignal;
    // the primitive runner catches the abort and commits StepFailed;
    // the main loop's `hasFailedStep` exit emits the terminal
    // RunFailed.
    const terminal = await waitForWorkflowRunComplete(
      env,
      DEPLOYMENT_ID,
      runId,
      { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
    );

    const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
    const types = events.map((e) => e.type);
    // Diagnostics aid: surface the observed sequence on a failed
    // assertion so an off-by-one ordering bug is debuggable from the
    // verification log alone.
    const observedSequence = `observed: ${types.join(" -> ")}`;

    expect(`${String(terminal.type)} (${observedSequence})`).toBe(
      `RunFailed (${observedSequence})`,
    );

    // The canonical observable sequence end-to-end.
    const runStartedIdx = types.indexOf("RunStarted");
    const step1StartedIdx = types.findIndex(
      (t, i) => t === "StepStarted" && events[i]?.body["stepId"] === "step1",
    );
    const step1CompletedIdx = types.findIndex(
      (t, i) => t === "StepCompleted" && events[i]?.body["stepId"] === "step1",
    );
    const gateStartedIdx = types.findIndex(
      (t, i) => t === "StepStarted" && events[i]?.body["stepId"] === "gate",
    );
    const signalAwaitedIdx = types.indexOf("SignalAwaited");
    const gateFailedIdx = types.findIndex(
      (t, i) => t === "StepFailed" && events[i]?.body["stepId"] === "gate",
    );
    const runFailedIdx = types.indexOf("RunFailed");

    expect(
      `runStarted@${String(runStartedIdx)} (${observedSequence})`,
    ).not.toBe(`runStarted@-1 (${observedSequence})`);
    expect(step1StartedIdx).toBeGreaterThan(runStartedIdx);
    expect(step1CompletedIdx).toBeGreaterThan(step1StartedIdx);
    expect(gateStartedIdx).toBeGreaterThan(step1CompletedIdx);
    expect(signalAwaitedIdx).toBeGreaterThan(gateStartedIdx);
    expect(gateFailedIdx).toBeGreaterThan(signalAwaitedIdx);
    expect(runFailedIdx).toBeGreaterThan(gateFailedIdx);

    const signalAwaitedBody = events[signalAwaitedIdx]?.body;
    if (signalAwaitedBody === undefined) throw new Error("unreachable");
    expect(signalAwaitedBody["signalName"]).toBe("never-arrives");
  }, 30_000);

  test("drain on a wait-mode awaitSignal keeps the step running until drainTimeout escalates", async () => {
    // Companion to the cancel-mode test. The supervisor's
    // drainTimeout accumulator only matters when the step is wait-mode:
    // wait-mode steps don't abort on the drain signal flip; instead the
    // per-run accumulator escalates after the policy's drainTimeoutMs
    // and commits a signed `CancelRequested{origin: supervisor-drain}`
    // event. The cancel-mode test never reaches the escalation path
    // because step1 aborts the moment the drain signal flips. A
    // regression that escalated wait-mode the moment drain fired (or
    // never escalated at all) would slip through the cancel-mode
    // assertion suite.
    const waitDeploymentId = "drain-roundtrip-wait-1";
    const agent1 = defineAgent({
      id: "agent-wait-step1",
      systemPrompt: "You are the first step agent in the wait companion.",
      tools: [],
      capabilities: [],
      inference: {
        sources: [{ provider: "anthropic", model: "mock-model" }],
      },
    });
    const deploymentMailAddress = deriveDeploymentAddress({
      deploymentId: waitDeploymentId,
      deploymentDomain: DEPLOYMENT_DOMAIN,
    });
    const workflow: WorkflowDefinition = defineWorkflow({
      id: `wf_${waitDeploymentId}`,
      trigger: { type: "mail", to: deploymentMailAddress },
      steps: {
        step1: step({ agent: agent1 }),
        gate: awaitSignal({
          name: "never-arrives",
          after: ["step1"],
          drainBehavior: "wait",
        }),
      },
    });
    const config: HarnessConfig = {
      sessionId: SESSION_ID,
      agentId: `ins_${waitDeploymentId}`,
      tenantId: "tenant-1",
      principalId: "prin_integration-1",
      agentAddress: deploymentMailAddress,
      systemPrompt: "Fallback prompt (overridden per step by orchestrator)",
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
    const operatorApprovals: ApprovalSet = new Set<string>([
      "inference.source:anthropic:mock-model",
      "director:@intx/agent/default",
      `mail.address:${deploymentMailAddress}`,
      `mail.send:${DEPLOYMENT_DOMAIN}`,
    ]);
    const launchSession: LaunchSessionFn = async (orchestratorParams) => {
      const deployContent = orchestratorParams.deployContent;
      await env.hub.sessionService.launchSession({
        agentAddress: orchestratorParams.agentAddress,
        agentId: orchestratorParams.agentId,
        instanceId: orchestratorParams.instanceId,
        config: orchestratorParams.config,
        deployContent: deployContent as Parameters<
          typeof env.hub.sessionService.launchSession
        >[0]["deployContent"],
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
          // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the wire validator carries WorkflowDefinition steps as Record<string, unknown>; the orchestrator emits the typed primitive union shape that satisfies the wire schema
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
            message: `drain-roundtrip wait test: write workflow repo ${args.workflowRepoId}`,
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
    const result = await orchestrator.deployWorkflow({
      workflow,
      config,
      deployContent: { systemPrompt: config.systemPrompt },
      operatorApprovals,
      deploymentId: waitDeploymentId,
      deploymentDomain: DEPLOYMENT_DOMAIN,
      hubPublicKey: "00".repeat(32),
    });
    expect(result.kind).toBe("multi-step");

    const workflowRunRepoId: RepoId = {
      kind: "workflow-run",
      id: deriveTrivialDeploymentId(deploymentMailAddress),
    };
    env.registerDeployment({
      deploymentId: waitDeploymentId,
      workflowDefinition: workflow,
      workflowRunRepoId,
      workflowRunRef: WORKFLOW_RUN_REF,
      mailAddress: deploymentMailAddress,
    });

    await fireMailTrigger(env, deploymentMailAddress, {
      messageId: "<drain-roundtrip-wait-1@integration.interchange>",
    });
    await waitFor(
      async () => {
        const events = await readWorkflowRunEventsForAnyRun(
          env,
          waitDeploymentId,
          workflowRunRepoId,
        );
        return events.some(
          (e) =>
            e.type === "SignalAwaited" &&
            e.body["signalName"] === "never-arrives",
        );
      },
      { diagnostics: env.sidecarDiagnostics, timeoutMs: 20_000 },
    );
    const runId = await findActiveRunId(env, workflowRunRepoId);

    // Snapshot the event log shape BEFORE issuing the drain so the
    // immediate-after assertion below distinguishes wait-mode (no
    // change for a window) from cancel-mode (StepFailed lands within
    // milliseconds of the drain signal flip).
    const eventsBeforeDrain = await readWorkflowRunEvents(
      env,
      waitDeploymentId,
      runId,
    );
    const typesBefore = eventsBeforeDrain.map((e) => e.type);
    expect(typesBefore).toContain("SignalAwaited");
    expect(typesBefore).not.toContain("StepFailed");

    initiateDrain(env, waitDeploymentId, { deadlineMs: DRAIN_DEADLINE_MS });

    // Wait-mode contract: the gate must keep waiting for the
    // drainTimeoutMs window the policy allows. The cancel-mode test
    // observes StepFailed within ~hundreds of milliseconds because
    // the awaitSignal aborts on the local controller; wait-mode
    // ignores the controller flip and stays in SignalAwaited until
    // either the signal arrives or the supervisor's drainTimeout
    // accumulator escalates with a signed CancelRequested. With
    // never-arrives the signal path can't fire. The supervisor's
    // drainTimeoutMs policy default is DEFAULT_DRAIN_TIMEOUT_MS
    // (60_000ms); sleeping 2_500ms is well inside the wait window.
    // A regression that aborted wait-mode on the drain signal flip
    // (within hundreds of milliseconds, the cancel-mode shape)
    // would commit StepFailed in this window and fail the
    // assertion below. A subtler regression that aborted
    // wait-mode anywhere between ~hundreds of ms and 2.5s would
    // also fail; a regression that aborted somewhere between 2.5s
    // and 60s would slip past this test -- the load-bearing
    // assertion is the cancel-mode shape, not partial-window
    // misbehaviour.
    await new Promise((r) => setTimeout(r, 2_500));
    const eventsDuringWait = await readWorkflowRunEvents(
      env,
      waitDeploymentId,
      runId,
    );
    const typesDuring = eventsDuringWait.map((e) => e.type);
    expect(typesDuring).not.toContain("StepFailed");
    expect(typesDuring).not.toContain("RunFailed");
    expect(typesDuring).not.toContain("RunCompleted");
    expect(typesDuring).not.toContain("RunCancelled");
  }, 30_000);
});

/**
 * Read every workflow-run event under any `runs/<runId>/events/`
 * subtree on the deployment's workflow-run repo. Used to discover the
 * runId the supervisor minted from the inbound mail bytes.
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

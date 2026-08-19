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
// the multi-step signal round-trip): the per-step launch callback drives
// `env.hub.sessionService.stageWorkflowStep` (the stage-only path, no warm
// harness) and the `sendMultiStepDeploy` hand-off is supplied against
// `env.hub.router.sendAgentDeploy` so the sidecar's deploy router
// takes the workflow-process spawn path.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import { deriveRunAddress, type ApprovalSet } from "@intx/workflow-deploy";
import { tenant as tenantTable } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedPrincipal } from "@intx/test-harness/seed";
import type { RepoId } from "@intx/hub-sessions";

import {
  SESSION_ID,
  SIDECAR_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  initiateDrain,
  listRunIds,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { signalGateEntry } from "./fixtures/signal-gate";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_drain-roundtrip-1";
const WAIT_DEPLOYMENT_ID = "run_drain-roundtrip-wait-1";

// Wire `deadlineMs` carried on the drain.deliver frame. The child
// echoes this in its drain log; the supervisor-side accumulator runs
// against the per-deployment `drainTimeoutMs` policy on its bindings
// (default 5_000 ms), independent of this value. The accumulator does
// not escalate in this test's flow because the cancel-mode awaitSignal
// step aborts the moment the drain signal flips.
const DRAIN_DEADLINE_MS = 1_000;

// The definition's own tenant, the caller principal that creates the
// definition assets, and the `workflow`-kind assets the frozen definitions
// project over (one per deployed anchor run id). The install/approve freeze
// and the anchor `workflow_run` insert both write against these, so they must
// exist in the real DB before each deploy runs.
const TENANT_ID = "tnt_drain_roundtrip";
const CALLER_PRINCIPAL_ID = "prn_drain_roundtrip";
const DEFINITION_ASSET_IDS: Record<string, string> = {
  [DEPLOYMENT_ID]: "ast_drain_roundtrip_cancel_wf",
  [WAIT_DEPLOYMENT_ID]: "ast_drain_roundtrip_wait_wf",
};

let env: DeployFlowEnv;
let h: TestDb;

beforeAll(async () => {
  h = await createTestDb();
  await h.db.insert(tenantTable).values({
    id: TENANT_ID,
    name: TENANT_ID,
    slug: TENANT_ID,
    domain: DEPLOYMENT_DOMAIN,
    parentId: null,
  });
  await seedPrincipal(h.db, {
    id: CALLER_PRINCIPAL_ID,
    tenantId: TENANT_ID,
    kind: "user",
  });
  for (const [anchorRunId, definitionAssetId] of Object.entries(
    DEFINITION_ASSET_IDS,
  )) {
    await seedAsset(h.db, {
      id: definitionAssetId,
      tenantId: TENANT_ID,
      kind: "workflow",
      name: `drain-roundtrip-wf-${anchorRunId}`,
      creatorPrincipalId: CALLER_PRINCIPAL_ID,
    });
  }

  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

describe.skipIf(!harnessDbEnvAvailable())("drain round-trip", () => {
  test("sidecar registers with hub", () => {
    expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
  });

  test("drain on a cancel-mode awaitSignal aborts the step and surfaces RunFailed", async () => {
    const deploymentMailAddress = deriveRunAddress({
      runId: DEPLOYMENT_ID,
      domain: DEPLOYMENT_DOMAIN,
    });

    const inferenceSource: InferenceSource = {
      id: "anthropic:mock-model",
      provider: "anthropic",
      baseURL: `http://localhost:${env.inference.server.port}`,
      apiKey: "sk-mock",
      model: "mock-model",
    };

    const config: HarnessConfig = {
      sessionId: SESSION_ID,
      agentId: `${DEPLOYMENT_ID}`,
      tenantId: "tenant-1",
      principalId: "prin_integration-1",
      agentAddress: deploymentMailAddress,
      systemPrompt: "Fallback prompt (overridden per step by the definition)",
      tools: [],
      grants: [],
      sources: [inferenceSource],
      defaultSource: "anthropic:mock-model",
    };

    const operatorApprovals: ApprovalSet = new Set<string>([
      "inference.source:anthropic:mock-model",
      "director:@intx/agent/default",
      `mail.address:${deploymentMailAddress}`,
      `mail.send:${DEPLOYMENT_DOMAIN}`,
    ]);

    // The gate is `drainBehavior: "cancel"` so a mid-flight drain
    // flips the runtime body's drain signal, the awaitSignal step's
    // local controller aborts, and the primitive runner commits
    // StepFailed. The supervisor's per-run drainTimeout accumulator
    // arms in parallel but does not escalate -- the step aborts
    // first, the run reaches RunFailed, and the accumulator stops
    // cleanly on the supervisor's shutdown path.
    const entryModule = signalGateEntry({
      address: deploymentMailAddress,
      signalName: "never-arrives",
      drainBehavior: "cancel",
      systemPrompt1: "You are the first step agent.",
      agentId1: "agent-step1",
      workflowId: `wf_${DEPLOYMENT_ID}`,
    });

    const definitionAssetId = DEFINITION_ASSET_IDS[DEPLOYMENT_ID];
    if (definitionAssetId === undefined) {
      throw new Error(
        `drain-roundtrip: no definition asset seeded for ${DEPLOYMENT_ID}`,
      );
    }

    const handle = await deployWorkflowSourceForTest(env, {
      entryModule,
      db: h.db,
      tenantId: TENANT_ID,
      definitionAssetId,
      anchorRunId: DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
      agentAddress: deploymentMailAddress,
      approvals: operatorApprovals,
      config,
      sources: { step1: [inferenceSource], gate: [inferenceSource] },
    });
    expect(handle.publicKey).toBeTruthy();

    const workflowRunRepoId = handle.workflowRunRepoId;

    // The source-ref frame round-trips through the real sidecar subprocess, so
    // routability is asynchronous. Wait for it before firing the trigger.
    await waitFor(
      () =>
        env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
      { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
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
    const waitDeploymentId = WAIT_DEPLOYMENT_ID;
    const deploymentMailAddress = deriveRunAddress({
      runId: waitDeploymentId,
      domain: DEPLOYMENT_DOMAIN,
    });
    const inferenceSource: InferenceSource = {
      id: "anthropic:mock-model",
      provider: "anthropic",
      baseURL: `http://localhost:${env.inference.server.port}`,
      apiKey: "sk-mock",
      model: "mock-model",
    };
    const config: HarnessConfig = {
      sessionId: SESSION_ID,
      agentId: `${waitDeploymentId}`,
      tenantId: "tenant-1",
      principalId: "prin_integration-1",
      agentAddress: deploymentMailAddress,
      systemPrompt: "Fallback prompt (overridden per step by the definition)",
      tools: [],
      grants: [],
      sources: [inferenceSource],
      defaultSource: "anthropic:mock-model",
    };
    const operatorApprovals: ApprovalSet = new Set<string>([
      "inference.source:anthropic:mock-model",
      "director:@intx/agent/default",
      `mail.address:${deploymentMailAddress}`,
      `mail.send:${DEPLOYMENT_DOMAIN}`,
    ]);
    const entryModule = signalGateEntry({
      address: deploymentMailAddress,
      signalName: "never-arrives",
      drainBehavior: "wait",
      systemPrompt1: "You are the first step agent in the wait companion.",
      agentId1: "agent-wait-step1",
      workflowId: `wf_${waitDeploymentId}`,
    });
    const definitionAssetId = DEFINITION_ASSET_IDS[waitDeploymentId];
    if (definitionAssetId === undefined) {
      throw new Error(
        `drain-roundtrip: no definition asset seeded for ${waitDeploymentId}`,
      );
    }
    const handle = await deployWorkflowSourceForTest(env, {
      entryModule,
      db: h.db,
      tenantId: TENANT_ID,
      definitionAssetId,
      anchorRunId: waitDeploymentId,
      deploymentDomain: DEPLOYMENT_DOMAIN,
      agentAddress: deploymentMailAddress,
      approvals: operatorApprovals,
      config,
      sources: { step1: [inferenceSource], gate: [inferenceSource] },
    });
    expect(handle.publicKey).toBeTruthy();

    const workflowRunRepoId = handle.workflowRunRepoId;

    await waitFor(
      () =>
        env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
      { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
    );

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
  anchorRunId: string,
  workflowRunRepoId: RepoId,
): Promise<{ runId: string; type: string; body: Record<string, unknown> }[]> {
  const runIds = await listRunIds(env, workflowRunRepoId);
  const out: { runId: string; type: string; body: Record<string, unknown> }[] =
    [];
  for (const runId of runIds) {
    const events = await readWorkflowRunEvents(env, anchorRunId, runId);
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

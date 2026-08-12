// FIFO mail-trigger serialization round-trip integration test.
//
// Deploys a multi-step workflow against the workflow-deploy
// orchestrator's multi-step branch, fires three distinct mails at the
// deployment's trigger address in quick succession, and verifies that the
// first fires the deployment's one stable run while the queued post-terminal
// mails are rejected in FIFO order.
//
// The supervisor's mail flow path enqueues every inbound mail into the
// workflow-run repo's `addresses/<segment>/inbox/` FIFO via
// `enqueueInbox`, and a per-deployment serial dispatch loop drains the
// inbox in arrival order (filename-prefix sort on `receivedAt`),
// forwarding the first entry to the workflow-process child as a
// `trigger.fire`. The loop waits for the run's terminal event before
// dequeueing the next entry; once terminal, it permanently rejects later
// entries and records that result while moving each processing entry into
// `consumed/<messageId>.json`. With three mails fired before the first run has
// reached terminal, the last two are forced to queue, which pins both FIFO
// ordering and the no-refire invariant.
//
// The deployment is intentionally multi-step (a two-step workflow
// rather than a trivial single-step one): the FIFO invariant lives
// only on the supervisor-driven multi-step path. The trivial-deploy
// branch routes mail directly through the session manager and does
// not exercise the claim-check substrate, so a "trivial workflow that
// handles mail" would not test the FIFO surface this commit pins.
//
// Crash-replay follow-up. A second test case for the supervisor's
// spawn-time `replayProcessingToInbox` behaviour -- writing a mock
// `processing/<messageId>.json` entry via `simulateProcessingCrash`
// before the supervisor spawns, then asserting the replay moves it
// back into `inbox/` and the dispatch loop consumes it -- is a
// follow-up. The fixture's `simulateProcessingCrash` helper composes
// `enqueueInbox` plus `dequeueToProcessing` against the workflow-run
// repo, but the existing fixture has no clean handle to seed that
// state BEFORE the supervisor spawns: the deployment is set up by
// the orchestrator (which spawns the supervisor synchronously) and
// the supervisor's replay runs once at spawn, off the critical path.
// A faithful crash-replay assertion requires either a supervisor
// recycle hook (which the fixture does not currently expose end-to-
// end) or a pre-spawn seeding hook on `deployWorkflow`. Adding either
// is a separate landing.
//
// The orchestrator's multi-step branch is composed in-test: the per-step
// launch callback drives `env.hub.sessionService.stageWorkflowStep` (the
// stage-only path, no warm harness) and the `sendMultiStepDeploy` hand-off
// is supplied against `env.hub.router.sendAgentDeploy` so the sidecar's
// deploy router takes the workflow-process spawn path. This mirrors the
// multistep-signal and drain-roundtrip tests' shape so a regression
// in any of the seven hops surfaces uniformly across the three.
//
// The pre-landed `deploy-flow-env` fixture supplies every helper this
// file consumes; this file does not modify the fixture.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { defineAgent, createDefaultDirectorRegistry } from "@intx/agent";
import type { HarnessConfig } from "@intx/types/runtime";
import { defineWorkflow, step, type WorkflowDefinition } from "@intx/workflow";
import {
  createWorkflowDeployOrchestrator,
  deriveRunAddress,
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
  readClaimCheckDir,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { waitForConsumedEntries } from "./fifo-mail-helpers";
import { toLaunchDeployContent } from "./launch-session-bridge";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "fifo-mail-1";
const WORKFLOW_RUN_REF = "refs/heads/main";

const MESSAGE_IDS: readonly string[] = [
  "<fifo-mail-1@integration.interchange>",
  "<fifo-mail-2@integration.interchange>",
  "<fifo-mail-3@integration.interchange>",
];

// A sustained-load companion in `fifo-mail-load.test.ts` pins the
// dispatch loop's serial discipline under pressure (the 3-mail case
// pins that the invariant exists; the load case pins that it
// survives a sustained batch of concurrent enqueues). The load case
// is held out of `make test`'s default run because it is a
// sustained-pressure test rather than a routine integration check;
// it runs via `make test-load` instead.

let env: DeployFlowEnv;

beforeAll(async () => {
  env = await startDeployFlowEnv();
});

afterAll(async () => {
  await env.teardown();
});

describe("FIFO mail-trigger serialization", () => {
  test("sidecar registers with hub", () => {
    expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
  });

  test("three mails preserve FIFO while only the first fires the stable run", async () => {
    const agent1 = defineAgent({
      id: "agent-fifo-step1",
      systemPrompt: "You are the first FIFO step agent.",
      tools: [],
      capabilities: [],
      inference: {
        sources: [{ provider: "anthropic", model: "mock-model" }],
      },
    });
    const agent2 = defineAgent({
      id: "agent-fifo-step2",
      systemPrompt: "You are the second FIFO step agent.",
      tools: [],
      capabilities: [],
      inference: {
        sources: [{ provider: "anthropic", model: "mock-model" }],
      },
    });

    const deploymentMailAddress = deriveRunAddress({
      runId: DEPLOYMENT_ID,
      domain: DEPLOYMENT_DOMAIN,
    });

    const workflow: WorkflowDefinition = defineWorkflow({
      id: `wf_${DEPLOYMENT_ID}`,
      trigger: { type: "mail", to: deploymentMailAddress },
      steps: {
        step1: step({ agent: agent1 }),
        step2: step({ agent: agent2, after: ["step1"] }),
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
      await env.hub.sessionService.stageWorkflowStep({
        agentAddress: orchestratorParams.agentAddress,
        agentId: orchestratorParams.agentId,
        runId: orchestratorParams.instanceId,
        config: orchestratorParams.config,
        deployContent: toLaunchDeployContent(deployContent),
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
            message: `fifo-mail test: write workflow repo ${args.workflowRepoId}`,
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

    // Fire all three mails in quick succession. `routeMail` is
    // synchronous against the hub-side mail bus; the supervisor's
    // `onMailMessage` enqueues each into the inbox FIFO in arrival
    // order. The supervisor's serial dispatch loop only dequeues the
    // next entry after the prior run's terminal event lands, so two
    // of the three mails are guaranteed to queue.
    const firedMessageIds: string[] = [];
    for (const messageId of MESSAGE_IDS) {
      const { messageId: routed } = await fireMailTrigger(
        env,
        deploymentMailAddress,
        { messageId },
      );
      firedMessageIds.push(routed);
    }
    expect(firedMessageIds).toEqual([...MESSAGE_IDS]);

    // The stable runId is the deployment address. Its event history is never
    // cleared: the first mail owns RunStarted and later queued mails become
    // terminal rejections in the claim-check index.
    const runId = deploymentMailAddress;

    const consumedEntries = await waitForConsumedEntries(
      env,
      workflowRunRepoId,
      deploymentMailAddress,
      MESSAGE_IDS,
      { timeoutMs: 90_000, diagnostics: env.sidecarDiagnostics },
    );
    const consumedMessageIds = consumedEntries.map((e) => e.messageId);
    for (const messageId of MESSAGE_IDS) {
      expect(consumedMessageIds).toContain(messageId);
    }
    expect(
      consumedEntries.find((entry) => entry.messageId === MESSAGE_IDS[0])
        ?.rejection,
    ).toBeUndefined();
    for (const messageId of MESSAGE_IDS.slice(1)) {
      expect(
        consumedEntries.find((entry) => entry.messageId === messageId)
          ?.rejection?.code,
      ).toBe("workflow_run_terminal");
    }

    // Wait for the one run to reach terminal.
    const terminal = await waitForWorkflowRunComplete(
      env,
      DEPLOYMENT_ID,
      runId,
      { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
    );
    expect(terminal.type).toBe("RunCompleted");

    const receivedAts = MESSAGE_IDS.map((mid) => {
      const entry = consumedEntries.find((e) => e.messageId === mid);
      if (entry === undefined) {
        throw new Error(
          `fifo-mail: consumed entry for ${mid} missing after all runs completed`,
        );
      }
      return entry.receivedAt;
    });
    // Strictly non-decreasing arrival order: `receivedAt` is a
    // millisecond timestamp the supervisor captures inside its
    // enqueue path, so successive mails fired in this test's tight
    // loop may share a millisecond. Strict-ascending would falsely
    // fail on a fast machine; the FIFO invariant the substrate
    // pins is `receivedAt` non-decreasing across the inbox key
    // sort, with the messageId tiebreak handling collisions.
    for (let i = 1; i < receivedAts.length; i += 1) {
      const prev = receivedAts[i - 1];
      const curr = receivedAts[i];
      if (prev === undefined || curr === undefined) {
        throw new Error("unreachable");
      }
      expect(curr).toBeGreaterThanOrEqual(prev);
    }

    // Inbox must be empty after every consumed/ entry lands.
    // The consumed/ wait above already guarantees every
    // dispatched run reached `markConsumed`, which removes the
    // entry from `processing/` atomically with the consumed/
    // write. The reads here are therefore one-shot.
    const inboxEntries = await readClaimCheckDir(
      env,
      workflowRunRepoId,
      deploymentMailAddress,
      "inbox",
    );
    expect(inboxEntries).toEqual([]);

    // Processing must be empty: every dispatched entry hit
    // `markConsumed` after its terminal event.
    const processingEntries = await readClaimCheckDir(
      env,
      workflowRunRepoId,
      deploymentMailAddress,
      "processing",
    );
    expect(processingEntries).toEqual([]);

    // Canonical event chain assertion for the one stable run:
    // the multi-step workflow above is `step1 -> step2`, so the
    // expected chain is `RunStarted -> StepStarted{step1} ->
    // StepCompleted{step1} -> StepStarted{step2} ->
    // StepCompleted{step2} -> RunCompleted`. No later mail may replace it.
    const currentEvents = await readWorkflowRunEvents(
      env,
      DEPLOYMENT_ID,
      runId,
    );
    const currentTypes = currentEvents.map((e) => e.type);
    const observedSequence = `observed: ${currentTypes.join(" -> ")}`;

    const runStartedIdx = currentTypes.indexOf("RunStarted");
    const step1StartedIdx = currentTypes.findIndex(
      (t, i) =>
        t === "StepStarted" && currentEvents[i]?.body["stepId"] === "step1",
    );
    const step1CompletedIdx = currentTypes.findIndex(
      (t, i) =>
        t === "StepCompleted" && currentEvents[i]?.body["stepId"] === "step1",
    );
    const step2StartedIdx = currentTypes.findIndex(
      (t, i) =>
        t === "StepStarted" && currentEvents[i]?.body["stepId"] === "step2",
    );
    const step2CompletedIdx = currentTypes.findIndex(
      (t, i) =>
        t === "StepCompleted" && currentEvents[i]?.body["stepId"] === "step2",
    );
    const runCompletedIdx = currentTypes.indexOf("RunCompleted");

    expect(
      `runStarted@${String(runStartedIdx)} (${observedSequence})`,
    ).not.toBe(`runStarted@-1 (${observedSequence})`);
    expect(step1StartedIdx).toBeGreaterThan(runStartedIdx);
    expect(step1CompletedIdx).toBeGreaterThan(step1StartedIdx);
    expect(step2StartedIdx).toBeGreaterThan(step1CompletedIdx);
    expect(step2CompletedIdx).toBeGreaterThan(step2StartedIdx);
    expect(runCompletedIdx).toBeGreaterThan(step2CompletedIdx);

    // The immutable RunStarted belongs to the first message.
    const runStartedBody = currentEvents[runStartedIdx]?.body;
    if (runStartedBody === undefined) throw new Error("unreachable");
    expect(runStartedBody["consumedMessageId"]).toBe(MESSAGE_IDS[0]);
  }, 60_000);
});

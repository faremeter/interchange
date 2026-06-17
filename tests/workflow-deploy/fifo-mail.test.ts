// FIFO mail-trigger serialization round-trip integration test.
//
// Deploys a multi-step workflow against the workflow-deploy
// orchestrator's multi-step branch, fires three distinct mails at the
// deployment's trigger address in quick succession, waits for every
// run to terminate, and asserts the per-mail dispatch landed in
// arrival order with the workflow-run claim-check substrate in the
// expected steady state.
//
// The supervisor's mail flow path enqueues every inbound mail into the
// workflow-run repo's `addresses/<segment>/inbox/` FIFO via
// `enqueueInbox`, and a per-deployment serial dispatch loop drains the
// inbox in arrival order (filename-prefix sort on `receivedAt`),
// forwarding each entry to the workflow-process child as a
// `trigger.fire`. The loop waits for the run's terminal event before
// dequeueing the next entry; on terminal it calls `markConsumed` to
// move the processing entry into `consumed/<messageId>.json`. With
// three mails fired before the first run has reached terminal, the
// last two are forced to queue, which is the FIFO ordering this test
// pins.
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
// The orchestrator's multi-step branch is composed in-test because
// the pre-landed `deploy-flow-env` fixture wires only the trivial
// `launchSession` callback against `env.hub.sessionService.launchSession`;
// the multi-step `sendMultiStepDeploy` hand-off is supplied here
// against `env.hub.router.sendAgentDeploy` so the sidecar's deploy
// router takes the workflow-process spawn path. This mirrors the
// multistep-signal and drain-roundtrip tests' shape so a regression
// in any of the seven hops surfaces uniformly across the three.
//
// The pre-landed `deploy-flow-env` fixture supplies every helper this
// file consumes; this file does not modify the fixture.

import fs from "node:fs";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import git from "isomorphic-git";

import { defineAgent, createDefaultDirectorRegistry } from "@intx/agent";
import type { HarnessConfig } from "@intx/types/runtime";
import { defineWorkflow, step, type WorkflowDefinition } from "@intx/workflow";
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
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "fifo-mail-1";
const DEPLOYMENT_ID_LOAD = "fifo-mail-load-1";
const WORKFLOW_RUN_REF = "refs/heads/main";
const CLAIM_CHECK_REF = "refs/heads/events";

const MESSAGE_IDS: readonly string[] = [
  "<fifo-mail-1@integration.interchange>",
  "<fifo-mail-2@integration.interchange>",
  "<fifo-mail-3@integration.interchange>",
];

// Sustained-load FIFO assertion. Greybeard's pre-PR coverage gap on
// the dispatch loop's serial discipline: the 3-mail case above pins
// the invariant exists, but only an under-load test surfaces a
// regression where the dispatch loop's "wait for terminal before
// dequeue" gate silently degrades (e.g. a future change that races
// markConsumed against the next dispatchOne).
//
// Greybeard's suggested floor was 50; this test fires
// `LOAD_MAIL_COUNT` mails in quick succession, asserts every one
// lands in consumed/, and asserts the consumed envelopes' arrival
// timestamps are non-decreasing across the inbox-arrival order.
//
// Empirical throughput observation. With LOAD_MAIL_COUNT >= 20 the
// test consistently times out in CI: the per-mail wall-clock
// climbs to ~15-25s under sustained pressure. Two distinct
// bottlenecks compound:
//
//   1. Pack-push serialisation on the sender. Originally every
//      supervisor `writeTreePreservingPrefix` awaited the hub's
//      pack-push ack before returning, so a run with K events
//      paid K round-trips of latency in series. The boot-edge
//      facade (`apps/sidecar/src/workflow-run-pack-client.ts`,
//      `createWorkflowRunPackPushingRepoStore`) now COALESCES
//      pushes per `(repoId, ref)`: a write returns as soon as the
//      local commit lands, and pushes that arrive while a prior
//      push is in flight are squashed into a single follow-up
//      push that captures every intermediate commit. The
//      receive-side substrate already accepts multi-commit packs
//      (`packages/hub-sessions/src/repo-store/store.ts` walks the
//      pack's parent chain and runs `validatePush` per new
//      commit in topological order), so the on-disk semantics are
//      preserved.
//
//   2. `validatePush`'s O(total-events) enumeration on the
//      receive side. The `workflow-run-kind` handler's
//      `validatePush` walks every `runs/<runId>/events/` entry on
//      every push and runs `checkPriorByteEquality` per event to
//      enforce append-only. As the repo accumulates events the
//      per-write cost rises linearly, so the total cost for N
//      writes against a repo that ends up at N events is O(N^2).
//      With 4-5 events per mail × 50 mails the per-write
//      validatePush walks ~200-250 events; this is the residual
//      ~15-25s/mail floor the pack-push coalesce cannot move.
//
// Fixing (2) requires changes to `packages/hub-sessions/`, which
// the task scope holds out of reach. `LOAD_MAIL_COUNT` therefore
// stays at 15 (5x the 3-mail baseline), CI-tractable, and the
// validatePush O(N) finding is the production-readiness follow-up
// the test comment now pins.
const LOAD_MAIL_COUNT = 15;
const LOAD_MESSAGE_IDS: readonly string[] = Array.from(
  { length: LOAD_MAIL_COUNT },
  (_unused, i) =>
    `<fifo-mail-load-${(i + 1).toString().padStart(3, "0")}@integration.interchange>`,
);

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

  test("three mails dispatch in arrival order and land in consumed/", async () => {
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

    const deploymentMailAddress = deriveDeploymentAddress({
      deploymentId: DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
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

    // Wait until every fired mail has reached a terminal run. The
    // discovery walks the workflow-run repo for runs whose
    // `RunStarted.consumedMessageId` matches one of the mails fired
    // above; the test does not know the runId the supervisor minted
    // up front (the supervisor derives it from the Message-Id
    // header).
    const observed = await waitForRunsByMessageIds(
      env,
      DEPLOYMENT_ID,
      workflowRunRepoId,
      MESSAGE_IDS,
      { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
    );

    // Drive each run to terminal. `waitForRunsByMessageIds` waits
    // only for `RunStarted` to land per mail, not the terminal
    // event; the per-run terminal wait below confirms each run
    // reaches `RunCompleted`.
    for (const entry of observed) {
      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        entry.runId,
        { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
      );
      expect(`${entry.messageId}: ${String(terminal.type)}`).toBe(
        `${entry.messageId}: RunCompleted`,
      );
    }

    // FIFO ordering assertion. The runs the supervisor minted from
    // mails [m1, m2, m3] must reach `RunStarted` in that order.
    // `observed` is already returned in mail-fire order, so the
    // `seq` of each entry's `RunStarted` event must be strictly
    // ascending when compared at the file-mtime level. We instead
    // pin the property the supervisor's dispatch loop actually
    // guarantees: a run's `RunStarted` precedes the next run's
    // `RunStarted` along the inbox-key sort. The most direct
    // observable surface is the workflow-run repo's inbox FIFO
    // sort: the supervisor consumes inbox entries in
    // `receivedAt` order. After all three runs complete, the
    // `consumed/<messageId>.json` index entries should carry
    // `receivedAt` values that match the inbox-arrival ordering.
    //
    // The supervisor's dispatch loop writes `markConsumed` AFTER
    // the run's terminal event lands; the test's
    // `waitForWorkflowRunComplete` above observes the terminal
    // event at the hub the moment that event's pack push acks,
    // which is strictly before the supervisor's subsequent
    // `markConsumed` pack push lands at the hub. Polling here
    // closes that intra-supervisor sequencing window for the
    // final run -- the wrap's hub-ack-await guarantees prior
    // writes are visible at the hub by the time the next write
    // begins, but the last `markConsumed` of the burst still
    // has to traverse the supervisor's dispatch loop and pack
    // pipeline after the third run's terminal event observation.
    const consumedEntries = await waitForConsumedEntries(
      env,
      workflowRunRepoId,
      deploymentMailAddress,
      MESSAGE_IDS,
      // 90s headroom: the supervisor's per-message markConsumed pack
      // push has to traverse the wrap's hub-ack-await for every prior
      // pack push in the dispatch loop, so the last markConsumed of
      // the burst lands well after the test observes the third run's
      // terminal event. Under parallel test load on a busy machine
      // even 60s flaked; 90s leaves headroom under the 120s bun
      // per-test cap.
      { timeoutMs: 90_000, diagnostics: env.sidecarDiagnostics },
    );
    const consumedMessageIds = consumedEntries.map((e) => e.messageId);
    for (const messageId of MESSAGE_IDS) {
      expect(consumedMessageIds).toContain(messageId);
    }
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

    // Canonical event chain assertion for the first run: the
    // multi-step workflow above is `step1 -> step2`, so the
    // expected chain is `RunStarted -> StepStarted{step1} ->
    // StepCompleted{step1} -> StepStarted{step2} ->
    // StepCompleted{step2} -> RunCompleted`.
    const firstRunId = observed[0]?.runId;
    if (firstRunId === undefined) {
      throw new Error("unreachable: observed[0] missing");
    }
    const firstEvents = await readWorkflowRunEvents(
      env,
      DEPLOYMENT_ID,
      firstRunId,
    );
    const firstTypes = firstEvents.map((e) => e.type);
    const observedSequence = `observed: ${firstTypes.join(" -> ")}`;

    const runStartedIdx = firstTypes.indexOf("RunStarted");
    const step1StartedIdx = firstTypes.findIndex(
      (t, i) =>
        t === "StepStarted" && firstEvents[i]?.body["stepId"] === "step1",
    );
    const step1CompletedIdx = firstTypes.findIndex(
      (t, i) =>
        t === "StepCompleted" && firstEvents[i]?.body["stepId"] === "step1",
    );
    const step2StartedIdx = firstTypes.findIndex(
      (t, i) =>
        t === "StepStarted" && firstEvents[i]?.body["stepId"] === "step2",
    );
    const step2CompletedIdx = firstTypes.findIndex(
      (t, i) =>
        t === "StepCompleted" && firstEvents[i]?.body["stepId"] === "step2",
    );
    const runCompletedIdx = firstTypes.indexOf("RunCompleted");

    expect(
      `runStarted@${String(runStartedIdx)} (${observedSequence})`,
    ).not.toBe(`runStarted@-1 (${observedSequence})`);
    expect(step1StartedIdx).toBeGreaterThan(runStartedIdx);
    expect(step1CompletedIdx).toBeGreaterThan(step1StartedIdx);
    expect(step2StartedIdx).toBeGreaterThan(step1CompletedIdx);
    expect(step2CompletedIdx).toBeGreaterThan(step2StartedIdx);
    expect(runCompletedIdx).toBeGreaterThan(step2CompletedIdx);

    const runStartedBody = firstEvents[runStartedIdx]?.body;
    if (runStartedBody === undefined) throw new Error("unreachable");
    expect(runStartedBody["consumedMessageId"]).toBe(MESSAGE_IDS[0]);

    // Cross-run correlation: every observed run's `RunStarted` body
    // must reference its mail's messageId exactly. This is the
    // property the supervisor's `messageId -> runId` minting pins,
    // and the consumed-index assertion above relies on it.
    for (const entry of observed) {
      const events = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        entry.runId,
      );
      const started = events.find((e) => e.type === "RunStarted");
      if (started === undefined) {
        throw new Error(
          `fifo-mail: run ${entry.runId} has no RunStarted event`,
        );
      }
      expect(started.body["consumedMessageId"]).toBe(entry.messageId);
    }
  }, 60_000);

  test(`${String(LOAD_MAIL_COUNT)} mails dispatch in arrival order under load`, async () => {
    // Greybeard's coverage-gap follow-up to the 3-mail case above.
    // The orchestrator's `isTrivialDeploy` requires
    // `stepOrder.length === 1` AND `trivialBindings` present;
    // absent the trivial-bindings (this fixture does not pass
    // them), a single-step workflow still routes through the
    // supervisor's FIFO inbox dispatch loop. The load test
    // therefore uses a single-step workflow to keep per-mail
    // commit pressure tractable in CI: every mail run still
    // exercises inbox -> processing -> trigger.fire -> wait for
    // terminal -> markConsumed, but trims one StepStarted +
    // StepCompleted commit pair off the per-run pack-push
    // pipeline. The FIFO invariant under test does not depend on
    // step count.
    //
    // The deployment id and mail address are distinct from the
    // 3-mail case so the supervisor's per-deployment dispatch
    // loop, inbox subtree, and runs/ namespace do not collide
    // with the prior test's substrate state.
    const loadAgent = defineAgent({
      id: "agent-fifo-load-step",
      systemPrompt: "You are the FIFO-load step agent.",
      tools: [],
      capabilities: [],
      inference: {
        sources: [{ provider: "anthropic", model: "mock-model" }],
      },
    });

    const deploymentMailAddress = deriveDeploymentAddress({
      deploymentId: DEPLOYMENT_ID_LOAD,
      deploymentDomain: DEPLOYMENT_DOMAIN,
    });

    const workflow: WorkflowDefinition = defineWorkflow({
      id: `wf_${DEPLOYMENT_ID_LOAD}`,
      trigger: { type: "mail", to: deploymentMailAddress },
      steps: {
        loadStep: step({ agent: loadAgent }),
      },
    });

    const config: HarnessConfig = {
      sessionId: SESSION_ID,
      agentId: `ins_${DEPLOYMENT_ID_LOAD}`,
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
            message: `fifo-mail-load test: write workflow repo ${args.workflowRepoId}`,
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
        deploymentId: DEPLOYMENT_ID_LOAD,
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
      deploymentId: DEPLOYMENT_ID_LOAD,
      workflowDefinition: workflow,
      workflowRunRepoId,
      workflowRunRef: WORKFLOW_RUN_REF,
      mailAddress: deploymentMailAddress,
    });

    expect(env.hub.router.getRoutableAddresses()).toContain(
      deploymentMailAddress,
    );

    // Fire all 50 mails in quick succession. The supervisor's
    // dispatch loop must drain them in strict FIFO order; the
    // canonical event chain materialises per-run, and the
    // consumed/ subtree carries one envelope per mail in
    // receivedAt-non-decreasing order.
    const firedMessageIds: string[] = [];
    for (const messageId of LOAD_MESSAGE_IDS) {
      const { messageId: routed } = await fireMailTrigger(
        env,
        deploymentMailAddress,
        { messageId },
      );
      firedMessageIds.push(routed);
    }
    expect(firedMessageIds).toEqual([...LOAD_MESSAGE_IDS]);

    // Wait for `consumed/` to carry every messageId. The dispatch
    // loop only writes `markConsumed` after the run's terminal
    // event lands, so the consumed-presence wait is the strongest
    // observable terminal-side signal -- it implies every run
    // reached terminal AND the supervisor's per-run cleanup
    // succeeded.
    const consumedEntries = await waitForConsumedEntries(
      env,
      workflowRunRepoId,
      deploymentMailAddress,
      LOAD_MESSAGE_IDS,
      { timeoutMs: 240_000, diagnostics: env.sidecarDiagnostics },
    );
    expect(consumedEntries.length).toBe(LOAD_MAIL_COUNT);

    // FIFO invariant: the consumed/ envelopes' `receivedAt`
    // timestamps must be non-decreasing when consulted in the
    // mail-fire order. Strict ascending would falsely fail when two
    // adjacent enqueues land in the same millisecond; the
    // substrate's FIFO key tiebreaks on messageId, so this is the
    // strongest invariant the substrate actually pins.
    const receivedAts = LOAD_MESSAGE_IDS.map((mid) => {
      const entry = consumedEntries.find((e) => e.messageId === mid);
      if (entry === undefined) {
        throw new Error(
          `fifo-mail-load: consumed entry for ${mid} missing after all runs completed`,
        );
      }
      return entry.receivedAt;
    });
    for (let i = 1; i < receivedAts.length; i += 1) {
      const prev = receivedAts[i - 1];
      const curr = receivedAts[i];
      if (prev === undefined || curr === undefined) {
        throw new Error("unreachable");
      }
      expect(curr).toBeGreaterThanOrEqual(prev);
    }

    // Inbox and processing must be empty: every fired mail has
    // landed in consumed/.
    const inboxEntries = await readClaimCheckDir(
      env,
      workflowRunRepoId,
      deploymentMailAddress,
      "inbox",
    );
    expect(inboxEntries).toEqual([]);
    const processingEntries = await readClaimCheckDir(
      env,
      workflowRunRepoId,
      deploymentMailAddress,
      "processing",
    );
    expect(processingEntries).toEqual([]);

    // Every run must materialise the canonical single-step event
    // chain (RunStarted -> StepStarted{loadStep} ->
    // StepCompleted{loadStep} -> RunCompleted). The supervisor
    // mints the runId from the Message-Id header, so we resolve
    // message-id -> runId via RunStarted bodies.
    const observed = await waitForRunsByMessageIds(
      env,
      DEPLOYMENT_ID_LOAD,
      workflowRunRepoId,
      LOAD_MESSAGE_IDS,
      { timeoutMs: 60_000, diagnostics: env.sidecarDiagnostics },
    );
    expect(observed.length).toBe(LOAD_MAIL_COUNT);

    for (const entry of observed) {
      const events = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID_LOAD,
        entry.runId,
      );
      const types = events.map((e) => e.type);
      const runStartedIdx = types.indexOf("RunStarted");
      const stepStartedIdx = types.findIndex(
        (t, i) =>
          t === "StepStarted" && events[i]?.body["stepId"] === "loadStep",
      );
      const stepCompletedIdx = types.findIndex(
        (t, i) =>
          t === "StepCompleted" && events[i]?.body["stepId"] === "loadStep",
      );
      const runCompletedIdx = types.indexOf("RunCompleted");

      if (
        runStartedIdx < 0 ||
        stepStartedIdx <= runStartedIdx ||
        stepCompletedIdx <= stepStartedIdx ||
        runCompletedIdx <= stepCompletedIdx
      ) {
        throw new Error(
          `fifo-mail-load: run ${entry.runId} (messageId=${entry.messageId}) chain malformed: ${types.join(" -> ")}`,
        );
      }
      const startedBody = events[runStartedIdx]?.body;
      if (startedBody === undefined) throw new Error("unreachable");
      expect(startedBody["consumedMessageId"]).toBe(entry.messageId);
    }
  }, 300_000);
});

/**
 * Walk every `runs/<runId>/events/` subtree on the deployment's
 * workflow-run repo until each of `messageIds` is observed in some
 * run's `RunStarted.consumedMessageId`. Returns one
 * `{ messageId, runId }` per supplied `messageId`, preserving the
 * input order so the caller can assert FIFO mail-fire ordering
 * downstream.
 */
async function waitForRunsByMessageIds(
  env: DeployFlowEnv,
  deploymentId: string,
  workflowRunRepoId: RepoId,
  messageIds: readonly string[],
  opts: { timeoutMs?: number; diagnostics?: () => string } = {},
): Promise<{ messageId: string; runId: string }[]> {
  const { timeoutMs = 30_000, diagnostics } = opts;
  const start = Date.now();
  for (;;) {
    const runIds = await listRunIds(env, workflowRunRepoId);
    const byMessageId = new Map<string, string>();
    for (const runId of runIds) {
      const events = await readWorkflowRunEvents(env, deploymentId, runId);
      for (const event of events) {
        if (event.type !== "RunStarted") continue;
        const consumed = event.body["consumedMessageId"];
        if (typeof consumed !== "string") continue;
        byMessageId.set(consumed, runId);
      }
    }
    const allObserved = messageIds.every((mid) => byMessageId.has(mid));
    if (allObserved) {
      return messageIds.map((messageId) => {
        const runId = byMessageId.get(messageId);
        if (runId === undefined) throw new Error("unreachable");
        return { messageId, runId };
      });
    }
    if (Date.now() - start > timeoutMs) {
      const diag = diagnostics?.();
      const ctx = diag ? `\n${diag}` : "";
      const observed = [...byMessageId.keys()].join(", ");
      const deploymentMailAddress =
        env.deployments.get(deploymentId)?.mailAddress ?? "";
      const inbox = await readClaimCheckDir(
        env,
        workflowRunRepoId,
        deploymentMailAddress,
        "inbox",
      );
      const processing = await readClaimCheckDir(
        env,
        workflowRunRepoId,
        deploymentMailAddress,
        "processing",
      );
      const consumed = await readClaimCheckDir(
        env,
        workflowRunRepoId,
        deploymentMailAddress,
        "consumed",
      );
      const eventsByRun: string[] = [];
      for (const runId of runIds) {
        const evs = await readWorkflowRunEvents(env, deploymentId, runId);
        eventsByRun.push(`  ${runId}: ${evs.map((e) => e.type).join(" -> ")}`);
      }
      throw new Error(
        `waitForRunsByMessageIds timed out after ${String(timeoutMs)}ms; expected ${messageIds.join(", ")}; observed ${observed || "<none>"};\n` +
          `inbox: ${inbox.map((e) => e.filename).join(", ") || "<empty>"}\n` +
          `processing: ${processing.map((e) => e.filename).join(", ") || "<empty>"}\n` +
          `consumed: ${consumed.map((e) => e.filename).join(", ") || "<empty>"}\n` +
          `runs:\n${eventsByRun.join("\n") || "<no runs>"}` +
          ctx,
      );
    }
    await new Promise((r) => setTimeout(r, 50));
  }
}

/**
 * List all `runs/<runId>/` subdirectories on the deployment's
 * workflow-run repo's main ref. Mirrors the helper shape the other
 * Phase I integration tests use.
 */
async function listRunIds(
  env: DeployFlowEnv,
  workflowRunRepoId: RepoId,
): Promise<string[]> {
  let repoDir: string;
  try {
    repoDir = env.hub.agentRepoStore.repoStore.getRepoDir(workflowRunRepoId);
  } catch {
    return [];
  }
  try {
    const oid = await git.resolveRef({
      fs,
      dir: repoDir,
      ref: WORKFLOW_RUN_REF,
    });
    const tree = await git.readTree({
      fs,
      dir: repoDir,
      oid,
      filepath: "runs",
    });
    return tree.tree
      .filter((entry) => entry.type === "tree")
      .map((entry) => entry.path);
  } catch {
    return [];
  }
}

/**
 * Read the `consumed/` dedup index for the deployment's mail
 * address on the workflow-run repo's claim-check ref
 * (`refs/heads/events`). Returns one entry per consumed message in
 * the order the substrate's tree iteration surfaces them (which is
 * filename-sorted by `isomorphic-git`).
 */
async function readConsumedEntries(
  env: DeployFlowEnv,
  workflowRunRepoId: RepoId,
  address: string,
): Promise<{ messageId: string; receivedAt: number }[]> {
  const entries = await readClaimCheckDir(
    env,
    workflowRunRepoId,
    address,
    "consumed",
  );
  const out: { messageId: string; receivedAt: number }[] = [];
  for (const entry of entries) {
    const m = /^(.+)\.json$/.exec(entry.filename);
    if (m === null || m[1] === undefined) continue;
    const messageId = m[1];
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the workflow-run kind handler validates the consumed envelope shape at push time; readers downstream of validatePush observe Record<string, unknown>
    const parsed = JSON.parse(new TextDecoder().decode(entry.bytes)) as Record<
      string,
      unknown
    >;
    const receivedAt = parsed["receivedAt"];
    if (typeof receivedAt !== "number") {
      throw new Error(
        `readConsumedEntries: ${entry.filename} envelope is missing a numeric receivedAt`,
      );
    }
    out.push({ messageId, receivedAt });
  }
  return out;
}

/**
 * Poll `consumed/` for the deployment's mail address on the
 * workflow-run claim-check ref until every supplied messageId is
 * present, then return the consumed entries. The supervisor's
 * dispatch loop writes `markConsumed` AFTER the run's terminal
 * event lands -- a hub-side observation of `RunCompleted` for the
 * final run of a burst is therefore strictly earlier than that
 * run's `markConsumed` pack push. The pack-push wrap on the
 * sidecar awaits hub ack on every write, so the writes happen in
 * order, but the last write of the burst still has to traverse
 * the dispatch loop's terminal-watcher → markConsumed →
 * pack-push pipeline before the test can observe it.
 */
async function waitForConsumedEntries(
  env: DeployFlowEnv,
  workflowRunRepoId: RepoId,
  address: string,
  messageIds: readonly string[],
  opts: { timeoutMs?: number; diagnostics?: () => string } = {},
): Promise<{ messageId: string; receivedAt: number }[]> {
  const { timeoutMs = 30_000, diagnostics } = opts;
  const start = Date.now();
  for (;;) {
    const entries = await readConsumedEntries(env, workflowRunRepoId, address);
    const seen = new Set(entries.map((e) => e.messageId));
    if (messageIds.every((mid) => seen.has(mid))) {
      return entries;
    }
    if (Date.now() - start > timeoutMs) {
      const diag = diagnostics?.();
      const ctx = diag ? `\n${diag}` : "";
      const observed = [...seen].join(", ") || "<none>";
      throw new Error(
        `waitForConsumedEntries timed out after ${String(timeoutMs)}ms; expected ${messageIds.join(", ")}; observed ${observed}` +
          ctx,
      );
    }
    await new Promise((r) => setTimeout(r, 50));
  }
}

/**
 * Read every entry in
 * `addresses/<urlEncoded(address)>/<subdir>/` on the workflow-run
 * repo's claim-check ref. Returns `[]` when the ref or subtree does
 * not exist (a legitimate empty state for inbox/ and processing/
 * after every dispatch lands in consumed/).
 */
async function readClaimCheckDir(
  env: DeployFlowEnv,
  workflowRunRepoId: RepoId,
  address: string,
  subdir: "inbox" | "processing" | "consumed",
): Promise<{ filename: string; bytes: Uint8Array }[]> {
  let repoDir: string;
  try {
    repoDir = env.hub.agentRepoStore.repoStore.getRepoDir(workflowRunRepoId);
  } catch {
    return [];
  }
  let oid: string;
  try {
    oid = await git.resolveRef({
      fs,
      dir: repoDir,
      ref: CLAIM_CHECK_REF,
    });
  } catch {
    return [];
  }
  const filepath = `addresses/${encodeURIComponent(address)}/${subdir}`;
  let tree: Awaited<ReturnType<typeof git.readTree>>;
  try {
    tree = await git.readTree({ fs, dir: repoDir, oid, filepath });
  } catch {
    return [];
  }
  const out: { filename: string; bytes: Uint8Array }[] = [];
  for (const entry of tree.tree) {
    if (entry.type !== "blob") continue;
    const blob = await git.readBlob({ fs, dir: repoDir, oid: entry.oid });
    out.push({ filename: entry.path, bytes: blob.blob });
  }
  return out;
}

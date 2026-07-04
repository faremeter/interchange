// FIFO mail-trigger sustained-load regression test.
//
// Companion to `fifo-mail.test.ts`'s 3-mail correctness case. The
// 3-mail case pins that the supervisor's dispatch loop drains the
// inbox in arrival order; this file pins that the invariant survives
// under sustained pressure -- a regression that raced the dispatch
// loop's "wait for terminal before dequeue" gate (e.g. by racing
// `markConsumed` against the next `dispatchOne`) would slip through
// the 3-mail case but surface here.
//
// Held out of `make test`'s default run because it is a
// sustained-pressure test: it fires a large batch of mails in quick
// succession to exercise the dispatch loop under load, which is a
// heavier workload than the routine integration suite carries. The
// test runs through the dedicated `make test-load` target
// instead; CI invokes that separately.
//
// The runtime-coverage rationale matches the 3-mail case: the
// supervisor's FIFO inbox dispatch loop is the only place these
// invariants live, so a regression in the supervisor surfaces here
// uniformly with the multistep-signal and drain-roundtrip tests.

import fs from "node:fs";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { defineAgent, createDefaultDirectorRegistry } from "@intx/agent";
import type { HarnessConfig } from "@intx/types/runtime";
import { defineWorkflow, step, type WorkflowDefinition } from "@intx/workflow";
import {
  createWorkflowDeployOrchestrator,
  deriveDeploymentAddress,
  type ApprovalSet,
  type DeploySingleStepFn,
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
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import {
  waitForConsumedEntries,
  waitForRunsByMessageIds,
} from "./fifo-mail-helpers";
import { toLaunchDeployContent } from "./launch-session-bridge";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID_LOAD = "fifo-mail-load-1";
const WORKFLOW_RUN_REF = "refs/heads/main";

// Sustained-load FIFO assertion. The 3-mail case in
// `fifo-mail.test.ts` pins the invariant exists, but only an
// under-load test surfaces a regression where the dispatch loop's
// "wait for terminal before dequeue" gate silently degrades (e.g.
// a future change that races markConsumed against the next
// dispatchOne).
//
// The sustained-load target is 50 mails, and that is the active
// `LOAD_MAIL_COUNT` setting. This test fires `LOAD_MAIL_COUNT` mails
// in quick succession, asserts every one lands in consumed/, and
// asserts the consumed envelopes' arrival timestamps are
// non-decreasing across the inbox-arrival order.
//
// Throughput notes. Two distinct costs once dominated per-mail
// wall-clock; one remains in force, the other has been eliminated:
//
//   1. Pack-push serialisation on the sender (still in force).
//      Originally every supervisor `writeTreePreservingPrefix`
//      awaited the hub's pack-push ack before returning, so a run
//      with K events paid K round-trips of latency in series. The
//      boot-edge facade (`apps/sidecar/src/workflow-run-pack-client.ts`,
//      `createWorkflowRunPackPushingRepoStore`) COALESCES pushes
//      per `(repoId, ref)`: a write returns as soon as the local
//      commit lands, and pushes that arrive while a prior push is
//      in flight are squashed into a single follow-up push that
//      captures every intermediate commit. The receive-side
//      substrate already accepts multi-commit packs
//      (`packages/hub-sessions/src/repo-store/store.ts` walks the
//      pack's parent chain and runs `validatePush` per new commit
//      in topological order), so the on-disk semantics are
//      preserved.
//
//   2. `validatePush` enumeration on the receive side (eliminated).
//      The `workflow-run-kind` handler's `validatePush` formerly
//      walked every `runs/<runId>/events/` entry on every push and
//      ran `checkPriorByteEquality` per event to enforce
//      append-only, so the per-write cost rose linearly with the
//      repo's total event count and the total cost for N writes was
//      O(N^2). `validatePush` is now scoped to only the run(s) a
//      given commit touches, so per-commit cost is bounded by that
//      run's own events (~4-5), not the repo's total event count.
//      The quadratic receive-side floor is gone.
//
// With the receive-side enumeration bounded to the touched run, 50
// mails complete well within the `make test-load` budget.
const LOAD_MAIL_COUNT = 50;
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
  // The fixture's teardown is the load test's only consumer of fs;
  // see `tests/hub-agent/lib/deploy-flow-env.ts` for the per-handle
  // teardown loop. The fs import is held above so the bun test
  // runner's module graph includes node:fs when the test file is
  // discovered standalone.
  void fs;
  await env.teardown();
});

describe("FIFO mail-trigger serialization under load", () => {
  test("sidecar registers with hub", () => {
    expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
  });

  test(`${String(LOAD_MAIL_COUNT)} mails dispatch in arrival order under load`, async () => {
    // Coverage-gap follow-up to the 3-mail case in
    // fifo-mail.test.ts. A single-step workflow still routes through
    // the supervisor's FIFO inbox dispatch loop, so the load test
    // uses one to keep per-mail commit pressure tractable in CI:
    // every mail run still
    // exercises inbox -> processing -> trigger.fire -> wait for
    // terminal -> markConsumed, but trims one StepStarted +
    // StepCompleted commit pair off the per-run pack-push
    // pipeline. The FIFO invariant under test does not depend on
    // step count.
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
      await env.hub.sessionService.stageWorkflowStep({
        agentAddress: orchestratorParams.agentAddress,
        agentId: orchestratorParams.agentId,
        instanceId: orchestratorParams.instanceId,
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

    const deploySingleStepAtHead: DeploySingleStepFn = (params) =>
      env.hub.sessionService.deploySingleStepAtHead(params);

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
      deploySingleStepAtHead,
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
      id: deriveDeploymentId(deploymentMailAddress),
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

    // Fire all mails in quick succession. The supervisor's dispatch
    // loop must drain them in strict FIFO order; the canonical event
    // chain materialises per-run, and the consumed/ subtree carries
    // one envelope per mail in receivedAt-non-decreasing order.
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

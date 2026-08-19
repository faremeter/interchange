// Multi-step workflow round-trip with signal-await integration test.
//
// Deploys a `step1 -> awaitSignal{name: "go"} -> step2` workflow through
// the workflow-deploy orchestrator's multi-step branch, fires the
// deployment's mail trigger, observes the runtime pause at
// `SignalAwaited`, injects a signal via the host-side signal channel,
// and asserts the runtime resumes through `step2` to `RunCompleted`.
//
// The orchestrator's multi-step branch is composed in-test: the per-step
// launch callback drives `env.hub.sessionService.stageWorkflowStep` (the
// stage-only path, no warm harness) and the `sendMultiStepDeploy` hand-off
// is supplied against `env.hub.router.sendAgentDeploy` so the sidecar's
// deploy router takes the workflow-process spawn path. The deployment
// handle is registered
// on the env via `registerDeployment` so the fixture's `injectSignal`,
// `readWorkflowRunEvents`, and `waitForWorkflowRunComplete` helpers can
// resolve it by id.
//
// The pre-landed `deploy-flow-env` fixture supplies every other helper;
// this file does not modify the fixture.
//
// Architectural-gap discipline: this test was previously authored
// against an un-wired multi-step transport surface. The plumbing that
// makes the deployment-level address routable, threads the workflow
// definition to the sidecar, spawns the workflow-process subprocess,
// and routes per-step pack pushes back to the hub now lands in the
// upstream commits this file's verification depends on.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import {
  deriveRunAddress,
  deriveStepAgentId,
  type ApprovalSet,
} from "@intx/workflow-deploy";
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
  injectSignal,
  listRunIds,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { signalGateEntry } from "./fixtures/signal-gate";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_multistep-signal-1";

// The definition's own tenant, the caller principal that creates the
// definition asset, and the `workflow`-kind asset the frozen definition
// projects over. The install/approve freeze and the anchor `workflow_run`
// insert both write against these, so they must exist in the real DB before
// the deploy runs.
const TENANT_ID = "tnt_multistep_signal";
const CALLER_PRINCIPAL_ID = "prn_multistep_signal";
const DEFINITION_ASSET_ID = "ast_multistep_signal_wf";

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
  await seedAsset(h.db, {
    id: DEFINITION_ASSET_ID,
    tenantId: TENANT_ID,
    kind: "workflow",
    name: "multistep-signal-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

describe.skipIf(!harnessDbEnvAvailable())(
  "multi-step workflow round-trip with signal-await",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("multi-step deploy provisions per-step state and resumes through awaitSignal", async () => {
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

      // Approval set: per-step grants plus the trigger-derived
      // `mail.address` / `mail.send` pair. The capability walk attaches
      // trigger grants to every step (including the `awaitSignal`
      // primitive); the operator approval set must therefore enumerate
      // them for every step that the walk surfaces.
      const operatorApprovals: ApprovalSet = new Set<string>([
        "inference.source:anthropic:mock-model",
        "director:@intx/agent/default",
        `mail.address:${deploymentMailAddress}`,
        `mail.send:${DEPLOYMENT_DOMAIN}`,
      ]);

      // Two distinct agent system prompts exercise the per-step prompt
      // wiring: each step's agent carries its own prompt in the bundled
      // source module, so different prompts produce different deploy trees
      // at each per-step `agent-state` repo.
      const entryModule = signalGateEntry({
        address: deploymentMailAddress,
        signalName: "go",
        systemPrompt1: "You are the first step agent.",
        systemPrompt2: "You are the second step agent.",
        agentId1: "agent-step1",
        agentId2: "agent-step2",
        workflowId: `wf_${DEPLOYMENT_ID}`,
      });

      const handle = await deployWorkflowSourceForTest(env, {
        entryModule,
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: deploymentMailAddress,
        approvals: operatorApprovals,
        config,
        sources: {
          step1: [inferenceSource],
          gate: [inferenceSource],
          step2: [inferenceSource],
        },
      });
      expect(handle.publicKey).toBeTruthy();

      const workflowRunRepoId = handle.workflowRunRepoId;

      // The source-ref frame round-trips through the real sidecar subprocess
      // (index the pack, check out the pinned subtree, register the address),
      // so routability is asynchronous. Wait for it before firing the trigger.
      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      // Per-step `agent-state` repos materialize on the hub: one per
      // step that carries an agent (the `awaitSignal` primitive does
      // not produce a per-step `agent-state` repo because it has no
      // `AgentDefinition`). The agent-state repo id is the per-step
      // `agentId` (not the per-step mail address): the mail address
      // carries `@` and `.` which the substrate's `SAFE_REPO_ID`
      // rejects, and the orchestrator's per-step `launchSession`
      // already provisions the repo under the safe `agentId`.
      const step1AgentId = deriveStepAgentId({
        runId: DEPLOYMENT_ID,
        stepId: "step1",
      });
      const step2AgentId = deriveStepAgentId({
        runId: DEPLOYMENT_ID,
        stepId: "step2",
      });
      const step1RepoDir = env.hub.agentRepoStore.repoStore.getRepoDir({
        kind: "agent-state",
        id: step1AgentId,
      });
      const step2RepoDir = env.hub.agentRepoStore.repoStore.getRepoDir({
        kind: "agent-state",
        id: step2AgentId,
      });
      expect(typeof step1RepoDir).toBe("string");
      expect(typeof step2RepoDir).toBe("string");

      // The deployment's trigger mail address must be routable on the
      // hub. The sidecar's deploy router takes the multi-step branch
      // and `sendAgentDeploy` records the deployment-level address on
      // the hub router's index, which is what `routeMail` consults.
      expect(env.hub.router.getRoutableAddresses()).toContain(
        deploymentMailAddress,
      );

      const { messageId } = await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<multistep-signal-1@integration.interchange>",
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
      const eventsBeforeSignal = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        runId,
      );
      const typesBeforeSignal = eventsBeforeSignal.map((e) => e.type);
      const runStartedIdx = typesBeforeSignal.indexOf("RunStarted");
      const step1StartedIdx = typesBeforeSignal.findIndex(
        (t, i) =>
          t === "StepStarted" &&
          eventsBeforeSignal[i]?.body["stepId"] === "step1",
      );
      const step1CompletedIdx = typesBeforeSignal.findIndex(
        (t, i) =>
          t === "StepCompleted" &&
          eventsBeforeSignal[i]?.body["stepId"] === "step1",
      );
      const signalAwaitedIdx = typesBeforeSignal.indexOf("SignalAwaited");

      expect(runStartedIdx).toBeGreaterThanOrEqual(0);
      expect(step1StartedIdx).toBeGreaterThan(runStartedIdx);
      expect(step1CompletedIdx).toBeGreaterThan(step1StartedIdx);
      expect(signalAwaitedIdx).toBeGreaterThan(step1CompletedIdx);

      const runStartedBody = eventsBeforeSignal[runStartedIdx]?.body;
      if (runStartedBody === undefined) throw new Error("unreachable");
      expect(runStartedBody["consumedMessageId"]).toBe(messageId);

      const signalAwaitedBody = eventsBeforeSignal[signalAwaitedIdx]?.body;
      if (signalAwaitedBody === undefined) throw new Error("unreachable");
      expect(signalAwaitedBody["signalName"]).toBe("go");

      // Inject the signal via the production signal-channel `deliver`
      // path. The fixture writes the `SignalReceived` blob against the
      // workflow-run repo at the hub.
      const injected = await injectSignal(env, DEPLOYMENT_ID, runId, "go", {
        resumed: true,
      });

      // Second-half event chain: SignalReceived{name:"go"} ->
      // StepStarted{step2} -> StepCompleted{step2} -> RunCompleted.
      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        runId,
        {
          timeoutMs: 20_000,
          diagnostics: env.sidecarDiagnostics,
        },
      );
      expect(terminal.type).toBe("RunCompleted");

      const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
      const types = events.map((e) => e.type);
      const signalReceivedIdx = types.indexOf("SignalReceived");
      const step2StartedIdx = types.findIndex(
        (t, i) => t === "StepStarted" && events[i]?.body["stepId"] === "step2",
      );
      const step2CompletedIdx = types.findIndex(
        (t, i) =>
          t === "StepCompleted" && events[i]?.body["stepId"] === "step2",
      );
      const runCompletedIdx = types.indexOf("RunCompleted");

      expect(signalReceivedIdx).toBeGreaterThan(signalAwaitedIdx);
      expect(step2StartedIdx).toBeGreaterThan(signalReceivedIdx);
      expect(step2CompletedIdx).toBeGreaterThan(step2StartedIdx);
      expect(runCompletedIdx).toBeGreaterThan(step2CompletedIdx);

      const signalReceivedBody = events[signalReceivedIdx]?.body;
      if (signalReceivedBody === undefined) throw new Error("unreachable");
      expect(signalReceivedBody["signalName"]).toBe("go");
      // The `signalId` minted by `injectSignal` must round-trip through
      // the hub -> sidecar -> supervisor -> workflow-process pipeline
      // intact; a mid-flight remint would be invisible if we only
      // checked `signalName`. Same for `payload`: a dropped payload
      // would substitute the wire schema's empty default.
      expect(signalReceivedBody["signalId"]).toBe(injected.signalId);
      expect(signalReceivedBody["payload"]).toEqual({ resumed: true });
    });
  },
);

/**
 * Read every workflow-run event under any `runs/<runId>/events/`
 * subtree on the deployment's workflow-run repo. Used to discover the
 * runId the supervisor minted from the inbound mail bytes; the test
 * does not know it up front.
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

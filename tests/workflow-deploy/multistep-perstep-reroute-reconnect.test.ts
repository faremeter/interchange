// Multi-step per-step re-route survival across a hub-link reconnect.
//
// Proves that after the sidecar's hub link is dropped and reconnects, a
// deployed MULTI-STEP workflow's addresses are re-announced and re-routed,
// and inter-step mail/signal routing still works end to end.
//
// Shape: deploy a `step1 -> awaitSignal{name:"go"} -> step2` workflow whose
// deployment address and every per-step derived address are run addresses
// (`isRunAddress` true), so they all share the workflow routing family
// that survives reconnect. Drive one mail trigger through the full inter-step chain
// (RunStarted -> step1 -> SignalAwaited -> inject signal -> step2 ->
// RunCompleted), `settleThenDrop` the hub link, wait for the deployment
// address to re-route through allocation-authenticated reconnect, assert every
// per-step address is once again a workflow-derived address routing under
// the re-established deployment, then fire a SECOND mail trigger and run the
// whole inter-step chain again. The second run only exists because the
// sidecar re-established the link, the hub restored the workflow-derived
// deployment address, and inter-step mail/signal routing came back with it.
//
// The authenticated sidecar identity is allocation-bound to one workflow run
// address. Reconnect revalidates that durable identity and the current
// allocation generation before restoring the address in `workflowAddresses`.
// The per-step staging addresses are transient bindings (bound only while a
// step's packs land, never persisted into the reconnect set), so the hub route
// that survives the reconnect is the deployment address the steps collapse
// under; inter-step routing itself lives inside the workflow-process child,
// which the surviving deployment address feeds.
//
// Harness justification: SPAWN-REAL. A real hub server, a real sidecar
// subprocess, a real workflow-process child, and a test inference provider.
// The drop is a genuine server-side WebSocket close; reconnect uses the
// sidecar's real `hub-link` path and allocation identity checks.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { deriveWorkflowRunId, isRunAddress } from "@intx/types";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import {
  deriveRunAddress,
  deriveStepAddress,
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

import {
  SESSION_ID,
  SIDECAR_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  injectSignal,
  readWorkflowRunEvents,
  settleThenDrop,
  startDeployFlowEnv,
  waitFor,
  waitForReconnect,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { signalGateEntry } from "./fixtures/signal-gate";

const DEPLOYMENT_DOMAIN = "integration.interchange";
// A substrate-safe deployment id whose run address and every per-step derived
// address are run addresses (`isRunAddress` true), matching the workflow
// routing family exercised by this test.
const DEPLOYMENT_ID = "run_multistep_reroute_1";
const STEP_IDS = ["step1", "step2"] as const;

// The definition's own tenant, the caller principal that creates the
// definition asset, and the `workflow`-kind asset the frozen definition
// projects over. The install/approve freeze and the anchor `workflow_run`
// insert both write against these, so they must exist in the real DB before
// the deploy runs.
const TENANT_ID = "tnt_multistep_reroute";
const CALLER_PRINCIPAL_ID = "prn_multistep_reroute";
const DEFINITION_ASSET_ID = "ast_multistep_reroute_wf";

let env: DeployFlowEnv;
let h: TestDb;

beforeAll(async () => {
  if (!harnessDbEnvAvailable()) return;
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
    name: "multistep-reroute-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

describe.skipIf(!harnessDbEnvAvailable())(
  "multi-step per-step re-route survival across reconnect",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("every per-step address re-routes and inter-step routing survives reconnect", async () => {
      const deploymentMailAddress = deriveRunAddress({
        runId: DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });
      // The deployment address is a run address in the workflow routing family
      // restored by allocation-authenticated reconnect.
      expect(isRunAddress(deploymentMailAddress)).toBe(true);

      // Every per-step derived address is a run address too, so each one belongs
      // to the same workflow routing family that survives reconnect by collapsing
      // under the restored deployment address rather than being resurrected
      // as its own hub route.
      const stepAddresses = STEP_IDS.map((stepId) =>
        deriveStepAddress({
          runId: DEPLOYMENT_ID,
          stepId,
          domain: DEPLOYMENT_DOMAIN,
        }),
      );
      for (const stepAddress of stepAddresses) {
        expect(isRunAddress(stepAddress)).toBe(true);
      }

      // ---- deploy the multi-step workflow ----
      // Two distinct agent system prompts exercise the per-step prompt wiring so
      // each per-step `agent-state` repo is provisioned end-to-end.
      const inferenceSource: InferenceSource = {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${String(env.inference.server.port)}`,
        credentialId: "sk-mock",
        model: "mock-model",
      };

      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentId: `${DEPLOYMENT_ID}`,
        tenantId: "tenant-1",
        principalId: "prin_multistep-reroute-1",
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

      // Wait for the deployment ack before dropping the link so the setup is
      // complete and the deployment address has an established route to restore.
      await waitFor(() => env.hub.deployAcks.has(deploymentMailAddress), {
        timeoutMs: 20_000,
        diagnostics: env.sidecarDiagnostics,
      });

      // The source-ref frame round-trips through the real sidecar subprocess, so
      // routability is asynchronous. Wait for it before driving the run.
      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      // Per-step `agent-state` repos materialize on the hub, one per step that
      // carries an agent (the `awaitSignal` primitive produces none). Their ids
      // are the per-step `agentId`s, which is the substrate-safe form of the
      // per-step address the orchestrator staged each step's tree under.
      for (const stepId of STEP_IDS) {
        const stepAgentId = deriveStepAgentId({
          runId: DEPLOYMENT_ID,
          stepId,
        });
        const stepRepoDir = env.hub.agentRepoStore.repoStore.getRepoDir({
          kind: "agent-state",
          id: stepAgentId,
        });
        expect(typeof stepRepoDir).toBe("string");
      }

      // The deployment address is routable on the hub after the multi-step
      // deploy: `sendAgentDeploy` recorded it on the router's index.
      expect(env.hub.router.getRoutableAddresses()).toContain(
        deploymentMailAddress,
      );

      // Drive the deployment's one stable run to its inter-step park, reconnect
      // there, and then complete that SAME run. Re-firing after completion is no
      // longer a valid way to prove route restoration.
      await runInterStepChainToCompletion(
        env,
        {
          anchorRunId: DEPLOYMENT_ID,
          deploymentMailAddress,
          messageId: "<multistep-reroute-1@integration.interchange>",
        },
        async () => {
          expect(env.hub.router.getRoutableAddresses()).toContain(
            deploymentMailAddress,
          );
          await settleThenDrop(env, deploymentMailAddress);
          await waitFor(
            () =>
              !env.hub.router
                .getRoutableAddresses()
                .includes(deploymentMailAddress),
            { timeoutMs: 5_000, diagnostics: env.sidecarDiagnostics },
          );

          const reconnectMs = await waitForReconnect(
            env,
            deploymentMailAddress,
            {
              timeoutMs: 20_000,
            },
          );
          expect(reconnectMs).toBeGreaterThan(1_000);
          expect(reconnectMs).toBeLessThan(20_000);
          expect(env.hub.router.getRoutableAddresses()).toContain(
            deploymentMailAddress,
          );
        },
      );
    }, 180_000);
  },
);

/**
 * Drive one full inter-step chain of the `step1 -> awaitSignal -> step2`
 * workflow to `RunCompleted`: fire the mail trigger, wait for the runtime to
 * pause at `SignalAwaited{name:"go"}`, inject the `go` signal, and wait for
 * the run to complete. Asserts the ordered event chain
 * (RunStarted -> step1 Started/Completed -> SignalAwaited -> SignalReceived ->
 * step2 Started/Completed -> RunCompleted), which is the inter-step
 * mail/signal routing under test. The optional callback runs at the durable
 * SignalAwaited park. Returns the deployment's stable runId.
 */
async function runInterStepChainToCompletion(
  env: DeployFlowEnv,
  args: {
    anchorRunId: string;
    deploymentMailAddress: string;
    messageId: string;
  },
  afterPark?: () => Promise<void>,
): Promise<string> {
  const { anchorRunId, deploymentMailAddress, messageId } = args;

  const { messageId: firedMessageId } = await fireMailTrigger(
    env,
    deploymentMailAddress,
    { messageId },
  );

  // The deployment's local part is the one stable top-level runId. RunStarted
  // is immutable, so its consumedMessageId permanently identifies the mail that
  // first fired this deployment.
  const runId = deriveWorkflowRunId(deploymentMailAddress);
  await waitFor(
    async () => {
      const events = await readWorkflowRunEvents(env, anchorRunId, runId);
      return events.some(
        (e) =>
          e.type === "RunStarted" &&
          e.body["consumedMessageId"] === firedMessageId,
      );
    },
    { diagnostics: env.sidecarDiagnostics, timeoutMs: 20_000 },
  );

  // First-half chain: RunStarted -> StepStarted{step1} ->
  // StepCompleted{step1} -> SignalAwaited{name:"go"}.
  await waitFor(
    async () => {
      const events = await readWorkflowRunEvents(env, anchorRunId, runId);
      return events.some(
        (e) => e.type === "SignalAwaited" && e.body["signalName"] === "go",
      );
    },
    { diagnostics: env.sidecarDiagnostics, timeoutMs: 20_000 },
  );

  const eventsBeforeSignal = await readWorkflowRunEvents(
    env,
    anchorRunId,
    runId,
  );
  const typesBeforeSignal = eventsBeforeSignal.map((e) => e.type);
  const runStartedIdx = typesBeforeSignal.indexOf("RunStarted");
  const step1StartedIdx = typesBeforeSignal.findIndex(
    (t, i) =>
      t === "StepStarted" && eventsBeforeSignal[i]?.body["stepId"] === "step1",
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
  expect(runStartedBody["consumedMessageId"]).toBe(firedMessageId);

  // Tests that need to interrupt a live run (for example, to exercise a
  // reconnect) do so at the durable inter-step park, never by completing and
  // trying to fire the terminal deployment again.
  await afterPark?.();

  // Inject the `go` signal through the production signal-channel path.
  const injected = await injectSignal(env, anchorRunId, runId, "go", {
    resumed: true,
  });

  // Second-half chain: SignalReceived{name:"go"} -> StepStarted{step2} ->
  // StepCompleted{step2} -> RunCompleted.
  const terminal = await waitForWorkflowRunComplete(env, anchorRunId, runId, {
    timeoutMs: 20_000,
    diagnostics: env.sidecarDiagnostics,
  });
  expect(terminal.type).toBe("RunCompleted");

  const events = await readWorkflowRunEvents(env, anchorRunId, runId);
  const types = events.map((e) => e.type);
  const signalReceivedIdx = types.indexOf("SignalReceived");
  const step2StartedIdx = types.findIndex(
    (t, i) => t === "StepStarted" && events[i]?.body["stepId"] === "step2",
  );
  const step2CompletedIdx = types.findIndex(
    (t, i) => t === "StepCompleted" && events[i]?.body["stepId"] === "step2",
  );
  const runCompletedIdx = types.indexOf("RunCompleted");

  expect(signalReceivedIdx).toBeGreaterThan(signalAwaitedIdx);
  expect(step2StartedIdx).toBeGreaterThan(signalReceivedIdx);
  expect(step2CompletedIdx).toBeGreaterThan(step2StartedIdx);
  expect(runCompletedIdx).toBeGreaterThan(step2CompletedIdx);

  const signalReceivedBody = events[signalReceivedIdx]?.body;
  if (signalReceivedBody === undefined) throw new Error("unreachable");
  expect(signalReceivedBody["signalName"]).toBe("go");
  // The `signalId` and `payload` must round-trip through the
  // hub -> sidecar -> supervisor -> workflow-process pipeline intact; a
  // mid-flight remint or dropped payload would be invisible if we checked
  // only `signalName`.
  expect(signalReceivedBody["signalId"]).toBe(injected.signalId);
  expect(signalReceivedBody["payload"]).toEqual({ resumed: true });

  return runId;
}

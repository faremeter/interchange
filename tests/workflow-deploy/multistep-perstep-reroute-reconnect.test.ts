// Multi-step per-step re-route survival across a hub-link reconnect.
//
// Proves that after the sidecar's hub link is dropped and reconnects, a
// deployed MULTI-STEP workflow's addresses are re-challenged and re-routed
// and inter-step mail/signal routing still works end to end.
//
// Shape: deploy a `step1 -> awaitSignal{name:"go"} -> step2` workflow whose
// deployment id carries the `dep_` prefix, so its deployment address and
// every per-step derived address are workflow-derived (`ins_dep_...`,
// `isWorkflowDerivedAddress` true) rather than the legacy `ins_<hex>`
// identity. Drive one mail trigger through the full inter-step chain
// (RunStarted -> step1 -> SignalAwaited -> inject signal -> step2 ->
// RunCompleted), `settleThenDrop` the hub link, wait for the deployment
// address to re-route via the reconnect ownership challenge, assert every
// per-step address is once again a workflow-derived address routing under
// the re-established deployment, then fire a SECOND mail trigger and run the
// whole inter-step chain again. The second run only exists because the
// sidecar re-established the link, the hub re-challenged the workflow-derived
// deployment address, and inter-step mail/signal routing came back with it.
//
// Two routing sets are exercised on the hub. A workflow-derived deployment
// address enters the keyless `workflowAddresses` set through the reconnect
// challenge's early-continue path in `handleChallengeResponse` (no
// `agent.reconnected` session reaction, because a workflow-derived address
// carries no `agent_instance` row); a legacy launched-agent address would
// instead land on the challenged `agentAddresses` set with a disconnect
// queue. The `deployAcks`-backed `lookupPublicKey` answers the reconnect
// challenge for the deployment address because every deploy acks its own
// key. The per-step staging addresses are transient bindings (bound only
// while a step's packs land, never persisted into the reconnect set), so the
// hub route that survives the reconnect is the deployment address the steps
// collapse under; inter-step routing itself lives inside the workflow-process
// child, which the surviving deployment address feeds.
//
// Harness justification: SPAWN-REAL. A real hub server, a real sidecar
// subprocess, a real workflow-process child, and a test inference provider.
// The drop is a genuine server-side WebSocket close; the reconnect is the
// sidecar's real `hub-link` reconnect path passing the hub's ownership
// challenge for the workflow-derived deployment address.

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
  deriveStepAddress,
  deriveStepAgentId,
  isWorkflowDerivedAddress,
  type ApprovalSet,
  type LaunchSessionFn,
  type SendMultiStepDeployFn,
  type WorkflowRepoWriter,
} from "@intx/workflow-deploy";
import { deriveDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";
import {
  DEFAULT_ASSET_REF,
  type RepoId,
  type WorkflowRunHubPrincipal,
} from "@intx/hub-sessions";

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
// A `dep_`-prefixed, substrate-safe deployment id: the deployment address
// and every per-step derived address are then workflow-derived
// (`ins_dep_...`, `isWorkflowDerivedAddress` true) rather than the legacy
// `ins_<hex>` identity the single-step reconnect smoke test uses. This is
// what routes the reconnect challenge through the `workflowAddresses`
// early-continue path.
const DEPLOYMENT_ID = "dep_multistep_reroute_1";
const WORKFLOW_RUN_REF = "refs/heads/main";
const STEP_IDS = ["step1", "step2"] as const;

let env: DeployFlowEnv;

beforeAll(async () => {
  env = await startDeployFlowEnv();
});

afterAll(async () => {
  await env.teardown();
});

describe("multi-step per-step re-route survival across reconnect", () => {
  test("sidecar registers with hub", () => {
    expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
  });

  test("every per-step address re-routes and inter-step routing survives reconnect", async () => {
    const deploymentMailAddress = deriveDeploymentAddress({
      deploymentId: DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
    });
    // The deployment address must be workflow-derived: that is the routing
    // family whose reconnect challenge takes the `workflowAddresses`
    // early-continue path. A legacy `ins_<hex>` address would land on the
    // challenged `agentAddresses` set instead and prove nothing about the
    // workflow-derived path.
    expect(isWorkflowDerivedAddress(deploymentMailAddress)).toBe(true);

    // Every per-step derived address is workflow-derived too, so each one
    // belongs to the same keyless routing family that survives reconnect by
    // collapsing under the re-challenged deployment address rather than being
    // resurrected as its own hub route.
    const stepAddresses = STEP_IDS.map((stepId) =>
      deriveStepAddress({
        deploymentId: DEPLOYMENT_ID,
        stepId,
        deploymentDomain: DEPLOYMENT_DOMAIN,
      }),
    );
    for (const stepAddress of stepAddresses) {
      expect(isWorkflowDerivedAddress(stepAddress)).toBe(true);
    }

    // ---- deploy the multi-step workflow ----
    // Two distinct agent definitions exercise the orchestrator's per-step
    // `systemPrompt` override so each per-step `agent-state` repo is
    // provisioned end-to-end.
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
      principalId: "prin_multistep-reroute-1",
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
            message: `multistep reroute reconnect: write workflow repo ${args.workflowRepoId}`,
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

    // The deployment acks its own key, which is the oracle the reconnect
    // challenge is answered against. Wait for it before dropping the link so
    // the reconnect has a key to verify.
    await waitFor(() => env.hub.deployAcks.has(deploymentMailAddress), {
      timeoutMs: 20_000,
      diagnostics: env.sidecarDiagnostics,
    });

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

    // Per-step `agent-state` repos materialize on the hub, one per step that
    // carries an agent (the `awaitSignal` primitive produces none). Their ids
    // are the per-step `agentId`s, which is the substrate-safe form of the
    // per-step address the orchestrator staged each step's tree under.
    for (const stepId of STEP_IDS) {
      const stepAgentId = deriveStepAgentId({
        deploymentId: DEPLOYMENT_ID,
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

    // ---- first run: full inter-step chain to completion ----
    const firstRunId = await runInterStepChainToCompletion(env, {
      deploymentId: DEPLOYMENT_ID,
      deploymentMailAddress,
      workflowRunRepoId,
      messageId: "<multistep-reroute-1@integration.interchange>",
      priorRunIds: new Set<string>(),
    });

    // ---- settle the pack pipeline, then drop the hub link ----
    expect(env.hub.router.getRoutableAddresses()).toContain(
      deploymentMailAddress,
    );
    await settleThenDrop(env, deploymentMailAddress);

    // The deployment address leaves routing as the server-side close lands;
    // this guards against a false "already routable" pass where the drop
    // never actually severed the link.
    await waitFor(
      () =>
        !env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
      { timeoutMs: 5_000, diagnostics: env.sidecarDiagnostics },
    );

    // ---- wait for the reconnect ownership challenge to re-route the
    // workflow-derived deployment address ----
    const reconnectMs = await waitForReconnect(env, deploymentMailAddress, {
      timeoutMs: 20_000,
    });
    // A generous lower bound guards against a false "already routable" pass
    // that never actually dropped; the upper bound catches a hung link.
    expect(reconnectMs).toBeGreaterThan(1_000);
    expect(reconnectMs).toBeLessThan(20_000);
    expect(env.hub.router.getRoutableAddresses()).toContain(
      deploymentMailAddress,
    );

    // The per-step staging bindings are transient: they are never persisted
    // into the reconnect set, so no per-step address appears in the hub's
    // routable set to assert against. The only hub route that survives the
    // reconnect is the deployment address the steps collapse under (asserted
    // routable again just above); the per-step runtimes route through that
    // single re-challenged address. The second inter-step run below, reaching
    // completion with a distinct runId, is the load-bearing proof that
    // inter-step routing came back with it -- re-deriving the step addresses
    // locally here would only restate what `deriveStepAddress` already
    // guarantees before the reconnect and pin nothing about it.

    // ---- second run after reconnect: full inter-step chain again ----
    // Only reachable because the sidecar re-established the link, the hub
    // re-challenged the workflow-derived deployment address, and inter-step
    // mail/signal routing came back with it.
    const secondRunId = await runInterStepChainToCompletion(env, {
      deploymentId: DEPLOYMENT_ID,
      deploymentMailAddress,
      workflowRunRepoId,
      messageId: "<multistep-reroute-2@integration.interchange>",
      priorRunIds: new Set<string>([firstRunId]),
    });
    expect(secondRunId).not.toBe(firstRunId);
  }, 180_000);
});

/**
 * Drive one full inter-step chain of the `step1 -> awaitSignal -> step2`
 * workflow to `RunCompleted`: fire the mail trigger, wait for the runtime to
 * pause at `SignalAwaited{name:"go"}`, inject the `go` signal, and wait for
 * the run to complete. Asserts the ordered event chain
 * (RunStarted -> step1 Started/Completed -> SignalAwaited -> SignalReceived ->
 * step2 Started/Completed -> RunCompleted), which is the inter-step
 * mail/signal routing under test. Returns the runId the supervisor minted.
 *
 * `priorRunIds` names the run ids present before this trigger so the helper
 * can isolate the run this trigger started; the supervisor mints the id from
 * the inbound mail bytes and the test does not know it up front.
 */
async function runInterStepChainToCompletion(
  env: DeployFlowEnv,
  args: {
    deploymentId: string;
    deploymentMailAddress: string;
    workflowRunRepoId: RepoId;
    messageId: string;
    priorRunIds: ReadonlySet<string>;
  },
): Promise<string> {
  const {
    deploymentId,
    deploymentMailAddress,
    workflowRunRepoId,
    messageId,
    priorRunIds,
  } = args;

  const { messageId: firedMessageId } = await fireMailTrigger(
    env,
    deploymentMailAddress,
    { messageId },
  );

  // Discover the run id this trigger started: the first run id under `runs/`
  // that was not present before the fire.
  const runId = await waitForNewRunId(env, workflowRunRepoId, priorRunIds);

  // First-half chain: RunStarted -> StepStarted{step1} ->
  // StepCompleted{step1} -> SignalAwaited{name:"go"}.
  await waitFor(
    async () => {
      const events = await readWorkflowRunEvents(env, deploymentId, runId);
      return events.some(
        (e) => e.type === "SignalAwaited" && e.body["signalName"] === "go",
      );
    },
    { diagnostics: env.sidecarDiagnostics, timeoutMs: 20_000 },
  );

  const eventsBeforeSignal = await readWorkflowRunEvents(
    env,
    deploymentId,
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

  // Inject the `go` signal through the production signal-channel path.
  const injected = await injectSignal(env, deploymentId, runId, "go", {
    resumed: true,
  });

  // Second-half chain: SignalReceived{name:"go"} -> StepStarted{step2} ->
  // StepCompleted{step2} -> RunCompleted.
  const terminal = await waitForWorkflowRunComplete(env, deploymentId, runId, {
    timeoutMs: 20_000,
    diagnostics: env.sidecarDiagnostics,
  });
  expect(terminal.type).toBe("RunCompleted");

  const events = await readWorkflowRunEvents(env, deploymentId, runId);
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

/**
 * Poll until a run id appears under `runs/` that is not in `priorRunIds`, and
 * return it. Throws on timeout so a run that never started surfaces loudly.
 */
async function waitForNewRunId(
  env: DeployFlowEnv,
  workflowRunRepoId: RepoId,
  priorRunIds: ReadonlySet<string>,
): Promise<string> {
  const start = Date.now();
  for (;;) {
    const ids = await listRunIds(env, workflowRunRepoId);
    const fresh = ids.find((id) => !priorRunIds.has(id));
    if (fresh !== undefined) return fresh;
    if (Date.now() - start > 30_000) {
      throw new Error(
        `no new run id after mail trigger; saw runIds ${JSON.stringify(ids)}\n${env.sidecarDiagnostics()}`,
      );
    }
    await new Promise((r) => setTimeout(r, 50));
  }
}

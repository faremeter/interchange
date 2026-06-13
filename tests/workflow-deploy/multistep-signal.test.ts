// Multi-step workflow round-trip with signal-await integration test.
//
// Deploys a `step1 -> awaitSignal{name: "go"} -> step2` workflow through
// the workflow-deploy orchestrator's multi-step branch, fires the
// deployment's mail trigger, observes the runtime pause at
// `SignalAwaited`, injects a signal via the host-side signal channel,
// and asserts the runtime resumes through `step2` to `RunCompleted`.
//
// The orchestrator's multi-step branch is composed in-test because the
// pre-landed `deploy-flow-env` fixture wires only the trivial
// `launchSession` callback against `env.hub.sessionService.launchSession`;
// the multi-step `sendMultiStepDeploy` hand-off is supplied here against
// `env.hub.router.sendAgentDeploy` so the sidecar's deploy router takes
// the workflow-process spawn path. The deployment handle is registered
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
  deriveStepAgentId,
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
  injectSignal,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "multistep-signal-1";
const WORKFLOW_RUN_REF = "refs/heads/main";

let env: DeployFlowEnv;

beforeAll(async () => {
  env = await startDeployFlowEnv();
});

afterAll(async () => {
  await env.teardown();
});

describe("multi-step workflow round-trip with signal-await", () => {
  test("sidecar registers with hub", () => {
    expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
  });

  test("multi-step deploy provisions per-step state and resumes through awaitSignal", async () => {
    // Two distinct agent definitions exercise the orchestrator's
    // per-step `systemPrompt` override: the multi-step branch
    // overrides `HarnessConfig.systemPrompt` from each step's agent
    // before calling `launchSession`, so different prompts produce
    // different deploy trees at each per-step `agent-state` repo.
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
          baseURL: `http://localhost:${env.inference.server.port}`,
          apiKey: "sk-mock",
          model: "mock-model",
        },
      ],
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

    // Per-step launch routes through the session service, mirroring
    // the fixture's trivial-branch wiring so each per-step
    // `agent-state` repo is provisioned end-to-end.
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

    // Multi-step hand-off goes directly through the router's
    // `sendAgentDeploy` with the workflow projection. The sidecar's
    // deploy router branches on `frame.workflow !== undefined` and
    // spawns the workflow-process subprocess.
    const sendMultiStepDeploy: SendMultiStepDeployFn = async (params) =>
      env.hub.router.sendAgentDeploy(params.agentAddress, params.config, {
        definition: {
          id: params.definition.id,
          triggers: [...params.definition.triggers],
          stepOrder: [...params.definition.stepOrder],
          // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the wire validator carries the WorkflowDefinition steps record as Record<string, unknown>; the orchestrator emits the typed primitive union shape that satisfies the wire schema
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
            message: `multistep-signal test: write workflow repo ${args.workflowRepoId}`,
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

    // The orchestrator threads `hubPublicKey` through to the
    // `sendMultiStepDeploy` callback verbatim; the router adds its
    // configured key to the on-wire frame, so the value supplied here
    // only has to satisfy the orchestrator's "required when multi-step"
    // contract.
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

    // The sidecar's deploy router slugs the deployment mail address
    // into the workflow-run repo id via `deriveTrivialDeploymentId`;
    // the helper queries `runs/<runId>/events/<seq>.json` against the
    // same id.
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

    // Per-step `agent-state` repos materialize on the hub: one per
    // step that carries an agent (the `awaitSignal` primitive does
    // not produce a per-step `agent-state` repo because it has no
    // `AgentDefinition`). The agent-state repo id is the per-step
    // `agentId` (not the per-step mail address): the mail address
    // carries `@` and `.` which the substrate's `SAFE_REPO_ID`
    // rejects, and the orchestrator's per-step `launchSession`
    // already provisions the repo under the safe `agentId`.
    const step1AgentId = deriveStepAgentId({
      deploymentId: DEPLOYMENT_ID,
      stepId: "step1",
    });
    const step2AgentId = deriveStepAgentId({
      deploymentId: DEPLOYMENT_ID,
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
    // The `signalId` minted by `injectSignal` must round-trip through
    // the hub -> sidecar -> supervisor -> workflow-process pipeline
    // intact; a mid-flight remint would be invisible if we only
    // checked `signalName`. Same for `payload`: a dropped payload
    // would substitute the wire schema's empty default.
    expect(signalReceivedBody["signalId"]).toBe(injected.signalId);
    expect(signalReceivedBody["payload"]).toEqual({ resumed: true });
  });
});

/**
 * Read every workflow-run event under any `runs/<runId>/events/`
 * subtree on the deployment's workflow-run repo. Used to discover the
 * runId the supervisor minted from the inbound mail bytes; the test
 * does not know it up front.
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

async function listRunIds(
  env: DeployFlowEnv,
  workflowRunRepoId: RepoId,
): Promise<string[]> {
  const fs = await import("node:fs");
  const git = (await import("isomorphic-git")).default;
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
      ref: "refs/heads/main",
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

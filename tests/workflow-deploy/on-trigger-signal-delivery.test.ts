// onTrigger body author-signal delivery integration test (signal-relay).
//
// The reachability gate for the CORE signal-relay capability: a deployed
// onTrigger section whose body parks on an author `awaitSignal` gate has that
// gate serviced by delivering the signal to the DEPLOYMENT (parent) run id --
// the body, a child run, never receives a signal directly. It proves the whole
// path end-to-end on the real deploy stack: the body's author gate surfaces up
// through the suspendable-child seam, the section proxies it as a signal-relay
// await on the container, a signal delivered to the parent run resolves that
// await, and the section relays it back down into the body, which continues.
//
// Shape: deploy a single-step workflow whose one step is an `onTrigger` section
// subscribed to the deployment mail address, with a NON-AGENT body -- a single
// `awaitSignal({ name })` gate with no timeout. (Body agent-step execution is
// not wired yet, INTR-310; the signal-relay feature is non-agent by nature, so
// the body IS an awaitSignal, exercising exactly the capability under test.)
//
//   1. Fire mail #1 -> the container run starts and spawns the body
//      `section__0`, which parks on `awaitSignal({ name: "proceed" })`. The
//      section proxies that gate UP as a signal-relay `SignalAwaited` on the
//      container over the SAME author name; the container is now awaiting
//      "proceed" and section__0 is parked (not complete).
//   2. Deliver "proceed" to the PARENT deployment run id (the container), the
//      way an operator would via the signals path. The container's await
//      resolves, the section relays the signal down into the body, the body's
//      gate completes, section__0 completes, and the section re-arms on its
//      input park for the next event.
//
// Load-bearing assertions: the container carries a signal-relay `SignalAwaited`
// for the author name (the proxied gate) and a `SignalReceived` for it (the
// delivery to the PARENT); `section__0` completes ONLY after the delivery (the
// relay reached the child body); the long-lived container never self-completes
// and re-arms on an input park.
//
// Harness justification: SPAWN-REAL. Real hub, real sidecar subprocess, real
// workflow-process child driving `runOnTrigger` with the production
// suspendable-child seam. No inference (the body is pure runtime). The signal
// is delivered through the sidecar router's `signal.deliver` (the delivery step
// the hub-api /signals route performs after its authz + reserved-name guards,
// which are covered by that route's own unit tests). This is a deploy-level
// test of the signal-relay capability C built.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { createDefaultDirectorRegistry } from "@intx/agent";
import type { HarnessConfig } from "@intx/types/runtime";
import {
  awaitSignal,
  defineWorkflow,
  onTrigger,
  type WorkflowDefinition,
} from "@intx/workflow";
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
  injectSignal,
  listRunIds,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { toLaunchDeployContent } from "./launch-session-bridge";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "on-trigger-signal-delivery-1";
const WORKFLOW_RUN_REF = "refs/heads/main";
const SECTION_ID = "section";
const SIGNAL_NAME = "proceed";

let env: DeployFlowEnv;

beforeAll(async () => {
  env = await startDeployFlowEnv();
});

afterAll(async () => {
  await env.teardown();
});

/**
 * The container run is the single run under the deployment's workflow-run repo
 * that is NOT a body child. Body children are `${SECTION_ID}__<n>`; the
 * container carries their `ChildSpawned`/`ChildCompleted` records in its own
 * log. Returns `undefined` until the container's `runs/` entry exists.
 */
async function findContainerRunId(
  target: DeployFlowEnv,
  workflowRunRepoId: RepoId,
): Promise<string | undefined> {
  const ids = await listRunIds(target, workflowRunRepoId);
  return ids.find((id) => !id.startsWith(`${SECTION_ID}__`));
}

async function readContainerEvents(
  target: DeployFlowEnv,
  workflowRunRepoId: RepoId,
): Promise<{ type: string; body: Record<string, unknown> }[]> {
  const containerRunId = await findContainerRunId(target, workflowRunRepoId);
  if (containerRunId === undefined) return [];
  return readWorkflowRunEvents(target, DEPLOYMENT_ID, containerRunId);
}

const hasChildCompleted = (
  events: { type: string; body: Record<string, unknown> }[],
  childRunId: string,
): boolean =>
  events.some(
    (e) => e.type === "ChildCompleted" && e.body["childRunId"] === childRunId,
  );

describe("onTrigger body author-signal delivered to the parent run reaches the body via the section", () => {
  test("sidecar registers with hub", () => {
    expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
  });

  test("a signal delivered to the deployment run resolves the body's awaitSignal gate", async () => {
    const deploymentMailAddress = deriveDeploymentAddress({
      deploymentId: DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
    });

    // The section body: a single non-agent `awaitSignal` gate (no timeout, so
    // no timer). The gate parks the body until the section relays the author
    // signal down; this is the signal-relay capability under test.
    const body: WorkflowDefinition = defineWorkflow({
      id: "authored-section-body",
      trigger: { type: "manual" },
      steps: { gate: awaitSignal({ name: SIGNAL_NAME }) },
    });

    const workflow: WorkflowDefinition = defineWorkflow({
      id: `wf_${DEPLOYMENT_ID}`,
      trigger: { type: "mail", to: deploymentMailAddress },
      steps: {
        [SECTION_ID]: onTrigger({
          on: { type: "mail", to: deploymentMailAddress },
          body,
        }),
      },
    });

    const config: HarnessConfig = {
      sessionId: SESSION_ID,
      agentId: `ins_${DEPLOYMENT_ID}`,
      tenantId: "tenant-1",
      principalId: "prin_on-trigger-signal-delivery-1",
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

    const deploySingleStepAtHead: DeploySingleStepFn = (params) =>
      env.hub.sessionService.deploySingleStepAtHead(params);

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
            message: `on-trigger signal-delivery test: write workflow repo ${args.workflowRepoId}`,
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

    // ---- event 0: fire the mail; the body parks on its author gate, and the
    // section proxies it up as a signal-relay await on the container ----
    await fireMailTrigger(env, deploymentMailAddress, {
      messageId: "<on-trigger-signal-delivery-1@integration.interchange>",
      content: "trigger the section",
    });

    await waitFor(
      async () => {
        const events = await readContainerEvents(env, workflowRunRepoId);
        return events.some(
          (e) =>
            e.type === "SignalAwaited" &&
            e.body["parkKind"] === "signal-relay" &&
            e.body["signalName"] === SIGNAL_NAME,
        );
      },
      { diagnostics: env.sidecarDiagnostics, timeoutMs: 30_000 },
    );

    const containerRunId = await findContainerRunId(env, workflowRunRepoId);
    if (containerRunId === undefined) {
      throw new Error("no container run under the workflow-run repo");
    }

    // The body is parked on its gate: section__0 spawned, not yet completed.
    const parked = await readWorkflowRunEvents(
      env,
      DEPLOYMENT_ID,
      containerRunId,
    );
    expect(
      parked.some(
        (e) =>
          e.type === "ChildSpawned" &&
          e.body["childRunId"] === `${SECTION_ID}__0`,
      ),
    ).toBe(true);
    expect(hasChildCompleted(parked, `${SECTION_ID}__0`)).toBe(false);

    // ---- deliver the author signal to the PARENT deployment run id ----
    // The body (a child run) never receives a signal directly; delivering to
    // the container run resolves the section's signal-relay await, and the
    // section relays it down into the body.
    await injectSignal(env, DEPLOYMENT_ID, containerRunId, SIGNAL_NAME, {
      go: true,
    });

    // The relay reached the body: its gate completed, section__0 completed, and
    // the section re-armed on its input park for the next event.
    await waitFor(
      async () => {
        const events = await readWorkflowRunEvents(
          env,
          DEPLOYMENT_ID,
          containerRunId,
        );
        return (
          hasChildCompleted(events, `${SECTION_ID}__0`) &&
          events.some(
            (e) => e.type === "SignalAwaited" && e.body["parkKind"] === "input",
          )
        );
      },
      { diagnostics: env.sidecarDiagnostics, timeoutMs: 30_000 },
    );

    const finalEvents = await readWorkflowRunEvents(
      env,
      DEPLOYMENT_ID,
      containerRunId,
    );
    const finalTypes = finalEvents.map((e) => e.type);

    // The container proxied the body's author gate up (signal-relay await) and
    // recorded the delivery to the PARENT run (SignalReceived on the same name).
    expect(
      finalEvents.some(
        (e) =>
          e.type === "SignalAwaited" &&
          e.body["parkKind"] === "signal-relay" &&
          e.body["signalName"] === SIGNAL_NAME,
      ),
    ).toBe(true);
    expect(
      finalEvents.some(
        (e) =>
          e.type === "SignalReceived" && e.body["signalName"] === SIGNAL_NAME,
      ),
    ).toBe(true);

    // The body completed only after the delivery (the relay reached the child).
    expect(hasChildCompleted(finalEvents, `${SECTION_ID}__0`)).toBe(true);

    // One long-lived run that never self-completes.
    expect(finalTypes.filter((t) => t === "RunStarted").length).toBe(1);
    expect(finalTypes).not.toContain("RunCompleted");
    expect(finalTypes).not.toContain("RunFailed");
    expect(finalTypes).not.toContain("RunCancelled");
  }, 120_000);
});

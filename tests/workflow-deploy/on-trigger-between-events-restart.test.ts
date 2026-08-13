// onTrigger between-events crash-recovery integration test (4c).
//
// The reachability gate for a long-lived onTrigger section that crashes
// while parked BETWEEN events -- idle on its input re-arm after one event's
// body completed and before the next event arrives. It proves the section
// survives a sidecar PROCESS death and services the next event, exercising
// the INPUT re-arm recovery path end-to-end on the real deploy stack.
//
// Shape: deploy a single-step workflow whose one step is an `onTrigger`
// section subscribed to the deployment mail address, with an echo-agent
// body. The deploy step materializes the inline body to its own workflow
// asset (`wf__section`) and rewrites the section to `{ ref }`; the runtime
// spawns each event's body as a child run by that ref.
//
//   1. Fire mail #1 -> the container run starts, spawns the body `section__0`
//      with the mail body as its input, the body echoes it and completes, and
//      the container re-arms on a snapshot-less `input` park -- parked between
//      events (no RunCompleted; a long-lived section never self-completes).
//   2. Quiesce, then KILL the sidecar subprocess (process death) while the
//      container is parked between events.
//   3. Start a fresh sidecar against the crashed process's SIDECAR_DATA_DIR.
//      Boot-time restore re-spawns the deployment; self-discovery re-includes
//      the CONTAINER run (it is the parent -- its log carries `ChildSpawned`
//      for `section__0`, so it is never itself a child) while EXCLUDING the
//      body child `section__0`. The restored container re-adopts its durable
//      input park (`planOnTriggerResume` -> reawait-input) and re-parks.
//   4. Fire mail #2 -> the supervisor's unified dispatch, finding the run
//      live but its input channel not yet re-armed, waits for the re-armed
//      input park and then delivers the mail as a `signal.deliver` (event 1,
//      NOT a spurious fresh trigger). The container spawns `section__1` with
//      mail #2's body, which echoes it and completes.
//
// Load-bearing assertions: exactly one `RunStarted`; `ChildSpawned` +
// `ChildCompleted` for BOTH `section__0` and `section__1`; the container
// never reaches a terminal event; and each event's body payload threaded --
// the echo inference records mail #1's body once and mail #2's body once, the
// second AFTER the restart, proving the between-events input re-arm delivered
// event 1's payload into a body re-spawned by the recovered section.
//
// Harness justification: SPAWN-REAL. Real hub, real sidecar subprocess, real
// workflow-process child driving `runOnTrigger` with the production
// suspendable-child seam, mock (echo) inference. The crash is a genuine kill
// of the sidecar subprocess; the restart is a fresh sidecar against the dead
// process's data dir, so recovery rides the production boot-time restore +
// self-discovery + reawait-input path. This is the first deploy-level test of
// a deployed onTrigger section.

import fs from "node:fs";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { createDefaultDirectorRegistry } from "@intx/agent";
import type { HarnessConfig } from "@intx/types/runtime";
import {
  defineWorkflow,
  onTrigger,
  sleep,
  type WorkflowDefinition,
} from "@intx/workflow";
import {
  createWorkflowDeployOrchestrator,
  deriveRunAddress,
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
  listRunIds,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  startSidecarSubprocess,
  waitFor,
  waitForReconnect,
  type DeployFlowEnv,
  type SidecarHandle,
} from "../hub-agent/lib/deploy-flow-env";
import { toLaunchDeployContent } from "./launch-session-bridge";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_on-trigger-between-events-restart-1";
const WORKFLOW_RUN_REF = "refs/heads/main";
const SECTION_ID = "section";

const FIRST_BODY = "First event body alpha-7391.";
const SECOND_BODY = "Second event body bravo-5520.";

let env: DeployFlowEnv;
let restartedSidecar: SidecarHandle | undefined;
const restartTempDirs: string[] = [];

beforeAll(async () => {
  env = await startDeployFlowEnv({ inferenceEchoUserMessage: true });
});

afterAll(async () => {
  if (restartedSidecar !== undefined) {
    restartedSidecar.proc.kill();
    await restartedSidecar.proc.exited;
  }
  await env.teardown();
  for (const dir of restartTempDirs.splice(0)) {
    await fs.promises.rm(dir, { recursive: true, force: true });
  }
});

/**
 * Settle the workflow-run pack-push pipeline (no drop). The crash below is
 * the drop (a process death), so only the quiescence guarantee is needed.
 */
async function settleWorkflowRunPacks(
  target: DeployFlowEnv,
  opts: { quietMs?: number; timeoutMs?: number } = {},
): Promise<void> {
  const quietMs = opts.quietMs ?? 500;
  const timeoutMs = opts.timeoutMs ?? 10_000;
  const start = Date.now();
  let lastCount = target.hub.workflowRunPackReceipts.count;
  let lastChange = Date.now();
  for (;;) {
    const current = target.hub.workflowRunPackReceipts.count;
    if (current !== lastCount) {
      lastCount = current;
      lastChange = Date.now();
    }
    if (Date.now() - lastChange >= quietMs) return;
    if (Date.now() - start > timeoutMs) {
      throw new Error(
        `settleWorkflowRunPacks: pack stream did not go quiet for ${String(quietMs)}ms within ${String(timeoutMs)}ms` +
          `\n${target.sidecarDiagnostics()}`,
      );
    }
    await new Promise((r) => setTimeout(r, 50));
  }
}

/**
 * The container run is the single run under the deployment's workflow-run
 * repo that is NOT a body child. Body children are `${SECTION_ID}__<n>`; the
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

describe("onTrigger section crash + restart -> restore re-includes the container, reawait-input services the next event", () => {
  test("sidecar registers with hub", () => {
    expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
  });

  test("a section parked between events survives a sidecar crash and services the next event", async () => {
    const deploymentMailAddress = deriveRunAddress({
      runId: DEPLOYMENT_ID,
      domain: DEPLOYMENT_DOMAIN,
    });

    // The section body: a single non-agent step that completes on its own so
    // the section re-arms for the next event. Body agent-step execution is not
    // wired yet (INTR-310), so the body exercises the runtime (a sleep timer)
    // rather than inference; the between-events recovery this test gates is a
    // property of the CONTAINER (its ChildSpawned/re-arm), not the body's work.
    const body: WorkflowDefinition = defineWorkflow({
      id: "authored-section-body",
      trigger: { type: "manual" },
      steps: { wait: sleep({ duration: 10 }) },
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
      agentId: `${DEPLOYMENT_ID}`,
      tenantId: "tenant-1",
      principalId: "prin_on-trigger-between-events-1",
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
        runId: orchestratorParams.instanceId,
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
            message: `on-trigger between-events test: write workflow repo ${args.workflowRepoId}`,
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

    // ---- event 0: fire mail #1, drive to the between-events input park ----
    await fireMailTrigger(env, deploymentMailAddress, {
      messageId: "<on-trigger-between-events-1@integration.interchange>",
      content: FIRST_BODY,
    });

    await waitFor(
      async () => {
        const events = await readContainerEvents(env, workflowRunRepoId);
        return (
          hasChildCompleted(events, `${SECTION_ID}__0`) &&
          events.some(
            (e) => e.type === "SignalAwaited" && e.body["parkKind"] === "input",
          )
        );
      },
      { diagnostics: env.sidecarDiagnostics, timeoutMs: 30_000 },
    );

    const containerRunId = await findContainerRunId(env, workflowRunRepoId);
    if (containerRunId === undefined) {
      throw new Error("no container run under the workflow-run repo");
    }

    // Parked between events: body 0 done, section re-armed on input, and the
    // long-lived container has NOT reached a terminal event.
    const parked = await readWorkflowRunEvents(
      env,
      DEPLOYMENT_ID,
      containerRunId,
    );
    const parkedTypes = parked.map((e) => e.type);
    expect(parkedTypes.filter((t) => t === "RunStarted").length).toBe(1);
    expect(
      parked.some(
        (e) =>
          e.type === "ChildSpawned" &&
          e.body["childRunId"] === `${SECTION_ID}__0`,
      ),
    ).toBe(true);
    expect(hasChildCompleted(parked, `${SECTION_ID}__0`)).toBe(true);
    expect(parkedTypes).not.toContain("RunCompleted");
    expect(parkedTypes).not.toContain("RunFailed");
    expect(parkedTypes).not.toContain("RunCancelled");

    // ---- quiesce, then CRASH the sidecar subprocess (process death) ----
    await settleWorkflowRunPacks(env);

    const crashedDataDir = env.sidecar.dataDir;
    env.sidecar.proc.kill();
    await env.sidecar.proc.exited;

    await waitFor(
      () =>
        !env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
      { timeoutMs: 10_000, diagnostics: env.sidecarDiagnostics },
    );

    // ---- RESTART: a fresh sidecar against the SAME data dir ----
    const hubPort = env.hub.server.port;
    if (hubPort === undefined) {
      throw new Error("hub.server.port is undefined after crash");
    }
    restartedSidecar = await startSidecarSubprocess({
      hubPort,
      registerTempDir: (dir) => {
        restartTempDirs.push(dir);
      },
      extraEnv: { SIDECAR_DATA_DIR: crashedDataDir },
    });
    const restoredDiagnostics = (): string =>
      `${env.sidecarDiagnostics()}\nrestored sidecar stderr:\n${restartedSidecar?.stderr.slice(-60).join("") ?? "<none>"}`;

    const reconnectMs = await waitForReconnect(env, deploymentMailAddress, {
      timeoutMs: 30_000,
    });
    expect(reconnectMs).toBeGreaterThan(0);
    expect(env.hub.router.getRoutableAddresses()).toContain(
      deploymentMailAddress,
    );

    // ---- event 1: fire mail #2, assert the recovered section services it ----
    // The restored container re-adopted its durable input park; the dispatch
    // loop waits for that re-arm and delivers mail #2 as event 1's input.
    //
    // Robustness: right after the restart the restored sidecar's hub link can
    // blip while its restore packs flush, so the event-1 mail can race a
    // transient drop ("Connection lost" on enqueue). Settle the restore pack
    // pipeline first, then fire -- retrying on the SAME messageId, which the run
    // dedups by signalId, so a retried delivery is idempotent and never spawns a
    // second event (the [section__0, section__1] assertion below stays exact).
    await settleWorkflowRunPacks(env, { timeoutMs: 30_000 });

    const secondMessageId =
      "<on-trigger-between-events-2@integration.interchange>";
    const deadline = Date.now() + 90_000;
    let serviced = false;
    while (Date.now() < deadline && !serviced) {
      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: secondMessageId,
        content: SECOND_BODY,
      }).catch(() => undefined);
      const pollDeadline = Date.now() + 12_000;
      while (Date.now() < pollDeadline) {
        const events = await readWorkflowRunEvents(
          env,
          DEPLOYMENT_ID,
          containerRunId,
        );
        if (hasChildCompleted(events, `${SECTION_ID}__1`)) {
          serviced = true;
          break;
        }
        await new Promise((r) => setTimeout(r, 200));
      }
    }
    if (!serviced) {
      throw new Error(
        `event 1 was not serviced after the restart within budget\n${restoredDiagnostics()}`,
      );
    }

    // ---- assert: one long-lived run serviced both events across the crash --
    const finalEvents = await readWorkflowRunEvents(
      env,
      DEPLOYMENT_ID,
      containerRunId,
    );
    const finalTypes = finalEvents.map((e) => e.type);

    // Exactly one RunStarted: the container is the SAME run before and after
    // the crash; the restart re-drives it, it does not start a second run.
    expect(finalTypes.filter((t) => t === "RunStarted").length).toBe(1);

    // Both events' bodies were spawned and completed, in order.
    const spawnedChildren = finalEvents.flatMap((e) =>
      e.type === "ChildSpawned" ? [e.body["childRunId"]] : [],
    );
    expect(spawnedChildren).toEqual([`${SECTION_ID}__0`, `${SECTION_ID}__1`]);
    expect(hasChildCompleted(finalEvents, `${SECTION_ID}__0`)).toBe(true);
    expect(hasChildCompleted(finalEvents, `${SECTION_ID}__1`)).toBe(true);

    // The long-lived section never self-completes.
    expect(finalTypes).not.toContain("RunCompleted");
    expect(finalTypes).not.toContain("RunFailed");
    expect(finalTypes).not.toContain("RunCancelled");

    // Load-bearing for the between-events recovery: section__1 exists only
    // because the restored container re-adopted its input park and the supervisor
    // routed mail #2 onto it as event 1 (not a spurious fresh trigger). Its
    // ChildSpawned lands AFTER the restart, so the crash-recovered section
    // serviced a genuinely new event.
    const secondSpawnIndex = finalEvents.findIndex(
      (e) =>
        e.type === "ChildSpawned" &&
        e.body["childRunId"] === `${SECTION_ID}__1`,
    );
    expect(secondSpawnIndex).toBeGreaterThanOrEqual(0);
  }, 180_000);
});

// Crash-at-awaitSignal survival integration test.
//
// Proves the acceptance requirement: a run parked at an `awaitSignal`
// gate survives a sidecar PROCESS crash. After crash + restart, the fresh
// process's boot-time restore re-spawns the deployment and re-seeds the
// parked run; delivering the awaited signal resumes it through to
// RunCompleted with effects applied exactly once.
//
// Shape: deploy a `step1 -> awaitSignal{name:"go"} -> step2` workflow
// through the workflow-deploy orchestrator's multi-step branch (the same
// wiring `multistep-signal.test.ts` uses), fire the mail trigger, and
// drive the run to the mid-run `SignalAwaited` pause. Quiesce the
// workflow-run pack pipeline, then KILL the sidecar subprocess (process
// death, not a hub-link drop). Start a fresh sidecar against the crashed
// process's SIDECAR_DATA_DIR: its `restoreWorkflowRuns()` re-spawns
// the deployment, whose workflow-process child re-seeds the parked run via
// `resumeFromEvents`. The runtime re-arms the awaiting-signal gate against
// the host-rehydrated signal channel (its `readState` reads the run's live
// reduced state). After reconnect, inject the signal; the run resumes
// through `step2` to `RunCompleted`.
//
// Effects are asserted exactly-once at the effect layer -- RunStarted,
// StepCompleted{step1}, StepCompleted{step2}, and RunCompleted each == 1,
// one runId -- NOT on the raw `SignalReceived` count: the reconnect/resume
// path may replay the delivery, and the state machine dedupes redeliveries
// by `signalId`, so a delivery-count assertion would be flaky by design.
//
// Harness justification: SPAWN-REAL. Real hub, real sidecar subprocess,
// real workflow-process child, mock inference. The crash is a genuine kill
// of the sidecar subprocess; the restart is a fresh sidecar against the
// dead process's SIDECAR_DATA_DIR, so survival rides the production
// boot-time restore path and the real reconnect ownership challenge.

import fs from "node:fs";

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
  injectSignal,
  listRunIds,
  readWorkflowRunEvents,
  settleWorkflowRunPacks,
  startDeployFlowEnv,
  startSidecarSubprocess,
  waitFor,
  waitForReconnect,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
  type SidecarHandle,
} from "../hub-agent/lib/deploy-flow-env";
import { signalGateEntry } from "./fixtures/signal-gate";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_crash-restart-resume-1";

// The definition's own tenant, the caller principal that creates the
// definition asset, and the `workflow`-kind asset the frozen definition
// projects over. The install/approve freeze and the anchor `workflow_run`
// insert both write against these, so they must exist in the real DB before
// the deploy runs.
const TENANT_ID = "tnt_crash_restart_resume";
const CALLER_PRINCIPAL_ID = "prn_crash_restart_resume";
const DEFINITION_ASSET_ID = "ast_crash_restart_resume_wf";

let env: DeployFlowEnv;
let h: TestDb;
let restartedSidecar: SidecarHandle | undefined;
const restartTempDirs: string[] = [];

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
    name: "crash-restart-resume-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (restartedSidecar !== undefined) {
    restartedSidecar.proc.kill();
    await restartedSidecar.proc.exited;
  }
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
  for (const dir of restartTempDirs.splice(0)) {
    await fs.promises.rm(dir, { recursive: true, force: true });
  }
});

async function readAllRunEvents(
  target: DeployFlowEnv,
  workflowRunRepoId: RepoId,
): Promise<{ runId: string; type: string; body: Record<string, unknown> }[]> {
  const runIds = await listRunIds(target, workflowRunRepoId);
  const out: { runId: string; type: string; body: Record<string, unknown> }[] =
    [];
  for (const runId of runIds) {
    const events = await readWorkflowRunEvents(target, DEPLOYMENT_ID, runId);
    for (const e of events) out.push({ runId, type: e.type, body: e.body });
  }
  return out;
}

describe.skipIf(!harnessDbEnvAvailable())(
  "sidecar crash + restart -> restore + reconnect resumes a run parked at awaitSignal",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("mid-run crash at awaitSignal, restore re-spawns, reconnect resumes to RunCompleted exactly once", async () => {
      const deploymentMailAddress = deriveRunAddress({
        runId: DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });

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
        principalId: "prin_crash-restart-1",
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

      const workflowRunRepoId = handle.workflowRunRepoId;

      // The source-ref frame round-trips through the real sidecar subprocess
      // (index the pack, check out the pinned subtree, register the address),
      // so routability is asynchronous. Wait for it before firing the trigger.
      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      // ---- fire the trigger, drive to the mid-run SignalAwaited pause ----
      const { messageId } = await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<crash-restart-resume-1@integration.interchange>",
      });

      await waitFor(
        async () => {
          const events = await readAllRunEvents(env, workflowRunRepoId);
          return events.some(
            (e) => e.type === "SignalAwaited" && e.body["signalName"] === "go",
          );
        },
        { diagnostics: env.sidecarDiagnostics, timeoutMs: 20_000 },
      );

      const runIdsAtPause = await listRunIds(env, workflowRunRepoId);
      const runId = runIdsAtPause[0];
      if (runId === undefined) {
        throw new Error(
          "no runs/ entry on the workflow-run repo at the SignalAwaited pause",
        );
      }

      const parked = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
      const parkedTypes = parked.map((e) => e.type);
      expect(parkedTypes).toContain("RunStarted");
      expect(
        parked.some(
          (e) => e.type === "StepCompleted" && e.body["stepId"] === "step1",
        ),
      ).toBe(true);
      expect(parkedTypes).toContain("SignalAwaited");
      expect(
        parked.some(
          (e) => e.type === "StepStarted" && e.body["stepId"] === "step2",
        ),
      ).toBe(false);
      expect(parkedTypes).not.toContain("RunCompleted");

      const runStartedBody = parked.find((e) => e.type === "RunStarted")?.body;
      if (runStartedBody === undefined) throw new Error("unreachable");
      expect(runStartedBody["consumedMessageId"]).toBe(messageId);

      // ---- quiesce, then CRASH the sidecar subprocess (process death) ----
      await settleWorkflowRunPacks(env);

      const crashedDataDir = env.sidecar.dataDir;
      env.sidecar.proc.kill();
      await env.sidecar.proc.exited;

      await waitFor(
        () =>
          !env.hub.router
            .getRoutableAddresses()
            .includes(deploymentMailAddress),
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

      // ---- wait for the restored deployment to re-establish the hub link ----
      const reconnectMs = await waitForReconnect(env, deploymentMailAddress, {
        timeoutMs: 30_000,
      });
      expect(reconnectMs).toBeGreaterThan(0);
      expect(env.hub.router.getRoutableAddresses()).toContain(
        deploymentMailAddress,
      );

      // ---- deliver the signal, assert resume to RunCompleted ----
      // The restored child re-armed the parked awaiting-signal gate against
      // the run's live reduced state; the injected signal now resolves the
      // re-armed awaiter and drives the run to completion.
      const injected = await injectSignal(env, DEPLOYMENT_ID, runId, "go", {
        resumed: true,
      });

      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        runId,
        { timeoutMs: 30_000, diagnostics: restoredDiagnostics },
      );
      expect(terminal.type).toBe("RunCompleted");

      // ---- exactly-once EFFECTS across the full (replay-inclusive) log ----
      const finalEvents = await readAllRunEvents(env, workflowRunRepoId);

      const distinctRunIds = new Set(finalEvents.map((e) => e.runId));
      expect(distinctRunIds).toEqual(new Set([runId]));

      const countType = (t: string): number =>
        finalEvents.filter((e) => e.type === t).length;
      const countStepCompleted = (stepId: string): number =>
        finalEvents.filter(
          (e) => e.type === "StepCompleted" && e.body["stepId"] === stepId,
        ).length;

      expect(countType("RunStarted")).toBe(1);
      expect(countStepCompleted("step1")).toBe(1);
      expect(countStepCompleted("step2")).toBe(1);
      expect(countType("RunCompleted")).toBe(1);

      const signalReceived = finalEvents.find(
        (e) => e.type === "SignalReceived" && e.body["signalName"] === "go",
      );
      if (signalReceived === undefined) {
        throw new Error(
          "no SignalReceived{go} effect after reconnect + resume",
        );
      }
      expect(signalReceived.body["signalId"]).toBe(injected.signalId);
      expect(signalReceived.body["payload"]).toEqual({ resumed: true });

      // Effect-layer exactly-once: each step's agent is invoked exactly once
      // across the crash + restart + resume. The mock inference server
      // records every request; each step agent carries a distinct system
      // prompt, so a recovery that re-ran a durably-completed step would show
      // a second invocation here even though the event log dedups the
      // duplicate. Match on the whole request JSON (the system prompt rides
      // in its own field the harness type does not surface).
      const invokedWithPrompt = (needle: string): number =>
        env.inference.requests.filter((r) => JSON.stringify(r).includes(needle))
          .length;
      expect(invokedWithPrompt("first step agent")).toBe(1);
      expect(invokedWithPrompt("second step agent")).toBe(1);
    }, 180_000);
  },
);

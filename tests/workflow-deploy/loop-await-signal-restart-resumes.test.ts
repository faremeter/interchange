// Deployed loop-body-awaitSignal crash-survival integration test.
//
// The deployed capstone for INTR-478: a loop whose BODY parks on an
// `awaitSignal` survives a sidecar PROCESS crash and resumes. A loop iteration
// runs through the suspendable-child seam, so the body's `awaitSignal` proxies
// up onto the loop container as a signal-relay await. After crash + restart,
// the fresh process's boot-time restore re-spawns the deployment and re-seeds
// the parked run; `runLoop` re-derives its cursor, re-establishes the
// container's signal relay (planLoopResume), and the injected signal resolves
// the parked iteration through to RunCompleted with effects applied once.
//
// This lifts the runtime-proven loop-suspend-resume behavior
// (packages/workflow/src/runtime/loop-suspend-resume.test.ts, an in-memory
// crash model) onto the real sidecar-kill/restore/reconnect path already proven
// for a top-level awaitSignal (crash-restart-reconnect-resumes.test.ts).
//
// Harness justification: SPAWN-REAL. Real hub, real sidecar subprocess, real
// workflow-process child, mock inference. The crash is a genuine kill of the
// sidecar subprocess; the restart is a fresh sidecar against the dead process's
// SIDECAR_DATA_DIR, so survival rides the production boot-time restore path.

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
import { loopAwaitSignalEntry } from "./fixtures/loop-await-signal-workflow";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_loop-await-signal-restart-1";

const TENANT_ID = "tnt_loop_await_signal_restart";
const CALLER_PRINCIPAL_ID = "prn_loop_await_signal_restart";
const DEFINITION_ASSET_ID = "ast_loop_await_signal_restart_wf";

let env: DeployFlowEnv;
let h: TestDb;
let restartedSidecar: SidecarHandle | undefined;
const restartTempDirs: string[] = [];

beforeAll(async () => {
  // A file-scope beforeAll fires even when describe.skipIf skips the
  // suite bodies, so it needs its own guard or a missing DB env throws
  // here. See the two-shape rule in tests/lib/db-harness.ts.
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
    name: "loop-await-signal-restart-wf",
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
  "sidecar crash + restart resumes a loop iteration parked at a body awaitSignal",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("loop body parks at awaitSignal, crashes, restore re-spawns, reconnect resumes to RunCompleted exactly once", async () => {
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
        principalId: "prin_loop-await-signal-1",
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

      const entryModule = loopAwaitSignalEntry({
        address: deploymentMailAddress,
        signalName: "go",
        workflowId: `wf_${DEPLOYMENT_ID}`,
      });

      // Omit `sources`: the deploy computes per-step pins via
      // buildInertProjectionStepSources, which recurses into the loop body and
      // pins the awaitSignal body step against the approved default placeholder.
      const handle = await deployWorkflowSourceForTest(env, {
        entryModule,
        loops: "./workflow.mjs",
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: deploymentMailAddress,
        approvals: operatorApprovals,
        config,
      });
      expect(handle.publicKey).toBeTruthy();

      const workflowRunRepoId = handle.workflowRunRepoId;

      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      // ---- fire the trigger, drive to the loop body's awaitSignal pause ----
      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<loop-await-signal-restart-1@integration.interchange>",
      });

      // A loop spawns child runs `<loopId>__<index>`; the top-level deployment
      // run (the one whose CONTAINER relay awaits the signal) is the one without
      // the `__` child-run marker. Wait for that run to appear, then for ITS
      // signal-relay park to be durable -- keying the wait on the child's
      // SignalAwaited instead would race the container-relay assertion below.
      const topLevelRunId = async (): Promise<string | undefined> =>
        (await listRunIds(env, workflowRunRepoId)).find(
          (id) => !id.includes("__"),
        );
      await waitFor(async () => (await topLevelRunId()) !== undefined, {
        diagnostics: env.sidecarDiagnostics,
        timeoutMs: 20_000,
      });
      const runId = await topLevelRunId();
      if (runId === undefined) {
        throw new Error("no top-level run after the trigger fired");
      }
      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
          return events.some(
            (e) => e.type === "SignalAwaited" && e.body["signalName"] === "go",
          );
        },
        { diagnostics: env.sidecarDiagnostics, timeoutMs: 20_000 },
      );

      const parked = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
      const parkedTypes = parked.map((e) => e.type);
      expect(parkedTypes).toContain("RunStarted");
      // The loop container proxied the body's awaitSignal up as a signal-relay
      // await on the author name; the iteration is parked, the run is not done.
      expect(
        parked.some(
          (e) => e.type === "SignalAwaited" && e.body["signalName"] === "go",
        ),
      ).toBe(true);
      expect(
        parked.some(
          (e) => e.type === "StepStarted" && e.body["stepId"] === "settle",
        ),
      ).toBe(false);
      expect(parkedTypes).not.toContain("RunCompleted");

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

      const reconnectMs = await waitForReconnect(env, deploymentMailAddress, {
        timeoutMs: 30_000,
      });
      expect(reconnectMs).toBeGreaterThan(0);
      expect(env.hub.router.getRoutableAddresses()).toContain(
        deploymentMailAddress,
      );

      // ---- deliver the signal, assert resume to RunCompleted ----
      // The restored child re-derived the loop cursor and re-established the
      // container's signal relay against the run's live reduced state; the
      // injected signal resolves the parked iteration and drives the loop to
      // convergence (settle runs, escalate pruned).
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

      const countStepCompleted = (runId2: string, stepId: string): number =>
        finalEvents.filter(
          (e) =>
            e.runId === runId2 &&
            e.type === "StepCompleted" &&
            e.body["stepId"] === stepId,
        ).length;

      // The top-level run started once and completed once; the loop converged so
      // its normal dependent `settle` ran once and `escalate` was pruned.
      expect(
        finalEvents.filter((e) => e.runId === runId && e.type === "RunStarted")
          .length,
      ).toBe(1);
      expect(
        finalEvents.filter(
          (e) => e.runId === runId && e.type === "RunCompleted",
        ).length,
      ).toBe(1);
      expect(countStepCompleted(runId, "settle")).toBe(1);

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

      // Effect-layer exactly-once: the convergence dependent's agent is invoked
      // exactly once across crash + restart + resume; the pruned escalate agent
      // never runs.
      const invokedWithPrompt = (needle: string): number =>
        env.inference.requests.filter((r) => JSON.stringify(r).includes(needle))
          .length;
      expect(invokedWithPrompt("settle-agent agent")).toBe(1);
      expect(invokedWithPrompt("escalate-agent agent")).toBe(0);
    }, 180_000);
  },
);

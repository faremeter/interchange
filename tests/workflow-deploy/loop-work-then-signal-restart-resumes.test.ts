// Deployed regression guard for the WRITE-ONCE loop-iteration grants rule, made
// observable by a PRE-PARK effect.
//
// A loop iteration writes its own capped `runs/<iterationRunId>/grants.json`
// once, at birth, before the body runs. That write commits to the shared
// workflow-run ref through a writer separate from the runtime's event log, so a
// repeat on the resume re-drive would race the replay re-appends (a single-
// writer seq violation) or clobber the iteration's committed `events/` and re-run
// the body. Write-once prevents the repeat: on resume the existing grants file
// is read back, the durable child log replays intact, and the body does not
// re-execute. The existing bare-awaitSignal restart test
// (loop-await-signal-restart-resumes) cannot observe a regression here -- its
// body does no work before parking, so a from-scratch re-spawn is
// indistinguishable from a replay.
//
// This fixture's body runs an observable agent step (`work`) and THEN parks on
// `awaitSignal`. After crash + restart + resume the test asserts `work` ran
// EXACTLY ONCE -- at the log layer (one `StepCompleted{work}` on the iteration
// run `rework__0`) and the effect layer (one inference invocation of the work
// agent). Reintroducing a resume-time grants re-write would either fail the run
// (seq conflict) or re-run `work`, driving a count to two.
//
// Harness justification: SPAWN-REAL. Real hub, real sidecar subprocess, real
// workflow-process child, mock inference. The crash is a genuine kill of the
// sidecar subprocess; the restart is a fresh sidecar against the dead process's
// SIDECAR_DATA_DIR, so survival rides the production boot-time restore path.

import fs from "node:fs";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import { deriveRunAddress, type ApprovalSet } from "@intx/workflow-deploy";
import { loopBodyRunId } from "@intx/workflow";
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
import { loopWorkThenSignalEntry } from "./fixtures/loop-work-then-signal-workflow";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_loop-work-then-signal-1";
const LOOP_STEP_ID = "rework";
const WORK_STEP_ID = "work";
// The loop iteration body child run for iteration 0. The runtime keys it on
// `<runId>__<loopStepId>__<index>` (`loopBodyRunId`); the run's own id is
// `DEPLOYMENT_ID`.
const BODY_CHILD_RUN_ID = loopBodyRunId(DEPLOYMENT_ID, LOOP_STEP_ID, 0);

const TENANT_ID = "tnt_loop_work_then_signal";
const CALLER_PRINCIPAL_ID = "prn_loop_work_then_signal";
const DEFINITION_ASSET_ID = "ast_loop_work_then_signal_wf";

let env: DeployFlowEnv;
let h: TestDb;
let restartedSidecar: SidecarHandle | undefined;
const restartTempDirs: string[] = [];

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
    name: "loop-work-then-signal-wf",
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

describe.skipIf(!harnessDbEnvAvailable())(
  "sidecar crash + restart preserves a loop iteration's pre-park work exactly once",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("loop body runs work, parks, crashes, resumes with work applied exactly once", async () => {
      const deploymentMailAddress = deriveRunAddress({
        runId: DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });

      const inferenceSource: InferenceSource = {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${String(env.inference.server.port)}`,
        apiKey: "sk-mock",
        model: "mock-model",
      };

      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentId: `${DEPLOYMENT_ID}`,
        tenantId: "tenant-1",
        principalId: "prin_loop-work-then-signal-1",
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

      const entryModule = loopWorkThenSignalEntry({
        address: deploymentMailAddress,
        signalName: "go",
        workflowId: `wf_${DEPLOYMENT_ID}`,
      });

      // Omit `sources`: the deploy computes per-step pins via
      // buildInertProjectionStepSources, which recurses into the loop body and
      // pins the work + awaitSignal body steps against the approved placeholder.
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
        messageId: "<loop-work-then-signal-restart-1@integration.interchange>",
      });

      // A loop spawns child runs `<loopId>__<index>`; the top-level deployment
      // run (the one whose CONTAINER relay awaits the signal) is the one without
      // the `__` child-run marker. Wait for that run to appear, then for ITS
      // signal-relay park to be durable.
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

      // The observable pre-park effect: the iteration ran `work` to completion in
      // its own child run BEFORE parking on the signal.
      const parkedBody = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        BODY_CHILD_RUN_ID,
      );
      expect(
        parkedBody.some(
          (e) =>
            e.type === "StepCompleted" && e.body["stepId"] === WORK_STEP_ID,
        ),
      ).toBe(true);
      expect(parkedBody.map((e) => e.type)).not.toContain("RunCompleted");

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
      // Convergence is itself the signal-delivery proof: the loop cannot reach
      // RunCompleted unless the injected signal resolved the parked iteration.
      await injectSignal(env, DEPLOYMENT_ID, runId, "go", {
        resumed: true,
      });

      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        runId,
        { timeoutMs: 30_000, diagnostics: restoredDiagnostics },
      );
      expect(terminal.type).toBe("RunCompleted");

      // ---- exactly-once: the pre-park `work` step survived the resume ----
      // Write-once leaves the iteration run's durable log untouched on resume, so
      // `work`'s StepCompleted survives and replays rather than re-running. A
      // single occurrence proves the resume neither re-wrote the grants file
      // (which would clobber the nested events subtree or fail the run on a seq
      // conflict) nor re-executed the step from scratch.
      const finalBody = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        BODY_CHILD_RUN_ID,
      );
      expect(
        finalBody.filter(
          (e) =>
            e.type === "StepCompleted" && e.body["stepId"] === WORK_STEP_ID,
        ).length,
      ).toBe(1);

      // Effect-layer exactly-once: the pre-park work agent is invoked exactly
      // once across crash + restart + resume, the convergence dependent once,
      // and the pruned escalate agent never.
      const invokedWithPrompt = (needle: string): number =>
        env.inference.requests.filter((r) => JSON.stringify(r).includes(needle))
          .length;
      expect(invokedWithPrompt("work-body-agent agent")).toBe(1);
      expect(invokedWithPrompt("settle-agent agent")).toBe(1);
      expect(invokedWithPrompt("escalate-agent agent")).toBe(0);
    }, 180_000);
  },
);

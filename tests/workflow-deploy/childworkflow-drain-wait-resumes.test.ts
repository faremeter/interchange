// Deployed drain survival for a parked childWorkflow.
//
// A childWorkflow stays unaddressable. Its author gate is relayed onto the
// deployment run, which is the one addressable run. The fixture omits
// `drainBehavior`, so the spawn step stores the constructor default
// `"wait"`. Drain consults that step, not the child's own `drainBehavior`.
// A drain signal therefore does not abort the spawn step. This test sends
// drain, confirms the parent log stays parked, then kills the sidecar and
// starts a replacement against the same data directory. The replacement
// reattaches the same child and the injected signal resolves the relay.
//
// This is not a redeploy. Redeploy rejects an address that is already
// deployed. The production path that drains and then replaces the process
// is `triggerRecycle`: it sends drain and then kills the child, and that
// drain call returns after the frame is sent. `deadlineMs` on the frame is
// log text, not the clock that decides whether the step aborts. `"wait"`
// stops the local abort. The supervisor still commits `CancelRequested`
// when `drainTimeoutMs` expires (default 60 seconds) for a cohort run that
// is not an input channel. The 2.5s window below does not prove that
// timeout path. A drain frame that never arrives also leaves the run
// parked, and this assertion cannot see that. The loop drain test
// (`loop-await-signal-drain-roundtrip.test.ts`) is the positive proof that
// this wire delivers drain. The kill uses `terminateSidecarSubprocess`.
// The sidecar has no graceful shutdown on that signal, so the kill does
// not start a second drain.
//
// Harness justification: SPAWN-REAL. Real hub, real sidecar subprocess, real
// workflow-process child, mock inference.

import fs from "node:fs";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import {
  createApprovalSet,
  deriveRunAddress,
  type ApprovalSet,
} from "@intx/workflow-deploy";
import { tenant as tenantTable } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedPrincipal,
  tenantSlugFromId,
} from "@intx/test-harness/seed";
import type { RepoId } from "@intx/hub-sessions";

import {
  SESSION_ID,
  SIDECAR_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  initiateDrain,
  injectSignal,
  listRunIds,
  readWorkflowRunEvents,
  settleWorkflowRunPacks,
  startDeployFlowEnv,
  startSidecarSubprocess,
  terminateSidecarSubprocess,
  waitFor,
  waitForReconnect,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
  type SidecarHandle,
} from "../hub-agent/lib/deploy-flow-env";
import { childWorkflowUnanswerableGateEntry } from "./fixtures/childworkflow-unanswerable-gate";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_childworkflow-drain-wait-1";
const SIGNAL_NAME = "go";

const TENANT_ID = "tnt_childworkflow_drain_wait";
const CALLER_PRINCIPAL_ID = "prn_childworkflow_drain_wait";
const DEFINITION_ASSET_ID = "ast_childworkflow_drain_wait_wf";

let env: DeployFlowEnv;
let h: TestDb;
let replacementSidecar: SidecarHandle | undefined;
const replacementTempDirs: string[] = [];

beforeAll(async () => {
  // A file-scope beforeAll fires even when describe.skipIf skips the
  // suite bodies, so it needs its own guard or a missing DB env throws
  // here. See the two-shape rule in tests/lib/db-harness.ts.
  if (!harnessDbEnvAvailable()) return;
  h = await createTestDb();
  await h.db.insert(tenantTable).values({
    id: TENANT_ID,
    name: TENANT_ID,
    slug: tenantSlugFromId(TENANT_ID),
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
    name: "childworkflow-drain-wait-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (replacementSidecar !== undefined) {
    await terminateSidecarSubprocess(replacementSidecar);
  }
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
  for (const dir of replacementTempDirs.splice(0)) {
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
    for (const event of events) {
      out.push({ runId, type: event.type, body: event.body });
    }
  }
  return out;
}

describe.skipIf(!harnessDbEnvAvailable())(
  "drain leaves a parked childWorkflow spawn step running, and a replacement process resolves it",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("a drain signal does not abort the spawn step, and the restored relay completes", async () => {
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
        agentId: DEPLOYMENT_ID,
        tenantId: "tenant-1",
        principalId: "prin_childworkflow-drain-wait-1",
        agentAddress: deploymentMailAddress,
        systemPrompt: "Fallback prompt (overridden per step by the definition)",
        tools: [],
        grants: [],
        sources: [inferenceSource],
        defaultSource: "anthropic:mock-model",
      };

      const operatorApprovals: ApprovalSet = createApprovalSet([
        "inference.source:anthropic:mock-model",
        "director:@intx/agent/default",
        `mail.address:${deploymentMailAddress}`,
        `mail.send:${DEPLOYMENT_DOMAIN}`,
      ]);

      const handle = await deployWorkflowSourceForTest(env, {
        entryModule: childWorkflowUnanswerableGateEntry({
          address: deploymentMailAddress,
          workflowId: `wf_${DEPLOYMENT_ID}`,
          childWorkflowId: `wf_child_${DEPLOYMENT_ID}`,
          signalName: SIGNAL_NAME,
        }),
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
        { diagnostics: env.sidecarDiagnostics },
      );

      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<childworkflow-drain-wait-1@integration.interchange>",
      });

      // The child run id is a random `run-...` and carries no `__` marker, so
      // the parent is the run whose relay await sits on the spawn step. The
      // child awaits its own step, `hold`.
      const parentRunId = async (): Promise<string | undefined> => {
        const ids = await listRunIds(env, workflowRunRepoId);
        for (const id of ids) {
          const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, id);
          if (
            events.some(
              (event) =>
                event.type === "SignalAwaited" &&
                event.body["stepId"] === "spawn" &&
                event.body["signalName"] === SIGNAL_NAME,
            )
          ) {
            return id;
          }
        }
        return undefined;
      };
      await waitFor(async () => (await parentRunId()) !== undefined, {
        diagnostics: env.sidecarDiagnostics,
      });
      const runId = await parentRunId();
      if (runId === undefined) {
        throw new Error("no parent run parked on the spawn step");
      }

      // Snapshot the parent log immediately before drain so the window
      // below distinguishes wait-mode (the park stays) from cancel-mode
      // (StepFailed lands within milliseconds of the drain signal).
      const eventsBeforeDrain = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        runId,
      );
      const typesBefore = eventsBeforeDrain.map((event) => event.type);
      expect(typesBefore).toContain("RunStarted");
      expect(typesBefore).toContain("ChildSpawned");
      expect(typesBefore).toContain("SignalAwaited");
      expect(typesBefore).not.toContain("StepFailed");

      // The helper's default `deadlineMs` is frame text. It is not the
      // policy that aborts the step, so this call does not pass one.
      initiateDrain(env, DEPLOYMENT_ID);

      // `"wait"` ignores the drain signal. Cancel-mode aborts within
      // hundreds of milliseconds; 2_500ms is inside the supervisor's
      // 60s `drainTimeoutMs` and far enough past a local abort that a
      // cancel-mode regression fails here. A regression between 2.5s
      // and 60s would slip past, and so would a drain frame that never
      // arrived: both leave this log parked. The loop drain test is the
      // proof that the same `initiateDrain` wire delivers drain.
      //
      // The sleep cannot become a state-based wait: the property is the
      // ABSENCE of StepFailed, and a predicate for "still absent" holds
      // the instant it is first checked.
      await new Promise((r) => setTimeout(r, 2_500));
      const eventsDuringWait = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        runId,
      );
      const typesDuring = eventsDuringWait.map((event) => event.type);
      expect(typesDuring).not.toContain("StepFailed");
      expect(typesDuring).not.toContain("RunFailed");
      expect(typesDuring).not.toContain("RunCompleted");
      expect(typesDuring).not.toContain("RunCancelled");
      expect(
        eventsDuringWait.some(
          (event) =>
            event.type === "SignalAwaited" &&
            event.body["stepId"] === "spawn" &&
            event.body["signalName"] === SIGNAL_NAME,
        ),
      ).toBe(true);

      await settleWorkflowRunPacks(env);

      const drainedDataDir = env.sidecar.dataDir;
      await terminateSidecarSubprocess(env.sidecar);

      await waitFor(
        () =>
          !env.hub.router
            .getRoutableAddresses()
            .includes(deploymentMailAddress),
        { diagnostics: env.sidecarDiagnostics },
      );

      const hubPort = env.hub.server.port;
      if (hubPort === undefined) {
        throw new Error("hub.server.port is undefined after the kill");
      }
      replacementSidecar = await startSidecarSubprocess({
        hubPort,
        registerTempDir: (dir) => {
          replacementTempDirs.push(dir);
        },
        extraEnv: { SIDECAR_DATA_DIR: drainedDataDir },
      });
      env.registerSidecar(replacementSidecar);

      await waitForReconnect(env, deploymentMailAddress);
      expect(env.hub.router.getRoutableAddresses()).toContain(
        deploymentMailAddress,
      );

      const injected = await injectSignal(
        env,
        DEPLOYMENT_ID,
        runId,
        SIGNAL_NAME,
        {
          resumed: true,
        },
      );

      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        runId,
        { diagnostics: env.sidecarDiagnostics },
      );
      expect(terminal.type).toBe("RunCompleted");

      const finalEvents = await readAllRunEvents(env, workflowRunRepoId);
      const onParent = (type: string): number =>
        finalEvents.filter(
          (event) => event.runId === runId && event.type === type,
        ).length;
      expect(onParent("RunStarted")).toBe(1);
      expect(onParent("RunCompleted")).toBe(1);
      expect(onParent("ChildSpawned")).toBe(1);

      const signalReceived = finalEvents.find(
        (event) =>
          event.runId === runId &&
          event.type === "SignalReceived" &&
          event.body["signalName"] === SIGNAL_NAME,
      );
      if (signalReceived === undefined) {
        throw new Error("no SignalReceived on the parent after resume");
      }
      expect(signalReceived.body["signalId"]).toBe(injected.signalId);
      expect(signalReceived.body["payload"]).toEqual({ resumed: true });
    }, 180_000);
  },
);

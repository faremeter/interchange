// Deployed drain-shed for a PARKED LOOP ITERATION.
//
// The runtime layer already pins the shed
// (packages/workflow/src/runtime/loop-suspend-resume.test.ts, "a parked
// iteration sheds on drain under the loop's default cancel behavior" -> the
// container's cancel-mode step aborts and the run settles failed). What that
// layer explicitly does NOT exercise is the supervisor's drain landing on a
// parked loop container through the real wire pipeline. This test pins that:
// hub `sendDrain` -> sidecar hub-link -> supervisor `drain` -> workflow-process
// child `DrainController`, with the drain landing on a loop step parked as
// `awaiting-signal` (the body's awaitSignal proxied up onto the container).
//
// A loop whose body parks on an `awaitSignal` is deployed by source-ref against
// the real hub + sidecar subprocess + mock inference. Firing the trigger drives
// iteration 0 to its park (the container relay awaits the author signal); no
// signal is ever delivered. Initiating drain flips the child's DrainController;
// the loop container defaults to `drainBehavior: "cancel"`, so the parked step
// aborts, commits StepFailed, and the run terminates RunFailed. The default is
// the loop's, not the body awaitSignal's -- a top-level awaitSignal author would
// have to opt into cancel, but a loop sheds by default.
//
// Regression guard for the loop iteration's LOCAL teardown: the container abort
// tears the in-process iteration down through its own cancel controller (no
// durable supervisor-signed CancelRequested, which its workflow-process
// principal -- writing through the run proxy -- cannot sign), so the iteration
// fails locally and the container unblocks and lands StepFailed{rework} ->
// RunFailed. Self-writing a supervisor CancelRequested instead would have the
// proxy author it as workflow-process, the kind handler reject it, the child
// wedge un-settled, and the container HANG here -- the exact defect this caught.
//
// Harness justification: SPAWN-REAL. Mirrors drain-roundtrip.test.ts (top-level
// cancel-mode awaitSignal) with a loop container instead.

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
  initiateDrain,
  listRunIds,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { loopAwaitSignalEntry } from "./fixtures/loop-await-signal-workflow";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_loop-drain-cancel-1";
const LOOP_STEP_ID = "rework";
const DRAIN_DEADLINE_MS = 1_000;

const TENANT_ID = "tnt_loop_drain_cancel";
const CALLER_PRINCIPAL_ID = "prn_loop_drain_cancel";
const DEFINITION_ASSET_ID = "ast_loop_drain_cancel_wf";

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
    name: "loop-drain-cancel-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

// The top-level container run (the one whose relay awaits the signal) is the run
// id without the `<loopId>__<index>` child-run marker.
async function topLevelRunId(
  workflowRunRepoId: RepoId,
): Promise<string | undefined> {
  return (await listRunIds(env, workflowRunRepoId)).find(
    (id) => !id.includes("__"),
  );
}

describe.skipIf(!harnessDbEnvAvailable())(
  "drain sheds a parked loop iteration",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("drain on a parked loop iteration aborts the container and surfaces RunFailed", async () => {
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
        principalId: "prin_loop-drain-cancel-1",
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
      // buildInertProjectionStepSources, which recurses into the loop body.
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

      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<loop-drain-cancel-1@integration.interchange>",
      });

      // Drive iteration 0 to its park: the container relay awaits the author
      // signal on the top-level run.
      await waitFor(
        async () => (await topLevelRunId(workflowRunRepoId)) !== undefined,
        {
          diagnostics: env.sidecarDiagnostics,
          timeoutMs: 20_000,
        },
      );
      const runId = await topLevelRunId(workflowRunRepoId);
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

      // The run is parked, not failed, before the drain.
      const parked = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
      const parkedTypes = parked.map((e) => e.type);
      expect(parkedTypes).toContain("SignalAwaited");
      expect(parkedTypes).not.toContain("StepFailed");
      expect(parkedTypes).not.toContain("RunFailed");

      // Ship the drain through the production wire pipeline. No signal is ever
      // delivered, so the loop's default cancel behavior is what settles the run.
      initiateDrain(env, DEPLOYMENT_ID, { deadlineMs: DRAIN_DEADLINE_MS });

      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        runId,
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );
      expect(terminal.type).toBe("RunFailed");

      // The cancel-mode shed aborts the loop container step and commits its
      // StepFailed before the terminal RunFailed.
      const finalEvents = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        runId,
      );
      const types = finalEvents.map((e) => e.type);
      const loopFailedIdx = types.findIndex(
        (t, i) =>
          t === "StepFailed" && finalEvents[i]?.body["stepId"] === LOOP_STEP_ID,
      );
      const runFailedIdx = types.indexOf("RunFailed");
      expect(loopFailedIdx).toBeGreaterThanOrEqual(0);
      expect(runFailedIdx).toBeGreaterThan(loopFailedIdx);
      expect(types).not.toContain("RunCompleted");
    }, 30_000);
  },
);

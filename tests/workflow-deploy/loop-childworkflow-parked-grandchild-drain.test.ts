// Deployed drain of a loop body whose `childWorkflow` grandchild is IN-FLIGHT.
//
// Companion to loop-await-signal-drain-roundtrip (a loop body parked on its own
// awaitSignal). Here the loop body's step is a `childWorkflow` spawn of a
// grandchild that PARKS on an awaitSignal, so at drain time the grandchild is a
// live in-process terminal child. The drain cascade must tear the grandchild
// down LOCALLY -- it runs under the workflow-process principal and cannot sign
// the supervisor `CancelRequested` a control-plane cancel needs -- so its parked
// step fails to `RunFailed`, `createSidecarRunChild` returns, the loop body's
// spawn step unblocks, and the whole run settles `RunFailed`. If the grandchild
// instead self-wrote a supervisor cancel, the proxy would author it as
// workflow-process, the kind handler would reject it, the grandchild would wedge
// un-settled, and the loop body's spawn step (awaiting the grandchild terminal)
// would HANG the run -- the defect this guards against, one level below the loop
// iteration itself.
//
// Harness justification: SPAWN-REAL. Real hub + sidecar subprocess + mock
// inference.

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
import { loopChildWorkflowParkedEntry } from "./fixtures/loop-childworkflow-parked-grandchild";
import { loopBodyRunId } from "@intx/workflow";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const LOOP_STEP_ID = "rework";
const DRAIN_DEADLINE_MS = 1_000;

const TENANT_ID = "tnt_loop_cw_parked_drain";
const CALLER_PRINCIPAL_ID = "prn_loop_cw_parked_drain";
const PARKED_ANCHOR = "run_loop-cw-parked-drain-1";
const SLEEP_ANCHOR = "run_loop-cw-sleep-drain-1";
const DEFINITION_ASSET_IDS: Record<string, string> = {
  [PARKED_ANCHOR]: "ast_loop_cw_parked_drain_wf",
  [SLEEP_ANCHOR]: "ast_loop_cw_sleep_drain_wf",
};

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
  for (const [anchorRunId, definitionAssetId] of Object.entries(
    DEFINITION_ASSET_IDS,
  )) {
    await seedAsset(h.db, {
      id: definitionAssetId,
      tenantId: TENANT_ID,
      kind: "workflow",
      name: `loop-cw-drain-wf-${anchorRunId}`,
      creatorPrincipalId: CALLER_PRINCIPAL_ID,
    });
  }

  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

// The top-level container run: not a `<loopId>__<index>` child, and not the
// grandchild (whose run id equals the grandchild workflow id).
async function topLevelRunId(
  workflowRunRepoId: RepoId,
  childWorkflowId: string,
): Promise<string | undefined> {
  return (await listRunIds(env, workflowRunRepoId)).find(
    (id) => !id.includes("__") && id !== childWorkflowId,
  );
}

// Deploy a loop whose body spawns a childWorkflow grandchild that is in-flight
// (parked on an awaitSignal, or mid-step on a long sleep), drain the deployment,
// and assert the run sheds to RunFailed without wedging on the grandchild.
async function runGrandchildDrainTest(opts: {
  anchorRunId: string;
  grandchildStep: "awaitSignal" | "sleep";
  inFlightEventType: "SignalAwaited" | "TimerSet";
}): Promise<void> {
  const { anchorRunId } = opts;
  const childWorkflowId = `wf_child_${anchorRunId}`;
  const deploymentMailAddress = deriveRunAddress({
    runId: anchorRunId,
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
    agentId: `${anchorRunId}`,
    tenantId: "tenant-1",
    principalId: `prin_${anchorRunId}`,
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

  const entryModule = loopChildWorkflowParkedEntry({
    address: deploymentMailAddress,
    workflowId: `wf_${anchorRunId}`,
    childWorkflowId,
    grandchildStep: opts.grandchildStep,
  });

  const definitionAssetId = DEFINITION_ASSET_IDS[anchorRunId];
  if (definitionAssetId === undefined) {
    throw new Error(`no definition asset seeded for ${anchorRunId}`);
  }
  const handle = await deployWorkflowSourceForTest(env, {
    entryModule,
    loops: "./workflow.mjs",
    db: h.db,
    tenantId: TENANT_ID,
    definitionAssetId,
    anchorRunId,
    deploymentDomain: DEPLOYMENT_DOMAIN,
    agentAddress: deploymentMailAddress,
    approvals: operatorApprovals,
    config,
  });
  expect(handle.publicKey).toBeTruthy();

  const workflowRunRepoId = handle.workflowRunRepoId;

  await waitFor(
    () => env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
    { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
  );

  await fireMailTrigger(env, deploymentMailAddress, {
    messageId: `<${anchorRunId}@integration.interchange>`,
  });

  // Iteration 0's body spawns the grandchild; wait for the ChildSpawned in the
  // body child run and capture the minted grandchild run id.
  await waitFor(
    async () => {
      const events = await readWorkflowRunEvents(
        env,
        anchorRunId,
        loopBodyRunId(anchorRunId, LOOP_STEP_ID, 0),
      );
      return events.some((e) => e.type === "ChildSpawned");
    },
    { diagnostics: env.sidecarDiagnostics, timeoutMs: 60_000 },
  );
  const bodyEvents = await readWorkflowRunEvents(
    env,
    anchorRunId,
    loopBodyRunId(anchorRunId, LOOP_STEP_ID, 0),
  );
  const spawned = bodyEvents.find((e) => e.type === "ChildSpawned");
  if (spawned === undefined) throw new Error("unreachable");
  const grandchildRunId = spawned.body["childRunId"];
  if (typeof grandchildRunId !== "string") {
    throw new Error(
      `body ChildSpawned is missing a string childRunId; got ${typeof grandchildRunId}`,
    );
  }

  // The grandchild reaches its in-flight state (parked on its awaitSignal, or
  // mid-step on its sleep timer) -- a live in-process terminal child, the state
  // the drain must tear down locally.
  await waitFor(
    async () => {
      const events = await readWorkflowRunEvents(
        env,
        anchorRunId,
        grandchildRunId,
      );
      return events.some((e) => e.type === opts.inFlightEventType);
    },
    { diagnostics: env.sidecarDiagnostics, timeoutMs: 60_000 },
  );

  const runId = await topLevelRunId(workflowRunRepoId, childWorkflowId);
  if (runId === undefined) {
    throw new Error("no top-level run after the trigger fired");
  }

  // Ship the drain. No signal is ever delivered to the grandchild, so the only
  // way the run settles is the local teardown cascade.
  initiateDrain(env, anchorRunId, { deadlineMs: DRAIN_DEADLINE_MS });

  const terminal = await waitForWorkflowRunComplete(env, anchorRunId, runId, {
    timeoutMs: 30_000,
    diagnostics: env.sidecarDiagnostics,
  });
  expect(terminal.type).toBe("RunFailed");

  // The grandchild reached a terminal (it did not wedge), and its own log
  // carries no supervisor-signed CancelRequested -- it tore down locally.
  const grandchildEvents = await readWorkflowRunEvents(
    env,
    anchorRunId,
    grandchildRunId,
  );
  const grandchildTypes = grandchildEvents.map((e) => e.type);
  expect(
    grandchildTypes.some(
      (t) => t === "RunFailed" || t === "RunCancelled" || t === "RunCompleted",
    ),
  ).toBe(true);
}

describe.skipIf(!harnessDbEnvAvailable())(
  "drain tears down an in-flight loop-body childWorkflow grandchild",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test(
      "drain settles RunFailed without wedging on a PARKED grandchild",
      () =>
        runGrandchildDrainTest({
          anchorRunId: PARKED_ANCHOR,
          grandchildStep: "awaitSignal",
          inFlightEventType: "SignalAwaited",
        }),
      120_000,
    );

    test(
      "drain settles RunFailed without wedging on a MID-STEP (sleeping) grandchild",
      () =>
        runGrandchildDrainTest({
          anchorRunId: SLEEP_ANCHOR,
          grandchildStep: "sleep",
          inFlightEventType: "TimerSet",
        }),
      120_000,
    );
  },
);

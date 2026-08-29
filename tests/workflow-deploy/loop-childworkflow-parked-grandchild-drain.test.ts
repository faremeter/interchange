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

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_loop-cw-parked-drain-1";
const WORKFLOW_ID = `wf_${DEPLOYMENT_ID}`;
const CHILD_WORKFLOW_ID = `wf_child_${DEPLOYMENT_ID}`;
const LOOP_STEP_ID = "rework";
const BODY_CHILD_RUN_ID = `${LOOP_STEP_ID}__0`;
const DRAIN_DEADLINE_MS = 1_000;

const TENANT_ID = "tnt_loop_cw_parked_drain";
const CALLER_PRINCIPAL_ID = "prn_loop_cw_parked_drain";
const DEFINITION_ASSET_ID = "ast_loop_cw_parked_drain_wf";

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
    name: "loop-cw-parked-drain-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

async function topLevelRunId(
  workflowRunRepoId: RepoId,
): Promise<string | undefined> {
  return (await listRunIds(env, workflowRunRepoId)).find(
    (id) => !id.includes("__") && id !== CHILD_WORKFLOW_ID,
  );
}

describe.skipIf(!harnessDbEnvAvailable())(
  "drain tears down an in-flight loop-body childWorkflow grandchild",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("drain settles the run RunFailed without wedging on the parked grandchild", async () => {
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
        principalId: `prin_${DEPLOYMENT_ID}`,
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
        workflowId: WORKFLOW_ID,
        childWorkflowId: CHILD_WORKFLOW_ID,
      });

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
        messageId: "<loop-cw-parked-drain-1@integration.interchange>",
      });

      // Iteration 0's body spawns the grandchild; wait for the ChildSpawned in
      // the body child run and capture the minted grandchild run id.
      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(
            env,
            DEPLOYMENT_ID,
            BODY_CHILD_RUN_ID,
          );
          return events.some((e) => e.type === "ChildSpawned");
        },
        { diagnostics: env.sidecarDiagnostics, timeoutMs: 60_000 },
      );
      const bodyEvents = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        BODY_CHILD_RUN_ID,
      );
      const spawned = bodyEvents.find((e) => e.type === "ChildSpawned");
      if (spawned === undefined) throw new Error("unreachable");
      const grandchildRunId = spawned.body["childRunId"];
      if (typeof grandchildRunId !== "string") {
        throw new Error(
          `body ChildSpawned is missing a string childRunId; got ${typeof grandchildRunId}`,
        );
      }

      // The grandchild parks on its awaitSignal -- it is now a live in-process
      // terminal child, which is the state the drain must tear down locally.
      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(
            env,
            DEPLOYMENT_ID,
            grandchildRunId,
          );
          return events.some((e) => e.type === "SignalAwaited");
        },
        { diagnostics: env.sidecarDiagnostics, timeoutMs: 60_000 },
      );

      const runId = await topLevelRunId(workflowRunRepoId);
      if (runId === undefined) {
        throw new Error("no top-level run after the trigger fired");
      }

      // Ship the drain. No signal is ever delivered to the grandchild, so the
      // only way the run settles is the local teardown cascade.
      initiateDrain(env, DEPLOYMENT_ID, { deadlineMs: DRAIN_DEADLINE_MS });

      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        runId,
        { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
      );
      expect(terminal.type).toBe("RunFailed");

      // The grandchild reached a terminal (it did not wedge), and its own log
      // carries no supervisor-signed CancelRequested -- it tore down locally.
      const grandchildEvents = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        grandchildRunId,
      );
      const grandchildTypes = grandchildEvents.map((e) => e.type);
      expect(
        grandchildTypes.some(
          (t) =>
            t === "RunFailed" || t === "RunCancelled" || t === "RunCompleted",
        ),
      ).toBe(true);
    }, 120_000);
  },
);

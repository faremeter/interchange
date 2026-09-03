// An interrupted workflow-run pack recovers on reconnect with no fresh trigger.
//
// The reconnect-LIVENESS proof for the workflow-run-events push. A single-step
// run commits every event in one batch, so the supervisor ships exactly one
// refs/heads/main pack. The harness arms an arm-once interrupt on the hub: the
// FIRST run-events pack is applied durably on the hub, then every live link is
// dropped BEFORE the ack, so the sidecar's push rejects and latches "Connection
// lost". The sidecar then reconnects and re-announces its deployment address.
//
// The contract this asserts: after the reconnect the run reaches RunCompleted
// on its own -- WITHOUT any fresh mail trigger to re-drive it. The advance-on-ack
// pack-tip cursor keeps the un-acked commits shippable (the data-integrity half);
// this test covers the liveness half -- the sidecar must re-drive the cancelled
// push once its address is routable again, and the re-ship must wait for the
// allocation-authenticated reconnect to re-route the address rather than racing
// ahead of it.
//
// The settled-drop control is the regression guard: a drop AFTER the pack stream
// goes quiet (no push mid-flight) must still reconnect and run a fresh trigger to
// completion, so the liveness fix does not break ordinary reconnect survival.
//
// Harness justification: SPAWN-REAL. A real hub server, a real sidecar
// subprocess, a real workflow-process child, and a test inference provider. The
// drop is a genuine server-side WebSocket close mid-transfer; the recovery is
// the sidecar's real hub-link reconnect path passing the allocation identity
// checks and re-driving the latched push.

import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { deriveWorkflowRunId, isRunAddress } from "@intx/types";
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
  listRunIds,
  readClaimCheckDir,
  readWorkflowRunEvents,
  settleThenDrop,
  startDeployFlowEnv,
  waitFor,
  waitForReconnect,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { singleStepAgentEntry } from "./fixtures/single-step-agent";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const WORKFLOW_RUN_REF = "refs/heads/main";
const STEP_ID = "step1";

// The two anchor run ids this file deploys. Each needs its own
// `workflow`-kind definition asset seeded, since the install/approve freeze
// projects the frozen definition over a distinct asset per deploy.
const ARMED_ANCHOR_RUN_ID = "run_dep1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a";
const SETTLED_ANCHOR_RUN_ID = "run_dep2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b";

// The definition's own tenant, the caller principal that creates the
// definition assets, and the two `workflow`-kind assets the frozen
// definitions project over. The install/approve freeze and the anchor
// `workflow_run` insert both write against these, so they must exist in the
// real DB before each deploy runs.
const TENANT_ID = "tnt_interrupted_pack_recovery";
const CALLER_PRINCIPAL_ID = "prn_interrupted_pack_recovery";
const DEFINITION_ASSET_IDS: Record<string, string> = {
  [ARMED_ANCHOR_RUN_ID]: "ast_interrupted_pack_armed_wf",
  [SETTLED_ANCHOR_RUN_ID]: "ast_interrupted_pack_settled_wf",
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
      name: `interrupted-pack-recovery-wf-${anchorRunId}`,
      creatorPrincipalId: CALLER_PRINCIPAL_ID,
    });
  }
});

afterAll(async () => {
  if (h !== undefined) await h.close();
});

beforeEach(async () => {
  if (!harnessDbEnvAvailable()) return;
  env = await startDeployFlowEnv();
});

afterEach(async () => {
  if (env !== undefined) await env.teardown();
});

/**
 * Deploy a one-step workflow BY SOURCE-REF and register its handle on the env.
 * Returns the deployment's mail address and workflow-run repo id.
 */
async function deploySingleStepWorkflow(
  anchorRunId: string,
): Promise<{ deploymentMailAddress: string; workflowRunRepoId: RepoId }> {
  const deploymentMailAddress = deriveRunAddress({
    runId: anchorRunId,
    domain: DEPLOYMENT_DOMAIN,
  });

  const definitionAssetId = DEFINITION_ASSET_IDS[anchorRunId];
  if (definitionAssetId === undefined) {
    throw new Error(
      `interrupted-pack recovery: no definition asset seeded for ${anchorRunId}`,
    );
  }

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

  const entryModule = singleStepAgentEntry({
    stepId: STEP_ID,
    systemPrompt: "You are the interrupted-pack recovery test agent.",
    address: deploymentMailAddress,
    agentId: `agent-${anchorRunId}`,
    workflowId: `wf_${anchorRunId}`,
  });

  const handle = await deployWorkflowSourceForTest(env, {
    entryModule,
    db: h.db,
    tenantId: TENANT_ID,
    definitionAssetId,
    anchorRunId,
    deploymentDomain: DEPLOYMENT_DOMAIN,
    agentAddress: deploymentMailAddress,
    approvals: operatorApprovals,
    config,
    sources: { [STEP_ID]: [inferenceSource] },
  });
  expect(handle.publicKey).toBeTruthy();

  await waitFor(() => env.hub.deployAcks.has(deploymentMailAddress), {
    timeoutMs: 20_000,
    diagnostics: env.sidecarDiagnostics,
  });

  const workflowRunRepoId = handle.workflowRunRepoId;

  await waitFor(
    () => env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
    { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
  );

  return { deploymentMailAddress, workflowRunRepoId };
}

/**
 * Poll the deployment's run event log until at least one run reaches a
 * RunCompleted terminal, or throw on timeout. Returns nothing; the assertion
 * is the absence of a throw.
 */
async function waitForAnyRunCompleted(
  anchorRunId: string,
  workflowRunRepoId: RepoId,
  timeoutMs: number,
): Promise<void> {
  const start = Date.now();
  for (;;) {
    const ids = await listRunIds(env, workflowRunRepoId);
    for (const id of ids) {
      const events = await readWorkflowRunEvents(env, anchorRunId, id);
      if (events.some((e) => e.type === "RunCompleted")) return;
    }
    if (Date.now() - start > timeoutMs) {
      throw new Error(
        `no run reached RunCompleted for ${anchorRunId} within ${String(timeoutMs)}ms; runIds=${JSON.stringify(ids)}\n${env.sidecarDiagnostics()}`,
      );
    }
    await new Promise((r) => setTimeout(r, 100));
  }
}

describe.skipIf(!harnessDbEnvAvailable())(
  "interrupted workflow-run pack recovers on reconnect",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("armed mid-pack drop: run completes after reconnect with no fresh trigger", async () => {
      const anchorRunId = ARMED_ANCHOR_RUN_ID;
      const { deploymentMailAddress, workflowRunRepoId } =
        await deploySingleStepWorkflow(anchorRunId);
      expect(isRunAddress(deploymentMailAddress)).toBe(true);

      // Arm the interrupt so the FIRST run-events pack of this run is applied
      // on the hub, then every live link is dropped before the ack. The
      // sidecar's push rejects and latches "Connection lost".
      env.hub.interrupt.armed = true;

      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<interrupted-pack-1@integration.interchange>",
        content: "trigger",
      });

      // Wait for the interrupt to fire (armed flips back to false) and the
      // address to leave routing as the dropped link closes.
      await waitFor(() => env.hub.interrupt.armed === false, {
        timeoutMs: 30_000,
        diagnostics: env.sidecarDiagnostics,
      });
      expect(env.hub.interrupt.interruptedRef).toBe(WORKFLOW_RUN_REF);
      await waitFor(
        () =>
          !env.hub.router
            .getRoutableAddresses()
            .includes(deploymentMailAddress),
        { timeoutMs: 10_000, diagnostics: env.sidecarDiagnostics },
      );

      // The sidecar reconnects and re-announces its deployment address.
      const reconnectMs = await waitForReconnect(env, deploymentMailAddress, {
        timeoutMs: 30_000,
      });
      expect(reconnectMs).toBeGreaterThan(0);

      // The liveness contract: the run reaches RunCompleted on its own, with
      // NO fresh mail trigger to re-drive it. Capture the run-id count so the
      // assertion below can also confirm no second run was minted.
      await waitForAnyRunCompleted(anchorRunId, workflowRunRepoId, 60_000);

      // Exactly one run exists: the recovery re-shipped the SAME run's events,
      // it did not mint a fresh run. A second run would mean the recovery
      // depended on a new trigger rather than re-driving the cancelled push.
      const finalRunIds = await listRunIds(env, workflowRunRepoId);
      expect(finalRunIds).toHaveLength(1);
    }, 180_000);

    test("settled drop control: a fresh trigger runs to completion after reconnect", async () => {
      const anchorRunId = SETTLED_ANCHOR_RUN_ID;
      const { deploymentMailAddress, workflowRunRepoId } =
        await deploySingleStepWorkflow(anchorRunId);

      // Drive a first run to completion so the pack stream has something to
      // go quiet after -- settleThenDrop waits for a no-new-pack quiet window.
      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<settled-control-1@integration.interchange>",
        content: "first",
      });
      await waitForAnyRunCompleted(anchorRunId, workflowRunRepoId, 60_000);

      // Drop the link only after the pack stream has drained (no push
      // mid-flight), then wait for the sidecar to reconnect and re-route.
      await settleThenDrop(env, deploymentMailAddress, {
        quietMs: 750,
        timeoutMs: 30_000,
      });
      await waitFor(
        () =>
          !env.hub.router
            .getRoutableAddresses()
            .includes(deploymentMailAddress),
        { timeoutMs: 10_000, diagnostics: env.sidecarDiagnostics },
      );
      await waitForReconnect(env, deploymentMailAddress, { timeoutMs: 30_000 });

      // A fresh trigger on the recovered link runs to completion. Retry the
      // trigger with a fresh message id per attempt: a trigger that lands while
      // a residual reconnect is in flight can be dropped before the supervisor
      // enqueues it.
      // Under the stable-runId model every trigger shares the same runId.
      // Fire triggers until one lands in consumed/ (meaning the dispatch
      // loop processed it and the run reached terminal).
      const runId = deriveWorkflowRunId(deploymentMailAddress);
      let attempt = 0;
      let consumedMessageId = "";
      const start = Date.now();
      for (;;) {
        attempt += 1;
        const messageId = `<settled-control-recovered-${String(attempt)}@integration.interchange>`;
        await fireMailTrigger(env, deploymentMailAddress, {
          messageId,
          content: "recovered",
        });
        const deadline = Date.now() + 10_000;
        while (Date.now() < deadline) {
          const consumed = await readClaimCheckDir(
            env,
            workflowRunRepoId,
            deploymentMailAddress,
            "consumed",
          );
          if (consumed.some((c) => c.filename.includes(messageId))) {
            consumedMessageId = messageId;
            break;
          }
          await new Promise((r) => setTimeout(r, 100));
        }
        if (consumedMessageId !== "") break;
        if (Date.now() - start > 60_000) {
          throw new Error(
            `no fresh run produced on the recovered link after ${String(attempt)} triggers\n${env.sidecarDiagnostics()}`,
          );
        }
      }
      const terminal = await waitForWorkflowRunComplete(
        env,
        anchorRunId,
        runId,
        {
          timeoutMs: 30_000,
          diagnostics: env.sidecarDiagnostics,
        },
      );
      expect(terminal.type).toBe("RunCompleted");
    }, 180_000);
  },
);

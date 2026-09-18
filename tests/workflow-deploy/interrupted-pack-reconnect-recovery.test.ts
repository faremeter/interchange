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
  PRODUCTION_RECONNECT_DELAY_MS,
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
  env = await startDeployFlowEnv({
    // This scenario IS the mid-pack disconnect race: the recovery machinery
    // (push cancel -> reconnect -> re-drive) assumes the disconnect is fully
    // processed before the reconnect opens. The production 3s delay is part of
    // that envelope -- a faster reconnect can reopen the link while the
    // interrupted push is still being torn down and fail the run.
    sidecarEnv: {
      SIDECAR_RECONNECT_DELAY_MS: PRODUCTION_RECONNECT_DELAY_MS,
    },
  });
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
  const operatorApprovals: ApprovalSet = createApprovalSet([
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
    diagnostics: env.sidecarDiagnostics,
  });

  const workflowRunRepoId = handle.workflowRunRepoId;

  await waitFor(
    () => env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
    { diagnostics: env.sidecarDiagnostics },
  );

  return { deploymentMailAddress, workflowRunRepoId };
}

/**
 * Poll the deployment's run event log until at least one run reaches a
 * RunCompleted terminal. Returns nothing; the assertion is that it returns at
 * all.
 *
 * Carries no deadline: the caller's test budget is the failsafe for a run that
 * never completes, and the harness `waitFor` is what puts the sidecar's output
 * on the env teardown's report when that happens.
 */
async function waitForAnyRunCompleted(
  anchorRunId: string,
  workflowRunRepoId: RepoId,
): Promise<void> {
  await waitFor(async () => {
    for (const id of await listRunIds(env, workflowRunRepoId)) {
      const events = await readWorkflowRunEvents(env, anchorRunId, id);
      if (events.some((e) => e.type === "RunCompleted")) return true;
    }
    return false;
  });
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
        diagnostics: env.sidecarDiagnostics,
      });
      expect(env.hub.interrupt.interruptedRef).toBe(WORKFLOW_RUN_REF);
      await waitFor(
        () =>
          !env.hub.router
            .getRoutableAddresses()
            .includes(deploymentMailAddress),
        { diagnostics: env.sidecarDiagnostics },
      );

      // The sidecar reconnects and re-announces its deployment address.
      await waitForReconnect(env, deploymentMailAddress);

      // The liveness contract: the run reaches RunCompleted on its own, with
      // NO fresh mail trigger to re-drive it. Capture the run-id count so the
      // assertion below can also confirm no second run was minted.
      await waitForAnyRunCompleted(anchorRunId, workflowRunRepoId);

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
      await waitForAnyRunCompleted(anchorRunId, workflowRunRepoId);

      // Drop the link only after the pack stream has drained (no push
      // mid-flight), then wait for the sidecar to reconnect and re-route.
      await settleThenDrop(env, deploymentMailAddress, {
        quietMs: 750,
      });
      await waitFor(
        () =>
          !env.hub.router
            .getRoutableAddresses()
            .includes(deploymentMailAddress),
        { diagnostics: env.sidecarDiagnostics },
      );
      await waitForReconnect(env, deploymentMailAddress);

      // A fresh trigger on the recovered link runs to completion. Retry the
      // trigger with a fresh message id per attempt: a trigger that lands while
      // a residual reconnect is in flight can be dropped before the supervisor
      // enqueues it.
      // Under the stable-runId model every trigger shares the same runId.
      // Fire triggers until one lands in consumed/ (meaning the dispatch
      // loop processed it and the run reached terminal).
      //
      // The per-attempt 10s below is the re-fire cadence, not a budget: a
      // dropped trigger produces no signal at all, so the only way to conclude
      // one was dropped is to stop waiting on it and re-fire. Removing the
      // bound would remove the retry. The retry itself carries no deadline;
      // this test's own budget is the failsafe for a link that never accepts a
      // trigger. It fires mail each iteration, so it cannot become a `waitFor`
      // predicate; `env.retrying` is what puts a wedge inside it on the env
      // teardown's in-flight wait report, and `checkTornDown` at the top of
      // each loop body is what lets teardown's stop end it -- ahead of the
      // fire or the read that pass would otherwise make against a dismantled
      // env.
      const runId = deriveWorkflowRunId(deploymentMailAddress);
      await env.retrying(
        `re-fire trigger until one is consumed for ${deploymentMailAddress}`,
        async (checkTornDown) => {
          let attempt = 0;
          for (;;) {
            checkTornDown();
            attempt += 1;
            const messageId = `<settled-control-recovered-${String(attempt)}@integration.interchange>`;
            await fireMailTrigger(env, deploymentMailAddress, {
              messageId,
              content: "recovered",
            });
            const reFireAfter = Date.now() + 10_000;
            while (Date.now() < reFireAfter) {
              checkTornDown();
              const consumed = await readClaimCheckDir(
                env,
                workflowRunRepoId,
                deploymentMailAddress,
                "consumed",
              );
              if (consumed.some((c) => c.filename.includes(messageId))) return;
              await new Promise((r) => setTimeout(r, 100));
            }
          }
        },
      );
      const terminal = await waitForWorkflowRunComplete(
        env,
        anchorRunId,
        runId,
        {
          diagnostics: env.sidecarDiagnostics,
        },
      );
      expect(terminal.type).toBe("RunCompleted");
    }, 180_000);
  },
);

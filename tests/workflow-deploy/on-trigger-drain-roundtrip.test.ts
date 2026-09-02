// Deployed drain-teardown for an onTrigger SECTION body -- the onTrigger analog
// of loop-await-signal-drain-roundtrip, exercising `runOnTrigger`'s container
// drive (distinct from `runLoop`) on the real proxy path.
//
// An onTrigger section body runs in-process under the workflow-process
// principal and cannot sign the supervisor `CancelRequested` a cascade cancel
// needs, so on a cancel-mode drain it tears down LOCALLY (its step fails, the
// body settles `failed`) rather than wedging on a rejected control-plane cancel.
// Two cases cover `runOnTrigger`'s two abort paths:
//   - PARKED body (awaitSignal): the container is at its signal-relay await, and
//     its abort catch `await`s the body's terminal before throwing -- a wedged
//     body would hang the container. Proves the body settles and the run reaches
//     RunFailed.
//   - TOLERATE + mid-step body (a long sleep): the container is awaiting the
//     body terminal, so the failed teardown reaches runOnTrigger's terminal
//     policy (not the relay abort path above). A cancel-mode drain sheds this
//     mid-step `tolerate` section to RunFailed -- a deployed smoke test that it
//     settles, not hangs/completes/stays alive. (This does NOT isolate the live
//     `abort.aborted` terminal-policy disjunct: under a cancel-mode drain
//     `parkOnSignalResult`'s own `shouldAbortForDrain` guard would end a
//     pre-fix re-arm too. The live disjunct is proven directly by the in-memory
//     on-trigger-tolerate-abort test, whose operator cancel with a `wait`
//     section reaches the re-arm without that guard.)
//
// Harness justification: SPAWN-REAL.

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
import {
  onTriggerBodyEntry,
  type OnTriggerBodyVariant,
} from "./fixtures/on-trigger-body";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const SECTION_ID = "section";
const BODY_CHILD_RUN_ID = `${SECTION_ID}__0`;
const DRAIN_DEADLINE_MS = 1_000;

const TENANT_ID = "tnt_on_trigger_drain";
const CALLER_PRINCIPAL_ID = "prn_on_trigger_drain";
const DEFINITION_ASSET_IDS: Record<string, string> = {
  "run_on-trigger-drain-parked-1": "ast_on_trigger_drain_parked_wf",
  "run_on-trigger-drain-tolerate-1": "ast_on_trigger_drain_tolerate_wf",
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
      name: `on-trigger-drain-wf-${anchorRunId}`,
      creatorPrincipalId: CALLER_PRINCIPAL_ID,
    });
  }

  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

async function containerRunId(
  workflowRunRepoId: RepoId,
): Promise<string | undefined> {
  return (await listRunIds(env, workflowRunRepoId)).find(
    (id) => !id.includes("__"),
  );
}

async function deployDrainSection(opts: {
  anchorRunId: string;
  body: OnTriggerBodyVariant;
  onBodyFailure?: "end" | "tolerate";
}): Promise<{ workflowRunRepoId: RepoId; deploymentMailAddress: string }> {
  const deploymentMailAddress = deriveRunAddress({
    runId: opts.anchorRunId,
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
    agentId: `${opts.anchorRunId}`,
    tenantId: "tenant-1",
    principalId: `prin_${opts.anchorRunId}`,
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
  const entryModule = onTriggerBodyEntry({
    address: deploymentMailAddress,
    sectionId: SECTION_ID,
    body: opts.body,
    workflowId: `wf_${opts.anchorRunId}`,
    drainBehavior: "cancel",
    ...(opts.onBodyFailure !== undefined
      ? { onBodyFailure: opts.onBodyFailure }
      : {}),
  });
  const definitionAssetId = DEFINITION_ASSET_IDS[opts.anchorRunId];
  if (definitionAssetId === undefined) {
    throw new Error(`no definition asset seeded for ${opts.anchorRunId}`);
  }
  const handle = await deployWorkflowSourceForTest(env, {
    entryModule,
    db: h.db,
    tenantId: TENANT_ID,
    definitionAssetId,
    anchorRunId: opts.anchorRunId,
    deploymentDomain: DEPLOYMENT_DOMAIN,
    agentAddress: deploymentMailAddress,
    approvals: operatorApprovals,
    config,
    sources: { [SECTION_ID]: [inferenceSource] },
  });
  expect(handle.publicKey).toBeTruthy();

  await waitFor(
    () => env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
    { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
  );
  return { workflowRunRepoId: handle.workflowRunRepoId, deploymentMailAddress };
}

describe.skipIf(!harnessDbEnvAvailable())(
  "onTrigger section drain teardown",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("drain on a section body parked at its awaitSignal gate settles RunFailed", async () => {
      const anchorRunId = "run_on-trigger-drain-parked-1";
      const { workflowRunRepoId, deploymentMailAddress } =
        await deployDrainSection({
          anchorRunId,
          body: { variant: "awaitSignal", signalName: "never-arrives" },
        });

      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: `<${anchorRunId}@integration.interchange>`,
        content: "trigger the section",
      });

      // The body parks on its gate; the section proxies it up as a signal-relay
      // await on the container -- the container is now awaiting the body terminal.
      const runId = await waitForContainerSignalRelay(workflowRunRepoId);

      initiateDrain(env, anchorRunId, { deadlineMs: DRAIN_DEADLINE_MS });

      const terminal = await waitForWorkflowRunComplete(
        env,
        anchorRunId,
        runId,
        {
          timeoutMs: 30_000,
          diagnostics: env.sidecarDiagnostics,
        },
      );
      expect(terminal.type).toBe("RunFailed");

      // The body child settled (it did not wedge on a rejected cancel), and its
      // log carries no supervisor-signed CancelRequested.
      const bodyEvents = await readWorkflowRunEvents(
        env,
        anchorRunId,
        BODY_CHILD_RUN_ID,
      );
      const bodyTypes = bodyEvents.map((e) => e.type);
      expect(
        bodyTypes.some(
          (t) =>
            t === "RunFailed" || t === "RunCancelled" || t === "RunCompleted",
        ),
      ).toBe(true);
      expect(bodyTypes).not.toContain("CancelRequested");
    }, 90_000);

    test("drain on a mid-step tolerate section ends it, does not re-arm", async () => {
      const anchorRunId = "run_on-trigger-drain-tolerate-1";
      const { workflowRunRepoId, deploymentMailAddress } =
        await deployDrainSection({
          anchorRunId,
          // A long sleep keeps the body mid-step (not parked), so the container is
          // awaiting the body terminal and the failed teardown reaches the
          // tolerate terminal policy.
          body: { variant: "sleep", duration: 600_000 },
          onBodyFailure: "tolerate",
        });

      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: `<${anchorRunId}@integration.interchange>`,
        content: "trigger the section",
      });

      // Wait for the body's sleep step to be in flight (its TimerSet is durable),
      // so the drain lands while the container awaits the body terminal.
      const runId = await waitFor(
        async () => (await containerRunId(workflowRunRepoId)) !== undefined,
        { diagnostics: env.sidecarDiagnostics, timeoutMs: 20_000 },
      ).then(() => containerRunId(workflowRunRepoId));
      if (runId === undefined) throw new Error("no container run");
      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(
            env,
            anchorRunId,
            BODY_CHILD_RUN_ID,
          );
          return events.some((e) => e.type === "TimerSet");
        },
        { diagnostics: env.sidecarDiagnostics, timeoutMs: 30_000 },
      );

      initiateDrain(env, anchorRunId, { deadlineMs: DRAIN_DEADLINE_MS });

      // The mid-step tolerate section reaches the terminal policy and sheds to
      // RunFailed -- it settles, it does not hang, complete, or stay alive.
      const terminal = await waitForWorkflowRunComplete(
        env,
        anchorRunId,
        runId,
        {
          timeoutMs: 30_000,
          diagnostics: env.sidecarDiagnostics,
        },
      );
      expect(terminal.type).toBe("RunFailed");
    }, 90_000);
  },
);

async function waitForContainerSignalRelay(
  workflowRunRepoId: RepoId,
): Promise<string> {
  // For a single-section onTrigger deployment the container run id equals the
  // deployment anchor run id, so `containerId` serves as both the repo anchor
  // and the run id in the reads below.
  const containerId = await waitFor(
    async () => (await containerRunId(workflowRunRepoId)) !== undefined,
    { diagnostics: env.sidecarDiagnostics, timeoutMs: 20_000 },
  ).then(() => containerRunId(workflowRunRepoId));
  if (containerId === undefined) throw new Error("no container run");
  await waitFor(
    async () => {
      const events = await readWorkflowRunEvents(env, containerId, containerId);
      return events.some(
        (e) =>
          e.type === "SignalAwaited" && e.body["parkKind"] === "signal-relay",
      );
    },
    { diagnostics: env.sidecarDiagnostics, timeoutMs: 30_000 },
  );
  return containerId;
}

// A deployment in standby records the time its earliest timer fires, on the
// sidecar and on the Hub.
//
// The run sleeps for an hour before its agent step, so its only queued timer
// is the sleep's. The Hub's lifecycle service puts the deployment in standby,
// which stops the child. The sidecar's standby record and the Hub's stored
// standby then both carry the sleep's `fireAt` as the wake time.
//
// The test does not wait for the timer to wake the child: the sidecar
// subprocess arms that wake on its real clock and offers no seam to drive it.
//
// Harness justification: SPAWN-REAL: real hub, real sidecar subprocess, real
// workflow-process child, mock inference.

import fs from "node:fs";
import path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import {
  sidecar as sidecarTable,
  sidecarAllocation,
  tenant as tenantTable,
} from "@intx/db/schema";
import {
  createWorkflowHistoryReceiveTracker,
  createWorkflowLifecycleService,
  createWorkflowRunReader,
} from "@intx/hub-sessions";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import { deriveRunAddress } from "@intx/workflow-deploy";
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

import {
  SESSION_ID,
  SIDECAR_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  listWorkflowHostChildren,
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { singleStepMailToolEntry } from "./fixtures/single-step-mail-tool";
import { timerFireAt, waitForTimerSet } from "./sleep-restart-helpers";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_standby-timer-wake-at";
const TENANT_ID = "tnt_standby_timer_wake_at";
const CALLER_PRINCIPAL_ID = "prn_standby_timer_wake_at";
const DEFINITION_ASSET_ID = "ast_standby_timer_wake_at_wf";
const NAP_STEP_ID = "nap";
const AGENT_STEP_ID = "step1";
// Far past the test's budget, so the timer cannot fire while it runs.
const NAP_MS = 60 * 60 * 1000;
const MANIFEST_DIGEST = "1".repeat(64);

let env: DeployFlowEnv;
let h: TestDb;

beforeAll(async () => {
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
    name: "standby-timer-wake-at-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });
  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

// The allocation the fixture's router binds this deployment to, as the Hub
// records it when it places the deployment, so the lifecycle service can reach
// the deployment's sidecar.
async function seedAllocation(target: {
  allocationId: string;
  generation: number;
}): Promise<void> {
  const provisioner = {
    provisionerId: "test",
    provisionerApiVersion: 2 as const,
    provisionerBindingFingerprint: "test:1",
    adapterManifestDigest: MANIFEST_DIGEST,
  };
  await h.db.insert(sidecarTable).values({
    id: SIDECAR_ID,
    tokenHashSha256: new Uint8Array(32),
    ...provisioner,
  });
  await h.db.insert(sidecarAllocation).values({
    id: target.allocationId,
    tenantId: TENANT_ID,
    anchorRunId: DEPLOYMENT_ID,
    ...provisioner,
    maxDisconnectedMs: 900_000,
    sidecarId: SIDECAR_ID,
    status: "allocated",
    generation: target.generation,
    ensureAcceptedGeneration: target.generation,
  });
}

describe.skipIf(!harnessDbEnvAvailable())(
  "a deployment in standby with a queued timer",
  () => {
    test("records the timer's fire time as its wake time on the sidecar and the Hub", async () => {
      const address = deriveRunAddress({
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
        principalId: "prin_standby-timer-wake-at",
        agentAddress: address,
        systemPrompt: "Fallback prompt (overridden per step by the definition)",
        tools: [],
        grants: [],
        sources: [inferenceSource],
        defaultSource: "anthropic:mock-model",
      };
      const handle = await deployWorkflowSourceForTest(env, {
        entryModule: singleStepMailToolEntry({
          variant: "fs",
          stepId: AGENT_STEP_ID,
          systemPrompt: "You are the standby timer agent.",
          address,
          workflowId: `wf_${DEPLOYMENT_ID}`,
          napMs: NAP_MS,
        }),
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: address,
        approvals: "approve-probed",
        config,
        // The deploy frame needs a source chain for every step, the sleep
        // included.
        sources: {
          [NAP_STEP_ID]: [inferenceSource],
          [AGENT_STEP_ID]: [inferenceSource],
        },
      });
      await waitFor(
        () => env.hub.router.getRoutableAddresses().includes(address),
        { diagnostics: env.sidecarDiagnostics },
      );

      await fireMailTrigger(env, address, {
        messageId: "<standby-timer-wake-at@integration.interchange>",
      });
      const runId = await waitForFirstRunId(env, handle.workflowRunRepoId, {
        diagnostics: env.sidecarDiagnostics,
      });
      await waitForTimerSet(env, DEPLOYMENT_ID, runId, NAP_STEP_ID);
      const wakeAt = new Date(
        await timerFireAt(env, DEPLOYMENT_ID, runId, NAP_STEP_ID),
      ).toISOString();

      const target = env.hub.prepareAllocationIdentity(
        DEPLOYMENT_ID,
        address,
        SIDECAR_ID,
      );
      await seedAllocation(target);
      let commandSettled = Promise.withResolvers<undefined>();
      // The sweep pauses between passes on this clock, which the test moves.
      let nowMs = Date.now();
      const lifecycle = createWorkflowLifecycleService({
        db: h.db,
        retentionRouter: env.hub.router,
        runReader: createWorkflowRunReader(env.hub.agentRepoStore.repoStore),
        historyReceives: createWorkflowHistoryReceiveTracker(),
        standbyRouter: env.hub.router,
        onStandbyCommandSettled: (runId) => {
          if (runId === DEPLOYMENT_ID) commandSettled.resolve(undefined);
        },
        now: () => new Date(nowMs),
      });
      expect(await lifecycle.requestStandby(TENANT_ID, DEPLOYMENT_ID)).toBe(
        "pending",
      );
      // Each pass the sweep sends the pending request again until the sidecar
      // answers it; the first answer can wait on history the child is still
      // pushing.
      const standby = async () =>
        (await lifecycle.getStatus(TENANT_ID, DEPLOYMENT_ID))?.standby;
      while ((await standby())?.state !== "standby") {
        commandSettled = Promise.withResolvers<undefined>();
        if (await lifecycle.reconcileNext()) await commandSettled.promise;
        // The sweep selects nothing while it pauses between passes.
        else nowMs += 1_000;
      }
      expect(listWorkflowHostChildren(env)).toEqual([]);

      const record: unknown = JSON.parse(
        await fs.promises.readFile(
          path.join(
            env.sidecar.dataDir,
            "workflow-runs",
            handle.workflowRunRepoId.id,
            "deployment.json",
          ),
          "utf8",
        ),
      );
      expect(
        typeof record === "object" && record !== null && "standby" in record
          ? record.standby
          : undefined,
      ).toEqual({ parked: [], wakeAt, tip: expect.any(String) });
      expect(await standby()).toMatchObject({ state: "standby", wakeAt });
    });
  },
);

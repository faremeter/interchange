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

import { tenant as tenantTable } from "@intx/db/schema";
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
import { startStandbySweep } from "./standby-helpers";
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
        agentAddress: address,
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
      const { lifecycle, standby, untilInStandby } = await startStandbySweep(
        env,
        h.db,
        {
          tenantId: TENANT_ID,
          deploymentId: DEPLOYMENT_ID,
          target,
        },
      );
      expect(await lifecycle.requestStandby(TENANT_ID, DEPLOYMENT_ID)).toBe(
        "pending",
      );
      await untilInStandby();
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
      ).toEqual({
        parked: [],
        wakeAt,
        tip: expect.any(String),
        definition: expect.objectContaining({ id: `wf_${DEPLOYMENT_ID}` }),
      });
      expect(await standby()).toMatchObject({ state: "standby", wakeAt });
    });
  },
);

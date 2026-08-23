// A deployed workflow whose loop body is an `action` runs it end to end: the
// child host resolves the action handler from the closure's interchange.actions
// module, invokes it against the per-run effect ledger each iteration, and the
// loop converges. This exercises the action runtime (invokeAction + effects) on
// the already-proven loop container.
//
// The loop converges after three iterations (see loop-action-workflow.ts), so
// the top-level run log carries three ChildSpawned records under the loop step;
// RunCompleted double-checks the action body ran each iteration and the hub
// accepted the per-iteration child-run terminal events.
//
// Harness justification: SPAWN-REAL. A real hub server, a real sidecar
// subprocess, and a real workflow-process child evaluating the deployed source.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { isRunAddress } from "@intx/types";
import type { HarnessConfig } from "@intx/types/runtime";
import { deriveRunAddress } from "@intx/workflow-deploy";
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
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { loopActionWorkflowEntry } from "./fixtures/loop-action-workflow";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_ac710ac710ac710ac710ac710ac710a";
const TENANT_ID = "tnt_loop_action_roundtrip";
const CALLER_PRINCIPAL_ID = "prn_loop_action_roundtrip";
const DEFINITION_ASSET_ID = "ast_loop_action_roundtrip_wf";

let env: DeployFlowEnv;
let h: TestDb;
let deploymentMailAddress: string;

beforeAll(async () => {
  deploymentMailAddress = deriveRunAddress({
    runId: DEPLOYMENT_ID,
    domain: DEPLOYMENT_DOMAIN,
  });

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
    name: "loop-action-roundtrip-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

describe.skipIf(!harnessDbEnvAvailable())(
  "a deployed loop-with-action-body workflow runs to completion",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("the loop runs its action body each iteration and completes", async () => {
      expect(isRunAddress(deploymentMailAddress)).toBe(true);

      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentId: DEPLOYMENT_ID,
        tenantId: "tenant-1",
        principalId: "prin_loop-action-roundtrip-1",
        agentAddress: deploymentMailAddress,
        systemPrompt: "unused (loop body is an action)",
        tools: [],
        grants: [],
        sources: [
          {
            id: "anthropic:mock-model",
            provider: "anthropic",
            baseURL: `http://localhost:${String(env.inference.server.port)}`,
            apiKey: "sk-mock",
            model: "mock-model",
          },
        ],
        defaultSource: "anthropic:mock-model",
      };

      const handle = await deployWorkflowSourceForTest(env, {
        entryModule: loopActionWorkflowEntry({
          address: deploymentMailAddress,
        }),
        // The entry exports the workflow, the loop fns, and the action handler.
        loops: "./workflow.mjs",
        actions: "./workflow.mjs",
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: deploymentMailAddress,
        approvals: "approve-probed",
        config,
        // Omit sources so the harness computes them via the real pin (the
        // non-agent loop/action steps take the approved default placeholder).
      });
      expect(handle.publicKey).toBeTruthy();

      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<loop-action-roundtrip-1@integration.interchange>",
        content: "go",
      });

      const runId = await waitForFirstRunId(env, handle.workflowRunRepoId, {
        timeoutMs: 20_000,
        diagnostics: env.sidecarDiagnostics,
      });

      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        runId,
        { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
      );
      expect(terminal.type).toBe("RunCompleted");

      // Three loop iterations, each running the action body. All child spawns in
      // this run are loop iterations (no onTrigger/childWorkflow primitive). The
      // child packs the ChildSpawned records incrementally and replication can
      // lag several seconds under load, so poll with a generous deadline.
      let loopSpawns = 0;
      const deadline = Date.now() + 30_000;
      for (;;) {
        const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
        loopSpawns = events.filter((e) => e.type === "ChildSpawned").length;
        if (loopSpawns >= 3 || Date.now() > deadline) break;
        await new Promise((r) => setTimeout(r, 100));
      }
      expect(loopSpawns).toBe(3);
    }, 120_000);
  },
);

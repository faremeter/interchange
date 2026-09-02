// A deployed workflow that uses a `loop` primitive runs it end to end: the
// child host loads the loop `while`/`carry` functions from the closure's
// `interchange.loops` module, drives the loop against the shared store, and the
// hub accepts the loop's per-iteration child-run packs.
//
// The fixture converges after exactly three iterations (see loop-workflow.ts),
// so the top-level run log carries three `ChildSpawned` records under the loop
// step. That count double-checks the app-provided `keepGoing`/`nextCount` were
// resolved and applied; `RunCompleted` double-checks the hub tolerated the loop
// child-run terminal events rather than failing their packs closed.
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
import { loopWorkflowEntry } from "./fixtures/loop-workflow";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_100ada100ada100ada100ada100ada1";
const TENANT_ID = "tnt_loop_roundtrip";
const CALLER_PRINCIPAL_ID = "prn_loop_roundtrip";
const DEFINITION_ASSET_ID = "ast_loop_roundtrip_wf";

let env: DeployFlowEnv;
let h: TestDb;
let deploymentMailAddress: string;

beforeAll(async () => {
  if (!harnessDbEnvAvailable()) return;
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
    name: "loop-roundtrip-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

describe.skipIf(!harnessDbEnvAvailable())(
  "a deployed loop workflow runs to completion",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("the loop converges after three iterations and the run completes", async () => {
      expect(isRunAddress(deploymentMailAddress)).toBe(true);

      // A source with no agent step needs no inference; the config still carries
      // the shape, so supply one placeholder source it never resolves.
      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentId: DEPLOYMENT_ID,
        tenantId: "tenant-1",
        principalId: "prin_loop-roundtrip-1",
        agentAddress: deploymentMailAddress,
        systemPrompt: "unused (no agent step)",
        tools: [],
        grants: [],
        sources: [
          {
            id: "anthropic:mock-model",
            provider: "anthropic",
            baseURL: `http://localhost:${String(env.inference.server.port)}`,
            credentialId: "sk-mock",
            model: "mock-model",
          },
        ],
        defaultSource: "anthropic:mock-model",
      };

      const handle = await deployWorkflowSourceForTest(env, {
        entryModule: loopWorkflowEntry({ address: deploymentMailAddress }),
        // The entry module exports both `workflow` and the loop fns, so point
        // interchange.loops at the same bundled entry.
        loops: "./workflow.mjs",
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: deploymentMailAddress,
        approvals: "approve-probed",
        config,
        // Omit sources so the harness computes them via the real source pin
        // (which recurses into the loop body), exercising the fix end to end.
      });
      expect(handle.publicKey).toBeTruthy();

      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<loop-roundtrip-1@integration.interchange>",
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

      // Exactly three loop iterations spawned (the app-provided while/carry
      // converged at three). All child spawns in this run are loop iterations:
      // the workflow has no onTrigger or childWorkflow primitive. Poll: the child
      // packs the ChildSpawned records incrementally, so read until the count
      // settles rather than racing replication.
      // The child packs the ChildSpawned records incrementally and replication
      // can lag several seconds under load, so poll with a generous deadline.
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

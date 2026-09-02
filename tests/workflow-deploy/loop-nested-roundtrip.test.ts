// A deployed workflow whose loop body contains a NESTED loop runs end to end:
// the child host lifts both loop bodies, an inner loop resolves its body ref
// from the shared bodies map (env inheritance), and the deploy-time source pin
// recurses through both bodies to pin the inner agent step's inference source.
//
// Both loops converge after exactly three iterations (see
// loop-nested-workflow.ts), so the top-level run carries three `ChildSpawned`
// under the outer loop, and the outer loop's first iteration body run carries
// three `ChildSpawned` under the inner loop. Those counts prove the nested loop
// actually iterated on the deployed path.
//
// Harness justification: SPAWN-REAL. A real hub server, a real sidecar
// subprocess, and a real workflow-process child evaluating the deployed source.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { isRunAddress } from "@intx/types";
import type { HarnessConfig } from "@intx/types/runtime";
import { deriveRunAddress } from "@intx/workflow-deploy";
import { loopBodyRunId } from "@intx/workflow";
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
import { loopNestedWorkflowEntry } from "./fixtures/loop-nested-workflow";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_100ada100ada100ada100ada100nest";
const TENANT_ID = "tnt_loop_nested_roundtrip";
const CALLER_PRINCIPAL_ID = "prn_loop_nested_roundtrip";
const DEFINITION_ASSET_ID = "ast_loop_nested_roundtrip_wf";
const OUTER_LOOP_STEP_ID = "outer";
const INNER_LOOP_STEP_ID = "inner";

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
    name: "loop-nested-roundtrip-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

async function countChildSpawns(
  runId: string,
  target: number,
  timeoutMs: number,
): Promise<number> {
  let count = 0;
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
    count = events.filter((e) => e.type === "ChildSpawned").length;
    if (count >= target || Date.now() > deadline) break;
    await new Promise((r) => setTimeout(r, 100));
  }
  return count;
}

describe.skipIf(!harnessDbEnvAvailable())(
  "a deployed nested loop workflow runs to completion",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("both loops converge after three iterations and the run completes", async () => {
      expect(isRunAddress(deploymentMailAddress)).toBe(true);

      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentId: DEPLOYMENT_ID,
        tenantId: "tenant-1",
        principalId: "prin_loop-nested-roundtrip-1",
        agentAddress: deploymentMailAddress,
        systemPrompt: "unused (agents run per loop-body step)",
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
        entryModule: loopNestedWorkflowEntry({
          address: deploymentMailAddress,
        }),
        // The entry module exports `workflow` and the loop fns, so point
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
        // Omit sources so the harness computes them via the real source pin,
        // which recurses through BOTH loop bodies to pin the inner agent step.
      });
      expect(handle.publicKey).toBeTruthy();

      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<loop-nested-roundtrip-1@integration.interchange>",
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

      // The outer loop spawned three iteration child runs on the top-level run.
      const outerSpawns = await countChildSpawns(runId, 3, 30_000);
      expect(outerSpawns).toBe(3);

      // The outer loop's first iteration body run spawned three inner-loop
      // iteration child runs -- the nested loop actually ran on the deployed
      // path, resolving its body ref from the shared bodies map.
      const outerBodyRunId = loopBodyRunId(runId, OUTER_LOOP_STEP_ID, 0);
      const innerSpawns = await countChildSpawns(outerBodyRunId, 3, 30_000);
      expect(innerSpawns).toBe(3);

      // The inner iteration body run is keyed under the outer iteration's run id.
      const innerBodyRunId = loopBodyRunId(
        outerBodyRunId,
        INNER_LOOP_STEP_ID,
        0,
      );
      const innerBodyEvents = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        innerBodyRunId,
      );
      expect(innerBodyEvents.length).toBeGreaterThan(0);
    }, 120_000);
  },
);

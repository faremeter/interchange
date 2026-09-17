// A `map` inner step INSIDE a loop body invokes a real tool, and the tool's
// real output lands in the per-item agent's turn.
//
// The sibling `loop-tool-invoke-roundtrip.test.ts` proves a plain agent step in
// a loop body can call a tool, and `map-fan-out-real-agent.test.ts` proves a
// TOP-LEVEL map iteration resolves its base step's staged tool tree. This
// composes the two, which nothing else does: `runMap` scopes its inner step's
// id to `<mapStepId>[<index>]` and every deploy-asset lookup strips that suffix
// with `baseStepId`, while the map itself lives in a loop body whose ids reach
// the deployment's flat namespace only through the executable-step walk's
// descent into loop bodies. A per-item tool call has to survive both
// transformations at once to find its credentials, source, and deploy tree.
//
// The assertion reads the tool_result's TEXT, not merely its presence. A tool
// call the authorize seam blocks still appends a tool_result -- an error one,
// carrying the throw's message -- and the agent then replies normally, so the
// run completes and a presence-only check passes while nothing ever ran. Only
// the tool's own return value (`wrote <filename>`) proves the map iteration
// executed it.
//
// The env runs exactly one workflow, and only the map's per-item agent carries
// a tool. The two top-level agent steps can produce no tool_result at all, so a
// tool_result in ANY captured request can only have originated in a map
// iteration inside the loop body.
//
// Harness justification: SPAWN-REAL. A real hub server, a real sidecar
// subprocess, and a real workflow-process child evaluating the deployed source.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import {
  createApprovalSet,
  deriveRunAddress,
  type ApprovalSet,
} from "@intx/workflow-deploy";
import { loopBodyRunId } from "@intx/workflow";
import { loadFrozenGrantSnapshot } from "@intx/db";
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
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import {
  deriveWireRunGrants,
  failureMessages,
  findContainerRunId,
  toolResultTexts,
} from "./nested-tool-invoke-helpers";
import { MAIL_TOOL_NAME } from "./fixtures/mail-tool";
import {
  EXPECTED_ITERATIONS,
  LOOP_BODY_MAP_STEP_ID,
  LOOP_STEP_ID,
  TOP_LEVEL_STEP_IDS,
  loopMapToolInvokeWorkflowEntry,
} from "./fixtures/loop-map-tool-invoke-workflow";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_loop-map-tool-invoke-1";
const WORKFLOW_ID = `wf_${DEPLOYMENT_ID}`;

const TENANT_ID = "tnt_loop_map_tool_invoke";
const CALLER_PRINCIPAL_ID = "prn_loop_map_tool_invoke";
const DEFINITION_ASSET_ID = "ast_loop_map_tool_invoke_wf";

// The filename the driven tool call writes under the step workdir, and the
// content the "fs" variant of `mail_send` returns for it. Matching this exact
// string is what distinguishes a real execution from an error tool_result.
const TOOL_OUTPUT_FILENAME = "loop-map-item-invoked.txt";
const EXPECTED_TOOL_RESULT = `wrote ${TOOL_OUTPUT_FILENAME}`;

let env: DeployFlowEnv;
let h: TestDb;

beforeAll(async () => {
  // A file-scope beforeAll fires even when describe.skipIf skips the suite
  // bodies, so it needs its own guard or a missing DB env throws here.
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
    name: "loop-map-tool-invoke-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  // Drive the inline `mail_send` tool on the first request that exposes it, on
  // every fresh conversation. Each map iteration runs its own agent, so
  // `inferenceToolCallEachRun` is what makes every item drive the tool rather
  // than only the first. The "fs" variant writes a file under the step workdir
  // with no env requirement, so the call runs for real in the iteration.
  env = await startDeployFlowEnv({
    inferenceToolCall: {
      toolName: MAIL_TOOL_NAME,
      input: { to: "loop map item tool ran", body: TOOL_OUTPUT_FILENAME },
    },
    inferenceToolCallEachRun: true,
  });
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

describe.skipIf(!harnessDbEnvAvailable())(
  "a map inner step inside a loop body invokes a real tool",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    // The per-step credentials, source, and deploy-tree tables are keyed by
    // base step id in one namespace shared by the top level and the loop body,
    // so a colliding body step id would resolve to a top-level step's entry
    // and make the round-trip below pass against the wrong step.
    test("the loop body's map step id collides with no top-level step id", () => {
      expect(TOP_LEVEL_STEP_IDS).not.toContain(LOOP_BODY_MAP_STEP_ID);
    });

    test("the map item agent calls its inline tool and the real result lands in a turn", async () => {
      const deploymentMailAddress = deriveRunAddress({
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
        principalId: `prin_${DEPLOYMENT_ID}`,
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
        // The map's per-item agent declares the inline tool, so the deploy walk
        // folds its grant into the top level through the loop body; the
        // operator must approve it.
        `tool:${MAIL_TOOL_NAME}`,
      ]);

      const handle = await deployWorkflowSourceForTest(env, {
        entryModule: loopMapToolInvokeWorkflowEntry({
          address: deploymentMailAddress,
          workflowId: WORKFLOW_ID,
        }),
        // The entry module exports both `workflow` and the loop fns, so point
        // interchange.loops at the same bundled entry.
        loops: "./workflow.mjs",
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: deploymentMailAddress,
        approvals: operatorApprovals,
        config,
        // Omit `sources` so the harness computes them via the real source pin,
        // which recurses into the loop body and pins the map step too.
      });
      expect(handle.publicKey).toBeTruthy();

      const workflowRunRepoId: RepoId = handle.workflowRunRepoId;

      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      // The map agent's tool grant reaches the run the way production delivers
      // it: the deploy walk folds the body step's `tool:` grant into the frozen
      // snapshot, and the trigger route projects that snapshot into the run's
      // grant rows. `fireMailTrigger` is the router-level helper, so unlike the
      // production route it does not materialize the rows itself -- deriving
      // them from the frozen snapshot here reproduces the delivery, and keeps
      // the grant the iteration authorizes against sourced from the probe walk
      // rather than a hand-authored constant.
      if (!handle.approved.approval.ok) {
        throw new Error("expected an approved definition");
      }
      const snapshot = await loadFrozenGrantSnapshot(
        h.db,
        handle.approved.approval.definitionId,
      );
      if (snapshot === null) {
        throw new Error("expected a frozen grant snapshot for the definition");
      }
      expect(
        snapshot.perStep.flatMap((s) =>
          s.grants.filter((g) => g.startsWith("tool:")),
        ),
      ).toContain(`tool:${MAIL_TOOL_NAME}`);

      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: `<${DEPLOYMENT_ID}@integration.interchange>`,
        content: "run the tool-bearing map inside the loop body",
        grants: deriveWireRunGrants(snapshot),
      });

      await waitFor(
        async () =>
          (await findContainerRunId(env, workflowRunRepoId)) !== undefined,
        { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
      );
      const runId = await findContainerRunId(env, workflowRunRepoId);
      if (runId === undefined) throw new Error("unreachable");

      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        runId,
        { timeoutMs: 60_000, diagnostics: env.sidecarDiagnostics },
      );

      // Read the body run's own log first: a body step's failure is recorded
      // there, and the container only sees the iteration's terminal status.
      const bodyRunId = loopBodyRunId(runId, LOOP_STEP_ID, 0);
      expect(
        failureMessages(
          await readWorkflowRunEvents(env, DEPLOYMENT_ID, bodyRunId),
        ),
      ).toEqual([]);

      const containerEvents = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        runId,
      );
      expect(failureMessages(containerEvents)).toEqual([]);
      expect(terminal.type).toBe("RunCompleted");

      // The loop ran its body once per iteration. All child spawns in this run
      // are loop iterations: the workflow has no onTrigger or childWorkflow
      // primitive.
      const loopSpawns = containerEvents.filter(
        (e) => e.type === "ChildSpawned",
      ).length;
      expect(loopSpawns).toBe(EXPECTED_ITERATIONS);

      // The map's per-item agent executed the tool and re-inferenced with its
      // REAL output. Every top-level step is toolless, so this result can only
      // be a map iteration's invocation landing in a turn. Asserting on the
      // text rather than the block's presence is what rejects an error
      // tool_result carrying an authorize failure, which the agent would answer
      // normally and let the run complete.
      const results = env.inference.requests.flatMap(toolResultTexts);
      expect(results).toContain(EXPECTED_TOOL_RESULT);
    }, 180_000);
  },
);

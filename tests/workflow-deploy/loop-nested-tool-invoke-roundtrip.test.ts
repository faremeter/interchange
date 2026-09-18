// An agent step inside a NESTED INNER loop invokes a real tool, and the tool's
// real output lands in its turn.
//
// The sibling `loop-tool-invoke-roundtrip.test.ts` proves the one-rung case,
// and `child-workflow-loop-tool-invoke-roundtrip.test.ts` proves a loop body
// inside a SPAWNED child. Loop-inside-loop is neither: an inner loop resolves
// its body ref from the same bodies map as its parent and inherits the parent's
// env one further rung down, so the innermost step's tool call authorizes
// against a credentials snapshot the executable-step walk had to reach through
// two nested descents rather than one.
//
// `loop-nested-roundtrip.test.ts` already proves a nested loop ITERATES on the
// deployed path, but its body agents declare `tools: []`, so it never reaches
// the tool-invocation authorize seam. This is the stronger property at that
// nesting depth.
//
// The assertion reads the tool_result's TEXT, not merely its presence. A tool
// call the authorize seam blocks still appends a tool_result -- an error one,
// carrying the throw's message -- and the agent then replies normally, so the
// run completes and a presence-only check passes while nothing ever ran. Only
// the tool's own return value (`wrote <filename>`) proves the innermost body
// executed it.
//
// The env runs exactly one workflow, and only the innermost body step carries a
// tool. Every other step -- both top-level arms, both loop containers, and the
// inner loop's exhausted arm -- is toolless, so a tool_result in ANY captured
// request can only have originated two rungs down.
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
  ALL_TOOLLESS_STEP_IDS,
  EXPECTED_ITERATIONS,
  INNER_BODY_STEP_ID,
  INNER_LOOP_STEP_ID,
  OUTER_LOOP_STEP_ID,
  loopNestedToolInvokeWorkflowEntry,
} from "./fixtures/loop-nested-tool-invoke-workflow";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_loop-nested-tool-invoke-1";
const WORKFLOW_ID = `wf_${DEPLOYMENT_ID}`;

const TENANT_ID = "tnt_loop_nested_tool_invoke";
const CALLER_PRINCIPAL_ID = "prn_loop_nested_tool_invoke";
const DEFINITION_ASSET_ID = "ast_loop_nested_tool_invoke_wf";

// The filename the driven tool call writes under the step workdir, and the
// content the "fs" variant of `mail_send` returns for it. Matching this exact
// string is what distinguishes a real execution from an error tool_result.
const TOOL_OUTPUT_FILENAME = "inner-loop-body-invoked.txt";
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
    name: "loop-nested-tool-invoke-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  // Drive the inline `mail_send` tool on the first request that exposes it, on
  // every run. Each inner-loop iteration is its own agent conversation, so
  // `inferenceToolCallEachRun` is what makes every iteration drive the tool
  // rather than only the first. The "fs" variant writes a file under the step
  // workdir with no env requirement, so the call runs for real in the body.
  env = await startDeployFlowEnv({
    inferenceToolCall: {
      toolName: MAIL_TOOL_NAME,
      input: { to: "inner loop body tool ran", body: TOOL_OUTPUT_FILENAME },
    },
    inferenceToolCallEachRun: true,
  });
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

// The `ChildSpawned` count on a run's log, read until it reaches `target`. The
// child packs those records incrementally, so a single read races replication.
// Returning the count rather than asserting inside lets the caller pin the
// exact value. The poll carries no deadline of its own: the test runner's
// budget is the failsafe for a count that never arrives, and the harness
// `waitFor` is what puts the sidecar's output on the env teardown's report.
async function countChildSpawns(
  runId: string,
  target: number,
): Promise<number> {
  let count = 0;
  await waitFor(async () => {
    const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
    count = events.filter((e) => e.type === "ChildSpawned").length;
    return count >= target;
  });
  return count;
}

describe.skipIf(!harnessDbEnvAvailable())(
  "a nested inner loop's body step invokes a real tool",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    // The per-step credentials, source, and deploy-tree tables are keyed by
    // base step id in one namespace shared by the top level and BOTH loop
    // bodies, so a colliding inner body step id would resolve to another
    // step's entry and make the round-trip below pass against the wrong step.
    test("the inner body step id collides with no other step id", () => {
      expect(ALL_TOOLLESS_STEP_IDS).not.toContain(INNER_BODY_STEP_ID);
    });

    test("the innermost body agent calls its inline tool and the real result lands in a turn", async () => {
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
        // The innermost body step declares the inline tool, so the deploy walk
        // folds its grant up through both loop bodies to the top level; the
        // operator must approve it.
        `tool:${MAIL_TOOL_NAME}`,
      ]);

      const handle = await deployWorkflowSourceForTest(env, {
        entryModule: loopNestedToolInvokeWorkflowEntry({
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
        // which recurses through BOTH loop bodies to pin the innermost step.
      });
      expect(handle.publicKey).toBeTruthy();

      const workflowRunRepoId: RepoId = handle.workflowRunRepoId;

      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { diagnostics: env.sidecarDiagnostics },
      );

      // The innermost step's tool grant reaches the run the way production
      // delivers it: the deploy walk folds the body step's `tool:` grant into
      // the frozen snapshot, and the trigger route projects that snapshot into
      // the run's grant rows. `fireMailTrigger` is the router-level helper, so
      // unlike the production route it does not materialize the rows itself --
      // deriving them from the frozen snapshot here reproduces the delivery,
      // and keeps the grant the innermost body authorizes against sourced from
      // the probe walk rather than a hand-authored constant.
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
        content: "run the tool-bearing innermost loop body",
        grants: deriveWireRunGrants(snapshot),
      });

      await waitFor(
        async () =>
          (await findContainerRunId(env, workflowRunRepoId)) !== undefined,
        { diagnostics: env.sidecarDiagnostics },
      );
      const runId = await findContainerRunId(env, workflowRunRepoId);
      if (runId === undefined) throw new Error("unreachable");

      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        runId,
        { diagnostics: env.sidecarDiagnostics },
      );

      // Read the two nested body run logs first: a body step's failure is
      // recorded in its own run's log, and each enclosing run only sees the
      // iteration's terminal status.
      const outerBodyRunId = loopBodyRunId(runId, OUTER_LOOP_STEP_ID, 0);
      const innerBodyRunId = loopBodyRunId(
        outerBodyRunId,
        INNER_LOOP_STEP_ID,
        0,
      );
      expect(
        failureMessages(
          await readWorkflowRunEvents(env, DEPLOYMENT_ID, innerBodyRunId),
        ),
      ).toEqual([]);

      expect(
        failureMessages(
          await readWorkflowRunEvents(env, DEPLOYMENT_ID, outerBodyRunId),
        ),
      ).toEqual([]);

      const containerEvents = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        runId,
      );
      expect(failureMessages(containerEvents)).toEqual([]);
      expect(terminal.type).toBe("RunCompleted");

      // Each loop ran its body once per iteration. All child spawns in this
      // workflow are loop iterations: it declares no onTrigger or childWorkflow
      // primitive. The outer count lands on the container run, the inner count
      // on the outer loop's first iteration body run -- a nested run whose log
      // the container's own terminal event does not order against, so that one
      // is polled rather than read once.
      expect(
        containerEvents.filter((e) => e.type === "ChildSpawned").length,
      ).toBe(EXPECTED_ITERATIONS);
      expect(await countChildSpawns(outerBodyRunId, EXPECTED_ITERATIONS)).toBe(
        EXPECTED_ITERATIONS,
      );

      // The innermost body's agent executed the tool and re-inferenced with its
      // REAL output. Every other step in the workflow is toolless, so this
      // result can only be the innermost body's invocation landing in a turn.
      // Asserting on the text rather than the block's presence is what rejects
      // an error tool_result carrying an authorize failure, which the agent
      // would answer normally and let the run complete.
      const results = env.inference.requests.flatMap(toolResultTexts);
      expect(results).toContain(EXPECTED_TOOL_RESULT);
    }, 180_000);
  },
);

// A `loop` body's agent step invokes a real tool ONE RUNG DOWN -- inside a
// spawned `childWorkflow` child rather than at the top level.
//
// The sibling `loop-tool-invoke-roundtrip.test.ts` proves the top-level case,
// which the deployment's own credentials snapshot covers. A spawned child does
// NOT share that snapshot: the sidecar mints a FRESH one over the child
// definition when it builds the child's run env, and a loop iteration inside
// the child re-enters the child's env, so the body step's tool call authorizes
// against the child's snapshot under the body step's own id. A snapshot minted
// over the child's `stepOrder` alone therefore leaves every loop nested in a
// spawned body unable to call a tool, even once the top-level case works. This
// is the case that pins the spawned-body mint.
//
// The assertion reads the tool_result's TEXT, not merely its presence. A tool
// call the authorize seam blocks still appends a tool_result -- an error one,
// carrying the throw's message -- and the agent then replies normally, so the
// run completes and a presence-only check passes while nothing ever ran. Only
// the tool's own return value (`wrote <filename>`) proves the body executed it.
//
// Every step outside the loop body is toolless, so a tool_result in ANY
// captured inference request can only have originated in the loop body.
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
  deployWorkflowSourceForTest,
  fireMailTrigger,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import {
  deriveWireRunGrants,
  failureMessages,
  toolResultTexts,
} from "./nested-tool-invoke-helpers";
import { MAIL_TOOL_NAME } from "./fixtures/mail-tool";
import {
  LOOP_BODY_STEP_ID,
  NON_BODY_STEP_IDS,
  childWorkflowLoopToolEntry,
} from "./fixtures/child-workflow-loop-tool-workflow";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_child-workflow-loop-tool-1";
const CHILD_DEPLOYMENT_ID = "run_child-workflow-loop-tool-child-1";
const WORKFLOW_ID = `wf_${DEPLOYMENT_ID}`;
const CHILD_WORKFLOW_ID = `wf_${CHILD_DEPLOYMENT_ID}`;

const TENANT_ID = "tnt_child_wf_loop_tool";
const CALLER_PRINCIPAL_ID = "prn_child_wf_loop_tool";
const DEFINITION_ASSET_ID = "ast_child_wf_loop_tool_wf";

// The filename the driven tool call writes under the step workdir, and the
// content the "fs" variant of `mail_send` returns for it. Matching this exact
// string is what distinguishes a real execution from an error tool_result.
const TOOL_OUTPUT_FILENAME = "nested-loop-body-invoked.txt";
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
    name: "child-workflow-loop-tool-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  // Drive the inline `mail_send` tool on the first request that exposes it, on
  // every run. Each loop iteration is its own agent conversation, so
  // `inferenceToolCallEachRun` is what makes every iteration drive the tool
  // rather than only the first. The "fs" variant writes a file under the step
  // workdir with no env requirement, so the call runs for real in the body.
  env = await startDeployFlowEnv({
    inferenceToolCall: {
      toolName: MAIL_TOOL_NAME,
      input: { to: "nested loop body tool ran", body: TOOL_OUTPUT_FILENAME },
    },
    inferenceToolCallEachRun: true,
  });
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

describe.skipIf(!harnessDbEnvAvailable())(
  "a loop body inside a spawned child invokes a real tool",
  () => {
    // The per-step credentials, source, and deploy-tree tables are keyed by
    // base step id, so a body step id colliding with another step's id would
    // resolve to that step's entry and make the round-trip below pass against
    // the wrong step.
    test("the loop body step id collides with no other step id", () => {
      expect(NON_BODY_STEP_IDS).not.toContain(LOOP_BODY_STEP_ID);
    });

    test("the nested body agent calls its inline tool and the real result lands in a turn", async () => {
      const deploymentMailAddress = deriveRunAddress({
        runId: DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });
      const childMailAddress = deriveRunAddress({
        runId: CHILD_DEPLOYMENT_ID,
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
        `mail.address:${childMailAddress}`,
        `mail.send:${DEPLOYMENT_DOMAIN}`,
        // The child's loop body declares the inline tool, so the deploy walk
        // folds its grant up into the parent's spawn step; the operator must
        // approve it.
        `tool:${MAIL_TOOL_NAME}`,
      ]);

      const handle = await deployWorkflowSourceForTest(env, {
        entryModule: childWorkflowLoopToolEntry({
          address: deploymentMailAddress,
          childAddress: childMailAddress,
          workflowId: WORKFLOW_ID,
          childWorkflowId: CHILD_WORKFLOW_ID,
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
        // Omit `sources` so the harness computes them via the real source pin.
      });
      expect(handle.publicKey).toBeTruthy();

      const workflowRunRepoId: RepoId = handle.workflowRunRepoId;

      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      // The tool grant reaches the run the way production delivers it: the
      // deploy walk folds the body step's `tool:` grant into the frozen
      // snapshot, and the trigger route projects that snapshot into the run's
      // grant rows. `fireMailTrigger` is the router-level helper, so unlike the
      // production route it does not materialize the rows itself. The child's
      // own grants are then capped from these at spawn, and the loop iteration
      // inherits the child's.
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
        content: "delegate to the child and run its tool-bearing loop body",
        grants: deriveWireRunGrants(snapshot),
      });

      const runId = await waitForFirstRunId(env, workflowRunRepoId, {
        diagnostics: env.sidecarDiagnostics,
        timeoutMs: 30_000,
      });

      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        runId,
        { timeoutMs: 60_000, diagnostics: env.sidecarDiagnostics },
      );

      const containerEvents = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        runId,
      );
      expect(failureMessages(containerEvents)).toEqual([]);
      expect(terminal.type).toBe("RunCompleted");

      // The nested body's agent executed the tool and re-inferenced with its
      // REAL output. Every step outside the loop body is toolless, so this
      // result can only be the nested body's invocation landing in a turn.
      const results = env.inference.requests.flatMap(toolResultTexts);
      expect(results).toContain(EXPECTED_TOOL_RESULT);
    }, 180_000);
  },
);

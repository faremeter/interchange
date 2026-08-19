// Phase 2 proof: a step's agent runs a REAL tool materialized in the
// spawned workflow-process child.
//
// Deploys a one-step workflow BY SOURCE-REF whose agent carries the inline
// `mail_send` tool from the `mail-tool.ts` fixture (a real `defineTool`
// module bundled into the workflow's source closure). The sidecar checks the
// pinned subtree out of the source pack, evaluates the bundle in-child, and
// feeds the step agent's live `AnnotatedToolFactory`s straight in. The tool's
// runtime name is the bare `definition.name`; the probe's capability walk
// already emitted a `tool:<name>` grant for it into the frozen snapshot, so
// the run authorizes the call through the per-run grants the trigger delivers.
//
// The mock inference server is configured to emit a `tool_use` turn calling
// the inline tool on the first request, then a text reply once the tool_result
// lands. The tool's `run` writes a sentinel file into the agent's `env.workdir`
// -- which, for a step agent, is the per-step workspace under the sidecar data
// dir. The test asserts that sentinel file exists, proving the tool actually
// EXECUTED in the child's filesystem view. It also asserts the mock saw the
// follow-up request (the tool_result round-trip) and the run reached a terminal
// phase.
//
// This is the test that proves real tools run in-child for Phase 2.

import fs from "node:fs";
import path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import type { WireGrantRule } from "@intx/types/grant-wire";
import { deriveRunAddress, type ApprovalSet } from "@intx/workflow-deploy";
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
import { MAIL_TOOL_NAME } from "./fixtures/mail-tool";
import { singleStepMailToolEntry } from "./fixtures/single-step-mail-tool";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_single-step-posix-tool-1";
const STEP_ID = "step1";
const AGENT_ID = "agent-step1";

// The tool's `run` writes a file named after its `body` arg with its
// `to` arg as content; the test drives those values and asserts the
// file lands in the child's per-step workspace.
const SENTINEL_FILENAME = "posix-tool-ran.txt";
const SENTINEL_CONTENT = "executed-in-child";

// The run's single grant, delivered per run via the `run.grants` frame the
// trigger sends. `fireMailTrigger` is the router-level helper, so it does not
// materialize the run grants itself the way the production route does; feeding
// the `tool:<name>` grant here reproduces the production per-run delivery so
// the child's authorize resolves the inline tool call against it.
const RUN_TOOL_GRANT: WireGrantRule = {
  id: `run-grant:tool:${MAIL_TOOL_NAME}`,
  resource: `tool:${MAIL_TOOL_NAME}`,
  action: "invoke",
  effect: "allow",
  origin: "creator",
  conditions: null,
  expiresAt: null,
  roleId: null,
  principalId: null,
};

// The definition's own tenant, the caller principal that creates the
// definition asset, and the `workflow`-kind asset the frozen definition
// projects over. The install/approve freeze and the anchor `workflow_run`
// insert both write against these, so they must exist in the real DB before
// the deploy runs.
const TENANT_ID = "tnt_single_step_posix_tool";
const CALLER_PRINCIPAL_ID = "prn_single_step_posix_tool";
const DEFINITION_ASSET_ID = "ast_single_step_posix_tool_wf";

let env: DeployFlowEnv;
let h: TestDb;

beforeAll(async () => {
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
    name: "single-step-posix-tool-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv({
    inferenceToolCall: {
      toolName: MAIL_TOOL_NAME,
      input: { to: SENTINEL_CONTENT, body: SENTINEL_FILENAME },
    },
  });
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

describe.skipIf(!harnessDbEnvAvailable())(
  "single-step posix-tool in-child execution",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("the spawned child materializes and runs a real tool for the step", async () => {
      const deploymentMailAddress = deriveRunAddress({
        runId: DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });

      const inferenceSource: InferenceSource = {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${env.inference.server.port}`,
        apiKey: "sk-mock",
        model: "mock-model",
      };

      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentId: `${DEPLOYMENT_ID}`,
        tenantId: "tenant-1",
        principalId: "prin_integration-1",
        agentAddress: deploymentMailAddress,
        systemPrompt: "Fallback prompt (overridden per step by the definition)",
        tools: [],
        grants: [],
        sources: [inferenceSource],
        defaultSource: "anthropic:mock-model",
      };

      // The operator approves exactly the surface the source workflow declares,
      // INCLUDING the inline tool's `tool:<name>` grant. That grant is frozen
      // into the credentials snapshot and, delivered per run, is what
      // authorizes the tool call at run time -- the source path's tool
      // authorization rides the snapshot, no sidecar floor.
      const operatorApprovals: ApprovalSet = new Set<string>([
        "inference.source:anthropic:mock-model",
        "director:@intx/agent/default",
        `mail.address:${deploymentMailAddress}`,
        `mail.send:${DEPLOYMENT_DOMAIN}`,
        `tool:${MAIL_TOOL_NAME}`,
      ]);

      const entryModule = singleStepMailToolEntry({
        variant: "fs",
        stepId: STEP_ID,
        systemPrompt: "You are the single-step tool agent.",
        address: deploymentMailAddress,
        agentId: AGENT_ID,
      });

      const handle = await deployWorkflowSourceForTest(env, {
        entryModule,
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: deploymentMailAddress,
        approvals: operatorApprovals,
        config,
        sources: { [STEP_ID]: [inferenceSource] },
      });
      expect(handle.publicKey).toBeTruthy();

      const workflowRunRepoId = handle.workflowRunRepoId;

      // The source-ref frame round-trips through the real sidecar subprocess
      // (index the pack, check out the pinned subtree, register the address),
      // so routability is asynchronous. Wait for it before firing the trigger.
      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<single-step-posix-tool-1@integration.interchange>",
        grants: [RUN_TOOL_GRANT],
      });

      const runId = await waitForFirstRunId(env, workflowRunRepoId, {
        diagnostics: env.sidecarDiagnostics,
        timeoutMs: 20_000,
      });

      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        runId,
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );
      if (terminal.type !== "RunCompleted") {
        const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
        const failed = events.find(
          (e) => e.type === "StepFailed" || e.type === "RunFailed",
        );
        throw new Error(
          `expected RunCompleted, got ${terminal.type}: ${JSON.stringify(failed?.body)}\n${env.sidecarDiagnostics()}`,
        );
      }
      expect(terminal.type).toBe("RunCompleted");

      // The model was driven to call the tool on its first turn, then the
      // tool_result fed a second inference turn. Two (or more) requests
      // means the tool executed and the agent looped back -- the tool did
      // not silently no-op.
      expect(env.inference.requests.length).toBeGreaterThanOrEqual(2);

      // The first request must have exposed the inline tool to the model --
      // proof the source closure evaluated in the child and the factory's
      // definition reached inference.
      const firstReq = env.inference.requests[0];
      if (firstReq === undefined)
        throw new Error("no inference request captured");
      const toolNames = (firstReq.tools ?? []).map((t) => t.name);
      expect(toolNames).toContain(MAIL_TOOL_NAME);

      // THE PROOF that the tool ran IN THE CHILD: the tool's `run` wrote a
      // sentinel file into `env.workdir`, which for the warm single-step
      // agent is the STABLE per-agent workspace rooted at
      // `workflow-step-state/<repoId>/warm/<stepId>/workspace` (keyed by the
      // step identity, not the per-message runId, so the workspace is reused
      // across messages and bounded to one dir per agent). The file's
      // presence (with the content the tool was given) means the source
      // workflow's OWN evaluated tool factory ran in the child's filesystem
      // view -- and that the `tool:<name>` grant authorized the call.
      const stepWorkspace = path.join(
        env.sidecar.dataDir,
        "workflow-step-state",
        workflowRunRepoId.id,
        "warm",
        encodeURIComponent(STEP_ID),
        "workspace",
      );
      const sentinelPath = path.join(stepWorkspace, SENTINEL_FILENAME);
      if (!fs.existsSync(sentinelPath)) {
        throw new Error(
          `tool sentinel file ${sentinelPath} was not written; the source workflow's own tool did not run in the child\n${env.sidecarDiagnostics()}`,
        );
      }
      expect(fs.readFileSync(sentinelPath, "utf-8")).toBe(SENTINEL_CONTENT);
    });
  },
);

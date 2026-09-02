// CP-tools verify artifact for a source-closure tool + whole-workflow failover.
//
// Proves a source-ref single-step workflow whose step agent carries its own
// inline `mail_send` tool (from the `mail-tool.ts` fixture, bundled into the
// workflow's source closure) materializes and runs that tool in the spawned
// child, and preserves whole-workflow failover.
//
// It folds in the failover check: the deploy's per-step source chain is a dead
// head (HTTP 500) followed by the healthy mock, both operator-approved. The
// child's reactor fails over forward off the dead head to the healthy tail --
// which drives the tool call -- so a single run proves (1) the agent's own
// source-closure tool materialized and ran in-child, and (2) whole-workflow
// failover is preserved for a single-step agent.

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
import { singleStepAgentEntry } from "./fixtures/single-step-agent";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_folded-tools-failover-1";
const STEP_ID = "step1";
const AGENT_ID = "agent-folded-tools-failover";

const SENTINEL_FILENAME = "folded-tool-ran.txt";
const SENTINEL_CONTENT = "executed-in-child";

// The run's tool grant, delivered per run via the `run.grants` frame the
// trigger sends, authorizing the inline tool call in the child.
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
const TENANT_ID = "tnt_folded_tools_failover";
const CALLER_PRINCIPAL_ID = "prn_folded_tools_failover";
const DEFINITION_ASSET_ID = "ast_folded_tools_failover_wf";

// The negative-control deploy's own identity, distinct from the failover
// deploy's so the two runs never collide on a substrate slug or DB row. Its
// definition asset is seeded alongside the failover one in `beforeAll`; the
// tenant and caller principal are shared.
const NEGCTL_DEPLOYMENT_ID = "run_folded-tools-failover-negctl-1";
const NEGCTL_AGENT_ID = "agent-folded-tools-failover-negctl";
const NEGCTL_DEFINITION_ASSET_ID = "ast_folded_tools_failover_negctl_wf";

// The healthy tail's text reply for an empty tool set. The negative control
// deploys with the dead head as the SOLE source, so the healthy mock is never
// reached; the synthesized provider-error reply it lands is NOT this string.
const HEALTHY_TEXT_REPLY = "I see these tools:";

let env: DeployFlowEnv;
let h: TestDb;
// The dead head source: always HTTP 500 (a retryable category) so the child's
// reactor fails over off it. Owned here, not by the deploy-flow fixture.
let headRequests = 0;
let deadHead: ReturnType<typeof Bun.serve>;

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
  await seedAsset(h.db, {
    id: DEFINITION_ASSET_ID,
    tenantId: TENANT_ID,
    kind: "workflow",
    name: "folded-tools-failover-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });
  await seedAsset(h.db, {
    id: NEGCTL_DEFINITION_ASSET_ID,
    tenantId: TENANT_ID,
    kind: "workflow",
    name: "folded-tools-failover-negctl-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv({
    inferenceToolCall: {
      toolName: MAIL_TOOL_NAME,
      input: { to: SENTINEL_CONTENT, body: SENTINEL_FILENAME },
    },
  });
  deadHead = Bun.serve({
    port: 0,
    fetch() {
      headRequests += 1;
      return new Response("upstream boom", { status: 500 });
    },
  });
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (deadHead !== undefined) await deadHead.stop(true);
  if (h !== undefined) await h.close();
});

describe.skipIf(!harnessDbEnvAvailable())(
  "source-closure tool + failover real-agent round-trip",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("a source-closure agent materializes its tool in-child and fails over to the healthy source", async () => {
      const deploymentMailAddress = deriveRunAddress({
        runId: DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });

      // A two-element failover chain: element 0 is the dead head (default), the
      // tail is the healthy mock. Both share the agent's declared (provider,
      // model), so the one inference-source approval covers both.
      const deadSource: InferenceSource = {
        id: "anthropic:dead-head",
        provider: "anthropic",
        baseURL: `http://localhost:${deadHead.port}`,
        credentialId: "sk-dead",
        model: "mock-model",
      };
      const healthySource: InferenceSource = {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${env.inference.server.port}`,
        credentialId: "sk-mock",
        model: "mock-model",
      };

      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentId: `${DEPLOYMENT_ID}`,
        tenantId: "tenant-1",
        principalId: "prin_folded-1",
        agentAddress: deploymentMailAddress,
        systemPrompt: "Fallback prompt (overridden per step by the definition)",
        tools: [],
        grants: [],
        sources: [deadSource, healthySource],
        defaultSource: "anthropic:dead-head",
      };

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
        systemPrompt: "You are the folded single-step tool agent.",
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
        // The per-step failover chain: dead head first, healthy tail second.
        sources: { [STEP_ID]: [deadSource, healthySource] },
      });
      expect(handle.publicKey).toBeTruthy();

      const workflowRunRepoId = handle.workflowRunRepoId;

      // The source-ref frame round-trips through the real sidecar subprocess,
      // so routability is asynchronous. Wait for it before firing the trigger.
      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<folded-tools-failover-1@integration.interchange>",
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
        {
          timeoutMs: 20_000,
          diagnostics: env.sidecarDiagnostics,
        },
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

      // Failover happened: the dead head was tried (and 500'd) before the
      // reactor advanced to the healthy tail that served the tool call.
      expect(headRequests).toBeGreaterThanOrEqual(1);
      expect(env.inference.requests.length).toBeGreaterThanOrEqual(2);

      // The materialized tool reached inference -- proof the source closure
      // evaluated in-child and the factory's definition reached inference.
      const firstServed = env.inference.requests[0];
      if (firstServed === undefined) {
        throw new Error("no inference request captured");
      }
      const toolNames = (firstServed.tools ?? []).map((t) => t.name);
      expect(toolNames).toContain(MAIL_TOOL_NAME);

      // THE PROOF the tool ran in-child: its `run` wrote a sentinel file into
      // the step agent's stable per-agent workspace.
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
          `tool sentinel file ${sentinelPath} was not written; the source-closure agent's tool did not materialize in the child\n${env.sidecarDiagnostics()}`,
        );
      }
      expect(fs.readFileSync(sentinelPath, "utf-8")).toBe(SENTINEL_CONTENT);
    });

    test("a chain with no healthy source completes with a synthesized error reply", async () => {
      // Negative control grounding the failover test above: with the dead head
      // as the SOLE source there is nothing to fail over to, yet the run still
      // lands `RunCompleted` -- the agent synthesizes a graceful provider-error
      // reply rather than failing the run. This is why the failover test cannot
      // lean on the terminal type and proves failover through the reply instead.
      const deploymentMailAddress = deriveRunAddress({
        runId: NEGCTL_DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });

      // The sole source is the dead head (HTTP 500). Its declared (provider,
      // model) is the agent's declared pair, so the one inference-source
      // approval covers it -- the deploy is well-formed, only the runtime
      // inference fails.
      const deadSource: InferenceSource = {
        id: "anthropic:dead-head",
        provider: "anthropic",
        baseURL: `http://localhost:${deadHead.port}`,
        credentialId: "sk-dead",
        model: "mock-model",
      };

      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentId: `${NEGCTL_DEPLOYMENT_ID}`,
        tenantId: "tenant-1",
        principalId: "prin_folded-negctl-1",
        agentAddress: deploymentMailAddress,
        systemPrompt: "Fallback prompt (overridden per step by the definition)",
        tools: [],
        grants: [],
        sources: [deadSource],
        defaultSource: "anthropic:dead-head",
      };

      const operatorApprovals: ApprovalSet = new Set<string>([
        "inference.source:anthropic:mock-model",
        "director:@intx/agent/default",
        `mail.address:${deploymentMailAddress}`,
        `mail.send:${DEPLOYMENT_DOMAIN}`,
      ]);

      // A tool-less single-step agent: the negative control is about the
      // inference-failure contract, not tool materialization.
      const entryModule = singleStepAgentEntry({
        stepId: STEP_ID,
        systemPrompt: "You are the folded failover negative-control agent.",
        address: deploymentMailAddress,
        agentId: NEGCTL_AGENT_ID,
      });

      const handle = await deployWorkflowSourceForTest(env, {
        entryModule,
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: NEGCTL_DEFINITION_ASSET_ID,
        anchorRunId: NEGCTL_DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: deploymentMailAddress,
        approvals: operatorApprovals,
        config,
        sources: { [STEP_ID]: [deadSource] },
      });
      expect(handle.publicKey).toBeTruthy();

      const workflowRunRepoId = handle.workflowRunRepoId;

      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<folded-tools-failover-negctl-1@integration.interchange>",
      });

      const runId = await waitForFirstRunId(env, workflowRunRepoId, {
        diagnostics: env.sidecarDiagnostics,
        timeoutMs: 20_000,
      });

      const terminal = await waitForWorkflowRunComplete(
        env,
        NEGCTL_DEPLOYMENT_ID,
        runId,
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );
      // Total inference failure still completes the run.
      expect(terminal.type).toBe("RunCompleted");

      const events = await readWorkflowRunEvents(
        env,
        NEGCTL_DEPLOYMENT_ID,
        runId,
      );
      const stepCompleted = events.find(
        (e) => e.type === "StepCompleted" && e.body["stepId"] === STEP_ID,
      );
      if (stepCompleted === undefined) {
        throw new Error("missing StepCompleted for the sole-dead-source step");
      }

      // The reply is the synthesized error, NOT the healthy tail's output --
      // exactly the outcome the failover test's success relies on ruling out.
      const reply = readStepReply(stepCompleted.body);
      expect(reply).not.toBe(HEALTHY_TEXT_REPLY);
    });
  },
);

/**
 * Extract the agent's reply string from a `StepCompleted` event body. A small
 * `{ reply, turn }` output inlines as `inline:<json>`.
 */
function readStepReply(body: Record<string, unknown>): string {
  const output = body["output"];
  if (typeof output !== "object" || output === null || !("ref" in output)) {
    throw new Error(
      `StepCompleted output is not a { ref } record: ${JSON.stringify(output)}`,
    );
  }
  const ref: unknown = output.ref;
  if (typeof ref !== "string") {
    throw new Error(`StepCompleted output ref is not a string: ${String(ref)}`);
  }
  const INLINE_PREFIX = "inline:";
  if (!ref.startsWith(INLINE_PREFIX)) {
    throw new Error(`expected an inline output ref, got ${ref}`);
  }
  const parsed: unknown = JSON.parse(ref.slice(INLINE_PREFIX.length));
  if (typeof parsed !== "object" || parsed === null || !("reply" in parsed)) {
    throw new Error(
      `step output does not carry a reply field: ${JSON.stringify(parsed)}`,
    );
  }
  const reply: unknown = parsed.reply;
  if (typeof reply !== "string") {
    throw new Error(
      `step output reply is not a string: ${JSON.stringify(parsed)}`,
    );
  }
  return reply;
}

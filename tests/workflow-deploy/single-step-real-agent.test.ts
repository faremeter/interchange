// Single-step real-agent round-trip integration test.
//
// The proof that the spawned workflow-process child runs a REAL agent
// for a step, not the placeholder stub. Deploys a one-step workflow BY
// SOURCE-REF (bundle a source entry module into a hub asset, probe it,
// approve+freeze it against a real DB, deploy the source-ref frame) against
// the real hub + real sidecar subprocess + mock inference fixture, fires the
// deployment's mail trigger, and asserts the step's committed output carries
// the agent's deterministic inference reply produced by `agent.send` -- NOT
// the old stub value `req.agent.id`.
//
// The mock inference server returns a canned assistant reply built from
// the tool names it was handed (`I see these tools: <names>`); with an
// empty tool set the reply is the stable prefix `I see these tools: `.
// That deterministic reply is the test-provider seam this phase drives
// the real agent against, since real inference in CI is impractical.
//
// The test additionally asserts the per-step agent storage/workspace
// materialized under the sidecar data dir, rooted per run/step in a
// `workflow-step-state/` subtree that is a sibling of the workflow-run
// repo's git directory (where the run-event log lives), so the per-step
// store cannot clobber the run-event tree.

import fs from "node:fs";
import path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import { deriveRunAddress, type ApprovalSet } from "@intx/workflow-deploy";
import { reconstructDurableConversation } from "@intx/sidecar-app/src/conversation-state";
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
import { singleStepAgentEntry } from "./fixtures/single-step-agent";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_single-step-real-agent-1";
const STEP_ID = "step1";
const AGENT_ID = "agent-step1";

// The mock inference server's reply for an empty tool set. The server
// builds `I see these tools: <names>` from the tool names it was given;
// with no tools the names list is empty and the agent surfaces the
// reply with trailing whitespace trimmed.
const EXPECTED_REPLY = "I see these tools:";

// The definition's own tenant, the caller principal that creates the
// definition asset, and the `workflow`-kind asset the frozen definition
// projects over. The install/approve freeze and the anchor `workflow_run`
// insert both write against these, so they must exist in the real DB before
// the deploy runs.
const TENANT_ID = "tnt_single_step_real_agent";
const CALLER_PRINCIPAL_ID = "prn_single_step_real_agent";
const DEFINITION_ASSET_ID = "ast_single_step_real_agent_wf";

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
    name: "single-step-real-agent-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

describe.skipIf(!harnessDbEnvAvailable())(
  "single-step real-agent round-trip",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("spawned child runs a real agent and commits its reply as the step output", async () => {
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

      const operatorApprovals: ApprovalSet = new Set<string>([
        "inference.source:anthropic:mock-model",
        "director:@intx/agent/default",
        `mail.address:${deploymentMailAddress}`,
        `mail.send:${DEPLOYMENT_DOMAIN}`,
      ]);

      const entryModule = singleStepAgentEntry({
        stepId: STEP_ID,
        systemPrompt: "You are the single-step agent.",
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

      const { messageId } = await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<single-step-real-agent-1@integration.interchange>",
      });

      // Wait for the run to reach a terminal phase, then read its full
      // event log. The supervisor mints the runId from the inbound mail
      // bytes; the test discovers it by listing `runs/`.
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
      expect(terminal.type).toBe("RunCompleted");

      const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
      const runStartedBody = events.find((e) => e.type === "RunStarted")?.body;
      if (runStartedBody === undefined) throw new Error("missing RunStarted");
      expect(runStartedBody["consumedMessageId"]).toBe(messageId);

      const stepCompleted = events.find(
        (e) => e.type === "StepCompleted" && e.body["stepId"] === STEP_ID,
      );
      if (stepCompleted === undefined) {
        throw new Error("missing StepCompleted for the single step");
      }

      // The proof: the step output is the REAL agent reply from
      // `agent.send` (the mock provider's deterministic output), not the
      // old stub value `req.agent.id` (which would have been the agent's
      // definition id, "agent-step1").
      const reply = readStepReply(stepCompleted.body);
      expect(reply).toBe(EXPECTED_REPLY);
      expect(reply).not.toBe(AGENT_ID);

      // The agent's inference call actually reached the mock provider, so
      // the reply is real model output rather than a synthesized constant.
      expect(env.inference.requests.length).toBeGreaterThan(0);

      // The warm single-step agent's workspace/tools are rooted at a STABLE
      // per-agent path under `workflow-step-state/<repoId>/warm/<stepId>/`
      // (keyed by the step identity like the durable conversation store, NOT
      // the per-message runId) so the cached agent reuses one workspace
      // across messages and the scratch is bounded to one dir per agent
      // rather than leaking a fresh per-run subtree. The subtree is a sibling
      // of the workflow-run repo's git directory; the run-event log lives
      // inside the workflow-run repo's own tree, so the warm root cannot
      // overlap it.
      const stepStoreDir = path.join(
        env.sidecar.dataDir,
        "workflow-step-state",
        workflowRunRepoId.id,
        "warm",
        encodeURIComponent(STEP_ID),
      );
      expect(fs.existsSync(stepStoreDir)).toBe(true);
      expect(fs.existsSync(path.join(stepStoreDir, "workspace"))).toBe(true);

      // The warm single-step agent's conversation ContextStore is durable
      // (Phase 4.5): its `.git` lives at the stable per-agent durable store
      // root, NOT under the per-run `attempt-1` dir, so the conversation
      // survives across runs and child respawn. The per-run dir therefore
      // carries the workspace + tool state but no conversation `.git`.
      const durableConversationDir = path.join(
        env.sidecar.dataDir,
        "agent-conversation-state",
        workflowRunRepoId.id,
        encodeURIComponent(STEP_ID),
      );
      expect(fs.existsSync(path.join(durableConversationDir, ".git"))).toBe(
        true,
      );

      // The conversation is mirrored through the real proxy -> supervisor
      // single-writer path to the workflow-run substrate at the per-agent
      // `agent-state/<stepId>/` path (Phase D1: a bucket-sharded WAL plus a
      // periodic checkpoint, no longer a single `conversation.json`), sibling
      // to the per-run event log under `runs/<runId>/...`. The supervisor's
      // substrate is the sidecar's on-disk workflow-run repo; reconstruct the
      // durable conversation from it (deterministic, no hub pack-push timing
      // dependency) and assert the agent's turn is durably committed.
      const sidecarWorkflowRunRepoDir = path.join(
        env.sidecar.dataDir,
        "workflow-runs",
        workflowRunRepoId.id,
      );
      const durableSubstrateAgentStateDir = path.join(
        sidecarWorkflowRunRepoDir,
        "agent-state",
        encodeURIComponent(STEP_ID),
      );
      const durableConversation = await reconstructDurableConversation(
        durableSubstrateAgentStateDir,
        STEP_ID,
      );
      if (durableConversation === null) {
        throw new Error("expected a durable conversation in the substrate");
      }
      expect(durableConversation.turns.length).toBeGreaterThan(0);

      // Neither the per-step root nor the durable conversation store lives
      // inside the workflow-run repo's working tree (where
      // `runs/<runId>/events/...` is committed).
      const workflowRunRepoDir =
        env.hub.agentRepoStore.repoStore.getRepoDir(workflowRunRepoId);
      expect(stepStoreDir.startsWith(workflowRunRepoDir)).toBe(false);
      expect(durableConversationDir.startsWith(workflowRunRepoDir)).toBe(false);
    });
  },
);

/**
 * Extract the agent's reply string from a `StepCompleted` event body.
 * The runtime records the step output through the blob substrate; a
 * small `{ reply, turn }` output inlines as `inline:<json>`, so the
 * reply is recovered by parsing the JSON after the `inline:` prefix.
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
    throw new Error(
      `expected an inline output ref for the small step output, got ${ref}`,
    );
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

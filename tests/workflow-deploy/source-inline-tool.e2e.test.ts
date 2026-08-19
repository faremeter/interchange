// A code-sourced workflow runs its OWN inline tool in-child.
//
// This is the proof that a source-ref-deployed workflow runs the tools
// declared in its own source, rather than tool-less. It deploys a one-step
// workflow BY SOURCE-REF whose agent carries the inline `mail_send` tool from
// the `mail-tool.ts` fixture (a real `defineTool` module bundled into the
// workflow's source closure), fires the deployment's mail trigger, and drives
// the mock inference server to call the inline tool.
//
// The evaluated closure carries the step agent's live `AnnotatedToolFactory`s
// on `req.agent.toolFactories`; the source-ref arm of the sidecar step
// build-env feeds them straight into the step agent (no pinned manifest is
// staged on this lineage). The tool's runtime name is the bare
// `definition.name`, which the probe's capability walk already emitted a
// `tool:<name>` grant for into the frozen credentials snapshot, so the run
// authorizes the call through the snapshot directly -- no tool-mark floor.
//
// Three assertions carry the proof that the tool RAN in-child, not tool-less:
//   1. the first inference request's tool list contains the inline tool's name
//      (the model saw the tool -- a tool-less run would send `tools: []`);
//   2. a second inference request landed (the tool_use -> tool_result
//      round-trip happened, so the tool executed and the agent looped back);
//   3. the tool's `run` wrote its sentinel file into the step agent's per-step
//      workspace under the sidecar data dir (the factory's `run` executed in
//      the child's filesystem view, and the call was authorized to run).

import fs from "node:fs";
import path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import { deriveRunAddress, type ApprovalSet } from "@intx/workflow-deploy";
import { loadFrozenGrantSnapshot } from "@intx/db";
import type { GrantEffect, GrantWalkSnapshot } from "@intx/types";
import type { WireGrantRule } from "@intx/types/grant-wire";
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

/**
 * Project a frozen grant-walk snapshot into the run's runtime `tool:`/`effect:`
 * grant rows, in the `run.grants` wire shape the trigger delivers. Mirrors the
 * production `deriveRunRuntimeGrantRows`/`runGrantToWire` tail (not exported
 * from `@intx/hub-api`): one row per distinct grant across steps, tool effect
 * taken from the step's `grantEffects` with `ask` winning over `allow`, effect
 * grants always `allow`. The rows are principal-agnostic (`principalId: null`),
 * matched by resource + action at the child's grant evaluator.
 */
function deriveWireRunGrants(snapshot: GrantWalkSnapshot): WireGrantRule[] {
  const effectByResource = new Map<string, GrantEffect>();
  for (const step of snapshot.perStep) {
    const grantEffects = new Map<string, GrantEffect>(
      Object.entries(step.grantEffects),
    );
    for (const grant of step.grants) {
      if (grant.startsWith("tool:")) {
        const effect = grantEffects.get(grant);
        if (effect === undefined) {
          throw new Error(
            `deriveWireRunGrants: tool grant ${JSON.stringify(grant)} has no grantEffects entry`,
          );
        }
        const existing = effectByResource.get(grant);
        if (existing === "ask" || effect === "ask") {
          effectByResource.set(grant, "ask");
        } else if (existing === undefined) {
          effectByResource.set(grant, effect);
        }
      } else if (grant.startsWith("effect:") && !effectByResource.has(grant)) {
        effectByResource.set(grant, "allow");
      }
    }
  }
  return [...effectByResource].map(([resource, effect]) => ({
    id: `run-grant:${resource}`,
    resource,
    action: "invoke",
    effect,
    origin: "creator",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: null,
  }));
}

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_source-inline-tool-1";
const STEP_ID = "step1";
const AGENT_ID = "source-inline-tool-agent";

// The `fs` variant of the inline mail tool writes a file named after its
// `body` arg with its `to` arg as content; the test drives those values and
// asserts the file lands in the step agent's per-step workspace.
const SENTINEL_FILENAME = "source-inline-tool-ran.txt";
const SENTINEL_CONTENT = "executed-in-child-from-source";

// The definition's own tenant, the caller principal that creates the
// definition asset, and the `workflow`-kind asset the frozen definition
// projects over. The install/approve freeze and the anchor `workflow_run`
// insert both write against these, so they must exist in the real DB before
// the deploy runs.
const TENANT_ID = "tnt_source_inline_tool";
const CALLER_PRINCIPAL_ID = "prn_source_inline_tool";
const DEFINITION_ASSET_ID = "ast_source_inline_tool_wf";

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
    name: "source-inline-tool-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  // Drive the mock inference server to call the inline tool on the first turn,
  // then reply with a text turn once the tool_result lands.
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
  "source-ref workflow runs its own inline tool in-child",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("the spawned child runs the workflow's own inline tool for the step", async () => {
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
      // into the credentials snapshot and is what authorizes the tool call at
      // run time -- the source path's tool authorization rides the snapshot, no
      // floor.
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
        systemPrompt: "You are the source inline-tool agent.",
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

      // The inline tool's authorization rides the frozen grant-walk snapshot,
      // not a sidecar floor: the probe's capability walk read the source
      // agent's `agent.toolFactories[].definitions[].name` and emitted a BARE
      // `tool:<name>` grant into the snapshot the operator approved and the
      // approve step froze onto the definition version row. Load it back and
      // assert the bare tool grant is present -- this is the property that
      // lets a source tool authorize with no floor (its runtime name is the
      // same bare `definition.name`).
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
      const snapshotToolGrants = snapshot.perStep.flatMap((s) =>
        s.grants.filter((g) => g.startsWith("tool:")),
      );
      expect(snapshotToolGrants).toContain(`tool:${MAIL_TOOL_NAME}`);

      // Project the frozen snapshot into the run's runtime grant rows the way
      // the production trigger route does (`deriveRunRuntimeGrantRows`), then
      // deliver them ahead of the trigger mail. `fireMailTrigger` is the
      // router-level helper, so unlike the production route it does not
      // materialize the run grants itself; feeding the snapshot-derived rows
      // here reproduces the production delivery. The grant the tool call
      // authorizes against therefore originates from the probe walk, not a
      // hand-authored constant.
      const runGrants = deriveWireRunGrants(snapshot);

      // The source-ref frame round-trips through the real sidecar subprocess
      // (index the pack, check out the pinned subtree, register the address),
      // so routability is asynchronous. Wait for it before firing the trigger.
      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<source-inline-tool-1@integration.interchange>",
        grants: runGrants,
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

      // Proof 1: the model was handed the inline tool. A tool-less run -- the
      // pre-change behavior for a source deploy -- would send `tools: []`, so a
      // non-empty list carrying the inline tool's name is the direct
      // disproof of tool-less.
      const firstReq = env.inference.requests[0];
      if (firstReq === undefined) {
        throw new Error("no inference request captured");
      }
      const toolNames = (firstReq.tools ?? []).map((t) => t.name);
      expect(toolNames).toContain(MAIL_TOOL_NAME);

      // Proof 2: the tool_use -> tool_result round-trip happened. The mock
      // emitted a `tool_use` on the first turn, the tool ran and appended a
      // tool_result, and the agent looped back for a second inference turn.
      expect(env.inference.requests.length).toBeGreaterThanOrEqual(2);

      // Proof 3: the tool's `run` executed IN THE CHILD's filesystem view. The
      // inline tool wrote its sentinel into `env.workdir`, which for the warm
      // single-step agent is the stable per-agent workspace rooted at
      // `workflow-step-state/<repoId>/warm/<stepId>/workspace`. The file's
      // presence (with the content the tool was given) means the source
      // workflow's OWN evaluated tool factory ran -- and that the `tool:<name>`
      // grant authorized the call through the frozen snapshot.
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
          `inline tool sentinel file ${sentinelPath} was not written; the source workflow's own tool did not run in the child\n${env.sidecarDiagnostics()}`,
        );
      }
      expect(fs.readFileSync(sentinelPath, "utf-8")).toBe(SENTINEL_CONTENT);
    });
  },
);

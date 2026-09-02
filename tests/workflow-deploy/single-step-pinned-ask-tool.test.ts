// End-to-end proof that an ask-marked tool authorizes on its own static
// mark and suspends for approval.
//
// A source-ref workflow's agent carries the inline `mail_send` tool from the
// `mail-tool.ts` fixture in its `ask` variant, whose static tool definition
// carries `approval: "ask"`. The deploy-time capability walk reads that mark
// and freezes a `tool:<name>` grant whose effect is `ask` into the credentials
// snapshot; the run's per-run grants (delivered on the trigger frame) carry
// that same `ask` effect. When the model calls the tool, the child's authorize
// resolves the `ask` effect and the call SUSPENDS.
//
// The proof: the run SUSPENDS (a `SignalAwaited` event lands on the
// workflow-run log) rather than completing or failing. That outcome is the
// discriminator across the three possibilities:
//   - silent allow -> the tool would run and the run would complete;
//   - deny -> the call would be blocked and the step would fail;
//   - ask -> the call suspends awaiting approval.
// The frozen snapshot's `ask` effect for the tool proves the suspend derives
// from the tool's own static mark, not a hand-set constant. The tool must NOT
// have run (no sentinel), and no terminal event may land while the step is
// parked.

import fs from "node:fs";
import path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import type { WireGrantRule } from "@intx/types/grant-wire";
import { deriveRunAddress, type ApprovalSet } from "@intx/workflow-deploy";
import { loadFrozenGrantSnapshot } from "@intx/db";
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
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { MAIL_TOOL_NAME } from "./fixtures/mail-tool";
import { singleStepMailToolEntry } from "./fixtures/single-step-mail-tool";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_single-step-pinned-ask-tool-1";
const STEP_ID = "step1";
const AGENT_ID = "agent-step1";

const SENTINEL_FILENAME = "ask-tool-ran.txt";
const SENTINEL_CONTENT = "should-not-run-until-approved";

// The definition's own tenant, the caller principal that creates the
// definition asset, and the `workflow`-kind asset the frozen definition
// projects over. The install/approve freeze and the anchor `workflow_run`
// insert both write against these, so they must exist in the real DB before
// the deploy runs.
const TENANT_ID = "tnt_single_step_pinned_ask_tool";
const CALLER_PRINCIPAL_ID = "prn_single_step_pinned_ask_tool";
const DEFINITION_ASSET_ID = "ast_single_step_pinned_ask_tool_wf";

let env: DeployFlowEnv;
let h: TestDb;

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
    name: "single-step-pinned-ask-tool-wf",
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

describe.skipIf(!harnessDbEnvAvailable())("single-step ask-marked tool", () => {
  test("sidecar registers with hub", () => {
    expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
  });

  test("suspends for approval on the ask effect derived from the tool's static mark", async () => {
    const deploymentMailAddress = deriveRunAddress({
      runId: DEPLOYMENT_ID,
      domain: DEPLOYMENT_DOMAIN,
    });

    const inferenceSource: InferenceSource = {
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
      `tool:${MAIL_TOOL_NAME}`,
    ]);

    const entryModule = singleStepMailToolEntry({
      variant: "ask",
      stepId: STEP_ID,
      systemPrompt: "You are the single-step ask-tool agent.",
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

    // The tool's suspend authority derives from its own static mark. The
    // probe's capability walk read the ask-marked `mail_send` definition and
    // froze a `tool:<name>` grant whose EFFECT is `ask` into the snapshot the
    // operator approved. Load it back and assert the tool grant carries the
    // `ask` effect -- this is what makes the tool call suspend rather than
    // run, and it originates from the tool's mark, not a hand-authored
    // constant.
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
    const askEffect = snapshot.perStep
      .map((s) => s.grantEffects[`tool:${MAIL_TOOL_NAME}`])
      .find((effect) => effect !== undefined);
    expect(askEffect).toBe("ask");

    // Deliver the run's tool grant carrying the snapshot's `ask` effect, the
    // way the production trigger route projects the frozen snapshot into
    // per-run grants. `fireMailTrigger` does not materialize run grants
    // itself, so feeding the ask-effect grant here reproduces the production
    // delivery; the call the model issues resolves against it and suspends.
    const askRunGrant: WireGrantRule = {
      id: `run-grant:tool:${MAIL_TOOL_NAME}`,
      resource: `tool:${MAIL_TOOL_NAME}`,
      action: "invoke",
      effect: "ask",
      origin: "creator",
      conditions: null,
      expiresAt: null,
      roleId: null,
      principalId: null,
    };

    // The source-ref frame round-trips through the real sidecar subprocess,
    // so routability is asynchronous. Wait for it before firing the trigger.
    await waitFor(
      () =>
        env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
      { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
    );

    await fireMailTrigger(env, deploymentMailAddress, {
      messageId: "<single-step-pinned-ask-tool-1@integration.interchange>",
      grants: [askRunGrant],
    });

    const runId = await waitForFirstRunId(env, workflowRunRepoId, {
      diagnostics: env.sidecarDiagnostics,
      timeoutMs: 20_000,
    });

    // The run parks on the tool's approval gate: a `SignalAwaited` event
    // lands on the workflow-run log. It reaches the hub through the
    // pack-push pipeline, so wait for it rather than racing the push. Only
    // an `ask` effect produces this outcome -- an `allow` would run the tool
    // and complete, a `deny` would fail the step.
    await waitFor(
      async () => {
        const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
        return events.some((e) => e.type === "SignalAwaited");
      },
      { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
    );

    const parkedEvents = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
    const parkedTypes = parkedEvents.map((e) => e.type);
    // Suspended, not silently allowed and not denied: no terminal event has
    // landed while the step is parked awaiting approval.
    expect(parkedTypes).not.toContain("RunCompleted");
    expect(parkedTypes).not.toContain("RunFailed");
    expect(parkedTypes).not.toContain("RunCancelled");

    // The tool has NOT run: the `ask` effect suspended the call before
    // execution. The sentinel would only appear if the tool executed.
    const stepWorkspace = path.join(
      env.sidecar.dataDir,
      "workflow-step-state",
      workflowRunRepoId.id,
      "warm",
      encodeURIComponent(STEP_ID),
      "workspace",
    );
    expect(fs.existsSync(path.join(stepWorkspace, SENTINEL_FILENAME))).toBe(
      false,
    );
  });
});

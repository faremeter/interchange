// Deployed behaviour of a gate inside a `childWorkflow` that nothing can
// answer.
//
// A terminal child has no address of its own and no container relaying
// decisions down to it, so an untimed gate there waits on a signal that can
// never arrive. Before this was refused, the deployment went quiet: the child
// held the gate, the spawn step held the child, the run never terminated, and
// because the suspension was never registered with the hub, no approval
// appeared for the tenant. An operator had nothing to act on and no error to
// read.
//
// What this asserts against a real hub and sidecar is the half that can
// regress: the run reaches a terminal naming the gate, where before it went
// quiet and stayed that way.
//
// Both park shapes are covered: an author-named gate, and the approval-shaped
// park the issue actually reports, where the child's tool call suspends on the
// control plane.
//
// Neither case asserts that no approval reached an operator, and the reason is
// worth recording. A terminal child's env carries no notify sink at all, so no
// park there registers a correlation whether it is refused or not. An
// assertion to that effect could not fail, and would read as a guarantee this
// suite does not provide. What it does assert is that no durable suspension is
// recorded and the run ends -- which is what separates the fixed behaviour
// from the reported one.
//
// A timed gate is the boundary case and must still work: its own timer
// resolves it with no upstream involvement.
//
// Harness justification: SPAWN-REAL. Real hub + sidecar subprocess + mock
// inference.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import {
  createApprovalSet,
  deriveRunAddress,
  type ApprovalSet,
} from "@intx/workflow-deploy";
import { tenant as tenantTable } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedPrincipal } from "@intx/test-harness/seed";
import { WireGrantRule } from "@intx/types/grant-wire";
import { MAIL_TOOL_NAME } from "./fixtures/mail-tool";
import type { RepoId } from "@intx/hub-sessions";

import {
  SESSION_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  listRunIds,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { childWorkflowUnanswerableGateEntry } from "./fixtures/childworkflow-unanswerable-gate";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const SIGNAL_NAME = "approve-the-child";

const TENANT_ID = "tnt_cw_unanswerable_gate";
const CALLER_PRINCIPAL_ID = "prn_cw_unanswerable_gate";
const UNANSWERABLE_ANCHOR = "run_cw-unanswerable-gate-1";
const TIMED_ANCHOR = "run_cw-timed-gate-1";
const ASK_ANCHOR = "run_cw-ask-tool-1";

// Effect `ask` is the point: the child's tool call suspends on the control
// plane instead of running, which is the park shape the issue reports.
const ASK_GRANT: WireGrantRule = {
  id: "grant-tool-ask",
  resource: `tool:${MAIL_TOOL_NAME}`,
  action: "invoke",
  effect: "ask",
  origin: "creator",
  conditions: null,
  expiresAt: null,
  roleId: null,
  principalId: null,
};
const DEFINITION_ASSET_IDS: Record<string, string> = {
  [UNANSWERABLE_ANCHOR]: "ast_cw_unanswerable_gate_wf",
  [TIMED_ANCHOR]: "ast_cw_timed_gate_wf",
  [ASK_ANCHOR]: "ast_cw_ask_tool_wf",
};

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
  for (const [anchorRunId, definitionAssetId] of Object.entries(
    DEFINITION_ASSET_IDS,
  )) {
    await seedAsset(h.db, {
      id: definitionAssetId,
      tenantId: TENANT_ID,
      kind: "workflow",
      name: `cw-gate-wf-${anchorRunId}`,
      creatorPrincipalId: CALLER_PRINCIPAL_ID,
    });
  }

  env = await startDeployFlowEnv({
    inferenceApprovalToolCall: {
      toolName: MAIL_TOOL_NAME,
      input: { to: "someone@example.com", body: "hello" },
      resultPrefix: "sent: ",
    },
  });
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

/**
 * The top-level container run: not the child, whose run id is the child's
 * workflow id. Polled, because firing the trigger only queues the run.
 */
async function topLevelRunId(
  workflowRunRepoId: RepoId,
  childWorkflowId: string,
): Promise<string> {
  let found: string | undefined;
  await waitFor(
    async () => {
      found = (await listRunIds(env, workflowRunRepoId)).find(
        (id) => !id.includes("__") && id !== childWorkflowId,
      );
      return found !== undefined;
    },
    { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
  );
  if (found === undefined)
    throw new Error("no top-level run after the trigger");
  return found;
}

/** The child's run id, as the parent recorded it when it spawned the child. */
async function spawnedChildRunId(
  anchorRunId: string,
  parentRunId: string,
): Promise<string> {
  let childRunId: string | undefined;
  await waitFor(
    async () => {
      const events = await readWorkflowRunEvents(env, anchorRunId, parentRunId);
      const spawned = events.find((e) => e.type === "ChildSpawned");
      const id = spawned?.body["childRunId"];
      if (typeof id === "string") childRunId = id;
      return childRunId !== undefined;
    },
    { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
  );
  if (childRunId === undefined)
    throw new Error("no ChildSpawned on the parent");
  return childRunId;
}

async function deployGateWorkflow(opts: {
  anchorRunId: string;
  timedGate: boolean;
  askTool?: boolean;
}): Promise<{ workflowRunRepoId: RepoId; childWorkflowId: string }> {
  const { anchorRunId } = opts;
  const childWorkflowId = `wf_child_${anchorRunId}`;
  const deploymentMailAddress = deriveRunAddress({
    runId: anchorRunId,
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
    agentId: anchorRunId,
    tenantId: "tenant-1",
    principalId: `prin_${anchorRunId}`,
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
    `tool:${MAIL_TOOL_NAME}`,
  ]);

  const definitionAssetId = DEFINITION_ASSET_IDS[anchorRunId];
  if (definitionAssetId === undefined) {
    throw new Error(`no definition asset seeded for ${anchorRunId}`);
  }
  const handle = await deployWorkflowSourceForTest(env, {
    entryModule: childWorkflowUnanswerableGateEntry({
      address: deploymentMailAddress,
      workflowId: `wf_${anchorRunId}`,
      childWorkflowId,
      signalName: SIGNAL_NAME,
      ...(opts.timedGate ? { timedGate: true } : {}),
      ...(opts.askTool === true ? { askTool: true } : {}),
    }),
    db: h.db,
    tenantId: TENANT_ID,
    definitionAssetId,
    anchorRunId,
    deploymentDomain: DEPLOYMENT_DOMAIN,
    agentAddress: deploymentMailAddress,
    approvals: operatorApprovals,
    config,
  });
  expect(handle.publicKey).toBeTruthy();

  await waitFor(
    () => env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
    { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
  );
  await fireMailTrigger(env, deploymentMailAddress, {
    messageId: `<${anchorRunId}@integration.interchange>`,
    ...(opts.askTool === true ? { grants: [ASK_GRANT] } : {}),
  });

  return { workflowRunRepoId: handle.workflowRunRepoId, childWorkflowId };
}

describe.skipIf(!harnessDbEnvAvailable())(
  "a childWorkflow gate with nothing upstream to answer it",
  () => {
    test("fails the run at the gate instead of going quiet", async () => {
      const { workflowRunRepoId, childWorkflowId } = await deployGateWorkflow({
        anchorRunId: UNANSWERABLE_ANCHOR,
        timedGate: false,
      });

      const runId = await topLevelRunId(workflowRunRepoId, childWorkflowId);

      // Before the refusal this never arrived: the run simply sat there.
      const terminal = await waitForWorkflowRunComplete(
        env,
        UNANSWERABLE_ANCHOR,
        runId,
        { timeoutMs: 60_000, diagnostics: env.sidecarDiagnostics },
      );
      expect(terminal.type).toBe("RunFailed");

      // The child failed at its own gate, naming the signal, so an author
      // reading the child's log learns which gate could not be held.
      const childRunId = await spawnedChildRunId(UNANSWERABLE_ANCHOR, runId);
      const childEvents = await readWorkflowRunEvents(
        env,
        UNANSWERABLE_ANCHOR,
        childRunId,
      );
      const childFailure = childEvents.find((e) => e.type === "StepFailed");
      expect(JSON.stringify(childFailure?.body ?? {})).toContain(SIGNAL_NAME);
    }, 120_000);

    test("refuses an approval park, so no approval reaches the tenant", async () => {
      const { workflowRunRepoId, childWorkflowId } = await deployGateWorkflow({
        anchorRunId: ASK_ANCHOR,
        timedGate: false,
        askTool: true,
      });

      const runId = await topLevelRunId(workflowRunRepoId, childWorkflowId);

      const terminal = await waitForWorkflowRunComplete(
        env,
        ASK_ANCHOR,
        runId,
        {
          timeoutMs: 60_000,
          diagnostics: env.sidecarDiagnostics,
        },
      );
      expect(terminal.type).toBe("RunFailed");

      // Nothing durable was left claiming the child is still waiting. This is
      // the assertion that distinguishes the two outcomes: without the refusal
      // the child commits this and waits on it forever.
      const childRunId = await spawnedChildRunId(ASK_ANCHOR, runId);
      const childTypes = (
        await readWorkflowRunEvents(env, ASK_ANCHOR, childRunId)
      ).map((e) => e.type);
      expect(childTypes).not.toContain("SignalAwaited");
    }, 120_000);

    test("still runs a timed gate, which resolves without anything upstream", async () => {
      const { workflowRunRepoId, childWorkflowId } = await deployGateWorkflow({
        anchorRunId: TIMED_ANCHOR,
        timedGate: true,
      });

      const runId = await topLevelRunId(workflowRunRepoId, childWorkflowId);

      const terminal = await waitForWorkflowRunComplete(
        env,
        TIMED_ANCHOR,
        runId,
        { timeoutMs: 60_000, diagnostics: env.sidecarDiagnostics },
      );
      expect(terminal.type).toBe("RunCompleted");

      // It genuinely parked and was released by its own timer, rather than
      // being refused at the gate.
      const childRunId = await spawnedChildRunId(TIMED_ANCHOR, runId);
      const childTypes = (
        await readWorkflowRunEvents(env, TIMED_ANCHOR, childRunId)
      ).map((e) => e.type);
      expect(childTypes).toContain("SignalAwaited");
      expect(childTypes).toContain("TimerFired");
    }, 120_000);
  },
);

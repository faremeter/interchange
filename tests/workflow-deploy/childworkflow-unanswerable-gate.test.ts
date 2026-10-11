// Deployed behaviour of a park inside a `childWorkflow`.
//
// The child stays unaddressable. The parent relays each park onto the
// deployment run, which is the run an operator addresses. An author-named
// gate is delivered there. An approval-marked tool call registers a hub
// approval on that same run, carrying the body's snapshot, and approving it
// through the real HTTP route resumes the child. A timed gate still settles
// on its own timer.
//
// Harness justification: SPAWN-REAL. Real hub + sidecar subprocess + mock
// inference. The approval case wires the fixture hub's register lookup to
// the real DB co-write and approves through the real route.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { and, eq, inArray } from "drizzle-orm";

import { createInMemoryGrantStore } from "@intx/authz";
import {
  createApprovalStore,
  createSignalCorrelationStore,
  createWorkflowRunStore,
} from "@intx/db";
import {
  approval,
  tenant as tenantTable,
  workflowDefinition,
  workflowRun,
} from "@intx/db/schema";
import { createApp, type GetSession } from "@intx/hub-api";
import { generateId } from "@intx/hub-common";
import type {
  EventCollectorRegistry,
  RepoId,
  SessionService,
} from "@intx/hub-sessions";
import { signalName } from "@intx/types";
import type { GrantRule } from "@intx/types/authz";
import { WireGrantRule } from "@intx/types/grant-wire";
import type {
  ApprovalSnapshot,
  HarnessConfig,
  InferenceSource,
} from "@intx/types/runtime";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedPrincipal,
  tenantSlugFromId,
} from "@intx/test-harness/seed";
import {
  createApprovalSet,
  deriveRunAddress,
  deriveWorkflowRunRepoId,
  type ApprovalSet,
} from "@intx/workflow-deploy";

import {
  SESSION_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  injectSignal,
  listRunIds,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { childWorkflowUnanswerableGateEntry } from "./fixtures/childworkflow-unanswerable-gate";
import { MAIL_TOOL_NAME } from "./fixtures/mail-tool";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const SIGNAL_NAME = "approve-the-child";

const TENANT_ID = "tnt_cw_unanswerable_gate";
const CALLER_PRINCIPAL_ID = "prn_cw_unanswerable_gate";
const APPROVER_USER_ID = "usr_cw_approver";
const APPROVER_PRINCIPAL_ID = "prn_cw_approver";
const DEFINITION_ID = "wfd_cw_unanswerable_gate";
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
// The approve route authorizes `approval:<anchorRunId>` / `resolve`. The
// anchor here is the deployment the ask-tool run is deployed as.
const approverGrant: GrantRule = {
  id: "grant-approver-resolve",
  resource: `approval:${ASK_ANCHOR}`,
  action: "resolve",
  effect: "allow",
  origin: "system",
  conditions: null,
  expiresAt: null,
  roleId: null,
  principalId: APPROVER_PRINCIPAL_ID,
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
    slug: tenantSlugFromId(TENANT_ID),
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
  const askAssetId = DEFINITION_ASSET_IDS[ASK_ANCHOR];
  if (askAssetId === undefined) {
    throw new Error("ask definition asset was not seeded");
  }
  await h.db.insert(workflowDefinition).values({
    id: DEFINITION_ID,
    tenantId: TENANT_ID,
    name: "cw-unanswerable-gate",
    assetId: askAssetId,
  });
  await seedPrincipal(h.db, {
    id: APPROVER_PRINCIPAL_ID,
    tenantId: TENANT_ID,
    kind: "user",
    refId: APPROVER_USER_ID,
    status: "active",
  });

  env = await startDeployFlowEnv({
    inferenceApprovalToolCall: {
      toolName: MAIL_TOOL_NAME,
      input: { to: "someone@example.com", body: "hello" },
      resultPrefix: "sent: ",
    },
    registerSignalCorrelation: createRegisterSignalCorrelation(h.db),
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

function notImpl(name: string): never {
  throw new Error(`childWorkflow relay mock: ${name} not implemented`);
}

function createMockGetSession(userId: string): GetSession {
  const now = new Date("2025-01-01");
  return async () => ({
    user: {
      id: userId,
      email: "approver@example.com",
      emailVerified: true,
      name: "Approver",
      createdAt: now,
      updatedAt: now,
    },
    session: {
      id: "session_cw_gate",
      userId,
      token: "tok_cw_gate",
      expiresAt: new Date("2999-01-01"),
      createdAt: now,
      updatedAt: now,
    },
  });
}

function createMockSessionService(): SessionService {
  return {
    stageWorkflowStep: () => notImpl("stageWorkflowStep"),
    endSession: () => notImpl("endSession"),
  };
}

function createMockEventCollectors(): EventCollectorRegistry {
  return {
    create: () => notImpl("create"),
    dispatch: () => notImpl("dispatch"),
    abandon: () => notImpl("abandon"),
    has: () => false,
    getStatus: () => undefined,
    getAccumulatedText: () => undefined,
    getCurrentTurnId: () => undefined,
    getLastTurnId: () => undefined,
  };
}

// The real hub co-write. The parked run's register frame lands the
// correlation and the approval on the deployment's anchor, keyed by the
// run id the frame names. Only the approval park emits that frame, so one
// definition row satisfies the run-row foreign key.
function createRegisterSignalCorrelation(db: TestDb["db"]) {
  const signalCorrelationStore = createSignalCorrelationStore(db);
  const approvalStore = createApprovalStore(db);
  const workflowRunStore = createWorkflowRunStore(db);
  return async ({
    correlationId,
    runId,
    anchorRunId,
    agentAddress,
    kind,
    approvalSnapshot,
  }: {
    correlationId: string;
    runId: string;
    anchorRunId: string;
    agentAddress: string;
    kind: "approval";
    approvalSnapshot: ApprovalSnapshot;
  }): Promise<void> => {
    const anchor = await db
      .select({
        id: workflowRun.id,
        tenantId: workflowRun.tenantId,
      })
      .from(workflowRun)
      .where(
        and(
          eq(workflowRun.address, agentAddress),
          inArray(workflowRun.status, ["deployed", "running"]),
        ),
      )
      .limit(1)
      .then((rows) => rows[0]);
    if (anchor === undefined) {
      throw new Error(
        `No live workflow run for address "${agentAddress}"; cannot register signal correlation ${correlationId}`,
      );
    }
    const addressSlug = deriveWorkflowRunRepoId(agentAddress);
    if (addressSlug !== anchorRunId) {
      throw new Error(
        `Anchor run id mismatch registering signal correlation ${correlationId}: frame claims "${anchorRunId}" but address "${agentAddress}" derives the workflow-run repo slug "${addressSlug}"`,
      );
    }
    const tenantId = anchor.tenantId;
    const anchorDbId = anchor.id;
    await db.transaction(async (tx) => {
      await workflowRunStore.createIfAbsent(
        {
          id: runId,
          anchorRunId: anchorDbId,
          tenantId,
          definitionId: DEFINITION_ID,
          principalId: null,
          status: "running",
        },
        tx,
      );
      await signalCorrelationStore.registerIfAbsent(
        {
          correlationId,
          tenantId,
          anchorRunId: anchorDbId,
          agentAddress,
          runId,
          signalName: signalName(correlationId),
          kind,
        },
        tx,
      );
      await approvalStore.createIfAbsent(
        {
          id: generateId("approval"),
          tenantId,
          anchorRunId: anchorDbId,
          runId,
          agentAddress,
          correlationId,
          status: "pending",
          toolDefinition: {
            name: approvalSnapshot.name,
            description: approvalSnapshot.description,
            inputSchema: approvalSnapshot.inputSchema,
          },
          toolArguments: approvalSnapshot.arguments,
          scope: null,
          timeoutAt: null,
        },
        tx,
      );
    });
  };
}

describe.skipIf(!harnessDbEnvAvailable())(
  "a childWorkflow park relayed onto the deployment run",
  () => {
    test("delivers an author signal on the deployment run into the child", async () => {
      const { workflowRunRepoId, childWorkflowId } = await deployGateWorkflow({
        anchorRunId: UNANSWERABLE_ANCHOR,
        timedGate: false,
      });

      const runId = await topLevelRunId(workflowRunRepoId, childWorkflowId);
      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(
            env,
            UNANSWERABLE_ANCHOR,
            runId,
          );
          return events.some(
            (event) =>
              event.type === "SignalAwaited" &&
              event.body["parkKind"] === "signal-relay" &&
              event.body["signalName"] === SIGNAL_NAME,
          );
        },
        { timeoutMs: 60_000, diagnostics: env.sidecarDiagnostics },
      );

      await injectSignal(env, UNANSWERABLE_ANCHOR, runId, SIGNAL_NAME, {
        ok: true,
      });

      const terminal = await waitForWorkflowRunComplete(
        env,
        UNANSWERABLE_ANCHOR,
        runId,
        { timeoutMs: 60_000, diagnostics: env.sidecarDiagnostics },
      );
      expect(terminal.type).toBe("RunCompleted");
    }, 120_000);

    test("registers the child's approval on the deployment run", async () => {
      const { workflowRunRepoId, childWorkflowId } = await deployGateWorkflow({
        anchorRunId: ASK_ANCHOR,
        timedGate: false,
        askTool: true,
      });

      const runId = await topLevelRunId(workflowRunRepoId, childWorkflowId);
      await waitFor(
        async () => {
          const rows = await h.db
            .select()
            .from(approval)
            .where(eq(approval.anchorRunId, ASK_ANCHOR));
          return rows.length === 1;
        },
        { timeoutMs: 60_000, diagnostics: env.sidecarDiagnostics },
      );

      const pending = await h.db
        .select()
        .from(approval)
        .where(eq(approval.anchorRunId, ASK_ANCHOR));
      expect(pending).toHaveLength(1);
      const approvalRow = pending[0];
      if (approvalRow === undefined) throw new Error("unreachable");
      const childRunId = await spawnedChildRunId(ASK_ANCHOR, runId);
      expect(approvalRow.status).toBe("pending");
      expect(approvalRow.runId).toBe(runId);
      expect(approvalRow.runId).not.toBe(childRunId);
      expect(approvalRow.toolDefinition).toEqual({
        name: MAIL_TOOL_NAME,
        description: "Send a mail message",
        inputSchema: {
          type: "object",
          properties: { to: { type: "string" }, body: { type: "string" } },
          required: ["to", "body"],
        },
      });
      expect(approvalRow.toolArguments).toEqual({
        to: "someone@example.com",
        body: "hello",
      });

      const approveApp = createApp({
        getSession: createMockGetSession(APPROVER_USER_ID),
        authHandler: () => new Response("", { status: 404 }),
        db: h.db,
        grantStore: createInMemoryGrantStore([approverGrant]),
        sidecarRouter: env.hub.router,
        sessionService: createMockSessionService(),
        eventCollectors: createMockEventCollectors(),
        assetService: null,
        repoStore: null,
        maxTarballBytes: 10_000_000,
      });
      const approveRes = await approveApp.request(
        `/api/tenants/${TENANT_ID}/approvals/${approvalRow.id}/approve`,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ scope: "once" }),
        },
      );
      expect(approveRes.status).toBe(200);

      const terminal = await waitForWorkflowRunComplete(
        env,
        ASK_ANCHOR,
        runId,
        {
          timeoutMs: 60_000,
          diagnostics: env.sidecarDiagnostics,
        },
      );
      expect(terminal.type).toBe("RunCompleted");
    }, 120_000);

    test("still runs a timed gate, which resolves on its own timer", async () => {
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

      // The child parked, and its own timer released the gate.
      const childRunId = await spawnedChildRunId(TIMED_ANCHOR, runId);
      const childTypes = (
        await readWorkflowRunEvents(env, TIMED_ANCHOR, childRunId)
      ).map((e) => e.type);
      expect(childTypes).toContain("SignalAwaited");
      expect(childTypes).toContain("TimerFired");
    }, 120_000);
  },
);

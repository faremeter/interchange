// CAPSTONE end-to-end approval-tracer integration test.
//
// Proves the approval tracer's Slice 0 headline through the real hub + sidecar
// stack, with no stubbed suspend and no stubbed resume: on approval the
// ORIGINAL tool runs and the agent continues with its real result.
//
//   deploy a single-step workflow whose agent calls a recording tool
//     -> the tool call hits an `ask` grant
//     -> the reactor SUSPENDS (mints a correlationId), capturing the parked
//        ToolCall on the pending operation
//     -> runStep parks the step on the reserved `__signal__:<corr>` channel
//     -> the sidecar emits `signal.correlation.register`
//     -> the hub co-writes the `signal_correlation` + `approval` rows
//     -> approve via the real hub HTTP route
//        (POST /api/tenants/:tenantId/approvals/:approvalId/approve, scope "once")
//     -> the resolver claims + resolves + `sendSignalDeliver`
//     -> the parked run RESUMES: the reactor grants a one-shot bypass keyed on
//        the parked ToolCall.id and RE-DISPATCHES the exact call on the
//        execute-tools rail, so the approved tool RUNS, its real result lands in
//        history, and the agent re-infers once off that result to a reply
//        -> the workflow reaches terminal `completed`.
//
// Load-bearing assertions:
//   1. Before approval: a pending `approval` row + `signal_correlation` row
//      exist for the minted correlationId (status `pending`), and the tool has
//      NOT run yet (no sentinel).
//   2. Approve once -> 200.
//   3. After approval: the run resumes, the recording tool executes EXACTLY
//      ONCE (sentinel written once with its content), the run reaches terminal
//      `completed` carrying the tool's REAL result (the resumed reply reflects
//      the tool output, not a re-inference that skipped the tool), the
//      post-resume conversation history is a well-formed tool_call/tool_result
//      sequence (the parked call is answered, no dangling tool_use), and there
//      was NO re-park (one `SignalAwaited`, one `approval` row, one
//      `signal_correlation` row -- the one-shot bypass let the re-dispatched
//      call through without a second suspension). Plus the resolved approval row
//      and the claimed correlation row.
//
// The mock model is the discriminator between fixed and broken. It re-issues the
// tool call on every inference whose history does not yet carry a tool_result
// answering it, and only replies once it sees that result. Under the OLD broken
// resume rail the decision arrived as a bare user turn (no tool_result), so the
// mock re-issued the call, re-hit the still-`ask` grant, and re-parked -- an
// endless loop that never completes. Under the NEW re-dispatch rail the approved
// call runs and its real tool_result lands in history, so the next inference
// sees it and the mock replies -- the run completes. A latched "call once" mock
// would hide the loop; this one does not.
//
// Harness composition. The suspend/register/resume half runs against the real
// sidecar subprocess through the shared `deploy-flow-env` fixture (the same
// harness `single-step-grants-bridge` and `multistep-signal` drive). The
// approval-store + approve-route half runs against a real migrated Postgres
// schema (`@intx/test-harness`), the same substrate the
// `signal-correlation-register` and `approvals` route tests use. The two are
// bridged by wiring the fixture hub's `registerSignalCorrelation` lookup to the
// real DB co-write and pointing the real `createApprovalRoutes` app at the same
// schema and the same sidecar router (so its `sendSignalDeliver` reaches the
// real subprocess). The deployment is seeded as a `workflow_deployment` row so
// the co-write resolves tenancy exactly as production does.
//
// Approval is driven through the REAL hub HTTP route, not the resolver: the
// fixture makes an authenticated tenant call the same way `credential-routes`
// does -- a mock betterAuth session bound to a seeded active user-principal,
// which `resolveTenant` resolves into the tenant + principal the route reads.
// The approver's authority is a real `approval:<deploymentId>` / `resolve`
// grant evaluated by the route's `authorize` call.
//
// Single-test file. The shared `deploy-flow-env` (real sidecar subprocess + its
// on-disk warm step-state) is `beforeAll`-scoped, while the DB resets per test.
// A second test in this describe block would inherit the first run's warm
// workspace, and "sentinel written exactly once" would silently stop meaning
// what it claims. A run-once guard below fails loud if a second test is ever
// added here rather than letting that assumption rot; a genuinely independent
// second scenario belongs in its own file with its own env.

import fs from "node:fs";
import path from "node:path";

import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { and, eq } from "drizzle-orm";

import { defineAgent, createDefaultDirectorRegistry } from "@intx/agent";
import { createInMemoryGrantStore } from "@intx/authz";
import {
  createApprovalStore,
  createSignalCorrelationStore,
  createWorkflowRunStore,
} from "@intx/db";
import {
  approval,
  signalCorrelation,
  workflowDeployment,
} from "@intx/db/schema";
import { createApp, type GetSession } from "@intx/hub-api";
import { generateId } from "@intx/hub-common";
import {
  WORKFLOW_RUN_AGENT_STATE_PREFIX,
  type EventCollectorRegistry,
  type RepoId,
  type SessionService,
  type WorkflowRunHubPrincipal,
} from "@intx/hub-sessions";
import { assertWellFormedToolSequence } from "@intx/inference";
import { reconstructDurableConversation } from "@intx/sidecar-app/src/conversation-state";
import { signalName } from "@intx/types";
import type { GrantRule } from "@intx/types/authz";
import { WireGrantRule } from "@intx/types/grant-wire";
import type {
  ApprovalSnapshot,
  ConversationTurn,
  HarnessConfig,
} from "@intx/types/runtime";
import type { ToolPackagePin } from "@intx/types/tool-packages";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedPrincipal,
  seedTenants,
  seedWorkflowDeployment,
} from "@intx/test-harness/seed";
import { defineWorkflow, step, type WorkflowDefinition } from "@intx/workflow";
import {
  createWorkflowDeployOrchestrator,
  deriveDeploymentAddress,
  type ApprovalSet,
  type DeploySingleStepFn,
  type LaunchSessionFn,
  type SendMultiStepDeployFn,
  type WorkflowRepoWriter,
} from "@intx/workflow-deploy";
import { deriveDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";

import {
  SESSION_ID,
  fireMailTrigger,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { toLaunchDeployContent } from "./launch-session-bridge";

const DEPLOYMENT_DOMAIN = "integration.interchange";
// A launched-agent instance id: `ins_` + a hex-shaped local part, mirroring
// the single-step grants-bridge shape. The deployment mail address is the
// legacy `ins_<hex>@<domain>` identity the agent-launch path mints, and the
// sidecar's deploy router recognizes the single-step projection and applies the
// launched-agent identity strategy.
const INSTANCE_LOCAL = "feedface0001feedface0002feedface";
const DEPLOYMENT_ID = INSTANCE_LOCAL;
const WORKFLOW_RUN_REF = "refs/heads/main";
const STEP_ID = "step1";

// The tool the model is told to call. The granted resource is
// `tool:<TOOL_NAME>` with action `invoke`; the agent's authorize gate fires
// that exact query when the model calls the tool. The grant's effect is `ask`,
// so the call SUSPENDS instead of running.
const TOOL_NAME = "@intx/tools-mail/sidecar-bundle:mail_send";
const ASK_RESOURCE = `tool:${TOOL_NAME}`;

// The sentinel the recording tool writes when (and only when) it actually runs
// in the child. Absent before approval (the ask grant suspends the call);
// written exactly once after approval (the re-dispatch runs the parked call).
// The synthetic `mail_send` bundle writes `SENTINEL_CONTENT` (its `to`
// argument) into the file named by its `body` argument, and returns
// `"wrote " + <filename>` as the tool result, which the resumed reply echoes.
const SENTINEL_FILENAME = "approval-tool-ran.txt";
const SENTINEL_CONTENT = "tool-executed";
// The tool's result string is `"wrote " + <body argument>`; see the synthetic
// bundle in deploy-flow-env. The mock's resumed reply is
// `RESUME_REPLY_PREFIX + <that result>`, so a test can assert the run continued
// with the tool's real output rather than a re-inference that skipped it.
const TOOL_RESULT = `wrote ${SENTINEL_FILENAME}`;
const RESUME_REPLY_PREFIX = "done: ";

const TOOL_PINS: readonly ToolPackagePin[] = [
  { name: "@intx/tools-mail", version: "0.1.2" },
];

// The operator-approved grant the hub ships in-band on the deploy frame. The
// grants bridge writes it into the legacy agent-state repo; the child reads it
// back to authorize the tool. Effect `ask` is the whole point: the tool call
// suspends awaiting an external decision rather than running.
const ASK_GRANT: WireGrantRule = {
  id: "grant-tool-ask",
  resource: ASK_RESOURCE,
  action: "invoke",
  effect: "ask",
  origin: "creator",
  conditions: null,
  expiresAt: null,
  roleId: null,
  principalId: null,
};

// Tenant / approver identity for the real HTTP approve call. The
// `workflow_deployment` row anchors tenancy for the co-write; the seeded
// user-principal (refId == the mock session's user id) is what `resolveTenant`
// resolves into the tenant + principal the approve route reads, and the grant
// below is what its `authorize` allows.
const TENANT_ID = "tnt_approval_capstone";
const APPROVER_USER_ID = "usr_approver";
const APPROVER_PRINCIPAL_ID = "prn_approver";
const DEFINITION_ASSET_ID = "ast_approval_wf";

let env: DeployFlowEnv;
let h: TestDb;

// The deployment mail address and the workflow-run repo slug the supervisor
// stamps onto the register frame's `deploymentId`. The co-write resolves
// tenancy by the address and cross-checks the slug, so the seeded
// `workflow_deployment` row is keyed by the slug with this address.
const deploymentMailAddress = deriveDeploymentAddress({
  deploymentId: DEPLOYMENT_ID,
  deploymentDomain: DEPLOYMENT_DOMAIN,
});
const deploymentSlug = deriveDeploymentId(deploymentMailAddress);

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
      id: "session_capstone",
      userId,
      token: "tok_capstone",
      expiresAt: new Date("2999-01-01"),
      createdAt: now,
      updatedAt: now,
    },
  });
}

function notImpl(name: string): never {
  throw new Error(`approval capstone mock: ${name} not implemented`);
}

function createMockSessionService(): SessionService {
  return {
    stageWorkflowStep: () => notImpl("stageWorkflowStep"),
    deployInstanceAtHead: () => notImpl("deployInstanceAtHead"),
    deployWorkflowDefinition: () => notImpl("deployWorkflowDefinition"),
    deploySingleStepAtHead: () => notImpl("deploySingleStepAtHead"),
    sendUserMessage: () => notImpl("sendUserMessage"),
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

// The `approval:<deploymentId>` / `resolve` grant for the approver. The
// resolver authorizes the exact `approval:<slug>` resource, so this is what
// lets the approve call through; without it the route returns 403.
const approverGrant: GrantRule = {
  id: "grant-approver-resolve",
  resource: `approval:${deploymentSlug}`,
  action: "resolve",
  effect: "allow",
  origin: "system",
  conditions: null,
  expiresAt: null,
  roleId: null,
  principalId: APPROVER_PRINCIPAL_ID,
};

/**
 * The real hub co-write, mirroring `createHubSessionLookups`'s
 * `registerSignalCorrelation`: resolve tenancy from the deployed
 * `workflow_deployment` the address names, cross-check the frame's
 * `deploymentId` against it, and co-write the `signal_correlation` + `approval`
 * rows in one transaction through the real stores. Wired into the fixture hub's
 * sidecar router so the `signal.correlation.register` frame the parked run emits
 * lands durable rows on the same schema the approve route reads.
 */
function createRegisterSignalCorrelation(db: TestDb["db"]) {
  const signalCorrelationStore = createSignalCorrelationStore(db);
  const approvalStore = createApprovalStore(db);
  const workflowRunStore = createWorkflowRunStore(db);
  return async ({
    correlationId,
    runId,
    deploymentId,
    agentAddress,
    kind,
    approvalSnapshot,
  }: {
    correlationId: string;
    runId: string;
    deploymentId: string;
    agentAddress: string;
    kind: "approval";
    approvalSnapshot: ApprovalSnapshot;
  }): Promise<void> => {
    const deployment = await db
      .select({
        id: workflowDeployment.id,
        tenantId: workflowDeployment.tenantId,
      })
      .from(workflowDeployment)
      .where(
        and(
          eq(workflowDeployment.address, agentAddress),
          eq(workflowDeployment.status, "deployed"),
        ),
      )
      .limit(1)
      .then((rows) => rows[0]);
    if (deployment === undefined) {
      throw new Error(
        `No deployed workflow deployment for address "${agentAddress}"; cannot register signal correlation ${correlationId}`,
      );
    }
    if (deployment.id !== deploymentId) {
      throw new Error(
        `Deployment id mismatch registering signal correlation ${correlationId}: frame claims "${deploymentId}" but address "${agentAddress}" resolves to "${deployment.id}"`,
      );
    }
    const tenantId = deployment.tenantId;
    await db.transaction(async (tx) => {
      // Mirror the production co-write: lazily anchor the run before the
      // correlation and approval reference it, so their runId FK resolves.
      await workflowRunStore.createIfAbsent(
        {
          id: runId,
          deploymentId,
          tenantId,
          principalId: null,
          status: "running",
        },
        tx,
      );
      await signalCorrelationStore.registerIfAbsent(
        {
          correlationId,
          tenantId,
          deploymentId,
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
          deploymentId,
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
  "approval tracer round-trip through the real hub + sidecar stack",
  () => {
    // Run-once guard. The env (sidecar subprocess + on-disk warm step-state) is
    // shared across the describe block, so a second test would inherit this
    // run's warm workspace and the "written exactly once" sentinel assertion
    // would silently stop meaning what it claims. Fail loud if a second test is
    // ever added to this block rather than letting the assumption rot.
    let hasRun = false;

    beforeAll(async () => {
      h = await createTestDb();
      env = await startDeployFlowEnv({
        // Persistent tool-call mock: re-issue the call until its result is in
        // history, then reply reflecting the result. This loops under the old
        // broken resume rail and completes under the fixed re-dispatch rail.
        inferenceApprovalToolCall: {
          toolName: TOOL_NAME,
          input: { to: SENTINEL_CONTENT, body: SENTINEL_FILENAME },
          resultPrefix: RESUME_REPLY_PREFIX,
        },
        // Wire the real DB co-write into the fixture hub so the parked run's
        // register frame writes real rows.
        registerSignalCorrelation: createRegisterSignalCorrelation(h.db),
      });
    });

    afterAll(async () => {
      await env.teardown();
      await h.close();
    });

    beforeEach(async () => {
      await h.reset();
    });

    test("runs the approved tool once and completes with its real result", async () => {
      if (hasRun) {
        throw new Error(
          "approval capstone assumes a single test per shared env: the warm " +
            "step-state carries across, so a second test would break the " +
            "written-exactly-once sentinel assertion. Add a new scenario in " +
            "its own file with its own env instead.",
        );
      }
      hasRun = true;

      // Seed the tenancy the co-write and the approve route resolve against:
      // a tenant, the workflow definition asset the deployment row references,
      // the deployment row itself (keyed by the run-repo slug, addressed by the
      // deployment mail address), and the active approver user-principal.
      await seedTenants(h.db, [{ id: TENANT_ID }]);
      await seedAsset(h.db, {
        id: DEFINITION_ASSET_ID,
        tenantId: TENANT_ID,
        kind: "workflow",
        name: "approval-capstone-wf",
      });
      await seedWorkflowDeployment(h.db, {
        id: deploymentSlug,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        address: deploymentMailAddress,
        publicKey: null,
        status: "deployed",
      });
      await seedPrincipal(h.db, {
        id: APPROVER_PRINCIPAL_ID,
        tenantId: TENANT_ID,
        kind: "user",
        refId: APPROVER_USER_ID,
        status: "active",
      });

      const agent = defineAgent({
        id: "agent-approval-capstone",
        systemPrompt: "You are the single-step agent under approval control.",
        tools: [],
        capabilities: [],
        inference: {
          sources: [{ provider: "anthropic", model: "mock-model" }],
        },
      });

      const workflow: WorkflowDefinition = defineWorkflow({
        id: `wf_${DEPLOYMENT_ID}`,
        trigger: { type: "mail", to: deploymentMailAddress },
        steps: {
          [STEP_ID]: step({ agent }),
        },
      });

      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentId: `ins_${DEPLOYMENT_ID}`,
        tenantId: "tenant-1",
        principalId: "prin_integration-1",
        agentAddress: deploymentMailAddress,
        systemPrompt:
          "Fallback prompt (overridden per step by the orchestrator)",
        tools: [],
        // The `ask`-effect grant is the whole point: it makes the tool call
        // suspend in the child instead of running.
        grants: [ASK_GRANT],
        sources: [
          {
            id: "anthropic:mock-model",
            provider: "anthropic",
            baseURL: `http://localhost:${env.inference.server.port}`,
            apiKey: "sk-mock",
            model: "mock-model",
          },
        ],
        defaultSource: "anthropic:mock-model",
      };

      const operatorApprovals: ApprovalSet = new Set<string>([
        "inference.source:anthropic:mock-model",
        "director:@intx/agent/default",
        `mail.address:${deploymentMailAddress}`,
        `mail.send:${DEPLOYMENT_DOMAIN}`,
      ]);

      const launchSession: LaunchSessionFn = async (orchestratorParams) => {
        await env.hub.sessionService.stageWorkflowStep({
          agentAddress: orchestratorParams.agentAddress,
          agentId: orchestratorParams.agentId,
          instanceId: orchestratorParams.instanceId,
          config: orchestratorParams.config,
          deployContent: toLaunchDeployContent(
            orchestratorParams.deployContent,
          ),
          ...(orchestratorParams.toolPackagePins !== undefined
            ? { toolPackagePins: orchestratorParams.toolPackagePins }
            : {}),
        });
      };

      const sendMultiStepDeploy: SendMultiStepDeployFn = async (params) =>
        env.hub.router.sendAgentDeploy(params.agentAddress, params.config, {
          definition: {
            id: params.definition.id,
            triggers: [...params.definition.triggers],
            stepOrder: [...params.definition.stepOrder],
            steps: params.definition.steps as Record<string, unknown>,
            ...(params.definition.state !== undefined
              ? { state: params.definition.state }
              : {}),
          },
          sources: params.sources,
        });

      const deploySingleStepAtHead: DeploySingleStepFn = (params) =>
        env.hub.sessionService.deploySingleStepAtHead(params);

      const workflowRepo: WorkflowRepoWriter = {
        async writeWorkflowRepo(args) {
          const repoId: RepoId = { kind: "workflow", id: args.workflowRepoId };
          const principal: WorkflowRunHubPrincipal = { kind: "hub" };
          const files: Record<string, string> = {};
          for (const [k, v] of args.files) {
            files[k] = v;
          }
          await env.hub.agentRepoStore.repoStore.writeTree(
            principal,
            repoId,
            "refs/heads/main",
            {
              files,
              message: `approval-capstone test: write workflow repo ${args.workflowRepoId}`,
            },
          );
        },
      };

      const orchestrator = createWorkflowDeployOrchestrator({
        directorRegistry: createDefaultDirectorRegistry(),
        workflowRepo,
        launchSession,
        sendMultiStepDeploy,
        deploySingleStepAtHead,
      });

      let result: Awaited<ReturnType<typeof orchestrator.deployWorkflow>>;
      try {
        result = await orchestrator.deployWorkflow({
          workflow,
          config,
          deployContent: { systemPrompt: config.systemPrompt },
          operatorApprovals,
          deploymentId: DEPLOYMENT_ID,
          deploymentDomain: DEPLOYMENT_DOMAIN,
          hubPublicKey: "00".repeat(32),
          toolPackagePins: TOOL_PINS,
        });
      } catch (cause) {
        const message = cause instanceof Error ? cause.message : String(cause);
        const diag = env.sidecarDiagnostics();
        throw new Error(
          `deployWorkflow failed: ${message}\n${diag.length > 0 ? diag : "<no sidecar diagnostics>"}`,
          { cause },
        );
      }
      expect(result.publicKey).toBeTruthy();

      const workflowRunRepoId: RepoId = {
        kind: "workflow-run",
        id: deploymentSlug,
      };
      env.registerDeployment({
        deploymentId: DEPLOYMENT_ID,
        workflowDefinition: workflow,
        workflowRunRepoId,
        workflowRunRef: WORKFLOW_RUN_REF,
        mailAddress: deploymentMailAddress,
      });

      // The sentinel path in the warm single-step agent's stable workspace.
      // Its absence before approval and single presence after are the
      // exactly-once-after-approval proof.
      const sentinelPath = path.join(
        env.sidecar.dataDir,
        "workflow-step-state",
        workflowRunRepoId.id,
        "warm",
        encodeURIComponent(STEP_ID),
        "workspace",
        SENTINEL_FILENAME,
      );

      // The warm agent's durable conversation dir on the sidecar's on-disk
      // substrate. The sidecar roots each workflow-run repo at
      // `<dataDir>/workflow-runs/<repoId>` (the same layout the deployment-record
      // path uses), and the durable conversation mirror lives under
      // `agent-state/<stepId>` inside it. `reconstructDurableConversation` reads
      // it the way the warm agent's own restore does, so the post-resume history
      // it returns is the real conversation, not a re-derivation.
      const agentStateDir = path.join(
        env.sidecar.dataDir,
        "workflow-runs",
        workflowRunRepoId.id,
        WORKFLOW_RUN_AGENT_STATE_PREFIX,
        encodeURIComponent(STEP_ID),
      );

      // Guard the single-test warm-state assumption at the source: no prior run
      // may have left the sentinel in this env before this test fires.
      expect(fs.existsSync(sentinelPath)).toBe(false);

      // Fire the trigger. The model turn calls the tool; the tool call hits the
      // ask grant and suspends, so the run parks rather than completing.
      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<approval-capstone-1@integration.interchange>",
        grants: [ASK_GRANT],
      });

      const runId = await waitForFirstRunId(env, workflowRunRepoId, {
        diagnostics: env.sidecarDiagnostics,
        timeoutMs: 20_000,
      });

      // ---- Assertion 1: parked before approval ----
      //
      // Wait for the co-written pending approval row to appear -- proof the
      // reactor suspended, the step parked, the register frame reached the hub,
      // and the hub co-wrote both rows.
      await waitFor(
        async () => {
          const rows = await h.db
            .select()
            .from(approval)
            .where(eq(approval.deploymentId, deploymentSlug));
          return rows.length === 1;
        },
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      const pendingApprovalRows = await h.db
        .select()
        .from(approval)
        .where(eq(approval.deploymentId, deploymentSlug));
      expect(pendingApprovalRows).toHaveLength(1);
      const approvalRow = pendingApprovalRows[0];
      if (approvalRow === undefined) throw new Error("unreachable");
      expect(approvalRow.status).toBe("pending");
      expect(approvalRow.runId).toBe(runId);
      expect(approvalRow.agentAddress).toBe(deploymentMailAddress);
      expect(approvalRow.resolvedAt).toBeNull();

      // The approval snapshot content survived the real cross-process path
      // (child -> sidecar -> hub -> co-write) intact. Every field the snapshot
      // carries is pinned verbatim: the name and arguments the model issued,
      // and the description and inputSchema of the resolved tool definition.
      // All four are test-owned -- the tool is the synthetic `@intx/tools-mail`
      // harness bundle whose `mail_send` definition is fixed in
      // `deploy-flow-env.ts` (the loader namespaces its name to TOOL_NAME) --
      // so a hop that dropped or mangled any field between the park and the
      // co-write fails this assertion.
      expect(approvalRow.toolDefinition).toEqual({
        name: TOOL_NAME,
        description: "Send a mail message",
        inputSchema: {
          type: "object",
          properties: { to: { type: "string" }, body: { type: "string" } },
          required: ["to", "body"],
        },
      });
      expect(approvalRow.toolArguments).toEqual({
        to: SENTINEL_CONTENT,
        body: SENTINEL_FILENAME,
      });

      const correlationId = approvalRow.correlationId;
      const approvalId = approvalRow.id;

      // The correlation routing row exists and is unresolved.
      const correlationRows = await h.db
        .select()
        .from(signalCorrelation)
        .where(eq(signalCorrelation.correlationId, correlationId));
      expect(correlationRows).toHaveLength(1);
      const correlationRow = correlationRows[0];
      if (correlationRow === undefined) throw new Error("unreachable");
      expect(correlationRow.runId).toBe(runId);
      expect(correlationRow.agentAddress).toBe(deploymentMailAddress);
      expect(correlationRow.signalName).toBe(signalName(correlationId));
      expect(correlationRow.resolvedAt).toBeNull();

      // The run has parked, not completed: the workflow-run log carries the
      // `SignalAwaited` marker for the reserved correlation channel. The event
      // reaches the hub through the pack-push pipeline, which lags the
      // synchronous register-frame co-write above, so wait for it to land
      // rather than racing the push. The step is suspended throughout, so the
      // tool cannot run while we wait.
      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
          return events.some(
            (e) =>
              e.type === "SignalAwaited" &&
              e.body["signalName"] === signalName(correlationId),
          );
        },
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      const parkedEvents = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        runId,
      );
      const parkedTypes = parkedEvents.map((e) => e.type);
      expect(parkedTypes).not.toContain("RunCompleted");
      expect(parkedTypes).not.toContain("RunFailed");

      // The tool has NOT run: the ask grant suspended the call before execution.
      expect(fs.existsSync(sentinelPath)).toBe(false);

      // ---- Assertion 2: approve once -> 200 (real HTTP route) ----
      //
      // The approve app shares the real DB schema and the fixture's real
      // sidecar router, so its `sendSignalDeliver` reaches the parked run in the
      // real subprocess. The mock session + seeded active user-principal satisfy
      // `resolveTenant`; the approver grant satisfies the route's authorize.
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
        `/api/tenants/${TENANT_ID}/approvals/${approvalId}/approve`,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ scope: "once" }),
        },
      );
      expect(approveRes.status).toBe(200);
      const approveBody: unknown = await approveRes.json();
      expect(approveBody).toMatchObject({
        id: approvalId,
        status: "approved",
        scope: "once",
      });

      // ---- Assertion 3: resume runs the tool once and completes ----
      //
      // The delivered decision resumes the parked run: the reactor grants a
      // one-shot bypass for the parked ToolCall.id and re-dispatches the exact
      // call, so the approved tool RUNS, its real result lands in history, and
      // the agent re-infers once off that result to a reply. The workflow
      // reaches terminal `completed`. If the fix regressed, the mock would
      // re-issue the call on a resultless re-inference, re-hit the ask grant,
      // and re-park -- this wait would time out.
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

      // The resume actually drove the run: the delivered decision landed as a
      // `SignalReceived` on the parked correlation channel, carrying the
      // approved outcome. (The log carries two `SignalReceived` entries for the
      // one delivery -- the wire `signal.deliver` path commits the canonical
      // one that resolves the parked `awaitNext`, and the runtime body commits
      // its own after `awaitNext` returns; both share the delivered `signalId`,
      // which the state machine's `observedSignalIds` dedup makes a state no-op.
      // So the load-bearing fact is the delivered outcome, not the raw count.)
      const finalEvents = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        runId,
      );
      const signalReceived = finalEvents.find(
        (e) =>
          e.type === "SignalReceived" &&
          e.body["signalName"] === signalName(correlationId),
      );
      if (signalReceived === undefined) {
        throw new Error(
          `no SignalReceived for the parked correlation ${correlationId}`,
        );
      }
      expect(signalReceived.body["payload"]).toEqual({ outcome: "approved" });

      // The approved tool ran EXACTLY ONCE: the sentinel exists and carries the
      // tool's write. It was absent before approval (asserted above), so its
      // presence is the re-dispatched call executing, and its single write with
      // the expected content is the exactly-once proof.
      expect(fs.existsSync(sentinelPath)).toBe(true);
      expect(fs.readFileSync(sentinelPath, "utf-8")).toBe(SENTINEL_CONTENT);

      // The run completed carrying the tool's REAL result. The resumed reply is
      // `RESUME_REPLY_PREFIX + <tool result>`, which the mock only emits after
      // seeing the tool_result in history -- so a completion that skipped the
      // tool (re-inference without a result) could not produce this reply. The
      // reply is the resumed step's output, committed on `StepCompleted`.
      const stepCompleted = finalEvents.find(
        (e) => e.type === "StepCompleted" && e.body["stepId"] === STEP_ID,
      );
      if (stepCompleted === undefined) {
        throw new Error(`no StepCompleted for ${STEP_ID}`);
      }
      const stepOutputRef = stepCompleted.body["output"];
      expect(JSON.stringify(stepOutputRef)).toContain(
        `${RESUME_REPLY_PREFIX}${TOOL_RESULT}`,
      );

      // The post-resume conversation history is a well-formed tool sequence.
      // `assertWellFormedToolSequence` checks that every tool_result has a
      // preceding tool_call and that no tool_call id or tool_result is
      // duplicated; it does NOT by itself require every call to be answered. The
      // "parked tool_use is answered, no dangling call" property comes from that
      // validator TOGETHER with the exactly-one-call / exactly-one-result counts
      // below: one call and one result, well-ordered, means the single call is
      // the one answered. Read the warm agent's durable conversation off the
      // sidecar's on-disk substrate so this runs against the real history.
      //
      // The durable mirror is committed at run boundaries and can lag the
      // terminal event by a mirror flush, so poll until the answered call
      // surfaces rather than racing the write.
      const readTurns = async (): Promise<ConversationTurn[]> => {
        const reconstructed = await reconstructDurableConversation(
          agentStateDir,
          STEP_ID,
        );
        return reconstructed === null ? [] : reconstructed.turns;
      };
      await waitFor(
        async () =>
          (await readTurns())
            .flatMap((t) => t.content)
            .some((b) => b.type === "tool_result"),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      const turns = await readTurns();
      assertWellFormedToolSequence(turns);
      const toolCallCount = turns
        .flatMap((t) => t.content)
        .filter((b) => b.type === "tool_call").length;
      const toolResultCount = turns
        .flatMap((t) => t.content)
        .filter((b) => b.type === "tool_result").length;
      expect(toolCallCount).toBe(1);
      expect(toolResultCount).toBe(1);

      // No re-park: the one-shot bypass let the re-dispatched call through
      // without a second suspension, so the log carries exactly one
      // `SignalAwaited` and one `StepCompleted`, and exactly one `approval` and
      // one `signal_correlation` row were ever raised for this deployment. A
      // second ask suspension would have minted a fresh correlation and a second
      // approval row.
      const signalAwaitedCount = finalEvents.filter(
        (e) => e.type === "SignalAwaited",
      ).length;
      expect(signalAwaitedCount).toBe(1);

      const stepCompletedCount = finalEvents.filter(
        (e) => e.type === "StepCompleted" && e.body["stepId"] === STEP_ID,
      ).length;
      expect(stepCompletedCount).toBe(1);

      const allApprovals = await h.db
        .select()
        .from(approval)
        .where(eq(approval.deploymentId, deploymentSlug));
      expect(allApprovals).toHaveLength(1);

      const allCorrelations = await h.db
        .select()
        .from(signalCorrelation)
        .where(eq(signalCorrelation.deploymentId, deploymentSlug));
      expect(allCorrelations).toHaveLength(1);

      // The approval row is terminal: approved, scoped once, resolved.
      const resolvedRows = await h.db
        .select()
        .from(approval)
        .where(eq(approval.id, approvalId));
      expect(resolvedRows).toHaveLength(1);
      const resolvedRow = resolvedRows[0];
      if (resolvedRow === undefined) throw new Error("unreachable");
      expect(resolvedRow.status).toBe("approved");
      expect(resolvedRow.scope).toBe("once");
      expect(resolvedRow.resolvedAt).not.toBeNull();

      // The correlation routing row is claimed: resolvedAt + the delivered
      // signalId are stamped, which is what makes a redelivered decision a
      // no-op.
      const claimedCorrelationRows = await h.db
        .select()
        .from(signalCorrelation)
        .where(eq(signalCorrelation.correlationId, correlationId));
      expect(claimedCorrelationRows).toHaveLength(1);
      const claimedCorrelation = claimedCorrelationRows[0];
      if (claimedCorrelation === undefined) throw new Error("unreachable");
      expect(claimedCorrelation.resolvedAt).not.toBeNull();
      expect(claimedCorrelation.signalId).not.toBeNull();
    });
  },
);

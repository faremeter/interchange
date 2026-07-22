// Acceptance test: a hub reconnect re-emits a parked run's correlation so the
// run becomes approvable again, and an approver can then resolve the recovered
// correlation to drive the parked run to completion.
//
// The scenario the ticket names is "suspend with the hub down -> hub comes up
// -> the correlation is registered and the run is approvable". The load-bearing
// state is the ABSENCE of the parked run's `signal_correlation` + `approval`
// rows at the hub (the `signal.correlation.register` frame the suspend emitted
// never co-wrote them) followed by their APPEARANCE after the hub link
// reconnects. The recovery is the sidecar's Trigger B: on the reconnect
// ownership challenge the hub-link fires `onWorkflowAddressesRoutable`, which
// calls `reEmitParkedCorrelations(address)`, which asks the deployment's live
// supervisor to re-query its child's durably-parked approval correlations and
// re-emit each through the suspension sink -> a fresh
// `signal.correlation.register` frame reaches the hub -> the co-write lands the
// rows -> the run is approvable. The test then approves the recovered
// correlation through the real resolve route and asserts the parked run resumes
// to RunCompleted, so the re-emitted registration is proven to be a live route
// the resolver can drive, not merely a row that reappeared.
//
// Making "the register did not co-write" deterministic
// ----------------------------------------------------
// Emitting the register while the hub link is genuinely down would require
// holding the child in inference across a link drop -- a race with no harness
// hook to gate it, and this file must not modify the shared harness. Two
// hub-side facts let the scenario be forced deterministically instead:
//
//   1. On a mere WebSocket drop the sidecar subprocess and the deployment's
//      supervisor stay alive; only the hub link reconnects. The parked run is
//      never resumed, so the child keeps the approval park in durable state and
//      still reports it to a `parked-correlations.request`. So dropping the link
//      does NOT respawn the child: Trigger A (child re-establishment) never
//      fires on this path, and the ONLY re-emit driver on reconnect is Trigger
//      B (`onWorkflowAddressesRoutable`).
//
//   2. The parked run's `signal_correlation` + `approval` rows are hub-side DB
//      state. Deleting them while the link is down reproduces exactly the state
//      a hub that missed the suspend-time register would be in: a child parked
//      on a correlation with no rows at the hub.
//
// So the test fires the trigger with the link up (the run parks and its initial
// register co-writes the rows -- proving the run genuinely parked and the
// co-write is wired), captures the correlationId, drops the link, DELETES both
// rows while disconnected, asserts they are absent, then reconnects and asserts
// they reappear. The reappearance can ONLY come from the reconnect re-emitting
// the still-parked correlation's register: no other actor writes these rows,
// the run is never resumed, and the child is never respawned. A regression that
// removed the Trigger B re-emit would leave the rows deleted and the poll would
// time out.
//
// Harness justification: SPAWN-REAL. A real hub server, a real sidecar
// subprocess, a real workflow-process child, and a test inference provider. The
// suspend/park half runs against the real sidecar through the shared
// `deploy-flow-env` fixture; the co-write + row assertions run against a real
// migrated Postgres schema (`@intx/test-harness`), bridged by wiring the fixture
// hub's `registerSignalCorrelation` lookup to the real DB co-write. The drop is
// a genuine server-side WebSocket close; the recovery is the sidecar's real
// `hub-link` reconnect passing the hub's ownership challenge and the supervisor
// re-emitting the parked correlation.
//
// Single-test file. The `deploy-flow-env` (real sidecar subprocess + its
// on-disk warm step-state) is `beforeAll`-scoped, while the DB resets per test.
// A second test would inherit the first run's warm workspace and live parked
// run; a run-once guard below fails loud if a second test is ever added here
// rather than letting that assumption rot.

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
  type EventCollectorRegistry,
  type RepoId,
  type SessionService,
  type WorkflowRunHubPrincipal,
} from "@intx/hub-sessions";
import { signalName } from "@intx/types";
import type { GrantRule } from "@intx/types/authz";
import type { ApprovalSnapshot, HarnessConfig } from "@intx/types/runtime";
import { WireGrantRule } from "@intx/types/grant-wire";
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
  isWorkflowDerivedAddress,
  type ApprovalSet,
  type DeploySingleStepFn,
  type LaunchSessionFn,
  type SendMultiStepDeployFn,
  type WorkflowRepoWriter,
} from "@intx/workflow-deploy";
import { deriveDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";

import {
  SESSION_ID,
  dropHubLink,
  fireMailTrigger,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  waitForReconnect,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { toLaunchDeployContent } from "./launch-session-bridge";

const DEPLOYMENT_DOMAIN = "integration.interchange";
// A legacy `ins_<hex>` deployment identity (not a workflow-derived
// `ins_dep_<...>` address), matching the reconnect-survival fixtures. The
// reconnect ownership challenge re-routes it and fires
// `onWorkflowAddressesRoutable`, which is what drives Trigger B.
const INSTANCE_LOCAL = "dep0ec0ffee0ec0ffee0ec0ffee0ec0f";
const DEPLOYMENT_ID = INSTANCE_LOCAL;
const WORKFLOW_RUN_REF = "refs/heads/main";
const STEP_ID = "step1";

// The tool the model is told to call. Its grant is `ask`, so the call SUSPENDS
// instead of running, parking the run on the reserved control-plane channel.
const TOOL_NAME = "@intx/tools-mail/sidecar-bundle:mail_send";
const ASK_RESOURCE = `tool:${TOOL_NAME}`;

// The `mail_send` arguments the mock issues. The synthetic bundle writes its
// `to` argument into the file named by `body`; the run parks before the tool
// ever runs, so these only travel as far as the approval snapshot's arguments.
const CALL_TO = "correlation-recovery";
const CALL_BODY = "reconnect-tool-ran.txt";
const RESUME_REPLY_PREFIX = "done: ";

const TOOL_PINS: readonly ToolPackagePin[] = [
  { name: "@intx/tools-mail", version: "0.1.2" },
];

// The operator-approved `ask` grant shipped in-band on the deploy frame. Effect
// `ask` is the whole point: the tool call suspends awaiting an external
// decision rather than running, so the run parks and mints a correlation.
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

const TENANT_ID = "tnt_reconnect_reemit";
const DEFINITION_ASSET_ID = "ast_reconnect_wf";
const APPROVER_USER_ID = "usr_reconnect_approver";
const APPROVER_PRINCIPAL_ID = "prn_reconnect_approver";

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

// Approve-route scaffolding: a mock approver session plus the
// `approval:<slug>`/`resolve` grant that authorizes the real approve route,
// pointed at the same DB and fixture sidecar router. Mirrors the capstone
// approval test's setup so the re-emitted, recovered correlation can be
// resolved through the real resolver, not just observed as approvable.
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
      id: "session_reconnect_reemit",
      userId,
      token: "tok_reconnect_reemit",
      expiresAt: new Date("2999-01-01"),
      createdAt: now,
      updatedAt: now,
    },
  });
}

function notImpl(name: string): never {
  throw new Error(`reconnect re-emit approve mock: ${name} not implemented`);
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
 * sidecar router so both the suspend-time register and the reconnect re-emit
 * land durable rows on the same schema this test reads. Idempotent via
 * `registerIfAbsent` / `createIfAbsent`, so a re-emit after a delete re-inserts.
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
  "a hub reconnect re-emits a parked run's correlation and resolves it to completion",
  () => {
    // Run-once guard. The env (sidecar subprocess + on-disk warm step-state +
    // the live parked run) is shared across the describe block, so a second
    // test would inherit this run's parked correlation and the "absent then
    // present" assertions would stop meaning what they claim. Fail loud if a
    // second test is ever added rather than letting the assumption rot; a
    // genuinely independent scenario belongs in its own file with its own env.
    let hasRun = false;

    beforeAll(async () => {
      h = await createTestDb();
      env = await startDeployFlowEnv({
        // Persistent tool-call mock: re-issue the ask-granted call until its
        // result is in history, then reply. The run parks on the first call --
        // exactly the durable state Trigger B re-emits on reconnect -- and only
        // completes once the recovered correlation is approved at the end.
        inferenceApprovalToolCall: {
          toolName: TOOL_NAME,
          input: { to: CALL_TO, body: CALL_BODY },
          resultPrefix: RESUME_REPLY_PREFIX,
        },
        // Wire the real DB co-write into the fixture hub so a register frame
        // that reaches the hub writes real rows.
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

    test("re-registers the parked correlation on reconnect and resolves it to completion", async () => {
      if (hasRun) {
        throw new Error(
          "reconnect re-emit acceptance test assumes a single test per shared " +
            "env: the live parked run and warm step-state carry across, so a " +
            "second test would break the absent-then-present row assertions. " +
            "Add a new scenario in its own file with its own env instead.",
        );
      }
      hasRun = true;

      // A legacy `ins_<hex>` deployment address routes through the challenged
      // reconnect path (the source of Trigger B), not the keyless workflow set.
      expect(isWorkflowDerivedAddress(deploymentMailAddress)).toBe(false);

      // Seed the tenancy the co-write resolves against: a tenant, the workflow
      // definition asset the deployment references, the deployment row itself
      // (keyed by the run-repo slug, addressed by the deployment mail address),
      // and an active approver principal.
      await seedTenants(h.db, [{ id: TENANT_ID }]);
      await seedAsset(h.db, {
        id: DEFINITION_ASSET_ID,
        tenantId: TENANT_ID,
        kind: "workflow",
        name: "reconnect-reemit-wf",
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
        id: "agent-reconnect-reemit",
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
        principalId: "prin_reconnect-reemit-1",
        agentAddress: deploymentMailAddress,
        systemPrompt:
          "Fallback prompt (overridden per step by the orchestrator)",
        tools: [],
        // The `ask`-effect grant makes the tool call suspend in the child
        // instead of running, so the run parks and mints a correlation.
        grants: [ASK_GRANT],
        sources: [
          {
            id: "anthropic:mock-model",
            provider: "anthropic",
            baseURL: `http://localhost:${String(env.inference.server.port)}`,
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
              message: `reconnect-reemit test: write workflow repo ${args.workflowRepoId}`,
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

      // Wait for the deployment to ack its key so the sidecar's hub link is
      // fully live and the address is routable before firing the trigger and
      // before the drop below has a challenged address to re-route.
      await waitFor(() => env.hub.deployAcks.has(deploymentMailAddress), {
        timeoutMs: 20_000,
        diagnostics: env.sidecarDiagnostics,
      });
      expect(env.hub.router.getRoutableAddresses()).toContain(
        deploymentMailAddress,
      );

      // ---- park the run (link up) ----
      //
      // Fire the trigger. The model calls the tool; the call hits the ask grant
      // and suspends, so the run parks and mints a correlation. Because the link
      // is up, the suspend-time `signal.correlation.register` co-writes the rows
      // -- which both proves the run genuinely parked and gives us the
      // correlationId to track across the drop/reconnect.
      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<reconnect-reemit-1@integration.interchange>",
      });

      const runId = await waitForFirstRunId(env, workflowRunRepoId, {
        diagnostics: env.sidecarDiagnostics,
        timeoutMs: 20_000,
      });

      // Wait for the co-written pending approval row -- proof the reactor
      // suspended, the step parked, the register reached the hub, and the hub
      // co-wrote both rows.
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

      const parkedApprovalRows = await h.db
        .select()
        .from(approval)
        .where(eq(approval.deploymentId, deploymentSlug));
      expect(parkedApprovalRows).toHaveLength(1);
      const parkedApproval = parkedApprovalRows[0];
      if (parkedApproval === undefined) throw new Error("unreachable");
      expect(parkedApproval.status).toBe("pending");
      expect(parkedApproval.runId).toBe(runId);
      const correlationId = parkedApproval.correlationId;

      // The run has parked, not completed: the workflow-run log carries the
      // `SignalAwaited` marker for the reserved correlation channel. This is the
      // durable park the child re-reports to Trigger B's `parked-correlations.
      // request`, so wait for it to land before disturbing the rows.
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
      const parkedTypes = (
        await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId)
      ).map((e) => e.type);
      expect(parkedTypes).not.toContain("RunCompleted");
      expect(parkedTypes).not.toContain("RunFailed");

      // ---- drop the hub link ----
      //
      // A raw drop, not a settle: the run is parked (no pack push is in flight
      // to interrupt), and severing the link is the whole point. The sidecar
      // subprocess and the deployment's supervisor stay alive; only the hub link
      // reconnects, so the parked run is never resumed and the child is never
      // respawned. The address leaves the hub's routing index on the drop.
      dropHubLink(env);
      await waitFor(
        () =>
          !env.hub.router
            .getRoutableAddresses()
            .includes(deploymentMailAddress),
        { timeoutMs: 10_000, diagnostics: env.sidecarDiagnostics },
      );

      // ---- reproduce "the register did not co-write" ----
      //
      // Delete the parked run's rows while the link is down. This is exactly the
      // hub-side state a suspend that emitted its register while the hub was
      // down would leave: a child parked on a correlation with no rows at the
      // hub. Deleting while disconnected guarantees no concurrent re-emit can
      // race the delete (the address is unrouted, so any queued register frame
      // is dropped by the hub's ownership gate until the challenge re-routes it).
      await h.db
        .delete(approval)
        .where(eq(approval.deploymentId, deploymentSlug));
      await h.db
        .delete(signalCorrelation)
        .where(eq(signalCorrelation.deploymentId, deploymentSlug));

      const approvalsWhileDown = await h.db
        .select()
        .from(approval)
        .where(eq(approval.deploymentId, deploymentSlug));
      expect(approvalsWhileDown).toHaveLength(0);
      const correlationsWhileDown = await h.db
        .select()
        .from(signalCorrelation)
        .where(eq(signalCorrelation.deploymentId, deploymentSlug));
      expect(correlationsWhileDown).toHaveLength(0);

      // ---- reconnect: Trigger B re-emits the parked correlation ----
      //
      // The sidecar reconnects and re-proves ownership of the deployment
      // address. Passing the challenge fires the hub-link's
      // `onWorkflowAddressesRoutable`, which calls `reEmitParkedCorrelations`:
      // the supervisor re-queries the child's still-parked approval correlation
      // and re-emits its register, which now reaches the routed hub and
      // co-writes the rows again.
      const reconnectMs = await waitForReconnect(env, deploymentMailAddress, {
        timeoutMs: 30_000,
      });
      // A lower bound guards against a false "already routable" pass that never
      // actually dropped; the upper bound catches a hung link.
      expect(reconnectMs).toBeGreaterThan(1_000);
      expect(reconnectMs).toBeLessThan(30_000);
      expect(env.hub.router.getRoutableAddresses()).toContain(
        deploymentMailAddress,
      );

      // ---- the acceptance criterion: the run is approvable again ----
      //
      // The re-emit is fire-and-forget (the reconnect fan-out never awaits it),
      // so poll with a bounded deadline for both rows to reappear. Their return
      // can only come from the reconnect re-registering the still-parked
      // correlation: no other actor writes these rows, the run is never resumed,
      // and the child is never respawned across a mere link drop. A regression
      // that dropped the Trigger B re-emit would leave the rows deleted and this
      // wait would time out.
      await waitFor(
        async () => {
          const correlations = await h.db
            .select()
            .from(signalCorrelation)
            .where(eq(signalCorrelation.correlationId, correlationId));
          const approvals = await h.db
            .select()
            .from(approval)
            .where(eq(approval.correlationId, correlationId));
          return correlations.length === 1 && approvals.length === 1;
        },
        { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
      );

      const reemittedCorrelations = await h.db
        .select()
        .from(signalCorrelation)
        .where(eq(signalCorrelation.correlationId, correlationId));
      expect(reemittedCorrelations).toHaveLength(1);
      const reemittedCorrelation = reemittedCorrelations[0];
      if (reemittedCorrelation === undefined) throw new Error("unreachable");
      expect(reemittedCorrelation.runId).toBe(runId);
      expect(reemittedCorrelation.agentAddress).toBe(deploymentMailAddress);
      expect(reemittedCorrelation.signalName).toBe(signalName(correlationId));
      expect(reemittedCorrelation.resolvedAt).toBeNull();

      const reemittedApprovals = await h.db
        .select()
        .from(approval)
        .where(eq(approval.correlationId, correlationId));
      expect(reemittedApprovals).toHaveLength(1);
      const reemittedApproval = reemittedApprovals[0];
      if (reemittedApproval === undefined) throw new Error("unreachable");
      // Pending + unresolved: the recovered run is approvable. The approve step
      // below then proves it is actually resolvable end-to-end.
      expect(reemittedApproval.status).toBe("pending");
      expect(reemittedApproval.runId).toBe(runId);
      expect(reemittedApproval.agentAddress).toBe(deploymentMailAddress);
      expect(reemittedApproval.resolvedAt).toBeNull();
      // The re-emitted register carried the child's durable approval snapshot,
      // so the approvable row exposes the same tool the model asked to invoke.
      expect(reemittedApproval.toolDefinition).toEqual({
        name: TOOL_NAME,
        description: "Send a mail message",
        inputSchema: {
          type: "object",
          properties: { to: { type: "string" }, body: { type: "string" } },
          required: ["to", "body"],
        },
      });
      expect(reemittedApproval.toolArguments).toEqual({
        to: CALL_TO,
        body: CALL_BODY,
      });

      // The recovered registration is not just approvable -- it is functionally
      // routable. Approve through the real resolve route (same DB, same fixture
      // sidecar router): the resolver claims the re-emitted correlation and
      // delivers the decision to the parked run in the real subprocess. If the
      // re-emitted row were a dead record rather than a live route, the resume
      // would never arrive and this would time out.
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
        `/api/tenants/${TENANT_ID}/approvals/${reemittedApproval.id}/approve`,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ scope: "once" }),
        },
      );
      expect(approveRes.status).toBe(200);

      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        runId,
        {
          timeoutMs: 30_000,
          diagnostics: env.sidecarDiagnostics,
        },
      );
      expect(terminal.type).toBe("RunCompleted");

      // The delivered decision landed on the parked correlation channel, and the
      // approval + correlation rows are now resolved/claimed -- still exactly one
      // of each, so the recovery did not strand or duplicate a row.
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
          `no SignalReceived for the recovered correlation ${correlationId}`,
        );
      }

      const resolvedApprovals = await h.db
        .select()
        .from(approval)
        .where(eq(approval.correlationId, correlationId));
      expect(resolvedApprovals).toHaveLength(1);
      const resolvedApproval = resolvedApprovals[0];
      if (resolvedApproval === undefined) throw new Error("unreachable");
      expect(resolvedApproval.status).toBe("approved");
      expect(resolvedApproval.resolvedAt).not.toBeNull();

      const claimedCorrelations = await h.db
        .select()
        .from(signalCorrelation)
        .where(eq(signalCorrelation.correlationId, correlationId));
      expect(claimedCorrelations).toHaveLength(1);
      const claimedCorrelation = claimedCorrelations[0];
      if (claimedCorrelation === undefined) throw new Error("unreachable");
      expect(claimedCorrelation.resolvedAt).not.toBeNull();
      expect(claimedCorrelation.signalId).not.toBeNull();
    }, 240_000);
  },
);

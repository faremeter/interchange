// End-to-end proof that a `scope: "always"` approval is a RUN-LOCAL standing
// approval on real substrate: after the operator approves-with-always, the SAME
// run does not ask again for that tool.
//
// Flow: deploy a single-step source-ref workflow whose agent carries the inline
// ask-marked `mail_send` tool -> the model's call suspends on the committed
// `ask` floor -> approve via the real hub HTTP route with scope "always" ->
// the resolver mutates the run's committed grant -> allow -> the parked call
// re-dispatches; a SECOND call to the SAME tool sees the now-`allow` grant and
// runs WITHOUT re-parking.
//
// Assertions: one pending row pair + one SignalAwaited while parked; approve ->
// 200; the run completes with BOTH calls executed and NO re-park (one
// SignalAwaited, one row pair) -- the mock must have seen two tool results.
//
// The mock is the discriminator: it counts `tool_result` blocks and issues the
// call twice before replying; if the mutated grant never reached the child, the
// second call re-hits the `ask` floor and re-parks, failing the single-park
// assertion. The two calls carry DISTINCT `tool_use` ids so the re-dispatched
// first call is not confused with the second.
//
// Why the second call is not defeated by the tool-mark floor: the deployment is
// SOURCE-REF, and the child skips the tool-mark floor on the source-ref lineage
// -- the frozen snapshot's `tool:<name>` grant authorizes the inline tool
// directly, so the mutated `allow` is the sole matching grant.
//
// Harness: SPAWN-REAL -- real hub server, sidecar subprocess, workflow-process
// child, and migrated Postgres schema; started and approved through the real hub
// HTTP routes with a test-local two-call inference mock.
//
// Single-test file: the env is beforeAll-scoped while the DB resets per test; a
// second test would inherit the first run's warm workspace and live run. A
// run-once guard below fails loud if a second test is ever added here.

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
import { and, eq, inArray } from "drizzle-orm";

import {
  createApprovalStore,
  createGrantStore,
  createSignalCorrelationStore,
  createWorkflowRunStore,
} from "@intx/db";
import {
  approval,
  signalCorrelation,
  tenant as tenantTable,
  workflowRun,
} from "@intx/db/schema";
import { createApp, type GetSession } from "@intx/hub-api";
import { generateId } from "@intx/hub-common";
import {
  createAssetService,
  type EventCollectorRegistry,
} from "@intx/hub-sessions";
import { signalName } from "@intx/types";
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
  seedGrant,
  seedPrincipal,
  seedPrincipalKey,
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
// A single-agent run id: `run_` + a hex-shaped local part, matching the other
// warm single-step approval fixtures so the sidecar's deploy router applies the
// single-agent identity strategy. The run collapses onto its anchor, so the
// deployment id, the anchor `workflow_run` id, and the run id are all this value.
const DEPLOYMENT_ID = "run_5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e";
const STEP_ID = "step1";
const STEP_AGENT_ID = "agent-scope-always";

// The two sequential calls the mock issues to the same ask-marked tool. Each
// call writes its `body` argument as a filename under the step workspace with the
// `to` argument as content. Distinct filenames make BOTH executions observable:
// the first appears only after the approve-with-always re-dispatch, the second
// only if the mutated grant let the second call run instead of re-parking.
const FIRST_CALL = {
  to: "scope-always-first",
  body: "scope-always-call-1.txt",
};
const SECOND_CALL = {
  to: "scope-always-second",
  body: "scope-always-call-2.txt",
};
const CALL_INPUTS: readonly { to: string; body: string }[] = [
  FIRST_CALL,
  SECOND_CALL,
];
// The mock replies (ending the run) with this prefix on the last tool result,
// but only after it has seen BOTH results in history -- so a completion is proof
// the second call actually ran.
const RESUME_REPLY_PREFIX = "done: ";

// Tenant / operator identity for the real HTTP calls. One operator principal
// holds both the `workflow-run:<id>/manage` grant the trigger route requires and
// the `approval:<id>/resolve` grant the approve route requires.
const TENANT_ID = "tnt_scope_always";
const OPERATOR_USER_ID = "usr_scope_always_operator";
const OPERATOR_PRINCIPAL_ID = "prn_scope_always_operator";
const DEFINITION_ASSET_ID = "ast_scope_always_wf";

let env: DeployFlowEnv;
let h: TestDb;
let inferenceServer: ReturnType<typeof startTwoCallMockInference> | undefined;

const deploymentMailAddress = deriveRunAddress({
  runId: DEPLOYMENT_ID,
  domain: DEPLOYMENT_DOMAIN,
});

// --- test-local two-call inference mock ------------------------------------
//
// A minimal Anthropic-style SSE server that issues the ask-marked tool call
// `CALL_INPUTS.length` times in a single run before replying: while fewer
// `tool_result` blocks are present in history it re-issues a `tool_use` (with a
// distinct id per call); once all are present it replies with
// `${RESUME_REPLY_PREFIX}<last result>`. The shared harness mock only drives one
// call per run, so this small server -- not a harness change -- is what lets the
// run make the SECOND call that proves the run's committed grant was mutated.

type MockBlock = {
  type?: string;
  text?: string;
  content?: string | MockBlock[];
};
type MockMessage = { role?: string; content?: string | MockBlock[] };
type MockRequest = { tools?: { name: string }[]; messages?: MockMessage[] };

function sse(event: string, data: unknown): string {
  return `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
}

function textTurnText(text: string): string[] {
  return [
    sse("message_start", {
      type: "message_start",
      message: {
        id: "msg_mock",
        type: "message",
        role: "assistant",
        content: [],
        model: "mock-model",
        stop_reason: null,
        usage: { input_tokens: 10, output_tokens: 0 },
      },
    }),
    sse("content_block_start", {
      type: "content_block_start",
      index: 0,
      content_block: { type: "text", text: "" },
    }),
    sse("content_block_delta", {
      type: "content_block_delta",
      index: 0,
      delta: { type: "text_delta", text },
    }),
    sse("content_block_stop", { type: "content_block_stop", index: 0 }),
    sse("message_delta", {
      type: "message_delta",
      delta: { stop_reason: "end_turn" },
      usage: { output_tokens: 20 },
    }),
    sse("message_stop", { type: "message_stop" }),
  ];
}

function toolUseTurn(
  toolName: string,
  input: Record<string, unknown>,
  toolUseId: string,
): string[] {
  return [
    sse("message_start", {
      type: "message_start",
      message: {
        id: "msg_mock_tooluse",
        type: "message",
        role: "assistant",
        content: [],
        model: "mock-model",
        stop_reason: null,
        usage: { input_tokens: 10, output_tokens: 0 },
      },
    }),
    sse("content_block_start", {
      type: "content_block_start",
      index: 0,
      content_block: {
        type: "tool_use",
        id: toolUseId,
        name: toolName,
        input: {},
      },
    }),
    sse("content_block_delta", {
      type: "content_block_delta",
      index: 0,
      delta: {
        type: "input_json_delta",
        partial_json: JSON.stringify(input),
      },
    }),
    sse("content_block_stop", { type: "content_block_stop", index: 0 }),
    sse("message_delta", {
      type: "message_delta",
      delta: { stop_reason: "tool_use" },
      usage: { output_tokens: 20 },
    }),
    sse("message_stop", { type: "message_stop" }),
  ];
}

function flattenBlockText(content: string | MockBlock[] | undefined): string {
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    return content
      .filter((b) => b.type === "text" && b.text !== undefined)
      .map((b) => b.text ?? "")
      .join("");
  }
  return "";
}

function countToolResults(req: MockRequest): number {
  let count = 0;
  for (const message of req.messages ?? []) {
    if (!Array.isArray(message.content)) continue;
    for (const block of message.content) {
      if (block.type === "tool_result") count += 1;
    }
  }
  return count;
}

function lastToolResultText(req: MockRequest): string {
  let last = "";
  for (const message of req.messages ?? []) {
    if (!Array.isArray(message.content)) continue;
    for (const block of message.content) {
      if (block.type === "tool_result") last = flattenBlockText(block.content);
    }
  }
  return last;
}

function startTwoCallMockInference(): {
  server: ReturnType<typeof Bun.serve>;
  requests: MockRequest[];
} {
  const requests: MockRequest[] = [];
  const server = Bun.serve({
    port: 0,
    async fetch(req) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- this is a test mock server that only receives requests from the sidecar under test; the Anthropic request shape is known
      const body = (await req.json()) as MockRequest;
      requests.push(body);

      const seenResults = countToolResults(body);
      let events: string[];
      if (seenResults < CALL_INPUTS.length) {
        const input = CALL_INPUTS[seenResults];
        if (input === undefined) throw new Error("unreachable");
        events = toolUseTurn(
          MAIL_TOOL_NAME,
          input,
          `toolu_scope_${seenResults}`,
        );
      } else {
        events = textTurnText(
          `${RESUME_REPLY_PREFIX}${lastToolResultText(body)}`,
        );
      }

      const stream = new ReadableStream({
        start(controller) {
          for (const event of events) {
            controller.enqueue(new TextEncoder().encode(event));
          }
          controller.close();
        },
      });
      return new Response(stream, {
        headers: { "content-type": "text/event-stream" },
      });
    },
  });
  return { server, requests };
}

// --- approve-route scaffolding ---------------------------------------------

function createMockGetSession(userId: string): GetSession {
  const now = new Date("2025-01-01");
  return async () => ({
    user: {
      id: userId,
      email: "operator@example.com",
      emailVerified: true,
      name: "Operator",
      createdAt: now,
      updatedAt: now,
    },
    session: {
      id: "session_scope_always",
      userId,
      token: "tok_scope_always",
      expiresAt: new Date("2999-01-01"),
      createdAt: now,
      updatedAt: now,
    },
  });
}

function notImpl(name: string): never {
  throw new Error(`scope-always mock: ${name} not implemented`);
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

/**
 * The real hub co-write, mirroring `createHubSessionLookups`'s
 * `registerSignalCorrelation`: resolve tenancy from the deployment's anchor run
 * the address names, cross-check the frame's `anchorRunId` against the slug the
 * address derives, and co-write the `signal_correlation` + `approval` rows in one
 * transaction. Wired into the fixture hub's sidecar router so the parked run's
 * `signal.correlation.register` frame lands durable rows the approve route reads.
 */
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
        definitionId: workflowRun.definitionId,
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
      // The real trigger route already anchored the run row, so this is a no-op;
      // it mirrors the production co-write's lazy anchor for parity.
      await workflowRunStore.createIfAbsent(
        {
          id: runId,
          anchorRunId: anchorDbId,
          tenantId,
          definitionId: anchor.definitionId,
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
  "a scope:always approval is run-local: the same run does not ask again",
  () => {
    // Run-once guard. The env (sidecar subprocess + on-disk warm step-state +
    // the live run) is shared across the describe block, so a second test would
    // inherit this run's warm workspace and completed run and the sentinel /
    // single-park assertions would stop meaning what they claim.
    let hasRun = false;

    beforeAll(async () => {
      h = await createTestDb();
      inferenceServer = startTwoCallMockInference();
      env = await startDeployFlowEnv({
        // Wire the real DB co-write so the parked run's register frame writes
        // real approval/correlation rows the approve route resolves.
        registerSignalCorrelation: createRegisterSignalCorrelation(h.db),
      });
    });

    afterAll(async () => {
      if (env !== undefined) await env.teardown();
      if (inferenceServer !== undefined)
        await inferenceServer.server.stop(true);
      if (h !== undefined) await h.close();
    });

    beforeEach(async () => {
      await h.reset();
    });

    test("resumes on approve-with-always and the second call runs without re-parking", async () => {
      if (hasRun) {
        throw new Error(
          "scope-always round-trip assumes a single test per shared env: the " +
            "warm step-state and live run carry across, so a second test would " +
            "break the sentinel and single-park assertions. Add a new scenario " +
            "in its own file with its own env instead.",
        );
      }
      hasRun = true;

      if (inferenceServer === undefined) throw new Error("unreachable");
      const inferencePort = inferenceServer.server.port;
      if (inferencePort === undefined) {
        throw new Error("two-call mock inference server has no bound port");
      }

      // Seed the tenancy the trigger route, the co-write, and the approve route
      // resolve against: a tenant whose domain matches the deploy domain (so the
      // trigger route's derived address matches the deployed sidecar address), an
      // active operator user-principal, the `workflow`-kind definition asset the
      // frozen definition projects over (its creator resolves creator grants),
      // and the operator's two grants.
      await h.db.insert(tenantTable).values({
        id: TENANT_ID,
        name: TENANT_ID,
        slug: tenantSlugFromId(TENANT_ID),
        domain: DEPLOYMENT_DOMAIN,
        parentId: null,
      });
      await seedPrincipal(h.db, {
        id: OPERATOR_PRINCIPAL_ID,
        tenantId: TENANT_ID,
        kind: "user",
        refId: OPERATOR_USER_ID,
        status: "active",
      });
      // The operator signs the trigger mail with its durable hub principal key,
      // which production mints at principal creation; the direct row insert
      // above bypasses that, so mint it here or the trigger's sign() throws.
      await seedPrincipalKey(h.db, OPERATOR_PRINCIPAL_ID);
      await seedAsset(h.db, {
        id: DEFINITION_ASSET_ID,
        tenantId: TENANT_ID,
        kind: "workflow",
        name: "scope-always-wf",
        creatorPrincipalId: OPERATOR_PRINCIPAL_ID,
      });
      await seedGrant(h.db, {
        id: "grant-operator-manage",
        tenantId: TENANT_ID,
        resource: `workflow-run:${DEPLOYMENT_ID}`,
        action: "manage",
        effect: "allow",
        origin: "system",
        principalId: OPERATOR_PRINCIPAL_ID,
      });
      await seedGrant(h.db, {
        id: "grant-operator-resolve",
        tenantId: TENANT_ID,
        resource: `approval:${DEPLOYMENT_ID}`,
        action: "resolve",
        effect: "allow",
        origin: "system",
        principalId: OPERATOR_PRINCIPAL_ID,
      });

      const inferenceSource: InferenceSource = {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${String(inferencePort)}`,
        credentialId: "sk-mock",
        model: "mock-model",
      };

      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentId: DEPLOYMENT_ID,
        tenantId: "tenant-1",
        principalId: "prin_integration-1",
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

      const entryModule = singleStepMailToolEntry({
        variant: "ask",
        stepId: STEP_ID,
        systemPrompt: "You are the single-step agent under standing approval.",
        address: deploymentMailAddress,
        agentId: STEP_AGENT_ID,
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

      // The source-ref frame round-trips through the real sidecar subprocess, so
      // routability is asynchronous. Wait for it before driving the trigger route.
      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { diagnostics: env.sidecarDiagnostics },
      );

      // The two per-call sentinel paths in the warm agent's stable workspace.
      const workspaceDir = path.join(
        env.sidecar.dataDir,
        "workflow-step-state",
        workflowRunRepoId.id,
        "warm",
        encodeURIComponent(STEP_ID),
        "workspace",
      );
      const firstCallSentinel = path.join(workspaceDir, FIRST_CALL.body);
      const secondCallSentinel = path.join(workspaceDir, SECOND_CALL.body);

      // Guard the single-test warm-state assumption: no prior run left a sentinel.
      expect(fs.existsSync(firstCallSentinel)).toBe(false);
      expect(fs.existsSync(secondCallSentinel)).toBe(false);

      // Build the real hub app. One operator principal drives both the trigger
      // route (workflow-run manage grant) and the approve route (approval resolve
      // grant); the DB grant store reads both seeded grants. The real
      // sessionService materializes and COMMITS the run's grants from the frozen
      // ask snapshot -- the grant `setRunToolGrantEffect` later mutates in place.
      const app = createApp({
        getSession: createMockGetSession(OPERATOR_USER_ID),
        authHandler: () => new Response("", { status: 404 }),
        db: h.db,
        grantStore: createGrantStore(h.db),
        sidecarRouter: env.hub.router,
        sessionService: env.hub.sessionService,
        eventCollectors: createMockEventCollectors(),
        assetService: createAssetService({
          db: h.db,
          repoStore: env.hub.agentRepoStore.repoStore,
        }),
        repoStore: env.hub.agentRepoStore.repoStore,
        maxTarballBytes: 10_000_000,
      });

      // ---- start the run through the real trigger route ----
      const triggerRes = await app.request(
        `/api/tenants/${TENANT_ID}/workflows/${DEPLOYMENT_ID}/mail`,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ content: "kick off the run" }),
        },
      );
      if (triggerRes.status !== 202) {
        const body: unknown = await triggerRes.json();
        throw new Error(
          `expected 202 from /mail, got ${String(triggerRes.status)}: ${JSON.stringify(body)}\n${env.sidecarDiagnostics()}`,
        );
      }

      const runId = await waitForFirstRunId(env, workflowRunRepoId, {
        diagnostics: env.sidecarDiagnostics,
      });

      // ---- Assertion 1: parked before approval ----
      await waitFor(
        async () => {
          const rows = await h.db
            .select()
            .from(approval)
            .where(eq(approval.anchorRunId, DEPLOYMENT_ID));
          return rows.length === 1;
        },
        { diagnostics: env.sidecarDiagnostics },
      );

      const pendingRows = await h.db
        .select()
        .from(approval)
        .where(eq(approval.anchorRunId, DEPLOYMENT_ID));
      expect(pendingRows).toHaveLength(1);
      const pendingRow = pendingRows[0];
      if (pendingRow === undefined) throw new Error("unreachable");
      expect(pendingRow.status).toBe("pending");
      expect(pendingRow.runId).toBe(runId);
      expect(pendingRow.agentAddress).toBe(deploymentMailAddress);
      expect(pendingRow.resolvedAt).toBeNull();
      // The parked call is the FIRST call the mock issued.
      expect(pendingRow.toolArguments).toEqual({ ...FIRST_CALL });
      const correlationId = pendingRow.correlationId;
      const approvalId = pendingRow.id;

      // The run parked, not completed: the log carries a `SignalAwaited` for the
      // reserved correlation channel and no terminal event.
      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
          return events.some(
            (e) =>
              e.type === "SignalAwaited" &&
              e.body["signalName"] === signalName(correlationId),
          );
        },
        { diagnostics: env.sidecarDiagnostics },
      );
      const parkedTypes = (
        await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId)
      ).map((e) => e.type);
      expect(parkedTypes).not.toContain("RunCompleted");
      expect(parkedTypes).not.toContain("RunFailed");
      expect(parkedTypes).not.toContain("RunCancelled");

      // Neither tool call has run: the ask floor suspended the first call before
      // execution, and the second call has not been issued yet.
      expect(fs.existsSync(firstCallSentinel)).toBe(false);
      expect(fs.existsSync(secondCallSentinel)).toBe(false);

      // ---- Assertion 2: approve with scope "always" -> 200 ----
      const approveRes = await app.request(
        `/api/tenants/${TENANT_ID}/approvals/${approvalId}/approve`,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ scope: "always" }),
        },
      );
      if (approveRes.status !== 200) {
        const body: unknown = await approveRes.json();
        throw new Error(
          `expected 200 from approve, got ${String(approveRes.status)}: ${JSON.stringify(body)}\n${env.sidecarDiagnostics()}`,
        );
      }
      const approveBody: unknown = await approveRes.json();
      expect(approveBody).toMatchObject({
        id: approvalId,
        status: "approved",
        scope: "always",
      });

      // ---- Assertion 3: resume + second call runs without re-parking ----
      //
      // The resolve mutated the run's committed grant ask -> allow for the tool
      // and pushed it to the live child. The parked first call re-dispatches and
      // runs; the agent then issues the SECOND call, which sees the now-`allow`
      // grant and RUNS instead of re-parking, so the mock sees a second tool
      // result and replies -> the run completes. A regression that never mutated
      // the grant would re-park the second call, mint a second
      // correlation/approval, and this wait would time out.
      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        runId,
        {
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

      // Both tool calls executed: each wrote its own sentinel. The first exists
      // only because the approve-with-always re-dispatched the parked call; the
      // second exists only because the mutated grant let the second call run.
      expect(fs.existsSync(firstCallSentinel)).toBe(true);
      expect(fs.readFileSync(firstCallSentinel, "utf-8")).toBe(FIRST_CALL.to);
      expect(fs.existsSync(secondCallSentinel)).toBe(true);
      expect(fs.readFileSync(secondCallSentinel, "utf-8")).toBe(SECOND_CALL.to);

      // No re-park: exactly one `SignalAwaited` for the run, and exactly one
      // `approval` / `signal_correlation` row ever raised. A second ask
      // suspension for the second call would have minted a fresh correlation and
      // a second approval row.
      const finalEvents = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        runId,
      );
      const signalAwaitedCount = finalEvents.filter(
        (e) => e.type === "SignalAwaited",
      ).length;
      expect(signalAwaitedCount).toBe(1);

      const allApprovals = await h.db
        .select()
        .from(approval)
        .where(eq(approval.anchorRunId, DEPLOYMENT_ID));
      expect(allApprovals).toHaveLength(1);

      const allCorrelations = await h.db
        .select()
        .from(signalCorrelation)
        .where(eq(signalCorrelation.anchorRunId, DEPLOYMENT_ID));
      expect(allCorrelations).toHaveLength(1);

      // The approval row is terminal: approved, scoped always, resolved.
      const resolvedRows = await h.db
        .select()
        .from(approval)
        .where(eq(approval.id, approvalId));
      expect(resolvedRows).toHaveLength(1);
      const resolvedRow = resolvedRows[0];
      if (resolvedRow === undefined) throw new Error("unreachable");
      expect(resolvedRow.status).toBe("approved");
      expect(resolvedRow.scope).toBe("always");
      expect(resolvedRow.resolvedAt).not.toBeNull();
    }, 240_000);
  },
);

// End-to-end proof that a `scope: "always"` approval reaches a LIVE CHILD run:
// after the operator approves-with-always a call parked inside a childWorkflow
// child, the SAME child does not ask again for that tool.
//
//   deploy a source-ref parent whose only step spawns an inline childWorkflow
//   child; the child's step agent carries the inline ask-marked `mail_send`
//   tool
//     -> start the run through the REAL trigger route, which commits the run's
//        grants (a `tool:<name>` grant whose effect is `ask`); the child is
//        born with a capped copy of them in its own write-once grants file
//     -> the child's agent calls the tool; the call hits the `ask` grant and
//        SUSPENDS, and the park is relayed to the deployment's top-level run,
//        whose register frame co-writes a pending `approval` row
//     -> approve via the REAL hub HTTP route with scope "always"
//     -> the resolver changes the top-level run's committed grant ask -> allow;
//        the hub router sends the committed grants ahead of the resume signal,
//        and the run child replaces its top-level grants snapshot
//     -> the parked call resumes and runs; the child's agent then makes a
//        SECOND call to the SAME tool IN THE SAME LIVE CHILD
//     -> the child reads the top-level decision through its grants overlay, so
//        the second call RUNS WITHOUT RE-PARKING, and the run completes.
//
// Load-bearing assertions:
//   1. Before approval: exactly one pending `approval` row, on the top-level
//      run, and no terminal event.
//   2. Approve with scope "always" -> 200.
//   3. After approval: the run reaches `RunCompleted` (the mock replies only
//      after it has seen TWO tool results, so the second call ran), and there
//      was exactly ONE `approval` / `signal_correlation` row: the second call
//      did not ask again.
//   4. The child's own grants file is unchanged: it still holds the `ask` it
//      was born with. The decision reaches the child at read time only.
//
// Harness composition: SPAWN-REAL, the same as scope-always-roundtrip: a real
// hub server, a real sidecar subprocess, a real workflow-process child, a real
// migrated Postgres schema, the real trigger and approve routes, the real
// register co-write and the real committed-grants loader. The inference server
// is a test-local two-call mock.
//
// Single-test file, for the same reason as scope-always-roundtrip: the
// sidecar env is shared across the describe block. A run-once guard fails
// loud if a second test is ever added here.

import fs from "node:fs";
import path from "node:path";

import { type } from "arktype";
import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { eq } from "drizzle-orm";

import { createGrantStore } from "@intx/db";
import {
  approval,
  signalCorrelation,
  tenant as tenantTable,
} from "@intx/db/schema";
import { createApp, createRunGrantsLoader } from "@intx/hub-api";
import { createAssetService, type RepoId } from "@intx/hub-sessions";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
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
  type ApprovalSet,
} from "@intx/workflow-deploy";

import {
  SESSION_ID,
  deployWorkflowSourceForTest,
  listRunIds,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { childWorkflowEntry } from "./fixtures/child-workflow";
import { MAIL_TOOL_NAME } from "./fixtures/mail-tool";
import {
  createMockEventCollectors,
  createMockGetSession,
  createRegisterSignalCorrelation,
  startTwoCallMockInference,
} from "./scope-always-helpers";

const DEPLOYMENT_DOMAIN = "integration.interchange";
// A childWorkflow deployment: the run is a multi-step container, not a
// single-agent run, so its id is not hex-shaped.
const DEPLOYMENT_ID = "run_cw-scope-always-1";
const PARENT_WORKFLOW_ID = `wf_${DEPLOYMENT_ID}`;
const CHILD_WORKFLOW_ID = `wf_child_${DEPLOYMENT_ID}`;
const SPAWN_STEP_ID = "spawn";
const CHILD_STEP_ID = "childStep";
const CHILD_AGENT_ID = "agent-cw-scope-always-child";

// The two sequential calls the mock issues to the same ask-marked tool, in the
// child's one step.
const CALL_INPUTS: readonly { to: string; body: string }[] = [
  { to: "cw-scope-always-first", body: "cw-scope-always-call-1.txt" },
  { to: "cw-scope-always-second", body: "cw-scope-always-call-2.txt" },
];
// The mock replies (ending the child step) with this prefix only after it has
// seen BOTH tool results, so a completed run is proof the second call ran.
const RESUME_REPLY_PREFIX = "done: ";

const TENANT_ID = "tnt_cw_scope_always";
const OPERATOR_USER_ID = "usr_cw_scope_always_operator";
const OPERATOR_PRINCIPAL_ID = "prn_cw_scope_always_operator";
const DEFINITION_ASSET_ID = "ast_cw_scope_always_wf";

const GrantsFile = type({ grants: "unknown[]" }).onUndeclaredKey("ignore");

let env: DeployFlowEnv;
let h: TestDb;
let inferenceServer: ReturnType<typeof startTwoCallMockInference> | undefined;

const deploymentMailAddress = deriveRunAddress({
  runId: DEPLOYMENT_ID,
  domain: DEPLOYMENT_DOMAIN,
});
const childMailAddress = deriveRunAddress({
  runId: `${DEPLOYMENT_ID}-child`,
  domain: DEPLOYMENT_DOMAIN,
});

/**
 * The top-level container run: not the child, whose run id derives from the
 * child's workflow id. Polled, because the trigger only queues the run.
 */
async function topLevelRunId(workflowRunRepoId: RepoId): Promise<string> {
  let found: string | undefined;
  await waitFor(
    async () => {
      found = (await listRunIds(env, workflowRunRepoId)).find(
        (id) => !id.includes("__") && id !== CHILD_WORKFLOW_ID,
      );
      return found !== undefined;
    },
    { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
  );
  if (found === undefined) {
    throw new Error("no top-level run after the trigger");
  }
  return found;
}

/** The child's run id, as the parent recorded it when it spawned the child. */
async function spawnedChildRunId(parentRunId: string): Promise<string> {
  let childRunId: string | undefined;
  await waitFor(
    async () => {
      const events = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        parentRunId,
      );
      const spawned = events.find((e) => e.type === "ChildSpawned");
      const id = spawned?.body["childRunId"];
      if (typeof id === "string") childRunId = id;
      return childRunId !== undefined;
    },
    { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
  );
  if (childRunId === undefined) {
    throw new Error("no ChildSpawned on the parent");
  }
  return childRunId;
}

// A run's grants file on the sidecar's on-disk workflow-run repo.
function readRunGrantsFile(repoId: string, runId: string): unknown[] {
  const filePath = path.join(
    env.sidecar.dataDir,
    "workflow-runs",
    repoId,
    "runs",
    runId,
    "grants.json",
  );
  const parsed: unknown = JSON.parse(fs.readFileSync(filePath, "utf-8"));
  return GrantsFile.assert(parsed).grants;
}

// The effect of the run's `tool:<mail_send>` grant in a grants file.
function toolEffects(grants: readonly unknown[]): unknown[] {
  return grants.flatMap((grant) =>
    typeof grant === "object" &&
    grant !== null &&
    "resource" in grant &&
    grant.resource === `tool:${MAIL_TOOL_NAME}` &&
    "effect" in grant
      ? [grant.effect]
      : [],
  );
}

describe.skipIf(!harnessDbEnvAvailable())(
  "a scope:always approval reaches a live childWorkflow child",
  () => {
    let hasRun = false;

    beforeAll(async () => {
      h = await createTestDb();
      inferenceServer = startTwoCallMockInference({
        toolName: MAIL_TOOL_NAME,
        calls: CALL_INPUTS,
        replyPrefix: RESUME_REPLY_PREFIX,
        toolUseIdPrefix: "toolu_cw_scope_",
      });
      env = await startDeployFlowEnv({
        registerSignalCorrelation: createRegisterSignalCorrelation(h.db),
        loadRunGrants: createRunGrantsLoader({ db: h.db }),
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

    test("the child's second call to the tool runs without re-parking", async () => {
      if (hasRun) {
        throw new Error(
          "childWorkflow scope-always round-trip assumes a single test per " +
            "shared env. Add a new scenario in its own file instead.",
        );
      }
      hasRun = true;

      if (inferenceServer === undefined) throw new Error("unreachable");
      const inferencePort = inferenceServer.server.port;
      if (inferencePort === undefined) {
        throw new Error("two-call mock inference server has no bound port");
      }

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
      await seedPrincipalKey(h.db, OPERATOR_PRINCIPAL_ID);
      await seedAsset(h.db, {
        id: DEFINITION_ASSET_ID,
        tenantId: TENANT_ID,
        kind: "workflow",
        name: "cw-scope-always-wf",
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
        `mail.address:${childMailAddress}`,
        `mail.send:${DEPLOYMENT_DOMAIN}`,
        `tool:${MAIL_TOOL_NAME}`,
      ]);

      const handle = await deployWorkflowSourceForTest(env, {
        entryModule: childWorkflowEntry({
          workflowId: PARENT_WORKFLOW_ID,
          address: deploymentMailAddress,
          steps: [],
          spawns: [
            {
              stepId: SPAWN_STEP_ID,
              after: [],
              child: {
                workflowId: CHILD_WORKFLOW_ID,
                address: childMailAddress,
                steps: [
                  {
                    stepId: CHILD_STEP_ID,
                    agentId: CHILD_AGENT_ID,
                    systemPrompt:
                      "You are the child step agent under standing approval.",
                    tool: "ask",
                  },
                ],
              },
            },
          ],
        }),
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: DEPLOYMENT_ID,
        sourceAuthorityPrincipalId: OPERATOR_PRINCIPAL_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: deploymentMailAddress,
        approvals: operatorApprovals,
        config,
      });
      expect(handle.publicKey).toBeTruthy();
      const workflowRunRepoId = handle.workflowRunRepoId;

      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { diagnostics: env.sidecarDiagnostics },
      );

      const app = createApp({
        getSession: createMockGetSession(OPERATOR_USER_ID, "cw_scope_always"),
        authHandler: () => new Response("", { status: 404 }),
        db: h.db,
        grantStore: createGrantStore(h.db),
        sidecarRouter: env.hub.router,
        sessionService: env.hub.sessionService,
        eventCollectors: createMockEventCollectors(
          "childWorkflow scope-always",
        ),
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

      const runId = await topLevelRunId(workflowRunRepoId);
      const childRunId = await spawnedChildRunId(runId);

      // ---- Assertion 1: parked on the top-level run before approval ----
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
      expect(pendingRow.toolArguments).toEqual({ ...CALL_INPUTS[0] });
      const approvalId = pendingRow.id;

      const parkedTypes = (
        await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId)
      ).map((e) => e.type);
      expect(parkedTypes).not.toContain("RunCompleted");
      expect(parkedTypes).not.toContain("RunFailed");

      // The child was born with the ask: its grants file is the cap's copy.
      const childGrantsAtBirth = readRunGrantsFile(
        workflowRunRepoId.id,
        childRunId,
      );
      expect(toolEffects(childGrantsAtBirth)).toEqual(["ask"]);

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
      expect(await approveRes.json()).toMatchObject({
        id: approvalId,
        status: "approved",
        scope: "always",
      });

      // ---- Assertion 3: the child's second call runs without re-parking ----
      //
      // A child that still read its birth grants would re-park the second
      // call, mint a second approval, and this wait would time out.
      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        runId,
        { diagnostics: env.sidecarDiagnostics },
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

      // ---- Assertion 4: the child's grants file is unchanged ----
      expect(readRunGrantsFile(workflowRunRepoId.id, childRunId)).toEqual(
        childGrantsAtBirth,
      );
    }, 240_000);
  },
);

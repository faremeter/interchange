// Phase 4.1 lock: a single-step agent routed through the
// spawned workflow-process child resolves its GRANTS, preserves its
// `run_<hex>` identity, and threads its inference events to the hub
// timeline keyed to the deploy's session.
//
// This is the foundation sub-step every later Phase 4 step keys off: if
// grants resolve EMPTY the child's authorize fails closed on every
// resource and every tool-using agent silently stops working. The test
// is therefore written to FAIL if the grants path is broken (the granted
// tool's authorize would deny, the run would not complete, and the frozen
// grant snapshot would carry no tool grant).
//
// The deploy is a one-step workflow deployed BY SOURCE-REF whose agent
// carries the inline `mail_send` tool from the `mail-tool.ts` fixture. The
// deploy mail address is the run's own top-level `run_<id>@<domain>`
// address. On the source path, tool authorization rides the frozen grant
// snapshot: the probe's capability walk reads the source agent's inline tool
// and emits a `tool:<name>` grant that the operator approves and the approve
// step freezes onto the definition version row; the run's per-run grants
// (delivered on the trigger frame) authorize the tool call at run time.
//
// Assertions:
//   (a) identity: the deploy-ack persisted the public key for the run
//       mail address, and `isRunAddress` recognizes it -- every routable
//       address now names one self-anchored run.
//   (b) grants resolve: the frozen grant snapshot (`loadFrozenGrantSnapshot`,
//       the source-path grant store) carries the granted tool's
//       `tool:<name>` grant. An empty snapshot here is the silent
//       zero-grants failure this sub-step exists to prevent.
//   (c) authorize round-trip: `evaluateGrants` (the exact evaluator the
//       child's authorize adapter uses) ALLOWS the granted resource and
//       FAILS CLOSED on an ungranted one, evaluated over the per-run grant
//       the trigger delivers. The behavioral half drives a mail message
//       whose model turn calls the granted tool: the tool's authorize
//       succeeds in the child, the tool runs, and the run reaches
//       `RunCompleted`.
//   (d) events: an `inference.start` reaches the hub's `agent.event` sink
//       carrying the deploy's sessionId.

import fs from "node:fs";
import path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { type } from "arktype";

import { evaluateGrants } from "@intx/authz";
import { isRunAddress } from "@intx/types";
import type { GrantRule } from "@intx/types/authz";
import { WireGrantRule } from "@intx/types/grant-wire";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
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
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { MAIL_TOOL_NAME } from "./fixtures/mail-tool";
import { singleStepMailToolEntry } from "./fixtures/single-step-mail-tool";

const DEPLOYMENT_DOMAIN = "integration.interchange";
// A single-agent run id: `run_` + a hex-shaped local part. The
// deploy address `run_<id>@<domain>` is the run's own top-level
// address, not a per-step derived address, so identity preservation
// is exercised.
const INSTANCE_LOCAL = "run_deadbeefcafe0001deadbeefcafe0002";
const DEPLOYMENT_ID = INSTANCE_LOCAL;
const STEP_ID = "step1";
const AGENT_ID = "agent-launched-grants";

// The granted resource is `tool:<MAIL_TOOL_NAME>` with action `invoke`; the
// agent's authorize gate fires that exact query when the model calls the tool.
const GRANTED_RESOURCE = `tool:${MAIL_TOOL_NAME}`;
const UNGRANTED_RESOURCE = "tool:@intx/some-other/bundle:forbidden_tool";

const SENTINEL_FILENAME = "grants-bridge-ran.txt";
const SENTINEL_CONTENT = "authorized-in-child";

// The run's tool grant, delivered per run via the `run.grants` frame the
// trigger sends. `fireMailTrigger` does not materialize run grants itself the
// way the production route does, so feeding the tool grant here reproduces the
// per-run delivery, and the child's authorize resolves the tool call against
// it. `expiresAt: null` keeps the rule non-expiring so the evaluator never
// compares a wire-serialized date.
const GRANTED_RULE: WireGrantRule = {
  id: "grant-tool-invoke",
  resource: GRANTED_RESOURCE,
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
const TENANT_ID = "tnt_single_step_grants_bridge";
const CALLER_PRINCIPAL_ID = "prn_single_step_grants_bridge";
const DEFINITION_ASSET_ID = "ast_single_step_grants_bridge_wf";

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
    name: "single-step-grants-bridge-wf",
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

describe.skipIf(!harnessDbEnvAvailable())(
  "single-step launched-agent grants via spawned child",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("grants resolve from the frozen snapshot, identity is preserved, and events carry the sessionId", async () => {
      const deploymentMailAddress = deriveRunAddress({
        runId: DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });

      // (a) precondition: the deployment address is the launched-agent
      // identity shape, not a workflow-derived address.
      expect(isRunAddress(deploymentMailAddress)).toBe(true);

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
        `tool:${MAIL_TOOL_NAME}`,
      ]);

      const entryModule = singleStepMailToolEntry({
        variant: "fs",
        stepId: STEP_ID,
        systemPrompt: "You are the single-step launched agent.",
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

      // (a) identity: the deploy-ack fired for the run's `run_<hex>`
      // address and persisted a public key. The ack listener keys on the
      // top-level run address (per-step derived addresses are a no-op), so a
      // captured ack for this address proves the identity survived the
      // child re-route.
      await waitFor(() => env.hub.deployAcks.has(deploymentMailAddress), {
        timeoutMs: 20_000,
        diagnostics: env.sidecarDiagnostics,
      });
      const ackKey = env.hub.deployAcks.get(deploymentMailAddress);
      expect(ackKey).toBeDefined();
      expect(typeof ackKey).toBe("string");
      expect((ackKey ?? "").length).toBeGreaterThan(0);

      const workflowRunRepoId = handle.workflowRunRepoId;

      // (b) grants resolve: the source path freezes the probed grant surface
      // onto the definition version row. Read the frozen snapshot back --
      // the source-path grant store -- and assert it carries the granted
      // tool's `tool:<name>` grant. An empty snapshot here is the silent
      // zero-grants failure this sub-step exists to prevent.
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
      expect(snapshotToolGrants).toContain(GRANTED_RESOURCE);

      // (c) authorize round-trip against the per-run grant the trigger
      // delivers -- the same evaluator the child's authorize adapter uses.
      // Validate the wire grant through the same `WireGrantRule` validator the
      // wire boundary uses; the validator coerces `expiresAt` back to a `Date`,
      // yielding a `GrantRule`-shaped entry the evaluator accepts. Granted
      // allows; ungranted fails closed (no allow effect).
      const validatedGrants = WireGrantRule.array()([GRANTED_RULE]);
      if (validatedGrants instanceof type.errors) {
        throw new Error(
          `run grant failed WireGrantRule validation: ${validatedGrants.summary}`,
        );
      }
      const runGrants: GrantRule[] = validatedGrants;

      const allowed = await evaluateGrants(
        [...runGrants],
        GRANTED_RESOURCE,
        "invoke",
      );
      expect(allowed.effect).toBe("allow");

      const denied = await evaluateGrants(
        [...runGrants],
        UNGRANTED_RESOURCE,
        "invoke",
      );
      // Fail closed: no grant matches, so the resolved effect is null (the
      // authorize layer treats a null effect as deny).
      expect(denied.effect).not.toBe("allow");

      // The source-ref frame round-trips through the real sidecar subprocess,
      // so routability is asynchronous. Wait for it before firing the trigger.
      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      // (c) behavioral: drive a mail message. The model turn calls the
      // granted tool; the tool's authorize succeeds in the child, the tool
      // runs (writes the sentinel), and the run reaches RunCompleted. If
      // the per-run grant had not landed the tool authorize would deny and
      // the run would not complete.
      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<single-step-grants-bridge-1@integration.interchange>",
        grants: [GRANTED_RULE],
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

      // The granted tool actually executed in the child (proof the
      // authorize allowed it): the tool wrote a sentinel into the warm
      // single-step agent's STABLE per-agent workspace, rooted at
      // `workflow-step-state/<repoId>/warm/<stepId>/workspace` (keyed by the
      // step identity, not the per-message runId).
      const sentinelPath = path.join(
        env.sidecar.dataDir,
        "workflow-step-state",
        workflowRunRepoId.id,
        "warm",
        encodeURIComponent(STEP_ID),
        "workspace",
        SENTINEL_FILENAME,
      );
      if (!fs.existsSync(sentinelPath)) {
        throw new Error(
          `granted tool sentinel ${sentinelPath} was not written; the tool authorize did not allow in the child\n${env.sidecarDiagnostics()}`,
        );
      }
      expect(fs.readFileSync(sentinelPath, "utf-8")).toBe(SENTINEL_CONTENT);

      // (d) events: an inference.start reached the hub's agent.event sink
      // carrying the deploy's sessionId. The sink is keyed by
      // (agentAddress, sessionId); the production wiring threads
      // config.sessionId through publishWorkflowInferenceEvent.
      await waitFor(
        () =>
          env.hub.agentEvents.some(
            (e) =>
              e.addr === deploymentMailAddress &&
              e.sid === SESSION_ID &&
              isInferenceStart(e.event),
          ),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );
      const inferenceStart = env.hub.agentEvents.find(
        (e) =>
          e.addr === deploymentMailAddress &&
          e.sid === SESSION_ID &&
          isInferenceStart(e.event),
      );
      expect(inferenceStart).toBeDefined();
      expect(inferenceStart?.sid).toBe(SESSION_ID);
    });
  },
);

function isInferenceStart(event: unknown): boolean {
  return (
    typeof event === "object" &&
    event !== null &&
    "type" in event &&
    (event as { type: unknown }).type === "inference.start"
  );
}

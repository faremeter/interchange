// A deployed workflow whose `action` carries `onFailure` routes end to end: the
// child host resolves the action handler from the closure's interchange.actions
// module, the handler throws, and the runtime routes the permanent failure to
// the named handler action instead of failing the run. RunCompleted (not
// RunFailed) plus the unit's routed StepFailed prove onFailure survives the
// deploy path -- projection, wire, and the deployed runtime -- and routes.
//
// Harness justification: SPAWN-REAL. A real hub server, a real sidecar
// subprocess, and a real workflow-process child evaluating the deployed source.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { isRunAddress } from "@intx/types";
import type { HarnessConfig } from "@intx/types/runtime";
import { deriveRunAddress } from "@intx/workflow-deploy";
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
import { onFailureActionWorkflowEntry } from "./fixtures/onfailure-action-workflow";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f";
const TENANT_ID = "tnt_onfailure_action_roundtrip";
const CALLER_PRINCIPAL_ID = "prn_onfailure_action_roundtrip";
const DEFINITION_ASSET_ID = "ast_onfailure_action_roundtrip_wf";

let env: DeployFlowEnv;
let h: TestDb;
let deploymentMailAddress: string;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

// Resolve the value a step's StepCompleted carries. The on-disk event stores
// the output as a blob reference; small values use the "inline:" prefix with
// the JSON inlined. A real handler result and a prune sentinel are both
// StepCompleted events, so the value is the only thing that tells them apart.
function stepCompletedOutput(
  events: readonly { type: string; body: Record<string, unknown> }[],
  stepId: string,
): Record<string, unknown> {
  const completed = events.find(
    (e) => e.type === "StepCompleted" && e.body["stepId"] === stepId,
  );
  if (completed === undefined) {
    throw new Error(`no StepCompleted event for step ${stepId}`);
  }
  const output = completed.body["output"];
  if (!isRecord(output)) {
    throw new Error(
      `step ${stepId} output is not an object: ${JSON.stringify(output)}`,
    );
  }
  const ref = output["ref"];
  if (typeof ref !== "string" || !ref.startsWith("inline:")) {
    throw new Error(
      `step ${stepId} output is not an inline ref: ${JSON.stringify(output)}`,
    );
  }
  const parsed: unknown = JSON.parse(ref.slice("inline:".length));
  if (!isRecord(parsed)) {
    throw new Error(
      `step ${stepId} output did not resolve to an object: ${ref}`,
    );
  }
  return parsed;
}

beforeAll(async () => {
  if (!harnessDbEnvAvailable()) return;
  deploymentMailAddress = deriveRunAddress({
    runId: DEPLOYMENT_ID,
    domain: DEPLOYMENT_DOMAIN,
  });

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
    name: "onfailure-action-roundtrip-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

describe.skipIf(!harnessDbEnvAvailable())(
  "a deployed action failure routes to its onFailure handler",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("the failing action routes to its handler and the run completes", async () => {
      expect(isRunAddress(deploymentMailAddress)).toBe(true);

      const inferenceSource = {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${String(env.inference.server.port)}`,
        apiKey: "sk-mock",
        model: "mock-model",
      };
      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentId: DEPLOYMENT_ID,
        tenantId: "tenant-1",
        principalId: "prin_onfailure-action-roundtrip-1",
        agentAddress: deploymentMailAddress,
        systemPrompt: "unused (every step is an action)",
        tools: [],
        grants: [],
        sources: [inferenceSource],
        defaultSource: "anthropic:mock-model",
      };

      const handle = await deployWorkflowSourceForTest(env, {
        entryModule: onFailureActionWorkflowEntry({
          address: deploymentMailAddress,
        }),
        // The entry exports the workflow and every action handler.
        actions: "./workflow.mjs",
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: deploymentMailAddress,
        approvals: "approve-probed",
        config,
        // Every step (all actions) pins the mock source as its placeholder;
        // without an agent nothing else establishes an approved source.
        sources: {
          unit: [inferenceSource],
          rescue: [inferenceSource],
          normal: [inferenceSource],
        },
      });
      expect(handle.publicKey).toBeTruthy();

      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<onfailure-action-roundtrip-1@integration.interchange>",
        content: "go",
      });

      const runId = await waitForFirstRunId(env, handle.workflowRunRepoId, {
        timeoutMs: 20_000,
        diagnostics: env.sidecarDiagnostics,
      });

      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        runId,
        { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
      );
      // The failure routed, so the run completes rather than failing.
      expect(terminal.type).toBe("RunCompleted");

      const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
      // The unit failed and routed to its handler.
      const unitFailed = events.find(
        (e) => e.type === "StepFailed" && e.body["stepId"] === "unit",
      );
      expect(unitFailed).toBeDefined();
      expect(unitFailed?.body["routedTo"]).toBe("rescue");
      // The unit routed rather than completing, so it has no StepCompleted.
      expect(
        events.some(
          (e) => e.type === "StepCompleted" && e.body["stepId"] === "unit",
        ),
      ).toBe(false);

      // The routed handler ran for real: its output is the handler's return
      // value, not a prune sentinel. A pruned step also emits StepCompleted, so
      // asserting existence alone would pass even if rescue had been pruned.
      const rescueOutput = stepCompletedOutput(events, "rescue");
      expect(rescueOutput["skipped"]).toBeUndefined();
      expect(rescueOutput["ok"]).toBe(true);

      // The non-routed dependent was pruned: its StepCompleted carries the skip
      // sentinel keyed to the routed unit, not a real handler return.
      const normalOutput = stepCompletedOutput(events, "normal");
      expect(normalOutput["skipped"]).toBe(true);
      expect(normalOutput["onFailureStepId"]).toBe("unit");
    }, 120_000);
  },
);

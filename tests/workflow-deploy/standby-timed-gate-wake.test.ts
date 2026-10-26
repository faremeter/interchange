// A due timer wakes a deployment in standby, and the timed gate it belongs to
// times out.
//
// The run parks on a timed `awaitSignal` gate. The Hub's lifecycle service puts
// the deployment in standby, which stops the child, and the sidecar wakes the
// child when the gate's timer is due. The new child's scheduler fires the timer
// while the resumed run is still on its way back to the gate, so the gate has to
// find that `TimerFired` in the log; the run then goes on to the gate's
// `onTimeout` step.
//
// Harness justification: SPAWN-REAL: real hub, real sidecar subprocess, real
// workflow-process child, mock inference.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { tenant as tenantTable } from "@intx/db/schema";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import { deriveRunAddress } from "@intx/workflow-deploy";
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
  SESSION_ID,
  SIDECAR_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  listWorkflowHostChildren,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { startStandbySweep } from "./standby-helpers";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_standby-timed-gate-wake";
const TENANT_ID = "tnt_standby_timed_gate_wake";
const CALLER_PRINCIPAL_ID = "prn_standby_timed_gate_wake";
const DEFINITION_ASSET_ID = "ast_standby_timed_gate_wake_wf";
// Long enough for standby to land before the timer is due, also when the
// test's lane runs other files alongside it.
const GATE_MS = 20_000;

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
  await seedAsset(h.db, {
    id: DEFINITION_ASSET_ID,
    tenantId: TENANT_ID,
    kind: "workflow",
    name: "standby-timed-gate-wake-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });
  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

function timedGateEntry(address: string): string {
  return `
import { awaitSignal, defineWorkflow, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";

const agent = defineAgent({
  id: "settle-agent",
  systemPrompt: "settle agent",
  tools: [],
  capabilities: [],
  inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
});

export const workflow = defineWorkflow({
  id: "wf_standby_timed_gate_wake",
  trigger: { type: "mail", to: ${JSON.stringify(address)} },
  steps: {
    hold: awaitSignal({ name: "go", timeout: ${String(GATE_MS)}, onTimeout: "settle" }),
    settle: step({ agent, after: ["hold"] }),
  },
});
`;
}

// The allocation the fixture's router binds this deployment to, as the Hub
// records it when it places the deployment, so the lifecycle service can reach
// the deployment's sidecar.
describe.skipIf(!harnessDbEnvAvailable())(
  "a deployment in standby on a timed gate",
  () => {
    test("runs past the gate's timeout once its due timer wakes the child", async () => {
      const address = deriveRunAddress({
        runId: DEPLOYMENT_ID,
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
        agentAddress: address,
        sources: [inferenceSource],
        defaultSource: "anthropic:mock-model",
      };
      const handle = await deployWorkflowSourceForTest(env, {
        entryModule: timedGateEntry(address),
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: address,
        approvals: "approve-probed",
        config,
        sources: { hold: [inferenceSource], settle: [inferenceSource] },
      });
      await waitFor(
        () => env.hub.router.getRoutableAddresses().includes(address),
        { diagnostics: env.sidecarDiagnostics },
      );
      await fireMailTrigger(env, address, {
        messageId: "<standby-timed-gate-wake@integration.interchange>",
      });
      const runId = await waitForFirstRunId(env, handle.workflowRunRepoId, {
        diagnostics: env.sidecarDiagnostics,
      });
      await waitFor(
        async () =>
          (await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId)).some(
            (e) => e.type === "TimerSet",
          ),
        { diagnostics: env.sidecarDiagnostics },
      );

      const { lifecycle, untilInStandby } = await startStandbySweep(env, h.db, {
        tenantId: TENANT_ID,
        deploymentId: DEPLOYMENT_ID,
        target: env.hub.prepareAllocationIdentity(
          DEPLOYMENT_ID,
          address,
          SIDECAR_ID,
        ),
      });
      const wakeReasons: (string | undefined)[] = [];
      const unsubscribe = env.hub.router.events.on(
        "deployment.standby",
        (report) => {
          wakeReasons.push(report.reason);
        },
      );
      expect(await lifecycle.requestStandby(TENANT_ID, DEPLOYMENT_ID)).toBe(
        "pending",
      );
      await untilInStandby();
      expect(listWorkflowHostChildren(env)).toEqual([]);

      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        runId,
      );
      expect(terminal.type).toBe("RunCompleted");
      await waitFor(() => wakeReasons.includes("Woken by timer"), {
        diagnostics: env.sidecarDiagnostics,
      });
      unsubscribe();

      const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
      expect(events.filter((e) => e.type === "TimerSet")).toHaveLength(1);
      expect(events.filter((e) => e.type === "TimerFired")).toHaveLength(1);
      expect(
        events.some(
          (e) => e.type === "StepCompleted" && e.body["stepId"] === "settle",
        ),
      ).toBe(true);
    }, 120_000);
  },
);

// A workflow made only of deterministic primitives deploys and runs.
//
// Its capability walk emits no `inference.source:` grant, because the walk
// emits those only from agent definitions. Under `approve-probed` the approved
// set therefore contains nothing that could approve the deploy's default
// source, so pinning every step through the operator-approval gate refused the
// first step and the deploy failed closed with "no approved inference source".
// A step that cannot invoke inference now takes the default as an inert
// placeholder instead, and the hub delivers no credential for it.
//
// This is the only fixture in this suite with no agent anywhere. The others
// carry one whether or not they need it, which is what let the gate refuse
// agent-free definitions unnoticed.
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
import { allActionWorkflowEntry } from "./fixtures/all-action-workflow";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_a11ac7104a11ac7104a11ac7104a11a";
const TENANT_ID = "tnt_all_action_deploy";
const CALLER_PRINCIPAL_ID = "prn_all_action_deploy";
const DEFINITION_ASSET_ID = "ast_all_action_deploy_wf";

let env: DeployFlowEnv;
let h: TestDb;
let deploymentMailAddress: string;

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
    name: "all-action-deploy-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

describe.skipIf(!harnessDbEnvAvailable())(
  "a workflow with no agent step deploys and runs",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("deploys under approve-probed and runs its actions", async () => {
      expect(isRunAddress(deploymentMailAddress)).toBe(true);

      // The catalog still carries a source, as every deploy does. Nothing in
      // the definition declares it, so nothing approves it -- which is the
      // condition under test.
      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentId: DEPLOYMENT_ID,
        tenantId: "tenant-1",
        principalId: "prin_all-action-deploy-1",
        agentAddress: deploymentMailAddress,
        systemPrompt: "unused (no agent step)",
        tools: [],
        grants: [],
        sources: [
          {
            id: "anthropic:mock-model",
            provider: "anthropic",
            baseURL: `http://localhost:${String(env.inference.server.port)}`,
            credentialId: "sk-mock",
            model: "mock-model",
          },
        ],
        defaultSource: "anthropic:mock-model",
      };

      const handle = await deployWorkflowSourceForTest(env, {
        entryModule: allActionWorkflowEntry({
          address: deploymentMailAddress,
        }),
        actions: "./workflow.mjs",
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: deploymentMailAddress,
        approvals: "approve-probed",
        config,
        // Omit sources so the harness computes them through the real pin. That
        // pin is what refused this definition before.
      });
      expect(handle.publicKey).toBeTruthy();

      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<all-action-deploy-1@integration.interchange>",
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
      expect(terminal.type).toBe("RunCompleted");

      // Both actions ran: the deploy did not merely survive the gate, the
      // deterministic steps executed on the child.
      const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
      const completed = events
        .filter((e) => e.type === "StepCompleted")
        .map((e) => e.body["stepId"]);
      expect(completed).toContain("gather");
      expect(completed).toContain("persist");
    }, 120_000);
  },
);

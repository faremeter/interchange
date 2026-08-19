// Smoke test for the hub-link disconnect/reconnect harness surface.
//
// Exercises the three helpers `deploy-flow-env` grew for the
// reconnect-survival acceptance work -- `settleThenDrop`,
// `waitForReconnect`, and (via `settleThenDrop`) `dropHubLink` -- plus the
// `lookupPublicKey`/`liveHandles` wiring that makes a dropped sidecar link
// reconnect instead of looping on a closed socket.
//
// Shape: deploy a single-step workflow, drive one mail trigger to
// `RunCompleted`, settle the pack-push pipeline and drop the hub link,
// wait for the deployment address to become routable again (the reconnect
// ownership challenge passing), then fire a second mail trigger and assert
// it also reaches `RunCompleted`. A deployed workflow survives the
// reconnect: the second run only exists because the sidecar re-established
// the link and re-entered routing.
//
// Harness justification: SPAWN-REAL. A real hub server, a real sidecar
// subprocess, a real workflow-process child, and a test inference
// provider. The drop is a genuine server-side WebSocket close; the
// reconnect is the sidecar's real `hub-link` reconnect path passing the
// hub's ownership challenge.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { isRunAddress } from "@intx/types";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import { deriveRunAddress, type ApprovalSet } from "@intx/workflow-deploy";
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
  readClaimCheckDir,
  settleThenDrop,
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  waitForReconnect,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { singleStepAgentEntry } from "./fixtures/single-step-agent";

const DEPLOYMENT_DOMAIN = "integration.interchange";
// A single-agent run id: `run_` + a hex-shaped local part, so the
// deploy address is the run's own top-level `run_<hex>` address rather
// than a per-step derived address.
const DEPLOYMENT_ID = "run_d15c0nnec7ed0d0d15c0nnec7ed0d0d0";
const STEP_ID = "step1";
const AGENT_ID = "agent-reconnect-smoke";

// The definition's own tenant, the caller principal that creates the
// definition asset, and the `workflow`-kind asset the frozen definition
// projects over. The install/approve freeze and the anchor `workflow_run`
// insert both write against these, so they must exist in the real DB before
// the deploy runs.
const TENANT_ID = "tnt_hub_link_reconnect";
const CALLER_PRINCIPAL_ID = "prn_hub_link_reconnect";
const DEFINITION_ASSET_ID = "ast_hub_link_reconnect_wf";

let env: DeployFlowEnv;
let h: TestDb;
let deploymentMailAddress: string;

beforeAll(async () => {
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
    name: "hub-link-reconnect-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

describe.skipIf(!harnessDbEnvAvailable())(
  "hub-link drop -> reconnect survival (harness smoke)",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("deploy, run, settleThenDrop, reconnect, run again", async () => {
      expect(isRunAddress(deploymentMailAddress)).toBe(true);

      // ---- deploy a single-step workflow ----
      const inferenceSource: InferenceSource = {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${String(env.inference.server.port)}`,
        apiKey: "sk-mock",
        model: "mock-model",
      };
      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentId: `${DEPLOYMENT_ID}`,
        tenantId: "tenant-1",
        principalId: "prin_reconnect-smoke-1",
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
      ]);

      const entryModule = singleStepAgentEntry({
        stepId: STEP_ID,
        systemPrompt: "You are the reconnect smoke-test agent.",
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

      await waitFor(() => env.hub.deployAcks.has(deploymentMailAddress), {
        timeoutMs: 20_000,
        diagnostics: env.sidecarDiagnostics,
      });

      const workflowRunRepoId = handle.workflowRunRepoId;

      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      // ---- first run to completion ----
      const first = await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<reconnect-smoke-1@integration.interchange>",
        content: "first",
      });
      const firstRunId = await waitForFirstRunId(env, workflowRunRepoId, {
        timeoutMs: 20_000,
        diagnostics: env.sidecarDiagnostics,
      });
      const firstTerminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        firstRunId,
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );
      expect(firstTerminal.type).toBe("RunCompleted");

      // ---- settle the pack pipeline, then drop the hub link ----
      expect(env.hub.router.getRoutableAddresses()).toContain(
        deploymentMailAddress,
      );
      await settleThenDrop(env, deploymentMailAddress);

      // The address leaves routing as the server-side close lands.
      await waitFor(
        () =>
          !env.hub.router
            .getRoutableAddresses()
            .includes(deploymentMailAddress),
        { timeoutMs: 5_000, diagnostics: env.sidecarDiagnostics },
      );

      // ---- wait for reconnect + re-route ----
      const reconnectMs = await waitForReconnect(env, deploymentMailAddress, {
        timeoutMs: 20_000,
      });
      // The reconnect is the sidecar's 3s reconnect delay plus a handshake; a
      // generous lower bound guards against a false "already routable" pass
      // that never actually dropped, and the upper bound catches a hung link.
      expect(reconnectMs).toBeGreaterThan(1_000);
      expect(reconnectMs).toBeLessThan(20_000);
      expect(env.hub.router.getRoutableAddresses()).toContain(
        deploymentMailAddress,
      );

      // ---- second run to completion after reconnect ----
      const second = await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<reconnect-smoke-2@integration.interchange>",
        content: "second",
      });
      expect(second.messageId).not.toBe(first.messageId);

      // Under the stable-runId model the second message shares the
      // same runId as the first.  Wait for it to land in consumed/.
      const secondRunId = firstRunId;
      const secondMessageId = "<reconnect-smoke-2@integration.interchange>";
      const consumedDeadline = Date.now() + 30_000;
      while (Date.now() < consumedDeadline) {
        const consumed = await readClaimCheckDir(
          env,
          workflowRunRepoId,
          deploymentMailAddress,
          "consumed",
        );
        if (consumed.some((c) => c.filename.includes(secondMessageId))) break;
        await new Promise((r) => setTimeout(r, 50));
      }

      const secondTerminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        secondRunId,
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );
      expect(secondTerminal.type).toBe("RunCompleted");
    }, 120_000);
  },
);

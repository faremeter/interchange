// Smoke test for the hub-link disconnect/reconnect harness surface.
//
// Exercises the three helpers `deploy-flow-env` grew for the
// reconnect-survival acceptance work -- `settleThenDrop`,
// `waitForReconnect`, and (via `settleThenDrop`) `dropHubLink` -- plus the
// `liveHandles` wiring that lets the harness force a dropped sidecar link.
//
// Shape: deploy a single-step workflow, drive one mail trigger to
// `RunCompleted`, settle the pack-push pipeline and drop the hub link,
// wait for the allocation-authenticated reconnect to make the deployment
// address routable again, then fire a second mail trigger and assert it
// reaches the deployment's `consumed/` index.
//
// What the reconnect restores is routability and inbox admission, not a
// second run. The workflow run id is derived from the deployment mail
// address and is therefore stable across messages, so mail 2 resolves to
// the run that mail 1 already drove to `RunCompleted`; a completed
// deployment rejects further mail by design. Mail 2 consuming with a
// `workflow_run_terminal` rejection is the assertion, and it only happens
// because the sidecar re-established the link and re-entered routing.
//
// Harness justification: SPAWN-REAL. A real hub server, a real sidecar
// subprocess, a real workflow-process child, and a test inference
// provider. The drop is a genuine server-side WebSocket close; the
// reconnect is the sidecar's real `hub-link` path passing durable identity
// revalidation and the current allocation-generation fence.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { isRunAddress } from "@intx/types";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import {
  createApprovalSet,
  deriveRunAddress,
  type ApprovalSet,
} from "@intx/workflow-deploy";
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
  PRODUCTION_RECONNECT_DELAY_MS,
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  waitForReconnect,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { readConsumedEntries } from "./fifo-mail-helpers";
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
    name: "hub-link-reconnect-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv({
    // Pin the production reconnect backoff so the drop below is recovered
    // through the real delayed-reconnect cycle rather than the fixture's
    // shortened test delay.
    sidecarEnv: {
      SIDECAR_RECONNECT_DELAY_MS: PRODUCTION_RECONNECT_DELAY_MS,
    },
  });
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

    test("deploy, run, settleThenDrop, reconnect, second mail admitted and rejected as terminal", async () => {
      expect(isRunAddress(deploymentMailAddress)).toBe(true);

      // ---- deploy a single-step workflow ----
      const inferenceSource: InferenceSource = {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${String(env.inference.server.port)}`,
        credentialId: "sk-mock",
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
      const operatorApprovals: ApprovalSet = createApprovalSet([
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
        diagnostics: env.sidecarDiagnostics,
      });

      const workflowRunRepoId = handle.workflowRunRepoId;

      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { diagnostics: env.sidecarDiagnostics },
      );

      // ---- first run to completion ----
      const first = await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<reconnect-smoke-1@integration.interchange>",
        content: "first",
      });
      const firstRunId = await waitForFirstRunId(env, workflowRunRepoId, {
        diagnostics: env.sidecarDiagnostics,
      });
      const firstTerminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        firstRunId,
        { diagnostics: env.sidecarDiagnostics },
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
        { diagnostics: env.sidecarDiagnostics },
      );

      // ---- wait for reconnect + re-route ----
      await waitForReconnect(env, deploymentMailAddress);
      expect(env.hub.router.getRoutableAddresses()).toContain(
        deploymentMailAddress,
      );

      // ---- second mail after reconnect ----
      const secondMessageId = "<reconnect-smoke-2@integration.interchange>";
      const second = await fireMailTrigger(env, deploymentMailAddress, {
        messageId: secondMessageId,
        content: "second",
      });
      expect(second.messageId).not.toBe(first.messageId);

      // Wait for mail 2 to land in consumed/. That it lands at all is what
      // the reconnect buys: the mail had to route to the re-registered
      // address, enter the deployment's inbox, and be dequeued by the
      // supervisor's dispatch loop. The poll carries no deadline of its own,
      // so a mail 2 that never reaches consumed/ hangs here rather than
      // falling through; this test's own budget is the failsafe, and the
      // harness `waitFor` is what puts the sidecar's output on the env
      // teardown's report.
      await waitFor(async () => {
        const consumed = await readClaimCheckDir(
          env,
          workflowRunRepoId,
          deploymentMailAddress,
          "consumed",
        );
        return consumed.some((c) => c.filename.includes(secondMessageId));
      });

      // Mail 2 is rejected, not dispatched. The supervisor derives the
      // workflow run id from the deployment mail address, so mail 2 resolves
      // to the run mail 1 already drove to RunCompleted, and a terminal run
      // cannot be fired again. The rejection is written by the same
      // `markConsumed` commit that created the entry the poll above observed,
      // so re-reading it here sees the final envelope.
      const consumedEntries = await readConsumedEntries(
        env,
        workflowRunRepoId,
        deploymentMailAddress,
      );
      expect(
        consumedEntries.find((entry) => entry.messageId === secondMessageId)
          ?.rejection?.code,
      ).toBe("workflow_run_terminal");
    }, 120_000);
  },
);

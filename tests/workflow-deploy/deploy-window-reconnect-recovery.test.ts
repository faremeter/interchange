// An allocation-authenticated reconnect does not depend on the deployment
// public-key projection. The provisioner token already binds the worker to the
// anchor and generation, so a reconnect remains routable even if that derived
// projection is temporarily absent.
//
// Harness justification: SPAWN-REAL. A real hub server, a real sidecar
// subprocess, a real workflow-process child, and a test inference provider.
// The drops are genuine server-side WebSocket closes; the recovery is the
// sidecar's real `hub-link` allocation-authenticated reconnect path.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { promises as fs } from "node:fs";
import path from "node:path";

import { isRunAddress } from "@intx/types";
import { createSidecarAllocationStore } from "@intx/db";
import {
  createSidecarAllocationReconciler,
  createSidecarPluginRegistry,
  restoreWorkflowRunToAllocation,
} from "@intx/hub-sessions";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import {
  createApprovalSet,
  deriveRunAddress,
  type ApprovalSet,
} from "@intx/workflow-deploy";
import {
  sidecar as sidecarTable,
  tenant as tenantTable,
} from "@intx/db/schema";
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
  dropHubLink,
  fireMailTrigger,
  listRunIds,
  PRODUCTION_RECONNECT_DELAY_MS,
  startDeployFlowEnv,
  waitFor,
  waitForReconnect,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { singleStepAgentEntry } from "./fixtures/single-step-agent";

const DEPLOYMENT_DOMAIN = "integration.interchange";
// A `run_` + hex-shaped local part, so the deploy address is the run's
// own top-level `run_<hex>@<domain>` address rather than a per-step
// derived address, matching the reconnect-survival fixture.
const DEPLOYMENT_ID = "run_dep10ec0ffee0ec0ffee0ec0ffee0ec0";
const STEP_ID = "step1";
const AGENT_ID = "agent-deploy-window-recovery";

// The definition's own tenant, the caller principal that creates the
// definition asset, and the `workflow`-kind asset the frozen definition
// projects over. The install/approve freeze and the anchor `workflow_run`
// insert both write against these, so they must exist in the real DB before
// the deploy runs.
const TENANT_ID = "tnt_deploy_window_recovery";
const CALLER_PRINCIPAL_ID = "prn_deploy_window_recovery";
const DEFINITION_ASSET_ID = "ast_deploy_window_recovery_wf";

let env: DeployFlowEnv;
let h: TestDb;
let deploymentMailAddress: string;
let deploymentConfig: HarnessConfig;

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
    name: "deploy-window-recovery-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv({
    // This test pins the production reconnect delay: the `reconnectMs > 1_000`
    // assertion proves the sidecar really cycled through its delayed reconnect
    // rather than instantly re-connecting.
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
  "deploy-window reconnect recovers cleanly",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("a reconnect remains authorized while the public-key projection is absent", async () => {
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
        principalId: "prin_deploy-window-recovery-1",
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
      deploymentConfig = config;

      const entryModule = singleStepAgentEntry({
        stepId: STEP_ID,
        systemPrompt: "You are the deploy-window recovery test agent.",
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

      // Wait for the deployment to ack its key so the sidecar is fully live.
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

      // Remove the derived public-key projection before dropping the link. It
      // is not reconnect authority and must not prevent route restoration.
      const ackedKey = env.hub.deployAcks.get(deploymentMailAddress);
      if (ackedKey === undefined) {
        throw new Error(
          `expected an acked key for ${deploymentMailAddress} after deploy`,
        );
      }
      env.hub.deployAcks.delete(deploymentMailAddress);
      dropHubLink(env);
      await waitFor(
        () =>
          !env.hub.router
            .getRoutableAddresses()
            .includes(deploymentMailAddress),
        { timeoutMs: 5_000, diagnostics: env.sidecarDiagnostics },
      );

      const reconnectMs = await waitForReconnect(env, deploymentMailAddress, {
        timeoutMs: 30_000,
      });
      env.hub.deployAcks.set(deploymentMailAddress, ackedKey);
      // A generous lower bound guards against a false "already routable" pass
      // that never actually dropped; the upper bound catches a hung link.
      expect(reconnectMs).toBeGreaterThan(1_000);
      expect(reconnectMs).toBeLessThan(30_000);
      expect(env.hub.router.getRoutableAddresses()).toContain(
        deploymentMailAddress,
      );

      // Let the recovered link settle before firing mail. The wedge window
      // left a backlog of failed workflow-run pack pushes that retry on the
      // fresh link; firing mail on top of that backlog can race a residual
      // "Connection lost" on the supervisor's inbox enqueue. Require the
      // address to stay continuously routable across a quiet window so the
      // trigger lands on a stable link.
      const settleStart = Date.now();
      let stableSince = Date.now();
      while (Date.now() - stableSince < 2_000) {
        if (
          !env.hub.router.getRoutableAddresses().includes(deploymentMailAddress)
        ) {
          stableSince = Date.now();
        }
        if (Date.now() - settleStart > 30_000) {
          throw new Error(
            `recovered link never held routable for 2s within 30s\n${env.sidecarDiagnostics()}`,
          );
        }
        await new Promise((r) => setTimeout(r, 100));
      }

      // ---- and a mail trigger runs to completion on the recovered link ----
      // Fire with retry: a trigger that lands while a residual reconnect is
      // in flight can be dropped before the supervisor enqueues it, producing
      // no run. Retry with a fresh message id (each keyed on the attempt so
      // the dedup index never collides) until a run appears, capping attempts
      // so a genuine wedge still fails loudly rather than looping forever.
      const runId = await (async () => {
        const start = Date.now();
        let attempt = 0;
        for (;;) {
          attempt += 1;
          await fireMailTrigger(env, deploymentMailAddress, {
            messageId: `<deploy-window-recovery-${String(attempt)}@integration.interchange>`,
            content: "recovered",
          });
          const deadline = Date.now() + 10_000;
          while (Date.now() < deadline) {
            const ids = await listRunIds(env, workflowRunRepoId);
            const first = ids[0];
            if (first !== undefined) return first;
            await new Promise((r) => setTimeout(r, 100));
          }
          if (Date.now() - start > 60_000) {
            throw new Error(
              `no run produced on the recovered link after ${String(attempt)} mail triggers\n${env.sidecarDiagnostics()}`,
            );
          }
        }
      })();
      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        runId,
        { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
      );
      expect(terminal.type).toBe("RunCompleted");
    }, 240_000);

    test("an outstanding deploy preserves the live worker's files before fenced cleanup", async () => {
      const router = env.hub.router;
      const target = env.hub.prepareAllocationIdentity(
        DEPLOYMENT_ID,
        deploymentMailAddress,
      );
      const store = createSidecarAllocationStore(h.db);
      await h.db.insert(sidecarTable).values({
        id: SIDECAR_ID,
        tokenHashSha256: new Uint8Array(32).fill(7),
      });
      await store.createAdopted({
        id: target.allocationId,
        anchorRunId: DEPLOYMENT_ID,
        tenantId: TENANT_ID,
        provisionerId: "test",
        provisionerApiVersion: 1,
        provisionerBindingFingerprint: "test:v1",
        generation: target.generation,
        sidecarId: SIDECAR_ID,
        connectDeadline: new Date(Date.now() + 60_000),
      });
      const leaseId = "interrupted-initialization";
      await store.claimNextReconcilable({ leaseId, leaseDurationMs: 60_000 });
      expect(
        await store.beginInitialization({
          ...target,
          anchorRunId: DEPLOYMENT_ID,
          tenantId: TENANT_ID,
          leaseId,
          signal: new AbortController().signal,
        }),
      ).not.toBeNull();

      const deployment = env.deployments.get(DEPLOYMENT_ID);
      if (deployment === undefined)
        throw new Error("Missing deployed workflow");
      const runDir = path.join(
        env.sidecar.dataDir,
        "workflow-runs",
        deployment.workflowRunRepoId.id,
      );
      const recordPath = path.join(runDir, "deployment.json");
      const record = await fs.readFile(recordPath, "utf8");
      const sentinelPath = path.join(runDir, "restore-must-not-delete.txt");
      await fs.writeFile(sentinelPath, "live workflow state");

      // A duplicate-deploy rejection clears the router's marker just as a late
      // deploy timeout does, while this real supervisor remains alive. The unit
      // regression exercises the cancellation/timeout ordering itself.
      await expect(
        router.sendAgentDeployToAllocation(
          target,
          deploymentMailAddress,
          deploymentConfig,
          {
            sources: {},
            approvedWireHash: "a".repeat(64),
            sourceRef: {
              source: { kind: "registry", registry: "npm" },
              closure: { schemaVersion: "1", topLevel: [], entries: [] },
            },
          },
        ),
      ).rejects.toThrow("already deployed");
      expect(await router.isAllocatedWorkflowActive(target)).toBe(false);
      expect(await router.isAllocatedSidecarReady(target)).toBe(true);
      await store.parkReconciliation(target.allocationId, leaseId, {
        kind: "retry-after-error",
        notBefore: new Date(0),
      });

      let restores = 0;
      const reconciler = createSidecarAllocationReconciler({
        allocationStore: store,
        router,
        hubWebSocketUrl: "ws://localhost/unused",
        plugins: createSidecarPluginRegistry({
          provisioners: [
            {
              id: "test",
              apiVersion: 1,
              bindingFingerprint: "test:v1",
              capabilities: [],
              async ensure() {
                throw new Error("must not ensure");
              },
              async destroy() {
                throw new Error("cleanup has not been claimed yet");
              },
            },
          ],
        }),
        onReady: async (_allocation, { signal }) => {
          restores += 1;
          await restoreWorkflowRunToAllocation({
            agentRepoStore: env.hub.agentRepoStore,
            allocationRouter: router,
            allocationTarget: target,
            agentAddress: deploymentMailAddress,
            signal,
          });
        },
      });
      expect(await reconciler.reconcileNext()).toBe(true);
      expect(restores).toBe(0);
      expect((await store.findById(target.allocationId))?.status).toBe(
        "releasing",
      );
      expect(await fs.readFile(recordPath, "utf8")).toBe(record);
      expect(await fs.readFile(sentinelPath, "utf8")).toBe(
        "live workflow state",
      );
    });
  },
);

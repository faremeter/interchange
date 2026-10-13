// Deployed childWorkflow crash-survival integration test.
//
// A childWorkflow stays unaddressable. Its author gate is relayed onto the
// deployment run, which is the one addressable run. After a sidecar process
// crash, boot-time restore re-spawns that deployment and reattaches the same
// child run from its log. The injected signal resolves the parent's relay.
//
// Harness justification: SPAWN-REAL. Real hub, real sidecar subprocess, real
// workflow-process child, mock inference. The crash is a genuine kill of the
// sidecar subprocess; the restart is a fresh sidecar against the dead process's
// SIDECAR_DATA_DIR, so survival rides the production boot-time restore path.

import fs from "node:fs";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

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
import {
  seedAsset,
  seedPrincipal,
  tenantSlugFromId,
} from "@intx/test-harness/seed";
import type { RepoId } from "@intx/hub-sessions";

import {
  SESSION_ID,
  SIDECAR_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  injectSignal,
  listRunIds,
  readWorkflowRunEvents,
  settleWorkflowRunPacks,
  startDeployFlowEnv,
  startSidecarSubprocess,
  terminateSidecarSubprocess,
  waitFor,
  waitForReconnect,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
  type SidecarHandle,
} from "../hub-agent/lib/deploy-flow-env";
import { childWorkflowUnanswerableGateEntry } from "./fixtures/childworkflow-unanswerable-gate";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_childworkflow-restart-1";
const SIGNAL_NAME = "go";

const TENANT_ID = "tnt_childworkflow_restart";
const CALLER_PRINCIPAL_ID = "prn_childworkflow_restart";
const DEFINITION_ASSET_ID = "ast_childworkflow_restart_wf";

let env: DeployFlowEnv;
let h: TestDb;
let restartedSidecar: SidecarHandle | undefined;
const restartTempDirs: string[] = [];

beforeAll(async () => {
  // A file-scope beforeAll fires even when describe.skipIf skips the
  // suite bodies, so it needs its own guard or a missing DB env throws
  // here. See the two-shape rule in tests/lib/db-harness.ts.
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
    name: "childworkflow-restart-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (restartedSidecar !== undefined) {
    await terminateSidecarSubprocess(restartedSidecar);
  }
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
  for (const dir of restartTempDirs.splice(0)) {
    await fs.promises.rm(dir, { recursive: true, force: true });
  }
});

async function readAllRunEvents(
  target: DeployFlowEnv,
  workflowRunRepoId: RepoId,
): Promise<{ runId: string; type: string; body: Record<string, unknown> }[]> {
  const runIds = await listRunIds(target, workflowRunRepoId);
  const out: { runId: string; type: string; body: Record<string, unknown> }[] =
    [];
  for (const runId of runIds) {
    const events = await readWorkflowRunEvents(target, DEPLOYMENT_ID, runId);
    for (const event of events) {
      out.push({ runId, type: event.type, body: event.body });
    }
  }
  return out;
}

describe.skipIf(!harnessDbEnvAvailable())(
  "sidecar crash + restart resumes a childWorkflow parked on an author gate",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("a child gate parks on the deployment run, crashes, and the restored relay completes", async () => {
      const deploymentMailAddress = deriveRunAddress({
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
        agentId: DEPLOYMENT_ID,
        tenantId: "tenant-1",
        principalId: "prin_childworkflow-restart-1",
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

      const handle = await deployWorkflowSourceForTest(env, {
        entryModule: childWorkflowUnanswerableGateEntry({
          address: deploymentMailAddress,
          workflowId: `wf_${DEPLOYMENT_ID}`,
          childWorkflowId: `wf_child_${DEPLOYMENT_ID}`,
          signalName: SIGNAL_NAME,
        }),
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: DEPLOYMENT_ID,
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

      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<childworkflow-restart-1@integration.interchange>",
      });

      // The child run id is a random `run-...` and carries no `__` marker, so
      // the parent is the run whose relay await sits on the spawn step. The
      // child awaits its own step, `hold`.
      const parentRunId = async (): Promise<string | undefined> => {
        const ids = await listRunIds(env, workflowRunRepoId);
        for (const id of ids) {
          const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, id);
          if (
            events.some(
              (event) =>
                event.type === "SignalAwaited" &&
                event.body["stepId"] === "spawn",
            )
          ) {
            return id;
          }
        }
        return undefined;
      };
      await waitFor(async () => (await parentRunId()) !== undefined, {
        diagnostics: env.sidecarDiagnostics,
      });
      const runId = await parentRunId();
      if (runId === undefined) {
        throw new Error("no parent run parked on the spawn step");
      }

      const parked = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
      expect(parked.map((event) => event.type)).toContain("RunStarted");
      expect(parked.map((event) => event.type)).toContain("ChildSpawned");
      expect(
        parked.some(
          (event) =>
            event.type === "SignalAwaited" &&
            event.body["signalName"] === SIGNAL_NAME,
        ),
      ).toBe(true);
      expect(parked.map((event) => event.type)).not.toContain("RunCompleted");

      await settleWorkflowRunPacks(env);

      const crashedDataDir = env.sidecar.dataDir;
      await terminateSidecarSubprocess(env.sidecar);

      await waitFor(
        () =>
          !env.hub.router
            .getRoutableAddresses()
            .includes(deploymentMailAddress),
        { diagnostics: env.sidecarDiagnostics },
      );

      const hubPort = env.hub.server.port;
      if (hubPort === undefined) {
        throw new Error("hub.server.port is undefined after crash");
      }
      restartedSidecar = await startSidecarSubprocess({
        hubPort,
        registerTempDir: (dir) => {
          restartTempDirs.push(dir);
        },
        extraEnv: { SIDECAR_DATA_DIR: crashedDataDir },
      });
      env.registerSidecar(restartedSidecar);

      await waitForReconnect(env, deploymentMailAddress);
      expect(env.hub.router.getRoutableAddresses()).toContain(
        deploymentMailAddress,
      );

      const injected = await injectSignal(
        env,
        DEPLOYMENT_ID,
        runId,
        SIGNAL_NAME,
        {
          resumed: true,
        },
      );

      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        runId,
        { diagnostics: env.sidecarDiagnostics },
      );
      expect(terminal.type).toBe("RunCompleted");

      const finalEvents = await readAllRunEvents(env, workflowRunRepoId);
      const onParent = (type: string): number =>
        finalEvents.filter(
          (event) => event.runId === runId && event.type === type,
        ).length;
      expect(onParent("RunStarted")).toBe(1);
      expect(onParent("RunCompleted")).toBe(1);
      expect(onParent("ChildSpawned")).toBe(1);

      const signalReceived = finalEvents.find(
        (event) =>
          event.runId === runId &&
          event.type === "SignalReceived" &&
          event.body["signalName"] === SIGNAL_NAME,
      );
      if (signalReceived === undefined) {
        throw new Error("no SignalReceived on the parent after resume");
      }
      expect(signalReceived.body["signalId"]).toBe(injected.signalId);
      expect(signalReceived.body["payload"]).toEqual({ resumed: true });
    }, 180_000);
  },
);

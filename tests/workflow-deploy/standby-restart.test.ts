// A deployment in standby survives a sidecar restart without its child: a run
// parked on a signal is put in standby, the sidecar is killed and restarted on
// its data dir, and the restore reports the standby with no child running. The
// signal then starts the child, which completes the run with each effect once.
//
// Harness justification: SPAWN-REAL: real hub, real sidecar subprocess, real
// workflow-process child, mock inference.

import fs from "node:fs";
import path from "node:path";

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
import {
  WorkflowControlHistoryPendingError,
  type SidecarEventMap,
} from "@intx/hub-sessions";

import {
  SESSION_ID,
  SIDECAR_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  injectSignal,
  listRunIds,
  listWorkflowHostChildren,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  startSidecarSubprocess,
  terminateSidecarSubprocess,
  waitFor,
  waitForReconnect,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
  type SidecarHandle,
} from "../hub-agent/lib/deploy-flow-env";
import { signalGateEntry } from "./fixtures/signal-gate";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_standby-restart-1";
const TENANT_ID = "tnt_standby_restart";
const CALLER_PRINCIPAL_ID = "prn_standby_restart";
const DEFINITION_ASSET_ID = "ast_standby_restart_wf";
// Longer than the test's budget, so the worker's acknowledgement, not the
// clock, settles the control request.
const CONTROL_TIMEOUT_MS = 300_000;

let env: DeployFlowEnv;
let h: TestDb;
let restartedSidecar: SidecarHandle | undefined;
const restartTempDirs: string[] = [];

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
    name: "standby-restart-wf",
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

describe.skipIf(!harnessDbEnvAvailable())(
  "sidecar restart of a deployment in standby",
  () => {
    test("the restore keeps the deployment in standby without a child, and the awaited signal resumes the run", async () => {
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
        agentId: DEPLOYMENT_ID,
        tenantId: "tenant-1",
        principalId: "prin_standby-restart-1",
        agentAddress: address,
        systemPrompt: "Fallback prompt (overridden per step by the definition)",
        tools: [],
        grants: [],
        sources: [inferenceSource],
        defaultSource: "anthropic:mock-model",
      };
      const approvals: ApprovalSet = createApprovalSet([
        "inference.source:anthropic:mock-model",
        "director:@intx/agent/default",
        `mail.address:${address}`,
        `mail.send:${DEPLOYMENT_DOMAIN}`,
      ]);
      const handle = await deployWorkflowSourceForTest(env, {
        entryModule: signalGateEntry({
          address,
          signalName: "go",
          systemPrompt1: "You are the first step agent.",
          systemPrompt2: "You are the second step agent.",
          agentId1: "agent-step1",
          agentId2: "agent-step2",
          workflowId: `wf_${DEPLOYMENT_ID}`,
        }),
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: address,
        approvals,
        config,
        sources: {
          step1: [inferenceSource],
          gate: [inferenceSource],
          step2: [inferenceSource],
        },
      });
      await waitFor(
        () => env.hub.router.getRoutableAddresses().includes(address),
        { diagnostics: env.sidecarDiagnostics },
      );

      await fireMailTrigger(env, address, {
        messageId: "<standby-restart-1@integration.interchange>",
      });
      let runId: string | undefined;
      await waitFor(
        async () => {
          for (const id of await listRunIds(env, handle.workflowRunRepoId)) {
            const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, id);
            if (
              events.some(
                (e) =>
                  e.type === "SignalAwaited" && e.body["signalName"] === "go",
              )
            )
              runId = id;
          }
          return runId !== undefined;
        },
        { diagnostics: env.sidecarDiagnostics },
      );
      if (runId === undefined) throw new Error("no run reached the gate");

      // The standby attempt can overtake the child's last pushes; a repeated
      // one answers again once that history has landed.
      const target = env.hub.prepareAllocationIdentity(
        DEPLOYMENT_ID,
        address,
        SIDECAR_ID,
      );
      await waitFor(
        async () => {
          try {
            await env.hub.router.putAllocationInStandby(
              target,
              CONTROL_TIMEOUT_MS,
            );
            return true;
          } catch (error) {
            if (error instanceof WorkflowControlHistoryPendingError)
              return false;
            throw error;
          }
        },
        { diagnostics: env.sidecarDiagnostics },
      );
      expect(listWorkflowHostChildren(env)).toEqual([]);
      const recordFile = path.join(
        env.sidecar.dataDir,
        "workflow-runs",
        handle.workflowRunRepoId.id,
        "deployment.json",
      );
      const standbyMark = async (): Promise<unknown> => {
        const record: unknown = JSON.parse(
          await fs.promises.readFile(recordFile, "utf8"),
        );
        return typeof record === "object" &&
          record !== null &&
          "standby" in record
          ? record.standby
          : undefined;
      };
      expect(await standbyMark()).toEqual({
        parked: [],
        tip: expect.any(String),
      });

      // ---- kill the sidecar, and start a fresh one on its data dir ----
      const dataDir = env.sidecar.dataDir;
      await terminateSidecarSubprocess(env.sidecar);
      await waitFor(
        () => !env.hub.router.getRoutableAddresses().includes(address),
        { diagnostics: env.sidecarDiagnostics },
      );
      const reports: SidecarEventMap["deployment.standby"][] = [];
      // The restore reports first, so the second report is the wake's.
      const wakeReported = Promise.withResolvers<undefined>();
      const unsubscribe = env.hub.router.events.on(
        "deployment.standby",
        (report) => {
          reports.push(report);
          if (reports.length === 2) wakeReported.resolve(undefined);
        },
      );
      try {
        const hubPort = env.hub.server.port;
        if (hubPort === undefined)
          throw new Error("hub.server.port is undefined after the kill");
        restartedSidecar = await startSidecarSubprocess({
          hubPort,
          registerTempDir: (dir) => {
            restartTempDirs.push(dir);
          },
          extraEnv: { SIDECAR_DATA_DIR: dataDir },
        });
        env.registerSidecar(restartedSidecar);
        await waitForReconnect(env, address);

        const restarted = { ...env, sidecar: restartedSidecar };
        expect(listWorkflowHostChildren(restarted)).toEqual([]);
        expect(reports).toEqual([{ ...target, inStandby: true }]);
        expect(await standbyMark()).toBeDefined();

        // ---- the awaited signal starts the child, which resumes the run ----
        await injectSignal(env, DEPLOYMENT_ID, runId, "go", { resumed: true });
        const terminal = await waitForWorkflowRunComplete(
          env,
          DEPLOYMENT_ID,
          runId,
          { diagnostics: env.sidecarDiagnostics },
        );
        expect(terminal.type).toBe("RunCompleted");
        await wakeReported.promise;
        expect(reports[1]).toEqual({
          ...target,
          inStandby: false,
          reason: "Woken by signal",
        });
        expect(await standbyMark()).toBeUndefined();
      } finally {
        unsubscribe();
      }

      const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
      const count = (match: (e: (typeof events)[number]) => boolean) =>
        events.filter(match).length;
      expect(count((e) => e.type === "RunStarted")).toBe(1);
      expect(
        count(
          (e) => e.type === "StepCompleted" && e.body["stepId"] === "step1",
        ),
      ).toBe(1);
      expect(
        count(
          (e) => e.type === "StepCompleted" && e.body["stepId"] === "step2",
        ),
      ).toBe(1);
      expect(count((e) => e.type === "RunCompleted")).toBe(1);
    }, 180_000);
  },
);

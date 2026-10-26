// The Hub's record of a standby it asked for survives a sidecar restart: the
// lifecycle service puts a run parked on a signal in standby, the sidecar is
// killed and restarted on its data dir, and the restore's report leaves the
// Hub recording the standby with no child running. A wake through the
// lifecycle service then runs the child again, and the signal completes the
// run with each step once.
//
// Harness justification: SPAWN-REAL: real hub, real sidecar subprocess, real
// workflow-process child, mock inference.

import fs from "node:fs";
import path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import {
  sidecar as sidecarTable,
  sidecarAllocation,
  tenant as tenantTable,
} from "@intx/db/schema";
import {
  createWorkflowHistoryReceiveTracker,
  createWorkflowLifecycleService,
  createWorkflowRunReader,
  type SidecarEventMap,
} from "@intx/hub-sessions";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import {
  createApprovalSet,
  deriveRunAddress,
  type ApprovalSet,
} from "@intx/workflow-deploy";
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
const DEPLOYMENT_ID = "run_standby-restart-hub-record-1";
const TENANT_ID = "tnt_standby_restart_hub_record";
const CALLER_PRINCIPAL_ID = "prn_standby_restart_hub_record";
const DEFINITION_ASSET_ID = "ast_standby_restart_hub_record_wf";
const MANIFEST_DIGEST = "1".repeat(64);

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
    name: "standby-restart-hub-record-wf",
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

// The allocation the fixture's router binds this deployment to, as the Hub
// records it when it places the deployment, so the lifecycle service can reach
// the deployment's sidecar.
async function seedAllocation(target: {
  allocationId: string;
  generation: number;
}): Promise<void> {
  const provisioner = {
    provisionerId: "test",
    provisionerApiVersion: 2 as const,
    provisionerBindingFingerprint: "test:1",
    adapterManifestDigest: MANIFEST_DIGEST,
  };
  await h.db.insert(sidecarTable).values({
    id: SIDECAR_ID,
    tokenHashSha256: new Uint8Array(32),
    ...provisioner,
  });
  await h.db.insert(sidecarAllocation).values({
    id: target.allocationId,
    tenantId: TENANT_ID,
    anchorRunId: DEPLOYMENT_ID,
    ...provisioner,
    maxDisconnectedMs: 900_000,
    sidecarId: SIDECAR_ID,
    status: "allocated",
    generation: target.generation,
    ensureAcceptedGeneration: target.generation,
  });
}

describe.skipIf(!harnessDbEnvAvailable())(
  "sidecar restart of a deployment the Hub put in standby",
  () => {
    test("the Hub still records the standby after the restore, and a wake it asks for resumes the run", async () => {
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
        principalId: "prin_standby-restart-hub-record-1",
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
        messageId: "<standby-restart-hub-record-1@integration.interchange>",
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

      const target = env.hub.prepareAllocationIdentity(
        DEPLOYMENT_ID,
        address,
        SIDECAR_ID,
      );
      await seedAllocation(target);
      let commandSettled = Promise.withResolvers<undefined>();
      // The sweep pauses between passes on this clock, which the test moves.
      let nowMs = Date.now();
      const lifecycle = createWorkflowLifecycleService({
        db: h.db,
        retentionRouter: env.hub.router,
        runReader: createWorkflowRunReader(env.hub.agentRepoStore.repoStore),
        historyReceives: createWorkflowHistoryReceiveTracker(),
        standbyRouter: env.hub.router,
        onStandbyCommandSettled: (settledRunId) => {
          if (settledRunId === DEPLOYMENT_ID) commandSettled.resolve(undefined);
        },
        now: () => new Date(nowMs),
      });
      const standby = async () =>
        (await lifecycle.getStatus(TENANT_ID, DEPLOYMENT_ID))?.standby;
      // The state once the sweep has carried out the pending request. This run
      // is the only one the sweep has work for, so a pass that selects a run
      // sends its command, and sends it again once that settles unanswered:
      // the sidecar's first answer can wait on history the child is still
      // pushing.
      const settled = async () => {
        for (;;) {
          const current = await standby();
          if (current?.state !== "entering" && current?.state !== "waking")
            return current;
          commandSettled = Promise.withResolvers<undefined>();
          if (await lifecycle.reconcileNext()) await commandSettled.promise;
          // The sweep selects nothing while it pauses between passes.
          else nowMs += 1_000;
        }
      };
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

      // Each report and the Hub's handling of it, wired as the Hub composes it.
      const observed: {
        report: SidecarEventMap["deployment.standby"];
        noted: Promise<void>;
      }[] = [];
      const unsubscribe = env.hub.router.events.on(
        "deployment.standby",
        (report) => {
          const noted = lifecycle.noteDeploymentInStandby(report);
          observed.push({ report, noted });
          return noted;
        },
      );
      try {
        expect(await lifecycle.requestStandby(TENANT_ID, DEPLOYMENT_ID)).toBe(
          "pending",
        );
        const recorded = await settled();
        expect(recorded).toEqual({
          state: "standby",
          changedAt: expect.any(String),
          wakeAt: null,
          reason: null,
        });
        expect(listWorkflowHostChildren(env)).toEqual([]);
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

        // The Hub emits the hello's reports as it restores the routes, so
        // the restore's report is in by now.
        expect(observed.map(({ report }) => report)).toEqual([
          { ...target, inStandby: true },
        ]);
        await Promise.all(observed.map(({ noted }) => noted));
        expect(await standby()).toEqual(recorded);
        const restarted = { ...env, sidecar: restartedSidecar };
        expect(listWorkflowHostChildren(restarted)).toEqual([]);
        expect(await standbyMark()).toBeDefined();

        // ---- the Hub wakes the child, and the signal resumes the run ----
        expect(await lifecycle.requestWake(TENANT_ID, DEPLOYMENT_ID)).toBe(
          "pending",
        );
        expect(await settled()).toEqual({
          state: "awake",
          changedAt: expect.any(String),
          wakeAt: null,
          reason: null,
        });
        expect(await standbyMark()).toBeUndefined();

        await injectSignal(env, DEPLOYMENT_ID, runId, "go", { resumed: true });
        const terminal = await waitForWorkflowRunComplete(
          env,
          DEPLOYMENT_ID,
          runId,
          { diagnostics: env.sidecarDiagnostics },
        );
        expect(terminal.type).toBe("RunCompleted");
        // A wake the Hub asked for, and a signal to a running child, report
        // nothing.
        expect(observed).toHaveLength(1);
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

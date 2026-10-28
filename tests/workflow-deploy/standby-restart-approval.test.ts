// A deployment in standby on an approval re-sends that approval after a
// sidecar restart without starting its child, and the decision then starts the
// child, which completes the run. The Hub's correlation lookup records each
// registration instead of writing approval rows, and the decision is the signal
// the approve route sends after claiming the approval.
//
// Harness justification: SPAWN-REAL: real hub, real sidecar subprocess, real
// workflow-process child, mock inference.

import fs from "node:fs";
import path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { tenant as tenantTable } from "@intx/db/schema";
import {
  WorkflowControlHistoryPendingError,
  type SidecarEventMap,
} from "@intx/hub-sessions";
import { signalName } from "@intx/types";
import type { WireGrantRule } from "@intx/types/grant-wire";
import type {
  ApprovalSnapshot,
  HarnessConfig,
  InferenceSource,
} from "@intx/types/runtime";
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
  createApprovalSet,
  deriveRunAddress,
  type ApprovalSet,
} from "@intx/workflow-deploy";

import {
  SESSION_ID,
  SIDECAR_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  injectSignal,
  listWorkflowHostChildren,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  startSidecarSubprocess,
  terminateSidecarSubprocess,
  waitFor,
  waitForFirstRunId,
  waitForReconnect,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
  type SidecarHandle,
} from "../hub-agent/lib/deploy-flow-env";
import { MAIL_TOOL_NAME } from "./fixtures/mail-tool";
import { singleStepMailToolEntry } from "./fixtures/single-step-mail-tool";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_standby-restart-approval-1";
const TENANT_ID = "tnt_standby_restart_approval";
const CALLER_PRINCIPAL_ID = "prn_standby_restart_approval";
const DEFINITION_ASSET_ID = "ast_standby_restart_approval_wf";
const STEP_ID = "step1";
// Longer than the test's budget, so the worker's acknowledgement, not the
// clock, settles the control request.
const CONTROL_TIMEOUT_MS = 300_000;
const CALL_INPUT = { to: "standby-restart", body: "standby-tool-ran.txt" };

const ASK_GRANT: WireGrantRule = {
  id: "grant-tool-ask",
  resource: `tool:${MAIL_TOOL_NAME}`,
  action: "invoke",
  effect: "ask",
  origin: "creator",
  conditions: null,
  expiresAt: null,
  roleId: null,
  principalId: null,
};

type Registration = {
  correlationId: string;
  runId: string;
  approvalSnapshot: ApprovalSnapshot;
};

let env: DeployFlowEnv;
let h: TestDb;
let restartedSidecar: SidecarHandle | undefined;
const restartTempDirs: string[] = [];
// Resolved by the next approval registration; the test re-arms it before the
// registration it awaits.
let nextRegistration = Promise.withResolvers<Registration>();

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
    name: "standby-restart-approval-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv({
    // The model calls the ask-granted tool until its result is in history,
    // so the run parks on the approval and completes once it is approved.
    inferenceApprovalToolCall: {
      toolName: MAIL_TOOL_NAME,
      input: CALL_INPUT,
      resultPrefix: "done: ",
    },
    registerSignalCorrelation: async ({
      correlationId,
      runId,
      approvalSnapshot,
    }) => {
      nextRegistration.resolve({ correlationId, runId, approvalSnapshot });
    },
  });
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
  "sidecar restart of a deployment in standby on an approval",
  () => {
    test("the restore re-registers the approval without a child, and its decision resumes the run", async () => {
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
        principalId: "prin_standby-restart-approval-1",
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
        `tool:${MAIL_TOOL_NAME}`,
      ]);
      const handle = await deployWorkflowSourceForTest(env, {
        entryModule: singleStepMailToolEntry({
          variant: "ask",
          stepId: STEP_ID,
          systemPrompt: "You are the single-step agent under approval control.",
          address,
          agentId: "agent-standby-restart-approval",
        }),
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: address,
        approvals,
        config,
        sources: { [STEP_ID]: [inferenceSource] },
      });
      await waitFor(
        () => env.hub.router.getRoutableAddresses().includes(address),
        { diagnostics: env.sidecarDiagnostics },
      );

      await fireMailTrigger(env, address, {
        messageId: "<standby-restart-approval-1@integration.interchange>",
        grants: [ASK_GRANT],
      });
      const runId = await waitForFirstRunId(env, handle.workflowRunRepoId, {
        diagnostics: env.sidecarDiagnostics,
      });
      // The deployment's one run is the only one that registers an approval.
      const parked = await nextRegistration.promise;
      expect(parked.runId).toBe(runId);
      const correlationId = parked.correlationId;
      await waitFor(
        async () =>
          (await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId)).some(
            (e) =>
              e.type === "SignalAwaited" &&
              e.body["signalName"] === signalName(correlationId),
          ),
        { diagnostics: env.sidecarDiagnostics },
      );

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
        parked: [
          expect.objectContaining({
            runId,
            correlationId,
            parkKind: "approval",
          }),
        ],
        tip: expect.any(String),
      });

      // ---- kill the sidecar, and start a fresh one on its data dir ----
      const dataDir = env.sidecar.dataDir;
      await terminateSidecarSubprocess(env.sidecar);
      await waitFor(
        () => !env.hub.router.getRoutableAddresses().includes(address),
        { diagnostics: env.sidecarDiagnostics },
      );
      nextRegistration = Promise.withResolvers<Registration>();
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

        // ---- the restored standby re-sends the approval it waits on ----
        const reRegistered = await nextRegistration.promise;
        const restarted = { ...env, sidecar: restartedSidecar };
        expect(listWorkflowHostChildren(restarted)).toEqual([]);
        expect(reRegistered).toEqual(parked);
        expect(reports).toEqual([{ ...target, inStandby: true }]);
        expect(await standbyMark()).toBeDefined();

        // ---- the approval's decision starts the child, which resumes the run ----
        await injectSignal(
          env,
          DEPLOYMENT_ID,
          runId,
          signalName(correlationId),
          {
            outcome: "approved",
          },
        );
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
          (e) =>
            e.type === "SignalReceived" &&
            e.body["signalName"] === signalName(correlationId),
        ),
      ).toBe(1);
      expect(count((e) => e.type === "RunCompleted")).toBe(1);
    }, 180_000);
  },
);

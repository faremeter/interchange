// A deployment in standby on an approval wakes for the approval's decision.
//
// The run parks on an approval, and standby stops its child. The decision is the
// signal the approve route sends after claiming the approval. It starts a new
// child, which resumes the run and completes it.
//
// Harness justification: SPAWN-REAL: real hub, real sidecar subprocess, real
// workflow-process child, mock inference.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { tenant as tenantTable } from "@intx/db/schema";
import { type SidecarEventMap } from "@intx/hub-sessions";
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
  waitFor,
  waitForFirstRunId,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { putInStandby } from "./standby-helpers";
import { MAIL_TOOL_NAME } from "./fixtures/mail-tool";
import { singleStepMailToolEntry } from "./fixtures/single-step-mail-tool";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_standby-approval-wake-1";
const TENANT_ID = "tnt_standby_approval_wake";
const CALLER_PRINCIPAL_ID = "prn_standby_approval_wake";
const DEFINITION_ASSET_ID = "ast_standby_approval_wake_wf";
const STEP_ID = "step1";
const CALL_INPUT = {
  to: "standby-approval-wake",
  body: "standby-approval-wake-tool-ran.txt",
};

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
// Resolved by the run's approval registration.
const nextRegistration = Promise.withResolvers<Registration>();

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
    name: "standby-approval-wake-wf",
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
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

describe.skipIf(!harnessDbEnvAvailable())(
  "a deployment in standby on an approval",
  () => {
    test("the approval's decision starts a new child, which completes the run", async () => {
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
          agentId: "agent-standby-approval-wake",
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
        messageId: "<standby-approval-wake-1@integration.interchange>",
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

      const target = env.hub.prepareAllocationIdentity(
        DEPLOYMENT_ID,
        address,
        SIDECAR_ID,
      );
      await putInStandby(env, target);
      expect(listWorkflowHostChildren(env)).toEqual([]);

      // ---- the approval's decision starts the child, which resumes the run ----
      const woken =
        Promise.withResolvers<SidecarEventMap["deployment.standby"]>();
      const unsubscribe = env.hub.router.events.on(
        "deployment.standby",
        (report) => {
          woken.resolve(report);
        },
      );
      try {
        await injectSignal(
          env,
          DEPLOYMENT_ID,
          runId,
          signalName(correlationId),
          { outcome: "approved" },
        );
        const terminal = await waitForWorkflowRunComplete(
          env,
          DEPLOYMENT_ID,
          runId,
          { diagnostics: env.sidecarDiagnostics },
        );
        expect(terminal.type).toBe("RunCompleted");
        expect(await woken.promise).toEqual({
          ...target,
          inStandby: false,
          reason: "Woken by signal",
        });
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

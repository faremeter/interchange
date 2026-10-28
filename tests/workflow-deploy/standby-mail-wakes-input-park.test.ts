// Mail wakes a deployment in standby whose run waits on input, and the new
// child answers it as the run's next turn.
//
// An unbounded single-step mail agent replies to mail 1 and parks on input.
// The deployment is put in standby, which stops the child. Mail 2 then wakes
// it: the new child resumes the run parked on input, and the supervisor hands
// it mail 2 on that input once the child reports the park, though the park
// was taken before the child started and sends no notice of its own.
//
// Harness justification: SPAWN-REAL: real hub, real sidecar subprocess, real
// workflow-process child, mock inference.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { parseHeaderSection } from "@intx/mime";
import { tenant as tenantTable } from "@intx/db/schema";
import {
  WorkflowControlHistoryPendingError,
  type SidecarEventMap,
} from "@intx/hub-sessions";
import type { WireGrantRule } from "@intx/types/grant-wire";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
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
import { deriveRunAddress } from "@intx/workflow-deploy";

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
  type DeployFlowEnv,
  type WorkflowRunEvent,
} from "../hub-agent/lib/deploy-flow-env";
import { singleStepMailInboxEntry } from "./fixtures/single-step-mail-inbox-agent";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_5ba1d0ba11ce5a11b0ba5eba11cafe0f";
const TENANT_ID = "tnt_standby_mail_input";
const CALLER_PRINCIPAL_ID = "prn_standby_mail_input";
const DEFINITION_ASSET_ID = "ast_standby_mail_input_wf";
const STEP_ID = "step1";
// Longer than the test's budget, so the worker's acknowledgement, not the
// clock, settles the control request.
const CONTROL_TIMEOUT_MS = 300_000;

const MAIL1_MESSAGE_ID = "<standby-input-m1@integration.interchange>";
const MAIL2_MESSAGE_ID = "<standby-input-m2@integration.interchange>";
const MAIL_FROM = "user-standby-input@integration.interchange";

const ALL_TOOL_GRANTS: WireGrantRule[] = [
  "mail_send",
  "mail_reply",
  "mail_search",
  "mail_read",
  "mail_wait",
].map((name) => ({
  id: `grant-tool-${name}`,
  resource: `tool:${name}`,
  action: "invoke",
  effect: "allow",
  origin: "creator",
  conditions: null,
  expiresAt: null,
  roleId: null,
  principalId: null,
}));

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
    name: "standby-mail-input-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });
  // Each turn replies with plain text and calls no tool, so the unbounded
  // step parks on input after every turn.
  env = await startDeployFlowEnv({
    inferenceScriptedTurns: [{ text: "Acknowledged." }],
  });
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

function inputParks(events: WorkflowRunEvent[]): number {
  return events.filter(
    (e) => e.type === "SignalAwaited" && e.body["parkKind"] === "input",
  ).length;
}

describe.skipIf(!harnessDbEnvAvailable())(
  "mail that wakes a deployment in standby",
  () => {
    test("reaches the run parked on input as its next turn", async () => {
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
        principalId: "prin_standby-mail-input",
        agentAddress: address,
        systemPrompt: "Fallback prompt (overridden per step by the definition)",
        tools: [],
        grants: [],
        sources: [inferenceSource],
        defaultSource: "anthropic:mock-model",
      };
      const handle = await deployWorkflowSourceForTest(env, {
        entryModule: singleStepMailInboxEntry({
          stepId: STEP_ID,
          systemPrompt: "You are the single-step mail-inbox agent.",
          address,
          agentId: "agent-standby-mail-input",
          triggers: "unbounded",
        }),
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: address,
        approvals: "approve-probed",
        config,
        sources: { [STEP_ID]: [inferenceSource] },
      });
      await waitFor(
        () => env.hub.router.getRoutableAddresses().includes(address),
        { diagnostics: env.sidecarDiagnostics },
      );
      const replyTo = (messageId: string) =>
        env.hub.outboundMail
          .map((m) => ({ m, headers: parseHeaderSection(m.raw).headers }))
          .find(
            ({ m, headers }) =>
              m.senderAddress === address &&
              headers.get("in-reply-to") === messageId,
          )?.headers;

      await fireMailTrigger(env, address, {
        messageId: MAIL1_MESSAGE_ID,
        from: MAIL_FROM,
        content: "First turn.",
        grants: ALL_TOOL_GRANTS,
      });
      const runId = await waitForFirstRunId(env, handle.workflowRunRepoId, {
        diagnostics: env.sidecarDiagnostics,
      });
      await waitFor(() => replyTo(MAIL1_MESSAGE_ID) !== undefined, {
        diagnostics: env.sidecarDiagnostics,
      });
      const reply1MessageId = replyTo(MAIL1_MESSAGE_ID)?.get("message-id");
      if (reply1MessageId === undefined) {
        throw new Error("the first reply carried no Message-Id");
      }
      await waitFor(
        async () =>
          inputParks(await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId)) >=
          1,
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

      const reported =
        Promise.withResolvers<SidecarEventMap["deployment.standby"]>();
      const unsubscribe = env.hub.router.events.on(
        "deployment.standby",
        (report) => {
          reported.resolve(report);
        },
      );
      try {
        // The user answers the first reply, continuing the conversation.
        await fireMailTrigger(env, address, {
          messageId: MAIL2_MESSAGE_ID,
          from: MAIL_FROM,
          content: "Second turn.",
          inReplyTo: reply1MessageId,
          references: [MAIL1_MESSAGE_ID, reply1MessageId],
          grants: ALL_TOOL_GRANTS,
        });
        await waitFor(() => replyTo(MAIL2_MESSAGE_ID) !== undefined, {
          diagnostics: env.sidecarDiagnostics,
        });
        expect(await reported.promise).toEqual({
          ...target,
          inStandby: false,
          reason: "Woken by mail",
        });
      } finally {
        unsubscribe();
      }

      const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
      // Mail 2 was the run's next turn on its input, not a run of its own.
      expect(events.filter((e) => e.type === "RunStarted")).toHaveLength(1);
      expect(
        events.filter(
          (e) =>
            e.type === "SignalReceived" &&
            e.body["signalId"] === MAIL2_MESSAGE_ID,
        ).length,
      ).toBeGreaterThanOrEqual(1);
    }, 180_000);
  },
);

// Mail wakes a deployment in standby whose onTrigger section waits on input,
// and the new child answers it as the section's next event. The answer
// continues the conversation of the event before the standby.
//
// The workflow has one onTrigger section on mail whose body has one agent
// step that sets `replyByMail: true`. The body run of mail 1 replies, and the
// section parks on input. The deployment is put in standby, which stops the
// child. Mail 2 then wakes it: the new child resumes the section parked on
// input, and the supervisor hands it mail 2 on that input once the child
// reports the park, though the park was taken before the child started and
// sends no notice of its own. The body run of mail 2 replies with the
// conversation of mail 1 in its context, so the conversation continued across
// the stop and start of the child process.
//
// Harness justification: SPAWN-REAL: real hub, real sidecar subprocess, real
// workflow-process child, mock inference.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { parseHeaderSection } from "@intx/mime";
import { tenant as tenantTable } from "@intx/db/schema";
import { type SidecarEventMap } from "@intx/hub-sessions";
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
import { deriveWorkflowRunId } from "@intx/types";
import { sectionBodyRunId } from "@intx/workflow";
import { deriveRunAddress } from "@intx/workflow-deploy";

import {
  SESSION_ID,
  SIDECAR_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  listWorkflowHostChildren,
  listRunIds,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  type DeployFlowEnv,
  type WorkflowRunEvent,
} from "../hub-agent/lib/deploy-flow-env";
import { putInStandby } from "./standby-helpers";
import {
  messageText,
  conversationRequestFor,
  withoutCacheMarkers,
} from "./fixtures/conversation-requests";
import { onTriggerBodyEntry } from "./fixtures/on-trigger-body";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_5ba1d0ba11ce5a11b0ba5eba11cafe0f";
const TENANT_ID = "tnt_standby_mail_input";
const CALLER_PRINCIPAL_ID = "prn_standby_mail_input";
const DEFINITION_ASSET_ID = "ast_standby_mail_input_wf";
const SECTION_ID = "section";
const BODY_STEP_ID = "reply";

const MAIL1_MESSAGE_ID = "<standby-input-m1@integration.interchange>";
const MAIL2_MESSAGE_ID = "<standby-input-m2@integration.interchange>";
const MAIL_FROM = "user-standby-input@integration.interchange";
const MAIL1_BODY = "First turn standby-input-5ba1d0.";
const MAIL2_BODY = "Second turn standby-input-5ba1d0.";
const TURN_REPLY = "Acknowledged.";

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
  // Each turn replies with plain text and calls no tool, so each body run
  // completes and the section parks on input after every event.
  env = await startDeployFlowEnv({
    inferenceScriptedTurns: [{ text: TURN_REPLY }],
  });
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

function inputParkNames(events: WorkflowRunEvent[]): Set<unknown> {
  return new Set(
    events
      .filter(
        (e) =>
          e.type === "SignalAwaited" &&
          e.body["stepId"] === SECTION_ID &&
          e.body["parkKind"] === "input",
      )
      .map((e) => e.body["signalName"]),
  );
}

describe.skipIf(!harnessDbEnvAvailable())(
  "mail that wakes a deployment in standby",
  () => {
    test("reaches the section parked on input as its next event, which continues the conversation", async () => {
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
      const handle = await deployWorkflowSourceForTest(env, {
        entryModule: onTriggerBodyEntry({
          address,
          sectionId: SECTION_ID,
          body: {
            variant: "agent",
            stepId: BODY_STEP_ID,
            systemPrompt: "You are the onTrigger body mail-inbox agent.",
            agentId: "agent-standby-mail-input",
            mailTools: true,
            replyByMail: true,
          },
          workflowId: `wf_${DEPLOYMENT_ID}`,
          bodyWorkflowId: `wf_${DEPLOYMENT_ID}_body`,
        }),
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: address,
        approvals: "approve-probed",
        config,
        sources: { [SECTION_ID]: [inferenceSource] },
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
        content: MAIL1_BODY,
        grants: ALL_TOOL_GRANTS,
      });
      const runId = deriveWorkflowRunId(address);
      await waitFor(() => replyTo(MAIL1_MESSAGE_ID) !== undefined, {
        diagnostics: env.sidecarDiagnostics,
      });
      const reply1MessageId = replyTo(MAIL1_MESSAGE_ID)?.get("message-id");
      if (reply1MessageId === undefined) {
        throw new Error("the first reply carried no Message-Id");
      }
      await waitFor(
        async () =>
          inputParkNames(await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId))
            .size >= 1,
        { diagnostics: env.sidecarDiagnostics },
      );

      const target = env.hub.prepareAllocationIdentity(
        DEPLOYMENT_ID,
        address,
        SIDECAR_ID,
      );
      await putInStandby(env, target);
      expect(listWorkflowHostChildren(env)).toEqual([]);

      const reported =
        Promise.withResolvers<SidecarEventMap["deployment.standby"]>();
      const unsubscribe = env.hub.router.events.on(
        "deployment.standby",
        (report) => {
          reported.resolve(report);
        },
      );
      let mail2Key: string;
      try {
        // The user answers the first reply, continuing the conversation.
        ({ deliveryKey: mail2Key } = await fireMailTrigger(env, address, {
          messageId: MAIL2_MESSAGE_ID,
          from: MAIL_FROM,
          content: MAIL2_BODY,
          inReplyTo: reply1MessageId,
          references: [MAIL1_MESSAGE_ID, reply1MessageId],
          grants: ALL_TOOL_GRANTS,
        }));
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

      // The run's history reaches the Hub after the reply does. The section
      // takes a follow-up mail on its input park as a signal named by the
      // mail's delivery key.
      const mail2Turns = (events: WorkflowRunEvent[]) =>
        events.filter(
          (e) =>
            e.type === "SignalReceived" &&
            inputParkNames(events).has(e.body["signalName"]) &&
            e.body["signalId"] === mail2Key,
        ).length;
      await waitFor(
        async () =>
          mail2Turns(await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId)) >=
          1,
        { diagnostics: env.sidecarDiagnostics },
      );
      const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
      // Mail 2 was the section's next event on its input, not a run of its
      // own: the only runs are the container and one body run for each mail.
      expect(mail2Turns(events)).toBe(1);
      expect(events.filter((e) => e.type === "RunStarted")).toHaveLength(1);
      const event0 = sectionBodyRunId(runId, SECTION_ID, 0);
      const event1 = sectionBodyRunId(runId, SECTION_ID, 1);
      expect(
        events.flatMap((e) =>
          e.type === "ChildSpawned" ? [e.body["childRunId"]] : [],
        ),
      ).toEqual([event0, event1]);
      expect(
        (await listRunIds(env, handle.workflowRunRepoId)).toSorted(),
      ).toEqual([runId, event0, event1].toSorted());

      // The body run of mail 2, on the new child, continued the conversation
      // of mail 1: its request holds the request of mail 1 and the reply to
      // it, then mail 2.
      const event0Turn = withoutCacheMarkers(
        conversationRequestFor(env.inference.requests, MAIL1_BODY, MAIL1_BODY)
          .messages ?? [],
      );
      const event1Request = conversationRequestFor(
        env.inference.requests,
        MAIL1_BODY,
        MAIL2_BODY,
      );
      const event1Turn = withoutCacheMarkers(event1Request.messages ?? []);
      expect(event1Turn.slice(0, event0Turn.length)).toEqual(event0Turn);
      const carriedReply = event1Request.messages?.[event0Turn.length];
      expect(carriedReply?.role).toBe("assistant");
      expect(messageText(carriedReply)).toContain(TURN_REPLY);
      expect(event1Turn).toHaveLength(event0Turn.length + 2);
    }, 180_000);
  },
);

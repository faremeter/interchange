// An interactive mail conversation through an onTrigger section on mail.
//
// The workflow has one top-level `onTrigger` section on the deployment
// address. Its body has one agent step that sets `replyByMail: true`. Each
// inbound mail is a new event of the section, and each event is a new body
// run. The body step keeps its conversation at its place key
// (`section.reply`), so each body run continues the conversation of the body
// run before it.
//
// Run model. Mail 1 starts the container run, whose section spawns the body
// run of event 0. After that body completes, the section parks on its input.
// The supervisor delivers mail 2 on that park as a `signal.deliver`, and the
// section spawns the body run of event 1. Each body step builds the inbound
// message from its mail and seeds the connector thread with it, so the
// connector thread continues across the body runs and the reply of event 1
// threads onto mail 2.
//
// The scripted mock returns a plain text reply on each turn and calls no
// tool. Each turn sends its reply through the step's connector reply path,
// which composes the outbound `In-Reply-To`/`To`/`Cc` from the durable
// connector thread. The delivered wire headers thus prove which mail each
// reply threaded onto.
//
// Assertions, read off the delivered `mail.outbound` bytes (via `persistMail`'s
// retained `raw`, parsed with `parseHeaderSection`), the run event log and the
// recorded inference requests:
//   (a) exactly ONE `RunStarted` for the container run across both mails,
//       consuming mail 1; mail 2 opened no run of its own and the container
//       reached no terminal event between the mails.
//   (b) R1's wire In-Reply-To == m1 (To == userA); R2's wire In-Reply-To == m2
//       (To == userB) -- each reply threaded onto its own mail. R2 also
//       carries the FULL References ancestry [m1, r1, m2] (mail 2's own
//       References plus mail 2's Message-Id), not a truncated [m2]; R1's is
//       the single-element [m1] the opener seeds.
//   (c) mail 2 was the section's next event: a `SignalReceived` on the
//       section whose signalId is mail 2's delivery key, one `ChildSpawned`
//       for each event, and two section input parks.
//   (d) connector cc-accumulation: R2 cc-includes userA, the participant of
//       the earlier mail -- the connector thread continued across the two
//       body runs rather than restarting on mail 2.
//   (e) the model saw the conversation: the request of event 1 holds the
//       request of event 0 and its reply before mail 2.
//
// The non-participant test also reads the sidecar's local step storage: each
// body run's `workflow-step-state/<repo>/runs/<bodyRunId>/` is gone once that
// body run ends, and a later mail still continues the thread.

import fs from "node:fs";
import path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { parseHeaderSection } from "@intx/mime";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import type { WireGrantRule } from "@intx/types/grant-wire";
import type { RepoId } from "@intx/hub-sessions";
import { deriveWorkflowRunId } from "@intx/types";
import { sectionBodyRunId } from "@intx/workflow";
import { deriveRunAddress } from "@intx/workflow-deploy";
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
  SESSION_ID,
  SIDECAR_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  type DeployFlowEnv,
  type WorkflowRunEvent,
} from "../hub-agent/lib/deploy-flow-env";
import {
  messageText,
  conversationRequestFor,
  withoutCacheMarkers,
} from "./fixtures/conversation-requests";
import { onTriggerBodyEntry } from "./fixtures/on-trigger-body";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_ma11500dba5eba11cafef00dba5eba55";
const SECTION_ID = "section";
const BODY_STEP_ID = "reply";
const AGENT_ID = "agent-mail-interactive";

const MAIL1_MESSAGE_ID = "<mail-int-m1-7c4e21@integration.interchange>";
const MAIL1_FROM = "userone-7c4e21@integration.interchange";
const MAIL1_BODY = "Turn one opener body marker mail-int-7c4e21.";

const MAIL2_MESSAGE_ID = "<mail-int-m2-7c4e21@integration.interchange>";
const MAIL2_FROM = "usertwo-7c4e21@integration.interchange";
const MAIL2_BODY = "Turn two continuation body marker mail-int-7c4e21.";

const MAIL3_MESSAGE_ID = "<mail-int-m3-7c4e21@integration.interchange>";
const MAIL3_FROM = "userthree-7c4e21@integration.interchange";
const MAIL3_BODY = "Turn three outsider body marker mail-int-7c4e21.";

const MAIL4_MESSAGE_ID = "<mail-int-m4-7c4e21@integration.interchange>";
const MAIL4_BODY = "Turn four owner body marker mail-int-7c4e21.";

// A plain text reply per turn: the body step produces an OUTPUT (so its body
// run completes and the section parks on its input) and calls no tool (so it
// never stays inside a single turn the way `mail_wait` would). The step invoker
// threads each reply from the durable connector thread seeded on that turn's
// mail.
const TURN_REPLY = "Acknowledged; replying on this turn.";

const ALL_TOOL_NAMES = [
  "mail_send",
  "mail_reply",
  "mail_search",
  "mail_read",
  "mail_wait",
] as const;
const ALL_TOOL_GRANTS: WireGrantRule[] = ALL_TOOL_NAMES.map((name) => ({
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

const TENANT_ID = "tnt_on_trigger_mail_turns";
const CALLER_PRINCIPAL_ID = "prn_on_trigger_mail_turns";
const DEFINITION_ASSET_ID = "ast_on_trigger_mail_turns_wf";

let env: DeployFlowEnv;
let h: TestDb;

// The two-mail test records the container run and the thread's last message
// for the non-participant test that follows it on the same deployment.
let threadRunId: string | undefined;
let threadLastMessageId: string | undefined;
let threadRepoId: RepoId | undefined;

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
    name: "on-trigger-mail-turns-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  // Every turn returns a plain text reply. The mock indexes scripted turns by
  // the number of tool_results in the request history; this agent calls no
  // tool, so the count stays 0 and each turn resolves to this single text turn.
  env = await startDeployFlowEnv({
    inferenceScriptedTurns: [{ text: TURN_REPLY }],
  });
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

/**
 * Wait for a delivered outbound reply from `sender` whose parsed `In-Reply-To`
 * header equals `inReplyTo`, then return its parsed headers. Reads the reply's
 * real headers off the delivered wire bytes the hub retained.
 */
async function waitForReplyInReplyTo(
  env: DeployFlowEnv,
  sender: string,
  inReplyTo: string,
): Promise<Map<string, string>> {
  await waitFor(
    () =>
      env.hub.outboundMail.some(
        (m) =>
          m.senderAddress === sender &&
          parseHeaderSection(m.raw).headers.get("in-reply-to") === inReplyTo,
      ),
    { diagnostics: env.sidecarDiagnostics },
  );
  const match = env.hub.outboundMail.find(
    (m) =>
      m.senderAddress === sender &&
      parseHeaderSection(m.raw).headers.get("in-reply-to") === inReplyTo,
  );
  if (match === undefined) {
    throw new Error(`no delivered reply with In-Reply-To ${inReplyTo}`);
  }
  return parseHeaderSection(match.raw).headers;
}

/** The Message-Ids of a delivered reply's `References` header, in order. */
function referencesOf(headers: Map<string, string>): string[] {
  return (headers.get("references") ?? "")
    .split(/\s+/)
    .filter((id) => id.length > 0);
}

/** The signal names of the section's input parks. */
function sectionInputSignalNames(events: WorkflowRunEvent[]): Set<unknown> {
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

/**
 * The run directories under the sidecar's local step storage for the
 * deployment. A body step's local store and workspace live in its body run's
 * directory there.
 */
async function stepStateRunDirs(
  env: DeployFlowEnv,
  repoId: RepoId,
): Promise<string[]> {
  return fs.promises.readdir(
    path.join(env.sidecar.dataDir, "workflow-step-state", repoId.id, "runs"),
  );
}

/** Count the section's input parks: the section parks on its input once
 * after each event's body run completes. */
function inputRearmCount(events: WorkflowRunEvent[]): number {
  return events.filter(
    (e) =>
      e.type === "SignalAwaited" &&
      e.body["stepId"] === SECTION_ID &&
      e.body["parkKind"] === "input",
  ).length;
}

describe.skipIf(!harnessDbEnvAvailable())(
  "an onTrigger section on mail services each inbound mail as a body run that continues the conversation",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("mail 2 is the section's next event via signal.deliver and its reply threads onto mail 2", async () => {
      const deploymentMailAddress = deriveRunAddress({
        runId: DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });

      const inferenceSource: InferenceSource = {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${env.inference.server.port}`,
        credentialId: "sk-mock",
        model: "mock-model",
      };

      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentAddress: deploymentMailAddress,
        sources: [inferenceSource],
        defaultSource: "anthropic:mock-model",
      };

      const entryModule = onTriggerBodyEntry({
        address: deploymentMailAddress,
        sectionId: SECTION_ID,
        body: {
          variant: "agent",
          stepId: BODY_STEP_ID,
          systemPrompt: "You are the onTrigger body mail-inbox agent.",
          agentId: AGENT_ID,
          mailTools: true,
          replyByMail: true,
        },
        workflowId: `wf_${DEPLOYMENT_ID}`,
        bodyWorkflowId: `wf_${DEPLOYMENT_ID}_body`,
      });

      const handle = await deployWorkflowSourceForTest(env, {
        entryModule,
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: deploymentMailAddress,
        approvals: "approve-probed",
        config,
        sources: { [SECTION_ID]: [inferenceSource] },
      });
      expect(handle.publicKey).toBeTruthy();
      threadRepoId = handle.workflowRunRepoId;

      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { diagnostics: env.sidecarDiagnostics },
      );

      // --- Event 0: opener from userA (trigger.fire) --------------------
      // userA cc's userB, which makes userB a participant that can continue
      // the thread.
      const mail1 = await fireMailTrigger(env, deploymentMailAddress, {
        messageId: MAIL1_MESSAGE_ID,
        from: MAIL1_FROM,
        cc: [MAIL2_FROM],
        content: MAIL1_BODY,
        grants: ALL_TOOL_GRANTS,
      });
      expect(mail1.messageId).toBe(MAIL1_MESSAGE_ID);

      // The container run's id is a property of the deployment address.
      const runId = deriveWorkflowRunId(deploymentMailAddress);
      threadRunId = runId;

      const r1Headers = await waitForReplyInReplyTo(
        env,
        deploymentMailAddress,
        MAIL1_MESSAGE_ID,
      );
      expect(r1Headers.get("in-reply-to")).toBe(MAIL1_MESSAGE_ID);
      expect(r1Headers.get("to")).toBe(MAIL1_FROM);
      // userB is addressed but has not spoken, so R1 does not cc userB.
      expect(r1Headers.get("cc")).toBeUndefined();
      // R1's own Message-Id anchors mail 2's threading, exactly as a mail
      // client continuing the conversation would set In-Reply-To.
      const r1MessageId = r1Headers.get("message-id");
      if (r1MessageId === undefined) {
        throw new Error("R1 carried no Message-Id");
      }

      // The body run of event 0 completed and the section parked on its
      // input. Wait for that park before firing mail 2 so the second mail
      // lands on the signal.deliver rail (event 1), not a race with an
      // in-flight body run.
      await waitFor(
        async () =>
          inputRearmCount(
            await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId),
          ) >= 1,
        { diagnostics: env.sidecarDiagnostics },
      );

      // --- Event 1: continuation from userB, dispatched as signal.deliver --
      // Reference the thread root (m1) and R1 so the connector router treats
      // mail 2 as a continuation of the SAME thread rather than a new one.
      // userB can continue it because userA addressed userB on mail 1.
      const mail2References = [MAIL1_MESSAGE_ID, r1MessageId];
      const mail2 = await fireMailTrigger(env, deploymentMailAddress, {
        messageId: MAIL2_MESSAGE_ID,
        from: MAIL2_FROM,
        content: MAIL2_BODY,
        inReplyTo: r1MessageId,
        references: mail2References,
        grants: ALL_TOOL_GRANTS,
      });
      expect(mail2.messageId).toBe(MAIL2_MESSAGE_ID);

      const r2Headers = await waitForReplyInReplyTo(
        env,
        deploymentMailAddress,
        MAIL2_MESSAGE_ID,
      );
      expect(r2Headers.get("in-reply-to")).toBe(MAIL2_MESSAGE_ID);
      expect(r2Headers.get("to")).toBe(MAIL2_FROM);
      threadLastMessageId = r2Headers.get("message-id");
      // Connector cc-accumulation: userA, the participant of mail 1, is
      // carried forward onto the reply to mail 2. This only happens if the
      // SAME connector thread advanced across the two body runs (mail 2 seeded
      // as a continuation), proving the thread continued rather than
      // restarting.
      expect(r2Headers.get("cc")).toContain(MAIL1_FROM);

      // R2 carries the full RFC 5322 References ancestry of the message it
      // answers: mail 2's own References followed by mail 2's Message-Id. The
      // connector thread holds that chain from the seed of event 1.
      expect(referencesOf(r2Headers)).toEqual([
        ...mail2References,
        MAIL2_MESSAGE_ID,
      ]);

      // R1 answered the thread opener, which carried no References of its own,
      // so its ancestry is the single opener Message-Id. Confirms the
      // full-chain path degrades to one element for a first reply rather than
      // emitting an empty or malformed References header.
      expect(referencesOf(r1Headers)).toEqual([MAIL1_MESSAGE_ID]);

      // Wait for the section to park again after event 1 so the event log
      // shows both serviced events before the structural assertions read it.
      await waitFor(
        async () =>
          inputRearmCount(
            await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId),
          ) >= 2,
        { diagnostics: env.sidecarDiagnostics },
      );

      const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);

      // Both mails ran under ONE container run. A single RunStarted consumed
      // mail 1; mail 2 opened no run of its own -- it was routed onto the
      // section's input park as event 1, not dispatched as a fresh run.
      const started = events.filter((e) => e.type === "RunStarted");
      expect(started).toHaveLength(1);
      expect(started[0]?.body["consumedDeliveryKey"]).toBe(mail1.deliveryKey);
      const consumed = started.map((e) => e.body["consumedDeliveryKey"]);
      expect(consumed).not.toContain(mail2.deliveryKey);

      // The container reached no terminal event between the mails: the
      // section kept it live, so mail 2 was serviced rather than refused as
      // terminal.
      const terminal = events.filter(
        (e) =>
          e.type === "RunCompleted" ||
          e.type === "RunFailed" ||
          e.type === "RunCancelled",
      );
      expect(terminal).toHaveLength(0);

      // Mail 2 was consumed as the input of event 1: the supervisor delivered
      // it on the section's input channel as a `signal.deliver` whose signalId
      // is mail 2's delivery key, and the child committed a SignalReceived
      // carrying it. An at-least-once redelivery can record it more than once
      // on the same input correlation; the state machine dedups by signalId,
      // so those extra commits service no extra event -- hence "at least one",
      // not "exactly one".
      const signalReceived = events.filter(
        (e) =>
          e.type === "SignalReceived" &&
          sectionInputSignalNames(events).has(e.body["signalName"]) &&
          e.body["signalId"] === mail2.deliveryKey,
      );
      expect(signalReceived.length).toBeGreaterThanOrEqual(1);

      // Two events serviced: one body run for each mail, and one section
      // input park after each body run.
      expect(
        events.flatMap((e) =>
          e.type === "ChildSpawned" ? [e.body["childRunId"]] : [],
        ),
      ).toEqual([
        sectionBodyRunId(runId, SECTION_ID, 0),
        sectionBodyRunId(runId, SECTION_ID, 1),
      ]);
      expect(inputRearmCount(events)).toBeGreaterThanOrEqual(2);

      // Each mail was serviced as exactly ONE event: exactly one delivered
      // outbound reply threads onto each inbound. A mail double-dispatched as
      // two events (the failure this test guards against) would send a second
      // reply onto the same inbound.
      const repliesThreadedOn = (inReplyTo: string): number =>
        env.hub.outboundMail.filter(
          (m) =>
            m.senderAddress === deploymentMailAddress &&
            parseHeaderSection(m.raw).headers.get("in-reply-to") === inReplyTo,
        ).length;
      expect(repliesThreadedOn(MAIL1_MESSAGE_ID)).toBe(1);
      expect(repliesThreadedOn(MAIL2_MESSAGE_ID)).toBe(1);

      // The body step of event 1 continued the conversation of event 0: its
      // request holds the request of event 0 and the reply to it, then mail 2.
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
    });

    test("a non-participant that names the thread gets its own reply and leaves the thread unchanged", async () => {
      const runId = threadRunId;
      const r2MessageId = threadLastMessageId;
      const repoId = threadRepoId;
      if (
        runId === undefined ||
        r2MessageId === undefined ||
        repoId === undefined
      ) {
        throw new Error("the two-mail thread test did not run first");
      }
      const deploymentMailAddress = deriveRunAddress({
        runId: DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });

      // --- Event 2: userC names the thread, but nobody addressed userC ----
      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: MAIL3_MESSAGE_ID,
        from: MAIL3_FROM,
        content: MAIL3_BODY,
        inReplyTo: r2MessageId,
        references: [MAIL1_MESSAGE_ID, r2MessageId],
        grants: ALL_TOOL_GRANTS,
      });

      // The reply goes to userC alone, under userC's mail.
      const r3Headers = await waitForReplyInReplyTo(
        env,
        deploymentMailAddress,
        MAIL3_MESSAGE_ID,
      );
      expect(r3Headers.get("to")).toBe(MAIL3_FROM);
      expect(r3Headers.get("cc")).toBeUndefined();

      await waitFor(
        async () =>
          inputRearmCount(
            await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId),
          ) >= 3,
        { diagnostics: env.sidecarDiagnostics },
      );

      // Each ended body run reclaimed its local storage. The `runs/`
      // directory exists because the body steps wrote there, and it keeps at
      // most the top-level run's directory, which holds no agent step here.
      const onlyTopLevelRunDir = async (): Promise<boolean> =>
        (await stepStateRunDirs(env, repoId)).every((dir) => dir === runId);
      await waitFor(onlyTopLevelRunDir, {
        diagnostics: env.sidecarDiagnostics,
      });

      // --- Event 3: userA answers R2, the thread's last message -----------
      // The passthrough did not advance the thread, so R2 is still its last
      // message, and userC is not on the reply.
      const mail4References = [MAIL1_MESSAGE_ID, r2MessageId];
      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: MAIL4_MESSAGE_ID,
        from: MAIL1_FROM,
        content: MAIL4_BODY,
        inReplyTo: r2MessageId,
        references: mail4References,
        grants: ALL_TOOL_GRANTS,
      });

      const r4Headers = await waitForReplyInReplyTo(
        env,
        deploymentMailAddress,
        MAIL4_MESSAGE_ID,
      );
      // The thread continued from the committed step state, not from the
      // body runs' reclaimed local stores.
      expect(r4Headers.get("to")).toBe(MAIL1_FROM);
      expect(r4Headers.get("cc")).toBe(MAIL2_FROM);
      expect(referencesOf(r4Headers)).toEqual([
        ...mail4References,
        MAIL4_MESSAGE_ID,
      ]);

      await waitFor(
        async () =>
          inputRearmCount(
            await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId),
          ) >= 4,
        { diagnostics: env.sidecarDiagnostics },
      );
      await waitFor(onlyTopLevelRunDir, {
        diagnostics: env.sidecarDiagnostics,
      });
    });
  },
);

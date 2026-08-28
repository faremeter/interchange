// Single-step warm-agent INTERACTIVE multi-turn conversation (INTR-480).
//
// Proves the interactive conversational mail path for a warm single-step agent
// whose step declares `triggers: "unbounded"`. Unlike the two-turn-thread
// sibling -- which holds the conversation open WITHIN one turn via `mail_wait`
// -- this agent replies to each inbound and RETURNS. The runtime re-arms the
// step on a snapshot-less `input` park after every turn, so the SECOND inbound
// mail is dispatched as turn 2 via `signal.deliver` on the SAME stable run
// (its runId is the deployment address's local part; see `deriveWorkflowRunId`
// -- "a property of the deployment, not of the trigger occurrence"), rather
// than opening a new run or being rejected as terminal.
//
// Run model. A batch step (`triggers` absent / `1`) completes on its first
// output, so a second inbound would hit a terminal run and be refused. With
// `triggers: "unbounded"` the step never self-completes: it re-arms on an input
// park (`run.ts` re-arm), and the supervisor's dispatch loop routes the next
// inbound onto that park as `signal.deliver` (turn 2) instead of `trigger.fire`
// (a fresh run). Turn 2's mail flows through the step-invoker's
// `buildInboundMessageFromMail` -> `seedInbound` connector hook exactly like
// turn 1, so the connector thread continues across the two dispatched turns and
// turn 2's reply threads onto turn-2's mail.
//
// The scripted mock drives that loop deterministically: on each turn it returns
// a plain text reply (no tool call, so the step produces an OUTPUT and the
// runtime re-arms; NO `mail_wait`, so the run does not stay inside one turn).
// Each reply is drained through the warm agent's connector reply path, which
// composes the outbound `In-Reply-To`/`To`/`Cc` from the durable connector
// thread the `seedInbound` hook advanced -- so the delivered wire headers are a
// faithful proof of which mail each turn's reply threaded onto.
//
// Assertions, read off the delivered `mail.outbound` bytes (via `persistMail`'s
// retained `raw`, parsed with `parseHeaderSection`) and the run event log:
//   (a) exactly ONE `RunStarted` for the deployment runId across both turns,
//       consuming mail 1; mail 2 opened no run of its own and the run reached
//       no terminal event between the turns (the unbounded re-arm kept it live).
//   (b) R1's wire In-Reply-To == m1 (To == userA); R2's wire In-Reply-To == m2
//       (To == userB) -- each turn's reply threaded onto that turn's mail. R2
//       also carries the FULL References ancestry [m1, r1, m2] (mail 2's own
//       References plus mail 2's Message-Id), not a truncated [m2]; R1's is the
//       single-element [m1] the opener seeds.
//   (c) mail 2 was consumed as turn 2 on the same run: a `SignalReceived` whose
//       signalId is mail 2's Message-Id, plus two input re-arms serviced.
//   (d) connector cc-accumulation: R2 cc-includes userA, the prior turn's
//       participant carried forward -- the connector thread continued across the
//       two dispatched turns rather than restarting on mail 2.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { parseHeaderSection } from "@intx/mime";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import type { WireGrantRule } from "@intx/types/grant-wire";
import { deriveRunAddress } from "@intx/workflow-deploy";
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
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  type DeployFlowEnv,
  type WorkflowRunEvent,
} from "../hub-agent/lib/deploy-flow-env";
import { singleStepMailInboxEntry } from "./fixtures/single-step-mail-inbox-agent";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_ma11500dba5eba11cafef00dba5eba55";
const STEP_ID = "step1";
const AGENT_ID = "agent-mail-interactive";

const MAIL1_MESSAGE_ID = "<mail-int-m1-7c4e21@integration.interchange>";
const MAIL1_FROM = "userone-7c4e21@integration.interchange";
const MAIL1_BODY = "Turn one opener body marker mail-int-7c4e21.";

const MAIL2_MESSAGE_ID = "<mail-int-m2-7c4e21@integration.interchange>";
const MAIL2_FROM = "usertwo-7c4e21@integration.interchange";
const MAIL2_BODY = "Turn two continuation body marker mail-int-7c4e21.";

// A plain text reply per turn: the step produces an OUTPUT (so the unbounded
// step re-arms on an input park) and calls no tool (so it never stays inside a
// single turn the way `mail_wait` would). The connector reply drain threads
// each reply from the durable connector thread seeded on that turn's mail.
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

const TENANT_ID = "tnt_single_step_mail_int";
const CALLER_PRINCIPAL_ID = "prn_single_step_mail_int";
const DEFINITION_ASSET_ID = "ast_single_step_mail_int_wf";

let env: DeployFlowEnv;
let h: TestDb;

beforeAll(async () => {
  if (!harnessDbEnvAvailable()) return;
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
    name: "single-step-mail-int-wf",
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
    { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
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

/** Count the input-park re-arms serviced: one `SignalAwaited{parkKind:input}`
 * is minted per completed turn of an unbounded step. */
function inputRearmCount(events: WorkflowRunEvent[]): number {
  return events.filter(
    (e) => e.type === "SignalAwaited" && e.body["parkKind"] === "input",
  ).length;
}

describe.skipIf(!harnessDbEnvAvailable())(
  "warm single-step unbounded agent services two inbound mails as re-armed turns on one run",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("mail 2 is dispatched as turn 2 via signal.deliver on the same run and its reply threads onto mail 2", async () => {
      const deploymentMailAddress = deriveRunAddress({
        runId: DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });

      const inferenceSource: InferenceSource = {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${env.inference.server.port}`,
        apiKey: "sk-mock",
        model: "mock-model",
      };

      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentId: `${DEPLOYMENT_ID}`,
        tenantId: "tenant-1",
        principalId: "prin_integration-1",
        agentAddress: deploymentMailAddress,
        systemPrompt: "Fallback prompt (overridden per step by the definition)",
        tools: [],
        grants: [],
        sources: [inferenceSource],
        defaultSource: "anthropic:mock-model",
      };

      const entryModule = singleStepMailInboxEntry({
        stepId: STEP_ID,
        systemPrompt: "You are the single-step mail-inbox agent.",
        address: deploymentMailAddress,
        agentId: AGENT_ID,
        triggers: "unbounded",
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
        sources: { [STEP_ID]: [inferenceSource] },
      });
      expect(handle.publicKey).toBeTruthy();

      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      // --- Turn 1: opener from userA (trigger.fire) ---------------------
      const mail1 = await fireMailTrigger(env, deploymentMailAddress, {
        messageId: MAIL1_MESSAGE_ID,
        from: MAIL1_FROM,
        content: MAIL1_BODY,
        grants: ALL_TOOL_GRANTS,
      });
      expect(mail1.messageId).toBe(MAIL1_MESSAGE_ID);

      const runId = await waitForFirstRunId(env, handle.workflowRunRepoId, {
        diagnostics: env.sidecarDiagnostics,
        timeoutMs: 20_000,
      });

      const r1Headers = await waitForReplyInReplyTo(
        env,
        deploymentMailAddress,
        MAIL1_MESSAGE_ID,
      );
      expect(r1Headers.get("in-reply-to")).toBe(MAIL1_MESSAGE_ID);
      expect(r1Headers.get("to")).toBe(MAIL1_FROM);
      // R1's own Message-Id anchors mail 2's threading, exactly as a mail
      // client continuing the conversation would set In-Reply-To.
      const r1MessageId = r1Headers.get("message-id");
      if (r1MessageId === undefined) {
        throw new Error("R1 carried no Message-Id");
      }

      // The step produced its turn-1 output and the unbounded runtime re-armed
      // on a fresh input park. Wait for that re-arm before firing mail 2 so the
      // second mail lands on the signal.deliver rail (turn 2), not a race with
      // an in-flight turn. A still-open run here also proves turn 1 did NOT go
      // terminal -- a batch step would have completed and refused mail 2.
      await waitFor(
        async () =>
          inputRearmCount(
            await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId),
          ) >= 1,
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      // --- Turn 2: continuation from userB, dispatched as signal.deliver ---
      // Reference the thread root (m1) and R1 so the connector router treats
      // mail 2 as a continuation of the SAME thread rather than a new one.
      const mail2 = await fireMailTrigger(env, deploymentMailAddress, {
        messageId: MAIL2_MESSAGE_ID,
        from: MAIL2_FROM,
        content: MAIL2_BODY,
        inReplyTo: r1MessageId,
        references: [MAIL1_MESSAGE_ID, r1MessageId],
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
      // Connector cc-accumulation: userA, turn 1's participant, is carried
      // forward onto turn 2's reply. This only happens if the SAME connector
      // thread advanced across the two dispatched turns (mail 2 seeded as a
      // continuation), proving the thread continued rather than restarting.
      expect(r2Headers.get("cc")).toContain(MAIL1_FROM);

      // R2 carries the FULL RFC 5322 References ancestry, not just the parent
      // it answers (INTR-480). Mail 2 arrived with References [m1, r1], so a
      // reply threaded onto it ships References [m1, r1, m2] -- mail 2's own
      // References chain plus mail 2's Message-Id. The reply path builds this
      // complete chain by looking the parent up in the run mailbox rather than
      // truncating to the single In-Reply-To element.
      const r2References = (r2Headers.get("references") ?? "")
        .split(/\s+/)
        .filter((id) => id.length > 0);
      expect(r2References).toEqual([
        MAIL1_MESSAGE_ID,
        r1MessageId,
        MAIL2_MESSAGE_ID,
      ]);

      // R1 answered the thread opener, which carried no References of its own,
      // so its ancestry is the single opener Message-Id. Confirms the
      // full-chain path degrades to one element for a first reply rather than
      // emitting an empty or malformed References header.
      const r1References = (r1Headers.get("references") ?? "")
        .split(/\s+/)
        .filter((id) => id.length > 0);
      expect(r1References).toEqual([MAIL1_MESSAGE_ID]);

      // Wait for turn 2 to complete its own re-arm so the event log reflects
      // both serviced turns before the structural assertions read it.
      await waitFor(
        async () =>
          inputRearmCount(
            await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId),
          ) >= 2,
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);

      // Both turns ran under ONE run. A single RunStarted consumed mail 1; mail
      // 2 opened no run of its own -- it was routed onto the input park as turn
      // 2, not dispatched as a fresh run.
      const started = events.filter((e) => e.type === "RunStarted");
      expect(started).toHaveLength(1);
      expect(started[0]?.body["consumedMessageId"]).toBe(MAIL1_MESSAGE_ID);
      const consumed = started.map((e) => e.body["consumedMessageId"]);
      expect(consumed).not.toContain(MAIL2_MESSAGE_ID);

      // The run reached no terminal event between the turns: the unbounded
      // re-arm kept it live, so mail 2 was serviced rather than refused as
      // terminal.
      const terminal = events.filter(
        (e) =>
          e.type === "RunCompleted" ||
          e.type === "RunFailed" ||
          e.type === "RunCancelled",
      );
      expect(terminal).toHaveLength(0);

      // Mail 2 was consumed as turn 2's input: the supervisor delivered it on
      // the input channel as a `signal.deliver` whose signalId is mail 2's
      // Message-Id, and the child committed a SignalReceived carrying it. An
      // at-least-once redelivery can record it more than once on the same input
      // correlation; the state machine dedups by signalId, so those extra
      // commits service no extra turn -- hence "at least one", not "exactly one".
      const signalReceived = events.filter(
        (e) =>
          e.type === "SignalReceived" &&
          e.body["signalId"] === MAIL2_MESSAGE_ID,
      );
      expect(signalReceived.length).toBeGreaterThanOrEqual(1);

      // Two turns serviced: one input re-arm per completed turn.
      expect(inputRearmCount(events)).toBeGreaterThanOrEqual(2);

      // Each mail was serviced as exactly ONE turn: exactly one delivered
      // outbound reply threads onto each inbound. A mail double-dispatched as
      // two turns (the failure this test guards against) would drain a second
      // reply onto the same inbound.
      const repliesThreadedOn = (inReplyTo: string): number =>
        env.hub.outboundMail.filter(
          (m) =>
            m.senderAddress === deploymentMailAddress &&
            parseHeaderSection(m.raw).headers.get("in-reply-to") === inReplyTo,
        ).length;
      expect(repliesThreadedOn(MAIL1_MESSAGE_ID)).toBe(1);
      expect(repliesThreadedOn(MAIL2_MESSAGE_ID)).toBe(1);
    });
  },
);

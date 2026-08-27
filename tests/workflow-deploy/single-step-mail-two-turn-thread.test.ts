// Single-step warm-agent two-turn threaded conversation (INTR-480).
//
// Proves that a warm single-step agent conducts a two-exchange conversation
// inside ONE run and threads each reply onto the message it answers --
// observed on the delivered wire bytes.
//
// Run model. A workflow deployment has exactly ONE addressable top-level run
// (its runId is the deployment's mail-address local part; see
// `deriveWorkflowRunId` -- "the runId is a property of the deployment, not of
// the individual trigger occurrence"). A second inbound mail is therefore NOT
// a second dispatched run; the conversation is held open WITHIN the one run by
// `mail_wait`. The agent replies to the first message, waits for the next
// inbound, and replies to that -- all in a single run whose connector/agent
// state is warm across the exchange.
//
// The scripted mock provider drives that loop deterministically:
//   turn 0: mail_send a reply to mail 1, threaded on m1 (inReplyTo = m1).
//   turn 1: mail_wait for the next sender (userB) -- blocks.
//   turn 2 (after mail 2 arrives): mail_send a reply to mail 2, threaded on m2.
//   turn 3: a closing text turn, which ends the run.
// The reply message-ids the agent threads on (m1, m2) are the ones THIS test
// fired, so every scripted tool input is known up front.
//
// Assertions, read off the delivered `mail.outbound` bytes (via `persistMail`'s
// retained `raw`, parsed with `parseHeaderSection`):
//   * A reply threaded onto mail 1: In-Reply-To == m1, To == userA.
//   * A reply threaded onto mail 2: In-Reply-To == m2, To == userB.
//   * The whole exchange ran under ONE run: a single RunStarted, consuming
//     m1; mail 2 never opened a RunStarted of its own -- it was consumed
//     mid-run by mail_wait, the continuation. The run completes well within
//     mail_wait's timeout, so its resolution came from mail 2's arrival, not a
//     wait expiry.

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
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { singleStepMailInboxEntry } from "./fixtures/single-step-mail-inbox-agent";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_ma11300dba5eba11cafef00dba5eba33";
const STEP_ID = "step1";
const AGENT_ID = "agent-mail-two-turn";

const MAIL1_MESSAGE_ID = "<mail-2t-m1-9b7e10@integration.interchange>";
const MAIL1_FROM = "userone-9b7e10@integration.interchange";
const MAIL1_BODY = "Turn one opener body marker mail-2t-9b7e10.";
const REPLY1_BODY = "Reply to the opener, threaded on mail one.";

const MAIL2_MESSAGE_ID = "<mail-2t-m2-9b7e10@integration.interchange>";
const MAIL2_FROM = "usertwo-9b7e10@integration.interchange";
const MAIL2_BODY = "Turn two continuation body marker mail-2t-9b7e10.";
const REPLY2_BODY = "Reply to the continuation, threaded on mail two.";

// mail_wait's own timeout. Kept far above a healthy notify-wake so that a run
// completing inside the (shorter) waitForWorkflowRunComplete budget proves the
// wait resolved from mail 2's arrival rather than from a wait expiry.
const MAIL_WAIT_TIMEOUT_S = 90;

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

const TENANT_ID = "tnt_single_step_mail_2t";
const CALLER_PRINCIPAL_ID = "prn_single_step_mail_2t";
const DEFINITION_ASSET_ID = "ast_single_step_mail_2t_wf";

let env: DeployFlowEnv;
let h: TestDb;

beforeAll(async () => {
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
    name: "single-step-mail-2t-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv({
    inferenceScriptedTurns: [
      // turn 0: reply to mail 1, threaded on m1.
      {
        toolUse: {
          name: "mail_send",
          input: {
            to: MAIL1_FROM,
            content: REPLY1_BODY,
            inReplyTo: MAIL1_MESSAGE_ID,
          },
        },
      },
      // turn 1: wait for the next inbound from userB -- held open mid-run.
      {
        toolUse: {
          name: "mail_wait",
          input: { query: { from: MAIL2_FROM }, timeout: MAIL_WAIT_TIMEOUT_S },
        },
      },
      // turn 2: reply to mail 2, threaded on m2.
      {
        toolUse: {
          name: "mail_send",
          input: {
            to: MAIL2_FROM,
            content: REPLY2_BODY,
            inReplyTo: MAIL2_MESSAGE_ID,
          },
        },
      },
      // turn 3: closing text turn ends the run.
      { text: "conversation complete" },
    ],
  });
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

/**
 * Wait for a delivered outbound reply from `sender` whose parsed `In-Reply-To`
 * header equals `inReplyTo`, then return its parsed headers.
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

describe.skipIf(!harnessDbEnvAvailable())(
  "warm single-step agent threads a two-turn conversation inside one run",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("R1 threads on mail 1, mail 2 continues the run via mail_wait, and R2 threads on mail 2", async () => {
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

      // --- Turn 1: opener from userA ------------------------------------
      const mail1 = await fireMailTrigger(env, deploymentMailAddress, {
        messageId: MAIL1_MESSAGE_ID,
        from: MAIL1_FROM,
        content: MAIL1_BODY,
        grants: ALL_TOOL_GRANTS,
      });
      expect(mail1.messageId).toBe(MAIL1_MESSAGE_ID);

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

      // The agent's next scripted turn is mail_wait (issued once the mail_send
      // result lands: a second inference request). Wait for that, then fire
      // mail 2 so it arrives while the run is blocked in the wait.
      await waitFor(() => env.inference.requests.length >= 2, {
        timeoutMs: 20_000,
        diagnostics: env.sidecarDiagnostics,
      });
      await new Promise((r) => setTimeout(r, 300));

      // --- Turn 2: continuation from userB, delivered mid-run -----------
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

      // --- One run held open across both exchanges ----------------------
      const runId = await waitForFirstRunId(env, handle.workflowRunRepoId, {
        diagnostics: env.sidecarDiagnostics,
        timeoutMs: 20_000,
      });
      // Completing inside this budget (well under mail_wait's 90s timeout)
      // proves the wait resolved from mail 2's arrival, not a timeout expiry.
      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        runId,
        { timeoutMs: 40_000, diagnostics: env.sidecarDiagnostics },
      );
      const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
      if (terminal.type !== "RunCompleted") {
        const failed = events.find(
          (e) => e.type === "StepFailed" || e.type === "RunFailed",
        );
        throw new Error(
          `expected RunCompleted, got ${terminal.type}: ${JSON.stringify(failed?.body)}\n${env.sidecarDiagnostics()}`,
        );
      }

      // The conversation ran under ONE run: a single RunStarted consuming mail
      // 1. Mail 2 opened no run of its own -- it was consumed mid-run by
      // mail_wait, the continuation, not dispatched as a second turn.
      const started = events.filter((e) => e.type === "RunStarted");
      expect(started).toHaveLength(1);
      expect(started[0]?.body["consumedMessageId"]).toBe(MAIL1_MESSAGE_ID);
      const consumed = started.map((e) => e.body["consumedMessageId"]);
      expect(consumed).not.toContain(MAIL2_MESSAGE_ID);
    });
  },
);

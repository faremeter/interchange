// Single-step INBOUND mail-loop integration test (INTR-480).
//
// The proof that the warm single-step agent's inbound mail surface is LIVE:
// an arrived mail is visible to the agent's `mail_search` read tool WITHIN the
// same turn, and the agent's reply drains back out as a signed outbound send.
// Deploys a warm single-step agent (source-ref lineage) that PINS the real
// `@intx/tools-mail` bundle -- so it carries `mail_search` / `mail_read` /
// `mail_wait` -- against the real hub + real sidecar subprocess + a mock
// inference fixture, fires a mail, and asserts the loop.
//
// The mock inference server drives `mail_search` deterministically: the first
// turn exposing the tool returns a `tool_use` calling `mail_search`; once the
// child runs it and the tool_result lands, the mock replies with
// `MAILSAW:<the tool_result content>`. Because the supervisor eager-commits an
// arrived message into the deployment's substrate INBOX BEFORE it wakes the
// dispatch that drives the turn (the mailbox commit is awaited ahead of
// `wakeDispatch`), the agent's `mail_search` -- backed by the now-wired
// supervisor transport inbound -- opens a committed snapshot that already
// holds the message. The reply therefore echoes the fired mail's own identity
// (its Message-Id and sender), the load-bearing proof that the delivered
// inbound was visible to the read tool in the same turn.
//
// Against the pre-activation behaviour (the transport's inbound throws "not
// wired"), `mail_search` would reject rather than return the message, the tool
// call would error, and the reply would not carry the fired mail's identity --
// this test fails there.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

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
// A single-agent run identity: `run_` + a hex-shaped local part. A
// single-step deployment is warm-kept, so this deployment exercises the warm
// agent path (durable conversation, connector reply drain, inbound surface).
const DEPLOYMENT_ID = "run_ma11100dba5eba11cafef00dba5eba11";
const STEP_ID = "step1";
const AGENT_ID = "agent-mail-inbox";

// The inbound mail's identity. The reply echoes the `mail_search` result, so
// these tokens appearing in the committed reply prove the read tool saw the
// delivered message in the same turn. Chosen distinctive so the substring
// assertions are unambiguous.
const INBOUND_MESSAGE_ID = "<mail-loop-inbound-7f3a91@integration.interchange>";
const INBOUND_FROM = "mailer-7f3a91@integration.interchange";
const INBOUND_BODY = "Inbound loop body marker mail-loop-7f3a91.";

// The mock echoes the tool_result behind this prefix, so the reply is
// `MAILSAW:<mail_search output JSON>`.
const RESULT_PREFIX = "MAILSAW:";

// The mail read tools the model may call, delivered as per-run grants ahead of
// the trigger. `fireMailTrigger` does not materialize run grants the way the
// production route does, so feeding them here reproduces the per-run delivery
// the child's authorize resolves each `tool:<name>` invoke against. Without the
// grant the tool call is denied and the turn cannot read the mailbox.
const READ_TOOL_NAMES = ["mail_search", "mail_read", "mail_wait"] as const;
const READ_TOOL_GRANTS: WireGrantRule[] = READ_TOOL_NAMES.map((name) => ({
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

// The definition's own tenant, the caller principal that creates the
// definition asset, and the `workflow`-kind asset the frozen definition
// projects over. The install/approve freeze and the anchor `workflow_run`
// insert both write against these, so they must exist in the real DB before
// the deploy runs.
const TENANT_ID = "tnt_single_step_mail_loop";
const CALLER_PRINCIPAL_ID = "prn_single_step_mail_loop";
const DEFINITION_ASSET_ID = "ast_single_step_mail_loop_wf";

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
    name: "single-step-mail-loop-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  // Drive `mail_search` on the first turn exposing it, then reply with the
  // tool_result content behind RESULT_PREFIX. `approvalToolCall` re-issues the
  // call until the history carries its result and replies once it lands; for an
  // allowed (non-suspending) tool that is one tool_use followed by the reply.
  env = await startDeployFlowEnv({
    inferenceApprovalToolCall: {
      toolName: "mail_search",
      input: {},
      resultPrefix: RESULT_PREFIX,
    },
  });
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

describe.skipIf(!harnessDbEnvAvailable())(
  "single-step inbound mail loop on the warm agent",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("mail_search sees the delivered inbound and the reply drains out signed", async () => {
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

      // approve-probed: approve exactly the surface the probe discovered (the
      // inference source, the director, the deployment mail address + send, and
      // every `tool:mail_*` the pinned bundle declares), so the gate freezes the
      // full mail toolset without enumerating each grant here.
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

      const workflowRunRepoId = handle.workflowRunRepoId;

      // The source-ref frame round-trips through the real sidecar subprocess
      // (index the pack, check out the pinned subtree, register the address),
      // so routability is asynchronous. Wait for it before firing the trigger.
      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      // Fire the inbound mail with the known identity + body, plus the per-run
      // grants the read tools authorize against.
      const inbound = await fireMailTrigger(env, deploymentMailAddress, {
        messageId: INBOUND_MESSAGE_ID,
        from: INBOUND_FROM,
        content: INBOUND_BODY,
        grants: READ_TOOL_GRANTS,
      });
      expect(inbound.messageId).toBe(INBOUND_MESSAGE_ID);

      const runId = await waitForFirstRunId(env, workflowRunRepoId, {
        diagnostics: env.sidecarDiagnostics,
        timeoutMs: 20_000,
      });

      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        runId,
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );
      if (terminal.type !== "RunCompleted") {
        const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
        const failed = events.find(
          (e) => e.type === "StepFailed" || e.type === "RunFailed",
        );
        throw new Error(
          `expected RunCompleted, got ${terminal.type}: ${JSON.stringify(failed?.body)}\n${env.sidecarDiagnostics()}`,
        );
      }

      const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
      const startedBody = events.find((e) => e.type === "RunStarted")?.body;
      if (startedBody === undefined) throw new Error("missing RunStarted");
      // Step-input delivery still drives the turn: the run consumed the mail
      // this test fired (AC5).
      expect(startedBody["consumedMessageId"]).toBe(inbound.messageId);

      // AC1 + AC2: the agent's `mail_search` returned the fired inbound within
      // the same turn. The mock replied with the tool_result content behind
      // RESULT_PREFIX, so the committed reply carries the delivered message's
      // own Message-Id and sender -- which could only come from the read tool
      // opening the committed INBOX. A "not wired" inbound surface would have
      // thrown inside `mail_search` instead, leaving the reply without them.
      const reply = readStepReply(stepCompletedBody(events));
      expect(reply.startsWith(RESULT_PREFIX)).toBe(true);
      expect(reply).toContain("mail-loop-inbound-7f3a91");
      expect(reply).toContain(INBOUND_FROM);

      // AC3: the reply drained back out as a signed outbound send. The warm
      // agent's connector reply drain composed a threaded reply from the
      // seeded inbound and sent it through the outbound bridge; the supervisor
      // signed it as the deployment address and the hub recorded the delivered
      // `mail.outbound` frame. A frame reaches `outboundMail` only after the
      // sidecar signed and delivered it, so its presence proves the signed
      // outbound composed end to end and is addressed to the original sender.
      await waitFor(
        () =>
          env.hub.outboundMail.some(
            (m) =>
              m.senderAddress === deploymentMailAddress &&
              m.recipients.includes(INBOUND_FROM),
          ),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );
      const replyOut = env.hub.outboundMail.find(
        (m) =>
          m.senderAddress === deploymentMailAddress &&
          m.recipients.includes(INBOUND_FROM),
      );
      expect(replyOut).toBeDefined();
    });
  },
);

/** Find the single step's `StepCompleted` event body. */
function stepCompletedBody(
  events: { type: string; body: Record<string, unknown> }[],
): Record<string, unknown> {
  const stepCompleted = events.find(
    (e) => e.type === "StepCompleted" && e.body["stepId"] === STEP_ID,
  );
  if (stepCompleted === undefined) {
    throw new Error("missing StepCompleted for the single step");
  }
  return stepCompleted.body;
}

/**
 * Extract the agent's reply string from a `StepCompleted` event body. The
 * runtime records the step output through the blob substrate; a small
 * `{ reply, turn }` output inlines as `inline:<json>`, so the reply is
 * recovered by parsing the JSON after the `inline:` prefix.
 */
function readStepReply(body: Record<string, unknown>): string {
  const output = body["output"];
  if (typeof output !== "object" || output === null || !("ref" in output)) {
    throw new Error(
      `StepCompleted output is not a { ref } record: ${JSON.stringify(output)}`,
    );
  }
  const ref: unknown = output.ref;
  if (typeof ref !== "string") {
    throw new Error(`StepCompleted output ref is not a string: ${String(ref)}`);
  }
  const INLINE_PREFIX = "inline:";
  if (!ref.startsWith(INLINE_PREFIX)) {
    throw new Error(
      `expected an inline output ref for the small step output, got ${ref}`,
    );
  }
  const parsed: unknown = JSON.parse(ref.slice(INLINE_PREFIX.length));
  if (typeof parsed !== "object" || parsed === null || !("reply" in parsed)) {
    throw new Error(
      `step output does not carry a reply field: ${JSON.stringify(parsed)}`,
    );
  }
  const reply: unknown = parsed.reply;
  if (typeof reply !== "string") {
    throw new Error(
      `step output reply is not a string: ${JSON.stringify(parsed)}`,
    );
  }
  return reply;
}

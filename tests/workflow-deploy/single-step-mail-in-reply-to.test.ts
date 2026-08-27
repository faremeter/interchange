// Single-step warm-agent In-Reply-To wire proof (INTR-480).
//
// Proves that the warm single-step agent's auto-reply carries an
// `In-Reply-To` header whose value EQUALS the inbound message's `Message-Id`
// -- observed on the delivered wire bytes, not inferred from a hub mail row.
//
// Obstacle and how this test observes the wire
// --------------------------------------------
// The deploy-flow harness's mock hub records outbound mail through its
// `persistMail` lookup, which the wire layer calls with the full signed MIME
// (`raw`) it base64-decoded from the delivered `mail.outbound` frame. The
// harness retains that `raw` on `env.hub.outboundMail`, so this test reads the
// reply's real `In-Reply-To`/`Message-ID` headers with `parseHeaderSection`
// (from `@intx/mime`). The bytes are exactly what the sidecar signed and
// delivered -- the in-memory hub transport verified the sender's signature to
// accept them -- so the header read is a faithful on-the-wire observation, not
// a harness-side reconstruction.
//
// Why the reply threads onto the inbound: the warm agent's connector thread
// opens on the first inbound (its `Message-Id` becomes the thread's
// `lastMessageId`), and the reply drain composes the outbound `inReplyTo` from
// that thread state. The outbound send path stamps it into the wire
// `In-Reply-To` header. So the reply's `In-Reply-To` must equal the inbound's
// `Message-Id`; this test fires a mail with a distinctive id and asserts that
// exact equality on the delivered bytes.

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
const DEPLOYMENT_ID = "run_ma11200dba5eba11cafef00dba5eba22";
const STEP_ID = "step1";
const AGENT_ID = "agent-mail-in-reply-to";

const INBOUND_MESSAGE_ID = "<mail-irt-inbound-c41d02@integration.interchange>";
const INBOUND_FROM = "mailer-c41d02@integration.interchange";
const INBOUND_BODY = "In-Reply-To wire proof body marker mail-irt-c41d02.";

const RESULT_PREFIX = "MAILSAW:";

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

const TENANT_ID = "tnt_single_step_mail_irt";
const CALLER_PRINCIPAL_ID = "prn_single_step_mail_irt";
const DEFINITION_ASSET_ID = "ast_single_step_mail_irt_wf";

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
    name: "single-step-mail-irt-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

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
  "warm single-step agent stamps the inbound Message-Id as the reply In-Reply-To",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("the delivered reply's In-Reply-To equals the inbound Message-Id on the wire", async () => {
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

      const inbound = await fireMailTrigger(env, deploymentMailAddress, {
        messageId: INBOUND_MESSAGE_ID,
        from: INBOUND_FROM,
        content: INBOUND_BODY,
        grants: READ_TOOL_GRANTS,
      });
      expect(inbound.messageId).toBe(INBOUND_MESSAGE_ID);

      // Let the run finish so a failure surfaces its StepFailed/RunFailed
      // body rather than timing out on the outbound wait below.
      const runId = await waitForFirstRunId(env, handle.workflowRunRepoId, {
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

      // The reply drained back out as a signed outbound send; wait for the
      // delivered `mail.outbound` frame the hub retained (raw signed MIME).
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
      if (replyOut === undefined) throw new Error("no delivered reply frame");

      // Read the delivered reply's real headers off the wire bytes. The
      // load-bearing assertion: the reply's In-Reply-To EQUALS the inbound's
      // own Message-Id (exact string, angle brackets and all), proving the
      // auto-reply threaded onto the message this test fired.
      const { headers } = parseHeaderSection(replyOut.raw);
      expect(headers.get("in-reply-to")).toBe(INBOUND_MESSAGE_ID);
      // The reply is addressed back to the inbound sender and carries the
      // inbound id in its References chain -- corroborating threading, not a
      // coincidental header collision.
      expect(headers.get("from")).toBe(deploymentMailAddress);
      expect(headers.get("to")).toBe(INBOUND_FROM);
      expect(headers.get("references")).toContain(INBOUND_MESSAGE_ID);
      // The reply has its own distinct Message-Id (it is a new message, not an
      // echo of the inbound), so In-Reply-To pointing at the inbound is a
      // genuine threading link.
      const replyMessageId = headers.get("message-id");
      expect(replyMessageId).toBeDefined();
      expect(replyMessageId).not.toBe(INBOUND_MESSAGE_ID);
    });
  },
);

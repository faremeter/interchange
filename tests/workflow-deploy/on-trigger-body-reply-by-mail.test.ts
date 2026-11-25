// An agent step inside an `onTrigger` section body that sets `replyByMail`
// replies on the mail that triggered the body.
//
// A section body is a spawned child run, so its step agent is built by the
// spawned-child step invoker, not the top-level one. That invoker builds each
// step over the step's own durable store and sends the reply of a turn that
// answers mail through the outbound bridge. This test reads the delivered
// reply off the wire: the signed MIME the mock hub retained.
//
// Harness justification: SPAWN-REAL. Real hub, real sidecar subprocess, real
// workflow-process child driving `runOnTrigger` with the production
// suspendable-child seam, and a real agent inside the body through the
// spawned-child invoker, driven against the mock inference fixture.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { parseHeaderSection } from "@intx/mime";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
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
  startDeployFlowEnv,
  waitFor,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { onTriggerBodyEntry } from "./fixtures/on-trigger-body";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const SECTION_ID = "section";
const BODY_STEP_ID = "work";

const TENANT_ID = "tnt_on_trigger_body_reply_by_mail";
const CALLER_PRINCIPAL_ID = "prn_on_trigger_body_reply_by_mail";
const DEPLOYMENT_ID = "run_on-trigger-body-reply-by-mail-1";
const ASSET_ID = "ast_on_trigger_body_reply_by_mail_wf";

const INBOUND_FROM = "asker@integration.interchange";
const INBOUND_MESSAGE_ID =
  "<on-trigger-body-reply-by-mail-1@integration.interchange>";
const REPLY_TEXT = "BODY-REPLY-BY-MAIL";

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
    id: ASSET_ID,
    tenantId: TENANT_ID,
    kind: "workflow",
    name: ASSET_ID,
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv({
    inferenceScriptedTurns: [{ text: REPLY_TEXT }],
  });
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

describe.skipIf(!harnessDbEnvAvailable())(
  "an onTrigger body agent that sets replyByMail",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("replies on the mail that triggered the body", async () => {
      const deploymentMailAddress = deriveRunAddress({
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
        agentAddress: deploymentMailAddress,
        sources: [inferenceSource],
        defaultSource: "anthropic:mock-model",
      };

      await deployWorkflowSourceForTest(env, {
        entryModule: onTriggerBodyEntry({
          address: deploymentMailAddress,
          sectionId: SECTION_ID,
          body: {
            variant: "agent",
            stepId: BODY_STEP_ID,
            systemPrompt: "You are the onTrigger body agent; answer the mail.",
            agentId: "agent-on-trigger-body-reply-by-mail",
            replyByMail: true,
          },
          workflowId: `wf_${DEPLOYMENT_ID}`,
          bodyWorkflowId: `wf_${DEPLOYMENT_ID}_body`,
        }),
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: ASSET_ID,
        anchorRunId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: deploymentMailAddress,
        approvals: "approve-probed",
        config,
        sources: { [SECTION_ID]: [inferenceSource] },
      });
      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { diagnostics: env.sidecarDiagnostics },
      );

      await fireMailTrigger(env, deploymentMailAddress, {
        from: INBOUND_FROM,
        messageId: INBOUND_MESSAGE_ID,
        content: "Please answer this mail.",
      });

      const isReply = (m: DeployFlowEnv["hub"]["outboundMail"][number]) =>
        m.senderAddress === deploymentMailAddress &&
        m.recipients.includes(INBOUND_FROM);
      await waitFor(() => env.hub.outboundMail.some(isReply), {
        diagnostics: env.sidecarDiagnostics,
      });
      const replies = env.hub.outboundMail.filter(isReply);
      expect(replies).toHaveLength(1);
      const reply = replies[0];
      if (reply === undefined) throw new Error("no delivered reply frame");

      const { headers } = parseHeaderSection(reply.raw);
      expect(headers.get("from")).toBe(deploymentMailAddress);
      expect(headers.get("to")).toBe(INBOUND_FROM);
      expect(headers.get("in-reply-to")).toBe(INBOUND_MESSAGE_ID);
      expect(headers.get("references")).toContain(INBOUND_MESSAGE_ID);
      // The delivered bytes carry the model's reply text.
      expect(new TextDecoder().decode(reply.raw)).toContain(REPLY_TEXT);
    });
  },
);

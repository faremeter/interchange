// Multi-step signed outbound send regression test.
//
// A genuine multi-step (2+ step) workflow deployment must register a
// signing identity for its deployment mail address on the host transport,
// exactly as a single-step deployment does. Every step of a multi-step
// deployment signs its outbound mail as the ONE deployment-wide address
// (`<anchorRunId>@<domain>`), so if that address is not registered a
// step's `env.transport.send` rejects with "not registered", the step
// fails, and the run fails.
//
// This deploys a two-step workflow BY SOURCE-REF whose sending step's agent
// carries the inline `mail_send` tool from the `mail-tool.ts` fixture in its
// transport variant (the mock inference drives the tool call on the first
// request that exposes it). The tool routes through the real outbound chain --
// supervisor-backed transport -> outbound bridge -> `outbound.message` IPC ->
// supervisor `sendOutbound` -> host transport SIGNED send -- so the send
// reaches the host transport as the deployment address. The sidecar forwards
// the delivered `mail.outbound` frame to the hub for persistence, where the
// fixture captures its signing sender. A captured frame whose sender is the
// deployment address is a load-bearing proof that the address held a
// registered signing identity; a registration gap would reject the send inside
// the step and forward no frame.
//
// The sender is a step of a multi-step deployment (not a single-step head),
// so this covers the deployment-scoped registration the single-step path
// already had and the multi-step path lacked.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import type { WireGrantRule } from "@intx/types/grant-wire";
import { deriveRunAddress, type ApprovalSet } from "@intx/workflow-deploy";
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
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { MAIL_TOOL_NAME } from "./fixtures/mail-tool";
import { twoStepMailToolEntry } from "./fixtures/two-step-mail-tool";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_multistep-signed-send-1";
// The first step in `stepOrder`; the mock drives its inference to call the
// mail tool, so it is the step that performs the signed send.
const SENDER_STEP_ID = "send";
const TAIL_STEP_ID = "tail";

const GRANTED_RESOURCE = `tool:${MAIL_TOOL_NAME}`;
const SENTINEL_FILENAME = "multistep-signed-send-receipt.txt";

// The run's tool grant, delivered per run via the `run.grants` frame the
// trigger sends, authorizing the transport-backed tool call in the child.
const GRANTED_RULE: WireGrantRule = {
  id: "grant-tool-invoke",
  resource: GRANTED_RESOURCE,
  action: "invoke",
  effect: "allow",
  origin: "creator",
  conditions: null,
  expiresAt: null,
  roleId: null,
  principalId: null,
};

// The definition's own tenant, the caller principal that creates the
// definition asset, and the `workflow`-kind asset the frozen definition
// projects over. The install/approve freeze and the anchor `workflow_run`
// insert both write against these, so they must exist in the real DB before
// the deploy runs.
const TENANT_ID = "tnt_multistep_signed_send";
const CALLER_PRINCIPAL_ID = "prn_multistep_signed_send";
const DEFINITION_ASSET_ID = "ast_multistep_signed_send_wf";

let env: DeployFlowEnv;
let h: TestDb;
let deploymentMailAddress: string;

beforeAll(async () => {
  if (!harnessDbEnvAvailable()) return;
  deploymentMailAddress = deriveRunAddress({
    runId: DEPLOYMENT_ID,
    domain: DEPLOYMENT_DOMAIN,
  });

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
    name: "multistep-signed-send-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  // The inline `mail_send` tool in its transport variant sends through the
  // real outbound chain and sentinels on receipt; `inferenceToolCall` drives
  // the model to call it. The send targets the deployment's own address -- a
  // local, registered recipient once the deployment identity is on the
  // transport -- so the signed send delivers without a remote leg.
  env = await startDeployFlowEnv({
    inferenceToolCall: {
      toolName: MAIL_TOOL_NAME,
      input: { to: deploymentMailAddress, body: SENTINEL_FILENAME },
    },
  });
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

describe.skipIf(!harnessDbEnvAvailable())(
  "multi-step signed outbound send",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("a step of a multi-step deployment signs and sends outbound mail", async () => {
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

      const operatorApprovals: ApprovalSet = new Set<string>([
        "inference.source:anthropic:mock-model",
        "director:@intx/agent/default",
        `mail.address:${deploymentMailAddress}`,
        `mail.send:${DEPLOYMENT_DOMAIN}`,
        `tool:${MAIL_TOOL_NAME}`,
      ]);

      const entryModule = twoStepMailToolEntry({
        variant: "transport",
        sendId: SENDER_STEP_ID,
        tailId: TAIL_STEP_ID,
        address: deploymentMailAddress,
        sendSystemPrompt: "You are the sending step agent.",
        tailSystemPrompt: "You are the trailing step agent.",
      });

      const handle = await deployWorkflowSourceForTest(env, {
        entryModule,
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: deploymentMailAddress,
        approvals: operatorApprovals,
        config,
        // A per-step source chain for EVERY step in the multi-step workflow.
        sources: {
          [SENDER_STEP_ID]: [inferenceSource],
          [TAIL_STEP_ID]: [inferenceSource],
        },
      });
      expect(handle.publicKey).toBeTruthy();

      const workflowRunRepoId = handle.workflowRunRepoId;

      // The source-ref frame round-trips through the real sidecar subprocess,
      // so routability is asynchronous. Wait for it before firing the trigger.
      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<multistep-signed-send-1@integration.interchange>",
        grants: [GRANTED_RULE],
      });

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
      expect(terminal.type).toBe("RunCompleted");

      // The proof of the fix: the sidecar signed and delivered a `mail.outbound`
      // frame whose SIGNING SENDER is the deployment mail address. A multi-step
      // step signs its outbound sends as that one deployment address, so the
      // frame reaches the hub only when the address holds a registered signing
      // identity on the host transport. Without the registration the send throws
      // "not registered" inside the step and no frame is ever forwarded, leaving
      // `outboundMail` empty.
      await waitFor(
        () =>
          env.hub.outboundMail.some(
            (m) => m.senderAddress === deploymentMailAddress,
          ),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );
      const signedSend = env.hub.outboundMail.find(
        (m) => m.senderAddress === deploymentMailAddress,
      );
      expect(signedSend).toBeDefined();
      expect(signedSend?.recipients).toContain(deploymentMailAddress);
    });
  },
);

// A multi-step workflow fails over to a new host mid-conversation and the
// conversation continues with no lost turns.
//
// The workflow's first step is a conversation: its trigger budget is three,
// so every inbound mail is its next turn, and `step2` runs once the third
// turn is done. Two turns run on the primary sidecar. Once the Hub holds
// every committed turn, the primary is killed -- its local disk goes with
// it. The deployment then moves to a second sidecar with a fresh data dir
// the way the Hub fails over to replacement capacity: the Hub's copy of the
// workflow-run history is replayed onto it and the same frozen definition is
// deployed there. The new child resumes the run from its event log with
// `step1` parked for its next turn, and the third mail is that turn.
//
// The proof is what the model sees. The mock inference server records every
// request and echoes each turn's inbound text, so the third turn's request
// must carry the second turn's request plus its reply, verbatim, before the
// new mail. The run then completes through `step2` on the replacement host.
//
// Harness justification: SPAWN-REAL. A real hub, two real sidecar
// subprocesses, real workflow-process children, and mock inference. The
// host loss is a kill of the primary sidecar process and its children.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { promises as fs } from "node:fs";

import { tenant as tenantTable } from "@intx/db/schema";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import {
  createApprovalSet,
  deriveRunAddress,
  type ApprovalSet,
} from "@intx/workflow-deploy";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedPrincipal } from "@intx/test-harness/seed";

import {
  SECOND_SIDECAR_ID,
  SECOND_TOKEN,
  SESSION_ID,
  SIDECAR_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  readWorkflowRunEvents,
  settleWorkflowRunPacks,
  startDeployFlowEnv,
  startSidecarSubprocess,
  terminateSidecarSubprocess,
  waitFor,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
  type InferenceMessage,
  type InferenceRequest,
  type SidecarHandle,
} from "../hub-agent/lib/deploy-flow-env";
import { twoStepEntry } from "./fixtures/two-step";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_fa11a7e0fa11a7e0fa11a7e0fa11a7e0";
const TENANT_ID = "tnt_multistep_conversation_failover";
const CALLER_PRINCIPAL_ID = "prn_multistep_conversation_failover";
const DEFINITION_ASSET_ID = "ast_multistep_conversation_failover_wf";

const MARKER_ONE = "failover-fa11a7-one";
const MARKER_TWO = "failover-fa11a7-two";
const MARKER_THREE = "failover-fa11a7-three";

let env: DeployFlowEnv;
let h: TestDb;
let replacement: SidecarHandle | undefined;
const replacementTempDirs: string[] = [];

function messageText(message: InferenceMessage | undefined): string {
  const content = message?.content;
  if (content === undefined) return "";
  if (typeof content === "string") return content;
  return content.map((block) => block.text ?? "").join("");
}

/**
 * The one `step1` request for the turn whose inbound mail carries `marker`.
 * `step1`'s conversation opens with the first mail, which tells its requests
 * apart from `step2`'s, whose input is `step1`'s last reply.
 */
function step1RequestFor(marker: string): InferenceRequest {
  const matches = env.inference.requests.filter(
    (request) =>
      messageText(request.messages?.[0]).includes(MARKER_ONE) &&
      messageText(request.messages?.at(-1)).includes(marker),
  );
  const [request, ...rest] = matches;
  if (request === undefined || rest.length > 0) {
    throw new Error(
      `expected one step1 request for ${marker}, found ${String(matches.length)}`,
    );
  }
  return request;
}

/**
 * The adapter marks the newest user message as a prompt-cache breakpoint, so
 * the same message carries the marker on one request and not the next. The
 * marker is request framing, not conversation, so it is dropped before
 * comparing context across requests.
 */
function withoutCacheMarkers(messages: InferenceMessage[]): unknown[] {
  return messages.map((message) =>
    typeof message.content === "string" || message.content === undefined
      ? message
      : {
          ...message,
          content: message.content.map((block) =>
            Object.fromEntries(
              Object.entries(block).filter(([key]) => key !== "cache_control"),
            ),
          ),
        },
  );
}

async function inputParks(): Promise<number> {
  const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, DEPLOYMENT_ID);
  return events.filter(
    (e) =>
      e.type === "SignalAwaited" &&
      e.body["stepId"] === "step1" &&
      e.body["parkKind"] === "input",
  ).length;
}

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
    name: "multistep-conversation-failover-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv({ inferenceEchoUserMessage: true });
});

afterAll(async () => {
  if (replacement !== undefined) await terminateSidecarSubprocess(replacement);
  for (const dir of replacementTempDirs) {
    await fs.rm(dir, { recursive: true, force: true });
  }
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

describe.skipIf(!harnessDbEnvAvailable())(
  "a multi-step conversation survives failover to a new host",
  () => {
    test("the conversational step continues on the replacement host with every turn", async () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
      const address = deriveRunAddress({
        runId: DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });
      const source: InferenceSource = {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${String(env.inference.server.port)}`,
        credentialId: "sk-mock",
        model: "mock-model",
      };
      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentId: DEPLOYMENT_ID,
        tenantId: TENANT_ID,
        principalId: CALLER_PRINCIPAL_ID,
        agentAddress: address,
        systemPrompt: "Fallback prompt (overridden per step by the definition)",
        tools: [],
        grants: [],
        sources: [source],
        defaultSource: source.id,
      };
      const approvals: ApprovalSet = createApprovalSet([
        `inference.source:${source.id}`,
        "director:@intx/agent/default",
        `mail.address:${address}`,
        `mail.send:${DEPLOYMENT_DOMAIN}`,
      ]);

      const handle = await deployWorkflowSourceForTest(env, {
        entryModule: twoStepEntry({
          address,
          systemPrompt1: "You are the conversational step.",
          systemPrompt2: "You summarize the conversation.",
          agentId1: "failover-conversation",
          agentId2: "failover-summary",
          workflowId: `wf_${DEPLOYMENT_ID}`,
          step1Triggers: 3,
        }),
        sidecarId: SIDECAR_ID,
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: address,
        approvals,
        config,
        sources: { step1: [source], step2: [source] },
      });
      await waitFor(
        () => env.hub.router.getRoutableAddresses().includes(address),
        {
          diagnostics: env.sidecarDiagnostics,
        },
      );

      // --- Two turns on the primary host ---------------------------------
      await fireMailTrigger(env, address, {
        content: `Opening turn ${MARKER_ONE}.`,
      });
      await waitFor(async () => (await inputParks()) >= 1, {
        diagnostics: env.sidecarDiagnostics,
      });
      await fireMailTrigger(env, address, {
        content: `Second turn ${MARKER_TWO}.`,
      });
      await waitFor(async () => (await inputParks()) >= 2, {
        diagnostics: env.sidecarDiagnostics,
      });
      await settleWorkflowRunPacks(env);

      // --- Lose the primary host -----------------------------------------
      await terminateSidecarSubprocess(env.sidecar);
      await waitFor(
        () => !env.hub.router.getRoutableAddresses().includes(address),
        { diagnostics: env.sidecarDiagnostics },
      );

      // --- Fail over to a replacement host with a fresh data dir ----------
      const hubPort = env.hub.server.port;
      if (hubPort === undefined) throw new Error("hub port is unset");
      replacement = await startSidecarSubprocess({
        hubPort,
        registerTempDir: (dir) => replacementTempDirs.push(dir),
        extraEnv: {
          SIDECAR_ID: SECOND_SIDECAR_ID,
          SIDECAR_TOKEN: SECOND_TOKEN,
        },
      });
      env.registerSidecar(replacement);
      await waitFor(
        () => env.hub.router.getConnectedSidecars().includes(SECOND_SIDECAR_ID),
        { diagnostics: env.sidecarDiagnostics },
      );
      await handle.redeploy(SECOND_SIDECAR_ID);
      await waitFor(
        () => env.hub.router.getRoutableAddresses().includes(address),
        {
          diagnostics: env.sidecarDiagnostics,
        },
      );

      // --- The third turn runs on the replacement host --------------------
      await fireMailTrigger(env, address, {
        content: `Third turn ${MARKER_THREE}.`,
      });
      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        DEPLOYMENT_ID,
        { diagnostics: env.sidecarDiagnostics },
      );
      expect(terminal.type).toBe("RunCompleted");

      // --- The model saw the whole conversation ---------------------------
      const secondTurn = withoutCacheMarkers(
        step1RequestFor(MARKER_TWO).messages ?? [],
      );
      const thirdTurnRequest = step1RequestFor(MARKER_THREE);
      const thirdTurn = withoutCacheMarkers(thirdTurnRequest.messages ?? []);
      expect(thirdTurn.slice(0, secondTurn.length)).toEqual(secondTurn);
      const carriedReply = thirdTurnRequest.messages?.[secondTurn.length];
      expect(carriedReply?.role).toBe("assistant");
      expect(messageText(carriedReply)).toContain(MARKER_TWO);
      expect(thirdTurn).toHaveLength(secondTurn.length + 2);
    });
  },
);

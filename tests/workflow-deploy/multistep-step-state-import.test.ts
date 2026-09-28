// A new deployment continues a conversational step from state exported off
// another deployment.
//
// Deployment A runs a multi-step workflow whose first step is a
// conversation: its trigger budget is three, so every inbound mail is its
// next turn. After two turns, `step1`'s state is exported from the Hub's copy
// of A's history, the way `GET /workflows/runs/:runId/steps/:stepId/state`
// reads it. Deployment B is a new deployment of the same definition that
// imports that snapshot the way `POST /workflows/deployments` does: the Hub
// files it as `step1`'s seed in B's history before B first starts, and the
// deploy replays that history onto the sidecar.
//
// The proof is what the model sees. The mock inference server records every
// request and echoes each turn's inbound text, so B's first request must
// carry A's second request plus its reply, verbatim, before B's own mail.
// B's step then commits the imported turns as its own state along with its
// first turn.
//
// Harness justification: SPAWN-REAL. A real hub, a real sidecar subprocess,
// real workflow-process children, and mock inference.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { tenant as tenantTable } from "@intx/db/schema";
import { readStepStateSnapshot } from "@intx/hub-sessions";
import type { StepStateImport } from "@intx/types";
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
  SESSION_ID,
  SIDECAR_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  readWorkflowRunEvents,
  settleWorkflowRunPacks,
  startDeployFlowEnv,
  waitFor,
  type DeployFlowEnv,
  type DeployWorkflowSourceForTestHandle,
  type InferenceMessage,
  type InferenceRequest,
} from "../hub-agent/lib/deploy-flow-env";
import { twoStepEntry } from "./fixtures/two-step";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_A = "run_1a9057a7e1a9057a7e1a9057a7e1a905";
const DEPLOYMENT_B = "run_1a9057a7e2b9057a7e2b9057a7e2b905";
const TENANT_ID = "tnt_multistep_step_state_import";
const CALLER_PRINCIPAL_ID = "prn_multistep_step_state_import";
const ASSET_A = "ast_multistep_step_state_import_a";
const ASSET_B = "ast_multistep_step_state_import_b";

const MARKER_ONE = "import-1a9057-one";
const MARKER_TWO = "import-1a9057-two";
const MARKER_THREE = "import-1a9057-three";

let env: DeployFlowEnv;
let h: TestDb;

function messageText(message: InferenceMessage | undefined): string {
  const content = message?.content;
  if (content === undefined) return "";
  if (typeof content === "string") return content;
  return content.map((block) => block.text ?? "").join("");
}

/**
 * The one `step1` request for the turn whose inbound mail carries `marker`.
 * Both deployments' `step1` conversations open with A's first mail, which
 * tells their requests apart from `step2`'s.
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

async function inputParks(deploymentId: string): Promise<number> {
  const events = await readWorkflowRunEvents(env, deploymentId, deploymentId);
  return events.filter(
    (e) =>
      e.type === "SignalAwaited" &&
      e.body["stepId"] === "step1" &&
      e.body["parkKind"] === "input",
  ).length;
}

function inferenceSource(): InferenceSource {
  return {
    id: "anthropic:mock-model",
    provider: "anthropic",
    baseURL: `http://localhost:${String(env.inference.server.port)}`,
    credentialId: "sk-mock",
    model: "mock-model",
  };
}

async function deployConversation(args: {
  anchorRunId: string;
  assetId: string;
  stepState?: StepStateImport;
}): Promise<{ address: string; handle: DeployWorkflowSourceForTestHandle }> {
  const address = deriveRunAddress({
    runId: args.anchorRunId,
    domain: DEPLOYMENT_DOMAIN,
  });
  const source = inferenceSource();
  const config: HarnessConfig = {
    sessionId: SESSION_ID,
    agentId: args.anchorRunId,
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
      agentId1: "import-conversation",
      agentId2: "import-summary",
      workflowId: `wf_${args.anchorRunId}`,
      step1Triggers: 3,
    }),
    sidecarId: SIDECAR_ID,
    db: h.db,
    tenantId: TENANT_ID,
    definitionAssetId: args.assetId,
    anchorRunId: args.anchorRunId,
    deploymentDomain: DEPLOYMENT_DOMAIN,
    agentAddress: address,
    approvals,
    config,
    sources: { step1: [source], step2: [source] },
    ...(args.stepState !== undefined ? { stepState: args.stepState } : {}),
  });
  await waitFor(() => env.hub.router.getRoutableAddresses().includes(address), {
    diagnostics: env.sidecarDiagnostics,
  });
  return { address, handle };
}

function exportStep1(handle: DeployWorkflowSourceForTestHandle) {
  return readStepStateSnapshot({
    repoStore: env.hub.agentRepoStore.repoStore,
    repoId: handle.workflowRunRepoId,
    runId: handle.anchorRunId,
    stepId: "step1",
  });
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
  for (const id of [ASSET_A, ASSET_B]) {
    await seedAsset(h.db, {
      id,
      tenantId: TENANT_ID,
      kind: "workflow",
      name: id,
      creatorPrincipalId: CALLER_PRINCIPAL_ID,
    });
  }

  env = await startDeployFlowEnv({ inferenceEchoUserMessage: true });
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

describe.skipIf(!harnessDbEnvAvailable())(
  "a new deployment continues a conversational step from imported state",
  () => {
    test("deployment B's model context continues deployment A's exactly", async () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);

      // --- Two turns on deployment A --------------------------------------
      const a = await deployConversation({
        anchorRunId: DEPLOYMENT_A,
        assetId: ASSET_A,
      });
      await fireMailTrigger(env, a.address, {
        content: `Opening turn ${MARKER_ONE}.`,
      });
      await waitFor(async () => (await inputParks(DEPLOYMENT_A)) >= 1, {
        diagnostics: env.sidecarDiagnostics,
      });
      await fireMailTrigger(env, a.address, {
        content: `Second turn ${MARKER_TWO}.`,
      });
      await waitFor(async () => (await inputParks(DEPLOYMENT_A)) >= 2, {
        diagnostics: env.sidecarDiagnostics,
      });
      await settleWorkflowRunPacks(env);

      // --- Export A's conversational step ---------------------------------
      const exported = await exportStep1(a.handle);
      if (exported === null) throw new Error("deployment A exported no state");
      expect(exported.turns.map((turn) => turn.role)).toEqual([
        "user",
        "assistant",
        "user",
        "assistant",
      ]);

      // --- Deployment B imports it ----------------------------------------
      const b = await deployConversation({
        anchorRunId: DEPLOYMENT_B,
        assetId: ASSET_B,
        stepState: { step1: exported },
      });
      expect(await exportStep1(b.handle)).toEqual(exported);

      await fireMailTrigger(env, b.address, {
        content: `Third turn ${MARKER_THREE}.`,
      });
      await waitFor(async () => (await inputParks(DEPLOYMENT_B)) >= 1, {
        diagnostics: env.sidecarDiagnostics,
      });

      // --- The model saw A's whole conversation ---------------------------
      const lastTurnOnA = withoutCacheMarkers(
        step1RequestFor(MARKER_TWO).messages ?? [],
      );
      const firstTurnOnB = step1RequestFor(MARKER_THREE);
      const continued = withoutCacheMarkers(firstTurnOnB.messages ?? []);
      expect(continued.slice(0, lastTurnOnA.length)).toEqual(lastTurnOnA);
      const carriedReply = firstTurnOnB.messages?.[lastTurnOnA.length];
      expect(carriedReply?.role).toBe("assistant");
      expect(messageText(carriedReply)).toContain(MARKER_TWO);
      expect(continued).toHaveLength(lastTurnOnA.length + 2);

      // --- B's own state now carries the imported turns -------------------
      await settleWorkflowRunPacks(env);
      const continuedState = await exportStep1(b.handle);
      expect(continuedState?.turns.slice(0, exported.turns.length)).toEqual(
        exported.turns,
      );
      expect(continuedState?.turns).toHaveLength(exported.turns.length + 2);
    });
  },
);

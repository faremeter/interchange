// Single-step message-input round-trip integration test (Phase 4.2).
//
// The proof that the inbound mail's BODY reaches the warm agent's
// `agent.send` as the step input -- not an empty or placeholder input.
// Deploys a one-step workflow through the workflow-deploy orchestrator's
// multi-step branch (which spawns the workflow-process subprocess)
// against the real hub + real sidecar subprocess + an echo inference
// fixture, fires a mail with a KNOWN body, and asserts the step's
// committed reply echoes that body.
//
// The mock inference server runs in echo mode: it reflects the last
// user message's text back as `echo:<text>`. The agent delivers the
// inbound conversation content as the user turn, so the echoed reply is
// the load-bearing proof that the mail body traversed
// trigger.payload -> step input -> synthesizeInputContent -> agent.send.
//
// Against the pre-4.2 behaviour (trigger.fire carries no bytes; the
// child threads no triggerPayload, so the first step's
// `{ from: "trigger.payload" }` input resolves to null/empty), the
// echoed reply would be `echo:` with no body -- this test fails there.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { type } from "arktype";

import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
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
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { singleStepAgentEntry } from "./fixtures/single-step-agent";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_single-step-message-input-1";
const STEP_ID = "step1";
const AGENT_ID = "agent-step1";

const FIRST_BODY = "First inbound body alpha-7391.";

// A non-text attachment MIME-encoded into the same inbound message. Its
// presence exercises the full attachment ingest: the supervisor commits the
// bytes to the workflow-run substrate and threads a reference through the
// trigger payload, and the child resolves the reference back to bytes before
// `agent.send` (a resolve failure would fail the run rather than complete it).
const ATTACHMENT_NAME = "photo.png";
const ATTACHMENT_BYTES = new TextEncoder().encode("fake-png-bytes-4821");

// The definition's own tenant, the caller principal that creates the
// definition asset, and the `workflow`-kind asset the frozen definition
// projects over. The install/approve freeze and the anchor `workflow_run`
// insert both write against these, so they must exist in the real DB before
// the deploy runs.
const TENANT_ID = "tnt_single_step_message_input";
const CALLER_PRINCIPAL_ID = "prn_single_step_message_input";
const DEFINITION_ASSET_ID = "ast_single_step_message_input_wf";

// A second single-step deployment for the non-text case. A run goes terminal
// after its one message, so the non-text message must drive its own run rather
// than arrive as a second mail to an already-completed run.
const DEPLOYMENT_ID_2 = "run_single-step-message-input-2";
const DEFINITION_ASSET_ID_2 = "ast_single_step_message_input_wf2";

// The `Mail` shape a RunStarted event's trigger payload carries. Shared by both
// tests (the text+attachment case and the non-text case).
const RunStartedTrigger = type({
  trigger: {
    payload: {
      headers: "object",
      rawHeaders: "object",
      parts: type({
        contentType: "string",
        ref: "string",
        "filename?": "string",
        "disposition?": "'inline' | 'attachment'",
        "text?": "string",
      }).array(),
    },
  },
});

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
    name: "single-step-message-input-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });
  await seedAsset(h.db, {
    id: DEFINITION_ASSET_ID_2,
    tenantId: TENANT_ID,
    kind: "workflow",
    name: "single-step-message-input-wf2",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv({ inferenceEchoUserMessage: true });
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

describe.skipIf(!harnessDbEnvAvailable())(
  "single-step message-input round-trip",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("the inbound mail body reaches agent.send as the step input", async () => {
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

      const operatorApprovals: ApprovalSet = new Set<string>([
        "inference.source:anthropic:mock-model",
        "director:@intx/agent/default",
        `mail.address:${deploymentMailAddress}`,
        `mail.send:${DEPLOYMENT_DOMAIN}`,
      ]);

      const entryModule = singleStepAgentEntry({
        stepId: STEP_ID,
        systemPrompt: "You are the single-step agent.",
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
        approvals: operatorApprovals,
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

      // First message: a KNOWN body. The runId the supervisor mints from
      // the inbound mail is the messageId; discover it from `runs/`.
      const first = await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<single-step-message-input-1@integration.interchange>",
        content: FIRST_BODY,
        attachments: [
          {
            name: ATTACHMENT_NAME,
            contentType: "image/png",
            data: ATTACHMENT_BYTES,
          },
        ],
      });

      const firstRunId = await waitForFirstRunId(env, workflowRunRepoId, {
        diagnostics: env.sidecarDiagnostics,
        timeoutMs: 20_000,
      });

      const firstTerminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        firstRunId,
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );
      expect(firstTerminal.type).toBe("RunCompleted");

      const firstEvents = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        firstRunId,
      );
      const firstStartedBody = firstEvents.find(
        (e) => e.type === "RunStarted",
      )?.body;
      if (firstStartedBody === undefined) throw new Error("missing RunStarted");
      expect(firstStartedBody["consumedMessageId"]).toBe(first.messageId);

      // The mail decoded end to end: the trigger payload is a `Mail` whose
      // parts include the text body AND the attachment, the supervisor having
      // committed each part's bytes to the workflow-run substrate and recorded
      // a ref. The run reaching RunCompleted (asserted above) is the proof the
      // child resolved the attachment ref back to bytes -- an unresolvable ref
      // fails the step rather than completing it.
      const RunStartedTrigger = type({
        trigger: {
          payload: {
            headers: "object",
            rawHeaders: "object",
            parts: type({
              contentType: "string",
              ref: "string",
              "filename?": "string",
              "disposition?": "'inline' | 'attachment'",
              "text?": "string",
            }).array(),
          },
        },
      });
      const startedTrigger = RunStartedTrigger(firstStartedBody);
      if (startedTrigger instanceof type.errors) {
        throw new Error(
          `RunStarted trigger payload shape unexpected: ${startedTrigger.summary}`,
        );
      }
      const parts = startedTrigger.trigger.payload.parts;
      const textPart = parts.find((p) => p.contentType === "text/plain");
      expect(textPart?.text).toBe(FIRST_BODY);
      const imagePart = parts.find((p) => p.contentType === "image/png");
      expect(imagePart?.filename).toBe(ATTACHMENT_NAME);
      expect(imagePart?.ref.startsWith("mail-part:///")).toBe(true);

      const firstReply = readStepReply(stepCompletedBody(firstEvents));

      // The load-bearing assertion: the agent's `agent.send` received the
      // mail body, so the echoed user turn (and thus the reply) carries
      // it. The agent's reactor frames the inbound conversation as
      // `[From: <sender>]\n\n<body>` before inference, so the echo is
      // `echo:[From: ...]\n\n<body>` -- the body substring is the proof.
      // Pre-4.2 the trigger payload was absent and the step input
      // resolved to null, so the echo would carry no body at all.
      expect(firstReply.startsWith("echo:")).toBe(true);
      expect(firstReply).toContain(FIRST_BODY);
      expect(firstReply).not.toBe("echo:");
      expect(firstReply).not.toBe("echo:null");
    });

    // A message with NO conversation body -- here an attachment with an empty
    // text part -- is a legitimate inbound message. The deployed ingest
    // delivers the non-text content as a Mail rather than flattening it to text
    // and calling `agent.send("")` (which `rejectEmptyStringIfPresent` throws
    // on -> StepFailed -> RunFailed), so the run reaches RunCompleted. The run
    // completing rather than failing is the load-bearing assertion. This
    // deploys its own single-step workflow (a run goes terminal after its one
    // message, so the non-text case must be that run's driving message, not a
    // second mail to an already-completed run).
    test("a non-text inbound message (no conversation body) completes the run", async () => {
      const deploymentMailAddress = deriveRunAddress({
        runId: DEPLOYMENT_ID_2,
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
        agentId: `${DEPLOYMENT_ID_2}`,
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
      ]);

      const entryModule = singleStepAgentEntry({
        stepId: STEP_ID,
        systemPrompt: "You are the single-step agent.",
        address: deploymentMailAddress,
        agentId: AGENT_ID,
      });

      const handle = await deployWorkflowSourceForTest(env, {
        entryModule,
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID_2,
        anchorRunId: DEPLOYMENT_ID_2,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: deploymentMailAddress,
        approvals: operatorApprovals,
        config,
        sources: { [STEP_ID]: [inferenceSource] },
      });
      expect(handle.publicKey).toBeTruthy();

      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      // The driving message: an attachment with an EMPTY conversation body.
      const audioBytes = new TextEncoder().encode("fake-audio-bytes-9f2c");
      const fired = await fireMailTrigger(env, deploymentMailAddress, {
        messageId:
          "<single-step-message-input-nontext@integration.interchange>",
        content: "", // no conversation body -- the empty-flatten crash case
        attachments: [
          { name: "clip.mp3", contentType: "audio/mpeg", data: audioBytes },
        ],
      });

      const runId = await waitForFirstRunId(env, handle.workflowRunRepoId, {
        diagnostics: env.sidecarDiagnostics,
        timeoutMs: 20_000,
      });

      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID_2,
        runId,
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );
      // The load-bearing assertion: the non-text message COMPLETED the run
      // instead of failing it. A regression surfaces as RunFailed here.
      expect(terminal.type).toBe("RunCompleted");

      const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID_2, runId);
      const startedBody = events.find((e) => e.type === "RunStarted")?.body;
      if (startedBody === undefined) {
        throw new Error("missing RunStarted for the non-text message");
      }
      expect(startedBody["consumedMessageId"]).toBe(fired.messageId);

      const started = RunStartedTrigger(startedBody);
      if (started instanceof type.errors) {
        throw new Error(
          `non-text RunStarted trigger payload shape unexpected: ${started.summary}`,
        );
      }
      // The decoded Mail carries the audio attachment as a committed,
      // resolvable part; the run completing proves the child resolved it back
      // to bytes for `agent.send` without an empty-string flatten.
      const audioPart = started.trigger.payload.parts.find(
        (p) => p.contentType === "audio/mpeg",
      );
      expect(audioPart?.filename).toBe("clip.mp3");
      expect(audioPart?.ref.startsWith("mail-part:///")).toBe(true);
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
 * Extract the agent's reply string from a `StepCompleted` event body.
 * The runtime records the step output through the blob substrate; a
 * small `{ reply, turn }` output inlines as `inline:<json>`, so the
 * reply is recovered by parsing the JSON after the `inline:` prefix.
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

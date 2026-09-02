// Single-step warm-agent mail_wait mid-turn wake (INTR-480).
//
// Proves that the supervisor's eager-commit + `mailbox.notify` decouples mail
// VISIBILITY from turn dispatch: a `mail_wait` blocked mid-turn wakes on a
// SECOND inbound mail that arrives while the agent is still inside that turn,
// with no second turn dispatched to carry it.
//
// Shape. The warm agent's mock provider calls `mail_wait` on its first turn,
// querying for a sender (`WAKE_FROM`) that has NOT written yet -- so the
// initial mailbox search finds nothing and the wait registers a watch and
// blocks. The trigger mail (mail 1, from a DIFFERENT sender) opened the run
// but does not satisfy the query. The test confirms the run is parked in
// `mail_wait` (started, no terminal event, the `mail_wait` inference issued),
// then fires mail 2 from `WAKE_FROM`. The supervisor eager-commits mail 2 into
// the per-run mailbox and fires `mailbox.notify`; the child's watch registry
// resolves the blocked `mail_wait` with mail 2's content, the turn completes,
// and the agent's reply echoes the waited message behind `WAITED:`.
//
// Load-bearing assertion: the run TRIGGERED BY MAIL 1 completes with a reply
// carrying MAIL 2's body. That reply could only be produced if `mail_wait`,
// blocked inside the mail-1 turn, received mail 2 through the eager-commit +
// notify path -- the mail-1 turn never dispatched a turn for mail 2. A reply
// carrying a `timeout` error (mail_wait expiring without a match) would fail
// this test, which is exactly the failure mode if eager-commit + notify did
// not wake the wait.

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
  type WorkflowRunEvent,
} from "../hub-agent/lib/deploy-flow-env";
import { singleStepMailInboxEntry } from "./fixtures/single-step-mail-inbox-agent";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_ma11400dba5eba11cafef00dba5eba44";
const STEP_ID = "step1";
const AGENT_ID = "agent-mail-wait-wake";

// Mail 1 opens the run; its sender does NOT match the mail_wait query, so the
// wait blocks rather than resolving on the trigger message.
const MAIL1_MESSAGE_ID = "<mail-wait-m1-5d20af@integration.interchange>";
const MAIL1_FROM = "opener-5d20af@integration.interchange";
const MAIL1_BODY = "Opener body that must NOT satisfy the wait 5d20af.";

// Mail 2 is the awaited arrival: its sender matches the mail_wait query, and
// its distinctive body must surface in the reply.
const MAIL2_MESSAGE_ID = "<mail-wait-m2-5d20af@integration.interchange>";
const WAKE_FROM = "waker-5d20af@integration.interchange";
const MAIL2_BODY = "Awaited arrival body marker mail-wait-5d20af.";

const RESULT_PREFIX = "WAITED:";

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

const TENANT_ID = "tnt_single_step_mail_wait";
const CALLER_PRINCIPAL_ID = "prn_single_step_mail_wait";
const DEFINITION_ASSET_ID = "ast_single_step_mail_wait_wf";

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
    name: "single-step-mail-wait-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  // The mock re-issues `mail_wait` until its result lands, then replies with
  // the result content behind RESULT_PREFIX. Because `mail_wait` only produces
  // a tool_result when it RESOLVES, the child issues exactly one inference,
  // then blocks in the wait until mail 2 arrives.
  env = await startDeployFlowEnv({
    inferenceApprovalToolCall: {
      toolName: "mail_wait",
      input: { query: { from: WAKE_FROM }, timeout: 60 },
      resultPrefix: RESULT_PREFIX,
    },
  });
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

describe.skipIf(!harnessDbEnvAvailable())(
  "warm single-step agent's mail_wait wakes mid-turn on a newly-arrived mail",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("mail_wait blocked in the mail-1 turn resolves on mail 2 delivered via eager-commit + notify", async () => {
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

      // Fire mail 1: opens the run. Its sender does not match the wait query.
      const mail1 = await fireMailTrigger(env, deploymentMailAddress, {
        messageId: MAIL1_MESSAGE_ID,
        from: MAIL1_FROM,
        content: MAIL1_BODY,
        grants: READ_TOOL_GRANTS,
      });
      expect(mail1.messageId).toBe(MAIL1_MESSAGE_ID);

      const runId = await waitForFirstRunId(env, handle.workflowRunRepoId, {
        diagnostics: env.sidecarDiagnostics,
        timeoutMs: 20_000,
      });

      // The turn is parked in mail_wait: the run started, the mail_wait
      // inference was issued, and NO terminal event has landed. Confirm this
      // holds across a settle window -- since mail 2 has not been fired,
      // mail_wait cannot have matched, so a still-open run proves the watch is
      // registered and blocking (not merely slow to start).
      await waitFor(
        () =>
          env.inference.requests.some((r) =>
            r.tools?.some((t) => t.name === "mail_wait"),
          ),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );
      const isTerminal = async (): Promise<boolean> => {
        const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
        return events.some(
          (e) =>
            e.type === "RunCompleted" ||
            e.type === "RunFailed" ||
            e.type === "RunCancelled",
        );
      };
      const started = async (): Promise<boolean> => {
        const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
        return events.some((e) => e.type === "RunStarted");
      };
      await waitFor(started, {
        timeoutMs: 20_000,
        diagnostics: env.sidecarDiagnostics,
      });
      const settleUntil = Date.now() + 1_000;
      while (Date.now() < settleUntil) {
        if (await isTerminal()) {
          throw new Error(
            "run reached a terminal state before mail 2 was fired; mail_wait did not block on the unmatched trigger",
          );
        }
        await new Promise((r) => setTimeout(r, 50));
      }

      // Fire mail 2 from the awaited sender WHILE the turn is blocked in
      // mail_wait. The supervisor eager-commits it and fires mailbox.notify.
      const mail2 = await fireMailTrigger(env, deploymentMailAddress, {
        messageId: MAIL2_MESSAGE_ID,
        from: WAKE_FROM,
        content: MAIL2_BODY,
        grants: READ_TOOL_GRANTS,
      });
      expect(mail2.messageId).toBe(MAIL2_MESSAGE_ID);

      // The mail-1 turn now completes: mail_wait resolved with mail 2.
      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        runId,
        { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
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

      // The run that consumed MAIL 1 is the one that produced the reply.
      const firstStarted = events.find((e) => e.type === "RunStarted");
      expect(firstStarted?.body["consumedMessageId"]).toBe(MAIL1_MESSAGE_ID);

      // The first (mail-1 turn) StepCompleted reply carries MAIL 2's body,
      // delivered through mail_wait's mid-turn wake. A timeout would have
      // produced a `timeout` error instead of the awaited content.
      const reply = firstStepReply(events);
      expect(reply.startsWith(RESULT_PREFIX)).toBe(true);
      expect(reply).toContain("mail-wait-5d20af");
      expect(reply).toContain(WAKE_FROM);
      expect(reply).not.toContain("timeout");
    });
  },
);

/** The first (lowest-seq) `StepCompleted` event's reply string. */
function firstStepReply(events: WorkflowRunEvent[]): string {
  const stepCompleted = events.find(
    (e) => e.type === "StepCompleted" && e.body["stepId"] === STEP_ID,
  );
  if (stepCompleted === undefined) {
    throw new Error("missing StepCompleted for the single step");
  }
  return readStepReply(stepCompleted.body);
}

/**
 * Extract the agent's reply string from a `StepCompleted` event body. The
 * runtime records the step output through the blob substrate; a small
 * `{ reply, turn }` output inlines as `inline:<json>`.
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

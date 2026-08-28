// FIFO mail-trigger serialization round-trip integration test.
//
// Deploys a multi-step workflow against the workflow-deploy
// orchestrator's multi-step branch, fires three distinct mails at the
// deployment's trigger address in quick succession, and verifies that the
// first fires the deployment's one stable run while the queued post-terminal
// mails are rejected in FIFO order.
//
// The supervisor's mail flow path enqueues every inbound mail into the
// workflow-run repo's `addresses/<segment>/inbox/` FIFO via
// `enqueueInbox`, and a per-deployment serial dispatch loop drains the
// inbox in arrival order (filename-prefix sort on `receivedAt`),
// forwarding the first entry to the workflow-process child as a
// `trigger.fire`. The loop waits for the run's terminal event before
// dequeueing the next entry; once terminal, it permanently rejects later
// entries and records that result while moving each processing entry into
// `consumed/<messageId>.json`. With three mails fired before the first run has
// reached terminal, the last two are forced to queue, which pins both FIFO
// ordering and the no-refire invariant.
//
// The deployment is intentionally multi-step (a two-step workflow
// rather than a trivial single-step one): the FIFO invariant lives
// only on the supervisor-driven multi-step path. The trivial-deploy
// branch routes mail directly through the session manager and does
// not exercise the claim-check substrate, so a "trivial workflow that
// handles mail" would not test the FIFO surface this commit pins.
//
// Crash-replay across a real child SIGKILL -- the respawn-time
// `replayProcessingToInbox` that keeps FIFO across an unexpected child
// exit -- is covered end to end in `crash-respawn-fifo.test.ts`.
//
// The deployment is deployed BY SOURCE-REF (bundle a source entry module into a
// hub asset, probe it, approve+freeze it against a real DB, deploy the
// source-ref frame). This mirrors the multistep-signal and drain-roundtrip
// tests' shape so a regression in any of the seven hops surfaces uniformly
// across the three.
//
// The pre-landed `deploy-flow-env` fixture supplies every helper this
// file consumes; this file does not modify the fixture.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

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
  readClaimCheckDir,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { waitForConsumedEntries } from "./fifo-mail-helpers";
import { twoStepEntry } from "./fixtures/two-step";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_fifo-mail-1";

const MESSAGE_IDS: readonly string[] = [
  "<fifo-mail-1@integration.interchange>",
  "<fifo-mail-2@integration.interchange>",
  "<fifo-mail-3@integration.interchange>",
];

// The definition's own tenant, the caller principal that creates the
// definition asset, and the `workflow`-kind asset the frozen definition
// projects over. The install/approve freeze and the anchor `workflow_run`
// insert both write against these, so they must exist in the real DB before
// the deploy runs.
const TENANT_ID = "tnt_fifo_mail";
const CALLER_PRINCIPAL_ID = "prn_fifo_mail";
const DEFINITION_ASSET_ID = "ast_fifo_mail_wf";

// A sustained-load companion in `fifo-mail-load.test.ts` pins the
// dispatch loop's serial discipline under pressure (the 3-mail case
// pins that the invariant exists; the load case pins that it
// survives a sustained batch of concurrent enqueues). The load case
// is held out of `make test`'s default run because it is a
// sustained-pressure test rather than a routine integration check;
// it runs via `make test-load` instead.

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
    name: "fifo-mail-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

describe.skipIf(!harnessDbEnvAvailable())(
  "FIFO mail-trigger serialization",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("three mails preserve FIFO while only the first fires the stable run", async () => {
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

      const entryModule = twoStepEntry({
        address: deploymentMailAddress,
        systemPrompt1: "You are the first FIFO step agent.",
        systemPrompt2: "You are the second FIFO step agent.",
        agentId1: "agent-fifo-step1",
        agentId2: "agent-fifo-step2",
        workflowId: `wf_${DEPLOYMENT_ID}`,
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
        sources: { step1: [inferenceSource], step2: [inferenceSource] },
      });
      expect(handle.publicKey).toBeTruthy();

      const workflowRunRepoId = handle.workflowRunRepoId;

      // The source-ref frame round-trips through the real sidecar subprocess
      // (index the pack, check out the pinned subtree, register the address),
      // so routability is asynchronous. Wait for it before firing the triggers.
      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      // Fire all three mails in quick succession. `routeMail` is
      // synchronous against the hub-side mail bus; the supervisor's
      // `onMailMessage` enqueues each into the inbox FIFO in arrival
      // order. The supervisor's serial dispatch loop only dequeues the
      // next entry after the prior run's terminal event lands, so two
      // of the three mails are guaranteed to queue.
      const firedMessageIds: string[] = [];
      for (const messageId of MESSAGE_IDS) {
        const { messageId: routed } = await fireMailTrigger(
          env,
          deploymentMailAddress,
          { messageId },
        );
        firedMessageIds.push(routed);
      }
      expect(firedMessageIds).toEqual([...MESSAGE_IDS]);

      // The stable runId is the deployment's local part. Its event history is
      // never cleared: the first mail owns RunStarted and later queued mails
      // become terminal rejections in the claim-check index.
      const runId = DEPLOYMENT_ID;

      const consumedEntries = await waitForConsumedEntries(
        env,
        workflowRunRepoId,
        deploymentMailAddress,
        MESSAGE_IDS,
        { timeoutMs: 90_000, diagnostics: env.sidecarDiagnostics },
      );
      const consumedMessageIds = consumedEntries.map((e) => e.messageId);
      for (const messageId of MESSAGE_IDS) {
        expect(consumedMessageIds).toContain(messageId);
      }
      expect(
        consumedEntries.find((entry) => entry.messageId === MESSAGE_IDS[0])
          ?.rejection,
      ).toBeUndefined();
      for (const messageId of MESSAGE_IDS.slice(1)) {
        expect(
          consumedEntries.find((entry) => entry.messageId === messageId)
            ?.rejection?.code,
        ).toBe("workflow_run_terminal");
      }

      // Wait for the one run to reach terminal.
      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        runId,
        { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
      );
      expect(terminal.type).toBe("RunCompleted");

      const receivedAts = MESSAGE_IDS.map((mid) => {
        const entry = consumedEntries.find((e) => e.messageId === mid);
        if (entry === undefined) {
          throw new Error(
            `fifo-mail: consumed entry for ${mid} missing after all runs completed`,
          );
        }
        return entry.receivedAt;
      });
      // Strictly non-decreasing arrival order: `receivedAt` is a
      // millisecond timestamp the supervisor captures inside its
      // enqueue path, so successive mails fired in this test's tight
      // loop may share a millisecond. Strict-ascending would falsely
      // fail on a fast machine; the FIFO invariant the substrate
      // pins is `receivedAt` non-decreasing across the inbox key
      // sort, with the messageId tiebreak handling collisions.
      for (let i = 1; i < receivedAts.length; i += 1) {
        const prev = receivedAts[i - 1];
        const curr = receivedAts[i];
        if (prev === undefined || curr === undefined) {
          throw new Error("unreachable");
        }
        expect(curr).toBeGreaterThanOrEqual(prev);
      }

      // Inbox must be empty after every consumed/ entry lands.
      // The consumed/ wait above already guarantees every
      // dispatched run reached `markConsumed`, which removes the
      // entry from `processing/` atomically with the consumed/
      // write. The reads here are therefore one-shot.
      const inboxEntries = await readClaimCheckDir(
        env,
        workflowRunRepoId,
        deploymentMailAddress,
        "inbox",
      );
      expect(inboxEntries).toEqual([]);

      // Processing must be empty: every dispatched entry hit
      // `markConsumed` after its terminal event.
      const processingEntries = await readClaimCheckDir(
        env,
        workflowRunRepoId,
        deploymentMailAddress,
        "processing",
      );
      expect(processingEntries).toEqual([]);

      // Canonical event chain assertion for the one stable run:
      // the multi-step workflow above is `step1 -> step2`, so the
      // expected chain is `RunStarted -> StepStarted{step1} ->
      // StepCompleted{step1} -> StepStarted{step2} ->
      // StepCompleted{step2} -> RunCompleted`. No later mail may replace it.
      const currentEvents = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        runId,
      );
      const currentTypes = currentEvents.map((e) => e.type);
      const observedSequence = `observed: ${currentTypes.join(" -> ")}`;

      const runStartedIdx = currentTypes.indexOf("RunStarted");
      const step1StartedIdx = currentTypes.findIndex(
        (t, i) =>
          t === "StepStarted" && currentEvents[i]?.body["stepId"] === "step1",
      );
      const step1CompletedIdx = currentTypes.findIndex(
        (t, i) =>
          t === "StepCompleted" && currentEvents[i]?.body["stepId"] === "step1",
      );
      const step2StartedIdx = currentTypes.findIndex(
        (t, i) =>
          t === "StepStarted" && currentEvents[i]?.body["stepId"] === "step2",
      );
      const step2CompletedIdx = currentTypes.findIndex(
        (t, i) =>
          t === "StepCompleted" && currentEvents[i]?.body["stepId"] === "step2",
      );
      const runCompletedIdx = currentTypes.indexOf("RunCompleted");

      expect(
        `runStarted@${String(runStartedIdx)} (${observedSequence})`,
      ).not.toBe(`runStarted@-1 (${observedSequence})`);
      expect(step1StartedIdx).toBeGreaterThan(runStartedIdx);
      expect(step1CompletedIdx).toBeGreaterThan(step1StartedIdx);
      expect(step2StartedIdx).toBeGreaterThan(step1CompletedIdx);
      expect(step2CompletedIdx).toBeGreaterThan(step2StartedIdx);
      expect(runCompletedIdx).toBeGreaterThan(step2CompletedIdx);

      // The immutable RunStarted belongs to the first message.
      const runStartedBody = currentEvents[runStartedIdx]?.body;
      if (runStartedBody === undefined) throw new Error("unreachable");
      expect(runStartedBody["consumedMessageId"]).toBe(MESSAGE_IDS[0]);
    }, 60_000);
  },
);

// FIFO mail-trigger sustained-load regression test.
//
// Companion to `fifo-mail.test.ts`'s 3-mail correctness case: the 3-mail case
// pins that the dispatch loop drains the inbox in arrival order; this file
// pins that the first mail alone fires the stable run and every post-terminal
// mail is rejected under sustained pressure.
//
// Held out of `make test`'s default run (sustained pressure: a large batch of
// mails in quick succession); runs through the dedicated `make test-load`
// target, which CI invokes separately.
//
// The runtime-coverage rationale matches the 3-mail case: the supervisor's
// FIFO inbox dispatch loop is the only place these invariants live, so a
// regression surfaces here uniformly with the multistep-signal and
// drain-roundtrip tests.

import fs from "node:fs";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import {
  createApprovalSet,
  deriveRunAddress,
  type ApprovalSet,
} from "@intx/workflow-deploy";
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
  readClaimCheckDir,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { waitForConsumedEntries } from "./fifo-mail-helpers";
import { singleStepAgentEntry } from "./fixtures/single-step-agent";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID_LOAD = "run_fifo-mail-load-1";
const STEP_ID = "loadStep";
const AGENT_ID = "agent-fifo-load-step";

// The tenant, caller principal, and `workflow`-kind definition asset the
// install/approve freeze and anchor `workflow_run` insert write against; they
// must exist in the real DB before the deploy runs.
const TENANT_ID = "tnt_fifo_mail_load";
const CALLER_PRINCIPAL_ID = "prn_fifo_mail_load";
const DEFINITION_ASSET_ID = "ast_fifo_mail_load_wf";

// Sustained-load FIFO assertion. The 3-mail case pins the invariant exists;
// only an under-load test surfaces a regression where the dispatch loop's
// "wait for terminal before dequeue" gate silently degrades (e.g. a future
// change that races markConsumed against the next dispatchOne). This test
// fires `LOAD_MAIL_COUNT` (50) mails in quick succession, asserts every one
// lands in consumed/, and asserts the consumed envelopes' arrival timestamps
// are non-decreasing across inbox-arrival order.
//
// Throughput notes. Two costs once dominated per-mail wall-clock; one remains
// in force, the other is eliminated:
//
//   1. Pack-push serialisation on the sender (still in force). The boot-edge
//      facade (`createWorkflowRunPackPushingRepoStore`) COALESCES pushes per
//      `(repoId, ref)`: a write returns as soon as the local commit lands, and
//      pushes arriving while a prior push is in flight are squashed into one
//      follow-up push. The receive-side substrate accepts multi-commit packs
//      (it walks the pack's parent chain and runs `validatePush` per new
//      commit in topological order).
//   2. `validatePush` enumeration on the receive side (eliminated). It was
//      scoped to every `runs/<runId>/events/` entry on every push (O(N^2)
//      overall); it now touches only the run(s) a given commit touches, so
//      per-commit cost is bounded by that run's own events (~4-5).
//
// With the receive-side enumeration bounded to the touched run, 50 mails
// complete well within the `make test-load` budget.
const LOAD_MAIL_COUNT = 50;
const LOAD_MESSAGE_IDS: readonly string[] = Array.from(
  { length: LOAD_MAIL_COUNT },
  (_unused, i) =>
    `<fifo-mail-load-${(i + 1).toString().padStart(3, "0")}@integration.interchange>`,
);

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
    id: DEFINITION_ASSET_ID,
    tenantId: TENANT_ID,
    kind: "workflow",
    name: "fifo-mail-load-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv();
});

afterAll(async () => {
  // The fixture's teardown is the load test's only consumer of fs;
  // see `tests/hub-agent/lib/deploy-flow-env.ts` for the per-handle
  // teardown loop. The fs import is held above so the bun test
  // runner's module graph includes node:fs when the test file is
  // discovered standalone.
  void fs;
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

describe.skipIf(!harnessDbEnvAvailable())(
  "FIFO mail-trigger serialization under load",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test(`${String(LOAD_MAIL_COUNT)} mails preserve terminal rejection order under load`, async () => {
      // Coverage-gap follow-up to the 3-mail case in fifo-mail.test.ts.
      // A single-step workflow still routes through the supervisor's FIFO
      // inbox dispatch loop, so the load test uses one to keep commit
      // pressure tractable in CI. The first mail exercises inbox -> processing
      // -> trigger.fire -> wait for terminal -> markConsumed; the rest
      // exercise FIFO dequeue and durable terminal rejection. The invariant
      // under test does not depend on step count.
      const deploymentMailAddress = deriveRunAddress({
        runId: DEPLOYMENT_ID_LOAD,
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
        agentId: `${DEPLOYMENT_ID_LOAD}`,
        tenantId: "tenant-1",
        principalId: "prin_integration-1",
        agentAddress: deploymentMailAddress,
        systemPrompt: "Fallback prompt (overridden per step by the definition)",
        tools: [],
        grants: [],
        sources: [inferenceSource],
        defaultSource: "anthropic:mock-model",
      };

      const operatorApprovals: ApprovalSet = createApprovalSet([
        "inference.source:anthropic:mock-model",
        "director:@intx/agent/default",
        `mail.address:${deploymentMailAddress}`,
        `mail.send:${DEPLOYMENT_DOMAIN}`,
      ]);

      const entryModule = singleStepAgentEntry({
        stepId: STEP_ID,
        systemPrompt: "You are the FIFO-load step agent.",
        address: deploymentMailAddress,
        agentId: AGENT_ID,
        workflowId: `wf_${DEPLOYMENT_ID_LOAD}`,
      });

      const handle = await deployWorkflowSourceForTest(env, {
        entryModule,
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: DEPLOYMENT_ID_LOAD,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: deploymentMailAddress,
        approvals: operatorApprovals,
        config,
        sources: { [STEP_ID]: [inferenceSource] },
      });
      expect(handle.publicKey).toBeTruthy();

      const workflowRunRepoId = handle.workflowRunRepoId;

      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { diagnostics: env.sidecarDiagnostics },
      );

      // Fire all mails in quick succession. The supervisor's dispatch
      // loop must drain them in strict FIFO order. The canonical event chain
      // materializes once, and consumed/ carries one envelope per mail in
      // receivedAt-non-decreasing order.
      const firedMessageIds: string[] = [];
      for (const messageId of LOAD_MESSAGE_IDS) {
        const { messageId: routed } = await fireMailTrigger(
          env,
          deploymentMailAddress,
          { messageId },
        );
        firedMessageIds.push(routed);
      }
      expect(firedMessageIds).toEqual([...LOAD_MESSAGE_IDS]);

      // Wait for `consumed/` to carry every messageId. The dispatch
      // first writes `markConsumed` after the run's terminal event lands, then
      // records the queued rejections. Observing every consumed entry proves the
      // terminal and rejection paths both completed.
      const consumedEntries = await waitForConsumedEntries(
        env,
        workflowRunRepoId,
        deploymentMailAddress,
        LOAD_MESSAGE_IDS,
        { diagnostics: env.sidecarDiagnostics },
      );
      expect(consumedEntries.length).toBe(LOAD_MAIL_COUNT);
      expect(
        consumedEntries.find((entry) => entry.messageId === LOAD_MESSAGE_IDS[0])
          ?.rejection,
      ).toBeUndefined();
      for (const messageId of LOAD_MESSAGE_IDS.slice(1)) {
        expect(
          consumedEntries.find((entry) => entry.messageId === messageId)
            ?.rejection?.code,
        ).toBe("workflow_run_terminal");
      }

      // FIFO invariant: the consumed/ envelopes' `receivedAt`
      // timestamps must be non-decreasing when consulted in the
      // mail-fire order. Strict ascending would falsely fail when two
      // adjacent enqueues land in the same millisecond; the
      // substrate's FIFO key tiebreaks on messageId, so this is the
      // strongest invariant the substrate actually pins.
      const receivedAts = LOAD_MESSAGE_IDS.map((mid) => {
        const entry = consumedEntries.find((e) => e.messageId === mid);
        if (entry === undefined) {
          throw new Error(
            `fifo-mail-load: consumed entry for ${mid} missing after all runs completed`,
          );
        }
        return entry.receivedAt;
      });
      for (let i = 1; i < receivedAts.length; i += 1) {
        const prev = receivedAts[i - 1];
        const curr = receivedAts[i];
        if (prev === undefined || curr === undefined) {
          throw new Error("unreachable");
        }
        expect(curr).toBeGreaterThanOrEqual(prev);
      }

      // Inbox and processing must be empty: every fired mail has
      // landed in consumed/.
      const inboxEntries = await readClaimCheckDir(
        env,
        workflowRunRepoId,
        deploymentMailAddress,
        "inbox",
      );
      expect(inboxEntries).toEqual([]);
      const processingEntries = await readClaimCheckDir(
        env,
        workflowRunRepoId,
        deploymentMailAddress,
        "processing",
      );
      expect(processingEntries).toEqual([]);

      // Under the stable-runId model all 50 messages target the deployment
      // address, but only the first fires it. The immutable event history remains
      // that one run; every later message is represented by its rejection receipt.
      const runId = DEPLOYMENT_ID_LOAD;
      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID_LOAD,
        runId,
        { diagnostics: env.sidecarDiagnostics },
      );
      expect(terminal.type).toBe("RunCompleted");

      // Verify the current (last) run's canonical single-step event chain.
      const events = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID_LOAD,
        runId,
      );
      const types = events.map((e) => e.type);
      const runStartedIdx = types.indexOf("RunStarted");
      const stepStartedIdx = types.findIndex(
        (t, i) =>
          t === "StepStarted" && events[i]?.body["stepId"] === "loadStep",
      );
      const stepCompletedIdx = types.findIndex(
        (t, i) =>
          t === "StepCompleted" && events[i]?.body["stepId"] === "loadStep",
      );
      const runCompletedIdx = types.indexOf("RunCompleted");

      if (
        runStartedIdx < 0 ||
        stepStartedIdx <= runStartedIdx ||
        stepCompletedIdx <= stepStartedIdx ||
        runCompletedIdx <= stepCompletedIdx
      ) {
        throw new Error(
          `fifo-mail-load: run ${runId} chain malformed: ${types.join(" -> ")}`,
        );
      }
      expect(events[runStartedIdx]?.body["consumedMessageId"]).toBe(
        LOAD_MESSAGE_IDS[0],
      );
    }, 300_000);
  },
);

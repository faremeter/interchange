// Crash-respawn acceptance-gate integration test.
//
// Proves the INTR-193 acceptance requirement end to end: a workflow-process
// child SIGKILLed mid-run is respawned by the supervisor WITHOUT external
// intervention, the stranded mail is replayed, and the deployment recovers
// with FIFO preserved -- all inside the SAME sidecar process (this is
// crash-respawn, not the boot-restore path that a full sidecar restart
// exercises).
//
// Shape: deploy a `step1 -> awaitSignal(go) -> step2` workflow, fire three
// mails, and drive the first mail's run to the mid-flight `SignalAwaited`
// pause (past `RunStarted`, before `RunCompleted`) -- the deterministic
// "mid-processing" point. Mail 0 owns the deployment's one stable run;
// mails 1 and 2 queue behind it. SIGKILL just the workflow-process child
// (not the sidecar). The in-process supervisor observes `handle.exited`,
// waits its respawn backoff, replays mail 0's stranded `processing/` entry
// back to `inbox/`, and spawns a fresh child that resumes the parked run
// from its committed events. Delivering the awaited signal drives the run
// to `RunCompleted`; mails 1 and 2 then consume as terminal rejections.
//
// The proof that CRASH-RESPAWN (not boot-restore, not redeploy) recovered
// the deployment is a conjunction: the sidecar process stays alive, a new
// workflow-child pid appears under it, and the run reaches `RunCompleted`
// after the post-respawn signal. Mail 0 reaching `consumed/` is the proof
// that the replay ran -- an un-replayed `processing/` entry is orphaned
// (the dead child's dispatch loop never reaches its `markConsumed`) and
// would never re-consume.
//
// Harness justification: SPAWN-REAL. Real hub, real sidecar subprocess,
// real workflow-process child, mock inference. The crash is a genuine
// SIGKILL of the child process; recovery rides the production respawn path.
// This discharges the crash-replay follow-up the FIFO mail test deferred.

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
  injectSignal,
  killWorkflowHostChild,
  listRunIds,
  listWorkflowHostChildren,
  readClaimCheckDir,
  readWorkflowRunEvents,
  settleWorkflowRunPacks,
  startDeployFlowEnv,
  waitFor,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { waitForConsumedEntries } from "./fifo-mail-helpers";
import { signalGateEntry } from "./fixtures/signal-gate";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_crash-respawn-fifo-1";

const MESSAGE_IDS: readonly string[] = [
  "<crash-respawn-1@integration.interchange>",
  "<crash-respawn-2@integration.interchange>",
  "<crash-respawn-3@integration.interchange>",
];

const TENANT_ID = "tnt_crash_respawn_fifo";
const CALLER_PRINCIPAL_ID = "prn_crash_respawn_fifo";
const DEFINITION_ASSET_ID = "ast_crash_respawn_fifo_wf";

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
    name: "crash-respawn-fifo-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

describe.skipIf(!harnessDbEnvAvailable())(
  "crash-respawn preserves FIFO across a mid-run child SIGKILL",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("a SIGKILLed child is respawned in-place, replays stranded mail, and recovers with FIFO intact", async () => {
      const deploymentMailAddress = deriveRunAddress({
        runId: DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });

      const inferenceSource: InferenceSource = {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${String(env.inference.server.port)}`,
        apiKey: "sk-mock",
        model: "mock-model",
      };

      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentId: `${DEPLOYMENT_ID}`,
        tenantId: "tenant-1",
        principalId: "prin_crash-respawn-1",
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

      const entryModule = signalGateEntry({
        address: deploymentMailAddress,
        signalName: "go",
        systemPrompt1: "You are the first crash-respawn step agent.",
        systemPrompt2: "You are the second crash-respawn step agent.",
        agentId1: "crash-respawn-agent1",
        agentId2: "crash-respawn-agent2",
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
        sources: {
          step1: [inferenceSource],
          gate: [inferenceSource],
          step2: [inferenceSource],
        },
      });
      expect(handle.publicKey).toBeTruthy();

      const workflowRunRepoId = handle.workflowRunRepoId;
      const runId = DEPLOYMENT_ID;

      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      // Fire all three mails. Mail 0 fires the stable run; mails 1 and 2
      // queue behind it in the inbox FIFO.
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

      // Drive mail 0's run to the mid-flight SignalAwaited pause: past
      // RunStarted, before RunCompleted. This is the "mid-processing"
      // point at which the child is killed.
      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
          return events.some(
            (e) => e.type === "SignalAwaited" && e.body["signalName"] === "go",
          );
        },
        { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
      );

      const parked = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
      const parkedTypes = parked.map((e) => e.type);
      expect(parkedTypes).toContain("RunStarted");
      expect(parkedTypes).toContain("SignalAwaited");
      expect(parkedTypes).not.toContain("RunCompleted");
      const runStartedBody = parked.find((e) => e.type === "RunStarted")?.body;
      if (runStartedBody === undefined) throw new Error("unreachable");
      expect(runStartedBody["consumedMessageId"]).toBe(MESSAGE_IDS[0]);

      // Mail 0 is mid-flight in `processing/` (it fired the run and the run
      // has not reached `markConsumed`); its stranded entry is what the
      // respawn must replay.
      const processingBeforeKill = await readClaimCheckDir(
        env,
        workflowRunRepoId,
        deploymentMailAddress,
        "processing",
      );
      expect(processingBeforeKill.length).toBeGreaterThanOrEqual(1);

      // Quiesce the pack pipeline so no push is mid-flight when the child
      // dies, then SIGKILL just the child.
      await settleWorkflowRunPacks(env);
      const killedPids = killWorkflowHostChild(env);
      expect(killedPids.length).toBeGreaterThanOrEqual(1);

      // The SIDECAR process itself is untouched -- this is an in-process
      // respawn, not a boot-restore. `exitCode` is null while running.
      expect(env.sidecar.proc.exitCode).toBeNull();

      // The supervisor respawns without intervention: a fresh workflow-child
      // pid (NOT one we killed -- a killed pid lingers briefly as a zombie)
      // appears under the same sidecar within the respawn backoff window.
      await waitFor(
        () =>
          listWorkflowHostChildren(env).some(
            (pid) => !killedPids.includes(pid),
          ),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      // Deliver the awaited signal to the resumed run. The respawn passes
      // through a brief `recycling` phase during which `deliverSignal` is
      // rejected, and `injectSignal` is fire-and-forget (no delivery ack),
      // so retry until the run terminates: each retry mints a fresh
      // signalId, delivery routes by address (always registered) and lands
      // once the respawned child is `running`, and the run completes on the
      // first "go" it receives while awaiting -- later duplicates hit a
      // terminal run and are ignored.
      const injectDeadline = Date.now() + 30_000;
      let terminal: Awaited<ReturnType<typeof waitForWorkflowRunComplete>>;
      for (;;) {
        await injectSignal(env, DEPLOYMENT_ID, runId, "go", { resumed: true });
        try {
          terminal = await waitForWorkflowRunComplete(
            env,
            DEPLOYMENT_ID,
            runId,
            {
              timeoutMs: 5_000,
            },
          );
          break;
        } catch (err) {
          if (Date.now() > injectDeadline) {
            throw new Error(
              `crash-respawn: run never completed after repeated signal delivery\n${env.sidecarDiagnostics()}`,
              { cause: err },
            );
          }
          await new Promise((r) => setTimeout(r, 1_000));
        }
      }
      // Explicit: `waitForWorkflowRunComplete` also accepts RunFailed as
      // terminal, so a broken resume (or a crash-loop latch) must fail here
      // rather than pass on a failed run.
      expect(terminal.type).toBe("RunCompleted");

      // All three mails consume with no drop and no duplication -- each
      // messageId reaches `consumed/` exactly once. Mail 0 reaching
      // `consumed/` at all is the proof the respawn replayed its stranded
      // `processing/` entry (an un-replayed entry is orphaned and never
      // consumed). The queued mails (1, 2) consume as
      // `workflow_run_terminal` rejections once the run is terminal. Mail
      // 0's own rejection is intentionally not asserted: after the replay,
      // whether its re-dequeue observes the run still live (parked) or
      // already terminal is a benign race against signal delivery, and
      // either way it reaches the terminal event exactly once.
      const consumedEntries = await waitForConsumedEntries(
        env,
        workflowRunRepoId,
        deploymentMailAddress,
        MESSAGE_IDS,
        { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
      );
      const consumedMessageIds = consumedEntries.map((e) => e.messageId);
      for (const messageId of MESSAGE_IDS) {
        expect(consumedMessageIds.filter((m) => m === messageId)).toEqual([
          messageId,
        ]);
      }
      for (const messageId of MESSAGE_IDS.slice(1)) {
        expect(
          consumedEntries.find((entry) => entry.messageId === messageId)
            ?.rejection?.code,
        ).toBe("workflow_run_terminal");
      }

      // FIFO: `receivedAt` is non-decreasing across the consumed entries in
      // message-id order (the stranded mail kept its original earlier key,
      // so it did not jump behind the mails that queued after it).
      const receivedAts = MESSAGE_IDS.map((mid) => {
        const entry = consumedEntries.find((e) => e.messageId === mid);
        if (entry === undefined) {
          throw new Error(`crash-respawn: consumed entry for ${mid} missing`);
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

      // Exactly one run, one RunStarted, one RunCompleted across the full
      // (replay-inclusive) log: the respawn resumed the existing run rather
      // than starting a second one.
      const runIdsAtEnd = await listRunIds(env, workflowRunRepoId);
      expect(runIdsAtEnd).toEqual([runId]);
      const finalEvents = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        runId,
      );
      const countType = (t: string): number =>
        finalEvents.filter((e) => e.type === t).length;
      expect(countType("RunStarted")).toBe(1);
      expect(countType("RunCompleted")).toBe(1);

      // Inbox and processing are drained: every entry reached `consumed/`.
      const inboxAtEnd = await readClaimCheckDir(
        env,
        workflowRunRepoId,
        deploymentMailAddress,
        "inbox",
      );
      expect(inboxAtEnd).toEqual([]);
      const processingAtEnd = await readClaimCheckDir(
        env,
        workflowRunRepoId,
        deploymentMailAddress,
        "processing",
      );
      expect(processingAtEnd).toEqual([]);
    }, 180_000);
  },
);

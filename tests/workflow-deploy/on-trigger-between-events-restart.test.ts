// onTrigger between-events crash-recovery integration test (4c).
//
// The reachability gate for a long-lived onTrigger section that crashes
// while parked BETWEEN events -- idle on its input re-arm after one event's
// body completed and before the next event arrives. It proves the section
// survives a sidecar PROCESS death and services the next event, exercising
// the INPUT re-arm recovery path end-to-end on the real deploy stack.
//
// Shape: deploy BY SOURCE-REF (bundle a source entry module into a hub asset,
// probe it, approve+freeze it against a real DB, deploy the source-ref frame) a
// single-step workflow whose one step is an `onTrigger` section subscribed to
// the deployment mail address, with a non-agent (sleep) body. The source-ref
// deploy stages the inline body to its own workflow asset (`wf__section`) and
// the runtime spawns each event's body as a child run by that ref.
//
//   1. Fire mail #1 -> the container run starts, spawns the body `section__0`
//      with the mail body as its input, the body sleeps and completes, and the
//      container re-arms on a snapshot-less `input` park -- parked between
//      events (no RunCompleted; a long-lived section never self-completes).
//   2. Quiesce, then KILL the sidecar subprocess (process death) while the
//      container is parked between events.
//   3. Start a fresh sidecar against the crashed process's SIDECAR_DATA_DIR.
//      Boot-time restore re-spawns the deployment; self-discovery re-includes
//      the CONTAINER run (it is the parent -- its log carries `ChildSpawned`
//      for `section__0`, so it is never itself a child) while EXCLUDING the
//      body child `section__0`. The restored container re-adopts its durable
//      input park (`planOnTriggerResume` -> reawait-input) and re-parks.
//   4. Fire mail #2 -> the supervisor's unified dispatch, finding the run
//      live but its input channel not yet re-armed, waits for the re-armed
//      input park and then delivers the mail as a `signal.deliver` (event 1,
//      NOT a spurious fresh trigger). The container spawns `section__1` with
//      mail #2's body, which completes.
//
// Load-bearing assertions: exactly one `RunStarted`; `ChildSpawned` +
// `ChildCompleted` for BOTH `section__0` and `section__1`; and the container
// never reaches a terminal event.
//
// Harness justification: SPAWN-REAL. Real hub, real sidecar subprocess, real
// workflow-process child driving `runOnTrigger` with the production
// suspendable-child seam, mock (echo) inference. The crash is a genuine kill
// of the sidecar subprocess; the restart is a fresh sidecar against the dead
// process's data dir, so recovery rides the production boot-time restore +
// self-discovery + reawait-input path. This is the first deploy-level test of
// a deployed onTrigger section.

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
import { seedAsset, seedPrincipal } from "@intx/test-harness/seed";
import type { RepoId } from "@intx/hub-sessions";

import {
  SESSION_ID,
  SIDECAR_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  isMailTriggerUnroutableError,
  listRunIds,
  readWorkflowRunEvents,
  settleWorkflowRunPacks,
  startDeployFlowEnv,
  startSidecarSubprocess,
  terminateSidecarSubprocess,
  waitFor,
  waitForReconnect,
  type DeployFlowEnv,
  type SidecarHandle,
} from "../hub-agent/lib/deploy-flow-env";
import { onTriggerBodyEntry } from "./fixtures/on-trigger-body";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_on-trigger-between-events-restart-1";
const SECTION_ID = "section";

const FIRST_BODY = "First event body alpha-7391.";
const SECOND_BODY = "Second event body bravo-5520.";

// The definition's own tenant, the caller principal that creates the
// definition asset, and the `workflow`-kind asset the frozen definition
// projects over. The install/approve freeze and the anchor `workflow_run`
// insert both write against these, so they must exist in the real DB before
// the deploy runs.
const TENANT_ID = "tnt_on_trigger_between_events";
const CALLER_PRINCIPAL_ID = "prn_on_trigger_between_events";
const DEFINITION_ASSET_ID = "ast_on_trigger_between_events_wf";

let env: DeployFlowEnv;
let h: TestDb;
let restartedSidecar: SidecarHandle | undefined;
const restartTempDirs: string[] = [];

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
    name: "on-trigger-between-events-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv({
    inferenceEchoUserMessage: true,
  });
});

afterAll(async () => {
  if (restartedSidecar !== undefined) {
    await terminateSidecarSubprocess(restartedSidecar);
  }
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
  for (const dir of restartTempDirs.splice(0)) {
    await fs.promises.rm(dir, { recursive: true, force: true });
  }
});

/**
 * The container run is the single run under the deployment's workflow-run
 * repo that is NOT a body child. Body children are `${SECTION_ID}__<n>`; the
 * container carries their `ChildSpawned`/`ChildCompleted` records in its own
 * log. Returns `undefined` until the container's `runs/` entry exists.
 */
async function findContainerRunId(
  target: DeployFlowEnv,
  workflowRunRepoId: RepoId,
): Promise<string | undefined> {
  const ids = await listRunIds(target, workflowRunRepoId);
  return ids.find((id) => !id.startsWith(`${SECTION_ID}__`));
}

async function readContainerEvents(
  target: DeployFlowEnv,
  workflowRunRepoId: RepoId,
): Promise<{ type: string; body: Record<string, unknown> }[]> {
  const containerRunId = await findContainerRunId(target, workflowRunRepoId);
  if (containerRunId === undefined) return [];
  return readWorkflowRunEvents(target, DEPLOYMENT_ID, containerRunId);
}

const hasChildCompleted = (
  events: { type: string; body: Record<string, unknown> }[],
  childRunId: string,
): boolean =>
  events.some(
    (e) => e.type === "ChildCompleted" && e.body["childRunId"] === childRunId,
  );

describe.skipIf(!harnessDbEnvAvailable())(
  "onTrigger section crash + restart -> restore re-includes the container, reawait-input services the next event",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("a section parked between events survives a sidecar crash and services the next event", async () => {
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
        agentId: `${DEPLOYMENT_ID}`,
        tenantId: "tenant-1",
        principalId: "prin_on-trigger-between-events-1",
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

      // The section body: a single non-agent step that completes on its own so
      // the section re-arms for the next event. The between-events recovery this
      // test gates is a property of the CONTAINER (its ChildSpawned/re-arm), not
      // the body's work, so the body exercises the runtime (a sleep timer)
      // rather than inference.
      const entryModule = onTriggerBodyEntry({
        address: deploymentMailAddress,
        sectionId: SECTION_ID,
        body: { variant: "sleep", stepId: "wait", duration: 10 },
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
        sources: { [SECTION_ID]: [inferenceSource] },
      });
      expect(handle.publicKey).toBeTruthy();

      const workflowRunRepoId: RepoId = handle.workflowRunRepoId;

      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { diagnostics: env.sidecarDiagnostics },
      );

      // ---- event 0: fire mail #1, drive to the between-events input park ----
      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<on-trigger-between-events-1@integration.interchange>",
        content: FIRST_BODY,
      });

      await waitFor(
        async () => {
          const events = await readContainerEvents(env, workflowRunRepoId);
          return (
            hasChildCompleted(events, `${SECTION_ID}__0`) &&
            events.some(
              (e) =>
                e.type === "SignalAwaited" && e.body["parkKind"] === "input",
            )
          );
        },
        { diagnostics: env.sidecarDiagnostics },
      );

      const containerRunId = await findContainerRunId(env, workflowRunRepoId);
      if (containerRunId === undefined) {
        throw new Error("no container run under the workflow-run repo");
      }

      // Parked between events: body 0 done, section re-armed on input, and the
      // long-lived container has NOT reached a terminal event.
      const parked = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        containerRunId,
      );
      const parkedTypes = parked.map((e) => e.type);
      expect(parkedTypes.filter((t) => t === "RunStarted").length).toBe(1);
      expect(
        parked.some(
          (e) =>
            e.type === "ChildSpawned" &&
            e.body["childRunId"] === `${SECTION_ID}__0`,
        ),
      ).toBe(true);
      expect(hasChildCompleted(parked, `${SECTION_ID}__0`)).toBe(true);
      expect(parkedTypes).not.toContain("RunCompleted");
      expect(parkedTypes).not.toContain("RunFailed");
      expect(parkedTypes).not.toContain("RunCancelled");

      // ---- quiesce, then CRASH the sidecar subprocess (process death) ----
      await settleWorkflowRunPacks(env);

      const crashedDataDir = env.sidecar.dataDir;
      await terminateSidecarSubprocess(env.sidecar);

      await waitFor(
        () =>
          !env.hub.router
            .getRoutableAddresses()
            .includes(deploymentMailAddress),
        { diagnostics: env.sidecarDiagnostics },
      );

      // ---- RESTART: a fresh sidecar against the SAME data dir ----
      const hubPort = env.hub.server.port;
      if (hubPort === undefined) {
        throw new Error("hub.server.port is undefined after crash");
      }
      restartedSidecar = await startSidecarSubprocess({
        hubPort,
        registerTempDir: (dir) => {
          restartTempDirs.push(dir);
        },
        extraEnv: { SIDECAR_DATA_DIR: crashedDataDir },
      });
      env.registerSidecar(restartedSidecar);
      await waitForReconnect(env, deploymentMailAddress);
      expect(env.hub.router.getRoutableAddresses()).toContain(
        deploymentMailAddress,
      );

      // ---- event 1: fire mail #2, assert the recovered section services it ----
      // The restored container re-adopted its durable input park; the dispatch
      // loop waits for that re-arm and delivers mail #2 as event 1's input.
      //
      // Robustness: right after the restart the restored sidecar's hub link can
      // blip while its restore packs flush, so the event-1 mail can race a
      // transient drop that takes the address out of the hub's routing index.
      // Settle the restore pack pipeline first, then fire -- retrying on the SAME
      // messageId, which the run dedups by signalId, so a retried delivery is
      // idempotent and never spawns a second event (the
      // [section__0, section__1] assertion below stays exact).
      await settleWorkflowRunPacks(env);

      // The 12s inner window is the re-fire cadence, not a budget for the
      // recovery: it decides how long one delivery attempt is given before the
      // next fire, and the retry itself carries no deadline. This test's own
      // budget is the failsafe for an event that is never serviced.
      //
      // Only an unroutable address means "the link is still blipping, fire
      // again". Any other failure -- a signing failure, a malformed address --
      // is a real fault, and with no deadline on this loop, re-firing through
      // it would hide it until the test budget expired and report a hang
      // instead of the fault. So it surfaces here with the fire's error as its
      // cause.
      //
      // The loop fires mail each iteration, so it cannot become a `waitFor`
      // predicate; `env.retrying` is what puts a wedge inside it on the env
      // teardown's in-flight wait report, and `checkTornDown` at the top of
      // each loop body is what lets teardown's stop end it -- ahead of the
      // fire or the read that pass would otherwise make against a dismantled
      // env.
      const secondMessageId =
        "<on-trigger-between-events-2@integration.interchange>";
      await env.retrying(
        `re-fire the event-1 mail until ${SECTION_ID}__1 completes`,
        async (checkTornDown) => {
          for (;;) {
            checkTornDown();
            try {
              await fireMailTrigger(env, deploymentMailAddress, {
                messageId: secondMessageId,
                content: SECOND_BODY,
              });
            } catch (err) {
              if (!isMailTriggerUnroutableError(err)) {
                throw new Error(
                  `on-trigger between-events: firing the event-1 mail failed for a reason other than an unroutable address\n${env.sidecarDiagnostics()}`,
                  { cause: err },
                );
              }
            }
            const reFireAfter = Date.now() + 12_000;
            while (Date.now() < reFireAfter) {
              checkTornDown();
              const events = await readWorkflowRunEvents(
                env,
                DEPLOYMENT_ID,
                containerRunId,
              );
              if (hasChildCompleted(events, `${SECTION_ID}__1`)) return;
              await new Promise((r) => setTimeout(r, 200));
            }
          }
        },
      );

      // ---- assert: one long-lived run serviced both events across the crash --
      const finalEvents = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        containerRunId,
      );
      const finalTypes = finalEvents.map((e) => e.type);

      // Exactly one RunStarted: the container is the SAME run before and after
      // the crash; the restart re-drives it, it does not start a second run.
      expect(finalTypes.filter((t) => t === "RunStarted").length).toBe(1);

      // Both events' bodies were spawned and completed, in order.
      const spawnedChildren = finalEvents.flatMap((e) =>
        e.type === "ChildSpawned" ? [e.body["childRunId"]] : [],
      );
      expect(spawnedChildren).toEqual([`${SECTION_ID}__0`, `${SECTION_ID}__1`]);
      expect(hasChildCompleted(finalEvents, `${SECTION_ID}__0`)).toBe(true);
      expect(hasChildCompleted(finalEvents, `${SECTION_ID}__1`)).toBe(true);

      // The long-lived section never self-completes.
      expect(finalTypes).not.toContain("RunCompleted");
      expect(finalTypes).not.toContain("RunFailed");
      expect(finalTypes).not.toContain("RunCancelled");

      // Load-bearing for the between-events recovery: section__1 exists only
      // because the restored container re-adopted its input park and the supervisor
      // routed mail #2 onto it as event 1 (not a spurious fresh trigger). Its
      // ChildSpawned lands AFTER the restart, so the crash-recovered section
      // serviced a genuinely new event.
      const secondSpawnIndex = finalEvents.findIndex(
        (e) =>
          e.type === "ChildSpawned" &&
          e.body["childRunId"] === `${SECTION_ID}__1`,
      );
      expect(secondSpawnIndex).toBeGreaterThanOrEqual(0);
    }, 180_000);
  },
);

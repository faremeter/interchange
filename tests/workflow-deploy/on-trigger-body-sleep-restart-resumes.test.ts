// onTrigger-body mid-sleep crash-recovery integration test.
//
// The reachability gate for an onTrigger section whose BODY is parked mid-`sleep`
// (`awaiting-timer`) when the sidecar dies. It proves that a restored container
// re-drives the mid-sleep body across a process death and the body's sleep
// resumes and completes -- the end-to-end capstone for INTR-485 on the real
// deploy stack.
//
// Distinct from `on-trigger-between-events-restart.test.ts`, whose sleep body
// (`duration: 10`) completes BEFORE the crash so the CONTAINER is parked between
// events at kill time. Here the body's sleep is long, so the BODY child is still
// parked in `awaiting-timer` at kill time; the container is a live in-flight
// resident awaiting the body terminal, with no container park.
//
//   1. Fire mail #1 -> the container run starts, spawns the body `section__0`,
//      the body sleeps. Wait until `section__0`'s durable log carries the sleep
//      step's `TimerSet` with no `StepCompleted` (parked mid-sleep) and the
//      container has `ChildSpawned(section__0)` with no `ChildCompleted`.
//   2. Quiesce, then KILL the sidecar subprocess while the body sleeps.
//   3. Start a fresh sidecar against the crashed process's SIDECAR_DATA_DIR.
//      Boot-time restore re-arms the body's unfired timer and re-includes the
//      container; the container's resume re-adopts the in-flight body
//      (`planOnTriggerResume` -> `readopt-in-flight-body`), re-spawns it from its
//      log, and the body re-adopts its own sleep timer (`isResumableSleepStep`).
//   4. The re-armed timer fires; the body's sleep completes and the container
//      records `ChildCompleted(section__0)` -- only AFTER the restart.
//
// Load-bearing assertions: exactly one `RunStarted` on the container; exactly one
// `TimerSet` and one `TimerFired` on the body child (no double-count on resume);
// `ChildSpawned` + `ChildCompleted` for `section__0`; the container never reaches
// a terminal event.
//
// Harness justification: SPAWN-REAL. Real hub, real sidecar subprocess, real
// workflow-process child, genuine kill + fresh-sidecar restart against the dead
// process's data dir, so recovery rides the production boot-time restore +
// self-discovery + container-re-drive + body timer re-adopt path.

import fs from "node:fs";

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
import type { RepoId } from "@intx/hub-sessions";

import {
  SESSION_ID,
  SIDECAR_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  listRunIds,
  readWorkflowRunEvents,
  settleWorkflowRunPacks,
  startDeployFlowEnv,
  startSidecarSubprocess,
  waitFor,
  waitForReconnect,
  type DeployFlowEnv,
  type SidecarHandle,
} from "../hub-agent/lib/deploy-flow-env";
import { onTriggerBodyEntry } from "./fixtures/on-trigger-body";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_on-trigger-body-sleep-restart-1";
const SECTION_ID = "section";
const BODY_STEP_ID = "wait";

// Long enough that detecting the body's durable park and killing the sidecar
// reliably beats the timer firing (the body must be parked AT crash time), while
// its persisted `fireAt` stays far inside the test budget so the re-armed timer
// fires after the restart.
const SLEEP_DURATION_MS = 30_000;

const FIRST_BODY = "First event body alpha-7391.";

const TENANT_ID = "tnt_on_trigger_body_sleep_restart";
const CALLER_PRINCIPAL_ID = "prn_on_trigger_body_sleep_restart";
const DEFINITION_ASSET_ID = "ast_on_trigger_body_sleep_restart_wf";

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
    name: "on-trigger-body-sleep-restart-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv({ inferenceEchoUserMessage: true });
});

afterAll(async () => {
  if (restartedSidecar !== undefined) {
    restartedSidecar.proc.kill();
    await restartedSidecar.proc.exited;
  }
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
  for (const dir of restartTempDirs.splice(0)) {
    await fs.promises.rm(dir, { recursive: true, force: true });
  }
});

// The container run is the single run under the deployment's workflow-run repo
// that is NOT a body child. Body children are `${SECTION_ID}__<n>`.
async function findContainerRunId(
  target: DeployFlowEnv,
  workflowRunRepoId: RepoId,
): Promise<string | undefined> {
  const ids = await listRunIds(target, workflowRunRepoId);
  return ids.find((id) => !id.startsWith(`${SECTION_ID}__`));
}

// The event-0 body child run id, once its `runs/` entry exists.
async function findBodyChildRunId(
  target: DeployFlowEnv,
  workflowRunRepoId: RepoId,
): Promise<string | undefined> {
  const ids = await listRunIds(target, workflowRunRepoId);
  return ids.find((id) => id === `${SECTION_ID}__0`);
}

const hasChildCompleted = (
  events: { type: string; body: Record<string, unknown> }[],
  childRunId: string,
): boolean =>
  events.some(
    (e) => e.type === "ChildCompleted" && e.body["childRunId"] === childRunId,
  );

describe.skipIf(!harnessDbEnvAvailable())(
  "onTrigger body parked mid-sleep survives a sidecar crash and completes after restart",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("a body parked mid-sleep resumes across a crash and completes", async () => {
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
        principalId: "prin_on-trigger-body-sleep-restart-1",
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

      // A long-duration sleep body: the section spawns it per event, and it
      // holds the body child mid-`sleep` so the crash lands while it is parked.
      const entryModule = onTriggerBodyEntry({
        address: deploymentMailAddress,
        sectionId: SECTION_ID,
        body: {
          variant: "sleep",
          stepId: BODY_STEP_ID,
          duration: SLEEP_DURATION_MS,
        },
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
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      // ---- fire mail #1, drive the body into its mid-sleep park -------------
      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<on-trigger-body-sleep-restart-1@integration.interchange>",
        content: FIRST_BODY,
      });

      // Wait until the body child is durably parked mid-sleep: its own log carries
      // the sleep step's TimerSet with no StepCompleted, and the container has
      // spawned but not completed it.
      await waitFor(
        async () => {
          const bodyRunId = await findBodyChildRunId(env, workflowRunRepoId);
          const containerRunId = await findContainerRunId(
            env,
            workflowRunRepoId,
          );
          if (bodyRunId === undefined || containerRunId === undefined) {
            return false;
          }
          const bodyEvents = await readWorkflowRunEvents(
            env,
            DEPLOYMENT_ID,
            bodyRunId,
          );
          const parked =
            bodyEvents.some(
              (e) => e.type === "TimerSet" && e.body["stepId"] === BODY_STEP_ID,
            ) && !bodyEvents.some((e) => e.type === "StepCompleted");
          const containerEvents = await readWorkflowRunEvents(
            env,
            DEPLOYMENT_ID,
            containerRunId,
          );
          return parked && !hasChildCompleted(containerEvents, bodyRunId);
        },
        { diagnostics: env.sidecarDiagnostics, timeoutMs: 30_000 },
      );

      const containerRunId = await findContainerRunId(env, workflowRunRepoId);
      if (containerRunId === undefined) {
        throw new Error("no container run under the workflow-run repo");
      }
      const bodyRunId = await findBodyChildRunId(env, workflowRunRepoId);
      if (bodyRunId === undefined) {
        throw new Error("no body child run under the workflow-run repo");
      }

      // ---- quiesce, then CRASH the sidecar while the body sleeps ------------
      await settleWorkflowRunPacks(env);

      const crashedDataDir = env.sidecar.dataDir;
      env.sidecar.proc.kill();
      await env.sidecar.proc.exited;

      await waitFor(
        () =>
          !env.hub.router
            .getRoutableAddresses()
            .includes(deploymentMailAddress),
        { timeoutMs: 10_000, diagnostics: env.sidecarDiagnostics },
      );

      // ---- RESTART: a fresh sidecar against the SAME data dir ---------------
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
      const restoredDiagnostics = (): string =>
        `${env.sidecarDiagnostics()}\nrestored sidecar stderr:\n${restartedSidecar?.stderr.slice(-60).join("") ?? "<none>"}`;

      const reconnectMs = await waitForReconnect(env, deploymentMailAddress, {
        timeoutMs: 30_000,
      });
      expect(reconnectMs).toBeGreaterThan(0);

      // ---- assert: the restored container re-drove the mid-sleep body, its
      //      timer re-armed and fired, and the body completed AFTER the restart.
      await waitFor(
        async () => {
          const containerEvents = await readWorkflowRunEvents(
            env,
            DEPLOYMENT_ID,
            containerRunId,
          );
          return hasChildCompleted(containerEvents, bodyRunId);
        },
        { diagnostics: restoredDiagnostics, timeoutMs: 90_000 },
      );

      const finalContainer = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        containerRunId,
      );
      const finalContainerTypes = finalContainer.map((e) => e.type);

      // The container is the SAME run before and after the crash.
      expect(finalContainerTypes.filter((t) => t === "RunStarted").length).toBe(
        1,
      );
      expect(
        finalContainer.some(
          (e) =>
            e.type === "ChildSpawned" && e.body["childRunId"] === bodyRunId,
        ),
      ).toBe(true);
      expect(hasChildCompleted(finalContainer, bodyRunId)).toBe(true);
      // The long-lived section never self-completes.
      expect(finalContainerTypes).not.toContain("RunCompleted");
      expect(finalContainerTypes).not.toContain("RunFailed");
      expect(finalContainerTypes).not.toContain("RunCancelled");

      // The body child re-adopted its durable timer: exactly one TimerSet /
      // TimerFired pair (no second timer minted on resume) and the sleep step
      // completed.
      const finalBody = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        bodyRunId,
      );
      const finalBodyTypes = finalBody.map((e) => e.type);
      expect(finalBodyTypes.filter((t) => t === "TimerSet").length).toBe(1);
      expect(finalBodyTypes.filter((t) => t === "TimerFired").length).toBe(1);
      expect(
        finalBody.some(
          (e) =>
            e.type === "StepCompleted" && e.body["stepId"] === BODY_STEP_ID,
        ),
      ).toBe(true);
    }, 180_000);
  },
);

// A run that holds hub run tokens calls the hub after a sidecar restart.
//
// The `hub-access-workflow` fixture runs a top-level `nap` sleep first and then
// `callHub`, an action that declares the reserved hub handle and reads its own
// run through the real hub REST app. The test triggers the run through the
// real `/mail` route, so the trigger mints the run's token. The sidecar dies
// while the run sleeps, and a fresh sidecar starts on the same data dir. No new
// trigger reaches the run, so the token the restarted run presents is the one
// it held before the crash.
//
// Before the crash the run log has a `TimerSet` and no `TimerFired`, and no
// request has reached the hub's port. After the restart the log has exactly one
// `TimerSet` and one `TimerFired`, and the hub call carried a run token and got
// a 200.
//
// Harness justification: SPAWN-REAL. Real hub REST app on a real port, real
// sidecar subprocess, real workflow child, and a real sidecar death.

import fs from "node:fs";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { eq } from "drizzle-orm";

import { runToken } from "@intx/db/schema";
import { deriveRunPrincipalId, RUN_TOKEN_PREFIX } from "@intx/hub-common";
import { harnessDbEnvAvailable } from "@intx/test-harness/db-harness";

import {
  readWorkflowRunEvents,
  terminateSidecarSubprocess,
  waitForWorkflowRunComplete,
  type SidecarHandle,
} from "../hub-agent/lib/deploy-flow-env";
import {
  HUB_CALLER_HANDLER,
  hubAccessWorkflowEntry,
} from "./fixtures/hub-access-workflow";
import {
  SELF_READ,
  hubCallOutcomes,
  startHubAccessEnv,
  type HubAccessEnv,
} from "./hub-access-env";
import { failureMessages } from "./nested-tool-invoke-helpers";
import {
  crashSidecar,
  restartSidecar,
  stepEventsOf,
  waitForTimerSet,
} from "./sleep-restart-helpers";

const TENANT_ID = "tnt_sleep_restart_hub";
const OTHER_TENANT_ID = "tnt_sleep_restart_hub_other";
const RUN_ID = "run_sleep-restart-hub";
const NAP_STEP_ID = "nap";

// Long enough that the test detects the park and kills the sidecar before the
// timer fires, while `fireAt` stays far inside the test budget. See
// `sleep-restart-keeps-authority.test.ts`.
const SLEEP_DURATION_MS = 12_000;

let t: HubAccessEnv | undefined;
let restarted: SidecarHandle | undefined;
const restartTempDirs: string[] = [];

beforeAll(async () => {
  // A file-scope beforeAll fires even when describe.skipIf skips the
  // suite bodies, so it needs its own guard or a missing DB env throws
  // here. See the two-shape rule in tests/lib/db-harness.ts.
  if (!harnessDbEnvAvailable()) return;
  t = await startHubAccessEnv({
    tenantId: TENANT_ID,
    callerUserId: "usr_sleep_restart_hub_caller",
    callerPrincipalId: "prn_sleep_restart_hub_caller",
  });
});

afterAll(async () => {
  if (restarted !== undefined) await terminateSidecarSubprocess(restarted);
  if (t !== undefined) await t.teardown();
  for (const dir of restartTempDirs.splice(0)) {
    await fs.promises.rm(dir, { recursive: true, force: true });
  }
});

describe.skipIf(!harnessDbEnvAvailable())(
  "a run with hub tokens calls the hub after a sidecar restart",
  () => {
    test("the resumed run reads its own run with the token it held before the crash", async () => {
      if (t === undefined) throw new Error("env did not start");
      const { env } = t;
      await t.seedTenancy(OTHER_TENANT_ID);
      const ownRunPath = `${t.runsPath}/${RUN_ID}`;
      await t.deploy({
        anchorRunId: RUN_ID,
        actions: "./workflow.mjs",
        entryModule: (address) =>
          hubAccessWorkflowEntry({
            address,
            workflowId: "wf_sleep_restart_hub",
            calls: { ownRun: ownRunPath },
            grantRequirements: [SELF_READ],
            napMs: SLEEP_DURATION_MS,
          }),
      });
      const address = env.deployments.get(RUN_ID)?.mailAddress;
      if (address === undefined) throw new Error("no deployment handle");

      // The trigger runs the anchor itself, so the run id is the anchor id.
      await t.trigger(RUN_ID);
      await waitForTimerSet(env, RUN_ID, RUN_ID, NAP_STEP_ID);
      const parked = await readWorkflowRunEvents(env, RUN_ID, RUN_ID);
      expect(parked.filter((e) => e.type === "TimerFired")).toHaveLength(0);
      expect(
        stepEventsOf(parked, "StepStarted", HUB_CALLER_HANDLER),
      ).toHaveLength(0);
      expect(t.received).toEqual([]);

      // The trigger minted one token for the run principal. Nothing mints
      // another, so the restarted run can only present this one.
      const principalId = await deriveRunPrincipalId(TENANT_ID, RUN_ID);
      const hub = t;
      const runTokenIds = async () =>
        (
          await hub.h.db
            .select({ id: runToken.id })
            .from(runToken)
            .where(eq(runToken.principalId, principalId))
        ).map((row) => row.id);
      const mintedBefore = await runTokenIds();
      expect(mintedBefore).toHaveLength(1);

      const dataDir = await crashSidecar(env, address);
      expect(t.received).toEqual([]);

      restarted = await restartSidecar(env, {
        address,
        dataDir,
        registerTempDir: (dir) => {
          restartTempDirs.push(dir);
        },
      });

      const terminal = await waitForWorkflowRunComplete(env, RUN_ID, RUN_ID, {
        diagnostics: env.sidecarDiagnostics,
      });
      const events = await readWorkflowRunEvents(env, RUN_ID, RUN_ID);
      if (terminal.type !== "RunCompleted") {
        throw new Error(
          `expected RunCompleted, got ${terminal.type}: ${JSON.stringify(failureMessages(events))}\n${env.sidecarDiagnostics()}`,
        );
      }
      expect(events.filter((e) => e.type === "TimerSet")).toHaveLength(1);
      expect(events.filter((e) => e.type === "TimerFired")).toHaveLength(1);

      expect(hubCallOutcomes(events, HUB_CALLER_HANDLER)).toEqual({
        ownRun: { status: 200, id: RUN_ID, message: null },
      });
      // Every request on the hub's port came from the run, and all of them
      // came after the restart.
      expect(t.received).toHaveLength(1);
      const [call] = t.received;
      expect(call?.path).toBe(ownRunPath);
      expect(call?.status).toBe(200);
      expect(
        call?.authorization?.startsWith(`Bearer ${RUN_TOKEN_PREFIX}`),
      ).toBe(true);
      expect(await runTokenIds()).toEqual(mintedBefore);
    }, 180_000);
  },
);

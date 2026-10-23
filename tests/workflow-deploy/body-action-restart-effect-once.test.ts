// A body action whose effect completed before a restart does not perform it
// again on resume.
//
// An action in a `childWorkflow` body, in a loop body, and in an `onTrigger`
// section body each append one marker line inside `perform`, then the body
// parks on its own signal. The park makes the action's `StepCompleted`
// durable. The test kills the sidecar, starts a fresh one on the same data
// dir, and signals each body. The resumed bodies skip the completed action and
// complete, so the marker file still holds one line per body and each body log
// one start and one completion of its action. The effect ledger lives in the
// killed workflow process, so no ledger hit can account for the missing second
// line.
//
// Harness justification: SPAWN-REAL. Real hub, real sidecar subprocess, real
// workflow-process child. The crash is a kill of the sidecar subprocess, and
// the restart rides the production boot-time restore path.

import fs from "node:fs";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { harnessDbEnvAvailable } from "@intx/test-harness/db-harness";

import {
  injectSignal,
  readWorkflowRunEvents,
  settleWorkflowRunPacks,
  startSidecarSubprocess,
  terminateSidecarSubprocess,
  waitFor,
  waitForReconnect,
  type SidecarHandle,
} from "../hub-agent/lib/deploy-flow-env";
import {
  stepEvents,
  startBodyActionRestartEnv,
  type BodyActionRestartEnv,
  type MarkHandler,
} from "./body-action-restart-env";
import {
  CHILD_MARK_HANDLER,
  CHILD_SIGNAL,
  LOOP_MARK_HANDLER,
  LOOP_SIGNAL,
  MARK_HANDLERS,
  SECTION_MARK_HANDLER,
  SECTION_SIGNAL,
} from "./fixtures/body-action-restart-workflow";

const TERMINAL_TYPES = new Set(["RunCompleted", "RunFailed", "RunCancelled"]);

const SIGNAL_OF: Record<MarkHandler, string> = {
  [CHILD_MARK_HANDLER]: CHILD_SIGNAL,
  [LOOP_MARK_HANDLER]: LOOP_SIGNAL,
  [SECTION_MARK_HANDLER]: SECTION_SIGNAL,
};

let t: BodyActionRestartEnv | undefined;
let restartedSidecar: SidecarHandle | undefined;
const restartTempDirs: string[] = [];

beforeAll(async () => {
  // A file-scope beforeAll fires even when describe.skipIf skips the
  // suite bodies, so it needs its own guard or a missing DB env throws
  // here. See the two-shape rule in tests/lib/db-harness.ts.
  if (!harnessDbEnvAvailable()) return;
  t = await startBodyActionRestartEnv({ name: "effect-once", block: false });
});

afterAll(async () => {
  if (restartedSidecar !== undefined) {
    await terminateSidecarSubprocess(restartedSidecar);
  }
  if (t !== undefined) await t.teardown();
  for (const dir of restartTempDirs.splice(0)) {
    await fs.promises.rm(dir, { recursive: true, force: true });
  }
});

describe.skipIf(!harnessDbEnvAvailable())(
  "a completed body action does not perform its effect again after a restart",
  () => {
    test("childWorkflow, loop and onTrigger body actions each mark once across a sidecar restart", async () => {
      if (t === undefined) throw new Error("env did not start");
      const bodies = t;
      const { env, deploymentId, mailAddress, runId } = bodies;

      // Each container relays its body's park onto the top-level run. A body's
      // `StepCompleted` for its action is flushed with its own park, before
      // the relay lands.
      await waitFor(
        async () => {
          const awaited = (
            await readWorkflowRunEvents(env, deploymentId, runId)
          )
            .filter((event) => event.type === "SignalAwaited")
            .map((event) => event.body["signalName"]);
          return MARK_HANDLERS.every((handler) =>
            awaited.includes(SIGNAL_OF[handler]),
          );
        },
        { diagnostics: env.sidecarDiagnostics },
      );

      for (const handler of MARK_HANDLERS) {
        expect(
          stepEvents(
            await bodies.bodyEvents(handler),
            "StepCompleted",
            handler,
          ),
        ).toHaveLength(1);
      }
      expect(bodies.markerLines().sort()).toEqual([...MARK_HANDLERS].sort());

      await settleWorkflowRunPacks(env);
      const crashedDataDir = env.sidecar.dataDir;
      await terminateSidecarSubprocess(env.sidecar);
      await waitFor(
        () => !env.hub.router.getRoutableAddresses().includes(mailAddress),
        { diagnostics: env.sidecarDiagnostics },
      );

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
      await waitForReconnect(env, mailAddress);

      // A signal posted at the top-level run reaches each body through its
      // container's relay. The body run ids are not addresses. The section's
      // relay names a correlation id, which its signal must carry.
      const relays = (
        await readWorkflowRunEvents(env, deploymentId, runId)
      ).filter((event) => event.type === "SignalAwaited");
      for (const handler of MARK_HANDLERS) {
        const correlationId = relays.find(
          (event) => event.body["signalName"] === SIGNAL_OF[handler],
        )?.body["correlationId"];
        await injectSignal(
          env,
          deploymentId,
          runId,
          SIGNAL_OF[handler],
          { resumed: true },
          typeof correlationId === "string" ? correlationId : undefined,
        );
      }

      // The section listens for more mail, so the top-level run does not end.
      // Each body run ends on its own log. A regression can instead repeat an
      // effect, end a body in failure, spawn a replacement body, or end the
      // top-level run, so each of those ends the wait too, and the assertions
      // below name the cause.
      await waitFor(
        async () => {
          if (bodies.markerLines().length > MARK_HANDLERS.length) return true;
          const top = await readWorkflowRunEvents(env, deploymentId, runId);
          if (top.some((event) => TERMINAL_TYPES.has(event.type))) return true;
          if (
            top.filter((event) => event.type === "ChildSpawned").length >
            MARK_HANDLERS.length
          ) {
            return true;
          }
          for (const handler of MARK_HANDLERS) {
            const events = await bodies.bodyEvents(handler);
            if (!events.some((event) => TERMINAL_TYPES.has(event.type))) {
              return false;
            }
          }
          return true;
        },
        { diagnostics: env.sidecarDiagnostics },
      );

      expect(bodies.markerLines().sort()).toEqual([...MARK_HANDLERS].sort());
      const top = await readWorkflowRunEvents(env, deploymentId, runId);
      expect(top.filter((event) => event.type === "ChildSpawned")).toHaveLength(
        MARK_HANDLERS.length,
      );
      expect(
        top.filter((event) => TERMINAL_TYPES.has(event.type)),
      ).toHaveLength(0);
      for (const handler of MARK_HANDLERS) {
        const events = await bodies.bodyEvents(handler);
        expect(
          events
            .filter((event) => TERMINAL_TYPES.has(event.type))
            .map((event) => event.type),
        ).toEqual(["RunCompleted"]);
        expect(stepEvents(events, "StepStarted", handler)).toHaveLength(1);
        expect(stepEvents(events, "StepCompleted", handler)).toHaveLength(1);
      }
    }, 180_000);
  },
);

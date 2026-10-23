// A body action that is in flight when the workflow process dies is not
// invoked again on resume, so its effect does not repeat.
//
// An action in a `childWorkflow` body, in a loop body, and in an `onTrigger`
// section body each append one marker line inside `perform` and then block, so
// no action completes. `runAction` makes each action's `StepStarted` durable
// before the handler runs. The test kills only the workflow process, which
// holds every body's in-memory effect ledger, and lets the supervisor respawn
// it. The resumed bodies must not call a handler again: a second call would
// append a second line, and with no ledger left nothing else stops it. Resume
// settles each action as failed with `crash-mid-invocation`.
//
// Harness justification: SPAWN-REAL. Real hub, real sidecar subprocess, real
// workflow-process child. The crash is a SIGKILL of the workflow process with
// no drain, so the action cannot settle as an ordinary failure first.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { harnessDbEnvAvailable } from "@intx/test-harness/db-harness";

import {
  killWorkflowHostChild,
  listWorkflowHostChildren,
  settleWorkflowRunPacks,
  waitFor,
} from "../hub-agent/lib/deploy-flow-env";
import {
  stepEvents,
  startBodyActionRestartEnv,
  type BodyActionRestartEnv,
} from "./body-action-restart-env";
import { MARK_HANDLERS } from "./fixtures/body-action-restart-workflow";

let t: BodyActionRestartEnv | undefined;

beforeAll(async () => {
  // A file-scope beforeAll fires even when describe.skipIf skips the
  // suite bodies, so it needs its own guard or a missing DB env throws
  // here. See the two-shape rule in tests/lib/db-harness.ts.
  if (!harnessDbEnvAvailable()) return;
  t = await startBodyActionRestartEnv({
    name: "crash-mid-effect",
    block: true,
  });
});

afterAll(async () => {
  if (t !== undefined) await t.teardown();
});

describe.skipIf(!harnessDbEnvAvailable())(
  "an in-flight body action is not invoked again after the workflow process dies",
  () => {
    test("childWorkflow, loop and onTrigger body actions each mark once across a workflow-process kill", async () => {
      if (t === undefined) throw new Error("env did not start");
      const bodies = t;
      const { env } = bodies;

      // A marker line is written after the action's durable `StepStarted`, so
      // one line per body means every action is in flight on the log.
      await waitFor(
        () => bodies.markerLines().length === MARK_HANDLERS.length,
        { diagnostics: env.sidecarDiagnostics },
      );
      expect(bodies.markerLines().sort()).toEqual([...MARK_HANDLERS].sort());

      await settleWorkflowRunPacks(env);
      const killedPids = killWorkflowHostChild(env);
      expect(env.sidecar.proc.exitCode).toBeNull();
      await waitFor(
        () =>
          listWorkflowHostChildren(env).some(
            (pid) => !killedPids.includes(pid),
          ),
        { diagnostics: env.sidecarDiagnostics },
      );

      // A regression calls a handler again, and that call blocks too, so its
      // action never settles. An extra marker line ends the wait instead, and
      // the assertions below name the repeat.
      await waitFor(
        async () => {
          if (bodies.markerLines().length > MARK_HANDLERS.length) return true;
          for (const handler of MARK_HANDLERS) {
            const events = await bodies.bodyEvents(handler);
            if (stepEvents(events, "StepFailed", handler).length === 0) {
              return false;
            }
          }
          return true;
        },
        { diagnostics: env.sidecarDiagnostics },
      );

      expect(bodies.markerLines().sort()).toEqual([...MARK_HANDLERS].sort());

      // Resume settles each in-flight action as failed rather than invoking it
      // again.
      for (const handler of MARK_HANDLERS) {
        const events = await bodies.bodyEvents(handler);
        expect(stepEvents(events, "StepStarted", handler)).toHaveLength(1);
        expect(stepEvents(events, "StepCompleted", handler)).toHaveLength(0);
        expect(
          stepEvents(events, "StepFailed", handler).map(
            (event) => event.body["error"],
          ),
        ).toEqual([expect.objectContaining({ code: "crash-mid-invocation" })]);
      }
    }, 180_000);
  },
);

// Helpers the sleep-restart authority tests share: find the parked timer on a
// run's log, crash the sidecar while the run sleeps, and start a fresh sidecar
// on the crashed process's data dir.

import {
  readWorkflowRunEvents,
  settleWorkflowRunPacks,
  startSidecarSubprocess,
  terminateSidecarSubprocess,
  waitFor,
  waitForReconnect,
  type DeployFlowEnv,
  type SidecarHandle,
  type WorkflowRunEvent,
} from "../hub-agent/lib/deploy-flow-env";

/** The events of `type` for `stepId` in a run log. */
export function stepEventsOf(
  events: readonly WorkflowRunEvent[],
  type: string,
  stepId: string,
): WorkflowRunEvent[] {
  return events.filter(
    (event) => event.type === type && event.body["stepId"] === stepId,
  );
}

/** Wait until the run's log carries the `TimerSet` of the sleep `stepId`. */
export async function waitForTimerSet(
  env: DeployFlowEnv,
  anchorRunId: string,
  runId: string,
  stepId: string,
): Promise<void> {
  await waitFor(
    async () => {
      const events = await readWorkflowRunEvents(env, anchorRunId, runId);
      return stepEventsOf(events, "TimerSet", stepId).length > 0;
    },
    { diagnostics: env.sidecarDiagnostics },
  );
}

/**
 * Wait until the `fireAt` of the sleep `stepId` has passed, so a restart
 * finds its timer already due. The wait is for a recorded clock time, not a
 * guessed deadline.
 */
export async function waitUntilTimerDue(
  env: DeployFlowEnv,
  anchorRunId: string,
  runId: string,
  stepId: string,
): Promise<void> {
  const remaining =
    (await timerFireAt(env, anchorRunId, runId, stepId)) - Date.now();
  if (remaining >= 0) {
    await new Promise((resolve) => setTimeout(resolve, remaining + 1));
  }
}

/** The `fireAt` of the sleep `stepId`, in epoch milliseconds. */
export async function timerFireAt(
  env: DeployFlowEnv,
  anchorRunId: string,
  runId: string,
  stepId: string,
): Promise<number> {
  const events = await readWorkflowRunEvents(env, anchorRunId, runId);
  const fireAt = stepEventsOf(events, "TimerSet", stepId)[0]?.body["fireAt"];
  if (typeof fireAt !== "string") {
    throw new Error(`no TimerSet with a fireAt for step ${stepId}`);
  }
  return Date.parse(fireAt);
}

/**
 * Quiesce the pack stream, then kill the env's sidecar and wait until the hub
 * no longer routes `address`. Returns the dead sidecar's data dir.
 *
 * `terminateSidecarSubprocess` sends SIGTERM first. The sidecar installs no
 * SIGTERM handler, so that is already an ungraceful death. The helper also
 * reaps the workflow child. A raw SIGKILL of the sidecar would orphan the
 * child, and the orphan would still hold the run's authority.
 */
export async function crashSidecar(
  env: DeployFlowEnv,
  address: string,
): Promise<string> {
  await settleWorkflowRunPacks(env);
  const dataDir = env.sidecar.dataDir;
  await terminateSidecarSubprocess(env.sidecar);
  await waitFor(
    () => !env.hub.router.getRoutableAddresses().includes(address),
    { diagnostics: env.sidecarDiagnostics },
  );
  return dataDir;
}

/**
 * Start a fresh sidecar on `dataDir`, register it on the env, and wait until
 * the hub routes `address` again. The caller owns the returned handle.
 */
export async function restartSidecar(
  env: DeployFlowEnv,
  opts: {
    address: string;
    dataDir: string;
    registerTempDir: (dir: string) => void;
  },
): Promise<SidecarHandle> {
  const hubPort = env.hub.server.port;
  if (hubPort === undefined) {
    throw new Error("hub.server.port is undefined after the crash");
  }
  const restarted = await startSidecarSubprocess({
    hubPort,
    registerTempDir: opts.registerTempDir,
    extraEnv: { SIDECAR_DATA_DIR: opts.dataDir },
  });
  env.registerSidecar(restarted);
  await waitForReconnect(env, opts.address);
  return restarted;
}

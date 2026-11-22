import type { SuspendableChildSettlement } from "@intx/workflow";
import type { SignalChannelHandle } from "../seams/signal-channel";

/**
 * The teardown that a spawned run's handle runs once the run settles. A
 * `childWorkflow` child, an onTrigger body and a loop iteration each get
 * their handle from `createSuspendableChildHandle`, which calls this once on
 * every settlement. The teardown always stops the run's own signal channel.
 *
 * It reclaims the run's local storage only when the run's terminal event is
 * durable (`terminal`). When the run's `complete` rejects (`uncommitted`),
 * the log has no terminal, and a restart recovers the run from its durable
 * prefix. That recovery reads the local store of a parked step, so the
 * directory stays. When nothing recovers the run, the directory stays until
 * undeploy. A run that stays parked does not settle, so its storage stays
 * while the park is open.
 *
 * The handle does not await or catch the teardown, so `cleanupRunStorage`
 * must not reject. The storage is reclaimed also when the channel stop fails.
 * `cleanupRunStorage` is absent only in a test or host that roots no local
 * storage for a run.
 */
export function createSpawnedRunCleanup(args: {
  signalChannel: Pick<SignalChannelHandle, "stop">;
  cleanupRunStorage: ((runId: string) => Promise<void>) | undefined;
  childRunId: string;
}): (settlement: SuspendableChildSettlement) => Promise<void> {
  const { signalChannel, cleanupRunStorage, childRunId } = args;
  return async (settlement) => {
    try {
      await signalChannel.stop();
    } finally {
      if (settlement.kind === "terminal") {
        await cleanupRunStorage?.(childRunId);
      }
    }
  };
}

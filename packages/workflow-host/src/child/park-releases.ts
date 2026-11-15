import { classifyTerminalEvent } from "@intx/hub-sessions/substrate";
import { getLogger } from "@intx/log";
import type { ControlParkKind } from "@intx/types/runtime-core";
import { correlationIdFromSignalName } from "@intx/types/signals";
import type { RepoStore, WorkflowEvent } from "@intx/workflow";

import type { ControlChannelSender } from "../ipc/control-sender";

const logger = getLogger(["workflow-host", "child"]);

type ParkRelease = { runId: string; correlationId?: string };

type ParkedEntry = {
  runId: string;
  correlationId: string;
  parkKind: ControlParkKind;
};

/** A read of committed history that leaves out what was released meanwhile. */
export interface ParkReleaseRead {
  /**
   * End the read and drop the approval waits it released while their writes
   * had not landed. Synchronous, so the caller can send the result before
   * any later release is sent.
   */
  leaveOutReleased<T extends ParkedEntry>(parked: readonly T[]): T[];
}

/**
 * The child's releases of approval waits. Every store that writes a run
 * which registers approvals takes `wrap`, so a write that retires a wait or
 * ends the run sends `park.release` first, whichever store flushes it. A run
 * spawned by another run registers none, since its parent relays its parks,
 * so a store that writes only child and body runs goes without.
 */
export interface ParkReleases {
  wrap(store: RepoStore): RepoStore;
  /**
   * Start a read of committed history. It leaves out what was held when it
   * began or was released before `leaveOutReleased`.
   */
  beginRead(): ParkReleaseRead;
}

/**
 * Send `park.release` ahead of every write that retires a wait on an
 * approval or ends its run, and hold it until that write settles. The sender
 * queues in call order and the write's own request takes the same sender, so
 * the supervisor stops resending the register before it can commit that
 * history. A send that fails is logged: the write that follows takes the same
 * channel and fails with it. A write that fails after its release leaves the
 * resends stopped until the run parks again or a later re-registration lists
 * the wait.
 */
export function createParkReleases(
  upstreamSender: ControlChannelSender,
): ParkReleases {
  const held = new Set<ParkRelease>();
  const reads = new Set<ParkRelease[]>();

  function hold(release: ParkRelease): () => void {
    held.add(release);
    for (const seen of reads) seen.push(release);
    return () => {
      held.delete(release);
    };
  }

  function release(
    runId: string,
    events: readonly WorkflowEvent[],
  ): () => void {
    const settles: (() => void)[] = [];
    for (const event of events) {
      const data = parkReleaseOf(runId, event);
      if (data === undefined) continue;
      settles.push(hold(data));
      upstreamSender
        .send({ type: "park.release", data })
        .catch((cause: unknown) => {
          const message =
            cause instanceof Error ? cause.message : String(cause);
          logger.error`park.release upstream send failed for runId=${runId}: ${message}`;
        });
    }
    return () => {
      for (const settle of settles) settle();
    };
  }

  return {
    wrap(store) {
      return {
        read: (runId) => store.read(runId),
        async append(runId, event) {
          const settle = release(runId, [event]);
          try {
            await store.append(runId, event);
          } finally {
            settle();
          }
        },
        async appendBatch(runId, events) {
          const settle = release(runId, events);
          try {
            await store.appendBatch(runId, events);
          } finally {
            settle();
          }
        },
        subscribe: (runId, opts) => store.subscribe(runId, opts),
      };
    },
    beginRead() {
      const seen = [...held];
      reads.add(seen);
      return {
        leaveOutReleased(parked) {
          reads.delete(seen);
          return parked.filter(
            (entry) =>
              entry.parkKind !== "approval" ||
              !seen.some((release) => releases(release, entry)),
          );
        },
      };
    },
  };
}

function releases(release: ParkRelease, entry: ParkedEntry): boolean {
  return (
    release.runId === entry.runId &&
    (release.correlationId === undefined ||
      release.correlationId === entry.correlationId)
  );
}

function parkReleaseOf(
  runId: string,
  event: WorkflowEvent,
): ParkRelease | undefined {
  if (event.kind === "SignalAwaitAbandoned") {
    const correlationId = correlationIdFromSignalName(event.signalName);
    return correlationId === undefined ? undefined : { runId, correlationId };
  }
  return classifyTerminalEvent(event.kind).terminal ? { runId } : undefined;
}

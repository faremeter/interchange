import { withRunCommitBarrier, type RepoStore } from "@intx/workflow/runtime";

import type { ControlChannelSender, ControlPayload } from "../ipc";

type PrepareCancellation = Extract<
  ControlPayload,
  { type: "cancel.prepare" }
>["data"];
type CancellationCommitted = Extract<
  ControlPayload,
  { type: "cancel.committed" }
>["data"];

/** Hold the runtime's event buffer until the supervisor finishes its signed write. */
export function createCancellationBarrier(
  repoStore: RepoStore,
  sender: ControlChannelSender,
) {
  const pending = new Map<string, (error: string | undefined) => void>();
  let closed: string | undefined;

  return {
    async prepare(data: PrepareCancellation): Promise<void> {
      if (closed !== undefined) throw new Error(closed);
      const committed = Promise.withResolvers<string | undefined>();
      pending.set(data.requestId, committed.resolve);
      let prepared = false;
      try {
        await withRunCommitBarrier({ repoStore }, data.runId, async () => {
          if (closed !== undefined) throw new Error(closed);
          await sender.send({
            type: "cancel.prepared",
            data: { requestId: data.requestId },
          });
          prepared = true;
          const error = await committed.promise;
          if (error !== undefined) throw new Error(error);
        });
      } catch (cause) {
        if (!prepared && closed === undefined) {
          await sender.send({
            type: "cancel.prepared",
            data: {
              requestId: data.requestId,
              error: cause instanceof Error ? cause.message : String(cause),
            },
          });
        }
        throw cause;
      } finally {
        pending.delete(data.requestId);
      }
    },
    complete(data: CancellationCommitted): void {
      pending.get(data.requestId)?.(data.error);
    },
    close(reason: string): void {
      closed = reason;
      for (const resolve of pending.values()) resolve(reason);
      pending.clear();
    },
  };
}

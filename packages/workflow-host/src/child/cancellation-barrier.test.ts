import { expect, test } from "bun:test";

import { createInMemoryRepoStore } from "@intx/workflow/runlocal";
import { withRunCommitBarrier } from "@intx/workflow/runtime";

import type { ControlPayload } from "../ipc";
import { createCancellationBarrier } from "./cancellation-barrier";

test.each(["commit", "failure", "disconnect"] as const)(
  "cancellation keeps runtime writes paused until %s and then releases them",
  async (outcome) => {
    const repoStore = createInMemoryRepoStore();
    const ready = Promise.withResolvers<undefined>();
    const sent: ControlPayload[] = [];
    const barrier = createCancellationBarrier(repoStore, {
      seq: 0,
      send: async (payload) => {
        sent.push(payload);
        ready.resolve(undefined);
      },
    });
    const runId = `run_barrier_${outcome}`;
    const preparing = barrier.prepare({
      requestId: "cancel-1",
      runId,
      reason: "Stop",
    });
    const result = preparing.then(
      () => "committed",
      (cause: unknown) =>
        cause instanceof Error ? cause.message : String(cause),
    );
    await ready.promise;
    expect(sent).toEqual([
      { type: "cancel.prepared", data: { requestId: "cancel-1" } },
    ]);

    let nextWriteStarted = false;
    const nextWrite = withRunCommitBarrier({ repoStore }, runId, async () => {
      nextWriteStarted = true;
    });
    // A reply for another request must not release this run's writer.
    barrier.complete({ requestId: "cancel-other" });
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(nextWriteStarted).toBe(false);

    if (outcome === "commit") barrier.complete({ requestId: "cancel-1" });
    else if (outcome === "failure")
      barrier.complete({ requestId: "cancel-1", error: "Write failed" });
    else barrier.close("Child disconnected");

    expect(await result).toBe(
      outcome === "commit"
        ? "committed"
        : outcome === "failure"
          ? "Write failed"
          : "Child disconnected",
    );
    await nextWrite;
    expect(nextWriteStarted).toBe(true);
  },
);

import { describe, test, expect } from "bun:test";

import type { RepoStore } from "../runtime/env";
import type { WorkflowEvent } from "../state-machine/index";
import { createInMemoryRepoStore } from "../runlocal/repo-store";
import { waitForEvent, waitForNthEvent } from "./log-waiters";

const RUN = "run-waiters";

// `DEFAULT_BUFFER_LIMIT` in `packages/workflow/src/runlocal/repo-store.ts`.
// Both waiters subscribe from seq 0 and pass no `bufferLimit`, so this is how
// much already-committed log a waiter can replay through.
const REPLAY_LIMIT = 1024;

function runStarted(seq: number, definitionHash = "hash"): WorkflowEvent {
  return {
    kind: "RunStarted",
    seq,
    at: new Date(0).toISOString(),
    runId: RUN,
    definitionHash,
    trigger: { type: "manual", payload: null },
  };
}

describe("waitForEvent", () => {
  test("is satisfied by an event committed before the call", async () => {
    const store = createInMemoryRepoStore();
    await store.append(RUN, runStarted(1));
    // Subscribing from seq 0 replays what is already committed, so a caller
    // that arrives late still sees the event rather than waiting for another.
    const event = await waitForEvent(
      store,
      RUN,
      (e) => e.kind === "RunStarted",
    );
    expect(event.kind).toBe("RunStarted");
  });

  test("resolves on an event committed after the call", async () => {
    const store = createInMemoryRepoStore();
    const waited = waitForEvent(store, RUN, (e) => e.kind === "RunStarted");
    await store.append(RUN, runStarted(1));
    expect((await waited).kind).toBe("RunStarted");
  });

  test("is satisfied by a log exactly at the subscription replay limit", async () => {
    const store = createInMemoryRepoStore();
    for (let seq = 1; seq <= REPLAY_LIMIT; seq++) {
      await store.append(RUN, runStarted(seq));
    }
    const event = await waitForEvent(store, RUN, (e) => e.seq === REPLAY_LIMIT);
    expect(event.seq).toBe(REPLAY_LIMIT);
  });

  test("throws rather than matching once the log passes the replay limit", async () => {
    const store = createInMemoryRepoStore();
    for (let seq = 1; seq <= REPLAY_LIMIT + 1; seq++) {
      await store.append(RUN, runStarted(seq));
    }
    // The replay the two tests above depend on is staged through the
    // subscription buffer, so it is bounded by that buffer rather than by the
    // log. Past the bound the waiter stops being "resolves on the matching
    // event" and becomes "throws", which is a run length away from any test
    // that subscribes from seq 0.
    await expect(
      waitForEvent(store, RUN, (e) => e.seq === REPLAY_LIMIT + 1),
    ).rejects.toThrow(/repo_store_subscribe_buffer_overrun/);
  });
});

describe("waitForNthEvent", () => {
  test("returns the count-th match, not the most recent", async () => {
    const store = createInMemoryRepoStore();
    await store.append(RUN, runStarted(1));
    await store.append(RUN, runStarted(2, "second"));
    await store.append(RUN, runStarted(3, "third"));
    // The contract the park waiter depends on: asking for the second match
    // yields the second, even though three are already committed.
    const second = await waitForNthEvent(
      store,
      RUN,
      (e) => e.kind === "RunStarted",
      2,
    );
    expect(second.seq).toBe(2);
  });

  test("a count below one is refused rather than silently treated as one", async () => {
    const store = createInMemoryRepoStore();
    await expect(waitForNthEvent(store, RUN, () => true, 0)).rejects.toThrow(
      /positive integer/,
    );
  });

  test("ends the subscription when the loop is left by a throw", async () => {
    const store = createInMemoryRepoStore();
    await store.append(RUN, runStarted(1));
    const signals: AbortSignal[] = [];
    const observed: RepoStore = {
      ...store,
      subscribe(runId, opts) {
        signals.push(opts.signal);
        return store.subscribe(runId, opts);
      },
    };

    await expect(
      waitForNthEvent(observed, RUN, () => {
        throw new Error("predicate threw");
      }),
    ).rejects.toThrow(/predicate threw/);

    // Asserted on the signal rather than on the store's subscriber set:
    // leaving a `for await` by a throw makes the iteration protocol call the
    // iterator's `return()`, which this store implements by closing the
    // subscription. Store state would therefore look correct even if the
    // waiter never aborted. The abort is what ends the subscription for an
    // implementation whose iterator does not honour `return()`, so the abort
    // itself is the thing worth asserting.
    const [signal] = signals;
    if (signal === undefined) {
      throw new Error("waitForNthEvent did not subscribe");
    }
    expect(signal.aborted).toBe(true);
  });
});

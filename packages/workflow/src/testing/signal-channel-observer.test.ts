import { describe, test, expect } from "bun:test";

import { createObservedSignalChannel } from "./signal-channel-observer";

const NAME = "__signal__:corr-1";

describe("createObservedSignalChannel counting", () => {
  test("a name the run has not awaited reports zero", () => {
    const channel = createObservedSignalChannel();
    expect(channel.awaitedCount(NAME)).toBe(0);
  });

  test("counts awaitNext calls per name, keeping names independent", async () => {
    const channel = createObservedSignalChannel();
    // Park on two names, one of them twice. Nothing delivers, so the awaits
    // stay pending; the counts are what this asserts, so the promises are
    // deliberately left unsettled and are resolved by the deliveries below.
    void channel.awaitNext(NAME);
    void channel.awaitNext(NAME);
    void channel.awaitNext("other");

    expect(channel.awaitedCount(NAME)).toBe(2);
    expect(channel.awaitedCount("other")).toBe(1);
    expect(channel.awaitedCount("never-awaited")).toBe(0);

    // Settle the three parked awaiters so no unresolved promise outlives the
    // test: a test owns every async operation it starts.
    await channel.deliver(NAME, { n: 1 }, "s1");
    await channel.deliver(NAME, { n: 2 }, "s2");
    await channel.deliver("other", { n: 3 }, "s3");
  });
});

describe("createObservedSignalChannel awaitAwaitedCount", () => {
  // The level-triggered property, and the reason this helper counts instead
  // of reporting the next call. A test does not control whether the run parks
  // before or after it asks to be told; an edge-triggered wait deadlocks on
  // the park-first interleaving, which is the common one.
  test("is satisfied by a park that happened BEFORE the call", async () => {
    const channel = createObservedSignalChannel();
    const parked = channel.awaitNext(NAME);
    expect(channel.awaitedCount(NAME)).toBe(1);

    await channel.awaitAwaitedCount(NAME, 1);

    await channel.deliver(NAME, { ok: true }, "s1");
    await parked;
  });

  test("resolves on a park that happens AFTER the call", async () => {
    const channel = createObservedSignalChannel();
    const waited = channel.awaitAwaitedCount(NAME, 1);
    const parked = channel.awaitNext(NAME);
    await waited;

    await channel.deliver(NAME, { ok: true }, "s1");
    await parked;
  });

  test("waits for the Nth park, not the first", async () => {
    const channel = createObservedSignalChannel();
    const waited = channel.awaitAwaitedCount(NAME, 2);

    const first = channel.awaitNext(NAME);
    // One park is not two. Give the waiter every chance to resolve early: a
    // macrotask turn drains the microtasks its continuation would run on.
    let resolved = false;
    void waited.then(() => {
      resolved = true;
    });
    await new Promise((r) => setTimeout(r, 0));
    expect(resolved).toBe(false);

    // The first awaiter must settle before the run would park again, which is
    // what the real re-park sequence does.
    await channel.deliver(NAME, { n: 1 }, "s1");
    await first;
    const second = channel.awaitNext(NAME);
    await waited;
    expect(channel.awaitedCount(NAME)).toBe(2);

    await channel.deliver(NAME, { n: 2 }, "s2");
    await second;
  });

  test("a park on a DIFFERENT name does not satisfy the wait", async () => {
    const channel = createObservedSignalChannel();
    const waited = channel.awaitAwaitedCount(NAME, 1);

    const other = channel.awaitNext("other");
    // A park on another name must not wake this waiter. As above, the yield is
    // a yield and not a duration bet: it drains the microtasks the waiter's
    // continuation would run on, so a wrongly-woken waiter is caught here.
    let resolved = false;
    void waited.then(() => {
      resolved = true;
    });
    await new Promise((r) => setTimeout(r, 0));
    expect(resolved).toBe(false);

    const wanted = channel.awaitNext(NAME);
    await waited;

    await channel.deliver("other", { ok: true }, "s1");
    await channel.deliver(NAME, { ok: true }, "s2");
    await other;
    await wanted;
  });

  test("a count below one is refused rather than silently treated as one", async () => {
    const channel = createObservedSignalChannel();
    await expect(channel.awaitAwaitedCount(NAME, 0)).rejects.toThrow(
      /positive integer/,
    );
    await expect(channel.awaitAwaitedCount(NAME, 1.5)).rejects.toThrow(
      /positive integer/,
    );
  });
});

describe("createObservedSignalChannel passthrough", () => {
  test("delivery reaches a live awaiter, payload and signalId intact", async () => {
    const channel = createObservedSignalChannel();
    const parked = channel.awaitNext(NAME);
    await channel.deliver(NAME, { outcome: "approved" }, "sig-1");
    expect(await parked).toEqual({
      payload: { outcome: "approved" },
      signalId: "sig-1",
    });
  });

  test("a delivery before the park is queued for it, as the unwrapped channel does", async () => {
    const channel = createObservedSignalChannel();
    await channel.deliver(NAME, { queued: true }, "sig-q");
    // The wrapper must not change the queueing semantics the runtime relies
    // on: a signal that arrives before the park is not lost.
    expect(await channel.awaitNext(NAME)).toEqual({
      payload: { queued: true },
      signalId: "sig-q",
    });
    // The park still counts even though it was satisfied from the queue.
    expect(channel.awaitedCount(NAME)).toBe(1);
  });

  test("an aborted await rejects and still counts as a park", async () => {
    const channel = createObservedSignalChannel();
    const abort = new AbortController();
    const parked = channel.awaitNext(NAME, abort.signal);
    expect(channel.awaitedCount(NAME)).toBe(1);
    abort.abort();
    await expect(parked).rejects.toThrow(/aborted/);
  });
});

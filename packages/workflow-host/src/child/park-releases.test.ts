import { describe, expect, test } from "bun:test";

import { signalName } from "@intx/types";
import type {
  RepoStore as RuntimeRepoStore,
  WorkflowEvent,
} from "@intx/workflow";
import { commitBuffered, withRunCommitBarrier } from "@intx/workflow/runtime";

import type { ControlChannelSender } from "../ipc/control-sender";
import { createParkReleases, type ParkReleases } from "./park-releases";

describe("createParkReleases", () => {
  const at = "2026-01-01T00:00:00.000Z";
  const retired: WorkflowEvent = {
    kind: "SignalAwaitAbandoned",
    seq: 4,
    at,
    stepId: "ask",
    signalName: signalName("corr-retired"),
  };
  const gateRetired: WorkflowEvent = {
    kind: "SignalAwaitAbandoned",
    seq: 5,
    at,
    stepId: "gate",
    signalName: "author-gate",
  };
  const ended: WorkflowEvent = { kind: "RunCancelled", seq: 6, at };

  // One log for releases and writes, so the test reads their order.
  function recording(): { store: RuntimeRepoStore; log: string[] } {
    const log: string[] = [];
    const sender: ControlChannelSender = {
      seq: 0,
      send: (payload) => {
        log.push(
          payload.type === "park.release"
            ? `release ${JSON.stringify(payload.data)}`
            : payload.type,
        );
        return Promise.resolve();
      },
    };
    const inner: RuntimeRepoStore = {
      read: () => Promise.resolve([]),
      append: (_runId, event) => {
        log.push(`write ${event.kind}`);
        return Promise.resolve();
      },
      appendBatch: (_runId, events) => {
        log.push(`write ${events.map((event) => event.kind).join(",")}`);
        return Promise.resolve();
      },
      subscribe: () => {
        throw new Error("not subscribed in this test");
      },
    };
    return { store: createParkReleases(sender).wrap(inner), log };
  }

  test("releases a retired approval wait before the write that records it", async () => {
    const { store, log } = recording();
    await store.append("run-1", retired);
    expect(log).toEqual([
      `release ${JSON.stringify({ runId: "run-1", correlationId: "corr-retired" })}`,
      "write SignalAwaitAbandoned",
    ]);
  });

  test("releases every wait of a run before the write that ends it", async () => {
    const { store, log } = recording();
    await store.appendBatch("run-1", [gateRetired, ended]);
    expect(log).toEqual([
      `release ${JSON.stringify({ runId: "run-1" })}`,
      "write SignalAwaitAbandoned,RunCancelled",
    ]);
  });

  // A store whose writes wait until the test lets them land, so a read can
  // run while one is in flight.
  function heldWrites(fail = false): {
    store: RuntimeRepoStore;
    releases: ParkReleases;
    land: () => void;
  } {
    const landed = Promise.withResolvers<undefined>();
    const inner: RuntimeRepoStore = {
      read: () => Promise.resolve([]),
      append: async () => {
        await landed.promise;
        if (fail) throw new Error("write failed");
      },
      appendBatch: async () => {
        await landed.promise;
        if (fail) throw new Error("write failed");
      },
      subscribe: () => {
        throw new Error("not subscribed in this test");
      },
    };
    const sender: ControlChannelSender = {
      seq: 0,
      send: () => Promise.resolve(),
    };
    const releases = createParkReleases(sender);
    return {
      store: releases.wrap(inner),
      releases,
      land: () => {
        landed.resolve(undefined);
      },
    };
  }

  const parked = (runId: string, correlationId: string) => ({
    runId,
    correlationId,
    parkKind: "approval" as const,
  });

  test("a read leaves out a wait whose retire is still being written", async () => {
    const { store, releases, land } = heldWrites();
    const writing = store.append("run-1", retired);

    const read = releases.beginRead();
    expect(
      read.leaveOutReleased([
        parked("run-1", "corr-retired"),
        parked("run-1", "corr-held"),
      ]),
    ).toEqual([parked("run-1", "corr-held")]);

    land();
    await writing;
    const after = releases.beginRead();
    expect(after.leaveOutReleased([parked("run-1", "corr-retired")])).toEqual([
      parked("run-1", "corr-retired"),
    ]);
  });

  test("a read leaves out a run that ended while the read ran", async () => {
    const { store, releases, land } = heldWrites();
    const read = releases.beginRead();
    const writing = store.append("run-1", ended);
    land();
    await writing;

    // The write landed before the answer went out, but the read may have
    // been taken before it did.
    expect(
      read.leaveOutReleased([
        parked("run-1", "corr-any"),
        parked("run-2", "corr-other"),
      ]),
    ).toEqual([parked("run-2", "corr-other")]);
  });

  test("a read keeps an input park of a run that ended", async () => {
    const { store, releases, land } = heldWrites();
    const read = releases.beginRead();
    const writing = store.append("run-1", ended);
    land();
    await writing;

    const input = {
      runId: "run-1",
      correlationId: "corr-in",
      parkKind: "input" as const,
    };
    expect(read.leaveOutReleased([input])).toEqual([input]);
  });

  test("a failed write stops leaving its wait out of later reads", async () => {
    const { store, releases, land } = heldWrites(true);
    const writing = store.append("run-1", retired);
    land();
    await expect(writing).rejects.toThrow("write failed");

    const read = releases.beginRead();
    expect(read.leaveOutReleased([parked("run-1", "corr-retired")])).toEqual([
      parked("run-1", "corr-retired"),
    ]);
  });

  test("a barrier through another wrapped store releases a buffered run end before writing it", async () => {
    const log: string[] = [];
    const durable: WorkflowEvent[] = [];
    const sender: ControlChannelSender = {
      seq: 0,
      send: (payload) => {
        log.push(`send ${payload.type}`);
        return Promise.resolve();
      },
    };
    const store = (tag: string): RuntimeRepoStore => ({
      read: () => Promise.resolve([...durable]),
      append: (_runId, event) => {
        durable.push(event);
        log.push(`${tag} write ${event.kind}`);
        return Promise.resolve();
      },
      appendBatch: (_runId, events) => {
        durable.push(...events);
        log.push(`${tag} write ${events.map((event) => event.kind).join(",")}`);
        return Promise.resolve();
      },
      subscribe: () => {
        throw new Error("not subscribed in this test");
      },
    });
    // Production wraps the runtime's and the scheduler's stores with one
    // set of releases.
    const releases = createParkReleases(sender);
    const runtime = releases.wrap(store("runtime"));
    const scheduler = releases.wrap(store("scheduler"));
    const runId = "run-barrier";
    await runtime.append(runId, {
      kind: "RunStarted",
      seq: 1,
      at,
      runId,
      definitionHash: "h",
      trigger: { type: "t", payload: null },
    });
    log.length = 0;
    await commitBuffered({ repoStore: runtime }, runId, {
      kind: "RunFailed",
      seq: 2,
      at,
      error: { message: "boom" },
    });

    await withRunCommitBarrier({ repoStore: scheduler }, runId, () =>
      Promise.resolve(),
    );

    expect(log).toEqual(["send park.release", "scheduler write RunFailed"]);
  });
});

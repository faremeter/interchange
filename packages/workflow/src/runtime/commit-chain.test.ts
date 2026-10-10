import { expect, test } from "bun:test";

import { createInMemoryRepoStore } from "../runlocal/repo-store";
import {
  commit,
  commitBuffered,
  dropChain,
  flushChain,
  reloadState,
  withRunCommitBarrier,
} from "./commit-chain";

test("external cancellation flushes buffered progress and excludes later runtime commits", async () => {
  const runId = "run_cancellation_barrier";
  const env = { repoStore: createInMemoryRepoStore() };
  const at = "2026-01-01T00:00:00.000Z";
  await commit(env, runId, {
    kind: "RunStarted",
    seq: 1,
    at,
    runId,
    definitionHash: "definition",
    trigger: { type: "manual", payload: null },
  });
  await commit(env, runId, {
    kind: "StepStarted",
    seq: 2,
    at,
    stepId: "work",
    attempt: 1,
    input: { ref: "inline:null" },
  });
  await commitBuffered(env, runId, {
    kind: "StepCompleted",
    seq: 3,
    at,
    stepId: "work",
    attempt: 1,
    output: { ref: "inline:null" },
  });

  const prepared = Promise.withResolvers<undefined>();
  const finishCancellation = Promise.withResolvers<undefined>();
  const cancellation = withRunCommitBarrier(env, runId, async () => {
    const events = await env.repoStore.read(runId);
    expect(events.at(-1)?.kind).toBe("StepCompleted");
    prepared.resolve(undefined);
    await finishCancellation.promise;
    await env.repoStore.append(runId, {
      kind: "CancelRequested",
      seq: (events.at(-1)?.seq ?? 0) + 1,
      at,
      origin: "supervisor-operator",
      reason: "Operator requested cancellation",
    });
  });
  await Promise.race([prepared.promise, cancellation]);
  const terminal = commit(env, runId, { kind: "RunCancelled", seq: 4, at });
  finishCancellation.resolve(undefined);
  try {
    await cancellation;
    await terminal;
    expect(
      (await env.repoStore.read(runId)).map(({ kind, seq }) => ({ kind, seq })),
    ).toEqual([
      { kind: "RunStarted", seq: 1 },
      { kind: "StepStarted", seq: 2 },
      { kind: "StepCompleted", seq: 3 },
      { kind: "CancelRequested", seq: 4 },
      { kind: "RunCancelled", seq: 5 },
    ]);
    expect((await reloadState(env, runId)).phase).toBe("cancelled");
  } finally {
    dropChain(runId);
  }
});

test("a failed barrier flush rejects the run's next flush with the same error", async () => {
  const runId = "run_barrier_flush_failure";
  const inner = createInMemoryRepoStore();
  const boom = new Error("append failed");
  let fail = false;
  let appendBatches = 0;
  const env = {
    repoStore: {
      ...inner,
      async appendBatch(
        id: string,
        events: Parameters<typeof inner.appendBatch>[1],
      ) {
        appendBatches += 1;
        if (fail) throw boom;
        await inner.appendBatch(id, events);
      },
    },
  };
  const at = "2026-01-01T00:00:00.000Z";
  try {
    await commitBuffered(env, runId, {
      kind: "RunStarted",
      seq: 0,
      at,
      runId,
      definitionHash: "definition",
      trigger: { type: "manual", payload: null },
    });
    fail = true;
    let wrote = false;
    await expect(
      withRunCommitBarrier(env, runId, async () => {
        wrote = true;
      }),
    ).rejects.toBe(boom);
    expect(wrote).toBe(false);
    const afterBarrier = appendBatches;
    await expect(flushChain(env, runId)).rejects.toBe(boom);
    await expect(
      commit(env, runId, {
        kind: "RunCompleted",
        seq: 0,
        at,
      }),
    ).rejects.toBe(boom);
    expect(appendBatches).toBe(afterBarrier);
    expect(await env.repoStore.read(runId)).toEqual([]);
  } finally {
    dropChain(runId);
  }
});

test("a rejected transition does not poison the next flush", async () => {
  const runId = "run_transition_rejection";
  const env = { repoStore: createInMemoryRepoStore() };
  const at = "2026-01-01T00:00:00.000Z";
  try {
    await expect(
      commitBuffered(env, runId, {
        kind: "StepStarted",
        seq: 1,
        at,
        stepId: "work",
        attempt: 1,
        input: { ref: "inline:null" },
      }),
    ).rejects.toThrow(/expected phase running/);
    await flushChain(env, runId);
    expect(await env.repoStore.read(runId)).toEqual([]);
  } finally {
    dropChain(runId);
  }
});

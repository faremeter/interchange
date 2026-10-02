import { expect, test } from "bun:test";

import { createInMemoryRepoStore } from "../runlocal/repo-store";
import {
  commit,
  commitBuffered,
  dropChain,
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

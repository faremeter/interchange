import { describe, expect, test } from "bun:test";

import type { TokenUsage } from "@intx/types/runtime";

import {
  buildStepStateCheckpoint,
  isSameStepStateInvocation,
  reconstructStepState,
  STEP_STATE_CHECKPOINT_FILE,
  STEP_STATE_CHECKPOINT_META_FILE,
  serializeStepStateWalEntry,
  stepStateWalEntryPath,
  type StepStateReader,
} from "./step-state";

const USAGE: TokenUsage = {
  input: 1,
  output: 2,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};

const TARGETS = {
  c1: {
    parts: {
      to: "carol@example.com",
      cc: [],
      inReplyTo: "<c@example.com>",
    },
  },
  c2: { parts: null },
};

// The state directory is the reader's root, so the paths the step-state
// helpers build from an empty state directory are the reader's paths.
function createMemoryReader(files: Record<string, string>): StepStateReader {
  return {
    readFile(relPath) {
      return Promise.resolve(files[relPath] ?? null);
    },
    listDir(relPath) {
      const prefix = `${relPath}/`;
      const names = new Set<string>();
      for (const filePath of Object.keys(files)) {
        if (!filePath.startsWith(prefix)) continue;
        const name = filePath.slice(prefix.length).split("/")[0];
        if (name !== undefined) names.add(name);
      }
      return Promise.resolve(names.size === 0 ? null : [...names]);
    },
  };
}

async function reconstruct(files: Record<string, string>) {
  const state = await reconstructStepState(createMemoryReader(files), "test");
  if (state === null) throw new Error("expected a reconstruction");
  return state;
}

describe("step state parked reply targets", () => {
  test("a WAL entry written without parked reply targets reads as none", async () => {
    const state = await reconstruct({
      [stepStateWalEntryPath("", 0)]: serializeStepStateWalEntry(0, [], {
        pendingOperations: [],
        tokenUsage: USAGE,
        connectorState: null,
      }),
    });

    expect(state.parkedReplyTargets).toEqual({});
  });

  test("a checkpoint written without parked reply targets reads as none", async () => {
    const state = await reconstruct(
      buildStepStateCheckpoint("", 1, {
        turns: [],
        pendingOperations: [],
        tokenUsage: USAGE,
        connectorState: null,
      }),
    );

    expect(state.parkedReplyTargets).toEqual({});
  });

  test("a checkpoint carries its parked reply targets", async () => {
    const state = await reconstruct(
      buildStepStateCheckpoint("", 1, {
        turns: [],
        pendingOperations: [],
        tokenUsage: USAGE,
        connectorState: null,
        parkedReplyTargets: TARGETS,
      }),
    );

    expect(state.parkedReplyTargets).toEqual(TARGETS);
  });

  test("a later WAL entry's empty targets replace the checkpoint's", async () => {
    const state = await reconstruct({
      ...buildStepStateCheckpoint("", 1, {
        turns: [],
        pendingOperations: [],
        tokenUsage: USAGE,
        connectorState: null,
        parkedReplyTargets: TARGETS,
      }),
      [stepStateWalEntryPath("", 1)]: serializeStepStateWalEntry(1, [], {
        pendingOperations: [],
        tokenUsage: USAGE,
        connectorState: null,
        parkedReplyTargets: {},
      }),
    });

    expect(state.boundaryCount).toBe(2);
    expect(state.parkedReplyTargets).toEqual({});
  });
});

describe("step state invocation stamp", () => {
  const INVOCATION = { runId: "run_body", stepId: "work", attempt: 2 };

  test("a checkpoint and a WAL entry carry their invocation stamp", async () => {
    const checkpointed = await reconstruct(
      buildStepStateCheckpoint("", 1, {
        turns: [],
        pendingOperations: [],
        tokenUsage: USAGE,
        connectorState: null,
        invocation: INVOCATION,
      }),
    );
    expect(checkpointed.invocation).toEqual(INVOCATION);

    const appended = await reconstruct({
      [stepStateWalEntryPath("", 0)]: serializeStepStateWalEntry(0, [], {
        pendingOperations: [],
        tokenUsage: USAGE,
        connectorState: null,
        invocation: INVOCATION,
      }),
    });
    expect(appended.invocation).toEqual(INVOCATION);
  });

  test("state stamped with a bare attempt reads as stamped by no invocation", async () => {
    const meta = {
      checkpointSeq: 1,
      turnCount: 0,
      pendingOperations: [],
      tokenUsage: USAGE,
      connectorState: null,
      attempt: 1,
    };
    const state = await reconstruct({
      [STEP_STATE_CHECKPOINT_FILE]: JSON.stringify({
        turns: [],
        pendingOperations: [],
        tokenUsage: USAGE,
        connectorState: null,
        attempt: 1,
      }),
      [STEP_STATE_CHECKPOINT_META_FILE]: JSON.stringify(meta),
      [stepStateWalEntryPath("", 1)]: JSON.stringify({
        seq: 1,
        turns: [],
        metadata: {
          pendingOperations: [],
          tokenUsage: USAGE,
          connectorState: null,
          attempt: 1,
        },
      }),
    });

    expect(state.boundaryCount).toBe(2);
    expect(state.invocation).toBeUndefined();
    expect("attempt" in state).toBe(false);
  });

  test("two stamps name the same invocation only when every field matches", () => {
    expect(isSameStepStateInvocation(INVOCATION, { ...INVOCATION })).toBe(true);
    expect(
      isSameStepStateInvocation(INVOCATION, { ...INVOCATION, runId: "other" }),
    ).toBe(false);
    expect(
      isSameStepStateInvocation(INVOCATION, { ...INVOCATION, stepId: "m[1]" }),
    ).toBe(false);
    expect(
      isSameStepStateInvocation(INVOCATION, { ...INVOCATION, attempt: 1 }),
    ).toBe(false);
  });
});

import { afterEach, describe, expect, test } from "bun:test";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto";
import type { StepStateSnapshot } from "@intx/types";
import type {
  ConnectorThreadState,
  ConversationTurn,
  PendingOperation,
  TokenUsage,
} from "@intx/types/runtime";

import { createAgentRepoStore } from "./agent-repo";
import type { RepoId, RepoStore } from "./repo-store/types";
import {
  buildStepStateCheckpoint,
  serializeStepStateWalEntry,
  stepStateWalEntryPath,
} from "./step-state";
import {
  parseStepStateSeed,
  readStepStateSnapshot,
} from "./step-state-snapshot";
import {
  workflowRunLegacyAgentStatePrefix,
  workflowRunStepSeedPath,
  workflowRunStepStatePrefix,
} from "./workflow-run-kind";

const REF = "refs/heads/main";
const RUN_ID = "run_snapshot";
const STEP_ID = "specialist";
const REPO_ID: RepoId = { kind: "workflow-run", id: "snapshot-deployment" };
const USAGE: TokenUsage = {
  input: 3,
  output: 4,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};
const THREAD: ConnectorThreadState = {
  threadRoot: "<root@client.test>",
  lastMessageId: "<reply@workflow.test>",
  replyTo: "user@client.test",
  cc: [],
};
const PENDING: PendingOperation = {
  correlationId: "corr-1",
  kind: "approval",
  registeredAt: 0,
  gateId: "gate-1",
};

const turn = (text: string): ConversationTurn => ({
  role: "user",
  content: [{ type: "text", text }],
  timestamp: 1,
});

const tempDirs: string[] = [];
afterEach(async () => {
  for (const dir of tempDirs.splice(0)) {
    await fs.rm(dir, { recursive: true, force: true });
  }
});

async function createStore(): Promise<RepoStore> {
  const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), "step-snapshot-"));
  tempDirs.push(dataDir);
  return createAgentRepoStore({ dataDir, signingKey: await generateKeyPair() })
    .repoStore;
}

/** A checkpoint of two turns followed by a one-turn WAL tail. */
function stateFiles(stateDir: string): Record<string, string> {
  return {
    ...buildStepStateCheckpoint(stateDir, 1, {
      turns: [turn("first"), turn("second")],
      pendingOperations: [],
      tokenUsage: USAGE,
      connectorState: null,
    }),
    [stepStateWalEntryPath(stateDir, 1)]: serializeStepStateWalEntry(
      1,
      [turn("third")],
      {
        pendingOperations: [PENDING],
        tokenUsage: USAGE,
        connectorState: THREAD,
        attempt: 1,
      },
    ),
  };
}

async function commit(store: RepoStore, files: Record<string, string>) {
  await store.writeTree({ kind: "hub" }, REPO_ID, REF, {
    files,
    message: "seed step state",
  });
}

function read(store: RepoStore, stepId = STEP_ID) {
  return readStepStateSnapshot({
    repoStore: store,
    repoId: REPO_ID,
    runId: RUN_ID,
    stepId,
  });
}

describe("readStepStateSnapshot", () => {
  test("exports the step's committed conversation without its pending operations", async () => {
    const store = await createStore();
    await commit(
      store,
      stateFiles(workflowRunStepStatePrefix(RUN_ID, STEP_ID)),
    );

    expect(await read(store)).toEqual({
      version: 1,
      turns: [turn("first"), turn("second"), turn("third")],
      tokenUsage: USAGE,
      connectorState: THREAD,
    });
  });

  test("falls back to a deployment's legacy agent-state copy", async () => {
    const store = await createStore();
    await commit(store, stateFiles(workflowRunLegacyAgentStatePrefix(STEP_ID)));

    expect((await read(store))?.turns).toEqual([
      turn("first"),
      turn("second"),
      turn("third"),
    ]);
  });

  test("prefers the steps subtree over a legacy copy", async () => {
    const store = await createStore();
    await commit(store, {
      ...stateFiles(workflowRunLegacyAgentStatePrefix(STEP_ID)),
      [stepStateWalEntryPath(workflowRunStepStatePrefix(RUN_ID, STEP_ID), 0)]:
        serializeStepStateWalEntry(0, [turn("current")], {
          pendingOperations: [],
          tokenUsage: USAGE,
          connectorState: null,
        }),
    });

    expect((await read(store))?.turns).toEqual([turn("current")]);
  });

  test("returns null for a step or deployment without committed state", async () => {
    const store = await createStore();
    expect(await read(store)).toBeNull();

    await commit(
      store,
      stateFiles(workflowRunStepStatePrefix(RUN_ID, STEP_ID)),
    );
    expect(await read(store, "other-step")).toBeNull();
  });

  test("exports the seed of a step that has no state of its own yet", async () => {
    const store = await createStore();
    const seed: StepStateSnapshot = {
      version: 1,
      turns: [turn("imported")],
      tokenUsage: USAGE,
      connectorState: THREAD,
    };
    await commit(store, {
      [workflowRunStepSeedPath(RUN_ID, STEP_ID)]: JSON.stringify(seed),
    });

    expect(await read(store)).toEqual(seed);
  });

  test("prefers the step's own state over its seed", async () => {
    const store = await createStore();
    await commit(store, {
      [workflowRunStepSeedPath(RUN_ID, STEP_ID)]: JSON.stringify({
        version: 1,
        turns: [turn("imported")],
        tokenUsage: USAGE,
        connectorState: null,
      }),
      ...stateFiles(workflowRunStepStatePrefix(RUN_ID, STEP_ID)),
    });

    expect((await read(store))?.turns).toEqual([
      turn("first"),
      turn("second"),
      turn("third"),
    ]);
  });

  test("refuses a seed that is not a valid snapshot", () => {
    expect(() => parseStepStateSeed("not json", "seed.json")).toThrow(
      /seed\.json is not valid JSON/,
    );
    expect(() =>
      parseStepStateSeed(JSON.stringify({ version: 1 }), "seed.json"),
    ).toThrow(/seed\.json is not a valid snapshot/);
  });

  test("refuses to export turns that are not valid conversation turns", async () => {
    const store = await createStore();
    const stateDir = workflowRunStepStatePrefix(RUN_ID, STEP_ID);
    await commit(store, {
      [stepStateWalEntryPath(stateDir, 0)]: serializeStepStateWalEntry(
        0,
        [{ role: "narrator", content: [], timestamp: 1 }],
        { pendingOperations: [], tokenUsage: USAGE, connectorState: null },
      ),
    });

    await expect(read(store)).rejects.toThrow(/not a valid snapshot/);
  });
});

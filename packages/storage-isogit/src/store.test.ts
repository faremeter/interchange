import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  createIsogitStore,
  currentBranch,
  createAndSwitchBranch,
  switchBranch,
  listBranches,
  logHistory,
} from "./index";
import type {
  ConversationMessage,
  TokenUsage,
} from "@interchange/types/runtime";

const ZERO_USAGE: TokenUsage = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};

async function makeTempDir(): Promise<string> {
  return fs.promises.mkdtemp(path.join(os.tmpdir(), "interchange-test-"));
}

const tempDirs: string[] = [];

async function tempDir(): Promise<string> {
  const d = await makeTempDir();
  tempDirs.push(d);
  return d;
}

afterEach(async () => {
  const dirs = tempDirs.splice(0);
  await Promise.all(
    dirs.map((d) => fs.promises.rm(d, { recursive: true, force: true })),
  );
});

describe("createIsogitStore", () => {
  test("initializes a new agent repo and returns a usable store", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);
    const { messages, pendingOperations, tokenUsage } = await store.load();

    expect(messages).toEqual([]);
    expect(pendingOperations).toEqual([]);
    expect(tokenUsage).toEqual(ZERO_USAGE);
  });

  test("is idempotent — calling twice returns a working store", async () => {
    const dir = await tempDir();
    await createIsogitStore(dir);
    const store = await createIsogitStore(dir);
    const { messages } = await store.load();
    expect(messages).toEqual([]);
  });
});

describe("save and load round-trip", () => {
  test("messages survive a commit/load cycle", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    const messages: ConversationMessage[] = [
      { role: "user", content: [{ type: "text", text: "Hello" }] },
      {
        role: "assistant",
        content: [{ type: "text", text: "Hi there" }],
        model: "test-model",
      },
    ];

    await store.commit(messages, [], ZERO_USAGE, "first checkpoint");
    const { messages: loaded } = await store.load();

    expect(loaded).toEqual(messages);
  });

  test("multiple commits accumulate correctly", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    const first: ConversationMessage[] = [
      { role: "user", content: [{ type: "text", text: "step 1" }] },
    ];
    const second: ConversationMessage[] = [
      ...first,
      {
        role: "assistant",
        content: [{ type: "text", text: "step 2" }],
        model: "m",
      },
    ];

    await store.commit(first, [], ZERO_USAGE, "step 1");
    await store.commit(second, [], ZERO_USAGE, "step 2");

    const { messages } = await store.load();
    expect(messages).toEqual(second);
  });
});

describe("checkpoint creates commit", () => {
  test("commit returns a ContextCommit with correct metadata", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    const commitResult = await store.commit(
      [],
      [],
      ZERO_USAGE,
      "named checkpoint",
    );

    expect(typeof commitResult.hash).toBe("string");
    expect(commitResult.hash.length).toBeGreaterThan(0);
    expect(commitResult.message).toBe("named checkpoint");
    expect(typeof commitResult.timestamp).toBe("number");
    expect(commitResult.timestamp).toBeGreaterThan(0);
  });

  test("log contains the checkpoint after commit", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    await store.commit([], [], ZERO_USAGE, "my checkpoint");
    const entries = await store.log(5);

    const found = entries.find((e) => e.message === "my checkpoint");
    expect(found).toBeDefined();
    if (!found) throw new Error("unreachable");
    expect(found.hash.length).toBeGreaterThan(0);
  });
});

describe("branch operations", () => {
  test("createAndSwitchBranch creates and activates a new branch", async () => {
    const dir = await tempDir();
    await createIsogitStore(dir);

    await createAndSwitchBranch(dir, "feature-branch");
    const branch = await currentBranch(dir);

    expect(branch).toBe("feature-branch");
  });

  test("changes on a branch are isolated from main", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    const mainMessages: ConversationMessage[] = [
      { role: "user", content: [{ type: "text", text: "on main" }] },
    ];
    await store.commit(mainMessages, [], ZERO_USAGE, "main work");

    await createAndSwitchBranch(dir, "experiment");

    const branchMessages: ConversationMessage[] = [
      { role: "user", content: [{ type: "text", text: "on branch" }] },
    ];
    await store.commit(branchMessages, [], ZERO_USAGE, "branch work");

    await switchBranch(dir, "main");

    const { messages } = await store.load();
    expect(messages).toEqual(mainMessages);
  });

  test("listBranches includes main and created branches", async () => {
    const dir = await tempDir();
    await createIsogitStore(dir);

    await createAndSwitchBranch(dir, "branch-a");
    await switchBranch(dir, "main");

    const branches = await listBranches(dir);
    expect(branches).toContain("main");
    expect(branches).toContain("branch-a");
  });
});

describe("history log", () => {
  test("log returns entries in reverse chronological order", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    await store.commit([], [], ZERO_USAGE, "commit one");
    await store.commit([], [], ZERO_USAGE, "commit two");

    const entries = await logHistory(dir, 5);

    const first = entries[0];
    const second = entries[1];
    if (!first || !second) throw new Error("unreachable");
    expect(first.message).toBe("commit two");
    expect(second.message).toBe("commit one");
  });

  test("parentHash chains commits together", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    const first = await store.commit([], [], ZERO_USAGE, "first");
    const second = await store.commit([], [], ZERO_USAGE, "second");

    expect(second.parentHash).toBe(first.hash);
  });
});

describe("readAt", () => {
  test("reads message history at an earlier commit", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    const v1: ConversationMessage[] = [
      { role: "user", content: [{ type: "text", text: "version 1" }] },
    ];
    const first = await store.commit(v1, [], ZERO_USAGE, "v1");

    const v2: ConversationMessage[] = [
      ...v1,
      {
        role: "assistant",
        content: [{ type: "text", text: "version 2" }],
        model: "m",
      },
    ];
    await store.commit(v2, [], ZERO_USAGE, "v2");

    const atFirst = await store.readAt(first.hash);
    expect(atFirst).toEqual(v1);
  });
});

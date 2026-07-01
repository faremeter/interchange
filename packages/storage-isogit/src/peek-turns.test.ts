import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createIsogitStore } from "./index";
import type { ConversationTurn, TokenUsage } from "@intx/types/runtime";

const ZERO_USAGE: TokenUsage = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};

const tempDirs: string[] = [];
async function tempDir(): Promise<string> {
  const d = await fs.promises.mkdtemp(path.join(os.tmpdir(), "peek-turns-"));
  tempDirs.push(d);
  return d;
}
afterEach(async () => {
  const dirs = tempDirs.splice(0);
  await Promise.all(
    dirs.map((d) => fs.promises.rm(d, { recursive: true, force: true })),
  );
});

function userTurn(text: string): ConversationTurn {
  return { role: "user", content: [{ type: "text", text }], timestamp: 1 };
}

describe("DurableMirrorReads.peekTurns", () => {
  test("returns [] before any writeTurns", async () => {
    const store = await createIsogitStore(await tempDir());
    expect(store.peekTurns()).toEqual([]);
  });

  test("returns the exact array reference handed to writeTurns", async () => {
    const store = await createIsogitStore(await tempDir());
    const arr: ConversationTurn[] = [userTurn("a")];
    await store.writeTurns(arr);
    expect(store.peekTurns()).toBe(arr); // same reference, not a copy
  });

  test("aliases the caller's array: in-place mutation is visible", async () => {
    const store = await createIsogitStore(await tempDir());
    const arr: ConversationTurn[] = [userTurn("a")];
    await store.writeTurns(arr);
    arr.push(userTurn("b")); // the reactor's appendTurn does exactly this
    // peekTurns now reports 2 turns though only 1 was persisted to disk.
    expect(store.peekTurns()).toHaveLength(2);
    const onDisk = await store.load();
    expect(onDisk.turns).toHaveLength(1);
  });

  test("peekTurns tracks the last writeTurns array", async () => {
    const store = await createIsogitStore(await tempDir());
    const first: ConversationTurn[] = [userTurn("first")];
    await store.writeTurns(first);
    const second: ConversationTurn[] = [
      userTurn("second-a"),
      userTurn("second-b"),
    ];
    await store.writeTurns(second);
    expect(store.peekTurns()).toBe(second);
  });
});

describe("DurableMirrorReads.loadMetadata", () => {
  test("returns fresh empty defaults when metadata.json is absent", async () => {
    const store = await createIsogitStore(await tempDir());
    const m1 = await store.loadMetadata();
    expect(m1.pendingOperations).toEqual([]);
    expect(m1.tokenUsage).toEqual(ZERO_USAGE);
    expect(m1.connectorState).toBeNull();
    // A fresh copy each call: distinct references, and mutating one
    // result must not leak into the next.
    m1.tokenUsage.input = 999;
    const m2 = await store.loadMetadata();
    expect(m2.tokenUsage).toEqual(ZERO_USAGE);
    expect(m2.pendingOperations).toEqual([]);
    expect(m2.pendingOperations).not.toBe(m1.pendingOperations);
  });

  test("load and loadMetadata agree after writeMetadata", async () => {
    const store = await createIsogitStore(await tempDir());
    const usage: TokenUsage = {
      input: 10,
      output: 20,
      cacheRead: 0,
      cacheWrite: 0,
      thinking: 0,
    };
    await store.writeMetadata({ pendingOperations: [], tokenUsage: usage });
    const viaLoad = await store.load();
    const viaMeta = await store.loadMetadata();
    expect(viaMeta.tokenUsage).toEqual(usage);
    expect(viaLoad.tokenUsage).toEqual(usage);
    expect(viaLoad.pendingOperations).toEqual(viaMeta.pendingOperations);
    expect(viaLoad.connectorState).toEqual(viaMeta.connectorState);
  });
});

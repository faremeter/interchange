import { describe, expect, spyOn, test } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { createAgentRepoStore } from "./agent-repo-store";

describe("AgentRepoStore.remove", () => {
  test("removes an agent directory and treats a missing one as removed", async () => {
    const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), "agent-repo-"));
    const store = createAgentRepoStore({ dataDir });
    await store.initRepo("agent@local.test");

    await store.remove("agent@local.test");
    await store.remove("agent@local.test");

    await expect(
      fs.stat(store.getAgentDir("agent@local.test")),
    ).rejects.toThrow();
    await fs.rm(dataDir, { recursive: true, force: true });
  });
  test("a failed directory sync rejects removal and is retried for an absent agent", async () => {
    const dataDir = await fs.mkdtemp(
      path.join(os.tmpdir(), "agent-repo-sync-"),
    );
    const store = createAgentRepoStore({ dataDir });
    const address = "agent@local.test";
    await store.initRepo(address);
    const parent = path.dirname(store.getAgentDir(address));
    const open = fs.open;
    let syncAttempts = 0;
    const spy = spyOn(fs, "open").mockImplementation(
      async (target, flags, mode) => {
        const handle = await open(target, flags, mode);
        if (target === parent) {
          const sync = handle.sync.bind(handle);
          spyOn(handle, "sync").mockImplementation(async () => {
            syncAttempts++;
            if (syncAttempts === 1)
              throw new Error("agent directory sync failed");
            await sync();
          });
        }
        return handle;
      },
    );
    try {
      await expect(store.remove(address)).rejects.toThrow(
        "agent directory sync failed",
      );
      await expect(fs.stat(store.getAgentDir(address))).rejects.toThrow();
      await store.remove(address);
      expect(syncAttempts).toBe(2);
    } finally {
      spy.mockRestore();
      await fs.rm(dataDir, { recursive: true, force: true });
    }
  });
});

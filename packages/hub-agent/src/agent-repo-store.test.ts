import { describe, expect, test } from "bun:test";
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
});

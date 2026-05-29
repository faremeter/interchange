import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import type { HarnessConfig } from "@intx/types/runtime";

import { createAgentRepoStore } from "./agent-repo-store";

const tempDirs: string[] = [];

async function tempDir(): Promise<string> {
  const d = await fs.mkdtemp(path.join(os.tmpdir(), "agent-repo-store-test-"));
  tempDirs.push(d);
  return d;
}

afterEach(async () => {
  const dirs = tempDirs.splice(0);
  await Promise.all(
    dirs.map((d) => fs.rm(d, { recursive: true, force: true })),
  );
});

function makeConfig(address: string): HarnessConfig {
  return {
    agentId: "test-agent",
    agentAddress: address,
    sessionId: "sess-1",
    principalId: "principal-1",
    tenantId: "tenant-1",
    systemPrompt: "test",
    tools: [],
    grants: [],
    sources: [
      {
        id: "test:test-model",
        provider: "test",
        apiKey: "key",
        baseURL: "http://localhost",
        model: "test-model",
      },
    ],
    defaultSource: "test:test-model",
  };
}

// Provision the on-disk shape `scanConfigs` looks for: a per-agent
// directory containing the keypair files (presence is checked, contents
// are not used here — repo-store tests do not need a real keypair).
async function provisionAgentDir(
  dataDir: string,
  agentName: string,
): Promise<string> {
  const dir = path.join(dataDir, agentName);
  await fs.mkdir(path.join(dir, "keys"), { recursive: true });
  await fs.writeFile(path.join(dir, "keys", "id_ed25519"), new Uint8Array());
  await fs.writeFile(
    path.join(dir, "keys", "id_ed25519.pub"),
    new Uint8Array(),
  );
  return dir;
}

describe("AgentRepoStore — config + pairing persistence", () => {
  test("persistPairing then persistConfig preserves the pairing key", async () => {
    const dataDir = await tempDir();
    const address = "agent@local";
    await provisionAgentDir(dataDir, "agent_at_local");

    const repo = createAgentRepoStore({ dataDir });
    await repo.persistConfig(address, makeConfig(address));
    await repo.persistPairing(address, "aabbcc");

    const entries = await repo.scanConfigs();
    expect(entries).toHaveLength(1);
    const entry = entries[0];
    if (entry === undefined) throw new Error("unreachable");
    expect(entry.hubPublicKey).toBe("aabbcc");
  });

  test("persistConfig after persistPairing keeps the pairing key intact", async () => {
    const dataDir = await tempDir();
    const address = "agent@local";
    await provisionAgentDir(dataDir, "agent_at_local");

    const repo = createAgentRepoStore({ dataDir });
    await repo.persistConfig(address, makeConfig(address));
    await repo.persistPairing(address, "aabbcc");

    const updatedConfig = { ...makeConfig(address), systemPrompt: "updated" };
    await repo.persistConfig(address, updatedConfig);

    const entries = await repo.scanConfigs();
    expect(entries).toHaveLength(1);
    const entry = entries[0];
    if (entry === undefined) throw new Error("unreachable");
    expect(entry.hubPublicKey).toBe("aabbcc");
    expect(entry.config.systemPrompt).toBe("updated");
  });

  test("absent pairing key returns undefined from scanConfigs", async () => {
    const dataDir = await tempDir();
    const address = "agent@local";
    await provisionAgentDir(dataDir, "agent_at_local");

    const repo = createAgentRepoStore({ dataDir });
    await repo.persistConfig(address, makeConfig(address));

    const entries = await repo.scanConfigs();
    expect(entries).toHaveLength(1);
    const entry = entries[0];
    if (entry === undefined) throw new Error("unreachable");
    expect(entry.hubPublicKey).toBeUndefined();
  });

  test("persistPairing on an unknown address throws", async () => {
    const dataDir = await tempDir();
    const repo = createAgentRepoStore({ dataDir });

    await expect(repo.persistPairing("nobody@local", "aabbcc")).rejects.toThrow(
      "no existing agent.json",
    );
  });
});

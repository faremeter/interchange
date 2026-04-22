import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import type { HarnessConfig as AgentConfig } from "@interchange/types/runtime";
import {
  persistAgentConfig,
  scanExistingAgents,
  loadOrGenerateKeyPair,
} from "./key-store";

const tempDirs: string[] = [];

async function tempDir(): Promise<string> {
  const d = await fs.mkdtemp(path.join(os.tmpdir(), "key-store-test-"));
  tempDirs.push(d);
  return d;
}

afterEach(async () => {
  const dirs = tempDirs.splice(0);
  await Promise.all(
    dirs.map((d) => fs.rm(d, { recursive: true, force: true })),
  );
});

function makeConfig(address: string): AgentConfig {
  return {
    agentId: "test-agent",
    agentAddress: address,
    sessionId: "sess-1",
    principalId: "principal-1",
    tenantId: "tenant-1",
    systemPrompt: "test",
    tools: [],
    grants: [],
    providers: [
      {
        provider: "test",
        apiKey: "key",
        baseURL: "http://localhost",
      },
    ],
    defaultModel: "test-model",
  };
}

describe("hub key persistence", () => {
  test("hubPublicKey survives persist and scan round-trip", async () => {
    const dataDir = await tempDir();
    const address = "agent@local";

    await loadOrGenerateKeyPair(dataDir, address);
    await persistAgentConfig(dataDir, address, makeConfig(address), "aabbcc");

    const entries = await scanExistingAgents(dataDir);
    expect(entries).toHaveLength(1);
    const entry = entries[0];
    if (entry === undefined) throw new Error("unreachable");
    expect(entry.hubPublicKey).toBe("aabbcc");
  });

  test("hubPublicKey is preserved when config is updated without explicit key", async () => {
    const dataDir = await tempDir();
    const address = "agent@local";

    await loadOrGenerateKeyPair(dataDir, address);
    await persistAgentConfig(dataDir, address, makeConfig(address), "aabbcc");

    const updatedConfig = { ...makeConfig(address), systemPrompt: "updated" };
    await persistAgentConfig(dataDir, address, updatedConfig);

    const entries = await scanExistingAgents(dataDir);
    expect(entries).toHaveLength(1);
    const entry = entries[0];
    if (entry === undefined) throw new Error("unreachable");
    expect(entry.hubPublicKey).toBe("aabbcc");
    expect(entry.config.systemPrompt).toBe("updated");
  });

  test("absent hubPublicKey returns undefined from scan", async () => {
    const dataDir = await tempDir();
    const address = "agent@local";

    await loadOrGenerateKeyPair(dataDir, address);
    await persistAgentConfig(dataDir, address, makeConfig(address));

    const entries = await scanExistingAgents(dataDir);
    expect(entries).toHaveLength(1);
    const entry = entries[0];
    if (entry === undefined) throw new Error("unreachable");
    expect(entry.hubPublicKey).toBeUndefined();
  });
});

import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import type { KeyPair } from "@intx/types/runtime";

import { createAgentKeyStore } from "./agent-key-store";

const tempDirs: string[] = [];

async function tempDir(): Promise<string> {
  const d = await fs.mkdtemp(path.join(os.tmpdir(), "agent-key-store-test-"));
  tempDirs.push(d);
  return d;
}

afterEach(async () => {
  const dirs = tempDirs.splice(0);
  await Promise.all(
    dirs.map((d) => fs.rm(d, { recursive: true, force: true })),
  );
});

function makeKeyPair(seed: number): KeyPair {
  const privateKey = new Uint8Array(32);
  const publicKey = new Uint8Array(32);
  for (let i = 0; i < 32; i++) {
    privateKey[i] = (seed + i) & 0xff;
    publicKey[i] = (seed * 2 + i) & 0xff;
  }
  return { privateKey, publicKey };
}

describe("AgentKeyStore — load-or-generate", () => {
  test("first call mints and persists a new keypair", async () => {
    const dataDir = await tempDir();
    let calls = 0;
    const store = createAgentKeyStore({
      dataDir,
      generateKeyPair: async () => {
        calls++;
        return makeKeyPair(7);
      },
    });

    const result = await store.loadOrGenerateKey("agent@local");
    expect(result.isNew).toBe(true);
    expect(calls).toBe(1);
    expect(result.keyPair.privateKey).toEqual(makeKeyPair(7).privateKey);

    const privPath = path.join(dataDir, "agent_at_local", "keys", "id_ed25519");
    const onDisk = await fs.readFile(privPath);
    expect(onDisk.length).toBe(32);
  });

  test("second call loads the persisted keypair without regenerating", async () => {
    const dataDir = await tempDir();
    let calls = 0;
    const store = createAgentKeyStore({
      dataDir,
      generateKeyPair: async () => {
        calls++;
        return makeKeyPair(42);
      },
    });

    const first = await store.loadOrGenerateKey("agent@local");
    const second = await store.loadOrGenerateKey("agent@local");

    expect(first.isNew).toBe(true);
    expect(second.isNew).toBe(false);
    expect(calls).toBe(1);
    expect(second.keyPair.publicKey).toEqual(first.keyPair.publicKey);
    expect(second.keyPair.privateKey).toEqual(first.keyPair.privateKey);
  });

  test("missing one half of the key pair surfaces a corruption error", async () => {
    const dataDir = await tempDir();
    const store = createAgentKeyStore({
      dataDir,
      generateKeyPair: async () => makeKeyPair(1),
    });
    await store.loadOrGenerateKey("agent@local");

    const pubPath = path.join(
      dataDir,
      "agent_at_local",
      "keys",
      "id_ed25519.pub",
    );
    await fs.rm(pubPath);

    await expect(store.loadOrGenerateKey("agent@local")).rejects.toThrow(
      /Corrupt key pair/,
    );
  });
});

describe("AgentKeyStore — scanKeys", () => {
  test("returns key entries whose directory has both files plus agent.json", async () => {
    const dataDir = await tempDir();
    const store = createAgentKeyStore({
      dataDir,
      generateKeyPair: async () => makeKeyPair(3),
    });
    await store.loadOrGenerateKey("agent@local");
    await fs.writeFile(
      path.join(dataDir, "agent_at_local", "agent.json"),
      JSON.stringify({ version: 1, address: "agent@local" }),
    );

    const entries = await store.scanKeys();
    expect(entries).toHaveLength(1);
    expect(entries[0]?.address).toBe("agent@local");
  });

  test("skips an agent directory with no agent.json", async () => {
    const dataDir = await tempDir();
    const store = createAgentKeyStore({
      dataDir,
      generateKeyPair: async () => makeKeyPair(5),
    });
    await store.loadOrGenerateKey("agent@local");

    const entries = await store.scanKeys();
    expect(entries).toEqual([]);
  });
});

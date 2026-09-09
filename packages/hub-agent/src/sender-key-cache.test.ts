import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { hexEncode } from "@intx/types";

import { createSenderKeyCache } from "./sender-key-cache";

const tempDirs: string[] = [];

async function tempDir(): Promise<string> {
  const d = await fs.mkdtemp(path.join(os.tmpdir(), "sender-key-cache-test-"));
  tempDirs.push(d);
  return d;
}

afterEach(async () => {
  const dirs = tempDirs.splice(0);
  await Promise.all(
    dirs.map((d) => fs.rm(d, { recursive: true, force: true })),
  );
});

// A plain durable write is enough for the cache's own logic; the atomicity and
// fsync of the production primitive are orthogonal to what these tests assert.
async function writeFileDurable(
  filePath: string,
  contents: string,
): Promise<void> {
  await fs.writeFile(filePath, contents);
}

function makeKey(seed: number): Uint8Array {
  const key = new Uint8Array(32);
  for (let i = 0; i < 32; i++) key[i] = (seed + i) & 0xff;
  return key;
}

const senderKeysDir = (dataDir: string) => path.join(dataDir, "sender-keys");

describe("SenderKeyCache", () => {
  test("caches a key and reads it back keyed on the full address", async () => {
    const dataDir = await tempDir();
    const cache = await createSenderKeyCache({ dataDir, writeFileDurable });

    const address = "run_abc@tenant.example.com";
    await cache.put(address, makeKey(3));

    expect(cache.get(address)).toEqual(makeKey(3));
    // The domain-qualified address is the key; a bare local part is a miss.
    expect(cache.get("run_abc")).toBeUndefined();
  });

  test("returns undefined for an unknown sender", async () => {
    const dataDir = await tempDir();
    const cache = await createSenderKeyCache({ dataDir, writeFileDurable });
    expect(cache.get("nobody@example.com")).toBeUndefined();
  });

  test("survives a restart by loading the keyring from disk", async () => {
    const dataDir = await tempDir();
    const first = await createSenderKeyCache({ dataDir, writeFileDurable });
    await first.put("alice@a.example", makeKey(5));
    await first.put("bob@b.example", makeKey(9));

    const restarted = await createSenderKeyCache({ dataDir, writeFileDurable });
    expect(restarted.get("alice@a.example")).toEqual(makeKey(5));
    expect(restarted.get("bob@b.example")).toEqual(makeKey(9));
  });

  test("keeps addresses that collide under lossy sanitization distinct", async () => {
    // "a.b@c.example" and "a_b@c.example" both flatten to one filename under
    // the AgentKeyStore sanitize scheme; the injective hex filename must not.
    const dataDir = await tempDir();
    const cache = await createSenderKeyCache({ dataDir, writeFileDurable });
    await cache.put("a.b@c.example", makeKey(1));
    await cache.put("a_b@c.example", makeKey(2));

    expect(cache.get("a.b@c.example")).toEqual(makeKey(1));
    expect(cache.get("a_b@c.example")).toEqual(makeKey(2));

    const files = await fs.readdir(senderKeysDir(dataDir));
    expect(files).toHaveLength(2);

    const restarted = await createSenderKeyCache({ dataDir, writeFileDurable });
    expect(restarted.get("a.b@c.example")).toEqual(makeKey(1));
    expect(restarted.get("a_b@c.example")).toEqual(makeKey(2));
  });

  test("the latest write for an address wins", async () => {
    const dataDir = await tempDir();
    const cache = await createSenderKeyCache({ dataDir, writeFileDurable });
    await cache.put("rotator@example.com", makeKey(4));
    await cache.put("rotator@example.com", makeKey(8));

    expect(cache.get("rotator@example.com")).toEqual(makeKey(8));
    const restarted = await createSenderKeyCache({ dataDir, writeFileDurable });
    expect(restarted.get("rotator@example.com")).toEqual(makeKey(8));
  });

  test("skips a corrupt file on load without dropping the other keys", async () => {
    const dataDir = await tempDir();
    const good = await createSenderKeyCache({ dataDir, writeFileDurable });
    await good.put("good@example.com", makeKey(6));

    const dir = senderKeysDir(dataDir);
    // A validly-named file whose contents are not a valid envelope.
    const corruptName = hexEncode(new TextEncoder().encode("corrupt@example"));
    await fs.writeFile(path.join(dir, corruptName), "{ not json");
    // A stray temp file the way a crashed atomic write would leave one.
    await fs.writeFile(path.join(dir, "leftover.tmp.123.abcd"), "junk");

    const restarted = await createSenderKeyCache({ dataDir, writeFileDurable });
    expect(restarted.get("good@example.com")).toEqual(makeKey(6));
    expect(restarted.get("corrupt@example")).toBeUndefined();
  });

  test("skips a file whose envelope address does not match its filename", async () => {
    const dataDir = await tempDir();
    await createSenderKeyCache({ dataDir, writeFileDurable });
    const dir = senderKeysDir(dataDir);
    await fs.mkdir(dir, { recursive: true });
    // File named for address A but carrying address B: a tamper/corruption the
    // filename/envelope cross-check must reject rather than serve under A.
    const nameForA = hexEncode(new TextEncoder().encode("a@example.com"));
    await fs.writeFile(
      path.join(dir, nameForA),
      JSON.stringify({
        address: "b@example.com",
        publicKey: hexEncode(makeKey(2)),
      }),
    );

    const restarted = await createSenderKeyCache({ dataDir, writeFileDurable });
    expect(restarted.get("a@example.com")).toBeUndefined();
    expect(restarted.get("b@example.com")).toBeUndefined();
  });

  test("rejects a wrong-length key at put", async () => {
    const dataDir = await tempDir();
    const cache = await createSenderKeyCache({ dataDir, writeFileDurable });
    await expect(
      cache.put("short@example.com", new Uint8Array(16)),
    ).rejects.toThrow(/32/);
    expect(cache.get("short@example.com")).toBeUndefined();
  });

  test("a failed durable write leaves no in-memory entry", async () => {
    const dataDir = await tempDir();
    const cache = await createSenderKeyCache({
      dataDir,
      writeFileDurable: async () => {
        throw new Error("disk full");
      },
    });
    await expect(cache.put("faulty@example.com", makeKey(7))).rejects.toThrow(
      "disk full",
    );
    // Memory is only updated after the durable write lands, so a failed write
    // must not leave a phantom key that a restart would not reproduce.
    expect(cache.get("faulty@example.com")).toBeUndefined();
  });
});

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

// The durable-removal counterpart; `force` makes it idempotent like the
// production `removeFileAtomicDurable`. Its atomicity/fsync are orthogonal to
// what these tests assert.
async function removeFileDurable(filePath: string): Promise<void> {
  await fs.rm(filePath, { force: true });
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
    const cache = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });

    const address = "run_abc@tenant.example.com";
    await cache.put(address, makeKey(3));

    expect(cache.get(address)).toEqual(makeKey(3));
    // The domain-qualified address is the key; a bare local part is a miss.
    expect(cache.get("run_abc")).toBeUndefined();
  });

  test("returns undefined for an unknown sender", async () => {
    const dataDir = await tempDir();
    const cache = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });
    expect(cache.get("nobody@example.com")).toBeUndefined();
  });

  test("survives a restart by loading the keyring from disk", async () => {
    const dataDir = await tempDir();
    const first = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });
    await first.put("alice@a.example", makeKey(5));
    await first.put("bob@b.example", makeKey(9));

    const restarted = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });
    expect(restarted.get("alice@a.example")).toEqual(makeKey(5));
    expect(restarted.get("bob@b.example")).toEqual(makeKey(9));
  });

  test("keeps addresses that collide under lossy sanitization distinct", async () => {
    // "a.b@c.example" and "a_b@c.example" both flatten to one filename under
    // the AgentKeyStore sanitize scheme; the injective hex filename must not.
    const dataDir = await tempDir();
    const cache = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });
    await cache.put("a.b@c.example", makeKey(1));
    await cache.put("a_b@c.example", makeKey(2));

    expect(cache.get("a.b@c.example")).toEqual(makeKey(1));
    expect(cache.get("a_b@c.example")).toEqual(makeKey(2));

    const files = await fs.readdir(senderKeysDir(dataDir));
    expect(files).toHaveLength(2);

    const restarted = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });
    expect(restarted.get("a.b@c.example")).toEqual(makeKey(1));
    expect(restarted.get("a_b@c.example")).toEqual(makeKey(2));
  });

  test("the latest write for an address wins", async () => {
    const dataDir = await tempDir();
    const cache = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });
    await cache.put("rotator@example.com", makeKey(4));
    await cache.put("rotator@example.com", makeKey(8));

    expect(cache.get("rotator@example.com")).toEqual(makeKey(8));
    const restarted = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });
    expect(restarted.get("rotator@example.com")).toEqual(makeKey(8));
  });

  test("refuses reads for a corrupt file's address without dropping the other keys", async () => {
    const dataDir = await tempDir();
    const good = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });
    await good.put("good@example.com", makeKey(6));

    const dir = senderKeysDir(dataDir);
    // A validly-named file whose contents are not a valid envelope.
    const corruptName = hexEncode(new TextEncoder().encode("corrupt@example"));
    await fs.writeFile(path.join(dir, corruptName), "{ not json");
    // A stray temp file the way a crashed atomic write would leave one.
    await fs.writeFile(path.join(dir, "leftover.tmp.123.abcd"), "junk");

    const restarted = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });
    expect(restarted.get("good@example.com")).toEqual(makeKey(6));
    // Returning undefined here would be indistinguishable from a sender the
    // cache was never given a key for, and the inbound gate reads that as an
    // author-relaxable condition rather than a fault.
    expect(() => restarted.get("corrupt@example")).toThrow(/failed to load/);
    // The unattributable temp file faults no address: it carries none, and one
    // must not deny the sidecar the keys it can read.
    expect([...restarted.rotatableAddresses()].sort()).toEqual([
      "corrupt@example",
      "good@example.com",
    ]);
  });

  test("faults no address for a corrupt file whose name is not an address", async () => {
    const dataDir = await tempDir();
    const dir = senderKeysDir(dataDir);
    await fs.mkdir(dir, { recursive: true });
    // Valid hex, so the filename decodes, but it names no `<local>@<domain>`
    // sender. There is nothing for the hub to re-resolve and nothing a reader
    // would ask for, so this stays a logged skip rather than a recorded fault.
    const notAnAddress = hexEncode(new TextEncoder().encode("notanaddress"));
    await fs.writeFile(path.join(dir, notAnAddress), "{ not json");

    const cache = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });
    expect(cache.rotatableAddresses()).toEqual([]);
    expect(cache.get("notanaddress")).toBeUndefined();
  });

  test("refuses reads for an address whose file carries truncated key material", async () => {
    // The partial-write shape: the envelope parses and its address agrees with
    // the filename, but the key is short, so nothing can verify against it.
    const dataDir = await tempDir();
    await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });
    const dir = senderKeysDir(dataDir);
    await fs.mkdir(dir, { recursive: true });
    const name = hexEncode(new TextEncoder().encode("short@example.com"));
    await fs.writeFile(
      path.join(dir, name),
      JSON.stringify({
        address: "short@example.com",
        publicKey: hexEncode(makeKey(2).slice(0, 31)),
      }),
    );

    const restarted = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });
    expect(() => restarted.get("short@example.com")).toThrow(/failed to load/);
  });

  test("refuses reads for a file whose envelope address does not match its filename", async () => {
    const dataDir = await tempDir();
    await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });
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

    const restarted = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });
    // The fault is attributed to the filename, which is the address a reader
    // would ask for. B's own entry lives under its own filename, so B is simply
    // not cached rather than faulted.
    expect(() => restarted.get("a@example.com")).toThrow(/failed to load/);
    expect(restarted.get("b@example.com")).toBeUndefined();
  });

  test("a load fault does not count as an address the cache holds a key for", async () => {
    const dataDir = await tempDir();
    const dir = senderKeysDir(dataDir);
    await fs.mkdir(dir, { recursive: true });
    const name = hexEncode(new TextEncoder().encode("corrupt@example.com"));
    await fs.writeFile(path.join(dir, name), "{ not json");

    const cache = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });
    // `addresses()` answers "which senders can this cache verify"; a faulted
    // address cannot. `rotatableAddresses()` answers a different question --
    // which senders the hub should re-resolve -- and a faulted one needs it
    // most, because re-pushing its key is what repairs the file.
    expect(cache.addresses()).toEqual([]);
    expect(cache.rotatableAddresses()).toEqual(["corrupt@example.com"]);
  });

  test("a run sender's load fault is not reported for the reconnect re-resolve", async () => {
    const dataDir = await tempDir();
    const dir = senderKeysDir(dataDir);
    await fs.mkdir(dir, { recursive: true });
    const name = hexEncode(new TextEncoder().encode("run_job1@tenant.example"));
    await fs.writeFile(path.join(dir, name), "{ not json");

    const cache = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });
    // The hub skips a reported run address whatever the sidecar sends, so
    // reporting one would be inert. A run sender's key is re-pushed on its next
    // grants barrier instead, and that write clears the fault.
    expect(cache.rotatableAddresses()).toEqual([]);
    expect(() => cache.get("run_job1@tenant.example")).toThrow(
      /failed to load/,
    );
  });

  test("writing a key clears the address's load fault", async () => {
    const dataDir = await tempDir();
    const dir = senderKeysDir(dataDir);
    await fs.mkdir(dir, { recursive: true });
    const name = hexEncode(new TextEncoder().encode("corrupt@example.com"));
    await fs.writeFile(path.join(dir, name), "{ not json");

    const cache = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });
    expect(() => cache.get("corrupt@example.com")).toThrow(/failed to load/);

    // The repair the reconnect re-resolve drives: the hub re-pushes the key and
    // the write replaces the corrupt file, so reads resume.
    await cache.put("corrupt@example.com", makeKey(11));

    expect(cache.get("corrupt@example.com")).toEqual(makeKey(11));
    expect(cache.rotatableAddresses()).toEqual(["corrupt@example.com"]);
    const restarted = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });
    expect(restarted.get("corrupt@example.com")).toEqual(makeKey(11));
  });

  test("evicting a faulted address clears the fault and removes its file", async () => {
    const dataDir = await tempDir();
    const dir = senderKeysDir(dataDir);
    await fs.mkdir(dir, { recursive: true });
    const name = hexEncode(new TextEncoder().encode("corrupt@example.com"));
    await fs.writeFile(path.join(dir, name), "{ not json");

    const cache = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });
    // The other repair the hub can drive: the sender's principal was deleted, so
    // there is no key to re-push and the corrupt file must simply go.
    await cache.evict("corrupt@example.com");

    expect(cache.get("corrupt@example.com")).toBeUndefined();
    expect(cache.rotatableAddresses()).toEqual([]);
    expect(await fs.readdir(dir)).toEqual([]);
  });

  test("a failed durable removal keeps a load fault in place", async () => {
    const dataDir = await tempDir();
    const dir = senderKeysDir(dataDir);
    await fs.mkdir(dir, { recursive: true });
    const name = hexEncode(new TextEncoder().encode("corrupt@example.com"));
    await fs.writeFile(path.join(dir, name), "{ not json");

    const cache = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable: async () => {
        throw new Error("unlink failed");
      },
    });
    // Symmetric with a held key: the fault is cleared only after the removal
    // lands, so a failed removal leaves the corrupt file on disk AND the
    // refusal in memory rather than resurrecting the fault on the next
    // restart.
    await expect(cache.evict("corrupt@example.com")).rejects.toThrow(
      "unlink failed",
    );
    expect(() => cache.get("corrupt@example.com")).toThrow(/failed to load/);
  });

  test("rejects a wrong-length key at put", async () => {
    const dataDir = await tempDir();
    const cache = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });
    await expect(
      cache.put("short@example.com", new Uint8Array(16)),
    ).rejects.toThrow(/32/);
    expect(cache.get("short@example.com")).toBeUndefined();
  });

  test("reports exactly the addresses it holds a key for", async () => {
    const dataDir = await tempDir();
    const cache = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });
    await cache.put("alice@a.example", makeKey(5));
    await cache.put("bob@b.example", makeKey(9));

    expect([...cache.addresses()].sort()).toEqual([
      "alice@a.example",
      "bob@b.example",
    ]);
  });

  test("reports only rotatable (non-run) addresses for the reconnect report", async () => {
    const dataDir = await tempDir();
    const cache = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });
    await cache.put("usr_alice@tenant.example", makeKey(5));
    await cache.put("run_job1@tenant.example", makeKey(9));
    await cache.put("usr_bob@tenant.example", makeKey(13));

    // A run sender's key is the immutable workflow_run.public_key, so only the
    // user senders are worth re-resolving on reconnect.
    expect([...cache.rotatableAddresses()].sort()).toEqual([
      "usr_alice@tenant.example",
      "usr_bob@tenant.example",
    ]);
    // The unfiltered snapshot still carries every cached address.
    expect(cache.addresses()).toHaveLength(3);
  });

  test("reports no addresses for a freshly constructed cache", async () => {
    const dataDir = await tempDir();
    // No keyring dir exists yet, so loadFromDisk finds nothing: the cold-start
    // path the reconnect reporter hits before any key has been cached.
    const cache = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });
    expect(cache.addresses()).toEqual([]);
  });

  test("returns a snapshot a later put does not retroactively mutate", async () => {
    const dataDir = await tempDir();
    const cache = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });
    await cache.put("alice@a.example", makeKey(5));

    const snapshot = cache.addresses();
    await cache.put("bob@b.example", makeKey(9));

    expect(snapshot).toEqual(["alice@a.example"]);
  });

  test("a failed durable write leaves no in-memory entry", async () => {
    const dataDir = await tempDir();
    const cache = await createSenderKeyCache({
      dataDir,
      writeFileDurable: async () => {
        throw new Error("disk full");
      },
      removeFileDurable,
    });
    await expect(cache.put("faulty@example.com", makeKey(7))).rejects.toThrow(
      "disk full",
    );
    // Memory is only updated after the durable write lands, so a failed write
    // must not leave a phantom key that a restart would not reproduce.
    expect(cache.get("faulty@example.com")).toBeUndefined();
  });

  test("evict durably removes a cached key from memory and disk", async () => {
    const dataDir = await tempDir();
    const cache = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });
    await cache.put("usr_alice@tenant.example", makeKey(5));
    await cache.put("usr_bob@tenant.example", makeKey(9));

    await cache.evict("usr_alice@tenant.example");

    // Gone from memory, and its on-disk file is gone while the sibling remains.
    expect(cache.get("usr_alice@tenant.example")).toBeUndefined();
    expect(cache.get("usr_bob@tenant.example")).toEqual(makeKey(9));
    const files = await fs.readdir(senderKeysDir(dataDir));
    expect(files).toHaveLength(1);

    // A restart does not resurrect the evicted key.
    const restarted = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });
    expect(restarted.get("usr_alice@tenant.example")).toBeUndefined();
    expect(restarted.get("usr_bob@tenant.example")).toEqual(makeKey(9));
  });

  test("evict is a no-op for an address the cache does not hold", async () => {
    const dataDir = await tempDir();
    const cache = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });
    // Idempotent: removing an absent address neither throws nor disturbs others.
    await cache.put("usr_bob@tenant.example", makeKey(9));
    await cache.evict("usr_ghost@tenant.example");
    expect(cache.get("usr_bob@tenant.example")).toEqual(makeKey(9));
  });

  test("a failed durable removal keeps the key in memory", async () => {
    const dataDir = await tempDir();
    const cache = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable: async () => {
        throw new Error("unlink failed");
      },
    });
    await cache.put("usr_alice@tenant.example", makeKey(5));

    // Disk-first, symmetric with put: the durable removal is awaited before the
    // map delete, so a removal fault throws and leaves BOTH stores holding the
    // key -- a restart would otherwise reload a key the caller believed evicted.
    await expect(cache.evict("usr_alice@tenant.example")).rejects.toThrow(
      "unlink failed",
    );
    expect(cache.get("usr_alice@tenant.example")).toEqual(makeKey(5));
  });
});

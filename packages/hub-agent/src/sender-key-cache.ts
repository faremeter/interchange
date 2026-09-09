// Local cache of the public keys the hub vouches for, keyed by sender
// address.
//
// A recipient sidecar verifies an inbound mail signature against the key
// the hub resolved for the message's authenticated sender. The hub
// co-delivers that key on the `run.grants` barrier (see the
// `senderIdentities` field on `RunGrantsFrame`); this cache is where the
// sidecar retains it so verification works locally and keeps working
// while the sidecar is disconnected from the hub.
//
// Peer to AgentKeyStore, but for a different custody role: AgentKeyStore
// holds this sidecar's OWN agents' key pairs, whereas this cache holds
// FOREIGN senders' public keys only. There is no private material here.
//
// The hub owns key freshness -- it re-pushes a rotated key on the next
// grant or reconnect -- so the cache has no TTL, generation, or eviction.
// A second entry for an address overwrites the first; the latest push
// wins.
//
// The whole keyring is loaded into memory at construction, so `get` is a
// synchronous memory read on the inbound-verify path and a restart sees
// every previously-cached key. The on-disk filename is the hex-encoded
// address bytes: an injective, reversible handle, so two addresses never
// collide onto one file the way the lossy `sanitizeAddress` scheme would.

import fsp from "node:fs/promises";
import path from "node:path";
import { type } from "arktype";
import { getLogger } from "@intx/log";
import { hasCode, hexDecode, hexEncode } from "@intx/types";

const logger = getLogger(["interchange", "hub-agent", "sender-key-cache"]);

// A raw Ed25519 public key is 32 bytes.
const ED25519_PUBLIC_KEY_BYTES = 32;

const SENDER_KEYS_DIR_NAME = "sender-keys";

// The on-disk envelope carries the address alongside the key so a loaded
// file is self-describing and a filename/content mismatch is detectable.
const StoredSenderKey = type({
  address: "string",
  publicKey: "string",
});

export type SenderKeyCacheDeps = {
  dataDir: string;
  /**
   * Durably persist `contents` at `path` (atomic replace + fsync). Injected
   * so the cache's write is at least as durable as the run-grants write it
   * gates, without this package depending on the sidecar app that owns the
   * durable-write primitive.
   */
  writeFileDurable: (path: string, contents: string) => Promise<void>;
};

export type SenderKeyCache = {
  /**
   * The cached public key for `address`, or `undefined` when none is known.
   * A synchronous memory read: the keyring is loaded at construction and
   * every `put` updates memory, so the map is authoritative for this
   * single-process sidecar.
   */
  get(address: string): Uint8Array | undefined;
  /**
   * Cache `publicKey` for `address`, persisting it durably before returning.
   * Throws if the durable write fails, so a caller that gates a downstream
   * durable write on this one can rely on "this key is on disk" once it
   * resolves.
   */
  put(address: string, publicKey: Uint8Array): Promise<void>;
};

export async function createSenderKeyCache(
  deps: SenderKeyCacheDeps,
): Promise<SenderKeyCache> {
  const { dataDir, writeFileDurable } = deps;
  const dir = path.join(dataDir, SENDER_KEYS_DIR_NAME);
  const keys = new Map<string, Uint8Array>();

  function keyPath(address: string): string {
    return path.join(dir, hexEncode(new TextEncoder().encode(address)));
  }

  async function loadFromDisk(): Promise<void> {
    let entries: string[];
    try {
      entries = await fsp.readdir(dir);
    } catch (err: unknown) {
      // A fresh sidecar has no keyring dir yet; that is an empty cache, not a
      // fault. Any other failure (EACCES, EIO, ...) must surface rather than
      // silently start with no cached keys.
      if (hasCode(err) && err.code === "ENOENT") return;
      throw err;
    }
    for (const entry of entries) {
      const loaded = await loadEntry(entry);
      if (loaded !== null) keys.set(loaded.address, loaded.publicKey);
    }
  }

  async function loadEntry(
    entry: string,
  ): Promise<{ address: string; publicKey: Uint8Array } | null> {
    // A crashed atomic write can leave a `<name>.tmp.<pid>.<rand>` file; its
    // name is not valid hex, so decoding the filename rejects it below. A
    // corrupt or mismatched file is logged and skipped rather than failing the
    // whole load -- one bad entry must not deny the sidecar its other keys.
    try {
      const filenameAddress = new TextDecoder().decode(hexDecode(entry));
      const raw = await fsp.readFile(path.join(dir, entry), "utf8");
      const parsed = StoredSenderKey(JSON.parse(raw));
      if (parsed instanceof type.errors) {
        throw new Error(`envelope invalid: ${parsed.summary}`);
      }
      if (parsed.address !== filenameAddress) {
        throw new Error(
          `filename address ${filenameAddress} does not match envelope address ${parsed.address}`,
        );
      }
      const publicKey = hexDecode(parsed.publicKey);
      if (publicKey.length !== ED25519_PUBLIC_KEY_BYTES) {
        throw new Error(
          `expected a ${ED25519_PUBLIC_KEY_BYTES}-byte key, got ${publicKey.length}`,
        );
      }
      return { address: parsed.address, publicKey };
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      logger.error`Skipping unreadable sender-key file ${entry}: ${message}`;
      return null;
    }
  }

  function get(address: string): Uint8Array | undefined {
    return keys.get(address);
  }

  async function put(address: string, publicKey: Uint8Array): Promise<void> {
    if (publicKey.length !== ED25519_PUBLIC_KEY_BYTES) {
      throw new Error(
        `refusing to cache a ${publicKey.length}-byte key for ${address}; expected ${ED25519_PUBLIC_KEY_BYTES}`,
      );
    }
    const contents = JSON.stringify({
      address,
      publicKey: hexEncode(publicKey),
    });
    await fsp.mkdir(dir, { recursive: true });
    await writeFileDurable(keyPath(address), contents);
    keys.set(address, publicKey);
  }

  await loadFromDisk();

  return { get, put };
}

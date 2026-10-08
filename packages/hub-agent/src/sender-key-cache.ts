// Local cache of the public keys the hub vouches for, keyed by sender
// address. The hub co-delivers each key on the `run.grants` barrier
// (`senderIdentities` on `RunGrantsFrame`); this cache retains it so
// inbound-verify works locally and while the sidecar is disconnected.
//
// FOREIGN senders' PUBLIC keys only -- no private material, unlike the
// own-agent key pairs AgentKeyStore holds. The hub owns freshness (it
// re-pushes a rotated key on the next grant or reconnect), so there is no
// TTL; eviction is revocation-driven via `sender.key.evict`, durably
// removing a key the hub no longer vouches for so a power loss cannot
// resurrect it.
//
// The whole keyring loads into memory at construction, so `get` is a
// synchronous memory read. The on-disk filename is the hex-encoded address
// bytes: injective and reversible, so two addresses never collide onto one
// file the way the lossy `sanitizeAddress` scheme would.
//
// A file that FAILS to load is not the same as a sender with no cached
// key, and the cache must not report it as one: the inbound-verify path
// reads an absent address as "no key available", which a policy can relax
// to admit, so dropping the entry would let an operator's corrupt file
// admit mail with no signature check. The load records the fault and
// `get` THROWS for it; the verify path's fault handling turns that throw
// into the verdict no policy relaxes. A fault clears on write or evict,
// and `rotatableAddresses` reports it so the reconnect re-push replaces
// the unreadable file.

import fsp from "node:fs/promises";
import path from "node:path";
import { type } from "arktype";
import { getLogger } from "@intx/log";
import {
  hasCode,
  hexDecode,
  hexEncode,
  isRunAddress,
  parseAddress,
} from "@intx/types";

const logger = getLogger(["interchange", "hub-agent", "sender-key-cache"]);

// A raw Ed25519 public key is 32 bytes.
const ED25519_PUBLIC_KEY_BYTES = 32;

/**
 * Length is the whole rule: a 32-byte value that is not the signer's key
 * imports fine and verifies to false, which is a check that ran and failed
 * rather than unusable material.
 */
export function isUsableSenderKey(publicKey: Uint8Array): boolean {
  return publicKey.length === ED25519_PUBLIC_KEY_BYTES;
}

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
   * from the durable-write owner so the cache's write is at least as durable
   * as the run-grants write it gates.
   */
  writeFileDurable: (path: string, contents: string) => Promise<void>;
  /**
   * Durably remove the file at `path` (unlink + parent-dir fsync). A raw
   * unlink can be resurrected by a power loss, re-staling the cache with a
   * revoked key, so the removal must be as durable as the write it reverses.
   */
  removeFileDurable: (path: string) => Promise<void>;
};

export type SenderKeyCache = {
  /**
   * The cached public key for `address`, or `undefined` when none is known.
   * A synchronous memory read; the keyring is authoritative for this
   * single-process sidecar.
   *
   * THROWS when the address's on-disk entry failed to load: `undefined` is a
   * sender condition (no key given), a load fault is an operator condition
   * about material that cannot be served, and the two must not share one
   * answer.
   */
  get(address: string): Uint8Array | undefined;
  /**
   * Cache `publicKey` for `address`, persisting it durably before returning.
   * Throws if the durable write fails, so a caller gating a downstream
   * durable write on this one can rely on "this key is on disk" once it
   * resolves. A successful write clears any load fault for `address`.
   */
  put(address: string, publicKey: Uint8Array): Promise<void>;
  /**
   * Remove the cached key for `address`, deleting its on-disk entry before
   * dropping it from memory. Disk-first mirrors `put`, so a failed removal
   * leaves both stores consistent (and the next reconnect retries) rather
   * than a memory-evicted key resurrecting from disk on restart. No-op for
   * an address the cache does not hold. Clears any load fault for `address`.
   */
  evict(address: string): Promise<void>;
  /**
   * Snapshot of every address the cache holds a key for. Fresh array,
   * decoupled from the backing map, so a later `put` does not change one a
   * caller still holds. Excludes faulted addresses: the cache holds no key
   * for them.
   */
  addresses(): string[];
  /**
   * The cached addresses the hub should re-resolve on reconnect, minus run
   * addresses. Two kinds qualify: addresses with a cached key (a user
   * sender's principal key can rotate while disconnected) and addresses
   * whose entry failed to load (the hub's re-push replaces the unreadable
   * file). Run addresses are excluded -- a run sender's key is the
   * immutable `workflow_run.public_key`, and its next grants barrier
   * re-pushes it anyway. A work-saving filter, not a trust boundary.
   */
  rotatableAddresses(): string[];
};

export async function createSenderKeyCache(
  deps: SenderKeyCacheDeps,
): Promise<SenderKeyCache> {
  const { dataDir, writeFileDurable, removeFileDurable } = deps;
  const dir = path.join(dataDir, SENDER_KEYS_DIR_NAME);
  const keys = new Map<string, Uint8Array>();
  // Addresses whose on-disk entry failed to load, mapped to the reason.
  // Disjoint from `keys`: a failed load stores no key, and `put`/`evict`
  // clear the fault after their disk work lands. `rotatableAddresses` relies
  // on that to concatenate without deduplicating.
  const loadFaults = new Map<string, string>();

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
    // A crashed atomic write leaves a `<name>.tmp.<pid>.<rand>` file whose
    // name is not valid hex, so decoding the filename rejects it below. A
    // corrupt or mismatched file does not fail the whole load, but is
    // attributed to an address so `get` can refuse it.
    let filenameAddress: string | null = null;
    try {
      filenameAddress = new TextDecoder().decode(hexDecode(entry));
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
      if (!isUsableSenderKey(publicKey)) {
        throw new Error(
          `expected a ${ED25519_PUBLIC_KEY_BYTES}-byte key, got ${publicKey.length}`,
        );
      }
      return { address: parsed.address, publicKey };
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      // Attributed to the address the FILENAME names, whatever the envelope
      // claims: that is the address a reader asks for. An entry that loads is
      // still keyed on the envelope address the cross-check proved equal.
      const faultedAddress =
        filenameAddress !== null && parseAddress(filenameAddress) !== null
          ? filenameAddress
          : null;
      if (faultedAddress === null) {
        // Debris rather than an entry: no address to refuse reads for, and
        // nothing the hub could re-push to repair.
        logger.error`Skipping unreadable sender-key file ${entry}: ${message}`;
        return null;
      }
      loadFaults.set(faultedAddress, message);
      logger.error`Sender-key file ${entry} for ${faultedAddress} did not load, so this sidecar refuses to verify that sender's mail until the hub re-pushes the key: ${message}`;
      return null;
    }
  }

  function get(address: string): Uint8Array | undefined {
    const fault = loadFaults.get(address);
    if (fault !== undefined) {
      throw new Error(
        `the cached sender key for ${JSON.stringify(address)} failed to load from the keyring: ${fault}`,
      );
    }
    return keys.get(address);
  }

  function addresses(): string[] {
    return [...keys.keys()];
  }

  function rotatableAddresses(): string[] {
    return [...addresses(), ...loadFaults.keys()].filter(
      (address) => !isRunAddress(address),
    );
  }

  async function put(address: string, publicKey: Uint8Array): Promise<void> {
    if (!isUsableSenderKey(publicKey)) {
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
    loadFaults.delete(address);
  }

  async function evict(address: string): Promise<void> {
    await removeFileDurable(keyPath(address));
    keys.delete(address);
    loadFaults.delete(address);
  }

  await loadFromDisk();

  return { get, put, evict, addresses, rotatableAddresses };
}

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
// grant or reconnect -- so the cache has no TTL or generation. A second
// entry for an address overwrites the first; the latest push wins.
// Eviction is revocation-driven, not time-driven: the hub sends a
// `sender.key.evict` when it re-resolves a reported cached sender to no
// durable key (a deleted principal), and the cache durably removes it so a
// power loss cannot resurrect a key the hub no longer vouches for.
//
// The whole keyring is loaded into memory at construction, so `get` is a
// synchronous memory read on the inbound-verify path and a restart sees
// every previously-cached key. The on-disk filename is the hex-encoded
// address bytes: an injective, reversible handle, so two addresses never
// collide onto one file the way the lossy `sanitizeAddress` scheme would.
//
// A file that FAILS to load is not the same condition as a sender with no
// cached key, and the cache must not report it as one: the inbound-verify path
// reads an absent address as "no key was available", which a workflow author can
// relax to admit, so dropping the entry would turn an operator's corrupt file
// into mail admitted with no signature check. The load therefore records the
// fault against the address and `get` THROWS for it; the verify path's own fault
// handling is what turns that throw into the verdict no policy relaxes. A fault
// clears when a key is written for the address or the address is evicted, and
// `rotatableAddresses` reports it for the reconnect re-resolve so the hub's
// re-push replaces the unreadable file.

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
   * so the cache's write is at least as durable as the run-grants write it
   * gates, without this package depending on the sidecar app that owns the
   * durable-write primitive.
   */
  writeFileDurable: (path: string, contents: string) => Promise<void>;
  /**
   * Durably remove the file at `path` (unlink + parent-dir fsync). Injected
   * from the same durable-primitive owner as `writeFileDurable`. A raw unlink
   * can be resurrected by a power loss, re-staling the cache with a key the hub
   * revoked -- the exact failure eviction exists to prevent -- so the removal
   * must be as durable as the write it reverses.
   */
  removeFileDurable: (path: string) => Promise<void>;
};

export type SenderKeyCache = {
  /**
   * The cached public key for `address`, or `undefined` when none is known.
   * A synchronous memory read: the keyring is loaded at construction and
   * every `put` updates memory, so the map is authoritative for this
   * single-process sidecar.
   *
   * THROWS when the address's on-disk entry failed to load. `undefined` means
   * the cache was never given a key for this sender, which a caller may treat as
   * a sender condition; a load fault is an operator condition about material the
   * cache was given and cannot serve, so the two must not share one answer.
   */
  get(address: string): Uint8Array | undefined;
  /**
   * Cache `publicKey` for `address`, persisting it durably before returning.
   * Throws if the durable write fails, so a caller that gates a downstream
   * durable write on this one can rely on "this key is on disk" once it
   * resolves. A successful write clears any load fault recorded for `address`:
   * the file it replaces is the one that failed to load.
   */
  put(address: string, publicKey: Uint8Array): Promise<void>;
  /**
   * Remove the cached key for `address`, durably deleting its on-disk entry
   * before dropping it from memory. Disk-first mirrors `put`'s write-then-set:
   * the map is rebuilt from disk on construction, so if the durable removal
   * fails and throws, both stores still hold the key (consistent, and the next
   * reconnect retries) rather than a memory-evicted key resurrecting from disk
   * on restart. A no-op for an address the cache does not hold. A successful
   * removal also clears any load fault recorded for `address`, and for the same
   * disk-first reason: the fault outlives a failed removal, because the file
   * that caused it is still there.
   */
  evict(address: string): Promise<void>;
  /**
   * A snapshot of every address the cache currently holds a key for. Returns a
   * fresh array, decoupled from the backing map, so a later `put` does not
   * change an array a caller is still holding -- the reported set stays stable
   * across an `await`. The cache reports addresses only; the key values stay
   * inside because the refresh-on-reconnect flow re-resolves each current key
   * hub-side rather than trusting the cached (possibly stale) one. An address
   * whose entry failed to load is NOT here: the cache holds no key for it.
   */
  addresses(): string[];
  /**
   * The cached addresses the hub should re-resolve on reconnect, minus run
   * addresses. Two kinds qualify: an address the cache holds a key for, because
   * a user sender's hub principal key can rotate while the sidecar is
   * disconnected; and an address whose on-disk entry failed to load, because the
   * hub's re-push is what replaces the file the cache cannot read. Run addresses
   * are excluded in both cases -- a run sender's key is the immutable
   * `workflow_run.public_key`, the hub skips a reported run address whatever this
   * returns, and a run sender's next grants barrier re-pushes its key anyway,
   * which repairs a faulted file through `put`. This is a work-saving filter, not
   * a trust boundary -- the hub resolves every reported address on its own.
   */
  rotatableAddresses(): string[];
};

export async function createSenderKeyCache(
  deps: SenderKeyCacheDeps,
): Promise<SenderKeyCache> {
  const { dataDir, writeFileDurable, removeFileDurable } = deps;
  const dir = path.join(dataDir, SENDER_KEYS_DIR_NAME);
  const keys = new Map<string, Uint8Array>();
  // Addresses whose on-disk entry failed to load, mapped to the reason. Disjoint
  // from `keys`: a failed load stores no key, and `put` and `evict` clear the
  // fault for the address they touch after their disk work lands, so no address
  // is ever in both. `rotatableAddresses` relies on that to concatenate this
  // map's keys onto `addresses()` without deduplicating them.
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
    // A crashed atomic write can leave a `<name>.tmp.<pid>.<rand>` file; its
    // name is not valid hex, so decoding the filename rejects it below. A
    // corrupt or mismatched file does not fail the whole load -- one bad entry
    // must not deny the sidecar its other keys -- but it is not simply dropped
    // either; the catch attributes it to an address so `get` can refuse it.
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
      // claims: that is the address a reader asks for, and the one whose file
      // this is. Only the fault is attributed here -- an entry that loads is
      // still keyed on the envelope address the cross-check just proved equal.
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

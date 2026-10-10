// Atomic, durable file replacement for the sidecar's non-rebuildable
// on-disk records. Distinct from the cache's rebuildable temp+rename
// (no fsync, a lost write just forces a re-fetch) and from
// `fsyncWriteFile`'s in-place fsync write (no atomicity, a torn write
// leaves a half-file): this is the tier for a sole restore source that
// must survive both a process kill and a power loss without ever
// exposing a torn record.

import fs from "node:fs/promises";
import { dirname } from "node:path";

import { getLogger } from "@intx/log";
import {
  syncDirectoryDurable,
  syncRemovedPathDurable,
} from "@intx/storage-isogit/node";
import { hasCode, hexEncode } from "@intx/types";

const logger = getLogger(["interchange", "sidecar", "atomic-write"]);

export interface AtomicWriteOptions {
  /** Permission mode applied when the temp file is created. */
  mode: number;
  /** Reject rather than degrade when the directory entry cannot be synced. */
  requireDirectorySync?: boolean;
}

/**
 * Replace `path` with `contents` atomically and durably. The bytes land
 * in a fresh per-write temp file that is fsynced and then `rename`d over
 * `path`; because rename is atomic within a directory, a reader only
 * ever observes the prior complete file or the new complete file, never
 * a torn one. The fsync before the rename is what extends that
 * guarantee past process death to OS crash / power loss: without it, the
 * ext4 delayed-allocation window can surface the renamed path as a
 * zero-length file after a power loss.
 *
 * The parent directory is fsynced after the rename so the new link is
 * itself durable. By default, a failed directory sync is logged. Set
 * `requireDirectorySync` when acknowledging this write permits a caller to
 * release an obligation that the previous file still records.
 *
 * `mode` is applied on the temp file's creation, so it takes effect on
 * every write. A plain in-place overwrite of an existing file would
 * silently keep the original file's mode instead.
 *
 * The temp file follows `createTarballCache`'s `.tmp.<pid>.<rand>`
 * naming convention for consistency across the sidecar's staged writes.
 */
export async function writeFileAtomicDurable(
  path: string,
  contents: string,
  options: AtomicWriteOptions,
): Promise<void> {
  const tmp = `${path}.tmp.${String(process.pid)}.${hexEncode(crypto.getRandomValues(new Uint8Array(8)))}`;
  try {
    const handle = await fs.open(tmp, "w", options.mode);
    try {
      await handle.writeFile(contents);
      await handle.sync();
    } finally {
      await handle.close();
    }
    await fs.rename(tmp, path);
  } catch (cause) {
    // The write failed and is about to rethrow; unlink the temp so a
    // failed write leaves no orphan. Best-effort: the temp may never
    // have been created, and a second failure here must not mask the
    // original cause.
    await fs.unlink(tmp).catch(() => undefined);
    throw cause;
  }

  try {
    await syncDirectoryDurable(dirname(path));
  } catch (err) {
    if (options.requireDirectorySync === true) throw err;
    logger.warn`parent-dir fsync failed for ${path}; durability is degraded but the file is renamed and fsynced — ${err instanceof Error ? err.message : String(err)}`;
  }
}

/**
 * Remove `path` durably: unlink it, then fsync the parent directory so the
 * removal survives a power loss. A raw `unlink` leaves the directory-entry
 * removal in the OS's delayed-metadata window, so a power loss can resurrect
 * the file -- for a cache whose on-disk entry is the restore source, a
 * resurrected entry re-stales the cache. Idempotent: a file already absent is a
 * completed removal, so ENOENT on the unlink is success.
 *
 * Directory sync runs even when the file is absent, committing a prior
 * removal whose sync failed. By default, sync failures are logged. Strict
 * mode propagates failures and, if the parent was also removed, syncs the
 * nearest remaining ancestor so that directory removal is durable too.
 */
export async function removeFileAtomicDurable(
  path: string,
  { requireDirectorySync = false }: { requireDirectorySync?: boolean } = {},
): Promise<void> {
  try {
    await fs.unlink(path);
  } catch (err) {
    if (!(hasCode(err) && err.code === "ENOENT")) throw err;
  }

  try {
    if (requireDirectorySync) await syncRemovedPathDurable(path);
    else await syncDirectoryDurable(dirname(path));
  } catch (err) {
    if (requireDirectorySync) throw err;
    logger.warn`parent-dir fsync failed for ${path}; durability is degraded but the file is unlinked — ${err instanceof Error ? err.message : String(err)}`;
  }
}

// Atomic, durable file replacement for the sidecar's non-rebuildable
// on-disk records. Distinct from the cache's rebuildable temp+rename
// (no fsync, a lost write just forces a re-fetch) and from
// `fsyncWriteFile`'s in-place fsync write (no atomicity, a torn write
// leaves a half-file): this is the tier for a sole restore source that
// must survive both a process kill and a power loss without ever
// exposing a torn record.

import { open, rename, unlink } from "node:fs/promises";
import { dirname } from "node:path";

import { getLogger } from "@intx/log";
import { hexEncode } from "@intx/types";

const logger = getLogger(["interchange", "sidecar", "atomic-write"]);

export interface AtomicWriteOptions {
  /** Permission mode applied when the temp file is created. */
  mode: number;
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
 * itself durable, but a filesystem that rejects directory fsync
 * (FAT/exFAT, some network mounts) only degrades durability -- the file
 * is already renamed and fsynced -- so that failure is logged, not
 * thrown.
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
    const handle = await open(tmp, "w", options.mode);
    try {
      await handle.writeFile(contents);
      await handle.sync();
    } finally {
      await handle.close();
    }
    await rename(tmp, path);
  } catch (cause) {
    // The write failed and is about to rethrow; unlink the temp so a
    // failed write leaves no orphan. Best-effort: the temp may never
    // have been created, and a second failure here must not mask the
    // original cause.
    await unlink(tmp).catch(() => undefined);
    throw cause;
  }

  try {
    const dirHandle = await open(dirname(path), "r");
    try {
      await dirHandle.sync();
    } finally {
      await dirHandle.close();
    }
  } catch (err) {
    logger.warn`parent-dir fsync failed for ${path}; durability is degraded but the file is renamed and fsynced — ${err instanceof Error ? err.message : String(err)}`;
  }
}

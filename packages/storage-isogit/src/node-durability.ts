import fs from "node:fs/promises";
import { dirname } from "node:path";

import { hasCode } from "@intx/types";

/** Persist a directory's entries, propagating unsupported or failed syncs. */
export async function syncDirectoryDurable(path: string): Promise<void> {
  const handle = await fs.open(path, "r");
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}

/**
 * Commit a removed file or tree's directory entry, including a prior removal
 * whose sync failed. If its parent was removed too, sync the nearest surviving
 * ancestor. Other failures must propagate before cleanup is acknowledged.
 */
export async function syncRemovedPathDurable(path: string): Promise<void> {
  let directory = dirname(path);
  for (;;) {
    try {
      await syncDirectoryDurable(directory);
      return;
    } catch (err) {
      const parent = dirname(directory);
      if (!hasCode(err) || err.code !== "ENOENT" || parent === directory)
        throw err;
      directory = parent;
    }
  }
}

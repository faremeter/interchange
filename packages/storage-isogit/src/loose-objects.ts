import { hasCode } from "@intx/types/has-code";
import { syncPath, type StorageRuntime } from "./runtime";
import {
  forgetSyncRecord,
  noteFanoutWrite,
  syncObjectsAndRefs,
} from "./sync-record";

// Inside objects/, so a rename from here into objects/pack/ or a fan-out
// directory stays in one storage namespace, where the runtime makes it atomic.
const STAGING_DIR_NAME = "pack-staging";

export function stagingRootOf(runtime: StorageRuntime, dir: string): string {
  return runtime.path.join(dir, ".git", "objects", STAGING_DIR_NAME);
}

/**
 * Writes loose objects so a crash leaves each one whole or absent: each is
 * written to a temporary file, synced, and renamed into place, over any
 * file already there. `keep` names an object already in place instead,
 * which is trusted to be whole although its directory entry may not be
 * synced yet: a file that a write which does not sync left torn after a
 * crash stays torn, and a read of it fails. `finish` syncs `objects/` and
 * the fan-out directory of every object written or kept, with what
 * `syncObjectsAndRefs` finds unsynced, so each is durable once it returns
 * on a runtime that syncs. A writer dropped before `finish` leaves its
 * fan-out directories to the next `syncObjectsAndRefs` of the repository.
 */
export type LooseObjectWriter = {
  write(oid: string, deflated: Uint8Array): Promise<void>;
  keep(oid: string): void;
  finish(): Promise<void>;
};

/**
 * A writer for the loose objects of the repository at `dir`. Each
 * temporary file is `tempPrefix` and a number; it must be in the
 * repository's `objects/`, so its rename stays in one storage namespace.
 */
export function createLooseObjectWriter(
  runtime: StorageRuntime,
  dir: string,
  tempPrefix: string,
): LooseObjectWriter {
  const objectsDir = runtime.path.join(dir, ".git", "objects");
  const fanouts = new Map<string, Promise<void>>();
  const ensureFanout = (fanout: string): Promise<void> => {
    let made = fanouts.get(fanout);
    if (made === undefined) {
      made = runtime.fs.mkdir(fanout).catch((cause: unknown) => {
        if (hasCode(cause) && cause.code === "EEXIST") return;
        throw cause;
      });
      fanouts.set(fanout, made);
    }
    return made;
  };
  let temps = 0;
  const forgettingOnFailure = async (step: () => Promise<void>) => {
    try {
      await step();
    } catch (cause) {
      forgetSyncRecord(runtime, dir);
      throw cause;
    }
  };
  return {
    write: (oid, deflated) =>
      forgettingOnFailure(async () => {
        const fanout = runtime.path.join(objectsDir, oid.slice(0, 2));
        noteFanoutWrite(runtime, dir, fanout);
        const temp = `${tempPrefix}${String(temps++)}`;
        try {
          await runtime.fs.writeFile(temp, deflated);
          await syncPath(runtime, temp);
          await ensureFanout(fanout);
          await runtime.fs.rename(
            temp,
            runtime.path.join(fanout, oid.slice(2)),
          );
        } catch (cause) {
          await runtime.fs.remove(temp, { force: true }).catch(() => undefined);
          throw cause;
        }
      }),
    keep(oid) {
      const fanout = runtime.path.join(objectsDir, oid.slice(0, 2));
      if (!fanouts.has(fanout)) fanouts.set(fanout, Promise.resolve());
    },
    finish: () =>
      forgettingOnFailure(async () => {
        await Promise.all(fanouts.values());
        await syncObjectsAndRefs(runtime, dir, [...fanouts.keys()]);
      }),
  };
}

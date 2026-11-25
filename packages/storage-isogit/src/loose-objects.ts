import type { FsClient } from "isomorphic-git";
import { hasCode } from "@intx/types/has-code";
import { hexEncode } from "@intx/types/hex";
import { promiseFsMethods, syncPath, type StorageRuntime } from "./runtime";
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

const LOOSE_OBJECT_PATH = /^[0-9a-f]{2}[/\\][0-9a-f]{38}$/;

/**
 * Writes loose objects so a crash leaves each one whole or absent: each is
 * written to a temporary file, synced, and renamed into place, over any
 * file already there. `keep` names an object already in place, whose
 * directory entry may not be synced yet. It trusts the file to be whole:
 * one a crash left torn, as in a repository an older Hub wrote without
 * syncing, stays torn, and a read of it fails. `finish` runs
 * `syncObjectsAndRefs` with the fan-out directory of every object written
 * or kept, so each is durable once it returns on a runtime that syncs. A
 * writer dropped before `finish` leaves its fan-out directories to the
 * next `syncObjectsAndRefs` of the repository.
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

/**
 * An isomorphic-git client for commits whose objects must be durable
 * before a ref names them. It writes each new loose object through a
 * `LooseObjectWriter`. isomorphic-git skips an object already in place,
 * and the client keeps it. Every other call passes through. Once
 * `finish` returns, every object made through `fs` is durable, and the
 * caller can move a ref to them, for example with `writeRefDurably`.
 *
 * The caller holds the repository's lock until `finish` returns, as for a
 * staged pack.
 */
export function createDurableObjectWrites(
  runtime: StorageRuntime,
  dir: string,
): { fs: FsClient; finish(): Promise<void> } {
  const objectsDir = runtime.path.resolve(
    runtime.path.join(dir, ".git", "objects"),
  );
  const stagingRoot = stagingRootOf(runtime, dir);
  const writer = createLooseObjectWriter(
    runtime,
    dir,
    runtime.path.join(
      stagingRoot,
      `write-${hexEncode(crypto.getRandomValues(new Uint8Array(8)))}-`,
    ),
  );
  let staging: Promise<void> | undefined;
  const looseOid = (filepath: unknown): string | undefined => {
    if (typeof filepath !== "string") return undefined;
    const relative = runtime.path.relative(
      objectsDir,
      runtime.path.resolve(filepath),
    );
    return LOOSE_OBJECT_PATH.test(relative)
      ? relative.replace(/[/\\]/, "")
      : undefined;
  };
  const base = promiseFsMethods(runtime.fs.git);
  // isomorphic-git binds `promises` only when it is an enumerable own
  // property; otherwise it binds the client as a callback filesystem.
  const fs: FsClient = {
    promises: {
      ...base,
      // isomorphic-git stats a loose object before it writes it, and skips
      // one it finds.
      stat: async (...args: unknown[]) => {
        const found = await base.stat(...args);
        const oid = looseOid(args[0]);
        if (oid !== undefined) writer.keep(oid);
        return found;
      },
      writeFile: async (...args: unknown[]) => {
        const [filepath, data] = args;
        const oid = looseOid(filepath);
        if (oid === undefined) return base.writeFile(...args);
        if (!(data instanceof Uint8Array)) {
          throw new TypeError(`loose object ${oid} is not bytes`);
        }
        staging ??= runtime.fs.mkdir(stagingRoot, { recursive: true });
        await staging;
        await writer.write(oid, data);
      },
    },
  };
  return { fs, finish: () => writer.finish() };
}

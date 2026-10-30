import { hasCode } from "@intx/types/has-code";
import { hexEncode } from "@intx/types/hex";

import type { StorageRuntime } from "./runtime";

// Version 2 pack index layout: magic, version, a 256-entry fanout of
// cumulative object counts by first byte, then the sorted object names.
const INDEX_MAGIC = [0xff, 0x74, 0x4f, 0x63];
const FANOUT_OFFSET = 8;
const NAMES_OFFSET = FANOUT_OFFSET + 256 * 4;
const NAME_BYTES = 20;

function addPackIndexNames(
  bytes: Uint8Array,
  filepath: string,
  names: Set<string>,
): void {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (
    bytes.byteLength < NAMES_OFFSET ||
    INDEX_MAGIC.some((byte, i) => view.getUint8(i) !== byte) ||
    view.getUint32(4) !== 2
  ) {
    throw new Error(`${filepath} is not a version 2 pack index`);
  }
  const count = view.getUint32(FANOUT_OFFSET + 255 * 4);
  if (bytes.byteLength < NAMES_OFFSET + count * NAME_BYTES) {
    throw new Error(`${filepath} is shorter than its ${String(count)} names`);
  }
  for (let i = 0; i < count; i++) {
    const start = NAMES_OFFSET + i * NAME_BYTES;
    names.add(hexEncode(bytes.subarray(start, start + NAME_BYTES)));
  }
}

export interface ObjectPresence {
  /**
   * Bring the set to the packs published now: read the index of each
   * pack published since the last update, and start over when a pack
   * read before is gone. Updates run one at a time.
   */
  update(): Promise<void>;
  /**
   * Whether the repository holds `oid` loose, or in a pack it held at
   * the last update.
   */
  holds(oid: string): Promise<boolean>;
}

/**
 * Tell whether the repository at `dir` holds an object, in a published
 * pack or loose, without reading the object. A receive asks this for
 * every blob its trees name, and reading each blob, as isomorphic-git's
 * object reads do, would cost the bytes of the whole tree per receive.
 * The names of every pack index go into one set, so a query costs one
 * lookup however many packs the repository has. A caller keeps the
 * result across receives, so each update reads only the indexes of the
 * packs published since the one before.
 */
export function createObjectPresence(
  runtime: StorageRuntime,
  dir: string,
): ObjectPresence {
  const objectsDir = runtime.path.join(dir, ".git", "objects");
  const packDir = runtime.path.join(objectsDir, "pack");
  let indexes = new Set<string>();
  let names = new Set<string>();
  let updating: Promise<void> = Promise.resolve();

  async function listIndexes(): Promise<string[]> {
    try {
      return (await runtime.fs.readdir(packDir)).filter((name) =>
        name.endsWith(".idx"),
      );
    } catch (cause) {
      if (hasCode(cause) && cause.code === "ENOENT") return [];
      throw cause;
    }
  }

  // A pack can go only by a GC or by a loss, and a set that kept its
  // names would answer for objects the repository no longer holds. The
  // rebuilt set replaces the old one only once it is complete, so a
  // query that runs alongside never sees a partial set.
  async function sync(): Promise<void> {
    const files = await listIndexes();
    const listed = new Set(files);
    const restart = [...indexes].some((file) => !listed.has(file));
    const nextIndexes = restart ? new Set<string>() : indexes;
    const nextNames = restart ? new Set<string>() : names;
    for (const file of files) {
      if (nextIndexes.has(file)) continue;
      const filepath = runtime.path.join(packDir, file);
      addPackIndexNames(
        await runtime.fs.readFile(filepath),
        filepath,
        nextNames,
      );
      nextIndexes.add(file);
    }
    indexes = nextIndexes;
    names = nextNames;
  }

  async function looseHolds(oid: string): Promise<boolean> {
    try {
      await runtime.fs.stat(
        runtime.path.join(objectsDir, oid.slice(0, 2), oid.slice(2)),
      );
      return true;
    } catch (cause) {
      if (hasCode(cause) && cause.code === "ENOENT") return false;
      throw cause;
    }
  }

  return {
    update() {
      updating = updating.catch(() => undefined).then(sync);
      return updating;
    },
    async holds(oid) {
      if (!/^[0-9a-f]{40}$/.test(oid)) {
        throw new Error(
          `object id ${JSON.stringify(oid)} is not 40 hex digits`,
        );
      }
      return names.has(oid) || looseHolds(oid);
    },
  };
}

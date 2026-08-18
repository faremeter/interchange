// Bound a pushed pack's per-object INFLATED size before `git.indexPack` runs.
//
// `git.indexPack` inflates every object to hash it; a single highly compressible
// blob (32 MiB of zeros inflates ~1000x) allocates its full inflated size in
// memory during indexing, before any disk-side checkout guard runs. This walks
// the pack object-by-object and inflates each with a HARD output cap: an object
// whose zlib stream inflates past the per-object ceiling is rejected before it
// can allocate. The cap is enforced on the ACTUAL inflation, not the size the
// object header claims, so a header that under-declares a bomb does not slip by.
//
// This is node-only (it uses `node:zlib`); the node entry wraps
// `indexPackIntoGitDir` with it, so index-pack.ts stays browser-safe. It does
// NOT reconstruct deltas, so a crafted delta chain that reconstructs to a large
// object is not caught here. That residual is DETECTED at the disk boundary by
// `writeTreeToDisk`'s cumulative-byte cap (which allocates the reconstructed
// blob once before rejecting it, so it bounds the total footprint, not that one
// blob's peak); the definitive bound on single-object inflation belongs at the
// hub ingest that first receives and indexes the pushed pack.

import zlib from "node:zlib";

import type { PackMaterializationLimits } from "./materialization-limits";

const PACK_HEADER_BYTES = 12;
const PACK_TRAILER_BYTES = 20; // trailing pack SHA-1 checksum
const OBJ_OFS_DELTA = 6;
const OBJ_REF_DELTA = 7;
const REF_DELTA_BASE_BYTES = 20;

interface ObjectHeader {
  readonly type: number;
  readonly headerLength: number;
}

// Read one byte, failing loud rather than returning `undefined` when the walk
// runs off the end of a truncated pack.
function byteAt(pack: Uint8Array, index: number): number {
  const b = pack[index];
  if (b === undefined) {
    throw new Error("pack object framing runs past the end of the pack");
  }
  return b;
}

// Read one object's variable-length type+size header, returning its type and
// the header's byte length. The declared size is not trusted for the bound (the
// inflate enforces it), only used to frame the header.
function readObjectHeader(pack: Uint8Array, offset: number): ObjectHeader {
  let ptr = offset;
  let c = byteAt(pack, ptr);
  ptr += 1;
  const type = (c >> 4) & 0b111;
  while ((c & 0x80) !== 0) {
    c = byteAt(pack, ptr);
    ptr += 1;
  }
  return { type, headerLength: ptr - offset };
}

// Skip an OFS_DELTA base-offset varint, returning its byte length. Only the
// length matters here; the offset value is not needed for the size walk.
function ofsDeltaLength(pack: Uint8Array, offset: number): number {
  let ptr = offset;
  let c = byteAt(pack, ptr);
  ptr += 1;
  while ((c & 0x80) !== 0) {
    c = byteAt(pack, ptr);
    ptr += 1;
  }
  return ptr - offset;
}

// Sentinel error the output-cap check raises, distinguished from a corrupt-
// stream zlib error so the caller can report the two differently.
const CAP_EXCEEDED = "pack-object-inflation-cap-exceeded";

// Inflate the zlib stream at `zlibStart`, discarding its output but rejecting
// once the produced bytes exceed `cap`, and resolve with the COMPRESSED bytes
// the stream consumed so the caller can advance to the next object. The cap is
// checked on the ACTUAL produced output (not the size the header claims), and
// the stream is destroyed the moment it is exceeded, so a bomb never allocates
// past ~`cap` plus one chunk.
function inflatedCompressedLength(
  pack: Uint8Array,
  zlibStart: number,
  cap: number,
): Promise<number> {
  return new Promise((resolve, reject) => {
    const inflate = zlib.createInflate();
    let produced = 0;
    inflate.on("data", (chunk: Buffer) => {
      produced += chunk.length;
      if (produced > cap) {
        inflate.destroy(new Error(CAP_EXCEEDED));
      }
    });
    inflate.on("end", () => {
      resolve(inflate.bytesWritten);
    });
    inflate.on("error", (err) => {
      reject(err);
    });
    inflate.end(
      Buffer.from(
        pack.buffer,
        pack.byteOffset + zlibStart,
        pack.length - zlibStart,
      ),
    );
  });
}

/**
 * Walk `pack`'s objects and fail loud if any object inflates past
 * `limits.maxObjectInflatedBytes`. Runs BEFORE `git.indexPack`, so a
 * single-huge-blob zip bomb is rejected before it can allocate its inflated
 * size.
 */
export async function assertPackInflationWithinBounds(
  pack: Uint8Array,
  limits: PackMaterializationLimits,
): Promise<void> {
  if (pack.length < PACK_HEADER_BYTES + PACK_TRAILER_BYTES) {
    throw new Error(
      "pack is shorter than a header plus trailer; refusing to scan",
    );
  }
  const view = new DataView(pack.buffer, pack.byteOffset, pack.byteLength);
  const objectCount = view.getUint32(8, false);
  // Reject an over-count pack from the header BEFORE the per-object inflation
  // walk: the count is the cheap O(1) gate, so it must precede inflating every
  // declared object. `indexPackIntoGitDir` re-checks the same cap, but only
  // after this scan; gating here rejects an object-count bomb up front.
  if (objectCount > limits.maxPackObjects) {
    throw new Error(
      `pack declares ${String(objectCount)} objects, exceeding the ${String(limits.maxPackObjects)}-object cap`,
    );
  }
  const cap = limits.maxObjectInflatedBytes;

  let offset = PACK_HEADER_BYTES;
  for (let i = 0; i < objectCount; i++) {
    const header = readObjectHeader(pack, offset);
    offset += header.headerLength;
    if (header.type === OBJ_OFS_DELTA) {
      offset += ofsDeltaLength(pack, offset);
    } else if (header.type === OBJ_REF_DELTA) {
      offset += REF_DELTA_BASE_BYTES;
    }

    let consumed: number;
    try {
      consumed = await inflatedCompressedLength(pack, offset, cap);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      if (message === CAP_EXCEEDED) {
        throw new Error(
          `pack object ${String(i)} inflates past the ${String(cap)}-byte per-object cap`,
        );
      }
      throw new Error(`pack object ${String(i)} is corrupt: ${message}`);
    }
    offset += consumed;
  }

  // The walk must land exactly on the trailer, else the object framing was
  // misread; fail loud rather than hand a pack we did not understand onward.
  if (offset !== pack.length - PACK_TRAILER_BYTES) {
    throw new Error(
      `pack object walk ended at ${String(offset)}, not the trailer at ${String(pack.length - PACK_TRAILER_BYTES)}`,
    );
  }
}

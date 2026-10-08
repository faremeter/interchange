// Pack chunking for `repo.pack.push` frames: split a packfile into base64 chunks.

import { base64Encode } from "@intx/types";

export const PACK_CHUNK_SIZE = 64 * 1024;

export type PackChunk = {
  seq: number;
  data: string;
};

/** Split a packfile into ordered chunks; each is at most PACK_CHUNK_SIZE bytes. */
export function chunkPack(pack: Uint8Array): PackChunk[] {
  const chunks: PackChunk[] = [];
  let seq = 0;
  for (let offset = 0; offset < pack.length; offset += PACK_CHUNK_SIZE) {
    const slice = pack.slice(offset, offset + PACK_CHUNK_SIZE);
    chunks.push({
      seq: seq++,
      data: base64Encode(slice),
    });
  }
  return chunks;
}

// Pack chunking utility for sending packfiles over the WebSocket.
//
// Splits a packfile into base64-encoded chunks suitable for repo.pack.push frames.

import { base64Encode } from "@intx/types";

export const PACK_CHUNK_SIZE = 64 * 1024;

export type PackChunk = {
  seq: number;
  data: string;
};

/**
 * Split a packfile into ordered chunks for transmission as repo.pack.push frames.
 * Each chunk is at most PACK_CHUNK_SIZE bytes before base64 encoding.
 */
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

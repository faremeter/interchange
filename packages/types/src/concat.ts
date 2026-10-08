// Concatenate byte arrays into a single Uint8Array.
//
// A Web-standard replacement for Node's `Buffer.concat`: sum the chunk
// lengths, allocate once, and copy each chunk in at its running offset.

export function concatBytes(chunks: Uint8Array[]): Uint8Array {
  let total = 0;
  for (const chunk of chunks) {
    total += chunk.length;
  }
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}

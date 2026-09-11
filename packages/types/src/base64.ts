// Base64 codec for byte strings.
//
// Used to ship binary mail bodies over text-only WebSocket frames.
// Centralizing here keeps the encoding stable across the sidecar/hub
// boundary and any other consumer that needs the same wire shape.

// Bytes per encoding chunk. Must be a multiple of 3 so each chunk except the
// last encodes to whole base64 groups with no padding, letting the per-chunk
// outputs be joined without re-encoding. The bound keeps intermediates small
// even for a body at the 44 MB `mail.outbound` cap, which the unchunked
// per-byte string build inflated to multi-GB RSS.
const BASE64_CHUNK_BYTES = 24576;

function encodeChunk(bytes: Uint8Array, start: number, end: number): string {
  const chars = new Array<string>(end - start);
  let i = 0;
  for (const byte of bytes.subarray(start, end)) {
    chars[i++] = String.fromCharCode(byte);
  }
  return btoa(chars.join(""));
}

export function base64Encode(bytes: Uint8Array): string {
  const parts: string[] = [];
  for (let start = 0; start < bytes.length; start += BASE64_CHUNK_BYTES) {
    const end = Math.min(start + BASE64_CHUNK_BYTES, bytes.length);
    parts.push(encodeChunk(bytes, start, end));
  }
  return parts.join("");
}

export function base64Decode(base64: string): Uint8Array {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}

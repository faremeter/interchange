/**
 * Text canonicalization for PGP/MIME signing: CRLF line endings, trailing
 * whitespace removed, 7-bit clean. Binary parts (base64, quoted-printable)
 * are handled at the MIME assembly layer.
 */

/**
 * Canonicalize text for PGP/MIME signing: strip trailing whitespace,
 * normalize CRLF/LF/CR to CRLF, and throw on any byte >= 0x80.
 */
export function canonicalizeText(text: string): Uint8Array {
  const lines = text.split(/\r\n|\r|\n/);
  const stripped = lines.map((line) => line.replace(/[ \t]+$/, ""));
  const canonical = stripped.join("\r\n");
  const bytes = new TextEncoder().encode(canonical);
  for (let i = 0; i < bytes.length; i++) {
    const byte = bytes[i];
    if (byte !== undefined && byte >= 0x80) {
      throw new Error(
        `Content is not 7-bit clean: byte 0x${byte.toString(16)} at offset ${i}`,
      );
    }
  }
  return bytes;
}

/**
 * Canonicalize already-encoded bytes (base64, quoted-printable): CRLF
 * normalization and trailing-whitespace stripping only, since transfer
 * encoding already constrains the bytes to printable ASCII.
 */
export function canonicalizeBytes(content: Uint8Array): Uint8Array {
  const text = new TextDecoder("utf-8").decode(content);
  const lines = text.split(/\r\n|\r|\n/);
  const stripped = lines.map((line) => line.replace(/[ \t]+$/, ""));
  const canonical = stripped.join("\r\n");
  return new TextEncoder().encode(canonical);
}

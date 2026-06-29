/**
 * Content canonicalization for PGP/MIME signed messages.
 *
 * Per MESSAGE.md signing process: CRLF line endings, trailing whitespace
 * removed, 7-bit encoding applied. This module handles text canonicalization
 * only — binary parts (base64, quoted-printable) are handled at the MIME
 * assembly layer.
 */

/**
 * Canonicalize text content for PGP/MIME signing.
 *
 * Rules (MESSAGE.md § Cryptographic Signing):
 *   1. Remove trailing whitespace from each line (spaces and tabs before CRLF)
 *   2. Normalize all line ending variants (CRLF, LF, CR) to CRLF
 *   3. Verify 7-bit cleanliness — throw if any byte is >= 0x80
 *
 * The output is a Uint8Array of ASCII bytes suitable for hashing.
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
 * Canonicalize already-encoded bytes (e.g. base64 or quoted-printable MIME
 * body text). Only CRLF normalization and trailing whitespace stripping are
 * applied; no 7-bit check is done because transfer-encoded content is already
 * constrained to printable ASCII by the encoding.
 */
export function canonicalizeBytes(content: Uint8Array): Uint8Array {
  const text = new TextDecoder("utf-8").decode(content);
  const lines = text.split(/\r\n|\r|\n/);
  const stripped = lines.map((line) => line.replace(/[ \t]+$/, ""));
  const canonical = stripped.join("\r\n");
  return new TextEncoder().encode(canonical);
}

// Hex codec for byte strings.
//
// Used across the codebase for Ed25519 key serialization, challenge
// nonces, and signatures on the wire. Centralizing here keeps the
// encoding stable and the error wording consistent.

export function hexEncode(bytes: Uint8Array): string {
  return Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

export function hexDecode(hex: string): Uint8Array {
  if (hex.length % 2 !== 0) {
    throw new Error(`hexDecode: odd-length input (${hex.length} chars)`);
  }
  if (!/^[0-9a-fA-F]*$/.test(hex)) {
    throw new Error("hexDecode: input contains non-hex characters");
  }
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(hex.substring(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}

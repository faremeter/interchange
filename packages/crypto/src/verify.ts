import { verifyEd25519 } from "./keys";
import {
  armorDecode,
  parseNewFormatPacket,
  parseSignaturePacketBody,
  buildSignatureHashInput,
  extractCreationTime,
} from "./pgp";

/**
 * Verify a PGP/MIME detached signature against content and a public key.
 *
 * Returns true if the signature is valid; throws for malformed input.
 *
 * Verification process (MESSAGE.md §Verification Process):
 *   1. Decode ASCII armor to get the raw packet bytes
 *   2. Parse the OpenPGP v4 signature packet
 *   3. Extract the creation time from hashed subpackets
 *   4. Reconstruct the hash input (content || sig_header || trailer) using
 *      the same creation time that was used during signing
 *   5. Verify the Ed25519 signature against the hash input and public key
 */
export async function verifyDetachedSignature(
  content: Uint8Array,
  signatureBytes: Uint8Array,
  publicKeyBytes: Uint8Array,
): Promise<boolean> {
  const armoredText = new TextDecoder().decode(signatureBytes);
  const packetData = armorDecode(armoredText);

  const { tag, body } = parseNewFormatPacket(packetData, 0);
  // Tag 2 = signature packet (RFC 4880 §4.3)
  if (tag !== 2) {
    throw new Error(`Expected signature packet (tag 2), got tag ${tag}`);
  }

  const parsed = parseSignaturePacketBody(body);

  const creationTime = extractCreationTime(parsed.hashedSubpackets);
  if (creationTime === undefined) {
    throw new Error(
      "Signature packet missing mandatory creation time subpacket",
    );
  }

  // Reconstruct the hash input using the original creation time.
  const { hashInput } = buildSignatureHashInput(content, creationTime);

  // Reassemble the raw 64-byte Ed25519 signature from r and s MPIs.
  // Both are native little-endian octet strings; pad to 32 bytes each.
  const rawSig = new Uint8Array(64);
  const rPadded = padToLength(parsed.r, 32);
  const sPadded = padToLength(parsed.s, 32);
  rawSig.set(rPadded, 0);
  rawSig.set(sPadded, 32);

  return verifyEd25519(hashInput, rawSig, publicKeyBytes);
}

/**
 * Pad a byte array to exactly `targetLen` bytes by appending zero bytes
 * at the end (little-endian padding = appending zeros to the high end).
 */
function padToLength(bytes: Uint8Array, targetLen: number): Uint8Array {
  if (bytes.length === targetLen) {
    return bytes;
  }
  if (bytes.length > targetLen) {
    throw new Error(
      `EdDSA component is ${bytes.length} bytes, expected at most ${targetLen}`,
    );
  }
  const padded = new Uint8Array(targetLen);
  padded.set(bytes, 0);
  return padded;
}

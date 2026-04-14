import { sign as nodeSign, createHash } from "node:crypto";
import { importPrivateKeyBytes } from "./keys";
import {
  buildSignatureHashInput,
  buildSignaturePacket,
  armorEncode,
} from "./pgp";

/**
 * Produce a PGP/MIME detached signature for the given content bytes.
 *
 * The content should already be canonicalized (CRLF line endings, trailing
 * whitespace stripped). The signature is returned as ASCII-armored text
 * suitable for use as the `application/pgp-signature` MIME part.
 *
 * Signing process (MESSAGE.md §Cryptographic Signing):
 *   1. Build the OpenPGP v4 signature header (with creation time subpacket)
 *   2. Concatenate content || sig_header || trailer to form the hash input
 *   3. Sign the hash input with Ed25519 — Node's crypto.sign(null, data, key)
 *      invokes Ed25519 which internally hashes data with SHA-512. The OpenPGP
 *      hash algorithm field (10 = SHA-512) documents this external hash step.
 *   4. Assemble the packet with MPI-encoded r and s components
 *   5. Wrap in ASCII armor
 *
 * The "left 16 bits of signed hash" field in the packet is extracted from
 * a separate SHA-512 computation over the hash input (RFC 4880 §5.2.3).
 */
export async function createDetachedSignature(
  content: Uint8Array,
  privateKeyBytes: Uint8Array,
): Promise<Uint8Array> {
  const creationTime = Math.floor(Date.now() / 1000);
  const { hashInput, header } = buildSignatureHashInput(content, creationTime);

  // Sign the full hash input with Ed25519. Node handles SHA-512 internally.
  const privateKey = importPrivateKeyBytes(privateKeyBytes);
  const rawSig = nodeSign(null, hashInput, privateKey);

  // Compute SHA-512 separately to extract the left-16-bits field.
  const digest = createHash("sha512").update(hashInput).digest();
  const leftHash = new Uint8Array(digest.buffer, digest.byteOffset, 2);

  const packet = buildSignaturePacket(header, rawSig, leftHash);
  const armored = armorEncode(packet);
  return new TextEncoder().encode(armored);
}

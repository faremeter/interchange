import { asArrayBuffer, signEd25519 } from "./keys";
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
 *   3. Sign the hash input with Ed25519 — Web Crypto's subtle.sign invokes
 *      Ed25519, which internally hashes data with SHA-512. The OpenPGP hash
 *      algorithm field (10 = SHA-512) documents this external hash step.
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

  // Sign the full hash input with Ed25519, which hashes with SHA-512
  // internally. The OpenPGP hash-algorithm field (10 = SHA-512)
  // documents that external hash step.
  const rawSig = await signEd25519(privateKeyBytes, hashInput);

  // Compute SHA-512 separately to extract the left-16-bits field.
  const digest = new Uint8Array(
    await crypto.subtle.digest("SHA-512", asArrayBuffer(hashInput)),
  );
  const leftHash = digest.subarray(0, 2);

  const packet = buildSignaturePacket(header, rawSig, leftHash);
  const armored = armorEncode(packet);
  return new TextEncoder().encode(armored);
}

/* eslint-disable @typescript-eslint/no-non-null-assertion -- binary MPI encoding with bounds checks */
/**
 * PGP/MIME signing via CryptoProvider.
 *
 * createDetachedSignature in @interchange/crypto-node takes raw private key
 * bytes, but callers that only hold a CryptoProvider (which does not expose
 * private key bytes) need this alternative. This module reimplements the PGP
 * packet construction using CryptoProvider.sign() on the correct hash input.
 *
 * CryptoProvider.sign(data) calls Node's crypto.sign(null, data, privateKey),
 * which invokes Ed25519 and internally hashes the data with SHA-512. This is
 * the same primitive used by createDetachedSignature. The only difference is
 * that we feed the PGP hash input (content + sig_header + trailer) to sign()
 * rather than the raw content bytes.
 *
 * This is consistent with how verifyDetachedSignature works: it reconstructs
 * the hash input from the content + creation time and calls nodeVerify on it.
 * As long as the signing and verification use identical hash inputs the
 * signatures round-trip correctly.
 */

import { createHash } from "node:crypto";
import { armorEncode } from "@interchange/crypto-node";
import type { CryptoProvider } from "@interchange/types/runtime";

// OpenPGP constants (verified against pgp.ts in crypto-node).
const PK_ALGO_EDDSA = 22;
const HASH_ALGO_SHA512 = 10;
const SIG_TYPE_BINARY = 0x00;
const SIG_VERSION_4 = 4;
const SUBPKT_CREATION_TIME = 2;

function buildHashedSubpackets(creationTimeUnix: number): Uint8Array {
  const subpkt = new Uint8Array(6);
  subpkt[0] = 5;
  subpkt[1] = SUBPKT_CREATION_TIME;
  subpkt[2] = (creationTimeUnix >>> 24) & 0xff;
  subpkt[3] = (creationTimeUnix >>> 16) & 0xff;
  subpkt[4] = (creationTimeUnix >>> 8) & 0xff;
  subpkt[5] = creationTimeUnix & 0xff;
  return subpkt;
}

function buildSigHashedHeader(hashedSubpackets: Uint8Array): {
  header: Uint8Array;
  trailer: Uint8Array;
} {
  const headerLen = 4 + 2 + hashedSubpackets.length;
  const header = new Uint8Array(headerLen);
  header[0] = SIG_VERSION_4;
  header[1] = SIG_TYPE_BINARY;
  header[2] = PK_ALGO_EDDSA;
  header[3] = HASH_ALGO_SHA512;
  header[4] = (hashedSubpackets.length >>> 8) & 0xff;
  header[5] = hashedSubpackets.length & 0xff;
  header.set(hashedSubpackets, 6);

  const trailer = new Uint8Array(6);
  trailer[0] = 0x04;
  trailer[1] = 0xff;
  trailer[2] = (headerLen >>> 24) & 0xff;
  trailer[3] = (headerLen >>> 16) & 0xff;
  trailer[4] = (headerLen >>> 8) & 0xff;
  trailer[5] = headerLen & 0xff;

  return { header, trailer };
}

function encodeMPI(nativeLE: Uint8Array): Uint8Array {
  let sigLen = nativeLE.length;
  while (sigLen > 0 && nativeLE[sigLen - 1] === 0) {
    sigLen--;
  }
  if (sigLen === 0) {
    return new Uint8Array(2);
  }
  const msByte = nativeLE[sigLen - 1]!;
  let msBits = 0;
  let tmp = msByte;
  while (tmp > 0) {
    msBits++;
    tmp >>= 1;
  }
  const bitCount = (sigLen - 1) * 8 + msBits;
  const result = new Uint8Array(2 + sigLen);
  result[0] = (bitCount >> 8) & 0xff;
  result[1] = bitCount & 0xff;
  result.set(nativeLE.subarray(0, sigLen), 2);
  return result;
}

function encodeNewFormatPacket(tag: number, body: Uint8Array): Uint8Array {
  const tagByte = 0xc0 | tag;
  let lengthBytes: Uint8Array;

  if (body.length <= 191) {
    lengthBytes = new Uint8Array([body.length]);
  } else if (body.length <= 8383) {
    const adjusted = body.length - 192;
    const b0 = ((adjusted >> 8) & 0xff) + 192;
    const b1 = adjusted & 0xff;
    lengthBytes = new Uint8Array([b0, b1]);
  } else {
    lengthBytes = new Uint8Array(5);
    lengthBytes[0] = 0xff;
    lengthBytes[1] = (body.length >>> 24) & 0xff;
    lengthBytes[2] = (body.length >>> 16) & 0xff;
    lengthBytes[3] = (body.length >>> 8) & 0xff;
    lengthBytes[4] = body.length & 0xff;
  }

  const packet = new Uint8Array(1 + lengthBytes.length + body.length);
  packet[0] = tagByte;
  packet.set(lengthBytes, 1);
  packet.set(body, 1 + lengthBytes.length);
  return packet;
}

/**
 * Produce a PGP/MIME detached signature using a CryptoProvider.
 *
 * Mirrors createDetachedSignature from crypto-node but accepts a
 * CryptoProvider instead of raw private key bytes.
 */
export async function createDetachedSignatureFromProvider(
  content: Uint8Array,
  provider: CryptoProvider,
): Promise<Uint8Array> {
  const creationTime = Math.floor(Date.now() / 1000);
  const hashedSubpackets = buildHashedSubpackets(creationTime);
  const { header, trailer } = buildSigHashedHeader(hashedSubpackets);

  // Build the hash input: content || sig_header || trailer
  const total = content.length + header.length + trailer.length;
  const hashInput = new Uint8Array(total);
  let offset = 0;
  hashInput.set(content, offset);
  offset += content.length;
  hashInput.set(header, offset);
  offset += header.length;
  hashInput.set(trailer, offset);

  // Sign the hash input with the provider's private key (Ed25519, SHA-512 internal).
  const rawSig = await provider.sign(hashInput);

  if (rawSig.length !== 64) {
    throw new Error(
      `CryptoProvider.sign returned ${rawSig.length} bytes; expected 64 (Ed25519 signature)`,
    );
  }

  // Compute SHA-512 over the hash input to extract the left-16-bits field.
  const digest = createHash("sha512").update(hashInput).digest();
  const leftHash = new Uint8Array(digest.buffer, digest.byteOffset, 2);

  // Build the signature packet body.
  const r = rawSig.slice(0, 32);
  const s = rawSig.slice(32, 64);
  const rMPI = encodeMPI(r);
  const sMPI = encodeMPI(s);

  const bodyLen = header.length + 2 + 2 + rMPI.length + sMPI.length;
  const body = new Uint8Array(bodyLen);
  let pos = 0;
  body.set(header, pos);
  pos += header.length;
  body[pos++] = 0;
  body[pos++] = 0;
  body[pos++] = leftHash[0] ?? 0;
  body[pos++] = leftHash[1] ?? 0;
  body.set(rMPI, pos);
  pos += rMPI.length;
  body.set(sMPI, pos);

  const packet = encodeNewFormatPacket(0x02, body);
  const armored = armorEncode(packet);
  return new TextEncoder().encode(armored);
}

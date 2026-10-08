/* eslint-disable @typescript-eslint/no-non-null-assertion -- Binary packet
 * parsing uses non-null assertions after explicit bounds checks. Every `!`
 * below is preceded by a length guard that proves the index is in range.
 * Rewriting these as conditionals would obscure the packet format. */

/**
 * OpenPGP packet encoding and ASCII armor for the RFC 4880 subset required
 * for EdDSA detached signatures over Ed25519 keys.
 *
 * Wire constants (RFC refs sit beside the numbered constants below):
 * algorithm 22 EdDSA, hash 10 SHA-512, signature type 0x00, version 4,
 * creation-time subpacket type 2. EdDSA r/s MPIs are native little-endian
 * octet strings with leading zero octets stripped (see `encodeMPI`), and
 * detached sigs carry no unhashed subpackets (see `buildSignaturePacket`).
 */

import { base64Encode, base64Decode } from "@intx/types";

import { asArrayBuffer } from "./keys";

// RFC 4880 §9.1: public-key algorithm IDs
const PK_ALGO_EDDSA = 22;

// RFC 4880 §9.4: hash algorithm IDs
const HASH_ALGO_SHA512 = 10;

// RFC 4880 §5.2.1: signature type
const SIG_TYPE_BINARY = 0x00;

// RFC 4880 §5.2.3: signature version
const SIG_VERSION_4 = 4;

// RFC 4880 §5.2.3.4: subpacket type IDs
const SUBPKT_CREATION_TIME = 2;

/**
 * Encode a value as an OpenPGP MPI (RFC 4880 §3.2): a 2-octet big-endian
 * bit count followed by the value's bytes with leading zero octets stripped.
 *
 * The EdDSA r/s input is a native little-endian octet string (not reversed
 * here); its bytes are kept as-is and the bit count is taken from the most
 * significant byte, the last byte in little-endian order.
 */
export function encodeMPI(nativeLE: Uint8Array): Uint8Array {
  // Trailing zero bytes in little-endian order are leading zeros numerically.
  let sigLen = nativeLE.length;
  while (sigLen > 0 && nativeLE[sigLen - 1] === 0) {
    sigLen--;
  }
  if (sigLen === 0) {
    // MPI encoding of zero: 2 zero bytes
    return new Uint8Array(2);
  }
  // The most significant byte in numeric terms is nativeLE[sigLen - 1].
  const msByte = nativeLE[sigLen - 1]!;
  // Count bits in the most significant byte.
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
  // Copy the bytes as-is (native little-endian for EdDSA r/s).
  result.set(nativeLE.subarray(0, sigLen), 2);
  return result;
}

/**
 * Decode an OpenPGP MPI from a buffer at a given offset.
 *
 * Returns the native bytes (little-endian for EdDSA) and the number of bytes
 * consumed from the buffer.
 */
export function decodeMPI(
  buf: Uint8Array,
  offset: number,
): { bytes: Uint8Array; consumed: number } {
  if (offset + 2 > buf.length) {
    throw new Error("MPI truncated: insufficient bytes for bit-count header");
  }
  const highByte = buf[offset];
  const lowByte = buf[offset + 1];
  if (highByte === undefined || lowByte === undefined) {
    throw new Error("MPI truncated: undefined bytes in header");
  }
  const bitCount = (highByte << 8) | lowByte;
  const byteCount = Math.ceil(bitCount / 8);
  if (offset + 2 + byteCount > buf.length) {
    throw new Error(
      `MPI truncated: need ${byteCount} bytes, only ${buf.length - offset - 2} available`,
    );
  }
  const bytes = buf.slice(offset + 2, offset + 2 + byteCount);
  return { bytes, consumed: 2 + byteCount };
}

/**
 * Build the hashed subpacket area for a v4 signature.
 *
 * The only mandatory hashed subpacket is the signature creation time
 * (RFC 4880 §5.2.3.4). We include it as a 4-byte Unix timestamp.
 */
function buildHashedSubpackets(creationTimeUnix: number): Uint8Array {
  // Subpacket: 1-byte length (5) + type 2 + 4-byte Unix timestamp.
  const subpkt = new Uint8Array(6);
  subpkt[0] = 5; // length of body
  subpkt[1] = SUBPKT_CREATION_TIME;
  subpkt[2] = (creationTimeUnix >>> 24) & 0xff;
  subpkt[3] = (creationTimeUnix >>> 16) & 0xff;
  subpkt[4] = (creationTimeUnix >>> 8) & 0xff;
  subpkt[5] = creationTimeUnix & 0xff;
  return subpkt;
}

/**
 * Build the v4 signature header (version through hashed subpackets) and the
 * 6-byte trailer RFC 4880 §5.2.4 appends when hashing.
 */
function buildSignatureHashedHeader(hashedSubpackets: Uint8Array): {
  header: Uint8Array;
  trailer: Uint8Array;
} {
  // v4 sig header: version, sigtype, pk_algo, hash_algo, hashed_subpkt_len (2), hashed_subpkts
  const headerLen = 4 + 2 + hashedSubpackets.length;
  const header = new Uint8Array(headerLen);
  header[0] = SIG_VERSION_4;
  header[1] = SIG_TYPE_BINARY;
  header[2] = PK_ALGO_EDDSA;
  header[3] = HASH_ALGO_SHA512;
  header[4] = (hashedSubpackets.length >>> 8) & 0xff;
  header[5] = hashedSubpackets.length & 0xff;
  header.set(hashedSubpackets, 6);

  // RFC 4880 §5.2.4: trailer for v4 signatures
  // 0x04 0xff <4-byte big-endian length of the hashed data>
  const trailer = new Uint8Array(6);
  trailer[0] = 0x04;
  trailer[1] = 0xff;
  trailer[2] = (headerLen >>> 24) & 0xff;
  trailer[3] = (headerLen >>> 16) & 0xff;
  trailer[4] = (headerLen >>> 8) & 0xff;
  trailer[5] = headerLen & 0xff;

  return { header, trailer };
}

/**
 * The data to hash for a v4 detached signature (RFC 4880 §5.2.4):
 * content || sig_header || trailer.
 */
export function buildSignatureHashInput(
  content: Uint8Array,
  creationTimeUnix: number,
): { hashInput: Uint8Array; header: Uint8Array } {
  const hashedSubpackets = buildHashedSubpackets(creationTimeUnix);
  const { header, trailer } = buildSignatureHashedHeader(hashedSubpackets);

  const total = content.length + header.length + trailer.length;
  const hashInput = new Uint8Array(total);
  let offset = 0;
  hashInput.set(content, offset);
  offset += content.length;
  hashInput.set(header, offset);
  offset += header.length;
  hashInput.set(trailer, offset);

  return { hashInput, header };
}

/**
 * Assemble a complete OpenPGP v4 signature packet: split the 64-byte
 * Ed25519 signature into 32-byte R and S components (native little-endian
 * per RFC 8032) and encode them as MPIs. Detached sigs carry no unhashed
 * subpackets — there is no key fingerprint or key ID to embed.
 */
export function buildSignaturePacket(
  sigHeader: Uint8Array,
  rawSig: Uint8Array,
  firstTwoHashBytes: Uint8Array,
): Uint8Array {
  if (rawSig.length !== 64) {
    throw new Error(`Ed25519 signature must be 64 bytes, got ${rawSig.length}`);
  }

  // R = first 32 bytes, S = last 32 (both native little-endian per RFC 8032).
  const r = rawSig.slice(0, 32);
  const s = rawSig.slice(32, 64);
  const rMPI = encodeMPI(r);
  const sMPI = encodeMPI(s);

  // Unhashed subpackets are empty: a 2-byte length field of 0x0000.
  const unhashedSubpacketsLen = 0;

  // Packet body per RFC 4880 §5.2.3:
  //   sig_header (hashed part) || unhashed_subpkts_len (2) || left_2_hash_bytes (2) || MPIs
  const bodyLen =
    sigHeader.length +
    2 +
    unhashedSubpacketsLen +
    2 +
    rMPI.length +
    sMPI.length;
  const body = new Uint8Array(bodyLen);
  let pos = 0;
  body.set(sigHeader, pos);
  pos += sigHeader.length;
  // Unhashed subpackets length: 0
  body[pos++] = 0;
  body[pos++] = 0;
  // Left 2 bytes of the hash
  body[pos++] = firstTwoHashBytes[0] ?? 0;
  body[pos++] = firstTwoHashBytes[1] ?? 0;
  body.set(rMPI, pos);
  pos += rMPI.length;
  body.set(sMPI, pos);

  // Wrap in a new-format packet header (RFC 4880 §4.2); signature tag = 2.
  return encodeNewFormatPacket(0x02, body);
}

/**
 * Encode a packet body as a new-format OpenPGP packet (RFC 4880 §4.2.2).
 *
 * Uses the "old length" (non-partial) encoding: 1-byte body length for
 * bodies ≤ 191 bytes, 2-byte encoding for bodies ≤ 8383 bytes, and 5-byte
 * encoding otherwise.
 */
function encodeNewFormatPacket(tag: number, body: Uint8Array): Uint8Array {
  const tagByte = 0xc0 | tag;
  let lengthBytes: Uint8Array;

  if (body.length <= 191) {
    lengthBytes = new Uint8Array([body.length]);
  } else if (body.length <= 8383) {
    // Two-octet length: ((b0 - 192) << 8) + b1 + 192
    const adjusted = body.length - 192;
    const b0 = ((adjusted >> 8) & 0xff) + 192;
    const b1 = adjusted & 0xff;
    lengthBytes = new Uint8Array([b0, b1]);
  } else {
    // Five-octet length: 0xff followed by 4-byte big-endian length
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
 * Sign a hash input with Ed25519, returning the raw 64-byte signature
 * (r || s, native little-endian per RFC 8032). The seam that lets callers
 * holding only a signing capability use this module's packet assembly.
 */
export type Ed25519Signer = (input: Uint8Array) => Promise<Uint8Array>;

/**
 * Build an ASCII-armored v4 detached signature, delegating the raw Ed25519
 * signing step to `sign`. The signer receives the OpenPGP hash input
 * (content || sig_header || trailer, RFC 4880 §5.2.4) and returns the raw
 * 64-byte signature over it; Web Crypto hashes that input with SHA-512
 * internally, which the hash-algorithm field (10) records. The left-2 hash
 * bytes come from a separate SHA-512 digest of the same input (RFC 4880
 * §5.2.3).
 */
export async function createDetachedSignatureWithSigner(
  content: Uint8Array,
  sign: Ed25519Signer,
): Promise<Uint8Array> {
  const creationTime = Math.floor(Date.now() / 1000);
  const { hashInput, header } = buildSignatureHashInput(content, creationTime);

  const rawSig = await sign(hashInput);

  const digest = new Uint8Array(
    await crypto.subtle.digest("SHA-512", asArrayBuffer(hashInput)),
  );
  const leftHash = digest.subarray(0, 2);

  const packet = buildSignaturePacket(header, rawSig, leftHash);
  return new TextEncoder().encode(armorEncode(packet));
}

/**
 * Parse a new-format OpenPGP packet.
 *
 * Returns the packet tag, body bytes, and total bytes consumed.
 */
export function parseNewFormatPacket(
  buf: Uint8Array,
  offset: number,
): {
  tag: number;
  body: Uint8Array;
  consumed: number;
} {
  if (offset >= buf.length) {
    throw new Error("Packet truncated: no bytes available");
  }
  const tagByte = buf[offset]!;
  if ((tagByte & 0x80) === 0) {
    throw new Error(`Invalid packet tag byte: 0x${tagByte.toString(16)}`);
  }

  let tag: number;
  let headerLen: number;
  let bodyLen: number;

  if ((tagByte & 0x40) !== 0) {
    // New format
    tag = tagByte & 0x3f;
    const first = buf[offset + 1];
    if (first === undefined) {
      throw new Error("Packet truncated: no length byte");
    }
    if (first <= 191) {
      bodyLen = first;
      headerLen = 2;
    } else if (first <= 223) {
      const second = buf[offset + 2];
      if (second === undefined) {
        throw new Error("Packet truncated: second length byte missing");
      }
      bodyLen = ((first - 192) << 8) + second + 192;
      headerLen = 3;
    } else if (first === 0xff) {
      if (offset + 6 > buf.length) {
        throw new Error("Packet truncated: 5-byte length encoding incomplete");
      }
      bodyLen =
        ((buf[offset + 2]! << 24) |
          (buf[offset + 3]! << 16) |
          (buf[offset + 4]! << 8) |
          buf[offset + 5]!) >>>
        0;
      headerLen = 6;
    } else {
      throw new Error(
        `Partial body lengths not supported in signature packets (first byte: 0x${first.toString(16)})`,
      );
    }
  } else {
    // Old format
    tag = (tagByte & 0x3c) >> 2;
    const lengthType = tagByte & 0x03;
    if (lengthType === 0) {
      const len = buf[offset + 1];
      if (len === undefined) throw new Error("Old-format packet truncated");
      bodyLen = len;
      headerLen = 2;
    } else if (lengthType === 1) {
      if (offset + 3 > buf.length)
        throw new Error("Old-format packet truncated");
      bodyLen = ((buf[offset + 1]! << 8) | buf[offset + 2]!) >>> 0;
      headerLen = 3;
    } else if (lengthType === 2) {
      if (offset + 5 > buf.length)
        throw new Error("Old-format packet truncated");
      bodyLen =
        ((buf[offset + 1]! << 24) |
          (buf[offset + 2]! << 16) |
          (buf[offset + 3]! << 8) |
          buf[offset + 4]!) >>>
        0;
      headerLen = 5;
    } else {
      throw new Error("Indeterminate-length old-format packets not supported");
    }
  }

  const end = offset + headerLen + bodyLen;
  if (end > buf.length) {
    throw new Error(
      `Packet body truncated: need ${bodyLen} bytes, only ${buf.length - offset - headerLen} available`,
    );
  }
  const body = buf.slice(offset + headerLen, end);
  return { tag, body, consumed: headerLen + bodyLen };
}

/**
 * Parse a v4 signature packet body.
 *
 * Returns the two MPI components (r and s as native little-endian bytes)
 * and the first two bytes of the hash.
 */
export function parseSignaturePacketBody(body: Uint8Array): {
  version: number;
  sigType: number;
  pkAlgo: number;
  hashAlgo: number;
  hashedSubpackets: Uint8Array;
  unhashedSubpackets: Uint8Array;
  leftHash: Uint8Array;
  r: Uint8Array;
  s: Uint8Array;
} {
  if (body.length < 6) {
    throw new Error("Signature packet body too short");
  }
  const version = body[0]!;
  if (version !== 4) {
    throw new Error(`Unsupported signature version: ${version} (expected 4)`);
  }
  const sigType = body[1]!;
  const pkAlgo = body[2]!;
  const hashAlgo = body[3]!;

  if (pkAlgo !== PK_ALGO_EDDSA) {
    throw new Error(
      `Unsupported public-key algorithm: ${pkAlgo} (expected ${PK_ALGO_EDDSA} for EdDSA)`,
    );
  }
  if (hashAlgo !== HASH_ALGO_SHA512) {
    throw new Error(
      `Unsupported hash algorithm: ${hashAlgo} (expected ${HASH_ALGO_SHA512} for SHA-512)`,
    );
  }

  let pos = 4;
  const hashedLen = ((body[pos]! << 8) | body[pos + 1]!) >>> 0;
  pos += 2;
  if (pos + hashedLen > body.length) {
    throw new Error("Signature packet: hashed subpackets truncated");
  }
  const hashedSubpackets = body.slice(pos, pos + hashedLen);
  pos += hashedLen;

  if (pos + 2 > body.length) {
    throw new Error("Signature packet: unhashed subpackets length missing");
  }
  const unhashedLen = ((body[pos]! << 8) | body[pos + 1]!) >>> 0;
  pos += 2;
  if (pos + unhashedLen > body.length) {
    throw new Error("Signature packet: unhashed subpackets truncated");
  }
  const unhashedSubpackets = body.slice(pos, pos + unhashedLen);
  pos += unhashedLen;

  if (pos + 2 > body.length) {
    throw new Error("Signature packet: left-hash bytes missing");
  }
  const leftHash = body.slice(pos, pos + 2);
  pos += 2;

  const rResult = decodeMPI(body, pos);
  pos += rResult.consumed;
  const sResult = decodeMPI(body, pos);

  return {
    version,
    sigType,
    pkAlgo,
    hashAlgo,
    hashedSubpackets,
    unhashedSubpackets,
    leftHash,
    r: rResult.bytes,
    s: sResult.bytes,
  };
}

/**
 * Extract the creation time (Unix seconds) from hashed subpackets.
 *
 * Returns undefined if no creation time subpacket is found.
 */
export function extractCreationTime(
  hashedSubpackets: Uint8Array,
): number | undefined {
  let pos = 0;
  while (pos < hashedSubpackets.length) {
    const pktLen = hashedSubpackets[pos];
    if (pktLen === undefined) break;
    pos++;
    if (pos + pktLen > hashedSubpackets.length) break;
    const type = hashedSubpackets[pos];
    if (type === SUBPKT_CREATION_TIME && pktLen >= 5) {
      const t =
        ((hashedSubpackets[pos + 1]! << 24) |
          (hashedSubpackets[pos + 2]! << 16) |
          (hashedSubpackets[pos + 3]! << 8) |
          hashedSubpackets[pos + 4]!) >>>
        0;
      return t;
    }
    pos += pktLen;
  }
  return undefined;
}

// ---------------------------------------------------------------------------
// ASCII Armor (RFC 4880 §6)
// ---------------------------------------------------------------------------

const ARMOR_HEADER = "-----BEGIN PGP SIGNATURE-----";
const ARMOR_FOOTER = "-----END PGP SIGNATURE-----";

/**
 * Encode binary packet data as ASCII armor (RFC 4880 §6): header line,
 * blank line, base64 on 76-char lines, CRC24 line, footer. CRC24 is
 * included for compatibility with other OpenPGP implementations.
 */
export function armorEncode(data: Uint8Array): string {
  const b64 = base64Encode(data);
  const lines: string[] = [];
  for (let i = 0; i < b64.length; i += 76) {
    lines.push(b64.slice(i, i + 76));
  }
  const crc = crc24(data);
  const crcB64 = base64Encode(
    new Uint8Array([(crc >> 16) & 0xff, (crc >> 8) & 0xff, crc & 0xff]),
  );
  return [ARMOR_HEADER, "", ...lines, `=${crcB64}`, ARMOR_FOOTER].join("\n");
}

/**
 * Decode ASCII-armored OpenPGP data.
 *
 * Strips the header/footer lines, decodes base64, and verifies the CRC24
 * checksum if present.
 */
export function armorDecode(armor: string): Uint8Array {
  const lines = armor.replace(/\r\n/g, "\n").split("\n");

  const headerIdx = lines.findIndex((l) => l === ARMOR_HEADER);
  if (headerIdx === -1) {
    throw new Error("ASCII armor: missing BEGIN PGP SIGNATURE header");
  }
  const footerIdx = lines.findIndex((l) => l === ARMOR_FOOTER);
  if (footerIdx === -1) {
    throw new Error("ASCII armor: missing END PGP SIGNATURE footer");
  }
  if (footerIdx <= headerIdx) {
    throw new Error("ASCII armor: footer appears before header");
  }

  // Skip blank lines and header key-value pairs after the BEGIN line.
  let bodyStart = headerIdx + 1;
  while (bodyStart < footerIdx && lines[bodyStart]?.trim() === "") {
    bodyStart++;
  }
  // Skip any header key: value lines (contain a colon before blank line).
  while (bodyStart < footerIdx && lines[bodyStart]?.includes(": ")) {
    bodyStart++;
  }
  // Skip the blank line separating headers from body.
  if (bodyStart < footerIdx && lines[bodyStart]?.trim() === "") {
    bodyStart++;
  }

  const bodyLines: string[] = [];
  let crcLine: string | undefined;
  for (let i = bodyStart; i < footerIdx; i++) {
    const line = lines[i]!;
    if (line.startsWith("=")) {
      crcLine = line.slice(1);
    } else {
      bodyLines.push(line);
    }
  }

  const data = base64Decode(bodyLines.join(""));

  if (crcLine !== undefined) {
    const expectedCRCBytes = base64Decode(crcLine);
    if (expectedCRCBytes.length === 3) {
      const expected =
        ((expectedCRCBytes[0]! << 16) |
          (expectedCRCBytes[1]! << 8) |
          expectedCRCBytes[2]!) >>>
        0;
      const actual = crc24(data);
      if (actual !== expected) {
        throw new Error(
          `ASCII armor CRC24 mismatch: expected 0x${expected.toString(16)}, got 0x${actual.toString(16)}`,
        );
      }
    }
  }

  return data;
}

/**
 * CRC-24 checksum per RFC 4880 §6.1 (polynomial 0x1864CFB, init 0xB704CE).
 */
function crc24(data: Uint8Array): number {
  const CRC24_INIT = 0xb704ce;
  const CRC24_POLY = 0x1864cfb;
  let crc = CRC24_INIT;
  for (const byte of data) {
    crc ^= byte << 16;
    for (let i = 0; i < 8; i++) {
      crc <<= 1;
      if (crc & 0x1000000) {
        crc ^= CRC24_POLY;
      }
    }
  }
  return crc & 0xffffff;
}

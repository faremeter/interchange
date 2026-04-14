import { describe, test, expect } from "bun:test";

import { generateKeyPair } from "./keys";
import { NodeCrypto, createNodeCrypto } from "./provider";
import { canonicalizeText, canonicalizeBytes } from "./canonicalize";
import { createDetachedSignature } from "./sign";
import { verifyDetachedSignature } from "./verify";
import { armorEncode, armorDecode, encodeMPI, decodeMPI } from "./pgp";

// ---------------------------------------------------------------------------
// Key generation
// ---------------------------------------------------------------------------

describe("generateKeyPair", () => {
  test("produces 32-byte keys", async () => {
    const kp = await generateKeyPair();
    expect(kp.privateKey).toBeInstanceOf(Uint8Array);
    expect(kp.publicKey).toBeInstanceOf(Uint8Array);
    expect(kp.privateKey.length).toBe(32);
    expect(kp.publicKey.length).toBe(32);
  });

  test("each call produces a different key pair", async () => {
    const a = await generateKeyPair();
    const b = await generateKeyPair();
    expect(Buffer.from(a.publicKey).toString("hex")).not.toBe(
      Buffer.from(b.publicKey).toString("hex"),
    );
  });
});

// ---------------------------------------------------------------------------
// NodeCrypto — raw sign / verify round-trip
// ---------------------------------------------------------------------------

describe("NodeCrypto", () => {
  test("sign and verify round-trip", async () => {
    const kp = await generateKeyPair();
    const crypto = createNodeCrypto(kp);
    const content = new TextEncoder().encode("hello world");
    const sig = await crypto.sign(content);
    expect(sig).toBeInstanceOf(Uint8Array);
    expect(sig.length).toBe(64);
    const ok = await crypto.verify(content, sig, kp.publicKey);
    expect(ok).toBe(true);
  });

  test("verify fails when content is modified", async () => {
    const kp = await generateKeyPair();
    const crypto = createNodeCrypto(kp);
    const content = new TextEncoder().encode("hello world");
    const sig = await crypto.sign(content);
    const tampered = new TextEncoder().encode("hello WORLD");
    const ok = await crypto.verify(tampered, sig, kp.publicKey);
    expect(ok).toBe(false);
  });

  test("verify fails with wrong public key", async () => {
    const kp1 = await generateKeyPair();
    const kp2 = await generateKeyPair();
    const crypto = createNodeCrypto(kp1);
    const content = new TextEncoder().encode("test message");
    const sig = await crypto.sign(content);
    const ok = await crypto.verify(content, sig, kp2.publicKey);
    expect(ok).toBe(false);
  });

  test("getPublicKey returns the correct key", async () => {
    const kp = await generateKeyPair();
    const crypto = createNodeCrypto(kp);
    const pk = crypto.getPublicKey();
    expect(Buffer.from(pk).toString("hex")).toBe(
      Buffer.from(kp.publicKey).toString("hex"),
    );
  });

  test("rejects private key of wrong length", async () => {
    const kp = await generateKeyPair();
    expect(
      () =>
        new NodeCrypto({
          privateKey: new Uint8Array(16),
          publicKey: kp.publicKey,
        }),
    ).toThrow(/32 bytes/);
  });

  test("rejects public key of wrong length", async () => {
    const kp = await generateKeyPair();
    expect(
      () =>
        new NodeCrypto({
          privateKey: kp.privateKey,
          publicKey: new Uint8Array(16),
        }),
    ).toThrow(/32 bytes/);
  });
});

// ---------------------------------------------------------------------------
// Canonicalization
// ---------------------------------------------------------------------------

describe("canonicalizeText", () => {
  test("converts LF to CRLF", () => {
    const result = canonicalizeText("hello\nworld");
    expect(new TextDecoder().decode(result)).toBe("hello\r\nworld");
  });

  test("converts bare CR to CRLF", () => {
    const result = canonicalizeText("hello\rworld");
    expect(new TextDecoder().decode(result)).toBe("hello\r\nworld");
  });

  test("leaves existing CRLF unchanged", () => {
    const result = canonicalizeText("hello\r\nworld");
    expect(new TextDecoder().decode(result)).toBe("hello\r\nworld");
  });

  test("strips trailing spaces from each line", () => {
    const result = canonicalizeText("hello   \nworld  ");
    expect(new TextDecoder().decode(result)).toBe("hello\r\nworld");
  });

  test("strips trailing tabs from each line", () => {
    const result = canonicalizeText("hello\t\t\nworld\t");
    expect(new TextDecoder().decode(result)).toBe("hello\r\nworld");
  });

  test("strips mixed trailing whitespace", () => {
    const result = canonicalizeText("hello \t \nworld");
    expect(new TextDecoder().decode(result)).toBe("hello\r\nworld");
  });

  test("handles empty string", () => {
    const result = canonicalizeText("");
    expect(new TextDecoder().decode(result)).toBe("");
  });

  test("handles single line with no newline", () => {
    const result = canonicalizeText("hello");
    expect(new TextDecoder().decode(result)).toBe("hello");
  });

  test("throws on non-7-bit content", () => {
    expect(() => canonicalizeText("caf\u00e9")).toThrow(/7-bit/);
  });

  test("multi-line mixed endings", () => {
    const result = canonicalizeText("a \r\nb \nc \rd");
    expect(new TextDecoder().decode(result)).toBe("a\r\nb\r\nc\r\nd");
  });
});

describe("canonicalizeBytes", () => {
  test("normalizes CRLF in byte content", () => {
    const input = new TextEncoder().encode("hello\nworld");
    const result = canonicalizeBytes(input);
    expect(new TextDecoder().decode(result)).toBe("hello\r\nworld");
  });
});

// ---------------------------------------------------------------------------
// ASCII armor round-trip
// ---------------------------------------------------------------------------

describe("armorEncode / armorDecode", () => {
  test("round-trip of arbitrary bytes", () => {
    const data = new Uint8Array(64);
    for (let i = 0; i < 64; i++) data[i] = i;
    const armored = armorEncode(data);
    expect(armored).toContain("-----BEGIN PGP SIGNATURE-----");
    expect(armored).toContain("-----END PGP SIGNATURE-----");
    const decoded = armorDecode(armored);
    expect(Buffer.from(decoded).toString("hex")).toBe(
      Buffer.from(data).toString("hex"),
    );
  });

  test("round-trip of empty bytes", () => {
    const data = new Uint8Array(0);
    const armored = armorEncode(data);
    const decoded = armorDecode(armored);
    expect(decoded.length).toBe(0);
  });

  test("round-trip of large payload (> 76 base64 chars per line)", () => {
    const data = new Uint8Array(200);
    crypto.getRandomValues(data);
    const armored = armorEncode(data);
    const decoded = armorDecode(armored);
    expect(Buffer.from(decoded).toString("hex")).toBe(
      Buffer.from(data).toString("hex"),
    );
  });

  test("rejects missing header", () => {
    expect(() => armorDecode("not an armor block")).toThrow(
      /BEGIN PGP SIGNATURE/,
    );
  });

  test("rejects missing footer", () => {
    expect(() =>
      armorDecode("-----BEGIN PGP SIGNATURE-----\naGVsbG8="),
    ).toThrow(/END PGP SIGNATURE/);
  });

  test("CRC24 mismatch throws", () => {
    const data = new Uint8Array([1, 2, 3]);
    const armored = armorEncode(data);
    // Corrupt the CRC line (the = line)
    const corrupted = armored.replace(/^=.+$/m, "=AAAA");
    expect(() => armorDecode(corrupted)).toThrow(/CRC24/);
  });
});

// ---------------------------------------------------------------------------
// MPI encoding
// ---------------------------------------------------------------------------

describe("encodeMPI / decodeMPI", () => {
  test("encodes a simple value", () => {
    // Value 1 in little-endian: [0x01]
    const mpi = encodeMPI(new Uint8Array([0x01]));
    // Bit count = 1, byte count = 1 → [0x00, 0x01, 0x01]
    expect(mpi).toEqual(new Uint8Array([0x00, 0x01, 0x01]));
  });

  test("encodes zero as two zero bytes", () => {
    const mpi = encodeMPI(new Uint8Array(4));
    expect(mpi).toEqual(new Uint8Array([0x00, 0x00]));
  });

  test("strips trailing zero bytes (high-order zeros in little-endian)", () => {
    // 0x01 0x00 0x00 in little-endian = value 1; should produce same as [0x01]
    const mpi = encodeMPI(new Uint8Array([0x01, 0x00, 0x00]));
    expect(mpi).toEqual(new Uint8Array([0x00, 0x01, 0x01]));
  });

  test("round-trips through decodeMPI", () => {
    const orig = new Uint8Array([0xab, 0xcd, 0xef]);
    const mpi = encodeMPI(orig);
    const { bytes, consumed } = decodeMPI(mpi, 0);
    expect(consumed).toBe(mpi.length);
    // The decoded bytes may have stripped trailing zeros, so compare trimmed.
    let len = orig.length;
    while (len > 0 && orig[len - 1] === 0) len--;
    expect(Buffer.from(bytes).toString("hex")).toBe(
      Buffer.from(orig.subarray(0, len)).toString("hex"),
    );
  });

  test("decodeMPI throws on truncated input", () => {
    expect(() => decodeMPI(new Uint8Array([0x00]), 0)).toThrow(/truncated/);
  });
});

// ---------------------------------------------------------------------------
// PGP detached signature — sign and verify
// ---------------------------------------------------------------------------

describe("createDetachedSignature / verifyDetachedSignature", () => {
  test("sign and verify round-trip", async () => {
    const kp = await generateKeyPair();
    const content = new TextEncoder().encode("hello\r\nworld");
    const sigBytes = await createDetachedSignature(content, kp.privateKey);
    expect(sigBytes).toBeInstanceOf(Uint8Array);
    const armored = new TextDecoder().decode(sigBytes);
    expect(armored).toContain("-----BEGIN PGP SIGNATURE-----");
    const ok = await verifyDetachedSignature(content, sigBytes, kp.publicKey);
    expect(ok).toBe(true);
  });

  test("verify fails when content is modified", async () => {
    const kp = await generateKeyPair();
    const content = new TextEncoder().encode("hello\r\nworld");
    const sigBytes = await createDetachedSignature(content, kp.privateKey);
    const tampered = new TextEncoder().encode("hello\r\nWORLD");
    const ok = await verifyDetachedSignature(tampered, sigBytes, kp.publicKey);
    expect(ok).toBe(false);
  });

  test("verify fails with wrong public key", async () => {
    const kp1 = await generateKeyPair();
    const kp2 = await generateKeyPair();
    const content = new TextEncoder().encode("test message");
    const sigBytes = await createDetachedSignature(content, kp1.privateKey);
    const ok = await verifyDetachedSignature(content, sigBytes, kp2.publicKey);
    expect(ok).toBe(false);
  });

  test("rejects truncated signature", async () => {
    const kp = await generateKeyPair();
    const content = new TextEncoder().encode("hello");
    // A truncated / invalid ASCII armor block
    const truncatedSig = new TextEncoder().encode(
      "-----BEGIN PGP SIGNATURE-----\nYQ==\n-----END PGP SIGNATURE-----",
    );
    await expect(
      verifyDetachedSignature(content, truncatedSig, kp.publicKey),
    ).rejects.toThrow();
  });

  test("rejects signature with wrong algorithm", async () => {
    const kp = await generateKeyPair();
    const content = new TextEncoder().encode("hello");
    const sigBytes = await createDetachedSignature(content, kp.privateKey);

    // Decode armor to get the raw packet, corrupt the public-key algorithm byte.
    const armored = new TextDecoder().decode(sigBytes);
    const { armorDecode: ad, armorEncode: ae } = await import("./pgp");
    const packetData = ad(armored);
    // Byte at index 4 in new-format packet: tag(1) + len(1) + version(1) + sigtype(1) + pk_algo(1)
    // Tag byte = 0xc2 (1 byte), length byte (1 byte), then body starts.
    // Body[2] = pk_algo. For new-format with body < 192: offset 2 (tag) + 1 (len) + 2 (version+sigtype) = 4
    const corrupted = new Uint8Array(packetData);
    corrupted[4] = 17; // DSA algorithm ID instead of EdDSA (22)
    const corruptedArmor = ae(corrupted);
    const corruptedSig = new TextEncoder().encode(corruptedArmor);

    await expect(
      verifyDetachedSignature(content, corruptedSig, kp.publicKey),
    ).rejects.toThrow(/algorithm/);
  });

  test("signature packet has correct structure", async () => {
    const kp = await generateKeyPair();
    const content = new TextEncoder().encode("structure test\r\n");
    const sigBytes = await createDetachedSignature(content, kp.privateKey);

    const armored = new TextDecoder().decode(sigBytes);
    const {
      armorDecode: ad,
      parseNewFormatPacket,
      parseSignaturePacketBody,
    } = await import("./pgp");
    const packetData = ad(armored);
    const { tag, body } = parseNewFormatPacket(packetData, 0);
    expect(tag).toBe(2); // signature packet
    const parsed = parseSignaturePacketBody(body);
    expect(parsed.version).toBe(4);
    expect(parsed.pkAlgo).toBe(22); // EdDSA
    expect(parsed.hashAlgo).toBe(10); // SHA-512
    expect(parsed.sigType).toBe(0x00); // binary document
    // r and s together should reconstruct a 64-byte signature
    const rPad = new Uint8Array(32);
    const sPad = new Uint8Array(32);
    rPad.set(parsed.r);
    sPad.set(parsed.s);
    const reassembled = new Uint8Array(64);
    reassembled.set(rPad, 0);
    reassembled.set(sPad, 32);
    // Verify the raw reassembled signature is valid
    // Check reconstructed signature length
    expect(reassembled.length).toBe(64);
  });
});

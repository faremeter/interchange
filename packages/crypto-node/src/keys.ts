import {
  generateKeyPairSync,
  createPrivateKey,
  createPublicKey,
  type KeyObject,
} from "node:crypto";
import type { KeyPair } from "@interchange/types/runtime";

/**
 * Generate a fresh Ed25519 key pair.
 *
 * Returns raw 32-byte key material. The private key is the 32-byte seed;
 * the public key is the 32-byte compressed point on Ed25519.
 */
export async function generateKeyPair(): Promise<KeyPair> {
  const { privateKey, publicKey } = generateKeyPairSync("ed25519");
  return {
    privateKey: exportPrivateKeyBytes(privateKey),
    publicKey: exportPublicKeyBytes(publicKey),
  };
}

/**
 * Export a Node KeyObject for an Ed25519 private key as 32 raw bytes.
 *
 * Node exports Ed25519 private keys in PKCS#8 DER format. The raw seed
 * occupies the last 32 bytes of that structure.
 */
export function exportPrivateKeyBytes(key: KeyObject): Uint8Array {
  const der = key.export({ type: "pkcs8", format: "der" }) as Buffer;
  if (der.length < 32) {
    throw new Error(
      `Unexpected PKCS8 DER length for Ed25519 private key: ${der.length}`,
    );
  }
  return new Uint8Array(der.buffer, der.byteOffset + der.length - 32, 32);
}

/**
 * Export a Node KeyObject for an Ed25519 public key as 32 raw bytes.
 *
 * Node exports Ed25519 public keys in SubjectPublicKeyInfo DER format.
 * The raw 32-byte point is always the last 32 bytes of that structure.
 */
export function exportPublicKeyBytes(key: KeyObject): Uint8Array {
  const der = key.export({ type: "spki", format: "der" }) as Buffer;
  if (der.length < 32) {
    throw new Error(
      `Unexpected SPKI DER length for Ed25519 public key: ${der.length}`,
    );
  }
  return new Uint8Array(der.buffer, der.byteOffset + der.length - 32, 32);
}

/**
 * Import a raw 32-byte Ed25519 private key seed into a Node KeyObject.
 *
 * Constructs a minimal PKCS#8 DER wrapper around the raw seed bytes.
 * The structure is fixed for Ed25519 and does not vary with key material.
 */
export function importPrivateKeyBytes(rawKey: Uint8Array): KeyObject {
  if (rawKey.length !== 32) {
    throw new Error(
      `Ed25519 private key must be 32 bytes, got ${rawKey.length}`,
    );
  }
  // PKCS#8 DER structure for Ed25519 private key (RFC 5958 / RFC 8410):
  //   30 2e             SEQUENCE
  //     02 01 00        INTEGER 0 (version)
  //     30 05           SEQUENCE (AlgorithmIdentifier)
  //       06 03 2b 65 70  OID 1.3.101.112 (id-Ed25519)
  //     04 22           OCTET STRING (OneAsymmetricKey)
  //       04 20         OCTET STRING (32-byte seed)
  //         [32 bytes of seed]
  const pkcs8 = new Uint8Array(48);
  pkcs8.set([
    0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70,
    0x04, 0x22, 0x04, 0x20,
  ]);
  pkcs8.set(rawKey, 16);
  return createPrivateKey({
    key: Buffer.from(pkcs8),
    format: "der",
    type: "pkcs8",
  });
}

/**
 * Import a raw 32-byte Ed25519 public key into a Node KeyObject.
 *
 * Constructs a minimal SubjectPublicKeyInfo DER wrapper around the raw
 * point bytes.
 */
export function importPublicKeyBytes(rawKey: Uint8Array): KeyObject {
  if (rawKey.length !== 32) {
    throw new Error(
      `Ed25519 public key must be 32 bytes, got ${rawKey.length}`,
    );
  }
  // SubjectPublicKeyInfo DER structure for Ed25519 public key (RFC 8410):
  //   30 2a             SEQUENCE
  //     30 05           SEQUENCE (AlgorithmIdentifier)
  //       06 03 2b 65 70  OID 1.3.101.112 (id-Ed25519)
  //     03 21           BIT STRING
  //       00            (0 unused bits)
  //       [32 bytes of compressed point]
  const spki = new Uint8Array(44);
  spki.set([
    0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
  ]);
  spki.set(rawKey, 12);
  return createPublicKey({
    key: Buffer.from(spki),
    format: "der",
    type: "spki",
  });
}

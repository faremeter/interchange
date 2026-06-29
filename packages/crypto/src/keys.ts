import type { KeyPair } from "@intx/types/runtime";

const ED25519 = { name: "Ed25519" } as const;

// PKCS#8 DER prefix for an Ed25519 private key (RFC 5958 / RFC 8410):
//   30 2e             SEQUENCE
//     02 01 00        INTEGER 0 (version)
//     30 05           SEQUENCE (AlgorithmIdentifier)
//       06 03 2b 65 70  OID 1.3.101.112 (id-Ed25519)
//     04 22           OCTET STRING (OneAsymmetricKey)
//       04 20         OCTET STRING (32-byte seed)
// The 32-byte raw seed follows. This framing is byte-identical to what
// `subtle.exportKey("pkcs8", ...)` emits for an Ed25519 private key, so
// `subtle.importKey("pkcs8", ...)` consumes the same structure.
const PKCS8_ED25519_PREFIX = new Uint8Array([
  0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04,
  0x22, 0x04, 0x20,
]);

// SubjectPublicKeyInfo DER prefix for an Ed25519 public key (RFC 8410):
//   30 2a             SEQUENCE
//     30 05           SEQUENCE (AlgorithmIdentifier)
//       06 03 2b 65 70  OID 1.3.101.112 (id-Ed25519)
//     03 21           BIT STRING
//       00            (0 unused bits)
// The 32-byte compressed point follows. Byte-identical to
// `subtle.exportKey("spki", ...)` output for an Ed25519 public key.
const SPKI_ED25519_PREFIX = new Uint8Array([
  0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
]);

/**
 * View a byte string as `ArrayBuffer`-backed so it satisfies the Web
 * Crypto `BufferSource` parameter types under TypeScript 5.9's generic
 * `Uint8Array<ArrayBufferLike>`. Funneling every `subtle.*` byte argument
 * through this one helper keeps the type workaround to a single
 * eslint-disable instead of scattering assertions across call sites.
 *
 * The assertion is type-only and erased at runtime: `subtle.*` copies and
 * validates its inputs, so a `SharedArrayBuffer`-backed view fails loudly
 * at the Web Crypto boundary rather than corrupting silently.
 *
 * See microsoft/TypeScript#62240.
 */
export function asArrayBuffer(b: Uint8Array): Uint8Array<ArrayBuffer> {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- ArrayBuffer-backed by construction at every call site; see microsoft/TypeScript#62240
  return b as Uint8Array<ArrayBuffer>;
}

/**
 * Generate a fresh Ed25519 key pair.
 *
 * Returns raw 32-byte key material. The private key is the 32-byte seed;
 * the public key is the 32-byte compressed point on Ed25519.
 *
 * Web Crypto exports the private key in PKCS#8 DER and the public key in
 * SubjectPublicKeyInfo DER; the raw 32 bytes occupy the last 32 bytes of
 * each structure.
 */
export async function generateKeyPair(): Promise<KeyPair> {
  const pair = await crypto.subtle.generateKey(ED25519, true, [
    "sign",
    "verify",
  ]);
  if (!("privateKey" in pair)) {
    throw new Error("Ed25519 generateKey did not return a key pair");
  }
  const pkcs8 = new Uint8Array(
    await crypto.subtle.exportKey("pkcs8", pair.privateKey),
  );
  const spki = new Uint8Array(
    await crypto.subtle.exportKey("spki", pair.publicKey),
  );
  if (pkcs8.length < 32 || spki.length < 32) {
    throw new Error(
      `Unexpected Ed25519 DER export lengths: pkcs8=${pkcs8.length}, spki=${spki.length}`,
    );
  }
  return {
    privateKey: pkcs8.slice(pkcs8.length - 32),
    publicKey: spki.slice(spki.length - 32),
  };
}

/**
 * Import a raw 32-byte Ed25519 private key seed into a Web Crypto
 * `CryptoKey` usable for signing.
 *
 * Wraps the raw seed in the fixed PKCS#8 DER structure that
 * `subtle.importKey("pkcs8", ...)` expects.
 */
export async function importPrivateKeyBytes(
  rawKey: Uint8Array,
): Promise<CryptoKey> {
  if (rawKey.length !== 32) {
    throw new Error(
      `Ed25519 private key must be 32 bytes, got ${rawKey.length}`,
    );
  }
  const pkcs8 = new Uint8Array(PKCS8_ED25519_PREFIX.length + 32);
  pkcs8.set(PKCS8_ED25519_PREFIX);
  pkcs8.set(rawKey, PKCS8_ED25519_PREFIX.length);
  return crypto.subtle.importKey(
    "pkcs8",
    asArrayBuffer(pkcs8),
    ED25519,
    false,
    ["sign"],
  );
}

/**
 * Import a raw 32-byte Ed25519 public key into a Web Crypto `CryptoKey`
 * usable for verification.
 *
 * Wraps the raw point in the fixed SubjectPublicKeyInfo DER structure
 * that `subtle.importKey("spki", ...)` expects.
 */
export async function importPublicKeyBytes(
  rawKey: Uint8Array,
): Promise<CryptoKey> {
  if (rawKey.length !== 32) {
    throw new Error(
      `Ed25519 public key must be 32 bytes, got ${rawKey.length}`,
    );
  }
  const spki = new Uint8Array(SPKI_ED25519_PREFIX.length + 32);
  spki.set(SPKI_ED25519_PREFIX);
  spki.set(rawKey, SPKI_ED25519_PREFIX.length);
  return crypto.subtle.importKey("spki", asArrayBuffer(spki), ED25519, false, [
    "verify",
  ]);
}

/**
 * Produce a raw 64-byte Ed25519 signature over `message` using the
 * 32-byte private key seed.
 *
 * This is the low-level signing primitive: no PGP or SSH framing, just
 * the RFC 8032 detached signature. Callers that need an envelope wrap
 * this output themselves.
 */
export async function signEd25519(
  seed: Uint8Array,
  message: Uint8Array,
): Promise<Uint8Array> {
  const key = await importPrivateKeyBytes(seed);
  const sig = await crypto.subtle.sign(ED25519, key, asArrayBuffer(message));
  return new Uint8Array(sig);
}

/**
 * Verify a raw Ed25519 signature over arbitrary data.
 *
 * Unlike `verifyDetachedSignature` which expects PGP-armored input,
 * this operates on raw bytes — suitable for challenge/response protocols
 * where no PGP framing is involved.
 */
export async function verifyEd25519(
  data: Uint8Array,
  signature: Uint8Array,
  publicKeyBytes: Uint8Array,
): Promise<boolean> {
  const publicKey = await importPublicKeyBytes(publicKeyBytes);
  return crypto.subtle.verify(
    ED25519,
    publicKey,
    asArrayBuffer(signature),
    asArrayBuffer(data),
  );
}

// IPC crypto primitives: raw Ed25519 sign/verify and HMAC-SHA256
// sign/verify. The control channel uses Ed25519 (the supervisor is the
// only signer; the child must not be able to forge supervisor commands);
// the event channel uses HMAC-SHA256 (both sides hold the same 32-byte
// key, and its per-frame cost keeps the stream affordable at
// InferenceEvent rates).
//
// Ed25519 comes from `@intx/crypto` -- bare 64-byte RFC 8032 signatures
// without the PGP framing the package's envelope helpers add, exactly the
// wire format this channel wants. HMAC-SHA256 uses Web Crypto `subtle`,
// verified by recomputing and comparing under an explicit constant-time
// XOR-accumulate rather than `subtle.verify`, whose constant-time
// behavior the spec does not guarantee.

import {
  signEd25519 as ed25519Sign,
  verifyEd25519 as ed25519Verify,
} from "@intx/crypto";
import { hexEncode } from "@intx/types";

const ED25519_SIGNATURE_BYTES = 64;
const ED25519_KEY_BYTES = 32;
const HMAC_KEY_BYTES = 32;
const HMAC_TAG_BYTES = 32;
const CHANNEL_ID_BYTES = 16;

/**
 * Mint the 32-byte HMAC key the supervisor passes to the child in
 * spawn-time env; the child never derives its own key or holds the
 * Ed25519 private key.
 */
export function generateHmacKey(): Uint8Array {
  return crypto.getRandomValues(new Uint8Array(HMAC_KEY_BYTES));
}

/**
 * Mint a fresh channelId: 16 bytes from `crypto.getRandomValues`,
 * hex-encoded so the value logs cleanly and round-trips through JSON.
 * The supervisor mints one per spawn and per recycle.
 */
export function generateChannelId(): string {
  return hexEncode(crypto.getRandomValues(new Uint8Array(CHANNEL_ID_BYTES)));
}

/**
 * Sign canonicalized envelope bytes with the supervisor's Ed25519
 * private key (32-byte seed); the caller owns canonicalization. Wraps
 * `@intx/crypto`'s raw primitive with fixed-length validation.
 */
export async function signEd25519(
  bytes: Uint8Array,
  privateKeySeed: Uint8Array,
): Promise<Uint8Array> {
  if (privateKeySeed.length !== ED25519_KEY_BYTES) {
    throw new Error(
      `IPC Ed25519 private key seed must be ${ED25519_KEY_BYTES} bytes, got ${privateKeySeed.length}`,
    );
  }
  return ed25519Sign(privateKeySeed, bytes);
}

export async function verifyEd25519(
  bytes: Uint8Array,
  signature: Uint8Array,
  publicKey: Uint8Array,
): Promise<boolean> {
  if (signature.length !== ED25519_SIGNATURE_BYTES) {
    throw new Error(
      `IPC Ed25519 signature must be ${ED25519_SIGNATURE_BYTES} bytes, got ${signature.length}`,
    );
  }
  if (publicKey.length !== ED25519_KEY_BYTES) {
    throw new Error(
      `IPC Ed25519 public key must be ${ED25519_KEY_BYTES} bytes, got ${publicKey.length}`,
    );
  }
  return ed25519Verify(bytes, signature, publicKey);
}

/**
 * Produce the 32-byte HMAC-SHA256 tag over the given envelope bytes
 * under the shared key; same primitive on both sides of the event
 * channel.
 */
export async function signHmac(
  bytes: Uint8Array,
  key: Uint8Array,
): Promise<Uint8Array> {
  if (key.length !== HMAC_KEY_BYTES) {
    throw new Error(
      `IPC HMAC key must be ${HMAC_KEY_BYTES} bytes, got ${key.length}`,
    );
  }
  const cryptoKey = await crypto.subtle.importKey(
    "raw",
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- ArrayBuffer-backed at the call site; Web Crypto's BufferSource type rejects Uint8Array<ArrayBufferLike> under TS 5.9 (microsoft/TypeScript#62240)
    key as Uint8Array<ArrayBuffer>,
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const tag = await crypto.subtle.sign(
    "HMAC",
    cryptoKey,
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- ArrayBuffer-backed at the call site; Web Crypto's BufferSource type rejects Uint8Array<ArrayBufferLike> under TS 5.9 (microsoft/TypeScript#62240)
    bytes as Uint8Array<ArrayBuffer>,
  );
  return new Uint8Array(tag);
}

/**
 * Constant-time byte comparison: the XOR accumulate is branch-free over
 * the byte range, so the position of the first mismatched byte is not
 * observable through timing. The only early return is a length mismatch,
 * which is not secret-dependent.
 */
function constantTimeEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) {
    return false;
  }
  let acc = 0;
  for (let i = 0; i < a.length; i++) {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- i is bounds-checked by the loop guard and a.length === b.length above
    acc |= (a[i] as number) ^ (b[i] as number);
  }
  return acc === 0;
}

/**
 * Verify an HMAC tag by recomputing and comparing in constant time.
 * Deliberately avoids `subtle.verify`, whose constant-time behavior the
 * Web Crypto spec does not guarantee.
 */
export async function verifyHmac(
  bytes: Uint8Array,
  tag: Uint8Array,
  key: Uint8Array,
): Promise<boolean> {
  if (tag.length !== HMAC_TAG_BYTES) {
    throw new Error(
      `IPC HMAC tag must be ${HMAC_TAG_BYTES} bytes, got ${tag.length}`,
    );
  }
  const expected = await signHmac(bytes, key);
  return constantTimeEqual(expected, tag);
}

export const IPC_CRYPTO = Object.freeze({
  ED25519_SIGNATURE_BYTES,
  ED25519_KEY_BYTES,
  HMAC_KEY_BYTES,
  HMAC_TAG_BYTES,
  CHANNEL_ID_BYTES,
});

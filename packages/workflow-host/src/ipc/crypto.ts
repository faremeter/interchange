// IPC crypto primitives: raw Ed25519 sign/verify and HMAC-SHA256 sign/verify.
//
// The control channel uses Ed25519 because the supervisor is the only
// signer and the child must not be able to forge supervisor commands.
// The event channel uses HMAC-SHA256 because both sides hold the same
// 32-byte secret and the cost-per-frame of HMAC over per-frame Ed25519
// is what keeps the event stream affordable at InferenceEvent rates.
//
// Ed25519 sign/verify come from `@intx/crypto`, whose raw
// `signEd25519`/`verifyEd25519` primitives produce and check the bare
// 64-byte RFC 8032 signature without the PGP packet framing and ASCII
// armor the package's envelope helpers add — exactly the wire format
// this channel wants. HMAC-SHA256 uses the Web Crypto `subtle` API; its
// tag is verified by recomputing the tag with `subtle.sign` and
// comparing under an explicit constant-time XOR-accumulate rather than
// `subtle.verify`, because the Web Crypto spec does not guarantee
// `verify` runs in constant time and this channel keeps ownership of
// that property. The wire format here is the raw 64-byte Ed25519
// signature and the raw 32-byte HMAC-SHA256 tag concatenated with the
// canonical-JSON payload bytes. Anything fancier would just pay PGP
// overhead per frame.

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
 * Mint a fresh control-channel HMAC key. Used by the supervisor at
 * spawn time. The child never derives its own key; it receives the
 * 32-byte secret in spawn-time env and never sees the Ed25519 private
 * key the supervisor uses on the control channel.
 */
export function generateHmacKey(): Uint8Array {
  return crypto.getRandomValues(new Uint8Array(HMAC_KEY_BYTES));
}

/**
 * Mint a fresh channelId per the channel-identity contract: 16 bytes
 * from `crypto.getRandomValues`, hex-encoded. The supervisor mints one at
 * every spawn and every recycle, passes it to the child in spawn-time
 * env, and rotates it on the next respawn. The hex encoding keeps the
 * value safe to log and round-trips cleanly through JSON.
 */
export function generateChannelId(): string {
  return hexEncode(crypto.getRandomValues(new Uint8Array(CHANNEL_ID_BYTES)));
}

/**
 * Sign the canonicalized envelope bytes with the supervisor's
 * Ed25519 private key. Caller is responsible for canonicalization;
 * this primitive does not see the structured envelope.
 *
 * The private-key bytes are the 32-byte Ed25519 seed. The raw signing
 * primitive lives in `@intx/crypto`; this module wraps it with the
 * channel's fixed-length validation.
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
 * Produce the 32-byte HMAC-SHA256 tag for the given canonicalized
 * envelope bytes under the shared key. Same primitive on both sides
 * of the event channel.
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
 * Constant-time byte comparison. A non-constant comparison would leak
 * the position of the first mismatched byte through a timing side
 * channel. The XOR accumulate is branch-free over the byte range; the
 * only early return is on a length mismatch, which is not secret-
 * dependent.
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
 * Verify an HMAC tag by recomputing it and comparing in constant time.
 * Deliberately avoids `subtle.verify`, whose constant-time behavior the
 * Web Crypto spec does not guarantee; this channel owns that property
 * via `constantTimeEqual`.
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

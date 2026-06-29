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
// this channel wants. HMAC-SHA256 still comes from Node's built-in
// `node:crypto`. The wire format here is the raw 64-byte Ed25519
// signature and the raw 32-byte HMAC-SHA256 tag concatenated with the
// canonical-JSON payload bytes. Anything fancier would just pay PGP
// overhead per frame.

import { createHmac, randomBytes, timingSafeEqual } from "node:crypto";

import {
  signEd25519 as ed25519Sign,
  verifyEd25519 as ed25519Verify,
} from "@intx/crypto";

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
  return new Uint8Array(randomBytes(HMAC_KEY_BYTES));
}

/**
 * Mint a fresh channelId per the channel-identity contract: 16 bytes
 * from `crypto.randomBytes`, hex-encoded. The supervisor mints one at
 * every spawn and every recycle, passes it to the child in spawn-time
 * env, and rotates it on the next respawn. The hex encoding keeps the
 * value safe to log and round-trips cleanly through JSON.
 */
export function generateChannelId(): string {
  return Buffer.from(randomBytes(CHANNEL_ID_BYTES)).toString("hex");
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
export function signHmac(bytes: Uint8Array, key: Uint8Array): Uint8Array {
  if (key.length !== HMAC_KEY_BYTES) {
    throw new Error(
      `IPC HMAC key must be ${HMAC_KEY_BYTES} bytes, got ${key.length}`,
    );
  }
  const mac = createHmac("sha256", key);
  mac.update(bytes);
  return new Uint8Array(mac.digest());
}

/**
 * Constant-time tag comparison for HMAC verification. A non-constant
 * comparison would leak the position of the first mismatched byte in
 * a timing side channel.
 */
export function verifyHmac(
  bytes: Uint8Array,
  tag: Uint8Array,
  key: Uint8Array,
): boolean {
  if (tag.length !== HMAC_TAG_BYTES) {
    throw new Error(
      `IPC HMAC tag must be ${HMAC_TAG_BYTES} bytes, got ${tag.length}`,
    );
  }
  const expected = signHmac(bytes, key);
  if (expected.length !== tag.length) {
    return false;
  }
  return timingSafeEqual(expected, tag);
}

export const IPC_CRYPTO = Object.freeze({
  ED25519_SIGNATURE_BYTES,
  ED25519_KEY_BYTES,
  HMAC_KEY_BYTES,
  HMAC_TAG_BYTES,
  CHANNEL_ID_BYTES,
});

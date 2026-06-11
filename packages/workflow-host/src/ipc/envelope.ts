// Frame envelope shared by both IPC channels.
//
// Every signed/HMACed frame carries `{ seq, channelId, payload }`
// inside the bytes the receiver authenticates. `seq` is a monotonic
// u64 counter the sender maintains per channel; `channelId` is the
// 16-byte hex identity the supervisor mints at every spawn and rotates
// at every recycle; `payload` is the channel-specific JSON value.
//
// Wire shape on disk for the control channel: one NDJSON line per
// frame, each line `{ envelope: <canonical-json-of-envelope>, sig:
// <hex Ed25519 signature> }`. For the event channel: a 4-byte big-
// endian length prefix, then `{ envelope, mac }` JSON in the same
// shape with `mac` carrying the hex HMAC tag. Both wires sign the
// canonical-JSON serialization of the envelope as a single bytestring.
// The on-wire byte representation of the envelope is exactly what the
// signer signs and the verifier verifies.
//
// Canonical JSON: keys appear in fixed insertion order (seq,
// channelId, payload). The payload's internal structure is preserved
// as the sender produced it; no recursive canonicalization is
// performed because the verifier never compares two structurally
// different serializations of the same logical value. Senders and
// receivers see the same bytes by construction (sender computes the
// signature over the exact serialization it then transmits; receiver
// verifies the signature over the exact bytes it received).

import { type } from "arktype";

/**
 * Validator for the inner envelope shape. `payload` is `unknown` here
 * because each channel narrows it further via its own typed schema
 * (control payload union vs. InferenceEvent forwarded over event).
 */
export const FrameEnvelope = type({
  seq: "number",
  channelId: "string",
  payload: "unknown",
});

export type FrameEnvelope = typeof FrameEnvelope.infer;

/**
 * Validator for the signed envelope wire shape. `sig` carries the
 * hex-encoded Ed25519 signature (128 hex chars / 64 bytes).
 */
export const SignedEnvelope = type({
  envelope: FrameEnvelope,
  sig: "string",
});

export type SignedEnvelope = typeof SignedEnvelope.infer;

/**
 * Validator for the MACed envelope wire shape. `mac` carries the
 * hex-encoded HMAC-SHA256 tag (64 hex chars / 32 bytes).
 */
export const MacedEnvelope = type({
  envelope: FrameEnvelope,
  mac: "string",
});

export type MacedEnvelope = typeof MacedEnvelope.infer;

/**
 * Produce the canonical byte serialization of an envelope. The
 * sender signs these bytes; the receiver verifies against these
 * bytes. Both sides reach the same bytestring deterministically
 * because the JSON serialization runs in insertion order over a
 * fixed-shape object.
 */
export function encodeEnvelope(envelope: FrameEnvelope): Uint8Array {
  const ordered = {
    seq: envelope.seq,
    channelId: envelope.channelId,
    payload: envelope.payload,
  };
  return new TextEncoder().encode(JSON.stringify(ordered));
}

/**
 * Parse a canonical envelope byte serialization back into the
 * structured shape. Used on the receiver side after the per-frame
 * MAC/signature check passes -- a structural failure on a frame
 * whose authentication tag matched is a programming bug at the
 * sender, not a tampering signal.
 */
export function decodeEnvelope(bytes: Uint8Array): FrameEnvelope {
  let parsed: unknown;
  try {
    parsed = JSON.parse(new TextDecoder().decode(bytes));
  } catch (cause) {
    throw new Error("IPC envelope bytes are not valid JSON", { cause });
  }
  const validated = FrameEnvelope(parsed);
  if (validated instanceof type.errors) {
    throw new Error(`IPC envelope failed validation: ${validated.summary}`);
  }
  return validated;
}

/**
 * Hex-encode a byte string. Centralized helper so the channel
 * implementations don't depend on `Buffer` directly.
 */
export function hexEncode(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString("hex");
}

/**
 * Decode a hex string back into bytes. Throws on non-hex input rather
 * than producing a truncated buffer the verifier would reject with a
 * less-clear error.
 */
export function hexDecode(hex: string): Uint8Array {
  if (!/^[0-9a-fA-F]*$/.test(hex)) {
    throw new Error("IPC hex value contains non-hex characters");
  }
  if (hex.length % 2 !== 0) {
    throw new Error("IPC hex value has odd length");
  }
  return new Uint8Array(Buffer.from(hex, "hex"));
}

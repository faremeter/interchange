// Frame envelope shared by both IPC channels: `{ seq, channelId, payload }`
// inside the authenticated bytes. `seq` is a monotonic u64 per channel;
// `channelId` is the 16-byte hex identity the supervisor mints at spawn
// and rotates at recycle.
//
// Wire shape: one NDJSON line per frame, `{ envelope: <canonical-json>,
// sig: <hex Ed25519> }` on the control channel, `{ envelope, mac }` with
// the hex HMAC tag on the event channel. Both wires sign the canonical
// JSON serialization of the envelope as one bytestring; canonical JSON =
// fixed insertion order (seq, channelId, payload), no recursive payload
// canonicalization because the verifier never compares two serializations
// of the same logical value.

import { type } from "arktype";

/**
 * Inner envelope shape; `payload` is `unknown` because each channel
 * narrows it via its own schema.
 */
export const FrameEnvelope = type({
  seq: "number",
  channelId: "string",
  payload: "unknown",
});

export type FrameEnvelope = typeof FrameEnvelope.infer;

/**
 * Signed wire envelope; `sig` is the hex Ed25519 signature (64 bytes).
 */
export const SignedEnvelope = type({
  envelope: FrameEnvelope,
  sig: "string",
});

export type SignedEnvelope = typeof SignedEnvelope.infer;

/**
 * MACed wire envelope; `mac` is the hex HMAC-SHA256 tag (32 bytes).
 */
export const MacedEnvelope = type({
  envelope: FrameEnvelope,
  mac: "string",
});

export type MacedEnvelope = typeof MacedEnvelope.infer;

/**
 * Canonical byte serialization of an envelope: the sender signs these
 * bytes, the receiver verifies them; both sides reach the same bytestring
 * because the JSON runs in insertion order over a fixed-shape object.
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
 * Parse a canonical envelope serialization back into the structured
 * shape, used after the per-frame MAC/signature check passes -- a
 * structural failure on an authenticated frame is a sender bug, not
 * tampering.
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

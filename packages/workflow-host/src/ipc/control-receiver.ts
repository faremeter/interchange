// Control channel: NDJSON over stdio, Ed25519-signed per direction.
//
// Two Ed25519 keypairs flow per spawn:
//   - Supervisor's keypair. The supervisor holds the private half and
//     signs every downstream (supervisor->child) frame. The matching
//     public half is passed to the child in spawn-time env
//     (`HOST_PUBKEY`) and the child verifies downstream frames
//     against it. The supervisor's PRIVATE KEY NEVER LEAVES THE
//     SUPERVISOR'S ADDRESS SPACE.
//   - Child's keypair. The child mints it at startup, holds the
//     private half in its own address space, and signs every upstream
//     (child->supervisor) frame. The matching public half rides as
//     `childPublicKey` on the upstream `ready` frame's payload; the
//     supervisor extracts it on receive and uses it to verify
//     subsequent upstream frames. The CHILD'S PRIVATE KEY NEVER
//     LEAVES THE CHILD'S ADDRESS SPACE.
//
// Upstream `ready` bootstraps the supervisor's view of the child's
// public key. The supervisor's receiver opens in bootstrap mode:
// the first frame's envelope is parsed structurally so the
// supervisor can extract `childPublicKey` from the payload, then the
// signature is verified against that key. Subsequent upstream frames
// verify against the same key. A child-signed frame whose claimed
// `childPublicKey` does not match the bootstrap value (or any
// non-`ready` first frame) crashes the receiver.
//
// Wire format: one signed envelope per line. Each line is the JSON
// serialization of `{ envelope: { seq, channelId, payload }, sig:
// <hex Ed25519> }`. The signature covers the canonical bytes of the
// envelope sub-object (see `envelope.ts`).
//
// Payload schemas live in control-payloads.ts. The receiver selects the schema
// for the message's discriminator; inference events use the separate event
// channel and cannot satisfy a control payload schema.

import { type } from "arktype";
import { hexDecode } from "@intx/types/hex";
import { encodeEnvelope, SignedEnvelope } from "./envelope";
import { verifyEd25519 } from "./crypto";
import { parseControlPayload, type ControlPayload } from "./control-payloads";
import type { NdjsonReader } from "./control-sender";

export interface ControlChannelReceiverOpts {
  /**
   * Public key used to verify every inbound frame. When `Uint8Array`
   * the value is fixed at construction time (the child's downstream
   * receiver uses the supervisor's pubkey from `HOST_PUBKEY`). When
   * `{ bootstrapFromReady: true }` the receiver opens in
   * bootstrap mode: the first frame must be `ready` and must carry
   * a `childPublicKey` hex-encoded Ed25519 public key in its payload.
   * The receiver extracts the key, verifies the first frame's
   * signature against it, then continues verifying subsequent frames
   * against the same key. The supervisor's upstream receiver opens
   * in bootstrap mode so the child can publish its own public key
   * over the wire without the supervisor ever holding the matching
   * private half.
   */
  publicKey: Uint8Array | { bootstrapFromReady: true };
  channelId: string;
  reader: NdjsonReader;
  /**
   * Invoked when any invariant is violated: signature failure,
   * channelId mismatch, non-monotonic seq, malformed payload. The
   * receiver's contract is to crash on any such violation. The
   * caller wires this to a process-exit path; tests inject a
   * recorder to assert on the failure mode.
   */
  onCrash: (reason: string) => void;
}

/**
 * Construct the child-side control-channel receiver. Yields one
 * verified, in-order `ControlPayload` per call. Any frame that
 * fails verification, carries a non-current channelId, or arrives
 * out of order calls `onCrash` and ends the iterator.
 */
export async function* receiveControlChannel(
  opts: ControlChannelReceiverOpts,
): AsyncGenerator<ControlPayload, void, void> {
  let highestSeq = 0;
  let activePublicKey: Uint8Array | null =
    opts.publicKey instanceof Uint8Array ? opts.publicKey : null;
  const bootstrapping = activePublicKey === null;
  for await (const line of opts.reader.read()) {
    if (line.length === 0) continue;

    let raw: unknown;
    try {
      raw = JSON.parse(line);
    } catch (cause) {
      opts.onCrash(
        `control channel received non-JSON line: ${errorMessage(cause)}`,
      );
      return;
    }

    const signed = SignedEnvelope(raw);
    if (signed instanceof type.errors) {
      opts.onCrash(
        `control channel envelope failed validation: ${signed.summary}`,
      );
      return;
    }

    let envelopeBytes: Uint8Array;
    try {
      envelopeBytes = encodeEnvelope(signed.envelope);
    } catch (cause) {
      opts.onCrash(
        `control channel envelope re-encode failed: ${errorMessage(cause)}`,
      );
      return;
    }

    let sigBytes: Uint8Array;
    try {
      sigBytes = hexDecode(signed.sig);
    } catch (cause) {
      opts.onCrash(
        `control channel signature decode failed: ${errorMessage(cause)}`,
      );
      return;
    }

    if (activePublicKey === null) {
      // Bootstrap mode: the first frame must be `ready`. Extract the
      // child's public key from the payload, then verify the
      // first frame's signature against it. The receiver crashes if
      // the payload is not a `ready` frame or carries a malformed
      // `childPublicKey`.
      const candidate = parseControlPayload(signed.envelope.payload);
      if (candidate instanceof type.errors) {
        opts.onCrash(
          `control channel bootstrap payload failed validation: ${candidate.summary}`,
        );
        return;
      }
      if (candidate.type !== "ready") {
        opts.onCrash(
          `control channel bootstrap expected a ready frame, got ${candidate.type}`,
        );
        return;
      }
      let bootstrapKey: Uint8Array;
      try {
        bootstrapKey = hexDecode(candidate.data.childPublicKey);
      } catch (cause) {
        opts.onCrash(
          `control channel bootstrap childPublicKey decode failed: ${errorMessage(cause)}`,
        );
        return;
      }
      activePublicKey = bootstrapKey;
    }

    const ok = await verifyEd25519(envelopeBytes, sigBytes, activePublicKey);
    if (!ok) {
      opts.onCrash(
        `control channel signature did not verify (seq=${String(signed.envelope.seq)}, channelId=${signed.envelope.channelId}${bootstrapping ? "; bootstrap" : ""})`,
      );
      return;
    }

    if (signed.envelope.channelId !== opts.channelId) {
      opts.onCrash(
        `control channel channelId mismatch: expected ${opts.channelId}, got ${signed.envelope.channelId} at seq=${String(signed.envelope.seq)}`,
      );
      return;
    }

    if (signed.envelope.seq <= highestSeq) {
      opts.onCrash(
        `control channel out-of-order seq: expected > ${String(highestSeq)}, got ${String(signed.envelope.seq)}`,
      );
      return;
    }
    if (signed.envelope.seq !== highestSeq + 1) {
      opts.onCrash(
        `control channel seq gap: expected ${String(highestSeq + 1)}, got ${String(signed.envelope.seq)}`,
      );
      return;
    }
    highestSeq = signed.envelope.seq;

    const payload = parseControlPayload(signed.envelope.payload);
    if (payload instanceof type.errors) {
      opts.onCrash(
        `control channel payload failed validation: ${payload.summary}`,
      );
      return;
    }

    yield payload;
  }
}

function errorMessage(cause: unknown): string {
  if (cause instanceof Error) return cause.message;
  return String(cause);
}

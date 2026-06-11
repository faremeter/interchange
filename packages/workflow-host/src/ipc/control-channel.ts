// Control channel: NDJSON over stdio, Ed25519-signed by the supervisor.
//
// The supervisor sends control frames; the workflow-process child
// verifies and applies them. The supervisor holds the Ed25519
// private key in its own address space and never hands it to the
// child -- the child only receives the matching public key in the
// spawn-time env (`HOST_PUBKEY`). The signing direction is one-way:
// child-to-supervisor traffic does not flow over this channel
// (replies come back via the event channel or by submitting commits
// against the workflow-run repo the supervisor observes).
//
// Wire format: one signed envelope per line. Each line is the JSON
// serialization of `{ envelope: { seq, channelId, payload }, sig:
// <hex Ed25519> }`. The signature covers the canonical bytes of the
// envelope sub-object (see `envelope.ts`).
//
// Payload union: every legal payload's discriminator lives in this
// module. The mixing-the-two-channels failure mode -- a "control"
// frame whose payload is structurally an InferenceEvent -- is
// prevented at the type level by the disjoint discriminated union.

import { type } from "arktype";

import {
  decodeEnvelope,
  encodeEnvelope,
  FrameEnvelope,
  hexDecode,
  hexEncode,
  SignedEnvelope,
} from "./envelope";
import { signEd25519, verifyEd25519 } from "./crypto";

/**
 * Discriminated union of every control-channel payload kind. The
 * `type` discriminator namespaces the control-plane vocabulary so a
 * future addition (e.g. `connector-bind`) lands by extending this
 * union and not by widening the envelope shape. Inference events
 * NEVER appear here; they ride the event channel.
 */
export const ControlPayload = type(
  {
    type: "'trigger.fire'",
    data: {
      runId: "string",
      messageId: "string",
      receivedAt: "number",
    },
  },
  "|",
  {
    type: "'signal.deliver'",
    data: {
      runId: "string",
      signalName: "string",
      signalId: "string",
      payload: "unknown",
    },
  },
)
  .or({
    type: "'drain'",
    data: {
      deadlineMs: "number",
    },
  })
  .or({
    type: "'recycle'",
    data: {
      reason: "string",
    },
  })
  .or({
    type: "'shutdown'",
    data: {
      reason: "string",
    },
  })
  .or({
    type: "'grants-updated'",
    data: {
      "stepHashes?": "Record<string, string>",
    },
  })
  .or({
    type: "'sources-updated'",
    data: {
      "sourceHashes?": "Record<string, string>",
    },
  })
  .or({
    type: "'ready'",
    data: {
      childPid: "number",
    },
  });

export type ControlPayload = typeof ControlPayload.infer;

export interface NdjsonWriter {
  write(line: string): Promise<void> | void;
}

export interface NdjsonReader {
  read(): AsyncIterableIterator<string>;
}

export interface ControlChannelSenderOpts {
  privateKeySeed: Uint8Array;
  channelId: string;
  writer: NdjsonWriter;
}

export interface ControlChannelSender {
  send(payload: ControlPayload): Promise<void>;
  readonly seq: number;
}

/**
 * Construct the supervisor-side control-channel sender. The
 * supervisor's Ed25519 seed lives in closure. The matching public
 * key flows to the child through spawn-time env -- never the seed.
 */
export function createControlChannelSender(
  opts: ControlChannelSenderOpts,
): ControlChannelSender {
  let seq = 0;
  return {
    get seq() {
      return seq;
    },
    async send(payload: ControlPayload) {
      seq += 1;
      const envelope: FrameEnvelope = {
        seq,
        channelId: opts.channelId,
        payload,
      };
      const envelopeBytes = encodeEnvelope(envelope);
      const sig = signEd25519(envelopeBytes, opts.privateKeySeed);
      const signed: SignedEnvelope = {
        envelope,
        sig: hexEncode(sig),
      };
      await opts.writer.write(JSON.stringify(signed) + "\n");
    },
  };
}

export interface ControlChannelReceiverOpts {
  publicKey: Uint8Array;
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

    const ok = verifyEd25519(envelopeBytes, sigBytes, opts.publicKey);
    if (!ok) {
      opts.onCrash(
        `control channel signature did not verify (seq=${String(signed.envelope.seq)}, channelId=${signed.envelope.channelId})`,
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

    const payload = ControlPayload(signed.envelope.payload);
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

/**
 * Re-export the envelope decoder for callers that need to inspect
 * a control frame's envelope without going through the receiver
 * iterator (testing harnesses that fuzz the wire format).
 */
export { decodeEnvelope };
